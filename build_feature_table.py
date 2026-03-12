#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build training feature table from one or more netkeiba race_result CSV files.

Inputs:
  - one or more race_result_*.csv files produced by the scraper/parser
Output:
  - feature_table.csv (one horse per row)

Main design:
  - Use only information available before each target race.
  - Keep maiden races in history, but exclude rows whose target race_class is 新馬 by default.
  - Add previous-race, recent-form, and jockey-based features.

Example:
  python build_feature_table.py \
    --inputs ./data/netkeiba_2025/race_result_2025.csv ./data/netkeiba_2026_01/race_result_2026-01.csv \
    --output ./features/feature_table_2025_2026-01.csv
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd


DISTANCE_BINS = [0, 1400, 1800, 2200, 10000]
DISTANCE_BIN_LABELS = ["short", "mile", "middle", "long"]


def read_csvs(paths: List[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        path = Path(p)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {p}")
        df = pd.read_csv(path, dtype=str)
        df["source_file"] = str(path)
        frames.append(df)
    if not frames:
        raise ValueError("No input CSVs provided.")
    return pd.concat(frames, ignore_index=True)


def to_numeric(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def normalize_text(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()


def prepare_base(df: pd.DataFrame) -> pd.DataFrame:
    normalize_text(
        df,
        [
            "race_id", "race_date", "place", "surface", "race_class", "grade",
            "weather", "ground_state", "horse_id", "horse_name", "sex",
            "jockey_id", "jockey_name", "trainer_id", "trainer_name",
            "margin", "corner_pass",
        ],
    )

    to_numeric(
        df,
        [
            "year", "distance", "field_size", "age", "gate", "horse_number",
            "assigned_weight", "finish_position", "popularity", "win_odds",
            "last3f", "body_weight", "body_weight_diff",
        ],
    )

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_id", "horse_id", "race_date"]).copy()

    # Stable row ordering inside the same day.
    sort_cols = ["horse_id", "race_date", "race_id"]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    # Distance band.
    df["distance_band"] = pd.cut(
        df["distance"],
        bins=DISTANCE_BINS,
        labels=DISTANCE_BIN_LABELS,
        right=False,
        include_lowest=True,
    ).astype(str)

    # Targets.
    df["is_top3"] = np.where(df["finish_position"].between(1, 3, inclusive="both"), 1, 0)
    df["is_win"] = np.where(df["finish_position"] == 1, 1, 0)

    return df


TARGET_EXCLUDE_RACE_CLASS = {"新馬"}


def add_previous_race_features(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("horse_id", sort=False)

    prev_map = {
        "finish_position": "prev_finish_position",
        "popularity": "prev_popularity",
        "win_odds": "prev_win_odds",
        "last3f": "prev_last3f",
        "margin": "prev_margin",
        "distance": "prev_distance",
        "surface": "prev_surface",
        "ground_state": "prev_ground_state",
        "body_weight": "prev_body_weight",
        "body_weight_diff": "prev_body_weight_diff",
        "jockey_id": "prev_jockey_id",
        "race_class": "prev_race_class",
        "grade": "prev_grade",
        "place": "prev_place",
        "race_date": "prev_race_date",
    }
    for src, dst in prev_map.items():
        if src in df.columns:
            df[dst] = g[src].shift(1)

    df["prev_days_since"] = (df["race_date"] - df["prev_race_date"]).dt.days
    df["same_jockey_as_prev"] = np.where(
        (df["jockey_id"] != "") & (df["jockey_id"] == df["prev_jockey_id"]), 1, 0
    )
    return df



def _past_n_stat(series: pd.Series, n: int, func: str) -> pd.Series:
    shifted = series.shift(1)
    roll = shifted.rolling(window=n, min_periods=1)
    if func == "mean":
        return roll.mean()
    if func == "sum":
        return roll.sum()
    raise ValueError(func)



def add_recent_form_features(df: pd.DataFrame) -> pd.DataFrame:
    def apply_group(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values(["race_date", "race_id"]).copy()
        g["avg_finish_position_3"] = _past_n_stat(g["finish_position"], 3, "mean")
        g["avg_popularity_3"] = _past_n_stat(g["popularity"], 3, "mean")
        g["avg_last3f_3"] = _past_n_stat(g["last3f"], 3, "mean")
        g["top3_count_5"] = _past_n_stat(g["is_top3"], 5, "sum")

        shifted_surface = g["surface"].shift(1)
        shifted_band = g["distance_band"].shift(1)
        shifted_finish = g["finish_position"].shift(1)

        same_surface_counts = []
        same_band_counts = []
        for i in range(len(g)):
            start = max(0, i - 5)
            hist_surface = shifted_surface.iloc[start:i]
            hist_band = shifted_band.iloc[start:i]
            cur_surface = g.iloc[i]["surface"]
            cur_band = g.iloc[i]["distance_band"]
            same_surface_counts.append(int((hist_surface == cur_surface).sum()))
            same_band_counts.append(int((hist_band == cur_band).sum()))

        g["same_surface_count_5"] = same_surface_counts
        g["same_distance_band_count_5"] = same_band_counts
        return g

    return (
        df.groupby("horse_id", group_keys=False, sort=False)
        .apply(apply_group)
        .reset_index(drop=True)
    )



def add_jockey_features(df: pd.DataFrame) -> pd.DataFrame:
    # Jockey recent form using only prior rides.
    def apply_jockey(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values(["race_date", "race_id"]).copy()
        g["jockey_top3_rate_30"] = g["is_top3"].shift(1).rolling(30, min_periods=3).mean()
        g["jockey_win_rate_30"] = g["is_win"].shift(1).rolling(30, min_periods=3).mean()
        g["jockey_avg_popularity_30"] = g["popularity"].shift(1).rolling(30, min_periods=3).mean()
        return g

    df = (
        df.groupby("jockey_id", group_keys=False, sort=False)
        .apply(apply_jockey)
        .reset_index(drop=True)
    )

    # Horse x jockey historical relationship.
    df = df.sort_values(["horse_id", "jockey_id", "race_date", "race_id"]).copy()
    hj = df.groupby(["horse_id", "jockey_id"], sort=False)
    df["horse_jockey_past_count"] = hj.cumcount()
    df["horse_jockey_avg_finish"] = hj["finish_position"].transform(lambda s: s.shift(1).expanding().mean())
    df["horse_jockey_top3_rate"] = hj["is_top3"].transform(lambda s: s.shift(1).expanding().mean())

    # Jockey x surface / place recent rates.
    for key_name, by_cols in {
        "jockey_surface": ["jockey_id", "surface"],
        "jockey_place": ["jockey_id", "place"],
        "jockey_class": ["jockey_id", "race_class"],
    }.items():
        grp = df.groupby(by_cols, sort=False)
        df[f"{key_name}_top3_rate"] = grp["is_top3"].transform(lambda s: s.shift(1).expanding().mean())

    return df



def select_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    wanted = [
        # current race / horse info
        "race_id", "race_date", "place_code", "place", "kai", "day", "race_num",
        "race_name", "grade", "surface", "distance", "turn_direction", "course_inout",
        "track_variant", "weather", "ground_state", "field_size", "race_class",
        "age_condition", "sex_condition", "weight_condition",
        "horse_id", "horse_name", "sex", "age", "gate", "horse_number",
        "jockey_id", "jockey_name", "assigned_weight", "trainer_id", "trainer_name",
        "body_weight", "body_weight_diff",
        # prev race
        "prev_finish_position", "prev_popularity", "prev_win_odds", "prev_last3f",
        "prev_margin", "prev_distance", "prev_surface", "prev_ground_state",
        "prev_body_weight", "prev_body_weight_diff", "prev_days_since",
        "prev_jockey_id", "same_jockey_as_prev", "prev_race_class", "prev_grade",
        "prev_place",
        # recent form
        "avg_finish_position_3", "avg_popularity_3", "avg_last3f_3", "top3_count_5",
        "same_surface_count_5", "same_distance_band_count_5",
        # jockey
        "jockey_top3_rate_30", "jockey_win_rate_30", "jockey_avg_popularity_30",
        "horse_jockey_past_count", "horse_jockey_avg_finish", "horse_jockey_top3_rate",
        "jockey_surface_top3_rate", "jockey_place_top3_rate", "jockey_class_top3_rate",
        # targets
        "finish_position", "is_top3", "is_win", "popularity", "win_odds", "last3f", "margin",
    ]
    cols = [c for c in wanted if c in df.columns]
    return df[cols].copy()



def build_features(df: pd.DataFrame, include_newma: bool = False) -> pd.DataFrame:
    df = prepare_base(df)
    df = add_previous_race_features(df)
    df = add_recent_form_features(df)
    df = add_jockey_features(df)

    # Exclude target rows for 新馬 by default, but still use them in history.
    if not include_newma:
        df = df[~df["race_class"].isin(TARGET_EXCLUDE_RACE_CLASS)].copy()

    # Optional convenience flags.
    df["has_prev_race"] = np.where(df["prev_race_date"].notna(), 1, 0)

    return select_output_columns(df).sort_values(["race_date", "race_id", "horse_number"], na_position="last")



def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build horse racing feature table from race_result CSV files.")
    p.add_argument("--inputs", nargs="+", required=True, help="Input race_result CSV files.")
    p.add_argument("--output", required=True, help="Output feature_table CSV path.")
    p.add_argument(
        "--include-shinba-target",
        action="store_true",
        help="Include 新馬 as target rows as well. By default, 新馬 is used only as history.",
    )
    return p.parse_args()



def main() -> None:
    args = parse_args()
    raw = read_csvs(args.inputs)
    feat = build_features(raw, include_newma=args.include_shinba_target)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    feat.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"saved: {out}")
    print(f"rows: {len(feat)}")
    print(f"cols: {len(feat.columns)}")

    # Lightweight missingness summary for key prior fields.
    key_prev_cols = [
        "prev_finish_position", "prev_popularity", "prev_win_odds", "prev_last3f", "prev_days_since"
    ]
    print("missing_ratio:")
    for c in key_prev_cols:
        if c in feat.columns:
            ratio = float(feat[c].isna().mean())
            print(f"  {c}: {ratio:.3f}")


if __name__ == "__main__":
    main()
