import argparse
import json
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd


DEFAULT_WIN_EV_THRESHOLDS = [1.00, 1.05, 1.10, 1.20, 1.50]
DEFAULT_PLACE_EV_THRESHOLDS = [1.00, 1.02, 1.05, 1.10, 1.20]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate expected value and ROI for win/place betting from prediction files."
    )
    parser.add_argument(
        "--pred-win-files",
        nargs="+",
        required=True,
        help="Path(s) to win model valid_predictions.csv",
    )
    parser.add_argument(
        "--pred-top3-files",
        nargs="+",
        required=True,
        help="Path(s) to top3 model valid_predictions.csv",
    )
    parser.add_argument(
        "--feature-files",
        nargs="+",
        required=True,
        help="Path(s) to feature_table csv used to recover horse_number from horse_id",
    )
    parser.add_argument(
        "--payout-files",
        nargs="+",
        required=True,
        help="Path(s) to horse-level payout csv (payback_win_place_*.csv)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to save merged detail and summary files",
    )
    parser.add_argument(
        "--win-ev-thresholds",
        nargs="*",
        type=float,
        default=DEFAULT_WIN_EV_THRESHOLDS,
        help="Thresholds for EV of win bet. Default: 1.00 1.05 1.10 1.20 1.50",
    )
    parser.add_argument(
        "--place-ev-thresholds",
        nargs="*",
        type=float,
        default=DEFAULT_PLACE_EV_THRESHOLDS,
        help="Thresholds for EV of place bet. Default: 1.00 1.02 1.05 1.10 1.20",
    )
    parser.add_argument(
        "--min-win-prob",
        type=float,
        default=0.0,
        help="Minimum pred_win to allow a win bet",
    )
    parser.add_argument(
        "--min-place-prob",
        type=float,
        default=0.0,
        help="Minimum pred_top3 to allow a place bet",
    )
    parser.add_argument(
        "--max-bets-per-race-win",
        type=int,
        default=0,
        help="If >0, keep only top N horses by win EV per race",
    )
    parser.add_argument(
        "--max-bets-per-race-place",
        type=int,
        default=0,
        help="If >0, keep only top N horses by place EV per race",
    )
    return parser.parse_args()


def load_concat_csv(paths: List[str], label: str) -> pd.DataFrame:
    dfs = []
    for p in paths:
        path = Path(p)
        if not path.exists():
            raise FileNotFoundError(f"{label} file not found: {path}")
        dfs.append(pd.read_csv(path, low_memory=False))
    if not dfs:
        raise ValueError(f"No {label} files given")
    return pd.concat(dfs, ignore_index=True)


def load_feature_mapping(feature_files: List[str]) -> pd.DataFrame:
    dfs = []
    use_cols = ["race_id", "race_date", "horse_id", "horse_number", "horse_name"]
    for p in feature_files:
        path = Path(p)
        if not path.exists():
            raise FileNotFoundError(f"feature file not found: {path}")
        df = pd.read_csv(path, usecols=lambda c: c in use_cols, low_memory=False)
        dfs.append(df)

    feat = pd.concat(dfs, ignore_index=True)

    required = {"race_id", "race_date", "horse_id", "horse_number"}
    missing = required - set(feat.columns)
    if missing:
        raise ValueError(f"feature mapping missing columns: {missing}")

    feat["race_id"] = feat["race_id"].astype(str)
    feat["horse_id"] = feat["horse_id"].astype(str)
    feat["race_date"] = pd.to_datetime(feat["race_date"], errors="coerce")
    feat["horse_number"] = pd.to_numeric(feat["horse_number"], errors="coerce")

    feat = feat.dropna(subset=["race_id", "horse_id", "race_date", "horse_number"]).copy()
    feat["horse_number"] = feat["horse_number"].astype(int)

    feat = (
        feat.groupby(["race_id", "horse_id"], as_index=False)
        .agg(
            {
                "race_date": "first",
                "horse_number": "first",
                "horse_name": "first" if "horse_name" in feat.columns else "size",
            }
        )
        .reset_index(drop=True)
    )
    return feat


def load_prediction_files(pred_files: List[str], prob_col_name: str) -> pd.DataFrame:
    pred = load_concat_csv(pred_files, prob_col_name)

    required = {"race_id", "race_date", "horse_id", "pred_proba"}
    missing = required - set(pred.columns)
    if missing:
        raise ValueError(f"prediction file for {prob_col_name} missing columns: {missing}")

    pred = pred.copy()
    pred["race_id"] = pred["race_id"].astype(str)
    pred["horse_id"] = pred["horse_id"].astype(str)
    pred["race_date"] = pd.to_datetime(pred["race_date"], errors="coerce")
    pred = pred.dropna(subset=["race_id", "horse_id", "race_date"]).copy()

    keep_cols = [c for c in pred.columns if c in {
        "race_id", "race_date", "horse_id", "horse_name",
        "finish_position", "is_top3", "is_win",
        "pred_proba", "pred_rank_in_race",
        "race_class", "grade", "place", "surface", "distance",
        "jockey_id", "jockey_name"
    }]
    pred = pred[keep_cols].copy()

    pred = pred.rename(columns={"pred_proba": prob_col_name})

    # 同一 race_id + horse_id が複数あれば先頭を採用
    pred = pred.sort_values(["race_date", "race_id", "horse_id"]).drop_duplicates(
        subset=["race_id", "horse_id"],
        keep="first",
    )

    return pred


def load_payout_files(payout_files: List[str]) -> pd.DataFrame:
    payout = load_concat_csv(payout_files, "payout")

    required = {
        "race_id", "race_date", "horse_number",
        "win_hit", "win_payout_yen", "place_hit", "place_payout_yen"
    }
    missing = required - set(payout.columns)
    if missing:
        raise ValueError(f"payout file missing columns: {missing}")

    payout = payout.copy()
    payout["race_id"] = payout["race_id"].astype(str)
    payout["race_date"] = pd.to_datetime(payout["race_date"], errors="coerce")
    payout["horse_number"] = pd.to_numeric(payout["horse_number"], errors="coerce")
    payout = payout.dropna(subset=["race_id", "race_date", "horse_number"]).copy()
    payout["horse_number"] = payout["horse_number"].astype(int)

    for c in ["win_hit", "win_payout_yen", "place_hit", "place_payout_yen"]:
        payout[c] = pd.to_numeric(payout[c], errors="coerce").fillna(0)

    opt_cols = [c for c in ["win_popularity", "place_popularity"] if c in payout.columns]
    keep_cols = [
        "race_id", "race_date", "horse_number",
        "win_hit", "win_payout_yen", "place_hit", "place_payout_yen",
        *opt_cols
    ]
    payout = payout[keep_cols].copy()

    payout = payout.sort_values(["race_date", "race_id", "horse_number"]).drop_duplicates(
        subset=["race_id", "horse_number"],
        keep="first",
    )

    return payout


def build_merged_table(
    pred_win: pd.DataFrame,
    pred_top3: pd.DataFrame,
    feature_map: pd.DataFrame,
    payout: pd.DataFrame,
) -> pd.DataFrame:
    merged = pred_win.merge(
        pred_top3[["race_id", "horse_id", "pred_top3"]],
        on=["race_id", "horse_id"],
        how="inner",
        validate="one_to_one",
    )

    merged = merged.merge(
        feature_map[["race_id", "horse_id", "race_date", "horse_number"]],
        on=["race_id", "horse_id"],
        how="left",
        validate="many_to_one",
    )

    if merged["horse_number"].isna().any():
        missing = int(merged["horse_number"].isna().sum())
        raise ValueError(f"horse_number could not be recovered for {missing} rows")

    merged["horse_number"] = merged["horse_number"].astype(int)

    merged = merged.merge(
        payout,
        on=["race_id", "horse_number"],
        how="left",
        validate="many_to_one",
        suffixes=("", "_payout"),
    )

    for c in ["win_hit", "win_payout_yen", "place_hit", "place_payout_yen"]:
        if c not in merged.columns:
            merged[c] = 0
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    merged["pred_win"] = pd.to_numeric(merged["pred_win"], errors="coerce")
    merged["pred_top3"] = pd.to_numeric(merged["pred_top3"], errors="coerce")

    # 100円ベット前提の期待回収率
    merged["ev_win"] = merged["pred_win"] * (merged["win_payout_yen"] / 100.0)
    merged["ev_place"] = merged["pred_top3"] * (merged["place_payout_yen"] / 100.0)

    # 実現回収額（当たった時だけ払戻）
    merged["realized_return_win_yen"] = merged["win_payout_yen"] * merged["win_hit"]
    merged["realized_return_place_yen"] = merged["place_payout_yen"] * merged["place_hit"]

    merged = merged.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)
    return merged


def rank_within_race(df: pd.DataFrame, score_col: str, rank_col: str) -> pd.DataFrame:
    df = df.copy()
    df[rank_col] = (
        df.groupby("race_id")[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    return df


def summarize_bets(
    df: pd.DataFrame,
    bet_type: str,
    ev_threshold: float,
    min_prob: float,
    max_bets_per_race: int,
) -> dict:
    if bet_type == "win":
        prob_col = "pred_win"
        ev_col = "ev_win"
        hit_col = "win_hit"
        payout_col = "realized_return_win_yen"
        rank_col = "rank_ev_win"
    elif bet_type == "place":
        prob_col = "pred_top3"
        ev_col = "ev_place"
        hit_col = "place_hit"
        payout_col = "realized_return_place_yen"
        rank_col = "rank_ev_place"
    else:
        raise ValueError("bet_type must be win or place")

    temp = df.copy()
    temp = temp[(temp[prob_col] >= min_prob) & (temp[ev_col] >= ev_threshold)].copy()

    if max_bets_per_race > 0:
        temp = temp[temp[rank_col] <= max_bets_per_race].copy()

    bets = len(temp)
    stake_yen = bets * 100
    return_yen = float(temp[payout_col].sum())
    hits = int((temp[hit_col] > 0).sum())

    hit_rate = float(hits / bets) if bets > 0 else np.nan
    roi = float(return_yen / stake_yen) if stake_yen > 0 else np.nan
    profit_yen = float(return_yen - stake_yen) if stake_yen > 0 else np.nan
    avg_ev = float(temp[ev_col].mean()) if bets > 0 else np.nan
    avg_prob = float(temp[prob_col].mean()) if bets > 0 else np.nan

    race_count = int(temp["race_id"].nunique()) if bets > 0 else 0

    return {
        "bet_type": bet_type,
        "ev_threshold": ev_threshold,
        "min_prob": min_prob,
        "max_bets_per_race": max_bets_per_race,
        "bets": bets,
        "race_count": race_count,
        "hits": hits,
        "hit_rate": hit_rate,
        "stake_yen": stake_yen,
        "return_yen": return_yen,
        "profit_yen": profit_yen,
        "roi": roi,
        "avg_ev": avg_ev,
        "avg_prob": avg_prob,
    }


def build_summary_tables(
    merged: pd.DataFrame,
    win_ev_thresholds: List[float],
    place_ev_thresholds: List[float],
    min_win_prob: float,
    min_place_prob: float,
    max_bets_per_race_win: int,
    max_bets_per_race_place: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = rank_within_race(merged, "ev_win", "rank_ev_win")
    merged = rank_within_race(merged, "ev_place", "rank_ev_place")

    win_rows = []
    for th in win_ev_thresholds:
        win_rows.append(
            summarize_bets(
                merged,
                bet_type="win",
                ev_threshold=th,
                min_prob=min_win_prob,
                max_bets_per_race=max_bets_per_race_win,
            )
        )

    place_rows = []
    for th in place_ev_thresholds:
        place_rows.append(
            summarize_bets(
                merged,
                bet_type="place",
                ev_threshold=th,
                min_prob=min_place_prob,
                max_bets_per_race=max_bets_per_race_place,
            )
        )

    return pd.DataFrame(win_rows), pd.DataFrame(place_rows)


def save_overview_json(
    output_path: Path,
    merged: pd.DataFrame,
    win_summary: pd.DataFrame,
    place_summary: pd.DataFrame,
) -> None:
    best_win = None
    best_place = None

    if not win_summary.empty and win_summary["roi"].notna().any():
        best_win = win_summary.sort_values(["roi", "profit_yen"], ascending=[False, False]).iloc[0].to_dict()

    if not place_summary.empty and place_summary["roi"].notna().any():
        best_place = place_summary.sort_values(["roi", "profit_yen"], ascending=[False, False]).iloc[0].to_dict()

    overview = {
        "rows_merged": int(len(merged)),
        "race_count": int(merged["race_id"].nunique()),
        "date_min": str(pd.to_datetime(merged["race_date"]).min().date()) if len(merged) else None,
        "date_max": str(pd.to_datetime(merged["race_date"]).max().date()) if len(merged) else None,
        "best_win_rule": best_win,
        "best_place_rule": best_place,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(overview, f, ensure_ascii=False, indent=2, default=str)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pred_win = load_prediction_files(args.pred_win_files, "pred_win")
    pred_top3 = load_prediction_files(args.pred_top3_files, "pred_top3")
    feature_map = load_feature_mapping(args.feature_files)
    payout = load_payout_files(args.payout_files)

    merged = build_merged_table(pred_win, pred_top3, feature_map, payout)

    win_summary, place_summary = build_summary_tables(
        merged=merged,
        win_ev_thresholds=args.win_ev_thresholds,
        place_ev_thresholds=args.place_ev_thresholds,
        min_win_prob=args.min_win_prob,
        min_place_prob=args.min_place_prob,
        max_bets_per_race_win=args.max_bets_per_race_win,
        max_bets_per_race_place=args.max_bets_per_race_place,
    )

    merged.to_csv(output_dir / "ev_roi_detail.csv", index=False, encoding="utf-8-sig")
    win_summary.to_csv(output_dir / "ev_roi_summary_win.csv", index=False, encoding="utf-8-sig")
    place_summary.to_csv(output_dir / "ev_roi_summary_place.csv", index=False, encoding="utf-8-sig")
    save_overview_json(output_dir / "ev_roi_overview.json", merged, win_summary, place_summary)

    print(f"saved output dir: {output_dir}")
    print(f"merged rows: {len(merged)}")
    print(f"merged race count: {merged['race_id'].nunique()}")

    print("best win summary:")
    if not win_summary.empty and win_summary["roi"].notna().any():
        best_win = win_summary.sort_values(["roi", "profit_yen"], ascending=[False, False]).iloc[0]
        for k in win_summary.columns:
            print(f"  {k}: {best_win[k]}")
    else:
        print("  no valid win bets")

    print("best place summary:")
    if not place_summary.empty and place_summary["roi"].notna().any():
        best_place = place_summary.sort_values(["roi", "profit_yen"], ascending=[False, False]).iloc[0]
        for k in place_summary.columns:
            print(f"  {k}: {best_place[k]}")
    else:
        print("  no valid place bets")


if __name__ == "__main__":
    main()