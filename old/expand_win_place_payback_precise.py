import argparse
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Expand netkeiba payback.csv to horse-level win/place payout table using race_result.csv."
    )
    parser.add_argument("--payback", required=True, help="Path to payback.csv")
    parser.add_argument("--race-result", required=True, help="Path to race_result.csv")
    parser.add_argument("--output", required=True, help="Path to output csv")
    return parser.parse_args()


def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def split_tokens_keep_commas(text: str) -> List[str]:
    text = normalize_str(text)
    if not text:
        return []
    return [t for t in text.split() if t]


def parse_int_safe(x):
    s = normalize_str(x).replace(",", "")
    if s == "":
        return pd.NA
    try:
        return int(s)
    except Exception:
        return pd.NA


def load_race_result(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    required = {
        "race_id",
        "race_date",
        "horse_number",
        "finish_position",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"race_result missing required columns: {missing}")

    # 数値化
    df["horse_number"] = pd.to_numeric(df["horse_number"], errors="coerce")
    df["finish_position_num"] = pd.to_numeric(df["finish_position"], errors="coerce")

    # 着順確定馬のみ対象
    df = df.dropna(subset=["horse_number", "finish_position_num"]).copy()
    df["horse_number"] = df["horse_number"].astype(int)

    return df


def load_payback(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    required = {
        "race_id",
        "race_date",
        "bet_type",
        "combination",
        "payout_yen",
        "popularity",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"payback missing required columns: {missing}")

    return df


def get_place_horses_for_race(race_df: pd.DataFrame) -> List[int]:
    """
    race_result から複勝対象馬を復元する。
    通常は1,2,3着。
    頭数が少ない/同着のケースを考慮するため、
    「3着以内の馬」をそのまま採用する。
    """
    top_df = race_df[race_df["finish_position_num"] <= 3].copy()
    top_df = top_df.sort_values(["finish_position_num", "horse_number"])
    return top_df["horse_number"].astype(int).tolist()


def expand_single_win(
    payback_df: pd.DataFrame,
    race_result_df: pd.DataFrame,
) -> pd.DataFrame:
    win_rows = payback_df[payback_df["bet_type"].astype(str).str.strip() == "単勝"].copy()
    if win_rows.empty:
        return pd.DataFrame(
            columns=[
                "race_id", "race_date", "horse_number",
                "win_hit", "win_payout_yen", "win_popularity"
            ]
        )

    recs = []
    for _, row in win_rows.iterrows():
        horse_no = parse_int_safe(row["combination"])
        payout = parse_int_safe(row["payout_yen"])
        pop = parse_int_safe(row["popularity"])

        if pd.isna(horse_no) or pd.isna(payout):
            continue

        recs.append(
            {
                "race_id": row["race_id"],
                "race_date": row["race_date"],
                "horse_number": int(horse_no),
                "win_hit": 1,
                "win_payout_yen": int(payout),
                "win_popularity": pop,
            }
        )

    out = pd.DataFrame(recs)
    return out


def expand_place_precise(
    payback_df: pd.DataFrame,
    race_result_df: pd.DataFrame,
) -> pd.DataFrame:
    place_rows = payback_df[payback_df["bet_type"].astype(str).str.strip() == "複勝"].copy()
    if place_rows.empty:
        return pd.DataFrame(
            columns=[
                "race_id", "race_date", "horse_number",
                "place_hit", "place_payout_yen", "place_popularity"
            ]
        )

    recs: List[Dict[str, Any]] = []

    race_groups = {race_id: g.copy() for race_id, g in race_result_df.groupby("race_id", sort=False)}

    for _, row in place_rows.iterrows():
        race_id = row["race_id"]
        race_date = row["race_date"]

        payout_tokens = split_tokens_keep_commas(row["payout_yen"])
        pop_tokens = split_tokens_keep_commas(row["popularity"])

        payout_values = [parse_int_safe(x) for x in payout_tokens]
        pop_values = [parse_int_safe(x) for x in pop_tokens]

        if race_id not in race_groups:
            continue

        race_df = race_groups[race_id]
        place_horses = get_place_horses_for_race(race_df)

        if len(place_horses) == 0:
            continue

        # 実際の複勝頭数に合わせる
        # netkeiba側の払戻件数と race_result 側の複勝対象件数がズレる場合は
        # 共通する最小個数だけ安全に割り当てる
        n = min(len(place_horses), len(payout_values))
        if n == 0:
            continue

        for i in range(n):
            horse_no = place_horses[i]
            payout = payout_values[i]
            pop = pop_values[i] if i < len(pop_values) else pd.NA

            if pd.isna(payout):
                continue

            recs.append(
                {
                    "race_id": race_id,
                    "race_date": race_date,
                    "horse_number": int(horse_no),
                    "place_hit": 1,
                    "place_payout_yen": int(payout),
                    "place_popularity": pop,
                }
            )

    out = pd.DataFrame(recs)
    return out


def build_base_from_race_result(race_result_df: pd.DataFrame) -> pd.DataFrame:
    base = race_result_df[["race_id", "race_date", "horse_number"]].drop_duplicates().copy()
    base = base.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)

    base["win_hit"] = 0
    base["win_payout_yen"] = 0
    base["win_popularity"] = pd.NA
    base["place_hit"] = 0
    base["place_payout_yen"] = 0
    base["place_popularity"] = pd.NA

    return base


def merge_payouts(
    base_df: pd.DataFrame,
    win_df: pd.DataFrame,
    place_df: pd.DataFrame,
) -> pd.DataFrame:
    out = base_df.copy()

    if not win_df.empty:
        out = out.merge(
            win_df,
            on=["race_id", "race_date", "horse_number"],
            how="left",
            suffixes=("", "_win_new"),
        )
        out["win_hit"] = out["win_hit_win_new"].fillna(out["win_hit"]).astype(int)
        out["win_payout_yen"] = out["win_payout_yen_win_new"].fillna(out["win_payout_yen"]).astype(int)
        out["win_popularity"] = out["win_popularity_win_new"].combine_first(out["win_popularity"])
        out = out.drop(columns=["win_hit_win_new", "win_payout_yen_win_new", "win_popularity_win_new"])

    if not place_df.empty:
        out = out.merge(
            place_df,
            on=["race_id", "race_date", "horse_number"],
            how="left",
            suffixes=("", "_place_new"),
        )
        out["place_hit"] = out["place_hit_place_new"].fillna(out["place_hit"]).astype(int)
        out["place_payout_yen"] = out["place_payout_yen_place_new"].fillna(out["place_payout_yen"]).astype(int)
        out["place_popularity"] = out["place_popularity_place_new"].combine_first(out["place_popularity"])
        out = out.drop(columns=["place_hit_place_new", "place_payout_yen_place_new", "place_popularity_place_new"])

    return out


def main() -> None:
    args = parse_args()

    payback_path = Path(args.payback)
    race_result_path = Path(args.race_result)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payback_df = load_payback(str(payback_path))
    race_result_df = load_race_result(str(race_result_path))

    base_df = build_base_from_race_result(race_result_df)
    win_df = expand_single_win(payback_df, race_result_df)
    place_df = expand_place_precise(payback_df, race_result_df)

    out = merge_payouts(base_df, win_df, place_df)
    out = out.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)

    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"saved: {output_path}")
    print(f"rows: {len(out)}")
    print("columns:")
    for c in out.columns:
        print(f"  {c}")

    # 軽い確認用
    print("summary:")
    print(f"  win_hit_sum: {int(out['win_hit'].sum())}")
    print(f"  place_hit_sum: {int(out['place_hit'].sum())}")


if __name__ == "__main__":
    main()