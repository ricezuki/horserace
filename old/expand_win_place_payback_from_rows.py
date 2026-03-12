import argparse
from pathlib import Path
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Expand netkeiba payback.csv (row-based win/place format) to horse-level payout table."
    )
    parser.add_argument("--payback", required=True, help="Path to payback.csv")
    parser.add_argument("--race-result", required=True, help="Path to race_result.csv")
    parser.add_argument("--output", required=True, help="Path to output csv")
    return parser.parse_args()


def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


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

    required = {"race_id", "race_date", "horse_number", "finish_position"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"race_result missing required columns: {missing}")

    df["horse_number"] = pd.to_numeric(df["horse_number"], errors="coerce")
    df["finish_position_num"] = pd.to_numeric(df["finish_position"], errors="coerce")

    df = df.dropna(subset=["horse_number"]).copy()
    df["horse_number"] = df["horse_number"].astype(int)

    return df


def load_payback(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    required = {"race_id", "race_date", "bet_type", "combination", "payout_yen", "popularity"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"payback missing required columns: {missing}")

    return df


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


def extract_win_df(payback_df: pd.DataFrame) -> pd.DataFrame:
    win_df = payback_df[payback_df["bet_type"].astype(str).str.strip() == "単勝"].copy()
    if win_df.empty:
        return pd.DataFrame(
            columns=["race_id", "race_date", "horse_number", "win_hit", "win_payout_yen", "win_popularity"]
        )

    win_df["horse_number"] = pd.to_numeric(win_df["combination"], errors="coerce")
    win_df["win_payout_yen"] = win_df["payout_yen"].apply(parse_int_safe)
    win_df["win_popularity"] = win_df["popularity"].apply(parse_int_safe)

    win_df = win_df.dropna(subset=["horse_number", "win_payout_yen"]).copy()
    win_df["horse_number"] = win_df["horse_number"].astype(int)
    win_df["win_hit"] = 1

    win_df = win_df[
        ["race_id", "race_date", "horse_number", "win_hit", "win_payout_yen", "win_popularity"]
    ].copy()

    win_df = (
        win_df.groupby(["race_id", "race_date", "horse_number"], as_index=False)
        .agg(
            {
                "win_hit": "max",
                "win_payout_yen": "max",
                "win_popularity": "max",
            }
        )
        .sort_values(["race_date", "race_id", "horse_number"])
        .reset_index(drop=True)
    )

    return win_df


def extract_place_df(payback_df: pd.DataFrame) -> pd.DataFrame:
    place_df = payback_df[payback_df["bet_type"].astype(str).str.strip() == "複勝"].copy()
    if place_df.empty:
        return pd.DataFrame(
            columns=["race_id", "race_date", "horse_number", "place_hit", "place_payout_yen", "place_popularity"]
        )

    place_df["horse_number"] = pd.to_numeric(place_df["combination"], errors="coerce")
    place_df["place_payout_yen"] = place_df["payout_yen"].apply(parse_int_safe)
    place_df["place_popularity"] = place_df["popularity"].apply(parse_int_safe)

    place_df = place_df.dropna(subset=["horse_number", "place_payout_yen"]).copy()
    place_df["horse_number"] = place_df["horse_number"].astype(int)
    place_df["place_hit"] = 1

    place_df = place_df[
        ["race_id", "race_date", "horse_number", "place_hit", "place_payout_yen", "place_popularity"]
    ].copy()

    place_df = (
        place_df.groupby(["race_id", "race_date", "horse_number"], as_index=False)
        .agg(
            {
                "place_hit": "max",
                "place_payout_yen": "max",
                "place_popularity": "max",
            }
        )
        .sort_values(["race_date", "race_id", "horse_number"])
        .reset_index(drop=True)
    )

    return place_df


def merge_payouts(base_df: pd.DataFrame, win_df: pd.DataFrame, place_df: pd.DataFrame) -> pd.DataFrame:
    out = base_df.copy()

    if not win_df.empty:
        out = out.merge(
            win_df,
            on=["race_id", "race_date", "horse_number"],
            how="left",
            suffixes=("", "_win_new"),
            validate="one_to_one",
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
            validate="one_to_one",
        )
        out["place_hit"] = out["place_hit_place_new"].fillna(out["place_hit"]).astype(int)
        out["place_payout_yen"] = out["place_payout_yen_place_new"].fillna(out["place_payout_yen"]).astype(int)
        out["place_popularity"] = out["place_popularity_place_new"].combine_first(out["place_popularity"])
        out = out.drop(
            columns=["place_hit_place_new", "place_payout_yen_place_new", "place_popularity_place_new"]
        )

    out = (
        out.groupby(["race_id", "race_date", "horse_number"], as_index=False)
        .agg(
            {
                "win_hit": "max",
                "win_payout_yen": "max",
                "win_popularity": "max",
                "place_hit": "max",
                "place_payout_yen": "max",
                "place_popularity": "max",
            }
        )
        .sort_values(["race_date", "race_id", "horse_number"])
        .reset_index(drop=True)
    )

    return out


def validate_output(out: pd.DataFrame) -> None:
    dup_cnt = out.duplicated(subset=["race_id", "race_date", "horse_number"]).sum()
    win_sum = out.groupby("race_id")["win_hit"].sum()
    place_sum = out.groupby("race_id")["place_hit"].sum()

    bad_win = win_sum[win_sum > 2]   # 1着同着は2まで許容
    bad_place = place_sum[place_sum > 3]

    print("validation:")
    print(f"  duplicate race_id+horse_number rows: {int(dup_cnt)}")
    print(f"  races with win_hit_sum > 2: {int(len(bad_win))}")
    print(f"  races with place_hit_sum > 3: {int(len(bad_place))}")

    if len(bad_win) > 0:
        print("  sample bad win races:")
        print(bad_win.head().to_string())

    if len(bad_place) > 0:
        print("  sample bad place races:")
        print(bad_place.head().to_string())


def main() -> None:
    args = parse_args()

    payback_path = Path(args.payback)
    race_result_path = Path(args.race_result)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payback_df = load_payback(str(payback_path))
    race_result_df = load_race_result(str(race_result_path))

    base_df = build_base_from_race_result(race_result_df)
    win_df = extract_win_df(payback_df)
    place_df = extract_place_df(payback_df)

    out = merge_payouts(base_df, win_df, place_df)
    out = out.sort_values(["race_date", "race_id", "horse_number"]).reset_index(drop=True)

    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"saved: {output_path}")
    print(f"rows: {len(out)}")
    print("columns:")
    for c in out.columns:
        print(f"  {c}")

    print("summary:")
    print(f"  win_hit_sum total: {int(out['win_hit'].sum())}")
    print(f"  place_hit_sum total: {int(out['place_hit'].sum())}")

    validate_output(out)


if __name__ == "__main__":
    main()