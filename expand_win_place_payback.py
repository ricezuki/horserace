import argparse
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Expand netkeiba payback.csv to horse-level win/place payout table."
    )
    parser.add_argument("--input", required=True, help="Path to payback.csv")
    parser.add_argument("--output", required=True, help="Path to output csv")
    return parser.parse_args()


def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def split_tokens(text: str) -> List[str]:
    text = normalize_str(text)
    if not text:
        return []
    return [t for t in text.replace(",", "").split() if t]


def parse_single_win_row(row: pd.Series) -> List[Dict[str, Any]]:
    """
    単勝:
      combination = "10"
      payout_yen  = "210"
      popularity  = "1"
    """
    horse_no = normalize_str(row["combination"])
    payout = normalize_str(row["payout_yen"])
    pop = normalize_str(row["popularity"])

    if not horse_no or not payout:
        return []

    return [
        {
            "race_id": row["race_id"],
            "race_date": row["race_date"],
            "horse_number": int(horse_no),
            "win_hit": 1,
            "win_payout_yen": int(payout.replace(",", "")),
            "win_popularity": int(pop) if pop else pd.NA,
            "place_hit": 0,
            "place_payout_yen": 0,
            "place_popularity": pd.NA,
        }
    ]


def parse_place_row(row: pd.Series) -> List[Dict[str, Any]]:
    """
    複勝:
      combination = "10"
      payout_yen  = "120 210 150"
      popularity  = "1 5 3"

    注意:
      netkeibaのpaybackでは複勝の組み合わせ列に1頭しか入っていない場合があるため、
      実際には payout/popularity の3要素をそのまま複勝順として並べるケースがある。
      ただし horse_number との対応付けには、通常は combination 側に複数頭番号が入っているのが理想。

    今回は以下の優先順で処理:
      1. combination が複数トークンなら、それを horse_number として使う
      2. combination が1トークンしかない場合は、その1頭だけ複勝対象として扱う
         （このケースでは完全対応できないので、その1頭分だけ展開）
    """
    comb_tokens = split_tokens(row["combination"])
    payout_tokens = split_tokens(row["payout_yen"])
    pop_tokens = split_tokens(row["popularity"])

    records = []

    # 理想ケース: 複数頭番号がある
    if len(comb_tokens) >= 2 and len(comb_tokens) == len(payout_tokens):
        for i, horse_no in enumerate(comb_tokens):
            payout = payout_tokens[i] if i < len(payout_tokens) else ""
            pop = pop_tokens[i] if i < len(pop_tokens) else ""
            if not horse_no or not payout:
                continue
            records.append(
                {
                    "race_id": row["race_id"],
                    "race_date": row["race_date"],
                    "horse_number": int(horse_no),
                    "win_hit": 0,
                    "win_payout_yen": 0,
                    "win_popularity": pd.NA,
                    "place_hit": 1,
                    "place_payout_yen": int(payout.replace(",", "")),
                    "place_popularity": int(pop) if pop else pd.NA,
                }
            )
        return records

    # 妥協ケース: combination が1頭だけ
    if len(comb_tokens) == 1 and len(payout_tokens) >= 1:
        horse_no = comb_tokens[0]
        payout = payout_tokens[0]
        pop = pop_tokens[0] if len(pop_tokens) >= 1 else ""
        records.append(
            {
                "race_id": row["race_id"],
                "race_date": row["race_date"],
                "horse_number": int(horse_no),
                "win_hit": 0,
                "win_payout_yen": 0,
                "win_popularity": pd.NA,
                "place_hit": 1,
                "place_payout_yen": int(payout.replace(",", "")),
                "place_popularity": int(pop) if pop else pd.NA,
            }
        )
        return records

    return []


def expand_payback(df: pd.DataFrame) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        bet_type = normalize_str(row["bet_type"])

        if bet_type == "単勝":
            records.extend(parse_single_win_row(row))
        elif bet_type == "複勝":
            records.extend(parse_place_row(row))

    out = pd.DataFrame(records)

    if out.empty:
        return out

    # 同一 race_id + horse_number で win/place を1行にまとめる
    agg = (
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

    return agg


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, low_memory=False)

    required_cols = {"race_id", "race_date", "bet_type", "combination", "payout_yen", "popularity"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = expand_payback(df)
    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"saved: {output_path}")
    print(f"rows: {len(out)}")
    if len(out) > 0:
        print("columns:")
        for c in out.columns:
            print(f"  {c}")


if __name__ == "__main__":
    main()