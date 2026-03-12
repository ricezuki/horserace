import argparse
import csv
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://race.netkeiba.com/odds/index.html"
DEFAULT_SLEEP = 1.0

ODDS_TYPE_LABELS = {
    "b1": "単勝複勝",
    "b3": "枠連",
    "b4": "馬連",
    "b5": "ワイド",
    "b6": "馬単",
    "b7": "3連複",
    "b8": "3連単",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape netkeiba odds pages from normal HTML and save normalized CSV."
    )
    parser.add_argument("--race-ids-file", help="Text/CSV file containing race_id values.")
    parser.add_argument("--race-ids", nargs="*", help="Race IDs directly.")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Sleep seconds between requests")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout seconds")
    parser.add_argument(
        "--user-agent",
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        help="User-Agent header",
    )
    parser.add_argument(
        "--types",
        nargs="*",
        default=["b1", "b3", "b4", "b5", "b6", "b7", "b8"],
        help="Odds types to fetch. Default: b1 b3 b4 b5 b6 b7 b8",
    )
    return parser.parse_args()


def load_race_ids(args: argparse.Namespace) -> List[str]:
    race_ids: List[str] = []

    if args.race_ids:
        race_ids.extend([x.strip() for x in args.race_ids if str(x).strip()])

    if args.race_ids_file:
        path = Path(args.race_ids_file)
        if not path.exists():
            raise FileNotFoundError(f"race ids file not found: {path}")

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
            f.seek(0)

            if "," in sample or "\t" in sample:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    val = str(row[0]).strip()
                    if re.fullmatch(r"\d{12}", val):
                        race_ids.append(val)
            else:
                for line in f:
                    val = line.strip()
                    if re.fullmatch(r"\d{12}", val):
                        race_ids.append(val)

    race_ids = list(dict.fromkeys(race_ids))
    if not race_ids:
        raise ValueError("No race_id found. Use --race-ids or --race-ids-file.")

    return race_ids


def build_url(odds_type: str, race_id: str) -> str:
    if odds_type == "b1":
        return f"{BASE_URL}?type=b1&race_id={race_id}&rf=shutuba_submenu"
    return f"{BASE_URL}?type={odds_type}&race_id={race_id}&housiki=c0&rf=shutuba_submenu"


def fetch_html(session: requests.Session, url: str, timeout: float) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()

    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "EUC-JP"
    return resp.text


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def to_float_odds(text: str) -> Optional[float]:
    s = clean_text(text).replace(",", "")
    if s in {"", "---.-", "---", "-", "取消"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def extract_race_meta(soup: BeautifulSoup, race_id: str) -> Dict[str, str]:
    title = clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

    race_name = ""
    race_date = ""
    race_num = ""

    m = re.search(r"(.+?) オッズ \| (\d{4}年\d{1,2}月\d{1,2}日) .+?(\d{1,2}R)", title)
    if m:
        race_name = clean_text(m.group(1))
        race_date = m.group(2)
        race_num = m.group(3)

    return {
        "race_id": race_id,
        "page_title": title,
        "race_name_from_title": race_name,
        "race_date_from_title": race_date,
        "race_num_from_title": race_num,
    }


def parse_popularity_from_row_text(text: str) -> Optional[int]:
    m = re.search(r"(\d+)人気", text)
    return int(m.group(1)) if m else None


def parse_horse_number_from_row(tr: BeautifulSoup) -> Optional[int]:
    # th, td を順に見て最初の妥当な馬番を拾う
    cells = tr.find_all(["th", "td"])
    for cell in cells:
        tx = clean_text(cell.get_text(" ", strip=True))
        if re.fullmatch(r"\d{1,2}", tx):
            n = int(tx)
            if 1 <= n <= 18:
                return n
    return None


def parse_horse_name_from_row(tr: BeautifulSoup) -> str:
    a = tr.select_one("a")
    if a:
        return clean_text(a.get_text(" ", strip=True))
    return ""


def find_first_odds_in_row(tr: BeautifulSoup) -> Optional[float]:
    # テキスト全体からオッズ候補を後ろ優先で拾う
    texts = [clean_text(x.get_text(" ", strip=True)) for x in tr.find_all(["th", "td", "span"])]
    for tx in reversed(texts):
        val = to_float_odds(tx)
        if val is not None:
            return val
    return None


def find_fuku_range_in_row(tr: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    row_text = clean_text(tr.get_text(" ", strip=True)).replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*[-〜~]\s*(\d+(?:\.\d+)?)", row_text)
    if m:
        return float(m.group(1)), float(m.group(2))

    val = find_first_odds_in_row(tr)
    return val, None


def parse_b1_tanfuku(soup: BeautifulSoup, race_id: str) -> List[Dict]:
    rows: List[Dict] = []
    meta = extract_race_meta(soup, race_id)

    tables = soup.select("table.RaceOdds_HorseList_Table")
    for table in tables:
        table_text = clean_text(table.get_text(" ", strip=True))

        # 単勝テーブル判定
        if "単勝" in table_text and "複勝" not in table_text:
            for tr in table.select("tbody tr"):
                horse_number = parse_horse_number_from_row(tr)
                if horse_number is None:
                    continue

                horse_name = parse_horse_name_from_row(tr)
                odds = find_first_odds_in_row(tr)
                popularity = parse_popularity_from_row_text(clean_text(tr.get_text(" ", strip=True)))

                rows.append({
                    **meta,
                    "odds_type": "b1",
                    "bet_type": "単勝",
                    "combination": str(horse_number),
                    "horse_number_1": horse_number,
                    "horse_number_2": None,
                    "horse_number_3": None,
                    "horse_name_1": horse_name,
                    "horse_name_2": "",
                    "horse_name_3": "",
                    "odds": odds,
                    "odds_max": None,
                    "popularity": popularity,
                })

        # 複勝テーブル判定
        elif "複勝" in table_text:
            for tr in table.select("tbody tr"):
                horse_number = parse_horse_number_from_row(tr)
                if horse_number is None:
                    continue

                horse_name = parse_horse_name_from_row(tr)
                odds_min, odds_max = find_fuku_range_in_row(tr)
                popularity = parse_popularity_from_row_text(clean_text(tr.get_text(" ", strip=True)))

                rows.append({
                    **meta,
                    "odds_type": "b1",
                    "bet_type": "複勝",
                    "combination": str(horse_number),
                    "horse_number_1": horse_number,
                    "horse_number_2": None,
                    "horse_number_3": None,
                    "horse_name_1": horse_name,
                    "horse_name_2": "",
                    "horse_name_3": "",
                    "odds": odds_min,
                    "odds_max": odds_max,
                    "popularity": popularity,
                })

    # テーブル判定で取れない場合のフォールバック
    if not rows:
        tan_block = soup.select_one("#odds_tan_block")
        if tan_block:
            for tr in tan_block.select("table tbody tr"):
                horse_number = parse_horse_number_from_row(tr)
                if horse_number is None:
                    continue
                rows.append({
                    **meta,
                    "odds_type": "b1",
                    "bet_type": "単勝",
                    "combination": str(horse_number),
                    "horse_number_1": horse_number,
                    "horse_number_2": None,
                    "horse_number_3": None,
                    "horse_name_1": parse_horse_name_from_row(tr),
                    "horse_name_2": "",
                    "horse_name_3": "",
                    "odds": find_first_odds_in_row(tr),
                    "odds_max": None,
                    "popularity": parse_popularity_from_row_text(clean_text(tr.get_text(" ", strip=True))),
                })

        fuku_block = soup.select_one("#odds_fuku_block")
        if fuku_block:
            for tr in fuku_block.select("table tbody tr"):
                horse_number = parse_horse_number_from_row(tr)
                if horse_number is None:
                    continue
                odds_min, odds_max = find_fuku_range_in_row(tr)
                rows.append({
                    **meta,
                    "odds_type": "b1",
                    "bet_type": "複勝",
                    "combination": str(horse_number),
                    "horse_number_1": horse_number,
                    "horse_number_2": None,
                    "horse_number_3": None,
                    "horse_name_1": parse_horse_name_from_row(tr),
                    "horse_name_2": "",
                    "horse_name_3": "",
                    "odds": odds_min,
                    "odds_max": odds_max,
                    "popularity": parse_popularity_from_row_text(clean_text(tr.get_text(" ", strip=True))),
                })

    # 重複除去
    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["race_id", "bet_type", "horse_number_1"], keep="first")
        return df.to_dict("records")

    return rows


def extract_numbers_from_id(odds_type_num: int, span_id: str) -> List[int]:
    m = re.match(rf"odds-{odds_type_num}-(\d+)$", span_id)
    if not m:
        return []

    raw = m.group(1)

    if odds_type_num in {3, 4, 5, 6}:
        if len(raw) % 2 != 0:
            return []
        return [int(raw[i:i + 2]) for i in range(0, len(raw), 2)]

    if odds_type_num in {7, 8}:
        if len(raw) != 6:
            return []
        return [int(raw[0:2]), int(raw[2:4]), int(raw[4:6])]

    return []


def infer_bet_label(odds_type: str) -> str:
    return {
        "b3": "枠連",
        "b4": "馬連",
        "b5": "ワイド",
        "b6": "馬単",
        "b7": "3連複",
        "b8": "3連単",
    }[odds_type]


def parse_combo_table_page(soup: BeautifulSoup, race_id: str, odds_type: str) -> List[Dict]:
    rows: List[Dict] = []
    meta = extract_race_meta(soup, race_id)
    odds_type_num = int(odds_type[1:])
    bet_label = infer_bet_label(odds_type)

    # 通常HTMLで値が入っている span / td を両方見に行く
    for span in soup.select(f"[id^='odds-{odds_type_num}-']"):
        span_id = span.get("id", "")
        comb_nums = extract_numbers_from_id(odds_type_num, span_id)
        if not comb_nums:
            continue

        odds = to_float_odds(span.get_text(" ", strip=True))
        parent_td = span.find_parent("td")
        popularity = None
        if parent_td:
            popularity = parse_popularity_from_row_text(clean_text(parent_td.get_text(" ", strip=True)))

        rows.append({
            **meta,
            "odds_type": odds_type,
            "bet_type": bet_label,
            "combination": "-".join(str(x) for x in comb_nums),
            "horse_number_1": comb_nums[0] if len(comb_nums) >= 1 else None,
            "horse_number_2": comb_nums[1] if len(comb_nums) >= 2 else None,
            "horse_number_3": comb_nums[2] if len(comb_nums) >= 3 else None,
            "horse_name_1": "",
            "horse_name_2": "",
            "horse_name_3": "",
            "odds": odds,
            "odds_max": None,
            "popularity": popularity,
        })

    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        )
        return df.to_dict("records")

    return rows


def parse_odds_page(html: str, race_id: str, odds_type: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    if odds_type == "b1":
        return parse_b1_tanfuku(soup, race_id)

    if odds_type in {"b3", "b4", "b5", "b6", "b7", "b8"}:
        return parse_combo_table_page(soup, race_id, odds_type)

    return []


def main() -> None:
    args = parse_args()
    race_ids = load_race_ids(args)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "User-Agent": args.user_agent,
        "Referer": "https://race.netkeiba.com/",
    })

    all_rows: List[Dict] = []
    error_rows: List[Dict] = []

    for i, race_id in enumerate(race_ids, start=1):
        print(f"[{i}/{len(race_ids)}] race_id={race_id}")
        for odds_type in args.types:
            url = build_url(odds_type, race_id)
            try:
                html = fetch_html(session, url, timeout=args.timeout)
                rows = parse_odds_page(html, race_id, odds_type)
                all_rows.extend(rows)
                print(f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): {len(rows)} rows")
            except Exception as e:
                error_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "url": url,
                    "error": str(e),
                })
                print(f"  {odds_type}: ERROR - {e}")

            time.sleep(args.sleep)

    df = pd.DataFrame(all_rows)

    if not df.empty:
        desired_cols = [
            "race_id",
            "race_date_from_title",
            "race_num_from_title",
            "race_name_from_title",
            "page_title",
            "odds_type",
            "bet_type",
            "combination",
            "horse_number_1",
            "horse_number_2",
            "horse_number_3",
            "horse_name_1",
            "horse_name_2",
            "horse_name_3",
            "odds",
            "odds_max",
            "popularity",
        ]
        for c in desired_cols:
            if c not in df.columns:
                df[c] = None

        df = df[desired_cols].copy()
        df = df.sort_values(
            ["race_id", "odds_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            na_position="last",
        ).reset_index(drop=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    if error_rows:
        err_path = output_path.with_name(output_path.stem + "_errors.csv")
        pd.DataFrame(error_rows).to_csv(err_path, index=False, encoding="utf-8-sig")
        print(f"errors saved: {err_path}")

    print(f"saved: {output_path}")
    print(f"rows: {len(df)}")


if __name__ == "__main__":
    main()