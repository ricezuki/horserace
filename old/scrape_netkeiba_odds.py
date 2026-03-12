import argparse
import csv
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://race.netkeiba.com/odds/index.html"

# 既存の出馬表/結果スクレイピングに合わせて sleep は残す
DEFAULT_SLEEP = 1.0

# type -> 券種名
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
        description="Scrape pre-race odds pages from netkeiba and save as normalized CSV."
    )
    parser.add_argument(
        "--race-ids-file",
        help="Text/CSV file containing race_id values. First column or one-per-line.",
    )
    parser.add_argument(
        "--race-ids",
        nargs="*",
        help="Race IDs directly. Example: 202510010111 202510010112",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Sleep seconds between requests",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Request timeout seconds",
    )
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


def fetch_html(
    session: requests.Session,
    url: str,
    timeout: float,
) -> str:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()

    # netkeiba odds pages are typically EUC-JP in these saved sources
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


def parse_b1_tanfuku(soup: BeautifulSoup, race_id: str) -> List[Dict]:
    rows: List[Dict] = []
    meta = extract_race_meta(soup, race_id)

    # 単勝
    tan_block = soup.select_one("#odds_tan_block table.RaceOdds_HorseList_Table")
    if tan_block:
        for tr in tan_block.select("tbody tr"):
            tds = tr.find_all("td")
            ths = tr.find_all("th")
            if not tds or not ths:
                continue

            # 想定: 枠 / 馬番 / 馬名 / オッズ / 人気
            texts = [clean_text(x.get_text(" ", strip=True)) for x in tr.find_all(["th", "td"])]
            if len(texts) < 4:
                continue

            horse_number = None
            popularity = None
            odds = None

            nums = re.findall(r"\d+", " | ".join(texts))
            if nums:
                horse_number = int(nums[0])

            # オッズと人気は後ろから拾う方が安定しやすい
            for tx in reversed(texts):
                if odds is None:
                    val = to_float_odds(tx)
                    if val is not None:
                        odds = val
                        continue
                if popularity is None:
                    m = re.search(r"(\d+)人気", tx)
                    if m:
                        popularity = int(m.group(1))

            horse_name = ""
            a = tr.select_one("a")
            if a:
                horse_name = clean_text(a.get_text(" ", strip=True))

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
                "horse_name_1": horse_name,
                "horse_name_2": "",
                "horse_name_3": "",
                "odds": odds,
                "popularity": popularity,
            })

    # 複勝
    fuku_block = soup.select_one("#odds_fuku_block table.RaceOdds_HorseList_Table")
    if fuku_block:
        for tr in fuku_block.select("tbody tr"):
            texts = [clean_text(x.get_text(" ", strip=True)) for x in tr.find_all(["th", "td"])]
            if len(texts) < 4:
                continue

            horse_number = None
            popularity = None
            odds = None

            nums = re.findall(r"\d+", " | ".join(texts))
            if nums:
                horse_number = int(nums[0])

            # 複勝は "2.3 - 3.1" のようなレンジ表記がありうる
            joined = " | ".join(texts).replace(",", "")
            m_odds_range = re.search(r"(\d+(?:\.\d+)?)\s*[-〜~]\s*(\d+(?:\.\d+)?)", joined)
            if m_odds_range:
                # 下限・上限を別列で保持したい場合は拡張可
                odds = float(m_odds_range.group(1))
                odds_max = float(m_odds_range.group(2))
            else:
                odds_max = None
                for tx in reversed(texts):
                    val = to_float_odds(tx)
                    if val is not None:
                        odds = val
                        break

            for tx in reversed(texts):
                m = re.search(r"(\d+)人気", tx)
                if m:
                    popularity = int(m.group(1))
                    break

            horse_name = ""
            a = tr.select_one("a")
            if a:
                horse_name = clean_text(a.get_text(" ", strip=True))

            if horse_number is None:
                continue

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
                "odds": odds,
                "odds_max": odds_max,
                "popularity": popularity,
            })

    return rows


def extract_numbers_from_id(odds_type_num: int, span_id: str) -> List[int]:
    """
    examples:
      odds-3-0808     -> [8, 8]
      odds-5-1516     -> [15, 16]
      odds-7-011718   -> [1, 17, 18]
    """
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

    for span in soup.select(f"span[id^='odds-{odds_type_num}-']"):
        span_id = span.get("id", "")
        comb_nums = extract_numbers_from_id(odds_type_num, span_id)
        if not comb_nums:
            continue

        odds = to_float_odds(span.get_text(" ", strip=True))

        parent_td = span.find_parent("td")
        popularity = None
        if parent_td:
            td_text = clean_text(parent_td.get_text(" ", strip=True))
            m_pop = re.search(r"(\d+)人気", td_text)
            if m_pop:
                popularity = int(m_pop.group(1))

        row = {
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
            "popularity": popularity,
        }
        rows.append(row)

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

                for row in rows:
                    row.setdefault("odds_max", None)

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

    df = None
    import pandas as pd
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