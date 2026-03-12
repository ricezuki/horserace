from __future__ import annotations

import argparse
import csv
import hashlib
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://race.netkeiba.com"
RESULT_URL = BASE + "/race/result.html?race_id={race_id}"
DISCOVERY_URLS = [
    BASE + "/top/race_list.html?kaisai_date={yyyymmdd}",
    BASE + "/top/race_list_sub.html?kaisai_date={yyyymmdd}",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": BASE + "/",
}

PLACE_MAP = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}
VALID_PLACE_CODES = set(PLACE_MAP.keys())

REQUEST_TIMEOUT = 20
DEFAULT_SLEEP_MIN = 3.0
DEFAULT_SLEEP_MAX = 6.0
DEFAULT_RETRIES = 3


@dataclass
class ScraperConfig:
    output_dir: Path
    save_raw_html: bool
    sleep_min: float
    sleep_max: float
    max_retries: int
    timeout: int


@dataclass
class ResultFetchOutcome:
    html: Optional[str]
    race_date: str
    status: str


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def month_to_range(month_str: str) -> Tuple[date, date]:
    start = datetime.strptime(month_str, "%Y-%m").date().replace(day=1)
    if start.month == 12:
        next_month = start.replace(year=start.year + 1, month=1, day=1)
    else:
        next_month = start.replace(month=start.month + 1, day=1)
    end = next_month - timedelta(days=1)
    return start, end


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def text_or_empty(el) -> str:
    if el is None:
        return ""
    return normalize_space(el.get_text(" ", strip=True))


def random_sleep(cfg: ScraperConfig) -> None:
    time.sleep(random.uniform(cfg.sleep_min, cfg.sleep_max))


def retry_sleep(attempt: int) -> None:
    time.sleep(10 * (2**attempt))


def ensure_dirs(cfg: ScraperConfig) -> None:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    if cfg.save_raw_html:
        (cfg.output_dir / "raw_html").mkdir(parents=True, exist_ok=True)


def fetch_text(session: requests.Session, url: str, cfg: ScraperConfig) -> Optional[str]:
    for attempt in range(cfg.max_retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=cfg.timeout)
            if resp.status_code == 200:
                resp.encoding = resp.encoding or "EUC-JP"
                text = resp.text
                if "RaceTable01" not in text and resp.apparent_encoding:
                    resp.encoding = resp.apparent_encoding
                    text = resp.text
                return text
            if resp.status_code in {429, 500, 502, 503, 504}:
                retry_sleep(attempt)
                continue
            return None
        except requests.RequestException:
            retry_sleep(attempt)
    return None


def load_existing_values(csv_path: Path, key_name: str) -> Set[str]:
    if not csv_path.exists():
        return set()
    values: Set[str] = set()
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get(key_name, "")
            if key:
                values.add(key)
    return values


def append_rows(csv_path: Path, rows: Sequence[Dict[str, str]]) -> None:
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def html_sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def parse_race_id_parts(race_id: str) -> Dict[str, str]:
    return {
        "year": race_id[0:4],
        "place_code": race_id[4:6],
        "kai": race_id[6:8],
        "day": race_id[8:10],
        "race_num": race_id[10:12],
    }


def race_id_is_structurally_valid(race_id: str, target_year: str) -> bool:
    if not re.fullmatch(r"\d{12}", race_id):
        return False
    if race_id[:4] != target_year:
        return False
    parts = parse_race_id_parts(race_id)
    if parts["place_code"] not in VALID_PLACE_CODES:
        return False
    try:
        kai = int(parts["kai"])
        day = int(parts["day"])
        race_num = int(parts["race_num"])
    except ValueError:
        return False
    return 1 <= kai <= 12 and 1 <= day <= 12 and 1 <= race_num <= 12


# ---------------------------
# discovery
# ---------------------------
def extract_race_ids_from_race_links(base_url: str, html: str, target_date: date) -> Set[str]:
    """
    その日の race_list 系ページに実際に載っているレースリンクだけを対象に race_id を拾う。
    HTML全体の文字列検索は行わない。
    """
    target_year = target_date.strftime("%Y")
    race_ids: Set[str] = set()
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = urljoin(base_url, href)
        parsed = urlparse(full)

        # race系リンク以外は無視
        if not parsed.path.startswith("/race/"):
            continue
        if parsed.path not in {"/race/result.html", "/race/shutuba.html", "/race/ochi.html"}:
            continue

        qs = parse_qs(parsed.query)
        rid = qs.get("race_id", [""])[0]
        if not race_id_is_structurally_valid(rid, target_year):
            continue

        # 表示テキストが空のリンクやJS用途リンクは避ける
        anchor_text = text_or_empty(a)
        if not anchor_text and "rf=race_list" not in href and "race_id=" not in href:
            continue

        race_ids.add(rid)

    return race_ids


def discover_race_ids_for_date(session: requests.Session, target_date: date, cfg: ScraperConfig) -> Set[str]:
    yyyymmdd = target_date.strftime("%Y%m%d")
    race_ids: Set[str] = set()

    for tmpl in DISCOVERY_URLS:
        url = tmpl.format(yyyymmdd=yyyymmdd)
        html = fetch_text(session, url, cfg)
        random_sleep(cfg)
        if not html:
            continue
        race_ids.update(extract_race_ids_from_race_links(url, html, target_date))

    return race_ids


# ---------------------------
# race page parsing
# ---------------------------
def parse_race_date_from_meta(soup: BeautifulSoup) -> str:
    desc = soup.select_one('meta[name="description"]')
    if desc:
        content = desc.get("content", "")
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", content)
        if m:
            y, mo, d = m.groups()
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    title = text_or_empty(soup.select_one("title"))
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    return ""


def parse_grade(race_name: str, race_data02: str, title_text: str) -> str:
    for source in (race_name, race_data02, title_text):
        m = re.search(r"\((G1|G2|G3|L)\)", source)
        if m:
            return m.group(1)
    return ""


def parse_conditions(race_data02: str) -> Dict[str, str]:
    race_class = ""
    age_condition = ""
    sex_condition = ""
    weight_condition = ""

    m = re.search(r"サラ系([^\s]+)", race_data02)
    if m:
        age_condition = m.group(1)

    if "牝" in race_data02 and "牡" not in race_data02:
        sex_condition = "牝"
    elif "牝" in race_data02 and "牡" in race_data02:
        sex_condition = "牡牝"
    else:
        sex_condition = "指定なし"

    if "ハンデ" in race_data02:
        weight_condition = "ハンデ"
    elif "別定" in race_data02:
        weight_condition = "別定"
    elif "馬齢" in race_data02:
        weight_condition = "馬齢"

    if any(x in race_data02 for x in ("G1", "G2", "G3")):
        race_class = "重賞"
    elif "オープン" in race_data02:
        race_class = "オープン"
    elif "3勝クラス" in race_data02:
        race_class = "3勝クラス"
    elif "2勝クラス" in race_data02:
        race_class = "2勝クラス"
    elif "1勝クラス" in race_data02:
        race_class = "1勝クラス"
    elif "新馬" in race_data02:
        race_class = "新馬"
    elif "未勝利" in race_data02:
        race_class = "未勝利"

    return {
        "race_class": race_class,
        "age_condition": age_condition,
        "sex_condition": sex_condition,
        "weight_condition": weight_condition,
    }


def parse_race_meta(soup: BeautifulSoup, race_id: str) -> Dict[str, str]:
    rid = parse_race_id_parts(race_id)
    race_name = text_or_empty(soup.select_one(".RaceName"))
    race_num = text_or_empty(soup.select_one(".RaceNum")) or rid["race_num"]
    race_data01 = text_or_empty(soup.select_one(".RaceData01"))
    race_data02 = text_or_empty(soup.select_one(".RaceData02"))
    race_date = parse_race_date_from_meta(soup)
    page_title = text_or_empty(soup.select_one("title"))

    start_time = ""
    surface = ""
    distance = ""
    turn_direction = ""
    weather = ""
    ground_state = ""
    field_size = ""
    purse_raw = ""

    m = re.search(r"(\d{1,2}:\d{2})発走", race_data01)
    if m:
        start_time = m.group(1)
    m = re.search(r"(芝|ダート|障害)\s*(\d+)m", race_data01)
    if m:
        surface = m.group(1)
        distance = m.group(2)
    m = re.search(r"\((右|左|直線)[^)]*\)", race_data01)
    if m:
        turn_direction = m.group(1)
    m = re.search(r"天候:([^ /]+)", race_data01)
    if m:
        weather = m.group(1)
    m = re.search(r"馬場:([^ /]+)", race_data01)
    if m:
        ground_state = m.group(1)
    m = re.search(r"(\d+)頭", race_data02)
    if m:
        field_size = m.group(1)
    m = re.search(r"本賞金:([0-9,]+(?:,[0-9,]+)*)万円", race_data02)
    if m:
        purse_raw = m.group(1)

    meta = {
        "race_id": race_id,
        "race_date": race_date,
        "year": rid["year"],
        "place_code": rid["place_code"],
        "place": PLACE_MAP.get(rid["place_code"], ""),
        "kai": rid["kai"],
        "day": rid["day"],
        "race_num": race_num,
        "race_name": race_name,
        "grade": parse_grade(race_name, race_data02, page_title),
        "start_time": start_time,
        "surface": surface,
        "distance": distance,
        "turn_direction": turn_direction,
        "weather": weather,
        "ground_state": ground_state,
        "field_size": field_size,
        "purse_raw": purse_raw,
        "race_data01_raw": race_data01,
        "race_data02_raw": race_data02,
    }
    meta.update(parse_conditions(race_data02))
    return meta


def parse_sex_age(text: str) -> Tuple[str, str]:
    m = re.match(r"([牡牝セ騙])\s*(\d+)", text)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def parse_body_weight(text: str) -> Tuple[str, str]:
    m = re.match(r"(\d+)\s*\(([+-]?\d+)\)", text)
    if m:
        return m.group(1), m.group(2)
    m = re.match(r"(\d+)", text)
    if m:
        return m.group(1), ""
    return "", ""


def extract_id_from_href(href: str, kind: str) -> str:
    patterns = {
        "horse": r"/horse/(\d+)/?",
        "jockey": r"/jockey/(?:result/recent/)?([^/]+)/?",
        "trainer": r"/trainer/(?:result/recent/)?([^/]+)/?",
    }
    m = re.search(patterns[kind], href)
    return m.group(1) if m else ""


def parse_corner_pass_table(soup: BeautifulSoup) -> str:
    table = soup.select_one("table.Corner_Num")
    if not table:
        return ""
    parts = []
    for tr in table.select("tr"):
        th = text_or_empty(tr.find("th"))
        td = text_or_empty(tr.find("td"))
        if th or td:
            parts.append(f"{th}:{td}" if th else td)
    return " | ".join(parts)


def parse_lap_table(soup: BeautifulSoup) -> Tuple[str, str]:
    table = soup.select_one("table.Race_HaronTime")
    pace = ""
    lap_text = ""
    pace_el = soup.find(string=lambda s: isinstance(s, str) and "ペース:" in s)
    if pace_el:
        pace = normalize_space(str(pace_el)).replace("ペース:", "").strip()

    if table:
        values = []
        for tr in table.select("tr"):
            cells = [text_or_empty(c) for c in tr.find_all(["th", "td"])]
            if cells:
                values.append(" / ".join(cells))
        lap_text = " | ".join(values)
    return pace, lap_text


def parse_result_rows(soup: BeautifulSoup, race_meta: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    corner_summary = parse_corner_pass_table(soup)
    pace, lap_summary = parse_lap_table(soup)

    table = soup.select_one("table.RaceTable01")
    if not table:
        return rows

    for tr in table.select("tr.HorseList"):
        tds = [text_or_empty(td) for td in tr.find_all("td")]
        if len(tds) < 15:
            continue

        horse_id = jockey_id = trainer_id = ""
        horse_a = tr.select_one('a[href*="/horse/"]')
        jockey_a = tr.select_one('a[href*="/jockey/"]')
        trainer_a = tr.select_one('a[href*="/trainer/"]')
        if horse_a and horse_a.get("href"):
            horse_id = extract_id_from_href(horse_a["href"], "horse")
        if jockey_a and jockey_a.get("href"):
            jockey_id = extract_id_from_href(jockey_a["href"], "jockey")
        if trainer_a and trainer_a.get("href"):
            trainer_id = extract_id_from_href(trainer_a["href"], "trainer")

        sex, age = parse_sex_age(tds[4])
        trainer_area = ""
        trainer_name = tds[13]
        m = re.match(r"(美浦|栗東|地方|海外)\s+(.*)", tds[13])
        if m:
            trainer_area = m.group(1)
            trainer_name = m.group(2)
        body_weight, body_weight_diff = parse_body_weight(tds[14])

        row = {
            **race_meta,
            "horse_id": horse_id,
            "horse_name": tds[3],
            "sex": sex,
            "age": age,
            "gate": tds[1],
            "horse_number": tds[2],
            "jockey_id": jockey_id,
            "jockey_name": tds[6],
            "assigned_weight": tds[5],
            "trainer_id": trainer_id,
            "trainer_area": trainer_area,
            "trainer_name": trainer_name,
            "finish_position": tds[0],
            "finish_time": tds[7],
            "margin": tds[8],
            "popularity": tds[9],
            "win_odds": tds[10],
            "last3f": tds[11],
            "corner_pass": tds[12],
            "corner_summary": corner_summary,
            "pace": pace,
            "lap_summary": lap_summary,
            "body_weight": body_weight,
            "body_weight_diff": body_weight_diff,
        }
        rows.append(row)

    return rows


def split_multi_value(text: str) -> List[str]:
    return [x for x in normalize_space(text).split(" ") if x]


def build_combinations(bet_type: str, raw_text: str) -> List[str]:
    tokens = split_multi_value(raw_text)
    if not tokens:
        return []
    if bet_type in {"単勝", "複勝"}:
        return tokens
    if bet_type == "ワイド":
        return ["-".join(tokens[i:i+2]) for i in range(0, len(tokens), 2) if len(tokens[i:i+2]) == 2]
    if bet_type in {"枠連", "馬連", "馬単", "枠単", "3連複", "3連単"}:
        return ["-".join(tokens)]
    return ["-".join(tokens)]


def parse_payback_rows(soup: BeautifulSoup, race_id: str, race_date: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for table in soup.select(".Payout_Detail_Table"):
        for tr in table.select("tr"):
            cells = [text_or_empty(x) for x in tr.find_all(["th", "td"])]
            if len(cells) < 4:
                continue
            bet_type = cells[0]
            combinations = build_combinations(bet_type, cells[1])
            payouts = split_multi_value(cells[2].replace("円", ""))
            popularities = split_multi_value(cells[3].replace("人気", ""))
            n = max(len(combinations), len(payouts), len(popularities))
            for i in range(n):
                rows.append(
                    {
                        "race_id": race_id,
                        "race_date": race_date,
                        "bet_type": bet_type,
                        "combination": combinations[i] if i < len(combinations) else "",
                        "payout_yen": payouts[i] if i < len(payouts) else "",
                        "popularity": popularities[i] if i < len(popularities) else "",
                    }
                )
    return rows


# ---------------------------
# fetch and validate result page
# ---------------------------
def result_page_seems_valid(html: str) -> bool:
    indicators = [
        "RaceTable01",
        "Payout_Detail_Table",
        "race/result.html?race_id=",
        "レース結果",
        "払戻",
    ]
    return any(ind in html for ind in indicators) and ("RaceName" in html or "RaceData01" in html)


def fetch_race_result_outcome(
    session: requests.Session,
    race_id: str,
    cfg: ScraperConfig,
    target_month: str,
) -> ResultFetchOutcome:
    url = RESULT_URL.format(race_id=race_id)
    html = fetch_text(session, url, cfg)
    random_sleep(cfg)

    if not html:
        return ResultFetchOutcome(html=None, race_date="", status="failed_or_unavailable")
    if not result_page_seems_valid(html):
        return ResultFetchOutcome(html=None, race_date="", status="failed_or_unavailable")

    soup = BeautifulSoup(html, "html.parser")
    race_date = parse_race_date_from_meta(soup)
    if not race_date:
        return ResultFetchOutcome(html=None, race_date="", status="date_not_found")
    if not race_date.startswith(target_month + "-"):
        return ResultFetchOutcome(html=None, race_date=race_date, status="skipped_out_of_month")

    return ResultFetchOutcome(html=html, race_date=race_date, status="ok")


def save_raw_html(cfg: ScraperConfig, race_id: str, html: str) -> None:
    if not cfg.save_raw_html:
        return
    path = cfg.output_dir / "raw_html" / f"{race_id}.html"
    path.write_text(html, encoding="utf-8")


# ---------------------------
# main flow
# ---------------------------
def scrape_month(month: str, cfg: ScraperConfig) -> None:
    start, end = month_to_range(month)
    ensure_dirs(cfg)

    result_csv = cfg.output_dir / f"race_result_{month}.csv"
    payback_csv = cfg.output_dir / f"payback_{month}.csv"
    meta_csv = cfg.output_dir / f"race_meta_{month}.csv"
    log_csv = cfg.output_dir / f"scrape_log_{month}.csv"

    done_race_ids = load_existing_values(result_csv, "race_id")
    session = requests.Session()

    all_race_ids: Set[str] = set()
    for d in daterange(start, end):
        discovered = discover_race_ids_for_date(session, d, cfg)
        if discovered:
            print(f"[DISCOVER] {d} -> {len(discovered)} candidate race_ids")
        all_race_ids.update(discovered)

    all_race_ids = {rid for rid in all_race_ids if rid not in done_race_ids}
    print(f"[INFO] target month={month} candidates={len(all_race_ids)}")

    for race_id in sorted(all_race_ids):
        outcome = fetch_race_result_outcome(session, race_id, cfg, month)
        if outcome.status != "ok" or not outcome.html:
            append_rows(
                log_csv,
                [{
                    "race_id": race_id,
                    "race_date": outcome.race_date,
                    "status": outcome.status,
                    "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    "html_sha1": "",
                }],
            )
            continue

        save_raw_html(cfg, race_id, outcome.html)
        soup = BeautifulSoup(outcome.html, "html.parser")
        race_meta = parse_race_meta(soup, race_id)
        result_rows = parse_result_rows(soup, race_meta)
        payback_rows = parse_payback_rows(soup, race_id, race_meta.get("race_date", ""))

        if not result_rows:
            append_rows(
                log_csv,
                [{
                    "race_id": race_id,
                    "race_date": race_meta.get("race_date", ""),
                    "status": "parsed_no_rows",
                    "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    "html_sha1": html_sha1(outcome.html),
                }],
            )
            continue

        append_rows(result_csv, result_rows)
        append_rows(meta_csv, [race_meta])
        append_rows(payback_csv, payback_rows)
        append_rows(
            log_csv,
            [{
                "race_id": race_id,
                "race_date": race_meta.get("race_date", ""),
                "status": "ok",
                "fetched_at": datetime.now().isoformat(timespec="seconds"),
                "html_sha1": html_sha1(outcome.html),
            }],
        )
        print(f"[OK] {race_id} date={race_meta.get('race_date','')} rows={len(result_rows)} paybacks={len(payback_rows)}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="netkeiba monthly scraper PoC (v3)")
    parser.add_argument("--month", required=True, help="target month in YYYY-MM format")
    parser.add_argument("--output-dir", default="./data/netkeiba_poc_v3", help="output directory")
    parser.add_argument("--save-raw-html", action="store_true", help="save fetched result html files")
    parser.add_argument("--sleep-min", type=float, default=DEFAULT_SLEEP_MIN)
    parser.add_argument("--sleep-max", type=float, default=DEFAULT_SLEEP_MAX)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    cfg = ScraperConfig(
        output_dir=Path(args.output_dir),
        save_raw_html=args.save_raw_html,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    scrape_month(args.month, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
