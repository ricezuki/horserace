#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
netkeiba scrape + parse unified script (safer grade parsing)

Features
- Supports period as YYYY or YYYY-MM
- Scrapes race result HTML with polite random sleep and retries
- Reuses cached HTML to avoid duplicate fetches
- Parses cached HTML into:
  - race_result_<period>.csv
  - race_meta_<period>.csv
  - payback_<period>.csv
  - scrape_log_<period>.csv
- Can run parse-only mode against existing cached HTML
- Year mode is processed internally month-by-month
- Grade parsing is strict and based on title / og:title / RaceName text only
  to avoid false positives such as "1勝クラス" -> "G1"

Examples
  python netkeiba_scrape_and_parse_v4.py --period 2025 --output-dir ./data/netkeiba_2025 --save-html
  python netkeiba_scrape_and_parse_v4.py --period 2026-01 --output-dir ./data/netkeiba_2026_01 --save-html
  python netkeiba_scrape_and_parse_v4.py --period 2025 --output-dir ./data/netkeiba_2025 --parse-only
"""
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

BET_TYPE_CLASS_MAP = {
    "Tansho": "単勝",
    "Fukusho": "複勝",
    "Wakuren": "枠連",
    "Umaren": "馬連",
    "Wide": "ワイド",
    "Umatan": "馬単",
    "Fuku3": "3連複",
    "Tan3": "3連単",
}

RACE_CLASS_PATTERNS = [
    (r"オープン", "オープン"),
    (r"3勝クラス", "3勝クラス"),
    (r"2勝クラス", "2勝クラス"),
    (r"1勝クラス", "1勝クラス"),
    (r"未勝利", "未勝利"),
    (r"新馬", "新馬"),
]
WEIGHT_CONDITIONS = ["馬齢", "別定", "定量", "ハンデ"]
CONDITION_RACE_CLASSES = {"新馬", "未勝利", "1勝クラス", "2勝クラス", "3勝クラス"}

# Grade is accepted only when explicitly written in parentheses.
GRADE_PATTERN = re.compile(
    r"\((G1|G2|G3|GI|GII|GIII|Jpn1|Jpn2|Jpn3|L)\)",
    re.IGNORECASE,
)


@dataclass
class Config:
    period: str
    output_dir: Path
    html_dir: Path
    parse_only: bool
    refresh_html: bool
    save_html: bool
    sleep_min: float
    sleep_max: float
    max_retries: int
    timeout: int


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def clean_text(s: Optional[str]) -> str:
    return normalize_space(s or "")


def normalize_zen_digits(s: Optional[str]) -> str:
    trans = str.maketrans("０１２３４５６７８９", "0123456789")
    return (s or "").translate(trans)


def daterange(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def parse_period(period: str) -> Tuple[date, date, str]:
    if re.fullmatch(r"\d{4}-\d{2}", period):
        start = datetime.strptime(period, "%Y-%m").date().replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1, day=1)
        else:
            next_month = start.replace(month=start.month + 1, day=1)
        end = next_month - timedelta(days=1)
        return start, end, period
    if re.fullmatch(r"\d{4}", period):
        year = int(period)
        return date(year, 1, 1), date(year, 12, 31), period
    raise ValueError("--period must be YYYY or YYYY-MM")


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


def random_sleep(cfg: Config) -> None:
    time.sleep(random.uniform(cfg.sleep_min, cfg.sleep_max))


def retry_sleep(attempt: int) -> None:
    time.sleep(10 * (2**attempt))


def html_sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def ensure_dirs(cfg: Config) -> None:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.html_dir.mkdir(parents=True, exist_ok=True)


# ---------------------------
# scraping helpers
# ---------------------------
def fetch_text(session: requests.Session, url: str, cfg: Config) -> Optional[str]:
    for attempt in range(cfg.max_retries):
        try:
            resp = session.get(url, headers=HEADERS, timeout=cfg.timeout)
            if resp.status_code == 200:
                text = resp.text
                if resp.apparent_encoding:
                    try:
                        resp.encoding = resp.apparent_encoding
                        text = resp.text
                    except Exception:
                        pass
                return text
            if resp.status_code in {429, 500, 502, 503, 504}:
                retry_sleep(attempt)
                continue
            return None
        except requests.RequestException:
            retry_sleep(attempt)
    return None


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


def extract_race_ids_from_race_links(base_url: str, html: str, target_date: date) -> Set[str]:
    target_year = target_date.strftime("%Y")
    race_ids: Set[str] = set()
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if not parsed.path.startswith("/race/"):
            continue
        if parsed.path not in {"/race/result.html", "/race/shutuba.html", "/race/ochi.html"}:
            continue
        qs = parse_qs(parsed.query)
        rid = qs.get("race_id", [""])[0]
        if not race_id_is_structurally_valid(rid, target_year):
            continue
        anchor_text = clean_text(a.get_text(" ", strip=True))
        if not anchor_text and "rf=race_list" not in href and "race_id=" not in href:
            continue
        race_ids.add(rid)

    return race_ids


def discover_race_ids_for_date(session: requests.Session, target_date: date, cfg: Config) -> Set[str]:
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


def result_page_looks_valid(html: str) -> bool:
    markers = ["RaceTable01", "RaceName", "RaceData01", "Payout_Detail_Table"]
    return sum(1 for m in markers if m in html) >= 2


def cache_path_for_race(cfg: Config, race_id: str) -> Path:
    return cfg.html_dir / f"{race_id}.html"


def fetch_or_load_race_html(session: requests.Session, race_id: str, cfg: Config) -> Tuple[Optional[str], str]:
    cache_path = cache_path_for_race(cfg, race_id)
    if cache_path.exists() and not cfg.refresh_html:
        try:
            return cache_path.read_text(encoding="utf-8", errors="ignore"), "cache"
        except Exception:
            pass

    if cfg.parse_only:
        return None, "missing_cache"

    url = RESULT_URL.format(race_id=race_id)
    html = fetch_text(session, url, cfg)
    random_sleep(cfg)
    if not html or not result_page_looks_valid(html):
        return None, "failed_or_unavailable"

    if cfg.save_html:
        cache_path.write_text(html, encoding="utf-8", errors="ignore")
    return html, "fetched"


# ---------------------------
# parsing helpers
# ---------------------------
def parse_race_date(soup: BeautifulSoup) -> str:
    desc = soup.select_one('meta[name="description"]')
    if desc:
        content = desc.get("content", "")
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", content)
        if m:
            y, mo, d = m.groups()
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    text = clean_text(soup.get_text(" ", strip=True))
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    return ""


def period_match(date_str: str, period: str) -> bool:
    return bool(date_str) and date_str.startswith(period)


def extract_id_from_href(href: str, kind: str) -> str:
    if kind == "horse":
        m = re.search(r"/horse/(\d+)", href)
    elif kind == "jockey":
        m = re.search(r"/jockey/(?:result/recent/)?(\d+)", href)
    elif kind == "trainer":
        m = re.search(r"/trainer/(?:result/recent/)?(\d+)", href)
    else:
        m = None
    return m.group(1) if m else ""


def parse_track_detail(race_data01_raw: str) -> Tuple[str, str, str, str, str]:
    surface = ""
    distance = ""
    turn_direction = ""
    course_inout = ""
    track_variant = ""

    m = re.search(r"(芝|ダ|障)\s*(\d+)m\s*\(([^)]*)\)", race_data01_raw)
    if m:
        surface = m.group(1)
        distance = m.group(2)
        inside = clean_text(m.group(3))
        parts = inside.split()
        if parts:
            turn_direction = parts[0]
        for p in parts[1:]:
            if p in {"内", "外"}:
                course_inout = p
            elif re.fullmatch(r"[A-D]", p):
                track_variant = p
    else:
        m = re.search(r"(芝|ダ|障)\s*(\d+)m", race_data01_raw)
        if m:
            surface = m.group(1)
            distance = m.group(2)
        m = re.search(r"\((右|左|直線)(?:\s+(内|外))?(?:\s+([A-D]))?\)", race_data01_raw)
        if m:
            turn_direction = m.group(1) or ""
            course_inout = m.group(2) or ""
            track_variant = m.group(3) or ""

    return surface, distance, turn_direction, course_inout, track_variant


def parse_race_class(race_data02_raw: str) -> str:
    src = normalize_zen_digits(race_data02_raw)
    for pat, label in RACE_CLASS_PATTERNS:
        if re.search(normalize_zen_digits(pat), src):
            return normalize_zen_digits(label)
    # fallback in case formatting is slightly odd
    m = re.search(r"([123])\s*勝クラス", src)
    if m:
        return f"{m.group(1)}勝クラス"
    return ""


def normalize_grade_token(token: str) -> str:
    t = token.strip().upper()
    roman_map = {"GI": "G1", "GII": "G2", "GIII": "G3"}
    if t in roman_map:
        return roman_map[t]
    if t.startswith("JPN"):
        return "Jpn" + t[3:]
    return t


def extract_grade_strict(soup: BeautifulSoup) -> str:
    candidates: List[str] = []

    og_title = soup.select_one('meta[property="og:title"]')
    if og_title and og_title.get("content"):
        candidates.append(og_title["content"])

    if soup.title and soup.title.string:
        candidates.append(soup.title.string)

    race_name_el = soup.select_one(".RaceName")
    if race_name_el:
        txt = race_name_el.get_text(" ", strip=True)
        if txt:
            candidates.append(txt)

    for text in candidates:
        m = GRADE_PATTERN.search(text)
        if m:
            return normalize_grade_token(m.group(1))

    return ""


def parse_age_condition(race_data02_raw: str) -> str:
    text = normalize_zen_digits(race_data02_raw)
    m = re.search(r"サラ系\s*([0-9]+歳以上|[0-9]+歳)", text)
    if m:
        return clean_text(m.group(1)).translate(str.maketrans("0123456789", "０１２３４５６７８９"))
    return ""


def parse_sex_condition(race_data02_raw: str) -> str:
    # remove spaces to stabilize patterns like "(混) 牝(特指)"
    text = re.sub(r"\s+", "", race_data02_raw)
    patterns = [
        r"牝\[指\]",
        r"牝\(特指\)",
        r"牝\(国際\)",
        r"牝\(国際\)\(特指\)",
        r"\(混\)牝\(特指\)",
        r"\(国際\)牝\(特指\)",
        r"\(混\)\[指\]",
        r"\(混\)\(特指\)",
        r"\(国際\)\(指\)",
        r"\(国際\)\(特指\)",
        r"\(国際\)",
        r"\(混\)",
        r"\[指\]",
        r"\(特指\)",
        r"牝",
    ]
    matches: List[str] = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            val = m.group(0)
            if val not in matches:
                matches.append(val)
    return "".join(matches) if matches else "指定なし"


def parse_weight_condition(race_data02_raw: str) -> str:
    for wc in WEIGHT_CONDITIONS:
        if wc in race_data02_raw:
            return wc
    return ""


def parse_race_meta(soup: BeautifulSoup, race_id: str) -> Dict[str, str]:
    race_name = clean_text(soup.select_one(".RaceName").get_text(" ", strip=True)) if soup.select_one(".RaceName") else ""
    rd1 = clean_text(soup.select_one(".RaceData01").get_text(" ", strip=True)) if soup.select_one(".RaceData01") else ""
    rd2 = clean_text(soup.select_one(".RaceData02").get_text(" ", strip=True)) if soup.select_one(".RaceData02") else ""
    race_date = parse_race_date(soup)

    start_time = ""
    m = re.search(r"(\d{1,2}:\d{2})発走", rd1)
    if m:
        start_time = m.group(1)

    surface, distance, turn_direction, course_inout, track_variant = parse_track_detail(rd1)
    weather = re.search(r"天候:([^ /]+)", rd1)
    ground_state = re.search(r"馬場:([^ /]+)", rd1)
    field_size = re.search(r"(\d+)頭", rd2)
    purse_raw = re.search(r"本賞金:([0-9,]+(?:,[0-9,]+)*)万円", rd2)

    place_code = race_id[4:6]
    race_class = parse_race_class(rd2)
    grade = extract_grade_strict(soup)

    # Never allow conditions/newcomer races to keep a grade.
    if race_class in CONDITION_RACE_CLASSES:
        grade = ""

    return {
        "race_id": race_id,
        "race_date": race_date,
        "year": race_id[:4],
        "place_code": place_code,
        "place": PLACE_MAP.get(place_code, ""),
        "kai": str(int(race_id[6:8])),
        "day": str(int(race_id[8:10])),
        "race_num": f"{int(race_id[10:12])}R",
        "race_name": race_name,
        "grade": grade,
        "start_time": start_time,
        "surface": surface,
        "distance": distance,
        "turn_direction": turn_direction,
        "course_inout": course_inout,
        "track_variant": track_variant,
        "track_detail": clean_text(" ".join([x for x in [turn_direction, course_inout, track_variant] if x])),
        "weather": weather.group(1) if weather else "",
        "ground_state": ground_state.group(1) if ground_state else "",
        "field_size": field_size.group(1) if field_size else "",
        "purse_raw": purse_raw.group(1) if purse_raw else "",
        "race_data01_raw": rd1,
        "race_data02_raw": rd2,
        "race_class": race_class,
        "age_condition": parse_age_condition(rd2),
        "sex_condition": parse_sex_condition(rd2),
        "weight_condition": parse_weight_condition(rd2),
    }


def parse_weight_cell(td) -> Tuple[str, str]:
    if td is None:
        return "", ""
    raw = clean_text(td.get_text(" ", strip=True))
    m = re.search(r"(\d+)\s*\(([+-]?\d+)\)", raw)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r"(\d+)", raw)
    return (m.group(1), "") if m else ("", "")


def parse_sex_age(text: str) -> Tuple[str, str]:
    m = re.match(r"([牡牝セ騙])(\d+)", clean_text(text))
    return (m.group(1), m.group(2)) if m else ("", "")


def parse_result_rows(soup: BeautifulSoup, race_meta: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for tr in soup.select("table.RaceTable01 tr.HorseList"):
        tds = tr.find_all("td")
        if len(tds) < 15:
            continue
        vals = [clean_text(td.get_text(" ", strip=True)) for td in tds]
        sex, age = parse_sex_age(vals[4])
        body_weight, body_weight_diff = parse_weight_cell(tds[14])

        horse_a = tr.select_one('a[href*="/horse/"]')
        jockey_a = tr.select_one('a[href*="/jockey/"]')
        trainer_a = tr.select_one('a[href*="/trainer/"]')

        trainer_area = ""
        trainer_name = vals[13]
        m = re.match(r"(美浦|栗東|地方|海外)\s*(.*)", vals[13])
        if m:
            trainer_area, trainer_name = m.group(1), m.group(2)

        rows.append({
            **race_meta,
            "horse_id": extract_id_from_href(horse_a.get("href", ""), "horse") if horse_a else "",
            "horse_name": vals[3],
            "sex": sex,
            "age": age,
            "gate": vals[1],
            "horse_number": vals[2],
            "jockey_id": extract_id_from_href(jockey_a.get("href", ""), "jockey") if jockey_a else "",
            "jockey_name": vals[6],
            "assigned_weight": vals[5],
            "trainer_id": extract_id_from_href(trainer_a.get("href", ""), "trainer") if trainer_a else "",
            "trainer_area": trainer_area,
            "trainer_name": trainer_name,
            "finish_position": vals[0],
            "finish_time": vals[7],
            "margin": vals[8],
            "popularity": vals[9],
            "win_odds": vals[10],
            "last3f": vals[11],
            "corner_pass": vals[12],
            "corner_summary": "",
            "body_weight": body_weight,
            "body_weight_diff": body_weight_diff,
        })
    return rows


def split_multi_values(text: str) -> List[str]:
    return [x for x in re.split(r"\s+", clean_text(text)) if x]


def _extract_combo_groups(result_td) -> List[str]:
    groups: List[str] = []
    uls = result_td.find_all("ul", recursive=False)
    if uls:
        for ul in uls:
            vals = [clean_text(li.get_text(" ", strip=True)) for li in ul.find_all("li", recursive=False)]
            vals = [v for v in vals if v]
            if vals:
                groups.append("-".join(vals))
        return groups
    for div in result_td.find_all("div", recursive=False):
        val = clean_text(div.get_text(" ", strip=True))
        if val:
            groups.append(val)
    if groups:
        return groups
    return split_multi_values(result_td.get_text(" ", strip=True))


def _extract_value_groups(td) -> List[str]:
    vals = []
    for s in td.stripped_strings:
        v = clean_text(s).replace("円", "").replace("人気", "")
        if v:
            vals.append(v)
    return vals


def parse_payback_rows(soup: BeautifulSoup, race_id: str, race_date: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for tr in soup.select("table.Payout_Detail_Table tr"):
        classes = tr.get("class", [])
        bet_type = ""
        for c in classes:
            if c in BET_TYPE_CLASS_MAP:
                bet_type = BET_TYPE_CLASS_MAP[c]
                break
        if not bet_type:
            continue

        tds = tr.find_all("td", recursive=False)
        if len(tds) < 3:
            continue

        combos = _extract_combo_groups(tds[0])
        payouts = _extract_value_groups(tds[1])
        pops = _extract_value_groups(tds[2])
        n = max(len(combos), len(payouts), len(pops))
        for i in range(n):
            combination = combos[i] if i < len(combos) else ""
            payout = payouts[i] if i < len(payouts) else ""
            popularity = pops[i] if i < len(pops) else ""
            if not combination and not payout and not popularity:
                continue
            rows.append({
                "race_id": race_id,
                "race_date": race_date,
                "bet_type": bet_type,
                "combination": combination,
                "payout_yen": payout,
                "popularity": popularity,
            })
    return rows


# ---------------------------
# period helpers
# ---------------------------
def iter_month_ranges(period: str) -> List[Tuple[date, date, str]]:
    if re.fullmatch(r"\d{4}", period):
        year = int(period)
        ranges = []
        for month in range(1, 13):
            start = date(year, month, 1)
            if month == 12:
                end = date(year, 12, 31)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)
            ranges.append((start, end, f"{year:04d}-{month:02d}"))
        return ranges
    start, end, label = parse_period(period)
    return [(start, end, label)]


# ---------------------------
# main workflow
# ---------------------------
def discover_all_race_ids(session: requests.Session, start: date, end: date, cfg: Config) -> Set[str]:
    race_ids: Set[str] = set()
    for d in daterange(start, end):
        race_ids.update(discover_race_ids_for_date(session, d, cfg))
    return race_ids


def process_html_text(html: str, race_id: str, cfg: Config) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    race_meta = parse_race_meta(soup, race_id)
    if not period_match(race_meta["race_date"], cfg.period):
        return [], [], [], {
            "race_id": race_id,
            "race_date": race_meta["race_date"],
            "status": "skipped_out_of_period",
            "html_sha1": html_sha1(html),
            "source": "html",
        }
    result_rows = parse_result_rows(soup, race_meta)
    payback_rows = parse_payback_rows(soup, race_id, race_meta["race_date"])
    return result_rows, [race_meta], payback_rows, {
        "race_id": race_id,
        "race_date": race_meta["race_date"],
        "status": "ok" if result_rows else "parsed_no_rows",
        "html_sha1": html_sha1(html),
        "source": "html",
    }


def collect_cached_race_ids(html_dir: Path) -> List[str]:
    return sorted({
        m.group(1)
        for path in html_dir.glob("*.html")
        if (m := re.search(r"(\d{12})", path.name))
    })


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", required=True, help="YYYY or YYYY-MM")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--html-dir", default=None, help="cache directory for raw html; default: <output-dir>/raw_html")
    parser.add_argument("--parse-only", action="store_true", help="do not access netkeiba; parse cached html only")
    parser.add_argument("--refresh-html", action="store_true", help="re-fetch html even if cache exists")
    parser.add_argument("--save-html", action="store_true", help="save fetched html to cache (enabled by default when scraping)")
    parser.add_argument("--sleep-min", type=float, default=3.0)
    parser.add_argument("--sleep-max", type=float, default=6.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    html_dir = Path(args.html_dir) if args.html_dir else output_dir / "raw_html"
    cfg = Config(
        period=args.period,
        output_dir=output_dir,
        html_dir=html_dir,
        parse_only=args.parse_only,
        refresh_html=args.refresh_html,
        save_html=(True if not args.parse_only else False) if not args.save_html else True,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    ensure_dirs(cfg)

    _, _, label = parse_period(cfg.period)
    result_csv = cfg.output_dir / f"race_result_{label}.csv"
    meta_csv = cfg.output_dir / f"race_meta_{label}.csv"
    payback_csv = cfg.output_dir / f"payback_{label}.csv"
    log_csv = cfg.output_dir / f"scrape_log_{label}.csv"

    # fresh output for reproducible runs
    for p in (result_csv, meta_csv, payback_csv, log_csv):
        if p.exists():
            p.unlink()

    session = requests.Session()
    month_ranges = iter_month_ranges(cfg.period)

    for month_start, month_end, month_label in month_ranges:
        print(f"processing {month_label} ...")
        if cfg.parse_only:
            race_ids = collect_cached_race_ids(cfg.html_dir)
        else:
            race_ids = sorted(discover_all_race_ids(session, month_start, month_end, cfg))

        for race_id in race_ids:
            html, source = fetch_or_load_race_html(session, race_id, cfg)
            if not html:
                append_rows(log_csv, [{
                    "race_id": race_id,
                    "race_date": "",
                    "status": source,
                    "html_sha1": "",
                    "source": source,
                    "sub_period": month_label,
                }])
                continue

            result_rows, meta_rows, payback_rows, log_row = process_html_text(html, race_id, cfg)
            log_row["source"] = source
            log_row["sub_period"] = month_label
            append_rows(result_csv, result_rows)
            append_rows(meta_csv, meta_rows)
            append_rows(payback_csv, payback_rows)
            append_rows(log_csv, [log_row])

    print("done")
    print(result_csv)
    print(meta_csv)
    print(payback_csv)
    print(log_csv)
    print(cfg.html_dir)


if __name__ == "__main__":
    main()
