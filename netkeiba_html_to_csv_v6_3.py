#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

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
    (r"G1", "G1"),
    (r"G2", "G2"),
    (r"G3", "G3"),
    (r"\bL\b|リステッド", "L"),
    (r"オープン", "オープン"),
    (r"3勝クラス", "3勝クラス"),
    (r"2勝クラス", "2勝クラス"),
    (r"1勝クラス", "1勝クラス"),
    (r"未勝利", "未勝利"),
    (r"新馬", "新馬"),
]

WEIGHT_CONDITIONS = ["馬齢", "別定", "定量", "ハンデ"]


@dataclass
class Config:
    html_dir: Path
    month: str
    output_dir: Path


def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def normalize_zen_digits(s: Optional[str]) -> str:
    trans = str.maketrans("０１２３４５６７８９", "0123456789")
    return (s or "").translate(trans)


def extract_race_id_from_filename(path: Path) -> Optional[str]:
    m = re.search(r"(\d{12})", path.name)
    return m.group(1) if m else None


def month_match(date_str: str, month: str) -> bool:
    return bool(date_str) and date_str.startswith(month + "-")


def append_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


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


def parse_race_date(soup: BeautifulSoup) -> str:
    text = clean_text(soup.get_text(" ", strip=True))
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    return ""


def parse_track_detail(race_data01_raw: str) -> Tuple[str, str, str, str]:
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


def parse_race_class(race_data02_raw: str, race_name: str) -> str:
    src = normalize_zen_digits(f"{race_name} {race_data02_raw}")
    for pat, label in RACE_CLASS_PATTERNS:
        if re.search(normalize_zen_digits(pat), src):
            return normalize_zen_digits(label)
    m = re.search(r"([123])勝クラス", src)
    if m:
        return f"{m.group(1)}勝クラス"
    return ""


def parse_age_condition(race_data02_raw: str) -> str:
    m = re.search(r"サラ系\s*([０-９0-9３４５６７８９歳以上]+)", race_data02_raw)
    if m:
        return clean_text(m.group(1))
    return ""


def parse_sex_condition(race_data02_raw: str) -> str:
    # remove common tokens and extract constraint-ish segments
    candidates = re.findall(r"(牝\[指\]|牝\(特指\)|牝\(国際\)|牝\(国際\)\(特指\)|牝\[\w\]|牝|\(混\)\[指\]|\(混\)\(特指\)|\(混\)|\(国際\)\(指\)|\(国際\)\(特指\)|\(国際\)|\[指\]|\(特指\))", race_data02_raw)
    merged = "".join(candidates)
    return merged if merged else "指定なし"


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
        "grade": parse_race_class(rd2, race_name) if parse_race_class(rd2, race_name) in {"G1","G2","G3","L"} else "",
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
        "race_class": parse_race_class(rd2, race_name),
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
    rows = []
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

        row = {
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
        }
        rows.append(row)
    return rows


def split_multi_values(text: str) -> List[str]:
    return [x for x in re.split(r"\s+", clean_text(text)) if x]


def _extract_combo_groups(result_td) -> List[str]:
    groups: List[str] = []

    # Pair/triple bets are usually structured as one <ul> per combination.
    uls = result_td.find_all("ul", recursive=False)
    if uls:
        for ul in uls:
            vals = [clean_text(li.get_text(" ", strip=True)) for li in ul.find_all("li", recursive=False)]
            vals = [v for v in vals if v]
            if vals:
                groups.append("-".join(vals))
        return groups

    # Single-horse bets (単勝/複勝) are usually structured as repeated <div><span>...</span></div>
    for div in result_td.find_all("div", recursive=False):
        val = clean_text(div.get_text(" ", strip=True))
        if val:
            groups.append(val)
    if groups:
        return groups

    # Fallback
    vals = split_multi_values(result_td.get_text(" ", strip=True))
    return vals


def _extract_value_groups(td) -> List[str]:
    # Payout / popularity cells often use a single <span> with <br> separators.
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

        # Align by actual combination groups; ignore blank DOM placeholders.
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


def process_file(path: Path, cfg: Config) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], Dict[str, str]]:
    race_id = extract_race_id_from_filename(path)
    if not race_id:
        return [], [], [], {"file": path.name, "status": "skipped_no_race_id", "race_id": "", "race_date": ""}

    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    race_meta = parse_race_meta(soup, race_id)
    if not month_match(race_meta["race_date"], cfg.month):
        return [], [], [], {"file": path.name, "status": "skipped_out_of_month", "race_id": race_id, "race_date": race_meta["race_date"]}

    result_rows = parse_result_rows(soup, race_meta)
    payback_rows = parse_payback_rows(soup, race_id, race_meta["race_date"])
    return result_rows, [race_meta], payback_rows, {"file": path.name, "status": "ok" if result_rows else "parsed_no_rows", "race_id": race_id, "race_date": race_meta["race_date"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html-dir", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    cfg = Config(html_dir=Path(args.html_dir), month=args.month, output_dir=Path(args.output_dir))
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    result_csv = cfg.output_dir / f"race_result_{cfg.month}.csv"
    meta_csv = cfg.output_dir / f"race_meta_{cfg.month}.csv"
    payback_csv = cfg.output_dir / f"payback_{cfg.month}.csv"
    log_csv = cfg.output_dir / f"parse_log_{cfg.month}.csv"

    for html_path in sorted(cfg.html_dir.glob("*.html")):
        result_rows, meta_rows, payback_rows, log_row = process_file(html_path, cfg)
        append_rows(result_csv, result_rows)
        append_rows(meta_csv, meta_rows)
        append_rows(payback_csv, payback_rows)
        append_rows(log_csv, [log_row])

    print("done")
    print(result_csv)
    print(meta_csv)
    print(payback_csv)
    print(log_csv)


if __name__ == "__main__":
    main()
