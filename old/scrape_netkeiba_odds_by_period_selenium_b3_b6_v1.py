import argparse
import asyncio
import math
import random
import re
import time
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


BASE_URL = "https://race.netkeiba.com/odds/index.html"

DEFAULT_TIMEOUT_MS = 15000
DEFAULT_RETRIES = 3
DEFAULT_SLEEP_MIN_MS = 1200
DEFAULT_SLEEP_MAX_MS = 2500
DEFAULT_RETRY_BACKOFF_MS = 3000

ODDS_TYPE_LABELS = {
    "b1": "単勝複勝",
    "b3": "枠連",
    "b4": "馬連",
    "b5": "ワイド",
    "b6": "馬単",
    "b7": "3連複",
    "b8": "3連単",
}

DESIRED_COLS = [
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

DEFAULT_FLUSH_EVERY_RACES = 50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape netkeiba odds by period. b1-b6 use Selenium, b7-b8 use Playwright."
    )
    parser.add_argument("--period", required=True, help="Target period. Examples: 2025 or 2026-01")
    parser.add_argument("--race-result", required=True, help="Path to race_result or race_meta CSV")
    parser.add_argument("--output", required=True, help="Output odds CSV path")
    parser.add_argument(
        "--types",
        nargs="*",
        default=["b1", "b3", "b4", "b5", "b6", "b7", "b8"],
        help="Odds types to fetch",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    parser.add_argument("--slow-mo", type=int, default=0)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--sleep-min-ms", type=int, default=DEFAULT_SLEEP_MIN_MS)
    parser.add_argument("--sleep-max-ms", type=int, default=DEFAULT_SLEEP_MAX_MS)
    parser.add_argument("--retry-backoff-ms", type=int, default=DEFAULT_RETRY_BACKOFF_MS)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--flush-every-races", type=int, default=DEFAULT_FLUSH_EVERY_RACES)
    return parser.parse_args()


def build_url(odds_type: str, race_id: str) -> str:
    if odds_type == "b1":
        return f"{BASE_URL}?type=b1&race_id={race_id}&rf=shutuba_submenu"
    return f"{BASE_URL}?type={odds_type}&race_id={race_id}&housiki=c0&rf=shutuba_submenu"


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def to_float_odds(text: str) -> Optional[float]:
    s = clean_text(text).replace(",", "")
    if s in {"", "---.-", "---", "-", "取消", "除外"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_popularity(text: str) -> Optional[int]:
    m = re.search(r"(\d+)人気", clean_text(text))
    return int(m.group(1)) if m else None


def extract_race_meta_from_title(title: str, race_id: str) -> Dict[str, str]:
    title = clean_text(title)
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


def load_race_input(path: str, period: str) -> Tuple[List[str], Dict[str, Optional[int]]]:
    df = pd.read_csv(path, low_memory=False)

    if "race_id" not in df.columns:
        raise ValueError("input CSV must contain race_id column")
    if "race_date" not in df.columns:
        raise ValueError("input CSV must contain race_date column")

    df["race_id"] = df["race_id"].astype(str)
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_date"]).copy()

    if re.fullmatch(r"\d{4}-\d{2}", period):
        start = pd.to_datetime(period + "-01")
        end = start + pd.offsets.MonthBegin(1)
        df = df[(df["race_date"] >= start) & (df["race_date"] < end)].copy()
    elif re.fullmatch(r"\d{4}", period):
        year = int(period)
        df = df[df["race_date"].dt.year == year].copy()
    else:
        raise ValueError("period must be YYYY or YYYY-MM")

    race_ids = sorted(df["race_id"].dropna().astype(str).unique().tolist())

    field_size_map: Dict[str, Optional[int]] = {}
    if "field_size" in df.columns:
        tmp = df[["race_id", "field_size"]].copy()
        tmp["field_size"] = pd.to_numeric(tmp["field_size"], errors="coerce")
        tmp = tmp.dropna(subset=["field_size"]).copy()
        if not tmp.empty:
            tmp["field_size"] = tmp["field_size"].astype(int)
            field_size_map = (
                tmp.drop_duplicates(subset=["race_id"], keep="last")
                .set_index("race_id")["field_size"]
                .to_dict()
            )

    starter_count_map: Dict[str, Optional[int]] = {}
    if "finish_position" in df.columns and "horse_number" in df.columns:
        rr = df[["race_id", "horse_number", "finish_position"]].copy()
        rr["horse_number_num"] = pd.to_numeric(rr["horse_number"], errors="coerce")
        rr["finish_position_str"] = rr["finish_position"].fillna("").astype(str).str.strip()
        exclude_statuses = {"除外", "取消", "取止", "競走除外", "出走取消"}
        rr = rr[rr["horse_number_num"].notna()].copy()
        rr = rr[~rr["finish_position_str"].isin(exclude_statuses)].copy()
        if not rr.empty:
            rr["horse_number_num"] = rr["horse_number_num"].astype(int)
            starter_count_map = (
                rr.groupby("race_id")["horse_number_num"]
                .nunique()
                .astype(int)
                .to_dict()
            )

    effective_count_map: Dict[str, Optional[int]] = {}
    for race_id in race_ids:
        effective_count_map[race_id] = starter_count_map.get(race_id, field_size_map.get(race_id))

    return race_ids, effective_count_map


def load_done_pairs(output_path: Path) -> set[tuple[str, str]]:
    if not output_path.exists():
        return set()
    try:
        df = pd.read_csv(output_path, usecols=["race_id", "odds_type"], low_memory=False)
        return set(zip(df["race_id"].astype(str), df["odds_type"].astype(str)))
    except Exception:
        return set()


def expected_count_for_odds_type(odds_type: str, field_size: Optional[int]) -> Optional[int]:
    if field_size is None or pd.isna(field_size):
        return None

    n = int(field_size)
    if n <= 0:
        return None

    if odds_type in {"b4", "b5"}:
        return math.comb(n, 2) if n >= 2 else 0
    if odds_type == "b6":
        return n * (n - 1) if n >= 2 else 0
    if odds_type == "b7":
        return math.comb(n, 3) if n >= 3 else 0
    if odds_type == "b8":
        return n * (n - 1) * (n - 2) if n >= 3 else 0
    return None


def validate_expected_count(rows: List[Dict], odds_type: str, field_size: Optional[int]) -> None:
    expected = expected_count_for_odds_type(odds_type, field_size)
    if expected is None:
        return

    if not rows:
        raise RuntimeError(f"{odds_type}: no rows collected; expected={expected}")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"{odds_type}: empty dataframe; expected={expected}")

    non_null = df[df["odds"].notna()].copy()
    actual = int(non_null["combination"].astype(str).nunique())

    if actual < expected:
        raise RuntimeError(
            f"{odds_type}: insufficient non-null odds rows; expected={expected}, actual={actual}, field_size={field_size}"
        )


async def wait_between_requests(args: argparse.Namespace) -> None:
    sleep_min = max(0, int(args.sleep_min_ms))
    sleep_max = max(sleep_min, int(args.sleep_max_ms))
    if sleep_max > 0:
        await asyncio.sleep(random.uniform(sleep_min, sleep_max) / 1000.0)


# ----------------------------
# Selenium for b1-b6
# ----------------------------

def selenium_parse_float(text: str) -> Optional[float]:
    s = clean_text(text).replace(",", "")
    if s in {"", "-", "---", "---.-", "取消", "除外"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def selenium_parse_range(text: str) -> tuple[Optional[float], Optional[float]]:
    s = clean_text(text).replace(",", "")
    s = s.replace("〜", "-").replace("～", "-")
    s = re.sub(r"[‐-‒–—―]", "-", s)
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*$", s)
    if m:
        return float(m.group(1)), float(m.group(2))
    one = selenium_parse_float(s)
    return one, None


def selenium_wait_until_b1_rows_stable(driver, timeout_sec: int = 20) -> None:
    deadline = time.time() + timeout_sec
    stable_count = 0
    last_sig = None

    while time.time() < deadline:
        try:
            tan_rows = driver.find_elements(By.CSS_SELECTOR, "#odds_tan_block table tbody tr")
            fuku_rows = driver.find_elements(By.CSS_SELECTOR, "#odds_fuku_block table tbody tr")
        except Exception:
            tan_rows = []
            fuku_rows = []

        tan_sig = [clean_text(r.text) for r in tan_rows]
        fuku_sig = [clean_text(r.text) for r in fuku_rows]
        sig = (tuple(tan_sig), tuple(fuku_sig))
        enough = len(tan_rows) > 0 and len(fuku_rows) > 0

        if sig == last_sig and enough:
            stable_count += 1
        else:
            stable_count = 0
            last_sig = sig

        if stable_count >= 2:
            return

        time.sleep(0.6)


def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            c = " ".join(str(x) for x in c if str(x) != "nan")
        cols.append(clean_text(str(c)))
    df = df.copy()
    df.columns = cols
    return df


def find_first_matching_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    for col in df.columns:
        col_norm = clean_text(str(col))
        col_norm_nospace = re.sub(r"\s+", "", col_norm)
        for k in keywords:
            k_nospace = re.sub(r"\s+", "", k)
            if k in col_norm or k_nospace in col_norm_nospace:
                return col
    return None


def coerce_horse_no_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.extract(r"(\d{1,2})")[0], errors="coerce")


def coerce_win_odds_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.replace(",", "", regex=False).str.strip()
    x = x.replace({"": None, "-": None, "---": None, "---.-": None, "取消": None, "除外": None})
    return pd.to_numeric(x, errors="coerce")


def split_place_odds_series(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    x = s.astype(str).str.replace(",", "", regex=False).str.strip()
    x = x.str.replace("〜", "-", regex=False).str.replace("～", "-", regex=False)
    x = x.str.replace(r"[‐-‒–—―]", "-", regex=True)

    extracted = x.str.extract(r"^\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*$")
    odds_min = pd.to_numeric(extracted[0], errors="coerce")
    odds_max = pd.to_numeric(extracted[1], errors="coerce")

    single = pd.to_numeric(x, errors="coerce")
    odds_min = odds_min.fillna(single)

    return odds_min, odds_max


def parse_b1_table_html(table_html: str, bet_type: str, race_id: str, meta: Dict[str, str]) -> List[Dict]:
    dfs = pd.read_html(StringIO(table_html))
    if not dfs:
        return []

    df = normalize_df_columns(dfs[0])

    horse_no_col = find_first_matching_column(df, ["馬番", "馬 番"])
    horse_name_col = find_first_matching_column(df, ["馬名", "馬 名"])
    odds_col = find_first_matching_column(df, ["オッズ"])

    if horse_no_col is None or odds_col is None:
        raise RuntimeError(f"b1 {bet_type}: required columns not found. columns={list(df.columns)}")

    horse_no = coerce_horse_no_series(df[horse_no_col])
    horse_name = df[horse_name_col].astype(str).map(clean_text) if horse_name_col else pd.Series([""] * len(df))

    if bet_type == "単勝":
        odds = coerce_win_odds_series(df[odds_col])
        odds_max = pd.Series([None] * len(df))
    else:
        odds, odds_max = split_place_odds_series(df[odds_col])

    out = []
    for i in range(len(df)):
        hn = horse_no.iloc[i]
        if pd.isna(hn):
            continue

        out.append({
            **meta,
            "odds_type": "b1",
            "bet_type": bet_type,
            "combination": str(int(hn)),
            "horse_number_1": int(hn),
            "horse_number_2": None,
            "horse_number_3": None,
            "horse_name_1": "" if pd.isna(horse_name.iloc[i]) else str(horse_name.iloc[i]),
            "horse_name_2": "",
            "horse_name_3": "",
            "odds": None if pd.isna(odds.iloc[i]) else float(odds.iloc[i]),
            "odds_max": None if pd.isna(odds_max.iloc[i]) else float(odds_max.iloc[i]),
            "popularity": None,
        })

    return out


def validate_b1_rows(rows: List[Dict], field_size: Optional[int] = None) -> None:
    if not rows:
        raise RuntimeError("b1: no rows")

    df = pd.DataFrame(rows)
    tan = df[df["bet_type"] == "単勝"].copy()
    fuku = df[df["bet_type"] == "複勝"].copy()

    if tan.empty or fuku.empty:
        raise RuntimeError(f"b1: missing side tan={len(tan)} fuku={len(fuku)}")

    tan_active = tan[tan["odds"].notna()].copy()
    fuku_active = fuku[fuku["odds"].notna() | fuku["odds_max"].notna()].copy()

    if field_size is not None and not pd.isna(field_size):
        expected = int(field_size)
        if tan_active["horse_number_1"].nunique() < expected or fuku_active["horse_number_1"].nunique() < expected:
            raise RuntimeError(
                f"b1: insufficient active horse count tan={tan_active['horse_number_1'].nunique()} "
                f"fuku={fuku_active['horse_number_1'].nunique()} expected={expected}"
            )

    tan_same_ratio = (
        (tan_active["odds"].fillna(-999) == tan_active["horse_number_1"].fillna(-888)).mean()
        if not tan_active.empty else 0
    )
    if tan_same_ratio >= 0.5:
        raise RuntimeError(f"b1: suspicious tan odds equal horse numbers ratio={tan_same_ratio:.2f}")

    fuku_range_ratio = fuku["odds_max"].notna().mean() if not fuku.empty else 0
    if len(fuku) > 0 and fuku_range_ratio < 0.5:
        raise RuntimeError(f"b1: suspicious fuku range ratio={fuku_range_ratio:.2f}")


def infer_active_horse_count_from_b1(rows: List[Dict]) -> Optional[int]:
    if not rows:
        return None

    df = pd.DataFrame(rows)
    if df.empty:
        return None

    tan = df[df["bet_type"] == "単勝"].copy()
    if tan.empty:
        return None

    tan["horse_number_1"] = pd.to_numeric(tan["horse_number_1"], errors="coerce")
    active = tan[tan["horse_number_1"].notna() & tan["odds"].notna()].copy()
    if active.empty:
        return None

    return int(active["horse_number_1"].nunique())


def create_selenium_driver(headless: bool, timeout_ms: int):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(max(30, int(timeout_ms / 1000)))
    driver.implicitly_wait(0)
    return driver


def scrape_b1_with_driver(driver, race_id: str, timeout_ms: int, field_size: Optional[int] = None) -> List[Dict]:
    url = build_url("b1", race_id)
    driver.get(url)
    selenium_wait_until_b1_rows_stable(driver, timeout_sec=max(20, int(timeout_ms / 1000)))

    title = driver.title
    meta = extract_race_meta_from_title(title, race_id)

    tan_table = driver.find_element(By.CSS_SELECTOR, "#odds_tan_block table")
    fuku_table = driver.find_element(By.CSS_SELECTOR, "#odds_fuku_block table")

    tan_html = tan_table.get_attribute("outerHTML")
    fuku_html = fuku_table.get_attribute("outerHTML")

    tan_rows = parse_b1_table_html(tan_html, "単勝", race_id, meta)
    fuku_rows = parse_b1_table_html(fuku_html, "複勝", race_id, meta)
    rows = tan_rows + fuku_rows

    validate_b1_rows(rows, field_size=field_size)

    df = pd.DataFrame(rows).drop_duplicates(
        subset=["race_id", "bet_type", "horse_number_1"],
        keep="first",
    )
    return df.to_dict("records")


def selenium_scrape_b1_page(
    driver,
    race_id: str,
    timeout_ms: int,
    field_size: Optional[int] = None,
    retries: int = 3,
    retry_backoff_ms: int = 3000,
) -> List[Dict]:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            return scrape_b1_with_driver(driver, race_id, timeout_ms, field_size)
        except Exception as e:
            last_error = e
            if attempt >= retries:
                raise
            time.sleep((retry_backoff_ms * attempt) / 1000.0)

    raise last_error




def selenium_wait_until_odds_nodes_ready(driver, odds_type: str, timeout_sec: int = 20, min_ratio: float = 0.9) -> None:
    odds_type_num = int(odds_type[1:])
    deadline = time.time() + timeout_sec
    stable_hits = 0
    last_total = -1
    last_filled = -1

    while time.time() < deadline:
        try:
            nodes = driver.find_elements(By.CSS_SELECTOR, f"[id^='odds-{odds_type_num}-']")
        except Exception:
            nodes = []

        total = len(nodes)
        filled = 0
        for n in nodes:
            t = clean_text(n.text)
            if t not in {"", "---", "---.-", "-", "取消", "除外"}:
                filled += 1

        enough = total > 0 and (filled / total) >= min_ratio if total > 0 else False

        if odds_type == "b3" and total > 0:
            enough = True

        if total == last_total and filled == last_filled and enough:
            stable_hits += 1
        else:
            stable_hits = 0
            last_total = total
            last_filled = filled

        if stable_hits >= 2:
            return

        time.sleep(0.4)


def selenium_extract_combo_rows_current_view(driver, race_id: str, odds_type: str) -> List[Dict]:
    meta = extract_race_meta_from_title(driver.title, race_id)
    odds_type_num = int(odds_type[1:])
    bet_label = infer_bet_label(odds_type)

    nodes = driver.find_elements(By.CSS_SELECTOR, f"[id^='odds-{odds_type_num}-']")
    rows: List[Dict] = []

    for node in nodes:
        try:
            span_id = node.get_attribute("id") or ""
            comb_nums = extract_numbers_from_id(odds_type_num, span_id)
            if not comb_nums:
                continue

            try:
                td = node.find_element(By.XPATH, "./ancestor::td[1]")
                td_text = clean_text(td.text)
            except Exception:
                td_text = ""

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
                "odds": to_float_odds(node.text),
                "odds_max": None,
                "popularity": parse_popularity(td_text),
            })
        except Exception:
            continue

    return rows


def selenium_parse_combo_page_simple(driver, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    selenium_wait_until_odds_nodes_ready(driver, odds_type, timeout_sec=max(20, int(timeout_ms / 1000)))
    rows = selenium_extract_combo_rows_current_view(driver, race_id, odds_type)

    if rows and any(r.get("odds") is None for r in rows):
        time.sleep(1.0)
        selenium_wait_until_odds_nodes_ready(driver, odds_type, timeout_sec=max(20, int(timeout_ms / 1000)))
        rows = selenium_extract_combo_rows_current_view(driver, race_id, odds_type)

    if rows:
        df = pd.DataFrame(rows)
        df["_has_odds"] = df["odds"].notna().astype(int)
        df = df.sort_values(
            ["_has_odds", "horse_number_1", "horse_number_2", "horse_number_3"],
            ascending=[False, True, True, True],
            na_position="last",
        )
        df = df.drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        ).drop(columns=["_has_odds"])
        return df.to_dict("records")

    return rows


def selenium_scrape_simple_odds_page(
    driver,
    race_id: str,
    odds_type: str,
    timeout_ms: int,
    retries: int = 3,
    retry_backoff_ms: int = 3000,
) -> List[Dict]:
    if odds_type not in {"b3", "b4", "b5", "b6"}:
        raise ValueError(f"selenium_scrape_simple_odds_page does not support odds_type={odds_type}")

    last_error = None
    url = build_url(odds_type, race_id)

    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            return selenium_parse_combo_page_simple(driver, race_id, odds_type, timeout_ms)
        except Exception as e:
            last_error = e
            if attempt >= retries:
                raise
            time.sleep((retry_backoff_ms * attempt) / 1000.0)

    raise last_error


# ----------------------------
# Playwright for b7-b8 only
# ----------------------------

async def wait_for_page_ready(page, odds_type: str, timeout_ms: int) -> None:
    odds_type_num = int(odds_type[1:])
    await page.wait_for_selector(f"[id^='odds-{odds_type_num}-']", timeout=timeout_ms)


async def wait_until_odds_filled(page, odds_type: str, timeout_ms: int, min_ratio: float = 0.9) -> None:
    await wait_for_page_ready(page, odds_type, timeout_ms)

    if odds_type in {"b3"}:
        return

    odds_type_num = int(odds_type[1:])
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000.0)

    stable_hits = 0
    last_total = -1
    last_filled = -1

    while True:
        stats = await page.evaluate(
            f"""
            () => {{
              const nodes = Array.from(document.querySelectorAll("[id^='odds-{odds_type_num}-']"));
              const total = nodes.length;
              const filled = nodes.filter(n => {{
                const t = (n.innerText || n.textContent || '').replace(/\\s+/g, ' ').trim();
                return t !== '' && t !== '---' && t !== '---.-' && t !== '-';
              }}).length;
              return {{ total, filled }};
            }}
            """
        )

        total = int(stats["total"])
        filled = int(stats["filled"])
        enough = total > 0 and filled / total >= min_ratio

        if total == last_total and filled == last_filled and enough:
            stable_hits += 1
        else:
            stable_hits = 0
            last_total = total
            last_filled = filled

        if stable_hits >= 2:
            return

        if asyncio.get_running_loop().time() >= deadline:
            return

        await page.wait_for_timeout(400)


async def extract_combo_rows_current_view(page, race_id: str, odds_type: str) -> List[Dict]:
    meta = extract_race_meta_from_title(await page.title(), race_id)
    odds_type_num = int(odds_type[1:])
    bet_label = infer_bet_label(odds_type)

    js = f"""
    () => {{
      function txt(el) {{
        return (el?.innerText || el?.textContent || '').replace(/\\s+/g, ' ').trim();
      }}

      const nodes = Array.from(document.querySelectorAll("[id^='odds-{odds_type_num}-']"));
      return nodes.map(node => {{
        const spanId = node.id || '';
        const td = node.closest('td');
        return {{
          span_id: spanId,
          odds_text: txt(node),
          td_text: txt(td)
        }};
      }});
    }}
    """
    parsed = await page.evaluate(js)
    rows: List[Dict] = []

    for item in parsed:
        span_id = item.get("span_id", "")
        comb_nums = extract_numbers_from_id(odds_type_num, span_id)
        if not comb_nums:
            continue

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
            "odds": to_float_odds(item.get("odds_text", "")),
            "odds_max": None,
            "popularity": parse_popularity(item.get("td_text", "")),
        })

    return rows


async def parse_combo_page_simple(page, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    await wait_until_odds_filled(page, odds_type, timeout_ms)
    rows = await extract_combo_rows_current_view(page, race_id, odds_type)

    if rows and any(r.get("odds") is None for r in rows):
        await page.wait_for_timeout(1000)
        await wait_until_odds_filled(page, odds_type, timeout_ms)
        rows = await extract_combo_rows_current_view(page, race_id, odds_type)

    if rows:
        df = pd.DataFrame(rows)
        df["_has_odds"] = df["odds"].notna().astype(int)
        df = df.sort_values(
            ["_has_odds", "horse_number_1", "horse_number_2", "horse_number_3"],
            ascending=[False, True, True, True],
            na_position="last",
        )
        df = df.drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        ).drop(columns=["_has_odds"])
        return df.to_dict("records")

    return rows


async def get_select_options(page) -> List[Dict[str, str]]:
    js = """
    () => {
      const selects = Array.from(document.querySelectorAll('select'));
      for (const sel of selects) {
        const options = Array.from(sel.options || []);
        const parsed = options
          .map(o => ({
            value: (o.value || '').trim(),
            text: (o.textContent || '').replace(/\\s+/g, ' ').trim()
          }))
          .filter(x => /^\\d{1,2}\\s/.test(x.text) || /^\\d{1,2}$/.test(x.value));

        if (parsed.length >= 5) {
          return parsed;
        }
      }
      return [];
    }
    """
    return await page.evaluate(js)


async def select_axis_option(page, option_value: str, option_text: str) -> bool:
    js = """
    ([value, text]) => {
      const selects = Array.from(document.querySelectorAll('select'));
      for (const sel of selects) {
        const options = Array.from(sel.options || []);
        const parsed = options
          .map(o => ({
            value: (o.value || '').trim(),
            text: (o.textContent || '').replace(/\\s+/g, ' ').trim()
          }))
          .filter(x => /^\\d{1,2}\\s/.test(x.text) || /^\\d{1,2}$/.test(x.value));

        if (parsed.length >= 5) {
          const hit = options.find(o =>
            ((o.value || '').trim() === value) ||
            (((o.textContent || '').replace(/\\s+/g, ' ').trim()) === text)
          );
          if (!hit) continue;

          sel.value = hit.value;
          sel.dispatchEvent(new Event('input', { bubbles: true }));
          sel.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
      }
      return false;
    }
    """
    return await page.evaluate(js, [option_value, option_text])


async def wait_for_select_view_change(page, odds_type: str, previous_signature: str, timeout_ms: int) -> None:
    odds_type_num = int(odds_type[1:])
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000.0)

    while True:
        signature = await page.evaluate(
            f"""
            () => {{
              const nodes = Array.from(document.querySelectorAll("[id^='odds-{odds_type_num}-']")).slice(0, 20);
              return nodes.map(n => {{
                const id = n.id || '';
                const t = (n.innerText || n.textContent || '').replace(/\\s+/g, ' ').trim();
                return id + ':' + t;
              }}).join('|');
            }}
            """
        )

        if signature and signature != previous_signature:
            return

        if asyncio.get_running_loop().time() >= deadline:
            return

        await page.wait_for_timeout(300)


async def current_view_signature(page, odds_type: str) -> str:
    odds_type_num = int(odds_type[1:])
    return await page.evaluate(
        f"""
        () => {{
          const nodes = Array.from(document.querySelectorAll("[id^='odds-{odds_type_num}-']")).slice(0, 20);
          return nodes.map(n => {{
            const id = n.id || '';
            const t = (n.innerText || n.textContent || '').replace(/\\s+/g, ' ').trim();
            return id + ':' + t;
          }}).join('|');
        }}
        """
    )


async def parse_trifecta_like_page(page, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    await wait_until_odds_filled(page, odds_type, timeout_ms, min_ratio=0.95)

    collected: List[Dict] = []
    collected.extend(await extract_combo_rows_current_view(page, race_id, odds_type))

    options = await get_select_options(page)

    for opt in options:
        try:
            before_sig = await current_view_signature(page, odds_type)
            changed = await select_axis_option(page, opt["value"], opt["text"])
            if not changed:
                continue

            await wait_for_select_view_change(page, odds_type, before_sig, timeout_ms)
            await wait_until_odds_filled(page, odds_type, timeout_ms, min_ratio=0.95)

            view_rows = await extract_combo_rows_current_view(page, race_id, odds_type)
            if view_rows and any(r.get("odds") is None for r in view_rows):
                await page.wait_for_timeout(1200)
                await wait_until_odds_filled(page, odds_type, timeout_ms, min_ratio=0.95)
                view_rows = await extract_combo_rows_current_view(page, race_id, odds_type)

            collected.extend(view_rows)
        except Exception:
            continue

    if collected:
        df = pd.DataFrame(collected)
        df["_has_odds"] = df["odds"].notna().astype(int)
        df = df.sort_values(
            ["_has_odds", "horse_number_1", "horse_number_2", "horse_number_3"],
            ascending=[False, True, True, True],
            na_position="last",
        )
        df = df.drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        ).drop(columns=["_has_odds"])
        return df.to_dict("records")

    return collected


async def scrape_one_page_playwright(page, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    url = build_url(odds_type, race_id)
    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

    if odds_type in {"b7", "b8"}:
        rows = await parse_trifecta_like_page(page, race_id, odds_type, timeout_ms)
    else:
        rows = await parse_combo_page_simple(page, race_id, odds_type, timeout_ms)

    return rows


async def scrape_one_page(
    playwright_page,
    selenium_driver,
    race_id: str,
    odds_type: str,
    timeout_ms: int,
    field_size: Optional[int],
    args: argparse.Namespace,
) -> List[Dict]:
    if odds_type == "b1":
        return selenium_scrape_b1_page(
            selenium_driver,
            race_id,
            timeout_ms,
            field_size,
            args.retries,
            args.retry_backoff_ms,
        )
    if odds_type in {"b3", "b4", "b5", "b6"}:
        return selenium_scrape_simple_odds_page(
            selenium_driver,
            race_id,
            odds_type,
            timeout_ms,
            args.retries,
            args.retry_backoff_ms,
        )
    return await scrape_one_page_playwright(playwright_page, race_id, odds_type, timeout_ms)


async def scrape_one_page_with_retry(
    playwright_page,
    selenium_driver,
    race_id: str,
    odds_type: str,
    timeout_ms: int,
    field_size: Optional[int],
    args: argparse.Namespace,
) -> List[Dict]:
    last_error = None
    retries = max(1, int(args.retries))

    for attempt in range(1, retries + 1):
        try:
            rows = await scrape_one_page(
                playwright_page=playwright_page,
                selenium_driver=selenium_driver,
                race_id=race_id,
                odds_type=odds_type,
                timeout_ms=timeout_ms,
                field_size=field_size,
                args=args,
            )

            if odds_type == "b1":
                validate_b1_rows(rows, field_size=field_size)
            else:
                validate_expected_count(rows, odds_type, field_size)

            return rows

        except Exception as e:
            last_error = e
            if attempt >= retries:
                raise

            backoff_ms = max(0, int(args.retry_backoff_ms)) * attempt
            if backoff_ms > 0:
                await asyncio.sleep(backoff_ms / 1000.0)

    raise last_error


def ensure_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in DESIRED_COLS:
        if c not in out.columns:
            out[c] = None
    return out[DESIRED_COLS].copy()


def merge_odds_frames(existing_df: pd.DataFrame, new_rows: List[Dict]) -> pd.DataFrame:
    frames = []
    if existing_df is not None and not existing_df.empty:
        frames.append(existing_df.copy())
    if new_rows:
        df_new = pd.DataFrame(new_rows)
        if not df_new.empty:
            frames.append(df_new)

    if frames:
        df_out = pd.concat(frames, ignore_index=True)
    else:
        df_out = pd.DataFrame(columns=DESIRED_COLS)

    if not df_out.empty:
        df_out["race_id"] = df_out["race_id"].astype(str)
        df_out["odds_type"] = df_out["odds_type"].astype(str)
        df_out["bet_type"] = df_out["bet_type"].astype(str)
        df_out["combination"] = df_out["combination"].astype(str)
        df_out["_has_odds"] = df_out["odds"].notna().astype(int)
        df_out = df_out.sort_values(
            ["_has_odds", "race_id", "odds_type", "bet_type", "combination"],
            ascending=[False, True, True, True, True],
            na_position="last",
        )
        df_out = df_out.drop_duplicates(
            subset=["race_id", "odds_type", "bet_type", "combination"],
            keep="first",
        ).drop(columns=["_has_odds"])
        df_out = ensure_output_schema(df_out)
        df_out = df_out.sort_values(
            ["race_id", "odds_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            na_position="last",
        ).reset_index(drop=True)
    else:
        df_out = pd.DataFrame(columns=DESIRED_COLS)

    return df_out


def merge_error_frames(existing_df: pd.DataFrame, new_rows: List[Dict]) -> pd.DataFrame:
    frames = []
    if existing_df is not None and not existing_df.empty:
        frames.append(existing_df.copy())
    if new_rows:
        df_new = pd.DataFrame(new_rows)
        if not df_new.empty:
            frames.append(df_new)

    if not frames:
        return pd.DataFrame(columns=["race_id", "odds_type", "url", "error"])

    df_err = pd.concat(frames, ignore_index=True)
    if not df_err.empty:
        df_err["race_id"] = df_err["race_id"].astype(str)
        df_err["odds_type"] = df_err["odds_type"].astype(str)
        df_err = df_err.drop_duplicates(
            subset=["race_id", "odds_type", "url", "error"],
            keep="last",
        ).reset_index(drop=True)
    return df_err


def flush_buffers(
    output_path: Path,
    error_path: Path,
    existing_odds_df: pd.DataFrame,
    existing_error_df: pd.DataFrame,
    all_rows_buffer: List[Dict],
    error_rows_buffer: List[Dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    odds_df = existing_odds_df
    err_df = existing_error_df

    if all_rows_buffer:
        odds_df = merge_odds_frames(existing_odds_df, all_rows_buffer)
        odds_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        all_rows_buffer.clear()

    if error_rows_buffer:
        err_df = merge_error_frames(existing_error_df, error_rows_buffer)
        err_df.to_csv(error_path, index=False, encoding="utf-8-sig")
        error_rows_buffer.clear()

    return odds_df, err_df


async def async_main(args: argparse.Namespace) -> None:
    race_ids, field_size_map = load_race_input(args.race_result, args.period)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    error_path = output_path.with_name(output_path.stem + "_errors.csv")

    if output_path.exists():
        existing_odds_df = ensure_output_schema(pd.read_csv(output_path, low_memory=False))
    else:
        existing_odds_df = pd.DataFrame(columns=DESIRED_COLS)

    if error_path.exists():
        existing_error_df = pd.read_csv(error_path, low_memory=False)
    else:
        existing_error_df = pd.DataFrame(columns=["race_id", "odds_type", "url", "error"])

    done_pairs = set()
    if args.resume and not existing_odds_df.empty:
        done_pairs = set(zip(existing_odds_df["race_id"].astype(str), existing_odds_df["odds_type"].astype(str)))

    all_rows_buffer: List[Dict] = []
    error_rows_buffer: List[Dict] = []
    flush_every_races = max(1, int(args.flush_every_races))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless, slow_mo=args.slow_mo)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="ja-JP",
        )
        playwright_page = await context.new_page()
        selenium_driver = create_selenium_driver(args.headless, args.timeout_ms)

        try:
            for i, race_id in enumerate(race_ids, start=1):
                print(f"[{i}/{len(race_ids)}] race_id={race_id}")
                effective_field_size = field_size_map.get(race_id)
                touched_race = False

                for odds_type in args.types:
                    if (race_id, odds_type) in done_pairs:
                        print(f"  {odds_type}: SKIP (already scraped)")
                        continue

                    touched_race = True

                    try:
                        rows = await scrape_one_page_with_retry(
                            playwright_page=playwright_page,
                            selenium_driver=selenium_driver,
                            race_id=race_id,
                            odds_type=odds_type,
                            timeout_ms=args.timeout_ms,
                            field_size=effective_field_size,
                            args=args,
                        )
                        all_rows_buffer.extend(rows)
                        done_pairs.add((race_id, odds_type))

                        if odds_type == "b1":
                            active_count = infer_active_horse_count_from_b1(rows)
                            if active_count is not None:
                                effective_field_size = active_count

                            df_b1 = pd.DataFrame(rows)
                            tan_cnt = int((df_b1["bet_type"] == "単勝").sum()) if not df_b1.empty else 0
                            fuku_cnt = int((df_b1["bet_type"] == "複勝").sum()) if not df_b1.empty else 0
                            active_tan = int(df_b1[(df_b1["bet_type"] == "単勝") & df_b1["odds"].notna()]["horse_number_1"].nunique()) if not df_b1.empty else 0
                            print(
                                f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): "
                                f"tan={tan_cnt}, fuku={fuku_cnt}, active={active_tan}"
                            )
                        else:
                            expected = expected_count_for_odds_type(odds_type, effective_field_size)
                            if expected is None:
                                print(f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): {len(rows)} rows")
                            else:
                                non_null_count = int(pd.DataFrame(rows)["odds"].notna().sum()) if rows else 0
                                print(
                                    f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): "
                                    f"{len(rows)} rows, non_null={non_null_count}, expected={expected}"
                                )

                    except PlaywrightTimeoutError as e:
                        error_rows_buffer.append({
                            "race_id": race_id,
                            "odds_type": odds_type,
                            "url": build_url(odds_type, race_id),
                            "error": f"timeout: {e}",
                        })
                        print(f"  {odds_type}: TIMEOUT")

                    except Exception as e:
                        error_rows_buffer.append({
                            "race_id": race_id,
                            "odds_type": odds_type,
                            "url": build_url(odds_type, race_id),
                            "error": str(e),
                        })
                        print(f"  {odds_type}: ERROR - {e}")

                if touched_race:
                    await wait_between_requests(args)

                if i % flush_every_races == 0:
                    existing_odds_df, existing_error_df = flush_buffers(
                        output_path=output_path,
                        error_path=error_path,
                        existing_odds_df=existing_odds_df,
                        existing_error_df=existing_error_df,
                        all_rows_buffer=all_rows_buffer,
                        error_rows_buffer=error_rows_buffer,
                    )
                    print(f"  flushed at race index {i}")

        finally:
            existing_odds_df, existing_error_df = flush_buffers(
                output_path=output_path,
                error_path=error_path,
                existing_odds_df=existing_odds_df,
                existing_error_df=existing_error_df,
                all_rows_buffer=all_rows_buffer,
                error_rows_buffer=error_rows_buffer,
            )
            try:
                selenium_driver.quit()
            except Exception:
                pass
            await playwright_page.close()
            await context.close()
            await browser.close()

    print(f"saved: {output_path}")
    if error_path.exists():
        print(f"errors saved: {error_path}")


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()