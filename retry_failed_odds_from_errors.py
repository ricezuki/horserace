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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry netkeiba odds scraping only for failed race_id + odds_type pairs from errors.csv."
    )
    parser.add_argument("--errors", required=True, help="Path to original *_errors.csv")
    parser.add_argument("--race-result", required=True, help="Path to race_result or race_meta CSV")
    parser.add_argument("--output", required=True, help="Path to original odds CSV to update in place")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    parser.add_argument("--slow-mo", type=int, default=0)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--sleep-min-ms", type=int, default=DEFAULT_SLEEP_MIN_MS)
    parser.add_argument("--sleep-max-ms", type=int, default=DEFAULT_SLEEP_MAX_MS)
    parser.add_argument("--retry-backoff-ms", type=int, default=DEFAULT_RETRY_BACKOFF_MS)
    parser.add_argument(
        "--remaining-errors-output",
        default=None,
        help="Path to save still-failed rows after retry. Default: <output stem>_remaining_errors.csv",
    )
    parser.add_argument(
        "--retried-log-output",
        default=None,
        help="Path to save successfully retried rows. Default: <output stem>_retried_pairs.csv",
    )
    return parser.parse_args()


def build_url(odds_type: str, race_id: str) -> str:
    if odds_type == "b1":
        return f"{BASE_URL}?type=b1&race_id={race_id}&rf=shutuba_submenu"
    return f"{BASE_URL}?type={odds_type}&race_id={race_id}&housiki=c0&rf=shutuba_submenu"


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


def load_race_input(path: str) -> Tuple[set[str], Dict[str, Optional[int]]]:
    df = pd.read_csv(path, low_memory=False)

    if "race_id" not in df.columns:
        raise ValueError("input CSV must contain race_id column")

    df["race_id"] = df["race_id"].astype(str)
    race_id_set = set(df["race_id"].dropna().astype(str).unique().tolist())

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

    return race_id_set, field_size_map


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
# Selenium for b1 only
# ----------------------------

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
    x = x.replace({"": None, "-": None, "---": None, "---.-": None, "取消": None})
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

    if field_size is not None and not pd.isna(field_size):
        expected = int(field_size)
        if tan["horse_number_1"].nunique() < expected or fuku["horse_number_1"].nunique() < expected:
            raise RuntimeError(
                f"b1: insufficient horse count tan={tan['horse_number_1'].nunique()} "
                f"fuku={fuku['horse_number_1'].nunique()} expected={expected}"
            )

    tan_same_ratio = (tan["odds"].fillna(-999) == tan["horse_number_1"].fillna(-888)).mean()
    if tan_same_ratio >= 0.5:
        raise RuntimeError(f"b1: suspicious tan odds equal horse numbers ratio={tan_same_ratio:.2f}")

    fuku_range_ratio = fuku["odds_max"].notna().mean()
    if fuku_range_ratio < 0.5:
        raise RuntimeError(f"b1: suspicious fuku range ratio={fuku_range_ratio:.2f}")


def selenium_scrape_b1_page(
    race_id: str,
    headless: bool,
    timeout_ms: int,
    field_size: Optional[int] = None,
    retries: int = 3,
    retry_backoff_ms: int = 3000,
) -> List[Dict]:
    url = build_url("b1", race_id)
    last_error = None

    for attempt in range(1, retries + 1):
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

        try:
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

        except Exception as e:
            last_error = e
            if attempt >= retries:
                raise
            time.sleep((retry_backoff_ms * attempt) / 1000.0)

        finally:
            driver.quit()

    raise last_error


# ----------------------------
# Playwright for non-b1
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


async def scrape_one_page_playwright(context, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    url = build_url(odds_type, race_id)
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        if odds_type in {"b7", "b8"}:
            rows = await parse_trifecta_like_page(page, race_id, odds_type, timeout_ms)
        else:
            rows = await parse_combo_page_simple(page, race_id, odds_type, timeout_ms)

        return rows
    finally:
        await page.close()


async def scrape_one_page(
    context,
    race_id: str,
    odds_type: str,
    timeout_ms: int,
    headless: bool,
    field_size: Optional[int],
    args: argparse.Namespace,
) -> List[Dict]:
    if odds_type == "b1":
        return await asyncio.to_thread(
            selenium_scrape_b1_page,
            race_id,
            headless,
            timeout_ms,
            field_size,
            args.retries,
            args.retry_backoff_ms,
        )
    return await scrape_one_page_playwright(context, race_id, odds_type, timeout_ms)


async def scrape_one_page_with_retry(
    context,
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
                context=context,
                race_id=race_id,
                odds_type=odds_type,
                timeout_ms=timeout_ms,
                headless=args.headless,
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


def load_retry_targets(errors_path: str) -> pd.DataFrame:
    df = pd.read_csv(errors_path, low_memory=False)

    required = {"race_id", "odds_type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"errors csv missing required columns: {missing}")

    df["race_id"] = df["race_id"].astype(str)
    df["odds_type"] = df["odds_type"].astype(str).str.strip()

    valid_types = {"b1", "b3", "b4", "b5", "b6", "b7", "b8"}
    df = df[df["odds_type"].isin(valid_types)].copy()

    df = df.drop_duplicates(subset=["race_id", "odds_type"], keep="last").reset_index(drop=True)
    return df


def ensure_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in DESIRED_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[DESIRED_COLS].copy()
    return df


def merge_and_save_output(output_path: Path, existing_df: pd.DataFrame, new_rows: List[Dict]) -> pd.DataFrame:
    frames = []

    if existing_df is not None and not existing_df.empty:
        frames.append(existing_df)

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        if not df_new.empty:
            frames.append(df_new)

    if frames:
        df_out = pd.concat(frames, ignore_index=True)
    else:
        df_out = pd.DataFrame(columns=DESIRED_COLS)

    if not df_out.empty:
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

    if not df_out.empty:
        for c in ["race_id", "odds_type", "bet_type", "combination"]:
            df_out[c] = df_out[c].astype(str)

        df_out = df_out.sort_values(
            ["race_id", "odds_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            na_position="last",
        ).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
    return df_out


async def async_main(args: argparse.Namespace) -> None:
    targets_df = load_retry_targets(args.errors)
    race_id_set, field_size_map = load_race_input(args.race_result)

    targets_df = targets_df[targets_df["race_id"].isin(race_id_set)].copy()
    if targets_df.empty:
        print("retry target is empty after filtering by race-result")
        return

    output_path = Path(args.output)
    remaining_errors_path = (
        Path(args.remaining_errors_output)
        if args.remaining_errors_output
        else output_path.with_name(output_path.stem + "_remaining_errors.csv")
    )
    retried_log_path = (
        Path(args.retried_log_output)
        if args.retried_log_output
        else output_path.with_name(output_path.stem + "_retried_pairs.csv")
    )

    if output_path.exists():
        existing_df = pd.read_csv(output_path, low_memory=False)
        existing_df["race_id"] = existing_df["race_id"].astype(str)
        existing_df["odds_type"] = existing_df["odds_type"].astype(str)
    else:
        existing_df = pd.DataFrame(columns=DESIRED_COLS)

    retry_pairs = set(zip(targets_df["race_id"], targets_df["odds_type"]))

    if not existing_df.empty:
        keep_mask = ~existing_df[["race_id", "odds_type"]].apply(tuple, axis=1).isin(retry_pairs)
        base_df = existing_df[keep_mask].copy()
    else:
        base_df = existing_df.copy()

    success_rows: List[Dict] = []
    remaining_error_rows: List[Dict] = []
    retried_log_rows: List[Dict] = []

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

        target_records = targets_df.to_dict("records")

        for i, row in enumerate(target_records, start=1):
            race_id = str(row["race_id"])
            odds_type = str(row["odds_type"])
            field_size = field_size_map.get(race_id)

            print(f"[{i}/{len(target_records)}] retry race_id={race_id}, odds_type={odds_type}")

            try:
                rows = await scrape_one_page_with_retry(
                    context=context,
                    race_id=race_id,
                    odds_type=odds_type,
                    timeout_ms=args.timeout_ms,
                    field_size=field_size,
                    args=args,
                )
                success_rows.extend(rows)

                retried_log_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "status": "success",
                    "rows": len(rows),
                    "url": build_url(odds_type, race_id),
                })

                if odds_type == "b1":
                    df_b1 = pd.DataFrame(rows)
                    tan_cnt = int((df_b1["bet_type"] == "単勝").sum()) if not df_b1.empty else 0
                    fuku_cnt = int((df_b1["bet_type"] == "複勝").sum()) if not df_b1.empty else 0
                    print(f"  {odds_type}: tan={tan_cnt}, fuku={fuku_cnt}")
                else:
                    non_null_count = int(pd.DataFrame(rows)["odds"].notna().sum()) if rows else 0
                    expected = expected_count_for_odds_type(odds_type, field_size)
                    if expected is None:
                        print(f"  {odds_type}: rows={len(rows)}, non_null={non_null_count}")
                    else:
                        print(f"  {odds_type}: rows={len(rows)}, non_null={non_null_count}, expected={expected}")

            except PlaywrightTimeoutError as e:
                remaining_error_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "url": build_url(odds_type, race_id),
                    "error": f"timeout: {e}",
                })
                retried_log_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "status": "timeout",
                    "rows": 0,
                    "url": build_url(odds_type, race_id),
                })
                print(f"  {odds_type}: TIMEOUT")

            except Exception as e:
                remaining_error_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "url": build_url(odds_type, race_id),
                    "error": str(e),
                })
                retried_log_rows.append({
                    "race_id": race_id,
                    "odds_type": odds_type,
                    "status": "error",
                    "rows": 0,
                    "url": build_url(odds_type, race_id),
                })
                print(f"  {odds_type}: ERROR - {e}")

            await wait_between_requests(args)

        await context.close()
        await browser.close()

    final_df = merge_and_save_output(output_path, base_df, success_rows)

    df_remaining = pd.DataFrame(remaining_error_rows, columns=["race_id", "odds_type", "url", "error"])
    df_remaining.to_csv(remaining_errors_path, index=False, encoding="utf-8-sig")

    df_log = pd.DataFrame(retried_log_rows, columns=["race_id", "odds_type", "status", "rows", "url"])
    df_log.to_csv(retried_log_path, index=False, encoding="utf-8-sig")

    print(f"saved updated odds: {output_path}")
    print(f"remaining errors: {remaining_errors_path}")
    print(f"retried log: {retried_log_path}")
    print(f"final rows: {len(final_df)}")


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()