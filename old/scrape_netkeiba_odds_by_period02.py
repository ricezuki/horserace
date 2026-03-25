import argparse
import asyncio
import random
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape netkeiba odds by period (year or year-month) using race_result CSV."
    )
    parser.add_argument(
        "--period",
        required=True,
        help="Target period. Examples: 2025 or 2026-01",
    )
    parser.add_argument(
        "--race-result",
        required=True,
        help="Path to race_result CSV for the target period",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output odds CSV path",
    )
    parser.add_argument(
        "--types",
        nargs="*",
        default=["b1", "b3", "b4", "b5", "b6", "b7", "b8"],
        help="Odds types to fetch. Default: b1 b3 b4 b5 b6 b7 b8",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    parser.add_argument("--slow-mo", type=int, default=0)
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Retry count for each race_id + odds_type (default enabled)",
    )
    parser.add_argument(
        "--sleep-min-ms",
        type=int,
        default=DEFAULT_SLEEP_MIN_MS,
        help="Minimum sleep between requests in milliseconds (default enabled)",
    )
    parser.add_argument(
        "--sleep-max-ms",
        type=int,
        default=DEFAULT_SLEEP_MAX_MS,
        help="Maximum sleep between requests in milliseconds (default enabled)",
    )
    parser.add_argument(
        "--retry-backoff-ms",
        type=int,
        default=DEFAULT_RETRY_BACKOFF_MS,
        help="Base backoff before retrying in milliseconds (default enabled)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="If output exists, skip already-scraped race_id + odds_type pairs",
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


def load_race_ids_from_race_result(path: str, period: str) -> List[str]:
    df = pd.read_csv(path, low_memory=False)

    if "race_id" not in df.columns:
        raise ValueError("race_result CSV must contain race_id column")

    if "race_date" not in df.columns:
        raise ValueError("race_result CSV must contain race_date column")

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
    return race_ids


def load_done_pairs(output_path: Path) -> set[tuple[str, str]]:
    if not output_path.exists():
        return set()

    try:
        df = pd.read_csv(output_path, usecols=["race_id", "odds_type"], low_memory=False)
        return set(zip(df["race_id"].astype(str), df["odds_type"].astype(str)))
    except Exception:
        return set()


async def wait_for_page_ready(page, odds_type: str, timeout_ms: int) -> None:
    if odds_type == "b1":
        await page.wait_for_selector("#odds_tan_block table tbody tr", timeout=timeout_ms)
        await page.wait_for_selector("#odds_fuku_block table tbody tr", timeout=timeout_ms)
    else:
        odds_type_num = int(odds_type[1:])
        await page.wait_for_selector(f"[id^='odds-{odds_type_num}-']", timeout=timeout_ms)


async def parse_b1_page(page, race_id: str, timeout_ms: int) -> List[Dict]:
    await wait_for_page_ready(page, "b1", timeout_ms)
    meta = extract_race_meta_from_title(await page.title(), race_id)

    js = """
    () => {
      function txt(el) {
        return (el?.innerText || el?.textContent || '').replace(/\\s+/g, ' ').trim();
      }

      function parseTableRows(rootSelector, betType) {
        const out = [];
        const rows = document.querySelectorAll(rootSelector + ' table tbody tr');

        rows.forEach(tr => {
          const cells = Array.from(tr.querySelectorAll('th, td'));
          if (cells.length < 2) return;

          const horseNoText = txt(cells[1]);
          if (!/^\\d{1,2}$/.test(horseNoText)) return;
          const horseNo = parseInt(horseNoText, 10);

          const horseName = txt(tr.querySelector('a'));
          const rowText = txt(tr).replace(/,/g, '');

          let odds = null;
          let oddsMax = null;
          let popularity = null;

          const mp = rowText.match(/(\\d+)人気/);
          if (mp) popularity = parseInt(mp[1], 10);

          if (betType === '複勝') {
            const mRange = rowText.match(/(\\d+(?:\\.\\d+)?)\\s*[-〜~]\\s*(\\d+(?:\\.\\d+)?)(?!.*\\d)/);
            if (mRange) {
              odds = parseFloat(mRange[1]);
              oddsMax = parseFloat(mRange[2]);
            } else {
              const nums = Array.from(rowText.matchAll(/\\d+(?:\\.\\d+)?/g)).map(m => m[0]);
              if (nums.length > 0) odds = parseFloat(nums[nums.length - 1]);
            }
          } else {
            const nums = Array.from(rowText.matchAll(/\\d+(?:\\.\\d+)?/g)).map(m => m[0]);
            if (nums.length > 0) odds = parseFloat(nums[nums.length - 1]);
          }

          out.push({
            bet_type: betType,
            combination: String(horseNo),
            horse_number_1: horseNo,
            horse_number_2: null,
            horse_number_3: null,
            horse_name_1: horseName,
            horse_name_2: '',
            horse_name_3: '',
            odds: odds,
            odds_max: oddsMax,
            popularity: popularity
          });
        });

        return out;
      }

      return [
        ...parseTableRows('#odds_tan_block', '単勝'),
        ...parseTableRows('#odds_fuku_block', '複勝')
      ];
    }
    """

    parsed = await page.evaluate(js)
    rows = [{**meta, "odds_type": "b1", **row} for row in parsed]

    if rows:
        df = pd.DataFrame(rows).drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1"],
            keep="first",
        )
        return df.to_dict("records")

    return rows


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
    await wait_for_page_ready(page, odds_type, timeout_ms)
    rows = await extract_combo_rows_current_view(page, race_id, odds_type)

    if rows:
        df = pd.DataFrame(rows).drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        )
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


async def parse_trifecta_like_page(page, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    await wait_for_page_ready(page, odds_type, timeout_ms)

    collected: List[Dict] = []
    collected.extend(await extract_combo_rows_current_view(page, race_id, odds_type))

    options = await get_select_options(page)

    for opt in options:
        try:
            changed = await select_axis_option(page, opt["value"], opt["text"])
            if not changed:
                continue

            await page.wait_for_timeout(1200)
            await wait_for_page_ready(page, odds_type, timeout_ms)
            collected.extend(await extract_combo_rows_current_view(page, race_id, odds_type))
        except Exception:
            continue

    if collected:
        df = pd.DataFrame(collected).drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        )
        return df.to_dict("records")

    return collected


async def scrape_one_page(context, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    url = build_url(odds_type, race_id)
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        if odds_type == "b1":
            rows = await parse_b1_page(page, race_id, timeout_ms)
        elif odds_type in {"b7", "b8"}:
            rows = await parse_trifecta_like_page(page, race_id, odds_type, timeout_ms)
        else:
            rows = await parse_combo_page_simple(page, race_id, odds_type, timeout_ms)

        return rows
    finally:
        await page.close()


async def scrape_one_page_with_retry(context, race_id: str, odds_type: str, args: argparse.Namespace) -> List[Dict]:
    retries = max(1, int(args.retries))
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            return await scrape_one_page(context, race_id, odds_type, args.timeout_ms)
        except Exception as e:
            last_error = e
            if attempt >= retries:
                raise

            backoff_ms = max(0, int(args.retry_backoff_ms)) * attempt
            if backoff_ms > 0:
                await asyncio.sleep(backoff_ms / 1000.0)

    raise last_error


async def async_main(args: argparse.Namespace) -> None:
    race_ids = load_race_ids_from_race_result(args.race_result, args.period)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    error_path = output_path.with_name(output_path.stem + "_errors.csv")
    done_pairs = load_done_pairs(output_path) if args.resume else set()

    all_rows_buffer: List[Dict] = []
    error_rows_buffer: List[Dict] = []

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

        for i, race_id in enumerate(race_ids, start=1):
            print(f"[{i}/{len(race_ids)}] race_id={race_id}")
            for odds_type in args.types:
                if (race_id, odds_type) in done_pairs:
                    print(f"  {odds_type}: SKIP (already scraped)")
                    continue

                try:
                    rows = await scrape_one_page_with_retry(context, race_id, odds_type, args)
                    all_rows_buffer.extend(rows)
                    print(f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): {len(rows)} rows")
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

                sleep_min = max(0, int(args.sleep_min_ms))
                sleep_max = max(sleep_min, int(args.sleep_max_ms))
                if sleep_max > 0:
                    await asyncio.sleep(random.uniform(sleep_min, sleep_max) / 1000.0)

            # race_idごとに途中保存
            if all_rows_buffer:
                df_new = pd.DataFrame(all_rows_buffer)
                if output_path.exists():
                    df_old = pd.read_csv(output_path, low_memory=False)
                    df_out = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df_out = df_new.copy()

                df_out = df_out.drop_duplicates(
                    subset=["race_id", "odds_type", "bet_type", "combination"],
                    keep="last",
                )

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
                    if c not in df_out.columns:
                        df_out[c] = None

                df_out = df_out[desired_cols].copy()
                df_out = df_out.sort_values(
                    ["race_id", "odds_type", "horse_number_1", "horse_number_2", "horse_number_3"],
                    na_position="last",
                ).reset_index(drop=True)
                df_out.to_csv(output_path, index=False, encoding="utf-8-sig")
                all_rows_buffer = []

            if error_rows_buffer:
                df_err_new = pd.DataFrame(error_rows_buffer)
                if error_path.exists():
                    df_err_old = pd.read_csv(error_path, low_memory=False)
                    df_err = pd.concat([df_err_old, df_err_new], ignore_index=True)
                else:
                    df_err = df_err_new.copy()

                df_err = df_err.drop_duplicates(
                    subset=["race_id", "odds_type", "url", "error"],
                    keep="last",
                )
                df_err.to_csv(error_path, index=False, encoding="utf-8-sig")
                error_rows_buffer = []

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