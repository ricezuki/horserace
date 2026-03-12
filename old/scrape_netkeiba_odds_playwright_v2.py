import argparse
import asyncio
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://race.netkeiba.com/odds/index.html"
DEFAULT_TIMEOUT_MS = 15000

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
        description="Scrape netkeiba odds pages using Playwright-rendered DOM (with tab switching)."
    )
    parser.add_argument("--race-ids-file", help="Text/CSV file containing race_id values.")
    parser.add_argument("--race-ids", nargs="*", help="Race IDs directly.")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument(
        "--types",
        nargs="*",
        default=["b1", "b3", "b4", "b5", "b6", "b7", "b8"],
        help="Odds types to fetch. Default: b1 b3 b4 b5 b6 b7 b8",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS, help="Wait timeout in milliseconds")
    parser.add_argument("--slow-mo", type=int, default=0, help="Playwright slow_mo milliseconds")
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


async def wait_for_b1(page, timeout_ms: int) -> None:
    await page.wait_for_selector("table.RaceOdds_HorseList_Table", timeout=timeout_ms)


async def wait_for_combo_page(page, odds_type: str, timeout_ms: int) -> None:
    odds_type_num = int(odds_type[1:])
    await page.wait_for_selector(f"[id^='odds-{odds_type_num}-']", timeout=timeout_ms)


async def click_if_possible(locator) -> bool:
    try:
        count = await locator.count()
        if count < 1:
            return False
        await locator.first.click()
        return True
    except Exception:
        return False


async def extract_b1_rows_current_view(page) -> List[Dict]:
    js = """
    () => {
      function txt(el) {
        return (el?.innerText || el?.textContent || '').replace(/\\s+/g, ' ').trim();
      }

      function parseHorseNumberFromCells(cells) {
        for (const c of cells) {
          const t = txt(c);
          if (/^\\d{1,2}$/.test(t)) {
            const n = parseInt(t, 10);
            if (n >= 1 && n <= 18) return n;
          }
        }
        return null;
      }

      function parseHorseName(tr) {
        const a = tr.querySelector('a');
        return txt(a);
      }

      function parseOddsAndPop(tr, isFuku) {
        const rowText = txt(tr).replace(/,/g, '');
        const cellsText = Array.from(tr.querySelectorAll('th,td,span')).map(x => txt(x));

        let odds = null;
        let oddsMax = null;

        if (isFuku) {
          const m = rowText.match(/(\\d+(?:\\.\\d+)?)\\s*[-〜~]\\s*(\\d+(?:\\.\\d+)?)/);
          if (m) {
            odds = parseFloat(m[1]);
            oddsMax = parseFloat(m[2]);
          }
        }

        if (odds === null) {
          for (let i = cellsText.length - 1; i >= 0; i--) {
            const s = (cellsText[i] || '').replace(/,/g, '');
            if (/^\\d+(?:\\.\\d+)?$/.test(s)) {
              odds = parseFloat(s);
              break;
            }
          }
        }

        let popularity = null;
        const mp = rowText.match(/(\\d+)人気/);
        if (mp) popularity = parseInt(mp[1], 10);

        return { odds, oddsMax, popularity };
      }

      const rows = [];

      const tanBlock = document.querySelector('#odds_tan_block');
      if (tanBlock) {
        tanBlock.querySelectorAll('table tbody tr').forEach(tr => {
          const cells = Array.from(tr.querySelectorAll('th,td'));
          const horseNo = parseHorseNumberFromCells(cells);
          if (!horseNo) return;
          const x = parseOddsAndPop(tr, false);
          rows.push({
            bet_type: '単勝',
            combination: String(horseNo),
            horse_number_1: horseNo,
            horse_number_2: null,
            horse_number_3: null,
            horse_name_1: parseHorseName(tr),
            horse_name_2: '',
            horse_name_3: '',
            odds: x.odds,
            odds_max: null,
            popularity: x.popularity
          });
        });
      }

      const fukuBlock = document.querySelector('#odds_fuku_block');
      if (fukuBlock) {
        fukuBlock.querySelectorAll('table tbody tr').forEach(tr => {
          const cells = Array.from(tr.querySelectorAll('th,td'));
          const horseNo = parseHorseNumberFromCells(cells);
          if (!horseNo) return;
          const x = parseOddsAndPop(tr, true);
          rows.push({
            bet_type: '複勝',
            combination: String(horseNo),
            horse_number_1: horseNo,
            horse_number_2: null,
            horse_number_3: null,
            horse_name_1: parseHorseName(tr),
            horse_name_2: '',
            horse_name_3: '',
            odds: x.odds,
            odds_max: x.oddsMax,
            popularity: x.popularity
          });
        });
      }

      return rows;
    }
    """
    return await page.evaluate(js)


async def parse_b1_page(page, race_id: str, timeout_ms: int) -> List[Dict]:
    meta = extract_race_meta_from_title(await page.title(), race_id)
    collected: List[Dict] = []

    # 初期表示分
    await wait_for_b1(page, timeout_ms)
    current_rows = await extract_b1_rows_current_view(page)
    for row in current_rows:
        collected.append({**meta, "odds_type": "b1", **row})

    # 枠タブらしきものを総当たり
    tab_selectors = [
        ".RaceOdds_FrameList li",
        ".OddsFrameList li",
        ".FrameList li",
        "[class*='Frame'] li",
        "ul li",
    ]

    clicked_any = False
    for selector in tab_selectors:
        tabs = page.locator(selector)
        count = await tabs.count()
        if count <= 1:
            continue

        for i in range(count):
            try:
                tab = tabs.nth(i)
                tab_text = clean_text(await tab.inner_text())
                # 枠番号らしいものだけ試す
                if not re.search(r"^[1-8]$", tab_text):
                    continue

                await tab.click()
                clicked_any = True
                await page.wait_for_timeout(600)
                rows = await extract_b1_rows_current_view(page)
                for row in rows:
                    collected.append({**meta, "odds_type": "b1", **row})
            except Exception:
                continue

        if clicked_any:
            break

    if collected:
        df = pd.DataFrame(collected).drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1"],
            keep="first",
        )
        return df.to_dict("records")

    return collected


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
    await wait_for_combo_page(page, odds_type, timeout_ms)
    rows = await extract_combo_rows_current_view(page, race_id, odds_type)
    if rows:
        df = pd.DataFrame(rows).drop_duplicates(
            subset=["race_id", "bet_type", "horse_number_1", "horse_number_2", "horse_number_3"],
            keep="first",
        )
        return df.to_dict("records")
    return rows


async def parse_trifecta_like_page(page, race_id: str, odds_type: str, timeout_ms: int) -> List[Dict]:
    await wait_for_combo_page(page, odds_type, timeout_ms)
    collected: List[Dict] = []

    # 初期表示（1番頭など）
    collected.extend(await extract_combo_rows_current_view(page, race_id, odds_type))

    # 軸馬/頭選択タブを総当たり
    candidate_selectors = [
        ".HorseNumList li",
        ".RaceOdds_HorseNumList li",
        ".Horse_List li",
        "[class*='HorseNum'] li",
        "[class*='Horse'] li",
        "ul li",
    ]

    clicked_any = False
    for selector in candidate_selectors:
        items = page.locator(selector)
        count = await items.count()
        if count <= 1:
            continue

        local_hit = 0
        for i in range(count):
            try:
                item = items.nth(i)
                tx = clean_text(await item.inner_text())
                # 軸馬候補らしい馬番のみ
                if not re.search(r"^\d{1,2}$", tx):
                    continue
                n = int(tx)
                if not (1 <= n <= 18):
                    continue

                await item.click()
                await page.wait_for_timeout(700)
                rows = await extract_combo_rows_current_view(page, race_id, odds_type)
                if rows:
                    collected.extend(rows)
                    local_hit += 1
                    clicked_any = True
            except Exception:
                continue

        if local_hit > 0:
            break

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


async def async_main(args: argparse.Namespace) -> None:
    race_ids = load_race_ids(args)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows: List[Dict] = []
    error_rows: List[Dict] = []

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
                try:
                    rows = await scrape_one_page(context, race_id, odds_type, args.timeout_ms)
                    all_rows.extend(rows)
                    print(f"  {odds_type} ({ODDS_TYPE_LABELS.get(odds_type, odds_type)}): {len(rows)} rows")
                except PlaywrightTimeoutError as e:
                    error_rows.append({
                        "race_id": race_id,
                        "odds_type": odds_type,
                        "url": build_url(odds_type, race_id),
                        "error": f"timeout: {e}",
                    })
                    print(f"  {odds_type}: TIMEOUT")
                except Exception as e:
                    error_rows.append({
                        "race_id": race_id,
                        "odds_type": odds_type,
                        "url": build_url(odds_type, race_id),
                        "error": str(e),
                    })
                    print(f"  {odds_type}: ERROR - {e}")

        await context.close()
        await browser.close()

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


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()