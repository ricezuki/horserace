# HorseRace スクリプト一覧と実行コマンド

このファイルは、HorseRaceプロジェクトで使う主要スクリプトの**ファイル名**と**実行コマンド**を一覧化したものです。  
対象は、データ取得、整形、特徴量作成、学習、評価です。

---

## 1. レース結果CSVから特徴量テーブルを作る

### ファイル名
`build_feature_table.py`

### 用途
複数の `race_result_*.csv` を結合し、学習用の `feature_table_*.csv` を作る。

### 例: 2024 + 2025 + 2026-01 を結合
```powershell
python build_feature_table.py --inputs ./data/netkeiba_2024/race_result_2024.csv ./data/netkeiba_2025/race_result_2025.csv ./data/netkeiba_2026_01/race_result_2026-01.csv --output ./features/feature_table_2024_2025_2026-01.csv
```

### 例: 2025 + 2026-01 を結合
```powershell
python build_feature_table.py --inputs ./data/netkeiba_2025/race_result_2025.csv ./data/netkeiba_2026_01/race_result_2026-01.csv --output ./features/feature_table_2025_2026-01.csv
```

### 例: 新馬戦も目的変数に含める
```powershell
python build_feature_table.py --inputs ./data/netkeiba_2025/race_result_2025.csv --output ./features/feature_table_2025_include_shinba.csv --include-shinba-target
```

---

## 2. 3着以内モデルを学習する

### ファイル名
`train_top3_model_v2.py`

### 用途
`feature_table_*.csv` を入力にして、各馬の**3着以内確率**を予測するモデルを学習する。

### 例: 2024-2026-01 で学習
```powershell
python train_top3_model_v2.py --feature-file ./features/feature_table_2024_2025_2026-01.csv --output-dir ./models/top3_baseline_v2_2024_2026-01
```

### 出力先の主なファイル
- `top3_model.joblib`
- `metrics.json`
- `valid_predictions.csv`
- `race_level_metrics.csv`
- `feature_columns.json`
- `training_meta.json`

---

## 3. 1着モデルを学習する

### ファイル名
`train_win_model.py`

### 用途
`feature_table_*.csv` を入力にして、各馬の**1着確率**を予測するモデルを学習する。

### 例: 2024-2026-01 で学習
```powershell
python train_win_model.py --feature-file ./features/feature_table_2024_2025_2026-01.csv --output-dir ./models/win_baseline_2024_2026-01
```

### 出力先の主なファイル
- `win_model.joblib`
- `metrics.json`
- `valid_predictions.csv`
- `race_level_metrics.csv`
- `feature_columns.json`
- `training_meta.json`

---

## 4. 払戻CSVを馬ごと1行の単勝・複勝テーブルに展開する（最終採用版）

### ファイル名
`expand_win_place_payback_from_rows_v2.py`

### 用途
`payback.csv` と `race_result.csv` をもとに、馬ごと1行の

- `win_hit`
- `win_payout_yen`
- `place_hit`
- `place_payout_yen`

を持つCSVを作る。  
現在の運用では、**これが払戻整形の主力スクリプト**。

### 例: 2025年分を展開
```powershell
python expand_win_place_payback_from_rows_v2.py --payback ./data/netkeiba_2025/payback_2025.csv --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/payback_win_place_2025_fixed_v2.csv
```

### 例: 2024年分を展開
```powershell
python expand_win_place_payback_from_rows_v2.py --payback ./data/netkeiba_2024/payback_2024.csv --race-result ./data/netkeiba_2024/race_result_2024.csv --output ./data/netkeiba_2024/payback_win_place_2024.csv
```

---

## 5. 払戻CSVを馬ごと1行に展開する（別版）

### ファイル名
`expand_win_place_payback_precise_v2.py`

### 用途
払戻展開の別実装。検証用・比較用として残っている。  
通常運用では `expand_win_place_payback_from_rows_v2.py` を優先する。

### 例
```powershell
python expand_win_place_payback_precise_v2.py --payback ./data/netkeiba_2025/payback_2025.csv --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/payback_win_place_precise_2025_v2.csv
```

---

## 6. 払戻CSVを馬ごと1行に展開する（旧版）

### ファイル名
`expand_win_place_payback.py`

### 用途
初期版。現在は基本的に**旧版**扱い。  
比較や経緯確認が必要な場合のみ使う。

### 例
```powershell
python expand_win_place_payback.py --input ./data/netkeiba_2025/payback_2025.csv --output ./data/netkeiba_2025/payback_win_place_old.csv
```

---

## 7. EV・ROI を評価する

### ファイル名
`evaluate_ev_roi.py`

### 用途
- `1着モデルの valid_predictions.csv`
- `3着以内モデルの valid_predictions.csv`
- `feature_table.csv`
- 払戻馬単位CSV

を結合して、単勝・複勝の**期待値と回収率**を検証する。

### 例: 2026-01 の払戻で検証
```powershell
python evaluate_ev_roi.py --pred-win-files ./models/win_baseline_2024_2026-01/valid_predictions.csv --pred-top3-files ./models/top3_baseline_v2_2024_2026-01/valid_predictions.csv --feature-files ./features/feature_table_2024_2025_2026-01.csv --payout-files ./data/netkeiba_2026_01/payback_win_place_2026-01.csv --output-dir ./analysis/ev_roi_check_2026_01
```

### 例: 複数年の払戻ファイルをまとめて指定
```powershell
python evaluate_ev_roi.py --pred-win-files ./models/win_baseline_2024_2026-01/valid_predictions.csv --pred-top3-files ./models/top3_baseline_v2_2024_2026-01/valid_predictions.csv --feature-files ./features/feature_table_2024_2025_2026-01.csv --payout-files ./data/netkeiba_2024/payback_win_place_2024.csv ./data/netkeiba_2025/payback_win_place_2025_fixed_v2.csv --output-dir ./analysis/ev_roi_check
```

### 出力先の主なファイル
- `ev_roi_detail.csv`
- `ev_roi_summary_win.csv`
- `ev_roi_summary_place.csv`
- `ev_roi_overview.json`

---

## 8. オッズを単レースでテスト取得する

### ファイル名
単レース検証で使っていた確定版スクリプト:
`scrape_netkeiba_odds_playwright_v7.py`

### 用途
Playwright で netkeiba のオッズページを開き、以下を取得する。

- 単勝
- 複勝
- 枠連
- 馬連
- ワイド
- 馬単
- 3連複
- 3連単

### 例: 小倉牝馬S のレースIDで単レース取得
```powershell
python scrape_netkeiba_odds_playwright_v7.py --race-ids 202510010111 --output ./data/odds_202510010111_playwright_v7.csv --headless
```

---

## 9. オッズを期間指定でまとめて取得する

### ファイル名
`scrape_netkeiba_odds_by_period.py`

### 用途
`race_result.csv` から対象期間の `race_id` を抽出し、期間単位でオッズCSVを作る。  
現在の方針では、**期間指定オッズ取得の主力スクリプト**。

### 例: 2026年1月分を取得
```powershell
python scrape_netkeiba_odds_by_period.py --period 2026-01 --race-result ./data/netkeiba_2026_01/race_result_2026-01.csv --output ./data/netkeiba_2026_01/odds_2026-01.csv --headless
```

### 例: 2025年分を取得
```powershell
python scrape_netkeiba_odds_by_period.py --period 2025 --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/odds_2025.csv --headless
```

### 例: 中断再開あり
```powershell
python scrape_netkeiba_odds_by_period.py --period 2025 --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/odds_2025.csv --headless --resume
```

---

## 10. 現時点の標準ワークフロー

### ステップ1: レース結果CSVを用意する
- `race_result_2024.csv`
- `race_result_2025.csv`
- `race_result_2026-01.csv`
- など

### ステップ2: 特徴量テーブルを作る
```powershell
python build_feature_table.py --inputs ./data/netkeiba_2024/race_result_2024.csv ./data/netkeiba_2025/race_result_2025.csv ./data/netkeiba_2026_01/race_result_2026-01.csv --output ./features/feature_table_2024_2025_2026-01.csv
```

### ステップ3: 3着以内モデルを学習
```powershell
python train_top3_model_v2.py --feature-file ./features/feature_table_2024_2025_2026-01.csv --output-dir ./models/top3_baseline_v2_2024_2026-01
```

### ステップ4: 1着モデルを学習
```powershell
python train_win_model.py --feature-file ./features/feature_table_2024_2025_2026-01.csv --output-dir ./models/win_baseline_2024_2026-01
```

### ステップ5: 払戻CSVを馬単位へ展開
```powershell
python expand_win_place_payback_from_rows_v2.py --payback ./data/netkeiba_2025/payback_2025.csv --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/payback_win_place_2025_fixed_v2.csv
```

### ステップ6: オッズを取得
```powershell
python scrape_netkeiba_odds_by_period.py --period 2025 --race-result ./data/netkeiba_2025/race_result_2025.csv --output ./data/netkeiba_2025/odds_2025.csv --headless --resume
```

### ステップ7: EV・ROI を検証
```powershell
python evaluate_ev_roi.py --pred-win-files ./models/win_baseline_2024_2026-01/valid_predictions.csv --pred-top3-files ./models/top3_baseline_v2_2024_2026-01/valid_predictions.csv --feature-files ./features/feature_table_2024_2025_2026-01.csv --payout-files ./data/netkeiba_2025/payback_win_place_2025_fixed_v2.csv --output-dir ./analysis/ev_roi_check_2025
```

---

## 11. 今後の改善の中心
今後は以下を改善対象とする。

1. 期間指定オッズ取得の安定運用
2. 予測確率と予想時点オッズの結合
3. 時系列バックテスト
4. 回収率最大化の条件探索
5. 払戻込みでの予想精度改善

---

## 12. 補足
- `train_top3_model.py` は旧版で、今は `train_top3_model_v2.py` を優先する
- 払戻展開は `expand_win_place_payback_from_rows_v2.py` を優先する
- オッズ取得は `scrape_netkeiba_odds_by_period.py` を主力にする
- 年単位取得でも内部処理は月単位分割が望ましい、というプロジェクト意図がある
