# horserace

競馬予想AI開発用リポジトリです。  
競馬データの取得、特徴量作成、モデル学習、回収率評価を行うためのスクリプトを管理します。

## このリポジトリで扱うもの

- netkeiba などからの競馬データ取得
- オッズ情報の取得
- HTML / 生データの整形
- 特徴量テーブルの作成
- 勝利予測・複勝圏予測モデルの学習
- EV / ROI 評価

## 現在の主な構成

```text
horserace/
├─ analysis/      # 分析結果・検証用ファイル
├─ features/      # 特徴量関連データ
├─ html/          # 保存したHTMLなどの中間データ
├─ models/        # 学習済みモデル置き場
├─ old/           # 旧版スクリプト
├─ .gitignore
├─ README.md
├─ build_feature_table.py
├─ evaluate_ev_roi.py
├─ expand_win_place_payback.py
├─ expand_win_place_payback_from_rows_v2.py
├─ expand_win_place_payback_precise_v2.py
├─ netkeiba_html_to_csv_v6_3.py
├─ netkeiba_scrape_and_parse_v4.py
├─ scrape_netkeiba_odds_playwright_v7.py
├─ train_top3_model_v2.py
└─ train_win_model.py