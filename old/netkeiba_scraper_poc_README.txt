netkeiba_scraper_poc.py

概要:
- 1か月単位で race.netkeiba.com のレース結果を収集する PoC 用スクレイパーです。
- 日付ページから race_id を発見し、各 race_id の結果ページから
  race_result / race_meta / payback / scrape_log を CSV 出力します。
- アクセス過多対策としてランダム sleep とリトライ時バックオフを入れています。
- 取得済み race_id は race_result_YYYY-MM.csv を見てスキップします。

主な出力:
- race_result_YYYY-MM.csv  : 1頭1行の主テーブル
- race_meta_YYYY-MM.csv    : レース単位メタ情報
- payback_YYYY-MM.csv      : 払戻テーブル
- scrape_log_YYYY-MM.csv   : 取得ログ
- raw_html/*.html          : --save-raw-html 指定時のみ

実行例:
python netkeiba_scraper_poc.py --month 2026-03 --output-dir ./data/netkeiba_poc --save-raw-html

待機時間を長めにしたい場合:
python netkeiba_scraper_poc.py --month 2026-03 --sleep-min 5 --sleep-max 9

注意:
- サイト構造が変わると race_id 発見ページの候補URLを調整する必要があります。
- 利用規約・robots.txt・アクセスルールは実運用前に必ず確認してください。
- PoC 段階では月次取得ですが、今後は週次増分更新モードを別コマンドで足す想定です。
