# horserace

競馬データ取得・加工用のスクリプトを管理するリポジトリです。  
主にスクレイピングやオッズ取得、レース単位のCSV出力などを目的としています。

## 目的

このリポジトリは、競馬関連データを取得・蓄積・検証するためのコードを管理するためのものです。

現時点では主に以下を想定しています。

- レースID単位でのデータ取得
- オッズ情報の取得
- 取得結果のCSV保存
- 中断に強いデータ収集フローの整備
- GitHub 上でのコード共有と履歴管理

## 前提環境

- Windows PowerShell での実行を想定
- Python 3.10 以上推奨
- Git によるバージョン管理
- 必要に応じて Playwright を利用

## ディレクトリ構成（例）

```text
horserace/
├─ data/               # 出力データ（通常は Git 管理しない）
├─ scripts/            # 補助スクリプト類（必要に応じて）
├─ *.py                # 取得・加工スクリプト
├─ .gitignore
└─ README.md
```

※ 実際の構成に応じて今後更新してください。

## セットアップ

### 1. リポジトリをクローン

```powershell
git clone git@github.com:ricezuki/horserace.git
cd horserace
```

### 2. 仮想環境を作成

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. 必要パッケージをインストール

`requirements.txt` がある場合:

```powershell
pip install -r requirements.txt
```

まだない場合は、利用しているライブラリを個別に入れてください。例:

```powershell
pip install pandas requests beautifulsoup4 lxml playwright
python -m playwright install
```

## 実行例

例として、レースIDを指定してスクリプトを実行する場合:

```powershell
python scrape_netkeiba_odds_playwright_v7.py --race-ids 202510010111 --output ./data/odds_202510010111.csv --headless
```

## 運用方針

### Git 管理対象
GitHub で管理するのは主に以下です。

- Python スクリプト
- 設定ファイル
- ドキュメント
- 小さなサンプルデータ
- 再現に必要な最小限の補助ファイル

### Git 管理しないもの
以下は通常コミットしません。

- 取得済みCSVなどの大量データ
- ログ
- 仮想環境
- 一時ファイル
- 認証情報や `.env`

## ブランチ運用

共有前提のため、`main` へ直接積むのではなく、作業単位でブランチを切ることを推奨します。

```powershell
git checkout -b feature/update-odds-scraper
git add .
git commit -m "Fix odds scraping logic"
git push -u origin feature/update-odds-scraper
```

## コミットメッセージ例

- `Fix single/place odds parsing`
- `Add resume logic for existing race IDs`
- `Update README and gitignore`
- `Refactor race result export flow`

## 注意事項

- サイト構造の変更によりスクレイピングコードが動かなくなる場合があります
- 取得対象サイトの利用規約・アクセス頻度には注意してください
- 認証情報や個人情報は絶対にコミットしないでください

## 今後の整備候補

- `requirements.txt` の追加
- ディレクトリ構成の整理
- 共通処理のモジュール化
- テストコードの追加
- サンプルデータの整備
- 実行手順の具体化

## メモ

この README は初期たたき台です。  
スクリプト名、依存ライブラリ、出力仕様、フォルダ構成が固まったら随時更新してください。
