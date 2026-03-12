#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
train_top3_model.py

feature_table.csv から、まずは 3着内予測 (is_top3) 用の初期モデルを学習するスクリプト。

方針:
- 目的変数: is_top3
- 時系列を壊さないよう、race_date で train / valid を分割
- 扱いやすさ重視で scikit-learn の HistGradientBoostingClassifier を利用
- 文字列カテゴリは OrdinalEncoder で数値化
- 学習後にモデル / メタ情報 / 指標 / 予測結果を保存

使用例:
python train_top3_model.py \
  --feature-file ./features/feature_table_2025_2026-01.csv \
  --output-dir ./models/top3_baseline
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder


TARGET_COL = "is_top3"
DATE_COL = "race_date"
DEFAULT_EXCLUDE_COLS = {
    # ターゲット・リークになり得る列
    "finish_position",
    "is_top3",
    "is_win",
    "finish_time",
    "margin",            # 今回レースの着差
    "last3f",            # 今回レースの上がり
    "corner_pass",       # 今回レースの通過順
    "corner_summary",    # 今回レース全体の通過順要約
    "popularity",        # 確定後の人気。予想時点で使わない前提
    "win_odds",          # 確定後オッズ。予想時点で使わない前提
    # 生テキスト
    "race_data01_raw",
    "race_data02_raw",
    # 名前列は基礎版では使わない
    "horse_name",
    "jockey_name",
    "trainer_name",
}


@dataclass
class SplitResult:
    train_df: pd.DataFrame
    valid_df: pd.DataFrame
    split_date: pd.Timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train baseline top3 model from feature_table.csv")
    parser.add_argument("--feature-file", required=True, help="Path to feature_table.csv")
    parser.add_argument("--output-dir", required=True, help="Directory to save model and reports")
    parser.add_argument(
        "--valid-days",
        type=int,
        default=31,
        help="Use the latest N days as validation window (default: 31)",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=1000,
        help="Minimum number of training rows required",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed",
    )
    return parser.parse_args()



def load_feature_table(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    if TARGET_COL not in df.columns:
        raise ValueError(f"{TARGET_COL} column not found in feature table")
    if DATE_COL not in df.columns:
        raise ValueError(f"{DATE_COL} column not found in feature table")

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TARGET_COL]).copy()
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")
    df = df.dropna(subset=[TARGET_COL]).copy()
    df[TARGET_COL] = df[TARGET_COL].astype(int)
    return df



def make_time_split(df: pd.DataFrame, valid_days: int, min_train_rows: int) -> SplitResult:
    max_date = df[DATE_COL].max()
    split_date = max_date - pd.Timedelta(days=valid_days)

    train_df = df[df[DATE_COL] < split_date].copy()
    valid_df = df[df[DATE_COL] >= split_date].copy()

    # 検証期間が短すぎて valid が空になる場合の保険
    if valid_df.empty:
        unique_dates = sorted(df[DATE_COL].dropna().unique())
        if len(unique_dates) < 2:
            raise ValueError("Not enough date diversity to build a validation split")
        fallback_date = pd.Timestamp(unique_dates[-1])
        train_df = df[df[DATE_COL] < fallback_date].copy()
        valid_df = df[df[DATE_COL] >= fallback_date].copy()
        split_date = fallback_date

    if len(train_df) < min_train_rows:
        raise ValueError(
            f"Training rows are too few: {len(train_df)} < {min_train_rows}. "
            "Collect more historical data before training."
        )
    if valid_df.empty:
        raise ValueError("Validation split is empty")

    return SplitResult(train_df=train_df, valid_df=valid_df, split_date=split_date)



def select_feature_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for col in df.columns:
        if col in DEFAULT_EXCLUDE_COLS:
            continue
        if col == DATE_COL:
            continue
        cols.append(col)
    return cols



def build_preprocessor(X: pd.DataFrame) -> Tuple[ColumnTransformer, List[str], List[str]]:
    categorical_cols = [c for c in X.columns if X[c].dtype == "object"]
    numeric_cols = [c for c in X.columns if c not in categorical_cols]

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="__MISSING__")),
            (
                "encoder",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                    encoded_missing_value=-1,
                ),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numeric_cols),
            ("cat", categorical_pipeline, categorical_cols),
        ],
        remainder="drop",
    )
    return preprocessor, numeric_cols, categorical_cols



def build_model(random_state: int) -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=0.05,
        max_iter=300,
        max_leaf_nodes=31,
        min_samples_leaf=50,
        l2_regularization=0.1,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=30,
        random_state=random_state,
    )



def evaluate(y_true: np.ndarray, y_prob: np.ndarray) -> dict:
    metrics = {
        "rows": int(len(y_true)),
        "positive_rate": float(np.mean(y_true)),
        "log_loss": float(log_loss(y_true, y_prob, labels=[0, 1])),
        "roc_auc": float(roc_auc_score(y_true, y_prob)),
        "pr_auc": float(average_precision_score(y_true, y_prob)),
    }

    # 上位馬抽出の感触を見る簡易指標
    for k in (1, 2, 3, 5):
        threshold = np.quantile(y_prob, 1 - min(0.999, k / max(len(y_prob), 1)))
        pred = (y_prob >= threshold).astype(int)
        metrics[f"top_quantile_precision_proxy_k{k}"] = float(
            y_true[pred == 1].mean() if (pred == 1).any() else 0.0
        )

    return metrics



def save_outputs(
    output_dir: Path,
    pipeline: Pipeline,
    feature_columns: List[str],
    numeric_cols: List[str],
    categorical_cols: List[str],
    split: SplitResult,
    valid_df: pd.DataFrame,
    y_prob: np.ndarray,
    metrics: dict,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    model_path = output_dir / "top3_model.joblib"
    meta_path = output_dir / "training_meta.json"
    metrics_path = output_dir / "metrics.json"
    preds_path = output_dir / "valid_predictions.csv"
    features_path = output_dir / "feature_columns.json"

    joblib.dump(pipeline, model_path)

    meta = {
        "target": TARGET_COL,
        "split_date": str(split.split_date.date()),
        "train_rows": int(len(split.train_df)),
        "valid_rows": int(len(split.valid_df)),
        "feature_count": int(len(feature_columns)),
        "feature_columns_path": str(features_path.name),
        "model_path": str(model_path.name),
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    feature_meta = {
        "all_features": feature_columns,
        "numeric_features": numeric_cols,
        "categorical_features": categorical_cols,
    }
    with open(features_path, "w", encoding="utf-8") as f:
        json.dump(feature_meta, f, ensure_ascii=False, indent=2)

    pred_df = valid_df[[DATE_COL, "race_id", "horse_id", "jockey_id", "race_class", "grade", "horse_number"]].copy()
    if "horse_name" in valid_df.columns:
        pred_df["horse_name"] = valid_df["horse_name"]
    pred_df["y_true"] = valid_df[TARGET_COL].values
    pred_df["pred_top3_prob"] = y_prob
    pred_df = pred_df.sort_values([DATE_COL, "race_id", "pred_top3_prob"], ascending=[True, True, False])
    pred_df.to_csv(preds_path, index=False, encoding="utf-8-sig")



def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)

    df = load_feature_table(args.feature_file)
    split = make_time_split(df, valid_days=args.valid_days, min_train_rows=args.min_train_rows)

    feature_columns = select_feature_columns(df)
    X_train = split.train_df[feature_columns].copy()
    y_train = split.train_df[TARGET_COL].astype(int).values
    X_valid = split.valid_df[feature_columns].copy()
    y_valid = split.valid_df[TARGET_COL].astype(int).values

    preprocessor, numeric_cols, categorical_cols = build_preprocessor(X_train)
    model = build_model(random_state=args.random_state)

    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", model),
        ]
    )

    pipeline.fit(X_train, y_train)
    y_prob = pipeline.predict_proba(X_valid)[:, 1]
    metrics = evaluate(y_valid, y_prob)

    save_outputs(
        output_dir=output_dir,
        pipeline=pipeline,
        feature_columns=feature_columns,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        split=split,
        valid_df=split.valid_df,
        y_prob=y_prob,
        metrics=metrics,
    )

    print(f"saved model dir: {output_dir}")
    print(f"train rows: {len(split.train_df)}")
    print(f"valid rows: {len(split.valid_df)}")
    print(f"split date: {split.split_date.date()}")
    print("metrics:")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.6f}")
        else:
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
