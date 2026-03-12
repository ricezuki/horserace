import argparse
import json
from pathlib import Path
from typing import List

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    average_precision_score,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


TARGET_COL = "is_win"

LEAKY_COLS = {
    "finish_position",
    "is_top3",
    "is_win",
    "margin",
    "last3f",
    "corner_pass",
    "corner_summary",
    "popularity",
    "win_odds",
    "finish_time",
}

ID_LIKE_COLS = {
    "race_id",
    "horse_id",
    "horse_name",
    "jockey_name",
    "trainer_name",
}

DATE_COLS = {"race_date"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train win prediction model with race-level evaluation."
    )
    parser.add_argument("--feature-file", required=True, help="Path to feature_table csv")
    parser.add_argument("--output-dir", required=True, help="Directory to save model and outputs")
    return parser.parse_args()


def load_feature_table(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    if "race_date" not in df.columns:
        raise ValueError("feature table must contain race_date")
    if TARGET_COL not in df.columns:
        raise ValueError(f"feature table must contain {TARGET_COL}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df.dropna(subset=["race_date"]).copy()

    return df


def pick_feature_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []

    for c in df.columns:
        if c in LEAKY_COLS:
            continue
        if c in ID_LIKE_COLS:
            continue
        if c in DATE_COLS:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            continue
        cols.append(c)

    return cols


def make_time_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    max_date = df["race_date"].max()
    split_date = max_date - pd.Timedelta(days=31)

    train_df = df[df["race_date"] <= split_date].copy()
    valid_df = df[df["race_date"] > split_date].copy()

    if len(train_df) == 0 or len(valid_df) == 0:
        raise ValueError("time split produced empty train or valid set")

    return train_df, valid_df, split_date


def build_pipeline(X: pd.DataFrame) -> Pipeline:
    numeric_cols = X.select_dtypes(include=[np.number, "bool"]).columns.tolist()
    categorical_cols = [c for c in X.columns if c not in numeric_cols]

    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, numeric_cols),
            ("cat", categorical_pipe, categorical_cols),
        ]
    )

    clf = HistGradientBoostingClassifier(
        loss="log_loss",
        learning_rate=0.05,
        max_depth=6,
        max_iter=300,
        min_samples_leaf=30,
        random_state=42,
    )

    pipe = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", clf),
        ]
    )
    return pipe


def compute_basic_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict:
    return {
        "rows": int(len(y_true)),
        "positive_rate": float(np.mean(y_true)),
        "log_loss": float(log_loss(y_true, y_prob)),
        "roc_auc": float(roc_auc_score(y_true, y_prob)),
        "pr_auc": float(average_precision_score(y_true, y_prob)),
    }


def compute_race_level_metrics(valid_out: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    records = []

    for race_id, g in valid_out.groupby("race_id", sort=False):
        gg = g.sort_values("pred_proba", ascending=False).copy()
        n = len(gg)
        actual_win_total = int(gg["is_win"].sum())  # 通常は1

        def calc_at_k(k: int) -> dict:
            topk = gg.head(min(k, n)).copy()
            hits = int(topk["is_win"].sum())

            hit_rate = float(hits > 0)
            precision = float(hits / len(topk)) if len(topk) > 0 else np.nan
            recall = float(hits / actual_win_total) if actual_win_total > 0 else np.nan

            return {
                f"top{k}_hits": hits,
                f"top{k}_hit_rate": hit_rate,
                f"top{k}_precision": precision,
                f"top{k}_recall": recall,
            }

        rec = {
            "race_id": race_id,
            "race_date": gg["race_date"].iloc[0],
            "field_size": int(n),
            "actual_win_total": actual_win_total,
        }
        rec.update(calc_at_k(1))
        rec.update(calc_at_k(3))
        rec.update(calc_at_k(5))
        records.append(rec)

    race_metrics_df = (
        pd.DataFrame(records)
        .sort_values(["race_date", "race_id"])
        .reset_index(drop=True)
    )

    summary = {
        "race_count": int(len(race_metrics_df)),
        "top1_win_hit_rate": float(race_metrics_df["top1_hit_rate"].mean()),
        "top1_win_precision": float(race_metrics_df["top1_precision"].mean()),
        "top1_win_recall": float(race_metrics_df["top1_recall"].mean(skipna=True)),
        "top3_win_hit_rate": float(race_metrics_df["top3_hit_rate"].mean()),
        "top3_win_precision": float(race_metrics_df["top3_precision"].mean()),
        "top3_win_recall": float(race_metrics_df["top3_recall"].mean(skipna=True)),
        "top5_win_hit_rate": float(race_metrics_df["top5_hit_rate"].mean()),
        "top5_win_precision": float(race_metrics_df["top5_precision"].mean()),
        "top5_win_recall": float(race_metrics_df["top5_recall"].mean(skipna=True)),
    }

    return summary, race_metrics_df


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_feature_table(args.feature_file)
    train_df, valid_df, split_date = make_time_split(df)

    feature_cols = pick_feature_columns(df)

    X_train = train_df[feature_cols].copy()
    y_train = train_df[TARGET_COL].astype(int).to_numpy()

    X_valid = valid_df[feature_cols].copy()
    y_valid = valid_df[TARGET_COL].astype(int).to_numpy()

    dt_cols_train = X_train.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns.tolist()
    dt_cols_valid = X_valid.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns.tolist()
    dt_cols = sorted(set(dt_cols_train + dt_cols_valid))

    if dt_cols:
        X_train = X_train.drop(columns=dt_cols, errors="ignore")
        X_valid = X_valid.drop(columns=dt_cols, errors="ignore")
        feature_cols = [c for c in feature_cols if c not in dt_cols]

    model = build_pipeline(X_train)
    model.fit(X_train, y_train)

    valid_prob = model.predict_proba(X_valid)[:, 1]

    basic_metrics = compute_basic_metrics(y_valid, valid_prob)

    keep_cols = [
        c
        for c in [
            "race_id",
            "race_date",
            "horse_id",
            "horse_name",
            "race_class",
            "grade",
            "place",
            "surface",
            "distance",
            "jockey_id",
            "jockey_name",
            "finish_position",
            "is_top3",
            "is_win",
        ]
        if c in valid_df.columns
    ]

    valid_out = valid_df[keep_cols].copy()
    valid_out["pred_proba"] = valid_prob
    valid_out["pred_rank_in_race"] = (
        valid_out.groupby("race_id")["pred_proba"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    race_summary_metrics, race_level_metrics_df = compute_race_level_metrics(valid_out)

    metrics = {}
    metrics.update(basic_metrics)
    metrics.update(race_summary_metrics)

    joblib.dump(model, output_dir / "win_model.joblib")

    with open(output_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

    with open(output_dir / "training_meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "feature_file": args.feature_file,
                "target": TARGET_COL,
                "split_date": str(split_date.date()),
                "train_rows": int(len(train_df)),
                "valid_rows": int(len(valid_df)),
                "dropped_datetime_cols": dt_cols,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    with open(output_dir / "feature_columns.json", "w", encoding="utf-8") as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)

    valid_out.sort_values(["race_date", "race_id", "pred_rank_in_race"]).to_csv(
        output_dir / "valid_predictions.csv",
        index=False,
        encoding="utf-8-sig",
    )

    race_level_metrics_df.to_csv(
        output_dir / "race_level_metrics.csv",
        index=False,
        encoding="utf-8-sig",
    )

    print(f"saved model dir: {output_dir}")
    print(f"train rows: {len(train_df)}")
    print(f"valid rows: {len(valid_df)}")
    print(f"split date: {split_date.date()}")
    if dt_cols:
        print("dropped datetime cols:")
        for c in dt_cols:
            print(f"  {c}")
    print("metrics:")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.6f}")
        else:
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()