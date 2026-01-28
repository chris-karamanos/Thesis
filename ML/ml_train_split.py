import os
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.base import clone
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss

from dotenv import load_dotenv
import psycopg2
import joblib


# -----------------------------
# DB loading
# -----------------------------
def read_view(dsn: str, view_name: str) -> pd.DataFrame:
    q = f"SELECT * FROM {view_name};"
    with psycopg2.connect(dsn) as conn:
        return pd.read_sql(q, conn)


# -----------------------------
# Ranking metrics per request_id
# -----------------------------
def precision_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    if len(y_true) == 0:
        return np.nan
    k = min(k, len(y_true))
    idx = np.argsort(-y_score)[:k]
    return float(np.sum(y_true[idx] == 1) / k)

def dcg_at_k(y_true_sorted: np.ndarray, k: int) -> float:
    k = min(k, len(y_true_sorted))
    gains = y_true_sorted[:k].astype(float)
    discounts = 1.0 / np.log2(np.arange(2, k + 2))
    return float(np.sum(gains * discounts))

def ndcg_at_k(y_true: np.ndarray, y_score: np.ndarray, k: int) -> float:
    if len(y_true) == 0:
        return np.nan
    order = np.argsort(-y_score)
    y_sorted = y_true[order]
    dcg = dcg_at_k(y_sorted, k)

    ideal_order = np.argsort(-y_true)  # since y_true is 0/1, this puts all 1s first
    y_ideal = y_true[ideal_order]
    idcg = dcg_at_k(y_ideal, k)
    if idcg == 0.0:
        return 0.0
    return dcg / idcg

def group_ranking_metrics(df: pd.DataFrame, score_col: str, k_list=(5,10,20)) -> dict:
    out = {}
    groups = df.groupby("request_id", dropna=False)
    for k in k_list:
        p_vals = []
        n_vals = []
        for _, g in groups:
            y = g["label"].to_numpy()
            s = g[score_col].to_numpy()
            p_vals.append(precision_at_k(y, s, k))
            n_vals.append(ndcg_at_k(y, s, k))
        out[f"Precision@{k}"] = float(np.nanmean(p_vals))
        out[f"NDCG@{k}"] = float(np.nanmean(n_vals))
    out["num_groups"] = int(groups.ngroups)
    return out


if __name__ == "__main__":

    load_dotenv()
    dsn = os.environ.get("NEWS_DB_DSN_HOST")
    if not dsn:
        raise RuntimeError("Set NEWS_DB_DSN environment variable first.")

    train_df = read_view(dsn, "training_dataset_train")
    val_df = read_view(dsn, "training_dataset_val")

    # Basic cleaning (defensive)
    needed_cols = ["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight", "request_id"]
    for c in needed_cols:
        if c not in train_df.columns or c not in val_df.columns:
            raise RuntimeError(f"Missing required column: {c}")

    # Replace extreme/invalid recency (optional)
    for df in (train_df, val_df):
        df["hours_since_publish"] = pd.to_numeric(df["hours_since_publish"], errors="coerce")
        df["cosine_similarity"] = pd.to_numeric(df["cosine_similarity"], errors="coerce")

    train_df = train_df.dropna(subset=["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight"])
    val_df   = val_df.dropna(subset=["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight"])

    X_train = train_df[["cosine_similarity", "hours_since_publish", "source", "category"]]
    y_train = train_df["label"].astype(int).to_numpy()
    w_train = train_df["weight"].astype(float).to_numpy()

    X_val = val_df[["cosine_similarity", "hours_since_publish", "source", "category"]]
    y_val = val_df["label"].astype(int).to_numpy()
    w_val = val_df["weight"].astype(float).to_numpy()

    numeric_features = ["cosine_similarity", "hours_since_publish"]
    categorical_features = ["source", "category"]

    pre = ColumnTransformer(
        transformers=[
            ("num", "passthrough", numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=2), categorical_features),
        ],
        remainder="drop"
    )

    # -----------------------------
    # Logistic Regression baseline
    # -----------------------------
    logreg = LogisticRegression(
        solver="liblinear",
        max_iter=2000
    )

    logreg_pipe = Pipeline([
        ("pre", pre),
        ("clf", logreg),
    ])

    logreg_pipe.fit(X_train, y_train, clf__sample_weight=w_train)
    val_proba_lr = logreg_pipe.predict_proba(X_val)[:, 1]

    print("\n=== Logistic Regression (weighted) ===")
    print("AUC:", roc_auc_score(y_val, val_proba_lr, sample_weight=w_val))
    print("PR-AUC:", average_precision_score(y_val, val_proba_lr, sample_weight=w_val))
    print("Brier:", brier_score_loss(y_val, val_proba_lr, sample_weight=w_val))

    val_scored_lr = val_df.copy()
    val_scored_lr["score"] = val_proba_lr
    print("Ranking metrics:", group_ranking_metrics(val_scored_lr, "score", k_list=(5,10,20)))


    # -----------------------------
    # LightGBM 
    # -----------------------------
    try:
        import lightgbm as lgb

        # Build matrices after preprocessing
        pre_lgb = clone(pre)
        X_train_mat = pre_lgb.fit_transform(X_train)
        X_val_mat = pre_lgb.transform(X_val)

        lgb_train = lgb.Dataset(X_train_mat, label=y_train, weight=w_train)
        lgb_val = lgb.Dataset(X_val_mat, label=y_val, weight=w_val, reference=lgb_train)

        params = {
            "objective": "binary",
            "metric": "auc",
            "learning_rate": 0.05,
            "num_leaves": 31,
            "min_data_in_leaf": 20,
            "feature_fraction": 0.9,
            "verbosity": -1,
        }

        gbm = lgb.train(
            params,
            lgb_train,
            num_boost_round=500,
            valid_sets=[lgb_val],
            callbacks=[lgb.early_stopping(stopping_rounds=30)]
        )

        val_proba_lgb = gbm.predict(X_val_mat)

        print("\n=== LightGBM (weighted) ===")
        print("AUC:", roc_auc_score(y_val, val_proba_lgb, sample_weight=w_val))
        print("PR-AUC:", average_precision_score(y_val, val_proba_lgb, sample_weight=w_val))
        print("Brier:", brier_score_loss(y_val, val_proba_lgb, sample_weight=w_val))

        val_scored_lgb = val_df.copy()
        val_scored_lgb["score"] = val_proba_lgb
        print("Ranking metrics:", group_ranking_metrics(val_scored_lgb, "score", k_list=(5,10,20)))

    except ImportError:
        print("\n[INFO] lightgbm is not installed. Skipping LightGBM training.")
        print("Install with: pip install lightgbm")
