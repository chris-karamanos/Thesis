import os
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss

from dotenv import load_dotenv
import lightgbm as lgb
import psycopg2


# ---------- DB ----------
def read_view(dsn: str, view_name: str) -> pd.DataFrame:
    q = f"SELECT * FROM {view_name};"
    with psycopg2.connect(dsn) as conn:
        return pd.read_sql(q, conn)


# ---------- Ranking metrics ----------
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

    ideal_order = np.argsort(-y_true)
    y_ideal = y_true[ideal_order]
    idcg = dcg_at_k(y_ideal, k)
    return (dcg / idcg) if idcg > 0 else 0.0

def group_ranking_metrics(df: pd.DataFrame, score_col: str, k_list=(5,10,20)) -> dict:
    out = {}
    groups = df.groupby("request_id", dropna=False)
    for k in k_list:
        p_vals, n_vals = [], []
        for _, g in groups:
            y = g["label"].to_numpy()
            s = g[score_col].to_numpy()
            p_vals.append(precision_at_k(y, s, k))
            n_vals.append(ndcg_at_k(y, s, k))
        out[f"Precision@{k}"] = float(np.nanmean(p_vals))
        out[f"NDCG@{k}"] = float(np.nanmean(n_vals))
    out["num_groups"] = int(groups.ngroups)
    return out


def mean_sd(values):
    arr = np.array(values, dtype=float)
    m = float(np.nanmean(arr))
    s = float(np.nanstd(arr, ddof=1)) if np.sum(~np.isnan(arr)) > 1 else 0.0
    return m, s


if __name__ == "__main__":
    
    load_dotenv()
    dsn = os.environ.get("NEWS_DB_DSN_HOST")
    if not dsn:
        raise RuntimeError("Set NEWS_DB_DSN environment variable first.")

    # Load once (all data with day)
    df = read_view(dsn, "training_dataset_day")

    # Defensive cleaning
    needed_cols = ["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight", "request_id", "shown_day"]
    for c in needed_cols:
        if c not in df.columns:
            raise RuntimeError(f"Missing required column: {c}")

    df["hours_since_publish"] = pd.to_numeric(df["hours_since_publish"], errors="coerce")
    df["cosine_similarity"] = pd.to_numeric(df["cosine_similarity"], errors="coerce")
    df["label"] = pd.to_numeric(df["label"], errors="coerce")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["shown_day"] = pd.to_datetime(df["shown_day"], errors="coerce").dt.date

    df = df.dropna(subset=["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight", "request_id", "shown_day"])
    df["label"] = df["label"].astype(int)

    # Sort unique days
    days = sorted(df["shown_day"].unique())
    if len(days) < 2:
        raise RuntimeError("Need at least 2 distinct days for rolling evaluation.")

    numeric_features = ["cosine_similarity", "hours_since_publish"]
    categorical_features = ["source", "category"]

    pre = ColumnTransformer(
        transformers=[
            ("num", "passthrough", numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=2), categorical_features),
        ],
        remainder="drop"
    )

    # Model
    logreg = LogisticRegression(solver="liblinear", max_iter=2000)
    pipe = Pipeline([("pre", pre), ("clf", logreg)])

    lgb_params = {
    "objective": "binary",
    "metric": "auc",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 20,
    "feature_fraction": 0.9,
    "verbosity": -1,
    }
    days = sorted(df["shown_day"].unique())
    fold_rows = []

    for i in range(1, len(days)):
        val_day = days[i]
        train_days = set(days[:i])

        train_df = df[df["shown_day"].isin(train_days)].copy()
        val_df = df[df["shown_day"] == val_day].copy()

        # -----------------------
        # Cosine-only baseline
        # -----------------------
        # Χρησιμοποιούμε απευθείας το cosine_similarity ως score
        scored_cos = val_df.copy()
        scored_cos["score"] = scored_cos["cosine_similarity"]

        rank_cos = group_ranking_metrics(scored_cos, "score", k_list=(5, 10, 20))

        n_train_groups = train_df["request_id"].nunique()
        n_val_groups = val_df["request_id"].nunique()

        X_train = train_df[["cosine_similarity", "hours_since_publish", "source", "category"]]
        y_train = train_df["label"].to_numpy()
        w_train = train_df["weight"].to_numpy()

        X_val = val_df[["cosine_similarity", "hours_since_publish", "source", "category"]]
        y_val = val_df["label"].to_numpy()
        w_val = val_df["weight"].to_numpy()

        # -----------------------
        # Logistic Regression
        # -----------------------
        pipe.fit(X_train, y_train, clf__sample_weight=w_train)
        val_proba_lr = pipe.predict_proba(X_val)[:, 1]

        auc_lr = roc_auc_score(y_val, val_proba_lr, sample_weight=w_val) if len(np.unique(y_val)) > 1 else np.nan
        pr_lr = average_precision_score(y_val, val_proba_lr, sample_weight=w_val)
        brier_lr = brier_score_loss(y_val, val_proba_lr, sample_weight=w_val)

        scored_lr = val_df.copy()
        scored_lr["score"] = val_proba_lr
        rank_lr = group_ranking_metrics(scored_lr, "score", k_list=(5, 10, 20))

        # -----------------------
        # LightGBM (fit per fold)
        # IMPORTANT: fit preprocessor on TRAIN only, then transform VAL
        # -----------------------
        pre_fold = ColumnTransformer(
            transformers=[
                ("num", "passthrough", ["cosine_similarity", "hours_since_publish"]),
                ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=2), ["source", "category"]),
            ],
            remainder="drop"
        )

        X_train_mat = pre_fold.fit_transform(X_train)
        X_val_mat = pre_fold.transform(X_val)

        dtrain = lgb.Dataset(X_train_mat, label=y_train, weight=w_train)
        dval = lgb.Dataset(X_val_mat, label=y_val, weight=w_val, reference=dtrain)

        gbm = lgb.train(
            lgb_params,
            dtrain,
            num_boost_round=500,
            valid_sets=[dval],
            callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
        )

        val_proba_lgb = gbm.predict(X_val_mat)

        auc_lgb = roc_auc_score(y_val, val_proba_lgb, sample_weight=w_val) if len(np.unique(y_val)) > 1 else np.nan
        pr_lgb = average_precision_score(y_val, val_proba_lgb, sample_weight=w_val)
        brier_lgb = brier_score_loss(y_val, val_proba_lgb, sample_weight=w_val)

        scored_lgb = val_df.copy()
        scored_lgb["score"] = val_proba_lgb
        rank_lgb = group_ranking_metrics(scored_lgb, "score", k_list=(5, 10, 20))

        # -----------------------
        # Store fold results (both models)
        # -----------------------
        fold_rows.append({
            "val_day": str(val_day),
            "train_groups": int(n_train_groups),
            "val_groups": int(n_val_groups),
            "val_rows": int(len(val_df)),

            "LR_AUC": float(auc_lr) if auc_lr == auc_lr else np.nan,
            "LR_PR_AUC": float(pr_lr),
            "LR_Brier": float(brier_lr),
            "LR_P@5": rank_lr["Precision@5"],
            "LR_NDCG@5": rank_lr["NDCG@5"],
            "LR_P@10": rank_lr["Precision@10"],
            "LR_NDCG@10": rank_lr["NDCG@10"],
            "LR_P@20": rank_lr["Precision@20"],
            "LR_NDCG@20": rank_lr["NDCG@20"],

            "LGB_AUC": float(auc_lgb) if auc_lgb == auc_lgb else np.nan,
            "LGB_PR_AUC": float(pr_lgb),
            "LGB_Brier": float(brier_lgb),
            "LGB_P@5": rank_lgb["Precision@5"],
            "LGB_NDCG@5": rank_lgb["NDCG@5"],
            "LGB_P@10": rank_lgb["Precision@10"],
            "LGB_NDCG@10": rank_lgb["NDCG@10"],
            "LGB_P@20": rank_lgb["Precision@20"],
            "LGB_NDCG@20": rank_lgb["NDCG@20"],

            "COS_P@5": rank_cos["Precision@5"],
            "COS_NDCG@5": rank_cos["NDCG@5"],
            "COS_P@10": rank_cos["Precision@10"],
            "COS_NDCG@10": rank_cos["NDCG@10"],
            "COS_P@20": rank_cos["Precision@20"],
            "COS_NDCG@20": rank_cos["NDCG@20"],
        })

    results = pd.DataFrame(fold_rows)

    print("\n=== Rolling day-based evaluation (weighted) ===")
    print(results.to_string(index=False))

    print("\n=== Summary (mean ± SD across folds) ===")
    summary_cols = [
        "LR_AUC","LR_PR_AUC","LR_Brier","LR_P@5","LR_NDCG@5","LR_P@10","LR_NDCG@10","LR_P@20","LR_NDCG@20",
        "LGB_AUC","LGB_PR_AUC","LGB_Brier","LGB_P@5","LGB_NDCG@5","LGB_P@10","LGB_NDCG@10","LGB_P@20","LGB_NDCG@20","COS_P@5","COS_NDCG@5",
        "COS_P@10","COS_NDCG@10","COS_P@20","COS_NDCG@20",
    ]
    for c in summary_cols:
        m, s = mean_sd(results[c].to_numpy())
        print(f"{c}: {m:.6f} ± {s:.6f}")

