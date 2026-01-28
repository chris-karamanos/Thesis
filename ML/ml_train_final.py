import os
import json
import time
import numpy as np
import pandas as pd
import psycopg2
import joblib
from dotenv import load_dotenv

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression


VIEW_NAME = "training_dataset"   
OUT_MODEL = "model_ranker.joblib"
OUT_META  = "model_ranker_meta.json"


def read_view(dsn: str, view_name: str) -> pd.DataFrame:
    q = f"SELECT * FROM {view_name};"
    with psycopg2.connect(dsn) as conn:
        return pd.read_sql(q, conn)



if __name__ == "__main__":

    load_dotenv()
    dsn = os.environ.get("NEWS_DB_DSN_HOST")
    if not dsn:
        raise RuntimeError("Set NEWS_DB_DSN environment variable first.")

    df = read_view(dsn, VIEW_NAME)

    # Required columns 
    needed = ["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight"]
    for c in needed:
        if c not in df.columns:
            raise RuntimeError(f"Missing required column '{c}' in {VIEW_NAME}")

    # Type cleaning 
    df["cosine_similarity"] = pd.to_numeric(df["cosine_similarity"], errors="coerce")
    df["hours_since_publish"] = pd.to_numeric(df["hours_since_publish"], errors="coerce")
    df["label"] = pd.to_numeric(df["label"], errors="coerce")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    df = df.dropna(subset=["cosine_similarity", "hours_since_publish", "source", "category", "label", "weight"])
    df["label"] = df["label"].astype(int)

    # Features / labels / weights
    feature_cols = ["cosine_similarity", "hours_since_publish", "source", "category"]
    X = df[feature_cols]
    y = df["label"].to_numpy()
    w = df["weight"].to_numpy()

    # Preprocess
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
    clf = LogisticRegression(
        solver="liblinear",
        max_iter=2000,
        C=1.0,               
    )

    pipe = Pipeline([("pre", pre), ("clf", clf)])

    t0 = time.time()
    pipe.fit(X, y, clf__sample_weight=w)
    train_secs = time.time() - t0

    # Save model artifact
    joblib.dump(pipe, OUT_MODEL)

    # Save metadata
    meta = {
        "view": VIEW_NAME,
        "n_rows": int(len(df)),
        "n_pos": int((df["label"] == 1).sum()),
        "n_neg": int((df["label"] == 0).sum()),
        "avg_weight": float(np.mean(w)),
        "feature_cols": feature_cols,
        "model": "LogisticRegression(liblinear)",
        "C": 1.0,
        "min_frequency_onehot": 2,
        "train_seconds": float(train_secs),
        "created_at_unix": int(time.time()),
    }

    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"Saved: {OUT_MODEL}")
    print(f"Saved: {OUT_META}")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
