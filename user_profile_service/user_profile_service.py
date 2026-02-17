from fastapi import FastAPI, HTTPException
from typing import Dict, List
import numpy as np
import psycopg
from ast import literal_eval


MIN_SIGNALS = 10

INTERACTION_WEIGHTS: Dict[str, float] = {
    "click": 0.5,
    "like": 1.0,
    "dislike": -1.0,
}

def get_db_conn():
    import os
    dsn = os.environ["NEWS_DB_DSN_DOCKER"]
    if(not dsn):
        raise KeyError("NEWS_DB_DSN_DOCKER")
    return psycopg.connect(dsn)

def fetch_user_interactions_with_embeddings(conn, user_id: int):
    """
    Fetches the most recent interaction for each article the user has interacted with in the last 21 days, along with the article's embedding.
    Interactions are weighted by type (like > click > dislike). Only articles with embeddings are considered.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.embedding,
                x.interaction_type
            FROM (
                SELECT DISTINCT ON (i.article_id)
                    i.article_id,
                    i.interaction_type
                FROM interactions i
                WHERE i.user_id = %s
                AND i.interaction_time >= NOW() - INTERVAL '21 days'
                ORDER BY
                    i.article_id,
                    CASE i.interaction_type
                        WHEN 'dislike' THEN 3
                        WHEN 'like'    THEN 2
                        WHEN 'click'   THEN 1
                        ELSE 0
                    END DESC
            ) x
            JOIN articles a ON a.id = x.article_id
            WHERE a.embedding IS NOT NULL;
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    vectors: List[np.ndarray] = []
    weights: List[float] = []

    for emb, interaction_type in rows:
        if emb is None:
            continue
        if isinstance(emb, str):
            emb = literal_eval(emb)
        w = INTERACTION_WEIGHTS.get(interaction_type, 0.0)
        if w == 0.0:
            continue
        vectors.append(np.array(emb, dtype=np.float32))
        weights.append(w)

    return vectors, weights

def compute_user_embedding(vectors: List[np.ndarray], weights: List[float]):
    """
    Computes a user embedding as a weighted average of article embeddings, where weights are determined by interaction type.
    """
    if not vectors:
        return None

    w = np.array(weights, dtype=np.float32)

    NEG_CAP = -3.0
    neg_sum = w[w < 0].sum()
    if neg_sum < NEG_CAP:
        scale = NEG_CAP / neg_sum
        w[w < 0] *= scale

    V = np.stack(vectors, axis=0)
    weighted_sum = (V * w[:, None]).sum(axis=0)
    total_weight = np.abs(w).sum()
    if total_weight == 0:
        return None

    user_vec = weighted_sum / total_weight

    norm = np.linalg.norm(user_vec)
    if norm > 0:
        user_vec = user_vec / norm

    return user_vec

def save_user_embedding(conn, user_id: int, embedding):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE users
            SET embedding = %s
            WHERE id = %s;
            """,
            (embedding.tolist() if embedding is not None else None, user_id),
        )
    conn.commit()

app = FastAPI()

@app.post("/users/{user_id}/recompute")
def recompute(user_id: int):
    try:
        conn = get_db_conn()
        vectors, weights = fetch_user_interactions_with_embeddings(conn, user_id)

        if len(vectors) < MIN_SIGNALS:
            save_user_embedding(conn, user_id, None)
            conn.close()
            return {
                "user_id": user_id,
                "updated": False,
                "cold_start": True,
                "n_signals": len(vectors),
            }
        
        user_emb = compute_user_embedding(vectors, weights)
        save_user_embedding(conn, user_id, user_emb)
        conn.close()

        return {
            "user_id": user_id,
            "updated": True,
            "cold_start": False,
            "n_signals": len(vectors),
        }
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"Missing env var: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
