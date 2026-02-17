import os
import math
import json
import requests
from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd

import psycopg2
import psycopg2.extras 
from pgvector.psycopg2 import register_vector


try:
    from dotenv import load_dotenv  
except Exception:
    load_dotenv = None



FASTAPI_URL = os.environ.get("RANKING_SERVICE_URL", "http://127.0.0.1:8008/rerank")

USER_ID = int(os.environ.get("TEST_USER_ID", "1"))
K = int(os.environ.get("TEST_TOP_K", "50"))
CANDIDATE_LIMIT = int(os.environ.get("TEST_CAND_LIMIT", "200"))

# for redundancy rate: count pairs with cosine similarity >= threshold
REDUNDANCY_SIM_THRESHOLD = float(os.environ.get("REDUNDANCY_SIM_THRESHOLD", "0.85"))

# choose a diversity level for the MMR run
DIVERSITY_LEVEL = float(os.environ.get("TEST_DIVERSITY_LEVEL", "1.0"))



# Helpers: metrics

def _normalize_rows(X: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(X, axis=1, keepdims=True) + 1e-12
    return X / norms


def _pairwise_cosine_sims(emb: np.ndarray) -> np.ndarray:
    """
    Returns the upper-triangular cosine similarities as a 1D vector.
    """
    if emb.shape[0] < 2:
        return np.array([], dtype=np.float32)

    emb_n = _normalize_rows(emb.astype(np.float32))
    sim = emb_n @ emb_n.T  # NxN

    n = sim.shape[0]
    iu = np.triu_indices(n, k=1)
    return sim[iu].astype(np.float32)


def entropy_from_labels(labels: List[str]) -> float:
    """
    Shannon entropy (natural log). 
    """
    if not labels:
        return 0.0
    counts: Dict[str, int] = {}
    for x in labels:
        counts[x] = counts.get(x, 0) + 1
    total = len(labels)
    ent = 0.0
    for c in counts.values():
        p = c / total
        ent -= p * math.log(p + 1e-12)
    return float(ent)


def compute_metrics(items: List[Dict[str, Any]], redundancy_thr: float) -> Dict[str, float]:
    """
    items: list of dicts with at least: embedding, source, category
    """
    n = len(items)
    if n == 0:
        return {
            "ILD": 0.0,
            "Avg cosine sim": 0.0,
            "Redundancy rate": 0.0,
            "#unique sources": 0.0,
            "Source entropy": 0.0,
            "#unique categories": 0.0,
            "Category entropy": 0.0,
        }

    emb = np.vstack([np.array(it["embedding"], dtype=np.float32) for it in items])
    sims = _pairwise_cosine_sims(emb)  # all i<j similarities
    if sims.size == 0:
        avg_sim = 0.0
        ild = 0.0
        red_rate = 0.0
    else:
        avg_sim = float(np.mean(sims))
        ild = float(np.mean(1.0 - sims))
        red_rate = float(np.mean(sims >= redundancy_thr))

    sources = [(it.get("source") or "") for it in items]
    categories = [(it.get("category") or "") for it in items]

    uniq_sources = len(set(sources))
    uniq_categories = len(set(categories))

    src_ent = entropy_from_labels(sources)
    cat_ent = entropy_from_labels(categories)

    return {
        "ILD": ild,
        "Avg cosine sim": avg_sim,
        "Redundancy rate": red_rate,
        "#unique sources": float(uniq_sources),
        "Source entropy": src_ent,
        "#unique categories": float(uniq_categories),
        "Category entropy": cat_ent,
    }


def rel_stats_from_scores(scores: List[float]) -> Dict[str, float]:
    if not scores:
        return {"avg_rel": 0.0, "min_rel": 0.0, "max_rel": 0.0}
    return {
        "avg_rel": float(np.mean(scores)),
        "min_rel": float(np.min(scores)),
        "max_rel": float(np.max(scores)),
    }



# db: get candidates from view

CAND_QUERY = """
SELECT
  article_id,
  title,
  source,
  category,
  language,
  published_at,
  distance,
  age_seconds,
  embedding
FROM final_user_candidate_list
WHERE user_id = %s
ORDER BY rn_final
LIMIT %s;
"""


def fetch_candidates(db_dsn: str, user_id: int, limit: int) -> List[Dict[str, Any]]:
    with psycopg2.connect(db_dsn) as conn:
        register_vector(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(CAND_QUERY, (user_id, limit))
            rows = cur.fetchall()
    # convert RealDictRow to plain dict and ensure JSON-serializable primitives
    out: List[Dict[str, Any]] = []
    for r in rows:
        emb = r.get("embedding")
        if emb is not None:
            emb = np.asarray(emb, dtype=np.float32).tolist()   # list of Python floats 
            emb = [float(x) for x in emb] 
        out.append(
            {
                "article_id": int(r["article_id"]),
                "title": r.get("title"),
                "source": r.get("source"),
                "category": r.get("category"),
                "language": r.get("language"),
                "published_at": str(r["published_at"]) if r.get("published_at") is not None else None,
                "distance": float(r["distance"]) if r.get("distance") is not None else None,
                "age_seconds": float(r["age_seconds"]) if r.get("age_seconds") is not None else None,
                "embedding": emb,
            }
        )
    return out



# Call FastAPI MMR reranker

def call_mmr_service(candidates: List[Dict[str, Any]], k: int, diversity_level: float) -> Dict[str, Any]:
    payload = {
        "diversity_level": float(diversity_level),
        "k": int(k),
        "candidates": candidates,
    }
    resp = requests.post(FASTAPI_URL, json=payload, timeout=120)
    
    if resp.status_code == 422:
        print("422 error")
    resp.raise_for_status()
    return resp.json()



# Baseline: No MMR

def baseline_no_mmr(candidates: List[Dict[str, Any]], k: int, fastapi_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Baseline "No MMR": rank purely by relevance score from the model, ignoring diversity.
    """

    all_ranked = call_mmr_service(candidates, k=len(candidates), diversity_level=0.0)

    # Build map: article_id -> rel_score 
    rel_map: Dict[int, float] = {}
    # even with diversity_level=0, service still runs MMR but rel_score is still the model output and is what we need for baseline.
    for it in all_ranked["items"]:
        rel_map[int(it["article_id"])] = float(it["rel_score"])

    # attach rel_score to candidates and sort
    cand_scored = []
    for c in candidates:
        rid = int(c["article_id"])
        rs = rel_map.get(rid, None)
        if rs is None:
            # if missing, push it down
            rs = -1.0
        c2 = dict(c)
        c2["rel_score"] = float(rs)
        cand_scored.append(c2)

    cand_scored.sort(key=lambda x: x["rel_score"], reverse=True)
    return cand_scored[:k]



# Pretty print table

def print_metric_table(m_no: Dict[str, float], m_yes: Dict[str, float]) -> None:
    rows = [
        "ILD",
        "Avg cosine sim",
        "Redundancy rate",
        "#unique sources",
        "Source entropy",
        "#unique categories",
        "Category entropy",
    ]

    print("\n" + "=" * 72)
    print(f"{'Metric':<22} {'No MMR':>14} {'With MMR':>14}")
    print("-" * 72)
    for r in rows:
        a = m_no.get(r, 0.0)
        b = m_yes.get(r, 0.0)
        if r.startswith("#"):
            print(f"{r:<22} {a:>14.0f} {b:>14.0f}")
        else:
            print(f"{r:<22} {a:>14.4f} {b:>14.4f}")
    print("=" * 72 + "\n")


def main():

    if load_dotenv is not None:
        here = os.path.dirname(os.path.abspath(__file__))
        thesis_root = os.path.abspath(os.path.join(here, ".."))
        env_path = os.path.join(thesis_root, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)

    db_dsn = os.environ.get("NEWS_DB_DSN_HOST")
    if not db_dsn:
        raise RuntimeError("NEWS_DB_DSN not found. Put it in Thesis/.env or export it in shell.")

    print(f"DB user_id={USER_ID} | candidates={CANDIDATE_LIMIT} | k={K}")
    print(f"MMR diversity_level={DIVERSITY_LEVEL} | redundancy_thr={REDUNDANCY_SIM_THRESHOLD}")
    print(f"FastAPI URL: {FASTAPI_URL}\n")


    candidates = fetch_candidates(db_dsn, USER_ID, CANDIDATE_LIMIT)
    if not candidates:
        print("No candidates returned from view.")
        return


    # with MMR
    mmr_res = call_mmr_service(candidates, k=K, diversity_level=DIVERSITY_LEVEL)
    selected_ids = [int(it["article_id"]) for it in mmr_res["items"]]
    selected_set = set(selected_ids)

    # build "With MMR" list with embeddings/metadata by joining back to candidates
    cand_by_id = {int(c["article_id"]): c for c in candidates}
    with_mmr_items = [cand_by_id[i] for i in selected_ids if i in cand_by_id]

    # no MMR baseline (pure ML sort)
    no_mmr_items = baseline_no_mmr(candidates, k=K, fastapi_result=mmr_res)

    #  Relevance stats (ML scores) 
    with_mmr_rel_scores = [float(it["rel_score"]) for it in mmr_res["items"]]

    # no MMR: already attached by baseline_no_mmr()
    no_mmr_rel_scores = [float(it.get("rel_score", 0.0)) for it in no_mmr_items]

    rs_no = rel_stats_from_scores(no_mmr_rel_scores)
    rs_yes = rel_stats_from_scores(with_mmr_rel_scores)

    # metrics
    m_no = compute_metrics(no_mmr_items, REDUNDANCY_SIM_THRESHOLD)
    m_yes = compute_metrics(with_mmr_items, REDUNDANCY_SIM_THRESHOLD)

    print_metric_table(m_no, m_yes)

    print("Relevance (model rel_score):")
    print(f"  No MMR   -> avg={rs_no['avg_rel']:.4f} | min={rs_no['min_rel']:.4f} | max={rs_no['max_rel']:.4f}")
    print(f"  With MMR -> avg={rs_yes['avg_rel']:.4f} | min={rs_yes['min_rel']:.4f} | max={rs_yes['max_rel']:.4f}\n")


    # print quick qualitative snapshot
    print("Top-10 (No MMR):")
    for j, it in enumerate(no_mmr_items[:10], start=1):
        print(f"{j:2d}. {it['article_id']} | {it.get('source')} | {it.get('category')} | {it.get('title')}")
    print("\nTop-10 (With MMR):")
    for j, it in enumerate(with_mmr_items[:10], start=1):
        print(f"{j:2d}. {it['article_id']} | {it.get('source')} | {it.get('category')} | {it.get('title')}")


if __name__ == "__main__":
    main()
