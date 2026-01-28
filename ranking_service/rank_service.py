"""
rank_service.py
FastAPI microservice:
- Builds ML feature vector exactly as in training (cosine_similarity, hours_since_publish, source, category)
- Loads model once via lifespan (replaces deprecated @app.on_event)
- Scores candidates with model_ranker.joblib
- MMR reranks with content/topic diversity + source/category/language penalties
- User-controlled tradeoff via diversity_level slider
- Source hard-cap scales in [15 -> 5] as diversity_level goes [0 -> 1]
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# -----------------------------
# Config
# -----------------------------
# Put your model here (relative to ranking_service/ folder)
MODEL_PATH = os.getenv("RANK_MODEL_PATH", "../ML/model_ranker.joblib")
DEFAULT_K = int(os.getenv("RANK_DEFAULT_K", "50"))


# -----------------------------
# Request/Response schemas
# -----------------------------
class Candidate(BaseModel):
    article_id: int
    title: Optional[str] = None
    source: Optional[str] = None
    category: Optional[str] = None
    language: Optional[str] = None

    # From candidate view
    published_at: Optional[str] = None
    distance: float = None        # pgvector <=> distance (assumed cosine distance)
    age_seconds: float = None     # NOW() - published_at in seconds

    # For MMR content diversity
    embedding: List[float] = Field(..., min_length=10)


class RerankRequest(BaseModel):
    diversity_level: float = Field(..., ge=0.0, le=1.0)
    k: int = Field(DEFAULT_K, ge=1, le=200)
    candidates: List[Candidate]


class RankedItem(BaseModel):
    article_id: int
    rank: int
    mmr_score: float
    rel_score: float

    source: Optional[str] = None
    category: Optional[str] = None
    language: Optional[str] = None
    title: Optional[str] = None


class RerankResponse(BaseModel):
    lambda_mmr: float
    max_per_source: int
    items: List[RankedItem]


# -----------------------------
# Globals loaded at startup
# -----------------------------
PIPE = None
FEATURE_COLS: Optional[List[str]] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load model artifacts once at startup
    global PIPE, FEATURE_COLS
    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Model file not found at: {MODEL_PATH}")

    PIPE = joblib.load(MODEL_PATH)

    # If available, enforce model feature columns at inference
    FEATURE_COLS = getattr(PIPE, "feature_names_in_", None)
    if FEATURE_COLS is not None:
        FEATURE_COLS = list(FEATURE_COLS)
    else:
        # Fallback to known training features (from your ml_train_final.py)
        FEATURE_COLS = ["cosine_similarity", "hours_since_publish", "source", "category"]

    yield
    # no cleanup needed


app = FastAPI(title="Ranker+MMR Service", lifespan=lifespan)


# -----------------------------
# Helpers
# -----------------------------
def _normalize_rows(X: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(X, axis=1, keepdims=True) + 1e-12
    return X / norms


def _mmr_params(div_level: float) -> Tuple[float, float, float, float, int]:
    """
    Map slider (0..1) -> MMR parameters.

    lambda_mmr: accuracy vs diversity tradeoff
      - div=0   -> lambda ~ 0.95 (mostly relevance)
      - div=1   -> lambda ~ 0.55 (more diversity, but not chaotic)

    gamma_*: soft penalties for repeated source/category/language
    max_per_source: hard cap in [15..5] as requested
    """
    lam = 0.95 - 0.40 * div_level  # 0.95 -> 0.55

    gamma_source = 0.08 * div_level
    gamma_category = 0.05 * div_level
    gamma_lang = 0.02 * div_level  # more mild

    # hard cap per source: 15 -> 5
    max_per_source = int(round(15 - 10 * div_level))
    max_per_source = max(5, max_per_source)

    return lam, gamma_source, gamma_category, gamma_lang, max_per_source


def _build_features_for_model(c: Candidate) -> Dict[str, Any]:
    """
    Build EXACT feature vector used at training time:
      - cosine_similarity
      - hours_since_publish
      - source
      - category

    Assumptions:
      - Candidate.distance is cosine distance (so cosine_similarity = 1 - distance)
      - Candidate.age_seconds is recency in seconds (so hours_since_publish = age_seconds / 3600)
    """
    if c.distance is None:
        raise HTTPException(
            status_code=400,
            detail=f"Missing 'distance' for article_id={c.article_id}. "
                   "Candidate list must provide pgvector distance.",
        )
    if c.age_seconds is None:
        raise HTTPException(
            status_code=400,
            detail=f"Missing 'age_seconds' for article_id={c.article_id}. "
                   "Candidate list must provide age_seconds.",
        )

    cosine_similarity = max(0.0, min(1.0, 1.0 - float(c.distance)))
    hours_since_publish = float(c.age_seconds) / 3600.0

    return {
        "cosine_similarity": cosine_similarity,
        "hours_since_publish": hours_since_publish,
        "source": c.source or "",
        "category": c.category or "",
    }


def _score_with_model(feature_rows: List[Dict[str, Any]]) -> np.ndarray:
    """
    Score candidates using the loaded pipeline.
    Expects rows already containing the correct training features.
    """
    if PIPE is None:
        raise HTTPException(status_code=500, detail="Model pipeline not loaded.")

    df = pd.DataFrame(feature_rows)

    # Enforce feature column order
    assert FEATURE_COLS is not None
    missing = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required feature columns for model: {missing}",
        )
    X = df[FEATURE_COLS]

    try:
        proba = PIPE.predict_proba(X)[:, 1]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model scoring failed: {e}")

    return proba.astype(np.float32)


def mmr_rerank(
    rel: np.ndarray,
    emb: np.ndarray,
    sources: List[str],
    categories: List[str],
    languages: List[str],
    k: int,
    diversity_level: float,
) -> Tuple[List[int], List[float], float, int]:
    """
    Multi-objective MMR:
      score = λ*rel - (1-λ)*maxSimToSelected
              - γs*count(source) - γc*count(category) - γl*count(lang)

    plus hard cap on max items per source.
    """
    n = int(len(rel))
    k = min(int(k), n)

    lam, g_s, g_c, g_l, max_per_source = _mmr_params(float(diversity_level))

    # cosine sim matrix NxN (N<=200 is fine)
    emb_n = _normalize_rows(emb)
    sim = emb_n @ emb_n.T

    selected: List[int] = []
    selected_scores: List[float] = []

    src_count: Dict[str, int] = {}
    cat_count: Dict[str, int] = {}
    lang_count: Dict[str, int] = {}

    remaining = set(range(n))

    # 1st: highest relevance
    first = int(np.argmax(rel))
    selected.append(first)
    remaining.remove(first)

    # For first item, define an MMR score consistent with formula (no redundancy yet)
    s0 = sources[first] or ""
    c0 = categories[first] or ""
    l0 = languages[first] or ""
    mmr_first = lam * float(rel[first])  # maxSim=0, penalties=0 at start
    selected_scores.append(mmr_first)

    src_count[s0] = src_count.get(s0, 0) + 1
    cat_count[c0] = cat_count.get(c0, 0) + 1
    lang_count[l0] = lang_count.get(l0, 0) + 1

    # iterative greedy selection
    while len(selected) < k and remaining:
        best_i = None
        best_score = -1e9

        sel_idx = np.array(selected, dtype=np.int32)

        for i in list(remaining):
            s = sources[i] or ""
            if src_count.get(s, 0) >= max_per_source:
                continue

            # redundancy penalty: similarity to closest already selected item
            max_sim = float(np.max(sim[i, sel_idx])) if len(sel_idx) else 0.0

            c = categories[i] or ""
            l = languages[i] or ""

            pen_s = src_count.get(s, 0)
            pen_c = cat_count.get(c, 0)
            pen_l = lang_count.get(l, 0)

            score = (
                lam * float(rel[i])
                - (1.0 - lam) * max_sim
                - g_s * pen_s
                - g_c * pen_c
                - g_l * pen_l
            )

            if score > best_score:
                best_score = score
                best_i = i

        # Fallback: if hard caps block everything, pick max relevance
        if best_i is None:
            best_i = max(list(remaining), key=lambda j: float(rel[j]))
            best_score = lam * float(rel[best_i])

        selected.append(best_i)
        remaining.remove(best_i)
        selected_scores.append(float(best_score))

        s = sources[best_i] or ""
        c = categories[best_i] or ""
        l = languages[best_i] or ""

        src_count[s] = src_count.get(s, 0) + 1
        cat_count[c] = cat_count.get(c, 0) + 1
        lang_count[l] = lang_count.get(l, 0) + 1

    return selected, selected_scores, lam, max_per_source


# -----------------------------
# Endpoint
# -----------------------------
@app.post("/rerank", response_model=RerankResponse)
def rerank(req: RerankRequest) -> RerankResponse:
    if not req.candidates:
        return RerankResponse(lambda_mmr=0.0, max_per_source=0, items=[])

    feature_rows: List[Dict[str, Any]] = []
    embeddings: List[np.ndarray] = []
    sources: List[str] = []
    categories: List[str] = []
    languages: List[str] = []

    for c in req.candidates:
        # build model features exactly as in training
        feature_rows.append(_build_features_for_model(c))

        # keep embedding for MMR content similarity
        embeddings.append(np.array(c.embedding, dtype=np.float32))

        sources.append(c.source or "")
        categories.append(c.category or "")
        languages.append(c.language or "")

    emb = np.vstack(embeddings)
    rel = _score_with_model(feature_rows)

    selected_idx, mmr_scores, lam, max_per_source = mmr_rerank(
        rel=rel,
        emb=emb,
        sources=sources,
        categories=categories,
        languages=languages,
        k=req.k,
        diversity_level=req.diversity_level,
    )

    items: List[RankedItem] = []
    for rank, (i, ms) in enumerate(zip(selected_idx, mmr_scores), start=1):
        cand = req.candidates[i]
        items.append(
            RankedItem(
                article_id=cand.article_id,
                rank=rank,
                mmr_score=float(ms),
                rel_score=float(rel[i]),
                source=cand.source,
                category=cand.category,
                language=cand.language,
                title=cand.title,
            )
        )

    return RerankResponse(lambda_mmr=float(lam), max_per_source=int(max_per_source), items=items)
