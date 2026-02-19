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



MODEL_PATH = os.getenv("RANK_MODEL_PATH", "model_ranker.joblib")
DEFAULT_K = int(os.getenv("RANK_DEFAULT_K", "50"))


# request/response schemas

class Candidate(BaseModel):
    article_id: int
    title: Optional[str] = None
    source: Optional[str] = None
    category: Optional[str] = None
    language: Optional[str] = None

    # From candidate view
    published_at: Optional[str] = None
    distance: float = None        # cosine distance
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
    explain_relevance: Optional[Dict[str, Any]] = None
    explain_diversity: Optional[Dict[str, Any]] = None

class RerankResponse(BaseModel):
    lambda_mmr: float
    max_per_source: int
    items: List[RankedItem]



# globals loaded at startup

PIPE = None
FEATURE_COLS: Optional[List[str]] = None
PRE = None
CLF = None
TRANSFORM_FEATURE_NAMES = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # load model artifacts once at startup
    global PIPE, FEATURE_COLS, PRE, CLF, TRANSFORM_FEATURE_NAMES
    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Model file not found at: {MODEL_PATH}")

    PIPE = joblib.load(MODEL_PATH)
    PRE = PIPE.named_steps.get("pre")
    CLF = PIPE.named_steps.get("clf")

    if PRE is not None:
    # feature names after preprocessing 
        try:
            TRANSFORM_FEATURE_NAMES = list(PRE.get_feature_names_out())
        except Exception:
            TRANSFORM_FEATURE_NAMES = None

    # if available, enforce model feature columns at inference
    FEATURE_COLS = getattr(PIPE, "feature_names_in_", None)
    if FEATURE_COLS is not None:
        FEATURE_COLS = list(FEATURE_COLS)
    else:
        # fallback to known training features 
        FEATURE_COLS = ["cosine_similarity", "hours_since_publish", "source", "category"]

    yield


app = FastAPI(title="Ranker+MMR Service", lifespan=lifespan)



def _normalize_rows(X: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(X, axis=1, keepdims=True) + 1e-12
    return X / norms


def _mmr_params(div_level: float) -> Tuple[float, float, float, float, int]:
    """
    Map slider (0..1) -> MMR parameters.

    lambda_mmr: accuracy vs diversity tradeoff
      - div=0   -> lambda ~ 0.95 (mostly relevance)
      - div=1   -> lambda ~ 0.55 (more diversity)

    gamma_*: soft penalties for repeated source/category/language
    max_per_source: hard cap in [15..5] as requested
    """
    lam = 0.95 - 0.40 * div_level  # 0.95 -> 0.55

    gamma_source = 0.08 * div_level
    gamma_category = 0.05 * div_level
    gamma_lang = 0.01 * div_level  # more mild

    # hard cap per source: 15 -> 5
    max_per_source = int(round(15 - 10 * div_level))
    max_per_source = max(5, max_per_source)

    return lam, gamma_source, gamma_category, gamma_lang, max_per_source


def _build_features_for_model(c: Candidate) -> Dict[str, Any]:
    """
    Build feature vector used at training time:
      - cosine_similarity
      - hours_since_publish
      - source
      - category
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
) -> Tuple[List[int], List[float], List[Dict[str, Any]], float, int]:
    """
    Multi-objective MMR with explicit explanation decomposition.

      score = λ*rel - (1-λ)*maxSimToSelected- γs*count(source) - γc*count(category) - γl*count(lang)

    plus hard cap on max items per source.

    Returns:
      selected_idx: indices selected in order
      selected_scores: MMR scores per selected item
      selected_debug: per-rank dict explaining why it was selected
      lambda_mmr: λ
      max_per_source: hard cap
    """
    n = int(len(rel))
    k = min(int(k), n)

    lam, g_s, g_c, g_l, max_per_source = _mmr_params(float(diversity_level))

    # cosine sim matrix NxN 
    emb_n = _normalize_rows(emb)
    sim = emb_n @ emb_n.T

    selected: List[int] = []
    selected_scores: List[float] = []
    selected_debug: List[Dict[str, Any]] = []

    src_count: Dict[str, int] = {}
    cat_count: Dict[str, int] = {}
    lang_count: Dict[str, int] = {}

    remaining = set(range(n))

    # 1st pick: highest relevance 
    first = int(np.argmax(rel))
    selected.append(first)
    remaining.remove(first)

    s0 = sources[first] or ""
    c0 = categories[first] or ""
    l0 = languages[first] or ""

    # For first item: no redundancy yet and no repetition penalties
    max_sim0 = 0.0
    pen_s0 = 0
    pen_c0 = 0
    pen_l0 = 0

    score0 = (
        lam * float(rel[first])
        - (1.0 - lam) * max_sim0
        - g_s * pen_s0
        - g_c * pen_c0
        - g_l * pen_l0
    )
    selected_scores.append(float(score0))

    selected_debug.append(
        {
            "lambda": float(lam),
            "rel": float(rel[first]),
            "max_sim_to_selected": float(max_sim0),
            "pen_source_count": int(pen_s0),
            "pen_category_count": int(pen_c0),
            "pen_language_count": int(pen_l0),
            "gamma_source": float(g_s),
            "gamma_category": float(g_c),
            "gamma_language": float(g_l),
            "hard_cap_max_per_source": int(max_per_source),
            "hard_cap_blocked": False,
            "message": "Επιλέχθηκε ως το πιο σχετικό άρθρο.",
        }
    )

    src_count[s0] = src_count.get(s0, 0) + 1
    cat_count[c0] = cat_count.get(c0, 0) + 1
    lang_count[l0] = lang_count.get(l0, 0) + 1

    # iterative greedy selection 
    while len(selected) < k and remaining:
        best_i = None
        best_score = -1e18
        best_dbg: Optional[Dict[str, Any]] = None

        sel_idx = np.array(selected, dtype=np.int32)

        # track if hard cap blocks candidates from their source
        blocked_any = False

        for i in list(remaining):
            s = sources[i] or ""
            if src_count.get(s, 0) >= max_per_source:
                blocked_any = True
                continue

            # redundancy penalty: similarity to closest already selected item
            max_sim = float(np.max(sim[i, sel_idx])) if len(sel_idx) else 0.0

            c = categories[i] or ""
            l = languages[i] or ""

            pen_s = int(src_count.get(s, 0))
            pen_c = int(cat_count.get(c, 0))
            pen_l = int(lang_count.get(l, 0))

            rel_i = float(rel[i])

            score_rel = lam * rel_i
            score_red = (1.0 - lam) * max_sim
            score_pen = (g_s * pen_s) + (g_c * pen_c) + (g_l * pen_l)

            score = score_rel - score_red - score_pen

            if score > best_score:
                best_score = score
                best_i = i

                # if redundancy dominates -> "diversity"
                # else -> "relevance"
                try:
                    rel_vals = np.array([float(rel[j]) for j in remaining], dtype=float)
                    rel_vals.sort()
                    # rank position of rel_i
                    rel_rank = int(np.searchsorted(rel_vals, rel_i, side="right"))
                    rel_pct = rel_rank / max(1, len(rel_vals))  # percentile
                except Exception:
                    rel_pct = 0.5

                # heuristics to explain the main driver of selection
                LOW_REL_PCT = 0.35
                HIGH_REL_PCT = 0.70
                LOW_REDUNDANCY = 0.55  # lower max_sim => more novelty

                # decide main driver
                if rel_pct >= HIGH_REL_PCT and score_rel >= (score_red + score_pen):
                    msg = "Επιλέχθηκε κυρίως λόγω υψηλής σχετικότητας με τα ενδιαφέροντά σας."
                elif rel_pct <= LOW_REL_PCT and max_sim <= LOW_REDUNDANCY:
                    msg = "Χαμηλή σχετικότητα, αλλά επιλέχθηκε για να αυξήσει την ποικιλία και να μειώσει την επανάληψη."
                elif score_pen > score_rel and score_pen > score_red:
                    msg = "Επιλέχθηκε με στόχο την ποικιλία (λήφθηκαν υπόψη ποινές σε πηγή/κατηγορία/γλώσσα)."
                elif score_red >= score_rel:
                    msg = "Επιλέχθηκε κυρίως για να μειώσει την επανάληψη στη λίστα (ποικιλία/novelty)."
                else:
                    msg = "Επιλέχθηκε ως ισορροπία μεταξύ σχετικότητας και ποικιλίας."


                best_dbg = {
                    "lambda": float(lam),
                    "lambda_user": float(diversity_level),
                    "rel": float(rel_i),
                    "max_sim_to_selected": float(max_sim),
                    "pen_source_count": int(pen_s),
                    "pen_category_count": int(pen_c),
                    "pen_language_count": int(pen_l),
                    "gamma_source": float(g_s),
                    "gamma_category": float(g_c),
                    "gamma_language": float(g_l),
                    "hard_cap_max_per_source": int(max_per_source),
                    "hard_cap_blocked": False,
                    "mmr_components": {
                        "lambda_rel": float(score_rel),
                        "(1-lambda)_redundancy": float(score_red),
                        "penalties_total": float(score_pen),
                    },
                    "message": msg,
                }

        # Fallback: if hard caps block everything, pick max relevance among remaining
        if best_i is None:
            best_i = max(list(remaining), key=lambda j: float(rel[j]))
            best_score = lam * float(rel[best_i])

            s = sources[best_i] or ""
            c = categories[best_i] or ""
            l = languages[best_i] or ""

            best_dbg = {
                "lambda": float(lam),
                "rel": float(rel[best_i]),
                "max_sim_to_selected": 0.0,
                "pen_source_count": int(src_count.get(s, 0)),
                "pen_category_count": int(cat_count.get(c, 0)),
                "pen_language_count": int(lang_count.get(l, 0)),
                "gamma_source": float(g_s),
                "gamma_category": float(g_c),
                "gamma_language": float(g_l),
                "hard_cap_max_per_source": int(max_per_source),
                "hard_cap_blocked": True,
                "message": "Hard cap blocked remaining sources; fell back to max relevance among remaining.",
            }

        selected.append(best_i)
        remaining.remove(best_i)
        selected_scores.append(float(best_score))
        selected_debug.append(best_dbg or {"message": "No debug info."})

        s = sources[best_i] or ""
        c = categories[best_i] or ""
        l = languages[best_i] or ""

        src_count[s] = src_count.get(s, 0) + 1
        cat_count[c] = cat_count.get(c, 0) + 1
        lang_count[l] = lang_count.get(l, 0) + 1

    return selected, selected_scores, selected_debug, float(lam), int(max_per_source)



def explain_relevance(feature_row: Dict[str, Any], age_seconds: Optional[float] = None, top_k: int = 3) -> Dict[str, Any]:
    """
    Returns per-item LR explanation based on log-odds contributions.
    """
    if PIPE is None or PRE is None or CLF is None:
        return {"reasons": [], "note": "explainability_not_available"}

    df = pd.DataFrame([feature_row])
    X = df[FEATURE_COLS]  # same order as scoring

    # transform -> 1 x D
    Xt = PRE.transform(X)

    # ensure dense for easy elementwise math
    if hasattr(Xt, "toarray"):
        Xt = Xt.toarray()

    x = Xt[0]  # shape (D,)
    coef = CLF.coef_[0]   # shape (D,)
    intercept = float(CLF.intercept_[0])

    contrib = x * coef  # log-odds contributions per transformed feature

    names = TRANSFORM_FEATURE_NAMES
    if not names or len(names) != len(contrib):
        # fallback: index-based
        names = [f"f{i}" for i in range(len(contrib))]

    # pick top positive and optionally top negative
    idx_sorted_pos = np.argsort(contrib)[::-1]
    top_pos = [i for i in idx_sorted_pos if contrib[i] > 0][:top_k]

    idx_sorted_neg = np.argsort(contrib)
    raw_neg = [i for i in idx_sorted_neg if contrib[i] < 0]

    OLD_THRESHOLD_SEC = 3 * 24 * 3600  # 3 days
    is_old = (age_seconds is not None) and (float(age_seconds) > OLD_THRESHOLD_SEC)    

    def _humanize_pos(fname: str) -> str:
        if "cosine_similarity" in fname:
            return f"Υψηλή σημασιολογική ομοιότητα με το προφίλ σας"
        if "hours_since_publish" in fname:
            return f"Πρόσφατο άρθρο"
        if "cat__source_" in fname:
            s = fname.split("cat__source_", 1)[1]
            return f"Προτίμηση πηγής: {s}"
        if "cat__category_" in fname:
            c = fname.split("cat__category_", 1)[1]
            return f"Προτίμηση κατηγορίας: {c}"
        return fname
    
    def _humanize_neg(fname: str) -> Optional[str]:
        if "cosine_similarity" in fname:
            return "Χαμηλή σημασιολογική ομοιότητα με το προφίλ σας"
        if "hours_since_publish" in fname:
            return "Παλιό άρθρο" if is_old else None  
        if "cat__source_" in fname:
            s = fname.split("cat__source_", 1)[1]
            return f"Μειωμένο ενδιαφέρον για την πηγή: {s}"
        if "cat__category_" in fname:
            c = fname.split("cat__category_", 1)[1]
            return f"Μειωμένο ενδιαφέρον για την κατηγορία: {c}"
        return fname

    reasons_pos = [
        {"feature": names[i], "contribution": float(contrib[i]), "text": _humanize_pos(names[i])}
        for i in top_pos
    ]
    reasons_neg = []
    for i in raw_neg:
        text = _humanize_neg(names[i])
        if text is None:
            continue
        reasons_neg.append({"feature": names[i], "contribution": float(contrib[i]), "text": text})
        if len(reasons_neg) >= min(2, top_k):
            break

    return {
        "intercept": intercept,
        "top_positive": reasons_pos,
        "top_negative": reasons_neg,
        "age_seconds": None if age_seconds is None else float(age_seconds),
        "old_threshold_seconds": float(OLD_THRESHOLD_SEC),
    }



# Endpoint

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

    selected_idx, mmr_scores, selected_debug, lam, max_per_source = mmr_rerank(
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
                explain_relevance=explain_relevance(feature_rows[i], age_seconds=req.candidates[i].age_seconds, top_k=3),
                explain_diversity=selected_debug[rank-1],
            )
        )

    return RerankResponse(lambda_mmr=float(lam), max_per_source=int(max_per_source), items=items)
