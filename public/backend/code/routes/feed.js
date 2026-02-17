const express = require("express");
const { pool } = require("../db");

const router = express.Router();

const ALLOWED = new Set(["click", "like", "share", "dislike"]);

const RANKING_URL = process.env.RANKING_SERVICE_URL || "http://ranking_service:8008/rerank";
const FEED_K = Number(process.env.FEED_K || 50);
const CANDIDATE_LIMIT = Number(process.env.CANDIDATE_LIMIT || 200);


function parsePgvectorToArray(v) {
  if (Array.isArray(v)) return v.map(Number);

  if (typeof v !== "string") return null;

  const s = v.trim();

  // remove surrounding [ ]
  const inner = s.startsWith("[") && s.endsWith("]") ? s.slice(1, -1) : s;

  // split by comma and convert to numbers
  const arr = inner
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x.length > 0)
    .map((x) => Number(x));

  // validate
  if (!arr.length || arr.some((n) => !Number.isFinite(n))) return null;
  return arr;
}


function clamp01(x) {
  const v = Number(x);
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, v));
}

function quantize01_step01(x) {
  return Math.round(clamp01(x) * 10) / 10; 
}

async function genRequestId() {
  const ridRes = await pool.query("SELECT gen_random_uuid() AS request_id;");
  return ridRes.rows[0].request_id;
}

async function hasUserEmbedding(userId) {
  const r = await pool.query("SELECT embedding IS NOT NULL AS ok FROM users WHERE id=$1;", [userId]);
  return Boolean(r.rows[0]?.ok);
}

async function logImpressions(userId, requestId, items, modelVersion) {
  if (!items || items.length === 0) return;

  const articleIds = items.map((r) => Number(r.article_id));
  const positions = items.map((_, idx) => idx + 1);

  await pool.query(
    `
    INSERT INTO impressions (user_id, article_id, shown_at, rank_position, request_id, model_version)
    SELECT
      $1::bigint AS user_id,
      x.article_id::bigint,
      NOW() AS shown_at,
      x.rank_position::int,
      $2::uuid AS request_id,
      $3::text AS model_version
    FROM (
      SELECT
        UNNEST($4::bigint[]) AS article_id,
        UNNEST($5::int[])    AS rank_position
    ) x;
    `,
    [userId, requestId, modelVersion, articleIds, positions]
  );
}

// cold-start feed
const COLD_CATS = ["Πολιτική", "Οικονομία", "Αθλητικά", "Gaming"];
const COLD_PER_CAT = 12;

async function fetchColdStart() {
  const r = await pool.query(
    `
    WITH base AS (
      SELECT
        a.id AS article_id,
        a.title, a.url, a.source, a.category, a.language,
        a.published_at, a.summary, a.image_url,
        ROW_NUMBER() OVER (
          PARTITION BY a.category
          ORDER BY a.published_at DESC
        ) AS rn
      FROM articles a
      WHERE a.published_at >= NOW() - INTERVAL '7 days'
        AND a.category = ANY($1::text[])
    )
    SELECT *
    FROM base
    WHERE rn <= $2
    ORDER BY published_at DESC;
    `,
    [COLD_CATS, COLD_PER_CAT]
  );

  return r.rows;
}

// pull top-N candidates from final_user_candidate_list view
async function fetchCandidates(userId, limit) {
  const r = await pool.query(
    `
    SELECT
      article_id,
      title,
      source,
      category,
      url,
      published_at,
      language,
      embedding,
      distance,
      age_seconds,
      rn_final
    FROM final_user_candidate_list
    WHERE user_id = $1
    ORDER BY rn_final
    LIMIT $2;
    `,
    [userId, limit]
  );
  return r.rows;
}

// fetch full render fields for a set of ids from articles.
 
async function hydrateArticlesByIds(articleIds) {
  if (!articleIds.length) return new Map();

  const r = await pool.query(
    `
    SELECT
      id AS article_id,
      title, url, source, category, language,
      published_at, summary, image_url
    FROM articles
    WHERE id = ANY($1::bigint[]);
    `,
    [articleIds]
  );

  return new Map(r.rows.map((x) => [Number(x.article_id), x]));
}


// GET /feed
router.get("/", async (req, res) => {
  
  const userId = Number(req.query.user_id);
  const k = Number(req.query.k || FEED_K);
  const diversityLevel = quantize01_step01(req.query.diversity_level ?? 0.5);
    

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ error: "Invalid user_id" });
  }
  if (!Number.isInteger(k) || k <= 0 || k > 200) {
    return res.status(400).json({ error: "Invalid k (1..200)" });
  }

  try {
    const requestId = await genRequestId();

    // cold-start if no embedding
    const ok = await hasUserEmbedding(userId);
    if (!ok) {
      const items = await fetchColdStart();
      await logImpressions(userId, requestId, items, "coldstart_balanced_v1");
      return res.json({ request_id: requestId, items, debug: { mode: "coldstart"} });
    }

    // candidates (top-200 from view)
    const candidates = await fetchCandidates(userId, CANDIDATE_LIMIT);

    if (!candidates.length) {
      // fallback
      const items = await fetchColdStart(k);
      await logImpressions(userId, requestId, items, "fallback_no_candidates_v1");
      return res.json({ request_id: requestId, items, debug: { mode: "fallback_no_candidates", diversity_level: diversityLevel } });
    }

    // call ranking_service (ML scoring + MMR)
    const payload = {
      diversity_level: diversityLevel,
      k,
      candidates: candidates.map((c) => ({
        article_id: c.article_id,
        title: c.title,
        source: c.source,
        category: c.category,
        language: c.language,
        published_at: c.published_at,
        distance: c.distance,
        age_seconds: c.age_seconds,
        embedding: parsePgvectorToArray(c.embedding),
      })),
    };

    const rr = await fetch(RANKING_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!rr.ok) {
      const t = await rr.text();
      throw new Error(`ranking_service failed: ${rr.status} ${t}`);
    }

    const out = await rr.json();
    const rankedItems = out.items || [];
    const rankedIds = rankedItems.map((x) => Number(x.article_id));

    const rankMetaById = new Map(
      rankedItems.map((x) => [Number(x.article_id), x])
    );

    // hydrate full render fields from DB
    const byId = await hydrateArticlesByIds(rankedIds);

    // merge: hydrated fields + rank meta
    const items = rankedIds
      .map((id) => {
        const base = byId.get(id);
        const meta = rankMetaById.get(id);
        if (!base) return null;
        return { ...base, ...meta }; // meta overwrites/adds fields
      })
      .filter(Boolean);


    // log impressions with model version that encodes diversity
    const modelVersion = `ml_mmr_v1_div${diversityLevel.toFixed(1)}`;
    await logImpressions(userId, requestId, items, modelVersion);

    return res.json({
      request_id: requestId,
      items,
      debug: {
        mode: "personalized_mmr",
        diversity_level: diversityLevel,
        lambda_mmr: out.lambda_mmr,
        max_per_source: out.max_per_source,
        candidates_in: candidates.length,
      },
    });
  } catch (err) {
    console.error("Error in GET /feed:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});


// POST /feed/interact

router.post("/interact", async (req, res) => {
  const { user_id, request_id, article_id, interaction_type} = req.body;

  const userId = Number(user_id);
  const articleId = Number(article_id);

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ error: "Invalid user_id" });
  }
  if (!Number.isInteger(articleId) || articleId <= 0) {
    return res.status(400).json({ error: "Invalid article_id" });
  }
  if (typeof request_id !== "string" || request_id.length < 10) {
    return res.status(400).json({ error: "Invalid request_id" });
  }
  if (!ALLOWED.has(interaction_type)) {
    return res.status(400).json({ error: "Invalid interaction_type" });
  }

  try {
    // integrity check: ensure impression exists
    const chk = await pool.query(
      `
      SELECT 1
      FROM impressions
      WHERE user_id = $1
        AND request_id = $2::uuid
        AND article_id = $3
      LIMIT 1;
      `,
      [userId, request_id, articleId]
    );

    if (chk.rows.length === 0) {
      return res.status(409).json({
        error: "No matching impression for this interaction (user_id, request_id, article_id).",
      });
    }

    await pool.query(
      `
      INSERT INTO interactions (user_id, article_id, request_id, interaction_type, interaction_time)
      VALUES ($1, $2, $3::uuid, $4, NOW());
      `,
      [userId, articleId, request_id, interaction_type?? null]
    );

    return res.json({ status: "ok" });
  } catch (err) {
    console.error("Error in POST /feed/interact:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

module.exports = router;
