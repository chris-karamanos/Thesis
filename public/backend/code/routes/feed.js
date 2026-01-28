const express = require("express");
const { pool } = require("../db");

const router = express.Router();

const ALLOWED = new Set(["click", "like", "share", "dislike"]);


router.get("/", async (req, res) => {
  const userId = Number(req.query.user_id);
  const k = Number(req.query.k || 600); 

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ error: "Invalid user_id" });
  }

  try {
    // 1) Generate a request_id in DB (ensures UUID validity, no extra deps)
    const ridRes = await pool.query("SELECT gen_random_uuid() AS request_id;");
    const requestId = ridRes.rows[0].request_id;

    // 2) Fetch candidates from your view
    const candidatesRes = await pool.query(
      `
        SELECT
          c.article_id,
          c.title,
          c.source,
          c.url,
          c.category,
          c.published_at,
          c.distance,
          a.image_url
        FROM user_semantic_candidates_balanced c
        JOIN articles a ON a.id = c.article_id
        WHERE c.user_id = $1
        ORDER BY c.distance ASC
        LIMIT $2;
      `,
      [userId, k]
    );

    const items = candidatesRes.rows;

    // 3) Log impressions (K rows) with rank_position
    // We do this in ONE query using UNNEST arrays.
    // If items empty, just return.
    if (items.length > 0) {
      const articleIds = items.map((r) => r.article_id);
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
        [userId, requestId, "bootstrap_semantic_v1", articleIds, positions]
      );
    }

    return res.json({ request_id: requestId, items, debug: {
    feed_js: "v_image_url_join_2026-01-23",
    first_row_keys: Object.keys(candidatesRes.rows[0] || {}),
    first_row_image_url: candidatesRes.rows[0]?.image_url ?? null
  } });
  } catch (err) {
    console.error("Error in GET /feed:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});


router.post("/interact", async (req, res) => {
  const { user_id, request_id, article_id, interaction_type, dwell_ms } = req.body;

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
    // Optional integrity check: ensure there is an impression for this (user, request, article)
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
      INSERT INTO interactions (user_id, article_id, request_id, interaction_type, interaction_time, dwell_ms)
      VALUES ($1, $2, $3::uuid, $4, NOW(), $5);
      `,
      [userId, articleId, request_id, interaction_type, dwell_ms ?? null]
    );

    return res.json({ status: "ok" });
  } catch (err) {
    console.error("Error in POST /feed/interact:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

module.exports = router;
 