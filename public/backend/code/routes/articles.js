const express = require("express");
const { pool } = require("../db");

const router = express.Router();

// GET /articles?limit=50&category=politics
router.get("/", async (req, res) => {
  const { limit = 50, category, source } = req.query;

  const params = [];
  const conditions = [];

  if (category) {
    params.push(category);
    conditions.push(`category = $${params.length}`);
  }

  if (source) {
    params.push(source);
    conditions.push(`source = $${params.length}`);
  }

  let query = "SELECT * FROM articles";
  if (conditions.length > 0) {
    query += " WHERE " + conditions.join(" AND ");
  }

  params.push(Number(limit));
  query += ` ORDER BY published_at DESC LIMIT $${params.length}`;

  try {
    const { rows } = await pool.query(query, params);
    res.json(rows);
  } catch (err) {
    console.error("Error fetching articles:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// GET /articles/:id
router.get("/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      "SELECT * FROM articles WHERE id = $1",
      [id]
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Article not found" });
    }

    res.json(rows[0]);
  } catch (err) {
    console.error("Error fetching article:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

module.exports = router;
