CREATE OR REPLACE VIEW user_semantic_candidates_balanced AS
WITH ranked_per_category AS (
  SELECT
    u.id AS user_id,
    u.username,
    a.id AS article_id,
    a.title,
    a.source,
    a.category,
    a.published_at,
    (a.embedding <=> u.embedding) AS distance,

    -- Rank μέσα σε κάθε (user, category)
    ROW_NUMBER() OVER (
      PARTITION BY u.id, a.category
      ORDER BY (a.embedding <=> u.embedding) ASC
    ) AS rn_category

  FROM users u
  JOIN articles a ON TRUE
  WHERE a.embedding IS NOT NULL
    AND a.published_at >= NOW() - INTERVAL '7 days'
    AND NOT EXISTS (
      SELECT 1
      FROM interactions i
      WHERE i.user_id = u.id
        AND i.article_id = a.id
    )
),

quota_applied AS (
  SELECT *
  FROM ranked_per_category
  WHERE
    (category = 'Πολιτική'  AND rn_category <= 45) OR
    (category = 'Οικονομία' AND rn_category <= 30) OR
    (category = 'Αθλητικά'  AND rn_category <= 20) OR
    (category = 'Gaming'    AND rn_category <= 5)
),

ranked_per_user AS (
  SELECT
    *,
    -- Τελικό ranking ανά χρήστη (μετά τα quotas)
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY distance ASC
    ) AS rn_user
  FROM quota_applied
)

SELECT
  user_id,
  username,
  article_id,
  title,
  source,
  category,
  published_at,
  distance,
  rn_category,
  rn_user
FROM ranked_per_user
WHERE rn_user <= 100
ORDER BY user_id, rn_user;

SELECT *
FROM user_semantic_candidates_balanced
WHERE user_id = 1;

