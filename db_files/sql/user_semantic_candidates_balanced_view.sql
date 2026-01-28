CREATE VIEW user_semantic_candidates_balanced AS
WITH base AS (
  SELECT
    u.id AS user_id,
    u.username,
    a.id AS article_id,
    a.title,
    a.source,
    a.url,
    a.category,
    a.published_at,
    (a.embedding <=> u.embedding) AS distance
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

-- 1) Forced candidates (μόνο από τις 3 πηγές)
forced_ranked AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.user_id
      ORDER BY b.distance ASC, b.published_at DESC, b.article_id ASC
    ) AS rn_forced
  FROM base b
  WHERE b.source IN ('bbc.com', 'bleacherreport.com', 'goal.com')
),

forced_picked AS (
  SELECT *
  FROM forced_ranked
  WHERE rn_forced <= 10
),

-- 2) Main candidates (όλα εκτός forced sources) με quotas ανά κατηγορία
ranked_per_category AS (
  SELECT
    b.*,

    ROW_NUMBER() OVER (
      PARTITION BY b.user_id, b.category
      ORDER BY b.distance ASC, b.published_at DESC, b.article_id ASC
    ) AS rn_category
  FROM base b
  WHERE b.source NOT IN ('bbc.com', 'bleacherreport.com', 'goal.com')
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

ranked_main AS (
  SELECT
    qa.*,
    ROW_NUMBER() OVER (
      PARTITION BY qa.user_id
      ORDER BY qa.distance ASC, qa.published_at DESC, qa.article_id ASC
    ) AS rn_main
  FROM quota_applied qa
),

main_picked AS (
  SELECT *
  FROM ranked_main
  WHERE rn_main <= 90
),

-- 3) Συνένωση: πρώτα τα 90 main, μετά τα 10 forced (ως rn_user 91..100)
combined AS (
  SELECT
    user_id,
    username,
    article_id,
    title,
    source,
    url,
    category,
    published_at,
    distance,
    rn_category,
    rn_main AS rn_user
  FROM main_picked

  UNION ALL

  SELECT
    user_id,
    username,
    article_id,
    title,
    source,
    url,
    category,
    published_at,
    distance,
    NULL::int AS rn_category,
    90 + rn_forced AS rn_user
  FROM forced_picked
)

SELECT *
FROM combined
WHERE rn_user <= 100
ORDER BY user_id, rn_user;



DROP VIEW IF EXISTS user_semantic_candidates_balanced;

SELECT *
FROM user_semantic_candidates_balanced
WHERE user_id = 1;


