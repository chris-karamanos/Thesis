CREATE OR REPLACE VIEW final_user_candidate_list AS
WITH per_user AS (
  SELECT
    u.id       AS user_id,
    u.username AS username,
    a.id       AS article_id,
    a.title,
    a.source,
    a.category,
    a.url,
    a.published_at,
    a.embedding,
    (a.embedding <=> u.embedding) AS distance,
    EXTRACT(EPOCH FROM (NOW() - a.published_at)) AS age_seconds
  FROM users u
  JOIN LATERAL (
    SELECT a.*
    FROM articles a
    WHERE u.embedding IS NOT NULL
      AND a.embedding IS NOT NULL
      AND a.published_at >= NOW() - INTERVAL '7 days'
      AND NOT EXISTS (
        SELECT 1
        FROM interactions i
        WHERE i.user_id = u.id
          AND i.article_id = a.id
      )
    ORDER BY (a.embedding <=> u.embedding) ASC
    LIMIT 1200
  ) a ON TRUE
),
ranked AS (
  SELECT
    p.*,
    ROW_NUMBER() OVER (PARTITION BY p.user_id, p.source   ORDER BY p.distance ASC)      AS rn_source,
    ROW_NUMBER() OVER (PARTITION BY p.user_id, p.category ORDER BY p.distance ASC)      AS rn_category,
    ROW_NUMBER() OVER (PARTITION BY p.user_id ORDER BY p.distance ASC)                  AS rank_by_distance,
    ROW_NUMBER() OVER (PARTITION BY p.user_id ORDER BY p.published_at DESC)             AS rank_by_recency
  FROM per_user p
),
capped AS (
  SELECT *
  FROM ranked
  WHERE rn_source <= 20
    AND (
      category IS NULL
      OR rn_category <= 120
    )
),
final_ranked AS (
  SELECT
    c.*,
    (
      c.rank_by_distance
      + (0.35 * c.rank_by_recency)
    ) AS final_rank
  FROM capped c
)
SELECT *
FROM (
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY f.user_id ORDER BY f.final_rank ASC) AS rn_final
  FROM final_ranked f
) t
WHERE rn_final <= 200
ORDER BY user_id, rn_final;



drop view if exists final_user_candidate_list;

SELECT
  user_id, username, rn_final, source, category, published_at, distance, final_rank, title
FROM final_user_candidate_list
WHERE user_id = 1
ORDER BY rn_final
LIMIT 50;


SELECT
  source,
  COUNT(*) AS n
FROM final_user_candidate_list
WHERE user_id = 1
GROUP BY source
ORDER BY n DESC, source;


SELECT category, COUNT(*) AS n
FROM final_user_candidate_list
GROUP BY category
ORDER BY n DESC;




WITH buckets AS (
  SELECT
    CASE
      WHEN published_at >= NOW() - INTERVAL '1 day'  THEN '0-1d'
      WHEN published_at >= NOW() - INTERVAL '2 days' THEN '1-2d'
      WHEN published_at >= NOW() - INTERVAL '3 days' THEN '2-3d'
      WHEN published_at >= NOW() - INTERVAL '5 days' THEN '3-5d'
      ELSE '5-7d'
    END AS bucket,
    COUNT(*) AS n
  FROM final_user_candidate_list
  WHERE user_id = 1
  GROUP BY 1
)
SELECT bucket, n
FROM buckets
ORDER BY
  CASE bucket
    WHEN '0-1d' THEN 1
    WHEN '1-2d' THEN 2
    WHEN '2-3d' THEN 3
    WHEN '3-5d' THEN 4
    ELSE 5
  END;



SELECT
  COUNT(*)                    AS n,
  MIN(distance)               AS min_dist,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY distance) AS p25,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY distance) AS p50,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY distance) AS p75,
  MAX(distance)               AS max_dist
FROM final_user_candidate_list
WHERE user_id = 1;



