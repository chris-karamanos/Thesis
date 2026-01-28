CREATE OR REPLACE VIEW training_explicit_aligned AS
SELECT
  it.user_id,
  it.article_id,

  -- group for ranking metrics (feed list)
  i.request_id AS request_id,

  -- timestamps
  i.shown_at,
  it.interaction_time,

  -- labels/weights
  CASE
    WHEN it.interaction_type = 'like' THEN 1
    WHEN it.interaction_type = 'dislike' THEN 0
  END AS label,
  1.0 AS weight,

  -- features
  (1 - (a.embedding <=> u.embedding)) AS cosine_similarity,
  a.source,
  a.category,
  a.published_at,
  EXTRACT(EPOCH FROM (i.shown_at - a.published_at)) / 3600.0 AS hours_since_publish

FROM interactions it
JOIN LATERAL (
  SELECT i2.*
  FROM impressions i2
  WHERE i2.user_id = it.user_id
    AND i2.article_id = it.article_id
    AND i2.shown_at <= it.interaction_time
  ORDER BY i2.shown_at DESC
  LIMIT 1
) i ON TRUE
JOIN articles a ON a.id = it.article_id
JOIN users u    ON u.id = it.user_id
WHERE it.user_id = 1
  AND it.interaction_type IN ('like', 'dislike')
  AND a.embedding IS NOT NULL
  AND u.embedding IS NOT NULL;



CREATE OR REPLACE VIEW training_implicit_aligned AS
SELECT
  i.user_id,
  i.article_id,
  i.request_id AS request_id,
  i.shown_at,
  NULL::timestamptz AS interaction_time,

  0 AS label,
  0.2 AS weight,

  (1 - (a.embedding <=> u.embedding)) AS cosine_similarity,
  a.source,
  a.category,
  a.published_at,
  EXTRACT(EPOCH FROM (i.shown_at - a.published_at)) / 3600.0 AS hours_since_publish

FROM impressions i
JOIN articles a ON a.id = i.article_id
JOIN users u    ON u.id = i.user_id
WHERE i.user_id = 1
  AND a.embedding IS NOT NULL
  AND u.embedding IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM interactions it
    WHERE it.user_id = i.user_id
      AND it.article_id = i.article_id
      AND it.interaction_type IN ('like', 'dislike')
      AND it.interaction_time >= i.shown_at
  );


CREATE OR REPLACE VIEW training_implicit_sampled AS
SELECT *
FROM training_implicit_aligned
ORDER BY random()
LIMIT (
  SELECT COUNT(*) FROM training_explicit_aligned
);


CREATE OR REPLACE VIEW training_dataset AS
SELECT * FROM training_explicit_aligned
UNION ALL
SELECT * FROM training_implicit_sampled;


SELECT
  COUNT(*) AS rows,
  SUM(label) AS positives,
  SUM(1 - label) AS negatives,
  AVG(weight) AS avg_weight
FROM training_dataset;

SELECT label, weight, COUNT(*)
FROM training_dataset
GROUP BY label, weight
ORDER BY label DESC, weight DESC;

SELECT viewname
FROM pg_views
WHERE schemaname = 'public'
  AND viewname LIKE 'training_%'
ORDER BY viewname;


CREATE OR REPLACE VIEW training_split_cutoff AS
SELECT
  to_timestamp(
    percentile_cont(0.8)
    WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM shown_at))
  ) AT TIME ZONE 'UTC' AS cutoff_shown_at
FROM training_dataset;


CREATE OR REPLACE VIEW training_dataset_train AS
SELECT td.*
FROM training_dataset td
CROSS JOIN training_split_cutoff c
WHERE td.shown_at < c.cutoff_shown_at;


CREATE OR REPLACE VIEW training_dataset_val AS
SELECT td.*
FROM training_dataset td
CROSS JOIN training_split_cutoff c
WHERE td.shown_at >= c.cutoff_shown_at;


SELECT 'train' AS split, COUNT(*) AS n, SUM(label) AS pos, SUM(1-label) AS neg
FROM training_dataset_train
UNION ALL
SELECT 'val' AS split, COUNT(*) AS n, SUM(label) AS pos, SUM(1-label) AS neg
FROM training_dataset_val;

SELECT * FROM training_split_cutoff;

SELECT
  DATE(shown_at) AS day,
  COUNT(DISTINCT request_id) AS sessions,
  COUNT(*) AS rows
FROM training_dataset
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW training_dataset_day AS
SELECT
  td.*,
  (td.shown_at AT TIME ZONE 'UTC')::date AS shown_day
FROM training_dataset td;