CREATE VIEW user_semantic_candidates AS
SELECT
    u.id          AS user_id,
    u.username    AS username,
    a.id          AS article_id,
    a.title       AS title,
    a.source      AS source,
    a.category    AS category,
    a.published_at,
    a.embedding,
    a.embedding <=> u.embedding AS distance
FROM users AS u
JOIN LATERAL (
    SELECT *
    FROM articles AS a
    WHERE a.embedding IS NOT NULL
      -- άρθρα που ο χρήστης ΔΕΝ έχει ήδη δει
      AND NOT EXISTS (
          SELECT 1
          FROM interactions i
          WHERE i.user_id = u.id
            AND i.article_id = a.id
      )
      -- προαιρετικά: μόνο πρόσφατα άρθρα
      AND a.published_at >= NOW() - INTERVAL '7 days'
    ORDER BY a.embedding <=> u.embedding   -- μικρότερη απόσταση = πιο σχετικό
    LIMIT 50                              -- top-200 candidates ανά χρήστη
) AS a ON TRUE;



SELECT *
FROM user_semantic_candidates
WHERE user_id = 1
