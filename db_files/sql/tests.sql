-- How many rows?
SELECT COUNT(*) FROM articles;

-- Peek at data
SELECT id, left(title,80) AS title, left(full_text,80), url, source, category, published_at, updated_at
FROM articles
ORDER BY id DESC
LIMIT 20;

-- Newest by publish time
SELECT id, left(title,80) AS title, published_at, source
FROM articles
ORDER BY published_at DESC NULLS LAST
LIMIT 20;

-- Did the trigger fill search_vector?
SELECT id, left(title,60) AS title, search_vector
FROM articles
WHERE search_vector IS NOT NULL
ORDER BY id DESC
LIMIT 10;

-- Rows that SHOULD have a vector but don't (should return 0 rows)
SELECT id, left(title,60) AS title
FROM articles
WHERE (coalesce(title,'') <> '' OR coalesce(full_text,'') <> '')
  AND search_vector IS NULL;


-- Simple keyword search
SELECT id, left(title,80) AS title,
       ts_rank_cd(search_vector, plainto_tsquery('simple','greece elections')) AS rank
FROM articles
WHERE search_vector @@ plainto_tsquery('simple','greece elections')
ORDER BY rank DESC
LIMIT 20;

-- Phrase-ish query (AND/OR/NOT with to_tsquery syntax)
SELECT id, left(title,80) AS title
FROM articles
WHERE search_vector @@ to_tsquery('simple', 'basketball & (europe|nba) & !transfer');


-- Any duplicate URLs? (should be none > 1)
SELECT url, COUNT(*) AS n
FROM articles
GROUP BY url
HAVING COUNT(*) > 1
ORDER BY n DESC;

-- Did updated_at change on re-scrape?
SELECT id, left(title,80) AS title, updated_at
FROM articles
ORDER BY updated_at DESC
LIMIT 10;


-- By source / category
SELECT source, category, COUNT(*) AS n
FROM articles
GROUP BY source, category
ORDER BY n DESC;

-- By language (if you set it)
SELECT language, COUNT(*) FROM articles GROUP BY language;

-- Date range sanity
SELECT MIN(published_at) AS oldest, MAX(published_at) AS newest FROM articles;

-- Indexes on table 'articles'
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename  = 'articles';


-- How many articles have embeddings?
SELECT 
    COUNT(*) AS total_articles,
    COUNT(embedding) AS with_embedding,
    COUNT(*) - COUNT(embedding) AS without_embedding
FROM articles;

-- Some article embeddings
SELECT 
    id,
    title,
    embedding
FROM articles
WHERE embedding IS NOT NULL
ORDER BY id
LIMIT 5;

delete from articles where published_at is null;
SELECT * FROM interactions;

SELECT username, embedding FROM users;

select *
from articles
where published_at is not null;

