CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE articles
ADD COLUMN embedding vector(384);

CREATE INDEX idx_articles_embedding
ON articles
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 10);