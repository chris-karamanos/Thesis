DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'impression_surface') THEN
    CREATE TYPE impression_surface AS ENUM ('feed', 'search', 'related', 'notification');
  END IF;
END $$;


-- Impressions Table
CREATE TABLE IF NOT EXISTS impressions (
  id            BIGSERIAL PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  article_id    BIGINT NOT NULL,
  shown_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  rank_position INTEGER NOT NULL,              -- 1..K (position in the list)
  surface       impression_surface NOT NULL DEFAULT 'feed',
  request_id    UUID NOT NULL DEFAULT gen_random_uuid(), -- groups impressions of a single "feed render"
  session_id    UUID NULL,                      -- group multiple requests into a session
  model_version TEXT NOT NULL, 

  -- Foreign keys 
  CONSTRAINT fk_impr_user
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,

  CONSTRAINT fk_impr_article
    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,

  CONSTRAINT chk_rank_position_positive CHECK (rank_position >= 1)
);


-- 3) Indices (critical for training queries & evaluation)

-- Efficient lookup of impressions per user in time order
CREATE INDEX IF NOT EXISTS idx_impr_user_time
  ON impressions (user_id, shown_at DESC);

-- Efficient join impressions -> interactions for a user
CREATE INDEX IF NOT EXISTS idx_impr_user_article_time
  ON impressions (user_id, article_id, shown_at DESC);

-- Efficient retrieval by request (all items shown in a single feed render)
CREATE INDEX IF NOT EXISTS idx_impr_request
  ON impressions (request_id);

-- Efficient filtering by article (useful for debugging)
CREATE INDEX IF NOT EXISTS idx_impr_article_time
  ON impressions (article_id, shown_at DESC);

-- Uniqueness to avoid accidental duplicates per request(prevents inserting the same article twice in the same feed render)
CREATE UNIQUE INDEX IF NOT EXISTS uq_impr_request_article
  ON impressions (request_id, article_id);


CREATE UNIQUE INDEX IF NOT EXISTS uq_impr_user_request_article
ON impressions (user_id, request_id, article_id);


