DROP TABLE IF EXISTS articles CASCADE;
CREATE TABLE articles (
    id            BIGSERIAL PRIMARY KEY,
    title         TEXT NOT NULL,
    url           TEXT NOT NULL UNIQUE,        
    content       TEXT,
    source        TEXT NOT NULL,               
    category      TEXT,                        
    published_at  TIMESTAMPTZ,                 -- timestamps basei utc 
    language      TEXT,                        
    scraped_at    TIMESTAMPTZ DEFAULT NOW(),   -- otan to eide o scraper
    updated_at    TIMESTAMPTZ DEFAULT NOW(),   -- otan kaname update
    search_vector tsvector                     -- gia psaksimo se keimeno
);

-- Keep search_vector in sync (title + content)
CREATE OR REPLACE FUNCTION articles_tsvector_trigger() RETURNS trigger AS $$
BEGIN
	IF TG_OP = 'INSERT'
	     OR NEW.title   IS DISTINCT FROM OLD.title
	     OR NEW.content IS DISTINCT FROM OLD.content
	THEN
		  NEW.search_vector :=
		    setweight(to_tsvector('simple', coalesce(NEW.title,'')), 'A') ||
		    setweight(to_tsvector('simple', coalesce(NEW.content,'')), 'B');
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsv_update BEFORE INSERT OR UPDATE
ON articles FOR EACH ROW EXECUTE FUNCTION articles_tsvector_trigger();

-- Full-text index
CREATE INDEX idx_articles_fts ON articles USING GIN (search_vector);
-- Useful filters
CREATE INDEX idx_articles_published_at ON articles (published_at DESC);
CREATE INDEX idx_articles_source ON articles (source);
CREATE INDEX idx_articles_category ON articles (category);

-- USERS
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    id          BIGSERIAL PRIMARY KEY,
    username    TEXT UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    preferences JSONB
);

-- INTERACTIONS
DROP TABLE IF EXISTS interactions CASCADE;
CREATE TABLE interactions (
    id               BIGSERIAL PRIMARY KEY,
    user_id          BIGINT REFERENCES users(id) ON DELETE CASCADE,
    article_id       BIGINT REFERENCES articles(id) ON DELETE CASCADE,
    interaction_type TEXT CHECK (interaction_type IN ('click','like','dislike','share')),
    interaction_time TIMESTAMPTZ DEFAULT NOW(),
    dwell_ms         INT,   -- posh wra ekatse (tha dw an tha to krathsw)
    UNIQUE (user_id, article_id, interaction_type)  
);
CREATE INDEX idx_interactions_user_time ON interactions (user_id, interaction_time DESC);
CREATE INDEX idx_interactions_article ON interactions (article_id);

-- EXPLANATIONS
DROP TABLE IF EXISTS explanations CASCADE;
CREATE TABLE explanations (
    id            BIGSERIAL PRIMARY KEY,
    article_id    BIGINT REFERENCES articles(id) ON DELETE CASCADE,
    method        TEXT CHECK (method IN ('SHAP','LIME')),
    model_version TEXT,               
    explanation   JSONB,             
    generated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (article_id, method, model_version)
);
CREATE INDEX idx_explanations_article ON explanations (article_id);


ALTER TABLE users
ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
ADD COLUMN IF NOT EXISTS embedding_updated_at timestamptz;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_set_updated_at ON users;

CREATE TRIGGER trg_users_set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();



CREATE OR REPLACE FUNCTION set_embedding_updated_at()
RETURNS trigger AS $$
BEGIN
  IF (NEW.embedding IS DISTINCT FROM OLD.embedding) THEN
    NEW.embedding_updated_at = now();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_set_embedding_updated_at ON users;

CREATE TRIGGER trg_users_set_embedding_updated_at
BEFORE UPDATE OF embedding ON users
FOR EACH ROW
EXECUTE FUNCTION set_embedding_updated_at();


ALTER TABLE articles ADD COLUMN IF NOT EXISTS image_url TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_impressions_request_article
ON impressions(request_id, article_id);


ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email text,
  ADD COLUMN IF NOT EXISTS password_hash text,
  ADD COLUMN IF NOT EXISTS last_login_at timestamptz;

--  Unique constraints (ώστε να μην υπάρχουν διπλότυπα)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'users_username_unique'
  ) THEN
    ALTER TABLE users ADD CONSTRAINT users_username_unique UNIQUE (username);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'users_email_unique'
  ) THEN
    ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email);
  END IF;
END $$;


CREATE TABLE IF NOT EXISTS "session" (
  "sid" varchar NOT NULL COLLATE "default",
  "sess" json NOT NULL,
  "expire" timestamp(6) NOT NULL
);

ALTER TABLE "session" ADD CONSTRAINT "session_pkey" PRIMARY KEY ("sid");

CREATE INDEX IF NOT EXISTS "IDX_session_expire" ON "session" ("expire");


UPDATE users
SET password_hash = '$2b$12$ontID47.CXEvAgKFGmt/5.fXRPpiEGvBFULRyH3JRfZVeGbI1A.62',
    updated_at = NOW()
WHERE username = 'chriss';

ALTER TABLE users
ALTER COLUMN password_hash SET NOT NULL;

