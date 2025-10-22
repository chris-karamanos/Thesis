CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT UNIQUE,
    content TEXT,
    source TEXT,
    category TEXT,
    published_at TIMESTAMP,
    language TEXT
);


CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    preferences JSONB 
);


CREATE TABLE interactions (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    article_id INT REFERENCES articles(id) ON DELETE CASCADE,
    interaction_type TEXT CHECK (interaction_type IN ('click', 'like', 'dislike', 'share')),
    interaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE explanations (
    id SERIAL PRIMARY KEY,
    article_id INT REFERENCES articles(id) ON DELETE CASCADE,
    method TEXT CHECK (method IN ('SHAP', 'LIME')),
    explanation JSONB, 
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
