ALTER TABLE users
ADD COLUMN embedding vector(384);


ALTER TABLE interactions
ADD CONSTRAINT interaction_type_chk
CHECK (interaction_type IN ('click', 'like', 'share', 'dislike'));


INSERT INTO users (username, created_at)
VALUES ('chriss', now())
RETURNING id, username;


SELECT id, title, category, source
FROM articles
WHERE source = 'in.gr'
ORDER BY scraped_at DESC
LIMIT 10;



INSERT INTO interactions (user_id, article_id, interaction_type)
VALUES
    (1, 1310, 'click'),
	(1, 1310, 'like'),
    (1, 1206, 'click'),
	(1, 1206, 'like'),
    (1, 1300, 'click'),
    (1, 1300, 'dislike'),
    (1, 1297, 'click'),
	(1, 1349, 'click'),
	(1, 1180, 'click'),
	(1, 1335, 'click'),
	(1, 1329, 'click'),
	(1, 1289, 'click'),
	(1, 1289, 'share');



