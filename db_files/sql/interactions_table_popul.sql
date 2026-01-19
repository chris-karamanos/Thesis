ALTER TABLE interactions
ADD CONSTRAINT interaction_type_chk
CHECK (interaction_type IN ('click', 'like', 'share', 'dislike'));


CREATE INDEX IF NOT EXISTS idx_interactions_user_request
ON interactions (user_id, request_id, interaction_time DESC);

CREATE INDEX IF NOT EXISTS idx_interactions_request_article
ON interactions (request_id, article_id);


ALTER TABLE interactions
ADD CONSTRAINT fk_interactions_impressions
FOREIGN KEY (user_id, request_id, article_id)
REFERENCES impressions (user_id, request_id, article_id)
ON DELETE CASCADE;


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


