CREATE OR REPLACE FUNCTION purge_old_articles(p_days int DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n_deleted integer;
BEGIN
  DELETE FROM articles
  WHERE published_at IS NOT NULL
    AND published_at < now() - make_interval(days => p_days);

  GET DIAGNOSTICS n_deleted = ROW_COUNT;  --ποσες σειρες διαγραφηκαν
  RETURN n_deleted;
END
$$;


SELECT purge_old_articles();
