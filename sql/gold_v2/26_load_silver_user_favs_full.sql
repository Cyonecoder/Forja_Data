BEGIN;

DELETE FROM silver_user_favs;

INSERT INTO silver_user_favs (
    id,
    user_id,
    content_id,
    created_at
)
SELECT
    id,
    user_id,
    content_id,
    created_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, user_id, content_id, created_at FROM user_fav'
) AS t(
    id BIGINT,
    user_id BIGINT,
    content_id BIGINT,
    created_at TIMESTAMPTZ
);

COMMIT;
