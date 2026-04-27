BEGIN;

DELETE FROM silver_profiles;

INSERT INTO silver_profiles (
    profile_id,
    user_id,
    name,
    created_at,
    updated_at
)
SELECT
    id,
    user_id,
    name,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, user_id, name, created_at, updated_at FROM profiles'
) AS t(
    id BIGINT,
    user_id BIGINT,
    name TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

COMMIT;
