BEGIN;

DELETE FROM silver_subscriptions;

INSERT INTO silver_subscriptions (
    subscription_id,
    user_id,
    subscription_type,
    started_at,
    ended_at,
    created_at,
    updated_at
)
SELECT
    id,
    user_id,
    type,
    started_at,
    ended_at,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, user_id, type, started_at, ended_at, created_at, updated_at FROM subscriptions'
) AS t(
    id BIGINT,
    user_id BIGINT,
    type TEXT,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

COMMIT;
