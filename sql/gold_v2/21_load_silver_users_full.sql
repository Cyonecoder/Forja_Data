BEGIN;

DELETE FROM silver_users;

INSERT INTO silver_users (
    user_id,
    email,
    phone,
    created_at,
    updated_at
)
SELECT
    id,
    email,
    phone,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, email, phone, created_at, updated_at FROM users'
) AS t(
    id BIGINT,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

COMMIT;
