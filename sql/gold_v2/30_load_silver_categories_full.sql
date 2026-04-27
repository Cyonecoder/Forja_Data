BEGIN;

DELETE FROM silver_categories;

INSERT INTO silver_categories (
    category_id,
    slug,
    name,
    created_at,
    updated_at
)
SELECT
    id,
    slug,
    name,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, slug, name, created_at, updated_at FROM categories'
) AS t(
    id BIGINT,
    slug TEXT,
    name JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

COMMIT;
