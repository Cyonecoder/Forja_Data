CREATE TABLE IF NOT EXISTS silver_categories (
    category_id      BIGINT PRIMARY KEY,
    slug             TEXT,
    name             JSONB,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ
);
