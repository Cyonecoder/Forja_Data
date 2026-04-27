CREATE TABLE IF NOT EXISTS silver_profiles (
    profile_id       BIGINT PRIMARY KEY,
    user_id          BIGINT,
    name             TEXT,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ
);
