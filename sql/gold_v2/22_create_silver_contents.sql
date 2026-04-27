CREATE TABLE IF NOT EXISTS silver_contents (
    content_id      BIGINT PRIMARY KEY,
    slug            TEXT,
    type            TEXT,
    program_id      BIGINT,
    series_slug     TEXT,
    season_slug     TEXT,
    episode_slug    TEXT,
    name_short      JSONB,
    name_long       JSONB,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);
