CREATE TABLE IF NOT EXISTS silver_watchings (
    w_id            BIGINT PRIMARY KEY,
    user_id         BIGINT,
    content_id      BIGINT,
    profile_id      BIGINT,
    device_id       TEXT,
    watch_duration  DOUBLE PRECISION,
    watched_at      TIMESTAMPTZ,
    country         TEXT,
    os              TEXT,
    browser         TEXT,
    city            TEXT,
    state           TEXT,
    lang            TEXT,
    system_lang     TEXT,
    isp             TEXT,

    -- Colonnes évolutives pour enrichissement futur
    content_title   TEXT,
    content_type    TEXT,
    cat_name        TEXT,

    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

ALTER TABLE silver_watchings
    ADD COLUMN IF NOT EXISTS content_title TEXT,
    ADD COLUMN IF NOT EXISTS content_type  TEXT,
    ADD COLUMN IF NOT EXISTS cat_name      TEXT;

CREATE INDEX IF NOT EXISTS idx_silver_watchings_watched_at
    ON silver_watchings(watched_at);

CREATE INDEX IF NOT EXISTS idx_silver_watchings_content
    ON silver_watchings(content_id, watched_at);

CREATE INDEX IF NOT EXISTS idx_silver_watchings_user
    ON silver_watchings(user_id, watched_at);
