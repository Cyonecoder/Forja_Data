CREATE TABLE IF NOT EXISTS gold_daily_ux_actions_v2 (
    report_date      DATE NOT NULL,
    action_type      TEXT NOT NULL,
    label            TEXT,
    content_id       TEXT,
    total_count      BIGINT,
    unique_users     BIGINT,
    unique_profiles  BIGINT,
    top_country      TEXT,
    top_os           TEXT,
    updated_at       TIMESTAMPTZ DEFAULT now()
);
