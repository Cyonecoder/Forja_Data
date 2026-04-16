CREATE TABLE IF NOT EXISTS stg_silver_watchings_daily (
    w_id                TEXT PRIMARY KEY,
    user_id             TEXT,
    content_id          TEXT,
    content_title       TEXT,
    content_type        TEXT,
    cat_name            TEXT,
    watch_duration      DOUBLE PRECISION,
    watched_at          TIMESTAMPTZ,
    report_date         DATE,
    country             TEXT,
    os                  TEXT,
    inserted_at         TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_silver_watchings_daily_report_date
ON stg_silver_watchings_daily(report_date);

CREATE INDEX IF NOT EXISTS idx_stg_silver_watchings_daily_content
ON stg_silver_watchings_daily(report_date, content_id);

CREATE INDEX IF NOT EXISTS idx_stg_silver_watchings_daily_user
ON stg_silver_watchings_daily(report_date, user_id);
