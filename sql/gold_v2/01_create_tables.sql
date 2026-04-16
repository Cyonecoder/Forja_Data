CREATE TABLE IF NOT EXISTS gold_daily_content_performance_v2 (
    report_date         DATE NOT NULL,
    content_id          TEXT NOT NULL,
    content_title       TEXT NOT NULL,
    content_type        TEXT,
    cat_name            TEXT,
    total_views         BIGINT NOT NULL DEFAULT 0,
    total_users         BIGINT NOT NULL DEFAULT 0,
    avg_duration_min    DOUBLE PRECISION,
    watch_time_total    DOUBLE PRECISION NOT NULL DEFAULT 0,
    completion_rate     DOUBLE PRECISION NOT NULL DEFAULT 0,
    top_country         TEXT,
    top_os              TEXT,
    updated_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (report_date, content_id)
);

CREATE TABLE IF NOT EXISTS gold_daily_users_v2 (
    report_date         DATE PRIMARY KEY,
    active_users        BIGINT NOT NULL DEFAULT 0,
    new_users           BIGINT NOT NULL DEFAULT 0,
    total_users_cumul   BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_daily_actions_v2 (
    report_date         DATE NOT NULL,
    action_type         TEXT NOT NULL,
    total_count         BIGINT NOT NULL DEFAULT 0,
    unique_users        BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    PRIMARY KEY (report_date, action_type)
);

CREATE TABLE IF NOT EXISTS gold_pipeline_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    pipeline_name       TEXT NOT NULL,
    run_date            DATE NOT NULL,
    step_name           TEXT NOT NULL,
    status              TEXT NOT NULL,
    rows_written        BIGINT DEFAULT 0,
    started_at          TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    finished_at         TIMESTAMP WITHOUT TIME ZONE,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_gold_pipeline_runs_date
ON gold_pipeline_runs(run_date, pipeline_name, step_name);
