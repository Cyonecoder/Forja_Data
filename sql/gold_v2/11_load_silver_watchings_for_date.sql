-- variables attendues : :run_date
BEGIN;

DELETE FROM silver_watchings
WHERE watched_at::date = :'run_date'::date;

INSERT INTO silver_watchings (
    w_id,
    user_id,
    content_id,
    profile_id,
    device_id,
    watch_duration,
    watched_at,
    country,
    os,
    browser,
    city,
    state,
    lang,
    system_lang,
    isp,
    created_at,
    updated_at
)
SELECT
    id,
    user_id,
    content_id,
    profile_id,
    device_id,
    duration::double precision,
    created_at,
    country,
    os,
    browser,
    city,
    state,
    lang,
    system_lang,
    isp,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, user_id, content_id, profile_id, device_id, duration, created_at, updated_at, country, os, browser, city, state, lang, system_lang, isp FROM watchings WHERE created_at::date = ''' || :'run_date' || ''''
) AS t(
    id BIGINT,
    user_id BIGINT,
    content_id BIGINT,
    profile_id BIGINT,
    device_id TEXT,
    duration TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    country TEXT,
    os TEXT,
    browser TEXT,
    city TEXT,
    state TEXT,
    lang TEXT,
    system_lang TEXT,
    isp TEXT
);

COMMIT;
