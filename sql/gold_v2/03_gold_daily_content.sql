BEGIN;

DELETE FROM gold_daily_content_performance_v2
WHERE report_date = :'run_date'::date;

INSERT INTO gold_daily_content_performance_v2 (
    report_date,
    content_id,
    content_title,
    content_type,
    cat_name,
    total_views,
    total_users,
    avg_duration_min,
    watch_time_total,
    completion_rate,
    top_country,
    top_os,
    updated_at
)
WITH base AS (
    SELECT
        watched_at::date AS report_date,
        content_id::text,
        COALESCE(NULLIF(content_title, ''), 'UNKNOWN_CONTENT') AS content_title,
        COALESCE(NULLIF(content_type, ''), 'unknown') AS content_type,
        cat_name,
        COALESCE(watch_duration, 0)::double precision AS watch_duration,
        user_id::text,
        country,
        os
    FROM silver_watchings
    WHERE watched_at::date = :'run_date'::date
),
country_ranked AS (
    SELECT report_date, content_id, country,
           ROW_NUMBER() OVER (
               PARTITION BY report_date, content_id
               ORDER BY COUNT(*) DESC, country
           ) AS rn
    FROM base
    WHERE country IS NOT NULL AND country <> ''
    GROUP BY report_date, content_id, country
),
os_ranked AS (
    SELECT report_date, content_id, os,
           ROW_NUMBER() OVER (
               PARTITION BY report_date, content_id
               ORDER BY COUNT(*) DESC, os
           ) AS rn
    FROM base
    WHERE os IS NOT NULL AND os <> ''
    GROUP BY report_date, content_id, os
)
SELECT
    b.report_date,
    b.content_id,
    MAX(b.content_title) AS content_title,
    MAX(b.content_type) AS content_type,
    MAX(b.cat_name) AS cat_name,
    COUNT(*) AS total_views,
    COUNT(DISTINCT b.user_id) AS total_users,
    ROUND((AVG(b.watch_duration) / 60.0)::numeric, 4)::double precision AS avg_duration_min,
    ROUND((SUM(b.watch_duration) / 60.0)::numeric, 4)::double precision AS watch_time_total,
    0.0 AS completion_rate,
    MAX(CASE WHEN cr.rn = 1 THEN cr.country END) AS top_country,
    MAX(CASE WHEN orr.rn = 1 THEN orr.os END) AS top_os,
    now()
FROM base b
LEFT JOIN country_ranked cr
    ON b.report_date = cr.report_date
   AND b.content_id = cr.content_id
LEFT JOIN os_ranked orr
    ON b.report_date = orr.report_date
   AND b.content_id = orr.content_id
GROUP BY b.report_date, b.content_id;

COMMIT;
