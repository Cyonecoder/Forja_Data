BEGIN;

DELETE FROM gold_daily_content_performance_v2
WHERE report_date = :'run_date'::date;

WITH contents_dedup AS (
    SELECT
        content_id,
        MAX(series_slug) AS series_slug,
        MAX(season_slug) AS season_slug,
        MAX(episode_slug) AS episode_slug
    FROM silver_contents
    GROUP BY content_id
),
content_agg AS (
    SELECT
        w.watched_at::date AS report_date,
        w.content_id::text AS content_id,
        MAX(COALESCE(NULLIF(w.content_title, ''), 'UNKNOWN_CONTENT')) AS content_title,
        MAX(COALESCE(NULLIF(w.content_type, ''), 'unknown')) AS content_type,
        MAX(w.cat_name) AS cat_name,
        COUNT(*) AS total_views,
        COUNT(DISTINCT w.user_id::text) AS total_users,
        ROUND((AVG(COALESCE(w.watch_duration, 0)) / 60.0)::numeric, 4)::double precision AS avg_duration_min,
        ROUND((SUM(COALESCE(w.watch_duration, 0)) / 60.0)::numeric, 4)::double precision AS watch_time_total
    FROM silver_watchings w
    WHERE w.watched_at::date = :'run_date'::date
    GROUP BY w.watched_at::date, w.content_id::text
),
country_top AS (
    SELECT report_date, content_id::text AS content_id, country AS top_country
    FROM (
        SELECT
            w.watched_at::date AS report_date,
            w.content_id,
            w.country,
            ROW_NUMBER() OVER (
                PARTITION BY w.watched_at::date, w.content_id
                ORDER BY COUNT(*) DESC, w.country
            ) AS rn
        FROM silver_watchings w
        WHERE w.watched_at::date = :'run_date'::date
          AND w.country IS NOT NULL
          AND w.country <> ''
        GROUP BY w.watched_at::date, w.content_id, w.country
    ) x
    WHERE rn = 1
),
os_top AS (
    SELECT report_date, content_id::text AS content_id, os AS top_os
    FROM (
        SELECT
            w.watched_at::date AS report_date,
            w.content_id,
            w.os,
            ROW_NUMBER() OVER (
                PARTITION BY w.watched_at::date, w.content_id
                ORDER BY COUNT(*) DESC, w.os
            ) AS rn
        FROM silver_watchings w
        WHERE w.watched_at::date = :'run_date'::date
          AND w.os IS NOT NULL
          AND w.os <> ''
        GROUP BY w.watched_at::date, w.content_id, w.os
    ) x
    WHERE rn = 1
)
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
    updated_at,
    series_slug,
    season_slug,
    episode_slug
)
SELECT
    a.report_date,
    a.content_id,
    a.content_title,
    a.content_type,
    a.cat_name,
    a.total_views,
    a.total_users,
    a.avg_duration_min,
    a.watch_time_total,
    0.0 AS completion_rate,
    ct.top_country,
    ot.top_os,
    now(),
    cd.series_slug,
    cd.season_slug,
    cd.episode_slug
FROM content_agg a
LEFT JOIN country_top ct
  ON a.report_date = ct.report_date
 AND a.content_id = ct.content_id
LEFT JOIN os_top ot
  ON a.report_date = ot.report_date
 AND a.content_id = ot.content_id
LEFT JOIN contents_dedup cd
  ON a.content_id::bigint = cd.content_id;

COMMIT;
