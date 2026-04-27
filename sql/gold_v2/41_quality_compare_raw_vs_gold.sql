WITH raw_day AS (
    SELECT
        watched_at::date AS report_date,
        COUNT(*) AS raw_total_views,
        COUNT(DISTINCT user_id) AS raw_total_users,
        ROUND((SUM(COALESCE(watch_duration,0)) / 60.0)::numeric, 2) AS raw_watch_time_total
    FROM silver_watchings
    WHERE watched_at::date = DATE '2026-03-04'
    GROUP BY watched_at::date
),
gold_day AS (
    SELECT
        report_date,
        SUM(total_views) AS gold_total_views,
        SUM(total_users) AS gold_total_users,
        ROUND(SUM(watch_time_total)::numeric, 2) AS gold_watch_time_total
    FROM gold_daily_content_performance_v2
    WHERE report_date = DATE '2026-03-04'
    GROUP BY report_date
)
SELECT
    r.report_date,
    r.raw_total_views,
    g.gold_total_views,
    r.raw_total_users,
    g.gold_total_users,
    r.raw_watch_time_total,
    g.gold_watch_time_total,
    (r.raw_total_views - g.gold_total_views) AS diff_views,
    (r.raw_watch_time_total - g.gold_watch_time_total) AS diff_watch_time
FROM raw_day r
JOIN gold_day g ON r.report_date = g.report_date;
