\timing on

\echo '=== RAW / SILVER : total views by day ==='
SELECT
    watched_at::date AS report_date,
    COUNT(*) AS total_views,
    COUNT(DISTINCT user_id) AS total_users,
    ROUND((SUM(COALESCE(watch_duration,0)) / 60.0)::numeric, 2) AS watch_time_total
FROM silver_watchings
WHERE watched_at::date = DATE '2026-03-04'
GROUP BY watched_at::date;

\echo '=== GOLD : total views by day ==='
SELECT
    report_date,
    SUM(total_views) AS total_views,
    SUM(total_users) AS total_users,
    ROUND(SUM(watch_time_total)::numeric, 2) AS watch_time_total
FROM gold_daily_content_performance_v2
WHERE report_date = DATE '2026-03-04'
GROUP BY report_date;

\echo '=== RAW / SILVER : top 10 contents ==='
SELECT
    watched_at::date AS report_date,
    content_id,
    COUNT(*) AS total_views,
    COUNT(DISTINCT user_id) AS total_users
FROM silver_watchings
WHERE watched_at::date = DATE '2026-03-04'
GROUP BY watched_at::date, content_id
ORDER BY total_views DESC
LIMIT 10;

\echo '=== GOLD : top 10 contents ==='
SELECT
    report_date,
    content_id,
    total_views,
    total_users
FROM gold_daily_content_performance_v2
WHERE report_date = DATE '2026-03-04'
ORDER BY total_views DESC
LIMIT 10;
