-- KPI 1 : vues par jour
SELECT
    report_date AS time,
    SUM(total_views) AS total_views
FROM gold_daily_content_performance_v2
GROUP BY report_date
ORDER BY report_date;

-- KPI 2 : users actifs par jour
SELECT
    report_date AS time,
    active_users
FROM gold_daily_users_v2
ORDER BY report_date;

-- KPI 3 : watch time par jour
SELECT
    report_date AS time,
    ROUND(SUM(watch_time_total)::numeric, 2) AS watch_time_total
FROM gold_daily_content_performance_v2
GROUP BY report_date
ORDER BY report_date;

-- KPI 4 : actions par type
SELECT
    report_date AS time,
    action_type,
    total_count
FROM gold_daily_actions_v2
ORDER BY report_date, action_type;

-- KPI 5 : top contenus sur période
SELECT
    content_title,
    COALESCE(series_slug, 'NO_SERIES') AS series_slug,
    SUM(total_views) AS total_views,
    SUM(total_users) AS total_users
FROM gold_daily_content_performance_v2
GROUP BY content_title, COALESCE(series_slug, 'NO_SERIES')
ORDER BY total_views DESC
LIMIT 20;
