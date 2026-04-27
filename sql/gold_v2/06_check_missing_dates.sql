SELECT missing_date::date
FROM generate_series(
    (SELECT MIN(report_date) FROM gold_daily_content_performance_v2),
    (SELECT MAX(report_date) FROM gold_daily_content_performance_v2),
    '1 day'::interval
) AS gs(missing_date)
WHERE missing_date::date NOT IN (
    SELECT DISTINCT report_date FROM gold_daily_content_performance_v2
)
ORDER BY 1;
