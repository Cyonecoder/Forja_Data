BEGIN;

DELETE FROM gold_daily_actions_v2
WHERE report_date = :'run_date'::date;

INSERT INTO gold_daily_actions_v2 (
    report_date,
    action_type,
    total_count,
    unique_users,
    updated_at
)
SELECT
    watched_at::date AS report_date,
    'watching' AS action_type,
    COUNT(*) AS total_count,
    COUNT(DISTINCT user_id::text) AS unique_users,
    now()
FROM silver_watchings
WHERE watched_at::date = :'run_date'::date
GROUP BY watched_at::date;

COMMIT;
