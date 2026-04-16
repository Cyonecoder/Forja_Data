BEGIN;

DELETE FROM gold_daily_users_v2
WHERE report_date = :'run_date'::date;

WITH first_seen AS (
    SELECT user_id::text AS user_id, MIN(watched_at::date) AS first_report_date
    FROM silver_watchings
    WHERE user_id IS NOT NULL
    GROUP BY user_id::text
),
active_day AS (
    SELECT
        watched_at::date AS report_date,
        COUNT(DISTINCT user_id::text) AS active_users
    FROM silver_watchings
    WHERE watched_at::date = :'run_date'::date
      AND user_id IS NOT NULL
    GROUP BY watched_at::date
),
new_day AS (
    SELECT
        first_report_date AS report_date,
        COUNT(*) AS new_users
    FROM first_seen
    WHERE first_report_date = :'run_date'::date
    GROUP BY first_report_date
),
cumul_day AS (
    SELECT
        :'run_date'::date AS report_date,
        COUNT(*) AS total_users_cumul
    FROM first_seen
    WHERE first_report_date <= :'run_date'::date
)
INSERT INTO gold_daily_users_v2 (
    report_date,
    active_users,
    new_users,
    total_users_cumul,
    updated_at
)
SELECT
    :'run_date'::date AS report_date,
    COALESCE(a.active_users, 0),
    COALESCE(n.new_users, 0),
    COALESCE(c.total_users_cumul, 0),
    now()
FROM active_day a
FULL OUTER JOIN new_day n ON a.report_date = n.report_date
FULL OUTER JOIN cumul_day c ON COALESCE(a.report_date, n.report_date) = c.report_date;

COMMIT;
