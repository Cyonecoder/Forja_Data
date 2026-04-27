INSERT INTO gold_daily_ux_actions_v2 (
    report_date,
    action_type,
    label,
    content_id,
    total_count,
    unique_users,
    unique_profiles,
    top_country,
    top_os,
    updated_at
)
WITH missing_dates AS (
    SELECT gs.d::date AS d
    FROM generate_series('2024-02-27'::date, '2026-04-20'::date, '1 day') AS gs(d)
    WHERE gs.d::date NOT IN (
        SELECT DISTINCT report_date
        FROM gold_daily_ux_actions_v2
    )
    AND gs.d::date < CURRENT_DATE
),
base AS (
    SELECT
        a.created_at::date AS report_date,
        COALESCE(NULLIF(a.action_type, ''), 'unknown') AS action_type,
        NULLIF(a.label, '') AS label,
        a.content_id::text AS content_id,
        a.user_id::text AS user_id,
        a.profile_id::text AS profile_id,
        a.country,
        a.os
    FROM missing_dates md
    JOIN silver_actions a ON a.created_at::date = md.d
    WHERE COALESCE(NULLIF(a.action_type, ''), 'unknown') IN (
        'view', 'active', 'click', 'search',
        'statistic_player_play', 'statistic_player_pause',
        'statistic_back', 'statistic_almost_watched'
    )
),
country_ranked AS (
    SELECT
        report_date, action_type, label, content_id, country,
        ROW_NUMBER() OVER (
            PARTITION BY report_date, action_type, COALESCE(label, ''), COALESCE(content_id, '')
            ORDER BY COUNT(*) DESC, country
        ) AS rn
    FROM base
    WHERE country IS NOT NULL AND country <> ''
    GROUP BY report_date, action_type, label, content_id, country
),
os_ranked AS (
    SELECT
        report_date, action_type, label, content_id, os,
        ROW_NUMBER() OVER (
            PARTITION BY report_date, action_type, COALESCE(label, ''), COALESCE(content_id, '')
            ORDER BY COUNT(*) DESC, os
        ) AS rn
    FROM base
    WHERE os IS NOT NULL AND os <> ''
    GROUP BY report_date, action_type, label, content_id, os
)
SELECT
    b.report_date,
    b.action_type,
    b.label,
    b.content_id,
    COUNT(*)                                     AS total_count,
    COUNT(DISTINCT b.user_id)                    AS unique_users,
    COUNT(DISTINCT b.profile_id)                 AS unique_profiles,
    MAX(CASE WHEN cr.rn = 1 THEN cr.country END) AS top_country,
    MAX(CASE WHEN orr.rn = 1 THEN orr.os END)    AS top_os,
    NOW()                                        AS updated_at
FROM base b
LEFT JOIN country_ranked cr
       ON b.report_date = cr.report_date
      AND b.action_type = cr.action_type
      AND COALESCE(b.label, '') = COALESCE(cr.label, '')
      AND COALESCE(b.content_id, '') = COALESCE(cr.content_id, '')
LEFT JOIN os_ranked orr
       ON b.report_date = orr.report_date
      AND b.action_type = orr.action_type
      AND COALESCE(b.label, '') = COALESCE(orr.label, '')
      AND COALESCE(b.content_id, '') = COALESCE(orr.content_id, '')
GROUP BY
    b.report_date,
    b.action_type,
    b.label,
    b.content_id
ON CONFLICT (report_date, action_type, label, content_id)
DO UPDATE SET
    total_count     = EXCLUDED.total_count,
    unique_users    = EXCLUDED.unique_users,
    unique_profiles = EXCLUDED.unique_profiles,
    top_country     = EXCLUDED.top_country,
    top_os          = EXCLUDED.top_os,
    updated_at      = NOW();
