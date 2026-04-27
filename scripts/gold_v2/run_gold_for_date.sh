#!/usr/bin/env bash
set -euo pipefail

RUNDATE="${1:-$(date -d yesterday +%F)}"
PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

echo "[GOLD_V2] START run_date=$RUNDATE"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUNDATE" < "$BASE_DIR/sql/gold_v2/33_gold_daily_content_enriched.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUNDATE" < "$BASE_DIR/sql/gold_v2/04_gold_daily_users.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUNDATE" < "$BASE_DIR/sql/gold_v2/34_gold_daily_actions_enriched.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUNDATE" < "$BASE_DIR/sql/gold_v2/38_gold_daily_ux_actions_enriched.sql"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -c "
INSERT INTO gold_pipeline_runs (
    pipeline_name,
    run_date,
    step_name,
    status,
    rows_written,
    started_at,
    finished_at
)
VALUES (
    'gold_v2_daily',
    DATE '$RUNDATE',
    'content_users_actions_ux_enriched',
    'success',
    0,
    now(),
    now()
);
"

echo "[GOLD_V2] DONE run_date=$RUNDATE"
