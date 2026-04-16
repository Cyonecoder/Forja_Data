#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:-$(date -d 'yesterday' +%F)}"
PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

echo "[GOLD_V2] START run_date=$RUN_DATE"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUN_DATE" < "$BASE_DIR/sql/gold_v2/03_gold_daily_content.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUN_DATE" < "$BASE_DIR/sql/gold_v2/04_gold_daily_users.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUN_DATE" < "$BASE_DIR/sql/gold_v2/05_gold_daily_actions.sql"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -c "
INSERT INTO gold_pipeline_runs (pipeline_name, run_date, step_name, status, rows_written, started_at, finished_at)
VALUES
('gold_v2_daily', DATE '$RUN_DATE', 'content_users_actions', 'success', 0, now(), now());
"

echo "[GOLD_V2] DONE run_date=$RUN_DATE"
