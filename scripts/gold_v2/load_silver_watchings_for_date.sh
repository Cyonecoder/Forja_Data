#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:?Usage: load_silver_watchings_for_date.sh YYYY-MM-DD}"
PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

echo "[SILVER_W] START $RUN_DATE"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -v run_date="$RUN_DATE" < "$BASE_DIR/sql/gold_v2/11_load_silver_watchings_for_date.sql"

echo "[SILVER_W] DONE $RUN_DATE"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" -c "
SELECT watched_at::date AS report_date, COUNT(*) AS rows
FROM silver_watchings
WHERE watched_at::date = DATE '$RUN_DATE'
GROUP BY watched_at::date;
"
