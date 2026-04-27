#!/usr/bin/env bash
set -euo pipefail

PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

RUN_DATE="${1:-$(date -d 'yesterday' +%F)}"

echo "[GOLD_UX_ACTIONS] START run_date=$RUN_DATE"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" \
  -v run_date="$RUN_DATE" \
  < "$BASE_DIR/sql/gold_v2/38_gold_daily_ux_actions_enriched.sql"

echo "[GOLD_UX_ACTIONS] DONE run_date=$RUN_DATE"
