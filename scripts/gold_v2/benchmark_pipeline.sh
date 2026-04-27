#!/usr/bin/env bash
set -euo pipefail

PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/40_benchmark_raw_vs_gold.sql"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/41_quality_compare_raw_vs_gold.sql"
