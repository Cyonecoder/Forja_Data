#!/usr/bin/env bash
set -euo pipefail

PG_CONT="forja_postgres"
PG_USER="snrt_readonly"
PG_DB="snrt_stats"
BASE_DIR="$HOME/forja_pipeline"

echo "[REF_SILVER] load silver_users"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/21_load_silver_users_full.sql"

echo "[REF_SILVER] load silver_contents"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/23_load_silver_contents_full.sql"

echo "[REF_SILVER] load silver_user_likes"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/25_load_silver_user_likes_full.sql"

echo "[REF_SILVER] load silver_user_favs"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/26_load_silver_user_favs_full.sql"

echo "[REF_SILVER] load silver_profiles"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/28_load_silver_profiles_full.sql"

echo "[REF_SILVER] load silver_categories"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/30_load_silver_categories_full.sql"

echo "[REF_SILVER] load silver_subscriptions"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/32_load_silver_subscriptions_full.sql"

echo "[REF_SILVER] load silver_actions"
docker exec -i "$PG_CONT" psql -U "$PG_USER" -d "$PG_DB" < "$BASE_DIR/sql/gold_v2/36_load_silver_actions_full.sql"

echo "[REF_SILVER] DONE"
