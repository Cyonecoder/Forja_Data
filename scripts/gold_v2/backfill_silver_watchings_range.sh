#!/usr/bin/env bash
set -euo pipefail

START_DATE="${1:?Usage: backfill_silver_watchings_range.sh YYYY-MM-DD YYYY-MM-DD}"
END_DATE="${2:?Usage: backfill_silver_watchings_range.sh YYYY-MM-DD YYYY-MM-DD}"

CUR="$START_DATE"

while [ "$(date -d "$CUR" +%s)" -le "$(date -d "$END_DATE" +%s)" ]; do
  echo "========== SILVER_WATCHINGS $CUR =========="
  "$HOME/forja_pipeline/scripts/gold_v2/load_silver_watchings_for_date.sh" "$CUR" \
    | tee -a "$HOME/forja_pipeline/logs/gold_v2/silver_watchings_backfill.log"
  CUR="$(date -I -d "$CUR + 1 day")"
done
