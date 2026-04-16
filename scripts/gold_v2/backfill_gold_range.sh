#!/usr/bin/env bash
set -euo pipefail

START_DATE="${1:?START_DATE manquante format YYYY-MM-DD}"
END_DATE="${2:?END_DATE manquante format YYYY-MM-DD}"
CUR="$START_DATE"

while [ "$(date -d "$CUR" +%s)" -le "$(date -d "$END_DATE" +%s)" ]; do
  echo "========== BACKFILL $CUR =========="
  "$HOME/forja_pipeline/scripts/gold_v2/run_gold_for_date.sh" "$CUR" | tee -a "$HOME/forja_pipeline/logs/gold_v2/backfill.log"
  CUR="$(date -I -d "$CUR + 1 day")"
done
