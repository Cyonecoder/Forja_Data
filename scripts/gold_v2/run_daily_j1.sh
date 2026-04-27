#!/usr/bin/env bash
set -euo pipefail

RUN_DATE="${1:-$(date -d 'yesterday' +%F)}"
BASE_DIR="$HOME/forja_pipeline"

echo "[DAILY_J1] START run_date=$RUN_DATE"

"$BASE_DIR/scripts/gold_v2/load_reference_silver_all.sh"
"$BASE_DIR/scripts/gold_v2/load_silver_watchings_for_date.sh" "$RUN_DATE"
"$BASE_DIR/scripts/gold_v2/run_gold_for_date.sh" "$RUN_DATE"
"$BASE_DIR/scripts/gold_v2/run_gold_ux_actions_for_date.sh" "$RUN_DATE"

echo "[DAILY_J1] DONE run_date=$RUN_DATE"
