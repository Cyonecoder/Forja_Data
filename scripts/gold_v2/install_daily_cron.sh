#!/usr/bin/env bash
set -euo pipefail

CRON_LINE="0 6 * * * cd $HOME/forja_pipeline && ./scripts/gold_v2/run_daily_j1.sh >> $HOME/forja_pipeline/logs/gold_v2/daily_j1_cron.log 2>&1"

( crontab -l 2>/dev/null | grep -v 'scripts/gold_v2/run_daily_j1.sh' ; echo "$CRON_LINE" ) | crontab -

echo "[CRON] Installed:"
crontab -l | grep 'scripts/gold_v2/run_daily_j1.sh'
