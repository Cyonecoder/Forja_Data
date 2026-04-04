#!/bin/bash
# ============================================================
#  FORJA PIPELINE STATUS — Script de monitoring complet
#  Usage: bash ~/forja_pipeline/scripts/pipeline_status.sh
# ============================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}✅ $1${NC}"; }
warn() { echo -e "  ${YELLOW}⚠️  $1${NC}"; }
fail() { echo -e "  ${RED}❌ $1${NC}"; }
info() { echo -e "  ${CYAN}ℹ️  $1${NC}"; }
sep()  { echo -e "${BLUE}${BOLD}══════════════════════════════════════════════════${NC}"; }
title(){ echo -e "\n${BOLD}${BLUE}▶ $1${NC}"; sep; }

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║      FORJA DATA LAKE — PIPELINE STATUS REPORT        ║${NC}"
echo -e "${BOLD}║      $(date '+%A %d/%m/%Y %H:%M:%S')                 ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"

# ═══════════════════════════════════════════════════
# 1. CONTAINERS DOCKER
# ═══════════════════════════════════════════════════
title "1. CONTAINERS DOCKER"
CONTAINERS=(
  "forja_zookeeper"
  "forja_kafka"
  "forja_schema_registry"
  "forja_kafka_ui"
  "forja_postgres"
  "forja_minio"
  "forja_spark_master"
  "forja_pipeline-spark-worker-2"
  "forja_bronze_consumer"
  "ga4-producer"
  "forja_grafana"
)
ALL_UP=true
for c in "${CONTAINERS[@]}"; do
  STATUS=$(docker inspect --format='{{.State.Status}}' "$c" 2>/dev/null)
  STARTED=$(docker inspect --format='{{.State.StartedAt}}' "$c" 2>/dev/null | cut -c1-19 | tr 'T' ' ')
  if [ "$STATUS" == "running" ]; then
    ok "$c → running (depuis $STARTED UTC)"
  elif [ -z "$STATUS" ]; then
    fail "$c → introuvable"; ALL_UP=false
  else
    fail "$c → $STATUS"; ALL_UP=false
  fi
done
echo ""
if $ALL_UP; then ok "Tous les containers sont UP"; else warn "Certains containers sont arrêtés"; fi

# ═══════════════════════════════════════════════════
# 2. KAFKA — TOPICS & MESSAGES
# ═══════════════════════════════════════════════════
title "2. KAFKA — TOPICS & MESSAGES"
TOPICS=$(docker exec forja_kafka kafka-topics \
  --bootstrap-server localhost:9092 --list 2>/dev/null)
if [ -z "$TOPICS" ]; then
  fail "Kafka inaccessible ou aucun topic"
else
  TOPIC_COUNT=$(echo "$TOPICS" | grep -c .)
  ok "Kafka actif — $TOPIC_COUNT topics"
  echo ""
  printf "  ${BOLD}%-38s %12s %10s${NC}\n" "TOPIC" "MESSAGES" "PARTITIONS"
  printf "  %-38s %12s %10s\n" "──────────────────────────────────────" "────────────" "──────────"
  while IFS= read -r topic; do
    [ -z "$topic" ] && continue
    PARTS=$(docker exec forja_kafka kafka-topics \
      --bootstrap-server localhost:9092 --describe --topic "$topic" 2>/dev/null \
      | grep -c "Leader:" || echo 0)
    END=$(docker exec forja_kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 --topic "$topic" --time -1 2>/dev/null \
      | awk -F':' '{sum+=$3} END {print sum+0}')
    START=$(docker exec forja_kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 --topic "$topic" --time -2 2>/dev/null \
      | awk -F':' '{sum+=$3} END {print sum+0}')
    MSGS=$((END - START))
    if [ "$MSGS" -gt 0 ] 2>/dev/null; then
      printf "  ${GREEN}%-38s %12s %10s${NC}\n" "$topic" "$MSGS" "$PARTS"
    else
      printf "  ${YELLOW}%-38s %12s %10s${NC}\n" "$topic" "0" "$PARTS"
    fi
  done <<< "$TOPICS"
fi

# ═══════════════════════════════════════════════════
# 3. GA4 PRODUCER
# ═══════════════════════════════════════════════════
title "3. GA4 PRODUCER"
GA4_STATUS=$(docker inspect --format='{{.State.Status}}' ga4-producer 2>/dev/null)
if [ "$GA4_STATUS" == "running" ]; then
  ok "ga4-producer → running"
  info "Derniers logs GA4 Producer:"
  docker logs ga4-producer --tail=6 2>/dev/null \
    | grep -E "INFO|ERROR|WARN|event|batch|sent" \
    | while read -r line; do echo "     $line"; done
else
  fail "ga4-producer → ${GA4_STATUS:-introuvable}"
fi
echo ""
GA4_END=$(docker exec forja_kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic ga4.events --time -1 2>/dev/null \
  | awk -F':' '{sum+=$3} END {print sum+0}')
GA4_START=$(docker exec forja_kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic ga4.events --time -2 2>/dev/null \
  | awk -F':' '{sum+=$3} END {print sum+0}')
GA4_NET=$((GA4_END - GA4_START))
info "Messages dans ga4.events : ${BOLD}$GA4_NET${NC}"
info "Dernier message GA4 Producer (offset latest):"
docker exec forja_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ga4.events \
  --from-beginning \
  --max-messages 1 \
  --property print.timestamp=true \
  --timeout-ms 5000 2>/dev/null \
  | tail -1 | while read -r line; do echo "     $line"; done || warn "Impossible de lire le dernier message GA4"

# ═══════════════════════════════════════════════════
# 4. MINIO — BRONZE & SILVER
# ═══════════════════════════════════════════════════
title "4. MINIO — BRONZE / SILVER"
MINIO_HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9001 2>/dev/null)
if [ "$MINIO_HTTP" == "200" ]; then
  ok "MinIO UI accessible → http://$(hostname -I | awk '{print $1}'):9001"
else
  warn "MinIO UI HTTP $MINIO_HTTP"
fi
echo ""
docker exec forja_minio mc alias set local http://localhost:9000 minioadmin minioadmin123 --quiet 2>/dev/null

for LAYER in bronze silver; do
  echo -e "\n  ${BOLD}[ $LAYER ]${NC}"
  for SOURCE in ga4 snrt; do
    MPATH="local/forja-datalake/$LAYER/$SOURCE"
    COUNT=$(docker exec forja_minio mc ls "$MPATH" --recursive 2>/dev/null | grep -c "." || echo 0)
    SIZE=$(docker exec forja_minio mc du "$MPATH" 2>/dev/null | awk '{print $1}' | head -1)
    LAST_LINE=$(docker exec forja_minio mc ls "$MPATH" --recursive 2>/dev/null | sort | tail -1)
    LAST_FILE=$(echo "$LAST_LINE" | awk '{print $4}')
    LAST_DATE=$(echo "$LAST_LINE" | awk '{print $1, $2}')
    if [ "$COUNT" -gt "0" ] 2>/dev/null; then
      ok "$LAYER/$SOURCE → $COUNT fichiers | Taille: $SIZE"
      info "  Dernier fichier capturé : ${BOLD}$LAST_FILE${NC} le ${BOLD}$LAST_DATE${NC}"
    else
      warn "$LAYER/$SOURCE → vide ou inaccessible"
    fi
  done
done

# BRONZE — vérification fraîcheur (dernier fichier < 24h ?)
echo ""
info "BRONZE — Fraîcheur des données (dernière capture):"
for SOURCE in ga4 snrt; do
  MPATH="local/forja-datalake/bronze/$SOURCE"
  LAST_TS=$(docker exec forja_minio mc ls "$MPATH" --recursive 2>/dev/null \
    | sort | tail -1 | awk '{print $1"T"$2}' | sed 's/\.[0-9]*//' )
  if [ -n "$LAST_TS" ]; then
    LAST_EPOCH=$(date -d "$LAST_TS" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "$LAST_TS" +%s 2>/dev/null)
    NOW_EPOCH=$(date +%s)
    DIFF_H=$(( (NOW_EPOCH - LAST_EPOCH) / 3600 ))
    DIFF_M=$(( ((NOW_EPOCH - LAST_EPOCH) % 3600) / 60 ))
    if [ "$DIFF_H" -lt 24 ] 2>/dev/null; then
      ok "bronze/$SOURCE — dernière capture il y a ${DIFF_H}h ${DIFF_M}min ✔"
    else
      warn "bronze/$SOURCE — dernière capture il y a ${DIFF_H}h ${DIFF_M}min ⚠️ (> 24h !)"
    fi
  else
    fail "bronze/$SOURCE — impossible de lire la date de dernière capture"
  fi
done

# ═══════════════════════════════════════════════════
# 5. SILVER — DERNIER TRAITEMENT
# ═══════════════════════════════════════════════════
title "5. SILVER — DERNIER TRAITEMENT SPARK"
echo -e "\n  ${BOLD}[ SILVER GA4 ]${NC}"
MPATH_SGA4="local/forja-datalake/silver/ga4"
LAST_GA4=$(docker exec forja_minio mc ls "$MPATH_SGA4" --recursive 2>/dev/null | sort | tail -1)
LAST_GA4_FILE=$(echo "$LAST_GA4" | awk '{print $4}')
LAST_GA4_DATE=$(echo "$LAST_GA4" | awk '{print $1, $2}')
COUNT_SGA4=$(docker exec forja_minio mc ls "$MPATH_SGA4" --recursive 2>/dev/null | grep -c "." || echo 0)
if [ "$COUNT_SGA4" -gt "0" ] 2>/dev/null; then
  ok "silver/ga4 → $COUNT_SGA4 fichiers traités"
  info "Dernier fichier Silver GA4 : ${BOLD}$LAST_GA4_FILE${NC} le ${BOLD}$LAST_GA4_DATE${NC}"
else
  warn "silver/ga4 → aucun fichier trouvé"
fi

echo -e "\n  ${BOLD}[ SILVER SNRT ]${NC}"
MPATH_SSNRT="local/forja-datalake/silver/snrt"
LAST_SNRT=$(docker exec forja_minio mc ls "$MPATH_SSNRT" --recursive 2>/dev/null | sort | tail -1)
LAST_SNRT_FILE=$(echo "$LAST_SNRT" | awk '{print $4}')
LAST_SNRT_DATE=$(echo "$LAST_SNRT" | awk '{print $1, $2}')
COUNT_SSNRT=$(docker exec forja_minio mc ls "$MPATH_SSNRT" --recursive 2>/dev/null | grep -c "." || echo 0)
if [ "$COUNT_SSNRT" -gt "0" ] 2>/dev/null; then
  ok "silver/snrt → $COUNT_SSNRT fichiers traités"
  info "Dernier fichier Silver SNRT : ${BOLD}$LAST_SNRT_FILE${NC} le ${BOLD}$LAST_SNRT_DATE${NC}"
else
  warn "silver/snrt → aucun fichier trouvé"
fi

echo ""
info "Silver Transform — 5 dernières lignes de log:"
if [ -f ~/forja_pipeline/logs/silver_transform.log ]; then
  tail -5 ~/forja_pipeline/logs/silver_transform.log \
    | grep -E "INFO|ERROR|WARN|Silver|rows|transform" \
    | while read -r line; do echo "     $line"; done
else
  warn "silver_transform.log absent — vérification logs container Spark:"
  docker logs forja_spark_master --tail=10 2>/dev/null \
    | grep -E "INFO|ERROR|silver|Silver|rows" \
    | while read -r line; do echo "     $line"; done
fi

# ═══════════════════════════════════════════════════
# 6. SPARK CLUSTER
# ═══════════════════════════════════════════════════
title "6. SPARK CLUSTER"
SPARK_HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 2>/dev/null)
if [ "$SPARK_HTTP" == "200" ]; then
  ok "Spark Master UI → http://$(hostname -I | awk '{print $1}'):8080"
else
  fail "Spark Master UI inaccessible (HTTP $SPARK_HTTP)"
fi
echo ""
info "Containers Spark actifs:"
docker ps --format "{{.Names}}|{{.Status}}" 2>/dev/null | grep -i spark | \
  while IFS='|' read name status; do
    echo -e "     ${GREEN}✅ $name → $status${NC}"
  done

# ═══════════════════════════════════════════════════
# 7. POSTGRESQL — GOLD LAYER
# ═══════════════════════════════════════════════════
title "7. POSTGRESQL — GOLD LAYER"
PG_READY=$(docker exec forja_postgres pg_isready -U snrt_readonly -d snrt_stats 2>/dev/null)
if echo "$PG_READY" | grep -q "accepting"; then
  ok "PostgreSQL → $PG_READY"
else
  fail "PostgreSQL → ${PG_READY:-inaccessible}"
fi
echo ""
printf "  ${BOLD}%-38s %8s %12s %12s %26s${NC}\n" "TABLE" "LIGNES" "MIN DATE" "MAX DATE" "DERNIERE MAJ"
printf "  %-38s %8s %12s %12s %26s\n" \
  "──────────────────────────────────" "──────" "──────────" "──────────" "────────────────────────"

GOLD_Q="
SELECT 'gold_snrt_content_performance'::text,
       count(*)::text,
       min(report_month)::text,
       max(report_month)::text,
       TO_CHAR(max(updated_at),'DD/MM/YYYY HH24:MI:SS')
FROM gold_snrt_content_performance
UNION ALL
SELECT 'gold_ga4_daily_stats',
       count(*)::text,
       min(report_date)::text,
       max(report_date)::text,
       TO_CHAR(max(updated_at),'DD/MM/YYYY HH24:MI:SS')
FROM gold_ga4_daily_stats
UNION ALL
SELECT 'gold_snrt_engagement',
       count(*)::text,
       min(report_month)::text,
       max(report_month)::text,
       TO_CHAR(max(updated_at),'DD/MM/YYYY HH24:MI:SS')
FROM gold_snrt_engagement;
"
docker exec forja_postgres psql -U snrt_readonly -d snrt_stats \
  -t -A -F'|' -c "$GOLD_Q" 2>/dev/null | \
while IFS='|' read tbl rows minp maxp upd; do
  tbl=$(echo "$tbl"|xargs); rows=$(echo "$rows"|xargs)
  minp=$(echo "$minp"|xargs); maxp=$(echo "$maxp"|xargs); upd=$(echo "$upd"|xargs)
  if [ "${rows:-0}" -gt 0 ] 2>/dev/null; then
    printf "  ${GREEN}%-38s %8s %12s %12s %26s${NC}\n" "$tbl" "$rows" "$minp" "$maxp" "$upd"
  else
    printf "  ${RED}%-38s %8s %12s %12s %26s${NC}\n" "$tbl" "0" "-" "-" "-"
  fi
done

echo ""
info "Gold SNRT — Fraîcheur (dernière ligne insérée):"
docker exec forja_postgres psql -U snrt_readonly -d snrt_stats \
  -t -A -F'|' -c "
SELECT content_title,
       report_month,
       total_views,
       TO_CHAR(updated_at,'DD/MM/YYYY HH24:MI:SS') as maj
FROM gold_snrt_content_performance
ORDER BY updated_at DESC LIMIT 3;" 2>/dev/null | \
while IFS='|' read title month vues maj; do
  printf "     ${GREEN}%-40s | %s | %s vues | MAJ: %s${NC}\n" "$title" "$month" "$vues" "$maj"
done

echo ""
info "Gold GA4 — Dernière date capturée par report_type:"
docker exec forja_postgres psql -U snrt_readonly -d snrt_stats \
  -t -A -F'|' -c "
SELECT report_type,
       MAX(report_date) as last_date,
       COUNT(*) as nb_lignes,
       TO_CHAR(MAX(updated_at),'DD/MM/YYYY HH24:MI:SS') as derniere_maj
FROM gold_ga4_daily_stats
GROUP BY report_type
ORDER BY last_date DESC;" 2>/dev/null | \
while IFS='|' read rtype rdate nb maj; do
  printf "     ${CYAN}%-25s → last: %-12s | %s lignes | MAJ: %s${NC}\n" "$rtype" "$rdate" "$nb" "$maj"
done

echo ""
info "GA4 — Users & Sessions par appareil:"
docker exec forja_postgres psql -U snrt_readonly -d snrt_stats \
  -t -A -F'|' -c "
SELECT device_category,
       SUM(active_users)::bigint AS users,
       SUM(sessions)::bigint AS sessions
FROM gold_ga4_daily_stats
WHERE report_type='device_info'
GROUP BY device_category ORDER BY 2 DESC;" 2>/dev/null | \
while IFS='|' read dev users sess; do
  printf "     ${CYAN}%-12s → %s users | %s sessions${NC}\n" "$dev" "$users" "$sess"
done

echo ""
info "Top 5 contenus SNRT par vues:"
docker exec forja_postgres psql -U snrt_readonly -d snrt_stats \
  -t -A -F'|' -c "
SELECT content_title, SUM(total_views)::bigint
FROM gold_snrt_content_performance
GROUP BY content_title ORDER BY 2 DESC LIMIT 5;" 2>/dev/null | \
while IFS='|' read title vues; do
  printf "     ${GREEN}%-50s %s vues${NC}\n" "$title" "$vues"
done

# ═══════════════════════════════════════════════════
# 8. GOLD JOBS — LOGS DERNIERE EXECUTION
# ═══════════════════════════════════════════════════
title "8. GOLD JOBS — DERNIERE EXECUTION"
printf "  ${BOLD}%-30s %-10s %s${NC}\n" "JOB" "STATUT" "DERNIERE LIGNE DE LOG"
printf "  %-30s %-10s %s\n" "──────────────────────────────" "──────────" "────────────────────────────────"
LOG_JOBS=(
  "gold_snrt:~/forja_pipeline/logs/gold_snrt.log"
  "gold_ga4:~/forja_pipeline/logs/gold_ga4.log"
  "silver_transform:~/forja_pipeline/logs/silver_transform.log"
  "bronze_consumer:~/forja_pipeline/logs/bronze_consumer.log"
)
for entry in "${LOG_JOBS[@]}"; do
  JOB=$(echo "$entry" | cut -d: -f1)
  LOG=$(echo "$entry" | cut -d: -f2 | tr -d ' ')
  LOG="${LOG/#\~/$HOME}"
  if [ -f "$LOG" ]; then
    LAST=$(tail -1 "$LOG" 2>/dev/null)
    if echo "$LAST" | grep -qi "error\|exception\|fail"; then
      printf "  ${RED}%-30s %-10s %s${NC}\n" "$JOB" "❌ ERREUR" "$LAST"
    elif echo "$LAST" | grep -qi "success\|done\|complete\|finished"; then
      printf "  ${GREEN}%-30s %-10s %s${NC}\n" "$JOB" "✅ OK" "$LAST"
    else
      printf "  ${YELLOW}%-30s %-10s %s${NC}\n" "$JOB" "⚠️ INFO" "$LAST"
    fi
  else
    printf "  ${YELLOW}%-30s %-10s %s${NC}\n" "$JOB" "—" "log absent"
  fi
done

echo ""
info "Logs bronze_consumer (container) — 5 dernières lignes:"
docker logs forja_bronze_consumer --tail=5 2>/dev/null \
  | while read -r line; do echo "     $line"; done || warn "Container bronze_consumer non accessible"

# ═══════════════════════════════════════════════════
# 9. RESUME GLOBAL
# ═══════════════════════════════════════════════════
title "9. RESUME GLOBAL"
echo -e "  ${BOLD}Composant              Statut${NC}"
echo -e "  ──────────────────────────────────────────"

check_component() {
  local NAME=$1; local CMD=$2; local EXPECT=$3
  RESULT=$(eval "$CMD" 2>/dev/null)
  if echo "$RESULT" | grep -q "$EXPECT"; then
    ok "$NAME"
  else
    fail "$NAME"
  fi
}

check_component "Docker containers" \
  "docker ps --format '{{.Names}}' | grep -c forja" "^[1-9]"

check_component "Kafka broker" \
  "docker exec forja_kafka kafka-topics --bootstrap-server localhost:9092 --list" "."

check_component "MinIO stockage" \
  "curl -s -o /dev/null -w '%{http_code}' http://localhost:9001" "200"

check_component "Spark Master UI" \
  "curl -s -o /dev/null -w '%{http_code}' http://localhost:8080" "200"

check_component "PostgreSQL Gold" \
  "docker exec forja_postgres pg_isready -U snrt_readonly -d snrt_stats" "accepting"

check_component "Bronze GA4 (fichiers présents)" \
  "docker exec forja_minio mc ls local/forja-datalake/bronze/ga4 --recursive" "."

check_component "Bronze SNRT (fichiers présents)" \
  "docker exec forja_minio mc ls local/forja-datalake/bronze/snrt --recursive" "."

check_component "Silver GA4 (fichiers présents)" \
  "docker exec forja_minio mc ls local/forja-datalake/silver/ga4 --recursive" "."

check_component "Silver SNRT (fichiers présents)" \
  "docker exec forja_minio mc ls local/forja-datalake/silver/snrt --recursive" "."

check_component "Gold snrt_content_performance" \
  "docker exec forja_postgres psql -U snrt_readonly -d snrt_stats -t -c 'SELECT COUNT(*) FROM gold_snrt_content_performance'" "[1-9]"

check_component "Gold ga4_daily_stats" \
  "docker exec forja_postgres psql -U snrt_readonly -d snrt_stats -t -c 'SELECT COUNT(*) FROM gold_ga4_daily_stats'" "[1-9]"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║         ✅ HEALTH CHECK TERMINÉ                      ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

