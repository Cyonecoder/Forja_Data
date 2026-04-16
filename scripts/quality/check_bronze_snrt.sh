#!/usr/bin/env bash
set -euo pipefail

echo "=== BRONZE SNRT FILES ==="
docker exec forja_minio mc ls local/forja-datalake/bronze/snrt/ --recursive | tail -n 50 || true

echo
echo "=== BRONZE WATCHINGS DAYS ==="
docker exec forja_minio mc find local/forja-datalake/bronze/snrt/watchings/ --name "*.parquet" | \
sed -n 's#.*year=\([0-9]\+\)/month=\([0-9]\+\)/day=\([0-9]\+\)/.*#\1-\2-\3#p' | \
sort | uniq | tail -n 50 || true

echo
echo "=== BRONZE WATCHINGS FILE COUNT ==="
docker exec forja_minio mc find local/forja-datalake/bronze/snrt/watchings/ --name "*.parquet" | wc -l
