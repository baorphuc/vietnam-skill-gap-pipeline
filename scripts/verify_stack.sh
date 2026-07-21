#!/bin/bash
# scripts/verify_stack.sh
# Run after docker-compose up to confirm all services are healthy

set -e

BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

ok()   { echo -e "${GREEN}✓${RESET} $1"; }
warn() { echo -e "${YELLOW}⚠${RESET} $1"; }
fail() { echo -e "${RED}✗${RESET} $1"; exit 1; }

echo -e "\n${BOLD}=== Vietnam Skill Gap Pipeline — Stack Verification ===${RESET}\n"

# ── Kafka ──────────────────────────────────────────────────
echo -e "${BOLD}[1/4] Kafka${RESET}"
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 \
  && ok "Kafka broker is reachable" \
  || fail "Kafka broker not reachable"

# Create topic if not exists
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw.jobs \
  --partitions 3 \
  --replication-factor 1 > /dev/null 2>&1
ok "Topic 'raw.jobs' ready"

# ── MinIO ──────────────────────────────────────────────────
echo -e "\n${BOLD}[2/4] MinIO${RESET}  (Kafka UI đang tắt mặc định để tiết kiệm RAM)"
curl -sf http://localhost:9000/minio/health/live > /dev/null \
  && ok "MinIO is healthy" \
  || fail "MinIO health check failed"

docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1
for bucket in "skillgap/bronze/raw_json" "skillgap/bronze/parsed_parquet" "skillgap/silver" "skillgap/gold"; do
  docker exec minio mc ls local/$bucket > /dev/null 2>&1 \
    && ok "Bucket '$bucket' exists" \
    || warn "Bucket '$bucket' missing — re-run minio-init"
done

# ── Spark ──────────────────────────────────────────────────
echo -e "\n${BOLD}[3/4] Spark${RESET}"
curl -sf http://localhost:8080 > /dev/null \
  && ok "Spark Master UI reachable at http://localhost:8080" \
  || warn "Spark Master UI not reachable yet"

# ── Airflow ────────────────────────────────────────────────
echo -e "\n${BOLD}[4/4] Airflow${RESET}"
curl -sf http://localhost:8081/health > /dev/null \
  && ok "Airflow Webserver healthy at http://localhost:8081 (admin/admin)" \
  || warn "Airflow Webserver not ready yet — may still be initializing"

echo -e "\n${BOLD}=== UI Endpoints ===${RESET}"
echo "  Kafka UI    → (tắt mặc định — bỏ comment trong docker-compose.yml nếu cần)"
echo "  MinIO UI    → http://localhost:9001  (minioadmin / minioadmin)"
echo "  Spark UI    → http://localhost:8080"
echo "  Airflow UI  → http://localhost:8081  (admin / admin)"
echo ""
