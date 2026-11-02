#!/usr/bin/env bash
# End-to-end: real Kafka + Postgres + the job (compact mode) in containers, a tiny test producer, assertions on what lands
# in Postgres and the dead-letter topic. Needs Docker, a built jar (cd flink-job && mvn package -DskipTests) and python3 + psycopg2.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f flink-job/target/stream-quality-job.jar ] || { echo "build the jar first: (cd flink-job && mvn -B package -DskipTests)"; exit 2; }
C="docker compose -f docker-compose.e2e.yml"
trap 'status=$?; [ $status -ne 0 ] && $C logs --tail=60 job || true; $C down -v >/dev/null 2>&1; exit $status' EXIT
$C up -d --build --wait kafka postgres
for t in e2e.orders e2e.quiet sq.dead-letter; do
  $C exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null
done
$C up -d job
export E2E_PG_HOST=127.0.0.1 E2E_PG_PORT=15433 E2E_PG_ADMIN=sq_admin E2E_PG_ADMIN_PASSWORD=admin
python3 scripts/e2e_check.py
