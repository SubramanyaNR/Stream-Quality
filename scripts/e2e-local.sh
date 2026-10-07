#!/usr/bin/env bash
# Docker-free end-to-end test: real Kafka (KRaft) + real Postgres + the Flink job on an embedded
# mini-cluster + the real generator. Prereqs (already running):
#   Kafka   at $E2E_BOOTSTRAP     (default 127.0.0.1:19092), topics auto-created here
#   Postgres at $E2E_PG_HOST:$E2E_PG_PORT (default 127.0.0.1:5432), database sq, with postgres/init/01_init.sql applied
#     (admin user via E2E_PG_ADMIN / E2E_PG_ADMIN_PASSWORD, default postgres/none)
#   Apicurio (optional) at $E2E_REGISTRY_URL (default http://127.0.0.1:18081/apis/registry/v3): enables structural checks
# Needs: $KAFKA_HOME (for kafka-topics.sh), maven, java, python3 with kafka-python, PyYAML, psycopg2.
set -euo pipefail
cd "$(dirname "$0")/.."
BOOT=${E2E_BOOTSTRAP:-127.0.0.1:19092}
PGURL="jdbc:postgresql://${E2E_PG_HOST:-127.0.0.1}:${E2E_PG_PORT:-5432}/${E2E_PG_DB:-sq}"
DLQ=sq.e2e-dead-letter; WORK=${E2E_WORK:-/tmp/sq-e2e}
: "${KAFKA_HOME:?set KAFKA_HOME}"
export JAVA_TOOL_OPTIONS=""
rm -rf "$WORK"; mkdir -p "$WORK/config"

for t in orders payments "$DLQ"; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1 || true
done
sleep 2
for t in orders payments "$DLQ"; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --create --topic "$t" --partitions 2 --replication-factor 1 >/dev/null
done
python3 scripts/pg_sql.py "TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat"

sed -e "s#^bootstrap.servers=.*#bootstrap.servers=$BOOT#" -e "s#^sq.dlq.topic=.*#sq.dlq.topic=$DLQ#" \
    -e "s#^sq.source.topics=.*#sq.source.topics=orders,payments#" config/kafka.properties > "$WORK/config/kafka.properties"
REG=${E2E_REGISTRY_URL:-http://127.0.0.1:18081/apis/registry/v3}; REG_ON=false
if curl -sf -m 3 "$REG/system/info" >/dev/null; then
  REG_ON=true; python3 scripts/register_schemas.py --url "$REG" >/dev/null
else echo "registry not reachable at $REG -> structural checks disabled for this run"; fi
sed -e "s#^sq.registry.enabled=.*#sq.registry.enabled=$REG_ON#" -e "s#^sq.registry.url=.*#sq.registry.url=$REG#" config/registry.properties > "$WORK/config/registry.properties"
sed -e 's/size: 60s/size: 5s/' -e 's/max_out_of_orderness: 5s/max_out_of_orderness: 2s/' \
    -e 's/min_history: 5/min_history: 3/' config/thresholds.yaml > "$WORK/config/thresholds.yaml"

( cd flink-job && mvn -q -B -DskipTests compile && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$WORK/cp.txt" )
SLF4J=$(find ~/.m2 -name 'slf4j-simple-*.jar' | head -1)
java -cp "flink-job/target/classes:$(cat "$WORK/cp.txt"):$SLF4J" io.streamquality.StreamQualityJob \
  --config-dir "$WORK/config" --postgres.url "$PGURL" --postgres.user sq_writer --postgres.password sq_writer_pw \
  --heartbeat.interval.sec 5 > "$WORK/job.log" 2>&1 &
JOB=$!; trap 'kill $JOB 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do grep -q "Assigned to partition" "$WORK/job.log" && break; sleep 1; done
grep -q "Assigned to partition" "$WORK/job.log" || { echo "job did not start"; tail -30 "$WORK/job.log"; exit 1; }

SINCE=$(date +%s)
python3 generator/generate.py --config "$WORK/config/kafka.properties" --scenario generator/scenarios/e2e.yaml
echo "waiting for last windows to close..."; sleep 14
python3 scripts/e2e_assert.py "$BOOT" "$DLQ" "$SINCE" "$REG_ON"
