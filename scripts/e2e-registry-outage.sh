#!/usr/bin/env bash
# Graceful degradation test: the job starts while the schema registry is DOWN (cold cache), then the registry comes back.
#   E2E_REGISTRY_TYPE=apicurio (default) | confluent | karapace | redpanda | ccompat      E2E_REGISTRY_URL (see e2e-local.sh)
#   E2E_SERIALIZATION=json (default) | confluent-json | confluent-avro
# json / confluent-json: statistical checks run throughout; structural rows (and failures) appear only once the registry is back.
# confluent-avro: Avro cannot be read without the registry, so while it is down records are COUNTED (volume) but ignored by every
#   field-level check (no 100%-null false alarms, no dead-letter flood); everything resumes on recovery.
# Same prereqs as e2e-local.sh plus hooks:  E2E_REGISTRY_STOP / E2E_REGISTRY_START (shell commands; START must return quickly)
set -euo pipefail
cd "$(dirname "$0")/.."
BOOT=${E2E_BOOTSTRAP:-127.0.0.1:19092}
PGURL="jdbc:postgresql://${E2E_PG_HOST:-127.0.0.1}:${E2E_PG_PORT:-5432}/${E2E_PG_DB:-sq}"
RTYPE=${E2E_REGISTRY_TYPE:-apicurio}; SER=${E2E_SERIALIZATION:-json}; DLQ=sq.e2e-dead-letter; WORK=${E2E_WORK:-/tmp/sq-e2e-outage}
if [ "$RTYPE" = apicurio ]; then REG=${E2E_REGISTRY_URL:-http://127.0.0.1:18081/apis/registry/v3}; PING="$REG/system/info"
else REG=${E2E_REGISTRY_URL:-http://127.0.0.1:18082}; PING="$REG/config"; fi
: "${KAFKA_HOME:?}" "${E2E_REGISTRY_STOP:?}" "${E2E_REGISTRY_START:?}"
export JAVA_TOOL_OPTIONS=""; rm -rf "$WORK"; mkdir -p "$WORK/config"
up() { curl -sf -m 2 "$PING" >/dev/null 2>&1; }
wait_up() { for _ in $(seq 1 90); do up && return 0; sleep 1; done; echo "registry did not come up"; exit 2; }

for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 2
for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --create --topic "$t" --partitions 2 --replication-factor 1 >/dev/null; done
python3 scripts/pg_sql.py "TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat"

sed -e "s#^bootstrap.servers=.*#bootstrap.servers=$BOOT#" -e "s#^sq.dlq.topic=.*#sq.dlq.topic=$DLQ#" config/kafka.properties > "$WORK/config/kafka.properties"
sed -e "s#^sq.registry.enabled=.*#sq.registry.enabled=true#" -e "s#^sq.registry.url=.*#sq.registry.url=$REG#" -e "s#^sq.registry.type=.*#sq.registry.type=$RTYPE#" \
    -e "s#^sq.registry.breaker.reset.ms=.*#sq.registry.breaker.reset.ms=5000#" -e "s#^sq.registry.timeout.ms=.*#sq.registry.timeout.ms=1500#" -e "s#^sq.registry.refresh.interval.ms=.*#sq.registry.refresh.interval.ms=5000#" \
    config/registry.properties > "$WORK/config/registry.properties"
sed -e 's/size: 60s/size: 5s/' -e 's/max_out_of_orderness: 5s/max_out_of_orderness: 2s/' -e 's/min_history: 5/min_history: 3/' config/thresholds.yaml > "$WORK/config/thresholds.yaml"
( cd flink-job && mvn -q -B -DskipTests compile && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$WORK/cp.txt" )
SLF4J=$(find ~/.m2 -name 'slf4j-simple-*.jar' | head -1)
GEN_ARGS=(--config "$WORK/config/kafka.properties" --scenario generator/scenarios/steady.yaml --serialization "$SER" --registry-url "$REG")
start_job() {
  java -cp "flink-job/target/classes:$(cat "$WORK/cp.txt"):$SLF4J" io.streamquality.StreamQualityJob --config-dir "$WORK/config" \
    --postgres.url "$PGURL" --postgres.user sq_writer --postgres.password sq_writer_pw --heartbeat.interval.sec 3 > "$WORK/job.log" 2>&1 &
  JOB=$!; trap 'kill $JOB 2>/dev/null || true' EXIT
  for _ in $(seq 1 60); do grep -q "Assigned to partition" "$WORK/job.log" && break; sleep 1; done
}

if [ "$SER" = json ]; then
  eval "$E2E_REGISTRY_STOP" || true
  for _ in $(seq 1 20); do up || break; sleep 1; done
  up && { echo "registry still up; cannot test outage"; exit 2; }
  echo "registry is DOWN"
  start_job
  python3 generator/generate.py "${GEN_ARGS[@]}" > "$WORK/gen.log" 2>&1 &
  GEN=$!; T_REF=$(date +%s)
  sleep 25
else
  # The generator must register its schemas, so it starts while the registry is still up; the registry is then taken down
  # BEFORE the job starts, so the job's schema cache is cold and every id lookup fails.
  up || { eval "$E2E_REGISTRY_START"; wait_up; }
  python3 scripts/register_schemas.py --type "$RTYPE" --url "$REG" >/dev/null
  python3 generator/generate.py "${GEN_ARGS[@]}" > "$WORK/gen.log" 2>&1 &
  GEN=$!; sleep 4
  eval "$E2E_REGISTRY_STOP" || true
  for _ in $(seq 1 20); do up || break; sleep 1; done
  up && { echo "registry still up; cannot test outage"; exit 2; }
  echo "registry is DOWN (cold-cache job start)"
  start_job; T_REF=$(date +%s)
  sleep 30
fi
echo "starting registry..."; eval "$E2E_REGISTRY_START"; wait_up
python3 scripts/register_schemas.py --type "$RTYPE" --url "$REG" >/dev/null; T_UP=$(date +%s)
echo "registry UP and schemas registered $((T_UP - T_REF))s after the job started consuming; waiting for generator + windows to close"
wait $GEN; sleep 12
python3 scripts/e2e_outage_assert.py "$T_REF" "$T_UP" "$SER" "$BOOT" "$DLQ"
