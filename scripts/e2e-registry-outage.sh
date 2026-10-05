#!/usr/bin/env bash
# Graceful degradation test: the job starts while the schema registry is DOWN, then the registry comes up.
# Expect: statistical checks run throughout; no structural rows (and no failures) while down; structural resumes
# once the registry is back and the circuit breaker lets a trial call through.
# Same prereqs as e2e-local.sh plus hooks:  E2E_REGISTRY_STOP / E2E_REGISTRY_START (shell commands; START must return quickly)
set -euo pipefail
cd "$(dirname "$0")/.."
BOOT=${E2E_BOOTSTRAP:-127.0.0.1:19092}; CH=${E2E_CH_URL:-http://127.0.0.1:8123}; CH_ADMIN=${E2E_CH_ADMIN:-default:admin}
REG=${E2E_REGISTRY_URL:-http://127.0.0.1:18081/apis/registry/v3}; DLQ=sq.e2e-dead-letter; WORK=${E2E_WORK:-/tmp/sq-e2e-outage}
: "${KAFKA_HOME:?}" "${E2E_REGISTRY_STOP:?}" "${E2E_REGISTRY_START:?}"
export JAVA_TOOL_OPTIONS=""; rm -rf "$WORK"; mkdir -p "$WORK/config"

for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 2
for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --create --topic "$t" --partitions 2 --replication-factor 1 >/dev/null; done
for tbl in check_results violations latest_status job_heartbeat; do curl -sf -u "$CH_ADMIN" "$CH" --data-binary "TRUNCATE TABLE sq.$tbl" >/dev/null; done

eval "$E2E_REGISTRY_STOP" || true
for _ in $(seq 1 20); do curl -sf -m 2 "$REG/system/info" >/dev/null 2>&1 || break; sleep 1; done
curl -sf -m 2 "$REG/system/info" >/dev/null 2>&1 && { echo "registry still up; cannot test outage"; exit 2; }
echo "registry is DOWN"

sed -e "s#^bootstrap.servers=.*#bootstrap.servers=$BOOT#" -e "s#^sq.dlq.topic=.*#sq.dlq.topic=$DLQ#" config/kafka.properties > "$WORK/config/kafka.properties"
sed -e "s#^sq.registry.enabled=.*#sq.registry.enabled=true#" -e "s#^sq.registry.url=.*#sq.registry.url=$REG#" \
    -e "s#^sq.registry.breaker.reset.ms=.*#sq.registry.breaker.reset.ms=5000#" -e "s#^sq.registry.timeout.ms=.*#sq.registry.timeout.ms=1500#" -e "s#^sq.registry.refresh.interval.ms=.*#sq.registry.refresh.interval.ms=5000#" \
    config/registry.properties > "$WORK/config/registry.properties"
sed -e 's/size: 60s/size: 5s/' -e 's/max_out_of_orderness: 5s/max_out_of_orderness: 2s/' -e 's/min_history: 5/min_history: 3/' config/thresholds.yaml > "$WORK/config/thresholds.yaml"

( cd flink-job && mvn -q -B -DskipTests compile && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$WORK/cp.txt" )
SLF4J=$(find ~/.m2 -name 'slf4j-simple-*.jar' | head -1)
java -cp "flink-job/target/classes:$(cat "$WORK/cp.txt"):$SLF4J" io.streamquality.StreamQualityJob --config-dir "$WORK/config" \
  --clickhouse.url "$CH" --clickhouse.user "${CH_ADMIN%%:*}" --clickhouse.password "${CH_ADMIN#*:}" --heartbeat.interval.sec 3 > "$WORK/job.log" 2>&1 &
JOB=$!; trap 'kill $JOB 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do grep -q "Assigned to partition" "$WORK/job.log" && break; sleep 1; done

python3 generator/generate.py --config "$WORK/config/kafka.properties" --scenario generator/scenarios/steady.yaml > "$WORK/gen.log" 2>&1 &
GEN=$!; T0=$(date +%s)
sleep 25
echo "starting registry..."; eval "$E2E_REGISTRY_START"
for _ in $(seq 1 90); do curl -sf -m 2 "$REG/system/info" >/dev/null 2>&1 && break; sleep 1; done
python3 scripts/register_schemas.py --url "$REG" >/dev/null; T_UP=$(date +%s)
echo "registry UP and schemas registered after $((T_UP - T0))s; waiting for generator + windows to close"
wait $GEN; sleep 12
python3 scripts/e2e_outage_assert.py "$CH" "$CH_ADMIN" "$T0" "$T_UP"
