#!/usr/bin/env bash
# Live test of the alert path: Flink gauge -> Prometheus rule -> Alertmanager -> alert-sink -> sq.alert_events.
# A null-rate spike must FIRE an alert and recovery must RESOLVE it; Alertmanager is set to re-send every 10 s so the
# sink's de-duplication is exercised for real.
# Prereqs running: Kafka ($E2E_BOOTSTRAP), Postgres (E2E_PG_*). Needs:
#   KAFKA_HOME, PROM_HOME (dir with ./prometheus), AM_HOME (dir with ./alertmanager), PROM_REPORTER_JAR (flink-metrics-prometheus-2.2.1.jar)
set -euo pipefail
cd "$(dirname "$0")/.."
BOOT=${E2E_BOOTSTRAP:-127.0.0.1:19092}; PGH=${E2E_PG_HOST:-127.0.0.1}; PGP=${E2E_PG_PORT:-5432}; PGD=${E2E_PG_DB:-sq}
WORK=${E2E_WORK:-/tmp/sq-e2e-alerts}; DLQ=sq.e2e-dead-letter
: "${KAFKA_HOME:?}" "${PROM_HOME:?}" "${AM_HOME:?}" "${PROM_REPORTER_JAR:?}"
export JAVA_TOOL_OPTIONS=""; rm -rf "$WORK"; mkdir -p "$WORK"/{config,rules,prom,am,fconf}
PIDS=(); cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; }; trap cleanup EXIT

for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --delete --topic "$t" >/dev/null 2>&1 || true; done; sleep 2
for t in orders payments "$DLQ"; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOT" --create --topic "$t" --partitions 2 --replication-factor 1 >/dev/null; done
python3 scripts/pg_sql.py "TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat, sq.alert_events"

# job config: 5 s windows, registry off
sed -e "s#^bootstrap.servers=.*#bootstrap.servers=$BOOT#" -e "s#^sq.dlq.topic=.*#sq.dlq.topic=$DLQ#" config/kafka.properties > "$WORK/config/kafka.properties"
sed -e "s#^sq.registry.enabled=.*#sq.registry.enabled=false#" config/registry.properties > "$WORK/config/registry.properties"
sed -e 's/size: 60s/size: 5s/' -e 's/max_out_of_orderness: 5s/max_out_of_orderness: 2s/' -e 's/min_history: 5/min_history: 3/' config/thresholds.yaml > "$WORK/config/thresholds.yaml"
# Prometheus: fast scrape/eval, WARN delay shortened (test only)
sed -e 's/^        for: 2m/        for: 10s/' observability/prometheus/rules/sq.yml > "$WORK/rules/sq.yml"
cat > "$WORK/prometheus.yml" <<EOT
global: { scrape_interval: 3s, evaluation_interval: 3s }
rule_files: [$WORK/rules/*.yml]
alerting: { alertmanagers: [{ static_configs: [{ targets: ["127.0.0.1:19094"] }] }] }
scrape_configs: [{ job_name: flink, static_configs: [{ targets: ["127.0.0.1:9249"] }] }]
EOT
sed -e 's#http://alert-sink:5001#http://127.0.0.1:15001#' -e 's/group_wait: 10s/group_wait: 2s/' -e 's/group_interval: 1m/group_interval: 5s/' -e 's/repeat_interval: 4h/repeat_interval: 10s/' \
    observability/alertmanager/alertmanager.yml > "$WORK/am.yml"
printf 'metrics.reporters: prom\nmetrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory\nmetrics.reporter.prom.port: 9249\n' > "$WORK/fconf/config.yaml"

POSTGRES_HOST=$PGH POSTGRES_PORT=$PGP POSTGRES_DB=$PGD POSTGRES_USER=sq_writer POSTGRES_PASSWORD=sq_writer_pw PORT=15001 python3 -u tools/alert-sink/app.py > "$WORK/sink.log" 2>&1 & PIDS+=($!)
"$AM_HOME/alertmanager" --config.file="$WORK/am.yml" --storage.path="$WORK/am" --web.listen-address=127.0.0.1:19094 --cluster.listen-address= > "$WORK/am.log" 2>&1 & PIDS+=($!)
"$PROM_HOME/prometheus" --config.file="$WORK/prometheus.yml" --storage.tsdb.path="$WORK/prom" --web.listen-address=127.0.0.1:19090 > "$WORK/prom.log" 2>&1 & PIDS+=($!)

( cd flink-job && mvn -q -B -DskipTests compile && mvn -q -B dependency:build-classpath -Dmdep.outputFile="$WORK/cp.txt" )
SLF4J=$(find ~/.m2 -name 'slf4j-simple-*.jar' | head -1)
FLINK_CONF_DIR="$WORK/fconf" java -cp "flink-job/target/classes:$(cat "$WORK/cp.txt"):$SLF4J:$PROM_REPORTER_JAR" io.streamquality.StreamQualityJob --config-dir "$WORK/config" \
  --postgres.url "jdbc:postgresql://$PGH:$PGP/$PGD" --postgres.user sq_writer --postgres.password sq_writer_pw --heartbeat.interval.sec 5 > "$WORK/job.log" 2>&1 & PIDS+=($!)
for _ in $(seq 1 60); do grep -q "Assigned to partition" "$WORK/job.log" && break; sleep 1; done

python3 generator/generate.py --config "$WORK/config/kafka.properties" --scenario generator/scenarios/alerts.yaml > "$WORK/gen.log" 2>&1
echo "traffic done; waiting for the resolve notification..."; sleep 25
python3 scripts/e2e_alerts_assert.py "$WORK/sink.log"
