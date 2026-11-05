#!/usr/bin/env bash
# `make demo`: shows the monitor reacting to real faults on YOUR Kafka cluster, without touching your topics.
#  1. creates the topic demo.payments
#  2. restarts the job (compact mode) watching only demo.* with config/demo/thresholds.yaml (20 s windows)
#  3. streams demo traffic that cycles through healthy -> null spike -> wrong types -> new field -> silence
# Open Grafana (http://localhost:3000) while it runs. Ctrl-C stops the traffic; `make demo-stop` puts the job back on your
# own topics and thresholds. Needs: the stack running in compact mode (make up), python3 on this host.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
TOPIC=demo.payments
TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
client_props "$TMP"
BOOT=$(get bootstrap.servers)

kcli "$TMP" kafka-topics.sh --bootstrap-server "$BOOT" --command-config /client.properties --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor "${DLQ_REPLICATION:-1}" >/dev/null
echo "topic $TOPIC ready"

export COMPOSE_ENV_FILES=cluster.env SQ_DEMO_THRESHOLDS=/opt/sq/config/demo/thresholds.yaml SQ_DEMO_TOPICS_PATTERN='demo\..*'
docker compose up -d sq-job >/dev/null
echo "job restarted in demo mode (watching demo.*). Dashboards: http://localhost:3000  - first alerts after ~2 minutes of warm-up."
echo "Ctrl-C stops the traffic; run 'make demo-stop' to return the job to your own topics."

python3 scripts/demo_producer.py | docker run --rm -i --network "$NET" --add-host=host.docker.internal:host-gateway \
  -v "$TMP:/client.properties:ro" "$CLI_IMAGE" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOT" --producer.config /client.properties --topic "$TOPIC"
