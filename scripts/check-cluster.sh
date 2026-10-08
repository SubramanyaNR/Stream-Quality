#!/usr/bin/env bash
# Verifies cluster.env before `make up`: can containers on the Kafka network and this host reach every broker?
set -uo pipefail
. "$(dirname "$0")/lib.sh"
ok() { printf '  ok    %s\n' "$*"; }
bad() { printf '  FAIL  %s\n' "$*"; FAILED=1; }
FAILED=0
TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
client_props "$TMP"
BOOT=$(get bootstrap.servers)

echo "1. Docker network '$NET'"
docker network inspect "$NET" >/dev/null 2>&1 && ok "exists" || { bad "not found - set KAFKA_DOCKER_NETWORK (docker network ls)"; exit 1; }

echo "2. Brokers reachable from containers on '$NET' via $BOOT"
OUT=$(kcli "$TMP" kafka-broker-api-versions.sh --bootstrap-server "$BOOT" --command-config /client.properties 2>&1)
if [ $? -eq 0 ]; then
  echo "$OUT" | grep -E '^\S+:[0-9]+ \(id: ' | sed 's/ -> (//' | while read -r line; do ok "broker ${line%% -*}"; done
else
  bad "cannot connect. Usually the broker advertises an address this network cannot resolve (see docs/KAFKA_CONNECTIVITY.md)"
  echo "$OUT" | grep -iE 'error|exception|timed out' | head -3 | sed 's/^/        /'
fi

echo "3. Dead-letter and monitored topics"
TOPICS=$(kcli "$TMP" kafka-topics.sh --bootstrap-server "$BOOT" --command-config /client.properties --list 2>/dev/null)
DLQ=$(get sq.dlq.topic)
echo "$TOPICS" | grep -qx "$DLQ" && ok "dead-letter topic '$DLQ' exists" || echo "  warn  dead-letter topic '$DLQ' missing (make up / make dlq creates it)"
PAT=$(get sq.source.topic.pattern); EXC=$(get sq.source.topic.exclude)
if [ -n "$PAT" ]; then
  N=$(echo "$TOPICS" | grep -E "^($PAT)\$" | { [ -n "$EXC" ] && grep -vE "^($EXC)\$" || cat; } | grep -vx "$DLQ" | wc -l)
  [ "$N" -gt 0 ] && ok "$N topic(s) match TOPICS_PATTERN" || echo "  warn  no topic matches TOPICS_PATTERN yet (picked up when created)"
fi

echo "4. This host via ${KAFKA_BOOTSTRAP_HOST:-<unset>}"
if [ -n "${KAFKA_BOOTSTRAP_HOST:-}" ]; then
  HP=$(mktemp); trap 'rm -f "$TMP" "$HP"' EXIT; client_props "$HP"
  docker run --rm --network host -v "$HP:/client.properties:ro" "$CLI_IMAGE" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_HOST" --command-config /client.properties >/dev/null 2>&1 \
    && ok "host can reach the brokers (generator will work)" || bad "host cannot reach $KAFKA_BOOTSTRAP_HOST"
fi

[ "$FAILED" -eq 0 ] && echo "all good" || { echo "problems found"; exit 1; }
