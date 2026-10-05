#!/usr/bin/env bash
# Pre-creates the dead-letter topic on the EXTERNAL cluster. Reads config/kafka.properties.
# Uses Kafka's own CLI via the apache/kafka image (no local Kafka install needed).
set -euo pipefail
cd "$(dirname "$0")/.."
PROPS=config/kafka.properties
get() { grep -E "^$1=" "$PROPS" | head -1 | cut -d= -f2-; }
DLQ=$(get sq.dlq.topic); DLQ=${DLQ:-sq.dead-letter}
PARTS=${DLQ_PARTITIONS:-3}; RF=${DLQ_REPLICATION:-1}; RETENTION_MS=${DLQ_RETENTION_MS:-604800000}

# Client props = everything except sq.* (env refs resolved by the shell via envsubst if present)
TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
grep -vE '^(#|sq\.|$)' "$PROPS" | { command -v envsubst >/dev/null && envsubst || cat; } \
  | sed -E 's/\$\{env:([A-Za-z0-9_]+)\}/${\1}/g' > "$TMP"
BOOT=$(get bootstrap.servers)

docker run --rm --add-host=host.docker.internal:host-gateway -v "$TMP:/client.properties:ro" \
  apache/kafka:4.2.2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" \
  --command-config /client.properties --create --if-not-exists --topic "$DLQ" \
  --partitions "$PARTS" --replication-factor "$RF" --config retention.ms="$RETENTION_MS"
echo "dead-letter topic '$DLQ' ready"
