#!/usr/bin/env bash
# Pre-creates the dead-letter topic on the EXTERNAL cluster. Reads config/kafka.properties.
# Uses Kafka's own CLI via the apache/kafka image (no local Kafka install needed).
set -euo pipefail
cd "$(dirname "$0")/.."
PROPS=${SQ_KAFKA_PROPS:-config/kafka.properties}
get() { grep -E "^$1=" "$PROPS" | head -1 | cut -d= -f2-; }
DLQ=$(get sq.dlq.topic); DLQ=${DLQ:-sq.dead-letter}
PARTS=${DLQ_PARTITIONS:-3}; RF=${DLQ_REPLICATION:-1}; RETENTION_MS=${DLQ_RETENTION_MS:-604800000}

# Client props = everything except sq.* ; ${env:NAME} resolved from the environment (same syntax as the Flink job).
TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
grep -vE '^(#|sq\.|$)' "$PROPS" | perl -pe 's/\$\{env:(\w+)\}/defined $ENV{$1} ? $ENV{$1} : die "env var $1 not set (referenced in $ARGV)\n"/ge' > "$TMP"
BOOT=$(get bootstrap.servers)

if [ -n "${KAFKA_HOME:-}" ]; then CLIENT_PROPS="$TMP"; else CLIENT_PROPS=/client.properties; fi
ARGS=(--bootstrap-server "$BOOT" --command-config "$CLIENT_PROPS" --create --if-not-exists --topic "$DLQ"
      --partitions "$PARTS" --replication-factor "$RF" --config retention.ms="$RETENTION_MS")
if [ -n "${KAFKA_HOME:-}" ]; then            # local Kafka CLI (tests / air-gapped)
  "$KAFKA_HOME/bin/kafka-topics.sh" "${ARGS[@]}"
else                                         # no Kafka install needed: use the Apache Kafka image's CLI
  docker run --rm --add-host=host.docker.internal:host-gateway -v "$TMP:/client.properties:ro" \
    apache/kafka:4.0.0 /opt/kafka/bin/kafka-topics.sh "${ARGS[@]}"
fi
echo "dead-letter topic '$DLQ' ready"
