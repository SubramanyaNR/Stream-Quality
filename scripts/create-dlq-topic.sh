#!/usr/bin/env bash
# Pre-creates the dead-letter topic on the cluster named in cluster.env.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
DLQ=$(get sq.dlq.topic); DLQ=${DLQ:-sq.dead-letter}
PARTS=${DLQ_PARTITIONS:-3}; RF=${DLQ_REPLICATION:-1}; RETENTION_MS=${DLQ_RETENTION_MS:-604800000}

TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
client_props "$TMP"
BOOT=$(get bootstrap.servers)

kcli "$TMP" kafka-topics.sh --bootstrap-server "$BOOT" --command-config /client.properties --create --if-not-exists \
  --topic "$DLQ" --partitions "$PARTS" --replication-factor "$RF" --config retention.ms="$RETENTION_MS"
echo "dead-letter topic '$DLQ' ready"
