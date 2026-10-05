#!/usr/bin/env bash
set -euo pipefail
# Submit once; if a job with the same name is already RUNNING, do nothing (idempotent `make up`).
JM=flink-jobmanager:8081
if /opt/flink/bin/flink list -m "$JM" -r 2>/dev/null | grep -q "stream-quality-monitor"; then
  echo "job already running"; exit 0
fi
exec /opt/flink/bin/flink run -m "$JM" -d "$JOB_JAR" --config-dir "${SQ_CONFIG_DIR:-/opt/sq/config}"
