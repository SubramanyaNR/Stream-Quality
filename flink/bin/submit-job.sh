#!/usr/bin/env bash
set -euo pipefail
# The job builds its environment from conf/config.yaml, so the client must find the JobManager there.
printf 'rest.address: flink-jobmanager\nrest.port: 8081\n' >> /opt/flink/conf/config.yaml
# Submit once; if a job with the same name is already RUNNING, do nothing (idempotent `make up`).
if /opt/flink/bin/flink list -r 2>/dev/null | grep -q "stream-quality-monitor"; then
  echo "job already running"; exit 0
fi
exec /opt/flink/bin/flink run -d "$JOB_JAR" --config-dir "${SQ_CONFIG_DIR:-/opt/sq/config}"
