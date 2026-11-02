#!/usr/bin/env bash
# Compact mode: the whole job (JobManager + TaskManager) in ONE JVM using Flink's own jars from /opt/flink/lib.
# State survives restarts: checkpoints go to SQ_STATE_DIR (a volume) and are retained on shutdown; on start the newest
# one is restored, so volume/seasonal baselines and Kafka offsets carry over.
set -euo pipefail
STATE_DIR="${SQ_STATE_DIR:-/var/lib/sq/state}"
mkdir -p "$STATE_DIR"
# Pick the newest retained checkpoint that has a _metadata file (a half-written one is skipped).
RESTORE=""
for d in $(ls -1dt "$STATE_DIR"/*/chk-* 2>/dev/null); do
  if [ -f "$d/_metadata" ]; then RESTORE="$d"; break; fi
done
CONF=/tmp/sq-flink-conf; mkdir -p "$CONF"
cat > "$CONF/config.yaml" <<CFG
state.checkpoints.dir: file://$STATE_DIR
execution.checkpointing.interval: 30s
execution.checkpointing.mode: AT_LEAST_ONCE
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
state.checkpoints.num-retained: 2
restart-strategy.type: exponential-delay
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
rest.bind-address: 0.0.0.0
taskmanager.memory.managed.size: ${SQ_MANAGED_MB:-16}m
taskmanager.memory.network.min: 16m
taskmanager.memory.network.max: 32m
taskmanager.memory.framework.off-heap.size: 32m
taskmanager.memory.jvm-metaspace.size: 96m
taskmanager.numberOfTaskSlots: 4
CFG
[ -n "$RESTORE" ] && { echo "restoring state from $RESTORE"; echo "execution.state-recovery.path: file://$RESTORE" >> "$CONF/config.yaml"; } || echo "no retained checkpoint: starting fresh"
export FLINK_CONF_DIR="$CONF"
exec java -Xmx"${SQ_HEAP:-256m}" -Xss512k -XX:+UseSerialGC -XX:MaxMetaspaceSize=128m -XX:ReservedCodeCacheSize=64m -XX:MaxDirectMemorySize=96m \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  -cp "/opt/flink/lib/*:/opt/flink/plugins/metrics-prometheus/*:${JOB_JAR:-/opt/flink/usrlib/stream-quality-job.jar}" \
  io.streamquality.StreamQualityJob --config-dir "${SQ_CONFIG_DIR:-/opt/sq/config}"
