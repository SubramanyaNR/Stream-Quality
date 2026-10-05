package io.streamquality.sink;

/**
 * Phase 1/2. Batched INSERT ... FORMAT JSONEachRow over ClickHouse HTTP (java.net.http, no driver dep).
 * - flush on size (5k rows) OR time (2s), and on checkpoint (SinkWriter.flush)
 * - retry with backoff; on exhaustion throw -> job restarts from checkpoint (at-least-once; table dedupes)
 * - target table chosen per record type: sq.check_results or sq.job_heartbeat
 */
public final class ClickHouseSink {
    private ClickHouseSink() {}
}
