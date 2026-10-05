package io.streamquality.sink;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batching ClickHouse sink over the HTTP interface (INSERT ... FORMAT JSONEachRow).
 * No JDBC driver dependency. Flushes when a table buffer reaches batchRows, every flushIntervalMs
 * (background timer) and on every checkpoint.
 * Retries with exponential backoff; when retries are exhausted it throws, the job restarts
 * from the last checkpoint and the replayed rows collapse in ReplacingMergeTree (at-least-once).
 */
public final class ClickHouseSink implements Sink<ChRow> {
    private final ClickHouseConfig cfg;

    public ClickHouseSink(ClickHouseConfig cfg) { this.cfg = cfg; }

    @Override
    public SinkWriter<ChRow> createWriter(WriterInitContext context) {
        return new Writer(cfg);
    }

    static final class Writer implements SinkWriter<ChRow> {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
        private final ClickHouseConfig cfg;
        private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        private final Map<String, List<String>> buffers = new HashMap<>();
        private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "clickhouse-sink-flusher"); t.setDaemon(true); return t;
        });
        private volatile Exception timerFailure;

        Writer(ClickHouseConfig cfg) {
            this.cfg = cfg;
            // Bounded latency independent of checkpointing: rows never wait longer than flushIntervalMs.
            if (cfg.flushIntervalMs() > 0) {
                timer.scheduleWithFixedDelay(() -> {
                    try { flush(false); } catch (Exception e) { timerFailure = e; }
                }, cfg.flushIntervalMs(), cfg.flushIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }

        private void rethrowTimerFailure() throws IOException {
            if (timerFailure != null) throw new IOException("ClickHouse background flush failed", timerFailure);
        }

        @Override
        public synchronized void write(ChRow row, Context context) throws IOException, InterruptedException {
            rethrowTimerFailure();
            List<String> buf = buffers.computeIfAbsent(row.table(), t -> new ArrayList<>());
            buf.add(row.json());
            if (buf.size() >= cfg.batchRows()) flushTable(row.table(), buf);
        }

        @Override
        public synchronized void flush(boolean endOfInput) throws IOException, InterruptedException {
            rethrowTimerFailure();
            for (Map.Entry<String, List<String>> e : buffers.entrySet()) flushTable(e.getKey(), e.getValue());
        }

        private void flushTable(String table, List<String> rows) throws IOException, InterruptedException {
            if (rows.isEmpty()) return;
            String query = "INSERT INTO " + cfg.database() + "." + table + " FORMAT JSONEachRow";
            String body = String.join("\n", rows);
            URI uri = URI.create(cfg.url() + "/?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8));
            IOException last = null;
            long backoff = 500;
            for (int attempt = 0; attempt <= cfg.maxRetries(); attempt++) {
                try {
                    HttpRequest req = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30))
                            .header("X-ClickHouse-User", cfg.user()).header("X-ClickHouse-Key", cfg.password())
                            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
                    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
                    if (resp.statusCode() == 200) { rows.clear(); return; }
                    last = new IOException("ClickHouse HTTP " + resp.statusCode() + ": " + resp.body());
                    // 4xx other than auth-timeouts are not retryable (bad data / schema)
                    if (resp.statusCode() >= 400 && resp.statusCode() < 500 && resp.statusCode() != 408 && resp.statusCode() != 429)
                        throw last;
                } catch (IOException e) {
                    if (e == last && e.getMessage().startsWith("ClickHouse HTTP 4")) throw e;
                    last = e;
                }
                LOG.warn("ClickHouse insert into {} failed (attempt {}): {}", table, attempt + 1, last.getMessage());
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 8000);
            }
            throw last;
        }

        @Override
        public void close() throws Exception {
            timer.shutdownNow();
            synchronized (this) { flush(true); }
        }
    }
}
