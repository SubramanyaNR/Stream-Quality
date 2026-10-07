package io.streamquality.sink;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batching PostgreSQL sink. A batch is sent as ONE statement: the rows travel as a JSON array that the server
 * expands (jsonb_to_recordset) and UPSERTS (ON CONFLICT DO UPDATE) - one round trip, and replays after a Flink
 * restart overwrite themselves exactly (the table has a real primary key), so at-least-once delivery yields
 * exactly-once results.
 *
 * Flushes when a table buffer reaches batchRows, every flushIntervalMs (background timer) and on every checkpoint.
 * Transient failures (connection loss, serialization failure, shutdown, resource limits) are retried with backoff on
 * a fresh connection; anything else (bad data, missing table, permissions) fails fast so the job surfaces it.
 */
public final class PostgresSink implements Sink<DbRow> {
    private final PostgresConfig cfg;

    public PostgresSink(PostgresConfig cfg) { this.cfg = cfg; }

    @Override
    public SinkWriter<DbRow> createWriter(WriterInitContext context) { return new Writer(cfg); }

    // JSON field names == column names; timestamps arrive as 'yyyy-MM-dd HH:mm:ss.SSS' UTC text.
    static final Map<String, String> SQL = Map.of(
            "check_results",
            "INSERT INTO sq.check_results (topic, field, check_type, window_start, window_end, value, threshold, status, details, job_id) "
                    + "SELECT x.topic, x.field, x.check_type, x.window_start::timestamp AT TIME ZONE 'UTC', x.window_end::timestamp AT TIME ZONE 'UTC', "
                    + "x.value, x.threshold, x.status::sq.check_status, x.details::jsonb, x.job_id "
                    + "FROM jsonb_to_recordset(?::jsonb) AS x(topic text, field text, check_type text, window_start text, window_end text, "
                    + "value double precision, threshold double precision, status text, details text, job_id text) "
                    + "ON CONFLICT (topic, check_type, field, window_start, window_end) DO UPDATE SET "
                    + "value = EXCLUDED.value, threshold = EXCLUDED.threshold, status = EXCLUDED.status, details = EXCLUDED.details, "
                    + "job_id = EXCLUDED.job_id, ingested_at = now()",
            "job_heartbeat",
            "INSERT INTO sq.job_heartbeat (job_id, ts, version, kafka_bootstrap, kafka_status, kafka_cluster_id, registry_status) "
                    + "SELECT x.job_id, x.ts::timestamp AT TIME ZONE 'UTC', x.version, x.kafka_bootstrap, x.kafka_status, x.kafka_cluster_id, x.registry_status "
                    + "FROM jsonb_to_recordset(?::jsonb) AS x(job_id text, ts text, version text, kafka_bootstrap text, kafka_status text, "
                    + "kafka_cluster_id text, registry_status text) ON CONFLICT (job_id, ts) DO NOTHING");

    static final class Writer implements SinkWriter<DbRow> {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.class);
        private final PostgresConfig cfg;
        private final Map<String, LinkedHashMap<String, String>> buffers = new LinkedHashMap<>();   // table -> (key -> json), last wins
        private long seq;
        private Connection conn;
        private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "postgres-sink-flusher"); t.setDaemon(true); return t;
        });
        private volatile Exception timerFailure;

        Writer(PostgresConfig cfg) {
            this.cfg = cfg;
            if (cfg.flushIntervalMs() > 0) {
                timer.scheduleWithFixedDelay(() -> {
                    try { flush(false); } catch (Exception e) { timerFailure = e; }
                }, cfg.flushIntervalMs(), cfg.flushIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }

        private void rethrowTimerFailure() throws IOException {
            if (timerFailure != null) throw new IOException("Postgres background flush failed", timerFailure);
        }

        @Override
        public synchronized void write(DbRow row, Context context) throws IOException, InterruptedException {
            rethrowTimerFailure();
            if (!SQL.containsKey(row.table())) throw new IOException("No insert statement for table " + row.table());
            LinkedHashMap<String, String> buf = buffers.computeIfAbsent(row.table(), t -> new LinkedHashMap<>());
            buf.remove(row.key());                                  // keep insertion order = arrival order of the LAST occurrence
            buf.put(row.key() != null ? row.key() : "#" + (seq++), row.json());
            if (buf.size() >= cfg.batchRows()) flushTable(row.table(), buf);
        }

        @Override
        public synchronized void flush(boolean endOfInput) throws IOException, InterruptedException {
            rethrowTimerFailure();
            for (Map.Entry<String, LinkedHashMap<String, String>> e : buffers.entrySet()) flushTable(e.getKey(), e.getValue());
        }

        private void flushTable(String table, LinkedHashMap<String, String> rows) throws IOException, InterruptedException {
            if (rows.isEmpty()) return;
            String payload = "[" + String.join(",", new ArrayList<>(rows.values())) + "]";
            Exception last = null;
            long backoff = 500;
            for (int attempt = 0; attempt <= cfg.maxRetries(); attempt++) {
                try {
                    try (PreparedStatement ps = connection().prepareStatement(SQL.get(table))) {
                        ps.setString(1, payload);
                        ps.executeUpdate();
                    }
                    rows.clear();
                    return;
                } catch (SQLException e) {
                    last = e;
                    closeQuietly();                                 // never reuse a connection that just failed
                    if (!isTransient(e)) throw new IOException("Postgres rejected insert into " + table + " (" + e.getSQLState() + "): " + e.getMessage(), e);
                    LOG.warn("Postgres insert into {} failed (attempt {}/{}): {}", table, attempt + 1, cfg.maxRetries() + 1, e.getMessage());
                }
                if (attempt < cfg.maxRetries()) { Thread.sleep(backoff); backoff = Math.min(backoff * 2, 8000); }
            }
            throw new IOException("Postgres unavailable after " + (cfg.maxRetries() + 1) + " attempts inserting into " + table, last);
        }

        /** Connection loss (08), serialization/deadlock (40), resources (53), admin shutdown/crash (57), system error (58); no SQLState = driver-level I/O. */
        static boolean isTransient(SQLException e) {
            String s = e.getSQLState();
            if (s == null) return true;
            return s.startsWith("08") || s.startsWith("40") || s.startsWith("53") || s.startsWith("57") || s.startsWith("58");
        }

        private Connection connection() throws SQLException {
            if (conn == null || conn.isClosed()) {
                Properties p = new Properties();
                p.setProperty("user", cfg.user());
                p.setProperty("password", cfg.password());
                p.setProperty("connectTimeout", "5");
                p.setProperty("socketTimeout", "30");
                p.setProperty("ApplicationName", "stream-quality-sink");
                conn = DriverManager.getConnection(cfg.url(), p);
                conn.setAutoCommit(true);
            }
            return conn;
        }

        private void closeQuietly() {
            try { if (conn != null) conn.close(); } catch (SQLException ignored) { /* already broken */ }
            conn = null;
        }

        @Override
        public void close() throws Exception {
            timer.shutdown();                                       // let an in-flight timer flush finish; never interrupt an INSERT
            timer.awaitTermination(30, TimeUnit.SECONDS);
            try { synchronized (this) { flush(true); } } finally { closeQuietly(); }
        }
    }
}
