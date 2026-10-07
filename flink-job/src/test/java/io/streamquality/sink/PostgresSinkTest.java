package io.streamquality.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs against a REAL Postgres with postgres/init/01_init.sql applied. Set SQ_TEST_PG_URL (e.g.
 * jdbc:postgresql://127.0.0.1:5432/sq) and optionally SQ_TEST_PG_ADMIN (user with TRUNCATE + SELECT, default "postgres").
 * Without it the tests are skipped (see docs/TESTING.md).
 */
class PostgresSinkTest {
    static final String URL = System.getenv("SQ_TEST_PG_URL");
    static final String ADMIN = System.getenv().getOrDefault("SQ_TEST_PG_ADMIN", "postgres");
    static final String ADMIN_PW = System.getenv().getOrDefault("SQ_TEST_PG_ADMIN_PASSWORD", "");
    Connection admin;

    @BeforeEach void setUp() throws SQLException {
        assumeTrue(URL != null, "SQ_TEST_PG_URL not set - skipping Postgres tests");
        admin = DriverManager.getConnection(URL, ADMIN, ADMIN_PW);
        admin.createStatement().execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat");
    }

    @AfterEach void tearDown() throws SQLException {
        if (admin != null) { admin.createStatement().execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat"); admin.close(); }
    }

    PostgresSink.Writer writer(int batch, int retries, long flushMs) {
        return new PostgresSink.Writer(new PostgresConfig(URL, "sq_writer", "sq_writer_pw", batch, retries, flushMs));
    }

    long count(String sql) throws SQLException {
        try (ResultSet rs = admin.createStatement().executeQuery(sql)) { rs.next(); return rs.getLong(1); }
    }

    static DbRow result(String topic, int minute, double value, String status) {
        String ws = String.format("2026-10-05 10:%02d:00.000", minute), we = String.format("2026-10-05 10:%02d:00.000", minute + 1);
        String json = String.format("{\"topic\":\"%s\",\"field\":\"\",\"check_type\":\"volume\",\"window_start\":\"%s\",\"window_end\":\"%s\","
                + "\"value\":%s,\"threshold\":0.0,\"status\":\"%s\",\"details\":\"{\\\"n\\\":%d}\",\"job_id\":\"j\"}", topic, ws, we, value, status, minute);
        return new DbRow("check_results", topic + "|volume||" + ws, json);
    }

    @Test void writesRowsWithCorrectTypesAndUtcTimestamps() throws Exception {
        var w = writer(100, 1, 0);
        w.write(result("orders", 3, 12.5, "warn"), null);
        w.flush(false);
        try (ResultSet rs = admin.createStatement().executeQuery(
                "SELECT value, status::text, details->>'n', to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM sq.check_results")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble(1)).isEqualTo(12.5);
            assertThat(rs.getString(2)).isEqualTo("warn");
            assertThat(rs.getString(3)).isEqualTo("3");                      // details is real jsonb
            assertThat(rs.getString(4)).isEqualTo("2026-10-05 10:03:00");    // timestamp interpreted as UTC regardless of server TZ
        }
        w.close();
    }

    @Test void duplicatesInsideOneBatchAndAcrossReplaysCollapse() throws Exception {
        var w = writer(100, 1, 0);
        w.write(result("orders", 1, 1.0, "ok"), null);
        w.write(result("orders", 1, 2.0, "fail"), null);                      // same key in the same batch: last wins (plain ON CONFLICT would error)
        w.write(result("orders", 2, 3.0, "ok"), null);
        w.flush(false);
        assertThat(count("SELECT count(*) FROM sq.check_results")).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM sq.check_results WHERE value = 2.0 AND status = 'fail'")).isEqualTo(1);
        w.write(result("orders", 1, 9.0, "warn"), null);                      // replay in a later batch overwrites
        w.flush(false);
        assertThat(count("SELECT count(*) FROM sq.check_results")).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM sq.check_results WHERE value = 9.0")).isEqualTo(1);
        w.close();
    }

    @Test void triggerMaintainsLatestStatus() throws Exception {
        var w = writer(100, 1, 0);
        w.write(result("orders", 5, 1.0, "fail"), null);
        w.write(result("orders", 2, 1.0, "ok"), null);                        // older window arriving after a newer one
        w.flush(false);
        assertThat(count("SELECT count(*) FROM sq.latest_status WHERE topic='orders' AND status='fail'")).isEqualTo(1);
        w.close();
    }

    @Test void batchSizeTriggersFlushWithoutExplicitFlush() throws Exception {
        var w = writer(3, 1, 0);
        for (int i = 0; i < 3; i++) w.write(result("t", i, i, "ok"), null);
        assertThat(count("SELECT count(*) FROM sq.check_results")).isEqualTo(3);
        w.close();
    }

    @Test void timerFlushesWithoutCheckpoint() throws Exception {
        var w = writer(1000, 1, 100);
        w.write(result("t", 1, 1, "ok"), null);
        long deadline = System.currentTimeMillis() + 5000;
        while (count("SELECT count(*) FROM sq.check_results") == 0 && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertThat(count("SELECT count(*) FROM sq.check_results")).isEqualTo(1);
        w.close();
    }

    @Test void heartbeatRowsAreInsertedAndReplaysIgnored() throws Exception {
        var w = writer(100, 1, 0);
        String hb = "{\"job_id\":\"j1\",\"ts\":\"2026-10-05 10:00:00.123\",\"version\":\"0.1\",\"kafka_bootstrap\":\"b:9092\","
                + "\"kafka_status\":\"up\",\"kafka_cluster_id\":\"c\",\"registry_status\":\"disabled\"}";
        w.write(new DbRow("job_heartbeat", null, hb), null);
        w.write(new DbRow("job_heartbeat", null, hb), null);
        w.flush(false);
        w.write(new DbRow("job_heartbeat", null, hb), null);
        w.flush(false);
        assertThat(count("SELECT count(*) FROM sq.job_heartbeat")).isEqualTo(1);
        w.close();
    }

    @Test void badDataFailsFastWithoutRetry() throws Exception {
        var w = writer(100, 5, 0);
        w.write(result("t", 1, 1, "bogus-status"), null);
        long t0 = System.currentTimeMillis();
        assertThatThrownBy(() -> w.flush(false)).hasMessageContaining("rejected").hasMessageContaining("22P02");
        assertThat(System.currentTimeMillis() - t0).isLessThan(2000);         // 5 retries with backoff would take > 15 s
    }

    @Test void unreachableDatabaseIsRetriedThenSurfaces() throws Exception {
        var w = new PostgresSink.Writer(new PostgresConfig("jdbc:postgresql://127.0.0.1:1/sq", "sq_writer", "x", 100, 1, 0));
        w.write(result("t", 1, 1, "ok"), null);
        assertThatThrownBy(() -> w.flush(false)).hasMessageContaining("unavailable after 2 attempts");
    }

    @Test void writerPrivilegesAreEnough() throws Exception {
        var w = writer(100, 0, 0);                                            // proves sq_writer can upsert incl. the trigger path
        w.write(result("t", 1, 1, "ok"), null); w.write(result("t", 1, 2, "ok"), null);
        w.flush(false);
        assertThat(count("SELECT count(*) FROM sq.check_results")).isEqualTo(1);
        w.close();
    }
}
