package io.streamquality.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClickHouseSinkTest {
    HttpServer server;
    final List<String> bodies = new CopyOnWriteArrayList<>();
    final List<String> queries = new CopyOnWriteArrayList<>();
    final List<String> users = new CopyOnWriteArrayList<>();
    final AtomicInteger failFirst = new AtomicInteger(0);
    volatile int forcedStatus = 200;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            if (failFirst.getAndDecrement() > 0) { ex.sendResponseHeaders(503, -1); ex.close(); return; }
            bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            queries.add(java.net.URLDecoder.decode(ex.getRequestURI().getRawQuery().substring("query=".length()), StandardCharsets.UTF_8));
            users.add(ex.getRequestHeaders().getFirst("X-ClickHouse-User"));
            byte[] msg = forcedStatus == 200 ? new byte[0] : "Code: 62. bad".getBytes();
            ex.sendResponseHeaders(forcedStatus, msg.length == 0 ? -1 : msg.length);
            if (msg.length > 0) ex.getResponseBody().write(msg);
            ex.close();
        });
        server.start();
    }

    @AfterEach void stop() { server.stop(0); }

    private ClickHouseSink.Writer writer(int batch, int retries) {
        String url = "http://127.0.0.1:" + server.getAddress().getPort();
        return new ClickHouseSink.Writer(new ClickHouseConfig(url, "sq", "u", "p", batch, retries, 0));
    }

    @Test
    void batchesBySizeAndFlushesRemainderPerTable() throws Exception {
        var w = writer(2, 1);
        w.write(new ChRow("check_results", "{\"a\":1}"), null);
        assertThat(bodies).isEmpty();                      // below batch size: buffered
        w.write(new ChRow("check_results", "{\"a\":2}"), null);
        assertThat(bodies).containsExactly("{\"a\":1}\n{\"a\":2}");
        w.write(new ChRow("job_heartbeat", "{\"b\":1}"), null);
        w.flush(false);
        assertThat(queries).containsExactly(
                "INSERT INTO sq.check_results FORMAT JSONEachRow", "INSERT INTO sq.job_heartbeat FORMAT JSONEachRow");
        assertThat(users).containsOnly("u");
        w.flush(false);                                    // nothing left -> no extra request
        assertThat(bodies).hasSize(2);
    }

    @Test
    void retriesTransientFailuresThenSucceeds() throws Exception {
        failFirst.set(2);
        var w = writer(10, 3);
        w.write(new ChRow("job_heartbeat", "{}"), null);
        w.flush(false);
        assertThat(bodies).hasSize(1);
    }

    @Test
    void clientErrorsAreNotRetriedAndSurface() throws Exception {
        forcedStatus = 400;
        var w = writer(10, 3);
        w.write(new ChRow("job_heartbeat", "{}"), null);
        assertThatThrownBy(() -> w.flush(false)).hasMessageContaining("HTTP 400");
        assertThat(bodies).hasSize(1);                     // exactly one attempt
    }

    @Test
    void exhaustedRetriesThrow() throws Exception {
        failFirst.set(100);
        var w = writer(10, 1);
        w.write(new ChRow("job_heartbeat", "{}"), null);
        assertThatThrownBy(() -> w.flush(false)).hasMessageContaining("503");
    }

    @Test
    void timerFlushesWithoutCheckpoint() throws Exception {
        String url = "http://127.0.0.1:" + server.getAddress().getPort();
        var w = new ClickHouseSink.Writer(new ClickHouseConfig(url, "sq", "u", "p", 1000, 1, 100));
        w.write(new ChRow("job_heartbeat", "{}"), null);
        long deadline = System.currentTimeMillis() + 3000;
        while (bodies.isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(50);
        assertThat(bodies).hasSize(1);
        w.close();
    }
}
