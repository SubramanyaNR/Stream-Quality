package io.streamquality.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Response shapes below were captured from a real Karapace (and match the Confluent API): see docs/TESTING.md for the live runs. */
class ConfluentRegistryClientTest {
    HttpServer server;
    final List<String> auth = new CopyOnWriteArrayList<>();
    final List<String> paths = new CopyOnWriteArrayList<>();

    @BeforeEach void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getRawPath();
            paths.add(path);
            auth.add(String.valueOf(ex.getRequestHeaders().getFirst("Authorization")));
            String body; int code = 200;
            switch (path) {
                case "/subjects/orders-value/versions/latest" ->
                        body = "{\"subject\":\"orders-value\",\"version\":3,\"id\":7,\"schema\":\"{\\\"type\\\":\\\"object\\\"}\",\"schemaType\":\"JSON\"}";
                case "/subjects/avro-value/versions/latest" ->      // Avro: no schemaType field at all
                        body = "{\"subject\":\"avro-value\",\"version\":1,\"id\":2,\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"R\\\",\\\"fields\\\":[]}\"}";
                case "/subjects/with%20space-value/versions/latest" ->
                        body = "{\"schema\":\"x\",\"schemaType\":\"PROTOBUF\"}";
                case "/schemas/ids/7" -> body = "{\"schema\":\"{\\\"type\\\":\\\"object\\\"}\",\"schemaType\":\"JSON\"}";
                case "/schemas/ids/2" -> body = "{\"schema\":\"\\\"string\\\"\"}";
                case "/schemas/ids/500" -> { code = 500; body = "{\"error_code\":50001,\"message\":\"boom\"}"; }
                case "/schemas/ids/13" -> body = "{\"nothing\":true}";
                case "/config" -> body = "{\"compatibilityLevel\":\"BACKWARD\"}";
                default -> { code = 404; body = "{\"error_code\":40401,\"message\":\"Subject not found.\"}"; }
            }
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(code, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
    }

    @AfterEach void stop() { server.stop(0); }

    ConfluentRegistryClient client(String... extra) {
        Properties p = new Properties();
        p.setProperty("sq.registry.enabled", "true");
        p.setProperty("sq.registry.type", "confluent");
        p.setProperty("sq.registry.url", "http://127.0.0.1:" + server.getAddress().getPort() + "/");   // trailing slash is tolerated
        p.setProperty("sq.registry.timeout.ms", "2000");
        for (int i = 0; i < extra.length; i += 2) p.setProperty(extra[i], extra[i + 1]);
        return new ConfluentRegistryClient(RegistryConfig.from(p));
    }

    @Test void latestBySubjectReadsSchemaAndType() throws Exception {
        var s = client().fetchLatest("orders-value").orElseThrow();
        assertThat(s.type()).isEqualTo("JSON");
        assertThat(s.text()).isEqualTo("{\"type\":\"object\"}");
    }

    @Test void missingSchemaTypeMeansAvro() throws Exception {
        assertThat(client().fetchLatest("avro-value").orElseThrow().type()).isEqualTo("AVRO");
        assertThat(client().fetchById(2).orElseThrow().type()).isEqualTo("AVRO");
    }

    @Test void byIdAndNotFound() throws Exception {
        assertThat(client().fetchById(7).orElseThrow().type()).isEqualTo("JSON");
        assertThat(client().fetchLatest("nope-value")).isEmpty();            // 404 -> empty, not an error
        assertThat(client().fetchById(999)).isEmpty();
        assertThat(client().supportsIds()).isTrue();
    }

    @Test void subjectsAreUrlEncoded() throws Exception {
        assertThat(client().fetchLatest("with space-value").orElseThrow().type()).isEqualTo("PROTOBUF");
        assertThat(paths).contains("/subjects/with%20space-value/versions/latest");
    }

    @Test void serverErrorsAndMalformedBodiesSurfaceAsIoException() {
        assertThatThrownBy(() -> client().fetchById(500)).isInstanceOf(IOException.class).hasMessageContaining("500");
        assertThatThrownBy(() -> client().fetchById(13)).isInstanceOf(IOException.class).hasMessageContaining("no 'schema'");
    }

    @Test void authHeaders() throws Exception {
        client().fetchById(7);
        assertThat(auth.get(0)).isEqualTo("null");                            // no credentials by default
        client("sq.registry.auth.type", "basic", "sq.registry.auth.username", "u", "sq.registry.auth.password", "p").fetchById(7);
        assertThat(auth.get(1)).isEqualTo("Basic dTpw");
        client("sq.registry.auth.type", "bearer", "sq.registry.auth.token", "tok").fetchById(7);
        assertThat(auth.get(2)).isEqualTo("Bearer tok");
    }

    @Test void pingUsesConfigEndpointAndReportsDown() {
        assertThat(client().ping()).isTrue();
        server.stop(0);
        assertThat(client().ping()).isFalse();
    }

    @Test void factoryMapsAliasesAndRejectsUnknownTypes() {
        for (String t : List.of("confluent", "karapace", "redpanda", "ccompat", "Karapace"))
            assertThat(RegistryClients.create(cfg(t))).isInstanceOf(ConfluentRegistryClient.class);
        assertThat(RegistryClients.create(cfg("apicurio"))).isInstanceOf(ApicurioV3Client.class);
        assertThatThrownBy(() -> RegistryClients.create(cfg("glue"))).hasMessageContaining("Unsupported sq.registry.type 'glue'");
    }

    static RegistryConfig cfg(String type) {
        Properties p = new Properties();
        p.setProperty("sq.registry.url", "http://localhost:1");
        p.setProperty("sq.registry.type", type);
        return RegistryConfig.from(p);
    }
}
