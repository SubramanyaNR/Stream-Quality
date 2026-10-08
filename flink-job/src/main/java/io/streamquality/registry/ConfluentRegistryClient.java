package io.streamquality.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

/**
 * Client for the Confluent Schema Registry REST API - the de-facto standard that is also implemented by Karapace,
 * Redpanda's built-in registry and Apicurio's compatibility endpoint (/apis/ccompat/v7). url = http://host:8081
 *
 *   GET /subjects/{subject}/versions/latest  -> {"subject","version","id","schema","schemaType"?}
 *   GET /schemas/ids/{id}                    -> {"schema","schemaType"?}
 *   404 (error_code 40401 subject / 40402 version / 40403 schema) -> empty;  schemaType absent => AVRO
 */
public final class ConfluentRegistryClient implements RegistryClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final RegistryConfig cfg;
    private final HttpClient http;

    public ConfluentRegistryClient(RegistryConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(cfg.timeoutMs())).build();
    }

    @Override
    public Optional<RegisteredSchema> fetchLatest(String subject) throws IOException {
        return parse(get("/subjects/" + enc(subject) + "/versions/latest"), "subject " + subject);
    }

    @Override public boolean supportsIds() { return true; }

    @Override
    public Optional<RegisteredSchema> fetchById(int id) throws IOException {
        return parse(get("/schemas/ids/" + id), "schema id " + id);
    }

    @Override
    public boolean ping() {
        try { return get("/config").statusCode() == 200; } catch (IOException e) { return false; }
    }

    private Optional<RegisteredSchema> parse(HttpResponse<String> r, String what) throws IOException {
        if (r.statusCode() == 404) return Optional.empty();
        if (r.statusCode() != 200) throw new IOException("Registry HTTP " + r.statusCode() + " for " + what);
        JsonNode n = MAPPER.readTree(r.body());
        if (!n.hasNonNull("schema")) throw new IOException("Registry response for " + what + " has no 'schema' field");
        return Optional.of(new RegisteredSchema(n.path("schemaType").asText(RegisteredSchema.AVRO).toUpperCase(), n.get("schema").asText()));
    }

    private HttpResponse<String> get(String path) throws IOException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(cfg.url() + path)).timeout(Duration.ofMillis(cfg.timeoutMs()))
                .header("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json");
        if ("basic".equalsIgnoreCase(cfg.authType()))
            b.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((cfg.username() + ":" + cfg.password()).getBytes(StandardCharsets.UTF_8)));
        else if ("bearer".equalsIgnoreCase(cfg.authType())) b.header("Authorization", "Bearer " + cfg.bearerToken());
        try { return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("interrupted", e); }
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20"); }
}
