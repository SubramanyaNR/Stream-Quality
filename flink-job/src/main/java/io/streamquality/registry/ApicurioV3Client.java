package io.streamquality.registry;

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

/** Apicurio Registry v3 core API. url = http://host:8080/apis/registry/v3 */
public final class ApicurioV3Client implements RegistryClient {
    private final RegistryConfig cfg;
    private final HttpClient http;

    public ApicurioV3Client(RegistryConfig cfg) {
        this.cfg = cfg;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(cfg.timeoutMs())).build();
    }

    @Override
    public Optional<RegisteredSchema> fetchLatest(String artifactId) throws IOException {
        String path = "/groups/" + enc(cfg.group()) + "/artifacts/" + enc(artifactId) + "/versions/branch=latest/content";
        HttpResponse<String> r = get(path);
        if (r.statusCode() == 404) return Optional.empty();
        if (r.statusCode() != 200) throw new IOException("Registry HTTP " + r.statusCode() + " for " + artifactId);
        return Optional.of(new RegisteredSchema(RegisteredSchema.JSON, r.body()));
    }

    @Override
    public boolean ping() {
        try { return get("/system/info").statusCode() == 200; } catch (IOException e) { return false; }
    }

    private HttpResponse<String> get(String path) throws IOException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(cfg.url() + path))
                .timeout(Duration.ofMillis(cfg.timeoutMs())).header("Accept", "application/json");
        if ("basic".equalsIgnoreCase(cfg.authType()))
            b.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((cfg.username() + ":" + cfg.password()).getBytes(StandardCharsets.UTF_8)));
        else if ("bearer".equalsIgnoreCase(cfg.authType())) b.header("Authorization", "Bearer " + cfg.bearerToken());
        try { return http.send(b.GET().build(), HttpResponse.BodyHandlers.ofString()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("interrupted", e); }
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20"); }
}
