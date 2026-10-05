package io.streamquality.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * thresholds.yaml, resolved with precedence  topics.T.fields.F.CHECK.KEY > topics.T.CHECK.KEY > defaults.CHECK.KEY > code default.
 * Holds the YAML text (Serializable) and parses lazily on each task.
 */
public final class Thresholds implements Serializable {
    private final String yaml;
    private transient JsonNode root;

    public Thresholds(String yaml) { this.yaml = yaml; }

    public static Thresholds load(Path file) throws IOException { return new Thresholds(Files.readString(file)); }

    private JsonNode root() {
        if (root == null) {
            try { root = new ObjectMapper(new YAMLFactory()).readTree(yaml); }
            catch (IOException e) { throw new IllegalStateException("Invalid thresholds.yaml", e); }
        }
        return root;
    }

    public long windowSizeMs() { return duration(root().path("window").path("size"), "60s"); }
    public long maxOutOfOrdernessMs() { return duration(root().path("window").path("max_out_of_orderness"), "10s"); }
    public long idleTimeoutMs() { return duration(root().path("window").path("idle_timeout"), "30s"); }
    public String eventTimeField() { return root().path("event_time_field").asText("ts"); }
    public int historyWindows() { return root().path("history_windows").asInt(30); }

    /** Monitored topics = keys under `topics:`. */
    public List<String> topics() {
        List<String> out = new ArrayList<>();
        root().path("topics").fieldNames().forEachRemaining(out::add);
        return out;
    }

    /** Fields configured for a topic (null-rate etc. is evaluated for each). */
    public List<String> fields(String topic) {
        List<String> out = new ArrayList<>();
        root().path("topics").path(topic).path("fields").fieldNames().forEachRemaining(out::add);
        return out;
    }

    public boolean required(String topic, String field) {
        return root().path("topics").path(topic).path("fields").path(field).path("required").asBoolean(false);
    }

    public boolean cardinalityEnabled(String topic, String field) {
        return root().path("topics").path(topic).path("fields").path(field).path("cardinality").asBoolean(false);
    }

    public double num(String topic, String field, String check, String key, double def) {
        JsonNode t = root().path("topics").path(topic);
        JsonNode[] chain = {
            field == null ? null : t.path("fields").path(field).path(check).path(key),
            t.path(check).path(key),
            root().path("defaults").path(check).path(key)};
        for (JsonNode n : chain) if (n != null && n.isNumber()) return n.asDouble();
        return def;
    }

    public boolean bool(String topic, String check, String key, boolean def) {
        JsonNode t = root().path("topics").path(topic).path(check).path(key);
        if (t.isBoolean()) return t.asBoolean();
        JsonNode d = root().path("defaults").path(check).path(key);
        return d.isBoolean() ? d.asBoolean() : def;
    }

    static long duration(JsonNode n, String def) {
        String s = n.isMissingNode() || n.isNull() ? def : n.asText();
        if (s.endsWith("ms")) return Long.parseLong(s.substring(0, s.length() - 2));
        if (s.endsWith("s")) return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1))).toMillis();
        if (s.endsWith("m")) return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1))).toMillis();
        throw new IllegalArgumentException("Bad duration '" + s + "' (use 500ms, 30s or 2m)");
    }
}
