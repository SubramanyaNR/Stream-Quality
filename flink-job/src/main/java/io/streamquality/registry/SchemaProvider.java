package io.streamquality.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-topic compiled-schema cache in front of a RegistryClient.
 *  - first lookup of a topic is synchronous (bounded by the client timeout) so coverage starts immediately;
 *  - later refreshes (every refreshMs) happen on a background thread, the hot path never blocks;
 *  - last good schema is kept when the registry fails (stale beats nothing);
 *  - a circuit breaker stops hammering a dead registry; while it is open and no schema is cached the
 *    result is SKIPPED, never INVALID: structural checks degrade, statistical checks are unaffected.
 */
public final class SchemaProvider implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaProvider.class);

    public enum State { VALID, INVALID, SKIPPED }
    public record Result(State state, String error) {
        static final Result VALID = new Result(State.VALID, null);
        static final Result SKIPPED = new Result(State.SKIPPED, null);
    }

    static final long NEGATIVE_TTL_MS = 30_000;

    private record Entry(JsonSchema schema, long loadedAt, boolean none) {}

    private final RegistryConfig cfg;
    private final RegistryClient client;
    private final LongSupplier clock;
    private final CircuitBreaker breaker;
    private final Map<String, Entry> cache = new ConcurrentHashMap<>();
    private final Map<String, Boolean> refreshing = new ConcurrentHashMap<>();
    private final ExecutorService refresher = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, "schema-refresh"); t.setDaemon(true); return t; });
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean up = true;

    public SchemaProvider(RegistryConfig cfg, RegistryClient client, LongSupplier clock) {
        this.cfg = cfg; this.client = client; this.clock = clock;
        this.breaker = new CircuitBreaker(cfg.breakerFailures(), cfg.breakerResetMs(), clock);
    }

    /** True while the registry is believed reachable (false once the breaker has opened or the last call failed). */
    public boolean isUp() { return up; }

    public Result validate(String topic, JsonNode payload) {
        Entry e = cache.get(topic);
        if (e == null) {
            e = load(topic);                                              // synchronous first load
            if (e == null) return Result.SKIPPED;
        } else if (clock.getAsLong() - e.loadedAt() >= ttl(e) && refreshing.putIfAbsent(topic, true) == null) {
            refresher.execute(() -> { try { load(topic); } finally { refreshing.remove(topic); } });
        }
        if (e.none()) return Result.SKIPPED;                              // registry has no schema for this topic
        Set<ValidationMessage> errors = e.schema().validate(payload);
        if (errors.isEmpty()) return Result.VALID;
        return new Result(State.INVALID, errors.iterator().next().getMessage());
    }

    /** "No schema registered yet" is re-checked quickly (<= 30 s) so a newly registered schema is picked up promptly. */
    private long ttl(Entry e) { return e.none() ? Math.min(cfg.refreshMs(), NEGATIVE_TTL_MS) : cfg.refreshMs(); }

    private synchronized Entry load(String topic) {
        if (!breaker.allow()) { up = false; return cache.get(topic); }
        try {
            Optional<String> text = client.fetchLatestSchema(cfg.artifactId(topic));
            breaker.success(); up = true;
            Entry e;
            if (text.isEmpty()) e = new Entry(null, clock.getAsLong(), true);
            else {
                JsonNode node = mapper.readTree(text.get());
                JsonSchemaFactory f = JsonSchemaFactory.getInstance(specOf(node));
                e = new Entry(f.getSchema(node), clock.getAsLong(), false);
            }
            cache.put(topic, e);
            return e;
        } catch (Exception ex) {
            breaker.failure(); up = false;
            LOG.warn("Schema registry lookup for topic {} failed ({}); structural checks use last good schema or are skipped", topic, ex.toString());
            return cache.get(topic);
        }
    }

    /** Honour the schema's own $schema; default to 2020-12. */
    static SpecVersion.VersionFlag specOf(JsonNode schema) {
        String u = schema.path("$schema").asText("");
        if (u.contains("draft-04")) return SpecVersion.VersionFlag.V4;
        if (u.contains("draft-06")) return SpecVersion.VersionFlag.V6;
        if (u.contains("draft-07")) return SpecVersion.VersionFlag.V7;
        if (u.contains("2019-09")) return SpecVersion.VersionFlag.V201909;
        return SpecVersion.VersionFlag.V202012;
    }

    @Override public void close() { refresher.shutdownNow(); }
}
