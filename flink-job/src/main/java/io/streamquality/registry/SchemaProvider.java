package io.streamquality.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
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

    // ---------------------------------------------------------------- lookup by schema id (Confluent wire format)
    public enum Lookup { FOUND, NOT_FOUND, UNAVAILABLE }
    public record IdLookup(Lookup state, RegisteredSchema schema) {}

    private final Map<Integer, RegisteredSchema> byId = new ConcurrentHashMap<>();       // ids are immutable: cached for good
    private final Map<Integer, Long> unknownIds = new ConcurrentHashMap<>();             // 404s, re-checked after NEGATIVE_TTL_MS
    private final Map<Integer, JsonSchema> compiledById = new ConcurrentHashMap<>();

    public boolean supportsIds() { return client.supportsIds(); }

    /** Schema for a wire-format id. UNAVAILABLE = registry unreachable / breaker open (callers degrade, never fail records for it). */
    public IdLookup lookupId(int id) {
        RegisteredSchema cached = byId.get(id);
        if (cached != null) return new IdLookup(Lookup.FOUND, cached);
        Long missingAt = unknownIds.get(id);
        if (missingAt != null && clock.getAsLong() - missingAt < NEGATIVE_TTL_MS) return new IdLookup(Lookup.NOT_FOUND, null);
        synchronized (this) {
            cached = byId.get(id);
            if (cached != null) return new IdLookup(Lookup.FOUND, cached);
            if (!breaker.allow()) { up = false; return new IdLookup(Lookup.UNAVAILABLE, null); }
            try {
                Optional<RegisteredSchema> found = client.fetchById(id);
                breaker.success(); up = true;
                if (found.isEmpty()) { unknownIds.put(id, clock.getAsLong()); return new IdLookup(Lookup.NOT_FOUND, null); }
                unknownIds.remove(id);
                byId.put(id, found.get());
                return new IdLookup(Lookup.FOUND, found.get());
            } catch (Exception ex) {
                breaker.failure(); up = false;
                LOG.warn("Schema registry lookup of id {} failed ({}); records using it are decoded best-effort", id, ex.toString());
                return new IdLookup(Lookup.UNAVAILABLE, null);
            }
        }
    }

    /** Validate a JSON payload against the exact (writer) schema it was serialized with. Non-JSON schema types are not validated here. */
    public Result validateWithSchema(int id, RegisteredSchema schema, JsonNode payload) {
        if (!RegisteredSchema.JSON.equals(schema.type())) return Result.SKIPPED;
        JsonSchema compiled = compiledById.computeIfAbsent(id, k -> compile(schema.text()));
        Set<ValidationMessage> errors = compiled.validate(payload);
        return errors.isEmpty() ? Result.VALID : new Result(State.INVALID, errors.iterator().next().getMessage());
    }

    private JsonSchema compile(String text) {
        try {
            JsonNode node = mapper.readTree(text);
            return JsonSchemaFactory.getInstance(specOf(node)).getSchema(node);
        } catch (Exception e) { throw new IllegalStateException("Registry returned an unparseable JSON Schema: " + e.getMessage(), e); }
    }

    /** "No schema registered yet" is re-checked quickly (<= 30 s) so a newly registered schema is picked up promptly. */
    private long ttl(Entry e) { return e.none() ? Math.min(cfg.refreshMs(), NEGATIVE_TTL_MS) : cfg.refreshMs(); }

    private synchronized Entry load(String topic) {
        if (!breaker.allow()) { up = false; return cache.get(topic); }
        try {
            Optional<RegisteredSchema> found = client.fetchLatest(cfg.artifactId(topic));
            breaker.success(); up = true;
            Entry e;
            if (found.isEmpty()) e = new Entry(null, clock.getAsLong(), true);
            else if (!RegisteredSchema.JSON.equals(found.get().type())) {
                // e.g. an Avro/Protobuf subject: cannot validate a plain-JSON payload with it. Treated like "no schema" (re-checked in <= 30 s).
                LOG.warn("Subject {} holds a {} schema; structural validation of plain JSON payloads needs a JSON Schema - skipped", cfg.artifactId(topic), found.get().type());
                e = new Entry(null, clock.getAsLong(), true);
            } else e = new Entry(compile(found.get().text()), clock.getAsLong(), false);
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
