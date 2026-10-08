package io.streamquality.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamquality.config.Thresholds;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.RawRecord;
import io.streamquality.decode.PayloadDecoder;
import io.streamquality.registry.RegistryClients;
import io.streamquality.registry.RegistryConfig;
import io.streamquality.registry.SchemaProvider;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * bytes -> ParsedRecord, or a DlqRecord on the side output.
 * Unparseable / non-object / bad-timestamp records go ONLY to the DLQ (they cannot be analysed).
 * Records missing a `required: true` field are analysed AND copied to the DLQ.
 */
public final class ParseFn extends ProcessFunction<RawRecord, ParsedRecord> {
    public static final OutputTag<DlqRecord> DLQ = new OutputTag<>("dlq") {};
    /** Event times more than this far in the future are treated as bad data. */
    static final long MAX_FUTURE_SKEW_MS = 24L * 3600 * 1000;
    /** Anything below this (1e11 ms ~ 1973) is assumed to be epoch SECONDS. */
    static final long SECONDS_CUTOFF = 100_000_000_000L;

    private final Thresholds thresholds;
    private final RegistryConfig registryCfg;
    private final boolean framing;
    private transient ObjectMapper mapper;
    private transient SchemaProvider schemas;
    private transient PayloadDecoder decoder;
    private transient AtomicInteger registryUp;

    public ParseFn(Thresholds thresholds, RegistryConfig registryCfg) { this(thresholds, registryCfg, true); }

    /** framing: recognise the Confluent wire format (sq.source.framing=auto). Needs a registry that supports lookup by id. */
    public ParseFn(Thresholds thresholds, RegistryConfig registryCfg, boolean framing) {
        this.thresholds = thresholds; this.registryCfg = registryCfg; this.framing = framing;
    }

    @Override
    public void open(OpenContext ctx) {
        mapper = new ObjectMapper();
        registryUp = new AtomicInteger(registryCfg != null && registryCfg.enabled() ? 1 : 0);
        getRuntimeContext().getMetricGroup().gauge("registry_up", (Gauge<Integer>) registryUp::get);
        if (registryCfg != null && registryCfg.enabled()) {
            schemas = new SchemaProvider(registryCfg, RegistryClients.create(registryCfg), System::currentTimeMillis);
        }
        decoder = new PayloadDecoder(mapper, schemas, framing);
    }

    @Override
    public void close() { if (schemas != null) schemas.close(); }

    @Override
    public void processElement(RawRecord r, Context ctx, Collector<ParsedRecord> out) {
        long now = System.currentTimeMillis();
        if (r.value() == null) { dlq(ctx, r, DlqRecord.NULL_PAYLOAD, "tombstone / null value"); return; }
        PayloadDecoder.Decoded d = decoder.decode(r.value());
        if (d.failReason() != null) { dlq(ctx, r, d.failReason(), trunc(d.failDetail())); return; }
        if (d.undecoded()) {          // readable only with the registry (e.g. Avro while it is down): count it for volume, skip field checks
            out.collect(new ParsedRecord(r.topic(), r.partition(), r.offset(), r.timestampMs(), false, now, false, Map.of(),
                    ParsedRecord.STRUCT_SKIPPED, null, true));
            registryUp.set(schemas != null && schemas.isUp() ? 1 : 0);
            return;
        }
        JsonNode root = d.json();
        if (root == null || !root.isObject()) { dlq(ctx, r, DlqRecord.NOT_AN_OBJECT, "payload is not a JSON object"); return; }

        long eventTime = r.timestampMs();
        boolean fromPayload = false;
        JsonNode ts = path(root, thresholds.eventTimeField());
        if (ts != null && !ts.isNull()) {
            long parsed = parseTime(ts);
            if (parsed < 0 || parsed > now + MAX_FUTURE_SKEW_MS) {
                dlq(ctx, r, DlqRecord.BAD_TIMESTAMP, "unparseable or far-future event time: " + trunc(ts.asText())); return;
            }
            eventTime = parsed;
            fromPayload = true;
        }

        List<String> fieldNames = thresholds.fields(r.topic());
        Map<String, String> fields = new HashMap<>();
        StringBuilder missingRequired = null;
        for (String f : fieldNames) {
            JsonNode v = path(root, f);
            if (v == null || v.isNull() || v.isMissingNode()) {
                if (thresholds.required(r.topic(), f)) {
                    if (missingRequired == null) missingRequired = new StringBuilder();
                    missingRequired.append(missingRequired.length() == 0 ? "" : ",").append(f);
                }
            } else {
                fields.put(f, v.isValueNode() ? v.asText() : v.toString());
            }
        }
        byte structural = ParsedRecord.STRUCT_SKIPPED;
        String structuralError = null;
        if (d.isAvro()) {
            structural = ParsedRecord.STRUCT_VALID;       // it decoded with the registered writer schema: structurally sound by construction
        } else if (schemas != null) {
            SchemaProvider.Result v = d.schema() != null ? schemas.validateWithSchema(d.schemaId(), d.schema(), root)   // framed JSON: the exact writer schema
                                                         : schemas.validate(r.topic(), root);                           // plain JSON: latest schema of the topic's subject
            registryUp.set(schemas.isUp() ? 1 : 0);
            if (v.state() == SchemaProvider.State.VALID) structural = ParsedRecord.STRUCT_VALID;
            else if (v.state() == SchemaProvider.State.INVALID) { structural = ParsedRecord.STRUCT_INVALID; structuralError = trunc(v.error()); }
        }
        out.collect(new ParsedRecord(r.topic(), r.partition(), r.offset(), eventTime, fromPayload, now, false, fields, structural, structuralError, false));
        if (structural == ParsedRecord.STRUCT_INVALID) dlq(ctx, r, DlqRecord.SCHEMA_VIOLATION, structuralError);
        if (missingRequired != null) dlq(ctx, r, DlqRecord.REQUIRED_FIELD_MISSING, missingRequired.toString());
    }

    static JsonNode path(JsonNode root, String dotted) {
        JsonNode n = root;
        for (String p : dotted.split("\\.")) { n = n.path(p); if (n.isMissingNode()) return null; }
        return n;
    }

    static long parseTime(JsonNode n) {
        try {
            if (n.isNumber()) { long v = n.asLong(); return v < SECONDS_CUTOFF ? v * 1000 : v; }
            String s = n.asText().trim();
            if (s.matches("-?\\d+")) { long v = Long.parseLong(s); return v < SECONDS_CUTOFF ? v * 1000 : v; }
            return Instant.parse(s).toEpochMilli();
        } catch (Exception e) { return -1; }
    }

    private static void dlq(Context ctx, RawRecord r, String reason, String detail) {
        ctx.output(DLQ, new DlqRecord(reason, detail, r.topic(), r.partition(), r.offset(), r.timestampMs(), r.key(), r.value()));
    }

    private static String trunc(String s) {
        if (s == null) return "";
        s = s.replace('\n', ' ');
        return s.length() > 200 ? s.substring(0, 200) : s;
    }
}
