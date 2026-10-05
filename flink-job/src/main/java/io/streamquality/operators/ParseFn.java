package io.streamquality.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamquality.config.Thresholds;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.RawRecord;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private transient ObjectMapper mapper;

    public ParseFn(Thresholds thresholds) { this.thresholds = thresholds; }

    @Override
    public void open(OpenContext ctx) { mapper = new ObjectMapper(); }

    @Override
    public void processElement(RawRecord r, Context ctx, Collector<ParsedRecord> out) {
        long now = System.currentTimeMillis();
        if (r.value() == null) { dlq(ctx, r, DlqRecord.NULL_PAYLOAD, "tombstone / null value"); return; }
        JsonNode root;
        try { root = mapper.readTree(r.value()); }
        catch (Exception e) { dlq(ctx, r, DlqRecord.INVALID_JSON, trunc(e.getMessage())); return; }
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
        out.collect(new ParsedRecord(r.topic(), r.partition(), r.offset(), eventTime, fromPayload, now, false, fields));
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
