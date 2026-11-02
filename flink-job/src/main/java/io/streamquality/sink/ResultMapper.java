package io.streamquality.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import io.streamquality.model.CheckResult;
import io.streamquality.model.Status;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** CheckResult -> JSON row for sq.check_results (keyed by the table's primary key so replays collapse). */
public final class ResultMapper {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private ResultMapper() {}

    public static DbRow toRow(CheckResult r, String jobId) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("topic", r.topic());
        n.put("field", r.field());
        n.put("check_type", r.checkType().wire());
        n.put("window_start", TS.format(Instant.ofEpochMilli(r.windowStartMs())));
        n.put("window_end", TS.format(Instant.ofEpochMilli(r.windowEndMs())));
        n.put("value", finite(r.value()));
        n.put("threshold", finite(r.threshold()));
        n.put("status", r.status().wire());
        // Storage: details only where it explains a violation; the numbers the baseline panels need become plain columns.
        if (r.status() != Status.OK) n.put("details", r.detailsJson());
        JsonNode d = parse(r.detailsJson());
        putNum(n, "baseline", d.has("baseline_rate") ? d.get("baseline_rate") : d.get("baseline"));
        putNum(n, "p50_ms", d.get("p50_ms"));
        putNum(n, "p99_ms", d.get("p99_ms"));
        n.put("job_id", jobId);
        String key = r.topic() + "|" + r.checkType().wire() + "|" + r.field() + "|" + r.windowStartMs() + "|" + r.windowEndMs();
        return new DbRow("check_results", key, n.toString());
    }

    private static JsonNode parse(String json) {
        try { JsonNode n = MAPPER.readTree(json); return n == null ? MAPPER.createObjectNode() : n; }
        catch (Exception e) { return MAPPER.createObjectNode(); }
    }

    private static void putNum(ObjectNode n, String col, JsonNode v) {
        if (v != null && v.isNumber() && Double.isFinite(v.asDouble())) n.put(col, v.asDouble());
    }

    private static double finite(double d) { return Double.isFinite(d) ? d : 0.0; }
}
