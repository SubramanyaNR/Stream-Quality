package io.streamquality.checks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Fraction of records failing JSON-Schema validation (schema from the registry). Records the parser could
 * not validate (registry down / no schema) are counted as `skipped` and excluded from the rate; if nothing
 * was checked in the window NO row is emitted - a registry outage must never look like a data-quality failure.
 */
public final class StructuralCheck implements QualityCheck<StructuralCheck.Acc> {
    public static final String NAME = "structural";
    private static final int MAX_ERROR_KINDS = 5;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class Acc implements java.io.Serializable {
        public long checked, invalid, skipped;
        public HashMap<String, Long> errors = new HashMap<>();
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        switch (r.structural()) {
            case ParsedRecord.STRUCT_VALID -> a.checked++;
            case ParsedRecord.STRUCT_INVALID -> {
                a.checked++; a.invalid++;
                String msg = r.structuralError() == null ? "invalid" : r.structuralError();
                if (a.errors.containsKey(msg) || a.errors.size() < MAX_ERROR_KINDS) a.errors.merge(msg, 1L, Long::sum);
            }
            default -> a.skipped++;
        }
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.checked += b.checked; a.invalid += b.invalid; a.skipped += b.skipped;
        b.errors.forEach((k, v) -> { if (a.errors.containsKey(k) || a.errors.size() < MAX_ERROR_KINDS) a.errors.merge(k, v, Long::sum); });
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        if (acc.checked == 0) return List.of();
        var th = ctx.thresholds();
        double warn = th.num(ctx.topic(), null, NAME, "warn", 0.0);
        double fail = th.num(ctx.topic(), null, NAME, "fail", 0.01);
        double rate = (double) acc.invalid / acc.checked;
        Status s = Classify.higherIsWorse(rate, warn, fail);
        ObjectNode d = MAPPER.createObjectNode();
        d.put("checked", acc.checked); d.put("invalid", acc.invalid); d.put("skipped", acc.skipped);
        ObjectNode errs = d.putObject("top_errors");
        acc.errors.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).forEach(e -> errs.put(e.getKey(), e.getValue()));
        return List.of(CheckResult.of(ctx.topic(), "", CheckType.STRUCTURAL, ctx.windowStartMs(), ctx.windowEndMs(), rate,
                s == Status.FAIL ? fail : warn, s, d.toString()));
    }
}
