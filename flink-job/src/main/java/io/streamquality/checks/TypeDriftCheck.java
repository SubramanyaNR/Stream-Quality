package io.streamquality.checks;

import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * JSON type mix per configured field (no registry needed). Per window it counts string/number/boolean/object/array
 * values (nulls are ignored, null_rate owns those) and measures the share of values NOT of the field's baseline type.
 * The baseline type is the most common dominant type of recent healthy windows (the window's own dominant type while
 * warming up). value = off-type share, so "20% of records carry a string where a number belongs" is 0.20, and a
 * complete flip of the dominant type is ~1.0.
 */
public final class TypeDriftCheck implements QualityCheck<TypeDriftCheck.Acc> {
    public static final String NAME = "type_drift";
    static final String[] TYPES = {"string", "number", "boolean", "object", "array"};

    public static class Acc implements java.io.Serializable {
        public HashMap<String, long[]> counts = new HashMap<>();
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        if (r.undecoded() || r.types() == null) return a;
        for (Map.Entry<String, String> e : r.types().entrySet()) {
            int i = indexOf(e.getValue());
            if (i >= 0) a.counts.computeIfAbsent(e.getKey(), k -> new long[TYPES.length])[i]++;
        }
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        for (Map.Entry<String, long[]> e : b.counts.entrySet()) {
            long[] t = a.counts.computeIfAbsent(e.getKey(), k -> new long[TYPES.length]);
            for (int i = 0; i < t.length; i++) t[i] += e.getValue()[i];
        }
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        List<CheckResult> out = new ArrayList<>();
        var th = ctx.thresholds();
        for (String f : th.fields(ctx.topic())) {
            long[] c = acc.counts.get(f);
            if (c == null) continue;
            long n = 0; int dominant = 0;
            for (int i = 0; i < c.length; i++) { n += c[i]; if (c[i] > c[dominant]) dominant = i; }
            if (n == 0) continue;
            int minHistory = (int) th.num(ctx.topic(), f, NAME, "min_history", 5);
            double[] hist = ctx.history().recent(CheckType.TYPE_DRIFT, f);
            int baseline = hist.length >= minHistory ? mode(hist) : dominant;
            double off = 1.0 - (double) c[baseline] / n;
            double warn = th.num(ctx.topic(), f, NAME, "warn", 0.01);
            double fail = th.num(ctx.topic(), f, NAME, "fail", 0.10);
            Status s = Classify.higherIsWorse(off, warn, fail);
            double threshold = s == Status.FAIL ? fail : warn;
            StringBuilder counts = new StringBuilder();
            for (int i = 0; i < c.length; i++) if (c[i] > 0)
                counts.append(counts.length() == 0 ? "" : ",").append('"').append(TYPES[i]).append("\":").append(c[i]);
            String details = String.format(Locale.ROOT, "{\"baseline\":\"%s\",\"dominant\":\"%s\",\"counts\":{%s},\"total\":%d,\"learned\":%s}",
                    TYPES[baseline], TYPES[dominant], counts, n, hist.length >= minHistory);
            out.add(CheckResult.of(ctx.topic(), f, CheckType.TYPE_DRIFT, ctx.windowStartMs(), ctx.windowEndMs(), off, threshold, s, details, dominant));
        }
        return out;
    }

    static int indexOf(String t) {
        for (int i = 0; i < TYPES.length; i++) if (TYPES[i].equals(t)) return i;
        return -1;
    }

    /** Most frequent type code among the samples (lowest code wins a tie). */
    static int mode(double[] codes) {
        int[] n = new int[TYPES.length];
        for (double d : codes) { int i = (int) d; if (i >= 0 && i < n.length) n[i]++; }
        int best = 0;
        for (int i = 1; i < n.length; i++) if (n[i] > n[best]) best = i;
        return best;
    }
}
