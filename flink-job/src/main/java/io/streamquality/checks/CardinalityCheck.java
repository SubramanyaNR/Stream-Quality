package io.streamquality.checks;

import io.streamquality.config.Thresholds;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.datasketches.hll.HllSketch;

/**
 * Approximate distinct values per field per window (HyperLogLog, lgK=12, ~1.6% error, <=4 KB), flagged
 * when the estimate moves by a factor of warn_ratio / fail_ratio vs the mean of the last N windows - in
 * EITHER direction (a spike means an id explosion; a collapse means a defaulted/constant field).
 * Only fields with `cardinality: true` are tracked. Windows with no non-null values emit no row.
 * Below min_baseline distinct values the field is too small for ratios to mean anything -> OK.
 */
public final class CardinalityCheck implements QualityCheck<CardinalityCheck.Acc> {
    public static final String NAME = "cardinality";
    static final int LG_K = 12;

    /** Sketches are kept as bytes so the accumulator is a plain, serializer-friendly state object. */
    public static class Acc implements java.io.Serializable {
        public HashMap<String, byte[]> sketches = new HashMap<>();
        public Acc() {}
    }

    private final Thresholds thresholds;

    public CardinalityCheck(Thresholds thresholds) { this.thresholds = thresholds; }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        for (Map.Entry<String, String> e : r.fields().entrySet()) {
            if (!thresholds.cardinalityEnabled(r.topic(), e.getKey())) continue;
            byte[] cur = a.sketches.get(e.getKey());
            HllSketch s = cur == null ? new HllSketch(LG_K) : HllSketch.heapify(cur);
            s.update(e.getValue());
            a.sketches.put(e.getKey(), s.toUpdatableByteArray());
        }
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        for (Map.Entry<String, byte[]> e : b.sketches.entrySet()) {
            byte[] mine = a.sketches.get(e.getKey());
            if (mine == null) { a.sketches.put(e.getKey(), e.getValue()); continue; }
            org.apache.datasketches.hll.Union u = new org.apache.datasketches.hll.Union(LG_K);
            u.update(HllSketch.heapify(mine));
            u.update(HllSketch.heapify(e.getValue()));
            a.sketches.put(e.getKey(), u.getResult().toUpdatableByteArray());
        }
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        List<CheckResult> out = new ArrayList<>();
        var th = ctx.thresholds();
        String t = ctx.topic();
        for (String f : th.fields(t)) {
            if (!th.cardinalityEnabled(t, f)) continue;
            byte[] bytes = acc.sketches.get(f);
            if (bytes == null) continue;
            HllSketch s = HllSketch.heapify(bytes);
            double est = s.getEstimate();
            if (est <= 0) continue;
            double warn = th.num(t, f, NAME, "warn_ratio", 2.0);
            double fail = th.num(t, f, NAME, "fail_ratio", 5.0);
            int minHistory = (int) th.num(t, f, NAME, "min_history", 5);
            double minBaseline = th.num(t, f, NAME, "min_baseline", 10);
            double[] hist = ctx.history().recent(CheckType.CARDINALITY, f);
            double base = History.mean(hist);
            boolean warm = hist.length >= minHistory && base >= minBaseline;
            double factor = warm ? Math.max(est / base, base / Math.max(est, 1.0)) : 1.0;
            Status st = warm ? Classify.higherIsWorse(factor, warn, fail) : Status.OK;
            double threshold = st == Status.FAIL ? fail : st == Status.WARN ? warn : 0;
            String details = String.format(Locale.ROOT,
                    "{\"estimate\":%.1f,\"lower_bound\":%.1f,\"upper_bound\":%.1f,\"baseline\":%s,\"factor\":%.2f,\"direction\":\"%s\",\"history_windows\":%d,\"warming_up\":%s}",
                    est, s.getLowerBound(2), s.getUpperBound(2),
                    Double.isNaN(base) ? "null" : String.format(Locale.ROOT, "%.1f", base),
                    factor, !warm ? "n/a" : est >= base ? "up" : "down", hist.length, !warm);
            out.add(CheckResult.of(t, f, CheckType.CARDINALITY, ctx.windowStartMs(), ctx.windowEndMs(), est, threshold, st, details));
        }
        return out;
    }
}
