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

/** Fraction of records where a configured field is missing or JSON null. One row per field per window. */
public final class NullRateCheck implements QualityCheck<NullRateCheck.Acc> {
    public static final String NAME = "null_rate";

    public static class Acc implements java.io.Serializable {
        public long total;
        public HashMap<String, Long> present = new HashMap<>();
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        if (r.undecoded()) return a;                    // unreadable record: its fields are unknown, not null
        a.total++;
        for (String f : r.fields().keySet()) a.present.merge(f, 1L, Long::sum);
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.total += b.total;
        for (Map.Entry<String, Long> e : b.present.entrySet()) a.present.merge(e.getKey(), e.getValue(), Long::sum);
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        List<CheckResult> out = new ArrayList<>();
        if (acc.total == 0) return out;                       // no data -> volume check owns that signal
        var th = ctx.thresholds();
        for (String f : th.fields(ctx.topic())) {
            long nulls = acc.total - acc.present.getOrDefault(f, 0L);
            double rate = (double) nulls / acc.total;
            double warn = th.num(ctx.topic(), f, NAME, "warn", 0.05);
            double fail = th.num(ctx.topic(), f, NAME, "fail", 0.20);
            Status s = Classify.higherIsWorse(rate, warn, fail);
            double threshold = s == Status.FAIL ? fail : warn;
            String details = String.format(Locale.ROOT, "{\"nulls\":%d,\"total\":%d,\"required\":%s}",
                    nulls, acc.total, th.required(ctx.topic(), f));
            out.add(CheckResult.of(ctx.topic(), f, CheckType.NULL_RATE, ctx.windowStartMs(), ctx.windowEndMs(), rate, threshold, s, details));
        }
        return out;
    }
}
