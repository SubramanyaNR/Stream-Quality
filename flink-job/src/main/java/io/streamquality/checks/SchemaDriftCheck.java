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
import java.util.Set;
import java.util.TreeSet;

/**
 * Top-level fields that appear or disappear in a topic (the commonest upstream-change symptom), learned from the data
 * itself, no registry needed. Every window records each field's presence rate as a baseline sample. Then:
 *  - appeared:    present in >= appear_min_rate of records but never seen in the learned baseline
 *  - disappeared: usually present (baseline mean >= min_presence) but now in < disappear_below of records
 * One row per topic per window; value = number of drifted fields; details lists the names.
 */
public final class SchemaDriftCheck implements QualityCheck<SchemaDriftCheck.Acc> {
    public static final String NAME = "schema_drift";
    static final int MAX_FIELDS = 200;      // bounds state for payloads with dynamic keys

    public static class Acc implements java.io.Serializable {
        public long total;
        public HashMap<String, Long> keys = new HashMap<>();
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        if (r.undecoded() || r.keys() == null) return a;
        a.total++;
        for (String k : r.keys()) if (a.keys.size() < MAX_FIELDS || a.keys.containsKey(k)) a.keys.merge(k, 1L, Long::sum);
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.total += b.total;
        for (Map.Entry<String, Long> e : b.keys.entrySet()) a.keys.merge(e.getKey(), e.getValue(), Long::sum);
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        if (acc.total == 0) return List.of();
        var th = ctx.thresholds();
        String t = ctx.topic();
        int minHistory = (int) th.num(t, null, NAME, "min_history", 5);
        double minPresence = th.num(t, null, NAME, "min_presence", 0.5);
        double appearMin = th.num(t, null, NAME, "appear_min_rate", 0.05);
        double disappearBelow = th.num(t, null, NAME, "disappear_below", 0.01);
        double warn = th.num(t, null, NAME, "warn", 0.0);
        double fail = th.num(t, null, NAME, "fail", 3.0);

        boolean warm = ctx.history().recent(CheckType.SCHEMA_DRIFT, "").length >= minHistory;
        Set<String> known = ctx.history().fieldsOf(CheckType.SCHEMA_DRIFT);
        Set<String> appeared = new TreeSet<>(), disappeared = new TreeSet<>();
        if (warm) {
            for (Map.Entry<String, Long> e : acc.keys.entrySet())
                if (!known.contains(e.getKey()) && (double) e.getValue() / acc.total >= appearMin) appeared.add(e.getKey());
            for (String f : known) {
                if (f.isEmpty()) continue;
                double base = History.mean(ctx.history().recent(CheckType.SCHEMA_DRIFT, f));
                double rate = (double) acc.keys.getOrDefault(f, 0L) / acc.total;
                if (base >= minPresence && rate < disappearBelow) disappeared.add(f);
            }
        }
        double value = appeared.size() + disappeared.size();
        Status s = Classify.higherIsWorse(value, warn, fail);
        String details = String.format(Locale.ROOT, "{\"appeared\":%s,\"disappeared\":%s,\"fields_seen\":%d,\"records\":%d,\"warming_up\":%s}",
                json(appeared), json(disappeared), acc.keys.size(), acc.total, !warm);
        return List.of(CheckResult.of(t, "", CheckType.SCHEMA_DRIFT, ctx.windowStartMs(), ctx.windowEndMs(), value,
                s == Status.FAIL ? fail : warn, s, details));
    }

    /** Presence rate of every field seen, and 0 for learned fields that are absent now. */
    @Override
    public Map<String, Double> extraSamples(Acc acc, WindowContext ctx) {
        Map<String, Double> out = new HashMap<>();
        if (acc.total == 0) return out;
        String p = CheckType.SCHEMA_DRIFT.wire() + "|";
        for (Map.Entry<String, Long> e : acc.keys.entrySet()) out.put(p + e.getKey(), (double) e.getValue() / acc.total);
        for (String f : ctx.history().fieldsOf(CheckType.SCHEMA_DRIFT)) if (!f.isEmpty()) out.putIfAbsent(p + f, 0.0);
        return out;
    }

    private static String json(Set<String> s) {
        List<String> l = new ArrayList<>();
        int i = 0;
        for (String x : s) { if (i++ >= 10) break; l.add("\"" + x.replace("\\", "\\\\").replace("\"", "\\\"") + "\""); }
        return "[" + String.join(",", l) + "]";
    }
}
