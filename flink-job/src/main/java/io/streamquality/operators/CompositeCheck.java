package io.streamquality.operators;

import io.streamquality.checks.History;
import io.streamquality.checks.HistoryPolicy;
import io.streamquality.checks.QualityCheck;
import io.streamquality.checks.WindowContext;
import io.streamquality.config.Thresholds;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.sink.ChRow;
import io.streamquality.sink.ResultMapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Runs every QualityCheck over one window of one topic and keeps the per-(check,field) value history. */
public final class CompositeCheck {
    private CompositeCheck() {}

    public static class Acc implements Serializable {
        public HashMap<String, Serializable> accs = new HashMap<>();
        public Acc() {}
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final class Agg implements AggregateFunction<ParsedRecord, Acc, Acc> {
        private final List<QualityCheck<?>> checks;
        public Agg(List<QualityCheck<?>> checks) { this.checks = checks; }

        @Override public Acc createAccumulator() {
            Acc a = new Acc();
            for (QualityCheck c : checks) a.accs.put(c.name(), c.createAccumulator());
            return a;
        }
        @Override public Acc add(ParsedRecord r, Acc acc) {
            if (r.synthetic()) return acc;                      // ticks only advance time
            for (QualityCheck c : checks) acc.accs.put(c.name(), c.add(r, (Serializable) acc.accs.get(c.name())));
            return acc;
        }
        @Override public Acc getResult(Acc acc) { return acc; }
        @Override public Acc merge(Acc a, Acc b) {
            for (QualityCheck c : checks)
                a.accs.put(c.name(), c.merge((Serializable) a.accs.get(c.name()), (Serializable) b.accs.get(c.name())));
            return a;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final class Eval extends ProcessWindowFunction<Acc, ChRow, String, TimeWindow> {
        private final List<QualityCheck<?>> checks;
        private final Thresholds thresholds;
        private final String jobId;

        // Latest status per (topic|check|field) exposed as a gauge: 0 ok / 1 warn / 2 fail. Label cardinality is
        // bounded by thresholds.yaml (only configured topics/fields), never by data values.
        private transient ConcurrentHashMap<String, AtomicInteger> statusGauges;
        private transient AtomicLong lastWindowEndSec;

        public Eval(List<QualityCheck<?>> checks, Thresholds thresholds, String jobId) {
            this.checks = checks; this.thresholds = thresholds; this.jobId = jobId;
        }

        @Override
        public void open(OpenContext ctx) throws Exception {
            super.open(ctx);
            statusGauges = new ConcurrentHashMap<>();
            lastWindowEndSec = new AtomicLong(0);
            getRuntimeContext().getMetricGroup().gauge("last_window_end_seconds", (Gauge<Long>) lastWindowEndSec::get);
        }

        static String statusKey(CheckResult r) { return r.topic() + "|" + r.checkType().wire() + "|" + r.field(); }

        private void publishStatus(CheckResult r) {
            String key = statusKey(r);
            AtomicInteger g = statusGauges.computeIfAbsent(key, k -> {
                AtomicInteger v = new AtomicInteger();
                MetricGroup mg = getRuntimeContext().getMetricGroup()
                        .addGroup("topic", r.topic()).addGroup("check_type", r.checkType().wire()).addGroup("field", r.field());
                mg.gauge("check_status", (Gauge<Integer>) v::get);
                return v;
            });
            g.set(r.status().ordinal());
        }

        @Override
        public void process(String topic, Context ctx, Iterable<Acc> elements, Collector<ChRow> out) throws Exception {
            Acc acc = elements.iterator().next();
            MapState<String, List<Double>> hist =
                    ctx.globalState().getMapState(new MapStateDescriptor<>("history", Types.STRING, Types.LIST(Types.DOUBLE)));
            History view = (type, field) -> {
                try {
                    List<Double> l = hist.get(type.wire() + "|" + field);
                    if (l == null) return new double[0];
                    double[] d = new double[l.size()];
                    for (int i = 0; i < d.length; i++) d[i] = l.get(i);
                    return d;
                } catch (Exception e) { throw new RuntimeException(e); }
            };
            long ws = ctx.window().getStart(), we = ctx.window().getEnd();
            WindowContext wc = new WindowContext() {
                public String topic() { return topic; }
                public long windowStartMs() { return ws; }
                public long windowEndMs() { return we; }
                public Thresholds thresholds() { return thresholds; }
                public History history() { return view; }
            };
            List<CheckResult> results = new ArrayList<>();
            for (QualityCheck c : checks) results.addAll(c.evaluate((Serializable) acc.accs.get(c.name()), wc));
            // Update history AFTER evaluating, so a window is never its own baseline. Anomalous windows are not
            // learned from (see HistoryPolicy), unless the anomaly persists long enough to be the new normal.
            MapState<String, Integer> streaks =
                    ctx.globalState().getMapState(new MapStateDescriptor<>("violation-streak", Types.STRING, Types.INT));
            int keep = thresholds.historyWindows();
            int rebaselineAfter = thresholds.rebaselineAfter();
            for (CheckResult r : results) {
                if (r.checkType() == CheckType.STRUCTURAL) continue;
                String key = r.checkType().wire() + "|" + r.field();
                Integer prev = streaks.get(key);
                int streak = HistoryPolicy.nextStreak(r.status(), prev == null ? 0 : prev);
                streaks.put(key, streak);
                if (!HistoryPolicy.shouldRecord(r.status(), streak, rebaselineAfter)) continue;
                List<Double> l = hist.get(key);
                if (l == null) l = new ArrayList<>();
                l.add(r.value());
                while (l.size() > keep) l.remove(0);
                hist.put(key, l);
            }
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (CheckResult r : results) { seen.add(statusKey(r)); publishStatus(r); out.collect(ResultMapper.toRow(r, jobId)); }
            // A check that emitted nothing this window (e.g. null_rate on an empty topic) must not keep a stale FAIL.
            statusGauges.forEach((k, g) -> { if (k.startsWith(topic + "|") && !seen.contains(k)) g.set(0); });
            lastWindowEndSec.accumulateAndGet(we / 1000, Math::max);
        }
    }
}
