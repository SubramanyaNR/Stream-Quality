package io.streamquality.operators;

import io.streamquality.checks.History;
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
import org.apache.flink.api.common.functions.AggregateFunction;
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

        public Eval(List<QualityCheck<?>> checks, Thresholds thresholds, String jobId) {
            this.checks = checks; this.thresholds = thresholds; this.jobId = jobId;
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
            // Update history AFTER evaluating, so a window is never its own baseline.
            int keep = thresholds.historyWindows();
            for (CheckResult r : results) {
                if (r.checkType() == CheckType.STRUCTURAL) continue;
                String key = r.checkType().wire() + "|" + r.field();
                List<Double> l = hist.get(key);
                if (l == null) l = new ArrayList<>();
                l.add(r.value());
                while (l.size() > keep) l.remove(0);
                hist.put(key, l);
            }
            for (CheckResult r : results) out.collect(ResultMapper.toRow(r, jobId));
        }
    }
}
