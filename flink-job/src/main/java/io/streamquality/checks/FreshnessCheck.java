package io.streamquality.checks;

import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.List;
import java.util.Locale;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

/**
 * Latency = Flink processing time at parse - event timestamp in the payload, in ms (negative / future
 * timestamps are clamped to 0 and counted). value = p95; details carry p50, p99, max, n, future count.
 * Only records whose timestamp came from the PAYLOAD are measured (Kafka CreateTime would hide producer lag).
 */
public final class FreshnessCheck implements QualityCheck<FreshnessCheck.Acc> {
    public static final String NAME = "freshness";
    static final int K = 200;

    public static class Acc implements java.io.Serializable {
        public byte[] sketch;        // KllDoublesSketch
        public long future;
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }

    @Override
    public Acc add(ParsedRecord r, Acc a) {
        if (!r.eventTimeFromPayload()) return a;
        long delta = r.processingTimeMs() - r.eventTimeMs();
        if (delta < 0) { a.future++; delta = 0; }
        KllDoublesSketch s = a.sketch == null ? KllDoublesSketch.newHeapInstance(K) : KllDoublesSketch.heapify(Memory.wrap(a.sketch));
        s.update((double) delta);
        a.sketch = s.toByteArray();
        return a;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        a.future += b.future;
        if (b.sketch == null) return a;
        if (a.sketch == null) { a.sketch = b.sketch; return a; }
        KllDoublesSketch s = KllDoublesSketch.heapify(Memory.wrap(a.sketch));
        s.merge(KllDoublesSketch.heapify(Memory.wrap(b.sketch)));
        a.sketch = s.toByteArray();
        return a;
    }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        if (acc.sketch == null) return List.of();
        KllDoublesSketch s = KllDoublesSketch.heapify(Memory.wrap(acc.sketch));
        if (s.isEmpty()) return List.of();
        var th = ctx.thresholds();
        double warn = th.num(ctx.topic(), null, NAME, "warn_p95_ms", 30_000);
        double fail = th.num(ctx.topic(), null, NAME, "fail_p95_ms", 120_000);
        double p95 = s.getQuantile(0.95);
        Status st = Classify.higherIsWorse(p95, warn, fail);
        double threshold = st == Status.FAIL ? fail : warn;
        String details = String.format(Locale.ROOT,
                "{\"p50_ms\":%.0f,\"p95_ms\":%.0f,\"p99_ms\":%.0f,\"max_ms\":%.0f,\"n\":%d,\"future_timestamps\":%d}",
                s.getQuantile(0.5), p95, s.getQuantile(0.99), s.getMaxItem(), s.getN(), acc.future);
        return List.of(CheckResult.of(ctx.topic(), "", CheckType.FRESHNESS, ctx.windowStartMs(), ctx.windowEndMs(), p95, threshold, st, details));
    }
}
