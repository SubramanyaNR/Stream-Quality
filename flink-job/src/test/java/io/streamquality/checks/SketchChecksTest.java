package io.streamquality.checks;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.config.Thresholds;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SketchChecksTest {
    static final Thresholds TH = new Thresholds("""
            defaults:
              cardinality: { warn_ratio: 2.0, fail_ratio: 5.0, min_history: 3, min_baseline: 10 }
              freshness: { warn_p95_ms: 30000, fail_p95_ms: 120000 }
            topics:
              t:
                fields:
                  cust: { cardinality: true }
                  other: {}
            """);

    static WindowContext ctx(double... cardHistory) {
        return new WindowContext() {
            public String topic() { return "t"; }
            public long windowStartMs() { return 0; }
            public long windowEndMs() { return 5_000; }
            public Thresholds thresholds() { return TH; }
            public History history() { return (type, f) -> type == CheckType.CARDINALITY ? cardHistory : new double[0]; }
        };
    }

    static CardinalityCheck.Acc distinct(CardinalityCheck c, int n) {
        var acc = c.createAccumulator();
        for (int i = 0; i < n; i++)
            c.add(new ParsedRecord("t", 0, i, 1, false, 1, false, Map.of("cust", "c" + i, "other", "x" + i)), acc);
        return acc;
    }

    @Test void estimatesDistinctWithinHllError() {
        var c = new CardinalityCheck(TH);
        CheckResult r = c.evaluate(distinct(c, 5000), ctx()).get(0);
        assertThat(r.field()).isEqualTo("cust");
        assertThat(r.value()).isBetween(5000 * 0.95, 5000 * 1.05);
        assertThat(r.status()).isEqualTo(Status.OK);                 // no history yet
        assertThat(r.detailsJson()).contains("\"warming_up\":true");
    }

    @Test void onlyFieldsMarkedCardinalityAreTracked() {
        var c = new CardinalityCheck(TH);
        assertThat(distinct(c, 20).sketches).containsOnlyKeys("cust");
    }

    @Test void spikeAndCollapseBothDetected() {
        var c = new CardinalityCheck(TH);
        double[] base = {100, 100, 100, 100};
        assertThat(c.evaluate(distinct(c, 110), ctx(base)).get(0).status()).isEqualTo(Status.OK);
        assertThat(c.evaluate(distinct(c, 250), ctx(base)).get(0).status()).isEqualTo(Status.WARN);   // 2.5x
        var fail = c.evaluate(distinct(c, 1000), ctx(base)).get(0);                                    // 10x
        assertThat(fail.status()).isEqualTo(Status.FAIL);
        assertThat(fail.detailsJson()).contains("\"direction\":\"up\"");
        var down = c.evaluate(distinct(c, 15), ctx(base)).get(0);                                      // /6.7
        assertThat(down.status()).isEqualTo(Status.FAIL);
        assertThat(down.detailsJson()).contains("\"direction\":\"down\"");
    }

    @Test void tinyBaselineAndEmptyWindowsAreQuiet() {
        var c = new CardinalityCheck(TH);
        assertThat(c.evaluate(distinct(c, 40), ctx(3, 3, 3, 3)).get(0).status()).isEqualTo(Status.OK);  // baseline < min_baseline
        assertThat(c.evaluate(c.createAccumulator(), ctx(100, 100, 100))).isEmpty();
    }

    @Test void hllMergeEqualsUnion() {
        var c = new CardinalityCheck(TH);
        var a = distinct(c, 300);
        var b = c.createAccumulator();
        for (int i = 200; i < 500; i++) c.add(new ParsedRecord("t", 0, i, 1, false, 1, false, Map.of("cust", "c" + i)), b);
        var r = c.evaluate(c.merge(a, b), ctx()).get(0);
        assertThat(r.value()).isBetween(500 * 0.95, 500 * 1.05);        // union of 0..299 and 200..499 = 500
    }

    static ParsedRecord at(long lagMs, boolean payloadTs) {
        long now = 1_000_000;
        return new ParsedRecord("t", 0, 0, now - lagMs, payloadTs, now, false, Map.of());
    }

    @Test void freshnessPercentilesAndStatus() {
        var c = new FreshnessCheck();
        var acc = c.createAccumulator();
        for (int i = 0; i < 100; i++) c.add(at(1000 + i, true), acc);          // ~1s lag
        CheckResult ok = c.evaluate(acc, ctx()).get(0);
        assertThat(ok.status()).isEqualTo(Status.OK);
        assertThat(ok.value()).isBetween(1090.0, 1100.0);
        assertThat(ok.detailsJson()).contains("\"n\":100");

        var stale = c.createAccumulator();
        for (int i = 0; i < 100; i++) c.add(at(150_000, true), stale);
        assertThat(c.evaluate(stale, ctx()).get(0).status()).isEqualTo(Status.FAIL);
        var warn = c.createAccumulator();
        for (int i = 0; i < 100; i++) c.add(at(60_000, true), warn);
        assertThat(c.evaluate(warn, ctx()).get(0).status()).isEqualTo(Status.WARN);
    }

    @Test void freshnessIgnoresKafkaTimestampFallbackAndClampsFuture() {
        var c = new FreshnessCheck();
        var acc = c.createAccumulator();
        c.add(at(500_000, false), acc);                                         // no payload ts -> not measured
        assertThat(c.evaluate(acc, ctx())).isEmpty();
        c.add(at(-60_000, true), acc);                                          // event in the future
        var r = c.evaluate(acc, ctx()).get(0);
        assertThat(r.value()).isZero();
        assertThat(r.detailsJson()).contains("\"future_timestamps\":1");
    }

    @Test void freshnessMergeCombinesSketches() {
        var c = new FreshnessCheck();
        var a = c.createAccumulator(); var b = c.createAccumulator();
        for (int i = 0; i < 50; i++) { c.add(at(1000, true), a); c.add(at(3000, true), b); }
        var r = c.evaluate(c.merge(a, b), ctx()).get(0);
        assertThat(r.detailsJson()).contains("\"n\":100");
        assertThat(r.value()).isEqualTo(3000.0);
    }
}
