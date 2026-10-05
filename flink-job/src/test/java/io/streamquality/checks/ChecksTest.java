package io.streamquality.checks;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.config.Thresholds;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ChecksTest {
    static final Thresholds TH = new Thresholds("""
            window: { size: 10s }
            defaults:
              null_rate: { warn: 0.05, fail: 0.20 }
              volume: { warn_drop_pct: 30, fail_drop_pct: 60, min_history: 3, fail_on_zero: true }
            topics:
              t:
                fields:
                  a: { null_rate: { warn: 0.0, fail: 0.5 } }
                  b: {}
            """);

    static WindowContext ctx(History h) {
        return new WindowContext() {
            public String topic() { return "t"; }
            public long windowStartMs() { return 0; }
            public long windowEndMs() { return 10_000; }
            public Thresholds thresholds() { return TH; }
            public History history() { return h; }
        };
    }

    static History hist(double... vals) { return (type, f) -> type == CheckType.VOLUME ? vals : new double[0]; }

    static ParsedRecord rec(Map<String, String> fields) {
        return new ParsedRecord("t", 0, 0, 1, false, 1, false, fields);
    }

    static VolumeCheck.Acc vol(long n) {
        var c = new VolumeCheck(); var a = c.createAccumulator();
        for (long i = 0; i < n; i++) c.add(rec(Map.of()), a);
        return a;
    }

    @Test void volumeRateIsCountOverWindowSeconds() {
        CheckResult r = new VolumeCheck().evaluate(vol(50), ctx(History.EMPTY)).get(0);
        assertThat(r.value()).isEqualTo(5.0);
        assertThat(r.status()).isEqualTo(Status.OK);          // warming up: no baseline yet
        assertThat(r.detailsJson()).contains("\"warming_up\":true");
    }

    @Test void volumeDropAgainstBaseline() {
        History h = hist(10, 10, 10, 10);                      // baseline 10 msg/s
        assertThat(new VolumeCheck().evaluate(vol(90), ctx(h)).get(0).status()).isEqualTo(Status.OK);    // 9/s  = -10%
        var warn = new VolumeCheck().evaluate(vol(60), ctx(h)).get(0);                                    // 6/s  = -40%
        assertThat(warn.status()).isEqualTo(Status.WARN);
        assertThat(warn.threshold()).isEqualTo(7.0);
        var fail = new VolumeCheck().evaluate(vol(20), ctx(h)).get(0);                                    // 2/s  = -80%
        assertThat(fail.status()).isEqualTo(Status.FAIL);
        assertThat(fail.threshold()).isEqualTo(4.0);
    }

    @Test void volumeSpikeUpIsNotAViolation() {
        assertThat(new VolumeCheck().evaluate(vol(1000), ctx(hist(10, 10, 10))).get(0).status()).isEqualTo(Status.OK);
    }

    @Test void emptyWindowFailsEvenWithoutBaseline() {
        var r = new VolumeCheck().evaluate(vol(0), ctx(History.EMPTY)).get(0);
        assertThat(r.status()).isEqualTo(Status.FAIL);
        assertThat(r.value()).isZero();
    }

    @Test void nullRatePerFieldWithStrictThresholds() {
        var c = new NullRateCheck(); var acc = c.createAccumulator();
        for (int i = 0; i < 10; i++) {
            Map<String, String> f = new HashMap<>();
            if (i >= 2) f.put("a", "x");                       // a null in 2/10 = 0.2
            f.put("b", "y");                                    // b never null
            c.add(rec(f), acc);
        }
        List<CheckResult> rs = c.evaluate(acc, ctx(History.EMPTY));
        assertThat(rs).hasSize(2);
        CheckResult a = rs.stream().filter(r -> r.field().equals("a")).findFirst().orElseThrow();
        CheckResult b = rs.stream().filter(r -> r.field().equals("b")).findFirst().orElseThrow();
        assertThat(a.value()).isEqualTo(0.2);
        assertThat(a.status()).isEqualTo(Status.WARN);         // > warn(0.0), <= fail(0.5)
        assertThat(a.threshold()).isEqualTo(0.0);
        assertThat(b.value()).isZero();
        assertThat(b.status()).isEqualTo(Status.OK);           // 0 is NOT > warn 0.05
    }

    @Test void nullRateMergeAndEmptyWindow() {
        var c = new NullRateCheck();
        var a1 = c.createAccumulator(); var a2 = c.createAccumulator();
        c.add(rec(Map.of("a", "1")), a1); c.add(rec(Map.of()), a2);
        var merged = c.merge(a1, a2);
        assertThat(merged.total).isEqualTo(2);
        assertThat(c.evaluate(merged, ctx(History.EMPTY)).stream().filter(r -> r.field().equals("a")).findFirst().orElseThrow().value()).isEqualTo(0.5);
        assertThat(c.evaluate(c.createAccumulator(), ctx(History.EMPTY))).isEmpty();   // no data -> no null-rate rows
    }
}
