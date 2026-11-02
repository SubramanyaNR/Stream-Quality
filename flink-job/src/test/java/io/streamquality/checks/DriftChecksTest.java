package io.streamquality.checks;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.config.Thresholds;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DriftChecksTest {
    static final Thresholds TH = new Thresholds("""
            window: { size: 60s }
            defaults:
              volume: { warn_drop_pct: 30, fail_drop_pct: 60, min_history: 5, fail_on_zero: true }
            topics:
              t:
                fields:
                  amount: {}
            """);

    static WindowContext ctx(History h) {
        return new WindowContext() {
            public String topic() { return "t"; }
            public long windowStartMs() { return 0; }
            public long windowEndMs() { return 60_000; }
            public Thresholds thresholds() { return TH; }
            public History history() { return h; }
        };
    }

    static ParsedRecord rec(Map<String, String> types, List<String> keys) {
        return new ParsedRecord("t", 0, 0, 1, false, 1, false, Map.of(), ParsedRecord.STRUCT_SKIPPED, null, false, types, keys);
    }

    // ---- type drift ----
    static TypeDriftCheck.Acc typeAcc(int numbers, int strings) {
        var c = new TypeDriftCheck(); var a = c.createAccumulator();
        for (int i = 0; i < numbers; i++) c.add(rec(Map.of("amount", "number"), null), a);
        for (int i = 0; i < strings; i++) c.add(rec(Map.of("amount", "string"), null), a);
        return a;
    }

    @Test void cleanWindowIsOk() {
        CheckResult r = new TypeDriftCheck().evaluate(typeAcc(100, 0), ctx(History.EMPTY)).get(0);
        assertThat(r.status()).isEqualTo(Status.OK);
        assertThat(r.value()).isZero();
    }

    @Test void twentyPercentWrongTypedFailsEvenWithoutHistory() {
        CheckResult r = new TypeDriftCheck().evaluate(typeAcc(80, 20), ctx(History.EMPTY)).get(0);
        assertThat(r.status()).isEqualTo(Status.FAIL);
        assertThat(r.value()).isCloseTo(0.2, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(r.detailsJson()).contains("\"number\":80").contains("\"string\":20");
    }

    @Test void smallOffTypeShareWarns() {
        assertThat(new TypeDriftCheck().evaluate(typeAcc(97, 3), ctx(History.EMPTY)).get(0).status()).isEqualTo(Status.WARN);
    }

    @Test void dominantTypeFlipIsCaughtAgainstLearnedBaseline() {
        // history says "number" (code 1); the window is now 100% strings -> off share 1.0
        History h = (t, f) -> t == CheckType.TYPE_DRIFT ? new double[] {1, 1, 1, 1, 1, 1} : new double[0];
        CheckResult r = new TypeDriftCheck().evaluate(typeAcc(0, 100), ctx(h)).get(0);
        assertThat(r.status()).isEqualTo(Status.FAIL);
        assertThat(r.value()).isEqualTo(1.0);
        assertThat(r.detailsJson()).contains("\"baseline\":\"number\"").contains("\"dominant\":\"string\"");
        assertThat(r.sample()).isEqualTo(0.0);       // remembers the dominant type (string = 0), not the off share
    }

    @Test void nullsAreIgnoredAndAllNullFieldEmitsNothing() {
        var c = new TypeDriftCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 10; i++) c.add(rec(Map.of(), null), a);
        assertThat(c.evaluate(a, ctx(History.EMPTY))).isEmpty();
    }

    // ---- schema drift ----
    static SchemaDriftCheck.Acc keysAcc(int n, List<String> keys) {
        var c = new SchemaDriftCheck(); var a = c.createAccumulator();
        for (int i = 0; i < n; i++) c.add(rec(Map.of(), keys), a);
        return a;
    }

    static History learned(Map<String, double[]> byField) {
        return new History() {
            public double[] recent(CheckType t, String f) {
                if (t != CheckType.SCHEMA_DRIFT) return new double[0];
                return f.isEmpty() ? new double[] {0, 0, 0, 0, 0, 0} : byField.getOrDefault(f, new double[0]);
            }
            public Set<String> fieldsOf(CheckType t) { return t == CheckType.SCHEMA_DRIFT ? byField.keySet() : Set.of(); }
        };
    }

    static final Map<String, double[]> BASE = Map.of(
            "id", new double[] {1, 1, 1, 1, 1, 1}, "amount", new double[] {1, 1, 1, 1, 1, 1});

    @Test void stableFieldSetIsOk() {
        CheckResult r = new SchemaDriftCheck().evaluate(keysAcc(50, List.of("id", "amount")), ctx(learned(BASE))).get(0);
        assertThat(r.status()).isEqualTo(Status.OK);
        assertThat(r.value()).isZero();
    }

    @Test void newFieldIsFlagged() {
        CheckResult r = new SchemaDriftCheck().evaluate(keysAcc(50, List.of("id", "amount", "currency_v2")), ctx(learned(BASE))).get(0);
        assertThat(r.status()).isEqualTo(Status.WARN);
        assertThat(r.detailsJson()).contains("\"appeared\":[\"currency_v2\"]");
    }

    @Test void vanishedFieldIsFlagged() {
        CheckResult r = new SchemaDriftCheck().evaluate(keysAcc(50, List.of("id")), ctx(learned(BASE))).get(0);
        assertThat(r.status()).isEqualTo(Status.WARN);
        assertThat(r.detailsJson()).contains("\"disappeared\":[\"amount\"]");
    }

    @Test void manyChangesFail() {
        CheckResult r = new SchemaDriftCheck().evaluate(keysAcc(50, List.of("a", "b", "c", "d")), ctx(learned(BASE))).get(0);
        assertThat(r.status()).isEqualTo(Status.FAIL);          // 4 appeared + 2 vanished > 3
    }

    @Test void warmingUpNeverAlerts() {
        CheckResult r = new SchemaDriftCheck().evaluate(keysAcc(50, List.of("id", "x")), ctx(History.EMPTY)).get(0);
        assertThat(r.status()).isEqualTo(Status.OK);
        assertThat(r.detailsJson()).contains("\"warming_up\":true");
    }

    @Test void rareFieldBelowAppearRateIsNotDrift() {
        var c = new SchemaDriftCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 99; i++) c.add(rec(Map.of(), List.of("id", "amount")), a);
        c.add(rec(Map.of(), List.of("id", "amount", "rare")), a);          // 1% of records
        assertThat(c.evaluate(a, ctx(learned(BASE))).get(0).status()).isEqualTo(Status.OK);
    }

    @Test void extraSamplesRecordPresenceRatesAndZerosForAbsentLearnedFields() {
        var c = new SchemaDriftCheck();
        Map<String, Double> s = c.extraSamples(keysAcc(10, List.of("id")), ctx(learned(BASE)));
        assertThat(s).containsEntry("schema_drift|id", 1.0).containsEntry("schema_drift|amount", 0.0);
    }

    // ---- seasonal baseline ----
    @Test void seasonalBaselineAveragesPriorCyclesOnly() {
        java.util.List<Double> l = new java.util.ArrayList<>();
        SeasonalBaseline.record(l, 10, 4.0, 7); SeasonalBaseline.record(l, 10, 6.0, 7);     // yesterday: mean 5
        SeasonalBaseline.record(l, 11, 100.0, 7);                                              // today (excluded)
        assertThat(SeasonalBaseline.priorMeans(l, 11)).containsExactly(5.0);
    }

    @Test void seasonalBaselineKeepsOnlyNewestCycles() {
        java.util.List<Double> l = new java.util.ArrayList<>();
        for (int c = 0; c < 10; c++) SeasonalBaseline.record(l, c, c, 3);
        assertThat(SeasonalBaseline.priorMeans(l, 99)).containsExactly(7.0, 8.0, 9.0);
    }

    @Test void volumeUsesSameHourYesterdayInsteadOfRecentWindows() {
        // recent windows are all "night" (1/s) but yesterday at this hour traffic was 10/s: 6/s now is a real drop
        History h = new History() {
            public double[] recent(CheckType t, String f) { return new double[] {1, 1, 1, 1, 1, 1}; }
            public double[] seasonal(CheckType t, String f) { return new double[] {10}; }
        };
        var c = new VolumeCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 360; i++) c.add(rec(Map.of(), null), a);                       // 6/s over 60s
        CheckResult r = c.evaluate(a, ctx(h)).get(0);
        assertThat(r.status()).isEqualTo(Status.WARN);
        assertThat(r.detailsJson()).contains("\"baseline_source\":\"seasonal\"");

        History night = new History() {                                                    // overnight dip, yesterday also low: no alarm
            public double[] recent(CheckType t, String f) { return new double[] {10, 10, 10, 10, 10, 10}; }
            public double[] seasonal(CheckType t, String f) { return new double[] {6}; }
        };
        assertThat(c.evaluate(a, ctx(night)).get(0).status()).isEqualTo(Status.OK);
    }
}
