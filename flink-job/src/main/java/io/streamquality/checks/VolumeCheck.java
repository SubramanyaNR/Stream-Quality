package io.streamquality.checks;

import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.List;
import java.util.Locale;

/**
 * Messages/second per topic. Status:
 *  - FAIL when the window is empty and fail_on_zero (default true)
 *  - else WARN/FAIL when rate dropped more than warn_drop_pct/fail_drop_pct vs the mean of the last
 *    N windows (needs min_history windows, default 5; otherwise OK while warming up)
 */
public final class VolumeCheck implements QualityCheck<VolumeCheck.Acc> {
    public static final String NAME = "volume";

    public static class Acc implements java.io.Serializable {
        public long count;
        public Acc() {}
    }

    @Override public String name() { return NAME; }
    @Override public Acc createAccumulator() { return new Acc(); }
    @Override public Acc add(ParsedRecord r, Acc a) { a.count++; return a; }
    @Override public Acc merge(Acc a, Acc b) { a.count += b.count; return a; }

    @Override
    public List<CheckResult> evaluate(Acc acc, WindowContext ctx) {
        var th = ctx.thresholds();
        String t = ctx.topic();
        double sec = ctx.windowSizeMs() / 1000.0;
        double rate = acc.count / sec;
        double warnDrop = th.num(t, null, NAME, "warn_drop_pct", 30);
        double failDrop = th.num(t, null, NAME, "fail_drop_pct", 60);
        int minHistory = (int) th.num(t, null, NAME, "min_history", 5);
        boolean failOnZero = th.bool(t, NAME, "fail_on_zero", true);

        double[] hist = ctx.history().recent(CheckType.VOLUME, "");
        double baseline = History.mean(hist);
        boolean warm = hist.length >= minHistory;
        double dropPct = warm && baseline > 0 ? (baseline - rate) / baseline * 100.0 : 0.0;

        Status status = Status.OK;
        double threshold = 0;
        if (acc.count == 0 && failOnZero) {
            status = Status.FAIL;
            threshold = warm && baseline > 0 ? baseline * (1 - failDrop / 100.0) : 0;
        } else if (warm && baseline > 0) {
            status = Classify.higherIsWorse(dropPct, warnDrop, failDrop);
            if (status == Status.FAIL) threshold = baseline * (1 - failDrop / 100.0);
            else if (status == Status.WARN) threshold = baseline * (1 - warnDrop / 100.0);
        }
        String details = String.format(Locale.ROOT,
                "{\"count\":%d,\"window_sec\":%.1f,\"baseline_rate\":%s,\"drop_pct\":%.2f,\"history_windows\":%d,\"warming_up\":%s}",
                acc.count, sec, Double.isNaN(baseline) ? "null" : String.format(Locale.ROOT, "%.4f", baseline),
                dropPct, hist.length, !warm);
        return List.of(CheckResult.of(t, "", CheckType.VOLUME, ctx.windowStartMs(), ctx.windowEndMs(), rate, threshold, status, details));
    }
}
