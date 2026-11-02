package io.streamquality.checks;

import java.util.ArrayList;
import java.util.List;

/**
 * Time-of-day (or time-of-week) baseline helpers. A "slot" is one hour of the cycle; for every slot we keep, per past
 * cycle, the sum and count of the values seen, as flat triples [cycle, sum, count] so it fits a Flink ListState of
 * doubles. A window is compared with the SAME slot of previous cycles (yesterday / last week), never with itself.
 */
public final class SeasonalBaseline {
    private SeasonalBaseline() {}

    public static final long BUCKET_MS = 3_600_000L;

    /** 0 = seasonality off. */
    public static long periodMs(String mode) {
        if (mode == null) return 0;
        return switch (mode.trim().toLowerCase()) {
            case "daily" -> 24 * BUCKET_MS;
            case "weekly" -> 7 * 24 * BUCKET_MS;
            default -> 0;
        };
    }

    public static int keepCycles(String mode) { return "weekly".equalsIgnoreCase(mode) ? 4 : 7; }

    public static long slot(long windowStartMs, long periodMs) { return (windowStartMs % periodMs) / BUCKET_MS; }

    public static long cycle(long windowStartMs, long periodMs) { return windowStartMs / periodMs; }

    /** Adds a value to the current cycle's triple; trims to the newest keepCycles cycles. */
    public static void record(List<Double> l, long cycle, double v, int keepCycles) {
        for (int i = 0; i + 2 < l.size(); i += 3) {
            if (l.get(i) == (double) cycle) {
                l.set(i + 1, l.get(i + 1) + v);
                l.set(i + 2, l.get(i + 2) + 1);
                return;
            }
        }
        l.add((double) cycle); l.add(v); l.add(1.0);
        while (l.size() > keepCycles * 3) { l.remove(0); l.remove(0); l.remove(0); }
    }

    /** Mean value of each PREVIOUS cycle (cycle != current), oldest first. */
    public static double[] priorMeans(List<Double> l, long currentCycle) {
        if (l == null) return new double[0];
        List<Double> out = new ArrayList<>();
        for (int i = 0; i + 2 < l.size(); i += 3)
            if (l.get(i) < currentCycle && l.get(i + 2) > 0) out.add(l.get(i + 1) / l.get(i + 2));
        double[] d = new double[out.size()];
        for (int i = 0; i < d.length; i++) d[i] = out.get(i);
        return d;
    }
}
