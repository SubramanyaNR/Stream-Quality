package io.streamquality.checks;

import io.streamquality.model.CheckType;

/** Recent per-window values (oldest -> newest, excluding the window being evaluated) for baseline-relative checks. */
public interface History {
    double[] recent(CheckType type, String field);

    /** Per-cycle mean values for the slot (e.g. hour of day) this window falls in, from previous cycles only. */
    default double[] seasonal(CheckType type, String field) { return new double[0]; }

    /** Fields that have a recorded history for this check type (schema drift: the learned field set). */
    default java.util.Set<String> fieldsOf(CheckType type) { return java.util.Set.of(); }

    History EMPTY = (t, f) -> new double[0];

    static double mean(double[] xs) {
        if (xs.length == 0) return Double.NaN;
        double s = 0;
        for (double x : xs) s += x;
        return s / xs.length;
    }
}
