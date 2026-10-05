package io.streamquality.checks;

import io.streamquality.model.CheckType;

/** Recent per-window values (oldest -> newest, excluding the window being evaluated) for baseline-relative checks. */
public interface History {
    double[] recent(CheckType type, String field);

    History EMPTY = (t, f) -> new double[0];

    static double mean(double[] xs) {
        if (xs.length == 0) return Double.NaN;
        double s = 0;
        for (double x : xs) s += x;
        return s / xs.length;
    }
}
