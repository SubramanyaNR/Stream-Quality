package io.streamquality.checks;

import io.streamquality.config.CheckThresholds;

/** What evaluate() may know about the window it is closing. */
public interface WindowContext {
    String topic();
    long windowStartMs();
    long windowEndMs();
    default long windowSizeMs() { return windowEndMs() - windowStartMs(); }
    CheckThresholds thresholds();
    /** Rolling 24h baseline for drift checks; EMPTY until min_baseline_windows reached. */
    BaselineView baseline();
    /** Wall clock, injectable for tests. */
    long nowMs();
}
