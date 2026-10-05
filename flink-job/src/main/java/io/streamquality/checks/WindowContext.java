package io.streamquality.checks;

import io.streamquality.config.CheckThresholds;

/** What evaluate() may know about the window it is closing. */
public interface WindowContext {
    String topic();
    long windowStartMs();
    long windowEndMs();
    default long windowSizeMs() { return windowEndMs() - windowStartMs(); }
    CheckThresholds thresholds();
    /** Short lookback (last N windows) for cardinality drift; EMPTY until N windows seen. */
    BaselineView baseline();
    /** Wall clock, injectable for tests. */
    long nowMs();
}
