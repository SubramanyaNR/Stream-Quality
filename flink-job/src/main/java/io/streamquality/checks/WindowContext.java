package io.streamquality.checks;

import io.streamquality.config.Thresholds;

/** What evaluate() may know about the window it is closing. */
public interface WindowContext {
    String topic();
    long windowStartMs();
    long windowEndMs();
    default long windowSizeMs() { return windowEndMs() - windowStartMs(); }
    Thresholds thresholds();
    History history();
}
