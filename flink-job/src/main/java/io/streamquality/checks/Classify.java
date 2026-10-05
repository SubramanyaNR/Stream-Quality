package io.streamquality.checks;

import io.streamquality.model.Status;

final class Classify {
    private Classify() {}

    /** Strictly greater-than: warn=0.0 means "any occurrence warns", not "everything warns". */
    static Status higherIsWorse(double value, double warn, double fail) {
        if (value > fail) return Status.FAIL;
        if (value > warn) return Status.WARN;
        return Status.OK;
    }
}
