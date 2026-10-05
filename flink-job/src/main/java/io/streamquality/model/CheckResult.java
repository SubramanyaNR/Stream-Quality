package io.streamquality.model;

import java.io.Serializable;

/** One row of sq.check_results. Immutable; details is a JSON string. */
public record CheckResult(
        String topic,
        String field,            // "" for topic-level checks
        CheckType checkType,
        long windowStartMs,
        long windowEndMs,
        double value,
        double threshold,
        Status status,
        String detailsJson) implements Serializable {

    public static CheckResult of(String topic, String field, CheckType type, long ws, long we,
                                 double value, double threshold, Status status, String details) {
        return new CheckResult(topic, field == null ? "" : field, type, ws, we, value, threshold, status,
                details == null ? "{}" : details);
    }
}
