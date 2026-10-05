package io.streamquality.checks;

import java.util.Optional;

/** Read-only view over keyed state holding recent per-(topic,field) window summaries. */
public interface BaselineView {
    Optional<Summary> forField(String field);

    record Summary(int windows, double mean, double stddev, double p50, double p95, double p99, double distinctMean) {}

    BaselineView EMPTY = f -> Optional.empty();
}
