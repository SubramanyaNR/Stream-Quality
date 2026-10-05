package io.streamquality.checks;

import java.util.Optional;

/** Read-only view over keyed RocksDB state holding per-(topic,field) window summaries for 24h. */
public interface BaselineView {
    Optional<Summary> forField(String field);

    record Summary(int windows, double mean, double stddev, double p50, double p95, double p99, double distinctMean) {}

    BaselineView EMPTY = f -> Optional.empty();
}
