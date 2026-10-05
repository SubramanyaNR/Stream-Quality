package io.streamquality.config;

import java.io.Serializable;

/** Resolved (defaults + per-topic overrides) thresholds, loaded from thresholds.yaml. */
public interface CheckThresholds extends Serializable {
    double warn(String checkType, String field);
    double fail(String checkType, String field);
}
