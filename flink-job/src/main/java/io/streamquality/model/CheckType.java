package io.streamquality.model;

public enum CheckType {
    VOLUME, NULL_RATE, CARDINALITY, FRESHNESS, DISTRIBUTION, STRUCTURAL, HEARTBEAT;

    public String wire() { return name().toLowerCase(); }
}
