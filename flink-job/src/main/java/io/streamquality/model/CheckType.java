package io.streamquality.model;

public enum CheckType {
    VOLUME, NULL_RATE, CARDINALITY, FRESHNESS, STRUCTURAL, HEARTBEAT;

    public String wire() { return name().toLowerCase(); }
}
