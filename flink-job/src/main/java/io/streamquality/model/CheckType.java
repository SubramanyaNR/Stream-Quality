package io.streamquality.model;

public enum CheckType {
    VOLUME, NULL_RATE, CARDINALITY, FRESHNESS, STRUCTURAL, TYPE_DRIFT, SCHEMA_DRIFT, HEARTBEAT;

    public String wire() { return name().toLowerCase(); }
}
