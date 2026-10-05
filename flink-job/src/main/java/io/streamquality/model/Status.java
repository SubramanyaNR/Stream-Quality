package io.streamquality.model;

public enum Status {
    OK, WARN, FAIL;

    public String wire() { return name().toLowerCase(); }

    public static Status worst(Status a, Status b) { return a.ordinal() >= b.ordinal() ? a : b; }
}
