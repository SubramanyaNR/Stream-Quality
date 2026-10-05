package io.streamquality.registry;

import java.util.function.LongSupplier;

/** Opens after N consecutive failures; after resetMs lets ONE trial call through (half-open). Not thread-safe by design. */
public final class CircuitBreaker {
    private final int threshold;
    private final long resetMs;
    private final LongSupplier clock;
    private int failures;
    private long openedAt = -1;

    public CircuitBreaker(int threshold, long resetMs, LongSupplier clock) {
        this.threshold = threshold; this.resetMs = resetMs; this.clock = clock;
    }

    /** May a call be attempted now? */
    public boolean allow() {
        if (openedAt < 0) return true;
        if (clock.getAsLong() - openedAt >= resetMs) { openedAt = clock.getAsLong(); return true; }   // half-open: re-arm the timer, allow one trial
        return false;
    }

    public void success() { failures = 0; openedAt = -1; }

    public void failure() {
        if (++failures >= threshold) openedAt = clock.getAsLong();
    }

    public boolean isOpen() { return openedAt >= 0; }
}
