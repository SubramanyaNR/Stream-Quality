package io.streamquality.registry;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {
    @Test void opensAfterThresholdAndHalfOpensAfterReset() {
        AtomicLong now = new AtomicLong(0);
        var b = new CircuitBreaker(3, 1000, now::get);
        b.failure(); b.failure();
        assertThat(b.allow()).isTrue();
        b.failure();                                   // 3rd consecutive failure -> open
        assertThat(b.isOpen()).isTrue();
        assertThat(b.allow()).isFalse();
        now.set(999);  assertThat(b.allow()).isFalse();
        now.set(1000); assertThat(b.allow()).isTrue(); // half-open: one trial
        assertThat(b.allow()).isFalse();               // ...and only one
        b.failure();                                   // trial failed -> stays open
        now.set(2500); assertThat(b.allow()).isTrue();
        b.success();
        assertThat(b.isOpen()).isFalse();
        assertThat(b.allow()).isTrue();
    }

    @Test void successResetsFailureCount() {
        var b = new CircuitBreaker(2, 1000, () -> 0);
        b.failure(); b.success(); b.failure();
        assertThat(b.isOpen()).isFalse();
    }
}
