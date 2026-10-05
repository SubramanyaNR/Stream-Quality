package io.streamquality.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ThresholdsTest {
    static final String YAML = """
            window: { size: 5s, max_out_of_orderness: 500ms, idle_timeout: 2m }
            event_time_field: ts
            defaults:
              null_rate: { warn: 0.05, fail: 0.20 }
              volume: { warn_drop_pct: 30, fail_on_zero: true }
            topics:
              orders:
                volume: { warn_drop_pct: 10, fail_on_zero: false }
                fields:
                  order_id: { required: true, null_rate: { warn: 0.0, fail: 0.01 } }
                  note: {}
              payments: { fields: { id: {} } }
            """;

    @Test
    void parsesWindowAndTopics() {
        var t = new Thresholds(YAML);
        assertThat(t.windowSizeMs()).isEqualTo(5000);
        assertThat(t.maxOutOfOrdernessMs()).isEqualTo(500);
        assertThat(t.idleTimeoutMs()).isEqualTo(120_000);
        assertThat(t.topics()).containsExactly("orders", "payments");
        assertThat(t.fields("orders")).containsExactly("order_id", "note");
        assertThat(t.required("orders", "order_id")).isTrue();
        assertThat(t.required("orders", "note")).isFalse();
    }

    @Test
    void precedenceFieldThenTopicThenDefaultThenCode() {
        var t = new Thresholds(YAML);
        assertThat(t.num("orders", "order_id", "null_rate", "fail", 9)).isEqualTo(0.01);   // field override
        assertThat(t.num("orders", "note", "null_rate", "fail", 9)).isEqualTo(0.20);       // falls to defaults
        assertThat(t.num("orders", null, "volume", "warn_drop_pct", 9)).isEqualTo(10);     // topic override
        assertThat(t.num("payments", null, "volume", "warn_drop_pct", 9)).isEqualTo(30);   // default
        assertThat(t.num("payments", null, "volume", "nope", 9)).isEqualTo(9);             // code default
        assertThat(t.bool("orders", "volume", "fail_on_zero", true)).isFalse();
        assertThat(t.bool("payments", "volume", "fail_on_zero", false)).isTrue();
    }

    @Test
    void shippedThresholdsFileLoads() throws Exception {
        var t = Thresholds.load(Path.of("..", "config", "thresholds.yaml"));
        assertThat(t.topics()).isNotEmpty();
        assertThat(t.windowSizeMs()).isPositive();
    }
}
