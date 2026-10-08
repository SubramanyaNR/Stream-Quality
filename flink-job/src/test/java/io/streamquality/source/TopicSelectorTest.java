package io.streamquality.source;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TopicSelectorTest {
    @Test
    void includesMatchesAndFullMatchOnly() {
        var p = TopicSelector.pattern("orders|payments", "", "sq.dead-letter");
        assertThat(p.matcher("orders").matches()).isTrue();
        assertThat(p.matcher("payments").matches()).isTrue();
        assertThat(p.matcher("orders.v2").matches()).isFalse();
    }

    @Test
    void excludeWinsAndDeadLetterIsAlwaysSkipped() {
        var p = TopicSelector.pattern(".*", "_.*|bank\\.flink\\..*", "sq.dead-letter");
        assertThat(p.matcher("bank.dbo.transactions").matches()).isTrue();
        assertThat(p.matcher("_schemas").matches()).isFalse();
        assertThat(p.matcher("bank.flink.spend_1m").matches()).isFalse();
        assertThat(p.matcher("sq.dead-letter").matches()).isFalse();
        assertThat(p.matcher("sq.dead-letter2").matches()).isTrue();
    }
}
