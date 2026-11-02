package io.streamquality.source;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.model.ParsedRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class TopicDiscoveryTest {
    static final Set<String> ALL = Set.of("bank.a", "bank.b", "sq.dead-letter", "other.x");

    static TopicDiscovery disc(int max, TopicDiscovery.Lister l) {
        return new TopicDiscovery(TopicSelector.pattern("bank\\..*", "", "sq.dead-letter"), max, l);
    }

    static List<String> ticks(TickFn fn) {
        List<ParsedRecord> out = new ArrayList<>();
        fn.flatMap(1L, new Collector<>() { public void collect(ParsedRecord r) { out.add(r); } public void close() {} });
        return out.stream().map(ParsedRecord::topic).sorted().toList();
    }

    @Test void selectsOnlyMatchingTopicsAndNeverTheDlq() throws Exception {
        assertThat(disc(10, () -> ALL).discover()).containsExactly("bank.a", "bank.b");
    }

    @Test void capsBroadPatterns() throws Exception {
        assertThat(disc(1, () -> ALL).discover()).containsExactly("bank.a");
    }

    @Test void ticksCoverConfiguredAndDiscoveredTopics() {
        TickFn fn = new TickFn(List.of("bank.a", "configured.only"), disc(10, () -> ALL), 60_000);
        assertThat(ticks(fn)).containsExactly("bank.a", "bank.b", "configured.only");
    }

    @Test void failedDiscoveryKeepsLastKnownTopics() {
        AtomicInteger n = new AtomicInteger();
        TickFn fn = new TickFn(List.of(), disc(10, () -> { if (n.getAndIncrement() > 0) throw new RuntimeException("broker down"); return ALL; }), 0);
        assertThat(ticks(fn)).containsExactly("bank.a", "bank.b");
        assertThat(ticks(fn)).containsExactly("bank.a", "bank.b");     // discovery now fails, list survives
    }

    @Test void withoutDiscoveryOnlyConfiguredTopicsTick() {
        assertThat(ticks(new TickFn(List.of("t")))).containsExactly("t");
    }
}
