package io.streamquality.source;

import io.streamquality.model.ParsedRecord;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One synthetic record per monitored topic per tick, event-time = wall clock. Why: a silent topic emits no
 * Kafka records, so without ticks the watermark stalls and NO window (hence no volume=0 row) is ever produced.
 * Monitored = the topics in thresholds.yaml plus, when a topic pattern is used, every topic matching it (re-discovered
 * every refreshMs; a failed refresh keeps the last known list).
 * Consequence: this is a real-time monitor; records older than (now - max_out_of_orderness) are late.
 */
public final class TickFn extends RichFlatMapFunction<Long, ParsedRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(TickFn.class);
    private final List<String> configured;
    private final TopicDiscovery discovery;     // null = configured topics only
    private final long refreshMs;
    private transient Set<String> current;
    private transient long lastRefresh;

    public TickFn(List<String> topics) { this(topics, null, 0); }

    public TickFn(List<String> configured, TopicDiscovery discovery, long refreshMs) {
        this.configured = configured; this.discovery = discovery; this.refreshMs = refreshMs;
    }

    @Override
    public void open(OpenContext ctx) {
        current = new LinkedHashSet<>(configured);
        lastRefresh = Long.MIN_VALUE / 2;
    }

    @Override
    public void flatMap(Long tick, Collector<ParsedRecord> out) {
        long now = System.currentTimeMillis();
        if (current == null) open(null);
        if (discovery != null && now - lastRefresh >= refreshMs) {
            lastRefresh = now;
            try {
                Set<String> next = new LinkedHashSet<>(configured);
                next.addAll(discovery.discover());
                if (!next.equals(current)) LOG.info("Silence detection now covers {} topics", next.size());
                current = next;
            } catch (Exception e) {
                LOG.warn("Topic discovery failed ({}); keeping {} known topics", e.getMessage(), current.size());
            }
        }
        for (String t : current) out.collect(ParsedRecord.tick(t, now));
    }
}
