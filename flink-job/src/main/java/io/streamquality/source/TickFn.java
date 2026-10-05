package io.streamquality.source;

import io.streamquality.model.ParsedRecord;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * One synthetic record per monitored topic per tick, event-time = wall clock. Why: a silent topic emits no
 * Kafka records, so without ticks the watermark stalls and NO window (hence no volume=0 row) is ever produced.
 * Consequence: this is a real-time monitor; records older than (now - max_out_of_orderness) are late.
 */
public final class TickFn implements FlatMapFunction<Long, ParsedRecord> {
    private final List<String> topics;
    public TickFn(List<String> topics) { this.topics = topics; }

    @Override
    public void flatMap(Long tick, Collector<ParsedRecord> out) {
        long now = System.currentTimeMillis();
        for (String t : topics) out.collect(ParsedRecord.tick(t, now));
    }
}
