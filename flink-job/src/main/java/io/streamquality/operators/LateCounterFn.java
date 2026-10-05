package io.streamquality.operators;

import io.streamquality.model.ParsedRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

/** Terminal sink for records that arrived after their window closed; exposes them as the `late_records` metric. */
public final class LateCounterFn extends RichFlatMapFunction<ParsedRecord, Void> {
    private transient Counter late;

    @Override public void open(OpenContext ctx) { late = getRuntimeContext().getMetricGroup().counter("late_records"); }

    @Override
    public void flatMap(ParsedRecord r, Collector<Void> out) { if (!r.synthetic()) late.inc(); }
}
