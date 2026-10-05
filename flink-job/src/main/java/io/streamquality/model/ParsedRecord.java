package io.streamquality.model;

import java.io.Serializable;
import java.util.Map;

/**
 * A Kafka record after parsing, reduced to what the checks need. Broker-agnostic.
 * fields: only monitored fields; an ABSENT key means missing or JSON null.
 * synthetic: a tick injected to advance watermarks / force empty windows; never counted as data.
 */
public record ParsedRecord(
        String topic,
        int partition,
        long offset,
        long eventTimeMs,
        boolean eventTimeFromPayload,
        long processingTimeMs,
        boolean synthetic,
        Map<String, String> fields) implements Serializable {

    public static ParsedRecord tick(String topic, long nowMs) {
        return new ParsedRecord(topic, -1, -1, nowMs, false, nowMs, true, Map.of());
    }
}
