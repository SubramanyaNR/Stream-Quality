package io.streamquality.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;

/**
 * A Kafka record after parsing. Broker-agnostic: only topic/partition/offset/timestamps
 * and the decoded payload. Raw bytes are kept ONLY on the dead-letter path.
 */
public record ParsedRecord(
        String topic,
        int partition,
        long offset,
        long kafkaTimestampMs,
        long eventTimeMs,          // from payload field (thresholds.yaml: event_time_field); falls back to kafka ts
        boolean eventTimeFromPayload,
        long processingTimeMs,     // wall clock at parse; basis for freshness
        JsonNode payload) implements Serializable {
}
