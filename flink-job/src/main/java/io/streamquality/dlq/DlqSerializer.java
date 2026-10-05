package io.streamquality.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Key = original key (preserves partitioning by entity); value = JSON envelope with base64 payload. */
public final class DlqSerializer implements KafkaRecordSerializationSchema<DlqRecord> {
    private final String dlqTopic;
    private transient ObjectMapper mapper;

    public DlqSerializer(String dlqTopic) { this.dlqTopic = dlqTopic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DlqRecord r, KafkaRecordSerializationSchema.KafkaSinkContext ctx, Long timestamp) {
        if (mapper == null) mapper = new ObjectMapper();
        ObjectNode n = mapper.createObjectNode();
        n.put("reason", r.reason());
        n.put("detail", r.detail());
        n.put("source_topic", r.topic());
        n.put("source_partition", r.partition());
        n.put("source_offset", r.offset());
        n.put("source_timestamp_ms", r.timestampMs());
        n.put("key_b64", r.originalKey() == null ? null : Base64.getEncoder().encodeToString(r.originalKey()));
        n.put("value_b64", r.originalValue() == null ? null : Base64.getEncoder().encodeToString(r.originalValue()));
        return new ProducerRecord<>(dlqTopic, null, r.originalKey(), n.toString().getBytes(StandardCharsets.UTF_8));
    }
}
