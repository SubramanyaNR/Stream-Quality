package io.streamquality.source;

import io.streamquality.model.RawRecord;
import java.io.IOException;
import java.util.LinkedHashMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public final class RawRecordDeserializer implements KafkaRecordDeserializationSchema<RawRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<RawRecord> out) throws IOException {
        LinkedHashMap<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : r.headers()) headers.put(h.key(), h.value());
        out.collect(new RawRecord(r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), r.value(), headers));
    }

    @Override
    public TypeInformation<RawRecord> getProducedType() { return TypeInformation.of(RawRecord.class); }
}
