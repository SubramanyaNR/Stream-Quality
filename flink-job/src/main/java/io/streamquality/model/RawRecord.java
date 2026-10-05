package io.streamquality.model;

import java.io.Serializable;
import java.util.LinkedHashMap;

/** Unparsed Kafka record: kept as bytes so the dead-letter path is lossless. */
public record RawRecord(String topic, int partition, long offset, long timestampMs,
                        byte[] key, byte[] value, LinkedHashMap<String, byte[]> headers) implements Serializable {}
