package io.streamquality.dlq;

import java.io.Serializable;

/** Dead-letter envelope. originalKey/originalValue are the untouched bytes. */
public record DlqRecord(String reason, String detail, String topic, int partition, long offset,
                        long timestampMs, byte[] originalKey, byte[] originalValue) implements Serializable {
    public static final String INVALID_JSON = "invalid_json";
    public static final String NULL_PAYLOAD = "null_payload";
    public static final String NOT_AN_OBJECT = "not_a_json_object";
    public static final String BAD_TIMESTAMP = "bad_timestamp";
    public static final String REQUIRED_FIELD_MISSING = "required_field_missing";
}
