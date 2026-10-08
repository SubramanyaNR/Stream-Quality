package io.streamquality.dlq;

import java.io.Serializable;

/** Dead-letter envelope. originalKey/originalValue are the untouched bytes. */
public record DlqRecord(String reason, String detail, String topic, int partition, long offset,
                        long timestampMs, byte[] originalKey, byte[] originalValue) implements Serializable {
    public static final String INVALID_JSON = "invalid_json";
    public static final String NULL_PAYLOAD = "null_payload";
    public static final String NOT_AN_OBJECT = "not_a_json_object";
    public static final String BAD_TIMESTAMP = "bad_timestamp";
    public static final String AVRO_DECODE_FAILED = "avro_decode_failed";
    public static final String UNSUPPORTED_FORMAT = "unsupported_format";
    public static final String UNKNOWN_SCHEMA_ID = "unknown_schema_id";
    public static final String SCHEMA_VIOLATION = "schema_violation";
    public static final String REQUIRED_FIELD_MISSING = "required_field_missing";
}
