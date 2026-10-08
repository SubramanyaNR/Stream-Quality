package io.streamquality.registry;

import java.io.Serializable;

/** A schema as stored in a registry. type: JSON | AVRO | PROTOBUF (Confluent's schemaType; Avro is the default when absent). */
public record RegisteredSchema(String type, String text) implements Serializable {
    public static final String JSON = "JSON", AVRO = "AVRO", PROTOBUF = "PROTOBUF";
}
