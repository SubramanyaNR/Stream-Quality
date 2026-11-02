package io.streamquality.registry;

import java.io.Serializable;
import java.util.List;

/**
 * A schema as stored in a registry. type: JSON | AVRO | PROTOBUF (Confluent's schemaType; Avro is the default when absent).
 * references: other registered schemas this one imports (Protobuf `import "x.proto"`): name = the import path.
 */
public record RegisteredSchema(String type, String text, List<Reference> references) implements Serializable {
    public static final String JSON = "JSON", AVRO = "AVRO", PROTOBUF = "PROTOBUF";

    public record Reference(String name, String subject, int version) implements Serializable {}

    public RegisteredSchema(String type, String text) { this(type, text, List.of()); }
}
