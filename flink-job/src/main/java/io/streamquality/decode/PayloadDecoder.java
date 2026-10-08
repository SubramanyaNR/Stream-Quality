package io.streamquality.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.registry.RegisteredSchema;
import io.streamquality.registry.SchemaProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

/**
 * Turns raw Kafka value bytes into a JSON tree, whatever the serializer was:
 *   plain JSON                                     -> parsed as is
 *   Confluent wire format  0x00 | 4-byte id | body -> schema fetched BY ID from the registry:
 *       JSON Schema  -> body is JSON, validated later against that exact schema
 *       AVRO         -> body is decoded with the writer schema, converted to JSON
 *       PROTOBUF     -> not supported (dead-lettered with reason unsupported_format)
 * Framing is only recognised when a registry that supports lookup by id is configured (sq.source.framing=auto, the
 * default); JSON text can never start with 0x00, so detection is unambiguous.
 *
 * Registry unreachable + id not cached: a framed body that is valid JSON is still read (validation is skipped);
 * anything else is returned as `undecoded` so volume still counts it but field-level checks ignore it.
 */
public final class PayloadDecoder {
    public static final int HEADER = 5;

    /** Outcome of decoding one value. Exactly one of: json != null | undecoded | failReason != null. */
    public record Decoded(JsonNode json, int schemaId, RegisteredSchema schema, boolean undecoded, String failReason, String failDetail) {
        static Decoded ok(JsonNode j, int id, RegisteredSchema s) { return new Decoded(j, id, s, false, null, null); }
        static Decoded fail(String reason, String detail) { return new Decoded(null, -1, null, false, reason, detail); }
        static Decoded undecodable() { return new Decoded(null, -1, null, true, null, null); }
        public boolean isAvro() { return schema != null && RegisteredSchema.AVRO.equals(schema.type()); }
    }

    private final ObjectMapper mapper;
    private final SchemaProvider schemas;                 // null = no registry
    private final boolean framing;
    private final Map<Integer, GenericDatumReader<GenericRecord>> avroReaders = new ConcurrentHashMap<>();

    public PayloadDecoder(ObjectMapper mapper, SchemaProvider schemas, boolean framingEnabled) {
        this.mapper = mapper;
        this.schemas = schemas;
        this.framing = framingEnabled && schemas != null && schemas.supportsIds();
    }

    public Decoded decode(byte[] value) {
        if (!framing || value.length < HEADER || value[0] != 0) return plain(value, 0, -1, null);
        int id = ((value[1] & 0xFF) << 24) | ((value[2] & 0xFF) << 16) | ((value[3] & 0xFF) << 8) | (value[4] & 0xFF);
        SchemaProvider.IdLookup l = schemas.lookupId(id);
        switch (l.state()) {
            case NOT_FOUND:
                return Decoded.fail(DlqRecord.UNKNOWN_SCHEMA_ID, "schema id " + id + " is not registered");
            case UNAVAILABLE: {
                Decoded best = plain(value, HEADER, -1, null);               // JSON bodies need no schema to be read
                return best.json() != null ? best : Decoded.undecodable();
            }
            default:
                break;
        }
        RegisteredSchema s = l.schema();
        switch (s.type()) {
            case RegisteredSchema.JSON:
                return plain(value, HEADER, id, s);
            case RegisteredSchema.AVRO:
                return avro(value, id, s);
            default:
                return Decoded.fail(DlqRecord.UNSUPPORTED_FORMAT, s.type() + " payloads are not supported (schema id " + id + ")");
        }
    }

    private Decoded plain(byte[] value, int offset, int id, RegisteredSchema s) {
        try {
            JsonNode n = mapper.readTree(value, offset, value.length - offset);
            if (n == null || n.isMissingNode()) return Decoded.fail(DlqRecord.INVALID_JSON, "empty payload");
            return Decoded.ok(n, id, s);
        } catch (Exception e) {
            return Decoded.fail(DlqRecord.INVALID_JSON, e.getMessage());
        }
    }

    private Decoded avro(byte[] value, int id, RegisteredSchema s) {
        try {
            GenericDatumReader<GenericRecord> reader = avroReaders.computeIfAbsent(id, k -> new GenericDatumReader<>(new Schema.Parser().parse(s.text())));
            Object datum = reader.read(null, DecoderFactory.get().binaryDecoder(value, HEADER, value.length - HEADER, null));
            return Decoded.ok(AvroJson.toJson(datum, mapper), id, s);
        } catch (Exception e) {
            return Decoded.fail(DlqRecord.AVRO_DECODE_FAILED, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
