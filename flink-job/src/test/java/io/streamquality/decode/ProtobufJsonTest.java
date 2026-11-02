package io.streamquality.decode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamquality.registry.RegisteredSchema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Payloads are hand-encoded (varints/tags), independent of the library that decodes them. */
class ProtobufJsonTest {
    static final String PROTO = """
            syntax = "proto3";
            package bank;
            import "common/money.proto";
            enum Status { UNKNOWN = 0; ACTIVE = 1; CLOSED = 2; }
            message Txn {
              string id = 1;
              int64 amount_cents = 2;
              double score = 3;
              repeated string tags = 4;
              Status status = 5;
              Inner inner = 6;
              map<string, string> attrs = 7;
              optional string note = 8;
              bool flag = 9;
              bytes blob = 10;
              common.Money money = 11;
              message Inner { string city = 1; }
            }
            message Other { string x = 1; }
            message Nums { repeated int32 xs = 1; sint64 z = 2; fixed32 f = 3; uint64 big = 4; oneof choice { string a = 5; int32 b = 6; } }
            """;
    static final String MONEY = "syntax = \"proto3\"; package common; message Money { string currency = 1; int64 units = 2; }";
    static final RegisteredSchema SCHEMA = new RegisteredSchema("PROTOBUF", PROTO,
            List.of(new RegisteredSchema.Reference("common/money.proto", "money-value", 1)));
    static final ObjectMapper M = new ObjectMapper();

    static ProtobufJson decoder() {
        return new ProtobufJson(M, ref -> ref.subject().equals("money-value") ? new RegisteredSchema("PROTOBUF", MONEY) : null);
    }

    // ---- tiny protobuf encoder ----
    static void varint(ByteArrayOutputStream o, long v) { while ((v & ~0x7FL) != 0) { o.write((int) ((v & 0x7F) | 0x80)); v >>>= 7; } o.write((int) v); }
    static void tag(ByteArrayOutputStream o, int field, int wire) { varint(o, ((long) field << 3) | wire); }
    static void str(ByteArrayOutputStream o, int field, String s) { bytes(o, field, s.getBytes(StandardCharsets.UTF_8)); }
    static void bytes(ByteArrayOutputStream o, int field, byte[] b) { tag(o, field, 2); varint(o, b.length); o.writeBytes(b); }
    static void num(ByteArrayOutputStream o, int field, long v) { tag(o, field, 0); varint(o, v); }
    static void dbl(ByteArrayOutputStream o, int field, double d) {
        tag(o, field, 1); long b = Double.doubleToLongBits(d);
        for (int i = 0; i < 8; i++) o.write((int) (b >>> (8 * i)) & 0xFF);
    }
    static byte[] framed(byte[] header, ByteArrayOutputStream body) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(0); o.writeBytes(new byte[] {0, 0, 0, 7}); o.writeBytes(header); o.writeBytes(body.toByteArray());
        return o.toByteArray();
    }
    static final byte[] IDX_FIRST = {0};                 // [0] shortened
    static JsonNode decode(byte[] header, ByteArrayOutputStream body) throws IOException {
        return decoder().decode(7, SCHEMA, framed(header, body), 5);
    }

    @Test void decodesEveryFieldKind() throws Exception {
        var b = new ByteArrayOutputStream();
        str(b, 1, "t-42"); num(b, 2, 9_000_000_000L); dbl(b, 3, 2.5);
        str(b, 4, "a"); str(b, 4, "b");
        num(b, 5, 2);
        var inner = new ByteArrayOutputStream(); str(inner, 1, "Oslo"); bytes(b, 6, inner.toByteArray());
        var entry = new ByteArrayOutputStream(); str(entry, 1, "k"); str(entry, 2, "v"); bytes(b, 7, entry.toByteArray());
        str(b, 8, "hello"); num(b, 9, 1); bytes(b, 10, new byte[] {1, 2, 3});
        var money = new ByteArrayOutputStream(); str(money, 1, "NOK"); num(money, 2, 100); bytes(b, 11, money.toByteArray());
        JsonNode j = decode(IDX_FIRST, b);
        assertThat(j.get("id").asText()).isEqualTo("t-42");
        assertThat(j.get("amount_cents").asLong()).isEqualTo(9_000_000_000L);
        assertThat(j.get("score").asDouble()).isEqualTo(2.5);
        assertThat(j.get("tags")).hasSize(2);
        assertThat(j.get("status").asText()).isEqualTo("CLOSED");
        assertThat(j.get("inner").get("city").asText()).isEqualTo("Oslo");
        assertThat(j.get("attrs").get("k").asText()).isEqualTo("v");
        assertThat(j.get("note").asText()).isEqualTo("hello");
        assertThat(j.get("flag").asBoolean()).isTrue();
        assertThat(j.get("blob").asText()).isEqualTo("AQID");
        assertThat(j.get("money").get("currency").asText()).isEqualTo("NOK");     // type from a registry REFERENCE
    }

    @Test void proto3DefaultsAreFilledSoZeroIsNotNull() throws Exception {
        var b = new ByteArrayOutputStream(); str(b, 1, "only-id");
        JsonNode j = decode(IDX_FIRST, b);
        assertThat(j.get("amount_cents").asLong()).isZero();
        assertThat(j.get("flag").asBoolean()).isFalse();
        assertThat(j.get("status").asText()).isEqualTo("UNKNOWN");
        assertThat(j.get("score").asDouble()).isZero();
        assertThat(j.has("note")).as("explicit `optional` keeps presence: unset = absent").isFalse();
        assertThat(j.has("inner")).as("unset message = absent").isFalse();
        assertThat(j.has("tags")).isFalse();
    }

    @Test void messageIndexSelectsTheMessage() throws Exception {
        var b = new ByteArrayOutputStream(); str(b, 1, "second-message");
        JsonNode j = decode(new byte[] {2, 2}, b);                     // count=1 (zigzag 2), index=1 (zigzag 2) -> Other
        assertThat(j.get("x").asText()).isEqualTo("second-message");
    }

    @Test void nestedMessageIndex() throws Exception {
        var b = new ByteArrayOutputStream(); str(b, 1, "Bergen");
        JsonNode j = decode(new byte[] {4, 0, 0}, b);                  // [0,0] -> Txn.Inner
        assertThat(j.get("city").asText()).isEqualTo("Bergen");
    }

    @Test void packedRepeatedZigzagFixedUnsignedOneofAndUnknownFields() throws Exception {
        var b = new ByteArrayOutputStream();
        var packed = new ByteArrayOutputStream(); varint(packed, 1); varint(packed, 300); varint(packed, 70000);
        bytes(b, 1, packed.toByteArray());                       // packed repeated int32
        num(b, 2, 5);                                            // sint64 zigzag(5) = -3
        tag(b, 3, 5); b.writeBytes(new byte[] {7, 0, 0, 0});     // fixed32 = 7
        num(b, 4, -1);                                           // uint64 max (as text)
        num(b, 6, 42);                                           // oneof branch b
        num(b, 99, 1);                                           // unknown field: skipped
        JsonNode j = decode(new byte[] {2, 4}, b);               // [2] -> Nums
        assertThat(j.get("xs")).hasSize(3);
        assertThat(j.get("xs").get(2).asInt()).isEqualTo(70000);
        assertThat(j.get("z").asLong()).isEqualTo(-3);
        assertThat(j.get("f").asLong()).isEqualTo(7);
        assertThat(j.get("big").asText()).isEqualTo("18446744073709551615");
        assertThat(j.get("b").asInt()).isEqualTo(42);
        assertThat(j.has("a")).as("unset oneof member stays absent").isFalse();
    }

    @Test void badIndexAndMalformedBodyAreDecodeErrors() {
        var b = new ByteArrayOutputStream(); str(b, 1, "x");
        assertThatThrownBy(() -> decode(new byte[] {2, 20}, b)).isInstanceOf(ProtobufJson.DecodeException.class);
        var bad = new ByteArrayOutputStream(); bad.writeBytes(new byte[] {0x0A, 0x7F, 0x01});          // length 127 but 1 byte follows
        assertThatThrownBy(() -> decode(IDX_FIRST, bad)).isInstanceOf(ProtobufJson.DecodeException.class);
    }

    @Test void missingReferenceIsASchemaProblemButRegistryOutageIsNot() {
        var b = new ByteArrayOutputStream(); str(b, 1, "x");
        assertThatThrownBy(() -> new ProtobufJson(M, ref -> null).decode(7, SCHEMA, framed(IDX_FIRST, b), 5))
                .isInstanceOf(ProtobufJson.DecodeException.class).hasMessageContaining("not found");
        assertThatThrownBy(() -> new ProtobufJson(M, ref -> { throw new IOException("connection refused"); }).decode(7, SCHEMA, framed(IDX_FIRST, b), 5))
                .isInstanceOf(IOException.class).isNotInstanceOf(ProtobufJson.DecodeException.class);
    }
}
