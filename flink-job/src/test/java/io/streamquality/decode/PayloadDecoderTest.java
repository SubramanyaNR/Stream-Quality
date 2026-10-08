package io.streamquality.decode;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.registry.ConfluentRegistryClient;
import io.streamquality.registry.RegistryConfig;
import io.streamquality.registry.SchemaProvider;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real HTTP between decoder -> SchemaProvider -> ConfluentRegistryClient -> a stub registry (swap-in for Confluent/Karapace). */
class PayloadDecoderTest {
    static final String AVRO_SCHEMA = """
            {"type":"record","name":"Order","fields":[
              {"name":"order_id","type":"string"},
              {"name":"customer_id","type":["null","string"],"default":null},
              {"name":"amount","type":"double"},
              {"name":"tags","type":{"type":"array","items":"string"}},
              {"name":"attrs","type":{"type":"map","values":"long"}},
              {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B"]}},
              {"name":"blob","type":"bytes"},
              {"name":"ts","type":"long"}]}""";
    static final String JSON_SCHEMA = "{\"type\":\"object\",\"required\":[\"order_id\"]}";
    static final ObjectMapper M = new ObjectMapper();

    HttpServer server;
    final Map<Integer, String> schemas = new ConcurrentHashMap<>();       // id -> response body
    volatile boolean down;
    SchemaProvider provider;

    @BeforeEach void start() throws Exception {
        schemas.put(1, body(JSON_SCHEMA, "JSON"));
        schemas.put(2, body(AVRO_SCHEMA, null));
        schemas.put(3, body("syntax = \"proto3\";", "PROTOBUF"));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/schemas/ids/", ex -> {
            if (down) { ex.sendResponseHeaders(503, -1); ex.close(); return; }
            String b = schemas.get(Integer.parseInt(ex.getRequestURI().getPath().substring("/schemas/ids/".length())));
            byte[] out = (b != null ? b : "{\"error_code\":40403,\"message\":\"Schema not found\"}").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(b != null ? 200 : 404, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        Properties p = new Properties();
        p.setProperty("sq.registry.enabled", "true");
        p.setProperty("sq.registry.type", "karapace");
        p.setProperty("sq.registry.url", "http://127.0.0.1:" + server.getAddress().getPort());
        p.setProperty("sq.registry.timeout.ms", "1500");
        p.setProperty("sq.registry.breaker.failures", "2");
        p.setProperty("sq.registry.breaker.reset.ms", "600000");
        RegistryConfig cfg = RegistryConfig.from(p);
        provider = new SchemaProvider(cfg, new ConfluentRegistryClient(cfg), System::currentTimeMillis);
    }

    @AfterEach void stop() { server.stop(0); provider.close(); }

    static String body(String schema, String type) throws Exception {
        var o = M.createObjectNode().put("schema", schema);
        if (type != null) o.put("schemaType", type);
        return o.toString();
    }

    static byte[] frame(int id, byte[] body) {
        ByteBuffer b = ByteBuffer.allocate(5 + body.length);
        b.put((byte) 0).putInt(id).put(body);
        return b.array();
    }

    static byte[] avroOrder(String customer) throws Exception {
        Schema s = new Schema.Parser().parse(AVRO_SCHEMA);
        GenericRecord r = new GenericData.Record(s);
        r.put("order_id", "o-1"); r.put("customer_id", customer); r.put("amount", 12.5);
        r.put("tags", List.of("x", "y")); r.put("attrs", Map.of("k", 7L));
        r.put("kind", new GenericData.EnumSymbol(s.getField("kind").schema(), "B"));
        r.put("blob", ByteBuffer.wrap(new byte[] {1, 2, 3})); r.put("ts", 1_700_000_000_000L);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(s).write(r, enc);
        enc.flush();
        return out.toByteArray();
    }

    PayloadDecoder decoder(boolean framing) { return new PayloadDecoder(M, provider, framing); }

    @Test void plainJsonIsUntouched() {
        var d = decoder(true).decode("{\"order_id\":\"a\"}".getBytes());
        assertThat(d.json().get("order_id").asText()).isEqualTo("a");
        assertThat(d.schemaId()).isEqualTo(-1);
    }

    @Test void framedJsonSchemaStripsHeaderAndKeepsTheWriterSchemaId() {
        var d = decoder(true).decode(frame(1, "{\"order_id\":\"a\"}".getBytes()));
        assertThat(d.json().get("order_id").asText()).isEqualTo("a");
        assertThat(d.schemaId()).isEqualTo(1);
        assertThat(d.schema().type()).isEqualTo("JSON");
        assertThat(provider.validateWithSchema(1, d.schema(), d.json()).state()).isEqualTo(SchemaProvider.State.VALID);
        var bad = decoder(true).decode(frame(1, "{\"nope\":1}".getBytes()));
        assertThat(provider.validateWithSchema(1, bad.schema(), bad.json()).state()).isEqualTo(SchemaProvider.State.INVALID);
    }

    @Test void avroDecodesToTheSameJsonShapeAsEverythingElse() throws Exception {
        var d = decoder(true).decode(frame(2, avroOrder("c-9")));
        assertThat(d.isAvro()).isTrue();
        var j = d.json();
        assertThat(j.get("order_id").asText()).isEqualTo("o-1");
        assertThat(j.get("customer_id").asText()).isEqualTo("c-9");          // union is transparent
        assertThat(j.get("amount").asDouble()).isEqualTo(12.5);
        assertThat(j.get("tags").get(1).asText()).isEqualTo("y");
        assertThat(j.get("attrs").get("k").asLong()).isEqualTo(7);
        assertThat(j.get("kind").asText()).isEqualTo("B");
        assertThat(j.get("blob").asText()).isEqualTo("AQID");                // base64
        assertThat(j.get("ts").asLong()).isEqualTo(1_700_000_000_000L);
    }

    @Test void avroNullUnionBranchBecomesJsonNull() throws Exception {
        assertThat(decoder(true).decode(frame(2, avroOrder(null))).json().get("customer_id").isNull()).isTrue();
    }

    @Test void corruptAvroUnknownIdAndProtobufAreDeadLetteredWithSpecificReasons() {
        assertThat(decoder(true).decode(frame(2, new byte[] {1, 2})).failReason()).isEqualTo(DlqRecord.AVRO_DECODE_FAILED);
        assertThat(decoder(true).decode(frame(999, new byte[] {1})).failReason()).isEqualTo(DlqRecord.UNKNOWN_SCHEMA_ID);
        assertThat(decoder(true).decode(frame(3, new byte[] {1})).failReason()).isEqualTo(DlqRecord.UNSUPPORTED_FORMAT);
        assertThat(decoder(true).decode(frame(1, "{broken".getBytes())).failReason()).isEqualTo(DlqRecord.INVALID_JSON);
    }

    @Test void registryDownFramedJsonIsStillReadButFramedAvroIsUndecodedNotDeadLettered() throws Exception {
        down = true;
        var json = decoder(true).decode(frame(41, "{\"order_id\":\"a\"}".getBytes()));
        assertThat(json.json()).isNotNull();
        assertThat(json.schema()).isNull();                                   // so validation is skipped, not failed
        var avro = decoder(true).decode(frame(42, avroOrder("c")));
        assertThat(avro.undecoded()).isTrue();
        assertThat(avro.failReason()).isNull();                               // no DLQ flood during a registry outage
        assertThat(provider.isUp()).isFalse();
    }

    @Test void framingCanBeDisabledAndPlainJsonStartingWithNulIsNeverMisread() {
        var d = decoder(false).decode(frame(1, "{\"order_id\":\"a\"}".getBytes()));
        assertThat(d.failReason()).isEqualTo(DlqRecord.INVALID_JSON);         // framing off: the header is not JSON
        assertThat(decoder(true).decode("".getBytes()).failReason()).isEqualTo(DlqRecord.INVALID_JSON);
    }

    @Test void schemasByIdAreCachedForGood() {
        var dec = decoder(true);
        dec.decode(frame(1, "{\"order_id\":\"a\"}".getBytes()));
        down = true;                                                          // registry dies: ids are immutable, so nothing is lost
        assertThat(dec.decode(frame(1, "{\"order_id\":\"b\"}".getBytes())).schema()).isNotNull();
    }
}
