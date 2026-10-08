package io.streamquality.operators;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.streamquality.checks.NullRateCheck;
import io.streamquality.config.Thresholds;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.RawRecord;
import io.streamquality.registry.RegistryConfig;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** ParseFn end to end with a Confluent-API registry over real HTTP (Karapace/Confluent/Redpanda all answer like this stub). */
class ParseFnRegistryTest {
    static final ObjectMapper M = new ObjectMapper();
    static final String AVRO = "{\"type\":\"record\",\"name\":\"O\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ts\",\"type\":\"long\"}]}";
    static final String JSON_SCHEMA = "{\"type\":\"object\",\"required\":[\"id\"],\"properties\":{\"id\":{\"type\":\"string\"}}}";
    static final Thresholds TH = new Thresholds("event_time_field: ts\ntopics: { t: { fields: { id: { required: true }, note: {} } } }");

    HttpServer server;
    volatile boolean down;
    OneInputStreamOperatorTestHarness<RawRecord, ParsedRecord> h;

    @BeforeEach void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/schemas/ids/", ex -> {
            int id = Integer.parseInt(ex.getRequestURI().getPath().substring("/schemas/ids/".length()));
            if (down) { ex.sendResponseHeaders(503, -1); ex.close(); return; }
            String b = switch (id) {
                case 1 -> M.createObjectNode().put("schema", JSON_SCHEMA).put("schemaType", "JSON").toString();
                case 2 -> M.createObjectNode().put("schema", AVRO).toString();
                default -> null;
            };
            byte[] out = (b != null ? b : "{\"error_code\":40403}").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(b != null ? 200 : 404, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        Properties p = new Properties();
        p.setProperty("sq.registry.enabled", "true"); p.setProperty("sq.registry.type", "confluent");
        p.setProperty("sq.registry.url", "http://127.0.0.1:" + server.getAddress().getPort());
        p.setProperty("sq.registry.timeout.ms", "1500"); p.setProperty("sq.registry.breaker.reset.ms", "600000");
        h = ProcessFunctionTestHarnesses.forProcessFunction(new ParseFn(TH, RegistryConfig.from(p), true));
        h.open();
    }

    @AfterEach void stop() throws Exception { h.close(); server.stop(0); }

    static RawRecord raw(byte[] v) { return new RawRecord("t", 0, 1, 1_700_000_000_000L, null, v, new LinkedHashMap<>()); }

    static byte[] frame(int id, byte[] body) { return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(id).put(body).array(); }

    static byte[] avro(String id, String note) throws Exception {
        Schema s = new Schema.Parser().parse(AVRO);
        GenericRecord r = new GenericData.Record(s);
        r.put("id", id); r.put("note", note); r.put("ts", 1_700_000_005_000L);
        var out = new ByteArrayOutputStream(); var enc = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(s).write(r, enc); enc.flush();
        return out.toByteArray();
    }

    List<DlqRecord> dlq() {
        return h.getSideOutput(ParseFn.DLQ) == null ? List.of() : h.getSideOutput(ParseFn.DLQ).stream().map(r -> (DlqRecord) r.getValue()).toList();
    }

    @Test void framedAvroYieldsFieldsPayloadEventTimeAndCountsAsStructurallyValid() throws Exception {
        h.processElement(raw(frame(2, avro("a-1", "hello"))), 0);
        ParsedRecord p = h.extractOutputValues().get(0);
        assertThat(p.fields()).containsEntry("id", "a-1").containsEntry("note", "hello");
        assertThat(p.eventTimeMs()).isEqualTo(1_700_000_005_000L);
        assertThat(p.eventTimeFromPayload()).isTrue();
        assertThat(p.structural()).isEqualTo(ParsedRecord.STRUCT_VALID);
        assertThat(p.undecoded()).isFalse();
        assertThat(dlq()).isEmpty();
    }

    @Test void avroNullBranchIsAMissingFieldForNullRate() throws Exception {
        h.processElement(raw(frame(2, avro("a-1", null))), 0);
        assertThat(h.extractOutputValues().get(0).fields()).containsOnlyKeys("id");
    }

    @Test void framedJsonIsValidatedAgainstTheSchemaItWasWrittenWith() throws Exception {
        h.processElement(raw(frame(1, "{\"id\":\"ok\",\"ts\":1700000005000}".getBytes())), 0);
        h.processElement(raw(frame(1, "{\"id\":42,\"ts\":1700000005000}".getBytes())), 0);        // id must be a string
        var out = h.extractOutputValues();
        assertThat(out.get(0).structural()).isEqualTo(ParsedRecord.STRUCT_VALID);
        assertThat(out.get(1).structural()).isEqualTo(ParsedRecord.STRUCT_INVALID);
        assertThat(dlq()).extracting(DlqRecord::reason).containsExactly(DlqRecord.SCHEMA_VIOLATION);
    }

    @Test void unknownIdAndCorruptAvroGoToTheDlqWithTheirOwnReasons() throws Exception {
        h.processElement(raw(frame(77, new byte[] {1})), 0);
        h.processElement(raw(frame(2, new byte[] {9, 9})), 0);
        assertThat(h.extractOutputValues()).isEmpty();
        assertThat(dlq()).extracting(DlqRecord::reason).containsExactly(DlqRecord.UNKNOWN_SCHEMA_ID, DlqRecord.AVRO_DECODE_FAILED);
    }

    @Test void registryOutageUndecodedAvroIsCountedButCannotInflateNullRates() throws Exception {
        down = true;
        h.processElement(raw(frame(2, avro("a-1", "x"))), 0);                // never seen this id before, registry is down
        ParsedRecord p = h.extractOutputValues().get(0);
        assertThat(p.undecoded()).isTrue();
        assertThat(p.fields()).isEmpty();
        assertThat(dlq()).isEmpty();                                          // an outage must not flood the DLQ
        var c = new NullRateCheck(); var acc = c.createAccumulator();
        c.add(p, acc);
        assertThat(acc.total).isZero();                                       // volume counts it; null-rate ignores it
    }
}
