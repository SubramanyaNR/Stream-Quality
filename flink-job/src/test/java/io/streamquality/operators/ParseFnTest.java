package io.streamquality.operators;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.config.Thresholds;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.RawRecord;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ParseFnTest {
    OneInputStreamOperatorTestHarness<RawRecord, ParsedRecord> h;

    @BeforeEach void open() throws Exception {
        var th = new Thresholds("""
                event_time_field: ts
                topics: { t: { fields: { id: { required: true }, user.name: {} } } }
                """);
        h = ProcessFunctionTestHarnesses.forProcessFunction(new ParseFn(th));
        h.open();
    }

    @AfterEach void close() throws Exception { h.close(); }

    static RawRecord raw(String json) {
        return new RawRecord("t", 1, 7, 1_700_000_000_000L, "k".getBytes(),
                json == null ? null : json.getBytes(StandardCharsets.UTF_8), new LinkedHashMap<>());
    }

    List<DlqRecord> dlq() {
        return h.getSideOutput(ParseFn.DLQ) == null ? List.of()
                : h.getSideOutput(ParseFn.DLQ).stream().map(r -> (DlqRecord) r.getValue()).toList();
    }

    @Test void goodRecordExtractsFieldsAndPayloadEventTime() throws Exception {
        h.processElement(raw("{\"id\":\"a\",\"ts\":1700000005,\"user\":{\"name\":\"bob\"}}"), 0);
        ParsedRecord p = h.extractOutputValues().get(0);
        assertThat(p.fields()).containsEntry("id", "a").containsEntry("user.name", "bob");
        assertThat(p.eventTimeMs()).isEqualTo(1_700_000_005_000L);       // epoch seconds auto-scaled
        assertThat(p.eventTimeFromPayload()).isTrue();
        assertThat(dlq()).isEmpty();
    }

    @Test void nullAndMissingFieldsAreAbsent() throws Exception {
        h.processElement(raw("{\"id\":\"a\",\"user\":{\"name\":null}}"), 0);
        ParsedRecord p = h.extractOutputValues().get(0);
        assertThat(p.fields()).containsOnlyKeys("id");
        assertThat(p.eventTimeFromPayload()).isFalse();                  // no ts -> kafka timestamp
        assertThat(p.eventTimeMs()).isEqualTo(1_700_000_000_000L);
    }

    @Test void invalidJsonGoesOnlyToDlqWithOriginalBytes() throws Exception {
        h.processElement(raw("{not json"), 0);
        assertThat(h.extractOutputValues()).isEmpty();
        DlqRecord d = dlq().get(0);
        assertThat(d.reason()).isEqualTo(DlqRecord.INVALID_JSON);
        assertThat(new String(d.originalValue())).isEqualTo("{not json");
        assertThat(d.offset()).isEqualTo(7);
    }

    @Test void tombstoneArrayAndBadTimestamp() throws Exception {
        h.processElement(raw(null), 0);
        h.processElement(raw("[1,2]"), 0);
        h.processElement(raw("{\"id\":\"a\",\"ts\":\"yesterday\"}"), 0);
        h.processElement(raw("{\"id\":\"a\",\"ts\":99999999999999}"), 0);   // far future
        assertThat(h.extractOutputValues()).isEmpty();
        assertThat(dlq()).extracting(DlqRecord::reason).containsExactly(
                DlqRecord.NULL_PAYLOAD, DlqRecord.NOT_AN_OBJECT, DlqRecord.BAD_TIMESTAMP, DlqRecord.BAD_TIMESTAMP);
    }

    @Test void missingRequiredFieldIsAnalysedAndDeadLettered() throws Exception {
        h.processElement(raw("{\"user\":{\"name\":\"x\"}}"), 0);
        assertThat(h.extractOutputValues()).hasSize(1);                  // still counted by null-rate
        assertThat(dlq()).hasSize(1);
        assertThat(dlq().get(0).reason()).isEqualTo(DlqRecord.REQUIRED_FIELD_MISSING);
        assertThat(dlq().get(0).detail()).isEqualTo("id");
    }
}
