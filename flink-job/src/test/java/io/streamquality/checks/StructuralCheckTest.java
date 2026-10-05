package io.streamquality.checks;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.config.Thresholds;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.Status;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StructuralCheckTest {
    static final Thresholds TH = new Thresholds("defaults: { structural: { warn: 0.0, fail: 0.05 } }\ntopics: { t: { fields: {} } }");
    static WindowContext ctx() {
        return new WindowContext() {
            public String topic() { return "t"; }
            public long windowStartMs() { return 0; }
            public long windowEndMs() { return 1000; }
            public Thresholds thresholds() { return TH; }
            public History history() { return History.EMPTY; }
        };
    }
    static ParsedRecord rec(byte s, String err) { return new ParsedRecord("t", 0, 0, 1, false, 1, false, Map.of(), s, err); }

    @Test void rateStatusAndTopErrors() {
        var c = new StructuralCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 90; i++) c.add(rec(ParsedRecord.STRUCT_VALID, null), a);
        for (int i = 0; i < 7; i++) c.add(rec(ParsedRecord.STRUCT_INVALID, "$.amount: string found, number expected"), a);
        for (int i = 0; i < 3; i++) c.add(rec(ParsedRecord.STRUCT_INVALID, "$.id: is missing"), a);
        var r = c.evaluate(a, ctx()).get(0);
        assertThat(r.value()).isEqualTo(0.1);
        assertThat(r.status()).isEqualTo(Status.FAIL);
        assertThat(r.detailsJson()).contains("\"checked\":100").contains("\"invalid\":10").contains("amount");
    }

    @Test void oneBadRecordWarns() {
        var c = new StructuralCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 999; i++) c.add(rec(ParsedRecord.STRUCT_VALID, null), a);
        c.add(rec(ParsedRecord.STRUCT_INVALID, "x"), a);
        assertThat(c.evaluate(a, ctx()).get(0).status()).isEqualTo(Status.WARN);   // 0.1% > warn 0, <= fail 5%
    }

    @Test void nothingCheckedEmitsNoRowSoRegistryOutagesAreInvisibleToQualityStatus() {
        var c = new StructuralCheck(); var a = c.createAccumulator();
        for (int i = 0; i < 50; i++) c.add(rec(ParsedRecord.STRUCT_SKIPPED, null), a);
        assertThat(c.evaluate(a, ctx())).isEmpty();
    }

    @Test void errorKindsAreCappedAndMerge() {
        var c = new StructuralCheck(); var a = c.createAccumulator(); var b = c.createAccumulator();
        for (int i = 0; i < 20; i++) c.add(rec(ParsedRecord.STRUCT_INVALID, "err" + i), a);
        assertThat(a.errors).hasSize(5);               // bounded memory/state regardless of payload variety
        c.add(rec(ParsedRecord.STRUCT_INVALID, "err0"), b);
        assertThat(c.merge(a, b).errors.get("err0")).isEqualTo(2L);
        assertThat(a.checked).isEqualTo(21);
    }
}
