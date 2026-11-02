package io.streamquality.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamquality.model.CheckResult;
import io.streamquality.model.CheckType;
import io.streamquality.model.Status;
import org.junit.jupiter.api.Test;

class ResultMapperTest {
    static JsonNode row(CheckResult r) throws Exception { return new ObjectMapper().readTree(ResultMapper.toRow(r, "j").json()); }

    @Test void okRowsDropDetailsButKeepBaselineNumbers() throws Exception {
        JsonNode n = row(CheckResult.of("t", "", CheckType.VOLUME, 0, 60_000, 20, 0, Status.OK,
                "{\"count\":1200,\"baseline_rate\":19.5,\"warming_up\":false}"));
        assertThat(n.has("details")).isFalse();
        assertThat(n.get("baseline").asDouble()).isEqualTo(19.5);
    }

    @Test void nonOkRowsKeepDetails() throws Exception {
        JsonNode n = row(CheckResult.of("t", "f", CheckType.NULL_RATE, 0, 60_000, 0.5, 0.2, Status.FAIL, "{\"nulls\":5,\"total\":10}"));
        assertThat(n.get("details").asText()).contains("\"nulls\":5");
    }

    @Test void freshnessPercentilesBecomeColumns() throws Exception {
        JsonNode n = row(CheckResult.of("t", "", CheckType.FRESHNESS, 0, 60_000, 900, 30000, Status.OK, "{\"p50_ms\":310,\"p99_ms\":1200}"));
        assertThat(n.get("p50_ms").asDouble()).isEqualTo(310);
        assertThat(n.get("p99_ms").asDouble()).isEqualTo(1200);
        assertThat(n.has("baseline")).isFalse();
    }

    @Test void cardinalityNullBaselineIsOmitted() throws Exception {
        JsonNode n = row(CheckResult.of("t", "f", CheckType.CARDINALITY, 0, 60_000, 10, 0, Status.OK, "{\"estimate\":10.0,\"baseline\":null}"));
        assertThat(n.has("baseline")).isFalse();
    }
}
