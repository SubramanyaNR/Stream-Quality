package io.streamquality.checks;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamquality.model.Status;
import org.junit.jupiter.api.Test;

class HistoryPolicyTest {
    @Test void healthyWindowsAreRecordedAnomaliesAreNot() {
        assertThat(HistoryPolicy.shouldRecord(Status.OK, 0, 30)).isTrue();
        assertThat(HistoryPolicy.shouldRecord(Status.WARN, 1, 30)).isFalse();
        assertThat(HistoryPolicy.shouldRecord(Status.FAIL, 5, 30)).isFalse();
    }

    @Test void streakCountsConsecutiveViolationsAndResetsOnOk() {
        int s = 0;
        s = HistoryPolicy.nextStreak(Status.WARN, s);
        s = HistoryPolicy.nextStreak(Status.FAIL, s);
        assertThat(s).isEqualTo(2);
        assertThat(HistoryPolicy.nextStreak(Status.OK, s)).isZero();
    }

    @Test void persistentShiftBecomesTheNewBaseline() {
        int s = 0, recorded = 0;
        for (int i = 0; i < 40; i++) {
            s = HistoryPolicy.nextStreak(Status.FAIL, s);
            if (HistoryPolicy.shouldRecord(Status.FAIL, s, 30)) recorded++;
        }
        assertThat(recorded).isEqualTo(10);              // windows 31..40: learning resumes after 30 consecutive violations
    }
}
