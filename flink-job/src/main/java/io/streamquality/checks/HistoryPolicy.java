package io.streamquality.checks;

import io.streamquality.model.Status;

/**
 * Which windows are allowed to become part of the baseline. A monitor must not learn from the anomalies it is
 * flagging (a sustained spike would drag the baseline up and silence itself), so only OK windows are recorded.
 * If a violation persists for more than `rebaselineAfter` consecutive windows it is treated as the new normal and
 * learning resumes - otherwise a legitimate permanent level shift would alert forever.
 */
public final class HistoryPolicy {
    private HistoryPolicy() {}

    public static int nextStreak(Status s, int streak) { return s == Status.OK ? 0 : streak + 1; }

    public static boolean shouldRecord(Status s, int streakAfter, int rebaselineAfter) {
        return s == Status.OK || streakAfter > rebaselineAfter;
    }
}
