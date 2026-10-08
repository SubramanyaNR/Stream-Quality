package io.streamquality.source;

import java.util.regex.Pattern;

/** Builds the topic regex for the Kafka source: include pattern minus exclude pattern minus the dead-letter topic. */
public final class TopicSelector {
    private TopicSelector() {}

    public static Pattern pattern(String include, String exclude, String dlqTopic) {
        String skip = Pattern.quote(dlqTopic);
        if (exclude != null && !exclude.isBlank()) skip = "(?:" + exclude.trim() + ")|" + skip;
        return Pattern.compile("(?!(?:" + skip + ")$)(?:" + include.trim() + ")");
    }
}
