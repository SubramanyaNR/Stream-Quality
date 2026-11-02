package io.streamquality.source;

import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/**
 * Finds the topics the Kafka source consumes through a regex, so silence detection (ticks -> empty windows) also
 * covers pattern-matched topics, not only those listed in thresholds.yaml. A silent topic emits no records, so
 * without this a pattern topic that goes quiet would never produce a volume=0 row.
 */
public final class TopicDiscovery implements Serializable {
    /** Lists all topic names; overridable for tests. */
    public interface Lister extends Serializable { Set<String> listTopics() throws Exception; }

    private final Pattern pattern;
    private final int maxTopics;
    private final Lister lister;

    public TopicDiscovery(Pattern pattern, int maxTopics, Lister lister) {
        this.pattern = pattern; this.maxTopics = maxTopics; this.lister = lister;
    }

    public static TopicDiscovery kafka(Properties kafkaClientProps, Pattern pattern, int maxTopics) {
        Properties p = new Properties();
        for (String k : kafkaClientProps.stringPropertyNames())
            if (AdminClientConfig.configNames().contains(k)) p.setProperty(k, kafkaClientProps.getProperty(k));
        p.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return new TopicDiscovery(pattern, maxTopics, () -> {
            try (AdminClient admin = AdminClient.create(p)) { return admin.listTopics().names().get(20, TimeUnit.SECONDS); }
        });
    }

    /** Matching topics (sorted, capped at maxTopics so a broad regex cannot explode tick and gauge volume). */
    public Set<String> discover() throws Exception {
        return select(lister.listTopics(), pattern, maxTopics);
    }

    static Set<String> select(Collection<String> all, Pattern pattern, int max) {
        Set<String> out = new TreeSet<>();
        for (String t : all) if (pattern.matcher(t).matches()) out.add(t);
        if (out.size() > max) {
            Set<String> capped = new TreeSet<>();
            for (String t : out) { if (capped.size() >= max) break; capped.add(t); }
            return capped;
        }
        return out;
    }
}
