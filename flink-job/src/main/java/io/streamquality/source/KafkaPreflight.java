package io.streamquality.source;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/** Fail fast, at submit time, with an actionable message (instead of a restart loop inside the cluster). */
public final class KafkaPreflight {
    private KafkaPreflight() {}

    public static void verifyTopicsExist(Properties kafkaClientProps, Collection<String> mustExist) throws Exception {
        Properties p = new Properties();
        for (String k : kafkaClientProps.stringPropertyNames())
            if (AdminClientConfig.configNames().contains(k)) p.setProperty(k, kafkaClientProps.getProperty(k));
        p.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        try (AdminClient admin = AdminClient.create(p)) {
            Set<String> existing;
            try { existing = admin.listTopics().names().get(20, TimeUnit.SECONDS); }
            catch (Exception e) {
                throw new IllegalStateException("Cannot reach Kafka at " + p.getProperty("bootstrap.servers")
                        + " with config/kafka.properties (check advertised listeners / auth): " + e.getMessage(), e);
            }
            for (String t : mustExist)
                if (!existing.contains(t))
                    throw new IllegalStateException("Required topic '" + t + "' does not exist. For the dead-letter topic run: make dlq");
        }
    }
}
