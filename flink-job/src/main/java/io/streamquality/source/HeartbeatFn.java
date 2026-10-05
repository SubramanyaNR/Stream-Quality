package io.streamquality.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.streamquality.sink.ChRow;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/**
 * Turns a tick into a sq.job_heartbeat row, after probing the external Kafka cluster with the
 * user-supplied client properties (the same ones the source/sink will use). Broker-flavour agnostic:
 * only describeCluster().
 */
public final class HeartbeatFn extends RichMapFunction<Long, ChRow> {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private final Properties kafkaClientProps;
    private final String jobId;
    private final String version;
    private final String registryStatus;
    private transient AdminClient admin;
    private transient ObjectMapper mapper;

    public HeartbeatFn(Properties kafkaClientProps, String jobId, String version, String registryStatus) {
        this.kafkaClientProps = kafkaClientProps;
        this.jobId = jobId;
        this.version = version;
        this.registryStatus = registryStatus;
    }

    @Override
    public void open(OpenContext ctx) {
        mapper = new ObjectMapper();
        Properties p = new Properties();
        // Only pass keys AdminClient understands; producer/consumer-only keys would just warn.
        for (String k : kafkaClientProps.stringPropertyNames()) {
            if (AdminClientConfig.configNames().contains(k)) p.setProperty(k, kafkaClientProps.getProperty(k));
        }
        p.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        p.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "8000");
        admin = AdminClient.create(p);
    }

    @Override
    public ChRow map(Long tick) {
        String status;
        String clusterId = "";
        try {
            var cluster = admin.describeCluster();
            clusterId = cluster.clusterId().get(8, TimeUnit.SECONDS);
            int nodes = cluster.nodes().get(8, TimeUnit.SECONDS).size();
            status = nodes > 0 ? "up" : "down";
        } catch (Exception e) {
            status = "down";
        }
        ObjectNode n = mapper.createObjectNode();
        n.put("job_id", jobId);
        n.put("ts", TS.format(Instant.now()));
        n.put("version", version);
        n.put("kafka_bootstrap", kafkaClientProps.getProperty("bootstrap.servers", ""));
        n.put("kafka_status", status);
        n.put("kafka_cluster_id", clusterId);
        n.put("registry_status", registryStatus);
        return new ChRow("job_heartbeat", n.toString());
    }

    @Override
    public void close() {
        if (admin != null) admin.close();
    }
}
