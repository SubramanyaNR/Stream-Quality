package io.streamquality;

import io.streamquality.checks.CardinalityCheck;
import io.streamquality.checks.FreshnessCheck;
import io.streamquality.checks.NullRateCheck;
import io.streamquality.checks.QualityCheck;
import io.streamquality.checks.VolumeCheck;
import io.streamquality.config.ConfigLoader;
import io.streamquality.config.Thresholds;
import io.streamquality.dlq.DlqRecord;
import io.streamquality.dlq.DlqSerializer;
import io.streamquality.model.ParsedRecord;
import io.streamquality.model.RawRecord;
import io.streamquality.operators.CompositeCheck;
import io.streamquality.operators.LateCounterFn;
import io.streamquality.operators.ParseFn;
import io.streamquality.source.KafkaPreflight;
import io.streamquality.source.RawRecordDeserializer;
import io.streamquality.source.TickFn;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import io.streamquality.sink.ChRow;
import io.streamquality.sink.ClickHouseConfig;
import io.streamquality.sink.ClickHouseSink;
import io.streamquality.source.HeartbeatFn;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Topology (built up phase by phase):
 *
 *   KafkaSource(topics) ──> Parse/Validate (ProcessFunction)
 *        │                      ├─ main: ParsedRecord ──> assign event-time WM
 *        │                      └─ side output DLQ_TAG ──> KafkaSink(sq.dead-letter)
 *        │
 *        └─> keyBy(topic) ─> window(Tumbling event-time) ─> CompositeCheckWindowFn(List<QualityCheck>)
 *                              ├─ CheckResult ──> ClickHouseSink (sq.check_results)
 *                              └─ cardinality only: small per-(topic,field) ring of recent estimates in keyed state
 *   Heartbeat source (1/min) ──────────────────────────────────> ClickHouseSink (sq.job_heartbeat)
 *
 * Args: --config-dir, --clickhouse.url, --clickhouse.user, --clickhouse.password (all also via env
 * CLICKHOUSE_URL/USER/PASSWORD), --heartbeat.interval.sec (60), --heartbeat.max (0 = unbounded).
 */
public final class StreamQualityJob {
    static final String VERSION = "0.1.0";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        // On a cluster this is ignored (the cluster's config wins); embedded runs honour FLINK_CONF_DIR.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
                org.apache.flink.configuration.GlobalConfiguration.loadConfiguration());
        env.getConfig().setGlobalJobParameters(params);
        build(env, params);
        env.execute("stream-quality-monitor");
    }

    /** Wires the topology; separated from main() so tests can run it on a MiniCluster. */
    public static void build(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        Path dir = Path.of(params.get("config-dir", "/opt/sq/config"));
        Properties kafkaAll = ConfigLoader.load(dir.resolve("kafka.properties"));
        Properties registry = ConfigLoader.load(dir.resolve("registry.properties"));
        Properties kafkaClient = ConfigLoader.kafkaClientProps(kafkaAll);
        Thresholds th = Thresholds.load(dir.resolve("thresholds.yaml"));
        int parallelism = params.getInt("parallelism", 1);
        env.setParallelism(parallelism);
        // Cluster config (docker-compose) normally sets this; make the job safe when run without it.
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) env.enableCheckpointing(30_000);

        ClickHouseConfig ch = ClickHouseConfig.of(
                arg(params, "clickhouse.url", "CLICKHOUSE_URL", "http://clickhouse:8123"),
                arg(params, "clickhouse.user", "CLICKHOUSE_USER", "sq_writer"),
                arg(params, "clickhouse.password", "CLICKHOUSE_PASSWORD", "sq_writer_pw"));

        String registryStatus = "true".equalsIgnoreCase(registry.getProperty("sq.registry.enabled", "false")) ? "unknown" : "disabled";
        String jobId = params.get("job-id", "sq-" + java.util.UUID.randomUUID().toString().substring(0, 8));

        // ---- topics + preflight (fail at submit time with a clear message) ----
        List<String> monitored = th.topics();
        String topicsProp = kafkaAll.getProperty("sq.source.topics", "");
        List<String> sourceTopics = topicsProp.isBlank() ? monitored
                : Arrays.stream(topicsProp.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
        String dlqTopic = kafkaAll.getProperty("sq.dlq.topic", "sq.dead-letter");
        boolean failOnMissing = Boolean.parseBoolean(kafkaAll.getProperty("sq.dlq.fail.on.missing", "true"));
        if (!params.getBoolean("skip-preflight", false)) {
            List<String> must = new ArrayList<>(sourceTopics);
            if (failOnMissing) must.add(dlqTopic);
            KafkaPreflight.verifyTopicsExist(kafkaClient, must);
        }

        // ---- heartbeat -> sq.job_heartbeat ----
        double hbSec = params.getDouble("heartbeat.interval.sec", 60);
        long hbMax = params.getLong("heartbeat.max", 0);
        DataGeneratorSource<Long> hbTicks = new DataGeneratorSource<>(i -> i, hbMax > 0 ? hbMax : Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1.0 / hbSec), Types.LONG);
        DataStream<ChRow> heartbeat = env.fromSource(hbTicks, WatermarkStrategy.noWatermarks(), "heartbeat-ticks")
                .setParallelism(1)
                .map(new HeartbeatFn(kafkaClient, jobId, VERSION, registryStatus)).name("heartbeat").setParallelism(1);

        // ---- Kafka source -> parse (+ DLQ side output) ----
        OffsetsInitializer start = "earliest".equalsIgnoreCase(kafkaAll.getProperty("auto.offset.reset", "latest"))
                ? OffsetsInitializer.earliest() : OffsetsInitializer.latest();
        KafkaSource<RawRecord> source = KafkaSource.<RawRecord>builder()
                .setProperties(kafkaClient)
                .setTopics(sourceTopics)
                .setStartingOffsets(start)
                .setDeserializer(new RawRecordDeserializer())
                .build();
        SingleOutputStreamOperator<ParsedRecord> parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .process(new ParseFn(th)).name("parse");

        KafkaSink<DlqRecord> dlqSink = KafkaSink.<DlqRecord>builder()
                .setKafkaProducerConfig(kafkaClient)
                .setRecordSerializer(new DlqSerializer(dlqTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        parsed.getSideOutput(ParseFn.DLQ).sinkTo(dlqSink).name("dead-letter-topic");

        // ---- ticks keep watermarks moving and force empty windows (see TickFn) ----
        long tickMs = params.getLong("tick.interval.ms", 1000);
        DataGeneratorSource<Long> tickSrc = new DataGeneratorSource<>(i -> i, Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000.0 / tickMs), Types.LONG);
        DataStream<ParsedRecord> ticks = env.fromSource(tickSrc, WatermarkStrategy.noWatermarks(), "tick-source")
                .setParallelism(1).flatMap(new TickFn(monitored)).name("ticks").setParallelism(1);

        // ---- windowed checks ----
        List<QualityCheck<?>> checks = List.of(new VolumeCheck(), new NullRateCheck(), new CardinalityCheck(th), new FreshnessCheck());
        OutputTag<ParsedRecord> late = new OutputTag<>("late") {};
        SingleOutputStreamOperator<ChRow> results = parsed.union(ticks)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ParsedRecord>forBoundedOutOfOrderness(Duration.ofMillis(th.maxOutOfOrdernessMs()))
                        .withTimestampAssigner((r, ts) -> r.processingTimeMs())   // INGESTION time: windows = what arrived, so stale events are still measured
                        .withIdleness(Duration.ofMillis(th.idleTimeoutMs())))
                .keyBy(ParsedRecord::topic)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(th.windowSizeMs())))
                .sideOutputLateData(late)
                .aggregate(new CompositeCheck.Agg(checks), new CompositeCheck.Eval(checks, th, jobId))
                .name("window-checks");
        results.getSideOutput(late).flatMap(new LateCounterFn()).name("late-records-metric");

        heartbeat.union(results).sinkTo(new ClickHouseSink(ch)).name("clickhouse").setParallelism(1);

        // Phase 3: + SchemaRegistryClient (circuit breaker) -> StructuralCheck
                // Phase 5 (distribution shift, 24h baseline): intentionally skipped
    }

    private static String arg(ParameterTool p, String key, String env, String def) {
        String v = p.get(key);
        if (v == null) v = System.getenv(env);
        return v != null ? v : def;
    }
}
