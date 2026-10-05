package io.streamquality;

import io.streamquality.config.ConfigLoader;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
 */
public final class StreamQualityJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        Path dir = Path.of(params.get("config-dir", "/opt/sq/config"));

        Properties kafka = ConfigLoader.load(dir.resolve("kafka.properties"));
        Properties registry = ConfigLoader.load(dir.resolve("registry.properties"));
        // thresholds.yaml loaded by ThresholdsLoader (Phase 2)

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // Phase 1: HeartbeatSource -> ClickHouseSink
        // Phase 2: KafkaSource -> ParseFn(+DLQ) -> VolumeCheck, NullRateCheck
        // Phase 3: + SchemaRegistryClient (circuit breaker) -> StructuralCheck
        // Phase 4: + CardinalityCheck (HLL), FreshnessCheck (KLL), watermarks
        // Phase 5 (distribution shift, 24h baseline): intentionally skipped

        env.execute("stream-quality-monitor");
    }
}
