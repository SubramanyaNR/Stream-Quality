# Architecture

## 1. Components and data flow

```
 EXTERNAL (not deployed here)                 DEPLOYED BY `make up`
┌───────────────────────────┐   consume   ┌──────────────────────────────────────────────┐
│ Apache Kafka (KRaft)      │────────────▶│ Flink job (JM + TM, RocksDB state)           │
│  topics: orders, payments │             │  1 Source  KafkaSource(topics|pattern)       │
│  sq.dead-letter (pre-made)│◀────────────│  2 Parse   JSON -> ParsedRecord              │
└───────────────────────────┘  DLQ side  │            └─ side output: bad/invalid -> DLQ │
┌───────────────────────────┐  output     │  3 Window  keyBy(topic), tumbling event-time │
│ Apicurio Registry (v3 API)│◀────────────│            CompositeCheckWindowFn runs all   │
└───────────────────────────┘ schema fetch│            QualityChecks in ONE pass         │
   (circuit-breaker; optional)            │  4 Baseline keyBy(topic|field) MapState 24h  │
                                          │            -> feeds DistributionShiftCheck   │
                                          └───────┬──────────────────────┬───────────────┘
                                   CheckResult rows│            metrics :9249 (sq_check_status)
                                                  ▼                      ▼
                                          ┌──────────────┐        ┌────────────┐  rules  ┌──────────────┐ webhook
                                          │ ClickHouse   │        │ Prometheus │────────▶│ Alertmanager │────────▶ receiver
                                          │ sq.*         │        └─────┬──────┘         └──────────────┘
                                          └──────┬───────┘              │
                                                 └───────▶ Grafana ◀────┘◀── Loki ◀── Alloy (container logs)
```

Flows:
1. **Data path**: Kafka -> parse -> (bad records -> DLQ topic) -> window checks -> ClickHouse.
2. **Alert path**: Flink publishes `sq_check_status{topic,field,check_type}` gauges -> Prometheus rules -> Alertmanager -> webhook. ClickHouse is the audit trail, Prometheus is the alert trigger (it already does `for:` durations, grouping, silences).
3. **Baseline path**: per-window per-field summaries -> keyed `MapState<windowStart, Summary>` in RocksDB, evicted past 24h by timers; drift check at window N reads the baseline built from windows < N (never includes itself).
4. **Degradation**: registry circuit breaker open => structural check emits nothing (and a `structural` row with status `ok`+details `{"skipped":"registry_unreachable"}` is NOT written; a gauge `sq_registry_up=0` is). All statistical checks unaffected.

## 2. Key decisions (made)

| # | Decision | Choice | Why |
|---|----------|--------|-----|
| 1 | Flink version / Java | Flink **2.2.1**, Java **17**, connector `5.0.0-2.2` | Current 2.x line; Flink 2.x dropped legacy `SourceFunction`/`SinkFunction`, so write only against the new Source/Sink API from day one. Java 17 because datasketches 7+ needs 17+, 8+ needs 21; we pin **6.2.0** to keep the Flink image on java17 with zero JVM flags. |
| 2 | Message format v1 | **JSON only**; Avro/Protobuf behind a `PayloadDecoder` interface in Phase 3+ | Structural checks via JSON Schema (networknt). Avoids Confluent wire format assumptions entirely. |
| 3 | Window | **60 s tumbling, event-time**, 10 s bounded out-of-orderness, 30 s allowed lateness, 30 s idle timeout | Per-second volume = count/60. Idle timeout is mandatory or one quiet partition freezes every window. Late data after lateness is counted (`late_records`) not silently lost. |
| 4 | One operator, many checks | `keyBy(topic)` + single `AggregateFunction` composing all `QualityCheck`s | One state read per record; fields fan out inside the accumulator rather than keying by field (avoids N× shuffle). Skew risk on a hot topic: set topic parallelism = partitions and pre-aggregate by `topic,partition` then merge. |
| 5 | Freshness basis | `processingTime - eventTime` on KLL sketch, report p50/p95/max | Only 3 numbers per window; KLL merges. Event time from payload field in `thresholds.yaml`, fallback Kafka timestamp (flagged in details). |
| 6 | Cardinality | datasketches `HllSketch(lgK=12)` per field per window; drift = ratio to baseline mean distinct | ~1.6% error, 4 KB. Do NOT use HLL union across 24h for the baseline; store per-window estimates. |
| 7 | Distribution | Welford (count, mean, M2) + KLL(k=200) per field per window; baseline = merged stats of last 24h windows; flag z = \|mean - baseline mean\| / baseline stddev-of-window-means, plus p95 shift ratio | Cheap, explainable, mergeable. KS test is out of scope. |
| 8 | Baseline storage | RocksDB incremental checkpoints, `MapState<Long,Summary>`, event-time timer eviction at 24h; **min_baseline_windows=60** before drift can fire (cold-start = status ok, details `warming_up`) | Per requirement; 1440 summaries per field x ~1 KB is tiny. |
| 9 | Delivery | **At-least-once + idempotent ClickHouse** (ReplacingMergeTree on window key). Not exactly-once. | ClickHouse has no 2PC; replays collapse. Dashboards must not double count: use `argMax`/`FINAL`-free latest views provided in init.sql. |
| 10 | ClickHouse writes | Custom batching sink over HTTP `JSONEachRow` | No JDBC driver dependency; flush on 5k rows/2 s/checkpoint. |
| 11 | Alerting | Flink metrics -> Prometheus rules -> Alertmanager webhook | Reuses `for:`, grouping, silences, inhibition. Gauge label cardinality bounded by thresholds.yaml (only configured fields). |
| 12 | Config | 3 files: `kafka.properties`, `registry.properties` (client props passed through verbatim, `sq.*` for monitor settings), `thresholds.yaml`; `${env:X}` for secrets; mounted read-only at `/opt/sq/config` on JM **and** TM | Nothing hardcoded. Confluent later = new `sq.registry.type`, same Kafka props. |
| 13 | Consumer group | Dedicated `group.id`, **no offset commit semantics relied on**; Flink checkpoints own offsets. | Never interferes with real consumers. Needs `READ` on topics + group ACLs. |
| 14 | Logs | **Grafana Alloy** -> Loki (Promtail is end-of-life) | |
| 15 | Deployment of job | One-shot `flink-job-submit` container, idempotent; session cluster | `make up` really is one command. |

## 3. Risks and gaps to close before coding

1. **Docker networking to external Kafka** (biggest time sink). Broker `advertised.listeners` must be resolvable from inside containers; `localhost:9092` advertised => connects, then fails. Document in `docs/KAFKA_CONNECTIVITY.md`; provide `scripts/check-connectivity.sh` that runs `kafka-broker-api-versions` through the same props.
2. **Kafka ACLs**: monitor needs READ topic+group, and WRITE/DESCRIBE on DLQ. SASL/SCRAM recommended over PLAIN.
3. **Watermarks vs. low-traffic topics**: idle partitions, and topics with no traffic produce *no window at all* -> volume check never reports zero! Fix: a heartbeat/timer-driven "expected topic" emitter that writes volume=0 rows for every configured topic each window (Phase 2 must-have).
4. **Event-time skew / bad timestamps** (future dates, epoch seconds vs ms): sanitise in Parse; out-of-range => DLQ reason `bad_timestamp`.
5. **DLQ amplification**: a systemic bug could DLQ 100% of traffic. Cap with a rate limiter + count in metrics; envelope keeps original key/headers/bytes + reason, not re-parsed JSON.
6. **Gauge cardinality in Prometheus**: only whitelisted topic/field combos; never label with values.
7. **Schema evolution / registry flaps**: cache last-good schema, TTL refresh, breaker; structural check reports `skipped`, never `fail`, when registry is down.
8. **Checkpoint/state growth**: RocksDB TTL + timers; verify with a 24h soak before Phase 5 sign-off.
9. **Threshold reload**: v1 = restart from savepoint; document it. Broadcast-state hot reload is a stretch.
10. **ClickHouse schema untested here**: `init.sql` was written without a running ClickHouse; first `make up` must be validated (esp. `CHECK` constraint + `ALTER ... ADD INDEX` on 26.x) - Phase 1 exit criterion.
11. **30-minute setup claim**: first image build downloads Maven deps (~3-5 min). Publish prebuilt image to GHCR before Phase 6; provide a "no Kafka handy" doc pointing to a throwaway single-node KRaft container for demo only (not part of the stack).
12. **Portfolio value**: ship a recorded scenario (`generator/scenarios/*.yaml`: null spike, volume drop, schema break, drift) so the dashboards show violations in the demo.
