# Architecture

## 1. Components and data flow

```
 EXTERNAL (not deployed here)               DEPLOYED BY `make up`
┌───────────────────────────┐ consume  ┌────────────────────────────────────────────────────────┐
│ Apache Kafka (KRaft)      │─────────▶│ Flink job  (JobManager + TaskManager, RocksDB state)   │
│  topics: orders, payments │          │  KafkaSource ─▶ ParseFn ──main──▶ ┐                    │
│  sq.dead-letter (pre-made)│◀─────────│     bytes→JSON, schema check      │  union(+ticks)     │
└───────────────────────────┘ DLQ side │        └─ side output ─▶ KafkaSink (dead-letter)       │
┌───────────────────────────┐ output   │  Tick source (1/s, per topic) ────┘                    │
│ Apicurio Registry (v3 API)│◀─────────│   ─▶ watermark(ingestion time) ─▶ keyBy(topic)         │
└───────────────────────────┘ schemas  │   ─▶ tumbling window ─▶ CompositeCheck                 │
   (cached, circuit breaker, optional) │        Volume | NullRate | Cardinality | Freshness |   │
                                       │        Structural   (+ per-check value history)        │
                                       │  Heartbeat source ─────────────────────────┐           │
                                       └────────┬───────────────────────────────────┼──────────┘
                                  gauges :9249  │             rows (JDBC upsert)▼
                                                ▼                         ┌─────────────────┐
              ┌────────────┐ rules ┌──────────────┐ webhook ┌──────────┐  │ PostgreSQL  sq.*│
              │ Prometheus │──────▶│ Alertmanager │────────▶│alert-sink│  └────────┬────────┘
              └─────┬──────┘       └──────────────┘         └────┬─────┘           │
                    └──────────────▶  Grafana  ◀─────────────────┼─────────────────┘
                                         ▲                       ▼
                                         └─────────────────── Loki ◀── Alloy (container logs)
```

Flows:
1. **Data path**: Kafka → parse (bad records → DLQ topic) → windowed checks → PostgreSQL (`sq.check_results`).
2. **Alert path**: the window operator exposes `check_status{topic,check_type,field}` gauges (0/1/2). Prometheus rules → Alertmanager → webhook. PostgreSQL is the audit trail; Prometheus owns alert timing/grouping/silencing.
3. **Registry path**: `ParseFn` validates each JSON payload against the topic's schema (`{topic}-value` artifact). Schemas are cached; refresh is async; a circuit breaker protects the hot path.
4. **Degradation**: registry down or no schema ⇒ records are `skipped` structurally, `StructuralCheck` emits **no row** (an outage never looks like a data-quality failure), every statistical check continues. `registry_up` gauge and the heartbeat's `registry_status` expose it.

## 2. Decisions (and what testing changed)

| # | Decision | Rationale / evidence |
|---|---|---|
| 1 | Flink 2.2.1, Java 17, connector `5.0.0-2.2`, datasketches **6.2.0** | Flink 2.x removed the legacy source/sink APIs: new API only. datasketches ≥7 needs a newer JVM than the image. |
| 2 | JSON only in v1 (`ParseFn` is the single decode point) | Avoids Confluent wire-format assumptions. Avro/Protobuf = a new decoder. |
| 3 | **Windows are tumbling over *ingestion time*** (when Flink parsed the record), payload timestamp used only for freshness | First design used payload event time; stale events (exactly what freshness must measure) arrived behind the watermark and were dropped as late. It also broke backfill. Found while designing the freshness check. |
| 4 | **Synthetic ticks** (per topic, 1/s) advance the watermark and force empty windows | A silent topic emits no records ⇒ no window ⇒ the volume check could never report 0. Verified by e2e (silence ⇒ `volume=0, fail`). |
| 5 | One keyed operator runs all checks (`CompositeCheck`); `QualityCheck<ACC>` is the contract | One state access per record. New check = one class + one list entry. |
| 6 | **Baseline = last N window values per (check, field)** in keyed state (`history_windows`, default 30), **learning only from OK windows** (`HistoryPolicy`); a violation lasting > `rebaseline_after` windows becomes the new normal | Drives volume drop and cardinality drift. Without the OK-only rule a sustained anomaly dragged the baseline toward itself and a real id explosion was downgraded from FAIL to WARN (found by a flaky e2e run). The 24 h RocksDB distribution baseline (Phase 5) is **descoped**; `History` is where it would plug in. |
| 7 | Cardinality: HLL lgK=12, factor = max(est/base, base/est), **both directions**, `min_baseline` guard | A collapse (field defaulted to a constant) is as bad as an id explosion. |
| 8 | Freshness: KLL k=200, p95 judged; only **payload** timestamps count | Kafka CreateTime would hide producer lag. Future timestamps clamped + counted. |
| 9 | Strictly-greater-than thresholds (`warn: 0.0` ⇒ any occurrence warns) | Makes "zero tolerance" expressible without everything warning. |
| 10 | **PostgreSQL** results store; at-least-once delivery + **upsert on a real primary key** `(topic, check_type, field, window_start, window_end)` | Replays after a restart overwrite themselves exactly: every query sees one row per window, no `FINAL`/`argMax`. A batch must not contain the same key twice (`ON CONFLICT DO UPDATE` would error), so the sink de-duplicates per batch, last wins. |
| 10a | Why Postgres, not ClickHouse (it was ClickHouse first) | The data is ~1 row per field per minute: far below where a columnar store pays off. Postgres is simpler to run and clone (no Grafana plugin download), gives exact idempotency, `jsonb` for `details`, a built-in Grafana datasource, and constraints/transactions. Cost: no automatic TTL (an hourly `sq.purge_old()`), no incremental materialized views (a trigger maintains `latest_status`; `violations` is a view), slower scans at very large scale. Switch point: > ~100M rows (1 s windows or thousands of fields) -> partitioning or TimescaleDB. |
| 10b | Analytics are SQL over the results table; incident detection is a **function** `sq.incidents(from, to)`, not a view | Window functions (gaps-and-islands) cannot have the dashboard's time filter pushed below them in a view, so a view would scan the whole table. The function filters first. An incident ends at the next ok window or a gap of > 3 window lengths, so a monitor outage never glues two incidents together. |
| 11 | Records failing a check are analysed **and** dead-lettered (required field missing, schema violation); unparseable ones only dead-lettered | DLQ envelope keeps original key/bytes (base64) + reason. |
| 12 | Postgres writes: custom batching sink; **one statement per batch** (rows travel as a JSON array expanded by `jsonb_to_recordset`, then upserted); flush on size, every 2 s, and on checkpoint | One round trip per batch. Bounded latency independent of checkpoint config (found when a run without checkpointing wrote nothing). Transient errors (connection, serialization, shutdown) retry on a fresh connection; bad data / permissions fail fast. |
| 13 | Alerting through Prometheus rules on Flink gauges; stale gauges reset to 0 when a check emits nothing | Otherwise a FAIL on a now-silent topic would never resolve. |
| 14 | `HttpClient` timeouts + breaker (3 failures, 60 s) + negative cache TTL ≤ 30 s | A 404 ("no schema yet") cached for the full 5-minute refresh left structural checks off after the schema was registered. Found by the outage e2e. |

## 3. Known limitations / next steps
- Real-time monitor: it measures what *arrives*; it is not a historical backfill tool.
- Parallelism: defaults to 1. `keyBy(topic)` means one hot topic = one subtask; pre-aggregating per partition is the first scaling step.
- Thresholds reload requires a job restart (savepoint-safe). Broadcast-state hot reload is a stretch.
- Distribution shift / 24 h baseline: descoped (Phase 5).
- Confluent: `sq.registry.type` + a `RegistryClient` implementation; Kafka side needs no change.
- Docker artefacts (Compose run, image build, Grafana rendering) could not be executed in the build sandbox; see `docs/TESTING.md` for exactly what was and was not verified.
