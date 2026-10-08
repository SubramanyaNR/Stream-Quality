# Stream Quality Monitor

In-stream, **operational** data-quality monitoring for Kafka. A Flink job attaches to your **existing** Apache Kafka
(KRaft) cluster, runs continuous windowed checks on every topic, writes one row per check per window to PostgreSQL,
alerts through Alertmanager, and shows it all in Grafana. Think Great Expectations - but running on the stream,
telling you *now* rather than gating a batch pipeline.

| Check | What it measures | Flags |
|---|---|---|
| **volume** | messages/s per topic | drop vs. recent windows; empty window |
| **null_rate** | share of null/missing values per field | per-field thresholds |
| **cardinality** | distinct values per field (HyperLogLog) | spike *or* collapse vs. recent windows |
| **freshness** | processing time − payload timestamp (p50/p95/p99) | p95 over threshold |
| **structural** | JSON-Schema violations (schema from Apicurio) | violation rate; skipped, never failed, if the registry is down |

Records that fail validation are routed by a Flink **side output** to a dead-letter Kafka topic (original bytes preserved).

> Scope: Phase 5 (distribution shift / 24 h RocksDB baseline) was deliberately skipped - see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
> Status: built and tested against real Kafka, PostgreSQL, Apicurio, Prometheus and Alertmanager; the Docker packaging itself
> could not be executed in the build environment - read [docs/TESTING.md](docs/TESTING.md) before trusting it.

## Quick start
Prereqs: Docker + Compose, `make`, `perl`, and an Apache Kafka cluster the containers can reach ([connectivity guide](docs/KAFKA_CONNECTIVITY.md)).

```bash
make up              # first run creates cluster.env and stops so you can edit it
$EDITOR cluster.env  # the one file: Kafka address, Docker network, topics, registry, passwords
make check           # verifies the brokers are reachable from the containers and from this host
make up              # dead-letter topic, build image, start stack, submit job
```
`cluster.env` is the only place connection details live. `config/*.properties` read it through `${env:NAME}`, and the
compose file and the scripts read the same file.

| Setting | Meaning |
|---|---|
| `KAFKA_BOOTSTRAP` | address the Flink containers use (the broker's advertised listener on the Kafka network) |
| `KAFKA_BOOTSTRAP_HOST` | address you use from this host (`make check`) |
| `KAFKA_DOCKER_NETWORK` | Docker network the Kafka containers live on; the Flink containers join it |
| `TOPICS_PATTERN` / `TOPICS_EXCLUDE` | regex of topics to monitor / skip. `TOPICS` is a plain comma list used when the pattern is empty |
| `DLQ_TOPIC`, `DLQ_REPLICATION` | dead-letter topic and its replication factor (1 for a single broker, 3 for three) |
| `REGISTRY_ENABLED`, `REGISTRY_URL` | schema registry (Apicurio v3); off by default |

Topics matching the pattern are picked up within a minute of being created. Topics not listed in `config/thresholds.yaml` get the
`defaults:` checks. Empty-window (silence) detection only covers topics that are listed there.

| UI | URL |
|---|---|
| Grafana (admin/admin) | http://localhost:3000 |
| Flink | http://localhost:8082 |
| Prometheus / Alertmanager | http://localhost:9090 / http://localhost:9093 |
| PostgreSQL | localhost:5432, db `sq` (`sq_reader` / `sq_reader_pw`) |

Kafka and Apicurio are **not** deployed by this project. Optional: `make schemas` registers `config/schemas/*.json`
(artifact `<topic>-value`) in your registry.

### See it work
Produce data with bankgen from the Kafka sandbox (`/opt/ai-sre/kafka-sandbox`), which has a clean mode and an anomaly mode:
```bash
make up-data                      # clean traffic on bank.dbo.transactions, bank.app.clickstream, bank.app.application_logs
make up-data DATA_MODE=anomaly    # cycles null spike, corrupt JSON, type break, volume drop, silence
```
Then open Grafana: **Topic health** (RAG per topic/check), **Field drill-down**, **Violation log**, and **Quality analytics**
(data-quality score, availability, incidents with durations, flappiest checks, correlated failures, week-over-week volume, alert history).

## Where things live
| Path | Purpose |
|---|---|
| `config/` | `kafka.properties`, `registry.properties`, `thresholds.yaml`, `schemas/` - the only place connection details live |
| `flink-job/` | the Flink job (Java 17). `checks/QualityCheck.java` is the contract each check implements |
| `flink/` | image build + job submit script |
| `postgres/init/01_init.sql` | tables, views, trigger, retention function, read/write roles |
| `observability/` | Prometheus rules, Alertmanager, Grafana provisioning; `build_dashboards.py` generates the dashboard JSON |
| `tools/alert-sink/` | Alertmanager webhook receiver that stores alerts in Postgres (`sq.alert_events`) |
| `scripts/` | DLQ topic creation, schema registration, SQL and dashboard-query tests |
| `docs/` | architecture and decisions, Kafka connectivity, testing |

## PostgreSQL output
`sq.check_results`: `topic, field, check_type, window_start, window_end, value, threshold, status(ok|warn|fail), details(jsonb)`
with a primary key on the window, so replays after a restart **overwrite themselves** (the sink upserts) - no dedup tricks needed
when querying. `sq.violations` (view: every warn/fail), `sq.latest_status` (kept current by a trigger), `sq.topic_health` (view),
`sq.job_heartbeat`, `sq.alert_events` / `sq.alert_history` (every alert Alertmanager delivered: when it fired, when it resolved), and `sq.incidents(from, to)` (consecutive unhealthy windows grouped into incidents). Retention is applied hourly by the `postgres-maintenance` container (`RESULTS_RETENTION_DAYS`, default 90).

## Adding a check
Implement `QualityCheck<ACC>` (accumulate per record, `evaluate` per window), add it to the list in `StreamQualityJob`, add
thresholds under `defaults:` in `thresholds.yaml`. `ChecksTest` shows the pattern.

## Development
```bash
make test            # unit tests, no services needed
make test-schema     # behavioural tests of the SQL schema (upsert, trigger, roles, retention)
make test-analytics  # seeds a known history and asserts the analytics dashboard's numbers exactly
make test-dashboards # runs every Grafana query against Postgres
```

## Security notes
Demo credentials (`sq_writer_pw`, `admin/admin`) are for local use; change them in `postgres/init/01_init.sql` and `cluster.env`
before exposing anything. Kafka secrets go in `cluster.env` and are referenced as `${env:NAME}` in `config/kafka.properties`.
