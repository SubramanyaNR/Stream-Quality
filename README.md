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
Prereqs: Docker + Compose, `make`, `perl`, an Apache Kafka cluster the containers can reach ([connectivity guide](docs/KAFKA_CONNECTIVITY.md)).

```bash
# 1. point it at your cluster (nothing is hardcoded in the job)
$EDITOR config/kafka.properties     # bootstrap.servers, security (PLAINTEXT / SASL / SSL blocks provided), topics
$EDITOR config/registry.properties  # Apicurio URL, or sq.registry.enabled=false
$EDITOR config/thresholds.yaml      # which topics/fields to monitor + thresholds

# 2. go
make up      # creates the dead-letter topic, builds the Flink image, starts the stack, submits the job
```
| UI | URL |
|---|---|
| Grafana (admin/admin) | http://localhost:3000 |
| Flink | http://localhost:8082 |
| Prometheus / Alertmanager | http://localhost:9090 / http://localhost:9093 |
| PostgreSQL | localhost:5432, db `sq` (`sq_reader` / `sq_reader_pw`) |

Kafka and Apicurio are **not** deployed by this project. Optional: `make schemas` registers `config/schemas/*.json`
(artifact `<topic>-value`) in your registry.

### See it work
```bash
pip install -r generator/requirements.txt
make gen SCENARIO=healthy                      # steady traffic
make gen SCENARIO=null_spike                   # nulls in required/optional fields
make gen SCENARIO=volume_drop                  # -80 % then silence
make gen SCENARIO=bad_payloads                 # corrupt JSON -> dead-letter topic
# BOOTSTRAP=<host:port> make gen ...           # host-reachable address of your Kafka (default localhost:9092)
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
| `generator/` | Python event generator + fault scenarios |
| `scripts/` | DLQ topic creation, schema registration, e2e + dashboard-query tests |
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
make test            # 45 unit tests, no services needed
make test-schema     # behavioural tests of the SQL schema (upsert, trigger, roles, retention)
make e2e             # real Kafka + Postgres (+ Apicurio) required, see docs/TESTING.md
make test-analytics  # seeds a known history and asserts the analytics dashboard's numbers exactly
make test-dashboards # runs every Grafana query against Postgres
```

## Security notes
Demo credentials (`sq_writer_pw`, `admin/admin`) are for local use; change them in `postgres/init/01_init.sql` and `.env`
before exposing anything. Kafka secrets go in `.env` and are referenced as `${env:NAME}` in `config/kafka.properties`.
