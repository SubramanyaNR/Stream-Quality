# Stream Quality Monitor

In-stream, operational data-quality monitoring: a Flink job attaches to an **existing** Apache Kafka (KRaft) cluster, runs windowed statistical checks, writes one row per check per window to ClickHouse, and alerts through Alertmanager. Grafana shows topic health, field drill-down and a violation log.

Checks: volume, null rate, cardinality drift (HLL), freshness, distribution shift (24 h RocksDB baseline), structural (Apicurio JSON Schema, optional).

> Status: Phase 0 - architecture, compose stack, ClickHouse schema and Flink skeleton. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start
```bash
# 1. point at your cluster
$EDITOR config/kafka.properties config/registry.properties config/thresholds.yaml
# 2. go
make up        # creates DLQ topic, builds Flink image, starts stack, submits job
```
Grafana http://localhost:3000 - Flink http://localhost:8082 - Prometheus :9090 - Alertmanager :9093 - ClickHouse :8123

Kafka and Apicurio are **not** deployed by this project.

## Layout
| Path | Purpose |
|---|---|
| `config/` | `kafka.properties`, `registry.properties`, `thresholds.yaml` (only place connection details live) |
| `flink-job/` | Java 17 Maven project; `checks/QualityCheck` is the operator contract |
| `flink/` | Dockerfile (multi-stage build) + job submit script |
| `clickhouse/init/` | DDL, materialized views, users |
| `observability/` | Prometheus rules, Alertmanager, Loki, Alloy, Grafana provisioning + dashboards |
| `generator/` | Python test event generator + fault scenarios (Phase 2) |
| `scripts/` | DLQ topic creation, stack wait |
| `tools/alert-sink/` | demo webhook receiver |
| `docs/` | architecture, decisions, risks |
