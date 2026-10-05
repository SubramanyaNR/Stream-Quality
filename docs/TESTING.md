# Testing: what was verified, and how

No Docker daemon was available while building this, so everything was tested against **real binaries run directly**:
Apache Kafka 4.0.0 (single-node KRaft), ClickHouse 25.8, Apicurio Registry 3.3.3, Prometheus 3.5, Alertmanager 0.28,
and the Flink job on an embedded Flink 2.2 mini-cluster.

| Layer | Command | What it proves |
|---|---|---|
| Unit (45 tests) | `make test` | parsing/DLQ routing, every check's maths and status rules, sketch merge, thresholds precedence, sink batching/retry/timer, schema cache + circuit breaker |
| End to end (28 assertions) | `make e2e` | Kafka → job → ClickHouse with scripted faults: null spike, id explosion, 150 s-stale events, schema break, −90 % volume, silence, corrupt JSON; DLQ contents; MVs; heartbeat |
| Registry outage | `make e2e-outage` | job starts with Apicurio **down**: statistical checks run, no structural rows/failures, structural resumes on recovery |
| Dashboards | `make e2e` then `make test-dashboards` (strict mode needs the fault data the e2e leaves behind) | every Grafana panel/variable query executes on ClickHouse as the read-only user (`--strict`: non-empty) |
| Alert path | manual (see below) | Flink gauge → Prometheus scrape → rule → Alertmanager → webhook |

`make e2e` needs Kafka, ClickHouse (with `clickhouse/init/01_init.sql` applied) and optionally Apicurio running locally;
see the header of `scripts/e2e-local.sh` for the env vars.

## Bugs that only real components exposed (all fixed)
- `topic_health` view mixed `DateTime64` with an interval (ClickHouse rejected it)
- hourly rollup MV double counted replays (removed)
- sink wrote nothing without checkpointing; `close()` could interrupt an in-flight INSERT
- window by payload time dropped stale events (switched to ingestion time)
- ClickHouse alias shadowed a column inside `WHERE`, silently emptying Grafana overlay series
- a cached 404 kept structural checks off for 5 minutes after the schema was registered
- invalid YAML in the shipped `thresholds.yaml` (`key:{` without a space)
- anomalous windows polluted the rolling baseline, weakening detection during sustained incidents (now: learn from OK windows only)
- ClickHouse compose healthcheck lacked the password; checkpoint volume not writable by the `flink` user; env substitution order in `create-dlq-topic.sh`

## NOT verified (no Docker / network access in the build sandbox)
- `docker compose up` end to end, the `flink/Dockerfile` build, the `flink-job-submit` flow on a real session cluster
- Grafana rendering and the ClickHouse datasource plugin (queries verified, JSON/provisioning schema not loaded by Grafana)
- Loki/Alloy log shipping
- Flink metrics via the *containerised* Prometheus plugin path (verified via classpath reporter; metric names match the rules)
- Kafka SASL/TLS modes (property templates only; the client config is passed through verbatim)
