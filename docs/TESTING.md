# Testing: what was verified, and how

No Docker daemon was available while building this, so everything was tested against **real binaries run directly**:
Apache Kafka 4.0.0 (single-node KRaft), PostgreSQL 17.5, Apicurio Registry 3.3.3, Prometheus 3.5, Alertmanager 0.28,
and the Flink job on an embedded Flink 2.2 mini-cluster.

| Layer | Command | What it proves |
|---|---|---|
| Unit (40 tests) + Postgres sink (9) | `make test` (set `SQ_TEST_PG_URL` to include the 9 sink tests; they are skipped otherwise) | parsing/DLQ routing, every check's maths and status rules, sketch merge, thresholds precedence, sink upsert/batching/retry/timer against a real Postgres, schema cache + circuit breaker |
| Schema (11 assertions) | `make test-schema` | PK upsert idempotency, trigger keeps the newest window, enum ordering, jsonb validation, read-only/writer privileges, retention |
| End to end (29 assertions) | `make e2e` | Kafka → job → PostgreSQL with scripted faults: null spike, id explosion, 150 s-stale events, schema break, −90 % volume, silence, corrupt JSON; DLQ contents; MVs; heartbeat |
| Registry outage | `make e2e-outage` | job starts with Apicurio **down**: statistical checks run, no structural rows/failures, structural resumes on recovery |
| Analytics (23 assertions) | `make test-analytics` | seeds a deterministic history (incidents of known length, a gap that must split one incident into two, flapping, correlated failures, last week's volume) and asserts the EXACT numbers the analytics panels return. The SQL is read from the shipped dashboard JSON, so tested = shipped |
| Dashboards (39 queries, 4 dashboards) | `make e2e` then `make test-dashboards` (strict mode needs the fault data the e2e leaves behind) | every Grafana panel/variable query executes on Postgres as the read-only user (`--strict`: non-empty) |
| Alert path | manual (see below) | Flink gauge → Prometheus scrape → rule → Alertmanager → webhook |

`make e2e` needs Kafka, PostgreSQL (database `sq`, `postgres/init/01_init.sql` applied) and optionally Apicurio running locally;
see the header of `scripts/e2e-local.sh` for the env vars.

## Bugs that only real components exposed (all fixed)
- window by payload time dropped stale events (switched to ingestion time)
- sink wrote nothing without checkpointing; `close()` could interrupt an in-flight INSERT
- anomalous windows polluted the rolling baseline, weakening detection during sustained incidents (now: learn from OK windows only)
- a cached 404 kept structural checks off for 5 minutes after the schema was registered
- invalid YAML in the shipped `thresholds.yaml` (`key:{` without a space)
- checkpoint volume not writable by the `flink` user; env substitution order in `create-dlq-topic.sh`
- history from the ClickHouse era, before the switch to Postgres: a materialized-view rollup double counted replays; a column alias
  shadowed the real column inside `WHERE` and silently emptied Grafana overlay series. Neither can occur with a primary-key upsert.
- (Postgres) `ON CONFLICT DO UPDATE` cannot touch one row twice in a statement, so the sink de-duplicates within a batch

## NOT verified (no Docker / network access in the build sandbox)
- `docker compose up` end to end, the `flink/Dockerfile` build, the `flink-job-submit` flow on a real session cluster
- Grafana rendering and the Postgres datasource provisioning (queries verified, JSON/provisioning schema not loaded by Grafana). The query
  tester expands `$__timeGroupAlias` with `date_bin`; real Grafana expands it to epoch arithmetic - same grouping, different output type
- the `postgres` and `postgres-maintenance` containers, including the first-boot init and the TCP+role healthcheck (Postgres itself was verified as a real 17.5 server)
- Loki/Alloy log shipping
- Flink metrics via the *containerised* Prometheus plugin path (verified via classpath reporter; metric names match the rules)
- Kafka SASL/TLS modes (property templates only; the client config is passed through verbatim)
