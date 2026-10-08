# Testing: what was verified, and how

No Docker daemon was available while building this, so everything was tested against **real binaries run directly**:
Apache Kafka 4.0.0 (single-node KRaft), PostgreSQL 17.5, Apicurio Registry 3.3.3, Prometheus 3.5, Alertmanager 0.28,
and the Flink job on an embedded Flink 2.2 mini-cluster.

| Layer | Command | What it proves |
|---|---|---|
| Unit (61 tests) + Postgres sink (9) | `make test` (set `SQ_TEST_PG_URL` to include the 9 sink tests; they are skipped otherwise) | parsing/DLQ routing, every check's maths and status rules, sketch merge, thresholds precedence, sink upsert/batching/retry/timer against a real Postgres, schema cache + circuit breaker |
| Schema (11 assertions) | `make test-schema` | PK upsert idempotency, trigger keeps the newest window, enum ordering, jsonb validation, read-only/writer privileges, retention |
| End to end (29 assertions) | `make e2e` | Kafka → job → PostgreSQL with scripted faults: null spike, id explosion, 150 s-stale events, schema break, −90 % volume, silence, corrupt JSON; DLQ contents; MVs; heartbeat |
| Registry matrix (29 assertions each) | `make e2e` with `E2E_REGISTRY_TYPE` / `E2E_SERIALIZATION` | the full fault-injection run against **real Karapace** (plain JSON, Confluent-framed JSON, Confluent-framed Avro) and **real Apicurio via its Confluent-compatible endpoint** (framed Avro, framed JSON), plus Apicurio native. Generator produces genuine wire-format traffic, registering schemas in the registry under test |
| Registry outage (9-14 assertions) | `make e2e-outage` (same env vars) | job starts with the registry **down** (cold cache): JSON - statistical checks run, structural resumes on recovery; **Avro - records counted for volume but ignored by field checks, no false alarms, no DLQ flood, everything resumes on recovery** (verified on Karapace) |
| Confluent client + decoder (unit) | `make test` | `ConfluentRegistryClient` against a stub that replays response shapes captured from real Karapace/Apicurio (schemaType absent = Avro, 404 codes), `PayloadDecoder` (JSON/Avro/unknown id/corrupt/protobuf/outage), `ParseFn` with framed payloads |
| Analytics (23 assertions) | `make test-analytics` | seeds a deterministic history (incidents of known length, a gap that must split one incident into two, flapping, correlated failures, last week's volume) and asserts the EXACT numbers the analytics panels return. The SQL is read from the shipped dashboard JSON, so tested = shipped |
| Dashboards (39 queries, 4 dashboards) | `make e2e` then `make test-dashboards` (strict mode needs the fault data the e2e leaves behind) | every Grafana panel/variable query executes on Postgres as the read-only user (`--strict`: non-empty) |
| Alert sink (21 assertions) | `make test-alert-sink` | webhook parsing, idempotent repeats, firing→resolved pairing, re-fire = new instance, 400 on bad JSON, **500 when the DB is down** (so Alertmanager retries), insert-only privileges |
| Alert path, live (8 assertions) | `make e2e-alerts` | real job + Prometheus + Alertmanager (re-sending every 10 s) + sink + Postgres: a null spike FIRES an alert, recovery RESOLVES it, exactly one firing + one resolved row despite repeated notifications, no alerts on a healthy topic while traffic flows. Needs `PROM_HOME`, `AM_HOME`, `PROM_REPORTER_JAR` (see script header) |

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

## Registry testing notes
- **Karapace** was built from its GitHub source into a virtualenv and run against the local Kafka. Its Avro dependency is Aiven's patched fork
  of the `avro` package (fetched from a GitHub archive that the sandbox blocks), so the **upstream PyPI `avro` was substituted**; basic
  register/lookup is unaffected, but this is not a byte-for-byte production Karapace build.
- **Confluent Schema Registry itself was not run** (Confluent's package hosts are unreachable from the sandbox). Support for it rests on the
  shared REST API: Karapace and Apicurio's compatibility endpoint were exercised with the same client, and stub tests replay their responses.
- Generating Confluent-framed Avro uses `fastavro`; Avro wrong-type violations cannot be produced (an Avro serializer refuses them), so the
  structural-FAIL assertions are JSON-only.

## NOT verified (no Docker / network access in the build sandbox)
- `docker compose up` end to end, the `flink/Dockerfile` build, the `flink-job-submit` flow on a real session cluster
- Grafana rendering and the Postgres datasource provisioning (queries verified, JSON/provisioning schema not loaded by Grafana). The query
  tester expands `$__timeGroupAlias` with `date_bin`; real Grafana expands it to epoch arithmetic - same grouping, different output type
- the `postgres` and `postgres-maintenance` containers, including the first-boot init and the TCP+role healthcheck (Postgres itself was verified as a real 17.5 server)
- Flink metrics via the *containerised* Prometheus plugin path (verified via classpath reporter; metric names match the rules)
- Kafka SASL/TLS modes (property templates only; the client config is passed through verbatim)
