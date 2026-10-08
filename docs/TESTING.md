# Testing: what was verified, and how

The Postgres, alerting, analytics and registry work was built in an environment without a Docker daemon, so it was tested against **real binaries run directly**
(the project owner exercised the Docker stack separately: see `CONTEXT.md`):
Apache Kafka 4.0.0 (single-node KRaft), PostgreSQL 17.5, Apicurio Registry 3.3.3, Prometheus 3.5, Alertmanager 0.28,
and the Flink job on an embedded Flink 2.2 mini-cluster.

| Layer | Command | What it proves |
|---|---|---|
| Unit tests + Postgres sink (9) | `make test` (set `SQ_TEST_PG_URL` to include the 9 sink tests; they are skipped otherwise) | parsing/DLQ routing, every check's maths and status rules, sketch merge, thresholds precedence, sink upsert/batching/retry/timer against a real Postgres, schema cache + circuit breaker |
| Schema (11 assertions) | `make test-schema` | PK upsert idempotency, trigger keeps the newest window, enum ordering, jsonb validation, read-only/writer privileges, retention |
| Registry client + decoder (unit) | `make test` | `ConfluentRegistryClient` against a stub that replays response shapes captured from real Karapace and Apicurio (schemaType absent = Avro, 404 codes), `PayloadDecoder` (plain/framed JSON, Avro, unknown id, corrupt Avro, protobuf, registry outage, ids cached for good), `ParseFn` with framed payloads |
| Analytics (23 assertions) | `make test-analytics` | seeds a deterministic history (incidents of known length, a gap that must split one incident into two, flapping, correlated failures, last week's volume) and asserts the EXACT numbers the analytics panels return. The SQL is read from the shipped dashboard JSON, so tested = shipped |
| Dashboards (39 queries, 4 dashboards) | `make test-dashboards` (`--strict` needs data: run bankgen in anomaly mode first) | every Grafana panel/variable query executes on Postgres as the read-only user (`--strict`: non-empty) |
| Alert sink (21 assertions) | `make test-alert-sink` | webhook parsing, idempotent repeats, firing→resolved pairing, re-fire = new instance, 400 on bad JSON, **500 when the DB is down** (so Alertmanager retries), insert-only privileges |

The old end-to-end scripts (`make e2e`, `e2e-outage`, `e2e-alerts`) were removed on 2026-10-08 with the generator.
End-to-end checking is now manual: run bankgen with `DATA_MODE=anomaly` (see the README) and watch Grafana and `sq.check_results`.

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

## Registry support: what was verified (2026-10-08)
Verified end to end **before the e2e scripts and generator were removed** (they live in git history at commit `832c91f`; the runs below
are not reproducible from the current tree, only the unit tests are):

| Registry (real server) | Traffic | Result |
|---|---|---|
| Karapace | plain JSON, subject-latest validation | full fault-injection run, 29/29 assertions |
| Karapace | Confluent-framed JSON Schema | 29/29 |
| Karapace | Confluent-framed Avro | 29/29 |
| Apicurio via `/apis/ccompat/v7` | framed Avro | 29/29 |
| Apicurio via `/apis/ccompat/v7` | framed JSON Schema | 29/29 (after fixing a timing-dependent assertion, not a product bug) |
| Apicurio native v3 | plain JSON | 29/29 (regression after the refactor) |
| Karapace, job started with the registry **down** | framed Avro | 14/14: records counted for volume, no null-rate rows or false alarms, nothing dead-lettered, all checks resumed on recovery |
| Karapace, job started with the registry **down** | framed JSON | 9/9 |
| Apicurio native, registry down at start | plain JSON | 9/9 (regression) |

Notes and limits:
- **Karapace** was built from its GitHub source into a virtualenv and run against a local Kafka. Its Avro dependency is Aiven's patched fork of
  the `avro` package (fetched from a GitHub archive the sandbox blocked), so the **upstream PyPI `avro` was substituted**; basic register/lookup
  is unaffected, but this is not a byte-for-byte production Karapace build.
- **Confluent Schema Registry itself was not run** (Confluent's package hosts were unreachable). Support for it rests on the shared REST API:
  Karapace and Apicurio's compatibility endpoint were exercised with the same client, and the unit stubs replay their responses.
- Test traffic in the Confluent wire format was produced with `fastavro`. Avro cannot carry a wrong-typed value, so structural violations were
  only exercised with JSON Schema.
- The shaded jar (Avro bundled, Jackson relocated) was smoke-tested with a real Avro encode/decode round trip.
- To re-verify against your own registry now: register schemas with `make schemas`, point `cluster.env` at it, and send Confluent-serialized
  traffic from your producer.

## Not verified by this author (no Docker daemon in that environment)
- the Docker image **with the Avro dependency** (the plain Docker stack was run by the project owner on 2026-10-07, before Avro was added); `alert-sink` container build
- Grafana rendering and the Postgres datasource provisioning (queries verified, JSON/provisioning schema not loaded by Grafana). The query
  tester expands `$__timeGroupAlias` with `date_bin`; real Grafana expands it to epoch arithmetic - same grouping, different output type
- the `postgres` and `postgres-maintenance` containers, including the first-boot init and the TCP+role healthcheck (Postgres itself was verified as a real 17.5 server)
- Flink metrics via the *containerised* Prometheus plugin path (verified via classpath reporter; metric names match the rules)
- Kafka SASL/TLS modes (property templates only; the client config is passed through verbatim)
