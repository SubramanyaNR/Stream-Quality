# Stream-Quality: handoff

Flink job that watches an existing Kafka cluster (volume, null rate, cardinality, freshness, schema checks), writes one row
per check per window to PostgreSQL, alerts via Alertmanager, shows it in Grafana. Corrupt or invalid records go to a dead-letter topic.

## Run it
```bash
make up              # first run creates cluster.env and stops
$EDITOR cluster.env  # Kafka address, Docker network, topics, registry, passwords
make check           # brokers reachable from the containers and from this host?
make up
```
Everything cluster-specific is in `cluster.env`. `config/*.properties` read it via `${env:NAME}`; compose and scripts read the same file.
Switch clusters by editing it and running `make up` again.

## Test data and current setup (2026-10-08)
`cluster.env` points at the kafka-sandbox started with `BROKERS=1` (`ks-kafka1:9092`, network `kafka-sandbox`, host `localhost:39092`,
auto-create on, no registry). Data comes from bankgen in `/opt/ai-sre/kafka-sandbox`, not from this repo:
```bash
make up BROKERS=1            # sandbox broker only, creates no topics
make up-data                 # clean bank traffic
make up-data DATA_MODE=anomaly   # cycles null spike, corrupt JSON, type break, volume drop, silence (ANOMALY_PHASE_SEC, default 90)
```
Monitored: `TOPICS_PATTERN` matches `bank.dbo.transactions`, `bank.app.clickstream`, `bank.app.application_logs`; their required fields
and `event_time_field: event_time` are in `config/thresholds.yaml`. This combination has not been run end to end yet.

## Tested earlier (2026-10-07), standalone single-broker, with the since-removed generator
Standalone `kafka` container on `kafka-net` (`KAFKA_BOOTSTRAP=kafka:29092`, host `localhost:9092`, RF 1).
- `make check`, `make dlq`, `make up` from a clean state; job RUNNING on Flink 2.2.1.
- Generator scenarios healthy, null_spike, bad_payloads: rows in `sq.check_results` (null_rate warn/fail on `orders.customer_id` and `order_id`),
  183 dead-letter records on `sq.dead-letter`, four Grafana dashboards provisioned, Postgres datasource healthy.
- Memory with Kafka running: flink-tm ~850 MB, flink-jm ~460 MB, grafana ~145 MB, postgres ~60 MB, prometheus ~40 MB; host used ~4.0 GB of 7.7 GB.
- 51 unit tests pass (`make test`, runs against `cluster.env.example` defaults).

## Fixed while getting it to run in Docker (the docs said Docker was never executed)
- `flink/Dockerfile`: the Prometheus reporter already ships in `plugins/` in Flink 2.x; `chmod` needed root.
- `flink/bin/submit-job.sh`: Flink 2 has no `-m`, and the job reads `conf/config.yaml`, so the client now writes `rest.address` there.
- `docker-compose.yml`: JobManager needs `rest.address`; an inline `#` comment in `FLINK_PROPERTIES` broke the TaskManager.
- `create-dlq-topic.sh`: now runs on the Kafka network and reads the resolved config.

## Not verified / known gaps
- 3-broker sandbox (`ks-kafka1..3`, RF 3): only the settings differ; not run yet.
- Silence detection (empty window) only covers topics listed in `config/thresholds.yaml`; pattern-matched topics get checks once data flows.
- Schema registry: off by default. `REGISTRY_TYPE` in `cluster.env` selects `apicurio` (native v3), or `confluent` / `karapace` / `redpanda` / `ccompat`
  (Confluent REST API: JSON Schema + Avro, Confluent wire format). Verified 2026-10-08 against a real Karapace and Apicurio's `/apis/ccompat/v7`
  (see docs/TESTING.md); **Confluent's own server was not run**. Protobuf is not supported (dead-lettered as `unsupported_format`). The Docker image
  has not been rebuilt/run since Avro was added to the shaded jar.
- A cluster outside Docker: set `KAFKA_DOCKER_NETWORK` to a network that exists (compose requires it); making it optional is open.
- The repo generator was removed (2026-10-08); see "Test data and current setup".
- The `scripts/e2e-*` scripts and their `make e2e*` targets were removed with the generator; end-to-end checking is manual with bankgen.
- Kubernetes packaging: not started.
