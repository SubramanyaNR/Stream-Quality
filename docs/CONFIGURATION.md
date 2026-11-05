# Configuration reference

Four files configure everything. `cluster.env` is the only one most people edit.

| File | What it holds | Changes take effect |
|---|---|---|
| `cluster.env` (from `cluster.env.example`, git-ignored) | connection details, passwords, run mode | `make up` / `docker compose up -d` |
| `config/thresholds.yaml` | which topics/fields to check and the limits | job restart (`docker compose restart sq-job`) |
| `config/kafka.properties` | Kafka client settings + `sq.*` source/DLQ options | job restart |
| `config/registry.properties` | schema registry connection | job restart |

`config/*.properties` read `cluster.env` through `${env:NAME}` or `${env:NAME:-default}`. Never put a `#` comment after a value
on the same line (it becomes part of the value; a unit test guards the shipped files).

## cluster.env

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Broker address as the job's container sees it (the advertised listener on that network) |
| `KAFKA_BOOTSTRAP_HOST` | `localhost:9092` | Broker address from this host (used by `make check`) |
| `KAFKA_DOCKER_NETWORK` | `kafka-net` | Docker network Kafka is on; `bridge` if Kafka is not in Docker |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, `SASL_SSL` (enable the matching block in `kafka.properties`) |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` / `KAFKA_TRUSTSTORE_PASSWORD` | empty | Only with a SASL/SSL block |
| `KAFKA_CLI_IMAGE` | `apache/kafka:3.9.1` | Image whose CLI `make check` / `make dlq` / `make demo` use |
| `TOPICS_PATTERN` | `orders\|payments` | Regex (full match) of topics to monitor. Matching topics are re-discovered every 60 s |
| `TOPICS_EXCLUDE` | `_.*` | Regex of topics to skip (the dead-letter topic is always skipped) |
| `TOPICS` | empty | Fixed comma list, used only when `TOPICS_PATTERN` is empty |
| `DLQ_TOPIC` / `DLQ_PARTITIONS` / `DLQ_REPLICATION` / `DLQ_RETENTION_MS` | `sq.dead-letter` / 3 / 1 / 7 days | Dead-letter topic created by `make up` |
| `REGISTRY_ENABLED` | `false` | Turn on schema-registry validation and wire-format decoding |
| `REGISTRY_TYPE` | `apicurio` | `apicurio` (native v3) or `confluent` / `karapace` / `redpanda` / `ccompat` (Confluent REST API) |
| `REGISTRY_URL` / `REGISTRY_USER` / `REGISTRY_PASSWORD` | | Registry endpoint and basic auth |
| `POSTGRES_ADMIN_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, `SQ_WRITER_PASSWORD`, `SQ_READER_PASSWORD` | generated | Leave empty: `make up` fills them with random values. Rotate: edit, `make migrate`, `docker compose up -d` |
| `SQ_BIND_ADDRESS` | `127.0.0.1` | Address the published ports listen on |
| `RESULTS_RETENTION_DAYS` / `HEARTBEAT_RETENTION_DAYS` | 30 / 14 | Postgres retention, purged hourly |
| `COMPOSE_PROFILES` | `compact` | `compact` = job in one ~350 MB JVM; `cluster` = Flink JobManager + TaskManager (RocksDB, scale-out) |
| `SQ_HEAP` / `SQ_JOB_MEM_LIMIT` | `256m` / `512m` | Compact mode heap and container limit |

## thresholds.yaml

Lookup order for every key: `topics.T.fields.F.CHECK.KEY` → `topics.T.CHECK.KEY` → `defaults.CHECK.KEY` → built-in default.
All comparisons are **strictly greater than**: `warn: 0.0` means "any occurrence warns".

### Global keys
| Key | Default | Meaning |
|---|---|---|
| `window.size` | `60s` | Tumbling window over **ingestion** time (when the job read the record). Units: `ms`, `s`, `m` |
| `window.max_out_of_orderness` | `10s` | Skew tolerance between parallel readers; later records are dropped and counted (`late_records`) |
| `window.idle_timeout` | `30s` | A partition without data stops holding back the watermark after this long |
| `event_time_field` | `ts` | JSON path (dots for nesting) of the payload timestamp: epoch s/ms or ISO-8601. Used only by freshness |
| `history_windows` | `30` | How many past windows baseline-relative checks remember |
| `rebaseline_after` | = `history_windows` | Only healthy windows feed the baseline; a violation lasting longer than this becomes the new normal |

### Per topic / field
```yaml
topics:
  orders:                       # topic name (a regex-discovered topic without an entry still gets volume/freshness/schema drift)
    volume: { fail_on_zero: false }          # topic-level override
    fields:
      order_id: { required: true, null_rate: { warn: 0.0, fail: 0.01 } }
      customer_id: { cardinality: true }
```
| Field key | Default | Meaning |
|---|---|---|
| `required` | `false` | A record missing this field is also copied to the dead-letter topic (`required_field_missing`) |
| `cardinality` | `false` | Track distinct values (HyperLogLog) for this field |

### Checks
| Check | Keys (default) | Value in results |
|---|---|---|
| `volume` | `warn_drop_pct` (30), `fail_drop_pct` (60), `min_history` (5), `fail_on_zero` (true), `seasonality` (`none` \| `daily` \| `weekly`), `seasonal_min_cycles` (1) | messages/s; drop vs baseline. With seasonality the baseline is the same hour of previous days/weeks |
| `null_rate` | `warn` (0.05), `fail` (0.20) | share of records where a configured field is missing or null |
| `cardinality` | `warn_ratio` (2.0), `fail_ratio` (5.0), `min_history` (5), `min_baseline` (10) | distinct-count estimate; ratio vs baseline in either direction |
| `freshness` | `warn_p95_ms` (30000), `fail_p95_ms` (120000) | p95 of (ingestion − payload time) |
| `structural` | `warn` (0.0), `fail` (0.01) | share of records violating the registered schema (registry on) |
| `type_drift` | `warn` (0.01), `fail` (0.10), `min_history` (5) | share of a field's values not of its usual JSON type |
| `schema_drift` | `warn` (0), `fail` (3), `min_history` (5), `min_presence` (0.5), `appear_min_rate` (0.05), `disappear_below` (0.01), `enabled` (true) | number of top-level fields that appeared or vanished |

## kafka.properties (`sq.*` keys; everything else is passed to the Kafka client unchanged)
| Key | Default | Meaning |
|---|---|---|
| `sq.source.topic.pattern` | `${env:TOPICS_PATTERN}` | Regex of topics to consume |
| `sq.source.topic.exclude` | `${env:TOPICS_EXCLUDE}` | Regex of topics to skip |
| `sq.source.topics` | empty | Fixed list when no pattern is set |
| `sq.source.framing` | `auto` | Recognise the Confluent wire format (`none` to disable) |
| `sq.dlq.topic` | `sq.dead-letter` | Dead-letter topic |
| `sq.dlq.fail.on.missing` | `true` | Refuse to start if the dead-letter topic does not exist |
| `sq.discovery.interval.ms` | `60000` | How often pattern-matched topics are re-discovered (silence detection) |
| `sq.discovery.max.topics` | `500` | Cap on discovered topics |

## registry.properties
| Key | Default | Meaning |
|---|---|---|
| `sq.registry.enabled` | `false` | |
| `sq.registry.type` | `apicurio` | see `REGISTRY_TYPE` |
| `sq.registry.url` | | |
| `sq.registry.group` | `default` | Apicurio group |
| `sq.registry.artifact.id.template` | `{topic}-value` | Subject/artifact name for a topic |
| `sq.registry.auth.type` | `none` | `none`, `basic`, `bearer` (`auth.username`, `auth.password`, `auth.token`) |
| `sq.registry.refresh.interval.ms` | `300000` | Re-fetch latest schemas |
| `sq.registry.timeout.ms` | `3000` | |
| `sq.registry.breaker.failures` / `sq.registry.breaker.reset.ms` | 3 / 60000 | Circuit breaker: after N failures stop calling for this long |

## Dead-letter reasons
| Reason | Record analysed too? | Meaning |
|---|---|---|
| `invalid_json` | no | Body is not JSON |
| `null_payload` | no | Tombstone / null value |
| `not_a_json_object` | no | JSON, but not an object |
| `bad_timestamp` | no | Event-time field unparseable or more than 24 h in the future |
| `unknown_schema_id` | no | Wire-format schema id not in the registry |
| `avro_decode_failed` / `protobuf_decode_failed` | no | Body does not match its writer schema |
| `unsupported_format` | no | Registered schema type the job cannot decode |
| `schema_violation` | yes | JSON fails its JSON Schema |
| `required_field_missing` | yes | A `required: true` field is missing or null |

## Metrics (Prometheus, port 9249 on the job)
| Metric (as exported) | Meaning |
|---|---|
| `flink_taskmanager_job_task_operator_topic_check_type_field_check_status` | latest status per topic/check/field: 0 ok, 1 warn, 2 fail |
| `flink_taskmanager_job_task_operator_last_window_end_seconds` | end of the newest evaluated window (stall detection) |
| `flink_taskmanager_job_task_operator_consumer_delay_ms` | ingestion time minus Kafka timestamp, max over ~10 s |
| `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max` | the monitor's own consumer lag |
| `flink_taskmanager_job_task_operator_registry_up` | 1 when the schema registry answers |
| `flink_taskmanager_job_task_operator_late_records` | records dropped for arriving after their window closed |

Alerts (`observability/prometheus/rules/sq.yml`): `DataQualityFail`, `DataQualityWarn` (2 min), `QualityMonitorStalled`,
`QualityMonitorMissing`, `QualityMonitorLagging` (> 10 000 records, 2 min), `QualityMonitorBehind` (> 60 s, 2 min).
