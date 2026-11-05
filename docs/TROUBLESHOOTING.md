# Troubleshooting

Start with `make check`: it tests the Docker network, broker reachability from containers and from this host, and the topics.

| Symptom | Likely cause | Fix |
|---|---|---|
| `make check`: "cannot connect" from containers | The broker advertises an address the job's network cannot resolve (`localhost:9092` is the usual culprit) | Point `KAFKA_BOOTSTRAP` at the listener advertised on `KAFKA_DOCKER_NETWORK`; see [KAFKA_CONNECTIVITY.md](KAFKA_CONNECTIVITY.md) |
| `make up` stops with "created cluster.env" | First run | Set the Kafka values in `cluster.env`, run `make up` again |
| Compose error "run make up (it generates passwords into cluster.env)" | Started compose without `cluster.env` or with empty passwords | Run `make up` (or `./scripts/init-secrets.sh`) |
| Job exits: "Required topic 'sq.dead-letter' does not exist" | Dead-letter topic missing | `make dlq` |
| Postgres auth errors after upgrading | Older database, new generated passwords | `make migrate` applies schema changes and the passwords from `cluster.env` |
| Dashboards empty, `sq-job` running | No traffic yet, or the topic is not matched by `TOPICS_PATTERN` | `make status`; check `TOPICS_PATTERN`; try `make demo` |
| A topic shows volume FAIL constantly | Topic is silent, or it is in `thresholds.yaml` but does not exist | Remove it from `thresholds.yaml`, or set `volume: { fail_on_zero: false }` for topics that are legitimately quiet |
| Every field 100% null | Field names in `thresholds.yaml` do not match the payload (nested fields need `a.b`) | Fix the field paths |
| Avro/Protobuf topics: volume counted, no field checks | Registry unreachable or disabled (records count but are not decoded) | `REGISTRY_ENABLED=true`, correct `REGISTRY_URL`/`REGISTRY_TYPE`; `registry_up` metric shows the state |
| `QualityMonitorBehind` / `QualityMonitorLagging` firing | The monitor cannot keep up, or producers back-date Kafka timestamps | More heap/parallelism (`cluster` profile scales out); check producer timestamps |
| Lots of `late_records` | `window.max_out_of_orderness` too small for your reader skew | Raise it (e.g. `10s`) |
| Grafana shows no styling / dark theme | `GF_PANELS_DISABLE_SANITIZE_HTML` or `GF_USERS_DEFAULT_THEME` not set, or a Grafana version other than 13.0.x | Use the shipped compose settings; the stylesheet relies on Grafana 13 class names |
| Prometheus target `flink-jobmanager` down | Expected in compact mode (no JobManager container) | Ignore, or remove that target in `observability/prometheus/prometheus.yml` |
| Can't reach Grafana from another machine | Ports bind to `127.0.0.1` by default | Use SSH port-forwarding or a VPN; set `SQ_BIND_ADDRESS=0.0.0.0` only behind a firewall/authenticating proxy (see SECURITY.md) |

Logs: `make logs` (job), `docker compose logs alert-sink grafana postgres`.
