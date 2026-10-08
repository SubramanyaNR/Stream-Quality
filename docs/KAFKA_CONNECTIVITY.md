# Connecting to your existing Kafka cluster

The monitor is configured from `cluster.env` (read by `config/kafka.properties`). Run `make check` to test it. The most common failure is **networking, not config**.

## The advertised-listener rule
A Kafka client connects to `bootstrap.servers`, receives the cluster metadata, then reconnects to each broker's
**advertised listener**. From inside the Flink containers that advertised address must be resolvable and reachable.

| Where Kafka runs | `bootstrap.servers` | Broker `advertised.listeners` must contain |
|---|---|---|
| Docker host (Linux/Mac/Win) | `host.docker.internal:9092` | `host.docker.internal:9092` (a *second* listener is fine) |
| Another machine / VM | `kafka.example.com:9092` | `kafka.example.com:9092` |
| Same Docker host, own network | `kafka:29092`; set `KAFKA_DOCKER_NETWORK=<that network>` and the Flink containers join it | the name on that network |

`localhost:9092` advertised ⇒ the client connects, fetches metadata, then tries `localhost` *inside the container*
and fails. Add a listener rather than changing the existing one:
```
listeners=PLAINTEXT://:9092,CONTAINERS://:9094
advertised.listeners=PLAINTEXT://localhost:9092,CONTAINERS://host.docker.internal:9094
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTAINERS:PLAINTEXT,CONTROLLER:PLAINTEXT
```

## Permissions the monitor needs (when ACLs are on)
- `READ` + `DESCRIBE` on each monitored topic and on group `stream-quality-monitor`
- `WRITE` + `DESCRIBE` on the dead-letter topic; `DESCRIBE` on the cluster (heartbeat probe, `listTopics`)
- the DLQ topic is created by `make dlq` (needs `CREATE` on it once)

## Verify before `make up`
`make dlq` doubles as a connectivity + auth test: it reads your `kafka.properties` and talks to the cluster.
The job also fails at submit time with an actionable message if a topic is missing or the cluster is unreachable.
