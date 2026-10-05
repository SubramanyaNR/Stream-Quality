-- =============================================================================
-- Stream Quality Monitor : ClickHouse schema (idempotent; safe to re-run)
-- Design rules:
--   * Flink is at-least-once -> every table is (Replacing)MergeTree keyed on the
--     natural window key so replays collapse. MVs target Replacing tables too.
--   * Dashboards read through views that use argMax/FINAL-free patterns.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS sq;

-- ---------- 1. Core fact table: one row per check result per window ----------
CREATE TABLE IF NOT EXISTS sq.check_results
(
    topic         LowCardinality(String),
    field         LowCardinality(String)  DEFAULT '',          -- '' = topic-level check
    check_type    LowCardinality(String),                       -- volume|null_rate|cardinality|freshness|structural|heartbeat
    window_start  DateTime64(3, 'UTC'),
    window_end    DateTime64(3, 'UTC'),
    value         Float64,
    threshold     Float64,                                      -- threshold that determined status (fail if fail, else warn)
    status        Enum8('ok' = 0, 'warn' = 1, 'fail' = 2),
    details       String CODEC(ZSTD(3)),                        -- JSON text
    job_id        LowCardinality(String)  DEFAULT '',
    ingested_at   DateTime64(3, 'UTC')    DEFAULT now64(3),
    CONSTRAINT details_is_json CHECK isValidJSON(details)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (topic, check_type, field, window_start, window_end)
TTL toDateTime(window_start) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Skip indexes: cheap wins for the violation log and details search
ALTER TABLE sq.check_results ADD INDEX IF NOT EXISTS idx_status status TYPE set(3) GRANULARITY 4;
ALTER TABLE sq.check_results ADD INDEX IF NOT EXISTS idx_window_end window_end TYPE minmax GRANULARITY 4;

-- ---------- 2. Violation audit log (every warn/fail, forever-ish) ------------
CREATE TABLE IF NOT EXISTS sq.violations
(
    topic LowCardinality(String), field LowCardinality(String), check_type LowCardinality(String),
    window_start DateTime64(3,'UTC'), window_end DateTime64(3,'UTC'),
    value Float64, threshold Float64,
    status Enum8('ok'=0,'warn'=1,'fail'=2),
    details String CODEC(ZSTD(3)), ingested_at DateTime64(3,'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, topic, check_type, field)
TTL toDateTime(window_start) + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS sq.mv_violations TO sq.violations AS
SELECT topic, field, check_type, window_start, window_end, value, threshold, status, details, ingested_at
FROM sq.check_results
WHERE status != 'ok' AND check_type != 'heartbeat';

-- ---------- 3. Latest state per (topic, check, field) -> RAG overview --------
CREATE TABLE IF NOT EXISTS sq.latest_status
(
    topic LowCardinality(String), field LowCardinality(String), check_type LowCardinality(String),
    window_end DateTime64(3,'UTC'), status Enum8('ok'=0,'warn'=1,'fail'=2), value Float64, threshold Float64
)
ENGINE = ReplacingMergeTree(window_end)
ORDER BY (topic, check_type, field);

CREATE MATERIALIZED VIEW IF NOT EXISTS sq.mv_latest_status TO sq.latest_status AS
SELECT topic, field, check_type, window_end, status, value, threshold
FROM sq.check_results WHERE check_type != 'heartbeat';

-- Topic x check RAG: worst status among fields, ignoring stale (> 5 min) rows.
CREATE VIEW IF NOT EXISTS sq.topic_health AS
SELECT topic, check_type,
       max(status)       AS worst_status,           -- Enum compares by value: fail > warn > ok
       max(window_end)   AS last_window_end,
       max(window_end) < now64(3) - INTERVAL 5 MINUTE AS is_stale
FROM sq.latest_status FINAL
GROUP BY topic, check_type;

-- ---------- 4. (no rollups) ----------------------------------------------------
-- An hourly AggregatingMergeTree rollup was tried and REMOVED: materialized views see raw insert
-- blocks before ReplacingMergeTree dedup, so replays after a Flink restart would double count.
-- Volume is ~1 row/field/minute; dashboards query check_results directly (use argMax or FINAL).

-- ---------- 5. Job heartbeat (Phase 1) ---------------------------------------
CREATE TABLE IF NOT EXISTS sq.job_heartbeat
(
    job_id LowCardinality(String), ts DateTime64(3,'UTC'), version LowCardinality(String),
    kafka_bootstrap String, kafka_status LowCardinality(String), kafka_cluster_id String,   -- up|down
    registry_status LowCardinality(String)                                                   -- up|down|disabled
)
ENGINE = MergeTree ORDER BY (job_id, ts) TTL toDateTime(ts) + INTERVAL 14 DAY;

-- ---------- 6. Least-privilege users ------------------------------------------
-- Flink writes only; Grafana reads only. (Passwords come from compose env.)
CREATE USER IF NOT EXISTS sq_writer IDENTIFIED WITH sha256_password BY 'sq_writer_pw';
GRANT INSERT, SELECT ON sq.* TO sq_writer;
CREATE USER IF NOT EXISTS sq_reader IDENTIFIED WITH sha256_password BY 'sq_reader_pw';
GRANT SELECT ON sq.* TO sq_reader;
