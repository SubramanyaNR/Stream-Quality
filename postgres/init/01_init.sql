-- =============================================================================
-- Stream Quality Monitor : PostgreSQL schema (idempotent; safe to re-run)
-- Runs once on first container start (docker-entrypoint-initdb.d) as the admin user.
-- Design rules:
--   * Flink is at-least-once -> check_results has a real PRIMARY KEY and the sink UPSERTS, so replays
--     are exactly idempotent. No FINAL / argMax tricks: every query sees one row per window.
--   * "Latest status" is maintained by a trigger (cheap to read; replay-safe via a window_end guard).
--   * Retention is a function (sq.purge_old) run hourly by the postgres-maintenance container.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS sq;

DO $$ BEGIN
    CREATE TYPE sq.check_status AS ENUM ('ok', 'warn', 'fail');       -- enum order: fail > warn > ok, max() works
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ---------- 1. Core fact table: one row per check result per window ----------
CREATE TABLE IF NOT EXISTS sq.check_results
(
    topic         text              NOT NULL,
    field         text              NOT NULL DEFAULT '',            -- '' = topic-level check
    check_type    text              NOT NULL,                       -- volume|null_rate|cardinality|freshness|structural
    window_start  timestamptz       NOT NULL,
    window_end    timestamptz       NOT NULL,
    value         double precision  NOT NULL,
    threshold     double precision  NOT NULL,                       -- threshold that determined status
    status        sq.check_status   NOT NULL,
    details       jsonb             NOT NULL DEFAULT '{}',
    job_id        text              NOT NULL DEFAULT '',
    ingested_at   timestamptz       NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, check_type, field, window_start, window_end)
);
-- time-range scans (every dashboard panel) and retention deletes
CREATE INDEX IF NOT EXISTS idx_check_results_window_start ON sq.check_results (window_start DESC);
-- the violation log reads only non-ok rows: a partial index keeps it tiny and fast
CREATE INDEX IF NOT EXISTS idx_check_results_violations ON sq.check_results (window_end DESC) WHERE status <> 'ok';

-- ---------- 2. Violation audit log: every warn/fail (a view: no second copy to keep in sync) ----------
CREATE OR REPLACE VIEW sq.violations AS
SELECT topic, field, check_type, window_start, window_end, value, threshold, status, details, ingested_at
FROM sq.check_results
WHERE status <> 'ok';

-- ---------- 3. Latest state per (topic, check, field) -> RAG overview ----------
CREATE TABLE IF NOT EXISTS sq.latest_status
(
    topic text NOT NULL, field text NOT NULL, check_type text NOT NULL,
    window_end timestamptz NOT NULL, status sq.check_status NOT NULL,
    value double precision NOT NULL, threshold double precision NOT NULL,
    PRIMARY KEY (topic, check_type, field)
);

CREATE OR REPLACE FUNCTION sq.upsert_latest_status() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sq.latest_status AS l (topic, field, check_type, window_end, status, value, threshold)
    VALUES (NEW.topic, NEW.field, NEW.check_type, NEW.window_end, NEW.status, NEW.value, NEW.threshold)
    ON CONFLICT (topic, check_type, field) DO UPDATE
       SET window_end = EXCLUDED.window_end, status = EXCLUDED.status, value = EXCLUDED.value, threshold = EXCLUDED.threshold
     WHERE EXCLUDED.window_end >= l.window_end;                       -- a replayed OLD window never overwrites a newer one
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_latest_status ON sq.check_results;
CREATE TRIGGER trg_latest_status AFTER INSERT OR UPDATE ON sq.check_results
    FOR EACH ROW EXECUTE FUNCTION sq.upsert_latest_status();

-- Topic x check RAG: worst status among fields; stale = no result for 5 minutes.
CREATE OR REPLACE VIEW sq.topic_health AS
SELECT topic, check_type,
       max(status)       AS worst_status,
       max(window_end)   AS last_window_end,
       max(window_end) < now() - interval '5 minutes' AS is_stale
FROM sq.latest_status
GROUP BY topic, check_type;

-- ---------- 4. Job heartbeat ----------
CREATE TABLE IF NOT EXISTS sq.job_heartbeat
(
    job_id text NOT NULL, ts timestamptz NOT NULL, version text NOT NULL DEFAULT '',
    kafka_bootstrap text NOT NULL DEFAULT '', kafka_status text NOT NULL DEFAULT '', kafka_cluster_id text NOT NULL DEFAULT '',
    registry_status text NOT NULL DEFAULT '',                       -- up|down|disabled
    PRIMARY KEY (job_id, ts)
);

-- ---------- 5. Retention ----------
-- Plain DELETEs are fine at this volume (~1 row/field/minute). If you shorten windows or monitor thousands of
-- fields (> ~100M rows), convert check_results to PARTITION BY RANGE (window_start) or use TimescaleDB.
CREATE OR REPLACE FUNCTION sq.purge_old(results_days int DEFAULT 90, heartbeat_days int DEFAULT 14) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE n bigint; m bigint;
BEGIN
    DELETE FROM sq.check_results WHERE window_start < now() - make_interval(days => results_days);
    GET DIAGNOSTICS n = ROW_COUNT;
    DELETE FROM sq.job_heartbeat WHERE ts < now() - make_interval(days => heartbeat_days);
    GET DIAGNOSTICS m = ROW_COUNT;
    RETURN n + m;
END $$;

-- ---------- 6. Least-privilege roles: Flink writes only, Grafana reads only ----------
-- (demo passwords; change them before exposing anything)
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sq_writer') THEN CREATE ROLE sq_writer LOGIN PASSWORD 'sq_writer_pw'; END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sq_reader') THEN CREATE ROLE sq_reader LOGIN PASSWORD 'sq_reader_pw'; END IF;
END $$;
GRANT USAGE ON SCHEMA sq TO sq_writer, sq_reader;
GRANT SELECT, INSERT, UPDATE ON sq.check_results, sq.latest_status, sq.job_heartbeat TO sq_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA sq TO sq_reader;                -- includes the views
GRANT SELECT ON sq.violations, sq.topic_health TO sq_writer;
