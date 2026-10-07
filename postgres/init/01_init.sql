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

-- ---------- 3b. Analytics: incidents ----------
-- An incident = a run of consecutive non-ok windows for one (topic, check_type, field). It ends at the next ok
-- window, or when windows stop arriving for > 3 window-lengths (monitor down / topic gone), so a gap never
-- silently glues two incidents together. A function (not a view) so the time filter is applied BEFORE the
-- window functions run: dashboards stay fast as the table grows. Incidents are clipped to [from_ts, to_ts].
CREATE OR REPLACE FUNCTION sq.incidents(from_ts timestamptz, to_ts timestamptz)
RETURNS TABLE (topic text, check_type text, field text, started timestamptz, ended timestamptz, duration interval,
               windows bigint, fail_windows bigint, worst_status sq.check_status)
LANGUAGE sql STABLE AS $$
    WITH ordered AS (
        SELECT r.topic, r.check_type, r.field, r.window_start, r.window_end, r.status,
               lag(r.window_end) OVER (PARTITION BY r.topic, r.check_type, r.field ORDER BY r.window_start) AS prev_end
        FROM sq.check_results r
        WHERE r.window_start >= from_ts AND r.window_start <= to_ts
    ), marked AS (
        -- the counter ticks on every ok window and every gap, so consecutive bad windows share one group id
        SELECT o.*, sum(CASE WHEN o.status = 'ok' OR o.prev_end IS NULL
                                  OR o.window_start - o.prev_end > 3 * (o.window_end - o.window_start) THEN 1 ELSE 0 END)
                    OVER (PARTITION BY o.topic, o.check_type, o.field ORDER BY o.window_start) AS grp
        FROM ordered o
    )
    SELECT m.topic, m.check_type, m.field, min(m.window_start), max(m.window_end), max(m.window_end) - min(m.window_start),
           count(*), count(*) FILTER (WHERE m.status = 'fail'), max(m.status)
    FROM marked m
    WHERE m.status <> 'ok'
    GROUP BY m.topic, m.check_type, m.field, m.grp
$$;

-- ---------- 4. Job heartbeat ----------
CREATE TABLE IF NOT EXISTS sq.job_heartbeat
(
    job_id text NOT NULL, ts timestamptz NOT NULL, version text NOT NULL DEFAULT '',
    kafka_bootstrap text NOT NULL DEFAULT '', kafka_status text NOT NULL DEFAULT '', kafka_cluster_id text NOT NULL DEFAULT '',
    registry_status text NOT NULL DEFAULT '',                       -- up|down|disabled
    PRIMARY KEY (job_id, ts)
);

-- ---------- 4b. Alert history (written by tools/alert-sink from Alertmanager webhooks) ----------
-- One row per (alert instance, status): Alertmanager re-sends "firing" notifications every repeat_interval, so the
-- unique key (fingerprint, status, starts_at) makes those repeats no-ops and leaves exactly one 'firing' and one
-- 'resolved' row per alert instance.
CREATE TABLE IF NOT EXISTS sq.alert_events
(
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now(),
    status      text        NOT NULL CHECK (status IN ('firing', 'resolved')),
    fingerprint text        NOT NULL,
    alertname   text        NOT NULL,
    severity    text        NOT NULL DEFAULT '',
    topic       text        NOT NULL DEFAULT '',
    check_type  text        NOT NULL DEFAULT '',
    field       text        NOT NULL DEFAULT '',
    starts_at   timestamptz NOT NULL,
    ends_at     timestamptz,                                        -- NULL while firing
    summary     text        NOT NULL DEFAULT '',
    labels      jsonb       NOT NULL DEFAULT '{}',
    annotations jsonb       NOT NULL DEFAULT '{}',
    UNIQUE (fingerprint, status, starts_at)
);
CREATE INDEX IF NOT EXISTS idx_alert_events_starts_at ON sq.alert_events (starts_at DESC);

-- One row per alert instance: when it fired, when (if) it resolved, how long it lasted.
CREATE OR REPLACE VIEW sq.alert_history AS
SELECT f.alertname, f.severity, f.topic, f.check_type, f.field, f.summary,
       f.starts_at                              AS fired_at,
       r.ends_at                                AS resolved_at,
       COALESCE(r.ends_at, now()) - f.starts_at AS duration,
       r.ends_at IS NULL                        AS firing
FROM sq.alert_events f
LEFT JOIN sq.alert_events r ON r.fingerprint = f.fingerprint AND r.starts_at = f.starts_at AND r.status = 'resolved'
WHERE f.status = 'firing';

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
    DELETE FROM sq.alert_events WHERE starts_at < now() - make_interval(days => results_days);   -- alert history follows result retention
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
GRANT SELECT, INSERT ON sq.alert_events TO sq_writer;                 -- the alert-sink (inserts only: it cannot rewrite history)
GRANT SELECT ON ALL TABLES IN SCHEMA sq TO sq_reader;                -- includes the views
GRANT SELECT ON sq.violations, sq.topic_health TO sq_writer;
GRANT EXECUTE ON FUNCTION sq.incidents(timestamptz, timestamptz) TO sq_reader, sq_writer;
