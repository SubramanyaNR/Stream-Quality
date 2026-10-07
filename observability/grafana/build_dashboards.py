#!/usr/bin/env python3
"""Generates the three provisioned dashboards into ./dashboards/*.json (the JSON is committed; edit THIS file).
All panel SQL is also executed against PostgreSQL by scripts/test_dashboard_queries.py."""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "sq-pg"}
OUT = Path(__file__).parent / "dashboards"
TABLE, TS = "table", "time_series"
STATUS_MAP = [{"type": "value", "options": {
    "0": {"text": "OK", "color": "green", "index": 0},
    "1": {"text": "WARN", "color": "orange", "index": 1},
    "2": {"text": "FAIL", "color": "red", "index": 2}}},
    {"type": "special", "options": {"match": "null", "result": {"text": "no data", "color": "text", "index": 3}}}]
STATUS_N = "CASE status WHEN 'ok' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END"


def target(sql, fmt=TABLE, ref="A"):
    # Time-series format conventions of the Grafana Postgres datasource: a "time" column, an optional "metric"
    # column (series name) and numeric value columns.
    return {"refId": ref, "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": " ".join(sql.split())}


def panel(pid, title, ptype, x, y, w, h, targets, desc="", defaults=None, overrides=None, options=None, ds=DS):
    return {"id": pid, "title": title, "type": ptype, "description": desc, "datasource": ds,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []}, "options": options or {}}


def dashboard(uid, title, panels, variables=None, links=None, desc=""):
    return {"uid": uid, "title": title, "description": desc, "tags": ["stream-quality"], "timezone": "utc",
            "schemaVersion": 39, "version": 1, "editable": True, "refresh": "10s",
            "time": {"from": "now-1h", "to": "now"}, "graphTooltip": 1,
            "templating": {"list": variables or []}, "links": links or [], "panels": panels,
            "annotations": {"list": []}}


def query_var(name, label, sql):
    q = " ".join(sql.split())
    return {"name": name, "label": label, "type": "query", "datasource": DS, "query": q, "definition": q,
            "refresh": 2, "includeAll": True, "multi": True, "allValue": None,
            "current": {"text": "All", "value": "$__all"}, "sort": 1}


NAV = [{"title": "Topic health", "type": "link", "url": "/d/sq-topic-health", "icon": "dashboard"},
       {"title": "Field drill-down", "type": "link", "url": "/d/sq-field-drilldown", "icon": "dashboard"},
       {"title": "Violation log", "type": "link", "url": "/d/sq-violations", "icon": "bolt"},
       {"title": "Quality analytics", "type": "link", "url": "/d/sq-analytics", "icon": "apps"}]

# ----------------------------------------------------------------------------- 1. topic health
checks = ["volume", "null_rate", "cardinality", "freshness", "structural"]
# max(...) FILTER is NULL when the topic has no result for that check -> rendered as "no data"
rag_cols = ",\n".join(f"max(status_n) FILTER (WHERE check_type = '{c}') AS {c}" for c in checks)
RAG_SQL = f"""
SELECT topic, {rag_cols}, max(window_end) AS last_window
FROM (SELECT topic, check_type, {STATUS_N} AS status_n, window_end
      FROM sq.latest_status WHERE window_end > now() - interval '5 minutes') s
GROUP BY topic ORDER BY topic"""
rag_overrides = [{"matcher": {"id": "byName", "options": c}, "properties": [
    {"id": "mappings", "value": STATUS_MAP}, {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}}]}
    for c in checks]
rag_overrides.append({"matcher": {"id": "byName", "options": "topic"}, "properties": [
    {"id": "links", "value": [{"title": "Drill down", "url": "/d/sq-field-drilldown?var-topic=${__value.raw}&${__url_time_range}"}]}]})

p1 = [
    panel(1, "Topic health (latest window per check)", "table", 0, 0, 16, 9, [target(RAG_SQL)],
          "Worst status across fields for each check type, from the latest window. 'no data' = no result in the last 5 minutes "
          "(check not configured for the topic, or the topic/monitor is silent). Click a topic to drill down.",
          defaults={"custom": {"align": "center"}}, overrides=rag_overrides),
    panel(2, "Topics failing", "stat", 16, 0, 4, 3, [target(
        "SELECT count(DISTINCT topic) AS failing FROM sq.latest_status WHERE status = 'fail' AND window_end > now() - interval '5 minutes'")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}}),
    panel(3, "Topics warning", "stat", 20, 0, 4, 3, [target(
        "SELECT count(DISTINCT topic) AS warning FROM sq.latest_status WHERE status = 'warn' AND window_end > now() - interval '5 minutes'")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}]}}),
    panel(4, "Monitor heartbeat age (s)", "stat", 16, 3, 4, 3, [target(
        "SELECT extract(epoch FROM now() - max(ts))::int AS age_s FROM sq.job_heartbeat")],
          "Seconds since the Flink job last wrote a heartbeat.",
          defaults={"unit": "s", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 120}, {"color": "red", "value": 300}]}}),
    panel(5, "Kafka reachable", "stat", 20, 3, 4, 3, [target(
        "SELECT kafka_status AS kafka FROM sq.job_heartbeat ORDER BY ts DESC LIMIT 1")],
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": "green"}, "down": {"text": "DOWN", "color": "red"}}}]}),
    panel(6, "Registry", "stat", 16, 6, 8, 3, [target("SELECT registry_status AS registry FROM sq.job_heartbeat ORDER BY ts DESC LIMIT 1")],
          "disabled / up / down. When down, structural checks are skipped; statistical checks continue.",
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": "green"}, "disabled": {"text": "DISABLED", "color": "text"},
                                                                  "down": {"text": "DOWN (structural skipped)", "color": "orange"}, "unknown": {"text": "UNKNOWN", "color": "text"}}}]}),
    panel(7, "Violations per minute by check type", "timeseries", 0, 9, 24, 8, [target(
        """SELECT $__timeGroupAlias(window_end, '1m'), check_type AS metric, count(*) AS violations
           FROM sq.violations WHERE $__timeFilter(window_end) GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}, "unit": "short"}),
]
d1 = dashboard("sq-topic-health", "Stream Quality - Topic health", p1, links=NAV,
               desc="One row per topic with a RAG status per check type.")

# ----------------------------------------------------------------------------- 2. field drill-down
TOPIC_F = "topic IN (${topic:singlequote})"
FIELD_F = "field IN (${field:singlequote})"
vars2 = [query_var("topic", "Topic", "SELECT DISTINCT topic FROM sq.latest_status ORDER BY 1"),
         query_var("field", "Field", "SELECT DISTINCT field FROM sq.latest_status WHERE field <> '' AND " + TOPIC_F + " ORDER BY 1")]
thr_style = {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}}
T = '"time"'


def ts_panel(pid, title, x, y, sql_main, sql_thr=None, unit="short", desc="", w=12):
    targets = [target(sql_main, TS, "A")]
    overrides = []
    if sql_thr:
        targets.append(target(sql_thr, TS, "B"))
        overrides.append({"matcher": {"id": "byFrameRefID", "options": "B"}, "properties": [thr_style, {"id": "custom.fillOpacity", "value": 0}]})
    return panel(pid, title, "timeseries", x, y, w, 8, targets, desc,
                 defaults={"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 10, "spanNulls": False}}, overrides=overrides)


def cr(extra):
    return f"FROM sq.check_results WHERE $__timeFilter(window_start) AND {extra}"


p2 = [
    ts_panel(1, "Null rate per field", 0, 0,
             f"SELECT window_start AS {T}, field AS metric, value AS null_rate {cr(f'check_type = {chr(39)}null_rate{chr(39)} AND {TOPIC_F} AND {FIELD_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, field || ' threshold' AS metric, threshold {cr(f'check_type = {chr(39)}null_rate{chr(39)} AND {TOPIC_F} AND {FIELD_F} AND status <> {chr(39)}ok{chr(39)}')} ORDER BY 1",
             "percentunit", "Fraction of records where the field is null/missing. Dashed = threshold that was crossed (shown only while violating)."),
    ts_panel(2, "Cardinality (HLL estimate) vs baseline", 12, 0,
             f"SELECT window_start AS {T}, field AS metric, value AS distinct_estimate {cr(f'check_type = {chr(39)}cardinality{chr(39)} AND {TOPIC_F} AND {FIELD_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, field || ' baseline' AS metric, (details->>'baseline')::float8 AS baseline {cr(f'check_type = {chr(39)}cardinality{chr(39)} AND {TOPIC_F} AND {FIELD_F} AND details->>{chr(39)}baseline{chr(39)} IS NOT NULL')} ORDER BY 1",
             "short", "Approximate distinct values per window (HyperLogLog, ~1.6% error) against the mean of recent healthy windows."),
    ts_panel(3, "Freshness: p50 / p95 / p99 latency", 0, 8,
             f"SELECT * FROM (SELECT window_start AS {T}, 'p50' AS metric, (details->>'p50_ms')::float8 AS ms {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')} UNION ALL "
             f"SELECT window_start, 'p95', value {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')} UNION ALL "
             f"SELECT window_start, 'p99', (details->>'p99_ms')::float8 {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')}) u ORDER BY 1",
             None, "ms", "Processing time minus the payload timestamp. Status is judged on p95."),
    ts_panel(4, "Volume (messages/s) vs baseline", 12, 8,
             f"SELECT window_start AS {T}, topic AS metric, value AS msgs_per_s {cr(f'check_type = {chr(39)}volume{chr(39)} AND {TOPIC_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, topic || ' baseline' AS metric, (details->>'baseline_rate')::float8 AS baseline {cr(f'check_type = {chr(39)}volume{chr(39)} AND {TOPIC_F} AND details->>{chr(39)}baseline_rate{chr(39)} IS NOT NULL')} ORDER BY 1",
             "short", "Messages per second per window against the mean of recent healthy windows."),
    panel(5, "Status timeline (all checks for selected topic/fields)", "state-timeline", 0, 16, 24, 8, [target(
        f"""SELECT window_start AS {T}, check_type || CASE WHEN field = '' THEN '' ELSE '.' || field END AS metric, {STATUS_N} AS status
            {cr(f"{TOPIC_F} AND (field = '' OR {FIELD_F})")} ORDER BY 1""", TS)],
          defaults={"mappings": STATUS_MAP, "color": {"mode": "thresholds"},
                    "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 2}]}},
          options={"mergeValues": True, "showValue": "never", "rowHeight": 0.8}),
]
d2 = dashboard("sq-field-drilldown", "Stream Quality - Field drill-down", p2, vars2, NAV,
               "Per-topic, per-field time series for null rate, cardinality, freshness and volume.")

# ----------------------------------------------------------------------------- 3. violation log
vars3 = [query_var("topic", "Topic", "SELECT DISTINCT topic FROM sq.violations ORDER BY 1"),
         query_var("check", "Check", "SELECT DISTINCT check_type FROM sq.violations ORDER BY 1"),
         query_var("status", "Status", "SELECT DISTINCT status::text FROM sq.violations ORDER BY 1")]
VF = f"{TOPIC_F} AND check_type IN (${{check:singlequote}}) AND status::text IN (${{status:singlequote}})"
LOG_SQL = f"""
SELECT window_end AS {T}, topic, field, check_type, status::text AS status, round(value::numeric, 4) AS value,
       round(threshold::numeric, 4) AS threshold, details::text AS details
FROM sq.violations WHERE $__timeFilter(window_end) AND {VF} ORDER BY window_end DESC LIMIT 1000"""
status_override = {"matcher": {"id": "byName", "options": "status"}, "properties": [
    {"id": "mappings", "value": [{"type": "value", "options": {"fail": {"text": "FAIL", "color": "red", "index": 0}, "warn": {"text": "WARN", "color": "orange", "index": 1}}}]},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}
p3 = [
    panel(1, "Violation audit log", "table", 0, 0, 24, 14, [target(LOG_SQL)],
          "Every warn/fail result, newest first (max 1000 rows in the selected range). `details` holds the check-specific JSON.",
          defaults={"custom": {"filterable": True}}, overrides=[status_override, {"matcher": {"id": "byName", "options": "details"}, "properties": [{"id": "custom.width", "value": 480}]}]),
    panel(2, "Violations over time", "timeseries", 0, 14, 12, 7, [target(
        f"""SELECT $__timeGroupAlias(window_end, '1m'), status::text AS metric, count(*) AS n
            FROM sq.violations WHERE $__timeFilter(window_end) AND {VF} GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}},
          overrides=[{"matcher": {"id": "byName", "options": "fail"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
                     {"matcher": {"id": "byName", "options": "warn"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "orange"}}]}]),
    panel(3, "Top offenders", "table", 12, 14, 12, 7, [target(
        f"""SELECT topic, field, check_type, count(*) FILTER (WHERE status = 'fail') AS fails, count(*) FILTER (WHERE status = 'warn') AS warns
            FROM sq.violations WHERE $__timeFilter(window_end) AND {VF} GROUP BY topic, field, check_type ORDER BY fails DESC, warns DESC LIMIT 20""")]),
    panel(4, "Alert history", "table", 0, 21, 24, 9, [target(
        f"""SELECT fired_at AS {T}, resolved_at, alertname, severity, topic, check_type, field,
                   CASE WHEN firing THEN 'FIRING' ELSE 'resolved' END AS state, extract(epoch FROM duration)::int AS duration_s
            FROM sq.alert_history WHERE $__timeFilter(fired_at) AND (topic = '' OR {TOPIC_F}) ORDER BY fired_at DESC LIMIT 200""")],
          "Every alert Alertmanager delivered (stored by the alert-sink in sq.alert_events). One row per alert instance: when it fired, when it resolved, how long it lasted.",
          overrides=[{"matcher": {"id": "byName", "options": "state"}, "properties": [
                     {"id": "mappings", "value": [{"type": "value", "options": {"FIRING": {"text": "FIRING", "color": "red", "index": 0}, "resolved": {"text": "resolved", "color": "green", "index": 1}}}]},
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}, {"matcher": {"id": "byName", "options": "duration_s"}, "properties": [{"id": "unit", "value": "s"}]}]),
]
d3 = dashboard("sq-violations", "Stream Quality - Violation log", p3, vars3, NAV,
               "Raw audit trail of every warn/fail with filters, plus delivered alert notifications.")


# ----------------------------------------------------------------------------- 4. quality analytics
# Everything here is SQL over sq.check_results / sq.incidents(); scripts/test_analytics.py runs these exact
# strings against a seeded history and asserts the numbers.
vars4 = [query_var("topic", "Topic", "SELECT DISTINCT topic FROM sq.latest_status ORDER BY 1")]
F = f"$__timeFilter(window_start) AND {TOPIC_F}"
SCORE = "CASE status WHEN 'ok' THEN 1.0 WHEN 'warn' THEN 0.5 ELSE 0.0 END"
INC = f"sq.incidents($__timeFrom(), $__timeTo()) WHERE {TOPIC_F}"
score_thr = {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "orange", "value": 95}, {"color": "green", "value": 99}]}
pct_ok_override = {"matcher": {"id": "byName", "options": "pct_ok"}, "properties": [
    {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}}, {"id": "unit", "value": "percent"},
    {"id": "thresholds", "value": score_thr}]}

p4 = [
    panel(1, "Data-quality score", "stat", 0, 0, 4, 4, [target(
        f"SELECT round((100 * avg({SCORE}))::numeric, 2) AS score FROM sq.check_results WHERE {F}")],
          "Share of check results that were healthy: ok = 1, warn = 0.5, fail = 0, averaged over every check and window in range.",
          defaults={"unit": "percent", "decimals": 2, "thresholds": score_thr}),
    panel(2, "Availability (windows OK)", "stat", 4, 0, 4, 4, [target(
        f"SELECT round(100.0 * count(*) FILTER (WHERE status = 'ok') / NULLIF(count(*), 0), 2) AS pct_ok FROM sq.check_results WHERE {F}")],
          "Share of check results with status ok, like an SLA for data health.",
          defaults={"unit": "percent", "decimals": 2, "thresholds": score_thr}),
    panel(3, "Open now", "stat", 8, 0, 4, 4, [target(
        f"SELECT count(*) AS open_now FROM sq.latest_status WHERE status <> 'ok' AND window_end > now() - interval '5 minutes' AND {TOPIC_F}")],
          "Checks whose latest window is warn/fail.",
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 3}]}}),
    panel(4, "Incident count", "stat", 12, 0, 4, 4, [target(f"SELECT count(*) AS incidents FROM {INC}")],
          "Runs of consecutive warn/fail windows for one check, in the selected range.",
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}]}}),
    panel(5, "Longest incident", "stat", 16, 0, 4, 4, [target(f"SELECT extract(epoch FROM max(duration))::int AS seconds FROM {INC}")],
          defaults={"unit": "s"}),
    panel(6, "Avg incident duration", "stat", 20, 0, 4, 4, [target(f"SELECT extract(epoch FROM avg(duration))::int AS seconds FROM {INC}")],
          "Mean time a check stays unhealthy once it breaks (a recovery-time proxy).", defaults={"unit": "s"}),
    panel(7, "Availability by topic and check", "table", 0, 4, 12, 9, [target(
        f"""SELECT topic, check_type, round(100.0 * count(*) FILTER (WHERE status = 'ok') / count(*), 2) AS pct_ok,
                   count(*) AS windows, count(*) FILTER (WHERE status = 'warn') AS warns, count(*) FILTER (WHERE status = 'fail') AS fails
            FROM sq.check_results WHERE {F} GROUP BY 1, 2 ORDER BY pct_ok, fails DESC""")],
          "Worst first. Colour: green >= 99 %, orange >= 95 %, red below.", overrides=[pct_ok_override]),
    panel(8, "Quality score over time", "timeseries", 12, 4, 12, 9, [target(
        f"""SELECT $__timeGroupAlias(window_start, $__interval), topic AS metric, round((100 * avg({SCORE}))::numeric, 2) AS score
            FROM sq.check_results WHERE {F} GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={"unit": "percent", "min": 0, "max": 100, "custom": {"lineWidth": 2, "fillOpacity": 8}}),
    panel(9, "Incident log", "table", 0, 13, 14, 10, [target(
        f"""SELECT started AS {T}, ended, topic, check_type, field, worst_status::text AS worst, windows, fail_windows,
                   extract(epoch FROM duration)::int AS duration_s
            FROM {INC} ORDER BY started DESC LIMIT 200""")],
          "One row per incident (consecutive unhealthy windows of one check; a gap of > 3 window lengths ends an incident).",
          overrides=[{"matcher": {"id": "byName", "options": "duration_s"}, "properties": [{"id": "unit", "value": "s"}]},
                     {"matcher": {"id": "byName", "options": "worst"}, "properties": [
                         {"id": "mappings", "value": [{"type": "value", "options": {"fail": {"text": "FAIL", "color": "red", "index": 0}, "warn": {"text": "WARN", "color": "orange", "index": 1}}}]},
                         {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}]),
    panel(10, "Incidents started per hour", "timeseries", 14, 13, 10, 10, [target(
        f"""SELECT $__timeGroupAlias(started, '1h'), check_type AS metric, count(*) AS incidents FROM {INC} GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}}),
    panel(11, "Flappiest checks", "table", 0, 23, 8, 9, [target(
        f"""SELECT topic, check_type, field, count(*) FILTER (WHERE flipped) AS flips, count(*) AS windows,
                   round(100.0 * count(*) FILTER (WHERE flipped) / count(*), 1) AS flip_pct
            FROM (SELECT topic, check_type, field, ((status = 'ok') <> (lag(status) OVER w = 'ok')) AS flipped
                  FROM sq.check_results WHERE {F}
                  WINDOW w AS (PARTITION BY topic, check_type, field ORDER BY window_start)) s
            GROUP BY 1, 2, 3 HAVING count(*) FILTER (WHERE flipped) > 0 ORDER BY flips DESC LIMIT 15""")],
          "Checks that keep flipping between ok and not-ok. Often a threshold that is too tight, not a real problem."),
    panel(12, "Worst null-rate fields", "table", 8, 23, 8, 9, [target(
        f"""SELECT topic, field, round((100 * avg(value))::numeric, 2) AS avg_null_pct, round((100 * max(value))::numeric, 2) AS max_null_pct,
                   count(*) FILTER (WHERE status <> 'ok') AS bad_windows, count(*) AS windows
            FROM sq.check_results WHERE {F} AND check_type = 'null_rate' GROUP BY 1, 2 ORDER BY avg_null_pct DESC LIMIT 15""")]),
    panel(13, "Correlated failures", "table", 16, 23, 8, 9, [target(
        f"""SELECT window_start AS {T}, topic, string_agg(DISTINCT check_type, ', ' ORDER BY check_type) AS failing_checks,
                   count(DISTINCT check_type) AS n_checks
            FROM sq.check_results WHERE {F} AND status <> 'ok' GROUP BY window_start, topic
            HAVING count(DISTINCT check_type) >= 2 ORDER BY 1 DESC LIMIT 100""")],
          "Windows where two or more different checks broke on the same topic at once: usually one upstream cause."),
    ts_panel(14, "Volume: now vs same time last week (hourly avg msgs/s)", 0, 32,
             f"""SELECT date_trunc('hour', window_start) AS {T}, topic AS metric, avg(value) AS msgs_per_s
                 FROM sq.check_results WHERE check_type = 'volume' AND {F} GROUP BY 1, 2 ORDER BY 1""",
             f"""SELECT date_trunc('hour', window_start) + interval '7 days' AS {T}, topic || ' (7d ago)' AS metric, avg(value) AS msgs_per_s
                 FROM sq.check_results WHERE check_type = 'volume' AND {TOPIC_F}
                   AND window_start >= $__timeFrom()::timestamptz - interval '7 days' AND window_start <= $__timeTo()::timestamptz - interval '7 days'
                 GROUP BY 1, 2 ORDER BY 1""",
             "short", "Hourly average throughput against the same hour one week earlier (dashed). Needs a week of history.", w=24),
    panel(15, "Alert history", "table", 0, 40, 16, 9, [target(
        f"""SELECT fired_at AS {T}, resolved_at, alertname, severity, topic, check_type, field,
                   CASE WHEN firing THEN 'FIRING' ELSE 'resolved' END AS state, extract(epoch FROM duration)::int AS duration_s
            FROM sq.alert_history WHERE $__timeFilter(fired_at) AND (topic = '' OR {TOPIC_F}) ORDER BY fired_at DESC LIMIT 200""")],
          "Alerts delivered by Alertmanager (sq.alert_history): when each fired, when it resolved, how long it lasted.",
          overrides=[{"matcher": {"id": "byName", "options": "state"}, "properties": [
                     {"id": "mappings", "value": [{"type": "value", "options": {"FIRING": {"text": "FIRING", "color": "red", "index": 0}, "resolved": {"text": "resolved", "color": "green", "index": 1}}}]},
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}, {"matcher": {"id": "byName", "options": "duration_s"}, "properties": [{"id": "unit", "value": "s"}]}]),
    panel(16, "Alerts fired per hour", "timeseries", 16, 40, 8, 9, [target(
        f"""SELECT $__timeGroupAlias(fired_at, '1h'), severity AS metric, count(*) AS alerts
            FROM sq.alert_history WHERE $__timeFilter(fired_at) AND (topic = '' OR {TOPIC_F}) GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}}),
]
d4 = dashboard("sq-analytics", "Stream Quality - Quality analytics", p4, vars4, NAV,
               "Data-quality score, availability, incidents, flapping checks and week-over-week trends computed with SQL.")
d4["time"] = {"from": "now-24h", "to": "now"}
d4["refresh"] = "1m"

OUT.mkdir(exist_ok=True)
for name, d in (("topic-health", d1), ("field-drilldown", d2), ("violation-log", d3), ("quality-analytics", d4)):
    (OUT / f"{name}.json").write_text(json.dumps(d, indent=2) + "\n")
print("wrote", [p.name for p in sorted(OUT.glob("*.json"))])
