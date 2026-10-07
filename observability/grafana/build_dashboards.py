#!/usr/bin/env python3
"""Generates the three provisioned dashboards into ./dashboards/*.json (the JSON is committed; edit THIS file).
All panel SQL is also executed against PostgreSQL by scripts/test_dashboard_queries.py."""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "sq-pg"}
LOKI = {"type": "loki", "uid": "sq-loki"}
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
       {"title": "Violation log", "type": "link", "url": "/d/sq-violations", "icon": "bolt"}]

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
    panel(4, "Alert notifications (Alertmanager webhook, via Loki)", "logs", 0, 21, 24, 8,
          [{"refId": "A", "datasource": LOKI, "expr": '{service="alert-sink"} | json', "queryType": "range"}],
          options={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"}, ds=LOKI),
]
d3 = dashboard("sq-violations", "Stream Quality - Violation log", p3, vars3, NAV,
               "Raw audit trail of every warn/fail with filters, plus delivered alert notifications.")

OUT.mkdir(exist_ok=True)
for name, d in (("topic-health", d1), ("field-drilldown", d2), ("violation-log", d3)):
    (OUT / f"{name}.json").write_text(json.dumps(d, indent=2) + "\n")
print("wrote", [p.name for p in sorted(OUT.glob("*.json"))])
