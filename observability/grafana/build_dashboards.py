#!/usr/bin/env python3
"""Generates the three provisioned dashboards into ./dashboards/*.json (the JSON is committed; edit THIS file).
All panel SQL is also executed against ClickHouse by scripts/test_dashboard_queries.py."""
import json
from pathlib import Path

DS = {"type": "grafana-clickhouse-datasource", "uid": "sq-ch"}
LOKI = {"type": "loki", "uid": "sq-loki"}
OUT = Path(__file__).parent / "dashboards"
TABLE, TS = 1, 0
STATUS_MAP = [{"type": "value", "options": {
    "0": {"text": "OK", "color": "green", "index": 0},
    "1": {"text": "WARN", "color": "orange", "index": 1},
    "2": {"text": "FAIL", "color": "red", "index": 2}}},
    {"type": "special", "options": {"match": "null", "result": {"text": "no data", "color": "text", "index": 3}}}]


def target(sql, fmt=TABLE, ref="A"):
    return {"refId": ref, "datasource": DS, "editorType": "sql", "format": fmt,
            "queryType": "table" if fmt == TABLE else "timeseries", "rawSql": " ".join(sql.split())}


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


def query_var(name, label, sql, multi=True, depends=""):
    return {"name": name, "label": label, "type": "query", "datasource": DS,
            "query": {"rawSql": " ".join(sql.split()), "editorType": "sql", "format": TABLE},
            "refresh": 2, "includeAll": True, "multi": multi, "allValue": None,
            "current": {"text": "All", "value": "$__all"}, "sort": 1}


NAV = [{"title": "Topic health", "type": "link", "url": "/d/sq-topic-health", "icon": "dashboard"},
       {"title": "Field drill-down", "type": "link", "url": "/d/sq-field-drilldown", "icon": "dashboard"},
       {"title": "Violation log", "type": "link", "url": "/d/sq-violations", "icon": "bolt"}]

# ----------------------------------------------------------------------------- 1. topic health
checks = ["volume", "null_rate", "cardinality", "freshness", "structural"]
rag_cols = ",\n".join(
    f"if(countIf(check_type = '{c}') = 0, NULL, toUInt8(maxIf(status_n, check_type = '{c}'))) AS {c}" for c in checks)
RAG_SQL = f"""
SELECT topic, {rag_cols}, max(window_end) AS last_window
FROM (SELECT topic, check_type, toUInt8(status) AS status_n, window_end
      FROM sq.latest_status FINAL WHERE window_end > now64(3) - INTERVAL 5 MINUTE)
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
        "SELECT countDistinct(topic) AS failing FROM sq.latest_status FINAL WHERE status = 'fail' AND window_end > now64(3) - INTERVAL 5 MINUTE")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}}),
    panel(3, "Topics warning", "stat", 20, 0, 4, 3, [target(
        "SELECT countDistinct(topic) AS warning FROM sq.latest_status FINAL WHERE status = 'warn' AND window_end > now64(3) - INTERVAL 5 MINUTE")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}]}}),
    panel(4, "Monitor heartbeat age (s)", "stat", 16, 3, 4, 3, [target(
        "SELECT dateDiff('second', max(ts), now64(3)) AS age_s FROM sq.job_heartbeat")],
          "Seconds since the Flink job last wrote a heartbeat.",
          defaults={"unit": "s", "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 120}, {"color": "red", "value": 300}]}}),
    panel(5, "Kafka reachable", "stat", 20, 3, 4, 3, [target(
        "SELECT argMax(kafka_status, ts) AS kafka FROM sq.job_heartbeat")],
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": "green"}, "down": {"text": "DOWN", "color": "red"}}}]}),
    panel(6, "Registry", "stat", 16, 6, 8, 3, [target("SELECT argMax(registry_status, ts) AS registry FROM sq.job_heartbeat")],
          "disabled / up / down. When down, structural checks are skipped; statistical checks continue.",
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": "green"}, "disabled": {"text": "DISABLED", "color": "text"},
                                                                  "down": {"text": "DOWN (structural skipped)", "color": "orange"}, "unknown": {"text": "UNKNOWN", "color": "text"}}}]}),
    panel(7, "Violations per minute by check type", "timeseries", 0, 9, 24, 8, [target(
        """SELECT toStartOfInterval(window_end, INTERVAL 1 MINUTE) AS time, check_type, count() AS violations
           FROM sq.violations FINAL WHERE $__timeFilter(window_end) GROUP BY time, check_type ORDER BY time""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}, "unit": "short"}),
]
d1 = dashboard("sq-topic-health", "Stream Quality - Topic health", p1, links=NAV,
               desc="One row per topic with a RAG status per check type.")

# ----------------------------------------------------------------------------- 2. field drill-down
TOPIC_F = "topic IN (${topic:singlequote})"
FIELD_F = "field IN (${field:singlequote})"
vars2 = [query_var("topic", "Topic", "SELECT DISTINCT topic FROM sq.latest_status ORDER BY topic"),
         query_var("field", "Field", "SELECT DISTINCT field FROM sq.latest_status WHERE field != '' AND " + TOPIC_F + " ORDER BY field")]
thr_style = {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}}


def ts_panel(pid, title, x, y, sql_main, sql_thr=None, unit="short", desc="", w=12):
    targets = [target(sql_main, TS, "A")]
    overrides = []
    if sql_thr:
        targets.append(target(sql_thr, TS, "B"))
        overrides.append({"matcher": {"id": "byFrameRefID", "options": "B"}, "properties": [thr_style, {"id": "custom.fillOpacity", "value": 0}]})
    return panel(pid, title, "timeseries", x, y, w, 8, targets, desc,
                 defaults={"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 10, "spanNulls": False}}, overrides=overrides)


p2 = [
    ts_panel(1, "Null rate per field", 0, 0,
             f"SELECT window_start AS time, field, value AS null_rate FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'null_rate' AND {TOPIC_F} AND {FIELD_F} ORDER BY time",
             f"SELECT window_start AS time, concat(field, ' threshold') AS series, threshold FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'null_rate' AND {TOPIC_F} AND {FIELD_F} AND status != 'ok' ORDER BY time",
             "percentunit", "Fraction of records where the field is null/missing. Dashed = threshold that was crossed (shown only while violating)."),
    ts_panel(2, "Cardinality (HLL estimate) vs baseline", 12, 0,
             f"SELECT window_start AS time, field, value AS distinct_estimate FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'cardinality' AND {TOPIC_F} AND {FIELD_F} ORDER BY time",
             f"SELECT window_start AS time, concat(field, ' baseline') AS series, JSONExtractFloat(details, 'baseline') AS baseline FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'cardinality' AND {TOPIC_F} AND {FIELD_F} AND JSONHas(details, 'baseline') AND JSONType(details, 'baseline') != 'Null' ORDER BY time",
             "short", "Approximate distinct values per window (HyperLogLog, ~1.6% error) against the mean of recent windows."),
    ts_panel(3, "Freshness: p50 / p95 / p99 latency", 0, 8,
             f"SELECT * FROM (SELECT window_start AS time, 'p50' AS series, JSONExtractFloat(details, 'p50_ms') AS ms FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'freshness' AND {TOPIC_F} UNION ALL "
             f"SELECT window_start, 'p95', value FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'freshness' AND {TOPIC_F} UNION ALL "
             f"SELECT window_start, 'p99', JSONExtractFloat(details, 'p99_ms') FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'freshness' AND {TOPIC_F}) ORDER BY time",
             None, "ms", "Processing time minus the payload timestamp. Status is judged on p95."),
    ts_panel(4, "Volume (messages/s) vs baseline", 12, 8,
             f"SELECT window_start AS time, topic, value AS msgs_per_s FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'volume' AND {TOPIC_F} ORDER BY time",
             f"SELECT window_start AS time, concat(topic, ' baseline') AS series, JSONExtractFloat(details, 'baseline_rate') AS baseline FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND check_type = 'volume' AND {TOPIC_F} AND JSONType(details, 'baseline_rate') != 'Null' ORDER BY time",
             "short", "Messages per second per window against the mean of recent windows."),
    panel(5, "Status timeline (all checks for selected topic/fields)", "state-timeline", 0, 16, 24, 8, [target(
        f"""SELECT window_start AS time, concat(check_type, if(field = '', '', concat('.', field))) AS series, toUInt8(status) AS status
            FROM sq.check_results FINAL WHERE $__timeFilter(window_start) AND {TOPIC_F} AND (field = '' OR {FIELD_F}) ORDER BY time""", TS)],
          defaults={"mappings": STATUS_MAP, "color": {"mode": "thresholds"},
                    "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 2}]}},
          options={"mergeValues": True, "showValue": "never", "rowHeight": 0.8}),
]
d2 = dashboard("sq-field-drilldown", "Stream Quality - Field drill-down", p2, vars2, NAV,
               "Per-topic, per-field time series for null rate, cardinality, freshness and volume.")

# ----------------------------------------------------------------------------- 3. violation log
vars3 = [query_var("topic", "Topic", "SELECT DISTINCT topic FROM sq.violations ORDER BY topic"),
         query_var("check", "Check", "SELECT DISTINCT check_type FROM sq.violations ORDER BY check_type"),
         query_var("status", "Status", "SELECT DISTINCT toString(status) FROM sq.violations ORDER BY 1")]
VF = f"{TOPIC_F} AND check_type IN (${{check:singlequote}}) AND toString(status) IN (${{status:singlequote}})"
LOG_SQL = f"""
SELECT window_end AS time, topic, field, check_type, toString(status) AS status, round(value, 4) AS value, round(threshold, 4) AS threshold, details
FROM sq.violations FINAL WHERE $__timeFilter(window_end) AND {VF} ORDER BY window_end DESC LIMIT 1000"""
status_override = {"matcher": {"id": "byName", "options": "status"}, "properties": [
    {"id": "mappings", "value": [{"type": "value", "options": {"fail": {"text": "FAIL", "color": "red", "index": 0}, "warn": {"text": "WARN", "color": "orange", "index": 1}}}]},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}
p3 = [
    panel(1, "Violation audit log", "table", 0, 0, 24, 14, [target(LOG_SQL)],
          "Every warn/fail result, newest first (max 1000 rows in the selected range). `details` holds the check-specific JSON.",
          defaults={"custom": {"filterable": True}}, overrides=[status_override, {"matcher": {"id": "byName", "options": "details"}, "properties": [{"id": "custom.width", "value": 480}]}]),
    panel(2, "Violations over time", "timeseries", 0, 14, 12, 7, [target(
        f"""SELECT toStartOfInterval(window_end, INTERVAL 1 MINUTE) AS time, toString(status) AS status, count() AS n
            FROM sq.violations FINAL WHERE $__timeFilter(window_end) AND {VF} GROUP BY time, status ORDER BY time""", TS)],
          defaults={"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 70}},
          overrides=[{"matcher": {"id": "byName", "options": "fail"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
                     {"matcher": {"id": "byName", "options": "warn"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "orange"}}]}]),
    panel(3, "Top offenders", "table", 12, 14, 12, 7, [target(
        f"""SELECT topic, field, check_type, countIf(status = 'fail') AS fails, countIf(status = 'warn') AS warns
            FROM sq.violations FINAL WHERE $__timeFilter(window_end) AND {VF} GROUP BY topic, field, check_type ORDER BY fails DESC, warns DESC LIMIT 20""")]),
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
