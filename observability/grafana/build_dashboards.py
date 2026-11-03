#!/usr/bin/env python3
"""Generates the three provisioned dashboards into ./dashboards/*.json (the JSON is committed; edit THIS file).
All panel SQL is also executed against PostgreSQL by scripts/test_dashboard_queries.py."""
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "sq-pg"}

# ---- one palette for everything (light theme: white cards on a lavender page, violet as the brand colour) ------------
# Categorical series colours were run through the dataviz validator (CVD separation, lightness band, chroma floor).
# Status colours are reserved: they are never used for a data series.
INK, MUTED, LINE = "#2B2350", "#6E6890", "#E8E2FB"
VIOLET, TEAL, BLUE, PINK = "#7C5CFC", "#06A59B", "#2F6FDE", "#E11D8F"
OK, WARN, FAIL = "#1FA971", "#F0A33A", "#E5484D"
OK_BG, WARN_BG, FAIL_BG = "rgba(31,169,113,0.14)", "rgba(240,163,58,0.20)", "rgba(229,72,77,0.16)"   # tinted cells; text stays dark
CHECK_COLORS = {"volume": VIOLET, "null_rate": TEAL, "cardinality": BLUE, "freshness": PINK, "structural": "#9B8AE8",
                "type_drift": "#0E7490", "schema_drift": "#B45FD0"}

CSS = """
body{background:#F5F2FF!important}
section[data-testid^="data-testid Panel header"]{background:#fff!important;border:1px solid #E8E2FB!important;border-radius:16px!important;
  box-shadow:0 1px 2px rgba(76,29,149,.05),0 10px 30px rgba(124,92,252,.08)!important;overflow:hidden}
section[data-testid^="data-testid Panel header"] h2{font-weight:600!important;letter-spacing:.01em;color:#2B2350!important}
section[data-testid="data-testid Panel header "]{background:transparent!important;border:0!important;box-shadow:none!important}
.sqhero{display:flex;align-items:center;gap:16px;padding:6px 6px 0}
.sqhero .logo{width:44px;height:44px;border-radius:14px;background:linear-gradient(135deg,#9B8AFF,#6D4AFF);display:grid;place-items:center;
  color:#fff;font-weight:700;font-size:20px;box-shadow:0 8px 20px rgba(109,74,255,.35)}
.sqhero h1{margin:0;font-size:24px;line-height:1.1;color:#2B2350;font-weight:700;letter-spacing:-.01em}
.sqhero p{margin:3px 0 0;color:#6E6890;font-size:13px}
.sqhero .key{margin-left:auto;display:flex;gap:14px;font-size:12px;color:#6E6890}
.sqhero .key i{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
a[data-testid^="data-testid Dashboard link"], .sq-nav a{border-radius:999px!important}
footer, [class*="footer"]{display:none!important}
[data-testid="data-testid navigation mega-menu"]{display:none!important}
main[class*="page-container"]{padding-left:0!important}
section[data-testid^="data-testid Panel header"] h2{font-size:13px!important}
header, [data-testid="data-testid Nav toolbar"], [data-testid="data-testid dashboard controls"]{background:#F5F2FF!important;border-color:#E8E2FB!important}
[data-testid="data-testid Dashboard link container"] a, [data-testid="data-testid Dashboard link"]{border-radius:999px!important;background:#fff!important;
  border:1px solid #D9CFFB!important;color:#5B3FD1!important}
section[data-testid^="data-testid Panel header"] a{color:#6D4AFF!important}
section[data-testid^="data-testid Panel header"] [role="columnheader"]{color:#6E6890!important;font-weight:600;text-transform:uppercase;font-size:11px;letter-spacing:.06em;background:#FAF8FF!important}
"""
HERO = """<style>%s</style><div class="sqhero"><div class="logo">SQ</div><div><h1>Stream Quality</h1><p>%s</p></div>
<div class="key"><span><i style="background:#1FA971"></i>OK</span><span><i style="background:#F0A33A"></i>Warn</span><span><i style="background:#E5484D"></i>Fail</span></div></div>"""
OUT = Path(__file__).parent / "dashboards"
TABLE, TS = "table", "time_series"
STATUS_MAP = [{"type": "value", "options": {
    "0": {"text": "OK", "color": OK_BG, "index": 0},
    "1": {"text": "WARN", "color": WARN_BG, "index": 1},
    "2": {"text": "FAIL", "color": FAIL_BG, "index": 2}}},
    {"type": "special", "options": {"match": "null", "result": {"text": "no data", "color": "#F1EEFB", "index": 3}}}]
STATUS_MAP_TEXT = [{"type": "value", "options": {   # same states for stat panels / text cells: solid colours
    "0": {"text": "OK", "color": OK, "index": 0}, "1": {"text": "WARN", "color": WARN, "index": 1}, "2": {"text": "FAIL", "color": FAIL, "index": 2}}}]
STATUS_N = "CASE status WHEN 'ok' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END"


def by_name(name, color, extra=()):
    return {"matcher": {"id": "byName", "options": name}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}, *extra]}


def check_colors():
    return [by_name(c, col) for c, col in CHECK_COLORS.items()]


def status_colors():
    return [by_name("fail", FAIL), by_name("warn", WARN)]


DASH = {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 6]}}
REFERENCE_LINES = [   # baselines and thresholds are reference lines: dashed, no fill, recessive; a crossed threshold is red
    {"matcher": {"id": "byRegexp", "options": ".*(baseline|7d ago)$"}, "properties": [
        {"id": "color", "value": {"mode": "fixed", "fixedColor": "#B7ADDF"}}, DASH, {"id": "custom.fillOpacity", "value": 0}]},
    {"matcher": {"id": "byRegexp", "options": ".* threshold$"}, "properties": [
        {"id": "color", "value": {"mode": "fixed", "fixedColor": FAIL}}, DASH, {"id": "custom.fillOpacity", "value": 0}]}]
LINE_DEFAULTS = {"color": {"mode": "shades", "fixedColor": VIOLET},
                 "custom": {"lineWidth": 2, "fillOpacity": 14, "gradientMode": "opacity", "showPoints": "never", "spanNulls": False}}
BAR_DEFAULTS = {"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}, "fillOpacity": 85, "lineWidth": 0, "barAlignment": 0}}


def target(sql, fmt=TABLE, ref="A"):
    # Time-series format conventions of the Grafana Postgres datasource: a "time" column, an optional "metric"
    # column (series name) and numeric value columns.
    return {"refId": ref, "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": " ".join(sql.split())}


def panel(pid, title, ptype, x, y, w, h, targets, desc="", defaults=None, overrides=None, options=None, ds=DS):
    if ptype == "stat":
        options = {"colorMode": "value", "graphMode": "none", "textMode": "auto", "justifyMode": "center",
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False}, **(options or {})}
        defaults = {"color": {"mode": "thresholds"}, **(defaults or {})}
    return {"id": pid, "title": title, "type": ptype, "description": desc, "datasource": ds,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []}, "options": options or {}}


def hero(subtitle):
    return {"id": 100, "title": "", "type": "text", "gridPos": {"x": 0, "y": 0, "w": 24, "h": 2}, "targets": [], "transparent": True,
            "options": {"mode": "html", "content": HERO % (CSS, subtitle)}, "fieldConfig": {"defaults": {}, "overrides": []}}


def dashboard(uid, title, panels, variables=None, links=None, desc=""):
    panels = [hero(desc)] + [dict(p, gridPos=dict(p["gridPos"], y=p["gridPos"]["y"] + 2)) for p in panels]
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
rag_overrides.append({"matcher": {"id": "byName", "options": "last_window"}, "properties": [{"id": "unit", "value": "time:HH:mm:ss"}, {"id": "custom.width", "value": 110}]})
rag_overrides.append({"matcher": {"id": "byName", "options": "topic"}, "properties": [
    {"id": "custom.width", "value": 270}, {"id": "custom.align", "value": "left"},
    {"id": "links", "value": [{"title": "Drill down", "url": "/d/sq-field-drilldown?var-topic=${__value.raw}&${__url_time_range}"}]}]})

p1 = [
    panel(1, "Topic health (latest window per check)", "table", 0, 3, 24, 7, [target(RAG_SQL)],
          "Worst status across fields for each check type, from the latest window. 'no data' = no result in the last 5 minutes "
          "(check not configured for the topic, or the topic/monitor is silent). Click a topic to drill down.",
          defaults={"custom": {"align": "center"}}, overrides=rag_overrides),
    panel(2, "Topics failing", "stat", 0, 0, 5, 3, [target(
        "SELECT count(DISTINCT topic) AS failing FROM sq.latest_status WHERE status = 'fail' AND window_end > now() - interval '5 minutes'")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": VIOLET, "value": None}, {"color": FAIL, "value": 1}]}}),
    panel(3, "Topics warning", "stat", 5, 0, 5, 3, [target(
        "SELECT count(DISTINCT topic) AS warning FROM sq.latest_status WHERE status = 'warn' AND window_end > now() - interval '5 minutes'")],
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": VIOLET, "value": None}, {"color": WARN, "value": 1}]}}),
    panel(4, "Monitor heartbeat age", "stat", 10, 0, 5, 3, [target(
        "SELECT extract(epoch FROM now() - max(ts))::int AS age_s FROM sq.job_heartbeat")],
          "Seconds since the Flink job last wrote a heartbeat.",
          defaults={"unit": "s", "thresholds": {"mode": "absolute", "steps": [{"color": OK, "value": None}, {"color": WARN, "value": 120}, {"color": FAIL, "value": 300}]}}),
    panel(5, "Kafka reachable", "stat", 15, 0, 4, 3, [target(
        "SELECT kafka_status AS kafka FROM sq.job_heartbeat ORDER BY ts DESC LIMIT 1")],
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": OK}, "down": {"text": "DOWN", "color": FAIL}}}]}),
    panel(6, "Registry", "stat", 19, 0, 5, 3, [target("SELECT registry_status AS registry FROM sq.job_heartbeat ORDER BY ts DESC LIMIT 1")],
          "disabled / up / down. When down, structural checks are skipped; statistical checks continue.",
          defaults={"mappings": [{"type": "value", "options": {"up": {"text": "UP", "color": OK}, "disabled": {"text": "DISABLED", "color": MUTED},
                                                                  "down": {"text": "DOWN (structural skipped)", "color": WARN}, "unknown": {"text": "UNKNOWN", "color": MUTED}}}]}),
    panel(7, "Violations per minute by check type", "timeseries", 0, 10, 24, 8, [target(
        """SELECT $__timeGroupAlias(window_end, '1m'), check_type AS metric, count(*) AS violations
           FROM sq.violations WHERE $__timeFilter(window_end) GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults={**BAR_DEFAULTS, "unit": "short"}, overrides=check_colors()),
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


def ts_panel(pid, title, x, y, sql_main, sql_thr=None, unit="short", desc="", w=12, extra=()):
    targets = [target(sql_main, TS, "A")]
    if sql_thr:
        targets.append(target(sql_thr, TS, "B"))
    return panel(pid, title, "timeseries", x, y, w, 8, targets, desc,
                 defaults={**LINE_DEFAULTS, "unit": unit}, overrides=[*REFERENCE_LINES, *extra])


def cr(extra):
    return f"FROM sq.check_results WHERE $__timeFilter(window_start) AND {extra}"


p2 = [
    ts_panel(1, "Null rate per field", 0, 0,
             f"SELECT window_start AS {T}, field AS metric, value AS null_rate {cr(f'check_type = {chr(39)}null_rate{chr(39)} AND {TOPIC_F} AND {FIELD_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, field || ' threshold' AS metric, threshold {cr(f'check_type = {chr(39)}null_rate{chr(39)} AND {TOPIC_F} AND {FIELD_F} AND status <> {chr(39)}ok{chr(39)}')} ORDER BY 1",
             "percentunit", "Fraction of records where the field is null/missing. Dashed = threshold that was crossed (shown only while violating)."),
    ts_panel(2, "Cardinality (HLL estimate) vs baseline", 12, 0,
             f"SELECT window_start AS {T}, field AS metric, value AS distinct_estimate {cr(f'check_type = {chr(39)}cardinality{chr(39)} AND {TOPIC_F} AND {FIELD_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, field || ' baseline' AS metric, baseline {cr(f'check_type = {chr(39)}cardinality{chr(39)} AND {TOPIC_F} AND {FIELD_F} AND baseline IS NOT NULL')} ORDER BY 1",
             "short", "Approximate distinct values per window (HyperLogLog, ~1.6% error) against the mean of recent healthy windows."),
    ts_panel(3, "Freshness: p50 / p95 / p99 latency", 0, 8,
             f"SELECT * FROM (SELECT window_start AS {T}, 'p50' AS metric, p50_ms AS ms {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')} UNION ALL "
             f"SELECT window_start, 'p95', value {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')} UNION ALL "
             f"SELECT window_start, 'p99', p99_ms {cr(f'check_type = {chr(39)}freshness{chr(39)} AND {TOPIC_F}')}) u ORDER BY 1",
             None, "ms", "Processing time minus the payload timestamp. Status is judged on p95.",
             extra=[by_name("p50", TEAL), by_name("p95", VIOLET), by_name("p99", PINK)]),
    ts_panel(4, "Volume (messages/s) vs baseline", 12, 8,
             f"SELECT window_start AS {T}, topic AS metric, value AS msgs_per_s {cr(f'check_type = {chr(39)}volume{chr(39)} AND {TOPIC_F}')} ORDER BY 1",
             f"SELECT window_start AS {T}, topic || ' baseline' AS metric, baseline {cr(f'check_type = {chr(39)}volume{chr(39)} AND {TOPIC_F} AND baseline IS NOT NULL')} ORDER BY 1",
             "short", "Messages per second per window against the mean of recent healthy windows."),
    panel(5, "Status timeline (all checks for selected topic/fields)", "state-timeline", 0, 16, 24, 8, [target(
        f"""SELECT window_start AS {T}, check_type || CASE WHEN field = '' THEN '' ELSE '.' || field END AS metric, {STATUS_N} AS status
            {cr(f"{TOPIC_F} AND (field = '' OR {FIELD_F})")} ORDER BY 1""", TS)],
          defaults={"mappings": STATUS_MAP_TEXT, "color": {"mode": "thresholds"},
                    "thresholds": {"mode": "absolute", "steps": [{"color": OK, "value": None}, {"color": WARN, "value": 1}, {"color": FAIL, "value": 2}]}},
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
    {"id": "mappings", "value": [{"type": "value", "options": {"fail": {"text": "FAIL", "color": FAIL, "index": 0}, "warn": {"text": "WARN", "color": WARN, "index": 1}}}]},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}
p3 = [
    panel(1, "Violation audit log", "table", 0, 0, 24, 14, [target(LOG_SQL)],
          "Every warn/fail result, newest first (max 1000 rows in the selected range). `details` holds the check-specific JSON.",
          defaults={"custom": {"filterable": True}}, overrides=[status_override, {"matcher": {"id": "byName", "options": "details"}, "properties": [{"id": "custom.width", "value": 480}]}]),
    panel(2, "Violations over time", "timeseries", 0, 14, 12, 7, [target(
        f"""SELECT $__timeGroupAlias(window_end, '1m'), status::text AS metric, count(*) AS n
            FROM sq.violations WHERE $__timeFilter(window_end) AND {VF} GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults=BAR_DEFAULTS, overrides=status_colors()),
    panel(3, "Top offenders", "table", 12, 14, 12, 7, [target(
        f"""SELECT topic, field, check_type, count(*) FILTER (WHERE status = 'fail') AS fails, count(*) FILTER (WHERE status = 'warn') AS warns
            FROM sq.violations WHERE $__timeFilter(window_end) AND {VF} GROUP BY topic, field, check_type ORDER BY fails DESC, warns DESC LIMIT 20""")]),
    panel(4, "Alert history", "table", 0, 21, 24, 9, [target(
        f"""SELECT fired_at AS {T}, resolved_at, alertname, severity, topic, check_type, field,
                   CASE WHEN firing THEN 'FIRING' ELSE 'resolved' END AS state, extract(epoch FROM duration)::int AS duration_s
            FROM sq.alert_history WHERE $__timeFilter(fired_at) AND (topic = '' OR {TOPIC_F}) ORDER BY fired_at DESC LIMIT 200""")],
          "Every alert Alertmanager delivered (stored by the alert-sink in sq.alert_events). One row per alert instance: when it fired, when it resolved, how long it lasted.",
          overrides=[{"matcher": {"id": "byName", "options": "state"}, "properties": [
                     {"id": "mappings", "value": [{"type": "value", "options": {"FIRING": {"text": "FIRING", "color": FAIL, "index": 0}, "resolved": {"text": "resolved", "color": OK, "index": 1}}}]},
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
score_thr = {"mode": "absolute", "steps": [{"color": FAIL, "value": None}, {"color": WARN, "value": 95}, {"color": OK, "value": 99}]}
pct_ok_override = {"matcher": {"id": "byName", "options": "pct_ok"}, "properties": [
    {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic", "valueDisplayMode": "text"}}, {"id": "unit", "value": "percent"},
    {"id": "min", "value": 80}, {"id": "max", "value": 100}, {"id": "color", "value": {"mode": "thresholds"}},
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
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": VIOLET, "value": None}, {"color": WARN, "value": 1}, {"color": FAIL, "value": 3}]}}),
    panel(4, "Incident count", "stat", 12, 0, 4, 4, [target(f"SELECT count(*) AS incidents FROM {INC}")],
          "Runs of consecutive warn/fail windows for one check, in the selected range.",
          defaults={"thresholds": {"mode": "absolute", "steps": [{"color": OK, "value": None}, {"color": WARN, "value": 1}]}}),
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
          defaults={**LINE_DEFAULTS, "unit": "percent", "min": 0, "max": 100}),
    panel(9, "Incident log", "table", 0, 13, 14, 10, [target(
        f"""SELECT started AS {T}, ended, topic, check_type, field, worst_status::text AS worst, windows, fail_windows,
                   extract(epoch FROM duration)::int AS duration_s
            FROM {INC} ORDER BY started DESC LIMIT 200""")],
          "One row per incident (consecutive unhealthy windows of one check; a gap of > 3 window lengths ends an incident).",
          overrides=[{"matcher": {"id": "byName", "options": "duration_s"}, "properties": [{"id": "unit", "value": "s"}]},
                     {"matcher": {"id": "byName", "options": "worst"}, "properties": [
                         {"id": "mappings", "value": [{"type": "value", "options": {"fail": {"text": "FAIL", "color": FAIL, "index": 0}, "warn": {"text": "WARN", "color": WARN, "index": 1}}}]},
                         {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}]),
    panel(10, "Incidents started per hour", "timeseries", 14, 13, 10, 10, [target(
        f"""SELECT $__timeGroupAlias(started, '1h'), check_type AS metric, count(*) AS incidents FROM {INC} GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults=BAR_DEFAULTS, overrides=check_colors()),
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
                     {"id": "mappings", "value": [{"type": "value", "options": {"FIRING": {"text": "FIRING", "color": FAIL, "index": 0}, "resolved": {"text": "resolved", "color": OK, "index": 1}}}]},
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}, {"matcher": {"id": "byName", "options": "duration_s"}, "properties": [{"id": "unit", "value": "s"}]}]),
    panel(16, "Alerts fired per hour", "timeseries", 16, 40, 8, 9, [target(
        f"""SELECT $__timeGroupAlias(fired_at, '1h'), severity AS metric, count(*) AS alerts
            FROM sq.alert_history WHERE $__timeFilter(fired_at) AND (topic = '' OR {TOPIC_F}) GROUP BY 1, 2 ORDER BY 1""", TS)],
          defaults=BAR_DEFAULTS, overrides=[by_name("critical", FAIL), by_name("warning", WARN)]),
]
d4 = dashboard("sq-analytics", "Stream Quality - Quality analytics", p4, vars4, NAV,
               "Data-quality score, availability, incidents, flapping checks and week-over-week trends computed with SQL.")
d4["time"] = {"from": "now-24h", "to": "now"}
d4["refresh"] = "1m"

OUT.mkdir(exist_ok=True)
for name, d in (("topic-health", d1), ("field-drilldown", d2), ("violation-log", d3), ("quality-analytics", d4)):
    (OUT / f"{name}.json").write_text(json.dumps(d, indent=2) + "\n")
print("wrote", [p.name for p in sorted(OUT.glob("*.json"))])
