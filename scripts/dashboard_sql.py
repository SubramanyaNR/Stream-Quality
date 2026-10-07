"""Loads the SQL shipped in the Grafana dashboards and expands Grafana's Postgres macros the way the datasource
would, so tests execute exactly what users get. Time range used for expansion: [now - 1 day, now + 1 hour]."""
import glob
import json
import re

VARS = {"topic": "'orders','payments'", "field": "'customer_id','order_id','payment_id'",
        "check": "'volume','null_rate','cardinality','freshness','structural'", "status": "'warn','fail'"}
FROM = "(now() - interval '1 day')"
TO = "(now() + interval '1 hour')"
INTERVAL = "5 minutes"


def subst(sql, variables=None):
    v = variables or VARS
    sql = re.sub(r"\$__timeFilter\((\w+)\)", rf"\1 BETWEEN {FROM} AND {TO}", sql)
    sql = re.sub(r"\$__timeFrom\(\)", FROM, sql)
    sql = re.sub(r"\$__timeTo\(\)", TO, sql)
    # Real Grafana expands this to epoch arithmetic; date_bin is the same grouping with a timestamp result.
    sql = re.sub(r"\$__timeGroupAlias\((\w+),\s*\$__interval\)", rf'''date_bin('{INTERVAL}', \1, TIMESTAMPTZ 'epoch') AS "time"''', sql)
    sql = re.sub(r"\$__timeGroupAlias\((\w+),\s*'(\d+)([mh])'\)",
                 lambda m: f'''date_bin('{m.group(2)} {"minutes" if m.group(3) == "m" else "hours"}', {m.group(1)}, TIMESTAMPTZ 'epoch') AS "time"''', sql)
    sql = re.sub(r"\$\{(\w+):singlequote\}", lambda m: v[m.group(1)], sql)
    assert "$__" not in sql and "${" not in sql, f"unsubstituted macro in: {sql[:140]}"
    return sql


def load():
    """[(dashboard_json, [(label, panel_title_or_var, refId_or_None, raw_sql, format)])]"""
    out = []
    for f in sorted(glob.glob("observability/grafana/dashboards/*.json")):
        d = json.load(open(f))
        qs = [(f"var:{x['name']}", x["name"], None, x["query"], "table") for x in d["templating"]["list"]]
        for p in d["panels"]:
            for t in p["targets"]:
                if "rawSql" in t:
                    qs.append((f"panel {p['id']} '{p['title']}' [{t['refId']}]", p["title"], t["refId"], t["rawSql"], t.get("format", "table")))
        out.append((d, qs))
    return out


def find(uid, title, ref="A"):
    """Raw SQL of one panel target, by dashboard uid + panel title + refId."""
    for d, qs in load():
        if d["uid"] == uid:
            for _label, t, r, sql, _fmt in qs:
                if t == title and r == ref:
                    return sql
    raise KeyError(f"{uid} / {title} / {ref}")
