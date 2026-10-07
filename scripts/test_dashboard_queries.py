#!/usr/bin/env python3
"""Executes every SQL query in the provisioned Grafana dashboards against a live Postgres (as the read-only user),
with Grafana macros/variables substituted. Catches SQL/schema drift without needing Grafana.
Usage: test_dashboard_queries.py [--strict]     (connection via the E2E_PG_* environment, see pgutil.py)
--strict: a query returning 0 rows is a failure (use after the e2e run, when all tables have data)."""
import glob
import json
import re
import sys

import pgutil

STRICT = "--strict" in sys.argv
VARS = {"topic": "'orders','payments'", "field": "'customer_id','order_id','payment_id'",
        "check": "'volume','null_rate','cardinality','freshness','structural'", "status": "'warn','fail'"}


def subst(sql):
    # Grafana's Postgres macros, expanded the way the datasource would (time range = last day + 1h)
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 BETWEEN now() - interval '1 day' AND now() + interval '1 hour'", sql)
    sql = re.sub(r"\$__timeGroupAlias\((\w+),\s*'(\d+)m'\)", r'''date_bin('\2 minutes', \1, TIMESTAMPTZ 'epoch') AS "time"''', sql)
    sql = re.sub(r"\$\{(\w+):singlequote\}", lambda m: VARS[m.group(1)], sql)
    assert "$__" not in sql and "${" not in sql, f"unsubstituted macro in: {sql[:120]}"
    return sql


bad = 0
for f in sorted(glob.glob("observability/grafana/dashboards/*.json")):
    d = json.load(open(f))
    queries = [(f"var:{v['name']}", v["query"]) for v in d["templating"]["list"]]
    for p in d["panels"]:
        for t in p["targets"]:
            if "rawSql" in t:
                queries.append((f"panel {p['id']} '{p['title']}' [{t['refId']}]", t["rawSql"]))
    print(f"\n{d['title']}  ({d['uid']})")
    for name, sql in queries:
        try:
            rows = pgutil.query(subst(sql), user="sq_reader", password="sq_reader_pw")
            if STRICT and not rows and "structural" not in sql:
                raise RuntimeError("0 rows (strict mode)")
            cols = list(rows[0].keys()) if rows else "(no rows)"
            print(f"  ok   {len(rows):>4} rows  {name}  cols={cols}")
            # Grafana time-series contract: first column must be named "time" for time_series targets
            if any(t.get("format") == "time_series" and t.get("rawSql") == sql for p in d["panels"] for t in p["targets"]) and rows:
                assert list(rows[0].keys())[0] == "time", f"first column of a time_series query must be 'time', got {list(rows[0].keys())[0]}"
        except Exception as e:
            bad += 1
            print(f"  FAIL {name}\n       {e}")
print("\nALL QUERIES OK" if not bad else f"\n{bad} QUERY FAILURES")
sys.exit(1 if bad else 0)
