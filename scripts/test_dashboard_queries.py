#!/usr/bin/env python3
"""Executes every SQL query in the provisioned Grafana dashboards against a live ClickHouse (as the read-only
user), with Grafana macros/variables substituted. Catches SQL/schema drift without needing Grafana.
Usage: test_dashboard_queries.py [ch_url] [user:pw] [--strict]
--strict: a query returning 0 rows is a failure (use after the e2e run, when all tables have data).
Beware: a ClickHouse alias shadows the column of the same name inside WHERE - this mode catches that class of bug."""
import glob
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

STRICT = '--strict' in sys.argv
sys.argv = [a for a in sys.argv if a != '--strict']
url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8123"
user, pw = (sys.argv[2] if len(sys.argv) > 2 else "sq_reader:sq_reader_pw").split(":", 1)
VARS = {"topic": "'orders','payments'", "field": "'customer_id','order_id','payment_id'",
        "check": "'volume','null_rate','cardinality','freshness','structural'", "status": "'warn','fail'"}


def subst(sql):
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 >= now64(3) - INTERVAL 1 DAY AND \1 <= now64(3) + INTERVAL 1 HOUR", sql)
    sql = re.sub(r"\$\{(\w+):singlequote\}", lambda m: VARS[m.group(1)], sql)
    assert "$__" not in sql and "${" not in sql, f"unsubstituted macro in: {sql[:120]}"
    return sql


def run(sql):
    req = urllib.request.Request(url + "/?" + urllib.parse.urlencode({"query": sql + " FORMAT JSON"}),
                                 headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": pw})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=30).read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()[:400])


bad = 0
for f in sorted(glob.glob("observability/grafana/dashboards/*.json")):
    d = json.load(open(f))
    queries = [(f"var:{v['name']}", v["query"]["rawSql"]) for v in d["templating"]["list"]]
    for p in d["panels"]:
        for t in p["targets"]:
            if "rawSql" in t:
                queries.append((f"panel {p['id']} '{p['title']}' [{t['refId']}]", t["rawSql"]))
    print(f"\n{d['title']}  ({d['uid']})")
    for name, sql in queries:
        try:
            r = run(subst(sql))
            cols = [c["name"] for c in r["meta"]]
            if STRICT and not r["data"] and "structural" not in sql:
                raise RuntimeError("0 rows (strict mode)")
            print(f"  ok   {len(r['data']):>4} rows  {name}  cols={cols}")
        except Exception as e:
            bad += 1
            print(f"  FAIL {name}\n       {e}")
print("\nALL QUERIES OK" if not bad else f"\n{bad} QUERY FAILURES")
sys.exit(1 if bad else 0)
