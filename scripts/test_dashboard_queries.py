#!/usr/bin/env python3
"""Executes every SQL query in the provisioned Grafana dashboards against a live Postgres (as the read-only user),
with Grafana macros/variables substituted. Catches SQL/schema drift without needing Grafana.
Usage: test_dashboard_queries.py [--strict]     (connection via the E2E_PG_* environment, see pgutil.py)
--strict: a query returning 0 rows is a failure (use after the e2e run, when all tables have data)."""
import sys

import dashboard_sql
import pgutil

STRICT = "--strict" in sys.argv
# Legitimately empty on a fresh install / short e2e: needs structural checks (registry on) a week of history, or an alerting stack.
MAY_BE_EMPTY = ("structural", "7d ago", "last week", "alert_history")   # alert_history: only filled when Prometheus/Alertmanager ran

bad = 0
for d, queries in dashboard_sql.load():
    print(f"\n{d['title']}  ({d['uid']})")
    for label, _title, _ref, sql, fmt in queries:
        try:
            rows = pgutil.query(dashboard_sql.subst(sql), user="sq_reader", password="sq_reader_pw")
            if STRICT and not rows and not any(m in sql or m in label for m in MAY_BE_EMPTY):
                raise RuntimeError("0 rows (strict mode)")
            cols = list(rows[0].keys()) if rows else "(no rows)"
            print(f"  ok   {len(rows):>4} rows  {label}  cols={cols}")
            if fmt == "time_series" and rows:   # Grafana time-series contract
                assert cols[0] == "time", f"first column of a time_series query must be 'time', got {cols[0]}"
        except Exception as e:
            bad += 1
            print(f"  FAIL {label}\n       {e}")
print("\nALL QUERIES OK" if not bad else f"\n{bad} QUERY FAILURES")
sys.exit(1 if bad else 0)
