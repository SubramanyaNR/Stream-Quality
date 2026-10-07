#!/usr/bin/env python3
"""Behavioural tests for postgres/init/01_init.sql against a live Postgres (needs psycopg2).
Usage: test_schema.py <host> <port> <admin_user> <dbname>   (schema must already be applied; tables are left clean)"""
import sys

import psycopg2

host, port, admin, db = sys.argv[1:5]
fails = []


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


def conn(user, pw=None):
    c = psycopg2.connect(host=host, port=port, user=user, password=pw, dbname=db)
    c.autocommit = True
    return c


a = conn(admin).cursor()
a.execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat")
w = conn("sq_writer", "sq_writer_pw").cursor()
r = conn("sq_reader", "sq_reader_pw").cursor()

INS = """INSERT INTO sq.check_results (topic, field, check_type, window_start, window_end, value, threshold, status, details)
         VALUES (%s,%s,%s,%s::timestamptz,%s::timestamptz,%s,%s,%s,%s::jsonb)
         ON CONFLICT (topic, check_type, field, window_start, window_end)
         DO UPDATE SET value = EXCLUDED.value, threshold = EXCLUDED.threshold, status = EXCLUDED.status, details = EXCLUDED.details"""
row = ("orders", "order_id", "null_rate", "2026-10-05 10:00:00+00", "2026-10-05 10:01:00+00", 0.3, 0.2, "fail", '{"nulls":30}')
w.execute(INS, row); w.execute(INS, row); w.execute(INS, row)                  # replays
check("replayed identical rows collapse to one (PK + upsert)", (a.execute("SELECT count(*) FROM sq.check_results") or a.fetchone())[0] == 1)
w.execute(INS, row[:5] + (0.01, 0.2, "ok", "{}"))                              # same window re-evaluated -> overwritten
a.execute("SELECT value, status::text FROM sq.check_results"); v = a.fetchone()
check("same window re-sent with a new verdict overwrites the old one", v == (0.01, "ok"), v)

w.execute(INS, ("orders", "order_id", "null_rate", "2026-10-05 10:01:00+00", "2026-10-05 10:02:00+00", 0.5, 0.2, "fail", '{"nulls":50}'))
w.execute(INS, ("orders", "", "volume", "2026-10-05 10:01:00+00", "2026-10-05 10:02:00+00", 5, 10, "warn", "{}"))
w.execute(INS, ("payments", "", "volume", "2026-10-05 10:01:00+00", "2026-10-05 10:02:00+00", 9, 0, "ok", "{}"))
w.execute(INS, row[:3] + ("2026-10-05 09:00:00+00", "2026-10-05 09:01:00+00", 0.0, 0.2, "ok", "{}"))   # an OLD window arriving late
a.execute("SELECT window_end, status::text FROM sq.latest_status WHERE topic='orders' AND check_type='null_rate'")
ls = a.fetchone()
check("latest_status keeps the NEWEST window; a late/replayed old window never overwrites it",
      ls[0].isoformat().startswith("2026-10-05T10:02") and ls[1] == "fail", ls)

r.execute("SELECT count(*) FROM sq.violations")
check("violations view = every non-ok row (2: the 10:00 window was overwritten to ok)", r.fetchone()[0] == 2, "")
r.execute("SELECT check_type, worst_status::text, is_stale FROM sq.topic_health WHERE topic='orders' ORDER BY check_type")
th = r.fetchall()
check("topic_health: worst status per check (enum ordering), stale flag", th == [("null_rate", "fail", True), ("volume", "warn", True)], th)

r.execute("SELECT status FROM sq.check_results LIMIT 1")
check("reader can SELECT", True)
try:
    r.execute("DELETE FROM sq.check_results"); check("reader must not DELETE", False)
except psycopg2.errors.InsufficientPrivilege:
    check("reader must not DELETE/INSERT (read-only role)", True)
try:
    w.execute("DELETE FROM sq.check_results"); check("writer must not DELETE", False)
except psycopg2.errors.InsufficientPrivilege:
    check("writer cannot DELETE (retention is admin-only)", True)

try:
    w.execute(INS, ("t", "", "volume", "2026-10-05 10:00:00+00", "2026-10-05 10:01:00+00", 1, 1, "bogus", "{}"))
    check("invalid status rejected by enum", False)
except psycopg2.errors.InvalidTextRepresentation:
    check("invalid status rejected by enum type", True)
try:
    w.execute(INS, ("t", "", "volume", "2026-10-05 10:00:00+00", "2026-10-05 10:01:00+00", 1, 1, "ok", "not json"))
    check("invalid JSON rejected by jsonb", False)
except psycopg2.errors.InvalidTextRepresentation:
    check("invalid JSON in details rejected by jsonb", True)

a.execute("TRUNCATE sq.check_results")
a.execute(INS, ("t", "", "volume", "2020-01-01 00:00:00+00", "2020-01-01 00:01:00+00", 1, 1, "ok", "{}"))
a.execute(INS, ("t", "", "volume", "2099-01-01 00:00:00+00", "2099-01-01 00:01:00+00", 1, 1, "ok", "{}"))
a.execute("SELECT sq.purge_old()")
a.execute("SELECT count(*) FROM sq.check_results")
check("purge_old removes only rows older than the retention window", a.fetchone()[0] == 1)
a.execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat")
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
