#!/usr/bin/env python3
"""Tests tools/alert-sink/app.py against a REAL Postgres: payload parsing, idempotent repeats, firing->resolved pairing in
sq.alert_history, HTTP error contract (400 bad JSON, 500 when the DB is down so Alertmanager retries), and privileges.
Connection via E2E_PG_* (see pgutil.py). Truncates sq.alert_events."""
import json
import os
import sys
import threading
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools", "alert-sink"))
import app  # noqa: E402
import pgutil  # noqa: E402

fails = []


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


def writer_conn():
    import psycopg2
    return psycopg2.connect(host=pgutil.HOST, port=pgutil.PORT, dbname=pgutil.DB, user="sq_writer", password="sq_writer_pw", connect_timeout=3)


pgutil.execute("TRUNCATE sq.alert_events")
srv = app.make_server(0, writer_conn)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()


def post(payload, raw=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/alert", data=raw if raw is not None else json.dumps(payload).encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def alert(status, fp="fp1", name="DataQualityFail", starts="2026-10-07T06:00:00.123456789Z", ends="0001-01-01T00:00:00Z", **labels):
    lab = {"alertname": name, "severity": "critical", "topic": "orders", "check_type": "null_rate", "field": "customer_id", **labels}
    return {"status": status, "labels": lab, "annotations": {"summary": f"{lab['check_type']} FAIL on {lab['topic']}"},
            "startsAt": starts, "endsAt": ends, "fingerprint": fp}


q = lambda sql: pgutil.query(sql)
count = lambda: q("SELECT count(*) AS c FROM sq.alert_events")[0]["c"]

check("firing webhook is stored (200)", post({"status": "firing", "alerts": [alert("firing")]}) == 200 and count() == 1)
row = q("SELECT * FROM sq.alert_events")[0]
check("labels mapped to columns, firing has no end, nanosecond timestamp accepted",
      (row["alertname"], row["topic"], row["check_type"], row["field"], row["severity"], row["ends_at"]) == ("DataQualityFail", "orders", "null_rate", "customer_id", "critical", None), row)
check("labels/annotations kept as jsonb", row["labels"]["topic"] == "orders" and "FAIL" in row["annotations"]["summary"])

for _ in range(3):
    post({"status": "firing", "alerts": [alert("firing")]})                    # Alertmanager repeat_interval re-sends
check("repeated 'firing' notifications are no-ops (still 1 row)", count() == 1, count())

hist = q("SELECT * FROM sq.alert_history")
check("alert_history: still firing, no resolve time", len(hist) == 1 and hist[0]["firing"] is True and hist[0]["resolved_at"] is None, hist)

post({"status": "resolved", "alerts": [alert("resolved", ends="2026-10-07T06:03:30.5Z")]})
hist = q("SELECT *, extract(epoch FROM duration) AS secs FROM sq.alert_history")
check("resolved notification closes the SAME alert instance (1 history row, not 2)", len(hist) == 1 and hist[0]["firing"] is False, hist)
check("duration = resolved - fired = 210.4 s", abs(float(hist[0]["secs"]) - 210.376544) < 0.01, hist[0]["secs"])
check("event table keeps both rows (1 firing + 1 resolved)", count() == 2)

post({"status": "firing", "alerts": [alert("firing", starts="2026-10-07T07:00:00Z")]})   # same alert fires AGAIN later: new instance
hist = q("SELECT firing FROM sq.alert_history ORDER BY fired_at")
check("a re-fire of the same fingerprint is a NEW instance in the history", [h["firing"] for h in hist] == [False, True], hist)

post({"status": "firing", "alerts": [alert("firing", fp="fp2", name="QualityMonitorStalled", topic="", check_type="", field="", severity="critical")]})
r2 = q("SELECT topic, check_type, field FROM sq.alert_events WHERE fingerprint = 'fp2'")[0]
check("alerts without topic/field labels are stored with empty strings", (r2["topic"], r2["check_type"], r2["field"]) == ("", "", ""), r2)

multi = {"status": "firing", "alerts": [alert("firing", fp="fp3", topic="payments"), alert("firing", fp="fp4", topic="orders", check_type="volume")]}
before = count(); post(multi)
check("one webhook with several alerts stores all of them", count() == before + 2)

check("invalid JSON -> 400", post(None, raw=b"{not json") == 400)
check("empty alert list -> 200 and nothing stored", (lambda b: post({"status": "firing", "alerts": []}) == 200 and count() == b)(count()))

# DB down -> 500 (Alertmanager will retry), and nothing half-written
def broken():
    import psycopg2
    return psycopg2.connect(host=pgutil.HOST, port=1, dbname=pgutil.DB, user="sq_writer", password="x", connect_timeout=1)


down = app.make_server(0, broken)
threading.Thread(target=down.serve_forever, daemon=True).start()
req = urllib.request.Request(f"http://127.0.0.1:{down.server_address[1]}/alert", data=json.dumps({"alerts": [alert("firing", fp="fp9")]}).encode(), method="POST")
try:
    urllib.request.urlopen(req, timeout=10); code = 200
except urllib.error.HTTPError as e:
    code = e.code
check("database unreachable -> HTTP 500 so Alertmanager retries", code == 500, code)
check("healthz endpoint", urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5).read() == b"ok")

# privileges: the sink role can insert but not rewrite history
import psycopg2  # noqa: E402
c = writer_conn(); c.autocommit = True
try:
    c.cursor().execute("DELETE FROM sq.alert_events"); check("sink role must not DELETE alert history", False)
except psycopg2.errors.InsufficientPrivilege:
    check("sink role cannot DELETE or UPDATE alert history (insert-only)", True)
try:
    c.cursor().execute("UPDATE sq.alert_events SET summary = 'x'"); check("sink role must not UPDATE", False)
except psycopg2.errors.InsufficientPrivilege:
    pass
r = psycopg2.connect(host=pgutil.HOST, port=pgutil.PORT, dbname=pgutil.DB, user="sq_reader", password="sq_reader_pw"); r.autocommit = True
rc = r.cursor(); rc.execute("SELECT count(*) FROM sq.alert_history"); rc.fetchone()
check("read-only role can query alert_history", True)

pgutil.execute("TRUNCATE sq.alert_events")
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
