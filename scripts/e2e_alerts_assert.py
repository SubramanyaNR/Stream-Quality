#!/usr/bin/env python3
"""Usage: e2e_alerts_assert.py <sink_log>"""
import json
import sys

import pgutil

fails = []


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


q = pgutil.query
hist = q("""SELECT alertname, severity, topic, check_type, field, fired_at, resolved_at, firing, extract(epoch FROM duration) AS secs
            FROM sq.alert_history WHERE alertname = 'DataQualityFail' AND topic = 'orders' AND check_type = 'null_rate' AND field = 'customer_id'""")
check("null-rate spike on orders.customer_id FIRED a critical DataQualityFail alert", len(hist) >= 1 and hist[0]["severity"] == "critical", hist)
check("...and recovery RESOLVED it (resolved_at set, firing = false)", bool(hist) and all(not h["firing"] and h["resolved_at"] is not None for h in hist), hist)
check("...and it lasted a plausible time (5-80 s: the 25 s spike plus window/evaluation lag)", bool(hist) and 5 <= float(hist[0]["secs"]) <= 80, [h["secs"] for h in hist])
ev = q("""SELECT status, count(*) AS c FROM sq.alert_events WHERE alertname = 'DataQualityFail' AND topic = 'orders' AND check_type = 'null_rate' AND field = 'customer_id'
          GROUP BY status""")
by = {r["status"]: r["c"] for r in ev}
check("exactly ONE firing row and ONE resolved row stored for that alert instance", by == {"firing": 1, "resolved": 1}, by)
lines = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{") and '"event": "alert"' in l]
fires = [l for l in lines if l["status"] == "firing" and l["labels"].get("check_type") == "null_rate" and l["labels"].get("field") == "customer_id"]
check("Alertmanager really re-sent 'firing' (repeat_interval 10 s) and the sink de-duplicated the repeats",
      len(fires) >= 2 and sum(1 for f in fires if f["new"]) == 1, [(f["new"]) for f in fires])
stored_stalled = q("SELECT count(*) AS c FROM sq.alert_events WHERE alertname IN ('QualityMonitorStalled', 'QualityMonitorMissing') AND status = 'firing'")[0]["c"]
check("no false monitor-stalled/missing alerts while the job was healthy", stored_stalled == 0, stored_stalled)
pay = q("SELECT check_type, fired_at, resolved_at, firing FROM sq.alert_history WHERE topic = 'payments'")
check("healthy topic (payments): only volume alerts, never null_rate/cardinality/freshness/structural", all(p["check_type"] == "volume" for p in pay), pay)
span = q("SELECT min(window_start) AS a, max(window_start) AS b FROM sq.check_results WHERE topic = 'payments' AND check_type = 'volume' AND value > 5")[0]
during = [p for p in pay if span["a"] + __import__("datetime").timedelta(seconds=10) <= p["fired_at"] <= span["b"]]
check("payments never alerted WHILE its traffic was flowing (its volume alerts = empty startup window and the silence after the generator stopped)", not during, during)
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
