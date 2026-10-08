#!/usr/bin/env python3
"""Usage: e2e_outage_assert.py <t_ref_epoch> <registry_up_epoch> [serialization] [bootstrap] [dlq_topic]
t_ref = when the job started consuming (windows before t_ref+8 s are ignored)."""
import sys

import pgutil

t0, t_up = int(sys.argv[1]), int(sys.argv[2])
SER = sys.argv[3] if len(sys.argv) > 3 else "json"
AVRO = SER == "confluent-avro"
fails = []
q = pgutil.query


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


def T(e):
    return f"to_timestamp({e})"


down = f"window_end <= {T(t_up)}"
vol_down = q(f"SELECT topic, count(*) AS c FROM sq.check_results WHERE check_type='volume' AND window_start >= {T(t0 + 8)} AND {down} GROUP BY topic")
check("statistical checks ran during the outage (volume rows for both topics)",
      {r["topic"] for r in vol_down} == {"orders", "payments"} and all(r["c"] >= 3 for r in vol_down), vol_down)
nulls_down = q(f"SELECT count(*) AS c, count(*) FILTER (WHERE status <> 'ok') AS bad FROM sq.check_results WHERE check_type='null_rate' AND window_start >= {T(t0 + 8)} AND {down}")[0]
if AVRO:
    rates = {r["topic"]: r["avg_rate"] for r in q(f"SELECT topic, avg(value) AS avg_rate FROM sq.check_results WHERE check_type='volume' AND window_start >= {T(t0 + 8)} AND {down} GROUP BY topic")}
    check("Avro, registry down: records are still COUNTED (orders ~20/s, payments ~10/s)", 14 < rates.get("orders", 0) < 26 and 7 < rates.get("payments", 0) < 13, rates)
    check("Avro, registry down: NO null-rate rows (unreadable records are ignored, not counted as 100% null)", nulls_down["c"] == 0, nulls_down)
    other = q(f"SELECT check_type, count(*) AS c FROM sq.check_results WHERE check_type IN ('cardinality','freshness','structural') AND {down} GROUP BY 1")
    check("Avro, registry down: no cardinality/freshness/structural rows from undecodable data", not other, other)
    fp = q("SELECT count(*) AS c FROM sq.check_results WHERE status <> 'ok' AND check_type <> 'volume' AND " + down)[0]
    check("Avro, registry down: no false field-level alarms at all", fp["c"] == 0, fp)
else:
    check("null-rate also ran during the outage", nulls_down["c"] >= 3, nulls_down)
gaps = q(f"""SELECT topic, max(g) AS mg FROM (SELECT topic, extract(epoch FROM window_start - lag(window_start) OVER (PARTITION BY topic ORDER BY window_start)) AS g
             FROM sq.check_results WHERE check_type='volume' AND window_start >= {T(t0 + 8)}) x GROUP BY topic""")
check("no missing windows across the outage/recovery boundary (max gap <= 10 s)", gaps and all(r["mg"] is not None and r["mg"] <= 10 for r in gaps), gaps)
st_down = q(f"SELECT count(*) AS c FROM sq.check_results WHERE check_type='structural' AND {down}")[0]
check("NO structural rows while the registry was down (skipped, not failed)", st_down["c"] == 0, st_down)
st_bad = q("SELECT count(*) AS c FROM sq.check_results WHERE check_type='structural' AND status <> 'ok'")[0]
check("registry outage never produced a structural failure/warning", st_bad["c"] == 0, st_bad)
st_up = q(f"SELECT topic, count(*) AS c FROM sq.check_results WHERE check_type='structural' AND window_start >= {T(t_up + 8)} GROUP BY topic")
if AVRO:
    resumed = q(f"SELECT topic, count(*) AS c, max(value) AS mx FROM sq.check_results WHERE check_type='null_rate' AND window_start >= {T(t_up + 8)} GROUP BY topic")
    check("Avro: field-level checks (null rate) RESUMED after the registry returned, for both topics", {r["topic"] for r in resumed} == {"orders", "payments"} and all(r["c"] >= 2 for r in resumed), resumed)
    check("Avro: after recovery the data is healthy again (no nulls)", all(r["mx"] == 0 for r in resumed), resumed)
check("structural checks resumed after the registry returned (both topics)", {r["topic"] for r in st_up} == {"orders", "payments"} and all(r["c"] >= 2 for r in st_up), st_up)
hb = {r["registry_status"] for r in q("SELECT DISTINCT registry_status FROM sq.job_heartbeat")}
check("heartbeat reported registry 'down' and later 'up'", {"down", "up"} <= hb, hb)
viol = q("SELECT count(*) AS c FROM sq.violations WHERE check_type='structural'")[0]
check("violation log has no structural entries from the outage", viol["c"] == 0, viol)
if len(sys.argv) > 5:
    from kafka import KafkaConsumer
    import json as _json
    c = KafkaConsumer(sys.argv[5], bootstrap_servers=sys.argv[4].split(","), auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=6000)
    reasons = {}
    for m in c:
        r = _json.loads(m.value)["reason"]; reasons[r] = reasons.get(r, 0) + 1
    check("a registry outage dead-lettered NOTHING (steady, valid traffic)", not reasons, reasons)
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
