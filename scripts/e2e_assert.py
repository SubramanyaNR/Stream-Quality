#!/usr/bin/env python3
"""Assertions for scripts/e2e-local.sh. Usage: e2e_assert.py <ch_url> <ch_user:pw> <bootstrap> <dlq_topic> <since_epoch_s>"""
import base64
import json
import sys
import urllib.parse
import urllib.request

ch_url, auth, bootstrap, dlq, since = sys.argv[1:6]
user, pw = auth.split(":", 1)
fails = []


def q(sql):
    req = urllib.request.Request(ch_url + "/?" + urllib.parse.urlencode({"query": sql + " FORMAT JSONEachRow"}),
                                 headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": pw})
    body = urllib.request.urlopen(req, timeout=20).read().decode()
    return [json.loads(l) for l in body.splitlines() if l.strip()]


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


S = f"window_start >= toDateTime64({since}, 3) "
hb = q("SELECT count() c, countIf(kafka_status='up') up, any(kafka_cluster_id) cid FROM sq.job_heartbeat")[0]
check("heartbeat rows written, Kafka reachable", int(hb["c"]) >= 2 and int(hb["up"]) >= 2, hb)
check("heartbeat carries cluster id", hb["cid"] != "", hb)

def rows(extra):
    return q(f"SELECT status, value, threshold, window_start, details FROM sq.check_results WHERE {S} AND {extra} ORDER BY window_start")

o_cust = rows("topic='orders' AND check_type='null_rate' AND field='customer_id'")
check("null_rate customer_id: has ok windows", any(r["status"] == "ok" for r in o_cust), len(o_cust))
check("null_rate customer_id: 40% nulls -> FAIL", any(r["status"] == "fail" and 0.25 < r["value"] < 0.55 for r in o_cust), [r["value"] for r in o_cust])
o_id = rows("topic='orders' AND check_type='null_rate' AND field='order_id'")
check("null_rate order_id (required, fail>1%): 5% nulls -> FAIL", any(r["status"] == "fail" for r in o_id), [r["value"] for r in o_id])
check("null_rate order_id: healthy windows ok", any(r["status"] == "ok" and r["value"] == 0 for r in o_id))

o_vol = rows("topic='orders' AND check_type='volume'")
check("volume orders: healthy ~20/s ok", any(r["status"] == "ok" and 12 < r["value"] < 28 for r in o_vol), [round(r["value"], 1) for r in o_vol])
check("volume orders: -90% drop -> FAIL with value>0 (baseline-relative)", any(r["status"] == "fail" and 0 < r["value"] < 6 and r["threshold"] > 0 for r in o_vol), [(r["status"], round(r["value"], 1)) for r in o_vol])
check("volume orders: silence -> FAIL value 0 (empty windows are emitted)", any(r["status"] == "fail" and r["value"] == 0 for r in o_vol))

p_vol = rows("topic='payments' AND check_type='volume'")
steady = [r for r in p_vol if 5 < r["value"] < 15]
check("volume payments: steady ~10/s", len(steady) >= 6, [round(r["value"], 1) for r in p_vol])
check("payments never failed once flowing (isolation between topics)",
      not any(r["status"] != "ok" and r["value"] > 5 for r in p_vol), [(r["status"], round(r["value"], 1)) for r in p_vol])

o_card = rows("topic='orders' AND check_type='cardinality' AND field='customer_id'")
check("cardinality customer_id: baseline ~15 distinct, ok", any(r["status"] == "ok" and 10 <= r["value"] <= 20 for r in o_card), [(r["status"], round(r["value"])) for r in o_card])
check("cardinality customer_id: id explosion -> FAIL (>5x)", any(r["status"] == "fail" and r["value"] > 60 for r in o_card), [(r["status"], round(r["value"])) for r in o_card])
check("cardinality: untracked fields produce no rows", not q(f"SELECT 1 FROM sq.check_results WHERE {S} AND check_type='cardinality' AND field != 'customer_id' LIMIT 1"))
o_fr = rows("topic='orders' AND check_type='freshness'")
check("freshness orders: healthy p95 < 5 s", any(r["status"] == "ok" and r["value"] < 5000 for r in o_fr), [(r["status"], round(r["value"])) for r in o_fr])
check("freshness orders: 150 s-old events -> FAIL (measured, not dropped as late)", any(r["status"] == "fail" and 140_000 < r["value"] < 175_000 for r in o_fr), [(r["status"], round(r["value"])) for r in o_fr])
p_fr = rows("topic='payments' AND check_type='freshness'")
check("freshness payments: always ok", p_fr and all(r["status"] == "ok" for r in p_fr), [(r["status"], round(r["value"])) for r in p_fr])

dups = q("SELECT count() c, uniqExact(topic, field, check_type, window_start, window_end) u FROM sq.check_results FINAL")[0]
check("one row per (topic, field, check, window)", int(dups["c"]) == int(dups["u"]), dups)
v = q("SELECT count() c FROM sq.violations FINAL")[0]
check("violations table populated by materialized view", int(v["c"]) > 0, v)
th = q("SELECT topic, check_type, worst_status FROM sq.topic_health")
check("topic_health view returns rows", len(th) > 0, th)

# --- dead-letter topic ---
from kafka import KafkaConsumer
c = KafkaConsumer(dlq, bootstrap_servers=bootstrap.split(","), auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=8000)
reasons = {}
good_roundtrip = False
for m in c:
    e = json.loads(m.value)
    reasons[e["reason"]] = reasons.get(e["reason"], 0) + 1
    if e["reason"] == "invalid_json":
        good_roundtrip = base64.b64decode(e["value_b64"]).startswith(b'{"order_id": "broken')
check("DLQ: corrupt JSON dead-lettered", reasons.get("invalid_json", 0) > 5, reasons)
check("DLQ: original bytes preserved (base64 roundtrip)", good_roundtrip)
check("DLQ: null required field dead-lettered", reasons.get("required_field_missing", 0) > 0, reasons)

print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
