#!/usr/bin/env python3
"""Assertions for scripts/e2e-local.sh. Usage: e2e_assert.py <bootstrap> <dlq_topic> <since_epoch_s> [registry_on:true|false] [serialization]"""
import base64
import json
import sys

import pgutil

bootstrap, dlq, since = sys.argv[1:4]
REGISTRY = len(sys.argv) > 4 and sys.argv[4] == "true"
AVRO = len(sys.argv) > 5 and sys.argv[5] == "confluent-avro"     # Avro cannot carry a wrong-typed value: no structural violations possible
fails = []


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


q = pgutil.query
S = f"window_start >= to_timestamp({since}) "
hb = q("SELECT count(*) AS c, count(*) FILTER (WHERE kafka_status = 'up') AS up, max(kafka_cluster_id) AS cid FROM sq.job_heartbeat")[0]
check("heartbeat rows written, Kafka reachable", hb["c"] >= 2 and hb["up"] >= 2, hb)
check("heartbeat carries cluster id", bool(hb["cid"]), hb)


def rows(extra):
    return q(f"SELECT status::text AS status, value, threshold, window_start, details::text AS details FROM sq.check_results WHERE {S} AND {extra} ORDER BY window_start")


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
# The generator stops mid-window, so the LAST window with traffic is a partial one (e.g. 6 msg/s instead of 10) that is
# rightly flagged just before the silence. Judge only windows that are followed by another window with traffic.
flowing = [r for i, r in enumerate(p_vol) if i + 1 < len(p_vol) and p_vol[i + 1]["value"] > 0]
check("payments never failed once flowing (isolation between topics)",
      not any(r["status"] != "ok" and r["value"] > 5 for r in flowing), [(r["status"], round(r["value"], 1)) for r in p_vol])

o_card = rows("topic='orders' AND check_type='cardinality' AND field='customer_id'")
check("cardinality customer_id: baseline ~15 distinct, ok", any(r["status"] == "ok" and 10 <= r["value"] <= 20 for r in o_card), [(r["status"], round(r["value"])) for r in o_card])
check("cardinality customer_id: id explosion -> FAIL (>5x)", any(r["status"] == "fail" and r["value"] > 60 for r in o_card), [(r["status"], round(r["value"])) for r in o_card])
check("cardinality: untracked fields produce no rows", not q(f"SELECT 1 FROM sq.check_results WHERE {S} AND check_type='cardinality' AND field <> 'customer_id' LIMIT 1"))
o_fr = rows("topic='orders' AND check_type='freshness'")
check("freshness orders: healthy p95 < 5 s", any(r["status"] == "ok" and r["value"] < 5000 for r in o_fr), [(r["status"], round(r["value"])) for r in o_fr])
check("freshness orders: 150 s-old events -> FAIL (measured, not dropped as late)", any(r["status"] == "fail" and 140_000 < r["value"] < 175_000 for r in o_fr), [(r["status"], round(r["value"])) for r in o_fr])
p_fr = rows("topic='payments' AND check_type='freshness'")
check("freshness payments: always ok", p_fr and all(r["status"] == "ok" for r in p_fr), [(r["status"], round(r["value"])) for r in p_fr])

if REGISTRY:
    o_st = rows("topic='orders' AND check_type='structural'")
    check("structural orders: valid traffic ok (rows exist and some are clean)", any(r["status"] == "ok" and r["value"] == 0 for r in o_st), [(r["status"], round(r["value"], 3)) for r in o_st])
    if AVRO:
        check("structural orders (Avro): every decoded record is valid by construction -> never a violation", o_st and all(r["status"] == "ok" for r in o_st), [(r["status"], round(r["value"], 3)) for r in o_st])
    else:
        check("structural orders: 30% wrong-typed amount -> FAIL naming the field",
              any(r["status"] == "fail" and "amount" in r["details"] and r["value"] > 0.15 for r in o_st), [(r["status"], round(r["value"], 3)) for r in o_st])
    p_st = rows("topic='payments' AND check_type='structural'")
    check("structural payments: always ok (schema-valid traffic)", p_st and all(r["status"] == "ok" for r in p_st), [(r["status"], round(r["value"], 3)) for r in p_st])
    check("heartbeat reports registry up", q("SELECT count(*) AS c FROM sq.job_heartbeat WHERE registry_status = 'up'")[0]["c"] >= 1)
else:
    print("SKIP structural assertions (registry disabled)")

dups = q("SELECT count(*) AS c, count(DISTINCT (topic, field, check_type, window_start, window_end)) AS u FROM sq.check_results")[0]
check("one row per (topic, field, check, window)", dups["c"] == dups["u"], dups)
v = q("SELECT count(*) AS c FROM sq.violations")[0]
check("violations view populated", v["c"] > 0, v)
ls = q("SELECT count(*) AS c FROM sq.latest_status")[0]
check("latest_status trigger populated the table", ls["c"] > 0, ls)
th = q("SELECT topic, check_type, worst_status::text AS worst FROM sq.topic_health")
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
if REGISTRY and not AVRO:
    check("DLQ: schema violations dead-lettered", reasons.get("schema_violation", 0) > 5, reasons)
if AVRO:
    check("DLQ (Avro): ONLY the intentionally corrupt/required-null records were dead-lettered, no decode failures or unknown ids",
          not any(r in reasons for r in ("avro_decode_failed", "unknown_schema_id", "unsupported_format")), reasons)

print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
