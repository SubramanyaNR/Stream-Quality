#!/usr/bin/env python3
"""Seeds ~3 hours of realistic results (3 topics, every check type, a few incidents, heartbeats and alerts) so the dashboards
have something to show without a running pipeline. TRUNCATES sq.check_results / latest_status / job_heartbeat / alert_events.
Connection via the E2E_PG_* environment (see pgutil.py)."""
import json, math, random, datetime as dt

import pgutil

random.seed(7)
NOW = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
W = 60                                    # seconds per window
N = 180                                   # windows (3 h)
TOPICS = {"bank.dbo.transactions": 42.0, "bank.app.clickstream": 120.0, "bank.app.application_logs": 18.0}
cur = pgutil.connect().cursor()
cur.execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat, sq.alert_events")


def put(topic, field, check, ws, value, thr, status, details=None, baseline=None, p50=None, p99=None):
    cur.execute("""INSERT INTO sq.check_results (topic, field, check_type, window_start, window_end, value, threshold, status, details, baseline, p50_ms, p99_ms)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (topic, field, check, ws, ws + dt.timedelta(seconds=W), value, thr, status, json.dumps(details) if details else None, baseline, p50, p99))


# incident windows (index from the start): (topic, check, field, from, to, status)
INC = [("bank.dbo.transactions", "null_rate", "account_id", 60, 71, "fail"),
       ("bank.dbo.transactions", "type_drift", "amount", 95, 99, "fail"),
       ("bank.app.clickstream", "volume", "", 120, 128, "fail"),
       ("bank.app.clickstream", "schema_drift", "", 40, 45, "warn"),
       ("bank.app.application_logs", "freshness", "", 150, 158, "warn"),
       ("bank.dbo.transactions", "volume", "", 20, 22, "warn")]


def bad(topic, check, field, i):
    for t, c, f, a, b, st in INC:
        if (t, c, f) == (topic, check, field) and a <= i <= b:
            return st
    return None


for i in range(N):
    ws = NOW - dt.timedelta(seconds=W * (N - i))
    for topic, rate in TOPICS.items():
        wave = 1 + 0.12 * math.sin(i / 14.0)
        base = rate * wave
        st = bad(topic, "volume", "", i)
        v = base * (0.2 if st == "fail" else 0.62 if st == "warn" else random.uniform(0.96, 1.04))
        put(topic, "", "volume", ws, v, base * 0.7 if st else 0, st or "ok", {"count": int(v * W), "drop_pct": 40} if st else None, baseline=base)
        st = bad(topic, "freshness", "", i)
        p95 = random.uniform(700, 1400) * (60 if st else 1)
        put(topic, "", "freshness", ws, p95, 30000 if st else 0, st or "ok", {"p95_ms": p95} if st else None, p50=p95 * 0.35, p99=p95 * 1.4)
        st = bad(topic, "structural", "", i)
        put(topic, "", "structural", ws, 0.0, 0.0, "ok")
        st = bad(topic, "schema_drift", "", i)
        put(topic, "", "schema_drift", ws, 1.0 if st else 0.0, 0.0, st or "ok", {"appeared": ["currency_v2"], "disappeared": []} if st else None)
        for f in ("txn_id", "account_id", "customer_id")[: 3 if topic.endswith("transactions") else 1]:
            st = bad(topic, "null_rate", f, i)
            nv = random.uniform(0.3, 0.45) if st == "fail" else random.uniform(0.0, 0.008)
            put(topic, f, "null_rate", ws, nv, 0.2 if st else 0.05, st or "ok", {"nulls": int(nv * 1000), "total": 1000} if st else None)
        if topic.endswith("transactions"):
            st = bad(topic, "type_drift", "amount", i)
            put(topic, "amount", "type_drift", ws, random.uniform(0.2, 0.28) if st else 0.0, 0.1 if st else 0.01, st or "ok",
                {"baseline": "number", "counts": {"number": 760, "string": 240}} if st else None)
            est = 4100 + random.uniform(-60, 60)
            put(topic, "customer_id", "cardinality", ws, est, 0.0, "ok", baseline=4080.0)
cur.execute("""INSERT INTO sq.job_heartbeat (job_id, ts, version, kafka_bootstrap, kafka_status, kafka_cluster_id, registry_status)
               SELECT 'sq-demo', g, '0.1.0', 'kafka:9092', 'up', 'demo-cluster', 'up' FROM generate_series(%s::timestamptz - interval '60 minutes', %s::timestamptz, interval '1 minute') g""", (NOW, NOW))
alerts = [("DataQualityFail", "critical", "bank.dbo.transactions", "null_rate", "account_id", 60, 72),
          ("DataQualityFail", "critical", "bank.dbo.transactions", "type_drift", "amount", 95, 100),
          ("DataQualityFail", "critical", "bank.app.clickstream", "volume", "", 120, 129),
          ("DataQualityWarn", "warning", "bank.app.application_logs", "freshness", "", 152, 159),
          ("QualityMonitorLagging", "critical", "", "", "", 30, 34)]
for k, (name, sev, topic, check, field, a, b) in enumerate(alerts):
    s = NOW - dt.timedelta(seconds=W * (N - a)); e = NOW - dt.timedelta(seconds=W * (N - b))
    for status, ends in (("firing", None), ("resolved", e)):
        cur.execute("""INSERT INTO sq.alert_events (status, fingerprint, alertname, severity, topic, check_type, field, starts_at, ends_at, summary)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (status, f"fp{k}", name, sev, topic, check, field, s, ends, f"{check} {status} on {topic}"))
print("seeded", N, "windows x", len(TOPICS), "topics")
