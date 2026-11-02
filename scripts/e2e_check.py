#!/usr/bin/env python3
"""Drives the e2e stack (see scripts/e2e.sh): produces a scripted scenario with a tiny test producer and asserts what the
job wrote to Postgres / the dead-letter topic. Phases (10 s windows): healthy -> wrong-typed values -> new field -> field gone.
A second pattern-matched topic stays silent the whole time (silence detection must flag it)."""
import json, random, subprocess, sys, time

import pgutil

COMPOSE = ["docker", "compose", "-f", "docker-compose.e2e.yml"]
KAFKA = COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/"]
fails = 0


def check(name, ok, detail=""):
    global fails
    print(("PASS " if ok else "FAIL ") + name + (f"  ({detail})" if detail and not ok else ""))
    fails += 0 if ok else 1


def rows(sql, *a):
    return pgutil.query(sql, a or None)


def wait_for(sql, secs, *a):
    end = time.time() + secs
    while time.time() < end:
        r = rows(sql, *a)
        if r and r[0]["n"] > 0:
            return True
        time.sleep(2)
    return False


# ---- wait for the job to produce its first window ----
print("waiting for the job to start producing results ...")
prod = subprocess.Popen(KAFKA[:-1] + ["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", "e2e.orders"],
                        stdin=subprocess.PIPE, text=True, bufsize=1)
t0 = time.time()
n = 0


def phase(name, secs, mutate=None):
    """Produce ~25 msg/s for `secs` seconds."""
    global n
    start = time.time()
    end = start + secs
    while time.time() < end:
        rec = {"id": f"o{n}", "amount": round(random.random() * 100, 2), "event_time": int(time.time() * 1000), "note": "x"}
        if mutate:
            mutate(rec)
        prod.stdin.write(json.dumps(rec) + "\n"); prod.stdin.flush(); n += 1
        time.sleep(0.04)
    return start, time.time()


def wrongtype(r):
    if random.random() < 0.25:
        r["amount"] = "N/A"


healthy = phase("healthy", 70)
wrong = phase("wrongtype", 40, wrongtype)
newf = phase("newfield", 40, lambda r: r.update(currency_v2="EUR"))
gone = phase("fieldgone", 40, lambda r: r.pop("note", None))
prod.stdin.write("this is not json\n"); prod.stdin.flush()      # -> dead-letter topic
time.sleep(15)
prod.stdin.close(); prod.wait(timeout=30)


def between(ph):   # window_start inside the phase (epoch seconds from the producer's clock; same host as Docker)
    return (ph[0] - 5, ph[1] + 5)


Q = ("SELECT count(*) n FROM sq.check_results WHERE topic=%s AND check_type=%s AND status=%s "
     "AND window_start BETWEEN to_timestamp(%s) AND to_timestamp(%s)")

check("job wrote results to Postgres", rows("SELECT count(*) n FROM sq.check_results WHERE topic='e2e.orders'")[0]["n"] > 10)
check("volume ok while healthy", rows(Q, "e2e.orders", "volume", "ok", *between(healthy))[0]["n"] >= 3)
bad = rows("SELECT to_char(window_start,'HH24:MI:SS') ws, check_type, field, status, value, details::text d FROM sq.check_results WHERE topic='e2e.orders' "
           "AND check_type IN ('type_drift','schema_drift') AND status<>'ok' AND window_start BETWEEN to_timestamp(%s) AND to_timestamp(%s) ORDER BY 1",
           healthy[0] + 25, healthy[1] - 5)
check("healthy phase raised no type/schema drift", not bad, str(bad[:3]))
check("type drift FAILS on ~25% wrong-typed amounts (no registry)", rows(Q, "e2e.orders", "type_drift", "fail", *between(wrong))[0]["n"] >= 1)
check("schema drift flags the new field", rows("SELECT count(*) n FROM sq.check_results WHERE topic='e2e.orders' AND check_type='schema_drift' "
      "AND status<>'ok' AND details::text LIKE '%%currency_v2%%' AND window_start BETWEEN to_timestamp(%s) AND to_timestamp(%s)", *between(newf))[0]["n"] >= 1)
check("schema drift flags the vanished field", rows("SELECT count(*) n FROM sq.check_results WHERE topic='e2e.orders' AND check_type='schema_drift' "
      "AND status<>'ok' AND details::text LIKE '%%\"note\"%%' AND window_start BETWEEN to_timestamp(%s) AND to_timestamp(%s)", *between(gone))[0]["n"] >= 1)
check("silent pattern-matched topic gets volume FAIL (not in thresholds.yaml)", wait_for(
      "SELECT count(*) n FROM sq.check_results WHERE topic='e2e.quiet' AND check_type='volume' AND status='fail'", 30))
check("ok rows carry no details JSON; baseline is a plain column",
      rows("SELECT count(*) n FROM sq.check_results WHERE status='ok' AND details IS NOT NULL")[0]["n"] == 0
      and rows("SELECT count(baseline) n FROM sq.check_results WHERE check_type='volume' AND topic='e2e.orders'")[0]["n"] > 0)
check("latest_status is maintained", rows("SELECT count(*) n FROM sq.latest_status WHERE topic='e2e.orders'")[0]["n"] >= 4)

out = subprocess.run(KAFKA[:-1] + ["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", "sq.dead-letter",
                                   "--from-beginning", "--max-messages", "1", "--timeout-ms", "20000"], capture_output=True, text=True).stdout
check("invalid JSON lands in the dead-letter topic", "invalid_json" in out, out[:200])

print("\nALL PASSED" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
