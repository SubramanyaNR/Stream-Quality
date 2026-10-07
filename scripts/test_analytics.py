#!/usr/bin/env python3
"""Seeds a deterministic history into sq.check_results and asserts the EXACT numbers produced by the analytics
dashboard's own SQL (loaded from the shipped dashboard JSON, so what is tested is what ships). Truncates the tables.
Connection via E2E_PG_* (see pgutil.py)."""
import sys
from datetime import datetime, timedelta, timezone

import dashboard_sql
import pgutil

fails = []


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


pgutil.execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat")
now = datetime.now(timezone.utc)
T0 = (now - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)        # an hour boundary, inside the last 24 h
W = timedelta(minutes=1)
INS = """INSERT INTO sq.check_results (topic, field, check_type, window_start, window_end, value, threshold, status, details)
         VALUES (%s,%s,%s,%s,%s,%s,0,%s,%s::jsonb)"""
conn = pgutil.connect()
cur = conn.cursor()


def put(topic, field, check, start, value, status, details="{}"):
    cur.execute(INS, (topic, field, check, start, start + W, value, status, details))


# --- topic "a", volume: 30 one-minute windows ---
#   ok x10 | fail x3 (incident 1: 3 min) | ok x5 | warn x2 + fail x2 (incident 2: 4 min, worst=fail) | ok x8
seq = ["ok"] * 10 + ["fail"] * 3 + ["ok"] * 5 + ["warn"] * 2 + ["fail"] * 2 + ["ok"] * 8
assert len(seq) == 30
for i, st in enumerate(seq):
    put("a", "", "volume", T0 + i * W, 20.0, st, '{"baseline_rate": 20.0}')
# --- topic "a", null_rate on field f: fails at the same windows as incident 1 -> a correlated failure ---
for i in range(30):
    put("a", "f", "null_rate", T0 + i * W, 0.5 if 10 <= i < 13 else 0.0, "fail" if 10 <= i < 13 else "ok")
# --- topic "b", null_rate g: fail fail <10 min GAP> fail fail  -> TWO incidents, not one ---
for i in (0, 1, 12, 13):
    put("b", "g", "null_rate", T0 + i * W, 0.3, "fail")
# --- last week: topic a volume, same hour, value 10 (week-over-week partner) ---
for i in range(10):
    put("a", "", "volume", T0 - timedelta(days=7) + i * W, 10.0, "ok")
cur.close()

q = lambda uid, title, ref="A": pgutil.query(dashboard_sql.subst(dashboard_sql.find(uid, title, ref),
                                             {**dashboard_sql.VARS, "topic": "'a','b'"}), user="sq_reader", password="sq_reader_pw")
AN = "sq-analytics"

# ---------- incidents ----------
inc = q(AN, "Incident log", "A")
by = {}
for r in inc:
    by.setdefault((r["topic"], r["check_type"], r["field"]), []).append(r)
a_vol = sorted(by[("a", "volume", "")], key=lambda r: r["time"])
check("a/volume has exactly 2 incidents", len(a_vol) == 2, len(a_vol))
check("incident 1: 3 windows, 180 s, worst=fail", (a_vol[0]["windows"], a_vol[0]["duration_s"], a_vol[0]["worst"]) == (3, 180, "fail"), a_vol[0])
check("incident 2: warn+fail merge into ONE incident (4 windows, 240 s, 2 fail windows)",
      (a_vol[1]["windows"], a_vol[1]["duration_s"], a_vol[1]["worst"], a_vol[1]["fail_windows"]) == (4, 240, "fail", 2), a_vol[1])
b_inc = by[("b", "null_rate", "g")]
check("a 10-minute gap splits one run of failures into 2 incidents", len(b_inc) == 2 and all(r["duration_s"] == 120 for r in b_inc), b_inc)
check("incident log total = 2 (a/vol) + 1 (a/null) + 2 (b) = 5", len(inc) == 5, len(inc))
check("ok windows never appear in incidents", all(r["worst"] in ("warn", "fail") for r in inc))

# ---------- headline stats (64 windows in range: 50 ok, 2 warn, 12 fail) ----------
one = lambda title, col: q(AN, title)[0][col]
check("Data-quality score = (50 + 0.5*2) / 64 = 79.69 %", abs(float(one("Data-quality score", "score")) - 79.69) < 0.011, one("Data-quality score", "score"))
check("Availability = 50 / 64 = 78.13 %", abs(float(one("Availability (windows OK)", "pct_ok")) - 78.13) < 0.011, one("Availability (windows OK)", "pct_ok"))
check("Incident count = 5", one("Incident count", "incidents") == 5)
check("Longest incident = 240 s", one("Longest incident", "seconds") == 240)
check("Avg incident duration = (180+240+180+120+120)/5 = 168 s", one("Avg incident duration", "seconds") == 168)
check("Open now = 0 (all seeded windows are hours old)", one("Open now", "open_now") == 0)

# ---------- availability table ----------
av = q(AN, "Availability by topic and check")
key = {(r["topic"], r["check_type"]): r for r in av}
check("availability: b/null_rate 0 %, a/volume 76.67 %, a/null_rate 90 %",
      (float(key[("b", "null_rate")]["pct_ok"]), float(key[("a", "volume")]["pct_ok"]), float(key[("a", "null_rate")]["pct_ok"])) == (0.0, 76.67, 90.0), av)
check("availability counts: a/volume 2 warns, 5 fails", (key[("a", "volume")]["warns"], key[("a", "volume")]["fails"]) == (2, 5), key[("a", "volume")])
check("availability is sorted worst-first", [r["topic"] + "/" + r["check_type"] for r in av] == ["b/null_rate", "a/volume", "a/null_rate"], [(r["topic"], r["check_type"]) for r in av])

# ---------- flapping ----------
fl = {(r["topic"], r["check_type"], r["field"]): r for r in q(AN, "Flappiest checks")}
check("flappiest: a/volume flips 4x (ok->fail, fail->ok, ok->warn, bad->ok); warn->fail is NOT a flip", fl[("a", "volume", "")]["flips"] == 4, fl)
check("flappiest: a/null_rate flips 2x; b/null_rate (never recovers) is excluded", fl[("a", "null_rate", "f")]["flips"] == 2 and ("b", "null_rate", "g") not in fl, fl)

# ---------- worst null-rate fields ----------
nr = q(AN, "Worst null-rate fields")
check("worst null-rate: b/g first (avg 30 %), a/f avg 5 % max 50 % over 3 bad windows",
      [r["field"] for r in nr] == ["g", "f"] and float(nr[0]["avg_null_pct"]) == 30.0
      and (float(nr[1]["avg_null_pct"]), float(nr[1]["max_null_pct"]), nr[1]["bad_windows"]) == (5.0, 50.0, 3), nr)

# ---------- correlated failures ----------
cf = q(AN, "Correlated failures")
check("correlated failures: exactly the 3 windows where a/volume AND a/null_rate failed together",
      len(cf) == 3 and all(r["topic"] == "a" and r["failing_checks"] == "null_rate, volume" and r["n_checks"] == 2 for r in cf), cf)

# ---------- time series ----------
sc = q(AN, "Quality score over time")
check("quality score over time: rows for both topics, all within 0..100", {r["metric"] for r in sc} == {"a", "b"} and all(0 <= float(r["score"]) <= 100 for r in sc), sc[:2])
iph = q(AN, "Incidents started per hour")
check("incidents started per hour: 2 volume + 3 null_rate in the seeded hour",
      {r["metric"]: r["incidents"] for r in iph} == {"volume": 2, "null_rate": 3} and len(iph) == 2, iph)
now_rows = q(AN, "Volume: now vs same time last week (hourly avg msgs/s)", "A")
prev_rows = q(AN, "Volume: now vs same time last week (hourly avg msgs/s)", "B")
check("week-over-week: this hour avg 20 msgs/s", [(r["metric"], float(r["msgs_per_s"])) for r in now_rows] == [("a", 20.0)], now_rows)
check("week-over-week: last week's hour (10 msgs/s) is shifted onto the SAME x position as this week's",
      [(r["metric"], float(r["msgs_per_s"])) for r in prev_rows] == [("a (7d ago)", 10.0)] and prev_rows[0]["time"] == now_rows[0]["time"], prev_rows)

pgutil.execute("TRUNCATE sq.check_results, sq.latest_status, sq.job_heartbeat")
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
