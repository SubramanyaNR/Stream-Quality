#!/usr/bin/env python3
"""Usage: e2e_outage_assert.py <t0_epoch> <registry_up_epoch>"""
import sys

import pgutil

t0, t_up = int(sys.argv[1]), int(sys.argv[2])
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
nulls_down = q(f"SELECT count(*) AS c FROM sq.check_results WHERE check_type='null_rate' AND window_start >= {T(t0 + 8)} AND {down}")[0]
check("null-rate also ran during the outage", nulls_down["c"] >= 3, nulls_down)
gaps = q(f"""SELECT topic, max(g) AS mg FROM (SELECT topic, extract(epoch FROM window_start - lag(window_start) OVER (PARTITION BY topic ORDER BY window_start)) AS g
             FROM sq.check_results WHERE check_type='volume' AND window_start >= {T(t0 + 8)}) x GROUP BY topic""")
check("no missing windows across the outage/recovery boundary (max gap <= 10 s)", gaps and all(r["mg"] is not None and r["mg"] <= 10 for r in gaps), gaps)
st_down = q(f"SELECT count(*) AS c FROM sq.check_results WHERE check_type='structural' AND {down}")[0]
check("NO structural rows while the registry was down (skipped, not failed)", st_down["c"] == 0, st_down)
st_bad = q("SELECT count(*) AS c FROM sq.check_results WHERE check_type='structural' AND status <> 'ok'")[0]
check("registry outage never produced a structural failure/warning", st_bad["c"] == 0, st_bad)
st_up = q(f"SELECT topic, count(*) AS c FROM sq.check_results WHERE check_type='structural' AND window_start >= {T(t_up + 8)} GROUP BY topic")
check("structural checks resumed after the registry returned (both topics)", {r["topic"] for r in st_up} == {"orders", "payments"} and all(r["c"] >= 2 for r in st_up), st_up)
hb = {r["registry_status"] for r in q("SELECT DISTINCT registry_status FROM sq.job_heartbeat")}
check("heartbeat reported registry 'down' and later 'up'", {"down", "up"} <= hb, hb)
viol = q("SELECT count(*) AS c FROM sq.violations WHERE check_type='structural'")[0]
check("violation log has no structural entries from the outage", viol["c"] == 0, viol)
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
