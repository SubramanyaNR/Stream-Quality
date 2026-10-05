#!/usr/bin/env python3
"""Usage: e2e_outage_assert.py <ch_url> <user:pw> <t0_epoch> <registry_up_epoch>"""
import json
import sys
import urllib.parse
import urllib.request

ch_url, auth, t0, t_up = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
user, pw = auth.split(":", 1)
fails = []


def q(sql):
    req = urllib.request.Request(ch_url + "/?" + urllib.parse.urlencode({"query": sql + " FORMAT JSONEachRow"}),
                                 headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": pw})
    return [json.loads(l) for l in urllib.request.urlopen(req, timeout=20).read().decode().splitlines() if l.strip()]


def check(name, cond, info=""):
    print(("PASS " if cond else "FAIL ") + name + (f"   [{info}]" if info and not cond else ""))
    if not cond:
        fails.append(name)


def T(e):
    return f"toDateTime64({e}, 3)"


down = f"window_end <= {T(t_up)}"
check_vol_down = q(f"SELECT topic, count() c FROM sq.check_results FINAL WHERE check_type='volume' AND window_start >= {T(t0 + 8)} AND {down} GROUP BY topic")
check("statistical checks ran during the outage (volume rows for both topics)",
      {r["topic"] for r in check_vol_down} == {"orders", "payments"} and all(int(r["c"]) >= 3 for r in check_vol_down), check_vol_down)
nulls_down = q(f"SELECT count() c FROM sq.check_results FINAL WHERE check_type='null_rate' AND window_start >= {T(t0 + 8)} AND {down}")[0]
check("null-rate also ran during the outage", int(nulls_down["c"]) >= 3, nulls_down)
gaps = q(f"SELECT topic, max(g) mg FROM (SELECT topic, dateDiff('second', lagInFrame(window_start, 1, window_start) OVER (PARTITION BY topic ORDER BY window_start), window_start) g FROM sq.check_results FINAL WHERE check_type='volume' AND window_start >= {T(t0 + 8)}) GROUP BY topic")
check("no missing windows across the outage/recovery boundary (max gap <= 10 s)", gaps and all(int(r["mg"]) <= 10 for r in gaps), gaps)
st_down = q(f"SELECT count() c FROM sq.check_results FINAL WHERE check_type='structural' AND window_end <= {T(t_up)}")[0]
check("NO structural rows while the registry was down (skipped, not failed)", int(st_down["c"]) == 0, st_down)
st_bad = q("SELECT count() c FROM sq.check_results FINAL WHERE check_type='structural' AND status != 'ok'")[0]
check("registry outage never produced a structural failure/warning", int(st_bad["c"]) == 0, st_bad)
st_up = q(f"SELECT topic, count() c FROM sq.check_results FINAL WHERE check_type='structural' AND window_start >= {T(t_up + 8)} GROUP BY topic")
check("structural checks resumed after the registry returned (both topics)", {r["topic"] for r in st_up} == {"orders", "payments"} and all(int(r["c"]) >= 2 for r in st_up), st_up)
hb = {r["registry_status"] for r in q("SELECT DISTINCT registry_status FROM sq.job_heartbeat")}
check("heartbeat reported registry 'down' and later 'up'", {"down", "up"} <= hb, hb)
viol = q("SELECT count() c FROM sq.violations FINAL WHERE check_type='structural'")[0]
check("violation log has no structural entries from the outage", int(viol["c"]) == 0, viol)
print(f"\n{'ALL PASSED' if not fails else 'FAILED: ' + ', '.join(fails)}")
sys.exit(1 if fails else 0)
