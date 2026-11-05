#!/usr/bin/env python3
"""Demo traffic for `make demo`: prints JSON payment events (one per line, ~20/s) and, in a loop, breaks them the way real
upstream changes do, so every check visibly reacts on the dashboards. Standard library only; the caller pipes stdout into
a Kafka console producer. The current phase is printed to stderr.

Phases (seconds): healthy 120 -> null spike 60 -> healthy 60 -> wrong types 60 -> healthy 60 -> new field 60
                  -> silence 60 -> healthy 60 -> repeat"""
import json
import random
import sys
import time

RATE = 20                       # events per second
PHASES = [("healthy", 120), ("null_spike", 60), ("healthy", 60), ("wrong_types", 60), ("healthy", 60),
          ("new_field", 60), ("silence", 60), ("healthy", 60)]
WHAT = {"healthy": "normal traffic",
        "null_spike": "30% of events lose account_id      -> null_rate FAIL",
        "wrong_types": "25% of amounts become strings     -> type_drift FAIL",
        "new_field": "producers add a 'channel' field     -> schema_drift WARN",
        "silence": "the topic goes quiet                   -> volume FAIL"}


def event(n):
    return {"payment_id": f"pay-{n}", "account_id": f"acc-{random.randint(1, 400)}", "customer_id": f"cus-{random.randint(1, 3000)}",
            "amount": round(random.lognormvariate(3.5, 0.8), 2), "currency": random.choice(["EUR", "USD", "GBP", "NOK"]),
            "event_time": int(time.time() * 1000)}


def main():
    n = 0
    while True:
        for phase, secs in PHASES:
            print(f"[demo] {phase:<12} {WHAT[phase]}", file=sys.stderr, flush=True)
            end = time.time() + secs
            while time.time() < end:
                if phase == "silence":
                    time.sleep(0.5)
                    continue
                e = event(n)
                n += 1
                if phase == "null_spike" and random.random() < 0.30:
                    del e["account_id"]
                if phase == "wrong_types" and random.random() < 0.25:
                    e["amount"] = random.choice(["N/A", "12,50", "unknown"])
                if phase == "new_field":
                    e["channel"] = random.choice(["web", "app", "pos"])
                sys.stdout.write(json.dumps(e) + "\n")
                sys.stdout.flush()
                time.sleep(1.0 / RATE)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        pass
