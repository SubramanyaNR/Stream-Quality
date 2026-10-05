#!/usr/bin/env python3
"""Test event generator: produces JSON events to Kafka with scripted faults.

Reads the SAME config/kafka.properties the Flink job uses (sq.* keys ignored, ${env:X} resolved).

  python3 generator/generate.py --scenario generator/scenarios/null_spike.yaml
  python3 generator/generate.py --scenario ... --dry-run     # print a few events, no Kafka

Scenario YAML (see generator/scenarios/*.yaml):
  topics:
    orders:
      rate: 20                     # messages/second
      null_rates: {customer_id: 0} # probability a field is emitted as null
      bad_json_rate: 0             # probability of emitting a corrupt payload
      type_break: {amount: 0.2}    # probability a field is emitted with the wrong type ('N/A')
      fields: {order_id: id, customer_id: {choice: 50}, amount: {float: [1, 500]}}
      phases:                      # optional; each overrides the keys above for `duration` seconds
        - {duration: 60}
        - {duration: 60, null_rates: {customer_id: 0.5}}
"""
import argparse
import json
import random
import re
import sys
import threading
import time
import uuid
from pathlib import Path

import yaml

ENV = re.compile(r"\$\{env:([A-Za-z0-9_]+)\}")


def load_props(path):
    import os
    props = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        props[k.strip()] = ENV.sub(lambda m: os.environ.get(m.group(1), ""), v.strip())
    return props


def producer_kwargs(props):
    """Translate Kafka client properties to kafka-python kwargs (Apache Kafka only; nothing vendor-specific)."""
    kw = {"bootstrap_servers": props["bootstrap.servers"].split(",")}
    proto = props.get("security.protocol", "PLAINTEXT")
    kw["security_protocol"] = proto
    if proto.startswith("SASL"):
        mech = props.get("sasl.mechanism", "PLAIN")
        kw["sasl_mechanism"] = mech
        jaas = props.get("sasl.jaas.config", "")
        u = re.search(r'username="([^"]*)"', jaas)
        p = re.search(r'password="([^"]*)"', jaas)
        kw["sasl_plain_username"] = u.group(1) if u else None
        kw["sasl_plain_password"] = p.group(1) if p else None
    if proto.endswith("SSL"):
        if props.get("ssl.truststore.location"):
            print("note: JKS truststores are not supported by kafka-python; set ssl_cafile via env SQ_GEN_CAFILE",
                  file=sys.stderr)
        import os
        if os.environ.get("SQ_GEN_CAFILE"):
            kw["ssl_cafile"] = os.environ["SQ_GEN_CAFILE"]
    return kw


def make_value(spec, rng):
    if spec == "id":
        return uuid.UUID(int=rng.getrandbits(128)).hex
    if isinstance(spec, dict) and "choice" in spec:
        return f"c{rng.randrange(int(spec['choice']))}"
    if isinstance(spec, dict) and "float" in spec:
        lo, hi = spec["float"]
        return round(rng.uniform(lo, hi), 2)
    if isinstance(spec, dict) and "int" in spec:
        lo, hi = spec["int"]
        return rng.randint(lo, hi)
    raise ValueError(f"unknown field spec {spec!r}")


def make_event(cfg, rng, now_ms):
    ev = {name: make_value(spec, rng) for name, spec in cfg["fields"].items()}
    for f, p in (cfg.get("null_rates") or {}).items():
        if f in ev and rng.random() < p:
            ev[f] = None
    for f, p in (cfg.get("type_break") or {}).items():       # wrong-typed value: schema violation, still valid JSON
        if f in ev and rng.random() < p:
            ev[f] = "N/A"
    lag = cfg.get("event_lag_ms", 0)
    ev["ts"] = now_ms - lag
    return ev


def phases_of(tcfg):
    return tcfg.get("phases") or [{"duration": tcfg.get("duration", 60)}]


def run_topic(topic, tcfg, send, stop, rng, dry=False):
    for ph in phases_of(tcfg):
        cfg = {**tcfg, **{k: v for k, v in ph.items() if k != "duration"}}
        end = time.time() + ph["duration"]
        while time.time() < end and not stop.is_set():
            t0 = time.time()
            rate = int(cfg.get("rate", 10))
            for i in range(rate):
                if cfg.get("bad_json_rate", 0) and rng.random() < cfg["bad_json_rate"]:
                    payload = b'{"order_id": "broken'            # truncated JSON
                else:
                    payload = json.dumps(make_event(cfg, rng, int(time.time() * 1000))).encode()
                send(topic, payload)
                if rate > 1 and not dry:
                    time.sleep(max(0, (t0 + (i + 1) / rate - time.time()) * 0.9))
            if dry:
                return
            time.sleep(max(0, 1 - (time.time() - t0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/kafka.properties")
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    scenario = yaml.safe_load(Path(a.scenario).read_text())
    stop = threading.Event()
    sent = {"n": 0}
    if a.dry_run:
        def send(topic, payload):
            if sent["n"] < 5:
                print(topic, payload.decode())
            sent["n"] += 1
        producer = None
    else:
        from kafka import KafkaProducer
        producer = KafkaProducer(**producer_kwargs(load_props(a.config)), linger_ms=20, acks="all")

        def send(topic, payload):
            producer.send(topic, payload)
            sent["n"] += 1

    threads = [threading.Thread(target=run_topic, args=(t, c, send, stop, random.Random(a.seed + i), a.dry_run))
               for i, (t, c) in enumerate(scenario["topics"].items())]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop.set()
    if producer:
        producer.flush()
    print(f"done: {sent['n']} messages sent")


if __name__ == "__main__":
    main()
