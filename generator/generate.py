#!/usr/bin/env python3
"""Test event generator: produces JSON events to Kafka with scripted faults.

Reads the SAME config/kafka.properties the Flink job uses (sq.* keys ignored, ${env:X} resolved).

  python3 generator/generate.py --scenario generator/scenarios/null_spike.yaml
  python3 generator/generate.py --scenario ... --dry-run     # print a few events, no Kafka

Serialization (--serialization):
  json              plain JSON                                                  (default)
  confluent-json    Confluent wire format (0x00 + 4-byte schema id) + JSON, schema from config/schemas/<topic>-value.json
  confluent-avro    Confluent wire format + Avro binary, schema from config/schemas/avro/<topic>-value.avsc
The two confluent-* modes register the schema (idempotently) in the registry given by --registry-url, which can be Confluent
Schema Registry, Karapace, Redpanda or Apicurio's ccompat endpoint. Avro cannot carry a wrong-typed value, so `type_break` is
ignored with confluent-avro (the serializer enforces the schema, which is exactly what a real producer would hit).

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
import io
import json
import struct
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


def register(registry_url, subject, schema_text, schema_type):
    """Idempotent: returns the existing id when the same schema is already registered. Works against any Confluent-API registry."""
    import urllib.request
    body = {"schema": schema_text}
    if schema_type != "AVRO":
        body["schemaType"] = schema_type
    req = urllib.request.Request(f"{registry_url.rstrip('/')}/subjects/{subject}/versions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=15).read())["id"]


def frame(schema_id, body):
    """Confluent wire format: magic byte 0, 4-byte big-endian schema id, then the serialized payload."""
    return b"\x00" + struct.pack(">I", schema_id) + body


def make_serializer(mode, topic, registry_url, schemas_dir):
    if mode == "json":
        return lambda ev: json.dumps(ev).encode()
    if not registry_url:
        sys.exit(f"--serialization {mode} needs --registry-url")
    base = Path(schemas_dir)
    if mode == "confluent-json":
        sid = register(registry_url, f"{topic}-value", (base / f"{topic}-value.json").read_text(), "JSON")
        return lambda ev: frame(sid, json.dumps(ev).encode())
    if mode == "confluent-avro":
        import fastavro
        text = (base / "avro" / f"{topic}-value.avsc").read_text()
        sid = register(registry_url, f"{topic}-avro-value", text, "AVRO")
        parsed = fastavro.parse_schema(json.loads(text))

        def ser(ev):
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, parsed, ev)
            return frame(sid, buf.getvalue())
        return ser
    sys.exit(f"unknown serialization {mode}")


def phases_of(tcfg):
    return tcfg.get("phases") or [{"duration": tcfg.get("duration", 60)}]


def run_topic(topic, tcfg, send, stop, rng, ser, dry=False):
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
                    payload = ser(make_event(cfg, rng, int(time.time() * 1000)))
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
    ap.add_argument("--bootstrap", help="override bootstrap.servers (the config file usually points at host.docker.internal, which only resolves inside containers)")
    ap.add_argument("--serialization", default="json", choices=["json", "confluent-json", "confluent-avro"])
    ap.add_argument("--registry-url", help="Confluent-API registry (Confluent SR / Karapace / Redpanda / Apicurio ccompat) for confluent-* modes")
    ap.add_argument("--schemas-dir", default="config/schemas")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    scenario = yaml.safe_load(Path(a.scenario).read_text())
    stop = threading.Event()
    sent = {"n": 0}
    if a.dry_run:
        def send(topic, payload):
            if sent["n"] < 5:
                print(topic, payload.decode(errors="replace"))
            sent["n"] += 1
        producer = None
    else:
        from kafka import KafkaProducer
        props = load_props(a.config)
        if a.bootstrap:
            props["bootstrap.servers"] = a.bootstrap
        producer = KafkaProducer(**producer_kwargs(props), linger_ms=20, acks="all")

        def send(topic, payload):
            producer.send(topic, payload)
            sent["n"] += 1

    sers = {}
    for t, c in scenario["topics"].items():
        sers[t] = make_serializer(a.serialization, t, a.registry_url, a.schemas_dir)
        if a.serialization == "confluent-avro":
            c.pop("type_break", None)                      # an Avro serializer cannot emit a wrong-typed value
            for ph in c.get("phases") or []:
                ph.pop("type_break", None)
    threads = [threading.Thread(target=run_topic, args=(t, c, send, stop, random.Random(a.seed + i), sers[t], a.dry_run))
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
