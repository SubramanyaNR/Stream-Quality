#!/usr/bin/env python3
"""Registers example schemas in your registry. Reads type/url/group/auth from config/registry.properties.
  sq.registry.type=apicurio                   -> JSON Schemas from config/schemas/*.json via Apicurio's native v3 API
  sq.registry.type=confluent|karapace|redpanda|ccompat
                                              -> Confluent REST API: JSON Schemas (config/schemas/*.json, schemaType JSON) and
                                                 Avro schemas (config/schemas/avro/*.avsc, schemaType AVRO), subject = file stem
Idempotent: identical content is not re-versioned.
  python3 scripts/register_schemas.py [--config config/registry.properties] [--schemas config/schemas] [--url ...] [--type ...] [--avro]"""
import argparse
import base64
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config/registry.properties")
ap.add_argument("--schemas", default="config/schemas")
ap.add_argument("--url")
ap.add_argument("--type")
ap.add_argument("--no-avro", action="store_true", help="register JSON Schemas only")
a = ap.parse_args()

props = {}
for line in Path(a.config).read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        props[k.strip()] = v.strip()
rtype = (a.type or props.get("sq.registry.type", "apicurio")).lower()
url = (a.url or props["sq.registry.url"]).replace("host.docker.internal", "localhost").rstrip("/")
group = props.get("sq.registry.group", "default")
headers = {"Content-Type": "application/json"}
if props.get("sq.registry.auth.type") == "basic":
    cred = f'{props.get("sq.registry.auth.username", "")}:{props.get("sq.registry.auth.password", "")}'
    headers["Authorization"] = "Basic " + base64.b64encode(cred.encode()).decode()
elif props.get("sq.registry.auth.type") == "bearer":
    headers["Authorization"] = "Bearer " + props.get("sq.registry.auth.token", "")


def post(path, body, hdrs=None):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode(), headers={**headers, **(hdrs or {})}, method="POST")
    try:
        return json.loads(urllib.request.urlopen(req, timeout=15).read())
    except urllib.error.HTTPError as e:
        print(f"FAILED {path}: HTTP {e.code} {e.read().decode()[:300]}", file=sys.stderr)
        sys.exit(1)


def register_apicurio(f):
    content = f.read_text()
    json.loads(content)
    body = {"artifactId": f.stem, "artifactType": "JSON",
            "firstVersion": {"content": {"content": content, "contentType": "application/json"}}}
    r = post(f"/groups/{urllib.parse.quote(group)}/artifacts?ifExists=FIND_OR_CREATE_VERSION", body)
    v = r.get("version", {})
    print(f"registered {f.stem}: version {v.get('version')} (globalId {v.get('globalId')})")


def register_confluent(f, schema_type):
    content = f.read_text()
    json.loads(content)
    body = {"schema": content}
    if schema_type != "AVRO":
        body["schemaType"] = schema_type
    r = post(f"/subjects/{urllib.parse.quote(f.stem)}/versions", body, {"Content-Type": "application/vnd.schemaregistry.v1+json"})
    print(f"registered {f.stem} [{schema_type}]: id {r.get('id')}")


base = Path(a.schemas)
if rtype == "apicurio":
    for f in sorted(base.glob("*.json")):
        register_apicurio(f)
elif rtype in ("confluent", "karapace", "redpanda", "ccompat"):
    for f in sorted(base.glob("*.json")):
        register_confluent(f, "JSON")
    if not a.no_avro:
        # A subject holds ONE schema lineage (and one type), so the Avro examples get their own subjects (<name>-avro-value).
        # Messages in the Confluent wire format carry the schema ID, so the subject name does not matter for them.
        for f in sorted((base / "avro").glob("*.avsc")):
            content = f.read_text()
            json.loads(content)
            subj = f.stem.replace("-value", "-avro-value")
            r = post(f"/subjects/{urllib.parse.quote(subj)}/versions", {"schema": content}, {"Content-Type": "application/vnd.schemaregistry.v1+json"})
            print(f"registered {subj} [AVRO]: id {r.get('id')}")
else:
    sys.exit(f"unsupported registry type {rtype}")
