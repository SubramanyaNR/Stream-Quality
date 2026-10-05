#!/usr/bin/env python3
"""Registers config/schemas/<artifactId>.json in an Apicurio v3 registry (artifactId = file name stem, i.e. '<topic>-value').
Reads url/group/auth from config/registry.properties. Idempotent: identical content is not re-versioned.
  python3 scripts/register_schemas.py [--config config/registry.properties] [--schemas config/schemas] [--url ...]"""
import argparse
import base64
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config/registry.properties")
ap.add_argument("--schemas", default="config/schemas")
ap.add_argument("--url")
a = ap.parse_args()

props = {}
for line in Path(a.config).read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        props[k.strip()] = v.strip()
url = (a.url or props["sq.registry.url"]).replace("host.docker.internal", "localhost").rstrip("/")
group = props.get("sq.registry.group", "default")
headers = {"Content-Type": "application/json"}
if props.get("sq.registry.auth.type") == "basic":
    cred = f'{props.get("sq.registry.auth.username", "")}:{props.get("sq.registry.auth.password", "")}'
    headers["Authorization"] = "Basic " + base64.b64encode(cred.encode()).decode()

for f in sorted(Path(a.schemas).glob("*.json")):
    content = f.read_text()
    json.loads(content)                                    # fail early on invalid JSON
    body = {"artifactId": f.stem, "artifactType": "JSON",
            "firstVersion": {"content": {"content": content, "contentType": "application/json"}}}
    req = urllib.request.Request(f"{url}/groups/{urllib.parse.quote(group)}/artifacts?ifExists=FIND_OR_CREATE_VERSION",
                                 data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        r = json.loads(urllib.request.urlopen(req, timeout=15).read())
        v = r.get("version", {})
        print(f"registered {f.stem}: version {v.get('version')} (globalId {v.get('globalId')})")
    except urllib.error.HTTPError as e:
        print(f"FAILED {f.stem}: HTTP {e.code} {e.read().decode()[:300]}", file=sys.stderr)
        sys.exit(1)
