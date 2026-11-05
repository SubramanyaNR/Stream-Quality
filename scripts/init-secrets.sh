#!/usr/bin/env bash
# Creates cluster.env from cluster.env.example on first use and fills every empty password with a random one, so no two
# installs share credentials. Safe to re-run: existing values are never changed; keys added by a newer version are
# appended. cluster.env is git-ignored and made readable by its owner only. Never prints a password.
set -euo pipefail
cd "$(dirname "$0")/.."
created=0
if [ ! -f cluster.env ]; then cp cluster.env.example cluster.env; created=1; fi
chmod 600 cluster.env
gen() { if command -v openssl >/dev/null; then openssl rand -hex 16; else python3 -c 'import secrets; print(secrets.token_hex(16))'; fi; }
for key in POSTGRES_ADMIN_PASSWORD GRAFANA_ADMIN_PASSWORD SQ_WRITER_PASSWORD SQ_READER_PASSWORD; do
  if grep -q "^${key}=" cluster.env; then
    if grep -q "^${key}=$" cluster.env; then
      sed -i "s/^${key}=\$/${key}=$(gen)/" cluster.env
      echo "generated ${key}"
    fi
  else
    echo "${key}=$(gen)" >> cluster.env
    echo "generated ${key} (added)"
  fi
done
if [ "$created" = 1 ]; then
  echo "created cluster.env with generated passwords - set your Kafka connection in it (KAFKA_BOOTSTRAP etc.), then run make up again"
  exit 3
fi
