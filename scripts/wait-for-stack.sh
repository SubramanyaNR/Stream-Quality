#!/usr/bin/env bash
set -euo pipefail
for url in http://localhost:8082/overview http://localhost:9090/-/ready http://localhost:3000/api/health; do
  printf 'waiting for %s ' "$url"
  for _ in $(seq 1 60); do curl -sf "$url" >/dev/null && { echo ok; break; } || { printf .; sleep 2; }; done
done
echo "Postgres: localhost:5432 (db sq)  Grafana: http://localhost:3000  Flink: http://localhost:8082  Prometheus: http://localhost:9090  Alertmanager: http://localhost:9093"
