"""Alertmanager webhook receiver that stores every alert in PostgreSQL (sq.alert_events) so alert history can be
queried and charted next to the quality data. Also logs one JSON line per alert to stdout (docker compose logs).

Alertmanager webhook payload (version 4): {"status", "receiver", "alerts": [{"status", "labels", "annotations",
"startsAt", "endsAt", "fingerprint"}, ...]}. Repeated 'firing' notifications for the same alert instance are
no-ops (unique key fingerprint+status+startsAt). If the database is unavailable the handler answers HTTP 500, so
Alertmanager retries the notification instead of dropping it.

Env: POSTGRES_HOST (postgres) POSTGRES_PORT (5432) POSTGRES_DB (sq) POSTGRES_USER (sq_writer) POSTGRES_PASSWORD, PORT (5001)
"""
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from psycopg2.extras import Json

INSERT = """INSERT INTO sq.alert_events
              (status, fingerprint, alertname, severity, topic, check_type, field, starts_at, ends_at, summary, labels, annotations)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fingerprint, status, starts_at) DO NOTHING"""
ZERO_TIME = "0001-01-01"


def connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"), port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "sq"), user=os.environ.get("POSTGRES_USER", "sq_writer"),
        password=os.environ.get("POSTGRES_PASSWORD", "sq_writer_pw"), connect_timeout=3)


def ts(value):
    """RFC3339 -> text Postgres accepts (nanoseconds trimmed to microseconds); Go's zero time -> None."""
    if not value or value.startswith(ZERO_TIME):
        return None
    return re.sub(r"(\.\d{6})\d+", r"\1", value)


def rows_for(payload):
    for a in payload.get("alerts", []):
        labels, ann = a.get("labels", {}), a.get("annotations", {})
        status = a.get("status", payload.get("status", "firing"))
        yield (status, a.get("fingerprint") or json.dumps(labels, sort_keys=True), labels.get("alertname", "unknown"),
               labels.get("severity", ""), labels.get("topic", ""), labels.get("check_type", ""), labels.get("field", ""),
               ts(a.get("startsAt")), ts(a.get("endsAt")) if status == "resolved" else None,
               ann.get("summary", ""), Json(labels), Json(ann))


def store(payload, connect_fn=connect):
    """Insert all alerts of one webhook call in a single transaction. Returns the number of NEW rows."""
    rows = list(rows_for(payload))
    if not rows:
        return 0
    conn = connect_fn()
    try:
        with conn, conn.cursor() as cur:                    # `with conn` = one transaction, rolled back on error
            new = 0
            for r in rows:
                cur.execute(INSERT, r)
                new += cur.rowcount
            return new
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    connect_fn = staticmethod(connect)

    def _reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply(200, b"ok") if self.path == "/healthz" else self._reply(404)

    def do_POST(self):
        try:
            payload = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        except ValueError:
            return self._reply(400, b"invalid json")
        try:
            new = store(payload, self.connect_fn)
        except Exception as e:                              # DB down / rejected: let Alertmanager retry
            print(json.dumps({"event": "store_failed", "error": str(e)[:300]}), file=sys.stderr, flush=True)
            return self._reply(500, b"store failed")
        for a in payload.get("alerts", []):
            print(json.dumps({"event": "alert", "status": a.get("status"), "labels": a.get("labels"),
                              "summary": a.get("annotations", {}).get("summary"), "new": bool(new)}), flush=True)
        self._reply(200, b"stored")

    def log_message(self, *args):
        pass


def make_server(port, connect_fn=connect):
    h = type("BoundHandler", (Handler,), {"connect_fn": staticmethod(connect_fn)})
    return HTTPServer(("0.0.0.0", port), h)


if __name__ == "__main__":
    make_server(int(os.environ.get("PORT", "5001"))).serve_forever()
