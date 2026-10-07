"""Shared Postgres access for the test scripts. Connection comes from the environment (libpq-style defaults):
E2E_PG_HOST (127.0.0.1) E2E_PG_PORT (5432) E2E_PG_DB (sq) E2E_PG_ADMIN (postgres) E2E_PG_ADMIN_PASSWORD ('')."""
import os

import psycopg2
import psycopg2.extras

HOST = os.environ.get("E2E_PG_HOST", "127.0.0.1")
PORT = int(os.environ.get("E2E_PG_PORT", "5432"))
DB = os.environ.get("E2E_PG_DB", "sq")
ADMIN = os.environ.get("E2E_PG_ADMIN", "postgres")
ADMIN_PW = os.environ.get("E2E_PG_ADMIN_PASSWORD", "")
JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"


def connect(user=None, password=None):
    c = psycopg2.connect(host=HOST, port=PORT, dbname=DB, user=user or ADMIN, password=password if user else ADMIN_PW)
    c.autocommit = True
    return c


def query(sql, params=None, user=None, password=None):
    """Rows as dicts."""
    with connect(user, password).cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def execute(sql, user=None, password=None):
    with connect(user, password).cursor() as cur:
        cur.execute(sql)
