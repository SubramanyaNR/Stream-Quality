#!/bin/sh
# Sets the passwords of the job's (sq_writer) and Grafana's (sq_reader) database roles from the environment.
# Runs on first start of an empty volume (docker-entrypoint-initdb.d, after 01_init.sql created the roles) and from
# `make migrate`, so changing a password in cluster.env and running `make migrate` applies it. Never prints a password.
set -eu
: "${SQ_WRITER_PASSWORD:?SQ_WRITER_PASSWORD is not set (run make up, which generates it into cluster.env)}"
: "${SQ_READER_PASSWORD:?SQ_READER_PASSWORD is not set (run make up, which generates it into cluster.env)}"
psql -v ON_ERROR_STOP=1 -q -v wpw="$SQ_WRITER_PASSWORD" -v rpw="$SQ_READER_PASSWORD" \
     --username "${POSTGRES_USER:-postgres}" --dbname "${POSTGRES_DB:-sq}" <<'SQL'
ALTER ROLE sq_writer PASSWORD :'wpw';
ALTER ROLE sq_reader PASSWORD :'rpw';
SQL
echo "sq_writer / sq_reader passwords set"
