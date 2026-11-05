# Security policy

## Reporting a vulnerability
Please do **not** open a public issue for security problems. Use GitHub's private vulnerability reporting
(the repository's **Security** tab → **Report a vulnerability**). You should get an answer within a week.
This is a small open-source project without a paid security team: fixes are best-effort.

Supported version: the latest release only.

## How the stack is meant to be run
Stream Quality is a self-hosted monitor for a team's own Kafka cluster. Out of the box:

- **No shared default passwords.** `make up` generates a random password for every account (Postgres admin, the job's
  `sq_writer` role, Grafana's `sq_reader` role, Grafana `admin`) into `cluster.env`. That file is git-ignored and
  `chmod 600`. Never commit it. Rotate a password by editing `cluster.env`, then `make migrate` and `docker compose up -d`.
- **Least privilege in Postgres.** The job can only insert/update results; the alert receiver can only insert alerts;
  Grafana's role is read-only.
- **Published ports bind to `127.0.0.1`** (`SQ_BIND_ADDRESS`). Grafana, Prometheus, Alertmanager, Postgres and the
  alert receiver are then reachable from this machine only.

## Things to know before exposing anything
- **Do not put Grafana, Prometheus, Alertmanager or Postgres on the public internet.** Prometheus and Alertmanager have no
  authentication. If people need remote access, use a VPN or an authenticating reverse proxy (with TLS) and give them
  Grafana *viewer* accounts.
- **Grafana runs with HTML sanitizing disabled** (`GF_PANELS_DISABLE_SANITIZE_HTML=true`) so the dashboards' header panel
  can carry their stylesheet. Anyone allowed to *edit* a dashboard could therefore add scripts. Keep editor rights to
  administrators.
- **Secrets are environment variables inside the containers**, so anyone with Docker access on the host can read them
  (`docker inspect`). Treat Docker access as administrator access. For stricter setups use Docker secrets or a secrets manager.
- **Kafka and schema-registry credentials** go in `cluster.env` and are referenced from `config/*.properties` as
  `${env:NAME}`. TLS truststores go in `config/certs/` (git-ignored).
- **Dead-letter records contain the original payload.** If your topics carry personal data, the dead-letter topic does too:
  give it the same ACLs and retention as the source topics.
