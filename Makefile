.PHONY: demo demo-stop demo-data e2e migrate check test-alert-sink test-schema test-analytics up down logs build dlq status test clean schemas dashboards test-dashboards
-include cluster.env
ifndef KAFKA_BOOTSTRAP
-include cluster.env.example
endif
export
export COMPOSE_ENV_FILES=cluster.env

up: ## one command: env, DLQ topic, build image, start stack, submit job
	@./scripts/init-secrets.sh
	@./scripts/check-cluster.sh
	@./scripts/create-dlq-topic.sh
	docker compose up -d --build
	@./scripts/wait-for-stack.sh
down: ; docker compose down
clean: ; docker compose down -v
build: ; docker compose build
logs: ; docker compose logs -f --tail=100 sq-job flink-taskmanager flink-job-submit
dlq: ; ./scripts/create-dlq-topic.sh
check: ; ./scripts/check-cluster.sh
migrate: ; docker compose exec -T postgres sh -c 'psql -U sq_admin -d sq -v ON_ERROR_STOP=1 -q -f /docker-entrypoint-initdb.d/01_init.sql && POSTGRES_USER=sq_admin POSTGRES_DB=sq /docker-entrypoint-initdb.d/02_role_passwords.sh'   # upgrade an existing database to the current schema (idempotent)
status: ; docker compose ps && docker compose exec -T postgres psql -U sq_admin -d sq -c "SELECT check_type, status, count(*) FROM sq.check_results GROUP BY 1,2 ORDER BY 1,2"
test: ; cd flink-job && mvn -B test   # set SQ_TEST_PG_URL to also run the Postgres sink tests
demo: ; ./scripts/demo.sh   # live fault-injection traffic on topic demo.payments; watch Grafana (compact mode)
demo-stop: ; COMPOSE_ENV_FILES=cluster.env docker compose up -d sq-job   # back to your own topics and thresholds
demo-data: ; E2E_PG_HOST=127.0.0.1 E2E_PG_PORT=5432 E2E_PG_ADMIN=sq_admin E2E_PG_ADMIN_PASSWORD=$${POSTGRES_ADMIN_PASSWORD} python3 scripts/seed_demo.py   # 3 h of fake results so the dashboards have something to show (TRUNCATES results!)
e2e: ; ./scripts/e2e.sh   # needs Docker + a built jar; real Kafka/Postgres/job in containers
test-analytics: ; python3 scripts/test_analytics.py   # seeds a history, asserts exact analytics numbers (TRUNCATES the tables)
test-alert-sink: ; python3 scripts/test_alert_sink.py   # truncates sq.alert_events
test-schema: ; python3 scripts/test_schema.py $${E2E_PG_HOST:-127.0.0.1} $${E2E_PG_PORT:-5432} $${E2E_PG_ADMIN:-postgres} $${E2E_PG_DB:-sq}
schemas: ; python3 scripts/register_schemas.py
dashboards: ; python3 observability/grafana/build_dashboards.py
test-dashboards: ; python3 scripts/test_dashboard_queries.py --strict
