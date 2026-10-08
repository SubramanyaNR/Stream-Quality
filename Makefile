.PHONY: check test-alert-sink test-schema test-analytics up down logs build dlq status test clean schemas dashboards test-dashboards
-include cluster.env
ifndef KAFKA_BOOTSTRAP
-include cluster.env.example
endif
export
export COMPOSE_ENV_FILES=cluster.env

up: ## one command: env, DLQ topic, build image, start stack, submit job
	@test -f cluster.env || { cp cluster.env.example cluster.env; echo "created cluster.env - edit it for your cluster, then run make up again"; exit 1; }
	@./scripts/check-cluster.sh
	@./scripts/create-dlq-topic.sh
	docker compose up -d --build
	@./scripts/wait-for-stack.sh
down: ; docker compose down
clean: ; docker compose down -v
build: ; docker compose build
logs: ; docker compose logs -f --tail=100 flink-taskmanager flink-job-submit
dlq: ; ./scripts/create-dlq-topic.sh
check: ; ./scripts/check-cluster.sh
status: ; docker compose ps && docker compose exec -T postgres psql -U sq_admin -d sq -c "SELECT check_type, status, count(*) FROM sq.check_results GROUP BY 1,2 ORDER BY 1,2"
test: ; cd flink-job && mvn -B test   # set SQ_TEST_PG_URL to also run the Postgres sink tests
test-analytics: ; python3 scripts/test_analytics.py   # seeds a history, asserts exact analytics numbers (TRUNCATES the tables)
test-alert-sink: ; python3 scripts/test_alert_sink.py   # truncates sq.alert_events
test-schema: ; python3 scripts/test_schema.py $${E2E_PG_HOST:-127.0.0.1} $${E2E_PG_PORT:-5432} $${E2E_PG_ADMIN:-postgres} $${E2E_PG_DB:-sq}
schemas: ; python3 scripts/register_schemas.py
dashboards: ; python3 observability/grafana/build_dashboards.py
test-dashboards: ; python3 scripts/test_dashboard_queries.py --strict
