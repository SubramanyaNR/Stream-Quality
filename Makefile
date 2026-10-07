.PHONY: test-schema up down logs build dlq status test gen clean schemas dashboards test-dashboards e2e e2e-outage
up: ## one command: env, DLQ topic, build image, start stack, submit job
	@test -f .env || cp .env.example .env
	@./scripts/create-dlq-topic.sh
	docker compose up -d --build
	@./scripts/wait-for-stack.sh
down: ; docker compose down
clean: ; docker compose down -v
build: ; docker compose build
logs: ; docker compose logs -f --tail=100 flink-taskmanager flink-job-submit
dlq: ; ./scripts/create-dlq-topic.sh
status: ; docker compose ps && docker compose exec -T postgres psql -U sq_admin -d sq -c "SELECT check_type, status, count(*) FROM sq.check_results GROUP BY 1,2 ORDER BY 1,2"
test: ; cd flink-job && mvn -B test   # set SQ_TEST_PG_URL to also run the Postgres sink tests
test-schema: ; python3 scripts/test_schema.py $${E2E_PG_HOST:-127.0.0.1} $${E2E_PG_PORT:-5432} $${E2E_PG_ADMIN:-postgres} $${E2E_PG_DB:-sq}
schemas: ; python3 scripts/register_schemas.py
dashboards: ; python3 observability/grafana/build_dashboards.py
test-dashboards: ; python3 scripts/test_dashboard_queries.py --strict
e2e: ; ./scripts/e2e-local.sh
e2e-outage: ; ./scripts/e2e-registry-outage.sh
SCENARIO ?= healthy
BOOTSTRAP ?= localhost:9092
gen: ; python3 generator/generate.py --bootstrap $(BOOTSTRAP) --scenario generator/scenarios/$(SCENARIO).yaml
