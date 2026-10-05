.PHONY: up down logs build dlq status test gen clean
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
status: ; docker compose ps && curl -s localhost:8123 --data-binary "SELECT check_type,status,count() FROM sq.check_results GROUP BY 1,2" -u default:$${CLICKHOUSE_ADMIN_PASSWORD:-admin}
test: ; cd flink-job && mvn -B test
gen: ; python3 generator/generate.py --config config/kafka.properties --scenario generator/scenarios/healthy.yaml
