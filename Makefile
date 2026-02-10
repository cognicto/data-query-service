# Sensor Data Query Service Makefile

.PHONY: help build run dev test clean install lint format docker-build docker-push

# Default target
help: ## Show this help message
	@echo "Sensor Data Query Service"
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Development
install: ## Install dependencies
	pip install -r requirements.txt

dev: ## Start development environment
	./scripts/start-dev.sh

run: ## Run service locally
	python -m app.main

test: ## Run tests
	pytest tests/ -v

test-api: ## Run API integration tests
	python scripts/test-queries.py

lint: ## Run linting
	flake8 app/ tests/
	mypy app/

format: ## Format code
	black app/ tests/ scripts/
	isort app/ tests/ scripts/

# Docker
docker-build: ## Build Docker image
	docker build -t sensor-query-service:latest .

docker-run: ## Run Docker container
	docker run --env-file .env -p 8080:8080 sensor-query-service:latest

docker-push: ## Push to registry (set REGISTRY variable)
	@if [ -z "$(REGISTRY)" ]; then echo "Set REGISTRY variable"; exit 1; fi
	docker tag sensor-query-service:latest $(REGISTRY)/sensor-query-service:latest
	docker push $(REGISTRY)/sensor-query-service:latest

# Compose
compose-up: ## Start with docker-compose
	docker-compose up -d

compose-dev: ## Start development stack with monitoring
	docker-compose --profile monitoring up -d

compose-down: ## Stop docker-compose
	docker-compose down

compose-logs: ## View logs
	docker-compose logs -f query-service

# Kubernetes
k8s-deploy: ## Deploy to Kubernetes
	kubectl apply -f deployment/kubernetes/

k8s-delete: ## Delete from Kubernetes
	kubectl delete -f deployment/kubernetes/

k8s-logs: ## View Kubernetes logs
	kubectl logs -f deployment/query-service -n sensor-query

# Monitoring
health: ## Check service health
	curl -f http://localhost:8080/health | jq

stats: ## Get service statistics
	curl http://localhost:8080/api/v1/stats | jq

metrics: ## Get Prometheus metrics
	curl http://localhost:8080/metrics

# Performance testing
load-test: ## Run basic load test
	@echo "Running load test..."
	@for i in {1..10}; do \
		curl -s -w "Response: %{http_code}, Time: %{time_total}s\n" \
		-o /dev/null \
		"http://localhost:8080/api/v1/query?sensors=quad_ch1&start_time=2024-01-01T00:00:00Z&end_time=2024-01-01T01:00:00Z" \
		|| true; \
	done

# Cache operations
cache-clear: ## Clear query cache
	curl -X POST http://localhost:8080/api/v1/cache/clear

# Data operations
list-sensors: ## List available sensors
	curl http://localhost:8080/api/v1/sensors | jq

list-assets: ## List available assets
	curl http://localhost:8080/api/v1/assets | jq

# Cleanup
clean: ## Clean up containers and images
	docker-compose down -v
	docker system prune -f

# Documentation
docs-serve: ## Serve API documentation
	@echo "API Documentation available at:"
	@echo "  Swagger UI: http://localhost:8080/docs"
	@echo "  ReDoc: http://localhost:8080/redoc"