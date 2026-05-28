.PHONY: help install install-dev lint format format-check type-check test test-cov \
        test-quick test-integration quality \
        docker-up docker-down docker-logs docker-ps docker-restart docker-build \
        db-init-gp db-create-tables db-connect-gp db-connect-ch \
        etl-extract etl-load etl-full \
        clean clean-all dev-setup

# ============================================================
# Meta
# ============================================================

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ============================================================
# Installation
# ============================================================

install:  ## Install production dependencies
	pip install -r requirements.txt

install-dev:  ## Install all dependencies (including dev tools)
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pip install -e .

# ============================================================
# Code Quality
# ============================================================

lint:  ## Run pylint on src/
	pylint src/

format:  ## Auto-format with black + isort
	black src/ tests/
	isort src/ tests/

format-check:  ## Check formatting without modifying files
	black --check src/ tests/
	isort --check-only src/ tests/

type-check:  ## Run mypy static type checker
	mypy src/

quality:  ## Run ALL quality checks (format → types → lint → tests)
	@echo "━━━ format-check ━━━"
	@$(MAKE) format-check
	@echo "\n━━━ type-check ━━━"
	@$(MAKE) type-check
	@echo "\n━━━ lint ━━━"
	@$(MAKE) lint
	@echo "\n━━━ tests ━━━"
	@$(MAKE) test
	@echo "\n✅  All quality checks passed!"

# ============================================================
# Testing
# ============================================================

test:  ## Run unit tests (no infrastructure needed)
	pytest tests/ -v -m "not integration"

test-cov:  ## Run unit tests with HTML coverage report
	pytest tests/ -m "not integration" \
	  --cov=src --cov-report=html --cov-report=term-missing

test-quick:  ## Run fast tests only (skip @pytest.mark.slow)
	pytest tests/ -v -m "not integration and not slow"

test-integration:  ## Run integration tests (requires Docker Compose up)
	pytest tests/ -v -m "integration"

# ============================================================
# Docker
# ============================================================

docker-up:  ## Start all Docker services (detached)
	docker compose up -d

docker-down:  ## Stop and remove containers
	docker compose down

docker-logs:  ## Follow logs from all containers
	docker compose logs -f

docker-ps:  ## Show running containers and their status
	docker compose ps

docker-restart:  ## Restart all containers
	docker compose restart

docker-build:  ## Rebuild images (after Dockerfile changes)
	docker compose build --no-cache

# ============================================================
# Database
# ============================================================

db-init-gp:  ## Run Greenplum init script (schemas + test table)
	psql -h $${GP_HOST:-localhost} -p $${GP_PORT:-5432} \
	     -U $${GP_USER:-gpadmin} -d $${GP_DATABASE:-jobs_db} \
	     -f scripts/init_gp.sql

db-create-tables:  ## Create all Greenplum tables (staging / core / marts)
	psql -h $${GP_HOST:-localhost} -p $${GP_PORT:-5432} \
	     -U $${GP_USER:-gpadmin} -d $${GP_DATABASE:-jobs_db} \
	     -f scripts/create_tables.sql

db-connect-gp:  ## Open a psql shell to Greenplum
	psql -h $${GP_HOST:-localhost} -p $${GP_PORT:-5432} \
	     -U $${GP_USER:-gpadmin} -d $${GP_DATABASE:-jobs_db}

db-connect-ch:  ## Open a clickhouse-client shell
	docker exec -it clickhouse clickhouse-client \
	     --user=$${CLICKHOUSE_USER:-chadmin} \
	     --password=$${CLICKHOUSE_PASSWORD:-}

# ============================================================
# ETL
# ============================================================

etl-extract:  ## Run hh.ru extractor manually
	python -m src.extractors.hh_extractor

etl-load:  ## Upload last extraction result to MinIO
	python -m src.loaders.s3_loader

etl-full:  ## Run full ETL cycle (extract → transform → load)
	@echo "━━━ Extracting from hh.ru ━━━"
	@$(MAKE) etl-extract
	@echo "\n━━━ Loading to MinIO ━━━"
	@$(MAKE) etl-load
	@echo "\n✅  ETL cycle complete!"

# ============================================================
# Cleanup
# ============================================================

clean:  ## Remove Python cache, coverage and build artifacts
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name ".mypy_cache"   -exec rm -rf {} + 2>/dev/null; true
	rm -rf htmlcov/ .coverage

clean-all:  ## Full clean including Docker volumes and Airflow logs
	@$(MAKE) clean
	docker compose down -v
	rm -rf airflow/logs/*

# ============================================================
# First-time dev setup
# ============================================================

dev-setup:  ## Bootstrap a fresh development environment
	@echo "━━━ Copying .env.example → .env ━━━"
	cp config/.env.example .env
	@echo "\n━━━ Installing dependencies ━━━"
	@$(MAKE) install-dev
	@echo "\n━━━ Creating runtime directories ━━━"
	mkdir -p airflow/dags airflow/logs airflow/plugins
	mkdir -p src/extractors src/loaders src/transformers src/utils
	mkdir -p tests config/grafana/provisioning
	@echo "\n━━━ Starting Docker services ━━━"
	@$(MAKE) docker-up
	@echo "\n✅  Dev setup complete! Edit .env, then run: make db-create-tables"
