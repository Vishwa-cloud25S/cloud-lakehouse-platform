.DEFAULT_GOAL := help
PY := python3
ENV ?= dev

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

install: ## Install dev dependencies
	$(PY) -m pip install -r requirements-dev.txt

lint: ## Ruff + black --check
	ruff check src tests
	black --check src tests

format: ## Auto-format
	ruff check --fix src tests
	black src tests

typecheck: ## mypy static analysis
	mypy

test: ## Unit tests
	pytest tests/unit

test-all: ## Unit + integration (needs Java 17 + pyspark)
	pytest tests --cov=src/lakehouse --cov-report=term-missing --cov-report=xml

seed: ## Generate sample source data
	$(PY) scripts/generate_sample_data.py --out data/samples --days 14 --evolve

demo: ## Run the whole medallion pipeline locally on Delta
	$(PY) scripts/run_local_pipeline.py --env local

bronze: ## Incremental Bronze ingest
	$(PY) -m lakehouse.jobs.ingest_bronze --env $(ENV) --dataset orders

silver: ## Build Silver (dedupe, conform, DQ, SCD2)
	$(PY) -m lakehouse.jobs.build_silver --env $(ENV) --dataset orders

gold: ## Build Gold star schema + aggregates
	$(PY) -m lakehouse.jobs.build_gold --env $(ENV)

maintenance: ## OPTIMIZE / ZORDER / VACUUM / ANALYZE
	$(PY) -m lakehouse.jobs.run_maintenance --env $(ENV)

tf-plan: ## Terraform plan for $(ENV)
	cd infra/terraform && terraform init -backend-config=envs/$(ENV)/backend.hcl && terraform plan -var-file=envs/$(ENV)/terraform.tfvars

tf-apply: ## Terraform apply for $(ENV)
	cd infra/terraform && terraform apply -var-file=envs/$(ENV)/terraform.tfvars

clean: ## Remove local build/spark artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov coverage.xml spark-warehouse metastore_db derby.log data/lake data/_checkpoints
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +

.PHONY: help install lint format typecheck test test-all seed demo bronze silver gold maintenance tf-plan tf-apply clean
