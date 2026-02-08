.DEFAULT_GOAL := help
SOURCE_DIR = ./src
PY_VERSIONS = 3.10 3.11 3.12 3.13 3.14 3.13t 3.14t 3.15 3.15t
export UV_MANAGED_PYTHON ?= 1

##@ CI/CD
.PHONY: build
build: ## Build
	uv build

.PHONY: cov
cov: ## Run tests with coverage
	uv run pytest -n auto --cov-report=term-missing --cov-config=pyproject.toml --cov=diskstore

##@ Quality
.PHONY: test
test: ## Run tests in current Python
	uv run pytest --lf -n auto

.PHONY: tests
tests: ## Run tests in all supporte Python versions
	for py_v in $(PY_VERSIONS); do \
		uv run --isolated -p $$py_v pytest -n auto; \
	done

.PHONY: check
check: ## Run all checks
	-uvx ty check ${SOURCE_DIR}
	uvx ruff check ${SOURCE_DIR}

.PHONY: ruff-check
ruff-check: ## Lint using ruff
	uvx ruff check ${SOURCE_DIR}

.PHONY: type-check
type-check: ## Type check with
	-uvx ty check ${SOURCE_DIR}
	uvx pyrefly check ${SOURCE_DIR}

.PHONY: format
format: ## Format files using ruff format
	uvx ruff format ${SOURCE_DIR}

.PHONY: bench
bench: ## run benchmarks
	#uv run --isolated --group benchmark scripts/benchmark_core.py
	uv run scripts/benchmark_core.py -p 1

.PHONY: bench-kv
bench-kv: ## run benchmarks kv
	uv run --isolated --group benchmark python -m IPython scripts/benchmark_kv_store.py

.PHONY: docs
docs: ## build docs
	cd docs && $(MAKE) html

##@ Utility
.PHONY: clean
clean: ## Delete all temporary files
	rm -rf .pytest_cache
	rm -rf **/.pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf __pycache__
	rm -rf **/__pycache__
	rm -rf build
	rm -rf dist
	rm -f .coverage

.PHONY: install
install: install-uv ## Install virtual environment
	uv sync --frozen

.PHONY: install-uv
install-uv: ## Install uv
	@command -v uv >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh

.PHONY: update-uv
update-uv: ## Update uv
	uv self update

.PHONY: update-lock
update-lock: ## Update lockfile
	uv lock --upgrade


.PHONY: help
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
