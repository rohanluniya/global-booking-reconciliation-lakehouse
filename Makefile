.PHONY: setup pipeline dbt serve test chaos bench plot lint typecheck static licenses sbom verify portable clean

PYTHON := uv run python

setup:
	uv sync --extra dev --python 3.12

pipeline:
	$(PYTHON) scripts/run_pipeline.py --export-results

dbt:
	FINANCE_DUCKDB_PATH=$$(pwd)/.local_data/lakehouse.duckdb DBT_PROFILES_DIR=$$(pwd)/dbt_project uv run dbt build --project-dir dbt_project --profiles-dir dbt_project

serve:
	uv run uvicorn services.finance_api.main:app --host 127.0.0.1 --port 8040

test:
	uv run pytest -v --junitxml=results/tests/pytest_report.xml

chaos:
	$(PYTHON) scripts/run_chaos_drills.py

bench:
	$(PYTHON) benchmarks/run_benchmarks.py

plot:
	$(PYTHON) benchmarks/plot_benchmarks.py

lint:
	uv run ruff check .

typecheck:
	uv run mypy booking_ledger services scripts benchmarks

static:
	$(PYTHON) scripts/verify_static.py

licenses:
	$(PYTHON) scripts/check_licenses.py

sbom:
	$(PYTHON) scripts/generate_sbom.py

verify: lint typecheck static licenses sbom pipeline chaos bench plot test

portable:
	bash scripts/run_portable.sh

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache .hypothesis dbt_project/target dbt_project/logs
