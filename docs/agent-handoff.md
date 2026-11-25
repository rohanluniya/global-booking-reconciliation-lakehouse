# Agent Handoff & Resume Notes

## Current Repository State

- **Runtime Profile Verified:** `local_native` (Python `3.12.8`, `uv.lock`, PyIceberg V2 `0.12.0`, DuckDB `2.0.0`, `dbt-core==1.11.17`, `dbt-duckdb==1.11.1`, FastAPI `0.163.2`, Uvicorn `0.54.0`).
- **Live Server Endpoint:** `http://127.0.0.1:8040/` (`/health`, `/metrics`, `/v1/*`).
- **Core Verification Commands:**
  - `uv run ruff check .` -> 0 errors
  - `uv run mypy booking_ledger services scripts benchmarks` -> 0 errors
  - `uv run python scripts/verify_static.py` -> PASS (0 unpinned tags, 0 syntax errors, 0 hygiene leaks)
  - `uv run python scripts/run_pipeline.py --export-results` -> 0 imbalance, 12/12 DQ pass, 35/35 `dbt build` pass
  - `uv run python scripts/run_chaos_drills.py` -> 7/7 chaos drills pass
  - `uv run python benchmarks/run_benchmarks.py && uv run python benchmarks/plot_benchmarks.py` -> `smoke`/`local`/`stress` + 50k float64 drift audit + Iceberg compaction benchmark
  - `uv run pytest -v --junitxml=results/tests/pytest_report.xml` -> 22/22 tests pass

## Open Items & Recommended Next Engineering Steps

1. **Container Integration Verification:** On a Linux or macOS machine with Docker Engine installed, run `docker compose --profile core up -d`, execute `ingestion/cdc/register_connector.sh`, and verify live Debezium 3.7.1 -> Kafka 4.3.2 -> Lakekeeper v0.15.1 ingestion.
2. **Distributed Spark & Trino Validation:** Run `docker compose --profile analytics up -d` and execute `processing/spark/iceberg_ledger_reconciliation_job.py` and `sql/trino/finance_reconciliation_queries.sql` against Lakekeeper + RustFS.
3. **OIDC/JWT Identity Provider:** Replace local dev API keys in `services/finance_api/main.py` with OIDC/JWKS bearer token validation before any non-loopback deployment.
