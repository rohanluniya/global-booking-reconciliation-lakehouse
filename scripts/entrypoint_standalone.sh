#!/usr/bin/env bash
set -euo pipefail

export FINANCE_DATA_DIR="${FINANCE_DATA_DIR:-/app/.local_data}"
export FINANCE_API_HOST="${FINANCE_API_HOST:-0.0.0.0}"
export FINANCE_API_PORT="${FINANCE_API_PORT:-8040}"

echo "[entrypoint] Running initial Global Booking & Financial Reconciliation Lakehouse pipeline + dbt build..."
python -m scripts.run_pipeline --export-results

echo "[entrypoint] Launching Finance Control Plane API & Dashboard on ${FINANCE_API_HOST}:${FINANCE_API_PORT}..."
exec uvicorn services.finance_api.main:app --host "${FINANCE_API_HOST}" --port "${FINANCE_API_PORT}"
