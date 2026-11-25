#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: 'uv' is required for zero-Docker portable execution. Install from https://docs.astral.sh/uv/"
  exit 1
fi

export FINANCE_API_HOST="${FINANCE_API_HOST:-127.0.0.1}"
export FINANCE_API_PORT="${FINANCE_API_PORT:-8040}"

echo "[1/3] Syncing pinned Python 3.12 virtual environment..."
uv sync --extra dev --python 3.12

echo "[2/3] Running Apache Iceberg V2 medallion pipeline, double-entry ledger, 3-way recon & dbt build..."
uv run python scripts/run_pipeline.py --export-results

echo "[3/3] Starting Finance Control Plane & Dashboard at http://${FINANCE_API_HOST}:${FINANCE_API_PORT}/ ..."
exec uv run uvicorn services.finance_api.main:app --host "${FINANCE_API_HOST}" --port "${FINANCE_API_PORT}"
