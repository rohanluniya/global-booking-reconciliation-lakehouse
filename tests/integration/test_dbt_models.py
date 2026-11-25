"""Integration test executing real `dbt build` (staging + marts + singular zero-sum tests) on DuckDB."""

from __future__ import annotations

from pathlib import Path

import pytest

from booking_ledger.config import Settings
from booking_ledger.iceberg_store import query_duckdb_sql
from booking_ledger.pipeline import run_full_pipeline
from scripts.run_pipeline import run_dbt_build


@pytest.mark.integration
def test_dbt_build_and_financial_marts(isolated_settings: Settings, tmp_path: Path) -> None:
    run_full_pipeline(isolated_settings, seed=7706, num_bookings=70)
    dbt_summary = run_dbt_build(isolated_settings.duckdb_path, tmp_path / "dbt_out")
    assert dbt_summary["returncode"] == 0, dbt_summary
    assert int(dbt_summary["total_nodes"]) >= 30
    assert dbt_summary["failed_nodes"] == 0

    tb_rows = query_duckdb_sql(
        isolated_settings.duckdb_path,
        "SELECT SUM(total_reporting_debit_minor) - SUM(total_reporting_credit_minor) AS diff FROM marts.mart_trial_balance",
    )
    assert tb_rows[0]["diff"] == 0

    rev_rows = query_duckdb_sql(
        isolated_settings.duckdb_path,
        "SELECT COUNT(*) AS cnt FROM marts.mart_revenue_recognition",
    )
    assert rev_rows[0]["cnt"] >= 2
