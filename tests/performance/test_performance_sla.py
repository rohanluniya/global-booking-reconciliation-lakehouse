"""Performance SLA & zero-drift regression gate for smoke profile."""

from __future__ import annotations

import pytest

from benchmarks.run_benchmarks import measure_profile, run_float_drift_benchmark


@pytest.mark.performance
def test_smoke_profile_throughput_and_zero_imbalance() -> None:
    res = measure_profile("smoke", 150, 20260501)
    assert res["trial_balance_imbalance_minor"] == 0
    assert res["e2e_iceberg_duckdb_ms"] < 15_000.0
    assert res["ledger_lines_per_sec"] > 500.0

    drift = run_float_drift_benchmark(5_000)
    assert drift["per_tx_mismatches"] > 0
