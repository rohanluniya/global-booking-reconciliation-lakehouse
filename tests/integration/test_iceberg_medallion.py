"""Integration tests for Apache Iceberg V2 medallion tables, time travel, compaction, and DQ."""

from __future__ import annotations

import pytest

from booking_ledger.config import Settings
from booking_ledger.iceberg_store import IcebergLakehouseStore, query_duckdb_sql
from booking_ledger.pipeline import run_full_pipeline


@pytest.mark.integration
def test_iceberg_v2_medallion_time_travel_and_compaction(isolated_settings: Settings) -> None:
    summary = run_full_pipeline(isolated_settings, seed=7705, num_bookings=80)
    assert summary["ledger_verification"]["balanced"] is True
    assert summary["dq_summary"]["all_passed"] is True
    assert summary["counts"]["quarantined_dlq_events"] >= 3

    store = IcebergLakehouseStore(isolated_settings)
    meta = store.list_table_metadata()
    assert len(meta) == 13
    assert all(t["format_version"] == 2 for t in meta)

    # Verify Iceberg V2 snapshot time-travel after appending an adjustment batch
    tbl = store.catalog.load_table("gold.ledger_journal_entries")
    snap_v1 = tbl.current_snapshot()
    assert snap_v1 is not None
    rows_v1 = store.read_table_rows("gold.ledger_journal_entries", snapshot_id=snap_v1.snapshot_id)

    # Append two extra snapshots to create multiple data files, then compact
    sample_two = rows_v1[:2]
    patched_a = [{**r, "entry_line_id": f"{r['entry_line_id']}_t1"} for r in sample_two]
    patched_b = [{**r, "entry_line_id": f"{r['entry_line_id']}_t2"} for r in sample_two]
    store.append_table("gold.ledger_journal_entries", patched_a)
    store.append_table("gold.ledger_journal_entries", patched_b)

    # Time travel back to snap_v1 returns exact historical row count
    historical_rows = store.read_table_rows("gold.ledger_journal_entries", snapshot_id=snap_v1.snapshot_id)
    assert len(historical_rows) == len(rows_v1)

    compact_res = store.compact_table("gold.ledger_journal_entries")
    assert compact_res["files_before"] >= 3
    assert compact_res["files_after"] == 1
    assert compact_res["row_count"] == len(rows_v1) + 4

    # Verify DuckDB analytical mirror matches Iceberg zero-sum invariant
    rows = query_duckdb_sql(
        isolated_settings.duckdb_path,
        "SELECT SUM(reporting_debit_minor) - SUM(reporting_credit_minor) AS diff FROM gold.ledger_journal_entries",
    )
    assert rows[0]["diff"] == 0
