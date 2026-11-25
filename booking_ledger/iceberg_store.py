"""Apache Iceberg V2 Medallion Lakehouse store (PyIceberg SQLite catalog + DuckDB views).

Manages 13 Apache Iceberg V2 tables across bronze, silver, gold, and quality namespaces:
  - bronze.cdc_booking_events
  - bronze.psp_settlements
  - bronze.bank_statements
  - bronze.quarantine_dlq
  - silver.booking_state_history
  - silver.dim_listings_scd2
  - silver.dim_tax_policies_scd2
  - silver.fx_rates_asof
  - gold.ledger_journal_entries
  - gold.reconciliation_results
  - gold.reconciliation_exceptions
  - gold.period_close_snapshots
  - quality.dq_results
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

from booking_ledger.config import Settings

TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "bronze.cdc_booking_events": pa.schema(
        [
            pa.field("event_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("listing_id", pa.string()),
            pa.field("guest_id", pa.string()),
            pa.field("host_id", pa.string()),
            pa.field("event_type", pa.string()),
            pa.field("cdc_op", pa.string()),
            pa.field("sequence_num", pa.int64()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
            pa.field("host_timezone", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("nights", pa.int64()),
            pa.field("room_subtotal_minor", pa.int64()),
            pa.field("platform_fee_minor", pa.int64()),
            pa.field("host_fee_minor", pa.int64()),
            pa.field("tax_minor", pa.int64()),
            pa.field("gross_total_minor", pa.int64()),
            pa.field("refund_minor", pa.int64()),
            pa.field("chargeback_minor", pa.int64()),
            pa.field("tax_policy_version", pa.string()),
            pa.field("jurisdiction", pa.string()),
            pa.field("payment_reference", pa.string()),
            pa.field("source_lsn", pa.int64()),
        ]
    ),
    "bronze.psp_settlements": pa.schema(
        [
            pa.field("settlement_id", pa.string()),
            pa.field("file_batch_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("payment_reference", pa.string()),
            pa.field("settlement_type", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("gross_minor", pa.int64()),
            pa.field("psp_fee_minor", pa.int64()),
            pa.field("net_minor", pa.int64()),
            pa.field("settled_at", pa.string()),
            pa.field("bank_transfer_ref", pa.string()),
        ]
    ),
    "bronze.bank_statements": pa.schema(
        [
            pa.field("statement_line_id", pa.string()),
            pa.field("bank_account", pa.string()),
            pa.field("bank_transfer_ref", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("payment_reference", pa.string()),
            pa.field("original_currency", pa.string()),
            pa.field("settled_original_minor", pa.int64()),
            pa.field("reporting_currency", pa.string()),
            pa.field("settled_reporting_minor", pa.int64()),
            pa.field("fx_rate_applied_micros", pa.int64()),
            pa.field("posted_at", pa.string()),
        ]
    ),
    "bronze.quarantine_dlq": pa.schema(
        [
            pa.field("dlq_id", pa.string()),
            pa.field("source_topic", pa.string()),
            pa.field("reason", pa.string()),
            pa.field("raw_payload_json", pa.string()),
            pa.field("quarantined_at", pa.string()),
        ]
    ),
    "silver.booking_state_history": pa.schema(
        [
            pa.field("state_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("event_id", pa.string()),
            pa.field("listing_id", pa.string()),
            pa.field("guest_id", pa.string()),
            pa.field("host_id", pa.string()),
            pa.field("sequence_num", pa.int64()),
            pa.field("event_type", pa.string()),
            pa.field("booking_status", pa.string()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
            pa.field("valid_from", pa.string()),
            pa.field("valid_to", pa.string()),
            pa.field("is_current", pa.bool_()),
            pa.field("host_timezone", pa.string()),
            pa.field("host_local_date", pa.string()),
            pa.field("utc_date", pa.string()),
            pa.field("accounting_period", pa.string()),
            pa.field("tz_cutoff_shifted", pa.bool_()),
            pa.field("currency", pa.string()),
            pa.field("nights", pa.int64()),
            pa.field("room_subtotal_minor", pa.int64()),
            pa.field("platform_fee_minor", pa.int64()),
            pa.field("host_fee_minor", pa.int64()),
            pa.field("tax_minor", pa.int64()),
            pa.field("gross_total_minor", pa.int64()),
            pa.field("refund_minor", pa.int64()),
            pa.field("chargeback_minor", pa.int64()),
            pa.field("tax_policy_version", pa.string()),
            pa.field("jurisdiction", pa.string()),
            pa.field("payment_reference", pa.string()),
        ]
    ),
    "silver.dim_listings_scd2": pa.schema(
        [
            pa.field("surrogate_key", pa.string()),
            pa.field("listing_id", pa.string()),
            pa.field("host_id", pa.string()),
            pa.field("jurisdiction", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("host_timezone", pa.string()),
            pa.field("nightly_rate_minor", pa.int64()),
            pa.field("platform_fee_bps", pa.int64()),
            pa.field("host_fee_bps", pa.int64()),
            pa.field("status", pa.string()),
            pa.field("version_num", pa.int64()),
            pa.field("valid_from", pa.string()),
            pa.field("valid_to", pa.string()),
            pa.field("is_current", pa.bool_()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
        ]
    ),
    "silver.dim_tax_policies_scd2": pa.schema(
        [
            pa.field("surrogate_key", pa.string()),
            pa.field("jurisdiction", pa.string()),
            pa.field("tax_policy_version", pa.string()),
            pa.field("tax_bps", pa.int64()),
            pa.field("version_num", pa.int64()),
            pa.field("valid_from", pa.string()),
            pa.field("valid_to", pa.string()),
            pa.field("is_current", pa.bool_()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
        ]
    ),
    "silver.fx_rates_asof": pa.schema(
        [
            pa.field("fx_id", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("reporting_currency", pa.string()),
            pa.field("effective_date", pa.string()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
            pa.field("rate_version", pa.string()),
            pa.field("rate_micros", pa.int64()),
            pa.field("source", pa.string()),
            pa.field("is_revision", pa.bool_()),
            pa.field("system_valid_from", pa.string()),
            pa.field("system_valid_to", pa.string()),
            pa.field("is_latest_version", pa.bool_()),
        ]
    ),
    "gold.ledger_journal_entries": pa.schema(
        [
            pa.field("entry_line_id", pa.string()),
            pa.field("journal_entry_id", pa.string()),
            pa.field("parent_entry_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("source_event_id", pa.string()),
            pa.field("accounting_period", pa.string()),
            pa.field("original_period", pa.string()),
            pa.field("entry_type", pa.string()),
            pa.field("account_code", pa.string()),
            pa.field("account_category", pa.string()),
            pa.field("original_currency", pa.string()),
            pa.field("debit_minor", pa.int64()),
            pa.field("credit_minor", pa.int64()),
            pa.field("reporting_currency", pa.string()),
            pa.field("fx_rate_micros", pa.int64()),
            pa.field("fx_rate_version", pa.string()),
            pa.field("reporting_debit_minor", pa.int64()),
            pa.field("reporting_credit_minor", pa.int64()),
            pa.field("effective_at", pa.string()),
            pa.field("recorded_at", pa.string()),
            pa.field("is_restatement", pa.bool_()),
        ]
    ),
    "gold.reconciliation_results": pa.schema(
        [
            pa.field("recon_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("accounting_period", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("ledger_net_minor", pa.int64()),
            pa.field("psp_net_minor", pa.int64()),
            pa.field("bank_settled_orig_minor", pa.int64()),
            pa.field("bank_settled_usd_minor", pa.int64()),
            pa.field("expected_bank_usd_minor", pa.int64()),
            pa.field("variance_minor", pa.int64()),
            pa.field("variance_usd_minor", pa.int64()),
            pa.field("classification", pa.string()),
            pa.field("reason", pa.string()),
            pa.field("booking_status", pa.string()),
            pa.field("reconciled_at", pa.string()),
        ]
    ),
    "gold.reconciliation_exceptions": pa.schema(
        [
            pa.field("exception_id", pa.string()),
            pa.field("recon_id", pa.string()),
            pa.field("booking_id", pa.string()),
            pa.field("accounting_period", pa.string()),
            pa.field("classification", pa.string()),
            pa.field("severity", pa.string()),
            pa.field("status", pa.string()),
            pa.field("currency", pa.string()),
            pa.field("variance_minor", pa.int64()),
            pa.field("variance_usd_minor", pa.int64()),
            pa.field("age_days", pa.int64()),
            pa.field("aging_bucket", pa.string()),
            pa.field("reason", pa.string()),
            pa.field("detected_at", pa.string()),
            pa.field("resolved_at", pa.string()),
            pa.field("resolved_by", pa.string()),
            pa.field("resolution_note", pa.string()),
            pa.field("adjustment_journal_entry_id", pa.string()),
        ]
    ),
    "gold.period_close_snapshots": pa.schema(
        [
            pa.field("snapshot_id", pa.string()),
            pa.field("accounting_period", pa.string()),
            pa.field("status", pa.string()),
            pa.field("closed_at", pa.string()),
            pa.field("closed_by", pa.string()),
            pa.field("entry_line_count", pa.int64()),
            pa.field("restatement_line_count", pa.int64()),
            pa.field("total_reporting_debits_minor", pa.int64()),
            pa.field("total_reporting_credits_minor", pa.int64()),
            pa.field("trial_balance_imbalance_minor", pa.int64()),
            pa.field("is_trial_balance_zero_sum", pa.bool_()),
            pa.field("reconciliation_match_rate", pa.float64()),
            pa.field("open_exception_count", pa.int64()),
            pa.field("open_exception_exposure_usd_minor", pa.int64()),
            pa.field("dq_pass_rate", pa.float64()),
            pa.field("accounts_json", pa.string()),
            pa.field("certificate_sha256", pa.string()),
        ]
    ),
    "quality.dq_results": pa.schema(
        [
            pa.field("run_id", pa.string()),
            pa.field("check_name", pa.string()),
            pa.field("table_name", pa.string()),
            pa.field("severity", pa.string()),
            pa.field("passed", pa.bool_()),
            pa.field("observed_value", pa.string()),
            pa.field("expected_rule", pa.string()),
            pa.field("checked_at", pa.string()),
        ]
    ),
}


def rows_to_arrow(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    columns: dict[str, list[Any]] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            val = row.get(field.name)
            if val is not None and pa.types.is_string(field.type) and not isinstance(val, str):
                val = json.dumps(val) if isinstance(val, (dict, list)) else str(val)
            columns[field.name].append(val)
    arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


class IcebergLakehouseStore:
    """PyIceberg V2 SQLite-backed lakehouse with snapshot time travel, compaction, and DuckDB views."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.ensure_dirs()
        self.catalog = SqlCatalog(
            "booking_lakehouse",
            **{
                "uri": f"sqlite:///{self.settings.catalog_db_path}",
                "warehouse": f"file://{self.settings.warehouse_dir}",
            },
        )
        for ns in ("bronze", "silver", "gold", "quality"):
            try:
                self.catalog.create_namespace(ns)
            except NamespaceAlreadyExistsError:
                pass

    def _get_or_create_table(self, table_id: str) -> Any:
        schema = TABLE_SCHEMAS[table_id]
        try:
            return self.catalog.load_table(table_id)
        except NoSuchTableError:
            return self.catalog.create_table(
                identifier=table_id,
                schema=schema,
                properties={"format-version": "2"},
            )

    def overwrite_table(self, table_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        tbl = self._get_or_create_table(table_id)
        arrow_tbl = rows_to_arrow(rows, TABLE_SCHEMAS[table_id])
        tbl.overwrite(arrow_tbl)
        tbl = self.catalog.load_table(table_id)
        snap = tbl.current_snapshot()
        return {
            "table": table_id,
            "rows": len(rows),
            "snapshot_id": snap.snapshot_id if snap else None,
            "snapshot_count": len(list(tbl.snapshots())),
            "format_version": tbl.metadata.format_version,
        }

    def append_table(self, table_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        tbl = self._get_or_create_table(table_id)
        arrow_tbl = rows_to_arrow(rows, TABLE_SCHEMAS[table_id])
        if len(rows) > 0:
            tbl.append(arrow_tbl)
        tbl = self.catalog.load_table(table_id)
        snap = tbl.current_snapshot()
        return {
            "table": table_id,
            "appended_rows": len(rows),
            "snapshot_id": snap.snapshot_id if snap else None,
            "snapshot_count": len(list(tbl.snapshots())),
        }

    def read_table_rows(
        self,
        table_id: str,
        snapshot_id: int | None = None,
    ) -> list[dict[str, Any]]:
        tbl = self.catalog.load_table(table_id)
        scan = tbl.scan(snapshot_id=snapshot_id) if snapshot_id is not None else tbl.scan()
        rows: list[dict[str, Any]] = scan.to_arrow().to_pylist()
        return rows

    def list_table_metadata(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for table_id in TABLE_SCHEMAS:
            try:
                tbl = self.catalog.load_table(table_id)
                snap = tbl.current_snapshot()
                data_files = [task.file.file_path for task in tbl.scan().plan_files()]
                row_count = tbl.scan().to_arrow().num_rows
                results.append(
                    {
                        "table": table_id,
                        "format_version": tbl.metadata.format_version,
                        "row_count": row_count,
                        "data_file_count": len(data_files),
                        "snapshot_count": len(list(tbl.snapshots())),
                        "current_snapshot_id": snap.snapshot_id if snap else None,
                    }
                )
            except NoSuchTableError:
                continue
        return results

    def compact_table(self, table_id: str) -> dict[str, Any]:
        """Rewrite fragmented Parquet data files into a single compacted Iceberg V2 snapshot."""
        tbl = self.catalog.load_table(table_id)
        files_before = len(list(tbl.scan().plan_files()))
        t0 = time.perf_counter()
        arrow_tbl = tbl.scan().to_arrow()
        read_before_ms = (time.perf_counter() - t0) * 1000.0

        t_compact = time.perf_counter()
        tbl.overwrite(arrow_tbl)
        compact_ms = (time.perf_counter() - t_compact) * 1000.0

        tbl = self.catalog.load_table(table_id)
        files_after = len(list(tbl.scan().plan_files()))
        t1 = time.perf_counter()
        after_tbl = tbl.scan().to_arrow()
        read_after_ms = (time.perf_counter() - t1) * 1000.0
        assert after_tbl.num_rows == arrow_tbl.num_rows

        return {
            "table": table_id,
            "row_count": after_tbl.num_rows,
            "files_before": files_before,
            "files_after": files_after,
            "compact_ms": round(compact_ms, 2),
            "scan_before_ms": round(read_before_ms, 2),
            "scan_after_ms": round(read_after_ms, 2),
        }

    def sync_duckdb_warehouse(self) -> dict[str, int]:
        """Materialize current Iceberg V2 table snapshots into DuckDB schemas for SQL & dbt marts."""
        con = duckdb.connect(str(self.settings.duckdb_path))
        counts: dict[str, int] = {}
        try:
            for schema_name in ("bronze", "silver", "gold", "quality", "marts"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            for table_id in TABLE_SCHEMAS:
                try:
                    tbl = self.catalog.load_table(table_id)
                except NoSuchTableError:
                    continue
                arrow_tbl = tbl.scan().to_arrow()
                con.register("_tmp_iceberg_arrow", arrow_tbl)
                con.execute(f"CREATE OR REPLACE TABLE {table_id} AS SELECT * FROM _tmp_iceberg_arrow")
                con.unregister("_tmp_iceberg_arrow")
                counts[table_id] = arrow_tbl.num_rows
        finally:
            con.close()
        return counts


def query_duckdb_sql(duckdb_path: Path, sql: str) -> list[dict[str, Any]]:
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rel = con.execute(sql)
        cols = [desc[0] for desc in rel.description]
        return [dict(zip(cols, row, strict=True)) for row in rel.fetchall()]
    finally:
        con.close()
