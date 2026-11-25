"""Apache Airflow 3.4.0 Daily & Month-End Financial Close Orchestration DAG.

Orchestrates:
  1. CDC bronze ingestion & Avro DLQ check
  2. Bi-temporal booking state & SCD2 dimension rebuild
  3. Double-entry ledger posting & zero-sum trial balance gate
  4. 3-way reconciliation (Ledger vs PSP vs Bank) & exception aging
  5. dbt build (staging views + marts + singular zero-sum tests)
  6. Data Quality gate + Period Close Sign-Off snapshot
  7. Iceberg V2 small-file compaction
"""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.decorators import dag, task
except ImportError:  # Allows syntax/import inspection outside Airflow container
    def dag(*_args, **_kwargs):
        return lambda fn: fn

    def task(*_args, **_kwargs):
        return lambda fn: fn


@dag(
    dag_id="global_booking_daily_financial_close",
    description="Daily & month-end booking ledger, 3-way reconciliation, dbt marts, DQ gate, and Iceberg V2 compaction",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "finance-data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["finance", "iceberg-v2", "double-entry", "reconciliation", "dbt"],
)
def daily_financial_close_dag():
    @task()
    def ingest_and_reconcile() -> dict:
        from booking_ledger.config import load_settings
        from booking_ledger.pipeline import run_full_pipeline

        settings = load_settings()
        summary = run_full_pipeline(settings)
        if not summary["ledger_verification"]["balanced"]:
            raise RuntimeError("Double-entry ledger zero-sum check failed")
        return {
            "run_id": summary["run_id"],
            "dq_all_passed": summary["dq_summary"]["all_passed"],
            "match_rate": summary["reconciliation_summary"]["match_rate"],
        }

    @task()
    def run_dbt_marts(pipeline_meta: dict) -> dict:
        from pathlib import Path

        from booking_ledger.config import REPO_ROOT, load_settings
        from scripts.run_pipeline import run_dbt_build

        settings = load_settings()
        res = run_dbt_build(settings.duckdb_path, Path(REPO_ROOT) / "results" / "dbt")
        if res.get("returncode") != 0:
            raise RuntimeError(f"dbt build failed: {res}")
        return {"pipeline_run_id": pipeline_meta["run_id"], "dbt_passed": res.get("passed_nodes")}

    @task()
    def compact_iceberg_gold(dbt_meta: dict) -> dict:
        from booking_ledger.config import load_settings
        from booking_ledger.iceberg_store import IcebergLakehouseStore

        store = IcebergLakehouseStore(load_settings())
        ledger_compact = store.compact_table("gold.ledger_journal_entries")
        recon_compact = store.compact_table("gold.reconciliation_results")
        return {"dbt_meta": dbt_meta, "ledger_compact": ledger_compact, "recon_compact": recon_compact}

    meta = ingest_and_reconcile()
    dbt_out = run_dbt_marts(meta)
    compact_iceberg_gold(dbt_out)


daily_financial_close_dag()
