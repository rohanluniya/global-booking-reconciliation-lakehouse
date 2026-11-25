"""End-to-end Medallion Lakehouse, Double-Entry Ledger, 3-Way Reconciliation & Period Close Pipeline."""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from booking_ledger.broker import LocalEventBroker
from booking_ledger.close_engine import close_accounting_period, post_late_restatement_entry
from booking_ledger.config import Settings
from booking_ledger.contracts import deterministic_id, validate_avro_record
from booking_ledger.dq import run_data_quality_checks
from booking_ledger.iceberg_store import IcebergLakehouseStore
from booking_ledger.ledger import (
    compute_trial_balance,
    generate_double_entry_ledger,
    verify_ledger_zero_sum,
)
from booking_ledger.lineage import emit_openlineage_event
from booking_ledger.logging_obs import METRICS, RUN_ID_CTX, get_logger, traced_stage
from booking_ledger.reconciliation import run_three_way_reconciliation
from booking_ledger.simulator import SimulationBundle, generate_simulation_bundle
from booking_ledger.temporal import (
    build_fx_rates_asof,
    build_scd2_dimension,
    reconstruct_booking_state_history,
)

LOGGER = get_logger("booking_ledger.pipeline")


def run_full_pipeline(
    settings: Settings,
    *,
    seed: int | None = None,
    num_bookings: int | None = None,
    bundle: SimulationBundle | None = None,
    hard_close_april: bool = True,
    inject_post_close_restatement: bool = True,
    reset_broker: bool = True,
) -> dict[str, Any]:
    """Execute the full source -> broker -> Iceberg V2 medallion -> ledger -> recon -> close -> DQ pipeline."""
    t_start = time.perf_counter()
    settings.ensure_dirs()
    run_id = str(uuid.uuid4())
    run_token = RUN_ID_CTX.set(run_id)
    effective_seed = bundle.seed if bundle is not None else (seed if seed is not None else settings.default_seed)
    effective_bookings = (
        bundle.num_bookings
        if bundle is not None
        else (num_bookings if num_bookings is not None else settings.default_bookings)
    )

    try:
        with traced_stage(
            "finance_medallion_pipeline",
            settings.traces_dir,
            {"run_id": run_id, "seed": effective_seed, "num_bookings": effective_bookings},
        ) as trace_id:
            # 1. Source Generation (deterministic seeded OLTP + PSP + Bank + SCD2 + FX)
            sim = bundle or generate_simulation_bundle(
                seed=effective_seed,
                num_bookings=effective_bookings,
                reporting_currency=settings.reporting_currency,
            )

            # 2. Durable Broker Ingestion + Avro Contract Validation + DLQ Quarantine
            broker = LocalEventBroker(settings.broker_dir)
            if reset_broker:
                for topic in ("cdc.booking_events", "psp.settlements", "bank.statements", "quarantine.dlq"):
                    broker.clear_topic(topic)

            broker.publish_batch(
                "cdc.booking_events",
                [(str(ev.get("booking_id", "unknown")), ev, {"trace_id": trace_id, "source": "postgres_outbox"}) for ev in sim.cdc_events],
            )
            broker.publish_batch(
                "psp.settlements",
                [(str(s.get("booking_id", "unknown")), s, {"trace_id": trace_id, "source": "psp_feed"}) for s in sim.psp_settlements],
            )
            broker.publish_batch(
                "bank.statements",
                [(str(b.get("booking_id", "unknown")), b, {"trace_id": trace_id, "source": "bank_mt940_sim"}) for b in sim.bank_statements],
            )

            cdc_msgs = broker.read_topic("cdc.booking_events", from_beginning=True)
            psp_msgs = broker.read_topic("psp.settlements", from_beginning=True)
            bank_msgs = broker.read_topic("bank.statements", from_beginning=True)

            valid_cdc: list[dict[str, Any]] = []
            quarantine_dlq: list[dict[str, Any]] = []
            now_iso = datetime.now(UTC).isoformat()

            for msg in cdc_msgs:
                ok, err = validate_avro_record(msg.payload, "booking_cdc_event.v1.avsc")
                if ok:
                    valid_cdc.append(msg.payload)
                    METRICS.cdc_events_ingested_total.labels(event_type=str(msg.payload["event_type"])).inc()
                else:
                    dlq_rec = {
                        "dlq_id": deterministic_id("dlq", msg.topic, msg.partition, msg.offset),
                        "source_topic": msg.topic,
                        "reason": f"Avro contract violation: {err}",
                        "raw_payload_json": json.dumps(msg.payload, default=str),
                        "quarantined_at": now_iso,
                    }
                    quarantine_dlq.append(dlq_rec)
                    broker.publish("quarantine.dlq", msg.key, dlq_rec, {"trace_id": trace_id})
                    METRICS.dlq_events_total.labels(reason="avro_schema_validation").inc()

            valid_psp: list[dict[str, Any]] = []
            for msg in psp_msgs:
                ok, err = validate_avro_record(msg.payload, "psp_settlement_event.v1.avsc")
                if ok:
                    valid_psp.append(msg.payload)
                else:
                    dlq_rec = {
                        "dlq_id": deterministic_id("dlq", msg.topic, msg.partition, msg.offset),
                        "source_topic": msg.topic,
                        "reason": f"PSP Avro violation: {err}",
                        "raw_payload_json": json.dumps(msg.payload, default=str),
                        "quarantined_at": now_iso,
                    }
                    quarantine_dlq.append(dlq_rec)
                    broker.publish("quarantine.dlq", msg.key, dlq_rec, {"trace_id": trace_id})
                    METRICS.dlq_events_total.labels(reason="psp_schema_validation").inc()

            valid_bank = [msg.payload for msg in bank_msgs]
            broker.commit_checkpoint("medallion_ingestor", "cdc.booking_events", cdc_msgs)
            broker.commit_checkpoint("medallion_ingestor", "psp.settlements", psp_msgs)
            broker.commit_checkpoint("medallion_ingestor", "bank.statements", bank_msgs)

            # 3. Silver Temporal Reconstruction, SCD Type 2 Dimensions, and As-Of FX
            with traced_stage("silver_temporal_and_scd2", settings.traces_dir, {"valid_cdc": len(valid_cdc)}):
                booking_history = reconstruct_booking_state_history(valid_cdc)
                dim_listings_scd2 = build_scd2_dimension(
                    sim.listing_versions, natural_key="listing_id", prefix="lst_scd2"
                )
                dim_tax_policies_scd2 = build_scd2_dimension(
                    sim.tax_policy_versions, natural_key="jurisdiction", prefix="tax_scd2"
                )
                fx_rates_asof = build_fx_rates_asof(sim.fx_rates)

            # 4. Gold Immutable Double-Entry Accounting Ledger
            with traced_stage("gold_double_entry_ledger", settings.traces_dir, {"history_rows": len(booking_history)}):
                ledger_entries = generate_double_entry_ledger(
                    booking_history=booking_history,
                    fx_rates=fx_rates_asof,
                    bank_statements=valid_bank,
                    reporting_currency=settings.reporting_currency,
                )
                for row in ledger_entries:
                    METRICS.ledger_entries_total.labels(entry_type=str(row["entry_type"])).inc()

            # 5. Gold 3-Way Reconciliation Engine & Exception Tickets
            with traced_stage("gold_three_way_reconciliation", settings.traces_dir):
                recon_results, recon_exceptions, recon_summary = run_three_way_reconciliation(
                    booking_history=booking_history,
                    ledger_entries=ledger_entries,
                    raw_psp_settlements=valid_psp,
                    bank_statements=valid_bank,
                    reporting_currency=settings.reporting_currency,
                )

            # 6. Period Close Simulation (2026-04 HARD_CLOSED, 2026-05 SOFT_CLOSE + optional late restatement)
            close_snapshots: list[dict[str, Any]] = []
            closed_periods: set[str] = set()
            if hard_close_april:
                # Run pre-close DQ check to gate HARD_CLOSE
                _, pre_dq = run_data_quality_checks(
                    run_id=run_id,
                    bronze_cdc=valid_cdc,
                    quarantine_dlq=quarantine_dlq,
                    booking_history=booking_history,
                    dim_listings_scd2=dim_listings_scd2,
                    dim_tax_policies_scd2=dim_tax_policies_scd2,
                    fx_rates_asof=fx_rates_asof,
                    ledger_entries=ledger_entries,
                    recon_results=recon_results,
                    close_snapshots=[],
                )
                april_cert = close_accounting_period(
                    period="2026-04",
                    target_state="HARD_CLOSED",
                    current_state="SOFT_CLOSE",
                    ledger_entries=ledger_entries,
                    recon_summary=recon_summary,
                    dq_pass_rate=pre_dq["pass_rate"],
                    actor="close-controller-bot",
                    closed_at="2026-05-03T18:00:00+00:00",
                )
                close_snapshots.append(april_cert)
                closed_periods.add("2026-04")

                if inject_post_close_restatement:
                    # Demonstrate closed-period immutability: a late chargeback belonging to 2026-04
                    # is routed into 2026-05 as an explicit Restatement entry.
                    april_bookings = [
                        r["booking_id"] for r in booking_history if r["accounting_period"] == "2026-04"
                    ]
                    if april_bookings:
                        _, restatement_lines = post_late_restatement_entry(
                            closed_period="2026-04",
                            closed_periods=closed_periods,
                            booking_id=april_bookings[0],
                            reason="Late issuer chargeback received post April hard-close",
                            amount_minor=12_500,
                            currency="USD",
                            recorded_at="2026-05-08T11:30:00+00:00",
                        )
                        ledger_entries.extend(restatement_lines)

                may_cert = close_accounting_period(
                    period="2026-05",
                    target_state="SOFT_CLOSE",
                    current_state="OPEN",
                    ledger_entries=ledger_entries,
                    recon_summary=recon_summary,
                    dq_pass_rate=pre_dq["pass_rate"],
                    actor="close-controller-bot",
                    closed_at="2026-05-26T00:05:00+00:00",
                )
                close_snapshots.append(may_cert)

            # 7. Final Data Quality Suite
            dq_rows, dq_summary = run_data_quality_checks(
                run_id=run_id,
                bronze_cdc=valid_cdc,
                quarantine_dlq=quarantine_dlq,
                booking_history=booking_history,
                dim_listings_scd2=dim_listings_scd2,
                dim_tax_policies_scd2=dim_tax_policies_scd2,
                fx_rates_asof=fx_rates_asof,
                ledger_entries=ledger_entries,
                recon_results=recon_results,
                close_snapshots=close_snapshots,
            )

            # 8. Persist all 13 Tables to Apache Iceberg V2 + Sync DuckDB Warehouse
            with traced_stage("iceberg_v2_commit_and_duckdb_sync", settings.traces_dir):
                store = IcebergLakehouseStore(settings)
                table_commits = {
                    "bronze.cdc_booking_events": store.overwrite_table("bronze.cdc_booking_events", valid_cdc),
                    "bronze.psp_settlements": store.overwrite_table("bronze.psp_settlements", valid_psp),
                    "bronze.bank_statements": store.overwrite_table("bronze.bank_statements", valid_bank),
                    "bronze.quarantine_dlq": store.overwrite_table("bronze.quarantine_dlq", quarantine_dlq),
                    "silver.booking_state_history": store.overwrite_table("silver.booking_state_history", booking_history),
                    "silver.dim_listings_scd2": store.overwrite_table("silver.dim_listings_scd2", dim_listings_scd2),
                    "silver.dim_tax_policies_scd2": store.overwrite_table("silver.dim_tax_policies_scd2", dim_tax_policies_scd2),
                    "silver.fx_rates_asof": store.overwrite_table("silver.fx_rates_asof", fx_rates_asof),
                    "gold.ledger_journal_entries": store.overwrite_table("gold.ledger_journal_entries", ledger_entries),
                    "gold.reconciliation_results": store.overwrite_table("gold.reconciliation_results", recon_results),
                    "gold.reconciliation_exceptions": store.overwrite_table("gold.reconciliation_exceptions", recon_exceptions),
                    "gold.period_close_snapshots": store.overwrite_table("gold.period_close_snapshots", close_snapshots),
                    "quality.dq_results": store.overwrite_table("quality.dq_results", dq_rows),
                }
                duckdb_counts = store.sync_duckdb_warehouse()

            # 9. Emit OpenLineage (Marquez-compatible) lineage graph
            emit_openlineage_event(
                lineage_dir=settings.lineage_dir,
                job_name="booking_medallion_ledger_reconciliation",
                event_type="COMPLETE",
                run_id=run_id,
                inputs=[
                    {"name": "bronze.cdc_booking_events", "row_count": len(valid_cdc)},
                    {"name": "bronze.psp_settlements", "row_count": len(valid_psp)},
                    {"name": "bronze.bank_statements", "row_count": len(valid_bank)},
                ],
                outputs=[
                    {"name": "silver.booking_state_history", "row_count": len(booking_history)},
                    {"name": "silver.dim_listings_scd2", "row_count": len(dim_listings_scd2)},
                    {"name": "silver.dim_tax_policies_scd2", "row_count": len(dim_tax_policies_scd2)},
                    {"name": "silver.fx_rates_asof", "row_count": len(fx_rates_asof)},
                    {"name": "gold.ledger_journal_entries", "row_count": len(ledger_entries)},
                    {"name": "gold.reconciliation_results", "row_count": len(recon_results)},
                    {"name": "gold.reconciliation_exceptions", "row_count": len(recon_exceptions)},
                    {"name": "gold.period_close_snapshots", "row_count": len(close_snapshots)},
                    {"name": "quality.dq_results", "row_count": len(dq_rows)},
                ],
            )

            # 10. Update Prometheus Metrics & Save Control Plane Snapshot
            zero_sum = verify_ledger_zero_sum(ledger_entries)
            trial_balance = compute_trial_balance(ledger_entries)
            METRICS.pipeline_runs_total.labels(status="success").inc()
            METRICS.trial_balance_imbalance_minor.set(zero_sum["global_imbalance_minor"])
            METRICS.reconciliation_match_rate.set(recon_summary["match_rate"])
            METRICS.open_exceptions_count.set(recon_summary["open_exception_count"])
            METRICS.open_exception_exposure_usd_minor.set(recon_summary["open_exception_exposure_usd_minor"])
            METRICS.dq_pass_rate.set(dq_summary["pass_rate"])
            METRICS.close_periods_locked_total.set(len(closed_periods))
            METRICS.mark_pipeline_success()

            elapsed_ms = round((time.perf_counter() - t_start) * 1000.0, 2)
            summary = {
                "run_id": run_id,
                "trace_id": trace_id,
                "completed_at": datetime.now(UTC).isoformat(),
                "elapsed_ms": elapsed_ms,
                "seed": effective_seed,
                "num_bookings": effective_bookings,
                "injected_fault_counts": sim.injected_fault_counts,
                "counts": {
                    "raw_cdc_events": len(sim.cdc_events),
                    "valid_cdc_events": len(valid_cdc),
                    "quarantined_dlq_events": len(quarantine_dlq),
                    "booking_state_history_rows": len(booking_history),
                    "dim_listings_scd2_rows": len(dim_listings_scd2),
                    "dim_tax_policies_scd2_rows": len(dim_tax_policies_scd2),
                    "fx_rates_asof_rows": len(fx_rates_asof),
                    "ledger_journal_lines": len(ledger_entries),
                    "ledger_journal_entries": zero_sum["journal_entry_count"],
                    "reconciliation_rows": len(recon_results),
                    "reconciliation_exceptions": len(recon_exceptions),
                    "close_snapshots": len(close_snapshots),
                },
                "ledger_verification": zero_sum,
                "trial_balance": trial_balance,
                "reconciliation_summary": recon_summary,
                "dq_summary": dq_summary,
                "close_periods": {c["accounting_period"]: c["status"] for c in close_snapshots},
                "iceberg_commits": table_commits,
                "duckdb_table_counts": duckdb_counts,
            }
            settings.state_snapshot_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            LOGGER.info(
                "Completed global booking reconciliation pipeline",
                extra={"structured": {"elapsed_ms": elapsed_ms, "bookings": effective_bookings, "imbalance": zero_sum["global_imbalance_minor"]}},
            )
            return summary
    except Exception:
        METRICS.pipeline_runs_total.labels(status="error").inc()
        raise
    finally:
        RUN_ID_CTX.reset(run_token)


def export_run_artifacts(settings: Settings, summary: dict[str, Any], results_dir: Path) -> None:
    """Persist sanitized DQ, lineage, and pipeline run summary artifacts under `results/`."""
    (results_dir / "dq").mkdir(parents=True, exist_ok=True)
    (results_dir / "lineage").mkdir(parents=True, exist_ok=True)
    store = IcebergLakehouseStore(settings)
    dq_rows = store.read_table_rows("quality.dq_results")
    (results_dir / "dq" / "dq_report.json").write_text(
        json.dumps({"summary": summary["dq_summary"], "checks": dq_rows}, indent=2),
        encoding="utf-8",
    )
    lineage_src = settings.lineage_dir / "openlineage_events.jsonl"
    if lineage_src.exists():
        events = [json.loads(line) for line in lineage_src.read_text(encoding="utf-8").splitlines() if line.strip()]
        (results_dir / "lineage" / "openlineage_sample.json").write_text(
            json.dumps(events[-3:], indent=2),
            encoding="utf-8",
        )
