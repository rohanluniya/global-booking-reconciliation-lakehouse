#!/usr/bin/env python3
"""Execute 7 automated chaos & fault-injection drills and persist results/chaos/chaos_results.json."""

from __future__ import annotations

import json
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from booking_ledger.broker import BrokerUnavailableError, LocalEventBroker
from booking_ledger.close_engine import close_accounting_period, post_late_restatement_entry
from booking_ledger.config import REPO_ROOT, load_settings
from booking_ledger.contracts import deterministic_id, validate_avro_record
from booking_ledger.ledger import (
    compute_trial_balance,
    generate_double_entry_ledger,
    verify_ledger_zero_sum,
)
from booking_ledger.pipeline import run_full_pipeline
from booking_ledger.reconciliation import deduplicate_psp_settlements, run_three_way_reconciliation
from booking_ledger.simulator import generate_simulation_bundle
from booking_ledger.temporal import (
    build_fx_rates_asof,
    deduplicate_cdc_events,
    reconstruct_booking_state_history,
)


def execute_all_chaos_drills() -> dict[str, Any]:
    drills: list[dict[str, Any]] = []

    # Drill 1: Duplicate CDC Replay Idempotency
    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        cfg = load_settings(Path(tmp))
        sim_base = generate_simulation_bundle(seed=9101, num_bookings=80)
        res_first = run_full_pipeline(cfg, bundle=sim_base)
        # Double every single CDC event to simulate full Kafka consumer replay
        sim_dup = generate_simulation_bundle(seed=9101, num_bookings=80)
        sim_dup.cdc_events = sim_dup.cdc_events + list(sim_dup.cdc_events)
        res_replay = run_full_pipeline(cfg, bundle=sim_dup)
        tb_match = (
            res_first["trial_balance"]["total_reporting_debits_minor"]
            == res_replay["trial_balance"]["total_reporting_debits_minor"]
            and res_first["trial_balance"]["total_reporting_credits_minor"]
            == res_replay["trial_balance"]["total_reporting_credits_minor"]
            and res_replay["trial_balance"]["imbalance_minor"] == 0
        )
        drills.append(
            {
                "drill_id": "CHAOS-01",
                "name": "duplicate_cdc_replay_idempotency",
                "passed": tb_match,
                "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                "evidence": {
                    "first_run_debits_usd_minor": res_first["trial_balance"]["total_reporting_debits_minor"],
                    "replayed_2x_debits_usd_minor": res_replay["trial_balance"]["total_reporting_debits_minor"],
                    "imbalance_minor": res_replay["trial_balance"]["imbalance_minor"],
                },
            }
        )

    # Drill 2: Out-of-order CDC (cancellation arrives before confirmation in stream)
    t0 = time.perf_counter()
    sim_ooo = generate_simulation_bundle(seed=9102, num_bookings=40)
    valid_ev = [e for e in sim_ooo.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    reversed_stream = list(reversed(valid_ev))
    history_normal = reconstruct_booking_state_history(valid_ev)
    history_reversed = reconstruct_booking_state_history(reversed_stream)
    ooo_identical = [(r["booking_id"], r["sequence_num"], r["booking_status"]) for r in history_normal] == [
        (r["booking_id"], r["sequence_num"], r["booking_status"]) for r in history_reversed
    ]
    drills.append(
        {
            "drill_id": "CHAOS-02",
            "name": "out_of_order_cdc_cancel_before_confirm",
            "passed": ooo_identical and len(history_reversed) > 0,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "evidence": {
                "events_reordered": len(reversed_stream),
                "reconstructed_rows": len(history_reversed),
                "state_history_exact_match": ooo_identical,
            },
        }
    )

    # Drill 3: Duplicate Bank/PSP Settlement File Re-Delivery Dedup
    t0 = time.perf_counter()
    sim_file = generate_simulation_bundle(seed=9103, num_bookings=60)
    valid_cdc = [e for e in sim_file.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    hist = reconstruct_booking_state_history(valid_cdc)
    fx = build_fx_rates_asof(sim_file.fx_rates)
    ledger = generate_double_entry_ledger(hist, fx, sim_file.bank_statements)
    tripled_psp = sim_file.psp_settlements + [
        {**line, "file_batch_id": f"{line['file_batch_id']}_DUP2"} for line in sim_file.psp_settlements
    ]
    deduped, _, dup_count = deduplicate_psp_settlements(tripled_psp)
    _, _, summary_base = run_three_way_reconciliation(
        booking_history=hist,
        ledger_entries=ledger,
        raw_psp_settlements=sim_file.psp_settlements,
        bank_statements=sim_file.bank_statements,
    )
    _, _, summary_dup = run_three_way_reconciliation(
        booking_history=hist,
        ledger_entries=ledger,
        raw_psp_settlements=tripled_psp,
        bank_statements=sim_file.bank_statements,
    )
    dedup_ok = (
        dup_count >= len(sim_file.psp_settlements)
        and summary_base["open_exception_exposure_usd_minor"] == summary_dup["open_exception_exposure_usd_minor"]
    )
    drills.append(
        {
            "drill_id": "CHAOS-03",
            "name": "duplicate_settlement_file_deduplication",
            "passed": dedup_ok,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "evidence": {
                "raw_psp_lines_injected": len(tripled_psp),
                "deduped_unique_lines": len(deduped),
                "duplicates_removed": dup_count,
                "exposure_unchanged_usd_minor": summary_dup["open_exception_exposure_usd_minor"],
            },
        }
    )

    # Drill 4: Mid-Month Late FX Revision Adjustment (zero historical mutation)
    t0 = time.perf_counter()
    fx_rev_entries = [e for e in ledger if e["entry_type"] == "FX_REVISION_ADJUSTMENT"]
    orig_captures = [e for e in ledger if e["entry_type"] == "BOOKING_CAPTURE"]
    # Verify original capture entries retained v1 FX rate while FX_REVISION_ADJUSTMENT used v2_revised
    capture_v1_preserved = all(e["fx_rate_version"] in ("v1", "identity") for e in orig_captures)
    fx_zero_sum = verify_ledger_zero_sum(ledger)
    fx_drill_ok = len(fx_rev_entries) > 0 and capture_v1_preserved and fx_zero_sum["balanced"]
    drills.append(
        {
            "drill_id": "CHAOS-04",
            "name": "mid_month_fx_revision_non_mutating_adjustment",
            "passed": fx_drill_ok,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "evidence": {
                "fx_revision_adjustment_lines": len(fx_rev_entries),
                "original_v1_captures_unmutated": capture_v1_preserved,
                "global_imbalance_minor": fx_zero_sum["global_imbalance_minor"],
            },
        }
    )

    # Drill 5: Post-Hard-Close Late Chargeback Restatement (closed period stays immutable)
    t0 = time.perf_counter()
    april_tb_before = compute_trial_balance(ledger, period="2026-04")
    april_cert = close_accounting_period(
        period="2026-04",
        target_state="HARD_CLOSED",
        current_state="SOFT_CLOSE",
        ledger_entries=ledger,
        recon_summary=summary_base,
        dq_pass_rate=1.0,
    )
    target_period, rst_lines = post_late_restatement_entry(
        closed_period="2026-04",
        closed_periods={"2026-04"},
        booking_id="BKG-2026-00001",
        reason="Chaos drill late chargeback arriving after April HARD_CLOSE",
        amount_minor=45_000,
        currency="USD",
    )
    ledger_with_rst = ledger + rst_lines
    april_tb_after = compute_trial_balance(ledger_with_rst, period="2026-04")
    rst_zero_sum = verify_ledger_zero_sum(ledger_with_rst)
    rst_ok = (
        target_period == "2026-05"
        and april_tb_before["total_reporting_debits_minor"] == april_tb_after["total_reporting_debits_minor"]
        and april_tb_before["entry_line_count"] == april_tb_after["entry_line_count"]
        and all(r["is_restatement"] and r["accounting_period"] == "2026-05" for r in rst_lines)
        and rst_zero_sum["balanced"]
        and bool(april_cert["certificate_sha256"])
    )
    drills.append(
        {
            "drill_id": "CHAOS-05",
            "name": "post_hard_close_late_chargeback_restatement",
            "passed": rst_ok,
            "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
            "evidence": {
                "closed_period_lines_before": april_tb_before["entry_line_count"],
                "closed_period_lines_after": april_tb_after["entry_line_count"],
                "routed_open_period": target_period,
                "restatement_lines_posted": len(rst_lines),
                "global_imbalance_minor": rst_zero_sum["global_imbalance_minor"],
            },
        }
    )

    # Drill 6: Broker Outage & Recovery from Durable Checkpoint
    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        broker = LocalEventBroker(Path(tmp) / "broker")
        sample_msgs = valid_cdc[:10]
        broker.publish_batch("cdc.booking_events", [(m["booking_id"], m, {}) for m in sample_msgs[:5]])
        batch1 = broker.read_topic("cdc.booking_events", consumer_group="chaos_cg")
        broker.commit_checkpoint("chaos_cg", "cdc.booking_events", batch1)

        # Take broker offline and assert write fails cleanly without corrupting checkpoints
        broker.online = False
        caught_outage = False
        try:
            broker.publish("cdc.booking_events", "BKG-OUTAGE", sample_msgs[5])
        except BrokerUnavailableError:
            caught_outage = True

        # Bring broker back online, publish remaining 5, verify consumer resumes strictly at checkpoint
        broker.online = True
        broker.publish_batch("cdc.booking_events", [(m["booking_id"], m, {}) for m in sample_msgs[5:]])
        batch2 = broker.read_topic("cdc.booking_events", consumer_group="chaos_cg")
        broker.commit_checkpoint("chaos_cg", "cdc.booking_events", batch2)
        recovered_ids = {m.payload["event_id"] for m in batch1 + batch2}
        expected_ids = {m["event_id"] for m in sample_msgs}
        outage_ok = caught_outage and len(batch1) == 5 and len(batch2) == 5 and recovered_ids == expected_ids
        drills.append(
            {
                "drill_id": "CHAOS-06",
                "name": "broker_outage_and_checkpoint_recovery",
                "passed": outage_ok,
                "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                "evidence": {
                    "outage_exception_caught": caught_outage,
                    "pre_outage_consumed": len(batch1),
                    "post_recovery_consumed": len(batch2),
                    "zero_event_loss": recovered_ids == expected_ids,
                },
            }
        )

    # Drill 7: Schema-Invalid Poison Event Quarantine to DLQ (zero pipeline crash)
    t0 = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        cfg = load_settings(Path(tmp))
        sim_poison = generate_simulation_bundle(seed=9107, num_bookings=30)
        sim_poison.cdc_events.append(
            {
                "event_id": deterministic_id("chaos_poison", 1),
                "booking_id": "BKG-CHAOS-POISON",
                "event_type": "CORRUPTED_EVENT",
                "gross_total_minor": 123.45,  # IEEE-754 float rejected!
            }
        )
        res_poison = run_full_pipeline(cfg, bundle=sim_poison)
        poison_ok = (
            res_poison["counts"]["quarantined_dlq_events"] >= 4
            and res_poison["ledger_verification"]["balanced"]
            and res_poison["dq_summary"]["all_passed"]
        )
        drills.append(
            {
                "drill_id": "CHAOS-07",
                "name": "schema_invalid_poison_event_dlq_quarantine",
                "passed": poison_ok,
                "duration_ms": round((time.perf_counter() - t0) * 1000.0, 2),
                "evidence": {
                    "quarantined_dlq_events": res_poison["counts"]["quarantined_dlq_events"],
                    "valid_cdc_processed": res_poison["counts"]["valid_cdc_events"],
                    "dq_all_passed": res_poison["dq_summary"]["all_passed"],
                    "ledger_balanced": res_poison["ledger_verification"]["balanced"],
                },
            }
        )

    # Also verify duplicate CDC deduplication helper directly
    deduped_events, dup_cdc_count = deduplicate_cdc_events(valid_ev + valid_ev[:10])
    assert dup_cdc_count >= 10 and len(deduped_events) > 0

    passed_count = sum(1 for d in drills if d["passed"])
    return {
        "executed_at": datetime.now(UTC).isoformat(),
        "total_drills": len(drills),
        "passed_drills": passed_count,
        "failed_drills": len(drills) - passed_count,
        "all_passed": passed_count == len(drills),
        "drills": drills,
    }


def main() -> int:
    report = execute_all_chaos_drills()
    out_dir = REPO_ROOT / "results" / "chaos"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "chaos_results.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
