"""Unit tests for period close sign-off certificates and post-hard-close restatement immutability."""

from __future__ import annotations

import pytest

from booking_ledger.close_engine import (
    PeriodCloseError,
    close_accounting_period,
    post_late_restatement_entry,
)
from booking_ledger.contracts import validate_avro_record
from booking_ledger.ledger import compute_trial_balance, generate_double_entry_ledger
from booking_ledger.reconciliation import run_three_way_reconciliation
from booking_ledger.simulator import SimulationBundle
from booking_ledger.temporal import build_fx_rates_asof, reconstruct_booking_state_history


@pytest.mark.unit
def test_hard_close_blocks_reopen_and_routes_late_adjustments_to_open_period(
    small_sim_bundle: SimulationBundle,
) -> None:
    valid_cdc = [e for e in small_sim_bundle.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    hist = reconstruct_booking_state_history(valid_cdc)
    fx = build_fx_rates_asof(small_sim_bundle.fx_rates)
    entries = generate_double_entry_ledger(hist, fx, small_sim_bundle.bank_statements)
    _, _, recon_summary = run_three_way_reconciliation(
        booking_history=hist,
        ledger_entries=entries,
        raw_psp_settlements=small_sim_bundle.psp_settlements,
        bank_statements=small_sim_bundle.bank_statements,
    )

    # DQ < 1.0 must block HARD_CLOSE
    with pytest.raises(PeriodCloseError):
        close_accounting_period(
            period="2026-04",
            target_state="HARD_CLOSED",
            current_state="SOFT_CLOSE",
            ledger_entries=entries,
            recon_summary=recon_summary,
            dq_pass_rate=0.9,
        )

    cert = close_accounting_period(
        period="2026-04",
        target_state="HARD_CLOSED",
        current_state="SOFT_CLOSE",
        ledger_entries=entries,
        recon_summary=recon_summary,
        dq_pass_rate=1.0,
    )
    assert cert["trial_balance_imbalance_minor"] == 0
    assert len(cert["certificate_sha256"]) == 64

    # Cannot revert HARD_CLOSED -> SOFT_CLOSE
    with pytest.raises(PeriodCloseError):
        close_accounting_period(
            period="2026-04",
            target_state="SOFT_CLOSE",
            current_state="HARD_CLOSED",
            ledger_entries=entries,
            recon_summary=recon_summary,
            dq_pass_rate=1.0,
        )

    # Late restatement routes to 2026-05 without mutating 2026-04
    tb_april_before = compute_trial_balance(entries, period="2026-04")
    target_period, rst = post_late_restatement_entry(
        closed_period="2026-04",
        closed_periods={"2026-04"},
        booking_id="BKG-2026-00001",
        reason="Post-close chargeback",
        amount_minor=15_000,
    )
    assert target_period == "2026-05"
    assert all(r["is_restatement"] and r["accounting_period"] == "2026-05" for r in rst)
    tb_april_after = compute_trial_balance(entries + rst, period="2026-04")
    assert tb_april_before["total_reporting_debits_minor"] == tb_april_after["total_reporting_debits_minor"]
