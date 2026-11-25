"""Unit tests for immutable double-entry accounting ledger invariants."""

from __future__ import annotations

import pytest

from booking_ledger.contracts import validate_avro_record
from booking_ledger.ledger import (
    LedgerInvariantError,
    build_balanced_journal_entry,
    compute_trial_balance,
    generate_double_entry_ledger,
    verify_ledger_zero_sum,
)
from booking_ledger.simulator import SimulationBundle
from booking_ledger.temporal import build_fx_rates_asof, reconstruct_booking_state_history


@pytest.mark.unit
def test_unbalanced_journal_entry_rejected() -> None:
    with pytest.raises(LedgerInvariantError):
        build_balanced_journal_entry(
            journal_entry_id="je_bad",
            booking_id="BKG-1",
            source_event_id="evt_1",
            accounting_period="2026-05",
            entry_type="TEST",
            original_currency="USD",
            reporting_currency="USD",
            fx_rate_micros=1_000_000,
            fx_rate_version="identity",
            effective_at="2026-05-01T00:00:00+00:00",
            recorded_at="2026-05-01T00:00:01+00:00",
            legs=[("CASH_CLEARING", 10_000, 0), ("HOST_PAYABLE", 0, 9_500)],
        )


@pytest.mark.unit
def test_full_ledger_zero_sum_reversals_and_fx_adjustments(small_sim_bundle: SimulationBundle) -> None:
    valid_cdc = [e for e in small_sim_bundle.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    hist = reconstruct_booking_state_history(valid_cdc)
    fx = build_fx_rates_asof(small_sim_bundle.fx_rates)
    entries = generate_double_entry_ledger(hist, fx, small_sim_bundle.bank_statements)

    zero_sum = verify_ledger_zero_sum(entries)
    assert zero_sum["balanced"] is True
    assert zero_sum["global_imbalance_minor"] == 0
    assert zero_sum["unbalanced_entries"] == []
    assert zero_sum["unbalanced_currencies"] == []

    # Verify modifications emitted explicit immutable reversals with parent_entry_id
    reversals = [e for e in entries if e["entry_type"] == "BOOKING_MODIFICATION_REVERSAL"]
    assert len(reversals) > 0
    assert all(r["parent_entry_id"] is not None for r in reversals)

    # Verify period-scoped trial balances are individually balanced
    for period in ("2026-04", "2026-05"):
        tb = compute_trial_balance(entries, period=period)
        assert tb["is_balanced"] is True
        assert tb["imbalance_minor"] == 0
