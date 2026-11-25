"""Unit tests for 3-way reconciliation engine, duplicate settlement dedup, and exception workflow."""

from __future__ import annotations

import pytest

from booking_ledger.contracts import validate_avro_record
from booking_ledger.ledger import generate_double_entry_ledger, verify_ledger_zero_sum
from booking_ledger.reconciliation import (
    VALID_CLASSIFICATIONS,
    resolve_exception_ticket,
    run_three_way_reconciliation,
)
from booking_ledger.simulator import SimulationBundle
from booking_ledger.temporal import build_fx_rates_asof, reconstruct_booking_state_history


@pytest.mark.unit
def test_three_way_reconciliation_and_exception_resolution(small_sim_bundle: SimulationBundle) -> None:
    valid_cdc = [e for e in small_sim_bundle.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    hist = reconstruct_booking_state_history(valid_cdc)
    fx = build_fx_rates_asof(small_sim_bundle.fx_rates)
    entries = generate_double_entry_ledger(hist, fx, small_sim_bundle.bank_statements)

    recon, exceptions, summary = run_three_way_reconciliation(
        booking_history=hist,
        ledger_entries=entries,
        raw_psp_settlements=small_sim_bundle.psp_settlements,
        bank_statements=small_sim_bundle.bank_statements,
    )
    assert len(recon) >= 90
    assert all(r["classification"] in VALID_CLASSIFICATIONS for r in recon)
    assert summary["duplicate_settlement_lines_deduped"] > 0
    assert len(exceptions) > 0

    # Resolve first open exception and verify adjustment JE preserves global zero-sum
    updated, adj_entries = resolve_exception_ticket(
        exceptions[0],
        actor="unit-test-auditor",
        resolution_note="Verified fee variance against PSP statement",
        target_status="RESOLVED_ADJUSTED",
    )
    assert updated["status"] == "RESOLVED_ADJUSTED"
    assert updated["resolved_at"] is not None
    combined_check = verify_ledger_zero_sum(entries + adj_entries)
    assert combined_check["balanced"] is True
