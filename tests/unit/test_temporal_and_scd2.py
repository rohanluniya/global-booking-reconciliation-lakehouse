"""Unit tests for bi-temporal booking reconstruction, timezone boundaries, SCD2, and as-of FX."""

from __future__ import annotations

import pytest

from booking_ledger.contracts import validate_avro_record
from booking_ledger.simulator import SimulationBundle
from booking_ledger.temporal import (
    build_fx_rates_asof,
    build_scd2_dimension,
    lookup_fx_as_of,
    query_booking_as_of,
    reconstruct_booking_state_history,
    verify_scd2_non_overlapping,
)


@pytest.mark.unit
def test_scd2_dimensions_have_zero_overlaps(small_sim_bundle: SimulationBundle) -> None:
    listings_scd2 = build_scd2_dimension(small_sim_bundle.listing_versions, "listing_id", prefix="lst")
    tax_scd2 = build_scd2_dimension(small_sim_bundle.tax_policy_versions, "jurisdiction", prefix="tax")

    ok_l, errs_l = verify_scd2_non_overlapping(listings_scd2, "listing_id")
    ok_t, errs_t = verify_scd2_non_overlapping(tax_scd2, "jurisdiction")
    assert ok_l, errs_l
    assert ok_t, errs_t
    assert any(r["version_num"] >= 2 for r in listings_scd2)
    assert any(r["version_num"] >= 2 for r in tax_scd2)


@pytest.mark.unit
def test_bitemporal_as_of_and_timezone_cutoff(small_sim_bundle: SimulationBundle) -> None:
    valid_cdc = [e for e in small_sim_bundle.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0]]
    history = reconstruct_booking_state_history(valid_cdc)
    assert any(r["tz_cutoff_shifted"] for r in history), "Expected explicit host-vs-UTC month cutoff shift bookings"

    first_bid = history[0]["booking_id"]
    b_rows = [r for r in history if r["booking_id"] == first_bid]
    assert len(b_rows) >= 3
    early_state = query_booking_as_of(history, first_bid, b_rows[0]["effective_at"])
    assert early_state is not None
    assert early_state["sequence_num"] == b_rows[0]["sequence_num"]


@pytest.mark.unit
def test_as_of_fx_respects_recorded_at_revisions(small_sim_bundle: SimulationBundle) -> None:
    fx = build_fx_rates_asof(small_sim_bundle.fx_rates)
    revised_rows = [r for r in fx if r["is_revision"]]
    assert len(revised_rows) > 0
    sample = revised_rows[0]
    eff_date = sample["effective_date"]
    curr = sample["currency"]
    # Before revision recorded_at -> returns v1
    before = lookup_fx_as_of(fx, curr, f"{eff_date}T12:00:00+00:00", recorded_as_of=f"{eff_date}T12:00:00+00:00")
    # Latest (no recorded_as_of cap) -> returns v2_revised
    after = lookup_fx_as_of(fx, curr, f"{eff_date}T12:00:00+00:00", recorded_as_of=None)
    assert before["rate_version"] == "v1"
    assert after["rate_version"] == "v2_revised"
    assert after["rate_micros"] != before["rate_micros"]
