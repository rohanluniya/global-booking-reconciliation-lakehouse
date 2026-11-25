"""Hypothesis property-based tests enforcing core financial & temporal invariants."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from booking_ledger.ledger import (
    build_balanced_journal_entry,
    compute_trial_balance,
    generate_double_entry_ledger,
    verify_ledger_zero_sum,
)
from booking_ledger.money import (
    CURRENCY_DECIMALS,
    FX_SCALE,
    allocate_bps_minor,
    convert_minor_to_reporting,
)
from booking_ledger.simulator import generate_simulation_bundle
from booking_ledger.temporal import (
    build_fx_rates_asof,
    build_scd2_dimension,
    reconstruct_booking_state_history,
    verify_scd2_non_overlapping,
)

CURRENCIES = list(CURRENCY_DECIMALS.keys())


@pytest.mark.unit
@settings(max_examples=80, deadline=None)
@given(
    room_minor=st.integers(min_value=1_000, max_value=5_000_000),
    plat_bps=st.integers(min_value=100, max_value=2000),
    host_bps=st.integers(min_value=50, max_value=800),
    tax_bps=st.integers(min_value=0, max_value=2500),
    currency=st.sampled_from(CURRENCIES),
    fx_micros=st.integers(min_value=5_000, max_value=2_000_000),
    refund_pct_bps=st.integers(min_value=0, max_value=10_000),
    has_chargeback=st.booleans(),
)
def test_property_double_entry_zero_sum_arbitrary_flows(
    room_minor: int,
    plat_bps: int,
    host_bps: int,
    tax_bps: int,
    currency: str,
    fx_micros: int,
    refund_pct_bps: int,
    has_chargeback: bool,
) -> None:
    eff_fx = FX_SCALE if currency == "USD" else fx_micros
    plat_fee = allocate_bps_minor(room_minor, plat_bps)
    host_fee = allocate_bps_minor(room_minor, host_bps)
    tax = allocate_bps_minor(room_minor + plat_fee, tax_bps)
    gross = room_minor + plat_fee + tax
    deferred = plat_fee + host_fee
    host_net = room_minor - host_fee

    all_lines = build_balanced_journal_entry(
        journal_entry_id="je_cap",
        booking_id="BKG-PROP",
        source_event_id="evt_cap",
        accounting_period="2026-05",
        entry_type="BOOKING_CAPTURE",
        original_currency=currency,
        reporting_currency="USD",
        fx_rate_micros=eff_fx,
        fx_rate_version="prop_v1",
        effective_at="2026-05-10T10:00:00+00:00",
        recorded_at="2026-05-10T10:00:01+00:00",
        legs=[
            ("ACCOUNTS_RECEIVABLE_PSP", gross, 0),
            ("DEFERRED_BOOKING_REVENUE", 0, deferred),
            ("HOST_PAYABLE", 0, host_net),
            ("TAX_PAYABLE", 0, tax),
        ],
    )

    if refund_pct_bps > 0:
        refund_amt = allocate_bps_minor(gross, refund_pct_bps)
        if refund_amt > 0:
            r_rev = allocate_bps_minor(deferred, refund_pct_bps)
            r_tax = allocate_bps_minor(tax, refund_pct_bps)
            r_host = refund_amt - r_rev - r_tax
            if r_host >= 0:
                all_lines.extend(
                    build_balanced_journal_entry(
                        journal_entry_id="je_ref",
                        booking_id="BKG-PROP",
                        source_event_id="evt_ref",
                        accounting_period="2026-05",
                        entry_type="PARTIAL_REFUND_ISSUED",
                        original_currency=currency,
                        reporting_currency="USD",
                        fx_rate_micros=eff_fx,
                        fx_rate_version="prop_v1",
                        effective_at="2026-05-11T10:00:00+00:00",
                        recorded_at="2026-05-11T10:00:01+00:00",
                        legs=[
                            ("REFUND_CONTRA_REVENUE", r_rev, 0),
                            ("HOST_PAYABLE", r_host, 0),
                            ("TAX_PAYABLE", r_tax, 0),
                            ("ACCOUNTS_RECEIVABLE_PSP", 0, refund_amt),
                        ],
                        parent_entry_id="je_cap",
                    )
                )

    if has_chargeback:
        all_lines.extend(
            build_balanced_journal_entry(
                journal_entry_id="je_cb",
                booking_id="BKG-PROP",
                source_event_id="evt_cb",
                accounting_period="2026-05",
                entry_type="CHARGEBACK_LOSS_RECOGNITION",
                original_currency=currency,
                reporting_currency="USD",
                fx_rate_micros=eff_fx,
                fx_rate_version="prop_v1",
                effective_at="2026-05-12T10:00:00+00:00",
                recorded_at="2026-05-12T10:00:01+00:00",
                legs=[("CHARGEBACK_LOSS", gross, 0), ("ACCOUNTS_RECEIVABLE_PSP", 0, gross)],
                parent_entry_id="je_cap",
            )
        )

    check = verify_ledger_zero_sum(all_lines)
    assert check["balanced"] is True
    assert check["global_imbalance_minor"] == 0
    assert all(isinstance(r["reporting_debit_minor"], int) for r in all_lines)


@pytest.mark.unit
@settings(max_examples=40, deadline=None)
@given(
    timestamps=st.lists(
        st.integers(min_value=1, max_value=28),
        min_size=1,
        max_size=8,
    )
)
def test_property_scd2_never_overlaps(timestamps: list[int]) -> None:
    raw = [
        {
            "listing_id": "LST-PROP-1",
            "nightly_rate_minor": 10_000 + idx * 500,
            "effective_at": f"2026-04-{day:02d}T00:00:00+00:00",
            "recorded_at": f"2026-04-{day:02d}T00:05:00+00:00",
        }
        for idx, day in enumerate(timestamps)
    ]
    scd2 = build_scd2_dimension(raw, "listing_id", prefix="prop")
    ok, errs = verify_scd2_non_overlapping(scd2, "listing_id")
    assert ok, errs


@pytest.mark.unit
@settings(max_examples=12, deadline=None)
@given(seed=st.integers(min_value=1, max_value=10_000))
def test_property_idempotent_replay_identical_trial_balance(seed: int) -> None:
    sim = generate_simulation_bundle(seed=seed, num_bookings=20)
    valid = [e for e in sim.cdc_events if isinstance(e.get("gross_total_minor"), int) and e.get("listing_id") and e.get("event_type") != "UNKNOWN_STATE_XYZ"]
    fx = build_fx_rates_asof(sim.fx_rates)

    hist1 = reconstruct_booking_state_history(valid)
    led1 = generate_double_entry_ledger(hist1, fx, sim.bank_statements)
    tb1 = compute_trial_balance(led1)

    # Duplicate and reverse stream order -> must yield bit-for-bit identical trial balance
    hist2 = reconstruct_booking_state_history(list(reversed(valid)) + valid)
    led2 = generate_double_entry_ledger(hist2, fx, sim.bank_statements)
    tb2 = compute_trial_balance(led2)

    assert tb1["total_reporting_debits_minor"] == tb2["total_reporting_debits_minor"]
    assert tb1["total_reporting_credits_minor"] == tb2["total_reporting_credits_minor"]
    assert tb2["imbalance_minor"] == 0


@pytest.mark.unit
@settings(max_examples=60, deadline=None)
@given(
    amount=st.integers(min_value=1, max_value=10_000_000),
    currency=st.sampled_from(CURRENCIES),
    fx_micros=st.integers(min_value=5_000, max_value=2_000_000),
)
def test_property_fx_conversion_returns_exact_int(amount: int, currency: str, fx_micros: int) -> None:
    eff_fx = FX_SCALE if currency == "USD" else fx_micros
    rep = convert_minor_to_reporting(amount, currency, "USD", eff_fx)
    assert isinstance(rep, int) and not isinstance(rep, bool)
    assert rep >= 0
