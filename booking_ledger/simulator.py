"""Deterministic multi-currency OLTP booking, outbox CDC, PSP, and bank simulator.

Produces realistic hospitality booking lifecycles with exact integer minor-unit
amounts, timezone boundaries, mid-period listing/tax SCD2 changes, late FX
revisions, and injected operational faults (out-of-order CDC, duplicate CDC,
duplicate settlement files, fee/FX variances, partial refund mismatches,
chargebacks, unsettled cash in transit, and schema-invalid poison events).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from booking_ledger.contracts import deterministic_id
from booking_ledger.money import (
    FX_SCALE,
    allocate_bps_minor,
    convert_minor_to_reporting,
)


@dataclass(frozen=True)
class MarketSpec:
    jurisdiction: str
    currency: str
    timezone: str
    base_nightly_min_minor: int
    base_nightly_max_minor: int
    initial_tax_bps: int
    revised_tax_bps: int | None
    tax_revision_date: str | None
    base_fx_to_usd_micros: int


MARKETS: list[MarketSpec] = [
    MarketSpec("US_NYC", "USD", "America/New_York", 14_000, 42_000, 1475, 1525, "2026-04-16T00:00:00+00:00", 1_000_000),
    MarketSpec("EU_PAR", "EUR", "Europe/Paris", 12_000, 38_000, 1000, 1100, "2026-05-01T00:00:00+00:00", 1_085_000),
    MarketSpec("GB_LON", "GBP", "Europe/London", 13_000, 39_000, 2000, None, None, 1_268_000),
    MarketSpec("JP_TYO", "JPY", "Asia/Tokyo", 14_500, 48_000, 1000, None, None, 6_540),
    MarketSpec("CA_TOR", "CAD", "America/Toronto", 15_000, 36_000, 1300, None, None, 728_000),
    MarketSpec("AU_SYD", "AUD", "Australia/Sydney", 16_000, 41_000, 1000, None, None, 652_000),
]


@dataclass
class SimulationBundle:
    seed: int
    num_bookings: int
    cdc_events: list[dict[str, Any]] = field(default_factory=list)
    psp_settlements: list[dict[str, Any]] = field(default_factory=list)
    bank_statements: list[dict[str, Any]] = field(default_factory=list)
    listing_versions: list[dict[str, Any]] = field(default_factory=list)
    tax_policy_versions: list[dict[str, Any]] = field(default_factory=list)
    fx_rates: list[dict[str, Any]] = field(default_factory=list)
    injected_fault_counts: dict[str, int] = field(default_factory=dict)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat()


def generate_simulation_bundle(
    seed: int = 20260505,
    num_bookings: int = 600,
    reporting_currency: str = "USD",
) -> SimulationBundle:
    """Generate a fully deterministic dataset across 2026-04 and 2026-05."""
    rng = random.Random(seed)
    bundle = SimulationBundle(seed=seed, num_bookings=num_bookings)
    faults: dict[str, int] = {
        "duplicate_cdc_events": 0,
        "out_of_order_cdc_events": 0,
        "duplicate_settlement_lines": 0,
        "timing_in_transit": 0,
        "fee_variance": 0,
        "fx_variance": 0,
        "partial_refund_mismatch": 0,
        "chargebacks": 0,
        "unmatched_orphan_lines": 0,
        "poison_schema_events": 0,
        "fx_revisions": 0,
        "timezone_month_boundary_bookings": 0,
        "payment_retries": 0,
        "booking_modifications": 0,
    }

    start_date = datetime(2026, 4, 1, 0, 0, 0, tzinfo=UTC)
    num_days = 55  # 2026-04-01 through 2026-05-25

    # 1. Generate Tax Policy SCD2 source versions
    for mkt in MARKETS:
        bundle.tax_policy_versions.append(
            {
                "jurisdiction": mkt.jurisdiction,
                "tax_policy_version": f"{mkt.jurisdiction}_v1",
                "tax_bps": mkt.initial_tax_bps,
                "effective_at": "2026-01-01T00:00:00+00:00",
                "recorded_at": "2026-01-01T00:00:00+00:00",
            }
        )
        if mkt.revised_tax_bps is not None and mkt.tax_revision_date is not None:
            bundle.tax_policy_versions.append(
                {
                    "jurisdiction": mkt.jurisdiction,
                    "tax_policy_version": f"{mkt.jurisdiction}_v2",
                    "tax_bps": mkt.revised_tax_bps,
                    "effective_at": mkt.tax_revision_date,
                    "recorded_at": mkt.tax_revision_date,
                }
            )

    # 2. Generate Listing SCD2 source versions (42 listings across 6 markets, ~40% undergo mid-period rate/fee change)
    listings: list[dict[str, Any]] = []
    for idx in range(42):
        mkt = MARKETS[idx % len(MARKETS)]
        listing_id = f"LST-{mkt.jurisdiction}-{idx + 1:03d}"
        host_id = f"HST-{idx % 18 + 1:03d}"
        nightly_v1 = rng.randint(mkt.base_nightly_min_minor, mkt.base_nightly_max_minor)
        # Round JPY to nearest 100 yen, others to nearest 100 minor units for realism
        nightly_v1 = (nightly_v1 // 100) * 100
        host_fee_bps_v1 = 300  # 3.00%
        platform_fee_bps_v1 = 1200  # 12.00%
        v1 = {
            "listing_id": listing_id,
            "host_id": host_id,
            "jurisdiction": mkt.jurisdiction,
            "currency": mkt.currency,
            "host_timezone": mkt.timezone,
            "nightly_rate_minor": nightly_v1,
            "platform_fee_bps": platform_fee_bps_v1,
            "host_fee_bps": host_fee_bps_v1,
            "status": "ACTIVE",
            "effective_at": "2026-01-01T00:00:00+00:00",
            "recorded_at": "2026-01-01T00:00:00+00:00",
        }
        bundle.listing_versions.append(v1)
        listings.append(v1)

        if idx % 3 == 0:
            change_day = start_date + timedelta(days=rng.randint(14, 38))
            nightly_v2 = int(nightly_v1 * 105 // 100)
            nightly_v2 = (nightly_v2 // 100) * 100
            v2 = {
                **v1,
                "nightly_rate_minor": nightly_v2,
                "host_fee_bps": 325,
                "effective_at": _iso(change_day),
                "recorded_at": _iso(change_day + timedelta(minutes=15)),
            }
            bundle.listing_versions.append(v2)

    # 3. Generate Daily FX Rates + Late FX Revisions (for EUR, GBP, JPY on specific dates)
    daily_fx_v1: dict[tuple[str, str], int] = {}
    for day_offset in range(num_days + 10):
        day_dt = start_date + timedelta(days=day_offset)
        day_str = day_dt.strftime("%Y-%m-%d")
        for mkt in MARKETS:
            if mkt.currency == reporting_currency:
                rate_micros = FX_SCALE
            else:
                drift_bps = rng.randint(-65, 65)
                rate_micros = max(100, mkt.base_fx_to_usd_micros + (mkt.base_fx_to_usd_micros * drift_bps) // 10_000)
            daily_fx_v1[(mkt.currency, day_str)] = rate_micros
            bundle.fx_rates.append(
                {
                    "currency": mkt.currency,
                    "reporting_currency": reporting_currency,
                    "effective_date": day_str,
                    "effective_at": _iso(day_dt),
                    "recorded_at": _iso(day_dt + timedelta(hours=1)),
                    "rate_version": "v1",
                    "rate_micros": rate_micros,
                    "source": "ECB_FIX_SIM",
                    "is_revision": False,
                }
            )
            # Inject controlled late FX revisions on the 10th and 24th of April/May for non-USD
            if mkt.currency in ("EUR", "GBP", "JPY") and day_dt.day in (10, 24) and day_offset < num_days:
                revised_micros = rate_micros + max(50, (rate_micros * 45) // 10_000)
                bundle.fx_rates.append(
                    {
                        "currency": mkt.currency,
                        "reporting_currency": reporting_currency,
                        "effective_date": day_str,
                        "effective_at": _iso(day_dt),
                        "recorded_at": _iso(day_dt + timedelta(days=2, hours=4)),
                        "rate_version": "v2_revised",
                        "rate_micros": revised_micros,
                        "source": "CENTRAL_BANK_CORRECTION_SIM",
                        "is_revision": True,
                    }
                )
                faults["fx_revisions"] += 1

    # Helper to look up active listing & tax policy at a timestamp
    def active_listing_version(listing_id: str, ts_iso: str) -> dict[str, Any]:
        candidates = [v for v in bundle.listing_versions if v["listing_id"] == listing_id and v["effective_at"] <= ts_iso]
        candidates.sort(key=lambda x: x["effective_at"])
        return candidates[-1]

    def active_tax_policy(jurisdiction: str, ts_iso: str) -> dict[str, Any]:
        candidates = [
            t for t in bundle.tax_policy_versions if t["jurisdiction"] == jurisdiction and t["effective_at"] <= ts_iso
        ]
        candidates.sort(key=lambda x: x["effective_at"])
        return candidates[-1]

    # 4. Generate Booking Lifecycles, PSP Settlements, and Bank Statements
    lsn = 100_000
    duplicate_psp_batch_lines: list[dict[str, Any]] = []

    for i in range(num_bookings):
        listing_base = listings[i % len(listings)]
        listing_id = listing_base["listing_id"]
        host_id = listing_base["host_id"]
        guest_id = f"GST-{rng.randint(1, max(50, num_bookings // 2)):05d}"
        booking_id = f"BKG-2026-{i + 1:05d}"
        payment_ref = f"PAY-{i + 1:06d}"

        # Deterministic placement across 2026-04 (55%) and 2026-05 (45%), plus explicit TZ boundary cases
        if i % 30 == 0:
            # Explicit month-end timezone boundary: Apr 30 22:30 New York == May 01 02:30 UTC
            hold_dt = datetime(2026, 4, 30, 22, 30, 0, tzinfo=ZoneInfo("America/New_York")).astimezone(UTC)
            faults["timezone_month_boundary_bookings"] += 1
        elif i % 30 == 15:
            # Explicit month-end timezone boundary: May 01 00:30 Tokyo == Apr 30 15:30 UTC
            hold_dt = datetime(2026, 5, 1, 0, 30, 0, tzinfo=ZoneInfo("Asia/Tokyo")).astimezone(UTC)
            faults["timezone_month_boundary_bookings"] += 1
        else:
            day_off = int((i * 48) / max(1, num_bookings)) + rng.randint(0, 2)
            day_off = min(day_off, 50)
            hold_dt = start_date + timedelta(
                days=day_off,
                hours=rng.randint(0, 23),
                minutes=rng.randint(0, 59),
            )

        l_ver = active_listing_version(listing_id, _iso(hold_dt))
        t_pol = active_tax_policy(l_ver["jurisdiction"], _iso(hold_dt))
        currency = l_ver["currency"]
        nights = rng.randint(1, 7)

        def calc_amounts(n_nights: int, lv: dict[str, Any], tp: dict[str, Any]) -> dict[str, int]:
            room = lv["nightly_rate_minor"] * n_nights
            plat_fee = allocate_bps_minor(room, lv["platform_fee_bps"])
            host_fee = allocate_bps_minor(room, lv["host_fee_bps"])
            tax = allocate_bps_minor(room + plat_fee, tp["tax_bps"])
            gross = room + plat_fee + tax
            return {
                "room_subtotal_minor": room,
                "platform_fee_minor": plat_fee,
                "host_fee_minor": host_fee,
                "tax_minor": tax,
                "gross_total_minor": gross,
            }

        amts = calc_amounts(nights, l_ver, t_pol)
        seq = 0
        booking_events: list[dict[str, Any]] = []

        def emit(
            event_type: str,
            eff_dt: datetime,
            rec_dt: datetime,
            curr_amts: dict[str, int],
            curr_nights: int,
            cdc_op: str = "u",
            refund_minor: int = 0,
            chargeback_minor: int = 0,
            pay_ref: str | None = payment_ref,
            tp_ver: str = t_pol["tax_policy_version"],
            _booking_id: str = booking_id,
            _listing_id: str = listing_id,
            _guest_id: str = guest_id,
            _host_id: str = host_id,
            _l_ver: dict[str, Any] = l_ver,
            _currency: str = currency,
            _events: list[dict[str, Any]] = booking_events,
        ) -> dict[str, Any]:
            nonlocal seq, lsn
            seq += 1
            lsn += 10
            ev = {
                "event_id": deterministic_id("evt", seed, _booking_id, seq, event_type),
                "booking_id": _booking_id,
                "listing_id": _listing_id,
                "guest_id": _guest_id,
                "host_id": _host_id,
                "event_type": event_type,
                "cdc_op": cdc_op,
                "sequence_num": seq,
                "effective_at": _iso(eff_dt),
                "recorded_at": _iso(rec_dt),
                "host_timezone": _l_ver["host_timezone"],
                "currency": _currency,
                "nights": curr_nights,
                "room_subtotal_minor": curr_amts["room_subtotal_minor"],
                "platform_fee_minor": curr_amts["platform_fee_minor"],
                "host_fee_minor": curr_amts["host_fee_minor"],
                "tax_minor": curr_amts["tax_minor"],
                "gross_total_minor": curr_amts["gross_total_minor"],
                "refund_minor": refund_minor,
                "chargeback_minor": chargeback_minor,
                "tax_policy_version": tp_ver,
                "jurisdiction": _l_ver["jurisdiction"],
                "payment_reference": pay_ref,
                "source_lsn": lsn,
            }
            _events.append(ev)
            return ev

        # Step 1: Hold -> Confirmed -> Auth -> Capture (or Retry)
        emit("BOOKING_HELD", hold_dt, hold_dt + timedelta(seconds=2), amts, nights, cdc_op="c", pay_ref=None)
        confirm_dt = hold_dt + timedelta(minutes=12)
        emit("BOOKING_CONFIRMED", confirm_dt, confirm_dt + timedelta(seconds=3), amts, nights)
        auth_dt = confirm_dt + timedelta(minutes=2)
        emit("PAYMENT_AUTHORIZED", auth_dt, auth_dt + timedelta(seconds=4), amts, nights)

        retry_payment = rng.random() < 0.08
        capture_dt = auth_dt + timedelta(minutes=25 if retry_payment else 5)
        if retry_payment:
            faults["payment_retries"] += 1
            emit("PAYMENT_RETRY_SUCCEEDED", capture_dt, capture_dt + timedelta(seconds=5), amts, nights)
        else:
            emit("PAYMENT_CAPTURED", capture_dt, capture_dt + timedelta(seconds=5), amts, nights)

        # Optional Step 2: Booking Modification (~12%)
        if rng.random() < 0.12:
            faults["booking_modifications"] += 1
            mod_dt = capture_dt + timedelta(hours=18)
            nights = nights + 1 if nights < 7 else max(1, nights - 1)
            l_ver_mod = active_listing_version(listing_id, _iso(mod_dt))
            t_pol_mod = active_tax_policy(l_ver["jurisdiction"], _iso(mod_dt))
            amts = calc_amounts(nights, l_ver_mod, t_pol_mod)
            emit("BOOKING_MODIFIED", mod_dt, mod_dt + timedelta(seconds=8), amts, nights, tp_ver=t_pol_mod["tax_policy_version"])
            effective_capture_dt = mod_dt
        else:
            effective_capture_dt = capture_dt

        # Determine final lifecycle branch
        roll = rng.random()
        expected_psp_fee = allocate_bps_minor(amts["gross_total_minor"], 290) + (30 if currency != "JPY" else 30)
        settled_dt = effective_capture_dt + timedelta(days=1, hours=6)
        bank_dt = settled_dt + timedelta(days=1, hours=4)
        bank_ref = f"BNK-{i + 1:06d}"
        file_batch_id = f"PSP_BATCH_{settled_dt.strftime('%Y%m%d')}"

        # Branch outcomes:
        # 0.00 - 0.03: Chargeback
        # 0.03 - 0.11: Partial Refund
        # 0.11 - 0.18: Full Cancellation + Refund
        # 0.18 - 1.00: Completed Stay + Payout
        is_chargeback = roll < 0.03
        is_partial_refund = 0.03 <= roll < 0.11
        is_full_cancel = 0.11 <= roll < 0.18

        psp_fee_actual = expected_psp_fee
        fx_fault = False
        timing_fault = False
        refund_mismatch = False

        if not (is_chargeback or is_partial_refund or is_full_cancel):
            # Normal completed booking + host payout
            checkin_dt = effective_capture_dt + timedelta(days=2)
            checkout_dt = checkin_dt + timedelta(days=nights)
            emit("BOOKING_CHECKED_IN", checkin_dt, checkin_dt + timedelta(seconds=10), amts, nights)
            emit("BOOKING_COMPLETED", checkout_dt, checkout_dt + timedelta(seconds=10), amts, nights)
            payout_sched_dt = checkout_dt + timedelta(hours=4)
            payout_settle_dt = payout_sched_dt + timedelta(hours=20)
            emit("HOST_PAYOUT_SCHEDULED", payout_sched_dt, payout_sched_dt + timedelta(seconds=5), amts, nights)
            emit("HOST_PAYOUT_SETTLED", payout_settle_dt, payout_settle_dt + timedelta(seconds=5), amts, nights)

            # Inject targeted recon faults on a controlled slice of normal completed bookings
            f_roll = rng.random()
            if f_roll < 0.035:
                timing_fault = True
                faults["timing_in_transit"] += 1
            elif f_roll < 0.065:
                psp_fee_actual += rng.randint(15, 85)
                faults["fee_variance"] += 1
            elif f_roll < 0.09 and currency != reporting_currency:
                fx_fault = True
                faults["fx_variance"] += 1
        elif is_full_cancel:
            cancel_dt = effective_capture_dt + timedelta(days=1)
            emit("BOOKING_CANCELLED", cancel_dt, cancel_dt + timedelta(seconds=5), amts, nights, refund_minor=amts["gross_total_minor"])
            emit("REFUND_ISSUED", cancel_dt + timedelta(minutes=10), cancel_dt + timedelta(minutes=11), amts, nights, refund_minor=amts["gross_total_minor"])
        elif is_partial_refund:
            refund_amt = allocate_bps_minor(amts["gross_total_minor"], 5000)
            cancel_dt = effective_capture_dt + timedelta(days=1)
            emit("BOOKING_CANCELLED", cancel_dt, cancel_dt + timedelta(seconds=5), amts, nights, refund_minor=refund_amt)
            emit("PARTIAL_REFUND_ISSUED", cancel_dt + timedelta(minutes=10), cancel_dt + timedelta(minutes=11), amts, nights, refund_minor=refund_amt)
            if rng.random() < 0.25:
                refund_mismatch = True
                faults["partial_refund_mismatch"] += 1
        else:
            # Chargeback
            faults["chargebacks"] += 1
            cb_open_dt = effective_capture_dt + timedelta(days=3)
            cb_lost_dt = cb_open_dt + timedelta(days=4)
            emit("CHARGEBACK_OPENED", cb_open_dt, cb_open_dt + timedelta(seconds=5), amts, nights, chargeback_minor=amts["gross_total_minor"])
            emit("CHARGEBACK_LOST", cb_lost_dt, cb_lost_dt + timedelta(seconds=5), amts, nights, chargeback_minor=amts["gross_total_minor"])

        # Emit PSP capture settlement line
        psp_net = amts["gross_total_minor"] - psp_fee_actual
        psp_capture = {
            "settlement_id": deterministic_id("psp", seed, booking_id, "CAPTURE"),
            "file_batch_id": file_batch_id,
            "booking_id": booking_id,
            "payment_reference": payment_ref,
            "settlement_type": "CAPTURE",
            "currency": currency,
            "gross_minor": amts["gross_total_minor"],
            "psp_fee_minor": psp_fee_actual,
            "net_minor": psp_net,
            "settled_at": _iso(settled_dt),
            "bank_transfer_ref": None if timing_fault else bank_ref,
        }
        bundle.psp_settlements.append(psp_capture)
        if i % 25 == 7:
            # Queue exact line for duplicate settlement file re-delivery
            dup_line = {**psp_capture, "file_batch_id": f"{file_batch_id}_REDELIVERED"}
            duplicate_psp_batch_lines.append(dup_line)

        # Emit PSP refund / chargeback lines
        if is_full_cancel:
            bundle.psp_settlements.append(
                {
                    "settlement_id": deterministic_id("psp", seed, booking_id, "REFUND"),
                    "file_batch_id": file_batch_id,
                    "booking_id": booking_id,
                    "payment_reference": payment_ref,
                    "settlement_type": "REFUND",
                    "currency": currency,
                    "gross_minor": -amts["gross_total_minor"],
                    "psp_fee_minor": 0,
                    "net_minor": -amts["gross_total_minor"],
                    "settled_at": _iso(settled_dt + timedelta(hours=2)),
                    "bank_transfer_ref": bank_ref,
                }
            )
        elif is_partial_refund:
            refund_amt = allocate_bps_minor(amts["gross_total_minor"], 5000)
            psp_refund_amt = refund_amt - (150 if currency != "JPY" else 150) if refund_mismatch else refund_amt
            bundle.psp_settlements.append(
                {
                    "settlement_id": deterministic_id("psp", seed, booking_id, "REFUND"),
                    "file_batch_id": file_batch_id,
                    "booking_id": booking_id,
                    "payment_reference": payment_ref,
                    "settlement_type": "REFUND",
                    "currency": currency,
                    "gross_minor": -psp_refund_amt,
                    "psp_fee_minor": 0,
                    "net_minor": -psp_refund_amt,
                    "settled_at": _iso(settled_dt + timedelta(hours=2)),
                    "bank_transfer_ref": bank_ref,
                }
            )
        elif is_chargeback:
            bundle.psp_settlements.append(
                {
                    "settlement_id": deterministic_id("psp", seed, booking_id, "CHARGEBACK"),
                    "file_batch_id": file_batch_id,
                    "booking_id": booking_id,
                    "payment_reference": payment_ref,
                    "settlement_type": "CHARGEBACK",
                    "currency": currency,
                    "gross_minor": -amts["gross_total_minor"],
                    "psp_fee_minor": 1500 if currency != "JPY" else 2000,
                    "net_minor": -(amts["gross_total_minor"] + (1500 if currency != "JPY" else 2000)),
                    "settled_at": _iso(settled_dt + timedelta(days=4)),
                    "bank_transfer_ref": bank_ref,
                }
            )

        # Emit Bank Statement settlement line (unless timing_in_transit)
        if not timing_fault:
            fx_day = settled_dt.strftime("%Y-%m-%d")
            fx_micros = daily_fx_v1.get((currency, fx_day), FX_SCALE)
            net_for_bank = psp_net
            if is_full_cancel:
                net_for_bank = psp_net - amts["gross_total_minor"]
            elif is_partial_refund:
                refund_amt = allocate_bps_minor(amts["gross_total_minor"], 5000)
                psp_refund_amt = refund_amt - 150 if refund_mismatch else refund_amt
                net_for_bank = psp_net - psp_refund_amt
            elif is_chargeback:
                net_for_bank = psp_net - amts["gross_total_minor"] - (1500 if currency != "JPY" else 2000)

            bank_usd_minor = convert_minor_to_reporting(net_for_bank, currency, reporting_currency, fx_micros)
            if fx_fault:
                bank_usd_minor -= rng.randint(45, 220)
            bundle.bank_statements.append(
                {
                    "statement_line_id": deterministic_id("bnk", seed, booking_id, bank_ref),
                    "bank_account": "TREASURY_USD_MAIN_01",
                    "bank_transfer_ref": bank_ref,
                    "booking_id": booking_id,
                    "payment_reference": payment_ref,
                    "original_currency": currency,
                    "settled_original_minor": net_for_bank,
                    "reporting_currency": reporting_currency,
                    "settled_reporting_minor": bank_usd_minor,
                    "fx_rate_applied_micros": fx_micros,
                    "posted_at": _iso(bank_dt),
                }
            )

        # Out-of-order CDC fault injection (~4% of bookings swap last two events in arrival stream)
        if len(booking_events) >= 4 and rng.random() < 0.04:
            booking_events[-1], booking_events[-2] = booking_events[-2], booking_events[-1]
            faults["out_of_order_cdc_events"] += 1

        bundle.cdc_events.extend(booking_events)

        # Duplicate CDC delivery fault (~5% of bookings duplicate capture or cancel event)
        if rng.random() < 0.05:
            dup_ev = dict(booking_events[min(3, len(booking_events) - 1)])
            bundle.cdc_events.append(dup_ev)
            faults["duplicate_cdc_events"] += 1

    # Append duplicate settlement batch lines
    bundle.psp_settlements.extend(duplicate_psp_batch_lines)
    faults["duplicate_settlement_lines"] = len(duplicate_psp_batch_lines)

    # Inject orphan PSP / Bank lines (unmatched)
    num_orphans = max(2, num_bookings // 120)
    for k in range(num_orphans):
        orphan_bkg = f"BKG-ORPHAN-{k + 1:04d}"
        orphan_pay = f"PAY-ORPHAN-{k + 1:04d}"
        bundle.psp_settlements.append(
            {
                "settlement_id": deterministic_id("psp_orphan", seed, k),
                "file_batch_id": "PSP_BATCH_20260518",
                "booking_id": orphan_bkg,
                "payment_reference": orphan_pay,
                "settlement_type": "CAPTURE",
                "currency": "USD",
                "gross_minor": 27_500 + k * 1_000,
                "psp_fee_minor": 828,
                "net_minor": 26_672 + k * 1_000,
                "settled_at": "2026-05-18T14:00:00+00:00",
                "bank_transfer_ref": f"BNK-ORPHAN-{k + 1:04d}",
            }
        )
        faults["unmatched_orphan_lines"] += 1

    # Inject schema-invalid poison CDC events (caught by FastAvro -> DLQ)
    num_poison = max(3, num_bookings // 100)
    poison_templates: list[dict[str, Any]] = [
        {
            "event_id": deterministic_id("poison", seed, "float_money"),
            "booking_id": "BKG-POISON-FLOAT",
            "event_type": "PAYMENT_CAPTURED",
            "cdc_op": "c",
            "sequence_num": 1,
            "gross_total_minor": 199.95,  # Invalid float!
            "currency": "USD",
        },
        {
            "event_id": deterministic_id("poison", seed, "bad_enum"),
            "booking_id": "BKG-POISON-ENUM",
            "listing_id": "LST-US_NYC-001",
            "guest_id": "GST-99999",
            "host_id": "HST-001",
            "event_type": "UNKNOWN_STATE_XYZ",  # Invalid enum!
            "cdc_op": "u",
            "sequence_num": 1,
            "effective_at": "2026-05-10T10:00:00+00:00",
            "recorded_at": "2026-05-10T10:00:01+00:00",
            "host_timezone": "America/New_York",
            "currency": "USD",
            "nights": 2,
            "room_subtotal_minor": 20_000,
            "platform_fee_minor": 2_400,
            "host_fee_minor": 600,
            "tax_minor": 3_000,
            "gross_total_minor": 25_400,
            "tax_policy_version": "US_NYC_v2",
            "jurisdiction": "US_NYC",
            "source_lsn": 999_991,
        },
        {
            "event_id": deterministic_id("poison", seed, "missing_fields"),
            "booking_id": "BKG-POISON-NULL",
            "event_type": "BOOKING_HELD",
            "currency": "EUR",
        },
    ]
    for p_idx in range(num_poison):
        tpl = dict(poison_templates[p_idx % len(poison_templates)])
        tpl["event_id"] = deterministic_id("poison", seed, p_idx)
        tpl["booking_id"] = f"{tpl['booking_id']}-{p_idx + 1}"
        bundle.cdc_events.append(tpl)
        faults["poison_schema_events"] += 1

    bundle.injected_fault_counts = faults
    return bundle
