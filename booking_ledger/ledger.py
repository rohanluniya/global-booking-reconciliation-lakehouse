"""Immutable append-only double-entry accounting ledger engine.

Every posted journal entry strictly enforces:
  SUM(debit_minor) == SUM(credit_minor)  (original currency)
  SUM(reporting_debit_minor) == SUM(reporting_credit_minor)  (reporting USD)

Corrections, modifications, refunds, chargebacks, FX revisions, and post-close
restatements NEVER mutate or delete prior rows; they emit immutable reversing
and/or adjusting entries with explicit `parent_entry_id` lineage.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from booking_ledger.contracts import deterministic_id
from booking_ledger.money import (
    allocate_bps_minor,
    assert_minor_int,
    convert_minor_to_reporting,
)
from booking_ledger.temporal import lookup_fx_as_of

CHART_OF_ACCOUNTS: dict[str, str] = {
    "CASH_CLEARING": "ASSET",
    "ACCOUNTS_RECEIVABLE_PSP": "ASSET",
    "DEFERRED_BOOKING_REVENUE": "LIABILITY",
    "RECOGNIZED_PLATFORM_REVENUE": "REVENUE",
    "HOST_PAYABLE": "LIABILITY",
    "TAX_PAYABLE": "LIABILITY",
    "PAYMENT_PROCESSING_FEE": "EXPENSE",
    "REFUND_CONTRA_REVENUE": "CONTRA_REVENUE",
    "CHARGEBACK_LOSS": "EXPENSE",
    "FX_GAIN_LOSS": "FX_ADJUSTMENT",
}


class LedgerInvariantError(ValueError):
    """Raised when a double-entry debit/credit balance invariant is violated."""


def next_accounting_period(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7])
    if month == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month + 1:02d}"


def build_balanced_journal_entry(
    *,
    journal_entry_id: str,
    booking_id: str,
    source_event_id: str,
    accounting_period: str,
    entry_type: str,
    original_currency: str,
    reporting_currency: str,
    fx_rate_micros: int,
    fx_rate_version: str,
    effective_at: str,
    recorded_at: str,
    legs: list[tuple[str, int, int]],
    parent_entry_id: str | None = None,
    is_restatement: bool = False,
    original_period: str | None = None,
) -> list[dict[str, Any]]:
    """Validate and emit an immutable, strictly balanced double-entry journal entry.

    `legs` is a list of `(account_code, debit_minor, credit_minor)` in original currency.
    Any +/-1 minor unit reporting-currency rounding residual from multi-leg FX translation
    is explicitly booked to `FX_GAIN_LOSS` so reporting debits ALWAYS equal reporting credits.
    """
    orig_debit_sum = 0
    orig_credit_sum = 0
    rep_debit_sum = 0
    rep_credit_sum = 0
    rows: list[dict[str, Any]] = []

    for idx, (acct, debit_val, credit_val) in enumerate(legs):
        if acct not in CHART_OF_ACCOUNTS:
            raise LedgerInvariantError(f"Unknown account code: {acct}")
        d_minor = assert_minor_int(debit_val, f"{acct}.debit_minor")
        c_minor = assert_minor_int(credit_val, f"{acct}.credit_minor")
        if d_minor < 0 or c_minor < 0:
            raise LedgerInvariantError(f"Negative debit/credit leg not allowed: {acct} d={d_minor} c={c_minor}")
        if d_minor > 0 and c_minor > 0:
            raise LedgerInvariantError(f"A single leg cannot carry both debit and credit: {acct}")
        if d_minor == 0 and c_minor == 0:
            continue

        orig_debit_sum += d_minor
        orig_credit_sum += c_minor
        rep_d = convert_minor_to_reporting(d_minor, original_currency, reporting_currency, fx_rate_micros) if d_minor else 0
        rep_c = convert_minor_to_reporting(c_minor, original_currency, reporting_currency, fx_rate_micros) if c_minor else 0
        rep_debit_sum += rep_d
        rep_credit_sum += rep_c

        rows.append(
            {
                "entry_line_id": deterministic_id("jln", journal_entry_id, idx, acct),
                "journal_entry_id": journal_entry_id,
                "parent_entry_id": parent_entry_id,
                "booking_id": booking_id,
                "source_event_id": source_event_id,
                "accounting_period": accounting_period,
                "original_period": original_period or accounting_period,
                "entry_type": entry_type,
                "account_code": acct,
                "account_category": CHART_OF_ACCOUNTS[acct],
                "original_currency": original_currency,
                "debit_minor": d_minor,
                "credit_minor": c_minor,
                "reporting_currency": reporting_currency,
                "fx_rate_micros": assert_minor_int(fx_rate_micros, "fx_rate_micros"),
                "fx_rate_version": fx_rate_version,
                "reporting_debit_minor": rep_d,
                "reporting_credit_minor": rep_c,
                "effective_at": effective_at,
                "recorded_at": recorded_at,
                "is_restatement": is_restatement,
            }
        )

    if orig_debit_sum != orig_credit_sum:
        raise LedgerInvariantError(
            f"Unbalanced original-currency journal entry {journal_entry_id}: debits={orig_debit_sum} != credits={orig_credit_sum}"
        )

    # Absorb any exact translation rounding residual (+/- 1..2 minor units) explicitly into FX_GAIN_LOSS
    rep_diff = rep_debit_sum - rep_credit_sum
    if rep_diff != 0:
        fx_d = 0 if rep_diff > 0 else abs(rep_diff)
        fx_c = rep_diff if rep_diff > 0 else 0
        rows.append(
            {
                "entry_line_id": deterministic_id("jln", journal_entry_id, "fx_round"),
                "journal_entry_id": journal_entry_id,
                "parent_entry_id": parent_entry_id,
                "booking_id": booking_id,
                "source_event_id": source_event_id,
                "accounting_period": accounting_period,
                "original_period": original_period or accounting_period,
                "entry_type": entry_type,
                "account_code": "FX_GAIN_LOSS",
                "account_category": CHART_OF_ACCOUNTS["FX_GAIN_LOSS"],
                "original_currency": original_currency,
                "debit_minor": 0,
                "credit_minor": 0,
                "reporting_currency": reporting_currency,
                "fx_rate_micros": fx_rate_micros,
                "fx_rate_version": fx_rate_version,
                "reporting_debit_minor": fx_d,
                "reporting_credit_minor": fx_c,
                "effective_at": effective_at,
                "recorded_at": recorded_at,
                "is_restatement": is_restatement,
            }
        )
        rep_debit_sum += fx_d
        rep_credit_sum += fx_c

    if rep_debit_sum != rep_credit_sum:
        raise LedgerInvariantError(
            f"Unbalanced reporting-currency journal entry {journal_entry_id}: debits={rep_debit_sum} != credits={rep_credit_sum}"
        )
    return rows


def build_reporting_only_adjustment(
    *,
    journal_entry_id: str,
    booking_id: str,
    source_event_id: str,
    accounting_period: str,
    entry_type: str,
    original_currency: str,
    reporting_currency: str,
    fx_rate_micros: int,
    fx_rate_version: str,
    effective_at: str,
    recorded_at: str,
    debit_account: str,
    credit_account: str,
    reporting_amount_minor: int,
    parent_entry_id: str | None = None,
    is_restatement: bool = False,
    original_period: str | None = None,
) -> list[dict[str, Any]]:
    """Emit a reporting-currency-only FX revaluation entry (e.g. late FX rate revision)."""
    amt = assert_minor_int(reporting_amount_minor, "reporting_amount_minor")
    if amt <= 0:
        return []
    for acct in (debit_account, credit_account):
        if acct not in CHART_OF_ACCOUNTS:
            raise LedgerInvariantError(f"Unknown account code: {acct}")
    return [
        {
            "entry_line_id": deterministic_id("jln", journal_entry_id, 0, debit_account),
            "journal_entry_id": journal_entry_id,
            "parent_entry_id": parent_entry_id,
            "booking_id": booking_id,
            "source_event_id": source_event_id,
            "accounting_period": accounting_period,
            "original_period": original_period or accounting_period,
            "entry_type": entry_type,
            "account_code": debit_account,
            "account_category": CHART_OF_ACCOUNTS[debit_account],
            "original_currency": original_currency,
            "debit_minor": 0,
            "credit_minor": 0,
            "reporting_currency": reporting_currency,
            "fx_rate_micros": fx_rate_micros,
            "fx_rate_version": fx_rate_version,
            "reporting_debit_minor": amt,
            "reporting_credit_minor": 0,
            "effective_at": effective_at,
            "recorded_at": recorded_at,
            "is_restatement": is_restatement,
        },
        {
            "entry_line_id": deterministic_id("jln", journal_entry_id, 1, credit_account),
            "journal_entry_id": journal_entry_id,
            "parent_entry_id": parent_entry_id,
            "booking_id": booking_id,
            "source_event_id": source_event_id,
            "accounting_period": accounting_period,
            "original_period": original_period or accounting_period,
            "entry_type": entry_type,
            "account_code": credit_account,
            "account_category": CHART_OF_ACCOUNTS[credit_account],
            "original_currency": original_currency,
            "debit_minor": 0,
            "credit_minor": 0,
            "reporting_currency": reporting_currency,
            "fx_rate_micros": fx_rate_micros,
            "fx_rate_version": fx_rate_version,
            "reporting_debit_minor": 0,
            "reporting_credit_minor": amt,
            "effective_at": effective_at,
            "recorded_at": recorded_at,
            "is_restatement": is_restatement,
        },
    ]


def generate_double_entry_ledger(
    booking_history: list[dict[str, Any]],
    fx_rates: list[dict[str, Any]],
    bank_statements: list[dict[str, Any]],
    reporting_currency: str = "USD",
    closed_periods: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Generate the complete immutable double-entry ledger from booking state history, FX rates, and bank clearings."""
    closed = closed_periods or set()
    entries: list[dict[str, Any]] = []
    # Track active booking capture entry for reversing on modification and FX revaluation
    active_capture_state: dict[str, dict[str, Any]] = {}

    def resolve_period(raw_period: str) -> tuple[str, bool]:
        if raw_period in closed:
            target = next_accounting_period(raw_period)
            while target in closed:
                target = next_accounting_period(target)
            return target, True
        return raw_period, False

    for row in booking_history:
        ev_type = str(row["event_type"])
        booking_id = str(row["booking_id"])
        source_event_id = str(row["event_id"])
        currency = str(row["currency"])
        effective_at = str(row["effective_at"])
        recorded_at = str(row["recorded_at"])
        orig_period = str(row["accounting_period"])
        post_period, is_restatement = resolve_period(orig_period)

        # Look up FX as known at event recording time (true bi-temporal as-of FX)
        fx_rec = lookup_fx_as_of(
            fx_rates,
            currency,
            effective_at,
            recorded_as_of=recorded_at,
            reporting_currency=reporting_currency,
        )
        fx_micros = int(fx_rec["rate_micros"])
        fx_version = str(fx_rec["rate_version"])

        room = int(row["room_subtotal_minor"])
        plat_fee = int(row["platform_fee_minor"])
        host_fee = int(row["host_fee_minor"])
        tax = int(row["tax_minor"])
        gross = int(row["gross_total_minor"])
        deferred_rev = plat_fee + host_fee
        host_net = room - host_fee

        if ev_type in ("PAYMENT_CAPTURED", "PAYMENT_RETRY_SUCCEEDED"):
            je_id = deterministic_id("je", source_event_id, "CAPTURE")
            legs = [
                ("ACCOUNTS_RECEIVABLE_PSP", gross, 0),
                ("DEFERRED_BOOKING_REVENUE", 0, deferred_rev),
                ("HOST_PAYABLE", 0, host_net),
                ("TAX_PAYABLE", 0, tax),
            ]
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="BOOKING_CAPTURE",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=legs,
                    is_restatement=is_restatement,
                )
            )
            # Accrue expected PSP processing fee
            expected_psp_fee = allocate_bps_minor(gross, 290) + 30
            fee_je_id = deterministic_id("je", source_event_id, "PSP_FEE")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=fee_je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="PSP_FEE_ACCRUAL",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("PAYMENT_PROCESSING_FEE", expected_psp_fee, 0),
                        ("ACCOUNTS_RECEIVABLE_PSP", 0, expected_psp_fee),
                    ],
                    parent_entry_id=je_id,
                    is_restatement=is_restatement,
                )
            )
            active_capture_state[booking_id] = {
                "journal_entry_id": je_id,
                "fee_journal_entry_id": fee_je_id,
                "gross": gross,
                "deferred_rev": deferred_rev,
                "host_net": host_net,
                "tax": tax,
                "expected_psp_fee": expected_psp_fee,
                "currency": currency,
                "fx_micros": fx_micros,
                "fx_version": fx_version,
                "effective_at": effective_at,
                "recorded_at": recorded_at,
                "accounting_period": post_period,
                "source_event_id": source_event_id,
            }

        elif ev_type == "BOOKING_MODIFIED" and booking_id in active_capture_state:
            prev = active_capture_state[booking_id]
            # 1. Immutable Reversal of prior capture (never mutate historical rows!)
            rev_je_id = deterministic_id("je", source_event_id, "MOD_REVERSAL")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=rev_je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="BOOKING_MODIFICATION_REVERSAL",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=prev["fx_micros"],
                    fx_rate_version=prev["fx_version"],
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("DEFERRED_BOOKING_REVENUE", prev["deferred_rev"], 0),
                        ("HOST_PAYABLE", prev["host_net"], 0),
                        ("TAX_PAYABLE", prev["tax"], 0),
                        ("ACCOUNTS_RECEIVABLE_PSP", 0, prev["gross"]),
                    ],
                    parent_entry_id=prev["journal_entry_id"],
                    is_restatement=is_restatement,
                )
            )
            # 2. Reverse prior PSP fee accrual
            rev_fee_id = deterministic_id("je", source_event_id, "MOD_FEE_REVERSAL")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=rev_fee_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="PSP_FEE_REVERSAL",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=prev["fx_micros"],
                    fx_rate_version=prev["fx_version"],
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("ACCOUNTS_RECEIVABLE_PSP", prev["expected_psp_fee"], 0),
                        ("PAYMENT_PROCESSING_FEE", 0, prev["expected_psp_fee"]),
                    ],
                    parent_entry_id=prev["fee_journal_entry_id"],
                    is_restatement=is_restatement,
                )
            )
            # 3. Post new adjusted booking capture
            adj_je_id = deterministic_id("je", source_event_id, "MOD_ADJUSTMENT")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=adj_je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="BOOKING_MODIFICATION_ADJUSTMENT",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("ACCOUNTS_RECEIVABLE_PSP", gross, 0),
                        ("DEFERRED_BOOKING_REVENUE", 0, deferred_rev),
                        ("HOST_PAYABLE", 0, host_net),
                        ("TAX_PAYABLE", 0, tax),
                    ],
                    parent_entry_id=rev_je_id,
                    is_restatement=is_restatement,
                )
            )
            new_psp_fee = allocate_bps_minor(gross, 290) + 30
            new_fee_id = deterministic_id("je", source_event_id, "MOD_FEE_ACCRUAL")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=new_fee_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="PSP_FEE_ACCRUAL",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("PAYMENT_PROCESSING_FEE", new_psp_fee, 0),
                        ("ACCOUNTS_RECEIVABLE_PSP", 0, new_psp_fee),
                    ],
                    parent_entry_id=adj_je_id,
                    is_restatement=is_restatement,
                )
            )
            active_capture_state[booking_id] = {
                "journal_entry_id": adj_je_id,
                "fee_journal_entry_id": new_fee_id,
                "gross": gross,
                "deferred_rev": deferred_rev,
                "host_net": host_net,
                "tax": tax,
                "expected_psp_fee": new_psp_fee,
                "currency": currency,
                "fx_micros": fx_micros,
                "fx_version": fx_version,
                "effective_at": effective_at,
                "recorded_at": recorded_at,
                "accounting_period": post_period,
                "source_event_id": source_event_id,
            }

        elif ev_type == "BOOKING_COMPLETED":
            je_id = deterministic_id("je", source_event_id, "REV_REC")
            parent_id = active_capture_state.get(booking_id, {}).get("journal_entry_id")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="REVENUE_RECOGNITION",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("DEFERRED_BOOKING_REVENUE", deferred_rev, 0),
                        ("RECOGNIZED_PLATFORM_REVENUE", 0, deferred_rev),
                    ],
                    parent_entry_id=parent_id,
                    is_restatement=is_restatement,
                )
            )

        elif ev_type == "HOST_PAYOUT_SETTLED":
            je_id = deterministic_id("je", source_event_id, "HOST_PAYOUT")
            parent_id = active_capture_state.get(booking_id, {}).get("journal_entry_id")
            entries.extend(
                build_balanced_journal_entry(
                    journal_entry_id=je_id,
                    booking_id=booking_id,
                    source_event_id=source_event_id,
                    accounting_period=post_period,
                    original_period=orig_period,
                    entry_type="HOST_PAYOUT",
                    original_currency=currency,
                    reporting_currency=reporting_currency,
                    fx_rate_micros=fx_micros,
                    fx_rate_version=fx_version,
                    effective_at=effective_at,
                    recorded_at=recorded_at,
                    legs=[
                        ("HOST_PAYABLE", host_net, 0),
                        ("CASH_CLEARING", 0, host_net),
                    ],
                    parent_entry_id=parent_id,
                    is_restatement=is_restatement,
                )
            )

        elif ev_type in ("REFUND_ISSUED", "PARTIAL_REFUND_ISSUED"):
            refund_minor = int(row.get("refund_minor", 0))
            if refund_minor > 0:
                je_id = deterministic_id("je", source_event_id, ev_type)
                parent_id = active_capture_state.get(booking_id, {}).get("journal_entry_id")
                if refund_minor >= gross:
                    r_rev = deferred_rev
                    r_tax = tax
                    r_host = refund_minor - r_rev - r_tax
                else:
                    # Proportional allocation with exact remainder absorbed by host share
                    ratio_bps = (refund_minor * 10_000) // max(1, gross)
                    r_rev = allocate_bps_minor(deferred_rev, ratio_bps)
                    r_tax = allocate_bps_minor(tax, ratio_bps)
                    r_host = refund_minor - r_rev - r_tax
                entries.extend(
                    build_balanced_journal_entry(
                        journal_entry_id=je_id,
                        booking_id=booking_id,
                        source_event_id=source_event_id,
                        accounting_period=post_period,
                        original_period=orig_period,
                        entry_type=ev_type,
                        original_currency=currency,
                        reporting_currency=reporting_currency,
                        fx_rate_micros=fx_micros,
                        fx_rate_version=fx_version,
                        effective_at=effective_at,
                        recorded_at=recorded_at,
                        legs=[
                            ("REFUND_CONTRA_REVENUE", r_rev, 0),
                            ("HOST_PAYABLE", r_host, 0),
                            ("TAX_PAYABLE", r_tax, 0),
                            ("ACCOUNTS_RECEIVABLE_PSP", 0, refund_minor),
                        ],
                        parent_entry_id=parent_id,
                        is_restatement=is_restatement,
                    )
                )

        elif ev_type == "CHARGEBACK_LOST":
            cb_minor = int(row.get("chargeback_minor", 0))
            if cb_minor > 0:
                je_id = deterministic_id("je", source_event_id, "CHARGEBACK")
                parent_id = active_capture_state.get(booking_id, {}).get("journal_entry_id")
                entries.extend(
                    build_balanced_journal_entry(
                        journal_entry_id=je_id,
                        booking_id=booking_id,
                        source_event_id=source_event_id,
                        accounting_period=post_period,
                        original_period=orig_period,
                        entry_type="CHARGEBACK_LOSS_RECOGNITION",
                        original_currency=currency,
                        reporting_currency=reporting_currency,
                        fx_rate_micros=fx_micros,
                        fx_rate_version=fx_version,
                        effective_at=effective_at,
                        recorded_at=recorded_at,
                        legs=[
                            ("CHARGEBACK_LOSS", cb_minor, 0),
                            ("ACCOUNTS_RECEIVABLE_PSP", 0, cb_minor),
                        ],
                        parent_entry_id=parent_id,
                        is_restatement=is_restatement,
                    )
                )

    # Bank Clearing entries (positive cash inflow from PSP into treasury bank)
    for bnk in bank_statements:
        booking_id = str(bnk["booking_id"])
        if booking_id not in active_capture_state:
            continue
        settled_orig = int(bnk["settled_original_minor"])
        if settled_orig <= 0:
            continue
        curr = str(bnk["original_currency"])
        posted_at = str(bnk["posted_at"])
        orig_period = posted_at[:7]
        post_period, is_restatement = resolve_period(orig_period)
        fx_micros = int(bnk["fx_rate_applied_micros"])
        je_id = deterministic_id("je", bnk["statement_line_id"], "BANK_CLEARING")
        entries.extend(
            build_balanced_journal_entry(
                journal_entry_id=je_id,
                booking_id=booking_id,
                source_event_id=str(bnk["statement_line_id"]),
                accounting_period=post_period,
                original_period=orig_period,
                entry_type="PSP_BANK_CLEARING",
                original_currency=curr,
                reporting_currency=reporting_currency,
                fx_rate_micros=fx_micros,
                fx_rate_version="bank_settled",
                effective_at=posted_at,
                recorded_at=posted_at,
                legs=[
                    ("CASH_CLEARING", settled_orig, 0),
                    ("ACCOUNTS_RECEIVABLE_PSP", 0, settled_orig),
                ],
                parent_entry_id=active_capture_state[booking_id]["journal_entry_id"],
                is_restatement=is_restatement,
            )
        )

    # Late FX Revision Adjustments: never overwrite prior entries; emit explicit FX_GAIN_LOSS adjustment
    for booking_id, state in sorted(active_capture_state.items()):
        curr = state["currency"]
        if curr == reporting_currency:
            continue
        latest_fx = lookup_fx_as_of(
            fx_rates,
            curr,
            state["effective_at"],
            recorded_as_of=None,
            reporting_currency=reporting_currency,
        )
        if str(latest_fx["rate_version"]) != str(state["fx_version"]) and int(latest_fx["rate_micros"]) != int(state["fx_micros"]):
            old_rep = convert_minor_to_reporting(state["gross"], curr, reporting_currency, int(state["fx_micros"]))
            new_rep = convert_minor_to_reporting(state["gross"], curr, reporting_currency, int(latest_fx["rate_micros"]))
            delta_rep = new_rep - old_rep
            if delta_rep != 0:
                rev_recorded = str(latest_fx["recorded_at"])
                orig_period = rev_recorded[:7]
                post_period, is_restatement = resolve_period(orig_period)
                je_id = deterministic_id("je", booking_id, latest_fx["fx_id"], "FX_REVAL")
                debit_acct = "ACCOUNTS_RECEIVABLE_PSP" if delta_rep > 0 else "FX_GAIN_LOSS"
                credit_acct = "FX_GAIN_LOSS" if delta_rep > 0 else "ACCOUNTS_RECEIVABLE_PSP"
                entries.extend(
                    build_reporting_only_adjustment(
                        journal_entry_id=je_id,
                        booking_id=booking_id,
                        source_event_id=str(latest_fx["fx_id"]),
                        accounting_period=post_period,
                        original_period=orig_period,
                        entry_type="FX_REVISION_ADJUSTMENT",
                        original_currency=curr,
                        reporting_currency=reporting_currency,
                        fx_rate_micros=int(latest_fx["rate_micros"]),
                        fx_rate_version=str(latest_fx["rate_version"]),
                        effective_at=state["effective_at"],
                        recorded_at=rev_recorded,
                        debit_account=debit_acct,
                        credit_account=credit_acct,
                        reporting_amount_minor=abs(delta_rep),
                        parent_entry_id=state["journal_entry_id"],
                        is_restatement=is_restatement,
                    )
                )

    return entries


def verify_ledger_zero_sum(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Verify debit == credit per journal_entry_id, per currency, and globally in reporting USD."""
    by_je_orig: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    by_je_rep: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    by_currency: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    total_rep_debit = 0
    total_rep_credit = 0

    for row in entries:
        je_id = str(row["journal_entry_id"])
        curr = str(row["original_currency"])
        d = assert_minor_int(row["debit_minor"], "debit_minor")
        c = assert_minor_int(row["credit_minor"], "credit_minor")
        rd = assert_minor_int(row["reporting_debit_minor"], "reporting_debit_minor")
        rc = assert_minor_int(row["reporting_credit_minor"], "reporting_credit_minor")

        by_je_orig[je_id][0] += d
        by_je_orig[je_id][1] += c
        by_je_rep[je_id][0] += rd
        by_je_rep[je_id][1] += rc
        by_currency[curr][0] += d
        by_currency[curr][1] += c
        total_rep_debit += rd
        total_rep_credit += rc

    unbalanced_entries = [
        je_id
        for je_id in by_je_orig
        if by_je_orig[je_id][0] != by_je_orig[je_id][1] or by_je_rep[je_id][0] != by_je_rep[je_id][1]
    ]
    unbalanced_currencies = [c for c, (d, cr) in by_currency.items() if d != cr]
    global_imbalance = abs(total_rep_debit - total_rep_credit)

    return {
        "balanced": len(unbalanced_entries) == 0 and len(unbalanced_currencies) == 0 and global_imbalance == 0,
        "journal_entry_count": len(by_je_orig),
        "entry_line_count": len(entries),
        "total_reporting_debits_minor": total_rep_debit,
        "total_reporting_credits_minor": total_rep_credit,
        "global_imbalance_minor": global_imbalance,
        "unbalanced_entries": unbalanced_entries,
        "unbalanced_currencies": unbalanced_currencies,
        "currency_totals": {c: {"debit_minor": v[0], "credit_minor": v[1]} for c, v in sorted(by_currency.items())},
    }


def compute_trial_balance(
    entries: list[dict[str, Any]],
    period: str | None = None,
) -> dict[str, Any]:
    """Compute exact account-level trial balance (optional period filter)."""
    filtered = [e for e in entries if period is None or e["accounting_period"] == period]
    acct_totals: dict[str, dict[str, Any]] = {}

    for acct, category in CHART_OF_ACCOUNTS.items():
        acct_totals[acct] = {
            "account_code": acct,
            "account_category": category,
            "reporting_debit_minor": 0,
            "reporting_credit_minor": 0,
            "net_reporting_minor": 0,
            "line_count": 0,
        }

    for row in filtered:
        acct = str(row["account_code"])
        bucket = acct_totals[acct]
        rd = int(row["reporting_debit_minor"])
        rc = int(row["reporting_credit_minor"])
        bucket["reporting_debit_minor"] += rd
        bucket["reporting_credit_minor"] += rc
        bucket["net_reporting_minor"] = bucket["reporting_debit_minor"] - bucket["reporting_credit_minor"]
        bucket["line_count"] += 1

    total_debits = sum(a["reporting_debit_minor"] for a in acct_totals.values())
    total_credits = sum(a["reporting_credit_minor"] for a in acct_totals.values())
    imbalance = abs(total_debits - total_credits)

    return {
        "period": period or "ALL",
        "reporting_currency": "USD",
        "accounts": list(acct_totals.values()),
        "total_reporting_debits_minor": total_debits,
        "total_reporting_credits_minor": total_credits,
        "imbalance_minor": imbalance,
        "is_balanced": imbalance == 0,
        "entry_line_count": len(filtered),
        "restatement_line_count": sum(1 for e in filtered if bool(e.get("is_restatement", False))),
    }
