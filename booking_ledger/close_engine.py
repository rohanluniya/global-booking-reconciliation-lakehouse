"""Daily & Month-End Financial Close Engine, Restatement Enforcement & Sign-Off Certificate."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from booking_ledger.contracts import deterministic_id
from booking_ledger.ledger import (
    build_balanced_journal_entry,
    compute_trial_balance,
    next_accounting_period,
)
from booking_ledger.money import FX_SCALE

VALID_PERIOD_STATES = ("OPEN", "SOFT_CLOSE", "HARD_CLOSED")


class PeriodCloseError(ValueError):
    """Raised when an invalid period close transition or mutation is attempted."""


def close_accounting_period(
    *,
    period: str,
    target_state: str,
    current_state: str,
    ledger_entries: list[dict[str, Any]],
    recon_summary: dict[str, Any],
    dq_pass_rate: float,
    actor: str = "system-close-controller",
    closed_at: str | None = None,
) -> dict[str, Any]:
    """Transition an accounting period (`OPEN -> SOFT_CLOSE -> HARD_CLOSED`) and issue Sign-Off Certificate."""
    if target_state not in ("SOFT_CLOSE", "HARD_CLOSED"):
        raise PeriodCloseError(f"Invalid target close state: {target_state}")
    if current_state == "HARD_CLOSED" and target_state != "HARD_CLOSED":
        raise PeriodCloseError(f"Period {period} is already HARD_CLOSED and cannot be reopened")
    if target_state == "HARD_CLOSED" and current_state == "OPEN":
        # Allow direct escalation only when trial balance & DQ pass cleanly; enforce checks below
        pass

    tb = compute_trial_balance(ledger_entries, period=period)
    if not tb["is_balanced"]:
        raise PeriodCloseError(
            f"Cannot close period {period}: trial balance imbalance = {tb['imbalance_minor']} minor units"
        )
    if target_state == "HARD_CLOSED" and dq_pass_rate < 1.0:
        raise PeriodCloseError(
            f"Cannot HARD_CLOSE period {period}: DQ pass rate is {dq_pass_rate:.4f} (< 1.0)"
        )

    close_ts = closed_at or datetime.now(UTC).isoformat()
    snapshot_id = deterministic_id("cls", period, target_state, tb["entry_line_count"])

    cert_payload = {
        "snapshot_id": snapshot_id,
        "accounting_period": period,
        "status": target_state,
        "closed_at": close_ts,
        "closed_by": actor,
        "entry_line_count": tb["entry_line_count"],
        "restatement_line_count": tb["restatement_line_count"],
        "total_reporting_debits_minor": tb["total_reporting_debits_minor"],
        "total_reporting_credits_minor": tb["total_reporting_credits_minor"],
        "trial_balance_imbalance_minor": tb["imbalance_minor"],
        "is_trial_balance_zero_sum": tb["is_balanced"],
        "reconciliation_match_rate": recon_summary["match_rate"],
        "open_exception_count": recon_summary["open_exception_count"],
        "open_exception_exposure_usd_minor": recon_summary["open_exception_exposure_usd_minor"],
        "dq_pass_rate": dq_pass_rate,
        "accounts_json": json.dumps(tb["accounts"], sort_keys=True),
    }
    digest = hashlib.sha256(json.dumps(cert_payload, sort_keys=True).encode("utf-8")).hexdigest()
    cert_payload["certificate_sha256"] = digest
    return cert_payload


def post_late_restatement_entry(
    *,
    closed_period: str,
    closed_periods: set[str],
    booking_id: str,
    reason: str,
    amount_minor: int,
    currency: str = "USD",
    fx_rate_micros: int = FX_SCALE,
    entry_type: str = "POST_CLOSE_CHARGEBACK_RESTATEMENT",
    debit_account: str = "CHARGEBACK_LOSS",
    credit_account: str = "ACCOUNTS_RECEIVABLE_PSP",
    parent_entry_id: str | None = None,
    recorded_at: str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Enforce closed-period immutability by routing late adjustments to the next open period as a Restatement."""
    if closed_period not in closed_periods:
        raise PeriodCloseError(f"Period {closed_period} is not HARD_CLOSED; post normal adjustment instead")
    target_period = next_accounting_period(closed_period)
    while target_period in closed_periods:
        target_period = next_accounting_period(target_period)

    ts = recorded_at or datetime.now(UTC).isoformat()
    je_id = deterministic_id("je_rst", booking_id, closed_period, target_period, reason, amount_minor)
    entries = build_balanced_journal_entry(
        journal_entry_id=je_id,
        booking_id=booking_id,
        source_event_id=deterministic_id("rst_src", booking_id, closed_period, reason),
        accounting_period=target_period,
        original_period=closed_period,
        entry_type=entry_type,
        original_currency=currency,
        reporting_currency="USD",
        fx_rate_micros=fx_rate_micros,
        fx_rate_version="restatement_asof",
        effective_at=f"{closed_period}-28T23:59:00+00:00",
        recorded_at=ts,
        legs=[(debit_account, amount_minor, 0), (credit_account, 0, amount_minor)],
        parent_entry_id=parent_entry_id,
        is_restatement=True,
    )
    return target_period, entries
