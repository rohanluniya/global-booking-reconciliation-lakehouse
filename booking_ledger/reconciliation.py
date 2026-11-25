"""3-Way Financial Reconciliation Engine (Internal Ledger vs PSP vs Bank) & Exception Workflow."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from booking_ledger.contracts import deterministic_id
from booking_ledger.ledger import build_balanced_journal_entry
from booking_ledger.money import FX_SCALE, convert_minor_to_reporting

VALID_CLASSIFICATIONS = (
    "MATCHED",
    "TIMING_IN_TRANSIT",
    "DUPLICATE_FILE_DEDUPED",
    "FEE_VARIANCE",
    "FX_VARIANCE",
    "PARTIAL_REFUND_MISMATCH",
    "CHARGEBACK_OPEN",
    "UNMATCHED_PSP_OR_BANK",
)


def aging_bucket_for_days(age_days: int) -> str:
    if age_days <= 1:
        return "0-1d"
    if age_days <= 7:
        return "2-7d"
    if age_days <= 30:
        return "8-30d"
    return ">30d"


def deduplicate_psp_settlements(
    raw_psp_lines: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str], int]:
    """Deduplicate duplicate payment processor settlement files deterministically.

    Returns `(deduped_lines, bookings_with_deduped_files, duplicate_line_count)`.
    """
    seen_ids: set[str] = set()
    deduped: list[dict[str, Any]] = []
    bookings_deduped: set[str] = set()
    dup_count = 0

    for line in sorted(raw_psp_lines, key=lambda r: (str(r["settlement_id"]), str(r["file_batch_id"]))):
        sig = (
            str(line["settlement_id"]),
            str(line["booking_id"]),
            str(line["settlement_type"]),
            int(line["net_minor"]),
        )
        sig_key = "|".join(str(x) for x in sig)
        if sig_key in seen_ids:
            dup_count += 1
            bookings_deduped.add(str(line["booking_id"]))
            continue
        seen_ids.add(sig_key)
        deduped.append(line)

    return deduped, bookings_deduped, dup_count


def run_three_way_reconciliation(
    *,
    booking_history: list[dict[str, Any]],
    ledger_entries: list[dict[str, Any]],
    raw_psp_settlements: list[dict[str, Any]],
    bank_statements: list[dict[str, Any]],
    as_of_timestamp: str = "2026-05-26T00:00:00+00:00",
    reporting_currency: str = "USD",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Reconcile Internal Ledger, PSP Settlements, and Bank Statements across all bookings."""
    as_of_dt = datetime.fromisoformat(as_of_timestamp)
    deduped_psp, deduped_bookings, dup_line_count = deduplicate_psp_settlements(raw_psp_settlements)

    # 1. Summarize internal ledger expectations per booking_id
    latest_booking: dict[str, dict[str, Any]] = {}
    for row in booking_history:
        if bool(row.get("is_current", False)):
            latest_booking[str(row["booking_id"])] = row

    ledger_by_booking: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "expected_capture_gross_minor": 0,
            "expected_psp_fee_minor": 0,
            "expected_refund_minor": 0,
            "expected_chargeback_minor": 0,
            "currency": "USD",
            "fx_rate_micros": FX_SCALE,
            "accounting_period": "2026-05",
            "latest_effective_at": as_of_timestamp,
        }
    )

    for entry in ledger_entries:
        bid = str(entry["booking_id"])
        acct = str(entry["account_code"])
        etype = str(entry["entry_type"])
        d = int(entry["debit_minor"])
        c = int(entry["credit_minor"])
        info = ledger_by_booking[bid]
        info["currency"] = str(entry["original_currency"])
        if etype in ("BOOKING_CAPTURE", "BOOKING_MODIFICATION_ADJUSTMENT") and acct == "ACCOUNTS_RECEIVABLE_PSP":
            info["expected_capture_gross_minor"] += d
            info["fx_rate_micros"] = int(entry["fx_rate_micros"])
            info["accounting_period"] = str(entry["accounting_period"])
            info["latest_effective_at"] = str(entry["effective_at"])
        elif etype == "BOOKING_MODIFICATION_REVERSAL" and acct == "ACCOUNTS_RECEIVABLE_PSP":
            info["expected_capture_gross_minor"] -= c
        elif etype == "PSP_FEE_ACCRUAL" and acct == "PAYMENT_PROCESSING_FEE":
            info["expected_psp_fee_minor"] += d
        elif etype == "PSP_FEE_REVERSAL" and acct == "PAYMENT_PROCESSING_FEE":
            info["expected_psp_fee_minor"] -= c
        elif etype in ("REFUND_ISSUED", "PARTIAL_REFUND_ISSUED") and acct == "ACCOUNTS_RECEIVABLE_PSP":
            info["expected_refund_minor"] += c
        elif etype == "CHARGEBACK_LOSS_RECOGNITION" and acct == "CHARGEBACK_LOSS":
            info["expected_chargeback_minor"] += d

    # 2. Summarize PSP settlements per booking_id
    psp_by_booking: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "capture_gross_minor": 0,
            "psp_fee_minor": 0,
            "refund_minor": 0,
            "chargeback_minor": 0,
            "net_minor": 0,
            "currency": "USD",
            "payment_reference": "",
            "settled_at": as_of_timestamp,
            "has_capture": False,
            "has_chargeback": False,
        }
    )
    for line in deduped_psp:
        bid = str(line["booking_id"])
        p = psp_by_booking[bid]
        p["currency"] = str(line["currency"])
        p["payment_reference"] = str(line["payment_reference"])
        p["settled_at"] = str(line["settled_at"])
        stype = str(line["settlement_type"])
        if stype == "CAPTURE":
            p["has_capture"] = True
            p["capture_gross_minor"] += int(line["gross_minor"])
            p["psp_fee_minor"] += int(line["psp_fee_minor"])
            p["net_minor"] += int(line["net_minor"])
        elif stype == "REFUND":
            p["refund_minor"] += abs(int(line["gross_minor"]))
            p["net_minor"] += int(line["net_minor"])
        elif stype == "CHARGEBACK":
            p["has_chargeback"] = True
            p["chargeback_minor"] += abs(int(line["gross_minor"]))
            p["net_minor"] += int(line["net_minor"])

    # 3. Summarize Bank statements per booking_id
    bank_by_booking: dict[str, dict[str, Any]] = {}
    for bnk in bank_statements:
        bid = str(bnk["booking_id"])
        bank_by_booking[bid] = {
            "settled_original_minor": int(bnk["settled_original_minor"]),
            "settled_reporting_minor": int(bnk["settled_reporting_minor"]),
            "fx_rate_applied_micros": int(bnk["fx_rate_applied_micros"]),
            "posted_at": str(bnk["posted_at"]),
            "bank_transfer_ref": str(bnk["bank_transfer_ref"]),
            "original_currency": str(bnk["original_currency"]),
        }

    all_booking_ids = sorted(set(ledger_by_booking.keys()) | set(psp_by_booking.keys()) | set(bank_by_booking.keys()))

    recon_results: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    counts_by_status: dict[str, int] = {k: 0 for k in VALID_CLASSIFICATIONS}

    for bid in all_booking_ids:
        in_bank = bid in bank_by_booking

        l_info = ledger_by_booking.get(bid)
        p_info = psp_by_booking.get(bid)
        b_info = bank_by_booking.get(bid)

        currency = (
            l_info["currency"]
            if l_info
            else (p_info["currency"] if p_info else (b_info["original_currency"] if b_info else "USD"))
        )
        fx_micros = (
            int(b_info["fx_rate_applied_micros"])
            if b_info
            else (int(l_info["fx_rate_micros"]) if l_info else FX_SCALE)
        )
        period = l_info["accounting_period"] if l_info else (p_info["settled_at"][:7] if p_info else "2026-05")
        event_ts_str = l_info["latest_effective_at"] if l_info else (p_info["settled_at"] if p_info else as_of_timestamp)
        age_days = max(0, (as_of_dt - datetime.fromisoformat(event_ts_str)).days)

        ledger_expected_net = (
            l_info["expected_capture_gross_minor"]
            - l_info["expected_psp_fee_minor"]
            - l_info["expected_refund_minor"]
            - l_info["expected_chargeback_minor"]
            if l_info
            else 0
        )
        psp_net = p_info["net_minor"] if p_info else 0
        bank_orig = b_info["settled_original_minor"] if b_info else 0
        bank_rep_usd = b_info["settled_reporting_minor"] if b_info else 0
        expected_bank_rep_usd = (
            convert_minor_to_reporting(bank_orig, currency, reporting_currency, fx_micros) if in_bank else 0
        )

        classification = "MATCHED"
        variance_minor = 0
        variance_usd_minor = 0
        reason = "3-way balanced across Internal Ledger, PSP Settlement, and Bank Statement"

        if l_info is None or p_info is None:
            classification = "UNMATCHED_PSP_OR_BANK"
            variance_minor = abs(ledger_expected_net - psp_net)
            variance_usd_minor = convert_minor_to_reporting(variance_minor, currency, reporting_currency, fx_micros)
            reason = "Orphan PSP/Bank settlement line without corresponding internal booking ledger entry"
        elif p_info["has_chargeback"] or (l_info["expected_chargeback_minor"] > 0):
            classification = "CHARGEBACK_OPEN"
            variance_minor = abs(ledger_expected_net - psp_net)
            variance_usd_minor = convert_minor_to_reporting(variance_minor, currency, reporting_currency, fx_micros)
            reason = "Chargeback dispute fee/principal exposure pending issuer representment review"
        elif not in_bank:
            classification = "TIMING_IN_TRANSIT"
            variance_minor = abs(psp_net)
            variance_usd_minor = convert_minor_to_reporting(variance_minor, currency, reporting_currency, fx_micros)
            reason = "Captured at PSP; cash payout in transit and not yet posted on bank statement"
        elif l_info["expected_refund_minor"] != p_info["refund_minor"]:
            classification = "PARTIAL_REFUND_MISMATCH"
            variance_minor = abs(l_info["expected_refund_minor"] - p_info["refund_minor"])
            variance_usd_minor = convert_minor_to_reporting(variance_minor, currency, reporting_currency, fx_micros)
            reason = (
                f"Ledger refund ({l_info['expected_refund_minor']}) != PSP refund ({p_info['refund_minor']})"
            )
        elif l_info["expected_psp_fee_minor"] != p_info["psp_fee_minor"]:
            classification = "FEE_VARIANCE"
            variance_minor = abs(l_info["expected_psp_fee_minor"] - p_info["psp_fee_minor"])
            variance_usd_minor = convert_minor_to_reporting(variance_minor, currency, reporting_currency, fx_micros)
            reason = (
                f"PSP fee ({p_info['psp_fee_minor']}) exceeded expected fee ({l_info['expected_psp_fee_minor']})"
            )
        elif bank_rep_usd != expected_bank_rep_usd:
            classification = "FX_VARIANCE"
            variance_minor = abs(bank_rep_usd - expected_bank_rep_usd)
            variance_usd_minor = variance_minor
            reason = f"Bank settled USD ({bank_rep_usd}) differs from as-of FX expected USD ({expected_bank_rep_usd})"
        elif bid in deduped_bookings:
            classification = "DUPLICATE_FILE_DEDUPED"
            variance_minor = 0
            variance_usd_minor = 0
            reason = "Duplicate PSP settlement batch detected and idempotently deduplicated with zero financial impact"

        counts_by_status[classification] += 1
        recon_id = deterministic_id("rcn", bid, period)
        recon_results.append(
            {
                "recon_id": recon_id,
                "booking_id": bid,
                "accounting_period": period,
                "currency": currency,
                "ledger_net_minor": ledger_expected_net,
                "psp_net_minor": psp_net,
                "bank_settled_orig_minor": bank_orig,
                "bank_settled_usd_minor": bank_rep_usd,
                "expected_bank_usd_minor": expected_bank_rep_usd,
                "variance_minor": variance_minor,
                "variance_usd_minor": variance_usd_minor,
                "classification": classification,
                "reason": reason,
                "booking_status": latest_booking.get(bid, {}).get("booking_status", "ORPHAN"),
                "reconciled_at": as_of_timestamp,
            }
        )

        # Create structured exception tickets for every actionable discrepancy
        if classification not in ("MATCHED", "DUPLICATE_FILE_DEDUPED"):
            severity = "HIGH" if classification in ("UNMATCHED_PSP_OR_BANK", "PARTIAL_REFUND_MISMATCH", "CHARGEBACK_OPEN") else "MEDIUM"
            if classification == "TIMING_IN_TRANSIT" and age_days <= 3:
                severity = "LOW"
            exc_id = deterministic_id("exc", bid, classification)
            exceptions.append(
                {
                    "exception_id": exc_id,
                    "recon_id": recon_id,
                    "booking_id": bid,
                    "accounting_period": period,
                    "classification": classification,
                    "severity": severity,
                    "status": "OPEN",
                    "currency": currency,
                    "variance_minor": variance_minor,
                    "variance_usd_minor": variance_usd_minor,
                    "age_days": age_days,
                    "aging_bucket": aging_bucket_for_days(age_days),
                    "reason": reason,
                    "detected_at": as_of_timestamp,
                    "resolved_at": None,
                    "resolved_by": None,
                    "resolution_note": None,
                    "adjustment_journal_entry_id": None,
                }
            )

    total_checked = len(recon_results)
    clean_matched = counts_by_status["MATCHED"] + counts_by_status["DUPLICATE_FILE_DEDUPED"]
    match_rate = round(clean_matched / total_checked, 6) if total_checked > 0 else 1.0
    open_exposure_usd_minor = sum(e["variance_usd_minor"] for e in exceptions if e["status"] != "RESOLVED_ADJUSTED")

    aging_summary = {"0-1d": 0, "2-7d": 0, "8-30d": 0, ">30d": 0}
    for exc in exceptions:
        aging_summary[exc["aging_bucket"]] += 1

    summary = {
        "total_transactions_checked": total_checked,
        "clean_matched_count": clean_matched,
        "match_rate": match_rate,
        "duplicate_settlement_lines_deduped": dup_line_count,
        "exception_count": len(exceptions),
        "open_exception_count": sum(1 for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"),
        "open_exception_exposure_usd_minor": open_exposure_usd_minor,
        "counts_by_classification": counts_by_status,
        "aging_buckets": aging_summary,
    }
    return recon_results, exceptions, summary


def resolve_exception_ticket(
    exception: dict[str, Any],
    *,
    actor: str,
    resolution_note: str,
    target_status: str = "RESOLVED_ADJUSTED",
    open_period: str = "2026-05",
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Advance an exception ticket (`OPEN -> INVESTIGATING -> RESOLVED_ADJUSTED`) and emit balanced adjustment JE."""
    if target_status not in ("INVESTIGATING", "RESOLVED_ADJUSTED"):
        raise ValueError(f"Unsupported exception transition status: {target_status}")
    now_iso = datetime.now(UTC).isoformat()
    updated = dict(exception)
    updated["status"] = target_status
    updated["resolution_note"] = resolution_note
    updated["resolved_by"] = actor

    adjustment_entries: list[dict[str, Any]] = []
    if target_status == "RESOLVED_ADJUSTED":
        updated["resolved_at"] = now_iso
        variance = int(updated["variance_minor"])
        if variance > 0:
            je_id = deterministic_id("je_res", updated["exception_id"], now_iso[:19])
            cls = str(updated["classification"])
            if cls == "FEE_VARIANCE":
                legs = [("PAYMENT_PROCESSING_FEE", variance, 0), ("ACCOUNTS_RECEIVABLE_PSP", 0, variance)]
            elif cls == "FX_VARIANCE":
                legs = [("FX_GAIN_LOSS", variance, 0), ("CASH_CLEARING", 0, variance)]
            elif cls == "PARTIAL_REFUND_MISMATCH":
                legs = [("REFUND_CONTRA_REVENUE", variance, 0), ("ACCOUNTS_RECEIVABLE_PSP", 0, variance)]
            elif cls == "CHARGEBACK_OPEN":
                legs = [("CHARGEBACK_LOSS", variance, 0), ("ACCOUNTS_RECEIVABLE_PSP", 0, variance)]
            else:
                legs = [("CASH_CLEARING", variance, 0), ("ACCOUNTS_RECEIVABLE_PSP", 0, variance)]

            curr = "USD" if cls == "FX_VARIANCE" else str(updated["currency"])
            adjustment_entries = build_balanced_journal_entry(
                journal_entry_id=je_id,
                booking_id=str(updated["booking_id"]),
                source_event_id=str(updated["exception_id"]),
                accounting_period=open_period,
                original_period=str(updated["accounting_period"]),
                entry_type="RECONCILIATION_ADJUSTMENT",
                original_currency=curr,
                reporting_currency="USD",
                fx_rate_micros=FX_SCALE if curr == "USD" else max(1, int((int(updated["variance_usd_minor"]) * FX_SCALE) // max(1, variance))),
                fx_rate_version="recon_resolution",
                effective_at=now_iso,
                recorded_at=now_iso,
                legs=legs,
                is_restatement=(str(updated["accounting_period"]) != open_period),
            )
            updated["adjustment_journal_entry_id"] = je_id

    return updated, adjustment_entries
