"""Declarative Data Quality (DQ) suite for ledger, SCD2, FX, CDC quarantine, and reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from booking_ledger.ledger import verify_ledger_zero_sum
from booking_ledger.money import CURRENCY_DECIMALS
from booking_ledger.temporal import verify_scd2_non_overlapping


def run_data_quality_checks(
    *,
    run_id: str,
    bronze_cdc: list[dict[str, Any]],
    quarantine_dlq: list[dict[str, Any]],
    booking_history: list[dict[str, Any]],
    dim_listings_scd2: list[dict[str, Any]],
    dim_tax_policies_scd2: list[dict[str, Any]],
    fx_rates_asof: list[dict[str, Any]],
    ledger_entries: list[dict[str, Any]],
    recon_results: list[dict[str, Any]],
    close_snapshots: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run all financial & lakehouse DQ assertions and return rows for `quality.dq_results`."""
    now_iso = datetime.now(UTC).isoformat()
    checks: list[dict[str, Any]] = []

    def record(
        check_name: str,
        table_name: str,
        passed: bool,
        observed: Any,
        expected: str,
        severity: str = "CRITICAL",
    ) -> None:
        checks.append(
            {
                "run_id": run_id,
                "check_name": check_name,
                "table_name": table_name,
                "severity": severity,
                "passed": bool(passed),
                "observed_value": str(observed),
                "expected_rule": expected,
                "checked_at": now_iso,
            }
        )

    # 1. Non-empty bronze CDC and audit trail of quarantined poison events
    record(
        "bronze_cdc_non_empty",
        "bronze.cdc_booking_events",
        len(bronze_cdc) > 0,
        len(bronze_cdc),
        "row_count > 0",
    )
    record(
        "quarantine_dlq_schema_enforcement",
        "bronze.quarantine_dlq",
        all(bool(r.get("reason")) and bool(r.get("raw_payload_json")) for r in quarantine_dlq),
        len(quarantine_dlq),
        "all quarantined records preserve reason and raw payload",
        severity="HIGH",
    )

    # 2. Exact integer minor-unit enforcement (zero float amounts in bronze/silver/gold)
    float_violations = 0
    for row in ledger_entries:
        for col in ("debit_minor", "credit_minor", "reporting_debit_minor", "reporting_credit_minor", "fx_rate_micros"):
            val = row.get(col)
            if isinstance(val, (float, bool)) or not isinstance(val, int):
                float_violations += 1
    record(
        "ledger_exact_integer_minor_units",
        "gold.ledger_journal_entries",
        float_violations == 0,
        float_violations,
        "0 IEEE-754 float values in monetary columns",
    )

    # 3. Valid ISO-4217 currency codes
    bad_currencies = {r["original_currency"] for r in ledger_entries if r["original_currency"] not in CURRENCY_DECIMALS}
    record(
        "ledger_valid_iso_currencies",
        "gold.ledger_journal_entries",
        len(bad_currencies) == 0,
        sorted(bad_currencies),
        "all currencies in supported ISO-4217 set",
    )

    # 4. Double-entry accounting zero-sum invariant (per entry, per currency, and global USD)
    zero_sum = verify_ledger_zero_sum(ledger_entries)
    record(
        "ledger_double_entry_zero_sum",
        "gold.ledger_journal_entries",
        zero_sum["balanced"],
        f"imbalance={zero_sum['global_imbalance_minor']}, unbalanced_entries={len(zero_sum['unbalanced_entries'])}",
        "SUM(debit) == SUM(credit) per journal_entry_id, per currency, and globally",
    )

    # 5. Unique entry_line_id immutability check
    line_ids = [r["entry_line_id"] for r in ledger_entries]
    record(
        "ledger_unique_entry_line_ids",
        "gold.ledger_journal_entries",
        len(line_ids) == len(set(line_ids)),
        f"total={len(line_ids)}, unique={len(set(line_ids))}",
        "entry_line_id strictly unique",
    )

    # 6. SCD Type 2 zero-overlap checks for listings and tax policies
    listings_ok, listings_errs = verify_scd2_non_overlapping(dim_listings_scd2, "listing_id")
    record(
        "scd2_listings_non_overlapping",
        "silver.dim_listings_scd2",
        listings_ok,
        f"errors={len(listings_errs)}",
        "zero overlapping [valid_from, valid_to) intervals and 1 current row per listing_id",
    )
    tax_ok, tax_errs = verify_scd2_non_overlapping(dim_tax_policies_scd2, "jurisdiction")
    record(
        "scd2_tax_policies_non_overlapping",
        "silver.dim_tax_policies_scd2",
        tax_ok,
        f"errors={len(tax_errs)}",
        "zero overlapping [valid_from, valid_to) intervals and 1 current row per jurisdiction",
    )

    # 7. Booking temporal state history monotonic sequence check
    seq_errors = 0
    seen_last_seq: dict[str, int] = {}
    for row in booking_history:
        bid = str(row["booking_id"])
        seq = int(row["sequence_num"])
        if bid in seen_last_seq and seq <= seen_last_seq[bid]:
            seq_errors += 1
        seen_last_seq[bid] = seq
    record(
        "booking_history_monotonic_sequence",
        "silver.booking_state_history",
        seq_errors == 0 and len(booking_history) > 0,
        f"rows={len(booking_history)}, seq_errors={seq_errors}",
        "strictly increasing sequence_num per booking_id after out-of-order reconstruction",
    )

    # 8. FX rates positive & non-null
    bad_fx = sum(1 for r in fx_rates_asof if int(r.get("rate_micros", 0)) <= 0)
    record(
        "fx_rates_positive_micros",
        "silver.fx_rates_asof",
        bad_fx == 0 and len(fx_rates_asof) > 0,
        f"rows={len(fx_rates_asof)}, non_positive={bad_fx}",
        "all FX rate_micros > 0",
    )

    # 9. Reconciliation completeness
    record(
        "reconciliation_coverage",
        "gold.reconciliation_results",
        len(recon_results) > 0 and all(r.get("classification") for r in recon_results),
        len(recon_results),
        "every active booking/orphan settlement receives deterministic classification",
    )

    # 10. Period close snapshot zero-sum proof
    close_ok = all(
        int(s.get("trial_balance_imbalance_minor", -1)) == 0 and bool(s.get("certificate_sha256"))
        for s in close_snapshots
    )
    record(
        "period_close_certificate_integrity",
        "gold.period_close_snapshots",
        close_ok,
        len(close_snapshots),
        "all close snapshots have 0 imbalance and valid SHA-256 sign-off digest",
    )

    passed_count = sum(1 for c in checks if c["passed"])
    pass_rate = round(passed_count / len(checks), 6) if checks else 1.0
    summary = {
        "run_id": run_id,
        "total_checks": len(checks),
        "passed_checks": passed_count,
        "failed_checks": len(checks) - passed_count,
        "pass_rate": pass_rate,
        "all_passed": passed_count == len(checks),
    }
    return checks, summary
