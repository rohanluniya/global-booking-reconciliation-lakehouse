"""Bi-temporal booking state reconstruction, SCD Type 2 builder, and as-of FX resolution."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from booking_ledger.contracts import deterministic_id
from booking_ledger.money import FX_SCALE

EVENT_TO_BOOKING_STATUS: dict[str, str] = {
    "BOOKING_HELD": "HOLD",
    "BOOKING_CONFIRMED": "CONFIRMED",
    "PAYMENT_AUTHORIZED": "CONFIRMED",
    "PAYMENT_CAPTURED": "PAID",
    "PAYMENT_RETRY_SUCCEEDED": "PAID",
    "BOOKING_MODIFIED": "MODIFIED",
    "BOOKING_CHECKED_IN": "CHECKED_IN",
    "BOOKING_COMPLETED": "COMPLETED",
    "HOST_PAYOUT_SCHEDULED": "COMPLETED",
    "HOST_PAYOUT_SETTLED": "PAYOUT_SETTLED",
    "BOOKING_CANCELLED": "CANCELLED",
    "REFUND_ISSUED": "REFUNDED",
    "PARTIAL_REFUND_ISSUED": "PARTIALLY_REFUNDED",
    "CHARGEBACK_OPENED": "DISPUTED",
    "CHARGEBACK_LOST": "CHARGEBACK_LOST",
}


def build_scd2_dimension(
    raw_versions: list[dict[str, Any]],
    natural_key: str,
    effective_col: str = "effective_at",
    recorded_col: str = "recorded_at",
    prefix: str = "scd2",
) -> list[dict[str, Any]]:
    """Build an SCD Type 2 dimension table with non-overlapping [valid_from, valid_to) intervals."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in raw_versions:
        key = str(row[natural_key])
        grouped.setdefault(key, []).append(dict(row))

    scd2_rows: list[dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        # Sort by business effective timestamp, then recorded timestamp
        versions = sorted(
            grouped[key],
            key=lambda r: (str(r[effective_col]), str(r.get(recorded_col, ""))),
        )
        # Deduplicate identical effective_at timestamps by taking the latest recorded_at
        deduped: list[dict[str, Any]] = []
        for ver in versions:
            if deduped and deduped[-1][effective_col] == ver[effective_col]:
                deduped[-1] = ver
            else:
                deduped.append(ver)

        for idx, ver in enumerate(deduped):
            valid_from = str(ver[effective_col])
            is_last = idx == len(deduped) - 1
            valid_to = None if is_last else str(deduped[idx + 1][effective_col])
            if valid_to is not None and valid_to <= valid_from:
                raise ValueError(f"SCD2 interval violation for {natural_key}={key}: {valid_from} >= {valid_to}")
            scd2_rows.append(
                {
                    **ver,
                    "surrogate_key": deterministic_id(prefix, key, idx + 1, valid_from),
                    "version_num": idx + 1,
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "is_current": is_last,
                }
            )
    return scd2_rows


def verify_scd2_non_overlapping(rows: list[dict[str, Any]], natural_key: str) -> tuple[bool, list[str]]:
    """Assert zero overlapping validity intervals and exactly one current row per natural key."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row[natural_key]), []).append(row)

    errors: list[str] = []
    for key, versions in grouped.items():
        versions_sorted = sorted(versions, key=lambda r: (int(r["version_num"]), str(r["valid_from"])))
        current_count = sum(1 for v in versions_sorted if bool(v["is_current"]))
        if current_count != 1:
            errors.append(f"{natural_key}={key} has {current_count} current rows (expected 1)")
        for idx in range(len(versions_sorted) - 1):
            curr = versions_sorted[idx]
            nxt = versions_sorted[idx + 1]
            if curr["valid_to"] is None:
                errors.append(f"{natural_key}={key} v{curr['version_num']} has null valid_to before next version")
            elif str(curr["valid_to"]) > str(nxt["valid_from"]):
                errors.append(
                    f"{natural_key}={key} overlap: v{curr['version_num']} valid_to={curr['valid_to']} > v{nxt['version_num']} valid_from={nxt['valid_from']}"
                )
    return len(errors) == 0, errors


def build_fx_rates_asof(raw_fx_rates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Structure FX rates with bi-temporal system validity (recorded_at) for reproducible as-of lookups."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in raw_fx_rates:
        key = (str(row["currency"]).upper(), str(row["effective_date"]))
        grouped.setdefault(key, []).append(dict(row))

    result: list[dict[str, Any]] = []
    for (curr, eff_date) in sorted(grouped.keys()):
        versions = sorted(grouped[(curr, eff_date)], key=lambda r: str(r["recorded_at"]))
        for idx, ver in enumerate(versions):
            is_last = idx == len(versions) - 1
            valid_from = str(ver["recorded_at"])
            valid_to = None if is_last else str(versions[idx + 1]["recorded_at"])
            result.append(
                {
                    **ver,
                    "fx_id": deterministic_id("fx", curr, eff_date, ver["rate_version"]),
                    "system_valid_from": valid_from,
                    "system_valid_to": valid_to,
                    "is_latest_version": is_last,
                }
            )
    return result


def lookup_fx_as_of(
    fx_rates: list[dict[str, Any]],
    currency: str,
    effective_at: str,
    recorded_as_of: str | None = None,
    reporting_currency: str = "USD",
) -> dict[str, Any]:
    """Resolve exact FX rate for (currency, effective_at) as known at `recorded_as_of`."""
    curr = currency.upper()
    rep = reporting_currency.upper()
    if curr == rep:
        return {
            "currency": curr,
            "reporting_currency": rep,
            "effective_date": effective_at[:10],
            "rate_version": "identity",
            "rate_micros": FX_SCALE,
            "is_revision": False,
        }

    eff_date = effective_at[:10]
    candidates = [
        r
        for r in fx_rates
        if r["currency"].upper() == curr
        and r["reporting_currency"].upper() == rep
        and r["effective_date"] <= eff_date
        and (recorded_as_of is None or str(r["recorded_at"]) <= recorded_as_of)
    ]
    if not candidates:
        # Fallback to earliest available rate for the currency if effective_date precedes window
        fallback = [r for r in fx_rates if r["currency"].upper() == curr and r["reporting_currency"].upper() == rep]
        if not fallback:
            raise KeyError(f"No FX rate found for {curr}->{rep} at {effective_at}")
        fallback.sort(key=lambda r: (str(r["effective_date"]), str(r["recorded_at"])))
        return fallback[0]

    # Pick latest effective_date <= eff_date, and latest recorded_at <= recorded_as_of
    candidates.sort(key=lambda r: (str(r["effective_date"]), str(r["recorded_at"])))
    return candidates[-1]


def deduplicate_cdc_events(raw_events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Idempotently deduplicate CDC outbox events by deterministic event_id."""
    seen: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for ev in raw_events:
        eid = str(ev["event_id"])
        if eid in seen:
            duplicates += 1
            continue
        seen[eid] = ev
    # Deterministic business ordering: booking_id, sequence_num, effective_at, source_lsn
    ordered = sorted(
        seen.values(),
        key=lambda e: (
            str(e["booking_id"]),
            int(e["sequence_num"]),
            str(e["effective_at"]),
            int(e["source_lsn"]),
        ),
    )
    return ordered, duplicates


def reconstruct_booking_state_history(valid_cdc_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reconstruct ordered bi-temporal booking state history from potentially out-of-order CDC events."""
    deduped, _ = deduplicate_cdc_events(valid_cdc_events)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for ev in deduped:
        grouped.setdefault(str(ev["booking_id"]), []).append(ev)

    history: list[dict[str, Any]] = []
    for booking_id in sorted(grouped.keys()):
        events = sorted(
            grouped[booking_id],
            key=lambda e: (int(e["sequence_num"]), str(e["effective_at"]), int(e["source_lsn"])),
        )
        for idx, ev in enumerate(events):
            eff_dt = datetime.fromisoformat(str(ev["effective_at"]))
            host_tz = ZoneInfo(str(ev["host_timezone"]))
            host_local_dt = eff_dt.astimezone(host_tz)
            utc_date = eff_dt.strftime("%Y-%m-%d")
            host_local_date = host_local_dt.strftime("%Y-%m-%d")
            utc_period = utc_date[:7]
            host_period = host_local_date[:7]
            is_last = idx == len(events) - 1
            valid_to = None if is_last else str(events[idx + 1]["effective_at"])

            history.append(
                {
                    "state_id": deterministic_id("bst", booking_id, ev["sequence_num"], ev["event_id"]),
                    "booking_id": booking_id,
                    "event_id": str(ev["event_id"]),
                    "listing_id": str(ev["listing_id"]),
                    "guest_id": str(ev["guest_id"]),
                    "host_id": str(ev["host_id"]),
                    "sequence_num": int(ev["sequence_num"]),
                    "event_type": str(ev["event_type"]),
                    "booking_status": EVENT_TO_BOOKING_STATUS.get(str(ev["event_type"]), "UNKNOWN"),
                    "effective_at": str(ev["effective_at"]),
                    "recorded_at": str(ev["recorded_at"]),
                    "valid_from": str(ev["effective_at"]),
                    "valid_to": valid_to,
                    "is_current": is_last,
                    "host_timezone": str(ev["host_timezone"]),
                    "host_local_date": host_local_date,
                    "utc_date": utc_date,
                    "accounting_period": utc_period,
                    "tz_cutoff_shifted": utc_period != host_period,
                    "currency": str(ev["currency"]),
                    "nights": int(ev["nights"]),
                    "room_subtotal_minor": int(ev["room_subtotal_minor"]),
                    "platform_fee_minor": int(ev["platform_fee_minor"]),
                    "host_fee_minor": int(ev["host_fee_minor"]),
                    "tax_minor": int(ev["tax_minor"]),
                    "gross_total_minor": int(ev["gross_total_minor"]),
                    "refund_minor": int(ev.get("refund_minor", 0)),
                    "chargeback_minor": int(ev.get("chargeback_minor", 0)),
                    "tax_policy_version": str(ev["tax_policy_version"]),
                    "jurisdiction": str(ev["jurisdiction"]),
                    "payment_reference": ev.get("payment_reference"),
                }
            )
    return history


def query_booking_as_of(
    history_rows: list[dict[str, Any]],
    booking_id: str,
    as_of_effective_at: str,
    as_of_recorded_at: str | None = None,
) -> dict[str, Any] | None:
    """Bi-temporal point-in-time lookup: state of `booking_id` at business time + optional system time."""
    matches = [
        r
        for r in history_rows
        if r["booking_id"] == booking_id
        and str(r["effective_at"]) <= as_of_effective_at
        and (as_of_recorded_at is None or str(r["recorded_at"]) <= as_of_recorded_at)
    ]
    if not matches:
        return None
    matches.sort(key=lambda r: (int(r["sequence_num"]), str(r["effective_at"])))
    return matches[-1]
