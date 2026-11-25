"""Exact integer minor-unit monetary arithmetic and deterministic FX conversion.

All financial amounts are represented as exact Python `int` minor units
(cents for USD/EUR/GBP/CAD/AUD, whole yen for JPY) or `Decimal` for intermediate
FX/percentage math. IEEE-754 `float` values are explicitly rejected on ledger paths
to eliminate binary floating-point drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

FX_SCALE = 1_000_000  # FX rates stored as exact integer micros (e.g. 1.084250 -> 1_084_250)

CURRENCY_DECIMALS: dict[str, int] = {
    "USD": 2,
    "EUR": 2,
    "GBP": 2,
    "JPY": 0,
    "CAD": 2,
    "AUD": 2,
}

CURRENCY_SYMBOLS: dict[str, str] = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "CAD": "CA$",
    "AUD": "A$",
}


def currency_decimals(currency: str) -> int:
    code = currency.upper()
    if code not in CURRENCY_DECIMALS:
        raise ValueError(f"Unsupported ISO-4217 currency: {currency}")
    return CURRENCY_DECIMALS[code]


def assert_minor_int(value: Any, field_name: str = "amount_minor") -> int:
    """Strictly enforce exact integer minor units; reject floats and booleans."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"{field_name} must be an exact int in minor units, got {type(value).__name__}: {value!r}"
        )
    return int(value)


def allocate_bps_minor(base_minor: int, bps: int) -> int:
    """Compute exact basis-point share (10,000 bps = 100%) using banker's rounding."""
    base = assert_minor_int(base_minor, "base_minor")
    rate_bps = assert_minor_int(bps, "bps")
    exact = (Decimal(base) * Decimal(rate_bps)) / Decimal(10_000)
    return int(exact.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


def convert_minor_to_reporting(
    amount_minor: int,
    from_currency: str,
    to_currency: str,
    fx_rate_micros: int,
) -> int:
    """Convert an exact integer minor-unit amount to reporting currency minor units.

    `fx_rate_micros` is the number of `to_currency` major units per 1 `from_currency`
    major unit, scaled by 1,000,000.
    Example: 1 EUR = 1.085000 USD -> fx_rate_micros = 1_085_000.
    Example: 1 JPY = 0.006500 USD -> fx_rate_micros = 6_500.
    """
    amt = assert_minor_int(amount_minor, "amount_minor")
    rate_micros = assert_minor_int(fx_rate_micros, "fx_rate_micros")
    if rate_micros <= 0:
        raise ValueError(f"fx_rate_micros must be positive, got {rate_micros}")
    from_code = from_currency.upper()
    to_code = to_currency.upper()
    if from_code == to_code:
        if rate_micros != FX_SCALE:
            raise ValueError(f"Identity FX conversion requires {FX_SCALE}, got {rate_micros}")
        return amt

    from_scale = Decimal(10) ** currency_decimals(from_code)
    to_scale = Decimal(10) ** currency_decimals(to_code)
    major_from = Decimal(amt) / from_scale
    major_to = major_from * (Decimal(rate_micros) / Decimal(FX_SCALE))
    minor_to = major_to * to_scale
    return int(minor_to.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


def format_minor(amount_minor: int, currency: str) -> str:
    """Format exact integer minor units into a deterministic human-readable currency string."""
    amt = assert_minor_int(amount_minor, "amount_minor")
    code = currency.upper()
    dec = currency_decimals(code)
    symbol = CURRENCY_SYMBOLS.get(code, f"{code} ")
    sign = "-" if amt < 0 else ""
    abs_amt = abs(amt)
    if dec == 0:
        return f"{sign}{symbol}{abs_amt:,}"
    major = Decimal(abs_amt) / (Decimal(10) ** dec)
    return f"{sign}{symbol}{major:,.{dec}f}"


@dataclass(frozen=True)
class FloatDriftAudit:
    sample_count: int
    exact_sum_minor: int
    float_sum_minor_rounded: int
    per_tx_mismatches: int
    cumulative_drift_minor: int
    max_single_tx_drift_minor: int


def audit_exact_int_vs_float64(
    samples: list[tuple[int, str, int]],
    reporting_currency: str = "USD",
) -> FloatDriftAudit:
    """Demonstrate exact integer+Decimal vs naive IEEE-754 float64 accumulation drift."""
    exact_total = 0
    float_running = 0.0
    per_tx_mismatches = 0
    max_single_drift = 0
    to_scale = 10 ** currency_decimals(reporting_currency)

    for amount_minor, curr, fx_micros in samples:
        exact_rep = convert_minor_to_reporting(amount_minor, curr, reporting_currency, fx_micros)
        exact_total += exact_rep

        from_scale = float(10 ** currency_decimals(curr))
        naive_major = (float(amount_minor) / from_scale) * (float(fx_micros) / float(FX_SCALE))
        naive_rep = int(naive_major * float(to_scale))  # classic float truncation / binary error
        float_running += naive_major
        diff = abs(exact_rep - naive_rep)
        if diff > 0:
            per_tx_mismatches += 1
            if diff > max_single_drift:
                max_single_drift = diff

    float_sum_minor = int(round(float_running * to_scale))
    return FloatDriftAudit(
        sample_count=len(samples),
        exact_sum_minor=exact_total,
        float_sum_minor_rounded=float_sum_minor,
        per_tx_mismatches=per_tx_mismatches,
        cumulative_drift_minor=abs(exact_total - float_sum_minor),
        max_single_tx_drift_minor=max_single_drift,
    )
