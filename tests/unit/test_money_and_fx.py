"""Unit tests for exact integer minor-unit arithmetic, FX conversion, and float rejection."""

from __future__ import annotations

import pytest

from booking_ledger.money import (
    FX_SCALE,
    allocate_bps_minor,
    assert_minor_int,
    audit_exact_int_vs_float64,
    convert_minor_to_reporting,
    format_minor,
)


@pytest.mark.unit
def test_rejects_float_and_bool_minor_units() -> None:
    assert assert_minor_int(12_345, "gross") == 12_345
    with pytest.raises(TypeError):
        assert_minor_int(123.45, "gross")
    with pytest.raises(TypeError):
        assert_minor_int(True, "gross")


@pytest.mark.unit
def test_multi_currency_fx_conversion_and_jpy_zero_decimals() -> None:
    # 10,000 JPY at 0.006500 USD/JPY = $65.00 = 6,500 USD cents
    assert convert_minor_to_reporting(10_000, "JPY", "USD", 6_500) == 6_500
    # 100.00 EUR (10,000 cents) at 1.085000 USD/EUR = $108.50 = 10,850 USD cents
    assert convert_minor_to_reporting(10_000, "EUR", "USD", 1_085_000) == 10_850
    # Identity USD -> USD
    assert convert_minor_to_reporting(99_999, "USD", "USD", FX_SCALE) == 99_999
    assert format_minor(10_000, "JPY") == "¥10,000"
    assert format_minor(10_850, "USD") == "$108.50"


@pytest.mark.unit
def test_basis_point_allocation_and_float_drift_audit() -> None:
    assert allocate_bps_minor(25_000, 1200) == 3_000
    samples = [(19_999, "EUR", 1_084_375), (33_333, "GBP", 1_268_125), (17_500, "JPY", 6_543)] * 500
    audit = audit_exact_int_vs_float64(samples)
    assert audit.sample_count == 1500
    assert audit.cumulative_drift_minor > 0
