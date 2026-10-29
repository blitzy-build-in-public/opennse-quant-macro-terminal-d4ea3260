"""Dividend yield and the quant engine's numeric boundary helpers.

Dividend yield (%) = dividend per share ÷ close × 100, from ``quant/valuation.py``; a missing dps
and a close <= 0 give ``None`` with a reason flag, while a zero dps is a defined 0% yield. Values
are unrounded until :func:`quantize_out`, the single rounding point, applies ROUND_HALF_UP.

This file also covers ``quant/numeric.py`` (places per kind, ``None`` pass-through, strict
parsing, zero-safe division, the shared constants and the frozen ``Violation`` record).

Every input is a ``Decimal`` built from a string literal and every expected value is a
hand-computed literal, so no binary float enters an assertion. The tests are pure: no database,
network or filesystem.
"""

from __future__ import annotations

import dataclasses
from datetime import date
from decimal import Decimal
from typing import get_args

import pytest

from opennse_terminal.config import DEFAULT_CURRENCY
from opennse_terminal.quant.asof import FundamentalsRow, fundamentals_as_of
from opennse_terminal.quant.numeric import (
    ANALYTICS_CURRENCY,
    HUNDRED,
    ONE,
    PLACES,
    ZERO,
    QuantizeKind,
    Violation,
    quantize_out,
    safe_div,
    to_decimal,
)
from opennse_terminal.quant.valuation import (
    Metric,
    ValuationInputs,
    ValuationResult,
    compute_valuation,
    dividend_yield_pct,
)

PERIOD_END = date(2025, 12, 31)
AS_OF = date(2026, 3, 31)  # PERIOD_END + 90 days: the first day the FY row counts as published.


def _value_counter(dps: Decimal | None, close: Decimal) -> ValuationResult:
    """Value a 1,000,000-share KES counter whose FY fundamentals are just past the 90-day lag.

    Currency is left to its defaults on both records, so the KES default is what is exercised.
    The non-dividend line items are complete, so no other metric is undefined for lack of input.
    """
    row = FundamentalsRow(
        period_end_date=PERIOD_END,
        net_income_kes=Decimal("2500000"),
        total_equity_kes=Decimal("12500000"),
        ebitda_kes=Decimal("4000000"),
        total_debt_kes=Decimal("5000000"),
        cash_and_equivalents_kes=Decimal("2000000"),
        dps_kes=dps,
    )
    available = fundamentals_as_of([row], AS_OF)
    assert available is row
    return compute_valuation(
        ValuationInputs(
            ticker="TEST",
            sector="Telecommunications",
            shares_outstanding=1_000_000,
            close_price=close,
            price_date=AS_OF,
            as_of=AS_OF,
            fundamentals=available,
        )
    )


def _dy_flags(flags: list[str]) -> list[str]:
    return [flag for flag in flags if flag.startswith("dy_")]


# ---------------------------------------------------------------------------------------------
# Dividend yield values (hand-computed)
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dps", "close", "raw", "quantized"),
    [
        pytest.param("1.25", "25.00", "5", "5.00", id="1.25/25.00=5->5.00"),
        pytest.param("0", "25.00", "0", "0.00", id="zero_dps=0->0.00"),
        pytest.param(
            "0.333",
            "7.00",
            "4.757142857142857142857142857",
            "4.76",
            id="0.333/7.00=4.757...->4.76",
        ),
        # 4.765 lies exactly halfway between 4.76 and 4.77: ROUND_HALF_UP gives 4.77 where the
        # ROUND_HALF_EVEN of the default context would give 4.76.
        pytest.param("0.4765", "10", "4.765", "4.77", id="0.4765/10=4.765->4.77_half_up"),
    ],
)
def test_dividend_yield_value_and_rounding(dps: str, close: str, raw: str, quantized: str) -> None:
    """dps ÷ close × 100 equals the hand-computed raw value and rounds once, HALF_UP, to 2 dp."""
    result = dividend_yield_pct(Decimal(dps), Decimal(close))

    assert isinstance(result, Metric)
    assert result.flag is None
    assert isinstance(result.value, Decimal)
    assert result.value == Decimal(raw)
    assert str(quantize_out(result.value, "pct")) == quantized


def test_zero_dps_is_a_defined_zero_yield_through_compute_valuation() -> None:
    """dps 0 on close 25.00 → yield 0.00, not null, and no dy_* flag."""
    result = _value_counter(Decimal("0"), Decimal("25.00"))

    assert result.dividend_yield_pct is not None
    assert result.dividend_yield_pct == ZERO
    assert str(quantize_out(result.dividend_yield_pct, "pct")) == "0.00"
    assert _dy_flags(result.flags) == []


def test_compute_valuation_dividend_yield_past_lag_is_five_pct() -> None:
    """1,000,000 KES shares at 25.00, FY row available at day 90, dps 1.25 → 5.00%."""
    result = _value_counter(Decimal("1.25"), Decimal("25.00"))

    assert result.dps_kes == Decimal("1.25")
    assert result.fundamentals_period_end == PERIOD_END
    assert result.market_cap_kes == Decimal("25000000")
    assert result.dividend_yield_pct is not None
    assert str(quantize_out(result.dividend_yield_pct, "pct")) == "5.00"
    assert result.flags == []


def test_kes_is_the_default_currency_of_valuation_inputs() -> None:
    """ValuationInputs and FundamentalsRow default to KES, so a KES pair matches by default."""
    row = FundamentalsRow(period_end_date=PERIOD_END)
    inputs = ValuationInputs(
        ticker="TEST",
        sector="Telecommunications",
        shares_outstanding=1,
        close_price=Decimal("1.00"),
        price_date=AS_OF,
        as_of=AS_OF,
    )

    assert row.currency == "KES"
    assert inputs.currency == "KES"


def test_analytics_currency_is_the_package_default_currency() -> None:
    """Quant restates config.DEFAULT_CURRENCY instead of importing config; the two must agree."""
    assert ANALYTICS_CURRENCY == DEFAULT_CURRENCY == "KES"


# ---------------------------------------------------------------------------------------------
# Dividend yield edge cases
# ---------------------------------------------------------------------------------------------


def test_missing_dps_gives_null_yield_with_flag() -> None:
    """dps None → yield None with dy_missing_dps, from the primitive and from compute_valuation."""
    primitive = dividend_yield_pct(None, Decimal("25.00"))
    assert primitive.value is None
    assert primitive.flag == "dy_missing_dps"

    result = _value_counter(None, Decimal("25.00"))
    assert result.dividend_yield_pct is None
    assert result.dps_kes is None
    assert "dy_missing_dps" in result.flags


@pytest.mark.parametrize("close", ["0", "-1.00"])
def test_non_positive_close_gives_null_yield_without_dividing(close: str) -> None:
    """Close 0 or −1.00 → yield None with dy_non_positive_price.

    The default decimal context traps DivisionByZero and InvalidOperation, so a division by the
    zero close would raise and fail this test rather than return a value.
    """
    primitive = dividend_yield_pct(Decimal("1.25"), Decimal(close))
    assert primitive.value is None
    assert primitive.flag == "dy_non_positive_price"

    result = _value_counter(Decimal("1.25"), Decimal(close))
    assert result.dividend_yield_pct is None
    assert "dy_non_positive_price" in result.flags


# ---------------------------------------------------------------------------------------------
# quant/numeric.py: quantize_out
# ---------------------------------------------------------------------------------------------

_KIND_CASES: list[tuple[QuantizeKind, int, str, str]] = [
    ("money", 2, "1234.565", "1234.57"),
    ("per_share", 2, "1234.565", "1234.57"),
    ("ratio", 2, "1234.565", "1234.57"),
    ("pct", 2, "1234.565", "1234.57"),
    ("z", 2, "1234.565", "1234.57"),
    ("index", 2, "1234.565", "1234.57"),
    ("tenor", 2, "1234.565", "1234.57"),
    ("discount_factor", 6, "0.9090909", "0.909091"),
]


def test_kind_cases_cover_every_quantize_kind() -> None:
    """The cases below name every QuantizeKind, and PLACES has exactly those kinds as keys."""
    kinds = {case[0] for case in _KIND_CASES}

    assert kinds == set(get_args(QuantizeKind))
    assert kinds == set(PLACES)


@pytest.mark.parametrize(("kind", "places", "value", "expected"), _KIND_CASES)
def test_quantize_out_places_per_kind(
    kind: QuantizeKind, places: int, value: str, expected: str
) -> None:
    """2 dp for every kind but discount_factor (6 dp): 1234.565 → 1234.57, 0.9090909 → 0.909091."""
    assert PLACES[kind] == places
    assert str(quantize_out(Decimal(value), kind)) == expected


@pytest.mark.parametrize("kind", get_args(QuantizeKind))
def test_quantize_out_passes_none_through(kind: QuantizeKind) -> None:
    """None stays None for every kind: an undefined metric is never rounded to zero."""
    assert quantize_out(None, kind) is None


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        pytest.param("-9.105", "pct", "-9.11", id="negative_half_rounds_away_from_zero"),
        pytest.param("7.5", "per_share", "7.50", id="trailing_zero_kept"),
        pytest.param("-0.001", "pct", "0.00", id="zero_result_is_unsigned"),
        # 30 significant digits at 6 dp: more than the 28-digit default context could hold.
        pytest.param(
            "123456789012345678901234.5678905",
            "discount_factor",
            "123456789012345678901234.567891",
            id="large_magnitude_keeps_every_place",
        ),
    ],
)
def test_quantize_out_rounding_contract(value: str, kind: QuantizeKind, expected: str) -> None:
    """HALF_UP away from zero (−9.105 → −9.11), trailing zeros kept, no −0.00, no precision loss."""
    assert str(quantize_out(Decimal(value), kind)) == expected


@pytest.mark.parametrize(
    ("value", "kind", "error"),
    [
        pytest.param(Decimal("NaN"), "pct", ValueError, id="nan"),
        pytest.param(Decimal("Infinity"), "pct", ValueError, id="infinity"),
        pytest.param(Decimal("1"), "percent", ValueError, id="unknown_kind"),
        pytest.param(1, "pct", TypeError, id="int"),
        pytest.param(True, "pct", TypeError, id="bool"),
        pytest.param("1.00", "pct", TypeError, id="str"),
    ],
)
def test_quantize_out_rejects_invalid_input(
    value: object, kind: str, error: type[Exception]
) -> None:
    """Non-finite values and unknown kinds raise ValueError; non-Decimal values raise TypeError."""
    with pytest.raises(error):
        quantize_out(value, kind)  # type: ignore[call-overload]


# ---------------------------------------------------------------------------------------------
# quant/numeric.py: to_decimal, safe_div, constants and Violation
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text", ["95.75", " 95.75 ", "95.75\n"], ids=["plain", "padded", "trailing_newline"]
)
def test_to_decimal_parses_exactly(text: str) -> None:
    """Text 95.75, surrounding whitespace stripped → Decimal("95.75") with its exact digits."""
    value = to_decimal(text)

    assert value == Decimal("95.75")
    assert str(value) == "95.75"


@pytest.mark.parametrize(
    "text",
    ["NaN", "Infinity", "-Infinity", "abc", "", "   ", "1,234"],
    ids=["nan", "infinity", "minus_infinity", "garbage", "empty", "blank", "thousands_separator"],
)
def test_to_decimal_rejects_non_finite_or_malformed_text(text: str) -> None:
    """NaN, infinities, garbage, blanks and thousands separators raise ValueError, as documented."""
    with pytest.raises(ValueError):
        to_decimal(text)


def test_to_decimal_rejects_a_float_argument() -> None:
    """A float argument raises TypeError, so no binary-float value can enter a calculation."""
    # The only float literal in tests/unit/quant: it exists solely to prove this guard.
    with pytest.raises(TypeError):
        to_decimal(0.1)  # type: ignore[arg-type]


def test_safe_div_returns_the_exact_quotient() -> None:
    """1 ÷ 4 → Decimal("0.25"), unrounded."""
    assert safe_div(Decimal("1"), Decimal("4")) == Decimal("0.25")


@pytest.mark.parametrize(
    "denominator", [ZERO, Decimal("0.00"), Decimal("-0")], ids=["ZERO", "0.00", "-0"]
)
def test_safe_div_by_zero_is_none_without_raising(denominator: Decimal) -> None:
    """1 ÷ 0 (any zero form) → None, never an exception or a zero."""
    assert safe_div(Decimal("1"), denominator) is None


@pytest.mark.parametrize(
    ("constant", "expected"), [(ZERO, 0), (ONE, 1), (HUNDRED, 100)], ids=["ZERO", "ONE", "HUNDRED"]
)
def test_numeric_constants_are_decimals(constant: Decimal, expected: int) -> None:
    """ZERO == 0, ONE == 1 and HUNDRED == 100, each a Decimal."""
    assert isinstance(constant, Decimal)
    assert constant == expected


def test_violation_is_a_frozen_coded_record() -> None:
    """Violation exposes code, field and message, and rejects assignment once built."""
    violation = Violation(
        "primary_exceeds_offer_size",
        "primary_proceeds_kes",
        "Primary proceeds exceed the offer size.",
    )

    assert violation.code == "primary_exceeds_offer_size"
    assert violation.field == "primary_proceeds_kes"
    assert violation.message == "Primary proceeds exceed the offer size."
    with pytest.raises(dataclasses.FrozenInstanceError):
        violation.code = "changed"  # type: ignore[misc]
