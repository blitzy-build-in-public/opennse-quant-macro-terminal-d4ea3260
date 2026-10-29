"""Valuation multiples: market cap, EPS, P/E, P/B, enterprise value and EV/EBITDA.

Every input is a synthetic in-memory ``Decimal`` built from a string, in KES unless a test needs a
currency mismatch. Expected values are hand-computed literals. An undefined metric must be
``None`` together with its reason code in ``flags``: never zero and never an exception. A NaN or
infinite input, which validated data never holds, is a caller error that raises ``ValueError``.

Base equity used throughout: close 25.00 KES, 1,000,000 shares, net income 2,500,000, equity
12,500,000, EBITDA 4,000,000, debt 5,000,000 and cash 2,000,000, giving market cap 25,000,000,
EPS 2.50, P/E 10.00, P/B 2.00, EV 28,000,000 and EV/EBITDA 7.00.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import pytest

from opennse_terminal.quant.asof import FundamentalsRow, fundamentals_as_of
from opennse_terminal.quant.numeric import QuantizeKind, quantize_out
from opennse_terminal.quant.valuation import (
    EV_EXCLUDED_SECTORS,
    STALE_PRICE_DAYS,
    Metric,
    ValuationInputs,
    ValuationResult,
    compute_valuation,
    dividend_yield_pct,
    enterprise_value,
    eps,
    ev_ebitda,
    market_cap,
    pb_ratio,
    pe_ratio,
    price_change_pct,
    sector_aggregates,
)

AS_OF = date(2026, 9, 30)


def fundamentals(**overrides: Any) -> FundamentalsRow:
    """Fiscal year to 2026-03-31, which is past the 90-day availability lag at ``AS_OF``."""
    base = FundamentalsRow(
        period_end_date=date(2026, 3, 31),
        period_type="FY",
        currency="KES",
        net_income_kes=Decimal("2500000"),
        total_equity_kes=Decimal("12500000"),
        ebitda_kes=Decimal("4000000"),
        total_debt_kes=Decimal("5000000"),
        cash_and_equivalents_kes=Decimal("2000000"),
        dps_kes=None,
        revenue_kes=Decimal("10000000"),
    )
    return replace(base, **overrides)


def inputs(**overrides: Any) -> ValuationInputs:
    """The base equity: 1,000,000 shares closing at 25.00 KES on ``AS_OF``."""
    base = ValuationInputs(
        ticker="TST",
        sector="Telecommunications",
        shares_outstanding=1_000_000,
        close_price=Decimal("25.00"),
        price_date=AS_OF,
        as_of=AS_OF,
        currency="KES",
        fundamentals=fundamentals(),
    )
    return replace(base, **overrides)


def valued(**fundamentals_overrides: Any) -> ValuationResult:
    """Value the base equity with some of its fundamentals replaced."""
    return compute_valuation(inputs(fundamentals=fundamentals(**fundamentals_overrides)))


def rounded(value: Decimal | None, kind: QuantizeKind) -> str:
    """The value as the API would serialize it: rounded once at the response boundary."""
    assert value is not None
    return str(quantize_out(value, kind))


# --- Base case ----------------------------------------------------------------------------------


def test_base_case_multiples_are_hand_computed_values() -> None:
    """Market cap 25.00 × 1,000,000 = 25,000,000; EPS 2,500,000 ÷ 1,000,000 = 2.50;
    P/E 25,000,000 ÷ 2,500,000 = 10.00; P/B 25,000,000 ÷ 12,500,000 = 2.00;
    EV 25,000,000 + 5,000,000 − 2,000,000 = 28,000,000; EV/EBITDA 28,000,000 ÷ 4,000,000 = 7.00.
    """
    result = compute_valuation(inputs())

    assert result.market_cap_kes == Decimal("25000000")
    assert rounded(result.market_cap_kes, "money") == "25000000.00"
    assert result.eps_kes == Decimal("2.5")
    assert rounded(result.eps_kes, "per_share") == "2.50"
    assert result.pe == Decimal("10")
    assert rounded(result.pe, "ratio") == "10.00"
    assert result.pb == Decimal("2")
    assert rounded(result.pb, "ratio") == "2.00"
    assert result.enterprise_value_kes == Decimal("28000000")
    assert rounded(result.enterprise_value_kes, "money") == "28000000.00"
    assert result.ev_ebitda == Decimal("7")
    assert rounded(result.ev_ebitda, "ratio") == "7.00"
    assert result.dividend_yield_pct is None
    assert result.flags == ["dy_missing_dps"]
    assert result.fundamentals_period_end == date(2026, 3, 31)


def test_base_case_carries_effective_inputs_for_sector_aggregates() -> None:
    """The result echoes the identity and the fundamentals actually used."""
    result = compute_valuation(inputs())

    assert (result.ticker, result.sector) == ("TST", "Telecommunications")
    assert result.shares_outstanding == 1_000_000
    assert result.close_price == Decimal("25.00")
    assert result.net_income_kes == Decimal("2500000")
    assert result.total_equity_kes == Decimal("12500000")
    assert result.ebitda_kes == Decimal("4000000")
    assert result.dps_kes is None


def test_dividend_paying_equity_has_no_flags() -> None:
    """dps 1.25 on close 25.00: yield 1.25 ÷ 25.00 × 100 = 5.00%, and every metric is defined."""
    result = valued(dps_kes=Decimal("1.25"))

    assert result.dividend_yield_pct == Decimal("5")
    assert rounded(result.dividend_yield_pct, "pct") == "5.00"
    assert result.flags == []


def test_primitives_on_base_numbers() -> None:
    """Each primitive on the base numbers, returning a value and no flag."""
    cap = market_cap(Decimal("25.00"), 1_000_000)
    assert cap == Decimal("25000000")

    assert eps(Decimal("2500000"), 1_000_000) == Metric(Decimal("2.5"), None)
    assert pe_ratio(cap, Decimal("2500000")) == Metric(Decimal("10"), None)
    assert pb_ratio(cap, Decimal("12500000")) == Metric(Decimal("2"), None)

    ev = enterprise_value(cap, Decimal("5000000"), Decimal("2000000"), sector="Telecommunications")
    assert ev == Metric(Decimal("28000000"), None)
    assert ev_ebitda(ev.value, Decimal("4000000")) == Metric(Decimal("7"), None)


# --- Earnings edge cases ------------------------------------------------------------------------


def test_negative_earnings_leave_pe_null_but_return_negative_eps() -> None:
    """NI −500,000: P/E undefined (pe_negative_earnings); EPS −500,000 ÷ 1,000,000 = −0.50."""
    result = valued(net_income_kes=Decimal("-500000"))

    assert result.pe is None
    assert "pe_negative_earnings" in result.flags
    assert result.eps_kes == Decimal("-0.5")
    assert rounded(result.eps_kes, "per_share") == "-0.50"
    assert pe_ratio(Decimal("25000000"), Decimal("-500000")) == Metric(None, "pe_negative_earnings")


def test_zero_earnings_leave_pe_null_and_eps_zero() -> None:
    """NI 0: P/E undefined (pe_zero_earnings), never a division by zero; EPS 0.00."""
    result = valued(net_income_kes=Decimal("0"))

    assert result.pe is None
    assert "pe_zero_earnings" in result.flags
    assert result.eps_kes == Decimal("0")
    assert rounded(result.eps_kes, "per_share") == "0.00"
    assert pe_ratio(Decimal("25000000"), Decimal("0")) == Metric(None, "pe_zero_earnings")


def test_missing_earnings_leave_pe_and_eps_null() -> None:
    """NI missing: P/E and EPS both undefined, reported once as pe_missing_earnings."""
    result = valued(net_income_kes=None)

    assert result.pe is None
    assert result.eps_kes is None
    assert result.flags.count("pe_missing_earnings") == 1
    assert eps(None, 1_000_000) == Metric(None, "pe_missing_earnings")
    assert pe_ratio(Decimal("25000000"), None) == Metric(None, "pe_missing_earnings")


# --- Book value edge cases ----------------------------------------------------------------------


@pytest.mark.parametrize("equity", ["0", "-1"])
def test_non_positive_equity_leaves_pb_null(equity: str) -> None:
    """Equity ≤ 0: P/B undefined with pb_non_positive_equity; P/E 10.00 is unaffected."""
    result = valued(total_equity_kes=Decimal(equity))

    assert result.pb is None
    assert "pb_non_positive_equity" in result.flags
    assert rounded(result.pe, "ratio") == "10.00"
    assert pb_ratio(Decimal("25000000"), Decimal(equity)) == Metric(None, "pb_non_positive_equity")


def test_missing_equity_leaves_pb_null() -> None:
    """Equity missing: P/B undefined with pb_missing_equity."""
    result = valued(total_equity_kes=None)

    assert result.pb is None
    assert "pb_missing_equity" in result.flags
    assert pb_ratio(Decimal("25000000"), None) == Metric(None, "pb_missing_equity")


# --- Enterprise value and EV/EBITDA edge cases --------------------------------------------------


def test_banking_sector_excludes_ev_even_with_ebitda_debt_and_cash_present() -> None:
    """Banking is checked first: EV and EV/EBITDA are undefined (ev_not_meaningful_for_banks)
    whatever EBITDA 4,000,000, debt 5,000,000 and cash 2,000,000 say; P/E 10.00 and P/B 2.00
    are still computed.
    """
    assert EV_EXCLUDED_SECTORS == frozenset({"Banking"})

    result = compute_valuation(inputs(sector="Banking"))

    assert result.enterprise_value_kes is None
    assert result.ev_ebitda is None
    assert "ev_not_meaningful_for_banks" in result.flags
    assert "ev_missing_inputs" not in result.flags
    assert not any(flag.startswith("ev_ebitda_") for flag in result.flags)
    assert "negative_enterprise_value" not in result.flags
    assert rounded(result.pe, "ratio") == "10.00"
    assert rounded(result.pb, "ratio") == "2.00"
    assert result.flags == ["ev_not_meaningful_for_banks", "dy_missing_dps"]

    banking_ev = enterprise_value(
        Decimal("25000000"), Decimal("5000000"), Decimal("2000000"), sector="Banking"
    )
    assert banking_ev == Metric(None, "ev_not_meaningful_for_banks")


@pytest.mark.parametrize("ebitda", ["0", "-1"])
def test_non_positive_ebitda_leaves_ev_ebitda_null_but_keeps_ev(ebitda: str) -> None:
    """EBITDA ≤ 0: EV/EBITDA undefined (ev_ebitda_non_positive_ebitda); EV stays 28,000,000.00."""
    result = valued(ebitda_kes=Decimal(ebitda))

    assert result.ev_ebitda is None
    assert "ev_ebitda_non_positive_ebitda" in result.flags
    assert rounded(result.enterprise_value_kes, "money") == "28000000.00"
    assert ev_ebitda(Decimal("28000000"), Decimal(ebitda)) == Metric(
        None, "ev_ebitda_non_positive_ebitda"
    )


def test_missing_ebitda_leaves_ev_ebitda_null_but_keeps_ev() -> None:
    """EBITDA missing: EV/EBITDA undefined (ev_ebitda_missing_ebitda); EV stays 28,000,000.00."""
    result = valued(ebitda_kes=None)

    assert result.ev_ebitda is None
    assert "ev_ebitda_missing_ebitda" in result.flags
    assert rounded(result.enterprise_value_kes, "money") == "28000000.00"
    assert ev_ebitda(Decimal("28000000"), None) == Metric(None, "ev_ebitda_missing_ebitda")


@pytest.mark.parametrize("missing", ["total_debt_kes", "cash_and_equivalents_kes"])
def test_missing_debt_or_cash_leaves_ev_and_ev_ebitda_null(missing: str) -> None:
    """Debt or cash missing: EV and EV/EBITDA undefined, explained once by ev_missing_inputs."""
    result = valued(**{missing: None})

    assert result.enterprise_value_kes is None
    assert result.ev_ebitda is None
    assert "ev_missing_inputs" in result.flags
    assert not any(flag.startswith("ev_ebitda_") for flag in result.flags)
    # The EV flag already explains the undefined multiple, so the ratio adds no flag of its own.
    assert ev_ebitda(None, Decimal("4000000")) == Metric(None, None)


def test_negative_enterprise_value_is_returned_as_computed_and_flagged() -> None:
    """Debt 0, cash 30,000,000: EV 25,000,000 + 0 − 30,000,000 = −5,000,000.00 and EV/EBITDA
    −5,000,000 ÷ 4,000,000 = −1.25, both returned, plus negative_enterprise_value.
    """
    result = valued(total_debt_kes=Decimal("0"), cash_and_equivalents_kes=Decimal("30000000"))

    assert result.enterprise_value_kes == Decimal("-5000000")
    assert rounded(result.enterprise_value_kes, "money") == "-5000000.00"
    assert result.ev_ebitda == Decimal("-1.25")
    assert rounded(result.ev_ebitda, "ratio") == "-1.25"
    assert "negative_enterprise_value" in result.flags
    assert enterprise_value(
        Decimal("25000000"), Decimal("0"), Decimal("30000000"), sector="Telecommunications"
    ) == Metric(Decimal("-5000000"), "negative_enterprise_value")


def test_zero_enterprise_value_is_returned_as_computed_and_flagged() -> None:
    """EV ≤ 0 includes zero. Debt 0, cash 25,000,000: EV 25,000,000 + 0 − 25,000,000 = 0.00 and
    EV/EBITDA 0 ÷ 4,000,000 = 0.00, both defined (not None), plus negative_enterprise_value.
    """
    result = valued(total_debt_kes=Decimal("0"), cash_and_equivalents_kes=Decimal("25000000"))

    assert result.enterprise_value_kes is not None
    assert result.enterprise_value_kes == Decimal("0")
    assert rounded(result.enterprise_value_kes, "money") == "0.00"
    assert result.ev_ebitda is not None
    assert result.ev_ebitda == Decimal("0")
    assert rounded(result.ev_ebitda, "ratio") == "0.00"
    assert "negative_enterprise_value" in result.flags
    assert not any(flag.startswith("ev_ebitda_") for flag in result.flags)
    assert result.flags == ["negative_enterprise_value", "dy_missing_dps"]
    assert enterprise_value(
        Decimal("25000000"), Decimal("0"), Decimal("25000000"), sector="Telecommunications"
    ) == Metric(Decimal("0"), "negative_enterprise_value")


# --- Shares, currency, availability and staleness -----------------------------------------------


def test_zero_shares_gives_zero_market_cap_and_null_per_share_multiples() -> None:
    """0 shares: market cap 25.00 × 0 = 0; EPS, P/E, P/B and EV/EBITDA undefined (zero_shares),
    with no DivisionByZero or InvalidOperation. EV = 0 + 5,000,000 − 2,000,000 = 3,000,000 does
    not divide by shares, so it is still computed.
    """
    result = compute_valuation(inputs(shares_outstanding=0))

    assert result.market_cap_kes == 0
    assert rounded(result.market_cap_kes, "money") == "0.00"
    assert result.eps_kes is None
    assert result.pe is None
    assert result.pb is None
    assert result.ev_ebitda is None
    assert "zero_shares" in result.flags
    assert result.enterprise_value_kes == Decimal("3000000")
    assert market_cap(Decimal("25.00"), 0) == 0
    assert eps(Decimal("2500000"), 0) == Metric(None, "zero_shares")


@pytest.mark.parametrize("shares", [-1, -1_000_000])
def test_negative_share_count_is_a_caller_error(shares: int) -> None:
    """A negative share count breaks the 0–10^12 database bound and raises ValueError."""
    with pytest.raises(ValueError, match="shares must be >= 0"):
        market_cap(Decimal("25.00"), shares)
    with pytest.raises(ValueError, match="shares must be >= 0"):
        eps(Decimal("2500000"), shares)
    with pytest.raises(ValueError, match="shares must be >= 0"):
        compute_valuation(inputs(shares_outstanding=shares))


def assert_fundamental_metrics_null(result: ValuationResult) -> None:
    """Every fundamentals-based metric and every effective fundamentals input is ``None``."""
    assert result.eps_kes is None
    assert result.pe is None
    assert result.pb is None
    assert result.enterprise_value_kes is None
    assert result.ev_ebitda is None
    assert result.dividend_yield_pct is None
    assert result.fundamentals_period_end is None
    assert result.net_income_kes is None
    assert result.total_equity_kes is None
    assert result.ebitda_kes is None
    assert result.dps_kes is None


def test_currency_mismatch_nulls_every_fundamental_metric() -> None:
    """USD fundamentals against a KES equity are refused (currency_mismatch); market cap
    25,000,000.00 KES is still computed.
    """
    result = compute_valuation(inputs(fundamentals=fundamentals(currency="USD")))

    assert_fundamental_metrics_null(result)
    assert result.flags == ["currency_mismatch"]
    assert rounded(result.market_cap_kes, "money") == "25000000.00"


@pytest.mark.parametrize(
    ("sector", "fundamentals_row", "price_age_days", "expected_flags"),
    [
        pytest.param(
            "Telecommunications",
            fundamentals(currency="USD", net_income_kes=Decimal("100")),
            0,
            ["currency_mismatch"],
            id="usd-equity-usd-fundamentals",
        ),
        pytest.param(
            "Telecommunications",
            fundamentals(net_income_kes=Decimal("100")),
            0,
            ["currency_mismatch"],
            id="usd-equity-kes-fundamentals",
        ),
        pytest.param("Telecommunications", None, 0, ["currency_mismatch"], id="usd-equity-alone"),
        pytest.param(
            "Banking",
            fundamentals(currency="USD", net_income_kes=Decimal("100")),
            11,
            ["stale_price", "currency_mismatch", "ev_not_meaningful_for_banks"],
            id="stale-usd-bank",
        ),
    ],
)
def test_non_kes_equity_is_refused_with_no_market_cap(
    sector: str,
    fundamentals_row: FundamentalsRow | None,
    price_age_days: int,
    expected_flags: list[str],
) -> None:
    """A USD equity, 100 shares closing at 10: close × shares is 1,000 USD, and with USD NI 100
    it would read as P/E 10.00. Analytics accept only KES and never convert, so market cap and
    every fundamental metric are undefined (currency_mismatch) whatever fundamentals it carries;
    a stale close and the bank EV policy are still reported.
    """
    result = compute_valuation(
        inputs(
            sector=sector,
            currency="USD",
            shares_outstanding=100,
            close_price=Decimal("10"),
            price_date=AS_OF - timedelta(days=price_age_days),
            fundamentals=fundamentals_row,
        )
    )

    assert result.market_cap_kes is None
    assert_fundamental_metrics_null(result)
    assert result.flags == expected_flags
    assert (result.shares_outstanding, result.close_price) == (100, Decimal("10"))


def test_no_fundamentals_nulls_every_fundamental_metric() -> None:
    """No fundamentals within the lag: every fundamental metric undefined (no_fundamentals);
    market cap 25,000,000.00 is still computed.
    """
    result = compute_valuation(inputs(fundamentals=None))

    assert_fundamental_metrics_null(result)
    assert result.flags == ["no_fundamentals"]
    assert rounded(result.market_cap_kes, "money") == "25000000.00"


@pytest.mark.parametrize(
    ("fundamentals_row", "reason"),
    [(None, "no_fundamentals"), (fundamentals(currency="USD"), "currency_mismatch")],
)
def test_banking_without_usable_fundamentals_still_reports_sector_policy(
    fundamentals_row: FundamentalsRow | None, reason: str
) -> None:
    """The bank EV policy does not depend on data, so it is reported alongside the reason."""
    result = compute_valuation(inputs(sector="Banking", fundamentals=fundamentals_row))

    assert_fundamental_metrics_null(result)
    assert result.flags == [reason, "ev_not_meaningful_for_banks"]


def test_close_older_than_ten_days_is_flagged_stale_but_still_valued() -> None:
    """Close 11 days before as_of: stale_price is added and P/E 10.00 is still computed."""
    assert STALE_PRICE_DAYS == 10

    result = compute_valuation(inputs(price_date=AS_OF - timedelta(days=11)))

    assert "stale_price" in result.flags
    assert result.flags[0] == "stale_price"
    assert rounded(result.pe, "ratio") == "10.00"


def test_close_exactly_ten_days_old_is_not_stale() -> None:
    """Close exactly 10 days before as_of is within the limit: no stale_price."""
    result = compute_valuation(inputs(price_date=AS_OF - timedelta(days=10)))

    assert "stale_price" not in result.flags
    assert rounded(result.pe, "ratio") == "10.00"


# --- Fundamentals availability lag --------------------------------------------------------------


def test_fiscal_year_is_unavailable_until_ninety_days_after_period_end() -> None:
    """period_end + 90 ≤ as_of: a row 89 days old is ignored, a row exactly 90 days old is used."""
    too_recent = fundamentals(period_end_date=AS_OF - timedelta(days=89))
    just_available = fundamentals(period_end_date=AS_OF - timedelta(days=90))

    assert fundamentals_as_of([too_recent], AS_OF) is None
    assert fundamentals_as_of([just_available], AS_OF) == just_available


def test_latest_available_fiscal_year_wins_and_half_years_are_skipped() -> None:
    """Of FY 2025-03-31, FY 2026-03-31 and a newer, available H1 2026-06-30 (2026-06-30 + 90 days
    = 2026-09-28 ≤ as_of), the latest fiscal year, FY 2026-03-31, is returned. On 2026-06-28,
    before FY 2026 becomes available on 2026-06-29, FY 2025 still applies.
    """
    fy_2025 = fundamentals(period_end_date=date(2025, 3, 31))
    fy_2026 = fundamentals(period_end_date=date(2026, 3, 31))
    h1_2026 = fundamentals(period_end_date=date(2026, 6, 30), period_type="H1")

    assert fundamentals_as_of([fy_2025, fy_2026, h1_2026], AS_OF) == fy_2026
    assert fundamentals_as_of([fy_2025, fy_2026], date(2026, 6, 28)) == fy_2025
    assert fundamentals_as_of([h1_2026], AS_OF) is None


def test_lag_parameter_is_honoured() -> None:
    """lag_days=0 makes a row available on its own period end; the default lag does not."""
    same_day = fundamentals(period_end_date=AS_OF)

    assert fundamentals_as_of([same_day], AS_OF, lag_days=0) == same_day
    assert fundamentals_as_of([same_day], AS_OF) is None


def test_lag_lookup_edge_inputs() -> None:
    """No rows gives None; a negative lag is a caller error; a cutoff before date.min gives None."""
    assert fundamentals_as_of([], AS_OF) is None
    with pytest.raises(ValueError, match="lag_days must be >= 0"):
        fundamentals_as_of([fundamentals()], AS_OF, lag_days=-1)
    assert fundamentals_as_of([fundamentals()], date.min + timedelta(days=10)) is None


def test_unavailable_fundamentals_value_as_no_fundamentals() -> None:
    """A fiscal year 89 days old is not yet published, so the valuation reports no_fundamentals."""
    row = fundamentals(period_end_date=AS_OF - timedelta(days=89))

    result = compute_valuation(inputs(fundamentals=fundamentals_as_of([row], AS_OF)))

    assert_fundamental_metrics_null(result)
    assert "no_fundamentals" in result.flags
    assert rounded(result.market_cap_kes, "money") == "25000000.00"


# --- Sector aggregates over matched eligible sets -----------------------------------------------


def member(sector: str = "Telecommunications", **overrides: Any) -> ValuationResult:
    """A sector constituent: the base equity with ``ValuationInputs`` fields replaced."""
    return compute_valuation(inputs(sector=sector, **overrides))


def member_b(sector: str = "Telecommunications") -> ValuationResult:
    """Close 10.00, 1,500,000 shares (market cap 15,000,000), debt 0, cash 0, EBITDA 1,000,000,
    no net income, no equity and dps 0.50.
    """
    return member(
        sector,
        ticker="TSB",
        shares_outstanding=1_500_000,
        close_price=Decimal("10.00"),
        fundamentals=fundamentals(
            net_income_kes=None,
            total_equity_kes=None,
            ebitda_kes=Decimal("1000000"),
            total_debt_kes=Decimal("0"),
            cash_and_equivalents_kes=Decimal("0"),
            dps_kes=Decimal("0.50"),
        ),
    )


def with_fundamentals(ticker: str, **fundamentals_overrides: Any) -> ValuationResult:
    """A base-equity constituent with some fundamentals replaced."""
    return member(ticker=ticker, fundamentals=fundamentals(**fundamentals_overrides))


def test_sector_aggregates_use_matched_eligible_sets() -> None:
    """A (base) and B: market cap 25,000,000 + 15,000,000 = 40,000,000.00;
    P/E 25,000,000 ÷ 2,500,000 = 10.00 over A only; P/B 25,000,000 ÷ 12,500,000 = 2.00 over A
    only; EV/EBITDA (28,000,000 + 15,000,000) ÷ (4,000,000 + 1,000,000) = 8.60 over both;
    dividend yield 0.50 × 1,500,000 ÷ 15,000,000 × 100 = 5.00% over B's market cap only.
    """
    aggregate = sector_aggregates("Telecommunications", [member(), member_b()])

    assert aggregate.sector == "Telecommunications"
    assert aggregate.constituent_count == 2
    assert rounded(aggregate.market_cap_kes, "money") == "40000000.00"
    assert aggregate.pe == Decimal("10")
    assert rounded(aggregate.pe, "ratio") == "10.00"
    assert aggregate.pe_eligible_count == 1
    assert rounded(aggregate.pb, "ratio") == "2.00"
    assert aggregate.pb_eligible_count == 1
    assert aggregate.ev_ebitda == Decimal("8.6")
    assert rounded(aggregate.ev_ebitda, "ratio") == "8.60"
    assert aggregate.ev_ebitda_eligible_count == 2
    assert aggregate.dividend_yield_pct == Decimal("5")
    assert rounded(aggregate.dividend_yield_pct, "pct") == "5.00"
    assert aggregate.dy_eligible_count == 1
    assert aggregate.flags == []


def refused_member(equity_currency: str) -> ValuationResult:
    """R: 1,000,000 shares at 10.00 whose fundamentals are in USD (NI 100,000, equity 1,000,000,
    EBITDA 500,000, debt 0, cash 0, dps 0.10), figures that would move every aggregate if they
    were summed with KES ones.
    """
    return member(
        ticker="USDR",
        currency=equity_currency,
        shares_outstanding=1_000_000,
        close_price=Decimal("10.00"),
        fundamentals=fundamentals(
            currency="USD",
            net_income_kes=Decimal("100000"),
            total_equity_kes=Decimal("1000000"),
            ebitda_kes=Decimal("500000"),
            total_debt_kes=Decimal("0"),
            cash_and_equivalents_kes=Decimal("0"),
            dps_kes=Decimal("0.10"),
        ),
    )


@pytest.mark.parametrize(
    ("equity_currency", "market_cap_total"),
    [
        pytest.param("USD", "25000000.00", id="usd-equity-left-out-of-the-total"),
        pytest.param("KES", "35000000.00", id="kes-equity-with-usd-fundamentals-keeps-its-cap"),
    ],
)
def test_sector_aggregates_leave_out_refused_currency_inputs(
    equity_currency: str, market_cap_total: str
) -> None:
    """A (the base equity, dps 1.25) plus R. Over A alone: P/E 25,000,000 ÷ 2,500,000 = 10.00,
    P/B 25,000,000 ÷ 12,500,000 = 2.00, EV/EBITDA 28,000,000 ÷ 4,000,000 = 7.00 and dividend
    yield 1.25 × 1,000,000 ÷ 25,000,000 × 100 = 5.00%; summing R's USD figures in would give
    P/E 35,000,000 ÷ 2,600,000 = 13.46 instead. A USD-listed R has no KES market cap, so the total
    is A's 25,000,000.00; a KES-listed R keeps its 10,000,000 cap: 35,000,000.00. Either way R
    counts as a constituent, joins no eligible set and flags the sector currency_mismatch.
    """
    kes_member = member(fundamentals=fundamentals(dps_kes=Decimal("1.25")))

    aggregate = sector_aggregates(
        "Telecommunications", [kes_member, refused_member(equity_currency)]
    )

    assert aggregate.constituent_count == 2
    assert rounded(aggregate.market_cap_kes, "money") == market_cap_total
    assert aggregate.pe == Decimal("10")
    assert rounded(aggregate.pe, "ratio") == "10.00"
    assert aggregate.pb == Decimal("2")
    assert aggregate.ev_ebitda == Decimal("7")
    assert aggregate.dividend_yield_pct == Decimal("5")
    assert rounded(aggregate.dividend_yield_pct, "pct") == "5.00"
    assert (
        aggregate.pe_eligible_count,
        aggregate.pb_eligible_count,
        aggregate.ev_ebitda_eligible_count,
        aggregate.dy_eligible_count,
    ) == (1, 1, 1, 1)
    assert aggregate.flags == ["currency_mismatch"]


def test_sector_of_only_refused_constituents_has_no_market_cap() -> None:
    """Only R, listed in USD: there is no KES market cap to sum, so the total is None rather than
    0.00, every aggregate is undefined over empty eligible sets, and R still counts as the one
    constituent.
    """
    aggregate = sector_aggregates("Telecommunications", [refused_member("USD")])

    assert aggregate.constituent_count == 1
    assert aggregate.market_cap_kes is None
    assert (aggregate.pe, aggregate.pb, aggregate.ev_ebitda) == (None, None, None)
    assert aggregate.dividend_yield_pct is None
    assert (
        aggregate.pe_eligible_count,
        aggregate.pb_eligible_count,
        aggregate.ev_ebitda_eligible_count,
        aggregate.dy_eligible_count,
    ) == (0, 0, 0, 0)
    assert aggregate.flags == [
        "currency_mismatch",
        "sector_pe_unavailable",
        "sector_pb_unavailable",
        "sector_ev_ebitda_unavailable",
        "sector_dy_unavailable",
    ]


def test_sector_pe_is_null_when_summed_earnings_are_not_positive() -> None:
    """NI −3,000,000 and +1,000,000 sum to −2,000,000 ≤ 0: sector P/E undefined."""
    members = [
        with_fundamentals("LOSS", net_income_kes=Decimal("-3000000")),
        with_fundamentals("GAIN", net_income_kes=Decimal("1000000")),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    assert aggregate.pe is None
    assert "sector_pe_unavailable" in aggregate.flags
    assert aggregate.pe_eligible_count == 2


def test_sector_pe_is_null_when_no_member_has_earnings() -> None:
    """Both NI missing: the P/E eligible set is empty, so sector P/E is undefined."""
    members = [
        with_fundamentals("NONE1", net_income_kes=None),
        with_fundamentals("NONE2", net_income_kes=None),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    assert aggregate.pe is None
    assert "sector_pe_unavailable" in aggregate.flags
    assert aggregate.pe_eligible_count == 0
    assert rounded(aggregate.pb, "ratio") == "2.00"


def test_sector_pb_is_null_when_summed_equity_is_not_positive() -> None:
    """Equity −5,000,000 and +1,000,000 sum to −4,000,000 ≤ 0: sector P/B undefined."""
    members = [
        with_fundamentals("NEG", total_equity_kes=Decimal("-5000000")),
        with_fundamentals("POS", total_equity_kes=Decimal("1000000")),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    assert aggregate.pb is None
    assert "sector_pb_unavailable" in aggregate.flags
    assert aggregate.pb_eligible_count == 2


def test_sector_ev_ebitda_is_null_when_summed_ebitda_is_not_positive() -> None:
    """EBITDA 0 and −1 sum to −1 ≤ 0: sector EV/EBITDA undefined over the two eligible members."""
    members = [
        with_fundamentals("ZERO", ebitda_kes=Decimal("0")),
        with_fundamentals("NEG", ebitda_kes=Decimal("-1")),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    assert aggregate.ev_ebitda is None
    assert "sector_ev_ebitda_unavailable" in aggregate.flags
    assert aggregate.ev_ebitda_eligible_count == 2
    assert rounded(aggregate.pe, "ratio") == "10.00"


@pytest.mark.parametrize(
    ("field", "first", "second", "undefined", "flag"),
    [
        pytest.param(
            "net_income_kes", "0", "0", "pe", "sector_pe_unavailable", id="net-income-all-zero"
        ),
        pytest.param(
            "net_income_kes",
            "-1000000",
            "1000000",
            "pe",
            "sector_pe_unavailable",
            id="net-income-canceling",
        ),
        pytest.param(
            "total_equity_kes", "0", "0", "pb", "sector_pb_unavailable", id="equity-all-zero"
        ),
        pytest.param(
            "total_equity_kes",
            "-1000000",
            "1000000",
            "pb",
            "sector_pb_unavailable",
            id="equity-canceling",
        ),
        pytest.param(
            "ebitda_kes",
            "0",
            "0",
            "ev_ebitda",
            "sector_ev_ebitda_unavailable",
            id="ebitda-all-zero",
        ),
        pytest.param(
            "ebitda_kes",
            "-1000000",
            "1000000",
            "ev_ebitda",
            "sector_ev_ebitda_unavailable",
            id="ebitda-canceling",
        ),
    ],
)
def test_sector_aggregate_is_null_when_summed_denominator_is_exactly_zero(
    field: str, first: str, second: str, undefined: str, flag: str
) -> None:
    """Two base-equity members whose NI, equity or EBITDA is 0 and 0, or −1,000,000 and
    +1,000,000: Σ = 0 ≤ 0, so that aggregate is undefined (None with its flag), never an exception
    from 50,000,000 ÷ 0 or 56,000,000 ÷ 0, and both members stay in its eligible set. The others
    stand: sector P/E 50,000,000 ÷ 5,000,000 = 10.00, P/B 50,000,000 ÷ 25,000,000 = 2.00 and
    EV/EBITDA 56,000,000 ÷ 8,000,000 = 7.00. dps is missing, so the dividend yield is undefined too.
    """
    members = [
        with_fundamentals("ONE", **{field: Decimal(first)}),
        with_fundamentals("TWO", **{field: Decimal(second)}),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    ratios = {"pe": aggregate.pe, "pb": aggregate.pb, "ev_ebitda": aggregate.ev_ebitda}
    assert ratios.pop(undefined) is None
    defined = {"pe": "10.00", "pb": "2.00", "ev_ebitda": "7.00"}
    del defined[undefined]
    assert {name: rounded(value, "ratio") for name, value in ratios.items()} == defined
    assert aggregate.flags == [flag, "sector_dy_unavailable"]
    assert aggregate.constituent_count == 2
    assert (
        aggregate.pe_eligible_count,
        aggregate.pb_eligible_count,
        aggregate.ev_ebitda_eligible_count,
        aggregate.dy_eligible_count,
    ) == (2, 2, 2, 0)


def test_banking_sector_ev_ebitda_is_never_aggregated() -> None:
    """Banking with EBITDA present: sector EV/EBITDA undefined (ev_not_meaningful_for_banks)
    with no eligible members, while sector P/E 25,000,000 ÷ 2,500,000 = 10.00 over A is computed.
    """
    aggregate = sector_aggregates("Banking", [member("Banking"), member_b("Banking")])

    assert aggregate.ev_ebitda is None
    assert "ev_not_meaningful_for_banks" in aggregate.flags
    assert "sector_ev_ebitda_unavailable" not in aggregate.flags
    assert aggregate.ev_ebitda_eligible_count == 0
    assert aggregate.pe is not None
    assert rounded(aggregate.pe, "ratio") == "10.00"


def test_sector_dividend_yield_is_null_when_eligible_members_have_zero_shares() -> None:
    """Every dps-paying member has 0 shares: Σ dividends ÷ Σ market cap is 0 ÷ 0, so the sector
    yield is undefined (sector_dy_unavailable) rather than raising or reporting 0.
    """
    members = [
        member(
            ticker="ZS1",
            shares_outstanding=0,
            fundamentals=fundamentals(dps_kes=Decimal("0.50")),
        ),
        member(
            ticker="ZS2",
            shares_outstanding=0,
            fundamentals=fundamentals(dps_kes=Decimal("1.00")),
        ),
    ]

    aggregate = sector_aggregates("Telecommunications", members)

    assert aggregate.dividend_yield_pct is None
    assert "sector_dy_unavailable" in aggregate.flags
    assert aggregate.dy_eligible_count == 2


def test_empty_sector_has_no_aggregates() -> None:
    """No constituents: no market cap, every aggregate undefined and every eligible count 0."""
    aggregate = sector_aggregates("Telecommunications", [])

    assert aggregate.constituent_count == 0
    assert aggregate.market_cap_kes is None
    assert (aggregate.pe, aggregate.pb, aggregate.ev_ebitda) == (None, None, None)
    assert aggregate.dividend_yield_pct is None
    assert aggregate.flags == [
        "sector_pe_unavailable",
        "sector_pb_unavailable",
        "sector_ev_ebitda_unavailable",
        "sector_dy_unavailable",
    ]
    assert (
        aggregate.pe_eligible_count,
        aggregate.pb_eligible_count,
        aggregate.ev_ebitda_eligible_count,
        aggregate.dy_eligible_count,
    ) == (0, 0, 0, 0)


# --- Session price change -----------------------------------------------------------------------


def test_price_change_pct_from_previous_close() -> None:
    """Close 26.00 against previous close 25.00: (26.00 − 25.00) ÷ 25.00 × 100 = 4.00%."""
    change = price_change_pct(Decimal("26.00"), Decimal("25.00"))

    assert change == Decimal("4")
    assert rounded(change, "pct") == "4.00"


@pytest.mark.parametrize("previous_close", [None, Decimal("0"), Decimal("-1.00")])
def test_price_change_pct_is_null_without_a_positive_previous_close(
    previous_close: Decimal | None,
) -> None:
    """No previous close, or one ≤ 0, leaves the change undefined rather than dividing by zero."""
    assert price_change_pct(Decimal("26.00"), previous_close) is None


# --- Non-finite inputs --------------------------------------------------------------------------

NON_FINITE = ["NaN", "sNaN", "Infinity", "-Infinity"]


@pytest.mark.parametrize("value", NON_FINITE)
@pytest.mark.parametrize(
    ("field", "name"),
    [
        ("close_price", "close"),
        ("net_income_kes", "net_income_kes"),
        ("total_equity_kes", "total_equity_kes"),
        ("ebitda_kes", "ebitda_kes"),
        ("total_debt_kes", "total_debt_kes"),
        ("cash_and_equivalents_kes", "cash_and_equivalents_kes"),
        ("dps_kes", "dps_kes"),
    ],
)
def test_non_finite_valuation_input_is_a_caller_error(field: str, name: str, value: str) -> None:
    """The base equity with dps 1.25 is fully valued (P/E 10.00, P/B 2.00, EV/EBITDA 7.00, yield
    5.00%). A NaN or infinite close or fundamentals figure raises ValueError naming it, rather than
    an infinite metric or a wrong finite one: equity +Infinity would give P/B 25,000,000 ÷ ∞,
    which rounds to 0.00 with no flag.
    """
    row = fundamentals(dps_kes=Decimal("1.25"))
    if field == "close_price":
        case = inputs(close_price=Decimal(value), fundamentals=row)
    else:
        case = inputs(fundamentals=replace(row, **{field: Decimal(value)}))

    with pytest.raises(ValueError, match=f"^{name} must be finite, got {value}$"):
        compute_valuation(case)


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        pytest.param({"shares_outstanding": 0}, "net_income_kes", id="zero-shares-net-income"),
        pytest.param({"shares_outstanding": 0}, "total_equity_kes", id="zero-shares-equity"),
        pytest.param({"shares_outstanding": 0}, "ebitda_kes", id="zero-shares-ebitda"),
        pytest.param({"sector": "Banking"}, "total_debt_kes", id="banking-debt"),
        pytest.param({"sector": "Banking"}, "cash_and_equivalents_kes", id="banking-cash"),
        pytest.param({"sector": "Banking"}, "ebitda_kes", id="banking-ebitda"),
    ],
)
def test_non_finite_input_raises_on_paths_that_skip_its_metric(
    overrides: dict[str, Any], field: str
) -> None:
    """Zero shares skips EPS, P/E, P/B and EV/EBITDA, and Banking skips EV and EV/EBITDA, yet an
    infinite figure those metrics would have read still raises instead of being carried into the
    result's effective inputs, where sector aggregates would sum it.
    """
    case = inputs(fundamentals=fundamentals(**{field: Decimal("Infinity")}), **overrides)

    with pytest.raises(ValueError, match=f"^{field} must be finite, got Infinity$"):
        compute_valuation(case)


@pytest.mark.parametrize("value", NON_FINITE)
def test_refused_fundamentals_are_not_checked(value: str) -> None:
    """USD fundamentals are refused (currency_mismatch) and never used, so a non-finite figure in
    them changes nothing: market cap 25,000,000.00 and every fundamental metric undefined.
    """
    row = fundamentals(currency="USD", net_income_kes=Decimal(value), dps_kes=Decimal(value))

    result = compute_valuation(inputs(fundamentals=row))

    assert_fundamental_metrics_null(result)
    assert result.flags == ["currency_mismatch"]
    assert rounded(result.market_cap_kes, "money") == "25000000.00"


@pytest.mark.parametrize("value", NON_FINITE)
@pytest.mark.parametrize(
    ("call", "name"),
    [
        pytest.param(lambda x: market_cap(x, 1_000_000), "close", id="market_cap-close"),
        pytest.param(lambda x: eps(x, 1_000_000), "net_income", id="eps-net_income"),
        pytest.param(
            lambda x: pe_ratio(x, Decimal("2500000")), "market_cap_kes", id="pe_ratio-market_cap"
        ),
        pytest.param(
            lambda x: pe_ratio(Decimal("25000000"), x), "net_income", id="pe_ratio-net_income"
        ),
        pytest.param(
            lambda x: pb_ratio(x, Decimal("12500000")), "market_cap_kes", id="pb_ratio-market_cap"
        ),
        pytest.param(lambda x: pb_ratio(Decimal("25000000"), x), "equity", id="pb_ratio-equity"),
        pytest.param(
            lambda x: enterprise_value(
                x, Decimal("5000000"), Decimal("2000000"), sector="Telecommunications"
            ),
            "market_cap_kes",
            id="enterprise_value-market_cap",
        ),
        pytest.param(
            lambda x: enterprise_value(
                Decimal("25000000"), x, Decimal("2000000"), sector="Telecommunications"
            ),
            "debt",
            id="enterprise_value-debt",
        ),
        pytest.param(
            lambda x: enterprise_value(
                Decimal("25000000"), Decimal("5000000"), x, sector="Telecommunications"
            ),
            "cash",
            id="enterprise_value-cash",
        ),
        pytest.param(
            lambda x: enterprise_value(
                Decimal("25000000"), x, Decimal("2000000"), sector="Banking"
            ),
            "debt",
            id="enterprise_value-banking-debt",
        ),
        pytest.param(lambda x: ev_ebitda(x, Decimal("4000000")), "ev", id="ev_ebitda-ev"),
        pytest.param(lambda x: ev_ebitda(Decimal("28000000"), x), "ebitda", id="ev_ebitda-ebitda"),
        pytest.param(
            lambda x: dividend_yield_pct(x, Decimal("25.00")), "dps", id="dividend_yield-dps"
        ),
        pytest.param(
            lambda x: dividend_yield_pct(Decimal("1.25"), x), "close", id="dividend_yield-close"
        ),
        pytest.param(
            lambda x: price_change_pct(x, Decimal("25.00")), "close", id="price_change-close"
        ),
        pytest.param(
            lambda x: price_change_pct(Decimal("26.00"), x),
            "previous_close",
            id="price_change-previous_close",
        ),
    ],
)
def test_primitive_rejects_a_non_finite_argument(
    call: Callable[[Decimal], object], name: str, value: str
) -> None:
    """Every primitive checks each Decimal argument at entry, the Banking EV policy included, so
    a NaN or infinite figure raises ValueError naming it instead of a signal or an infinite value.
    """
    with pytest.raises(ValueError, match=f"^{name} must be finite, got {value}$"):
        call(Decimal(value))


@pytest.mark.parametrize("value", NON_FINITE)
@pytest.mark.parametrize(
    "field",
    [
        "market_cap_kes",
        "net_income_kes",
        "total_equity_kes",
        "enterprise_value_kes",
        "ebitda_kes",
        "dps_kes",
    ],
)
def test_sector_aggregates_reject_a_non_finite_member_figure(field: str, value: str) -> None:
    """A hand-built ValuationResult can carry a figure compute_valuation never returns. Summed in,
    NI +Infinity would give sector P/E 25,000,000 ÷ ∞ = 0.00 with no sector_pe_unavailable, so a
    NaN or infinite summed figure raises ValueError naming it.
    """
    base = member(fundamentals=fundamentals(dps_kes=Decimal("1.25")))
    hostile = replace(base, **{field: Decimal(value)})

    with pytest.raises(ValueError, match=f"^{field} must be finite, got {value}$"):
        sector_aggregates("Telecommunications", [member_b(), hostile])
