"""Shared input records of the quant engine and the as-of lookups over them.

Every quant module takes these plain records rather than ORM rows, so formulas stay free of I/O and
are testable with literals. Units follow the database columns they mirror:

* ``*_kes`` fields are whole Kenyan shillings (KES, the default currency), never thousands;
* ``*_pct`` fields are percentage points, so ``Decimal("6.80")`` means 6.80%;
* volumes are integer share counts.

The lookups require their ``rows`` sorted ascending by the record's date field, as services load
them with ``ORDER BY``. Unsorted input is a caller error and gives undefined results: it is not
detected, because checking order would cost the O(log n) bisection its advantage on the daily
calls the backtester and valuation bands make.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Final, Literal, NamedTuple, get_args

__all__ = [
    "DEFAULT_FUNDAMENTALS_LAG_DAYS",
    "DEFAULT_MACRO_MAX_STALENESS_DAYS",
    "AsOfValue",
    "FundamentalsRow",
    "MacroColumn",
    "MacroRow",
    "PriceRow",
    "close_as_of",
    "fundamentals_as_of",
    "macro_as_of",
]

DEFAULT_FUNDAMENTALS_LAG_DAYS: Final = 90
"""Days after ``period_end_date`` before a fiscal-year row is treated as published."""

DEFAULT_MACRO_MAX_STALENESS_DAYS: Final = 62
"""Oldest macro observation, in calendar days, an as-of lookup still accepts."""

MacroColumn = Literal[
    "inflation_yoy_pct",
    "cbr_rate_pct",
    "interbank_rate_pct",
    "tbill_91d_pct",
    "tbill_182d_pct",
    "tbill_364d_pct",
    "fx_kes_usd",
    "fx_reserves_usd_m",
]
"""An indicator field of :class:`MacroRow` that :func:`macro_as_of` can resolve."""

# macro_as_of reads the column with getattr, which the type checker cannot see through, so the
# name is also checked at run time against the same literal set.
_MACRO_COLUMNS: Final[frozenset[str]] = frozenset(get_args(MacroColumn))


@dataclass(frozen=True, slots=True)
class PriceRow:
    """One observed trading session of one ticker, mirroring ``terminal_daily_prices``.

    A session the ticker did not trade, or that no source recorded, has no ``PriceRow``: missing
    days are never zero-filled, while a recorded day with ``volume == 0`` is a real observation.

    Attributes:
        ticker: Exchange ticker, for example ``"SCOM"``.
        trade_date: Session date in Africa/Nairobi.
        close_price: Closing price in KES per share.
        volume: Shares traded.
        turnover_kes: Value traded in KES.
        foreign_net_flow_kes: Foreign investors' net purchases in KES (positive means net
            buying), or ``None`` where the session has no flow data.
    """

    ticker: str
    trade_date: date
    close_price: Decimal
    volume: int
    turnover_kes: Decimal
    foreign_net_flow_kes: Decimal | None = None


@dataclass(frozen=True, slots=True)
class FundamentalsRow:
    """One reporting period of one company, mirroring ``terminal_fundamentals``.

    Every line item is optional, because filings and imports can supply a subset.

    Attributes:
        period_end_date: Last day of the reporting period.
        period_type: ``"FY"`` (fiscal year) or ``"H1"`` (half year).
        currency: ISO 4217 code of every monetary field; ``"KES"`` unless stated otherwise.
        net_income_kes: Profit for the period.
        total_equity_kes: Shareholders' equity at period end.
        ebitda_kes: Earnings before interest, tax, depreciation and amortisation.
        total_debt_kes: Borrowings at period end.
        cash_and_equivalents_kes: Cash and cash equivalents at period end.
        dps_kes: Dividend per share, in KES per share.
        revenue_kes: Revenue for the period.
    """

    period_end_date: date
    period_type: str = "FY"
    currency: str = "KES"
    net_income_kes: Decimal | None = None
    total_equity_kes: Decimal | None = None
    ebitda_kes: Decimal | None = None
    total_debt_kes: Decimal | None = None
    cash_and_equivalents_kes: Decimal | None = None
    dps_kes: Decimal | None = None
    revenue_kes: Decimal | None = None


@dataclass(frozen=True, slots=True)
class MacroRow:
    """One macro observation date, mirroring ``terminal_macro_indicators``.

    Series arrive on different cadences, so any indicator may be ``None`` on a given date.

    Attributes:
        observation_date: Date the values refer to (month-end for monthly series).
        inflation_yoy_pct: 12-month consumer price inflation, in percentage points.
        cbr_rate_pct: Central Bank Rate, in percentage points.
        interbank_rate_pct: Interbank rate, in percentage points.
        tbill_91d_pct: 91-day Treasury bill yield, in percentage points.
        tbill_182d_pct: 182-day Treasury bill yield, in percentage points.
        tbill_364d_pct: 364-day Treasury bill yield, in percentage points.
        fx_kes_usd: Exchange rate in KES per USD.
        fx_reserves_usd_m: Central bank foreign exchange reserves in millions of USD.
    """

    observation_date: date
    inflation_yoy_pct: Decimal | None = None
    cbr_rate_pct: Decimal | None = None
    interbank_rate_pct: Decimal | None = None
    tbill_91d_pct: Decimal | None = None
    tbill_182d_pct: Decimal | None = None
    tbill_364d_pct: Decimal | None = None
    fx_kes_usd: Decimal | None = None
    fx_reserves_usd_m: Decimal | None = None


class AsOfValue(NamedTuple):
    """A value resolved as of a day, with the date it was actually observed."""

    value: Decimal
    observation_date: date


def _require_non_negative(name: str, days: int | None) -> None:
    if days is not None and days < 0:
        raise ValueError(f"{name} must be >= 0, got {days}")


def fundamentals_as_of(
    rows: Sequence[FundamentalsRow],
    as_of: date,
    lag_days: int = DEFAULT_FUNDAMENTALS_LAG_DAYS,
) -> FundamentalsRow | None:
    """Return the latest fiscal-year row that was available on ``as_of``.

    A row counts as available once ``period_end_date + lag_days <= as_of``. Publication dates are
    not tracked, so the lag approximates when results became public: it reduces look-ahead bias
    but does not remove it, because a company that reports later than the lag is still used from
    the lag onwards. Only ``period_type == "FY"`` rows are returned; half-year rows are skipped.

    Args:
        rows: One company's periods, sorted ascending by ``period_end_date``.
        as_of: Day on which the analysis stands.
        lag_days: Calendar days between period end and assumed availability.

    Returns:
        The qualifying row, or ``None`` when no fiscal-year row is available yet.

    Raises:
        ValueError: If ``lag_days`` is negative.
    """
    _require_non_negative("lag_days", lag_days)
    try:
        cutoff = as_of - timedelta(days=lag_days)
    except OverflowError:
        # The cutoff falls before date.min, so no period can have ended in time.
        return None
    index = bisect_right(rows, cutoff, key=lambda row: row.period_end_date)
    for position in range(index - 1, -1, -1):
        row = rows[position]
        if row.period_type == "FY":
            return row
    return None


def macro_as_of(
    rows: Sequence[MacroRow],
    column: MacroColumn,
    day: date,
    max_staleness_days: int | None = DEFAULT_MACRO_MAX_STALENESS_DAYS,
) -> AsOfValue | None:
    """Return the latest non-null ``column`` value observed on or before ``day``.

    Rows whose ``column`` is null are skipped rather than ending the search, so a newer row that
    carries only other indicators (a weekly reserves row, for example) never hides an older
    Treasury bill yield.

    Args:
        rows: Macro observations sorted ascending by ``observation_date``.
        column: The indicator to resolve.
        day: Day to resolve the value for.
        max_staleness_days: Oldest acceptable observation, in calendar days before ``day``;
            ``None`` accepts any age.

    Returns:
        The value with its ``observation_date``, or ``None`` when no non-null value exists on or
        before ``day`` within the staleness limit.

    Raises:
        ValueError: If ``column`` is not an indicator field or ``max_staleness_days`` is negative.
    """
    if column not in _MACRO_COLUMNS:
        raise ValueError(f"unknown macro column {column!r}")
    _require_non_negative("max_staleness_days", max_staleness_days)
    index = bisect_right(rows, day, key=lambda row: row.observation_date)
    for position in range(index - 1, -1, -1):
        row = rows[position]
        age_days = (day - row.observation_date).days
        if max_staleness_days is not None and age_days > max_staleness_days:
            # Older rows are staler still, so the search can stop here.
            return None
        value = getattr(row, column)
        if isinstance(value, Decimal):
            return AsOfValue(value, row.observation_date)
    return None


def close_as_of(
    rows: Sequence[PriceRow],
    day: date,
    max_age_days: int | None = None,
) -> AsOfValue | None:
    """Return the latest close on or before ``day``.

    Args:
        rows: One ticker's sessions, sorted ascending by ``trade_date``.
        day: Day to resolve the close for.
        max_age_days: Oldest acceptable session, in calendar days before ``day`` (the sector
            heatmap uses 10); ``None`` accepts any age.

    Returns:
        ``AsOfValue(close_price, trade_date)``, or ``None`` when there is no session on or before
        ``day`` or the latest one is older than ``max_age_days``.

    Raises:
        ValueError: If ``max_age_days`` is negative.
    """
    _require_non_negative("max_age_days", max_age_days)
    index = bisect_right(rows, day, key=lambda row: row.trade_date)
    if index == 0:
        return None
    row = rows[index - 1]
    if max_age_days is not None and (day - row.trade_date).days > max_age_days:
        return None
    return AsOfValue(row.close_price, row.trade_date)
