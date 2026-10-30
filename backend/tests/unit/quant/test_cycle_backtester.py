"""Event and election cycle backtester (``quant/backtest.py``) and ``asof.macro_as_of``.

Every expected value below is hand-computed from the scenario its docstring states. Inputs are
``Decimal`` values built from strings in KES; results are asserted exactly, either as ``Decimal``
or as the string ``quantize_out`` produces at the response boundary (``index``, ``pct``,
``ratio`` and ``money`` all round to 2 dp).

Most tests share one standard scenario:

* event T = Tue 2022-08-09 ("2022 Cycle"), window ±10 calendar days = 2022-07-30 .. 2022-08-19;
* market sessions are the weekdays 2022-06-27 .. 2022-08-19 (analytics use the observed sessions
  they are given and never a holiday list), so 25 sessions precede the anchor for MA20;
* the anchor is Mon 2022-08-01, the first session in the window, at offset -8;
* data runs through 2022-08-19, the window end, so the cycle is ``complete``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import cast

import pytest

from opennse_terminal.quant.asof import (
    AsOfValue,
    FundamentalsRow,
    MacroColumn,
    MacroRow,
    PriceRow,
    macro_as_of,
)
from opennse_terminal.quant.backtest import (
    BacktestCandidate,
    CycleBacktestResult,
    CycleEvent,
    CycleMarkerResult,
    CycleMarkersResult,
    CyclePointResult,
    CycleProjection,
    CycleWindow,
    MarkerMetric,
    ProjectionPoint,
    add_months,
    build_projection,
    resolve_window,
    run_cycle_backtest,
    with_projection,
)
from opennse_terminal.quant.numeric import QuantizeKind, Violation, quantize_out

# ---------------------------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------------------------


def weekdays(start: date, end: date) -> list[date]:
    """Every Monday-Friday date from ``start`` to ``end`` inclusive."""
    days: list[date] = []
    day = start
    while day <= end:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def drop(sessions: Sequence[date], *days: date) -> list[date]:
    """``sessions`` without ``days``: a ticker that has no row on those sessions."""
    missing = set(days)
    return [day for day in sessions if day not in missing]


def piecewise(initial: str, changes: Mapping[date, str]) -> Callable[[date], str]:
    """A step path: ``initial`` until the first change date, then each change from its date on."""
    ordered = sorted(changes.items())

    def value_on(day: date) -> str:
        current = initial
        for start, value in ordered:
            if start <= day:
                current = value
        return current

    return value_on


def after(day: date, before: str, later: str) -> Callable[[date], str]:
    """``before`` up to and including ``day``, ``later`` from the next calendar day on."""
    return piecewise(before, {day + timedelta(days=1): later})


def prices(
    ticker: str,
    sessions: Sequence[date],
    close: str | Callable[[date], str],
    *,
    turnover: str | Callable[[date], str] = "1000",
    flow: str | Callable[[date], str | None] | None = None,
) -> list[PriceRow]:
    """One actual ``PriceRow`` per session, with a constant volume of 1,000 shares."""

    def text(series: str | Callable[[date], str], day: date) -> str:
        return series if isinstance(series, str) else series(day)

    def flow_on(day: date) -> Decimal | None:
        value = flow(day) if callable(flow) else flow
        return None if value is None else Decimal(value)

    return [
        PriceRow(
            ticker=ticker,
            trade_date=day,
            close_price=Decimal(text(close, day)),
            volume=1000,
            turnover_kes=Decimal(text(turnover, day)),
            foreign_net_flow_kes=flow_on(day),
        )
        for day in sessions
    ]


def fy(
    period_end: date, ni: str | None, *, currency: str = "KES", period_type: str = "FY"
) -> FundamentalsRow:
    """A fiscal-year fundamentals row carrying only net income (KES unless stated)."""
    return FundamentalsRow(
        period_end_date=period_end,
        period_type=period_type,
        currency=currency,
        net_income_kes=None if ni is None else Decimal(ni),
    )


def candidate(
    ticker: str,
    shares: int,
    rows: Sequence[PriceRow],
    fundamentals: Sequence[FundamentalsRow] = (),
    *,
    currency: str = "KES",
) -> BacktestCandidate:
    return BacktestCandidate(
        ticker=ticker,
        shares_outstanding=shares,
        prices=list(rows),
        fundamentals=list(fundamentals),
        currency=currency,
    )


def days_window(event_date: date, pre_days: int, post_days: int) -> CycleWindow:
    window = resolve_window(event_date, pre_days=pre_days, post_days=post_days)
    assert isinstance(window, CycleWindow)
    return window


def default_window(event_date: date) -> CycleWindow:
    """The default window, ±180 calendar days."""
    window = resolve_window(event_date)
    assert isinstance(window, CycleWindow)
    return window


def months_window(event_date: date, pre_months: int, post_months: int) -> CycleWindow:
    window = resolve_window(event_date, pre_months=pre_months, post_months=post_months)
    assert isinstance(window, CycleWindow)
    return window


def q(value: Decimal | None, kind: QuantizeKind) -> str | None:
    """The value as the API serializes it, or ``None``."""
    return None if value is None else str(quantize_out(value, kind))


def column(points: Sequence[CyclePointResult], name: str, kind: QuantizeKind) -> list[str | None]:
    return [q(getattr(point, name), kind) for point in points]


def point_on(result: CycleBacktestResult, day: date) -> CyclePointResult:
    matches = [point for point in result.points if point.trade_date == day]
    assert len(matches) == 1
    return matches[0]


def assert_earliest_minimum(
    points: Sequence[CyclePointResult], marker: CycleMarkerResult | None, metric: MarkerMetric
) -> None:
    """``marker`` is the first point carrying the minimum non-null ``metric`` of ``points``."""
    defined = [(point, getattr(point, metric)) for point in points]
    defined = [(point, value) for point, value in defined if value is not None]
    assert defined
    lowest = min(value for _, value in defined)
    first = next(point for point, value in defined if value == lowest)
    assert marker == CycleMarkerResult(first.offset_days, first.trade_date, metric, lowest)


# ---------------------------------------------------------------------------------------------
# Standard scenario
# ---------------------------------------------------------------------------------------------

EVENT = CycleEvent("2022 Cycle", date(2022, 8, 9))
WINDOW = days_window(EVENT.event_date, 10, 10)
SESSIONS = weekdays(date(2022, 6, 27), date(2022, 8, 19))
ANCHOR = date(2022, 8, 1)
DATA_THROUGH = date(2022, 8, 19)
WINDOW_DAYS = weekdays(ANCHOR, DATA_THROUGH)
OFFSETS = [-8, -7, -6, -5, -4, -1, 0, 1, 2, 3, 6, 7, 8, 9, 10]
FY2021 = date(2021, 12, 31)
"""FY2021 results are available from 2021-12-31 + 90 days = 2022-03-31, before the anchor."""


def run_standard(
    candidates: Sequence[BacktestCandidate],
    *,
    sessions: Sequence[date] = SESSIONS,
    macro_rows: Sequence[MacroRow] = (),
    lag_days: int = 90,
) -> CycleBacktestResult:
    return run_cycle_backtest(
        EVENT,
        WINDOW,
        candidates,
        sessions,
        macro_rows,
        data_available_through=DATA_THROUGH,
        lag_days=lag_days,
    )


def pair(
    *,
    x_close: str | Callable[[date], str] = "10.00",
    y_close: str | Callable[[date], str] = "20.00",
    x_fundamentals: Sequence[FundamentalsRow] = (),
    y_fundamentals: Sequence[FundamentalsRow] = (),
    turnover: str | Callable[[date], str] = "1000",
    shares: tuple[int, int] = (100, 50),
    sessions: Sequence[date] = SESSIONS,
) -> list[BacktestCandidate]:
    """X (100 shares, close 10.00) and Y (50 shares, close 20.00): 1,000 KES of cap each."""
    return [
        candidate(
            "X", shares[0], prices("X", sessions, x_close, turnover=turnover), x_fundamentals
        ),
        candidate(
            "Y", shares[1], prices("Y", sessions, y_close, turnover=turnover), y_fundamentals
        ),
    ]


# ---------------------------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("event_date", "expected"),
    [
        pytest.param(
            date(2022, 8, 9),
            CycleWindow(
                event_date=date(2022, 8, 9),
                window_start=date(2022, 2, 10),
                window_end=date(2023, 2, 5),
                pre_days=180,
                post_days=180,
            ),
            id="2022",
        ),
        pytest.param(
            date(2027, 8, 10),
            CycleWindow(
                event_date=date(2027, 8, 10),
                window_start=date(2027, 2, 11),
                window_end=date(2028, 2, 6),
                pre_days=180,
                post_days=180,
            ),
            id="2027",
        ),
    ],
)
def test_default_window_is_180_calendar_days_each_side(
    event_date: date, expected: CycleWindow
) -> None:
    """With no window field, T-180 .. T+180 calendar days: 2022-08-09 gives 2022-02-10 ..
    2023-02-05 and 2027-08-10 gives 2027-02-11 .. 2028-02-06; the month fields stay unset."""
    assert resolve_window(event_date) == expected


@pytest.mark.parametrize(
    ("pre_months", "post_months", "start", "end"),
    [
        pytest.param(12, 12, date(2026, 8, 10), date(2028, 8, 10), id="12-12"),
        pytest.param(6, None, date(2027, 2, 10), date(2028, 8, 10), id="post-defaults-to-12"),
        pytest.param(None, 3, date(2026, 8, 10), date(2027, 11, 10), id="pre-defaults-to-12"),
    ],
)
def test_months_window_uses_calendar_month_arithmetic(
    pre_months: int | None, post_months: int | None, start: date, end: date
) -> None:
    """The months form around 2027-08-10: ±12 months is 2026-08-10 .. 2028-08-10, and a missing
    side defaults to 12 months. The day fields stay unset."""
    window = resolve_window(date(2027, 8, 10), pre_months=pre_months, post_months=post_months)
    assert window == CycleWindow(
        event_date=date(2027, 8, 10),
        window_start=start,
        window_end=end,
        pre_months=12 if pre_months is None else pre_months,
        post_months=12 if post_months is None else post_months,
    )


@pytest.mark.parametrize(
    ("start", "months", "expected"),
    [
        pytest.param(date(2024, 2, 29), 12, date(2025, 2, 28), id="leap-day-plus-12"),
        pytest.param(date(2024, 1, 31), 1, date(2024, 2, 29), id="jan-31-plus-1-leap"),
        pytest.param(date(2023, 1, 31), 1, date(2023, 2, 28), id="jan-31-plus-1"),
        pytest.param(date(2024, 3, 31), -1, date(2024, 2, 29), id="negative-clamps"),
        pytest.param(date(2027, 8, 10), -12, date(2026, 8, 10), id="minus-12"),
        pytest.param(date(2024, 12, 31), 2, date(2025, 2, 28), id="year-rollover"),
        pytest.param(date(2024, 1, 15), -13, date(2022, 12, 15), id="year-rollback"),
    ],
)
def test_add_months_clamps_to_the_target_months_last_day(
    start: date, months: int, expected: date
) -> None:
    """Whole calendar months, the day clamped to the target month's length."""
    assert add_months(start, months) == expected


@pytest.mark.parametrize(
    ("fields", "field"),
    [
        pytest.param({"pre_days": 180, "pre_months": 12}, "pre_months", id="pre_days-pre_months"),
        pytest.param({"pre_months": 12, "post_days": 30}, "pre_months", id="pre_months-post_days"),
        pytest.param(
            {"post_days": 30, "post_months": 3}, "post_months", id="post_days-post_months"
        ),
        pytest.param({"pre_days": 30, "post_months": 3}, "post_months", id="pre_days-post_months"),
        pytest.param(
            {"pre_days": 180, "post_days": 180, "pre_months": 12, "post_months": 12},
            "pre_months",
            id="all-four",
        ),
    ],
)
def test_days_and_months_together_are_a_conflicting_window_violation(
    fields: dict[str, int], field: str
) -> None:
    """A day field with a month field mixes the two forms: a coded violation, not a window,
    reported against a month field the caller supplied, ``pre_months`` when it is set and
    ``post_months`` otherwise."""
    result = resolve_window(date(2027, 8, 10), **fields)
    assert isinstance(result, Violation)
    assert result.code == "conflicting_window_units"
    assert result.field == field
    assert result.message


@pytest.mark.parametrize(
    "fields",
    [
        pytest.param({"pre_days": 0}, id="pre_days-0"),
        pytest.param({"post_days": 366}, id="post_days-366"),
        pytest.param({"pre_months": 13}, id="pre_months-13"),
        pytest.param({"post_months": 0}, id="post_months-0"),
    ],
)
def test_out_of_range_window_fields_are_a_caller_error(fields: dict[str, int]) -> None:
    """Days are bounded to 1-365 and months to 1-12 before the API calls in."""
    with pytest.raises(ValueError, match="must be between 1 and"):
        resolve_window(date(2022, 8, 9), **fields)


@pytest.mark.parametrize(
    ("event_date", "fields", "requested"),
    [
        pytest.param(
            date(1, 1, 1), {}, "180 days before and 180 days after", id="days-default-at-date-min"
        ),
        pytest.param(
            date(1, 6, 29),
            {},
            "180 days before and 180 days after",
            id="days-default-179-days-after-date-min",
        ),
        pytest.param(
            date(9999, 12, 31),
            {},
            "180 days before and 180 days after",
            id="days-default-at-date-max",
        ),
        pytest.param(
            date(9999, 12, 1),
            {"post_days": 31},
            "180 days before and 31 days after",
            id="days-post-31-from-30-days-before-date-max",
        ),
        pytest.param(
            date(1, 6, 15),
            {"pre_months": 12},
            "12 months before and 12 months after",
            id="months-pre-12-into-year-0",
        ),
        pytest.param(
            date(1, 12, 31),
            {"pre_months": 12},
            "12 months before and 12 months after",
            id="months-pre-12-one-month-short",
        ),
        pytest.param(
            date(9999, 6, 15),
            {"post_months": 12},
            "12 months before and 12 months after",
            id="months-post-12-into-year-10000",
        ),
        pytest.param(
            date(9999, 1, 1),
            {"pre_months": 1, "post_months": 12},
            "1 month before and 12 months after",
            id="months-post-12-one-month-past",
        ),
    ],
)
def test_window_beyond_the_supported_dates_is_an_out_of_range_violation(
    event_date: date, fields: dict[str, int], requested: str
) -> None:
    """Valid field values whose window would start before 0001-01-01 or end after 9999-12-31: a
    coded violation against ``event_dates``, never a raw date-arithmetic exception.

    0001-06-29 is 179 days after 0001-01-01 (31 + 28 + 31 + 30 + 31 + 28), one short of 180;
    9999-12-01 is 30 days before 9999-12-31; 12 months before 0001-12-31 is 0000-12."""
    result = resolve_window(event_date, **fields)
    assert isinstance(result, Violation)
    assert result.code == "window_out_of_range"
    assert result.field == "event_dates"
    assert f"{requested} the event date {event_date.isoformat()}" in result.message
    assert "0001-01-01 to 9999-12-31" in result.message


@pytest.mark.parametrize(
    ("event_date", "fields", "expected"),
    [
        pytest.param(
            date(1, 6, 30),
            {},
            CycleWindow(
                date(1, 6, 30), date(1, 1, 1), date(1, 12, 27), pre_days=180, post_days=180
            ),
            id="days-default-starts-on-date-min",
        ),
        pytest.param(
            date(9999, 7, 4),
            {},
            CycleWindow(
                date(9999, 7, 4), date(9999, 1, 5), date(9999, 12, 31), pre_days=180, post_days=180
            ),
            id="days-default-ends-on-date-max",
        ),
        pytest.param(
            date(1, 6, 29),
            {"pre_days": 179, "post_days": 1},
            CycleWindow(date(1, 6, 29), date(1, 1, 1), date(1, 6, 30), pre_days=179, post_days=1),
            id="days-pre-179-starts-on-date-min",
        ),
        pytest.param(
            date(9999, 12, 1),
            {"pre_days": 1, "post_days": 30},
            CycleWindow(
                date(9999, 12, 1), date(9999, 11, 30), date(9999, 12, 31), pre_days=1, post_days=30
            ),
            id="days-post-30-ends-on-date-max",
        ),
        pytest.param(
            date(2, 1, 1),
            {"pre_months": 12},
            CycleWindow(date(2, 1, 1), date(1, 1, 1), date(3, 1, 1), pre_months=12, post_months=12),
            id="months-pre-12-starts-on-date-min",
        ),
        pytest.param(
            date(9998, 12, 31),
            {"post_months": 12},
            CycleWindow(
                date(9998, 12, 31),
                date(9997, 12, 31),
                date(9999, 12, 31),
                pre_months=12,
                post_months=12,
            ),
            id="months-post-12-ends-on-date-max",
        ),
    ],
)
def test_window_reaching_exactly_date_min_or_date_max_is_resolved(
    event_date: date, fields: dict[str, int], expected: CycleWindow
) -> None:
    """A window whose first or last day is exactly 0001-01-01 or 9999-12-31 is still resolved.

    0001-06-30 is day 181 of year 1, so 180 days earlier is 0001-01-01, and 180 days later is
    0001-12-27; 9999-07-04 plus 180 days is 9999-12-31 and minus 180 days is 9999-01-05."""
    assert resolve_window(event_date, **fields) == expected


def test_standard_window_runs_from_2022_07_30_to_2022_08_19() -> None:
    """±10 days around Tue 2022-08-09: Sat 2022-07-30 .. Fri 2022-08-19."""
    assert WINDOW == CycleWindow(
        event_date=date(2022, 8, 9),
        window_start=date(2022, 7, 30),
        window_end=date(2022, 8, 19),
        pre_days=10,
        post_days=10,
    )


def test_window_for_another_event_is_rejected() -> None:
    """A window resolved around a different event date is a caller error."""
    other = days_window(date(2017, 8, 8), 10, 10)
    with pytest.raises(ValueError, match="window is for 2017-08-08"):
        run_cycle_backtest(EVENT, other, pair(), SESSIONS, [], data_available_through=DATA_THROUGH)


# ---------------------------------------------------------------------------------------------
# Index path, cohort and common anchor
# ---------------------------------------------------------------------------------------------


def test_two_ticker_cycle_index_pe_turnover_and_summary() -> None:
    """X: 100 shares at 10.00 through the anchor, then 9.00; Y: 50 shares at 20.00, then 18.00.

    Capitalization is 1,000 + 1,000 = 2,000 at the anchor and 900 + 900 = 1,800 afterwards, so
    the price index is 100.00 then 90.00 (1,800 / 2,000). Index at T is 90.00: pre-event return
    -10.00%, post-event return 0.00% (90 / 90), max drawdown -10.00%. Both report FY2021 NI 100,
    so P/E is 2,000 / 200 = 10.00 at the anchor and 1,800 / 200 = 9.00 afterwards: P/E index
    90.00, and the floor (first 9.00, Tue 2022-08-02) compresses P/E by -10.00%. Turnover is
    2,000 a session, so MA20 and the turnover index stay at 100.00.
    """
    result = run_standard(
        pair(
            x_close=after(ANCHOR, "10.00", "9.00"),
            y_close=after(ANCHOR, "20.00", "18.00"),
            x_fundamentals=[fy(FY2021, "100")],
            y_fundamentals=[fy(FY2021, "100")],
        )
    )

    assert result.label == "2022 Cycle"
    assert result.event_date == date(2022, 8, 9)
    assert (result.window_start, result.window_end) == (date(2022, 7, 30), date(2022, 8, 19))
    assert result.status == "complete"
    assert result.anchor_date == ANCHOR
    assert result.days_until_window_start is None
    assert result.constituents == ["X", "Y"]
    assert result.excluded_constituents == []
    assert result.flags == []
    assert result.projection is None

    points = result.points
    assert [point.trade_date for point in points] == WINDOW_DAYS
    assert [point.offset_days for point in points] == OFFSETS
    assert column(points, "price_index", "index") == ["100.00"] + ["90.00"] * 14
    assert column(points, "pe_ratio", "ratio") == ["10.00"] + ["9.00"] * 14
    assert column(points, "pe_index", "index") == ["100.00"] + ["90.00"] * 14
    assert column(points, "turnover_index", "index") == ["100.00"] * 15
    assert all(point.cumulative_foreign_net_kes is None for point in points)
    assert all(point.inflation_yoy_pct is None for point in points)

    assert result.summary is not None
    assert q(result.summary.pre_event_return_pct, "pct") == "-10.00"
    assert q(result.summary.post_event_return_pct, "pct") == "0.00"
    assert q(result.summary.max_drawdown_pct, "pct") == "-10.00"
    assert q(result.summary.pe_compression_pct, "pct") == "-10.00"

    assert result.markers == CycleMarkersResult(
        valuation_floor=CycleMarkerResult(-7, date(2022, 8, 2), "pe_ratio", Decimal("9.00")),
        volume_exhaustion=CycleMarkerResult(-8, ANCHOR, "turnover_index", Decimal("100.00")),
    )


def test_price_index_weights_each_member_by_its_capitalization() -> None:
    """X: 100 shares at 10.00 through the anchor, then 12.00 (+20%); Y: 150 shares at 20.00
    through T, then 18.00 (-10%).

    The members enter with unequal capitalizations, 1,000 and 3,000 (4,000 at the anchor), so Y
    weighs three times as much as X: the index is 100 * (1,200 + 3,000) / 4,000 = 105.00 from Tue
    2022-08-02 through T and 100 * (1,200 + 2,700) / 4,000 = 97.50 after it. An equal-weight mean
    of the members' returns would give 100 * (1.20 + 1.00) / 2 = 110.00, then
    100 * (1.20 + 0.90) / 2 = 105.00. Index at T is 105.00: pre-event return +5.00% (not
    +10.00%), post-event return 97.50 / 105.00 - 1 = -7.14% (not -4.55%) and max drawdown -7.14%
    from the 105.00 peak. Neither member reports earnings, so P/E is null throughout, the
    valuation floor falls back to the lowest price index, 97.50 on Wed 2022-08-10 (not the
    anchor's 100.00), and P/E compression is null. Turnover is 2,000 a session, so the turnover
    index stays at 100.00.
    """
    result = run_standard(
        pair(
            x_close=after(ANCHOR, "10.00", "12.00"),
            y_close=after(EVENT.event_date, "20.00", "18.00"),
            shares=(100, 150),
        )
    )

    assert result.status == "complete"
    assert result.constituents == ["X", "Y"]
    assert result.excluded_constituents == []
    assert result.flags == ["pe_anchor_unavailable"]

    points = result.points
    assert [point.offset_days for point in points] == OFFSETS
    assert column(points, "price_index", "index") == ["100.00"] + ["105.00"] * 6 + ["97.50"] * 8
    assert column(points, "pe_ratio", "ratio") == [None] * 15
    assert column(points, "turnover_index", "index") == ["100.00"] * 15

    assert result.summary is not None
    assert q(result.summary.pre_event_return_pct, "pct") == "5.00"
    assert q(result.summary.post_event_return_pct, "pct") == "-7.14"
    assert q(result.summary.max_drawdown_pct, "pct") == "-7.14"
    assert result.summary.pe_compression_pct is None

    assert result.markers == CycleMarkersResult(
        valuation_floor=CycleMarkerResult(1, date(2022, 8, 10), "price_index", Decimal("97.50")),
        volume_exhaustion=CycleMarkerResult(-8, ANCHOR, "turnover_index", Decimal("100.00")),
    )


def test_counter_first_trading_after_the_anchor_is_excluded() -> None:
    """Z (1,000,000 shares at 50.00) first trades on 2022-08-02, the session after the anchor.

    It has no base close, so it is excluded rather than entering mid-cycle: the cohort stays
    X and Y (flat at 10.00 / 20.00) and the price index on 2022-08-02 is still 100.00 instead of
    jumping to 100 * 50,002,000 / 2,000.
    """
    late = candidate("Z", 1_000_000, prices("Z", SESSIONS[26:], "50.00"))
    result = run_standard([late, *pair()])

    assert result.constituents == ["X", "Y"]
    assert result.excluded_constituents == ["Z"]
    assert q(point_on(result, date(2022, 8, 2)).price_index, "index") == "100.00"
    assert column(result.points, "price_index", "index") == ["100.00"] * 15


@pytest.mark.parametrize(
    ("last_row", "included"),
    [
        pytest.param(date(2022, 7, 18), True, id="10-sessions-back-included"),
        pytest.param(date(2022, 7, 15), False, id="11-sessions-back-excluded"),
    ],
)
def test_base_close_must_lie_within_10_market_sessions_of_the_anchor(
    last_row: date, included: bool
) -> None:
    """W's latest actual close on or before the anchor (Mon 2022-08-01) is its last row.

    Counting market sessions back from the anchor, Mon 2022-07-18 is the 10th (inside the
    boundary: W joins the cohort) and Fri 2022-07-15 the 11th (outside: W is excluded).
    """
    stale = candidate("W", 10, prices("W", [day for day in SESSIONS if day <= last_row], "5.00"))
    result = run_standard([stale, *pair()])

    if included:
        assert result.constituents == ["W", "X", "Y"]
        assert result.excluded_constituents == []
    else:
        assert result.constituents == ["X", "Y"]
        assert result.excluded_constituents == ["W"]
    assert column(result.points, "price_index", "index") == ["100.00"] * 15


def test_missing_sessions_after_the_anchor_forward_fill_the_members_own_close() -> None:
    """X has no row on Wed 2022-08-03 and closes 12.00 from Thu 2022-08-04; Y is flat at 20.00.

    On 2022-08-03 X carries its last actual close (10.00) rather than dropping out of the
    capitalization, so the index stays 100.00 (not 50.00); from 2022-08-04 it is
    100 * (1,200 + 1,000) / 2,000 = 110.00. Points begin at the anchor, never before it.
    """
    x_rows = prices(
        "X", drop(SESSIONS, date(2022, 8, 3)), after(date(2022, 8, 3), "10.00", "12.00")
    )
    y_rows = prices("Y", SESSIONS, "20.00")
    result = run_standard([candidate("X", 100, x_rows), candidate("Y", 50, y_rows)])

    assert result.points[0].trade_date == ANCHOR
    assert result.points[0].trade_date == result.anchor_date
    assert [point.trade_date for point in result.points] == WINDOW_DAYS
    assert column(result.points, "price_index", "index") == ["100.00"] * 3 + ["110.00"] * 12


def test_base_close_is_the_latest_actual_close_before_an_untraded_anchor() -> None:
    """X has no row on the anchor; its base close is 10.00 from Fri 2022-07-29, and it closes
    11.00 from 2022-08-02. Y is flat at 20.00. The index is 100.00 at the anchor and
    100 * (1,100 + 1,000) / 2,000 = 105.00 afterwards."""
    x_rows = prices("X", drop(SESSIONS, ANCHOR), after(ANCHOR, "10.00", "11.00"))
    y_rows = prices("Y", SESSIONS, "20.00")
    result = run_standard([candidate("X", 100, x_rows), candidate("Y", 50, y_rows)])

    assert result.constituents == ["X", "Y"]
    assert column(result.points, "price_index", "index") == ["100.00"] + ["105.00"] * 14


@pytest.mark.parametrize(
    "u_fundamentals_currency",
    [
        pytest.param("USD", id="usd-listing-usd-fundamentals"),
        pytest.param("KES", id="usd-listing-kes-fundamentals"),
    ],
)
def test_candidate_listed_outside_kes_is_excluded_from_the_cohort(
    u_fundamentals_currency: str,
) -> None:
    """X: KES, 100 shares at 10.00 (cap 1,000), FY2021 NI 100. U: listed in USD, 50 shares at
    20.00 (cap 1,000 USD) and 40.00 after the anchor, turnover 5,000 after the anchor and a
    foreign flow of 500 every session, FY2021 NI 10. Z (KES) first trades after the anchor.

    Summing U with X would give P/E 2,000 / 110 = 18.18 at the anchor and a price index of
    100 * 3,000 / 2,000 = 150.00 afterwards. U is excluded instead, whatever currency its
    fundamentals carry, and listed with Z in the sorted excluded tickers: P/E is X's
    1,000 / 100 = 10.00, the price and turnover indices stay 100.00, none of U's flow enters, and
    the cycle is flagged currency_mismatch.
    """
    x = candidate("X", 100, prices("X", SESSIONS, "10.00"), [fy(FY2021, "100")])
    u = candidate(
        "U",
        50,
        prices(
            "U",
            SESSIONS,
            after(ANCHOR, "20.00", "40.00"),
            turnover=after(ANCHOR, "1000", "5000"),
            flow="500",
        ),
        [fy(FY2021, "10", currency=u_fundamentals_currency)],
        currency="USD",
    )
    late = candidate("Z", 1_000_000, prices("Z", SESSIONS[26:], "50.00"))

    result = run_standard([late, u, x])

    assert result.status == "complete"
    assert result.constituents == ["X"]
    assert result.excluded_constituents == ["U", "Z"]
    assert result.flags == ["currency_mismatch"]
    assert column(result.points, "pe_ratio", "ratio") == ["10.00"] * 15
    assert column(result.points, "price_index", "index") == ["100.00"] * 15
    assert column(result.points, "turnover_index", "index") == ["100.00"] * 15
    assert all(point.cumulative_foreign_net_kes is None for point in result.points)


@pytest.mark.parametrize(
    ("x_fundamentals", "y_fundamentals", "expected_pe", "expected_flags"),
    [
        pytest.param([fy(FY2021, "100")], [], "10.00", [], id="y-without-fundamentals"),
        pytest.param([fy(FY2021, "100")], [fy(FY2021, None)], "10.00", [], id="y-without-ni"),
        pytest.param(
            [fy(FY2021, "300")], [fy(FY2021, "-100")], "10.00", [], id="loss-in-matched-set"
        ),
        pytest.param(
            [fy(FY2021, "100", currency="USD")],
            [fy(FY2021, "50")],
            "20.00",
            ["currency_mismatch"],
            id="x-in-usd",
        ),
        pytest.param(
            [fy(FY2021, "100", period_type="H1")],
            [fy(FY2021, "40")],
            "25.00",
            [],
            id="x-half-year",
        ),
    ],
)
def test_pe_uses_a_matched_set_of_members_with_available_earnings(
    x_fundamentals: list[FundamentalsRow],
    y_fundamentals: list[FundamentalsRow],
    expected_pe: str,
    expected_flags: list[str],
) -> None:
    """X and Y are flat with 1,000 KES of capitalization each; P/E sums cap and NI over E_t only.

    * Y without fundamentals or without NI: X alone, 1,000 / 100 = 10.00 (not 2,000 / 100).
    * X earns 300 and Y loses 100: both in E_t, 2,000 / 200 = 10.00.
    * X reports in USD against a KES listing: its row is refused, not converted, so Y alone gives
      1,000 / 50 = 20.00 and the cycle is flagged currency_mismatch.
    * X has only a half-year row: Y alone, 1,000 / 40 = 25.00.
    """
    result = run_standard(pair(x_fundamentals=x_fundamentals, y_fundamentals=y_fundamentals))

    assert column(result.points, "pe_ratio", "ratio") == [expected_pe] * 15
    assert column(result.points, "pe_index", "index") == ["100.00"] * 15
    assert result.flags == expected_flags


@pytest.mark.parametrize(
    ("lag_days", "expected_pe"),
    [
        pytest.param(90, ["10.00"] * 15, id="default-90-days"),
        pytest.param(40, ["10.00"] * 6 + ["1.00"] * 9, id="40-days"),
    ],
)
def test_fundamentals_lag_applies_inside_the_backtest(
    lag_days: int, expected_pe: list[str]
) -> None:
    """X (cap 1,000) reports FY2021 NI 100 and FY to 2022-06-30 NI 1,000; Y has none.

    Under the 90-day lag the newer year is available only from 2022-09-28, after the window, so
    P/E stays 1,000 / 100 = 10.00. Under a 40-day lag it is available from 2022-08-09 (T), so
    P/E is 10.00 through 2022-08-08 and 1,000 / 1,000 = 1.00 from T.
    """
    x_fundamentals = [fy(FY2021, "100"), fy(date(2022, 6, 30), "1000")]
    result = run_standard(pair(x_fundamentals=x_fundamentals), lag_days=lag_days)

    assert column(result.points, "pe_ratio", "ratio") == expected_pe
    assert q(point_on(result, date(2022, 8, 9)).pe_ratio, "ratio") == expected_pe[6]


def test_cumulative_foreign_flow_runs_from_the_anchor_and_skips_null_days() -> None:
    """X nets +100 KES of foreign buying on every row, including the 25 pre-window sessions,
    except Wed 2022-08-03 (no flow data); Y has no flow data at all.

    The running sum starts at the anchor (100.00, not 2,600.00), adds 100 a session and nothing
    on 2022-08-03: 100, 200, 200, 300, ... 1,400.00 on 2022-08-19.
    """
    x_rows = prices(
        "X", SESSIONS, "10.00", flow=lambda day: None if day == date(2022, 8, 3) else "100"
    )
    y_rows = prices("Y", SESSIONS, "20.00")
    result = run_standard([candidate("X", 100, x_rows), candidate("Y", 50, y_rows)])

    cumulative = [point.cumulative_foreign_net_kes for point in result.points]
    assert all(isinstance(value, Decimal) for value in cumulative)
    assert column(result.points, "cumulative_foreign_net_kes", "money") == [
        "100.00",
        "200.00",
        "200.00",
        "300.00",
        "400.00",
        "500.00",
        "600.00",
        "700.00",
        "800.00",
        "900.00",
        "1000.00",
        "1100.00",
        "1200.00",
        "1300.00",
        "1400.00",
    ]


def test_cumulative_foreign_flow_is_null_until_the_first_flow() -> None:
    """No flow on the anchor; from 2022-08-02 X nets +100 and Y -40 a session (+60 combined):
    the running sum is None at the anchor, then 60.00, 120.00, 180.00, ..."""
    x_rows = prices("X", SESSIONS, "10.00", flow=lambda day: "100" if day > ANCHOR else None)
    y_rows = prices("Y", SESSIONS, "20.00", flow=lambda day: "-40" if day > ANCHOR else None)
    result = run_standard([candidate("X", 100, x_rows), candidate("Y", 50, y_rows)])

    assert column(result.points, "cumulative_foreign_net_kes", "money")[:4] == [
        None,
        "60.00",
        "120.00",
        "180.00",
    ]
    assert q(result.points[-1].cumulative_foreign_net_kes, "money") == "840.00"


# ---------------------------------------------------------------------------------------------
# Undefined normalizations
# ---------------------------------------------------------------------------------------------


def zero_capitalization_cycle() -> CycleBacktestResult:
    """X and Y hold 0 shares and report no fundamentals: anchor capitalization is 0."""
    return run_standard(
        pair(
            x_close=after(ANCHOR, "10.00", "9.00"),
            y_close=after(ANCHOR, "20.00", "18.00"),
            shares=(0, 0),
        )
    )


def short_turnover_history_cycle() -> CycleBacktestResult:
    """Sessions and rows begin Wed 2022-07-27, 3 sessions before the anchor: MA20 at the anchor
    has 4 observations, below the 5 it needs."""
    sessions = weekdays(date(2022, 7, 27), DATA_THROUGH)
    fundamentals = [fy(FY2021, "100")]
    return run_standard(
        pair(x_fundamentals=fundamentals, y_fundamentals=fundamentals, sessions=sessions),
        sessions=sessions,
    )


def zero_turnover_cycle() -> CycleBacktestResult:
    """Turnover is 0 on every row up to and including the anchor and 1,000 afterwards."""
    fundamentals = [fy(FY2021, "100")]
    return run_standard(
        pair(
            x_fundamentals=fundamentals,
            y_fundamentals=fundamentals,
            turnover=after(ANCHOR, "0", "1000"),
        )
    )


def loss_making_cycle() -> CycleBacktestResult:
    """Both members lose 100 in FY2021, so the matched set's NI is -200 <= 0."""
    fundamentals = [fy(FY2021, "-100")]
    return run_standard(
        pair(
            x_close=after(ANCHOR, "10.00", "9.00"),
            y_close=after(ANCHOR, "20.00", "18.00"),
            x_fundamentals=fundamentals,
            y_fundamentals=fundamentals,
        )
    )


@pytest.mark.parametrize(
    ("build", "flags", "null_fields"),
    [
        pytest.param(
            zero_capitalization_cycle,
            ["zero_anchor_capitalization", "pe_anchor_unavailable"],
            ("price_index", "pe_ratio", "pe_index"),
            id="zero_anchor_capitalization",
        ),
        pytest.param(
            short_turnover_history_cycle,
            ["insufficient_turnover_history"],
            ("turnover_index",),
            id="insufficient_turnover_history",
        ),
        pytest.param(
            zero_turnover_cycle,
            ["zero_anchor_turnover"],
            ("turnover_index",),
            id="zero_anchor_turnover",
        ),
        pytest.param(
            loss_making_cycle,
            ["pe_anchor_unavailable"],
            ("pe_ratio", "pe_index"),
            id="pe_anchor_unavailable",
        ),
    ],
)
def test_undefined_normalization_is_null_throughout_and_flagged(
    build: Callable[[], CycleBacktestResult], flags: list[str], null_fields: tuple[str, ...]
) -> None:
    """Each undefined normalization nulls its series on every point and names its reason code;
    the cycle itself is still returned with all 15 points."""
    result = build()

    assert result.status == "complete"
    assert result.flags == flags
    assert len(result.points) == 15
    for name in null_fields:
        assert [getattr(point, name) for point in result.points] == [None] * 15


def test_zero_anchor_capitalization_nulls_price_summary_and_floor() -> None:
    """0 shares make every capitalization 0: no index, no returns, no drawdown, and with P/E
    also null there is no valuation floor. No division by zero is raised. Turnover is
    unaffected, so the volume exhaustion marker is the first 100.00 point (the anchor)."""
    result = zero_capitalization_cycle()

    assert result.summary is not None
    assert result.summary.pre_event_return_pct is None
    assert result.summary.post_event_return_pct is None
    assert result.summary.max_drawdown_pct is None
    assert result.summary.pe_compression_pct is None
    assert result.markers is not None
    assert result.markers.valuation_floor is None
    assert result.markers.volume_exhaustion == CycleMarkerResult(
        -8, ANCHOR, "turnover_index", Decimal("100.00")
    )


@pytest.mark.parametrize(
    ("first_session", "turnover_defined"),
    [
        pytest.param(date(2022, 7, 26), True, id="5-observations"),
        pytest.param(date(2022, 7, 27), False, id="4-observations"),
    ],
)
def test_turnover_index_needs_5_ma20_observations_at_the_anchor(
    first_session: date, turnover_defined: bool
) -> None:
    """History from Tue 2022-07-26 gives MA20 5 observations at the anchor (index 100.00);
    from Wed 2022-07-27 only 4, so the index is null and there is no volume exhaustion."""
    sessions = weekdays(first_session, DATA_THROUGH)
    result = run_standard(pair(sessions=sessions), sessions=sessions)

    assert result.markers is not None
    if turnover_defined:
        assert column(result.points, "turnover_index", "index") == ["100.00"] * 15
        assert "insufficient_turnover_history" not in result.flags
        assert result.markers.volume_exhaustion is not None
    else:
        assert column(result.points, "turnover_index", "index") == [None] * 15
        assert "insufficient_turnover_history" in result.flags
        assert result.markers.volume_exhaustion is None


def test_missing_turnover_sessions_are_skipped_not_zero_filled() -> None:
    """All 25 pre-anchor sessions exist, but the cohort's rows begin Wed 2022-07-27. The 21
    sessions without a row are not counted as zero turnover: MA20 still has only 4
    observations, so the turnover index is null with ``insufficient_turnover_history``."""
    rows_from = weekdays(date(2022, 7, 27), DATA_THROUGH)
    result = run_standard(pair(sessions=rows_from))

    assert "insufficient_turnover_history" in result.flags
    assert column(result.points, "turnover_index", "index") == [None] * 15


def test_zero_anchor_turnover_leaves_no_volume_exhaustion() -> None:
    """MA20 at the anchor is the mean of 20 zero-turnover sessions, 0: the index would divide by
    zero, so it is null on every point (even once turnover resumes) and no marker is set."""
    result = zero_turnover_cycle()

    assert result.markers is not None
    assert result.markers.volume_exhaustion is None
    assert result.markers.valuation_floor is not None


def test_non_positive_earnings_fall_back_to_the_price_index_floor() -> None:
    """With Σ NI = -200 the matched P/E is null throughout, so P/E compression is null and the
    valuation floor falls back to the price index: the first 90.00 point, Tue 2022-08-02."""
    result = loss_making_cycle()

    assert result.summary is not None
    assert result.summary.pe_compression_pct is None
    assert q(result.summary.max_drawdown_pct, "pct") == "-10.00"
    assert result.markers is not None
    assert result.markers.valuation_floor == CycleMarkerResult(
        -7, date(2022, 8, 2), "price_index", Decimal("90.00")
    )


@pytest.mark.parametrize(
    "net_income",
    [
        pytest.param(("0", "0"), id="zero-earnings"),
        pytest.param(("-100", "-100"), id="negative-earnings"),
        pytest.param(("100", "-100"), id="earnings-net-to-zero"),
    ],
)
def test_zero_or_negative_matched_earnings_make_pe_unavailable(
    net_income: tuple[str, str],
) -> None:
    """Σ NI of the matched set is 0, -200 or 100 - 100 = 0: P/E is undefined, never 0 or
    negative, and the anchor P/E flag is raised."""
    result = run_standard(
        pair(
            x_fundamentals=[fy(FY2021, net_income[0])],
            y_fundamentals=[fy(FY2021, net_income[1])],
        )
    )

    assert result.flags == ["pe_anchor_unavailable"]
    assert column(result.points, "pe_ratio", "ratio") == [None] * 15


def test_pe_that_becomes_available_after_the_anchor_has_no_index() -> None:
    """X's only year ends 2022-05-10, available from 2022-08-08 under the 90-day lag; Y has no
    fundamentals. The anchor P/E is null (``pe_anchor_unavailable``), so ``pe_index`` and P/E
    compression are null, but ``pe_ratio`` is still returned where defined: 1,000 / 100 = 10.00
    from Mon 2022-08-08, which is also the valuation floor."""
    result = run_standard(pair(x_fundamentals=[fy(date(2022, 5, 10), "100")]))

    assert result.flags == ["pe_anchor_unavailable"]
    assert column(result.points, "pe_ratio", "ratio") == [None] * 5 + ["10.00"] * 10
    assert column(result.points, "pe_index", "index") == [None] * 15
    assert result.summary is not None
    assert result.summary.pe_compression_pct is None
    assert result.markers is not None
    assert result.markers.valuation_floor == CycleMarkerResult(
        -1, date(2022, 8, 8), "pe_ratio", Decimal("10.00")
    )


def test_no_pre_event_data_nulls_the_event_returns() -> None:
    """Window ±10/+20 days (2022-07-30 .. 2022-08-29), but sessions and rows begin Wed
    2022-08-10, after T. The anchor is 2022-08-10 (offset +1), every point lies after T, and with
    no index at T both event returns are null. Flat closes keep the drawdown at 0.00."""
    window = days_window(EVENT.event_date, 10, 20)
    sessions = weekdays(date(2022, 8, 10), date(2022, 8, 29))
    fundamentals = [fy(FY2021, "100")]
    result = run_cycle_backtest(
        EVENT,
        window,
        pair(x_fundamentals=fundamentals, y_fundamentals=fundamentals, sessions=sessions),
        sessions,
        [],
        data_available_through=date(2022, 8, 29),
    )

    assert result.status == "complete"
    assert result.anchor_date == date(2022, 8, 10)
    assert result.flags == ["insufficient_turnover_history", "no_pre_event_data"]
    assert result.points[0].offset_days == 1
    assert all(point.offset_days > 0 for point in result.points)
    assert result.summary is not None
    assert result.summary.pre_event_return_pct is None
    assert result.summary.post_event_return_pct is None
    assert q(result.summary.max_drawdown_pct, "pct") == "0.00"


@pytest.mark.parametrize(
    ("candidates", "expected_flags"),
    [
        pytest.param(pair(sessions=SESSIONS[26:]), ["empty_cohort"], id="no-base-close"),
        pytest.param(
            [
                candidate("X", 100, prices("X", SESSIONS, "10.00"), currency="USD"),
                candidate("Y", 50, prices("Y", SESSIONS, "20.00"), currency="USD"),
            ],
            ["currency_mismatch", "empty_cohort"],
            id="every-candidate-listed-in-usd",
        ),
    ],
)
def test_empty_cohort_with_data_in_the_window_is_no_constituents(
    candidates: list[BacktestCandidate], expected_flags: list[str]
) -> None:
    """Either X and Y both first trade on 2022-08-02, after the anchor, so neither has a base
    close; or both are listed in USD, so neither may join a KES cohort. The cohort is empty: the
    cycle keeps its anchor and excluded tickers but has no points, markers or summary."""
    result = run_standard(candidates)

    assert result.status == "no_constituents"
    assert result.flags == expected_flags
    assert result.anchor_date == ANCHOR
    assert result.constituents == []
    assert result.excluded_constituents == ["X", "Y"]
    assert result.points == []
    assert result.markers is None
    assert result.summary is None
    assert result.projection is None


# ---------------------------------------------------------------------------------------------
# Markers and overlays
# ---------------------------------------------------------------------------------------------

DIP_CLOSE = piecewise(
    "10.00",
    {
        date(2022, 8, 2): "9.00",
        date(2022, 8, 3): "9.50",
        date(2022, 8, 4): "9.00",
        date(2022, 8, 5): "9.50",
    },
)
"""One ticker, 100 shares: price index 100 -> 90 -> 95 -> 90 -> 95 (flat to the window end)."""

DIP_TURNOVER = piecewise("1000", {date(2022, 8, 2): "0", date(2022, 8, 8): "1000"})
"""1,000 a session, except four zero-turnover rows from Tue 2022-08-02 to Fri 2022-08-05."""


@pytest.mark.parametrize(
    ("fundamentals", "expected_floor"),
    [
        pytest.param(
            [],
            CycleMarkerResult(-7, date(2022, 8, 2), "price_index", Decimal("90.00")),
            id="price-fallback",
        ),
        pytest.param(
            [fy(FY2021, "100")],
            CycleMarkerResult(-7, date(2022, 8, 2), "pe_ratio", Decimal("9.00")),
            id="pe-ratio",
        ),
    ],
)
def test_markers_are_the_earliest_minima_of_the_returned_points(
    fundamentals: list[FundamentalsRow], expected_floor: CycleMarkerResult
) -> None:
    """Closes 10.00, 9.00, 9.50, 9.00, then 9.50 give a price index of 100, 90, 95, 90, 95; with
    NI 100 the P/E path is 10.00, 9.00, 9.50, 9.00, 9.50. Either way the minimum is tied on
    2022-08-02 and 2022-08-04 and the earlier point is the valuation floor.

    Turnover 1,000 a session with zero-turnover rows on 2022-08-02 .. 2022-08-05 takes MA20 from
    1,000 at the anchor to (16 * 1,000 + 4 * 0) / 20 = 800 on 2022-08-05, where it stays while
    the zeros remain in the 20-session span: volume exhaustion is the first 80.00 point,
    2022-08-05 (offset -4).
    """
    rows = prices("X", SESSIONS, DIP_CLOSE, turnover=DIP_TURNOVER)
    result = run_standard([candidate("X", 100, rows, fundamentals)])

    assert column(result.points, "price_index", "index")[:5] == [
        "100.00",
        "90.00",
        "95.00",
        "90.00",
        "95.00",
    ]
    assert column(result.points, "turnover_index", "index") == [
        "100.00",
        "95.00",
        "90.00",
        "85.00",
        *["80.00"] * 11,
    ]
    assert result.markers is not None
    assert result.markers.valuation_floor == expected_floor
    assert result.markers.volume_exhaustion == CycleMarkerResult(
        -4, date(2022, 8, 5), "turnover_index", Decimal("80.00")
    )
    assert_earliest_minimum(result.points, result.markers.valuation_floor, expected_floor.metric)
    assert_earliest_minimum(result.points, result.markers.volume_exhaustion, "turnover_index")

    assert result.summary is not None
    assert q(result.summary.max_drawdown_pct, "pct") == "-10.00"
    assert q(result.summary.pre_event_return_pct, "pct") == "-5.00"
    assert q(result.summary.post_event_return_pct, "pct") == "0.00"
    expected_compression = "-10.00" if expected_floor.metric == "pe_ratio" else None
    assert q(result.summary.pe_compression_pct, "pct") == expected_compression


@pytest.mark.parametrize(
    ("close", "expected_index", "post_event", "max_drawdown", "expected_floor"),
    [
        pytest.param(
            piecewise(
                "10.00",
                {
                    date(2022, 8, 2): "12.00",
                    date(2022, 8, 10): "15.00",
                    date(2022, 8, 19): "9.00",
                },
            ),
            ["100.00"] + ["120.00"] * 6 + ["150.00"] * 7 + ["90.00"],
            "-25.00",
            "-40.00",
            CycleMarkerResult(10, date(2022, 8, 19), "price_index", Decimal("90.00")),
            id="new-peak-after-t-then-a-fall",
        ),
        pytest.param(
            piecewise(
                "10.00",
                {
                    date(2022, 8, 2): "12.00",
                    date(2022, 8, 10): "9.00",
                    date(2022, 8, 15): "15.00",
                    date(2022, 8, 19): "13.50",
                },
            ),
            ["100.00"] + ["120.00"] * 6 + ["90.00"] * 3 + ["150.00"] * 4 + ["135.00"],
            "12.50",
            "-25.00",
            CycleMarkerResult(1, date(2022, 8, 10), "price_index", Decimal("90.00")),
            id="trough-before-a-higher-peak",
        ),
    ],
)
def test_summary_measures_returns_from_the_index_at_t_and_drawdown_from_the_running_peak(
    close: Callable[[date], str],
    expected_index: list[str],
    post_event: str,
    max_drawdown: str,
    expected_floor: CycleMarkerResult,
) -> None:
    """One ticker X (100 shares, no fundamentals) closes 10.00 at the anchor and 12.00 from Tue
    2022-08-02 through T (Tue 2022-08-09), so the index at T is 100 * 1,200 / 1,000 = 120.00 and
    the pre-event return is (120 / 100 - 1) * 100 = +20.00% on both paths.

    * New peak after T, then a fall: 15.00 from Wed 2022-08-10 through Thu 2022-08-18 and 9.00 on
      Fri 2022-08-19 give 100, then 120 (6 points), 150 (7 points) and 90. Post-event return
      (90 / 120 - 1) * 100 = -25.00%; max drawdown from the running maximum 150 is
      (90 / 150 - 1) * 100 = -40.00%, not the anchor-relative (90 / 100 - 1) * 100 = -10.00%.
    * Trough before a higher peak: 9.00 from Wed 2022-08-10, 15.00 from Mon 2022-08-15 and 13.50
      on Fri 2022-08-19 give 100, then 120 (6 points), 90 (3 points), 150 (4 points) and 135.
      Post-event return (135 / 120 - 1) * 100 = +12.50%; max drawdown is the fall to 90 from the
      running maximum 120 then, (90 / 120 - 1) * 100 = -25.00%, not the global range
      (90 / 150 - 1) * 100 = -40.00%, the anchor-relative -10.00% or the last point against its
      running maximum, (135 / 150 - 1) * 100 = -10.00%.

    P/E is null throughout, so the valuation floor falls back to the earliest minimum price index
    (90.00) and P/E compression is null.
    """
    result = run_standard([candidate("X", 100, prices("X", SESSIONS, close))])

    assert result.status == "complete"
    assert result.flags == ["pe_anchor_unavailable"]
    assert [point.trade_date for point in result.points] == WINDOW_DAYS
    assert column(result.points, "price_index", "index") == expected_index

    assert result.summary is not None
    assert q(result.summary.pre_event_return_pct, "pct") == "20.00"
    assert q(result.summary.post_event_return_pct, "pct") == post_event
    assert q(result.summary.max_drawdown_pct, "pct") == max_drawdown
    assert result.summary.pe_compression_pct is None
    assert result.markers is not None
    assert result.markers.valuation_floor == expected_floor


def test_macro_overlays_are_as_of_and_at_most_62_days_old() -> None:
    """The only macro row is 2022-05-31 (inflation 7.10, 91-day 7.90, 364-day 9.80). At the
    anchor, Mon 2022-08-01, it is 62 days old and every overlay carries it; from Tue 2022-08-02
    (63 days) the overlays are null. Overlays never feed the markers or summary."""
    macro = [
        MacroRow(
            observation_date=date(2022, 5, 31),
            inflation_yoy_pct=Decimal("7.10"),
            tbill_91d_pct=Decimal("7.90"),
            tbill_364d_pct=Decimal("9.80"),
        )
    ]
    result = run_standard(pair(), macro_rows=macro)

    anchor = result.points[0]
    assert (anchor.inflation_yoy_pct, anchor.tbill_91d_pct, anchor.tbill_364d_pct) == (
        Decimal("7.10"),
        Decimal("7.90"),
        Decimal("9.80"),
    )
    for point in result.points[1:]:
        assert point.inflation_yoy_pct is None
        assert point.tbill_91d_pct is None
        assert point.tbill_364d_pct is None
    assert result.markers == run_standard(pair()).markers
    assert result.summary == run_standard(pair()).summary


def test_macro_overlay_takes_each_columns_latest_non_null_value() -> None:
    """Rows 2022-06-30 (inflation 7.90, bills 8.00 / 9.90) and 2022-07-31 (inflation 8.30 only):
    on the anchor, inflation is 8.30 from July while both T-bill overlays stay on June's
    values, since the newer row carries no bill yields."""
    macro = [
        MacroRow(
            observation_date=date(2022, 6, 30),
            inflation_yoy_pct=Decimal("7.90"),
            tbill_91d_pct=Decimal("8.00"),
            tbill_364d_pct=Decimal("9.90"),
        ),
        MacroRow(observation_date=date(2022, 7, 31), inflation_yoy_pct=Decimal("8.30")),
    ]
    anchor = run_standard(pair(), macro_rows=macro).points[0]

    assert anchor.inflation_yoy_pct == Decimal("8.30")
    assert anchor.tbill_91d_pct == Decimal("8.00")
    assert anchor.tbill_364d_pct == Decimal("9.90")


MACRO_ROWS = [
    MacroRow(observation_date=date(2022, 5, 31), tbill_91d_pct=Decimal("7.90")),
    MacroRow(observation_date=date(2022, 6, 30), fx_reserves_usd_m=Decimal("7850.00")),
    MacroRow(observation_date=date(2022, 9, 30), tbill_91d_pct=Decimal("8.10")),
]
"""A bill yield on 2022-05-31, a reserves-only row on 2022-06-30 and a newer yield on 09-30."""


@pytest.mark.parametrize(
    ("day", "max_staleness_days", "expected"),
    [
        pytest.param(
            date(2022, 7, 15), 62, AsOfValue(Decimal("7.90"), date(2022, 5, 31)), id="skips-null"
        ),
        pytest.param(
            date(2022, 8, 1), 62, AsOfValue(Decimal("7.90"), date(2022, 5, 31)), id="62-days"
        ),
        pytest.param(date(2022, 8, 2), 62, None, id="63-days"),
        pytest.param(
            date(2022, 9, 29), None, AsOfValue(Decimal("7.90"), date(2022, 5, 31)), id="no-limit"
        ),
        pytest.param(
            date(2022, 9, 30), 62, AsOfValue(Decimal("8.10"), date(2022, 9, 30)), id="same-day"
        ),
        pytest.param(date(2022, 5, 30), 62, None, id="before-first-row"),
    ],
)
def test_macro_as_of_resolves_the_latest_non_null_value_within_the_staleness_limit(
    day: date, max_staleness_days: int | None, expected: AsOfValue | None
) -> None:
    """The 2022-06-30 reserves-only row never hides the older bill yield; the 2022-09-30 row is
    ignored before its date; 62 days old is accepted and 63 is not, unless the limit is
    ``None``."""
    assert macro_as_of(MACRO_ROWS, "tbill_91d_pct", day, max_staleness_days) == expected


def test_macro_as_of_defaults_to_a_62_day_limit_and_handles_empty_columns() -> None:
    """Without an explicit limit 2022-08-02 (63 days after 2022-05-31) resolves to None; a column
    no row carries, or an empty row list, resolves to None too."""
    assert macro_as_of(MACRO_ROWS, "tbill_91d_pct", date(2022, 8, 1)) == AsOfValue(
        Decimal("7.90"), date(2022, 5, 31)
    )
    assert macro_as_of(MACRO_ROWS, "tbill_91d_pct", date(2022, 8, 2)) is None
    assert macro_as_of(MACRO_ROWS, "fx_kes_usd", date(2022, 7, 15)) is None
    assert macro_as_of(MACRO_ROWS, "fx_kes_usd", date(2022, 7, 15), None) is None
    assert macro_as_of([], "inflation_yoy_pct", date(2022, 7, 15)) is None


def test_macro_as_of_rejects_unknown_columns_and_negative_limits() -> None:
    """Both are caller errors."""
    with pytest.raises(ValueError, match="unknown macro column"):
        macro_as_of(MACRO_ROWS, cast(MacroColumn, "gdp_growth_pct"), date(2022, 7, 15))
    with pytest.raises(ValueError, match="max_staleness_days must be >= 0"):
        macro_as_of(MACRO_ROWS, "tbill_91d_pct", date(2022, 7, 15), -1)


# ---------------------------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------------------------


def complete_cycle(
    event: CycleEvent, sessions: Sequence[date], close_after_event: str
) -> CycleBacktestResult:
    """One ticker (100 shares) closing 10.00 through T and ``close_after_event`` from T+1, over a
    T-10 .. T+40 day window with data past the window end."""
    rows = prices("X", sessions, after(event.event_date, "10.00", close_after_event))
    result = run_cycle_backtest(
        event,
        days_window(event.event_date, 10, 40),
        [candidate("X", 100, rows)],
        sessions,
        [],
        data_available_through=sessions[-1],
    )
    assert result.status == "complete"
    return result


def cycle_2013() -> CycleBacktestResult:
    """2013-03-04 (window 2013-02-22 .. 2013-04-13): price index 110.00 from offset +1."""
    return complete_cycle(
        CycleEvent("2013 Cycle", date(2013, 3, 4)),
        weekdays(date(2013, 1, 21), date(2013, 4, 15)),
        "11.00",
    )


def cycle_2017() -> CycleBacktestResult:
    """2017-08-08 (window 2017-07-29 .. 2017-09-17): price index 90.00 from offset +1."""
    return complete_cycle(
        CycleEvent("2017 Cycle", date(2017, 8, 8)),
        weekdays(date(2017, 6, 26), date(2017, 9, 18)),
        "9.00",
    )


FUTURE_EVENT = CycleEvent("2027 Election Watch", date(2027, 8, 10))
FUTURE_SESSIONS = weekdays(date(2026, 6, 1), date(2026, 9, 30))
FUTURE_DATA_THROUGH = date(2026, 9, 30)


def future_cycle(window: CycleWindow) -> CycleBacktestResult:
    """The 2027 election with data through 2026-09-30: X (100 shares) closes 10.00 through
    2026-08-10 and 12.00 afterwards; Y (50 shares) is flat at 20.00; neither has fundamentals.
    Capitalization goes from 2,000 to 2,200, a price index of 100.00 then 110.00."""
    x_close = after(date(2026, 8, 10), "10.00", "12.00")
    candidates = [
        candidate("X", 100, prices("X", FUTURE_SESSIONS, x_close)),
        candidate("Y", 50, prices("Y", FUTURE_SESSIONS, "20.00")),
    ]
    return run_cycle_backtest(
        FUTURE_EVENT,
        window,
        candidates,
        FUTURE_SESSIONS,
        [],
        data_available_through=FUTURE_DATA_THROUGH,
    )


def projection_at(projection: CycleProjection, offset: int) -> ProjectionPoint:
    matches = [point for point in projection.points if point.offset_days == offset]
    assert len(matches) == 1
    return matches[0]


def test_projection_averages_the_complete_cycles_at_each_offset() -> None:
    """2013's price index is 110.00 and 2017's 90.00 from offset +1; at offset 30 (2013-04-03
    and 2017-09-07, both sessions) the projection is (110 + 90) / 2 = 100.00. Turnover is flat
    in both (100.00) and neither has P/E, so the projected P/E index is null.

    The 2027 target (same T-10 .. T+40 window) is awaiting data: the projection is attached,
    labelled as such, and never mixed into its observed ``points``.
    """
    target = run_cycle_backtest(
        FUTURE_EVENT,
        days_window(FUTURE_EVENT.event_date, 10, 40),
        [],
        FUTURE_SESSIONS,
        [],
        data_available_through=FUTURE_DATA_THROUGH,
    )
    assert target.status == "awaiting_data"

    projected = with_projection(target, [cycle_2013(), cycle_2017()])

    assert projected.projection is not None
    assert projected.projection.is_projection is True
    assert projected.projection.basis == ["2013 Cycle", "2017 Cycle"]
    assert projection_at(projected.projection, 30) == ProjectionPoint(
        offset_days=30,
        price_index=Decimal("100"),
        turnover_index=Decimal("100"),
        pe_index=None,
    )
    assert q(projection_at(projected.projection, 30).price_index, "index") == "100.00"
    assert q(projection_at(projected.projection, 0).price_index, "index") == "100.00"
    assert projected.points == []
    assert projected.markers is None
    assert projected.summary is None
    assert projected.flags == []
    assert projected.status == "awaiting_data"


def test_projection_holds_no_value_outside_the_complete_cycles_windows() -> None:
    """Target window T-20 .. T+60 against basis windows T-10 .. T+40: offsets -20 .. -11 and
    41 .. 60 lie outside every basis window, so the projection has points only for -10 .. 40.
    At -10 and -9 only 2013 has a point (its anchor, a Friday at -10; 2017's anchor is at -8),
    and its 100.00 is the mean."""
    target = run_cycle_backtest(
        FUTURE_EVENT,
        days_window(FUTURE_EVENT.event_date, 20, 60),
        [],
        FUTURE_SESSIONS,
        [],
        data_available_through=FUTURE_DATA_THROUGH,
    )
    projection = build_projection(target, [cycle_2013(), cycle_2017()])

    assert projection is not None
    assert [point.offset_days for point in projection.points] == list(range(-10, 41))
    assert q(projection_at(projection, -10).price_index, "index") == "100.00"
    assert q(projection_at(projection, 40).price_index, "index") == "100.00"


def stepped_cycle(
    event: CycleEvent,
    post_days: int,
    sessions: Sequence[date],
    a_close: Callable[[date], str],
    turnover: Callable[[date], str],
    a_fundamentals: Sequence[FundamentalsRow],
) -> CycleBacktestResult:
    """A (100 shares) closing ``a_close`` with ``a_fundamentals`` and B (100 shares) flat at
    10.00 without fundamentals, each trading ``turnover`` KES a session, over a
    T-10 .. T+``post_days`` window with data past the window end. Both start at 1,000 KES of
    capitalization; only A has earnings, so P/E follows A alone while the price index follows
    A + B."""
    result = run_cycle_backtest(
        event,
        days_window(event.event_date, 10, post_days),
        [
            candidate("A", 100, prices("A", sessions, a_close, turnover=turnover), a_fundamentals),
            candidate("B", 100, prices("B", sessions, "10.00", turnover=turnover)),
        ],
        sessions,
        [],
        data_available_through=sessions[-1],
    )
    assert result.status == "complete"
    assert result.flags == []
    return result


def stepped_cycle_2013() -> CycleBacktestResult:
    """Mon 2013-03-04, window T-10 .. T+40 = 2013-02-22 .. 2013-04-13, anchored on Fri
    2013-02-22 (-10). No ticker trades on Wed 2013-03-06 (+2), so it is not a session.

    * A closes 10.00 through Tue 2013-03-05 (+1) and 14.00 from Thu 2013-03-07 (+3): the price
      index is 100.00, then 100 * (1,400 + 1,000) / 2,000 = 120.00.
    * A reports FY2011 NI 100 and FY2012 NI 200; FY2012 is usable from 2012-12-31 + 90 days =
      Sun 2013-03-31 (+27), so first on Mon 2013-04-01 (+28). P/E is 1,000 / 100 = 10 at the
      anchor, 1,400 / 100 = 14 (P/E index 140.00) from +3 and 1,400 / 200 = 7 (70.00) from +28.
    * Each ticker trades 1,000 KES a session, then 2,000 from Mon 2013-03-11 (+7). MA20 is 2,000
      at the anchor and 2,000 + 100k after k sessions at 4,000, a turnover index of 100 + 5k:
      105.00 at +7, 115.00 at +9, 125.00 at +11, 150.00 at +18, 155.00 at +21, 175.00 at +25,
      180.00 at +28 and 200.00 from +32 (k = 20).
    """
    return stepped_cycle(
        CycleEvent("2013 Cycle", date(2013, 3, 4)),
        40,
        drop(weekdays(date(2013, 1, 21), date(2013, 4, 15)), date(2013, 3, 6)),
        after(date(2013, 3, 5), "10.00", "14.00"),
        after(date(2013, 3, 10), "1000", "2000"),
        [fy(date(2011, 12, 31), "100"), fy(date(2012, 12, 31), "200")],
    )


def stepped_cycle_2017() -> CycleBacktestResult:
    """Tue 2017-08-08, window T-10 .. T+20 = 2017-07-29 .. 2017-08-28, 20 days shorter than
    2013's, anchored on Mon 2017-07-31 (-8).

    * A closes 10.00 through Fri 2017-08-18 (+10) and 8.00 from Mon 2017-08-21 (+13): the price
      index is 100.00, then 100 * (800 + 1,000) / 2,000 = 90.00.
    * A reports FY2016 NI 100: P/E is 10 at the anchor and 8 from +13, a P/E index of 80.00.
    * Each ticker trades 1,000 KES a session, then 500 from Mon 2017-08-14 (+6). MA20 is 2,000 at
      the anchor and 2,000 - 50k after k sessions at 1,000, a turnover index of 100 - 2.5k:
      97.50 at +6, 90.00 at +9, 87.50 at +10, 85.00 at +13 and 72.50 at +20 (k = 11).
    """
    return stepped_cycle(
        CycleEvent("2017 Cycle", date(2017, 8, 8)),
        20,
        weekdays(date(2017, 6, 26), date(2017, 8, 29)),
        after(date(2017, 8, 18), "10.00", "8.00"),
        after(date(2017, 8, 13), "1000", "500"),
        [fy(date(2016, 12, 31), "100")],
    )


def stepped_projection() -> CycleBacktestResult:
    """The 2027 target over T-12 .. T+45 (2027-07-29 .. 2027-09-24), awaiting data, with the
    projection of the stepped 2013 and 2017 cycles attached."""
    target = run_cycle_backtest(
        FUTURE_EVENT,
        days_window(FUTURE_EVENT.event_date, 12, 45),
        [],
        FUTURE_SESSIONS,
        [],
        data_available_through=FUTURE_DATA_THROUGH,
    )
    assert target.status == "awaiting_data"
    return with_projection(target, [stepped_cycle_2013(), stepped_cycle_2017()])


def test_projection_covers_only_offsets_inside_a_basis_cycles_own_window() -> None:
    """The target asks for offsets -12 .. 45. 2013's window covers -10 .. 40 and 2017's
    -10 .. 20, and neither has a point before -10 (2013's anchor; 2017's is at -8), so the
    projection has a point at every offset from -10 to 40. None lies beyond 40: no basis window
    reaches there, although both cycles have points before it."""
    projected = stepped_projection()

    assert projected.status == "awaiting_data"
    assert projected.points == []
    assert projected.flags == []
    assert projected.projection is not None
    assert projected.projection.is_projection is True
    assert projected.projection.basis == ["2013 Cycle", "2017 Cycle"]
    assert [point.offset_days for point in projected.projection.points] == list(range(-10, 41))


@pytest.mark.parametrize(
    ("offset", "price_index", "turnover_index", "pe_index"),
    [
        pytest.param(1, "100.00", "100.00", "100.00", id="before-any-change"),
        pytest.param(2, "100.00", "100.00", "100.00", id="2013-missing-weekday-before-its-rise"),
        pytest.param(3, "110.00", "100.00", "120.00", id="on-2013-price-and-pe-rise"),
        pytest.param(5, "110.00", "100.00", "120.00", id="weekend-before-both-turnover-steps"),
        pytest.param(6, "110.00", "98.75", "120.00", id="on-2017-turnover-step"),
        pytest.param(9, "110.00", "102.50", "120.00", id="between-changes"),
        pytest.param(11, "110.00", "106.25", "120.00", id="2017-saturday-before-its-fall"),
        pytest.param(13, "105.00", "105.00", "110.00", id="on-2017-price-and-pe-fall"),
        pytest.param(20, "105.00", "111.25", "110.00", id="last-offset-of-2017-window"),
        pytest.param(21, "120.00", "155.00", "140.00", id="first-offset-only-2013-covers"),
        pytest.param(27, "120.00", "175.00", "140.00", id="2013-sunday-before-fy2012-earnings"),
        pytest.param(28, "120.00", "180.00", "70.00", id="on-fy2012-earnings"),
        pytest.param(40, "120.00", "200.00", "70.00", id="last-offset-of-2013-window"),
    ],
)
def test_projection_means_each_cycles_last_value_at_or_before_the_offset(
    offset: int, price_index: str, turnover_index: str, pe_index: str
) -> None:
    """Each metric is the mean, over the basis cycles whose own window covers the offset, of the
    cycle's last point on or before it, never its next one (see the stepped 2013 and 2017
    cycles for their paths):

    * +1: both cycles are still at 100.00 on all three metrics.
    * +2: 2013 has no session (Wed 2013-03-06) and holds its +1 values; its next session, +3,
      would give a price of (120 + 100) / 2 = 110.00 and a P/E index of (140 + 100) / 2 = 120.00.
    * +3: price (120 + 100) / 2 = 110.00 and P/E (140 + 100) / 2 = 120.00; turnover is 100.00.
    * +5: 2013's Saturday holds Fri +4 and 2017's Sunday Fri +3, so turnover is
      (100 + 100) / 2 = 100.00, not (105 + 97.5) / 2 = 101.25 from their Monday sessions.
    * +6: 2013's Sunday still holds +4: turnover (100 + 97.5) / 2 = 98.75.
    * +9: turnover (115 + 90) / 2 = 102.50.
    * +11: 2017's Saturday holds Fri +10: price 110.00, turnover (125 + 87.5) / 2 = 106.25 and
      P/E 120.00, not the 105.00, 105.00 and 110.00 its Monday +13 would give.
    * +13: price (120 + 90) / 2 = 105.00, turnover (125 + 85) / 2 = 105.00 with 2013's Sunday
      holding Fri +11 (its Monday +14 would give 107.50), P/E (140 + 80) / 2 = 110.00.
    * +20: 2017's last covered offset, 2013's Sunday holding Fri +18: turnover
      (150 + 72.5) / 2 = 111.25.
    * +21 .. +40: only 2013's window covers them, so its own values are the mean: at +21
      120.00, 155.00 and 140.00, not (120 + 90) / 2 = 105.00 with 2017's +20 point averaged in;
      on Sun +27, the day before FY2012 earnings are first used, P/E is still 140.00 (175.00
      turnover from Fri +25); from +28 it is 70.00; Sat +40 holds Fri +39 (200.00 turnover).
    """
    projected = stepped_projection()

    assert projected.projection is not None
    point = projection_at(projected.projection, offset)
    assert q(point.price_index, "index") == price_index
    assert q(point.turnover_index, "index") == turnover_index
    assert q(point.pe_index, "index") == pe_index


def test_projection_ignores_cycles_that_are_not_complete() -> None:
    """A partial or awaiting cycle in the basis list is not averaged: with 2013, 2017 and the
    awaiting 2027 target supplied, the basis is still 2013 and 2017."""
    target = future_cycle(days_window(FUTURE_EVENT.event_date, 10, 40))
    projection = build_projection(target, [cycle_2013(), target, cycle_2017()])

    assert projection is not None
    assert projection.basis == ["2013 Cycle", "2017 Cycle"]


@pytest.mark.parametrize(
    "basis_count",
    [pytest.param(0, id="no-complete-cycle"), pytest.param(1, id="one-complete-cycle")],
)
def test_fewer_than_two_complete_cycles_is_an_insufficient_projection_basis(
    basis_count: int,
) -> None:
    """One complete cycle (or none) cannot be averaged: no projection, and the target cycle is
    flagged ``insufficient_projection_basis``."""
    target = future_cycle(default_window(FUTURE_EVENT.event_date))
    basis = [cycle_2013()][:basis_count]

    assert build_projection(target, basis) is None
    projected = with_projection(target, basis)
    assert projected.projection is None
    assert projected.flags == ["insufficient_projection_basis"]


def test_with_projection_leaves_complete_cycles_unchanged() -> None:
    """Only ``partial`` and ``awaiting_data`` cycles are projected."""
    complete = cycle_2013()

    assert with_projection(complete, [cycle_2017(), cycle_2017()]) is complete


# ---------------------------------------------------------------------------------------------
# Future and partial events: 2027-08-10 with data through 2026-09-30
# ---------------------------------------------------------------------------------------------


def test_future_event_at_180_days_is_awaiting_data_with_a_projection() -> None:
    """At ±180 days the 2027 window opens 2027-02-11, after the data ends: ``awaiting_data`` with
    no points, markers or summary. ``days_until_window_start`` is measured from
    ``data_available_through``: 2026-09-30 to 2027-02-11 is 134 days. With the 2013 and 2017
    complete cycles the projection is attached."""
    result = future_cycle(default_window(FUTURE_EVENT.event_date))

    assert result.label == "2027 Election Watch"
    assert (result.window_start, result.window_end) == (date(2027, 2, 11), date(2028, 2, 6))
    assert result.status == "awaiting_data"
    assert result.days_until_window_start == 134
    assert result.anchor_date is None
    assert result.points == []
    assert result.markers is None
    assert result.summary is None
    assert result.constituents == []
    assert result.flags == []

    projected = with_projection(result, [cycle_2013(), cycle_2017()])
    assert projected.projection is not None
    assert projected.projection.basis == ["2013 Cycle", "2017 Cycle"]
    assert projected.points == []


def test_future_event_at_12_months_is_partial_through_the_last_session() -> None:
    """At ±12 months the window is 2026-08-10 .. 2028-08-10, so the data that ends 2026-09-30
    lies partly inside it: ``partial``, with observed points from Mon 2026-08-10 (the anchor) to
    Wed 2026-09-30. The index is 100.00 then 110.00; every point precedes T, so the index at T
    is the last one (110.00, a +10.00% pre-event return) and the post-event return is null."""
    result = future_cycle(months_window(FUTURE_EVENT.event_date, 12, 12))

    assert (result.window_start, result.window_end) == (date(2026, 8, 10), date(2028, 8, 10))
    assert result.status == "partial"
    assert result.anchor_date == date(2026, 8, 10)
    assert result.days_until_window_start is None
    assert result.constituents == ["X", "Y"]
    assert result.points[0].trade_date == date(2026, 8, 10)
    assert result.points[0].offset_days == -365
    assert result.points[-1].trade_date == date(2026, 9, 30)
    assert result.points[-1].offset_days == -314
    assert q(result.points[0].price_index, "index") == "100.00"
    assert q(result.points[-1].price_index, "index") == "110.00"
    assert result.summary is not None
    assert q(result.summary.pre_event_return_pct, "pct") == "10.00"
    assert result.summary.post_event_return_pct is None
    assert q(result.summary.max_drawdown_pct, "pct") == "0.00"
    assert result.markers is not None
    assert result.flags == ["pe_anchor_unavailable"]

    projected = with_projection(result, [cycle_2013(), cycle_2017()])
    assert projected.projection is not None
    assert projected.projection.basis == ["2013 Cycle", "2017 Cycle"]
    assert projected.points == result.points
    assert projected.flags == ["pe_anchor_unavailable"]


def test_partial_cycle_points_stop_at_data_available_through() -> None:
    """The standard 2022 window with data only through Fri 2022-08-12: ``partial``, 10 points
    ending on 2022-08-12; rows recorded after that date are not read."""
    result = run_cycle_backtest(
        EVENT, WINDOW, pair(), SESSIONS, [], data_available_through=date(2022, 8, 12)
    )

    assert result.status == "partial"
    assert [point.trade_date for point in result.points] == WINDOW_DAYS[:10]


def test_data_reaching_the_window_end_is_complete() -> None:
    """In the standard scenario the data runs through 2022-08-19, the window end."""
    assert run_standard(pair()).status == "complete"


@pytest.mark.parametrize(
    ("data_available_through", "days_until"),
    [
        pytest.param(None, None, id="no-data"),
        pytest.param(date(2022, 7, 30), None, id="data-ends-on-a-non-session-window-start"),
    ],
)
def test_no_session_inside_the_window_is_awaiting_data(
    data_available_through: date | None, days_until: int | None
) -> None:
    """With no data at all, or data that ends on the window start (Sat 2022-07-30, not a
    session), no session lies inside the window: ``awaiting_data``, and no countdown, because
    the data does not end before the window opens."""
    result = run_cycle_backtest(
        EVENT,
        WINDOW,
        pair(),
        SESSIONS,
        [],
        data_available_through=data_available_through,
    )

    assert result.status == "awaiting_data"
    assert result.days_until_window_start == days_until
    assert result.points == []
    assert result.markers is None
    assert result.summary is None
