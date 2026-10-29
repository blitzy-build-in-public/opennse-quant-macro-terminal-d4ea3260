"""Unit tests for the foreign versus local net flow estimator, ``quant.flows.net_flow_series``.

Expected values are hand-computed from the formulas in the module docstring. Amounts are KES and
percentages are percentage points. Inputs and expectations are ``Decimal`` values built from
strings, and rounded expectations go through the single output rounding step, ``quantize_out``
with ``"money"`` or ``"pct"``, exactly as the service applies it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from opennse_terminal.quant.asof import PriceRow
from opennse_terminal.quant.flows import (
    DEFAULT_FLOW_ROLLING_WINDOW,
    MAX_FLOW_WINDOW,
    MIN_FLOW_WINDOW,
    FlowSeriesResult,
    net_flow_series,
)
from opennse_terminal.quant.numeric import QuantizeKind, quantize_out

CLOSE = Decimal("10.00")

# Mon 2026-09-28, Tue 2026-09-29 and Wed 2026-09-30.
D1 = date(2026, 9, 28)
D2 = date(2026, 9, 29)
D3 = date(2026, 9, 30)

# Seven consecutive weekday sessions, Mon 2026-09-21 to Tue 2026-09-29.
WEEK_SESSIONS = [
    date(2026, 9, 21),
    date(2026, 9, 22),
    date(2026, 9, 23),
    date(2026, 9, 24),
    date(2026, 9, 25),
    date(2026, 9, 28),
    date(2026, 9, 29),
]


def row(ticker: str, day: date, turnover: str, flow: str | None) -> PriceRow:
    """Build one price row at a close of KES 10.00, with the volume its turnover implies."""
    turnover_kes = Decimal(turnover)
    return PriceRow(
        ticker=ticker,
        trade_date=day,
        close_price=CLOSE,
        volume=int(turnover_kes / CLOSE),
        turnover_kes=turnover_kes,
        foreign_net_flow_kes=None if flow is None else Decimal(flow),
    )


def rounded(value: Decimal | None, kind: QuantizeKind) -> str:
    """Return a non-null ``value`` rounded once for output, as its JSON string."""
    assert value is not None
    return str(quantize_out(value, kind))


def assert_zero_sum(result: FlowSeriesResult) -> None:
    """Assert local = -foreign on every point with flow data and on the totals, null with it."""
    for point in result.series:
        if point.foreign_net_kes is None:
            assert point.local_net_kes is None
        else:
            assert point.local_net_kes == -point.foreign_net_kes
    if result.totals is not None:
        assert result.totals.local_net_kes == -result.totals.foreign_net_kes


def test_reference_case_daily_flows_coverage_and_totals() -> None:
    """One ticker over three sessions, the middle one without flow data.

    d1: flow 10,000 on turnover 100,000 -> local -10,000, 10.00% of turnover, cumulative 10,000.
    d2: flow null (turnover 90,000) -> flows, turnover and percentage null; cumulative stays
    10,000 because a null day adds nothing.
    d3: flow -4,000 on turnover 80,000 -> local 4,000, -5.00% of turnover, cumulative 6,000.00.
    Coverage 2 / 3 x 100 = 66.67%. Totals: foreign 6,000, local -6,000, 2 of 3 sessions, and
    6,000 / (100,000 + 80,000) x 100 = 3.33% of the turnover of the sessions with data.
    """
    result = net_flow_series(
        [
            row("SCOM", D1, "100000", "10000"),
            row("SCOM", D2, "90000", None),
            row("SCOM", D3, "80000", "-4000"),
        ],
        [D1, D2, D3],
    )

    assert result.status == "ok"
    assert result.flags == []
    assert [point.trade_date for point in result.series] == [D1, D2, D3]
    first, second, third = result.series

    assert first.foreign_net_kes == Decimal("10000")
    assert first.local_net_kes == Decimal("-10000")
    assert first.turnover_kes == Decimal("100000")
    assert rounded(first.foreign_net_pct_of_turnover, "pct") == "10.00"
    assert first.cumulative_foreign_net_kes == Decimal("10000")

    assert second.foreign_net_kes is None
    assert second.local_net_kes is None
    assert second.turnover_kes is None
    assert second.foreign_net_pct_of_turnover is None
    assert second.cumulative_foreign_net_kes == Decimal("10000")

    assert third.foreign_net_kes == Decimal("-4000")
    assert third.local_net_kes == Decimal("4000")
    assert third.turnover_kes == Decimal("80000")
    assert rounded(third.foreign_net_pct_of_turnover, "pct") == "-5.00"
    assert rounded(third.cumulative_foreign_net_kes, "money") == "6000.00"

    # Three sessions never fill the default 20-session rolling window.
    assert all(point.rolling_foreign_net_kes is None for point in result.series)

    assert rounded(result.coverage_pct, "pct") == "66.67"
    totals = result.totals
    assert totals is not None
    assert totals.foreign_net_kes == Decimal("6000")
    assert totals.local_net_kes == Decimal("-6000")
    assert rounded(totals.foreign_net_pct_of_turnover, "pct") == "3.33"
    assert totals.sessions_with_data == 2
    assert totals.sessions_in_range == 3
    assert_zero_sum(result)


def test_rolling_sum_covers_last_n_sessions_including_t() -> None:
    """A 5-session window over seven sessions with flows 1,000, 2,000, ..., 7,000.

    Sessions 1-4 hold no full window, so their rolling sum is null. Session 5 sums sessions 1-5
    (15,000), session 6 sums 2-6 (20,000) and session 7 sums 3-7 (25,000). The cumulative sum
    at session 7 is 28,000.
    """
    flows = ["1000", "2000", "3000", "4000", "5000", "6000", "7000"]
    rows = [
        row("SCOM", day, "100000", flow) for day, flow in zip(WEEK_SESSIONS, flows, strict=True)
    ]

    result = net_flow_series(rows, WEEK_SESSIONS, rolling_window=5)

    assert [point.rolling_foreign_net_kes for point in result.series] == [
        None,
        None,
        None,
        None,
        Decimal("15000"),
        Decimal("20000"),
        Decimal("25000"),
    ]
    assert result.series[-1].cumulative_foreign_net_kes == Decimal("28000")
    assert_zero_sum(result)


def test_rolling_sum_skips_null_days_and_is_null_without_data_in_window() -> None:
    """A 5-session window over six sessions where only session 1 carries flow (1,000).

    Session 5's window (sessions 1-5) holds four null days, which add nothing: 1,000. Session
    6's window (sessions 2-6) has no flow data at all, so its rolling sum is null rather than
    zero, while the cumulative sum stays 1,000. Coverage is 1 / 6 x 100 = 16.67%.
    """
    sessions = WEEK_SESSIONS[:6]
    flows = ["1000", None, None, None, None, None]
    rows = [row("SCOM", day, "100000", flow) for day, flow in zip(sessions, flows, strict=True)]

    result = net_flow_series(rows, sessions, rolling_window=5)

    assert result.series[4].rolling_foreign_net_kes == Decimal("1000")
    assert result.series[5].rolling_foreign_net_kes is None
    assert all(point.cumulative_foreign_net_kes == Decimal("1000") for point in result.series)
    assert rounded(result.coverage_pct, "pct") == "16.67"


def test_default_rolling_window_is_20_sessions() -> None:
    """Twenty sessions with a flow of 100 each and no ``rolling_window`` argument.

    The window bounds are 5 and 250 with a default of 20, so session 19 holds no full window
    (null) and session 20 sums all twenty flows: 2,000. The estimator counts the sessions it is
    given, so twenty consecutive calendar days serve as twenty sessions.
    """
    assert (MIN_FLOW_WINDOW, DEFAULT_FLOW_ROLLING_WINDOW, MAX_FLOW_WINDOW) == (5, 20, 250)
    sessions = [date(2026, 9, day) for day in range(1, 21)]
    rows = [row("SCOM", day, "100000", "100") for day in sessions]

    result = net_flow_series(rows, sessions)

    assert result.series[18].rolling_foreign_net_kes is None
    assert result.series[19].rolling_foreign_net_kes == Decimal("2000")


@pytest.mark.parametrize("window", [5, 250])
def test_rolling_window_bounds_are_inclusive(window: int) -> None:
    """The smallest (5) and largest (250) windows are accepted; three sessions fill neither."""
    rows = [row("SCOM", day, "100000", "1000") for day in (D1, D2, D3)]

    result = net_flow_series(rows, [D1, D2, D3], rolling_window=window)

    assert all(point.rolling_foreign_net_kes is None for point in result.series)
    assert result.series[-1].cumulative_foreign_net_kes == Decimal("3000")


@pytest.mark.parametrize("window", [4, 251])
def test_rejects_rolling_window_outside_bounds(window: int) -> None:
    """A window one session below 5 or above 250 is a caller error."""
    with pytest.raises(ValueError, match="rolling_window must be between 5 and 250"):
        net_flow_series([], [D1, D2, D3], rolling_window=window)


@pytest.mark.parametrize(
    "sessions",
    [pytest.param([D1, D1], id="duplicate"), pytest.param([D2, D1], id="descending")],
)
def test_rejects_sessions_not_strictly_ascending(sessions: list[date]) -> None:
    """Sessions must be strictly ascending: a repeated or a descending date is a caller error."""
    with pytest.raises(ValueError, match="strictly ascending"):
        net_flow_series([], sessions)


def test_constituents_without_flow_are_left_out_of_flow_and_turnover() -> None:
    """Two tickers over three sessions, rows supplied out of order.

    d1: A flows 5,000 on turnover 50,000; B has no flow (turnover 50,000) -> foreign 5,000 over
    A's turnover alone, 50,000, so 10.00% of turnover.
    d2: A 3,000 on 40,000 and B -1,000 on 60,000 -> foreign 2,000 on turnover 100,000, 2.00%.
    d3: both null -> foreign, local, turnover and percentage null; cumulative stays 7,000.
    Coverage 2 / 3 x 100 = 66.67%. Totals: foreign 7,000, local -7,000, and
    7,000 / (50,000 + 100,000) x 100 = 4.67% of turnover.
    """
    rows = [
        row("B", D3, "30000", None),
        row("A", D1, "50000", "5000"),
        row("B", D2, "60000", "-1000"),
        row("A", D3, "70000", None),
        row("B", D1, "50000", None),
        row("A", D2, "40000", "3000"),
    ]

    result = net_flow_series(rows, [D1, D2, D3])

    first, second, third = result.series
    assert first.foreign_net_kes == Decimal("5000")
    assert first.turnover_kes == Decimal("50000")
    assert rounded(first.foreign_net_pct_of_turnover, "pct") == "10.00"

    assert second.foreign_net_kes == Decimal("2000")
    assert second.local_net_kes == Decimal("-2000")
    assert second.turnover_kes == Decimal("100000")
    assert rounded(second.foreign_net_pct_of_turnover, "pct") == "2.00"

    assert third.foreign_net_kes is None
    assert third.local_net_kes is None
    assert third.turnover_kes is None
    assert third.foreign_net_pct_of_turnover is None
    assert third.cumulative_foreign_net_kes == Decimal("7000")

    assert rounded(result.coverage_pct, "pct") == "66.67"
    totals = result.totals
    assert totals is not None
    assert totals.foreign_net_kes == Decimal("7000")
    assert totals.local_net_kes == Decimal("-7000")
    assert rounded(totals.foreign_net_pct_of_turnover, "pct") == "4.67"
    assert_zero_sum(result)


def test_zero_turnover_gives_null_percentage() -> None:
    """One session with flow 0 on turnover 0.

    The flows are defined (foreign 0, local 0, coverage 100.00%), but the percentage of a zero
    turnover is undefined: null on the point and on the totals, never a division error.
    """
    result = net_flow_series([row("SCOM", D1, "0", "0")], [D1])

    point = result.series[0]
    assert point.foreign_net_kes == Decimal("0")
    assert point.local_net_kes == Decimal("0")
    assert point.turnover_kes == Decimal("0")
    assert point.foreign_net_pct_of_turnover is None
    assert rounded(point.local_net_kes, "money") == "0.00"

    assert result.status == "ok"
    assert rounded(result.coverage_pct, "pct") == "100.00"
    totals = result.totals
    assert totals is not None
    assert totals.foreign_net_kes == Decimal("0")
    assert totals.foreign_net_pct_of_turnover is None
    assert (totals.sessions_with_data, totals.sessions_in_range) == (1, 1)


@pytest.mark.parametrize(
    ("rows", "sessions"),
    [
        pytest.param(
            [row("SCOM", day, "100000", None) for day in (D1, D2, D3)],
            [D1, D2, D3],
            id="every_flow_null",
        ),
        pytest.param(
            # Sat 2026-09-26 is not one of the sessions, so its flow is ignored.
            [row("SCOM", date(2026, 9, 26), "100000", "5000")],
            [D1, D2, D3],
            id="flow_only_off_session",
        ),
        pytest.param([], [], id="no_sessions"),
    ],
)
def test_no_flow_data_in_range(rows: list[PriceRow], sessions: list[date]) -> None:
    """No session in the range carries flow data.

    The status and its flag are ``no_flow_data``, the totals are null rather than zeros, the
    coverage is 0.00%, and the series still holds one point per session, with null flows, a
    cumulative sum of 0 and a null rolling sum.
    """
    result = net_flow_series(rows, sessions)

    assert result.status == "no_flow_data"
    assert result.flags == ["no_flow_data"]
    assert result.totals is None
    assert result.coverage_pct == Decimal("0")
    assert rounded(result.coverage_pct, "pct") == "0.00"
    assert [point.trade_date for point in result.series] == sessions
    for point in result.series:
        assert point.foreign_net_kes is None
        assert point.local_net_kes is None
        assert point.turnover_kes is None
        assert point.foreign_net_pct_of_turnover is None
        assert point.rolling_foreign_net_kes is None
        assert point.cumulative_foreign_net_kes == Decimal("0")


def test_session_without_row_counts_in_coverage_denominator() -> None:
    """Four sessions: flow 1,000, then no row at all, then flow -500, then a row without flow.

    A session with no row is missing data, not a zero-flow day: it stays in the denominator, so
    coverage is 2 / 4 x 100 = 50.00% with 2 of 4 sessions in the totals, and its point is null.
    Totals: foreign 500, local -500, and 500 / (20,000 + 10,000) x 100 = 1.67% of turnover.
    """
    sessions = WEEK_SESSIONS[3:7]
    rows = [
        row("SCOM", sessions[0], "20000", "1000"),
        row("SCOM", sessions[2], "10000", "-500"),
        row("SCOM", sessions[3], "15000", None),
    ]

    result = net_flow_series(rows, sessions)

    assert [point.trade_date for point in result.series] == sessions
    missing = result.series[1]
    assert missing.foreign_net_kes is None
    assert missing.turnover_kes is None
    assert missing.cumulative_foreign_net_kes == Decimal("1000")

    assert rounded(result.coverage_pct, "pct") == "50.00"
    totals = result.totals
    assert totals is not None
    assert totals.foreign_net_kes == Decimal("500")
    assert totals.local_net_kes == Decimal("-500")
    assert rounded(totals.foreign_net_pct_of_turnover, "pct") == "1.67"
    assert (totals.sessions_with_data, totals.sessions_in_range) == (2, 4)
    assert_zero_sum(result)
