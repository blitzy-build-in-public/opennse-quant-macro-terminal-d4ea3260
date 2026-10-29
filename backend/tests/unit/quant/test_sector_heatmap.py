"""Sector rotation returns: capitalization-weighted sector and market returns per period.

Formula under test, in percentage points over the members that qualify for a period ``s`` to ``e``:

    return_pct = (sum(shares * close_e) / sum(shares * close_s) - 1) * 100

A member qualifies when it has a close at most 10 calendar days old on or before both boundaries.
``relative_return_pct`` is the sector return minus the market return, and is null on the market
row. Boundaries are the last observed market sessions on or before each month-end or Friday,
clamped to ``as_of``. Every expected value is a hand-computed literal, compared after the single
``quantize_out(value, "pct")`` rounding step that services apply. Prices are in KES per share.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, timedelta
from decimal import Decimal

import pytest

from opennse_terminal.quant.asof import AsOfValue, PriceRow, close_as_of
from opennse_terminal.quant.heatmap import (
    CLOSE_MAX_AGE_DAYS,
    DEFAULT_HEATMAP_PERIODS,
    HeatmapCellResult,
    HeatmapInterval,
    HeatmapMember,
    HeatmapResult,
    SectorHeatmapResult,
    period_boundaries,
    sector_returns,
)
from opennse_terminal.quant.numeric import quantize_out

S = date(2026, 8, 31)
"""Opening boundary of the base period (Monday)."""

E = date(2026, 9, 30)
"""Closing boundary of the base period (Wednesday)."""


def price(ticker: str, day: date, close: str) -> PriceRow:
    """One session of ``ticker`` closing at ``close`` KES; volume and turnover are fixed."""
    return PriceRow(
        ticker=ticker,
        trade_date=day,
        close_price=Decimal(close),
        volume=1000,
        turnover_kes=Decimal("1000.00"),
    )


def member(ticker: str, sector: str, shares: int, closes: Mapping[date, str]) -> HeatmapMember:
    """A heatmap member whose prices are ``closes`` (session date to KES close), sorted."""
    return HeatmapMember(
        ticker=ticker,
        sector=sector,
        shares_outstanding=shares,
        prices=[price(ticker, day, closes[day]) for day in sorted(closes)],
    )


def pct(value: Decimal | None) -> str | None:
    """The value as a service returns it: rounded once to 2 dp, or ``None`` when undefined."""
    if value is None:
        return None
    return str(quantize_out(value, "pct"))


def weekdays(start: date, end: date, excluded: Iterable[date] = ()) -> list[date]:
    """Monday-to-Friday dates from ``start`` to ``end`` inclusive, without ``excluded``."""
    skipped = set(excluded)
    days: list[date] = []
    day = start
    while day <= end:
        if day.weekday() < 5 and day not in skipped:
            days.append(day)
        day += timedelta(days=1)
    return days


def sector_row(result: HeatmapResult, sector: str) -> SectorHeatmapResult:
    """The heatmap row of ``sector``; fails the test when the row is absent."""
    rows = [row for row in result.sectors if row.sector == sector]
    assert len(rows) == 1, f"expected exactly one row for sector {sector!r}"
    return rows[0]


def reference_members() -> list[HeatmapMember]:
    """A: 100 shares, 10.00 -> 11.00. B: 100 shares, 10.00 -> 9.00. Both priced on S and E."""
    return [
        member("AAA", "A", 100, {S: "10.00", E: "11.00"}),
        member("BBB", "B", 100, {S: "10.00", E: "9.00"}),
    ]


def test_module_constants() -> None:
    """The 10-day close tolerance and the 12-period default of the API contract."""
    assert CLOSE_MAX_AGE_DAYS == 10
    assert DEFAULT_HEATMAP_PERIODS == 12


def test_reference_case_hand_computed() -> None:
    """A: 100x11 / 100x10 - 1 = +10.00%. B: 100x9 / 100x10 - 1 = -10.00%.

    Market: (1,100 + 900) / (1,000 + 1,000) - 1 = 0.00%. Relative: A 10 - 0 = +10.00%, B -10.00%.
    Members are passed B first; rows come back sorted by sector name.
    """
    result = sector_returns(list(reversed(reference_members())), [S, E])

    assert result.period_ends == [E]
    assert [row.sector for row in result.sectors] == ["A", "B"]

    [a_cell] = sector_row(result, "A").cells
    assert a_cell.period_end == E
    assert pct(a_cell.return_pct) == "10.00"
    assert pct(a_cell.relative_return_pct) == "10.00"
    assert a_cell.constituents == 1

    [b_cell] = sector_row(result, "B").cells
    assert b_cell.period_end == E
    assert pct(b_cell.return_pct) == "-10.00"
    assert pct(b_cell.relative_return_pct) == "-10.00"
    assert b_cell.constituents == 1

    [market_cell] = result.market
    assert market_cell.period_end == E
    assert pct(market_cell.return_pct) == "0.00"
    assert market_cell.relative_return_pct is None
    assert market_cell.constituents == 2


def test_multiple_periods_in_boundary_order() -> None:
    """Three boundaries give two cells per row, each closing at its own boundary.

    Period 1 (2026-07-31 to S): A 100x10 / 100x10 - 1 = 0.00%; B 100x10 / 100x8 - 1 = +25.00%;
    market (1,000 + 1,000) / (1,000 + 800) - 1 = +11.11%; relative A -11.11%, B +13.89%.
    Period 2 (S to E): A +10.00%, B -10.00%, market 0.00%, relative +10.00% and -10.00%.
    """
    july_end = date(2026, 7, 31)
    members = [
        member("AAA", "A", 100, {july_end: "10.00", S: "10.00", E: "11.00"}),
        member("BBB", "B", 100, {july_end: "8.00", S: "10.00", E: "9.00"}),
    ]

    result = sector_returns(members, [july_end, S, E])

    assert result.period_ends == [S, E]
    assert [cell.period_end for cell in result.market] == [S, E]
    assert [pct(cell.return_pct) for cell in result.market] == ["11.11", "0.00"]
    assert [cell.constituents for cell in result.market] == [2, 2]

    a_cells = sector_row(result, "A").cells
    assert [cell.period_end for cell in a_cells] == [S, E]
    assert [pct(cell.return_pct) for cell in a_cells] == ["0.00", "10.00"]
    assert [pct(cell.relative_return_pct) for cell in a_cells] == ["-11.11", "10.00"]

    b_cells = sector_row(result, "B").cells
    assert [cell.period_end for cell in b_cells] == [S, E]
    assert [pct(cell.return_pct) for cell in b_cells] == ["25.00", "-10.00"]
    assert [pct(cell.relative_return_pct) for cell in b_cells] == ["13.89", "-10.00"]


def test_unequal_share_members_are_capitalization_weighted() -> None:
    """A: (100x11 + 300x18) / (100x10 + 300x20) - 1 = 6,500 / 7,000 - 1 = -7.14%.

    B: 100x20 / 100x10 - 1 = +100.00%. Market: (6,500 + 2,000) / (7,000 + 1,000) - 1 = +6.25%.
    Relative: A -7.14... - 6.25 = -13.39%, B 100 - 6.25 = +93.75%. A's two members differ in
    shares, start price and return, so equal share counts (29 / 30 - 1 = -3.33%), share-weighted
    member returns ((100x10 - 300x10) / 400 = -5.00%) or AAA alone (+10.00%) give other A returns.
    """
    members = [
        member("AAA", "A", 100, {S: "10.00", E: "11.00"}),
        member("AAB", "A", 300, {S: "20.00", E: "18.00"}),
        member("BBB", "B", 100, {S: "10.00", E: "20.00"}),
    ]

    result = sector_returns(members, [S, E])

    assert result.period_ends == [E]
    assert [row.sector for row in result.sectors] == ["A", "B"]

    [a_cell] = sector_row(result, "A").cells
    assert a_cell.period_end == E
    assert pct(a_cell.return_pct) == "-7.14"
    assert pct(a_cell.relative_return_pct) == "-13.39"
    assert a_cell.constituents == 2

    [b_cell] = sector_row(result, "B").cells
    assert b_cell.period_end == E
    assert pct(b_cell.return_pct) == "100.00"
    assert pct(b_cell.relative_return_pct) == "93.75"
    assert b_cell.constituents == 1

    [market_cell] = result.market
    assert market_cell.period_end == E
    assert pct(market_cell.return_pct) == "6.25"
    assert market_cell.relative_return_pct is None
    assert market_cell.constituents == 3


def test_sector_without_priced_constituent_is_null_and_leaves_market_unchanged() -> None:
    """C has no price rows, so no member qualifies: its cell is null with 0 constituents.

    The market stays A + B only: (1,100 + 900) / (1,000 + 1,000) - 1 = 0.00%, and A's relative
    return stays +10.00%.
    """
    members = [*reference_members(), member("CCC", "C", 100, {})]

    result = sector_returns(members, [S, E])

    assert sector_row(result, "C").cells == [HeatmapCellResult(E, None, None, 0)]
    [market_cell] = result.market
    assert pct(market_cell.return_pct) == "0.00"
    assert market_cell.constituents == 2
    [a_cell] = sector_row(result, "A").cells
    assert pct(a_cell.relative_return_pct) == "10.00"


def test_zero_share_sector_is_null_without_division_error() -> None:
    """D has 0 shares and valid closes (10.00 -> 12.00): it qualifies but its start cap is 0.

    The cell is null rather than a DivisionByZero; D still counts as a qualifying constituent.
    D adds 0 KES at both boundaries, so the market return stays 0.00% over 3 constituents.
    """
    members = [*reference_members(), member("DDD", "D", 0, {S: "10.00", E: "12.00"})]

    result = sector_returns(members, [S, E])

    assert sector_row(result, "D").cells == [HeatmapCellResult(E, None, None, 1)]
    [market_cell] = result.market
    assert pct(market_cell.return_pct) == "0.00"
    assert market_cell.constituents == 3


def test_market_of_zero_share_members_only_is_null() -> None:
    """With D as the only member, the market's start cap is 0 x 10.00 = 0: both rows are null."""
    result = sector_returns([member("DDD", "D", 0, {S: "10.00", E: "12.00"})], [S, E])

    assert result.market == [HeatmapCellResult(E, None, None, 1)]
    assert sector_row(result, "D").cells == [HeatmapCellResult(E, None, None, 1)]


@pytest.mark.parametrize(
    ("closes", "x_return", "market_return", "market_constituents"),
    [
        pytest.param(
            {date(2026, 8, 20): "10.00", E: "20.00"},
            None,
            "0.00",
            2,
            id="start-close-11-days-old",
        ),
        pytest.param(
            {S: "10.00", date(2026, 9, 19): "20.00"},
            None,
            "0.00",
            2,
            id="end-close-11-days-old",
        ),
        pytest.param(
            {date(2026, 8, 21): "10.00", date(2026, 9, 20): "20.00"},
            "100.00",
            "33.33",
            3,
            id="closes-10-days-old",
        ),
    ],
)
def test_close_age_limit_at_each_boundary(
    closes: dict[date, str],
    x_return: str | None,
    market_return: str,
    market_constituents: int,
) -> None:
    """X (100 shares, 10.00 -> 20.00) qualifies only with closes at most 10 days old at S and E.

    11 days old at either boundary: X's cell is null and the market is A + B, 0.00%.
    10 days old at both: X 100x20 / 100x10 - 1 = +100.00%; market
    (1,100 + 900 + 2,000) / (1,000 + 1,000 + 1,000) - 1 = +33.33%.
    """
    members = [*reference_members(), member("XXX", "X", 100, closes)]

    result = sector_returns(members, [S, E])

    [x_cell] = sector_row(result, "X").cells
    assert pct(x_cell.return_pct) == x_return
    assert x_cell.constituents == (0 if x_return is None else 1)
    [market_cell] = result.market
    assert pct(market_cell.return_pct) == market_return
    assert market_cell.constituents == market_constituents


def test_fewer_than_two_boundaries_define_no_period() -> None:
    """One boundary opens no period: no period ends, and every row is present but empty."""
    result = sector_returns(reference_members(), [E])

    assert result == HeatmapResult(
        period_ends=[],
        market=[],
        sectors=[SectorHeatmapResult("A", []), SectorHeatmapResult("B", [])],
    )


def test_no_members_gives_a_null_market_and_no_sector_rows() -> None:
    """Without members the market sums are 0 KES at both boundaries, so its return is null."""
    result = sector_returns([], [S, E])

    assert result.market == [HeatmapCellResult(E, None, None, 0)]
    assert result.sectors == []


@pytest.mark.parametrize(
    "boundaries",
    [pytest.param([E, S], id="descending"), pytest.param([S, S], id="repeated")],
)
def test_boundaries_must_be_strictly_ascending(boundaries: list[date]) -> None:
    """Out-of-order or repeated boundaries are a caller error, not an empty result."""
    with pytest.raises(ValueError, match="strictly ascending"):
        sector_returns(reference_members(), boundaries)


CLOSE_ROWS = [
    price("AAA", date(2026, 9, 1), "10.00"),
    price("AAA", date(2026, 9, 10), "11.00"),
]
"""Two sessions of one ticker for the direct ``close_as_of`` checks."""


@pytest.mark.parametrize(
    ("day", "max_age_days", "expected"),
    [
        pytest.param(
            date(2026, 9, 20),
            10,
            AsOfValue(Decimal("11.00"), date(2026, 9, 10)),
            id="10-days-old-accepted",
        ),
        pytest.param(date(2026, 9, 21), 10, None, id="11-days-old-rejected"),
        pytest.param(
            date(2027, 9, 21),
            None,
            AsOfValue(Decimal("11.00"), date(2026, 9, 10)),
            id="no-age-limit",
        ),
        pytest.param(
            date(2026, 9, 1),
            0,
            AsOfValue(Decimal("10.00"), date(2026, 9, 1)),
            id="same-day-inclusive",
        ),
        pytest.param(
            date(2026, 9, 9),
            None,
            AsOfValue(Decimal("10.00"), date(2026, 9, 1)),
            id="between-sessions",
        ),
        pytest.param(date(2026, 8, 31), None, None, id="before-first-row"),
    ],
)
def test_close_as_of(day: date, max_age_days: int | None, expected: AsOfValue | None) -> None:
    """The latest close on or before ``day``, rejected when older than ``max_age_days``.

    2026-09-20 - 2026-09-10 = 10 days (accepted at a limit of 10); 2026-09-21 is 11 days.
    """
    result = close_as_of(CLOSE_ROWS, day, max_age_days=max_age_days)

    assert result == expected
    if expected is not None:
        assert result is not None
        assert str(result.value) == str(expected.value)


def test_close_as_of_without_rows_is_none() -> None:
    """A ticker with no sessions has no close on any day."""
    assert close_as_of([], E) is None


def test_close_as_of_rejects_negative_age_limit() -> None:
    """A negative age limit is a caller error."""
    with pytest.raises(ValueError, match="max_age_days"):
        close_as_of(CLOSE_ROWS, E, max_age_days=-1)


MONTH_SESSIONS = weekdays(date(2026, 4, 1), date(2026, 9, 30), excluded=[date(2026, 5, 29)])
"""Weekdays from Wed 2026-04-01 to Wed 2026-09-30, with Fri 2026-05-29 closed.

2026-05-31 is a Sunday and 2026-05-29 has no session, so May's boundary is Thu 2026-05-28.
"""

WEEK_SESSIONS = weekdays(date(2026, 9, 1), date(2026, 9, 30), excluded=[date(2026, 9, 18)])
"""Weekdays in September 2026 from Tue 2026-09-01, with Fri 2026-09-18 closed."""


@pytest.mark.parametrize(
    ("periods", "as_of", "expected"),
    [
        pytest.param(
            3,
            date(2026, 9, 30),
            [date(2026, 6, 30), date(2026, 7, 31), date(2026, 8, 31), date(2026, 9, 30)],
            id="periods-plus-one-boundaries",
        ),
        pytest.param(
            1,
            date(2026, 6, 30),
            [date(2026, 5, 28), date(2026, 6, 30)],
            id="weekend-and-closed-month-end",
        ),
        pytest.param(
            1,
            date(2026, 9, 16),
            [date(2026, 8, 31), date(2026, 9, 16)],
            id="clamped-to-as-of",
        ),
        pytest.param(
            DEFAULT_HEATMAP_PERIODS,
            date(2026, 9, 30),
            [
                date(2026, 4, 30),
                date(2026, 5, 28),
                date(2026, 6, 30),
                date(2026, 7, 31),
                date(2026, 8, 31),
                date(2026, 9, 30),
            ],
            id="short-history",
        ),
    ],
)
def test_month_boundaries(periods: int, as_of: date, expected: list[date]) -> None:
    """Last session on or before each month-end, the latest clamped to ``as_of``.

    The month-end of September (2026-09-30) is after as_of 2026-09-16, so that period closes on
    the last session on or before 2026-09-16, itself. With 12 periods the targets reach back to
    2025-09-30, but sessions begin 2026-04-01: 2025-09-30 to 2026-03-31 are dropped, leaving 6
    boundaries (5 periods).
    """
    assert period_boundaries(MONTH_SESSIONS, "month", periods, as_of) == expected


@pytest.mark.parametrize(
    ("periods", "as_of", "expected"),
    [
        pytest.param(
            2,
            date(2026, 9, 25),
            [date(2026, 9, 11), date(2026, 9, 17), date(2026, 9, 25)],
            id="closed-friday-rolls-back",
        ),
        pytest.param(
            1,
            date(2026, 9, 16),
            [date(2026, 9, 11), date(2026, 9, 16)],
            id="clamped-to-midweek-as-of",
        ),
        pytest.param(
            1,
            date(2026, 9, 20),
            [date(2026, 9, 11), date(2026, 9, 17)],
            id="sunday-as-of-uses-its-iso-week",
        ),
        pytest.param(
            12,
            date(2026, 9, 25),
            [date(2026, 9, 4), date(2026, 9, 11), date(2026, 9, 17), date(2026, 9, 25)],
            id="short-history",
        ),
    ],
)
def test_week_boundaries(periods: int, as_of: date, expected: list[date]) -> None:
    """Last session on or before each Friday, the latest clamped to ``as_of``.

    Fri 2026-09-18 is closed, so its week ends Thu 2026-09-17. Wed 2026-09-16 precedes its Friday
    and closes its own week. Sun 2026-09-20 belongs to the ISO week of Fri 2026-09-18. Sessions
    begin Tue 2026-09-01, so Fridays from 2026-08-28 back are dropped.
    """
    assert period_boundaries(WEEK_SESSIONS, "week", periods, as_of) == expected


@pytest.mark.parametrize(
    ("sessions", "expected"),
    [
        pytest.param([date(2026, 7, 15)], [date(2026, 7, 15)], id="targets-collapse"),
        pytest.param([], [], id="no-sessions"),
    ],
)
def test_sparse_sessions_give_fewer_boundaries(sessions: list[date], expected: list[date]) -> None:
    """Targets 2026-06-30 to 2026-09-30 over one session 2026-07-15: 06-30 has none on or before
    it and the rest all resolve to 07-15, which appears once. No sessions give no boundaries.
    """
    assert period_boundaries(sessions, "month", 3, date(2026, 9, 30)) == expected


@pytest.mark.parametrize("interval", ["month", "week"])
def test_boundaries_stop_at_the_first_representable_date(interval: HeatmapInterval) -> None:
    """At as_of 0001-01-03 the earlier month-end or Friday precedes date.min and is skipped.

    The month target 0001-01-31 and the Friday 0001-01-05 both clamp to 0001-01-03, whose last
    session is 0001-01-01.
    """
    assert period_boundaries([date(1, 1, 1)], interval, 3, date(1, 1, 3)) == [date(1, 1, 1)]


def test_period_boundaries_rejects_zero_periods() -> None:
    """At least one period is required."""
    with pytest.raises(ValueError, match="periods"):
        period_boundaries(MONTH_SESSIONS, "month", 0, E)


def test_period_boundaries_rejects_unknown_interval() -> None:
    """Only month and week intervals exist."""
    with pytest.raises(ValueError, match="interval"):
        period_boundaries(MONTH_SESSIONS, "quarter", 1, E)  # type: ignore[arg-type]
