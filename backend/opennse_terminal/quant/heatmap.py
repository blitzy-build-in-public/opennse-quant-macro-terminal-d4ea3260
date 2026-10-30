"""Sector rotation returns for the market overview heatmap.

The heatmap compares capitalization-weighted price returns per sector against the whole market,
period by period. Period boundaries are the last observed market sessions on or before each
month-end or Friday: the distinct ``trade_date`` values the database holds, never a holiday list,
so a calendar error cannot move a boundary onto a day without prices. A month-end or Friday that
resolves to a boundary already taken is replaced by an earlier one, so every requested period is
returned whenever the history reaches back far enough.

Returns are in percentage points (``Decimal("10")`` means +10%) at full context precision; services
round them once with ``quantize_out(value, "pct")``.
"""

from __future__ import annotations

from bisect import bisect_right
from calendar import monthrange
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Final, Literal

from opennse_terminal.quant.asof import PriceRow, close_as_of
from opennse_terminal.quant.numeric import HUNDRED, ZERO, safe_div

__all__ = [
    "CLOSE_MAX_AGE_DAYS",
    "DEFAULT_HEATMAP_PERIODS",
    "HeatmapCellResult",
    "HeatmapInterval",
    "HeatmapMember",
    "HeatmapResult",
    "SectorHeatmapResult",
    "period_boundaries",
    "sector_returns",
]

DEFAULT_HEATMAP_PERIODS: Final = 12
"""Periods returned when the caller gives none; the API accepts 1 to 36."""

CLOSE_MAX_AGE_DAYS: Final = 10
"""Oldest close, in calendar days on or before a boundary, that still lets a member qualify."""

HeatmapInterval = Literal["month", "week"]
"""Period length: calendar months ending at month-ends, or ISO weeks ending on Fridays."""

_FRIDAY: Final = 4
"""``date.weekday()`` of Friday."""


@dataclass(frozen=True, slots=True)
class HeatmapMember:
    """One listed equity as the heatmap sees it.

    Attributes:
        ticker: Exchange ticker, for example ``"SCOM"``.
        sector: Sector name exactly as stored in ``terminal_equities.sector``.
        shares_outstanding: Shares in issue, the capitalization weight.
        prices: The ticker's sessions, sorted ascending by ``trade_date``.
    """

    ticker: str
    sector: str
    shares_outstanding: int
    prices: Sequence[PriceRow]


@dataclass(frozen=True, slots=True)
class HeatmapCellResult:
    """The return of one sector, or of the market, over one period.

    Attributes:
        period_end: The boundary session that closes the period.
        return_pct: Capitalization-weighted price return in percentage points, or ``None`` when no
            member qualifies or the qualifying members' starting capitalization is zero.
        relative_return_pct: ``return_pct`` minus the market's return in percentage points;
            ``None`` when either is undefined, and always ``None`` on the market row.
        constituents: Members with a qualifying close at both boundaries of the period.
    """

    period_end: date
    return_pct: Decimal | None
    relative_return_pct: Decimal | None
    constituents: int


@dataclass(frozen=True, slots=True)
class SectorHeatmapResult:
    """One heatmap row: a sector's cells, one per period in ``HeatmapResult.period_ends`` order."""

    sector: str
    cells: list[HeatmapCellResult]


@dataclass(frozen=True, slots=True)
class HeatmapResult:
    """The whole heatmap.

    Attributes:
        period_ends: The closing boundary of each period, ascending.
        market: The market row, one cell per period.
        sectors: One row per sector present among the members, sorted by sector name.
    """

    period_ends: list[date]
    market: list[HeatmapCellResult]
    sectors: list[SectorHeatmapResult]


@dataclass(slots=True)
class _CapTotals:
    """Running capitalization sums, in KES, of the members qualifying for one period."""

    start_cap_kes: Decimal = ZERO
    end_cap_kes: Decimal = ZERO
    constituents: int = 0

    def add(self, start_cap_kes: Decimal, end_cap_kes: Decimal) -> None:
        self.start_cap_kes += start_cap_kes
        self.end_cap_kes += end_cap_kes
        self.constituents += 1

    def return_pct(self) -> Decimal | None:
        # (end / start - 1) x 100 written as (end - start) / start x 100, which saves a rounding
        # step. With no qualifying member the start total is still zero, so both null cases
        # (empty set, zero total capitalization) come out of safe_div as None.
        growth = safe_div(self.end_cap_kes - self.start_cap_kes, self.start_cap_kes)
        if growth is None:
            return None
        return growth * HUNDRED


def _month_end_of(day: date) -> date:
    """The last calendar day of ``day``'s month."""
    return date(day.year, day.month, monthrange(day.year, day.month)[1])


def _friday_of(day: date) -> date:
    """The Friday of ``day``'s ISO week."""
    # ISO weeks run Monday to Sunday, so a weekend day maps back to the Friday just gone. The
    # Friday of any representable week is representable, because date.max is itself a Friday.
    return day + timedelta(days=_FRIDAY - day.weekday())


def _month_end_before(day: date) -> date | None:
    """The latest month-end strictly before ``day``, or ``None`` when it precedes date.min."""
    try:
        return day.replace(day=1) - timedelta(days=1)
    except OverflowError:
        return None


def _friday_before(day: date) -> date | None:
    """The latest Friday strictly before ``day``, or ``None`` when it precedes date.min."""
    days_back = (day.weekday() - _FRIDAY) % 7 or 7
    try:
        return day - timedelta(days=days_back)
    except OverflowError:
        return None


def period_boundaries(
    sessions: Sequence[date],
    interval: HeatmapInterval,
    periods: int,
    as_of: date,
) -> list[date]:
    """Return the boundary sessions of the ``periods`` periods ending at ``as_of``.

    Boundaries are found walking back from ``as_of``. The first target is the month-end of
    ``as_of``'s month (``interval="month"``) or the Friday of its ISO week (``interval="week"``),
    clamped to ``as_of`` so the latest period runs to date. Each boundary is the last observed
    market session on or before its target, and the next target is the latest month-end or Friday
    strictly before that boundary. A month-end or Friday that would resolve to a boundary already
    taken (a non-session ``as_of``, or a month or week without sessions) is thereby replaced by an
    earlier one, so ``periods + 1`` distinct boundaries are returned whenever the history reaches
    that far. The walk stops early only when a target has no session on or before it or would
    precede date.min, so a short history gives fewer boundaries.

    ``boundaries[0]`` opens the first period and ``boundaries[1:]`` are the period ends, ready for
    :func:`sector_returns`.

    Examples:
        ``as_of`` 2026-09-30, ``"month"``, 12 periods: the last sessions on or before 2025-09-30,
        2025-10-31, ..., 2026-08-31 and 2026-09-30, which is 13 boundaries and 12 periods.

        ``as_of`` 2026-05-01, ``"month"``, 3 periods, on a weekday calendar where 2026-05-01 is not
        a session: the first target 2026-05-01 resolves to 2026-04-30, which also closes April, so
        the walk continues from March and returns 2026-01-30, 2026-02-27, 2026-03-31 and
        2026-04-30.

    Args:
        sessions: Observed market sessions, sorted ascending and distinct.
        interval: Period length.
        periods: Number of periods wanted, at least 1.
        as_of: Last day of the latest period.

    Returns:
        ``periods + 1`` distinct sessions, ascending, or fewer when the history runs out.

    Raises:
        ValueError: If ``periods`` is below 1 or ``interval`` is not a :data:`HeatmapInterval`.
    """
    if periods < 1:
        raise ValueError(f"periods must be >= 1, got {periods}")
    if interval == "month":
        target: date | None = min(_month_end_of(as_of), as_of)
        step_back = _month_end_before
    elif interval == "week":
        target = min(_friday_of(as_of), as_of)
        step_back = _friday_before
    else:
        raise ValueError(f"unknown heatmap interval {interval!r}")
    # Every month-end or Friday from a boundary's own target down to that boundary resolves to the
    # boundary again, so the walk jumps straight below it: one bisection per boundary, however long
    # a stretch without sessions (as_of years after the data) is.
    boundaries: list[date] = []
    while target is not None and len(boundaries) <= periods:
        index = bisect_right(sessions, target)
        if index == 0:
            break
        boundary = sessions[index - 1]
        boundaries.append(boundary)
        target = step_back(boundary)
    boundaries.reverse()
    return boundaries


def sector_returns(
    members: Sequence[HeatmapMember],
    boundaries: Sequence[date],
) -> HeatmapResult:
    """Compute capitalization-weighted sector and market returns between consecutive boundaries.

    For each period from boundary ``s`` to boundary ``e``, a member qualifies when it has a close
    on or before each boundary that is at most :data:`CLOSE_MAX_AGE_DAYS` calendar days old. Over
    a set of qualifying members::

        return_pct = (sum(shares * close_e) / sum(shares * close_s) - 1) * 100

    in percentage points. Each sector's set is its own qualifying members, and the market's set is
    every qualifying member. A set that is empty, or whose starting capitalization sums to zero
    (every member has zero shares), has a ``None`` return. A sector's ``relative_return_pct`` is
    its return minus the market's when both are defined, else ``None``.

    Args:
        members: The equities to aggregate, each with its sector, share count and prices.
        boundaries: Boundary sessions, strictly ascending, as returned by
            :func:`period_boundaries`.

    Returns:
        One cell per period for the market and for every sector present in ``members``. Fewer than
        two boundaries define no period, so every cell list is empty.

    Raises:
        ValueError: If ``boundaries`` is not strictly ascending.
    """
    for index in range(1, len(boundaries)):
        if boundaries[index] <= boundaries[index - 1]:
            raise ValueError("boundaries must be strictly ascending")

    sector_names = sorted({member.sector for member in members})
    market_cells: list[HeatmapCellResult] = []
    sector_cells: dict[str, list[HeatmapCellResult]] = {name: [] for name in sector_names}

    for index in range(1, len(boundaries)):
        start, end = boundaries[index - 1], boundaries[index]
        market = _CapTotals()
        by_sector = {name: _CapTotals() for name in sector_names}
        for member in members:
            start_close = close_as_of(member.prices, start, max_age_days=CLOSE_MAX_AGE_DAYS)
            if start_close is None:
                continue
            end_close = close_as_of(member.prices, end, max_age_days=CLOSE_MAX_AGE_DAYS)
            if end_close is None:
                continue
            start_cap_kes = start_close.value * member.shares_outstanding
            end_cap_kes = end_close.value * member.shares_outstanding
            market.add(start_cap_kes, end_cap_kes)
            by_sector[member.sector].add(start_cap_kes, end_cap_kes)

        market_return = market.return_pct()
        market_cells.append(HeatmapCellResult(end, market_return, None, market.constituents))
        for name in sector_names:
            totals = by_sector[name]
            sector_return = totals.return_pct()
            relative = (
                None
                if sector_return is None or market_return is None
                else sector_return - market_return
            )
            sector_cells[name].append(
                HeatmapCellResult(end, sector_return, relative, totals.constituents)
            )

    return HeatmapResult(
        period_ends=list(boundaries[1:]),
        market=market_cells,
        sectors=[SectorHeatmapResult(name, sector_cells[name]) for name in sector_names],
    )
