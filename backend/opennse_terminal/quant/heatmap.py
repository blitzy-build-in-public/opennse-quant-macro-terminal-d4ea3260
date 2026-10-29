"""Sector rotation returns for the market overview heatmap.

The heatmap compares capitalization-weighted price returns per sector against the whole market,
period by period. Period boundaries are the last observed market sessions on or before each
month-end or Friday: the distinct ``trade_date`` values the database holds, never a holiday list,
so a calendar error cannot move a boundary onto a day without prices.

Returns are in percentage points (``Decimal("10")`` means +10%) at full context precision; services
round them once with ``quantize_out(value, "pct")``.
"""

from __future__ import annotations

from bisect import bisect_right
from calendar import monthrange
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import MINYEAR, date, timedelta
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


def _month_end_targets(as_of: date, periods: int) -> list[date]:
    """Month-ends of ``as_of``'s month and of the ``periods`` months before it, newest first."""
    targets: list[date] = []
    month_number = as_of.year * 12 + as_of.month - 1
    for offset in range(periods + 1):
        year, month_index = divmod(month_number - offset, 12)
        if year < MINYEAR:
            break
        month = month_index + 1
        targets.append(date(year, month, monthrange(year, month)[1]))
    return targets


def _friday_targets(as_of: date, periods: int) -> list[date]:
    """The Friday of ``as_of``'s ISO week and the ``periods`` Fridays before it, newest first."""
    # ISO weeks run Monday to Sunday, so a weekend as_of maps back to the Friday just gone. The
    # Friday of any representable week is representable, because date.max is itself a Friday.
    friday = as_of + timedelta(days=_FRIDAY - as_of.weekday())
    targets: list[date] = []
    for offset in range(periods + 1):
        try:
            targets.append(friday - timedelta(weeks=offset))
        except OverflowError:
            break
    return targets


def period_boundaries(
    sessions: Sequence[date],
    interval: HeatmapInterval,
    periods: int,
    as_of: date,
) -> list[date]:
    """Return the boundary sessions of the ``periods`` periods ending at ``as_of``.

    Each target date is a month-end (``interval="month"``) or a Friday (``interval="week"``):
    the one of ``as_of``'s month or ISO week and the ``periods`` before it. A target after
    ``as_of`` is clamped to ``as_of``, so the latest period runs to date. Each boundary is the last
    observed market session on or before its clamped target. Targets with no session on or before
    them are dropped, and targets that resolve to the same session collapse into one, so sparse
    data gives fewer boundaries.

    ``boundaries[0]`` opens the first period and ``boundaries[1:]`` are the period ends, ready for
    :func:`sector_returns`.

    Example:
        ``as_of`` 2026-09-30, ``"month"``, 12 periods: the last sessions on or before 2025-09-30,
        2025-10-31, ..., 2026-08-31 and 2026-09-30, which is 13 boundaries and 12 periods.

    Args:
        sessions: Observed market sessions, sorted ascending and distinct.
        interval: Period length.
        periods: Number of periods wanted, at least 1.
        as_of: Last day of the latest period.

    Returns:
        Up to ``periods + 1`` distinct sessions, ascending.

    Raises:
        ValueError: If ``periods`` is below 1 or ``interval`` is not a :data:`HeatmapInterval`.
    """
    if periods < 1:
        raise ValueError(f"periods must be >= 1, got {periods}")
    if interval == "month":
        targets = _month_end_targets(as_of, periods)
    elif interval == "week":
        targets = _friday_targets(as_of, periods)
    else:
        raise ValueError(f"unknown heatmap interval {interval!r}")
    boundaries: set[date] = set()
    for target in targets:
        index = bisect_right(sessions, min(target, as_of))
        if index > 0:
            boundaries.add(sessions[index - 1])
    return sorted(boundaries)


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
