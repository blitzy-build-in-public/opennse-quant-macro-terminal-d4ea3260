"""Event and election cycle backtester.

Maps NSE price, turnover, valuation and foreign-flow behaviour over a window around each event
date (an election by default) onto one normalized, event-relative axis, so different cycles
overlay on the same chart. Every point carries ``offset_days = trade_date - event_date``.

Conventions shared with the rest of the quant engine:

* Arithmetic is ``Decimal`` under the default context and nothing here rounds; services quantize
  index levels with ``"index"``, P/E with ``"ratio"``, percentages with ``"pct"`` and KES with
  ``"money"``.
* An undefined value is ``None`` and the reason is a code in the cycle's ``flags``.
* Sessions are the observed market calendar: the distinct trade dates present in the price table.
  Callers pass sessions and price rows starting at least :data:`MA_SESSIONS` market sessions
  before ``window_start`` (the MA20 warm-up and the :data:`BASE_CLOSE_MAX_SESSIONS` base-close
  lookback read before the window) and running through ``window_end`` or the end of the data.

Normalization, per cycle, over a cohort fixed at the anchor (the first session in the window):

* ``price_index_t = 100 * cap_t / cap_anchor`` with ``cap_t = sum(shares * close_t)``;
* ``turnover_index_t = 100 * MA20_t / MA20_anchor``, MA20 being the mean cohort turnover over the
  observed sessions among the last 20 market sessions;
* ``pe_ratio_t = sum(shares * close_t) / sum(net income)`` over the members with available
  earnings on ``t`` (a matched set), and ``pe_index_t = 100 * pe_t / pe_anchor``;
* ``cumulative_foreign_net_kes_t``, the running sum of the cohort's foreign net flow from the
  anchor.

Future or partly observed events are returned as ``awaiting_data`` or ``partial``. Their
:class:`CycleProjection` is the mean of the complete cycles' normalized paths: a descriptive
average of past behaviour, not a forecast, and never stored.
"""

from __future__ import annotations

import calendar
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from decimal import Decimal
from statistics import mean
from typing import Final, Literal

from opennse_terminal.quant.asof import (
    DEFAULT_FUNDAMENTALS_LAG_DAYS,
    FundamentalsRow,
    MacroColumn,
    MacroRow,
    PriceRow,
    fundamentals_as_of,
    macro_as_of,
)
from opennse_terminal.quant.numeric import HUNDRED, ZERO, Violation, safe_div

__all__ = [
    "BASE_CLOSE_MAX_SESSIONS",
    "DEFAULT_POST_DAYS",
    "DEFAULT_POST_MONTHS",
    "DEFAULT_PRE_DAYS",
    "DEFAULT_PRE_MONTHS",
    "MAX_WINDOW_DAYS",
    "MAX_WINDOW_MONTHS",
    "MA_MIN_OBSERVATIONS",
    "MA_SESSIONS",
    "MIN_PROJECTION_BASIS",
    "OVERLAY_MAX_STALENESS_DAYS",
    "BacktestCandidate",
    "CycleBacktestResult",
    "CycleEvent",
    "CycleMarkerResult",
    "CycleMarkersResult",
    "CyclePointResult",
    "CycleProjection",
    "CycleStatus",
    "CycleSummaryResult",
    "CycleWindow",
    "MarkerMetric",
    "ProjectionPoint",
    "add_months",
    "build_projection",
    "resolve_window",
    "run_cycle_backtest",
    "with_projection",
]

DEFAULT_PRE_DAYS: Final = 180
"""Calendar days before the event in the default window (the "T-180 to T+180" contract)."""
DEFAULT_POST_DAYS: Final = 180
"""Calendar days after the event in the default window."""
DEFAULT_PRE_MONTHS: Final = 12
"""Months before the event when the months form is used without ``pre_months``."""
DEFAULT_POST_MONTHS: Final = 12
"""Months after the event when the months form is used without ``post_months``."""
MAX_WINDOW_DAYS: Final = 365
"""Largest accepted ``pre_days`` or ``post_days``."""
MAX_WINDOW_MONTHS: Final = 12
"""Largest accepted ``pre_months`` or ``post_months``."""
BASE_CLOSE_MAX_SESSIONS: Final = 10
"""Market sessions before the anchor within which a candidate's base close must lie."""
MA_SESSIONS: Final = 20
"""Market sessions spanned by the trailing turnover mean (MA20)."""
MA_MIN_OBSERVATIONS: Final = 5
"""Observed sessions the trailing turnover mean needs to be defined."""
OVERLAY_MAX_STALENESS_DAYS: Final = 62
"""Oldest macro observation, in calendar days, an overlay value may come from."""
MIN_PROJECTION_BASIS: Final = 2
"""Complete cycles a projection must average over."""

CycleStatus = Literal["complete", "partial", "awaiting_data", "no_constituents"]
"""Data state of one cycle: window fully observed, partly observed, not yet open, or no cohort."""

MarkerMetric = Literal["pe_ratio", "price_index", "turnover_index"]
"""The point field a :class:`CycleMarkerResult` was taken from."""

_PointMetric = Literal["price_index", "turnover_index", "pe_ratio", "pe_index"]

_PROJECTED_STATUSES: Final[frozenset[str]] = frozenset({"partial", "awaiting_data"})


def add_months(d: date, months: int) -> date:
    """Shift ``d`` by whole calendar months, clamping the day to the target month's length.

    ``months`` may be negative. ``add_months(date(2024, 2, 29), 12)`` is 2025-02-28 and
    ``add_months(date(2027, 8, 10), -12)`` is 2026-08-10.

    Raises:
        ValueError: If the result falls outside the years ``date`` supports.
    """
    year, month_index = divmod(d.year * 12 + d.month - 1 + months, 12)
    month = month_index + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


@dataclass(frozen=True, slots=True)
class CycleWindow:
    """The calendar window around one event.

    Only the form that produced the window is set: ``pre_days``/``post_days`` for the days form,
    ``pre_months``/``post_months`` for the months form; the other pair is ``None``.
    """

    event_date: date
    window_start: date
    window_end: date
    pre_days: int | None = None
    post_days: int | None = None
    pre_months: int | None = None
    post_months: int | None = None


def _require_range(name: str, value: int, upper: int) -> None:
    if not 1 <= value <= upper:
        raise ValueError(f"{name} must be between 1 and {upper}, got {value}")


def resolve_window(
    event_date: date,
    *,
    pre_days: int | None = None,
    post_days: int | None = None,
    pre_months: int | None = None,
    post_months: int | None = None,
) -> CycleWindow | Violation:
    """Resolve the window ``[T - pre, T + post]`` around ``event_date`` (T).

    The window is given in exactly one of two forms:

    * **months**, used when ``pre_months`` or ``post_months`` is set: calendar-month arithmetic by
      :func:`add_months`, a missing side defaulting to 12;
    * **days**, used otherwise: calendar days, a missing side defaulting to 180.

    Returns:
        The window, or a ``conflicting_window_units`` :class:`Violation` (reported against
        ``pre_months``) when a day field and a month field are both supplied. The route raises the
        violation as a coded 422; it is a returned value because the rule spans several fields.

    Raises:
        ValueError: If a day value is outside 1-365 or a month value outside 1-12. The API bounds
            each field before calling, so this signals a caller error.
    """
    days_given = pre_days is not None or post_days is not None
    months_given = pre_months is not None or post_months is not None
    if days_given and months_given:
        return Violation(
            code="conflicting_window_units",
            field="pre_months",
            message=(
                "Give the window either in days (pre_days, post_days) or in months "
                "(pre_months, post_months), not both."
            ),
        )
    if months_given:
        before = DEFAULT_PRE_MONTHS if pre_months is None else pre_months
        after = DEFAULT_POST_MONTHS if post_months is None else post_months
        _require_range("pre_months", before, MAX_WINDOW_MONTHS)
        _require_range("post_months", after, MAX_WINDOW_MONTHS)
        return CycleWindow(
            event_date=event_date,
            window_start=add_months(event_date, -before),
            window_end=add_months(event_date, after),
            pre_months=before,
            post_months=after,
        )
    before = DEFAULT_PRE_DAYS if pre_days is None else pre_days
    after = DEFAULT_POST_DAYS if post_days is None else post_days
    _require_range("pre_days", before, MAX_WINDOW_DAYS)
    _require_range("post_days", after, MAX_WINDOW_DAYS)
    return CycleWindow(
        event_date=event_date,
        window_start=event_date - timedelta(days=before),
        window_end=event_date + timedelta(days=after),
        pre_days=before,
        post_days=after,
    )


@dataclass(frozen=True, slots=True)
class CycleEvent:
    """One event to backtest.

    Attributes:
        label: Display label, from ``terminal_cycles`` or ``"Event YYYY-MM-DD"`` for an ad hoc
            date.
        event_date: The event day, T.
    """

    label: str
    event_date: date


@dataclass(frozen=True, slots=True)
class BacktestCandidate:
    """One equity that may join a cycle's cohort.

    Attributes:
        ticker: Exchange ticker.
        shares_outstanding: Shares used for every capitalization in the cycle.
        prices: The ticker's actual sessions, sorted ascending by ``trade_date``; never
            zero-filled or forward-filled.
        fundamentals: The company's reporting periods, sorted ascending by ``period_end_date``.
        currency: Trading currency; fundamentals in another currency are left out of P/E.
    """

    ticker: str
    shares_outstanding: int
    prices: Sequence[PriceRow]
    fundamentals: Sequence[FundamentalsRow]
    currency: str = "KES"


@dataclass(frozen=True, slots=True)
class CyclePointResult:
    """One market session of a cycle, on the event-relative axis.

    Index levels are base 100 at the anchor. ``pe_ratio`` is a multiple, the flow is KES and the
    overlays are percentage points as of the session (each at most 62 days old).
    """

    offset_days: int
    trade_date: date
    price_index: Decimal | None = None
    turnover_index: Decimal | None = None
    pe_ratio: Decimal | None = None
    pe_index: Decimal | None = None
    cumulative_foreign_net_kes: Decimal | None = None
    inflation_yoy_pct: Decimal | None = None
    tbill_91d_pct: Decimal | None = None
    tbill_364d_pct: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CycleMarkerResult:
    """A point singled out on the cycle chart, with the metric and value that selected it."""

    offset_days: int
    trade_date: date
    metric: MarkerMetric
    value: Decimal


@dataclass(frozen=True, slots=True)
class CycleMarkersResult:
    """The valuation floor (minimum P/E, else minimum price index) and the volume exhaustion
    point (minimum turnover index); each ``None`` when its metric is null throughout."""

    valuation_floor: CycleMarkerResult | None
    volume_exhaustion: CycleMarkerResult | None


@dataclass(frozen=True, slots=True)
class CycleSummaryResult:
    """Cycle returns in percentage points; "index at T" is the last point on or before T.

    Attributes:
        pre_event_return_pct: ``(index at T / 100 - 1) * 100``.
        post_event_return_pct: ``(last index / index at T - 1) * 100``; ``None`` when no point
            lies after T.
        max_drawdown_pct: ``min_t (index_t / running maximum - 1) * 100``.
        pe_compression_pct: ``(P/E at the valuation floor / anchor P/E - 1) * 100``; ``None``
            when the floor came from the price-index fallback.
    """

    pre_event_return_pct: Decimal | None
    post_event_return_pct: Decimal | None
    max_drawdown_pct: Decimal | None
    pe_compression_pct: Decimal | None


@dataclass(frozen=True, slots=True)
class ProjectionPoint:
    """The mean normalized levels of the complete cycles at one calendar offset."""

    offset_days: int
    price_index: Decimal | None
    turnover_index: Decimal | None
    pe_index: Decimal | None


@dataclass(frozen=True, slots=True)
class CycleProjection:
    """A labelled average of past cycles' paths, never a forecast and never stored.

    Attributes:
        basis: Labels of the complete cycles averaged, in input order.
        points: One point per calendar offset of the target window with any defined mean.
        is_projection: Always ``True``, so a consumer cannot mistake it for observed data.
    """

    basis: list[str]
    points: list[ProjectionPoint]
    is_projection: Literal[True] = True


@dataclass(frozen=True, slots=True)
class CycleBacktestResult:
    """One cycle of a backtest; field names match the API's ``CycleResult``.

    ``points`` is empty, and ``markers`` and ``summary`` are ``None``, for the statuses
    ``awaiting_data`` and ``no_constituents``. ``constituents`` and ``excluded_constituents`` are
    sorted tickers.
    """

    label: str
    event_date: date
    window_start: date
    window_end: date
    status: CycleStatus
    anchor_date: date | None = None
    days_until_window_start: int | None = None
    constituents: list[str] = field(default_factory=list)
    excluded_constituents: list[str] = field(default_factory=list)
    points: list[CyclePointResult] = field(default_factory=list)
    markers: CycleMarkersResult | None = None
    summary: CycleSummaryResult | None = None
    projection: CycleProjection | None = None
    flags: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _Member:
    """A cohort member with its fixed share count and its base close at the anchor."""

    candidate: BacktestCandidate
    shares: Decimal
    base_close: Decimal


def _select_cohort(
    candidates: Sequence[BacktestCandidate], anchor: date, earliest_base: date
) -> tuple[list[_Member], list[str]]:
    """Split ``candidates`` into the cohort and the excluded tickers, both ordered by ticker.

    A candidate's base close is its latest actual close on or before ``anchor`` and no earlier
    than ``earliest_base``. Requiring an observed close at or before the anchor keeps a counter
    that first trades inside the window from entering mid-cycle and creating an artificial
    return; a candidate without a base close is excluded.
    """
    members: list[_Member] = []
    excluded: list[str] = []
    for candidate in sorted(candidates, key=lambda item: item.ticker):
        position = bisect_right(candidate.prices, anchor, key=lambda row: row.trade_date)
        if position and candidate.prices[position - 1].trade_date >= earliest_base:
            base_close = candidate.prices[position - 1].close_price
            members.append(_Member(candidate, Decimal(candidate.shares_outstanding), base_close))
        else:
            excluded.append(candidate.ticker)
    return members, excluded


def _closes_by_day(member: _Member, days: Sequence[date]) -> list[Decimal]:
    """Return ``member``'s close on each of ``days``, which begin at the anchor.

    The anchor takes the base close. Each later day takes the latest actual close on or before
    it, so a missing session is forward-filled from the member's own last actual close only.
    """
    prices = member.candidate.prices
    position = bisect_right(prices, days[0], key=lambda row: row.trade_date)
    current = member.base_close
    closes: list[Decimal] = []
    for day in days:
        while position < len(prices) and prices[position].trade_date <= day:
            current = prices[position].close_price
            position += 1
        closes.append(current)
    return closes


def _daily_totals(
    members: Sequence[_Member], first: date, last: date
) -> tuple[dict[date, Decimal], dict[date, Decimal]]:
    """Sum the cohort's actual rows per session between ``first`` and ``last`` inclusive.

    Returns:
        ``(turnover_kes by day, foreign_net_flow_kes by day)``. A day appears in the turnover map
        only when at least one member has a row, and in the flow map only when at least one of
        those rows carries a flow value.
    """
    turnover: dict[date, Decimal] = {}
    flows: dict[date, Decimal] = {}
    for member in members:
        prices = member.candidate.prices
        low = bisect_left(prices, first, key=lambda row: row.trade_date)
        high = bisect_right(prices, last, key=lambda row: row.trade_date)
        for row in prices[low:high]:
            turnover[row.trade_date] = turnover.get(row.trade_date, ZERO) + row.turnover_kes
            if row.foreign_net_flow_kes is not None:
                flows[row.trade_date] = flows.get(row.trade_date, ZERO) + row.foreign_net_flow_kes
    return turnover, flows


def _ma20(
    turnover_by_day: dict[date, Decimal], sessions: Sequence[date], index: int
) -> Decimal | None:
    """Mean cohort turnover over the observed sessions among the :data:`MA_SESSIONS` market
    sessions ending at ``sessions[index]``; ``None`` below :data:`MA_MIN_OBSERVATIONS`."""
    span = sessions[max(0, index - MA_SESSIONS + 1) : index + 1]
    observed = [turnover_by_day[day] for day in span if day in turnover_by_day]
    if len(observed) < MA_MIN_OBSERVATIONS:
        return None
    return mean(observed)


def _pe_at(
    members: Sequence[_Member], closes: Sequence[Decimal], day: date, lag_days: int
) -> Decimal | None:
    """Cohort P/E on ``day`` over the matched set of members with available earnings.

    The matched set holds the members whose latest fiscal year available on ``day`` (under the
    ``lag_days`` publication lag) has net income in the member's own currency. Capitalization
    and earnings are summed over that same set, so a member without earnings never inflates the
    numerator. ``None`` when the set is empty or its summed earnings are zero or negative.
    """
    capitalization = ZERO
    earnings = ZERO
    for member, close in zip(members, closes, strict=True):
        row = fundamentals_as_of(member.candidate.fundamentals, day, lag_days)
        if row is None or row.net_income_kes is None or row.currency != member.candidate.currency:
            continue
        capitalization += member.shares * close
        earnings += row.net_income_kes
    if earnings <= ZERO:
        return None
    return capitalization / earnings


def _rebase(value: Decimal | None, base: Decimal | None) -> Decimal | None:
    """``100 * value / base``, or ``None`` when either is missing or ``base`` is zero."""
    if value is None or base is None:
        return None
    return safe_div(HUNDRED * value, base)


def _overlay(rows: Sequence[MacroRow], column: MacroColumn, day: date) -> Decimal | None:
    resolved = macro_as_of(rows, column, day, OVERLAY_MAX_STALENESS_DAYS)
    return None if resolved is None else resolved.value


def _point_value(point: CyclePointResult, metric: _PointMetric) -> Decimal | None:
    if metric == "price_index":
        return point.price_index
    if metric == "turnover_index":
        return point.turnover_index
    if metric == "pe_ratio":
        return point.pe_ratio
    return point.pe_index


def _min_marker(
    points: Sequence[CyclePointResult], metric: MarkerMetric
) -> CycleMarkerResult | None:
    """The point with the minimum non-null ``metric``; the earliest wins a tie."""
    best: tuple[CyclePointResult, Decimal] | None = None
    for point in points:
        value = _point_value(point, metric)
        if value is not None and (best is None or value < best[1]):
            best = (point, value)
    if best is None:
        return None
    point, value = best
    return CycleMarkerResult(point.offset_days, point.trade_date, metric, value)


def _markers(points: Sequence[CyclePointResult]) -> CycleMarkersResult:
    """The valuation floor (minimum P/E, else minimum price index) and the volume exhaustion
    point (minimum turnover index)."""
    floor = _min_marker(points, "pe_ratio")
    if floor is None:
        floor = _min_marker(points, "price_index")
    return CycleMarkersResult(
        valuation_floor=floor, volume_exhaustion=_min_marker(points, "turnover_index")
    )


def _summary(
    points: Sequence[CyclePointResult],
    event_date: date,
    floor: CycleMarkerResult | None,
    pe_base: Decimal | None,
) -> CycleSummaryResult:
    """Returns around the event, the maximum drawdown and the P/E compression of a cycle.

    ``points`` is non-empty and ascending. "Index at T" is the price index of the last point on
    or before ``event_date``; ``pe_base`` is the anchor P/E, or ``None`` when it is unusable.
    """
    pre_event = [point for point in points if point.trade_date <= event_date]
    index_at_event = pre_event[-1].price_index if pre_event else None
    last = points[-1]

    pre_event_return: Decimal | None = None
    post_event_return: Decimal | None = None
    if index_at_event is not None:
        pre_event_return = (index_at_event / HUNDRED - 1) * HUNDRED
        if last.trade_date > event_date and last.price_index is not None:
            growth = safe_div(last.price_index, index_at_event)
            post_event_return = None if growth is None else (growth - 1) * HUNDRED

    running_max: Decimal | None = None
    max_drawdown: Decimal | None = None
    for point in points:
        level = point.price_index
        if level is None:
            continue
        running_max = level if running_max is None else max(running_max, level)
        ratio = safe_div(level, running_max)
        drawdown = None if ratio is None else (ratio - 1) * HUNDRED
        if drawdown is not None and (max_drawdown is None or drawdown < max_drawdown):
            max_drawdown = drawdown

    pe_compression: Decimal | None = None
    if floor is not None and floor.metric == "pe_ratio" and pe_base is not None:
        pe_compression = (floor.value / pe_base - 1) * HUNDRED

    return CycleSummaryResult(
        pre_event_return_pct=pre_event_return,
        post_event_return_pct=post_event_return,
        max_drawdown_pct=max_drawdown,
        pe_compression_pct=pe_compression,
    )


def _awaiting_data(
    event: CycleEvent, window: CycleWindow, data_available_through: date | None
) -> CycleBacktestResult:
    """A cycle with no observed session inside its window.

    ``days_until_window_start`` is set when the data ends before the window opens.
    """
    days_until: int | None = None
    if data_available_through is not None and data_available_through < window.window_start:
        days_until = (window.window_start - data_available_through).days
    return CycleBacktestResult(
        label=event.label,
        event_date=event.event_date,
        window_start=window.window_start,
        window_end=window.window_end,
        status="awaiting_data",
        days_until_window_start=days_until,
    )


def run_cycle_backtest(
    event: CycleEvent,
    window: CycleWindow,
    candidates: Sequence[BacktestCandidate],
    sessions: Sequence[date],
    macro_rows: Sequence[MacroRow],
    *,
    data_available_through: date | None,
    lag_days: int = DEFAULT_FUNDAMENTALS_LAG_DAYS,
) -> CycleBacktestResult:
    """Backtest one event over ``window``.

    The anchor is the first market session in the window. The cohort C is every candidate with
    an actual close on the anchor or one of the :data:`BASE_CLOSE_MAX_SESSIONS` market sessions
    before it, and is fixed for the cycle. For each session t from the anchor to the last
    observed session in the window:

    * ``price_index_t = 100 * cap_t / cap_anchor``, ``cap_t = sum_C(shares * close_t)``, each
      member's close forward-filled from its own last actual close after the anchor;
    * ``turnover_index_t = 100 * MA20_t / MA20_anchor``, MA20 being the mean of the cohort's
      summed turnover over the sessions with rows among the last 20 market sessions (at least
      5), read back before the window;
    * ``pe_ratio_t = sum_E(shares * close_t) / sum_E(net income)`` over the matched set E_t of
      members with earnings available on t, ``None`` when E_t is empty or its earnings are not
      positive; ``pe_index_t = 100 * pe_t / pe_anchor``;
    * ``cumulative_foreign_net_kes_t``: the running sum from the anchor of the cohort's non-null
      foreign net flows (``None`` until the first one);
    * inflation and 91/364-day T-bill overlays as of t, at most
      :data:`OVERLAY_MAX_STALENESS_DAYS` old. Overlays feed neither markers nor the summary.

    Undefined normalizations are ``None`` throughout and flagged: ``zero_anchor_capitalization``,
    ``insufficient_turnover_history``, ``zero_anchor_turnover``, ``pe_anchor_unavailable``, and
    ``no_pre_event_data`` when the window's data begins after the event.

    Args:
        event: The event and its label.
        window: Its resolved window (see :func:`resolve_window`).
        candidates: Equities that may join the cohort, each with ascending actual rows.
        sessions: Ascending, distinct observed market sessions, starting at least
            :data:`MA_SESSIONS` sessions before ``window.window_start``.
        macro_rows: Macro observations, ascending by ``observation_date``.
        data_available_through: Latest trade date among the candidates; ``None`` when there is
            no data.
        lag_days: Publication lag applied to fiscal-year fundamentals.

    Returns:
        The cycle with status ``complete`` (data reaches ``window_end``), ``partial`` (data ends
        inside the window), ``awaiting_data`` (no session inside the window yet; ``points`` empty,
        ``markers`` and ``summary`` ``None``) or ``no_constituents`` (flag ``empty_cohort``, same
        empty shape). ``projection`` is left ``None``; see :func:`with_projection`.

    Raises:
        ValueError: If ``window`` was resolved for a different event date.
    """
    if window.event_date != event.event_date:
        raise ValueError(
            f"window is for {window.event_date.isoformat()}, event is "
            f"{event.event_date.isoformat()}"
        )
    if data_available_through is None:
        return _awaiting_data(event, window, None)
    start = bisect_left(sessions, window.window_start)
    end = bisect_right(sessions, min(window.window_end, data_available_through))
    if start >= end:
        return _awaiting_data(event, window, data_available_through)

    anchor = sessions[start]
    earliest_base = sessions[max(0, start - BASE_CLOSE_MAX_SESSIONS)]
    members, excluded = _select_cohort(candidates, anchor, earliest_base)
    if not members:
        return CycleBacktestResult(
            label=event.label,
            event_date=event.event_date,
            window_start=window.window_start,
            window_end=window.window_end,
            status="no_constituents",
            anchor_date=anchor,
            excluded_constituents=excluded,
            flags=["empty_cohort"],
        )

    days = sessions[start:end]
    member_closes = [_closes_by_day(member, days) for member in members]
    day_closes = [[closes[k] for closes in member_closes] for k in range(len(days))]
    turnover_by_day, flow_by_day = _daily_totals(
        members, sessions[max(0, start - MA_SESSIONS + 1)], days[-1]
    )

    flags: list[str] = []
    caps = [
        sum((member.shares * close for member, close in zip(members, closes, strict=True)), ZERO)
        for closes in day_closes
    ]
    cap_anchor = caps[0]
    if cap_anchor.is_zero():
        flags.append("zero_anchor_capitalization")
    ma_anchor = _ma20(turnover_by_day, sessions, start)
    if ma_anchor is None:
        flags.append("insufficient_turnover_history")
    elif ma_anchor.is_zero():
        flags.append("zero_anchor_turnover")
    pe_values = [
        _pe_at(members, closes, day, lag_days) for closes, day in zip(day_closes, days, strict=True)
    ]
    pe_anchor = pe_values[0]
    pe_base = pe_anchor if pe_anchor is not None and pe_anchor > ZERO else None
    if pe_base is None:
        flags.append("pe_anchor_unavailable")

    points: list[CyclePointResult] = []
    cumulative_flow: Decimal | None = None
    for k, day in enumerate(days):
        flow = flow_by_day.get(day)
        if flow is not None:
            cumulative_flow = flow if cumulative_flow is None else cumulative_flow + flow
        points.append(
            CyclePointResult(
                offset_days=(day - event.event_date).days,
                trade_date=day,
                price_index=_rebase(caps[k], cap_anchor),
                turnover_index=_rebase(_ma20(turnover_by_day, sessions, start + k), ma_anchor),
                pe_ratio=pe_values[k],
                pe_index=_rebase(pe_values[k], pe_base),
                cumulative_foreign_net_kes=cumulative_flow,
                inflation_yoy_pct=_overlay(macro_rows, "inflation_yoy_pct", day),
                tbill_91d_pct=_overlay(macro_rows, "tbill_91d_pct", day),
                tbill_364d_pct=_overlay(macro_rows, "tbill_364d_pct", day),
            )
        )
    if points[0].trade_date > event.event_date:
        flags.append("no_pre_event_data")

    markers = _markers(points)
    status: CycleStatus = "complete" if data_available_through >= window.window_end else "partial"
    return CycleBacktestResult(
        label=event.label,
        event_date=event.event_date,
        window_start=window.window_start,
        window_end=window.window_end,
        status=status,
        anchor_date=anchor,
        constituents=[member.candidate.ticker for member in members],
        excluded_constituents=excluded,
        points=points,
        markers=markers,
        summary=_summary(points, event.event_date, markers.valuation_floor, pe_base),
        flags=flags,
    )


def _mean_or_none(values: Sequence[Decimal]) -> Decimal | None:
    return mean(values) if values else None


def build_projection(
    target: CycleBacktestResult, complete_cycles: Sequence[CycleBacktestResult]
) -> CycleProjection | None:
    """Average the complete cycles' normalized paths over ``target``'s window offsets.

    For every calendar offset ``o`` of the target window, each of ``price_index``,
    ``turnover_index`` and ``pe_index`` is the mean across the complete cycles of the cycle's
    last point with ``offset_days <= o``. A cycle contributes only where ``o`` lies inside its
    own window and that point's value is non-null. Offsets where every mean is undefined are
    omitted.

    This is a descriptive average of past behaviour, not a forecast.

    Returns:
        The projection, with ``basis`` listing the averaged cycles' labels in input order, or
        ``None`` when fewer than :data:`MIN_PROJECTION_BASIS` cycles in ``complete_cycles`` have
        status ``complete``.
    """
    basis = [cycle for cycle in complete_cycles if cycle.status == "complete"]
    if len(basis) < MIN_PROJECTION_BASIS:
        return None
    tracks = [
        (
            (cycle.window_start - cycle.event_date).days,
            (cycle.window_end - cycle.event_date).days,
            [point.offset_days for point in cycle.points],
            cycle.points,
        )
        for cycle in basis
    ]
    metrics: tuple[_PointMetric, ...] = ("price_index", "turnover_index", "pe_index")
    first = (target.window_start - target.event_date).days
    last = (target.window_end - target.event_date).days
    points: list[ProjectionPoint] = []
    for offset in range(first, last + 1):
        values: dict[_PointMetric, list[Decimal]] = {metric: [] for metric in metrics}
        for track_start, track_end, offsets, track_points in tracks:
            position = bisect_right(offsets, offset)
            if not track_start <= offset <= track_end or position == 0:
                continue
            point = track_points[position - 1]
            for metric in metrics:
                value = _point_value(point, metric)
                if value is not None:
                    values[metric].append(value)
        price_index = _mean_or_none(values["price_index"])
        turnover_index = _mean_or_none(values["turnover_index"])
        pe_index = _mean_or_none(values["pe_index"])
        if price_index is not None or turnover_index is not None or pe_index is not None:
            points.append(ProjectionPoint(offset, price_index, turnover_index, pe_index))
    return CycleProjection(basis=[cycle.label for cycle in basis], points=points)


def with_projection(
    target: CycleBacktestResult, complete_cycles: Sequence[CycleBacktestResult]
) -> CycleBacktestResult:
    """Attach :func:`build_projection` to a ``partial`` or ``awaiting_data`` cycle.

    When no projection can be built, the cycle gains the flag ``insufficient_projection_basis``.
    Cycles with any other status are returned unchanged.
    """
    if target.status not in _PROJECTED_STATUSES:
        return target
    projection = build_projection(target, complete_cycles)
    flags = list(target.flags)
    if projection is None:
        flags.append("insufficient_projection_basis")
    return replace(target, projection=projection, flags=flags)
