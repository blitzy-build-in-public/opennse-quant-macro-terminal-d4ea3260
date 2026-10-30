"""Event and election cycle backtest service: ``GET /api/v1/terminal/cycles/backtest``.

Resolves the request (window form, event dates and candidate equities), reads the stored history
through SQLAlchemy 2.0 async ``select()`` statements with bound parameters only, runs the pure
:func:`~opennse_terminal.quant.backtest.run_cycle_backtest` once per event in a worker thread
(:func:`~opennse_terminal.services.offload.compute`, which stops before the next event once the
client has gone away), attaches the labelled projection to partial and awaiting-data cycles when
at least two cycles are complete, and rounds every output value exactly once with
:func:`~opennse_terminal.quant.numeric.quantize_out` while building the ``schemas.cycles``
response. Every formula lives in ``quant.backtest``; nothing here converts to ``float``.

Only stored rows are read, seeded or ingested alike. When at least two cycles are complete, a
future event such as the 2027 election is answered with their descriptive projection, which is
computed per request and never written to the database; no future-dated row is ever assumed.

Each request issues a constant number of statements, whatever the number of events, and every
read of price, session or macro history is bounded by the requested windows, so its cost does not
grow with stored history between or outside them. The observed sessions of every cycle are one
distinct ``trade_date`` query over the windows up to the end of the candidates' data, and one
``UNION`` of backward queries, each limited to the 30 sessions before one window. The price
history of every cycle is one Core column query over the cycles' session spans, read as column
rows rather than ORM entities and sliced per cycle in Python. The overlay rows of every cycle
are one statement over the cycles' observed ranges, each widened by the 62-day overlay staleness
limit. Each statement merges the ranges that overlap or touch and reads nothing between the
others, never the whole span from the first window to the last. A cycle whose window holds no
observed data reads no lead-in and no price, and a request of only such cycles reads no lead-in,
price or overlay.

Business-rule errors are coded ``HTTPException`` bodies ``{code, message, field}`` (see
:class:`~opennse_terminal.schemas.common.ErrorDetail`). The module registers no exception handlers
and never imports ``opennse_terminal.api``, which imports the services.
"""

import logging
from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

from fastapi import HTTPException
from sqlalchemy import CompoundSelect, Select, and_, func, or_, select, union
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.config import TerminalSettings
from opennse_terminal.models import (
    TerminalCycle,
    TerminalDailyPrice,
    TerminalEquity,
    TerminalMacroIndicator,
)
from opennse_terminal.quant.asof import FundamentalsRow, MacroRow, PriceRow
from opennse_terminal.quant.backtest import (
    BASE_CLOSE_MAX_SESSIONS,
    MA_SESSIONS,
    OVERLAY_MAX_STALENESS_DAYS,
    BacktestCandidate,
    CycleBacktestResult,
    CycleEvent,
    CyclePointResult,
    CycleWindow,
    resolve_window,
    run_cycle_backtest,
    with_projection,
)
from opennse_terminal.quant.numeric import HUNDRED, Violation, quantize_out
from opennse_terminal.schemas.common import ErrorDetail
from opennse_terminal.schemas.cycles import (
    BacktestResponse,
    BacktestWindow,
    CycleMarker,
    CycleMarkers,
    CyclePoint,
    CycleProjection,
    CycleProjectionPoint,
    CycleResult,
    CycleSummary,
    MarkerMetric,
)
from opennse_terminal.services.market_service import (
    ensure_sector,
    ensure_ticker,
    load_equities,
    load_fundamentals_rows,
    load_price_rows_in_ranges,
)
from opennse_terminal.services.offload import AbandonCheck, compute

__all__ = ["get_backtest"]

logger = logging.getLogger("opennse_terminal.services.cycle_service")

_HTTP_422: Final = 422

_LEAD_SESSIONS: Final = MA_SESSIONS + BASE_CLOSE_MAX_SESSIONS
"""Market sessions, with their price history, read before each window start.

The trailing turnover mean at the anchor reads back 20 sessions and the base close up to 10, so
30 sessions cover both with room to spare."""

_WINDOW_PROBE_DATE: Final = date(2000, 1, 1)
"""Event date used to check the window form before any statement runs.

Which form a request uses, and whether it mixes the two, does not depend on the event date, and
a window of at most 365 days or 12 months either side of this date is always representable."""

_EMPTY_STATUSES: Final[frozenset[str]] = frozenset({"awaiting_data", "no_constituents"})
"""Statuses whose cycle has no points, markers or summary (null objects, not objects of nulls)."""


# ---------------------------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------------------------


def _coded_error(
    status_code: int, code: str, message: str, field: str | None = None
) -> HTTPException:
    """Build the coded error ``{"detail": {"code", "message", "field"}}`` for ``status_code``."""
    return HTTPException(
        status_code, detail=ErrorDetail(code=code, message=message, field=field).model_dump()
    )


def _violation_error(violation: Violation) -> HTTPException:
    """Raise a window rule failure from ``quant.backtest.resolve_window`` as a coded 422."""
    return _coded_error(_HTTP_422, violation.code, violation.message, violation.field)


def _resolve(
    event_date: date,
    pre_days: int | None,
    post_days: int | None,
    pre_months: int | None,
    post_months: int | None,
) -> CycleWindow:
    """Resolve the window around ``event_date``, raising a violated window rule as a coded 422."""
    window = resolve_window(
        event_date,
        pre_days=pre_days,
        post_days=post_days,
        pre_months=pre_months,
        post_months=post_months,
    )
    if isinstance(window, Violation):
        raise _violation_error(window)
    return window


def _days_before(day: date, days: int) -> date:
    """``day`` minus ``days`` calendar days, clamped to ``date.min``."""
    try:
        return day - timedelta(days=days)
    except OverflowError:
        return date.min


def _default_label(event_date: date) -> str:
    """Label of an event date that has no ``terminal_cycles`` row."""
    return f"Event {event_date.isoformat()}"


# ---------------------------------------------------------------------------------------------
# Input resolution
# ---------------------------------------------------------------------------------------------


async def _resolve_events(session: AsyncSession, requested: Sequence[date]) -> list[CycleEvent]:
    """Return the events to backtest, ascending by date.

    ``requested`` holds the request's dates, ascending and without repeats. Each is labelled from
    the ``terminal_cycles`` row of the same ``event_date`` when one exists, otherwise
    ``"Event YYYY-MM-DD"``. Without requested dates, every ``terminal_cycles`` row is used with
    its own label.

    Raises:
        HTTPException: 422 ``no_event_dates`` (field ``event_dates``) when neither the request
            nor ``terminal_cycles`` yields an event.
    """
    if requested:
        result = await session.execute(
            select(TerminalCycle.event_date, TerminalCycle.label).where(
                TerminalCycle.event_date.in_(requested)
            )
        )
        labels = {stored_date: label for stored_date, label in result}
        return [CycleEvent(labels.get(day, _default_label(day)), day) for day in requested]
    result = await session.execute(
        select(TerminalCycle.label, TerminalCycle.event_date).order_by(TerminalCycle.event_date)
    )
    events = [CycleEvent(label, stored_date) for label, stored_date in result]
    if not events:
        raise _coded_error(
            _HTTP_422,
            "no_event_dates",
            "No event dates were given and terminal_cycles holds no events. Pass event_dates "
            "or run the seeder to load the default election cycles.",
            "event_dates",
        )
    return events


async def _resolve_candidates(
    session: AsyncSession, tickers: Sequence[str] | None, sector: str | None
) -> list[TerminalEquity]:
    """Return the candidate equities, ordered by ticker.

    The candidates are the requested ``tickers`` (trimmed, upper-cased, repeats dropped), the
    equities of ``sector`` (matched exactly as stored), the requested tickers that belong to the
    sector when both are given, or every equity when neither is.

    Raises:
        HTTPException: 404 ``unknown_ticker`` (field ``tickers``) for a ticker that is not a
            known equity; 404 ``unknown_sector`` (field ``sector``) for a sector without one.
    """
    if tickers:
        equities = await load_equities(session, tickers=tickers)
        known = {equity.ticker for equity in equities}
        for ticker in sorted({ticker.strip().upper() for ticker in tickers}):
            if ticker not in known:
                # Raises the shared 404 unknown_ticker body for the first unknown ticker.
                await ensure_ticker(session, ticker, field="tickers")
        if sector is not None:
            await ensure_sector(session, sector)
            equities = [equity for equity in equities if equity.sector == sector]
        return equities
    if sector is not None:
        await ensure_sector(session, sector)
        return await load_equities(session, sector=sector)
    return await load_equities(session)


# ---------------------------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------------------------


async def _data_available_through(session: AsyncSession, tickers: Sequence[str]) -> date | None:
    """Latest stored ``trade_date`` among ``tickers``; ``None`` when none of them has a price."""
    if not tickers:
        return None
    result = await session.execute(
        select(func.max(TerminalDailyPrice.trade_date)).where(
            TerminalDailyPrice.ticker.in_(tickers)
        )
    )
    latest: date | None = result.scalar_one()
    return latest


async def _sessions_in_ranges(
    session: AsyncSession, ranges: Sequence[tuple[date, date]]
) -> list[date]:
    """The observed market sessions inside any of the inclusive, non-empty ``ranges``, ascending.

    One ``DISTINCT trade_date`` query with one ``BETWEEN`` per range, so the sessions between the
    ranges are never read.
    """
    stmt = (
        select(TerminalDailyPrice.trade_date)
        .where(or_(*(TerminalDailyPrice.trade_date.between(start, end) for start, end in ranges)))
        .distinct()
        .order_by(TerminalDailyPrice.trade_date)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def _sessions_before_each(
    session: AsyncSession, days: Sequence[date], count: int
) -> list[date]:
    """The last ``count`` observed market sessions strictly before each of ``days``, combined.

    One statement for every day of the non-empty ``days``: a backward ``DISTINCT trade_date``
    query with ``LIMIT count`` per day, joined by ``UNION`` when there are several, so only those
    lead-ins are read however much history precedes them; every session before a day is included
    when fewer than ``count`` exist. Returns their distinct dates, ascending.
    """
    lead_ins = [
        select(TerminalDailyPrice.trade_date)
        .where(TerminalDailyPrice.trade_date < day)
        .distinct()
        .order_by(TerminalDailyPrice.trade_date.desc())
        .limit(count)
        for day in days
    ]
    stmt: Select[tuple[date]] | CompoundSelect[tuple[date]] = (
        lead_ins[0] if len(lead_ins) == 1 else union(*lead_ins)
    )
    result = await session.execute(stmt)
    return sorted(set(result.scalars().all()))


async def _cycle_sessions(
    session: AsyncSession, windows: Sequence[CycleWindow], data_available_through: date | None
) -> list[list[date]]:
    """The observed market sessions the backtest of each of ``windows`` reads, ascending.

    For each window, these are its own sessions, from ``window_start`` to ``window_end`` or
    ``data_available_through``, whichever is earlier, preceded by the last :data:`_LEAD_SESSIONS`
    sessions before ``window_start`` (every earlier session when fewer exist). Positions counted
    back from the anchor, the first session in the window, are therefore those of the full
    observed calendar for the turnover mean and the base close, which read at most 19 and 10
    sessions back. A window that holds no session up to the end of the data gets an empty list
    and reads no lead-in: its cycle is then ``awaiting_data``.

    Two statements serve every window, whatever their number. The first reads the in-window
    sessions of every window that can hold data, over their ranges merged where they overlap or
    touch (no statement when none can). The second reads the lead-ins of the windows that hold a
    session, one backward query per distinct window start (no statement when none does). A
    window's lead-in is the last :data:`_LEAD_SESSIONS` sessions before its start among both
    results: its own backward query returns exactly those, and every other date read is an
    observed session as well.
    """
    if data_available_through is None:
        return [[] for _ in windows]
    spans = [
        (window.window_start, min(window.window_end, data_available_through)) for window in windows
    ]
    observable = [(start, last_day) for start, last_day in spans if last_day >= start]
    if not observable:
        return [[] for _ in windows]
    calendar = await _sessions_in_ranges(session, _merge_ranges(observable))
    in_window = [
        calendar[bisect_left(calendar, start) : bisect_right(calendar, last_day)]
        for start, last_day in spans
    ]
    starts = sorted({start for (start, _), days in zip(spans, in_window, strict=True) if days})
    if not starts:
        return in_window
    lead_ins = await _sessions_before_each(session, starts, _LEAD_SESSIONS)
    known = sorted({*calendar, *lead_ins})
    cycle_sessions: list[list[date]] = []
    for (start, _), days in zip(spans, in_window, strict=True):
        if not days:
            cycle_sessions.append([])
            continue
        before = bisect_left(known, start)
        cycle_sessions.append([*known[max(0, before - _LEAD_SESSIONS) : before], *days])
    return cycle_sessions


def _merge_ranges(ranges: Iterable[tuple[date, date]]) -> list[tuple[date, date]]:
    """Merge inclusive ``(start, end)`` date ranges that overlap or touch, ascending by start."""
    merged: list[tuple[date, date]] = []
    for start, end in sorted(ranges):
        if merged and (start - merged[-1][1]).days <= 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


async def _load_overlay_rows(
    session: AsyncSession, ranges: Sequence[tuple[date, date]]
) -> list[MacroRow]:
    """Macro rows carrying an overlay series (inflation, 91-day or 364-day bill), ascending.

    One statement reads the rows observed within any of the inclusive, disjoint ``ranges``; no
    statement runs when ``ranges`` is empty. An overlay value older than 62 days is never used,
    so callers pass, per cycle, its window start minus that limit through its last observed
    session.
    """
    if not ranges:
        return []
    stmt = (
        select(
            TerminalMacroIndicator.observation_date,
            TerminalMacroIndicator.inflation_yoy_pct,
            TerminalMacroIndicator.tbill_91d_pct,
            TerminalMacroIndicator.tbill_364d_pct,
        )
        .where(
            or_(
                *(
                    and_(
                        TerminalMacroIndicator.observation_date >= start,
                        TerminalMacroIndicator.observation_date <= end,
                    )
                    for start, end in ranges
                )
            ),
            or_(
                TerminalMacroIndicator.inflation_yoy_pct.is_not(None),
                TerminalMacroIndicator.tbill_91d_pct.is_not(None),
                TerminalMacroIndicator.tbill_364d_pct.is_not(None),
            ),
        )
        .order_by(TerminalMacroIndicator.observation_date)
    )
    result = await session.execute(stmt)
    return [
        MacroRow(
            observation_date=observation_date,
            inflation_yoy_pct=inflation,
            tbill_91d_pct=tbill_91d,
            tbill_364d_pct=tbill_364d,
        )
        for observation_date, inflation, tbill_91d, tbill_364d in result
    ]


async def _load_cycle_prices(
    session: AsyncSession, tickers: Sequence[str], cycle_sessions: Sequence[Sequence[date]]
) -> list[dict[str, list[PriceRow]]]:
    """Price rows each cycle's backtest reads, per ticker, from one Core column query.

    A cycle's span runs over its ``cycle_sessions`` (see :func:`_cycle_sessions`): from the first
    lead-in session, :data:`_LEAD_SESSIONS` sessions before the window's first session (the
    turnover mean and base-close lookback read before the window), to the window's last observed
    session. One statement reads the spans of every cycle, merged where they overlap or touch
    (:func:`~opennse_terminal.services.market_service.load_price_rows_in_ranges`), and each cycle
    gets the rows of its own span: every candidate ticker as a key, with its rows ascending, as a
    read of that span alone returns them. A cycle without sessions needs no price row, because it
    is then ``awaiting_data``, and gets an empty mapping; when no cycle has sessions or there is
    no ticker, no statement runs.
    """
    spans = [(observed[0], observed[-1]) for observed in cycle_sessions if observed]
    if not tickers or not spans:
        return [{} for _ in cycle_sessions]
    rows = await load_price_rows_in_ranges(session, tickers, _merge_ranges(spans))
    dates = {ticker: [row.trade_date for row in history] for ticker, history in rows.items()}
    per_cycle: list[dict[str, list[PriceRow]]] = []
    for observed in cycle_sessions:
        if not observed:
            per_cycle.append({})
            continue
        first, last = observed[0], observed[-1]
        per_cycle.append(
            {
                ticker: history[
                    bisect_left(dates[ticker], first) : bisect_right(dates[ticker], last)
                ]
                for ticker, history in rows.items()
            }
        )
    return per_cycle


def _candidates_for_cycle(
    equities: Sequence[TerminalEquity],
    prices: dict[str, list[PriceRow]],
    fundamentals: dict[str, list[FundamentalsRow]],
) -> list[BacktestCandidate]:
    """One quant candidate per equity: its fixed share count, listing currency and history."""
    return [
        BacktestCandidate(
            ticker=equity.ticker,
            shares_outstanding=equity.shares_outstanding,
            prices=prices.get(equity.ticker, []),
            fundamentals=fundamentals.get(equity.ticker, []),
            currency=equity.currency,
        )
        for equity in equities
    ]


# ---------------------------------------------------------------------------------------------
# Response assembly (the single rounding point)
# ---------------------------------------------------------------------------------------------


def _point(point: CyclePointResult) -> CyclePoint:
    """Quantize one cycle point: indices and P/E to 2 dp, KES to 2 dp, overlays in % to 2 dp."""
    return CyclePoint(
        offset_days=point.offset_days,
        trade_date=point.trade_date,
        price_index=quantize_out(point.price_index, "index"),
        turnover_index=quantize_out(point.turnover_index, "index"),
        pe_ratio=quantize_out(point.pe_ratio, "ratio"),
        pe_index=quantize_out(point.pe_index, "index"),
        cumulative_foreign_net_kes=quantize_out(point.cumulative_foreign_net_kes, "money"),
        inflation_yoy_pct=quantize_out(point.inflation_yoy_pct, "pct"),
        tbill_91d_pct=quantize_out(point.tbill_91d_pct, "pct"),
        tbill_364d_pct=quantize_out(point.tbill_364d_pct, "pct"),
    )


def _metric_value(point: CyclePoint, metric: MarkerMetric) -> Decimal | None:
    if metric == "pe_ratio":
        return point.pe_ratio
    if metric == "price_index":
        return point.price_index
    return point.turnover_index


def _earliest_minimum(
    points: Sequence[CyclePoint], metric: MarkerMetric
) -> tuple[int, CycleMarker] | None:
    """The earliest returned point with the minimum non-null ``metric``, and its position.

    Selected on the quantized values the client receives, so a marker always equals the minimum
    a client recomputes from ``points``; two raw values that round to the same level resolve to
    the earlier session.
    """
    best: tuple[int, Decimal] | None = None
    for position, point in enumerate(points):
        value = _metric_value(point, metric)
        if value is not None and (best is None or value < best[1]):
            best = (position, value)
    if best is None:
        return None
    position, value = best
    point = points[position]
    marker = CycleMarker(
        offset_days=point.offset_days, trade_date=point.trade_date, metric=metric, value=value
    )
    return position, marker


def _pe_compression(
    result: CycleBacktestResult, floor: tuple[int, CycleMarker] | None
) -> Decimal | None:
    """P/E compression at the returned valuation floor, in percentage points, unrounded.

    ``(P/E at the floor / anchor P/E - 1) x 100``; ``None`` when the floor came from the
    price-index fallback or the anchor P/E is unavailable. When the floor resolved on the
    quantized points is not the session the quant layer chose, the figure is recomputed from that
    session's raw P/E index, since ``pe_index - 100`` equals the same expression.
    """
    if floor is None or result.summary is None:
        return None
    position, marker = floor
    if marker.metric != "pe_ratio" or "pe_anchor_unavailable" in result.flags:
        return None
    quant_floor = result.markers.valuation_floor if result.markers is not None else None
    if (
        quant_floor is not None
        and quant_floor.metric == "pe_ratio"
        and quant_floor.trade_date == marker.trade_date
    ):
        return result.summary.pe_compression_pct
    raw_pe_index = result.points[position].pe_index
    return None if raw_pe_index is None else raw_pe_index - HUNDRED


def _projection(result: CycleBacktestResult) -> CycleProjection | None:
    """Quantize the labelled projection of a partial or awaiting-data cycle (index levels, 2 dp)."""
    if result.projection is None:
        return None
    return CycleProjection(
        is_projection=True,
        basis=list(result.projection.basis),
        points=[
            CycleProjectionPoint(
                offset_days=point.offset_days,
                price_index=quantize_out(point.price_index, "index"),
                turnover_index=quantize_out(point.turnover_index, "index"),
                pe_index=quantize_out(point.pe_index, "index"),
            )
            for point in result.projection.points
        ],
    )


def _cycle(result: CycleBacktestResult) -> CycleResult:
    """Build one ``CycleResult``, re-deriving the markers from the quantized points.

    ``awaiting_data`` and ``no_constituents`` cycles carry no points and null ``markers`` and
    ``summary``; every other cycle carries both objects, each member null where undefined.
    """
    points: list[CyclePoint] = []
    markers: CycleMarkers | None = None
    summary: CycleSummary | None = None
    if result.status not in _EMPTY_STATUSES:
        points = [_point(point) for point in result.points]
        floor = _earliest_minimum(points, "pe_ratio") or _earliest_minimum(points, "price_index")
        exhaustion = _earliest_minimum(points, "turnover_index")
        markers = CycleMarkers(
            valuation_floor=None if floor is None else floor[1],
            volume_exhaustion=None if exhaustion is None else exhaustion[1],
        )
        if result.summary is not None:
            summary = CycleSummary(
                pre_event_return_pct=quantize_out(result.summary.pre_event_return_pct, "pct"),
                post_event_return_pct=quantize_out(result.summary.post_event_return_pct, "pct"),
                max_drawdown_pct=quantize_out(result.summary.max_drawdown_pct, "pct"),
                pe_compression_pct=quantize_out(_pe_compression(result, floor), "pct"),
            )
    return CycleResult(
        label=result.label,
        event_date=result.event_date,
        window_start=result.window_start,
        window_end=result.window_end,
        status=result.status,
        anchor_date=result.anchor_date,
        days_until_window_start=result.days_until_window_start,
        constituents=sorted(result.constituents),
        excluded_constituents=sorted(result.excluded_constituents),
        points=points,
        markers=markers,
        summary=summary,
        projection=_projection(result),
        flags=list(result.flags),
    )


# ---------------------------------------------------------------------------------------------
# GET /cycles/backtest
# ---------------------------------------------------------------------------------------------


async def get_backtest(
    session: AsyncSession,
    settings: TerminalSettings,
    *,
    event_dates: Sequence[date] | None = None,
    pre_days: int | None = None,
    post_days: int | None = None,
    pre_months: int | None = None,
    post_months: int | None = None,
    tickers: Sequence[str] | None = None,
    sector: str | None = None,
    abandoned: AbandonCheck | None = None,
) -> BacktestResponse:
    """Build ``GET /cycles/backtest``: normalized NSE paths around each event, with overlays.

    **Window.** Calendar days ``[T - pre_days, T + post_days]`` around each event date T, 180 days
    each side when no window parameter is given (the "T-180 to T+180" contract), or calendar
    months ``[T - pre_months, T + post_months]`` (1-12 each, a missing side defaulting to 12),
    the day clamped to the target month's last day, so 2024-02-29 plus 12 months is 2025-02-28.
    ``BacktestResponse.window`` echoes the effective form: days with null months, or months with
    null days. Every point carries ``offset_days = trade_date - T`` in calendar days.

    **Events.** The requested ``event_dates`` (repeats dropped, ascending), each labelled from
    ``terminal_cycles`` when stored there and ``"Event YYYY-MM-DD"`` otherwise; without them,
    every ``terminal_cycles`` row in date order.

    **Candidates.** The requested ``tickers``, the equities of ``sector``, the requested tickers
    in that sector when both are given, or every equity. Each cycle's cohort is fixed at its
    anchor, the first market session in the window: a candidate joins only with an actual close
    within the 10 market sessions on or before it, and only when listed in KES.

    **Units.** ``price_index``, ``turnover_index`` and ``pe_index`` are index levels, base 100 at
    the anchor; ``pe_ratio`` is a multiple (x); returns, drawdowns, compression and the inflation
    and Treasury bill overlays are percentage points; ``cumulative_foreign_net_kes`` is KES. All
    are ``Decimal`` values rounded once here to 2 dp (ROUND_HALF_UP).

    **Fundamentals.** A fiscal year enters P/E ``settings.fundamentals_lag_days`` (90 by default)
    after its period end. Publication dates are not tracked, so the lag reduces look-ahead bias
    but does not eliminate it: a company reporting later than the lag is still used from the
    lag onwards. Fundamentals in a currency other than KES are refused by the quant layer
    (``currency_mismatch``), never converted.

    **Statuses.** ``complete`` when the candidates' data reaches ``window_end``; ``partial`` when
    it ends inside the window; ``awaiting_data`` when the window holds no observed data (with
    ``days_until_window_start`` when the data ends before it opens); ``no_constituents`` when no
    candidate has a base close. ``partial`` and ``awaiting_data`` cycles carry a ``projection``:
    the mean of the request's complete cycles' normalized paths at each offset, labelled with its
    ``basis``, descriptive rather than a forecast and never stored. With fewer than 2 complete
    cycles it is null and the cycle is flagged ``insufficient_projection_basis``.

    **Computation.** After the reads, each cycle is backtested in a worker thread, one cycle at a
    time, so the event loop keeps serving other requests. ``abandoned``, when given, is awaited
    before each cycle, so an abandoned request stops within one cycle.

    Args:
        session: An async session on the terminal database, the host's own when overridden; it
            is only read.
        settings: Terminal settings, for the fundamentals publication lag.
        event_dates: Event dates T; ``None`` or empty for every ``terminal_cycles`` row.
        pre_days: Calendar days before T (1-365).
        post_days: Calendar days after T (1-365).
        pre_months: Calendar months before T (1-12).
        post_months: Calendar months after T (1-12).
        tickers: Candidate tickers; ``None`` or empty for no ticker restriction.
        sector: Candidate sector, matched exactly as stored (for example ``Banking``).
        abandoned: Probe of whether the client went away, such as the route's
            ``request.is_disconnected``; ``None`` never stops early.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away; the cycles not
            yet backtested are never computed.
        HTTPException: 422 ``conflicting_window_units`` when a day and a month parameter are both
            given, checked before any statement runs; 422 ``window_out_of_range`` when a window
            would leave the representable dates; 422 ``no_event_dates`` when neither the request
            nor ``terminal_cycles`` yields an event; 404 ``unknown_ticker`` or ``unknown_sector``.
        ValueError: If a window value is outside its 1-365 or 1-12 range, which the route bounds
            before calling.
    """
    window_args = (pre_days, post_days, pre_months, post_months)
    form = _resolve(_WINDOW_PROBE_DATE, *window_args)
    # Requested dates are known before any statement, so their windows are checked up front.
    requested = {day: _resolve(day, *window_args) for day in sorted(set(event_dates or ()))}

    events = await _resolve_events(session, list(requested))
    windows = [
        requested[event.event_date]
        if event.event_date in requested
        else _resolve(event.event_date, *window_args)
        for event in events
    ]
    equities = await _resolve_candidates(session, tickers, sector)
    candidate_tickers = [equity.ticker for equity in equities]

    data_available_through = await _data_available_through(session, candidate_tickers)
    cycle_sessions = await _cycle_sessions(session, windows, data_available_through)
    fundamentals = await load_fundamentals_rows(session, candidate_tickers)
    macro_rows = await _load_overlay_rows(
        session,
        _merge_ranges(
            (_days_before(window.window_start, OVERLAY_MAX_STALENESS_DAYS), observed[-1])
            for window, observed in zip(windows, cycle_sessions, strict=True)
            if observed
        ),
    )

    cycle_prices = await _load_cycle_prices(session, candidate_tickers, cycle_sessions)

    results: list[CycleBacktestResult] = []
    for event, window, observed, prices in zip(
        events, windows, cycle_sessions, cycle_prices, strict=True
    ):
        results.append(
            await compute(
                abandoned,
                run_cycle_backtest,
                event,
                window,
                _candidates_for_cycle(equities, prices, fundamentals),
                observed,
                macro_rows,
                data_available_through=data_available_through,
                lag_days=settings.fundamentals_lag_days,
            )
        )
    complete = [result for result in results if result.status == "complete"]
    results = [with_projection(result, complete) for result in results]

    logger.debug(
        "cycle backtest over %d candidates through %s: %s",
        len(candidate_tickers),
        data_available_through,
        # Labels are stored text: repr escapes CR, LF and every other control or line-separator
        # character, so one backtest stays one log record on one line whatever a label holds.
        ", ".join(f"{result.label!r}={result.status}" for result in results),
    )
    return BacktestResponse(
        window=BacktestWindow(
            pre_days=form.pre_days,
            post_days=form.post_days,
            pre_months=form.pre_months,
            post_months=form.post_months,
        ),
        candidates=sorted(candidate_tickers),
        data_available_through=data_available_through,
        cycles=[_cycle(result) for result in results],
    )
