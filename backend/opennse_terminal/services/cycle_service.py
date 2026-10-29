"""Event and election cycle backtest service: ``GET /api/v1/terminal/cycles/backtest``.

Resolves the request (window form, event dates and candidate equities), reads the stored history
through SQLAlchemy 2.0 async ``select()`` statements with bound parameters only, runs the pure
:func:`~opennse_terminal.quant.backtest.run_cycle_backtest` once per event, attaches the labelled
projection to future or partly observed cycles, and rounds every output value exactly once with
:func:`~opennse_terminal.quant.numeric.quantize_out` while building the ``schemas.cycles``
response. Every formula lives in ``quant.backtest``; nothing here converts to ``float``.

Only stored rows are read, seeded or ingested alike. A future event such as the 2027 election is
answered with the descriptive projection of the completed cycles, which is computed per request
and never written to the database; no future-dated row is ever assumed.

Each request issues a constant number of statements per cycle: the price history of a cycle is
one Core column query over its window and a short lead-in, never the union of every window as
ORM entities.

Business-rule errors are coded ``HTTPException`` bodies ``{code, message, field}`` (see
:class:`~opennse_terminal.schemas.common.ErrorDetail`). The module registers no exception handlers
and never imports ``opennse_terminal.api``, which imports the services.
"""

import logging
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import Final

from fastapi import HTTPException
from sqlalchemy import func, or_, select
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
    load_market_sessions,
    load_price_rows,
)

__all__ = ["get_backtest"]

logger = logging.getLogger("opennse_terminal.services.cycle_service")

_HTTP_422: Final = 422

_LEAD_SESSIONS: Final = MA_SESSIONS + BASE_CLOSE_MAX_SESSIONS
"""Market sessions of price history read before each window start.

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


async def _load_overlay_rows(session: AsyncSession, start: date, end: date) -> list[MacroRow]:
    """Macro rows carrying an overlay series (inflation, 91-day or 364-day bill), ascending.

    Rows from ``start`` to ``end`` inclusive are read: an overlay value older than 62 days is
    never used, so callers pass the earliest window start minus that limit.
    """
    stmt = (
        select(
            TerminalMacroIndicator.observation_date,
            TerminalMacroIndicator.inflation_yoy_pct,
            TerminalMacroIndicator.tbill_91d_pct,
            TerminalMacroIndicator.tbill_364d_pct,
        )
        .where(
            TerminalMacroIndicator.observation_date >= start,
            TerminalMacroIndicator.observation_date <= end,
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
    session: AsyncSession,
    tickers: Sequence[str],
    window: CycleWindow,
    sessions: Sequence[date],
    data_available_through: date | None,
) -> dict[str, list[PriceRow]]:
    """Price rows the backtest of ``window`` reads, per ticker, in one Core column query.

    The range runs from the market session :data:`_LEAD_SESSIONS` sessions before the window's
    first session (the turnover mean and base-close lookback read before the window) to the
    window end or the end of the data, whichever is earlier. A window without an observed session
    needs no price row, because the cycle is then ``awaiting_data``.
    """
    if not tickers or data_available_through is None:
        return {}
    first = bisect_left(sessions, window.window_start)
    last = bisect_right(sessions, min(window.window_end, data_available_through))
    if first >= last:
        return {}
    lead_start = sessions[max(0, first - _LEAD_SESSIONS)]
    return await load_price_rows(session, tickers, lead_start, sessions[last - 1])


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
    """Quantize the labelled projection of a partial or future cycle (index levels, 2 dp)."""
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
    it ends inside the window; ``awaiting_data`` when the window holds no data yet (with
    ``days_until_window_start`` when the data ends before it opens); ``no_constituents`` when no
    candidate has a base close. ``partial`` and ``awaiting_data`` cycles carry a ``projection``:
    the mean of the request's complete cycles' normalized paths at each offset, labelled with its
    ``basis``, descriptive rather than a forecast and never stored. With fewer than 2 complete
    cycles it is null and the cycle is flagged ``insufficient_projection_basis``.

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

    Raises:
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
    sessions = await load_market_sessions(session)
    fundamentals = await load_fundamentals_rows(session, candidate_tickers)
    macro_rows = await _load_overlay_rows(
        session,
        _days_before(min(window.window_start for window in windows), OVERLAY_MAX_STALENESS_DAYS),
        max(window.window_end for window in windows),
    )

    results: list[CycleBacktestResult] = []
    for event, window in zip(events, windows, strict=True):
        prices = await _load_cycle_prices(
            session, candidate_tickers, window, sessions, data_available_through
        )
        results.append(
            run_cycle_backtest(
                event,
                window,
                _candidates_for_cycle(equities, prices, fundamentals),
                sessions,
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
        ", ".join(f"{result.label}={result.status}" for result in results),
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
