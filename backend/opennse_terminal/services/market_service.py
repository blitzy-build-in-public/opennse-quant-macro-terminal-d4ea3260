"""Market analytics service: screener, sector heatmap, valuation bands, anomalies and net flows.

Serves the five ``/api/v1/terminal/market/*`` endpoints and provides the shared loaders other
services reuse (``cycle_service`` reads market sessions, equities, prices and fundamentals through
them). The layering is ``api -> services -> quant``: every function here reads PostgreSQL through
SQLAlchemy 2.0 async ``select()`` statements with bound parameters only, turns rows into the plain
``quant`` input records, calls the pure formulas, and rounds each output value exactly once with
:func:`~opennse_terminal.quant.numeric.quantize_out` (ROUND_HALF_UP) while building the
``schemas.market`` response. Nothing here converts to ``float``.

Market sessions are the observed market calendar: the distinct ``trade_date`` values any ticker
has in ``terminal_daily_prices``, never a holiday list. Each endpoint issues a constant number of
statements, whatever the number of tickers or days, and price history is read as Core column
rows rather than ORM entities.

Every read runs on the event loop. The screener, valuation bands, anomalies and flows then hand
their pure computation to a worker thread through
:func:`~opennse_terminal.services.offload.compute`, in chunks (one per Z-score series for the
anomaly feed), so the event loop keeps serving other requests meanwhile. Given an ``abandoned``
check, such as the route's ``request.is_disconnected``, they stop before the next chunk once the
client has gone away and raise :class:`~opennse_terminal.services.offload.RequestAbandonedError`.

Units: KES amounts are whole shillings, multiples are ratios (x), yields, returns and changes are
percentage points, volumes are shares and Z-scores are standard deviations. Closes (per share)
and turnover are in the equity's listing currency (``currency``, KES by default), read and
returned as stored and never converted, so the screener carries a non-KES equity's close and
turnover in its own currency, named by the row's ``currency``. Market cap, EPS, P/E, P/B,
EV/EBITDA, dividend yield and their sector aggregates are computed in KES only: the quant layer
refuses a non-KES equity outright, and a KES equity's fundamentals not in KES (its market cap
stands), both with ``currency_mismatch``. The screener and the anomaly feed, whose Z-scores and
items name no currency, score volume for every equity but turnover only for equities listed in
KES, so a scored turnover is always KES: a non-KES screener row has a null ``turnover_z`` with
``turnover_z:currency_mismatch`` and is never a turnover anomaly, as it is never a turnover item
of the feed. Net flows sum the KES-listed constituents only, leaving any other out with
``currency_mismatch``.

Business-rule errors are coded ``HTTPException`` bodies ``{code, message, field}`` (see
:class:`~opennse_terminal.schemas.common.ErrorDetail`). The module registers no exception handlers
and never imports ``opennse_terminal.api``, which imports the services.
"""

import logging
from collections import defaultdict
from collections.abc import Collection, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

from fastapi import HTTPException, status
from sqlalchemy import Select, func, or_, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.config import TerminalSettings
from opennse_terminal.models import TerminalDailyPrice, TerminalEquity, TerminalFundamental
from opennse_terminal.quant.asof import FundamentalsRow, PriceRow, close_as_of, fundamentals_as_of
from opennse_terminal.quant.backtest import add_months
from opennse_terminal.quant.bands import BandsResult, BandStatsResult, valuation_bands
from opennse_terminal.quant.flows import DEFAULT_FLOW_ROLLING_WINDOW, net_flow_series
from opennse_terminal.quant.heatmap import (
    CLOSE_MAX_AGE_DAYS,
    HeatmapCellResult,
    HeatmapMember,
    period_boundaries,
    sector_returns,
)
from opennse_terminal.quant.numeric import ANALYTICS_CURRENCY, quantize_out
from opennse_terminal.quant.valuation import (
    ValuationInputs,
    ValuationResult,
    compute_valuation,
    price_change_pct,
    sector_aggregates,
)
from opennse_terminal.quant.zscore import Observation, ZScorePoint, is_spike, rolling_zscores
from opennse_terminal.schemas.common import CoverageMeta, ErrorDetail, SeriesPoint
from opennse_terminal.schemas.market import (
    DEFAULT_THRESHOLD,
    DEFAULT_Z_WINDOW,
    AnomaliesResponse,
    AnomalyItem,
    AnomalyMetric,
    BandMetric,
    BandStats,
    FlowPoint,
    FlowsResponse,
    FlowTotals,
    HeatmapCell,
    HeatmapInterval,
    ScreenerParams,
    ScreenerResponse,
    ScreenerRow,
    ScreenerSort,
    SectorAggregate,
    SectorHeatmapResponse,
    SectorHeatmapRow,
    SortOrder,
    SubjectType,
    ValuationBandsResponse,
)
from opennse_terminal.services.offload import AbandonCheck, compute
from opennse_terminal.services.watchlist_service import OwnerPrincipal, owned_watchlist_tickers

__all__ = [
    "EXCHANGE_TIMEZONE",
    "FLOW_DEFAULT_RANGE_DAYS",
    "MARKET_SUBJECT",
    "SECTOR_BAND_CLOSE_MAX_AGE_DAYS",
    "ensure_sector",
    "ensure_ticker",
    "get_anomalies",
    "get_flows",
    "get_screener",
    "get_sector_heatmap",
    "get_valuation_bands",
    "latest_trade_date",
    "load_equities",
    "load_fundamentals_rows",
    "load_market_sessions",
    "load_price_rows",
    "load_price_rows_in_ranges",
]

logger = logging.getLogger("opennse_terminal.services.market_service")

EXCHANGE_TIMEZONE: Final = "Africa/Nairobi"
"""Timezone of the fallback ``as_of`` (the current exchange date) when no settings are passed and
no price row exists at all."""

FLOW_DEFAULT_RANGE_DAYS: Final = 365
"""Calendar days between the default flow ``start`` and ``end``."""

MARKET_SUBJECT: Final = "market"
"""``FlowsResponse.subject`` when neither a ticker nor a sector is given."""

SECTOR_BAND_CLOSE_MAX_AGE_DAYS: Final = 10
"""Oldest close, in calendar days on or before a session, a member contributes to a sector's
daily valuation-band value; an older close leaves the member out that day. The carry applies only
on a session on which at least one member of the sector has a close: a session on which no member
traded has no sector value."""

_SeriesMetric = Literal["volume", "turnover"]

_HTTP_404: Final = status.HTTP_404_NOT_FOUND
# Starlette 1.x deprecates the HTTP_422_UNPROCESSABLE_ENTITY name; the status code is the same.
_HTTP_422: Final = status.HTTP_422_UNPROCESSABLE_CONTENT


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


def _normalise_ticker(ticker: str) -> str:
    """Return ``ticker`` as stored in ``terminal_equities``: trimmed and upper-case (``SCOM``)."""
    return ticker.strip().upper()


def _normalise_tickers(tickers: Collection[str]) -> list[str]:
    """Normalise ``tickers``, drop repeats and sort them."""
    return sorted({_normalise_ticker(ticker) for ticker in tickers})


def _today(settings: TerminalSettings | None) -> date:
    """The current local date in the configured timezone (Africa/Nairobi by default)."""
    tz = settings.tzinfo if settings is not None else ZoneInfo(EXCHANGE_TIMEZONE)
    return datetime.now(tz).date()


def _days_before(day: date, days: int) -> date:
    """``day`` minus ``days`` calendar days, clamped to ``date.min``."""
    try:
        return day - timedelta(days=days)
    except OverflowError:
        return date.min


def _months_before(day: date, months: int) -> date:
    """``day`` minus ``months`` calendar months (day clamped to month length), or ``date.min``."""
    try:
        return add_months(day, -months)
    except ValueError:
        return date.min


async def _resolve_as_of(
    session: AsyncSession, as_of: date | None, settings: TerminalSettings | None
) -> tuple[date, bool]:
    """Return the effective ``as_of`` and whether the price table is worth querying for it.

    The requested ``as_of`` wins; otherwise the latest stored ``trade_date``. With neither, no
    price row exists at all, and the result is the current local date with ``False``, so the
    caller returns its empty response without further statements.
    """
    if as_of is not None:
        return as_of, True
    latest = await latest_trade_date(session)
    if latest is None:
        return _today(settings), False
    return latest, True


# ---------------------------------------------------------------------------------------------
# Shared loaders (public: cycle_service reuses them)
# ---------------------------------------------------------------------------------------------


def _price_select() -> Select[tuple[str, date, Decimal, int, Decimal, Decimal | None]]:
    """The price-history columns the quant records need, and nothing else."""
    return select(
        TerminalDailyPrice.ticker,
        TerminalDailyPrice.trade_date,
        TerminalDailyPrice.close_price,
        TerminalDailyPrice.volume,
        TerminalDailyPrice.turnover_kes,
        TerminalDailyPrice.foreign_net_flow_kes,
    )


async def _collect_price_rows(
    session: AsyncSession, stmt: Select[Any], tickers: Sequence[str] | None
) -> dict[str, list[PriceRow]]:
    """Run a price-column statement ordered by ``(ticker, trade_date)`` and group its rows.

    Every ticker of ``tickers`` gets a key, an empty list when it has no row; with ``None`` the
    keys are the tickers that have rows.
    """
    grouped: dict[str, list[PriceRow]] = {ticker: [] for ticker in tickers or ()}
    result = await session.execute(stmt)
    for ticker, trade_date, close_price, volume, turnover_kes, flow_kes in result:
        grouped.setdefault(ticker, []).append(
            PriceRow(
                ticker=ticker,
                trade_date=trade_date,
                close_price=close_price,
                volume=volume,
                turnover_kes=turnover_kes,
                foreign_net_flow_kes=flow_kes,
            )
        )
    return grouped


async def load_market_sessions(
    session: AsyncSession, start: date | None = None, end: date | None = None
) -> list[date]:
    """Return the observed market sessions, ascending: distinct ``trade_date`` values of any ticker.

    This is the market calendar every analytic counts sessions on (Z-score windows, rolling sums,
    coverage, heatmap boundaries, band sampling, cycle anchors). A day on which no ticker has a
    row is not a session; no holiday list is consulted.

    Args:
        session: An async session on the terminal database; it is only read.
        start: Earliest session to return, inclusive; ``None`` for no lower bound.
        end: Latest session to return, inclusive; ``None`` for no upper bound.
    """
    stmt = select(TerminalDailyPrice.trade_date).distinct().order_by(TerminalDailyPrice.trade_date)
    if start is not None:
        stmt = stmt.where(TerminalDailyPrice.trade_date >= start)
    if end is not None:
        stmt = stmt.where(TerminalDailyPrice.trade_date <= end)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def _last_sessions(session: AsyncSession, end: date, count: int) -> list[date]:
    """Return the last ``count`` market sessions on or before ``end``, ascending."""
    stmt = (
        select(TerminalDailyPrice.trade_date)
        .where(TerminalDailyPrice.trade_date <= end)
        .distinct()
        .order_by(TerminalDailyPrice.trade_date.desc())
        .limit(count)
    )
    result = await session.execute(stmt)
    return sorted(result.scalars().all())


async def latest_trade_date(session: AsyncSession) -> date | None:
    """Return the latest stored ``trade_date`` of any ticker, the default ``as_of`` everywhere.

    ``None`` when ``terminal_daily_prices`` is empty.
    """
    result = await session.execute(select(func.max(TerminalDailyPrice.trade_date)))
    latest: date | None = result.scalar_one()
    return latest


async def load_equities(
    session: AsyncSession,
    *,
    tickers: Collection[str] | None = None,
    sector: str | None = None,
) -> list[TerminalEquity]:
    """Return equities ordered by ticker, optionally restricted to ``tickers`` and ``sector``.

    Tickers are trimmed and upper-cased; unknown ones are simply absent from the result. An empty
    ``tickers`` collection returns no equity. ``sector`` matches ``terminal_equities.sector``
    exactly as stored (for example ``Banking``).
    """
    stmt = select(TerminalEquity).order_by(TerminalEquity.ticker)
    if tickers is not None:
        wanted = _normalise_tickers(tickers)
        if not wanted:
            return []
        stmt = stmt.where(TerminalEquity.ticker.in_(wanted))
    if sector is not None:
        stmt = stmt.where(TerminalEquity.sector == sector)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def ensure_ticker(
    session: AsyncSession, ticker: str, *, field: str = "ticker"
) -> TerminalEquity:
    """Return the equity of ``ticker`` (trimmed and upper-cased).

    Raises:
        HTTPException: 404 ``unknown_ticker`` reported against ``field`` when no equity has that
            ticker.
    """
    normalised = _normalise_ticker(ticker)
    result = await session.execute(
        select(TerminalEquity).where(TerminalEquity.ticker == normalised)
    )
    equity = result.scalar_one_or_none()
    if equity is None:
        raise _coded_error(
            _HTTP_404, "unknown_ticker", f"Ticker {normalised!r} is not a known equity.", field
        )
    return equity


async def ensure_sector(session: AsyncSession, sector: str, *, field: str = "sector") -> None:
    """Check that at least one equity is in ``sector``, matched exactly as stored.

    Raises:
        HTTPException: 404 ``unknown_sector`` reported against ``field`` otherwise.
    """
    result = await session.execute(
        select(TerminalEquity.id).where(TerminalEquity.sector == sector).limit(1)
    )
    if result.first() is None:
        raise _coded_error(
            _HTTP_404, "unknown_sector", f"Sector {sector!r} has no listed equity.", field
        )


async def load_price_rows(
    session: AsyncSession,
    tickers: Collection[str] | None,
    start: date | None,
    end: date | None,
) -> dict[str, list[PriceRow]]:
    """Return price history per ticker, each list ascending by ``trade_date``, in one statement.

    Only the columns the quant records need are read: ticker, trade date, volume in shares, and
    the close per share, turnover and foreign net flow in the equity's listing currency (KES by
    default) as stored, never converted. The read is served by the ``(ticker, trade_date)``
    unique index. Sessions a ticker did not trade have no row and are not filled.

    Args:
        session: An async session on the terminal database; it is only read.
        tickers: Tickers to load (trimmed and upper-cased), each returned as a key even without
            rows; ``None`` loads every ticker that has rows in the range.
        start: Earliest ``trade_date``, inclusive; ``None`` for no lower bound.
        end: Latest ``trade_date``, inclusive; ``None`` for no upper bound.
    """
    stmt = _price_select().order_by(TerminalDailyPrice.ticker, TerminalDailyPrice.trade_date)
    keys: list[str] | None = None
    if tickers is not None:
        keys = _normalise_tickers(tickers)
        if not keys:
            return {}
        stmt = stmt.where(TerminalDailyPrice.ticker.in_(keys))
    if start is not None:
        stmt = stmt.where(TerminalDailyPrice.trade_date >= start)
    if end is not None:
        stmt = stmt.where(TerminalDailyPrice.trade_date <= end)
    return await _collect_price_rows(session, stmt, keys)


async def load_price_rows_in_ranges(
    session: AsyncSession,
    tickers: Collection[str],
    ranges: Sequence[tuple[date, date]],
) -> dict[str, list[PriceRow]]:
    """Return price history per ticker within any of several date ranges, in one statement.

    The rows of each ticker dated inside any inclusive ``(start, end)`` range of ``ranges``,
    ascending by ``trade_date``, with the same columns and units as :func:`load_price_rows`.
    One ``ticker IN (...) AND (trade_date BETWEEN ... OR ...)`` statement reads them, served by
    the ``(ticker, trade_date)`` unique index and the ``trade_date`` index, so the history
    between the ranges is never read. A caller that needs several windows passes them merged
    and slices each window out of the result, which then holds exactly the rows
    :func:`load_price_rows` returns for that window.

    Args:
        session: An async session on the terminal database; it is only read.
        tickers: Tickers to load (trimmed and upper-cased), each returned as a key even without
            rows. An empty collection returns ``{}`` without a statement.
        ranges: Inclusive ``(start, end)`` date ranges. Without any, every key is returned empty
            and no statement runs.
    """
    keys = _normalise_tickers(tickers)
    if not keys:
        return {}
    if not ranges:
        return {ticker: [] for ticker in keys}
    stmt = (
        _price_select()
        .where(
            TerminalDailyPrice.ticker.in_(keys),
            or_(*(TerminalDailyPrice.trade_date.between(start, end) for start, end in ranges)),
        )
        .order_by(TerminalDailyPrice.ticker, TerminalDailyPrice.trade_date)
    )
    return await _collect_price_rows(session, stmt, keys)


async def load_fundamentals_rows(
    session: AsyncSession, tickers: Collection[str] | None
) -> dict[str, list[FundamentalsRow]]:
    """Return every reported period per ticker, ascending by ``period_end_date``.

    Rows of both period types and every currency are returned with their ``currency``; amounts
    are whole units of it (KES by default), never thousands and never converted, and ``dps_kes``
    is the dividend per share in it. :func:`~opennse_terminal.quant.asof.fundamentals_as_of`
    keeps fiscal years only and applies the publication lag; the valuation and the backtester
    refuse a row not in KES with ``currency_mismatch``.

    Args:
        session: An async session on the terminal database; it is only read.
        tickers: Tickers to load (trimmed and upper-cased), each returned as a key even without
            rows; ``None`` loads every ticker that has rows.
    """
    stmt = select(
        TerminalFundamental.ticker,
        TerminalFundamental.period_end_date,
        TerminalFundamental.period_type,
        TerminalFundamental.currency,
        TerminalFundamental.net_income_kes,
        TerminalFundamental.total_equity_kes,
        TerminalFundamental.ebitda_kes,
        TerminalFundamental.total_debt_kes,
        TerminalFundamental.cash_and_equivalents_kes,
        TerminalFundamental.dps_kes,
        TerminalFundamental.revenue_kes,
    ).order_by(
        TerminalFundamental.ticker,
        TerminalFundamental.period_end_date,
        TerminalFundamental.period_type,
    )
    keys: list[str] = []
    if tickers is not None:
        keys = _normalise_tickers(tickers)
        if not keys:
            return {}
        stmt = stmt.where(TerminalFundamental.ticker.in_(keys))
    grouped: dict[str, list[FundamentalsRow]] = {ticker: [] for ticker in keys}
    result = await session.execute(stmt)
    for (
        ticker,
        period_end_date,
        period_type,
        currency,
        net_income_kes,
        total_equity_kes,
        ebitda_kes,
        total_debt_kes,
        cash_kes,
        dps_kes,
        revenue_kes,
    ) in result:
        grouped.setdefault(ticker, []).append(
            FundamentalsRow(
                period_end_date=period_end_date,
                period_type=period_type,
                currency=currency,
                net_income_kes=net_income_kes,
                total_equity_kes=total_equity_kes,
                ebitda_kes=ebitda_kes,
                total_debt_kes=total_debt_kes,
                cash_and_equivalents_kes=cash_kes,
                dps_kes=dps_kes,
                revenue_kes=revenue_kes,
            )
        )
    return grouped


# ---------------------------------------------------------------------------------------------
# GET /market/screener
# ---------------------------------------------------------------------------------------------


async def _load_recent_rows(
    session: AsyncSession, as_of: date, per_ticker: int
) -> dict[str, list[PriceRow]]:
    """Return each equity's last ``per_ticker`` sessions on or before ``as_of``, ascending.

    One LATERAL statement walks the ``(ticker, trade_date)`` index backwards once per equity, so
    a counter that stopped trading long ago still yields its own latest row and the rows before
    it, without reading anyone's older history.
    """
    recent = (
        _price_select()
        .where(
            TerminalDailyPrice.ticker == TerminalEquity.ticker,
            TerminalDailyPrice.trade_date <= as_of,
        )
        .order_by(TerminalDailyPrice.trade_date.desc())
        .limit(per_ticker)
        .lateral("recent_prices")
    )
    stmt = (
        select(
            recent.c.ticker,
            recent.c.trade_date,
            recent.c.close_price,
            recent.c.volume,
            recent.c.turnover_kes,
            recent.c.foreign_net_flow_kes,
        )
        .select_from(TerminalEquity)
        .join(recent, true())
        .order_by(recent.c.ticker, recent.c.trade_date)
    )
    return await _collect_price_rows(session, stmt, None)


def _observations(rows: Sequence[PriceRow], metric: _SeriesMetric) -> list[Observation]:
    """One ticker's ``metric`` per row: volume as an exact ``Decimal`` of shares, or turnover."""
    if metric == "volume":
        return [Observation(row.trade_date, Decimal(row.volume)) for row in rows]
    return [Observation(row.trade_date, row.turnover_kes) for row in rows]


def _within(value: Decimal | None, low: Decimal | None, high: Decimal | None) -> bool:
    """Whether ``value`` passes the inclusive bounds; a null value fails any bound on it."""
    if low is None and high is None:
        return True
    if value is None:
        return False
    return (low is None or value >= low) and (high is None or value <= high)


def _passes_filters(
    row: ScreenerRow, params: ScreenerParams, watchlist: frozenset[str] | None
) -> bool:
    """Apply the screener filters to one row's quantized values, the values the client sees."""
    if params.sector is not None and row.sector != params.sector:
        return False
    if watchlist is not None and row.ticker not in watchlist:
        return False
    if params.anomalies_only and not (row.is_volume_anomaly or row.is_turnover_anomaly):
        return False
    return (
        _within(row.pe, params.min_pe, params.max_pe)
        and _within(row.pb, params.min_pb, params.max_pb)
        and _within(row.ev_ebitda, params.min_ev_ebitda, params.max_ev_ebitda)
        and _within(row.dividend_yield_pct, params.min_dividend_yield, None)
        and _within(row.volume_z, params.min_volume_z, None)
    )


def _sort_value(row: ScreenerRow, sort: ScreenerSort) -> Decimal | None:
    """The quantized metric ``sort`` names (any column but ``ticker``)."""
    if sort == "market_cap_kes":
        return row.market_cap_kes
    if sort == "pe":
        return row.pe
    if sort == "pb":
        return row.pb
    if sort == "ev_ebitda":
        return row.ev_ebitda
    if sort == "dividend_yield_pct":
        return row.dividend_yield_pct
    return row.volume_z


def _sort_rows(
    rows: Sequence[ScreenerRow], sort: ScreenerSort, order: SortOrder
) -> list[ScreenerRow]:
    """Sort by ``sort`` in ``order``; nulls last in either order, ties by ticker ascending."""
    by_ticker = sorted(rows, key=lambda row: row.ticker)
    descending = order == "desc"
    if sort == "ticker":
        return by_ticker[::-1] if descending else by_ticker
    present: list[tuple[Decimal, ScreenerRow]] = []
    missing: list[ScreenerRow] = []
    for row in by_ticker:
        value = _sort_value(row, sort)
        if value is None:
            missing.append(row)
        else:
            present.append((value, row))
    # list.sort is stable with reverse=True too, so equal values keep ticker order.
    present.sort(key=lambda pair: pair[0], reverse=descending)
    return [row for _, row in present] + missing


def _screener_row(
    equity: TerminalEquity,
    history: Sequence[PriceRow],
    valuation: ValuationResult,
    volume_point: ZScorePoint,
    turnover_point: ZScorePoint | None,
    threshold: Decimal,
) -> ScreenerRow:
    """Build one screener row, quantizing every value once.

    The spike flags apply :func:`~opennse_terminal.quant.zscore.is_spike` to the unrounded
    Z-scores, the one rule the anomaly feed uses too. ``turnover_point`` is ``None`` for an
    equity not listed in KES, whose turnover is not scored: ``turnover_z`` is null with
    ``turnover_z:currency_mismatch`` and the row is never a turnover anomaly.
    """
    latest = history[-1]
    previous_close = history[-2].close_price if len(history) > 1 else None
    change = price_change_pct(latest.close_price, previous_close)
    flags = list(valuation.flags)
    if change.flag is not None:
        flags.append(f"change_pct:{change.flag}")
    flags.extend(f"volume_z:{code}" for code in volume_point.flags)
    turnover_z: Decimal | None = None
    if turnover_point is None:
        flags.append("turnover_z:currency_mismatch")
    else:
        flags.extend(f"turnover_z:{code}" for code in turnover_point.flags)
        turnover_z = turnover_point.z
    return ScreenerRow(
        ticker=equity.ticker,
        company_name=equity.company_name,
        sector=equity.sector,
        currency=equity.currency,
        trade_date=latest.trade_date,
        close_price=quantize_out(latest.close_price, "per_share"),
        change_pct=quantize_out(change.value, "pct"),
        market_cap_kes=quantize_out(valuation.market_cap_kes, "money"),
        eps_kes=quantize_out(valuation.eps_kes, "per_share"),
        pe=quantize_out(valuation.pe, "ratio"),
        pb=quantize_out(valuation.pb, "ratio"),
        ev_ebitda=quantize_out(valuation.ev_ebitda, "ratio"),
        dividend_yield_pct=quantize_out(valuation.dividend_yield_pct, "pct"),
        volume=latest.volume,
        turnover_kes=quantize_out(latest.turnover_kes, "money"),
        volume_z=quantize_out(volume_point.z, "z"),
        turnover_z=quantize_out(turnover_z, "z"),
        is_volume_anomaly=is_spike(volume_point.z, threshold),
        is_turnover_anomaly=is_spike(turnover_z, threshold),
        free_float_pct=quantize_out(equity.free_float_pct, "pct"),
        fundamentals_period_end=valuation.fundamentals_period_end,
        flags=flags,
    )


def _sector_aggregate_rows(
    equities: Sequence[TerminalEquity], valuations: dict[str, ValuationResult]
) -> list[SectorAggregate]:
    """One aggregate per sector of the whole universe, sorted by sector name.

    ``constituent_count`` counts every equity of the sector, including one without a close on or
    before ``as_of``, which cannot be valued and so enters no eligible set; the eligible counts
    beside it then show the partial coverage instead of hiding it.
    """
    by_sector: dict[str, list[TerminalEquity]] = defaultdict(list)
    for equity in equities:
        by_sector[equity.sector].append(equity)
    aggregates: list[SectorAggregate] = []
    for sector in sorted(by_sector):
        members = by_sector[sector]
        result = sector_aggregates(
            sector, [valuations[m.ticker] for m in members if m.ticker in valuations]
        )
        aggregates.append(
            SectorAggregate(
                sector=sector,
                constituent_count=len(members),
                market_cap_kes=quantize_out(result.market_cap_kes, "money"),
                pe=quantize_out(result.pe, "ratio"),
                pb=quantize_out(result.pb, "ratio"),
                ev_ebitda=quantize_out(result.ev_ebitda, "ratio"),
                dividend_yield_pct=quantize_out(result.dividend_yield_pct, "pct"),
                pe_eligible_count=result.pe_eligible_count,
                pb_eligible_count=result.pb_eligible_count,
                ev_ebitda_eligible_count=result.ev_ebitda_eligible_count,
                dy_eligible_count=result.dy_eligible_count,
                flags=list(result.flags),
            )
        )
    return aggregates


def _screener_rows(
    equities: Sequence[TerminalEquity],
    recent: dict[str, list[PriceRow]],
    sessions: Sequence[date],
    fundamentals: dict[str, list[FundamentalsRow]],
    as_of: date,
    params: ScreenerParams,
    lag_days: int,
) -> tuple[dict[str, ValuationResult], list[ScreenerRow]]:
    """Value every equity with a close on or before ``as_of`` and score its latest session.

    Pure computation over rows already read, which :func:`get_screener` runs in a worker thread.
    Returns the valuations by ticker, for the sector aggregates, and one quantized row per
    valued equity in ``equities`` order. Only the latest session is scored (``score_from``),
    against the up to ``z_window`` rows before it, which serve as its window observations.
    Volume is scored for every equity, turnover only for one listed in KES.
    """
    valuations: dict[str, ValuationResult] = {}
    rows: list[ScreenerRow] = []
    for equity in equities:
        history = recent.get(equity.ticker)
        if not history:
            continue
        latest = history[-1]
        valuation = compute_valuation(
            ValuationInputs(
                ticker=equity.ticker,
                sector=equity.sector,
                shares_outstanding=equity.shares_outstanding,
                close_price=latest.close_price,
                price_date=latest.trade_date,
                as_of=as_of,
                currency=equity.currency,
                fundamentals=fundamentals_as_of(
                    fundamentals.get(equity.ticker, []), as_of, lag_days=lag_days
                ),
            )
        )
        valuations[equity.ticker] = valuation
        volume_point = rolling_zscores(
            _observations(history, "volume"),
            sessions,
            window=params.z_window,
            score_from=latest.trade_date,
        )[-1]
        # Turnover is an amount in the listing currency and turnover_z names no currency, so, as
        # in the anomaly feed, only a KES-listed equity's turnover is scored, never a converted
        # one; volume, in shares, is scored for every equity. One rule then decides both the
        # screener's turnover anomalies and the feed's turnover items.
        turnover_point = (
            rolling_zscores(
                _observations(history, "turnover"),
                sessions,
                window=params.z_window,
                score_from=latest.trade_date,
            )[-1]
            if equity.currency == ANALYTICS_CURRENCY
            else None
        )
        rows.append(
            _screener_row(
                equity, history, valuation, volume_point, turnover_point, params.anomaly_threshold
            )
        )
    return valuations, rows


async def get_screener(
    session: AsyncSession,
    params: ScreenerParams,
    principal: OwnerPrincipal,
    settings: TerminalSettings,
    *,
    abandoned: AbandonCheck | None = None,
) -> ScreenerResponse:
    """Build ``GET /market/screener``: every equity valued as of ``as_of``, filtered and sorted.

    Per row (as of ``as_of``, default the latest stored ``trade_date``): the latest close on or
    before ``as_of`` (per share) and its change from the previous close (%); market cap (KES);
    EPS (KES per share); P/E, P/B and EV/EBITDA (x); dividend yield (%); volume (shares) and
    turnover of that session; and the rolling ``z_window`` volume and turnover Z-scores of that
    session against the ticker's own prior rows. The close and turnover are in the equity's
    ``currency`` (KES by default), as stored and never converted. Market cap, EPS, the multiples
    and dividend yield are KES-only: a non-KES equity has them all null with
    ``currency_mismatch``, and a KES equity whose fundamentals are not in KES has all but the
    market cap null with it. The turnover Z-score is KES-only too, as in the anomaly feed: a
    non-KES equity's turnover is not scored, so its ``turnover_z`` is null with
    ``turnover_z:currency_mismatch`` and it is never a turnover anomaly, while its volume
    (shares) is scored. Fundamentals are the fiscal year available
    ``settings.fundamentals_lag_days`` after period end. Banking equities have a
    null EV/EBITDA with ``ev_not_meaningful_for_banks``. A null change carries its reason
    prefixed ``change_pct:``, as in ``change_pct:no_previous_close`` on a counter's first
    session, and Z-score reasons are prefixed ``volume_z:`` or ``turnover_z:``. A row is an
    anomaly when its unrounded Z-score is at or above ``anomaly_threshold`` (positive side only).

    Filters compare the quantized values; a null metric fails any filter on it. Rows sort by
    ``sort`` in ``order`` with nulls last and ties by ticker. ``sector_aggregates`` covers every
    sector of the universe as of ``as_of``, independent of the filters. With no price row at
    all, ``as_of`` is today's date in ``settings.timezone`` and both lists are empty.

    The valuation and scoring run in a worker thread after the reads. ``abandoned``, when given,
    is awaited before it starts.

    Errors, in check order:

    * 422 ``min_exceeds_max`` (``field`` is the ``min_*`` parameter) for a ``min_*`` above its
      ``max_*``;
    * 404 ``unknown_sector`` for a ``sector`` without equities;
    * 404 ``watchlist_not_found`` for a ``watchlist_id`` the principal does not own.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away; nothing is
            computed.
    """
    bounds = (
        ("min_pe", params.min_pe, "max_pe", params.max_pe),
        ("min_pb", params.min_pb, "max_pb", params.max_pb),
        ("min_ev_ebitda", params.min_ev_ebitda, "max_ev_ebitda", params.max_ev_ebitda),
    )
    for min_name, low, max_name, high in bounds:
        if low is not None and high is not None and low > high:
            raise _coded_error(
                _HTTP_422,
                "min_exceeds_max",
                f"{min_name} ({low}) must not exceed {max_name} ({high}).",
                min_name,
            )
    if params.sector is not None:
        await ensure_sector(session, params.sector)
    watchlist: frozenset[str] | None = None
    if params.watchlist_id is not None:
        watchlist = frozenset(
            await owned_watchlist_tickers(session, principal, params.watchlist_id)
        )

    as_of, has_prices = await _resolve_as_of(session, params.as_of, settings)
    empty = ScreenerResponse(
        as_of=as_of,
        z_window=params.z_window,
        anomaly_threshold=params.anomaly_threshold,
        count=0,
        rows=[],
        sector_aggregates=[],
    )
    if not has_prices:
        return empty
    equities = await load_equities(session)
    # The latest row plus up to z_window prior rows is all a latest-session Z-score can use.
    recent = await _load_recent_rows(session, as_of, params.z_window + 1)
    if not recent:
        return empty
    # Sessions from the earliest loaded row on cover every window: rows older than the first
    # session are never in a window, and none was loaded.
    sessions = await load_market_sessions(
        session, min(rows[0].trade_date for rows in recent.values()), as_of
    )
    fundamentals = await load_fundamentals_rows(session, None)

    valuations, rows = await compute(
        abandoned,
        _screener_rows,
        equities,
        recent,
        sessions,
        fundamentals,
        as_of,
        params,
        settings.fundamentals_lag_days,
    )

    selected = _sort_rows(
        [row for row in rows if _passes_filters(row, params, watchlist)], params.sort, params.order
    )
    logger.debug("screener as of %s: %d of %d rows selected", as_of, len(selected), len(rows))
    return ScreenerResponse(
        as_of=as_of,
        z_window=params.z_window,
        anomaly_threshold=params.anomaly_threshold,
        count=len(selected),
        rows=selected,
        sector_aggregates=_sector_aggregate_rows(equities, valuations),
    )


# ---------------------------------------------------------------------------------------------
# GET /market/sector-heatmap
# ---------------------------------------------------------------------------------------------


async def _load_closes_near(
    session: AsyncSession, tickers: Sequence[str], boundaries: Sequence[date], max_age_days: int
) -> dict[str, list[PriceRow]]:
    """Return the rows dated within ``max_age_days`` on or before any boundary, per ticker.

    A boundary's latest close within the age limit lies in its own range, so the rows outside
    every range never change a heatmap cell and are not read.
    """
    if not tickers or not boundaries:
        return {ticker: [] for ticker in tickers}
    near_boundary = or_(
        *(
            TerminalDailyPrice.trade_date.between(_days_before(boundary, max_age_days), boundary)
            for boundary in boundaries
        )
    )
    stmt = (
        _price_select()
        .where(TerminalDailyPrice.ticker.in_(tickers), near_boundary)
        .order_by(TerminalDailyPrice.ticker, TerminalDailyPrice.trade_date)
    )
    return await _collect_price_rows(session, stmt, tickers)


def _heatmap_cell(cell: HeatmapCellResult) -> HeatmapCell:
    """Quantize one heatmap cell; both returns are percentage points at 2 dp."""
    return HeatmapCell(
        period_end=cell.period_end,
        return_pct=quantize_out(cell.return_pct, "pct"),
        relative_return_pct=quantize_out(cell.relative_return_pct, "pct"),
        constituents=cell.constituents,
    )


async def get_sector_heatmap(
    session: AsyncSession,
    *,
    interval: HeatmapInterval = "month",
    periods: int = 12,
    as_of: date | None = None,
    settings: TerminalSettings | None = None,
) -> SectorHeatmapResponse:
    """Build ``GET /market/sector-heatmap``: capitalization-weighted sector and market returns.

    Boundaries are the last market sessions on or before each month-end (``interval="month"``)
    or Friday (``"week"``) for the ``periods`` periods ending at ``as_of`` (default the latest
    stored ``trade_date``). For each period from boundary ``s`` to ``e``::

        return_pct = (sum(shares x close_e) / sum(shares x close_s) - 1) x 100

    over the constituents with a close within 10 calendar days on or before both boundaries, in
    percentage points; ``relative_return_pct`` is the sector return minus the market return and
    is null on the market row. A sector without a qualifying constituent, or whose qualifying
    constituents have zero starting capitalization, has a null cell. Sectors are sorted by name.

    Only equities listed in KES take part: summing capitalizations across currencies would mix
    units, and nothing is converted. With fewer than two boundaries every list is empty; with no
    price row at all ``as_of`` is today's date in ``settings.timezone`` (Africa/Nairobi when no
    settings are given). Raises no coded error.
    """
    resolved, has_prices = await _resolve_as_of(session, as_of, settings)
    empty = SectorHeatmapResponse(
        as_of=resolved, interval=interval, period_ends=[], market=[], sectors=[]
    )
    if not has_prices:
        return empty
    sessions = await load_market_sessions(session, None, resolved)
    boundaries = period_boundaries(sessions, interval, periods, resolved)
    if len(boundaries) < 2:
        return empty

    equities = await load_equities(session)
    members = [equity for equity in equities if equity.currency == ANALYTICS_CURRENCY]
    if len(members) < len(equities):
        logger.debug(
            "sector heatmap leaves out %d equities not listed in %s",
            len(equities) - len(members),
            ANALYTICS_CURRENCY,
        )
    prices = await _load_closes_near(
        session, [equity.ticker for equity in members], boundaries, CLOSE_MAX_AGE_DAYS
    )
    result = sector_returns(
        [
            HeatmapMember(
                ticker=equity.ticker,
                sector=equity.sector,
                shares_outstanding=equity.shares_outstanding,
                prices=prices.get(equity.ticker, []),
            )
            for equity in members
        ],
        boundaries,
    )
    return SectorHeatmapResponse(
        as_of=resolved,
        interval=interval,
        period_ends=list(result.period_ends),
        market=[_heatmap_cell(cell) for cell in result.market],
        sectors=[
            SectorHeatmapRow(sector=row.sector, cells=[_heatmap_cell(cell) for cell in row.cells])
            for row in result.sectors
        ],
    )


# ---------------------------------------------------------------------------------------------
# GET /market/valuation-bands
# ---------------------------------------------------------------------------------------------


def _counter_daily(
    equity: TerminalEquity,
    prices: Sequence[PriceRow],
    fundamentals: Sequence[FundamentalsRow],
    metric: BandMetric,
    lag_days: int,
) -> tuple[list[tuple[date, Decimal | None]], bool]:
    """The counter's daily multiple on each of its sessions, and whether a currency was refused."""
    daily: list[tuple[date, Decimal | None]] = []
    refused = False
    for row in prices:
        result = compute_valuation(
            ValuationInputs(
                ticker=equity.ticker,
                sector=equity.sector,
                shares_outstanding=equity.shares_outstanding,
                close_price=row.close_price,
                price_date=row.trade_date,
                as_of=row.trade_date,
                currency=equity.currency,
                fundamentals=fundamentals_as_of(fundamentals, row.trade_date, lag_days=lag_days),
            )
        )
        refused = refused or "currency_mismatch" in result.flags
        daily.append((row.trade_date, _band_value(result, metric)))
    return daily, refused


def _sector_daily(
    sector: str,
    members: Sequence[TerminalEquity],
    prices: dict[str, list[PriceRow]],
    fundamentals: dict[str, list[FundamentalsRow]],
    sessions: Sequence[date],
    metric: BandMetric,
    lag_days: int,
) -> tuple[list[tuple[date, Decimal | None]], bool]:
    """The sector's matched-set daily multiple on each of its sessions, and whether a currency was
    refused.

    The sector's sessions are those of ``sessions`` on which at least one member has a price row.
    A session on which no member traded is not a session with a close for the sector: it gets no
    entry at all, neither a value nor a null day. On each of the sector's sessions every member
    contributes its latest close within ``SECTOR_BAND_CLOSE_MAX_AGE_DAYS`` on or before the
    session, valued with that session's available fundamentals and the equity's
    ``shares_outstanding``; a member without such a close is left out that day. A one-member
    sector therefore has exactly its counter's sessions, each valued at that day's close.
    """
    traded = {row.trade_date for equity in members for row in prices.get(equity.ticker, [])}
    daily: list[tuple[date, Decimal | None]] = []
    refused = False
    for day in sessions:
        if day not in traded:
            continue
        results: list[ValuationResult] = []
        for equity in members:
            close = close_as_of(
                prices.get(equity.ticker, []), day, max_age_days=SECTOR_BAND_CLOSE_MAX_AGE_DAYS
            )
            if close is None:
                continue
            results.append(
                compute_valuation(
                    ValuationInputs(
                        ticker=equity.ticker,
                        sector=equity.sector,
                        shares_outstanding=equity.shares_outstanding,
                        close_price=close.value,
                        price_date=close.observation_date,
                        as_of=day,
                        currency=equity.currency,
                        fundamentals=fundamentals_as_of(
                            fundamentals.get(equity.ticker, []), day, lag_days=lag_days
                        ),
                    )
                )
            )
        aggregate = sector_aggregates(sector, results)
        refused = refused or "currency_mismatch" in aggregate.flags
        daily.append((day, aggregate.pe if metric == "pe" else aggregate.pb))
    return daily, refused


def _band_value(result: ValuationResult, metric: BandMetric) -> Decimal | None:
    """The counter's P/E or P/B, as ``metric`` names."""
    return result.pe if metric == "pe" else result.pb


def _subject_bands(
    subject: str,
    subject_type: SubjectType,
    members: Sequence[TerminalEquity],
    prices: dict[str, list[PriceRow]],
    fundamentals: dict[str, list[FundamentalsRow]],
    sessions: Sequence[date],
    metric: BandMetric,
    lag_days: int,
    requested_start: date,
) -> tuple[BandsResult, bool]:
    """The subject's daily ``metric`` summarized into bands, and whether a currency was refused.

    Pure computation over rows already read, which :func:`get_valuation_bands` runs in a worker
    thread: the counter's daily multiple (:func:`_counter_daily`, from ``members[0]``) or the
    sector's (:func:`_sector_daily`), then
    :func:`~opennse_terminal.quant.bands.valuation_bands` over ``sessions`` from
    ``requested_start``.
    """
    if subject_type == "ticker":
        counter = members[0]
        daily, refused = _counter_daily(
            counter, prices[counter.ticker], fundamentals[counter.ticker], metric, lag_days
        )
    else:
        daily, refused = _sector_daily(
            subject, members, prices, fundamentals, sessions, metric, lag_days
        )
    bands = valuation_bands(
        daily, requested_start=requested_start, sessions=sessions, metric=metric
    )
    return bands, refused


def _band_stats(stats: BandStatsResult) -> BandStats:
    """Quantize band statistics: every multiple as a ratio (x), the percentile as a percentage."""
    return BandStats(
        observations=stats.observations,
        mean=quantize_out(stats.mean, "ratio"),
        stdev=quantize_out(stats.stdev, "ratio"),
        minus_2sd=quantize_out(stats.minus_2sd, "ratio"),
        minus_1sd=quantize_out(stats.minus_1sd, "ratio"),
        plus_1sd=quantize_out(stats.plus_1sd, "ratio"),
        plus_2sd=quantize_out(stats.plus_2sd, "ratio"),
        min=quantize_out(stats.min, "ratio"),
        p25=quantize_out(stats.p25, "ratio"),
        median=quantize_out(stats.median, "ratio"),
        p75=quantize_out(stats.p75, "ratio"),
        max=quantize_out(stats.max, "ratio"),
        current=quantize_out(stats.current, "ratio"),
        current_percentile=quantize_out(stats.current_percentile, "pct"),
    )


async def get_valuation_bands(
    session: AsyncSession,
    settings: TerminalSettings,
    *,
    ticker: str | None = None,
    sector: str | None = None,
    metric: BandMetric = "pe",
    lookback_years: int = 5,
    as_of: date | None = None,
    abandoned: AbandonCheck | None = None,
) -> ValuationBandsResponse:
    """Build ``GET /market/valuation-bands``: historical P/E or P/B bands of a counter or sector.

    The lookback runs from ``requested_start`` (``as_of`` minus ``lookback_years`` calendar
    years, the day clamped to the month's length) to ``as_of`` (default the latest stored
    ``trade_date``). The daily multiple (x) is computed on each market session with a close:

    * per counter, on every session the ticker has a close, from that close and the fiscal-year
      fundamentals available that day (``settings.fundamentals_lag_days`` after period end);
    * per sector, on every market session on which at least one member has a close, as the
      matched-set sector aggregate over the members whose latest close is at most 10 calendar
      days old on that day (a member that did not trade that day carries such a close), each
      with that day's fundamentals and the equity's ``shares_outstanding``. A session on which
      no member traded has no sector value, so a one-member sector has its counter's sessions.

    Undefined days (losses for P/E, non-positive equity for P/B) are excluded. The statistics
    (mean, sample deviation, mean +/- 1 and 2 deviations, min, quartiles, max, current, all x;
    ``current_percentile`` in %) need at least 60 values and are present only when ``status``
    is ``ok``; ``series`` is the weekly transport sample. ``flags`` adds ``currency_mismatch``
    when a value or member was refused for a non-KES currency. A lookback without any session
    with a close for the subject has 0 observations: ``status`` is ``insufficient_history``,
    with an empty series and no ``coverage_start``; ``no_positive_earnings`` (pe) and
    ``no_positive_equity`` (pb) are reserved for a lookback whose sessions are all undefined.
    With no price row at all, ``as_of`` is today's date in ``settings.timezone``.

    The daily series and its statistics are computed in a worker thread after the reads.
    ``abandoned``, when given, is awaited before that starts.

    Errors: 422 ``exactly_one_subject`` unless exactly one of ``ticker`` and ``sector`` is given;
    404 ``unknown_ticker`` or ``unknown_sector``.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away; nothing is
            computed.
    """
    subject_type: SubjectType
    if ticker is not None and sector is None:
        equity = await ensure_ticker(session, ticker)
        members = [equity]
        subject = equity.ticker
        subject_type = "ticker"
    elif sector is not None and ticker is None:
        await ensure_sector(session, sector)
        members = await load_equities(session, sector=sector)
        subject = sector
        subject_type = "sector"
    else:
        raise _coded_error(
            _HTTP_422, "exactly_one_subject", "Give exactly one of ticker or sector.", None
        )

    resolved, _has_prices = await _resolve_as_of(session, as_of, settings)
    requested_start = _months_before(resolved, 12 * lookback_years)
    sessions = await load_market_sessions(session, requested_start, resolved)
    tickers = [member.ticker for member in members]
    lag_days = settings.fundamentals_lag_days
    fundamentals = await load_fundamentals_rows(session, tickers)
    if subject_type == "ticker":
        prices = await load_price_rows(session, tickers, requested_start, resolved)
    else:
        prices = await load_price_rows(
            session,
            tickers,
            _days_before(requested_start, SECTOR_BAND_CLOSE_MAX_AGE_DAYS),
            resolved,
        )

    bands, refused = await compute(
        abandoned,
        _subject_bands,
        subject,
        subject_type,
        members,
        prices,
        fundamentals,
        sessions,
        metric,
        lag_days,
        requested_start,
    )
    flags = list(bands.flags)
    if refused:
        flags.append("currency_mismatch")
    return ValuationBandsResponse(
        subject=subject,
        subject_type=subject_type,
        metric=metric,
        as_of=resolved,
        coverage=CoverageMeta(
            requested_start=bands.requested_start,
            coverage_start=bands.coverage_start,
            is_partial_coverage=bands.is_partial_coverage,
        ),
        status=bands.status,
        flags=flags,
        series=[
            SeriesPoint(date=day, value=quantize_out(value, "ratio")) for day, value in bands.series
        ],
        stats=(
            _band_stats(bands.stats) if bands.status == "ok" and bands.stats is not None else None
        ),
    )


# ---------------------------------------------------------------------------------------------
# GET /market/anomalies
# ---------------------------------------------------------------------------------------------


def _anomaly_metrics(metric: AnomalyMetric) -> tuple[_SeriesMetric, ...]:
    """The series ``metric`` asks the feed to scan."""
    if metric == "volume":
        return ("volume",)
    if metric == "turnover":
        return ("turnover",)
    return ("volume", "turnover")


def _anomaly_item(
    ticker: str, metric: _SeriesMetric, point: ZScorePoint, threshold: Decimal
) -> AnomalyItem | None:
    """Quantize a positive spike as an item, or ``None`` when ``point`` is not one."""
    if (
        point.z is None
        or point.rolling_mean is None
        or point.rolling_stdev is None
        or not is_spike(point.z, threshold)
    ):
        return None
    return AnomalyItem(
        ticker=ticker,
        trade_date=point.trade_date,
        metric=metric,
        # A volume is an integral Decimal of shares and stays exact; turnover is at 2 dp in KES,
        # because get_anomalies scores turnover only for equities listed in KES.
        value=point.value if metric == "volume" else quantize_out(point.value, "money"),
        rolling_mean=quantize_out(point.rolling_mean, "money"),
        rolling_stdev=quantize_out(point.rolling_stdev, "money"),
        z_score=quantize_out(point.z, "z"),
        observations_used=point.observations_used,
    )


def _series_spikes(
    ticker: str,
    metric: _SeriesMetric,
    rows: Sequence[PriceRow],
    sessions: Sequence[date],
    window: int,
    threshold: Decimal,
    first_target: date,
) -> list[AnomalyItem]:
    """The quantized positive spikes of one ticker's ``metric`` dated ``first_target`` or later.

    Pure computation over rows already read, which :func:`get_anomalies` runs in a worker thread
    once per series. Only the rows from ``first_target`` on are scored (``score_from``); the
    earlier rows serve only as their window observations.
    """
    items: list[AnomalyItem] = []
    for point in rolling_zscores(
        _observations(rows, metric), sessions, window=window, score_from=first_target
    ):
        item = _anomaly_item(ticker, metric, point, threshold)
        if item is not None:
            items.append(item)
    return items


async def get_anomalies(
    session: AsyncSession,
    *,
    metric: AnomalyMetric = "both",
    window: int = DEFAULT_Z_WINDOW,
    threshold: Decimal = DEFAULT_THRESHOLD,
    lookback_sessions: int = 20,
    ticker: str | None = None,
    sector: str | None = None,
    abandoned: AbandonCheck | None = None,
) -> AnomaliesResponse:
    """Build ``GET /market/anomalies``: recent positive volume and turnover spikes.

    Scans the last ``lookback_sessions`` market sessions up to the latest stored ``trade_date``.
    Each row of each candidate is scored against up to ``window`` of the ticker's own rows from
    the ``2 x window`` market sessions before it (sessions without a row are skipped, never
    zero-filled), and kept when its unrounded Z-score is at or above ``threshold``. Negative
    Z-scores are never anomalies. ``value``, ``rolling_mean`` and ``rolling_stdev`` are in the
    metric's unit (shares for volume, KES for turnover); the mean and deviation are at 2 dp and
    ``z_score`` is in standard deviations at 2 dp.

    Candidates are every equity, or ``ticker``, or the equities of ``sector``; with both, the
    ticker only if it belongs to the sector. Volume is scored for every candidate whatever its
    currency. Turnover is scored only for candidates listed in KES: an item names no currency,
    so a non-KES equity's turnover, stored in its own currency, is never scored, returned or
    converted, and ``metric="turnover"`` for such a ticker gives no items. Items sort by
    ``trade_date`` descending, then ``z_score`` descending, then ticker. An empty price table
    gives no items.

    Each ticker's volume series, and its turnover series where scored, runs in a worker thread,
    one series at a time, after the reads. ``abandoned``, when given, is awaited before each
    series, so an abandoned request stops within one series.

    Errors: 404 ``unknown_ticker`` or ``unknown_sector``.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away; the series not
            yet scored are never computed.
    """
    equity = await ensure_ticker(session, ticker) if ticker is not None else None
    if sector is not None:
        await ensure_sector(session, sector)
    empty = AnomaliesResponse(window=window, threshold=threshold, items=[])
    if equity is not None and sector is not None and equity.sector != sector:
        return empty
    latest = await latest_trade_date(session)
    if latest is None:
        return empty

    sessions = await _last_sessions(session, latest, lookback_sessions + 2 * window)
    first_target = sessions[-lookback_sessions:][0]
    series_metrics = _anomaly_metrics(metric)
    candidates: list[TerminalEquity]
    tickers: list[str] | None
    if equity is not None:
        candidates = [equity]
        tickers = [equity.ticker]
    elif sector is not None:
        candidates = await load_equities(session, sector=sector)
        tickers = [member.ticker for member in candidates]
    else:
        candidates = await load_equities(session)
        tickers = None
    # Turnover is an amount in the listing currency and an item names no currency, so only a
    # KES-listed equity's turnover is scored, never a converted one; volume, in shares, is scored
    # for every candidate. A ticker missing from the candidates is refused turnover the same way.
    turnover_tickers = {
        candidate.ticker for candidate in candidates if candidate.currency == ANALYTICS_CURRENCY
    }
    if "turnover" in series_metrics and len(turnover_tickers) < len(candidates):
        logger.debug(
            "anomaly feed scores no turnover for %d equities not listed in %s",
            len(candidates) - len(turnover_tickers),
            ANALYTICS_CURRENCY,
        )
    prices = await load_price_rows(session, tickers, sessions[0], latest)

    items: list[AnomalyItem] = []
    for name, rows in prices.items():
        for series in series_metrics:
            if series == "turnover" and name not in turnover_tickers:
                continue
            items.extend(
                await compute(
                    abandoned,
                    _series_spikes,
                    name,
                    series,
                    rows,
                    sessions,
                    window,
                    threshold,
                    first_target,
                )
            )
    items.sort(
        key=lambda item: (
            -item.trade_date.toordinal(),
            -item.z_score,
            item.ticker,
            item.metric,
        )
    )
    return AnomaliesResponse(window=window, threshold=threshold, items=items)


# ---------------------------------------------------------------------------------------------
# GET /market/flows
# ---------------------------------------------------------------------------------------------


async def get_flows(
    session: AsyncSession,
    *,
    ticker: str | None = None,
    sector: str | None = None,
    as_of: date | None = None,
    start: date | None = None,
    end: date | None = None,
    rolling_window: int = DEFAULT_FLOW_ROLLING_WINDOW,
    settings: TerminalSettings | None = None,
    abandoned: AbandonCheck | None = None,
) -> FlowsResponse:
    """Build ``GET /market/flows``: estimated foreign versus local net flows of a subject set.

    The subject is one ticker, one sector, or the whole market when neither is given. Defaults:
    ``as_of`` is the latest stored ``trade_date``, ``end`` is ``as_of`` and ``start`` is ``end``
    minus 365 calendar days; with no price row at all, ``as_of`` is today's date in
    ``settings.timezone`` (Africa/Nairobi when no settings are given). For each market session
    in ``[start, end]``: foreign net flow (KES) summed over the KES-listed constituents with
    flow data, local net flow = -foreign (the zero-sum identity), the running sum from
    ``start``, the sum over the last ``rolling_window`` sessions, and the foreign net flow as a
    percentage of the same constituents' turnover (KES). ``coverage_pct`` is sessions with flow
    data over sessions in the range (%). With no flow data, ``status`` is ``no_flow_data`` and
    ``totals`` is null.

    Only constituents listed in KES are summed, because flow and turnover are stored in each
    equity's listing currency and nothing is converted. A constituent listed in another currency
    is left out, never converted, and ``flags`` adds ``currency_mismatch``; a subject without a
    KES-listed constituent therefore has ``no_flow_data``, null totals, coverage 0.00 and a null
    flow on every session. Sessions remain the observed market calendar of every ticker.

    The series is computed in a worker thread after the reads. ``abandoned``, when given, is
    awaited before that starts.

    Errors, in check order: 422 ``conflicting_subjects`` (``field="sector"``) when both a ticker
    and a sector are given; 404 ``unknown_ticker`` or ``unknown_sector``; 422
    ``start_after_end`` (``field="start"``) when the resolved ``start`` is after ``end``.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away; nothing is
            computed.
    """
    if ticker is not None and sector is not None:
        raise _coded_error(
            _HTTP_422,
            "conflicting_subjects",
            "Give a ticker or a sector, not both; give neither for the whole market.",
            "sector",
        )
    equities: list[TerminalEquity]
    if ticker is not None:
        equity = await ensure_ticker(session, ticker)
        subject = equity.ticker
        equities = [equity]
    elif sector is not None:
        await ensure_sector(session, sector)
        subject = sector
        equities = await load_equities(session, sector=sector)
    else:
        subject = MARKET_SUBJECT
        equities = await load_equities(session)
    admitted = [listed.ticker for listed in equities if listed.currency == ANALYTICS_CURRENCY]
    refused = len(admitted) < len(equities)
    if refused:
        logger.debug(
            "net flows of %s leave out %d equities not listed in %s",
            subject,
            len(equities) - len(admitted),
            ANALYTICS_CURRENCY,
        )

    resolved, _has_prices = await _resolve_as_of(session, as_of, settings)
    range_end = end if end is not None else resolved
    range_start = start if start is not None else _days_before(range_end, FLOW_DEFAULT_RANGE_DAYS)
    if range_start > range_end:
        raise _coded_error(
            _HTTP_422,
            "start_after_end",
            f"start ({range_start}) must not be after end ({range_end}).",
            "start",
        )
    sessions = await load_market_sessions(session, range_start, range_end)
    prices = await load_price_rows(session, admitted, range_start, range_end)
    result = await compute(
        abandoned,
        net_flow_series,
        [row for rows in prices.values() for row in rows],
        sessions,
        rolling_window=rolling_window,
    )

    flags = list(result.flags)
    if refused:
        flags.append("currency_mismatch")
    totals = result.totals
    return FlowsResponse(
        subject=subject,
        as_of=resolved,
        start=range_start,
        end=range_end,
        status=result.status,
        flags=flags,
        coverage_pct=quantize_out(result.coverage_pct, "pct"),
        totals=(
            None
            if totals is None
            else FlowTotals(
                foreign_net_kes=quantize_out(totals.foreign_net_kes, "money"),
                local_net_kes=quantize_out(totals.local_net_kes, "money"),
                foreign_net_pct_of_turnover=quantize_out(totals.foreign_net_pct_of_turnover, "pct"),
                sessions_with_data=totals.sessions_with_data,
                sessions_in_range=totals.sessions_in_range,
            )
        ),
        series=[
            FlowPoint(
                trade_date=point.trade_date,
                foreign_net_kes=quantize_out(point.foreign_net_kes, "money"),
                # Rounded from its own raw value: ROUND_HALF_UP is symmetric about zero, so it
                # equals the negated foreign amount exactly.
                local_net_kes=quantize_out(point.local_net_kes, "money"),
                cumulative_foreign_net_kes=quantize_out(point.cumulative_foreign_net_kes, "money"),
                rolling_foreign_net_kes=quantize_out(point.rolling_foreign_net_kes, "money"),
                turnover_kes=quantize_out(point.turnover_kes, "money"),
                foreign_net_pct_of_turnover=quantize_out(point.foreign_net_pct_of_turnover, "pct"),
            )
            for point in result.series
        ],
    )
