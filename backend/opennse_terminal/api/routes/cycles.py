"""The ``/cycles/backtest`` endpoint: the event and election cycle backtester.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint.

The route binds its query through one query-parameter model, ``Annotated[BacktestParams,
Query()]``, which declares every parameter with its single-field bounds and forbids unknown keys:
a single-parameter type, length or range failure, and an unknown parameter
(``extra_forbidden``), is FastAPI's standard 422 list. ``cycle_service`` checks every rule that
relates two parameters and every unknown entity, raising the coded body
``{"detail": {"code", "message", "field"}}`` (``conflicting_window_units``,
``window_out_of_range``, ``no_event_dates``, ``unknown_ticker``, ``unknown_sector``). The service
also rounds every value exactly once, and the response serializes each ``Decimal`` as a JSON
string. The service is called through its module so that tests can replace it.

The service backtests each cycle in a worker thread after its reads, so the event loop keeps
serving other requests. The route passes ``request.is_disconnected``, which the service checks
before each cycle; once the client has gone away the service raises ``RequestAbandonedError``
and the route answers 499 (client closed request), a response the server discards. ``Request``
is not a query parameter and does not appear in the OpenAPI document.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of every route signature.
"""

from typing import Annotated, Any, Final

from fastapi import APIRouter, Query, Request, Response

from opennse_terminal.api.deps import SessionDep, SettingsDep
from opennse_terminal.schemas import (
    BacktestParams,
    BacktestResponse,
    ErrorResponse,
    ValidationErrorResponse,
)
from opennse_terminal.services import cycle_service
from opennse_terminal.services.offload import CLIENT_CLOSED_REQUEST, RequestAbandonedError

__all__ = ["SECTOR_MAX_LENGTH", "TICKER_MAX_LENGTH", "get_cycle_backtest", "router"]

# The two length limits are the ones BacktestParams declares through the shared Ticker and
# SectorName types; they stay exported here for callers of this module.
TICKER_MAX_LENGTH: Final = 16
"""Longest element of ``tickers``, matching ``terminal_equities.ticker`` (``VARCHAR(16)``)."""

SECTOR_MAX_LENGTH: Final = 100
"""Longest ``sector`` parameter, matching ``terminal_equities.sector`` (``VARCHAR(100)``)."""

_BACKTEST_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    404: {"model": ErrorResponse, "description": "unknown_ticker or unknown_sector"},
    422: {
        "model": ErrorResponse | ValidationErrorResponse,
        "description": (
            "Coded rule error (no_event_dates, conflicting_window_units, window_out_of_range) "
            "or standard validation error"
        ),
    },
}

router = APIRouter()


@router.get(
    "/cycles/backtest",
    response_model=BacktestResponse,
    summary="Event and election cycle backtest with macro overlays and projections",
    responses=_BACKTEST_RESPONSES,
)
async def get_cycle_backtest(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    params: Annotated[BacktestParams, Query()],
) -> BacktestResponse | Response:
    """Map NSE price, turnover, P/E and foreign-flow paths around each event date T.

    The window is given either in calendar days (``pre_days``/``post_days``, 1-365, default 180
    each side when no window parameter is given) or in calendar months
    (``pre_months``/``post_months``, 1-12), never both. Month arithmetic clamps the day to the
    target month's last day, so 2024-02-29 plus 12 months is 2025-02-28. Every point carries
    ``offset_days = trade_date - T`` so cycles overlay on one axis; ``price_index``,
    ``turnover_index`` and ``pe_index`` are base 100 at the anchor, the first market session in
    the window, and the inflation and Treasury bill overlays are aligned as of each session.

    Each cycle's ``status`` is ``complete`` (data reaches the window end), ``partial`` (data ends
    inside the window), ``awaiting_data`` (no observed data in the window) or
    ``no_constituents`` (no candidate has a base close). ``days_until_window_start`` is set only
    when the data ends before the window opens, and is null otherwise. Partial and awaiting-data
    cycles carry a ``projection`` only when at least two of the request's cycles are complete:
    a labelled descriptive average of those completed cycles' normalized paths, with the cycles
    used in ``basis``. It is not a forecast and is never stored. With fewer complete cycles,
    ``projection`` is null and the cycle carries the flag ``insufficient_projection_basis``.

    Every parameter and bound is declared by ``BacktestParams``; ``event_dates`` and ``tickers``
    are repeatable, and an unknown parameter, alone or beside declared ones, is a standard 422
    with one ``extra_forbidden`` entry per unknown name, and the service does not run.
    """
    try:
        return await cycle_service.get_backtest(
            session,
            settings,
            event_dates=params.event_dates,
            pre_days=params.pre_days,
            post_days=params.post_days,
            pre_months=params.pre_months,
            post_months=params.post_months,
            tickers=params.tickers,
            sector=params.sector,
            abandoned=request.is_disconnected,
        )
    except RequestAbandonedError:
        return Response(status_code=CLIENT_CLOSED_REQUEST)
