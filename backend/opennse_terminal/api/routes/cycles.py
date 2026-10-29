"""The ``/cycles/backtest`` endpoint: the event and election cycle backtester.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint.

The route binds and bounds single query parameters only. ``cycle_service`` checks every rule
that relates two parameters and every unknown entity, raising the coded body
``{"detail": {"code", "message", "field"}}`` (``conflicting_window_units``,
``window_out_of_range``, ``no_event_dates``, ``unknown_ticker``, ``unknown_sector``); a
single-parameter type, length or range failure is FastAPI's standard 422 list. The service also
rounds every value exactly once, and the response serializes each ``Decimal`` as a JSON string.
The service is called through its module so that tests can replace it.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of every route signature.
"""

from datetime import date
from typing import Annotated, Any, Final

from fastapi import APIRouter, Query
from pydantic import StringConstraints

from opennse_terminal.api.deps import SessionDep, SettingsDep
from opennse_terminal.schemas import (
    MAX_BACKTEST_TICKERS,
    MAX_EVENT_DATES,
    MAX_WINDOW_DAYS,
    MAX_WINDOW_MONTHS,
    BacktestResponse,
    ErrorResponse,
    ValidationErrorResponse,
)
from opennse_terminal.services import cycle_service

__all__ = ["SECTOR_MAX_LENGTH", "TICKER_MAX_LENGTH", "get_cycle_backtest", "router"]

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

# PostgreSQL text cannot hold a NUL character, so a ticker or sector carrying one is refused as a
# single-field 422 here rather than failing as a 500 when the service queries it.
_NO_NUL_PATTERN: Final = r"^[^\x00]*$"

# Query(max_length=...) on a list bounds the number of repeats, not each value, so every ticker
# is bounded by its element type.
_TickerItem = Annotated[
    str,
    StringConstraints(min_length=1, max_length=TICKER_MAX_LENGTH, pattern=_NO_NUL_PATTERN),
]

# The four window parameters default to None, never 180: the service applies the 180-day default
# only when no window parameter is given, and a day default here would make every months request
# look like both forms were supplied (conflicting_window_units).
_PreDaysQuery = Annotated[
    int | None,
    Query(
        ge=1,
        le=MAX_WINDOW_DAYS,
        description=(
            "Calendar days before each event date. Default 180 days each side when no window "
            "parameter is given; cannot be combined with pre_months or post_months."
        ),
    ),
]
_PostDaysQuery = Annotated[
    int | None,
    Query(
        ge=1,
        le=MAX_WINDOW_DAYS,
        description=(
            "Calendar days after each event date. Default 180 days each side when no window "
            "parameter is given; cannot be combined with pre_months or post_months."
        ),
    ),
]
_PreMonthsQuery = Annotated[
    int | None,
    Query(
        ge=1,
        le=MAX_WINDOW_MONTHS,
        description=(
            "Calendar months before each event date, the day clamped to the target month's last "
            "day (12 when only post_months is given). Without any window parameter the window is "
            "180 days each side; cannot be combined with pre_days or post_days."
        ),
    ),
]
_PostMonthsQuery = Annotated[
    int | None,
    Query(
        ge=1,
        le=MAX_WINDOW_MONTHS,
        description=(
            "Calendar months after each event date, the day clamped to the target month's last "
            "day (12 when only pre_months is given). Without any window parameter the window is "
            "180 days each side; cannot be combined with pre_days or post_days."
        ),
    ),
]

router = APIRouter()


@router.get(
    "/cycles/backtest",
    response_model=BacktestResponse,
    summary="Event and election cycle backtest with macro overlays and projections",
    responses=_BACKTEST_RESPONSES,
)
async def get_cycle_backtest(
    session: SessionDep,
    settings: SettingsDep,
    event_dates: Annotated[
        list[date] | None,
        Query(
            max_length=MAX_EVENT_DATES,
            description="Event dates T (repeatable). Defaults to every terminal_cycles row.",
        ),
    ] = None,
    pre_days: _PreDaysQuery = None,
    post_days: _PostDaysQuery = None,
    pre_months: _PreMonthsQuery = None,
    post_months: _PostMonthsQuery = None,
    tickers: Annotated[
        list[_TickerItem] | None,
        Query(
            max_length=MAX_BACKTEST_TICKERS,
            description=(
                "Candidate tickers (repeatable). Defaults to every equity, or to the equities of "
                "sector; an unknown ticker is a 404."
            ),
        ),
    ] = None,
    sector: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=SECTOR_MAX_LENGTH,
            pattern=_NO_NUL_PATTERN,
            description="Candidate sector as stored, for example Banking; unknown is a 404.",
        ),
    ] = None,
) -> BacktestResponse:
    """Map NSE price, turnover, P/E and foreign-flow paths around each event date T.

    The window is given either in calendar days (``pre_days``/``post_days``, 1-365, default 180
    each side when no window parameter is given) or in calendar months
    (``pre_months``/``post_months``, 1-12), never both. Month arithmetic clamps the day to the
    target month's last day, so 2024-02-29 plus 12 months is 2025-02-28. Every point carries
    ``offset_days = trade_date - T`` so cycles overlay on one axis; ``price_index``,
    ``turnover_index`` and ``pe_index`` are base 100 at the anchor, the first market session in
    the window, and the inflation and Treasury bill overlays are aligned as of each session.

    Each cycle's ``status`` is ``complete`` (data reaches the window end), ``partial`` (data ends
    inside the window), ``awaiting_data`` (no data in the window yet, with
    ``days_until_window_start``) or ``no_constituents`` (no candidate has a base close). Partial
    and awaiting-data cycles carry a ``projection``: a labelled descriptive average of the
    request's completed cycles' normalized paths, with the cycles used in ``basis``. It is not
    a forecast and is never stored.
    """
    return await cycle_service.get_backtest(
        session,
        settings,
        event_dates=event_dates,
        pre_days=pre_days,
        post_days=post_days,
        pre_months=pre_months,
        post_months=post_months,
        tickers=tickers,
        sector=sector,
    )
