"""The five ``/market/*`` endpoints: screener, sector heatmap, valuation bands, anomalies, flows.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint. Only the screener reads the principal, to scope its ``watchlist_id``
filter to the caller's own watchlists; nothing here looks up users.

The routes bind and bound single query parameters only. ``market_service`` checks every rule
that relates two parameters and every unknown entity, raising the coded body
``{"detail": {"code", "message", "field"}}`` (``min_exceeds_max``, ``exactly_one_subject``,
``conflicting_subjects``, ``start_after_end``, ``unknown_ticker``, ``unknown_sector``,
``watchlist_not_found``); a single-parameter type or range failure is FastAPI's standard 422 list.
The service also rounds every value exactly once, and the responses serialize each ``Decimal`` as
a JSON string. Decimal parameters are bound as ``Decimal``, never ``float``. The service is called
through its module so that tests can replace it.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of every route signature.
"""

from datetime import date
from decimal import Decimal
from typing import Annotated, Any, Final

from fastapi import APIRouter, Query
from pydantic import Field

from opennse_terminal.api.deps import PrincipalDep, SessionDep, SettingsDep
from opennse_terminal.schemas import (
    DEFAULT_THRESHOLD,
    DEFAULT_Z_WINDOW,
    THRESHOLD_MAX,
    THRESHOLD_MIN,
    Z_WINDOW_MAX,
    Z_WINDOW_MIN,
    AnomaliesResponse,
    AnomalyMetric,
    BandMetric,
    ErrorResponse,
    FlowsResponse,
    HeatmapInterval,
    RequestDecimal,
    ScreenerParams,
    ScreenerResponse,
    SectorHeatmapResponse,
    ValidationErrorResponse,
    ValuationBandsResponse,
)
from opennse_terminal.services import market_service

__all__ = [
    "DEFAULT_ANOMALY_THRESHOLD",
    "DEFAULT_HEATMAP_PERIODS",
    "DEFAULT_LOOKBACK_SESSIONS",
    "DEFAULT_LOOKBACK_YEARS",
    "DEFAULT_ROLLING_WINDOW",
    "DEFAULT_Z_WINDOW",
    "MAX_HEATMAP_PERIODS",
    "MAX_LOOKBACK_SESSIONS",
    "MAX_LOOKBACK_YEARS",
    "MAX_THRESHOLD",
    "MAX_Z_WINDOW",
    "MIN_THRESHOLD",
    "MIN_Z_WINDOW",
    "SECTOR_MAX_LENGTH",
    "TICKER_MAX_LENGTH",
    "AsOfQuery",
    "SectorQuery",
    "TickerQuery",
    "get_anomalies",
    "get_flows",
    "get_screener",
    "get_sector_heatmap",
    "get_valuation_bands",
    "router",
]

# The Z-score window and spike-threshold bounds are the ones ScreenerParams declares, so the
# screener and the anomaly feed can never accept different ranges.
DEFAULT_ANOMALY_THRESHOLD: Final[Decimal] = DEFAULT_THRESHOLD
"""Default spike threshold of the anomaly feed, in standard deviations (2.0)."""

MIN_THRESHOLD: Final[Decimal] = THRESHOLD_MIN
"""Smallest accepted spike threshold, in standard deviations (0.5)."""

MAX_THRESHOLD: Final[Decimal] = THRESHOLD_MAX
"""Largest accepted spike threshold, in standard deviations (10)."""

MIN_Z_WINDOW: Final[int] = Z_WINDOW_MIN
"""Smallest accepted rolling window N, for the Z-score and the flow rolling sum (5)."""

MAX_Z_WINDOW: Final[int] = Z_WINDOW_MAX
"""Largest accepted rolling window N, for the Z-score and the flow rolling sum (250)."""

DEFAULT_LOOKBACK_SESSIONS: Final = 20
"""Market sessions, ending at the latest stored trade date, that the anomaly feed scans."""

MAX_LOOKBACK_SESSIONS: Final = 250
"""Largest accepted ``lookback_sessions``."""

DEFAULT_HEATMAP_PERIODS: Final = 12
"""Heatmap periods ending at ``as_of`` when the request omits ``periods``."""

MAX_HEATMAP_PERIODS: Final = 36
"""Largest accepted ``periods``."""

DEFAULT_LOOKBACK_YEARS: Final = 5
"""Valuation-band lookback, in calendar years, when the request omits ``lookback_years``."""

MAX_LOOKBACK_YEARS: Final = 15
"""Largest accepted ``lookback_years``."""

DEFAULT_ROLLING_WINDOW: Final = 20
"""Market sessions summed by the flow ``rolling_foreign_net_kes`` series by default."""

TICKER_MAX_LENGTH: Final = 16
"""Longest ``ticker`` parameter, matching ``terminal_equities.ticker`` (``VARCHAR(16)``)."""

SECTOR_MAX_LENGTH: Final = 100
"""Longest ``sector`` parameter, matching ``terminal_equities.sector`` (``VARCHAR(100)``)."""

_CODED_OR_VALIDATION_422: Final = (
    "Coded business-rule error, a detail object with code, message and field ({codes}), or "
    "FastAPI's standard validation list for a single-parameter type or range failure"
)

_SCREENER_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    404: {"model": ErrorResponse, "description": "unknown_sector or watchlist_not_found"},
    422: {
        "model": ErrorResponse | ValidationErrorResponse,
        "description": _CODED_OR_VALIDATION_422.format(
            codes="min_exceeds_max, with field set to the min_* parameter"
        ),
    },
}

_BANDS_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    404: {"model": ErrorResponse, "description": "unknown_ticker or unknown_sector"},
    422: {
        "model": ErrorResponse | ValidationErrorResponse,
        "description": _CODED_OR_VALIDATION_422.format(codes="exactly_one_subject"),
    },
}

_ANOMALIES_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    404: {"model": ErrorResponse, "description": "unknown_ticker or unknown_sector"},
}

_FLOWS_RESPONSES: Final[dict[int | str, dict[str, Any]]] = {
    404: {"model": ErrorResponse, "description": "unknown_ticker or unknown_sector"},
    422: {
        "model": ErrorResponse | ValidationErrorResponse,
        "description": _CODED_OR_VALIDATION_422.format(
            codes="conflicting_subjects or start_after_end"
        ),
    },
}

# PostgreSQL text cannot hold a NUL character, so a ticker or sector carrying one is refused as a
# single-field 422 here rather than failing as a 500 when the service queries it.
_NO_NUL_PATTERN: Final = r"^[^\x00]*$"

TickerQuery = Annotated[
    str | None,
    Query(
        min_length=1,
        max_length=TICKER_MAX_LENGTH,
        pattern=_NO_NUL_PATTERN,
        description="NSE ticker symbol, for example SCOM; an unknown ticker is a 404.",
    ),
]
SectorQuery = Annotated[
    str | None,
    Query(
        min_length=1,
        max_length=SECTOR_MAX_LENGTH,
        pattern=_NO_NUL_PATTERN,
        description="Sector name as stored, for example Banking; an unknown sector is a 404.",
    ),
]
AsOfQuery = Annotated[
    date | None,
    Query(description="Valuation date (YYYY-MM-DD); defaults to the latest stored trade_date."),
]

router = APIRouter()


@router.get(
    "/market/screener",
    response_model=ScreenerResponse,
    summary="Equity screener with valuation multiples, Z-score anomalies and sector aggregates",
    responses=_SCREENER_RESPONSES,
)
async def get_screener(
    session: SessionDep,
    settings: SettingsDep,
    principal: PrincipalDep,
    params: Annotated[ScreenerParams, Query()],
) -> ScreenerResponse:
    """Value every equity as of ``as_of`` (default the latest stored trade date), filter and sort.

    Each row carries the latest close (KES per share) and its change (%), market cap (KES),
    EPS (KES per share), P/E, P/B and EV/EBITDA (x), dividend yield (percentage points), volume
    (shares), turnover (KES), and the rolling ``z_window`` (default 20) volume and turnover
    Z-scores. A row is an anomaly when a Z-score is at or above ``anomaly_threshold`` (default
    2.0, positive side only). Banking equities have a null EV/EBITDA with
    ``ev_not_meaningful_for_banks``. A null metric fails every filter on it; rows sort by ``sort``
    (default ``market_cap_kes``) in ``order`` (default ``desc``). ``sector_aggregates`` holds one
    entry per sector, independent of the filters.

    ``watchlist_id`` resolves only among the caller principal's own watchlists. Every parameter
    and bound is declared by ``ScreenerParams``; an unknown parameter is a standard 422.
    """
    return await market_service.get_screener(session, params, principal, settings)


@router.get(
    "/market/sector-heatmap",
    response_model=SectorHeatmapResponse,
    summary="Capitalization-weighted sector rotation returns per period",
)
async def get_sector_heatmap(
    session: SessionDep,
    settings: SettingsDep,
    interval: Annotated[
        HeatmapInterval,
        Query(
            description=(
                "Period boundaries: the last market session on or before each month-end "
                "(month) or each Friday (week)."
            ),
        ),
    ] = "month",
    periods: Annotated[
        int,
        Query(ge=1, le=MAX_HEATMAP_PERIODS, description="Number of periods ending at as_of."),
    ] = DEFAULT_HEATMAP_PERIODS,
    as_of: AsOfQuery = None,
) -> SectorHeatmapResponse:
    """Return sector and market returns over ``periods`` periods (default 12, range 1-36).

    Each cell is the capitalization-weighted return of the period in percentage points, over the
    constituents with a close within 10 calendar days on or before both boundaries;
    ``relative_return_pct`` is the sector return minus the market return and is null on the
    market row. A sector without a qualifying constituent has a null cell. The endpoint defines
    no coded error: with no price data every list is empty.
    """
    # settings is passed so that the no-data fallback as_of uses the configured (or host
    # supplied) timezone rather than the service's Africa/Nairobi default.
    return await market_service.get_sector_heatmap(
        session, interval=interval, periods=periods, as_of=as_of, settings=settings
    )


@router.get(
    "/market/valuation-bands",
    response_model=ValuationBandsResponse,
    summary="Historical P/E or P/B valuation bands of one counter or one sector",
    responses=_BANDS_RESPONSES,
)
async def get_valuation_bands(
    session: SessionDep,
    settings: SettingsDep,
    ticker: TickerQuery = None,
    sector: SectorQuery = None,
    metric: Annotated[BandMetric, Query(description="The banded multiple: pe or pb.")] = "pe",
    lookback_years: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LOOKBACK_YEARS,
            description="Lookback in calendar years, ending at as_of.",
        ),
    ] = DEFAULT_LOOKBACK_YEARS,
    as_of: AsOfQuery = None,
) -> ValuationBandsResponse:
    """Return the daily ``metric`` (default ``pe``) bands over ``lookback_years`` (default 5).

    Give exactly one of ``ticker`` and ``sector``. The multiple (x) is computed on each market
    session with a close, from that day's close and the fundamentals available by then: for a
    sector, each session on which at least one member traded, a member that did not trade that
    day carrying its close of up to 10 calendar days before. Null days are excluded. ``stats``
    (mean, sample deviation, mean +/- 1 and 2 deviations, quartiles, min, max and current, in x;
    ``current_percentile`` in percent) is present only when ``status`` is ``ok``, which needs at
    least 60 daily values. ``series`` samples the last session of each ISO week, and
    ``coverage`` reports whether the data starts more than 7 days after the requested lookback
    start.
    """
    return await market_service.get_valuation_bands(
        session,
        settings,
        ticker=ticker,
        sector=sector,
        metric=metric,
        lookback_years=lookback_years,
        as_of=as_of,
    )


@router.get(
    "/market/anomalies",
    response_model=AnomaliesResponse,
    summary="Recent positive volume and turnover Z-score spikes",
    responses=_ANOMALIES_RESPONSES,
)
async def get_anomalies(
    session: SessionDep,
    metric: Annotated[
        AnomalyMetric,
        Query(description="Metric to scan: volume, turnover or both."),
    ] = "both",
    window: Annotated[
        int,
        Query(
            ge=MIN_Z_WINDOW,
            le=MAX_Z_WINDOW,
            description="Rolling Z-score window N, in the ticker's own prior rows.",
        ),
    ] = DEFAULT_Z_WINDOW,
    # Bounded through pydantic Field, not Query(ge=..., le=...): Query types its bounds as
    # float, and the threshold stays a Decimal end to end.
    threshold: Annotated[
        RequestDecimal,
        Field(ge=MIN_THRESHOLD, le=MAX_THRESHOLD),
        Query(description="Spike threshold in standard deviations: z >= threshold is a spike."),
    ] = DEFAULT_ANOMALY_THRESHOLD,
    lookback_sessions: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LOOKBACK_SESSIONS,
            description="Market sessions scanned, ending at the latest stored trade_date.",
        ),
    ] = DEFAULT_LOOKBACK_SESSIONS,
    ticker: TickerQuery = None,
    sector: SectorQuery = None,
) -> AnomaliesResponse:
    """Return the spikes of the last ``lookback_sessions`` (default 20) market sessions.

    Each row is scored against up to ``window`` (default 20) of the ticker's own rows from the
    ``2 x window`` market sessions before it; sessions without a row are skipped, never
    zero-filled. A row is a spike when its Z-score is at or above ``threshold`` (default 2.0);
    negative Z-scores never are. ``value``, ``rolling_mean`` and ``rolling_stdev`` are in shares
    for volume and KES for turnover, and ``z_score`` in standard deviations. Items sort by
    ``trade_date`` descending, then ``z_score`` descending. Candidates are every equity, or
    ``ticker``, or the equities of ``sector``.
    """
    return await market_service.get_anomalies(
        session,
        metric=metric,
        window=window,
        threshold=threshold,
        lookback_sessions=lookback_sessions,
        ticker=ticker,
        sector=sector,
    )


@router.get(
    "/market/flows",
    response_model=FlowsResponse,
    summary="Estimated foreign versus local net capital flows",
    responses=_FLOWS_RESPONSES,
)
async def get_flows(
    session: SessionDep,
    settings: SettingsDep,
    ticker: TickerQuery = None,
    sector: SectorQuery = None,
    as_of: AsOfQuery = None,
    start: Annotated[
        date | None,
        Query(description="First date of the range (YYYY-MM-DD); defaults to end - 365 days."),
    ] = None,
    end: Annotated[
        date | None,
        Query(description="Last date of the range (YYYY-MM-DD); defaults to as_of."),
    ] = None,
    rolling_window: Annotated[
        int,
        Query(
            ge=MIN_Z_WINDOW,
            le=MAX_Z_WINDOW,
            description="Market sessions summed by rolling_foreign_net_kes.",
        ),
    ] = DEFAULT_ROLLING_WINDOW,
) -> FlowsResponse:
    """Return the daily foreign and local net flows of a ticker, a sector or the whole market.

    Give at most one of ``ticker`` and ``sector``; neither means the whole market. Foreign net
    flow (KES) sums the constituents with flow data on each market session, and local net flow
    is its negative (aggregate buys equal aggregate sells). The series adds the running sum from
    ``start``, the sum over the last ``rolling_window`` (default 20) sessions, and the foreign
    net flow as a percentage of the same constituents' turnover. ``coverage_pct`` is the share
    of sessions with flow data, in percent; with none, ``status`` is ``no_flow_data`` and
    ``totals`` is null.
    """
    # settings is passed so that the no-data fallback as_of uses the configured (or host
    # supplied) timezone rather than the service's Africa/Nairobi default.
    return await market_service.get_flows(
        session,
        ticker=ticker,
        sector=sector,
        as_of=as_of,
        start=start,
        end=end,
        rolling_window=rolling_window,
        settings=settings,
    )
