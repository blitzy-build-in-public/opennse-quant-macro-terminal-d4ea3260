"""The five ``/market/*`` endpoints: screener, sector heatmap, valuation bands, anomalies, flows.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint. Only the screener reads the principal, to scope its ``watchlist_id``
filter to the caller's own watchlists; nothing here looks up users.

Each route binds its query through one query-parameter model, ``Annotated[<Model>, Query()]``
(``ScreenerParams``, ``SectorHeatmapParams``, ``ValuationBandsParams``, ``AnomaliesParams``,
``FlowsParams``), which declares every parameter with its single-field bounds and forbids unknown
keys: a single-parameter type or range failure, and an unknown parameter (``extra_forbidden``),
is FastAPI's standard 422 list. ``market_service`` checks every rule that relates two parameters
and every unknown entity, raising the coded body ``{"detail": {"code", "message", "field"}}``
(``min_exceeds_max``, ``exactly_one_subject``, ``conflicting_subjects``, ``start_after_end``,
``unknown_ticker``, ``unknown_sector``, ``watchlist_not_found``). The service also rounds every
value exactly once, and the responses serialize each ``Decimal`` as a JSON string. Decimal
parameters are bound as ``Decimal``, never ``float``. The service is called through its module so
that tests can replace it.

The screener, valuation bands, anomalies and flows compute in a worker thread after their reads,
so the event loop keeps serving other requests. Each of these routes passes
``request.is_disconnected`` to its service, which checks it before each chunk of computation;
once the client has gone away the service raises ``RequestAbandonedError`` and the route answers 499
(client closed request), a response the server discards. ``Request`` is not a query parameter
and does not appear in the OpenAPI document.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of every route signature.
"""

from decimal import Decimal
from typing import Annotated, Any, Final

from fastapi import APIRouter, Query, Request, Response

from opennse_terminal.api.deps import PrincipalDep, SessionDep, SettingsDep
from opennse_terminal.schemas import (
    DEFAULT_HEATMAP_PERIODS,
    DEFAULT_LOOKBACK_SESSIONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_ROLLING_WINDOW,
    DEFAULT_THRESHOLD,
    DEFAULT_Z_WINDOW,
    HEATMAP_PERIODS_MAX,
    LOOKBACK_SESSIONS_MAX,
    LOOKBACK_YEARS_MAX,
    THRESHOLD_MAX,
    THRESHOLD_MIN,
    Z_WINDOW_MAX,
    Z_WINDOW_MIN,
    AnomaliesParams,
    AnomaliesResponse,
    ErrorResponse,
    FlowsParams,
    FlowsResponse,
    ScreenerParams,
    ScreenerResponse,
    SectorHeatmapParams,
    SectorHeatmapResponse,
    ValidationErrorResponse,
    ValuationBandsParams,
    ValuationBandsResponse,
)
from opennse_terminal.services import market_service
from opennse_terminal.services.offload import CLIENT_CLOSED_REQUEST, RequestAbandonedError

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
    "get_anomalies",
    "get_flows",
    "get_screener",
    "get_sector_heatmap",
    "get_valuation_bands",
    "router",
]

# Every default and bound below is the one the query-parameter models declare: the defaults
# DEFAULT_HEATMAP_PERIODS, DEFAULT_LOOKBACK_YEARS, DEFAULT_LOOKBACK_SESSIONS,
# DEFAULT_ROLLING_WINDOW and DEFAULT_Z_WINDOW are re-exported from the schemas as they are, and
# the names below alias the schema constants. The Z-score window and spike-threshold bounds are
# shared by ScreenerParams and AnomaliesParams, so the screener and the anomaly feed can never
# accept different ranges.
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

MAX_LOOKBACK_SESSIONS: Final[int] = LOOKBACK_SESSIONS_MAX
"""Largest accepted ``lookback_sessions`` (250)."""

MAX_HEATMAP_PERIODS: Final[int] = HEATMAP_PERIODS_MAX
"""Largest accepted ``periods`` (36)."""

MAX_LOOKBACK_YEARS: Final[int] = LOOKBACK_YEARS_MAX
"""Largest accepted ``lookback_years`` (15)."""

TICKER_MAX_LENGTH: Final = 16
"""Longest ``ticker`` parameter, matching ``terminal_equities.ticker`` (``VARCHAR(16)``)."""

SECTOR_MAX_LENGTH: Final = 100
"""Longest ``sector`` parameter, matching ``terminal_equities.sector`` (``VARCHAR(100)``)."""

_CODED_OR_VALIDATION_422: Final = (
    "Coded business-rule error, a detail object with code, message and field ({codes}), or "
    "FastAPI's standard validation list for a single-parameter type or range failure or an "
    "unknown parameter"
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

router = APIRouter()


@router.get(
    "/market/screener",
    response_model=ScreenerResponse,
    summary="Equity screener with valuation multiples, Z-score anomalies and sector aggregates",
    responses=_SCREENER_RESPONSES,
)
async def get_screener(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    principal: PrincipalDep,
    params: Annotated[ScreenerParams, Query()],
) -> ScreenerResponse | Response:
    """Value every equity as of ``as_of`` (default the latest stored trade date), filter and sort.

    Each row carries the latest close (per share) and that session's turnover in the equity's
    ``currency`` (KES by default), never converted, with the close's change (%), the volume
    (shares) and the rolling ``z_window`` (default 20) volume and turnover Z-scores. Market cap
    (KES), EPS (KES per share), P/E, P/B and EV/EBITDA (x) and dividend yield (percentage
    points) are computed in KES only: a non-KES equity has them all null with
    ``currency_mismatch``, and a KES equity whose fundamentals are not in KES has all but the
    market cap null with it. A row is an anomaly, and ``anomalies_only`` keeps it, when its
    unrounded Z-score is at or above ``anomaly_threshold`` (default 2.0, positive side only).
    Banking equities have a null EV/EBITDA with ``ev_not_meaningful_for_banks``. A null metric
    fails every filter on it; rows sort by ``sort`` (default ``market_cap_kes``) in ``order``
    (default ``desc``). The metric filters, ``min_volume_z`` included, and ``sort`` compare the
    2 dp values shown: a raw volume Z-score of 1.996 shows ``volume_z`` 2.00 and passes
    ``min_volume_z=2`` but is not an anomaly at threshold 2. ``sector_aggregates`` holds one
    entry per sector, independent of the filters.

    ``watchlist_id`` resolves only among the caller principal's own watchlists. Every parameter
    and bound is declared by ``ScreenerParams``; an unknown parameter is a standard 422.
    """
    try:
        return await market_service.get_screener(
            session, params, principal, settings, abandoned=request.is_disconnected
        )
    except RequestAbandonedError:
        return Response(status_code=CLIENT_CLOSED_REQUEST)


@router.get(
    "/market/sector-heatmap",
    response_model=SectorHeatmapResponse,
    summary="Capitalization-weighted sector rotation returns per period",
)
async def get_sector_heatmap(
    session: SessionDep,
    settings: SettingsDep,
    params: Annotated[SectorHeatmapParams, Query()],
) -> SectorHeatmapResponse:
    """Return sector and market returns over ``periods`` periods (default 12, range 1-36).

    Each cell is the capitalization-weighted return of the period in percentage points, over the
    constituents with a close within 10 calendar days on or before both boundaries;
    ``relative_return_pct`` is the sector return minus the market return and is null on the
    market row. A sector without a qualifying constituent has a null cell. The endpoint defines
    no coded error: with no price data every list is empty. Every parameter and bound is declared
    by ``SectorHeatmapParams``; an unknown parameter is a standard 422.
    """
    # settings is passed so that the no-data fallback as_of uses the configured (or host
    # supplied) timezone rather than the service's Africa/Nairobi default.
    return await market_service.get_sector_heatmap(
        session,
        interval=params.interval,
        periods=params.periods,
        as_of=params.as_of,
        settings=settings,
    )


@router.get(
    "/market/valuation-bands",
    response_model=ValuationBandsResponse,
    summary="Historical P/E or P/B valuation bands of one counter or one sector",
    responses=_BANDS_RESPONSES,
)
async def get_valuation_bands(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    params: Annotated[ValuationBandsParams, Query()],
) -> ValuationBandsResponse | Response:
    """Return the daily ``metric`` (default ``pe``) bands over ``lookback_years`` (default 5).

    Give exactly one of ``ticker`` and ``sector``. The multiple (x) is computed on each market
    session with a close, from that day's close and the fundamentals available by then: for a
    sector, each session on which at least one member traded, a member that did not trade that
    day carrying its close of up to 10 calendar days before. Null days are excluded. ``stats``
    (mean, sample deviation, mean +/- 1 and 2 deviations, quartiles, min, max and current, in x;
    ``current_percentile`` in percent) is present only when ``status`` is ``ok``, which needs at
    least 60 daily values. ``series`` is a weekly as-of transport sample: one point per ISO week
    with a non-null daily value, dated on the week's last market session on or before ``as_of``
    and carrying the week's latest non-null daily value, which may come from an earlier session
    than that date; weeks without a value are omitted, and ``stats`` uses the daily values.
    ``coverage`` reports whether the data starts more than 7 days after the requested lookback
    start. Every parameter and bound is declared by ``ValuationBandsParams``; an unknown
    parameter is a standard 422.
    """
    try:
        return await market_service.get_valuation_bands(
            session,
            settings,
            ticker=params.ticker,
            sector=params.sector,
            metric=params.metric,
            lookback_years=params.lookback_years,
            as_of=params.as_of,
            abandoned=request.is_disconnected,
        )
    except RequestAbandonedError:
        return Response(status_code=CLIENT_CLOSED_REQUEST)


@router.get(
    "/market/anomalies",
    response_model=AnomaliesResponse,
    summary="Recent positive volume and turnover Z-score spikes",
    responses=_ANOMALIES_RESPONSES,
)
async def get_anomalies(
    request: Request,
    session: SessionDep,
    params: Annotated[AnomaliesParams, Query()],
) -> AnomaliesResponse | Response:
    """Return the spikes of the last ``lookback_sessions`` (default 20) market sessions.

    Each row is scored against up to ``window`` (default 20) of the ticker's own rows from the
    ``2 x window`` market sessions before it; sessions without a row are skipped, never
    zero-filled. A row is a spike when its unrounded Z-score is at or above ``threshold``
    (default 2.0); negative Z-scores never are. ``value``, ``rolling_mean`` and ``rolling_stdev``
    are in shares for volume and in KES for turnover; ``z_score`` is in standard deviations,
    rounded to 2 dp after the spike test. Items sort by ``trade_date`` descending, then
    ``z_score`` descending. Candidates are every equity, or ``ticker``, or the equities of
    ``sector``: volume is scored for each of them, turnover only for those listed in KES, so a
    non-KES equity's turnover is never scored, returned or converted. Every parameter and bound
    is declared by ``AnomaliesParams``, the threshold as a ``Decimal``; an unknown parameter is
    a standard 422.
    """
    try:
        return await market_service.get_anomalies(
            session,
            metric=params.metric,
            window=params.window,
            threshold=params.threshold,
            lookback_sessions=params.lookback_sessions,
            ticker=params.ticker,
            sector=params.sector,
            abandoned=request.is_disconnected,
        )
    except RequestAbandonedError:
        return Response(status_code=CLIENT_CLOSED_REQUEST)


@router.get(
    "/market/flows",
    response_model=FlowsResponse,
    summary="Estimated foreign versus local net capital flows",
    responses=_FLOWS_RESPONSES,
)
async def get_flows(
    request: Request,
    session: SessionDep,
    settings: SettingsDep,
    params: Annotated[FlowsParams, Query()],
) -> FlowsResponse | Response:
    """Return the daily foreign and local net flows of a ticker, a sector or the whole market.

    Give at most one of ``ticker`` and ``sector``; neither means the whole market. Foreign net
    flow (KES) sums the KES-listed constituents with flow data on each market session, and local
    net flow is its negative (aggregate buys equal aggregate sells). The series adds the running
    sum from ``start``, the sum over the last ``rolling_window`` (default 20) sessions, and the
    foreign net flow as a percentage of the same constituents' turnover. ``coverage_pct`` is the
    share of sessions with flow data, in percent; with none, ``status`` is ``no_flow_data`` and
    ``totals`` is null. A constituent listed in another currency is left out, never converted,
    and ``flags`` adds ``currency_mismatch``, so a subject without a KES-listed constituent has
    ``no_flow_data``. Every parameter and bound is declared by ``FlowsParams``; an unknown
    parameter is a standard 422.
    """
    # settings is passed so that the no-data fallback as_of uses the configured (or host
    # supplied) timezone rather than the service's Africa/Nairobi default.
    try:
        return await market_service.get_flows(
            session,
            ticker=params.ticker,
            sector=params.sector,
            as_of=params.as_of,
            start=params.start,
            end=params.end,
            rolling_window=params.rolling_window,
            settings=settings,
            abandoned=request.is_disconnected,
        )
    except RequestAbandonedError:
        return Response(status_code=CLIENT_CLOSED_REQUEST)
