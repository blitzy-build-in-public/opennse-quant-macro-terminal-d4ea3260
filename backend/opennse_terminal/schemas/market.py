"""Request and response contract of the ``/market`` endpoints.

It covers ``GET /market/screener``, ``/market/sector-heatmap``, ``/market/valuation-bands``,
``/market/anomalies`` and ``/market/flows``. Like every schema module it imports only the standard
library, Pydantic and :mod:`opennse_terminal.schemas.common`.

Money, prices, ratios, percentages, Z-scores and band statistics are ``Decimal`` values, sent as
JSON strings that the services have already rounded once with ``quant.numeric.quantize_out``
(2 dp for money, per-share KES, ratios, percentages and Z-scores). Volumes, share counts and
observation counts are JSON integers. An undefined metric is ``null`` with its reason code in the
enclosing ``flags`` list, never zero.

Every rule that relates two parameters is checked by the service and raised as a coded 422 rather
than by a Pydantic validator: ``min_exceeds_max`` (``field`` is the ``min_*`` parameter),
``exactly_one_subject``, ``conflicting_subjects`` and ``start_after_end``.

The ``as_of`` field of a response is never null. Services echo the request's ``as_of``, else the
latest stored ``trade_date``, else (no price row exists at all) the current date in the configured
timezone (``TERMINAL_TIMEZONE``, Africa/Nairobi by default), and then return empty lists.
"""

import datetime as dt
import uuid
from decimal import Decimal
from typing import Final, Literal

from pydantic import Field

from opennse_terminal.schemas.common import (
    CoverageMeta,
    Pct,
    Ratio,
    RequestDecimal,
    SectorName,
    SeriesPoint,
    TerminalRequestModel,
    TerminalResponseModel,
    Ticker,
)

__all__ = [
    "DEFAULT_HEATMAP_PERIODS",
    "DEFAULT_LOOKBACK_SESSIONS",
    "DEFAULT_LOOKBACK_YEARS",
    "DEFAULT_ROLLING_WINDOW",
    "DEFAULT_THRESHOLD",
    "DEFAULT_Z_WINDOW",
    "HEATMAP_PERIODS_MAX",
    "LOOKBACK_SESSIONS_MAX",
    "LOOKBACK_YEARS_MAX",
    "THRESHOLD_MAX",
    "THRESHOLD_MIN",
    "Z_WINDOW_MAX",
    "Z_WINDOW_MIN",
    "AnomaliesParams",
    "AnomaliesResponse",
    "AnomalyItem",
    "AnomalyMetric",
    "BandMetric",
    "BandStats",
    "BandsStatus",
    "FlowPoint",
    "FlowTotals",
    "FlowsParams",
    "FlowsResponse",
    "FlowsStatus",
    "HeatmapCell",
    "HeatmapInterval",
    "ScreenerParams",
    "ScreenerResponse",
    "ScreenerRow",
    "ScreenerSort",
    "SectorAggregate",
    "SectorHeatmapParams",
    "SectorHeatmapResponse",
    "SectorHeatmapRow",
    "SortOrder",
    "SubjectType",
    "ValuationBandsParams",
    "ValuationBandsResponse",
]


# ---------------------------------------------------------------------------------------------
# Query literals and limits
# ---------------------------------------------------------------------------------------------

ScreenerSort = Literal[
    "ticker", "market_cap_kes", "pe", "pb", "ev_ebitda", "dividend_yield_pct", "volume_z"
]
"""Screener columns a caller may sort by."""

SortOrder = Literal["asc", "desc"]
"""Sort direction of the screener rows."""

HeatmapInterval = Literal["month", "week"]
"""Period boundaries of the sector heatmap: the last market session on or before each month-end
or each Friday."""

BandMetric = Literal["pe", "pb"]
"""The multiple a valuation band is drawn for: P/E or P/B."""

AnomalyMetric = Literal["volume", "turnover", "both"]
"""Metrics the anomaly feed scans; ``both`` scans volume and turnover."""

SubjectType = Literal["ticker", "sector"]
"""Whether a valuation band describes one counter or one sector aggregate."""

BandsStatus = Literal["ok", "insufficient_history", "no_positive_earnings", "no_positive_equity"]
"""Outcome of a valuation-band request; ``stats`` is present only for ``ok``."""

FlowsStatus = Literal["ok", "no_flow_data"]
"""Outcome of a net-flow request; ``totals`` is absent for ``no_flow_data``."""

Z_WINDOW_MIN: Final[int] = 5
"""Smallest rolling Z-score window, in the ticker's own rows."""

Z_WINDOW_MAX: Final[int] = 250
"""Largest rolling Z-score window, in the ticker's own rows."""

DEFAULT_Z_WINDOW: Final[int] = 20
"""Default rolling Z-score window: the 20-day volume Z-score."""

THRESHOLD_MIN: Final[Decimal] = Decimal("0.5")
"""Smallest spike threshold a caller may request, in standard deviations."""

THRESHOLD_MAX: Final[Decimal] = Decimal("10")
"""Largest spike threshold a caller may request, in standard deviations."""

DEFAULT_THRESHOLD: Final[Decimal] = Decimal("2.0")
"""Default spike threshold: a Z-score of 2.0 or more is an anomaly."""

HEATMAP_PERIODS_MAX: Final[int] = 36
"""Largest number of sector-heatmap periods a caller may request."""

DEFAULT_HEATMAP_PERIODS: Final[int] = 12
"""Sector-heatmap periods ending at ``as_of`` when the request omits ``periods``."""

LOOKBACK_YEARS_MAX: Final[int] = 15
"""Longest valuation-band lookback a caller may request, in calendar years."""

DEFAULT_LOOKBACK_YEARS: Final[int] = 5
"""Valuation-band lookback, in calendar years, when the request omits ``lookback_years``."""

LOOKBACK_SESSIONS_MAX: Final[int] = 250
"""Most market sessions the anomaly feed may be asked to scan."""

DEFAULT_LOOKBACK_SESSIONS: Final[int] = 20
"""Market sessions, ending at the latest stored trade date, that the anomaly feed scans by
default."""

DEFAULT_ROLLING_WINDOW: Final[int] = 20
"""Market sessions summed by the flow ``rolling_foreign_net_kes`` series by default."""


# ---------------------------------------------------------------------------------------------
# GET /market/screener
# ---------------------------------------------------------------------------------------------


class ScreenerParams(TerminalRequestModel):
    """Query parameters of ``GET /market/screener``, shared by the route and the service.

    The route declares it as a FastAPI query-parameter model, ``Annotated[ScreenerParams,
    Query()]`` (supported since FastAPI 0.115), so each field appears as an individual query
    parameter in OpenAPI and an unknown parameter is FastAPI's standard 422 ``extra_forbidden``.

    Filtering: a row whose metric is null fails every filter on that metric. ``anomalies_only``
    keeps rows where ``is_volume_anomaly`` or ``is_turnover_anomaly`` is true, which means an
    unrounded Z-score at or above ``anomaly_threshold`` on the positive side only. The metric
    filters, ``min_volume_z`` included, and ``sort`` compare the 2 dp values the rows show, so
    ``min_volume_z=2`` keeps a row whose ``volume_z`` shows 2.00 from a raw 1.996, which
    ``anomalies_only`` at threshold 2 drops. A ``min_*`` above its ``max_*`` is the service's
    coded 422 ``min_exceeds_max``, not a validation error here.
    """

    sector: SectorName | None = Field(
        default=None,
        description="Keep only equities in this sector, for example Banking; unknown is a 404.",
    )
    watchlist_id: uuid.UUID | None = Field(
        default=None,
        description="Keep only the tickers of this watchlist of the caller; unknown is a 404.",
    )
    min_pe: Ratio | None = Field(default=None, description="Minimum P/E ratio (x), inclusive.")
    max_pe: Ratio | None = Field(default=None, description="Maximum P/E ratio (x), inclusive.")
    min_pb: Ratio | None = Field(default=None, description="Minimum P/B ratio (x), inclusive.")
    max_pb: Ratio | None = Field(default=None, description="Maximum P/B ratio (x), inclusive.")
    min_ev_ebitda: Ratio | None = Field(
        default=None, description="Minimum EV/EBITDA multiple (x), inclusive."
    )
    max_ev_ebitda: Ratio | None = Field(
        default=None, description="Maximum EV/EBITDA multiple (x), inclusive."
    )
    min_dividend_yield: Pct | None = Field(
        default=None,
        description="Minimum dividend yield in percentage points (5.00 means 5%), inclusive.",
    )
    min_volume_z: RequestDecimal | None = Field(
        default=None,
        description=(
            "Minimum rolling volume Z-score (standard deviations), inclusive, compared with the "
            "2 dp volume_z shown; the anomaly flags compare the unrounded score instead."
        ),
    )
    anomalies_only: bool = Field(
        default=False,
        description=(
            "Keep only rows with is_volume_anomaly or is_turnover_anomaly: an unrounded Z-score "
            "at or above the threshold, positive side only."
        ),
    )
    anomaly_threshold: RequestDecimal = Field(
        default=DEFAULT_THRESHOLD,
        ge=THRESHOLD_MIN,
        le=THRESHOLD_MAX,
        description=(
            "Spike threshold in standard deviations: an unrounded Z-score >= threshold is an "
            "anomaly."
        ),
    )
    z_window: int = Field(
        default=DEFAULT_Z_WINDOW,
        ge=Z_WINDOW_MIN,
        le=Z_WINDOW_MAX,
        description="Rolling Z-score window N, in the ticker's own prior rows.",
    )
    as_of: dt.date | None = Field(
        default=None,
        description="Valuation date (YYYY-MM-DD); defaults to the latest stored trade_date.",
    )
    sort: ScreenerSort = Field(default="market_cap_kes", description="Column to sort rows by.")
    order: SortOrder = Field(default="desc", description="Sort direction of the rows.")


class ScreenerRow(TerminalResponseModel):
    """One equity in the screener, valued as of the response's ``as_of``."""

    ticker: str = Field(description="NSE ticker symbol, for example SCOM.")
    company_name: str = Field(description="Registered company name.")
    sector: str = Field(description="Sector name as stored, for example Banking.")
    currency: str = Field(
        description=(
            "ISO 4217 code of the equity's listing currency, KES by default; close_price and "
            "turnover_kes are in it, never converted. The computed metrics are KES-only and "
            "flag currency_mismatch otherwise: a non-KES equity has a null market_cap_kes, and "
            "a non-KES equity or non-KES fundamentals give null eps_kes, pe, pb, ev_ebitda, "
            "dividend_yield_pct and fundamentals_period_end."
        ),
    )
    trade_date: dt.date = Field(description="Date of the latest close on or before as_of.")
    close_price: Decimal = Field(
        description="Closing price per share on trade_date, in currency (KES by default).",
    )
    change_pct: Decimal | None = Field(
        description=(
            "Change from the previous close, in percent; null without a previous close, with "
            "change_pct:no_previous_close in flags."
        ),
    )
    market_cap_kes: Decimal | None = Field(
        description=(
            "Market capitalization (close x shares outstanding), in KES; null with "
            "currency_mismatch for an equity not listed in KES, which is never converted."
        ),
    )
    eps_kes: Decimal | None = Field(description="Earnings per share, in KES per share.")
    pe: Decimal | None = Field(description="Price-to-earnings ratio (x).")
    pb: Decimal | None = Field(description="Price-to-book ratio (x).")
    ev_ebitda: Decimal | None = Field(
        description="Enterprise value to EBITDA (x); null for every Banking-sector equity.",
    )
    dividend_yield_pct: Decimal | None = Field(
        description="Dividend per share over close, in percent.",
    )
    volume: int = Field(description="Shares traded on trade_date.")
    turnover_kes: Decimal = Field(
        description=(
            "Value traded on trade_date, in currency (KES by default): a non-KES equity's "
            "turnover is returned as stored, not converted to KES."
        ),
    )
    volume_z: Decimal | None = Field(
        description="Rolling volume Z-score on trade_date (standard deviations).",
    )
    turnover_z: Decimal | None = Field(
        description="Rolling turnover Z-score on trade_date (standard deviations).",
    )
    is_volume_anomaly: bool = Field(
        description=(
            "True when the unrounded volume Z-score is at or above the threshold (positive side "
            "only). Comparing the 2 dp volume_z instead can give a different answer: raw 1.996 "
            "shows 2.00 yet is not an anomaly at threshold 2."
        ),
    )
    is_turnover_anomaly: bool = Field(
        description=(
            "True when the unrounded turnover Z-score is at or above the threshold (positive "
            "side only); comparing the 2 dp turnover_z instead can give a different answer, as "
            "for is_volume_anomaly."
        ),
    )
    free_float_pct: Decimal | None = Field(
        description="Free float as a percentage of shares outstanding.",
    )
    fundamentals_period_end: dt.date | None = Field(
        description=(
            "Period end of the FY fundamentals used: the latest whose availability lag (90 days "
            "after period end by default) has passed by as_of; null when none qualifies."
        ),
    )
    flags: list[str] = Field(
        default_factory=list,
        description=(
            "Reason codes for null or qualified values: valuation codes such as "
            "ev_not_meaningful_for_banks, the session-change code prefixed change_pct:, as in "
            "change_pct:no_previous_close, and Z-score codes prefixed volume_z: or turnover_z:, "
            "for example volume_z:insufficient_observations."
        ),
    )


class SectorAggregate(TerminalResponseModel):
    """Point-in-time valuation of one sector over matched eligible sets.

    Computed over every equity in the sector as of the screener's ``as_of``, independent of the
    screener's filters. Each multiple sums its numerator and denominator over the same eligible
    constituents, whose count is reported beside ``constituent_count``.
    """

    sector: str = Field(description="Sector name as stored, for example Banking.")
    constituent_count: int = Field(description="Equities in the sector.")
    market_cap_kes: Decimal | None = Field(
        description=(
            "Market capitalization summed over the constituents listed in KES, in KES; null when "
            "there is none. Non-KES constituents are left out and flag currency_mismatch."
        ),
    )
    pe: Decimal | None = Field(
        description="Sector P/E (x): sum of market cap over sum of net income, NI-eligible set.",
    )
    pb: Decimal | None = Field(
        description="Sector P/B (x): sum of market cap over sum of equity, equity-eligible set.",
    )
    ev_ebitda: Decimal | None = Field(
        description="Sector EV/EBITDA (x) over non-Banking members with EV and EBITDA.",
    )
    dividend_yield_pct: Decimal | None = Field(
        description="Capitalization-weighted dividend yield of the dps-eligible set, in percent.",
    )
    pe_eligible_count: int = Field(description="Constituents with net income, used for pe.")
    pb_eligible_count: int = Field(description="Constituents with total equity, used for pb.")
    ev_ebitda_eligible_count: int = Field(
        description="Non-Banking constituents with EV and EBITDA, used for ev_ebitda.",
    )
    dy_eligible_count: int = Field(
        description="Constituents with a dividend per share, used for dividend_yield_pct.",
    )
    flags: list[str] = Field(
        default_factory=list,
        description=(
            "Reason codes for null values, for example ev_not_meaningful_for_banks or "
            "sector_pe_unavailable."
        ),
    )


class ScreenerResponse(TerminalResponseModel):
    """Body of ``GET /market/screener``."""

    as_of: dt.date = Field(
        description=(
            "Valuation date: the requested as_of, else the latest stored trade_date, else the "
            "current date in the configured timezone (TERMINAL_TIMEZONE, Africa/Nairobi by "
            "default) when no price exists (with empty lists)."
        ),
    )
    z_window: int = Field(description="Rolling Z-score window N used, in the ticker's own rows.")
    anomaly_threshold: Decimal = Field(
        description="Spike threshold used, in standard deviations.",
    )
    count: int = Field(description="Number of rows returned; equals len(rows).")
    rows: list[ScreenerRow] = Field(description="Equities passing every filter, sorted.")
    sector_aggregates: list[SectorAggregate] = Field(
        description="One entry per sector in the universe, independent of the filters.",
    )


# ---------------------------------------------------------------------------------------------
# GET /market/sector-heatmap and GET /market/valuation-bands
# ---------------------------------------------------------------------------------------------


class SectorHeatmapParams(TerminalRequestModel):
    """Query parameters of ``GET /market/sector-heatmap``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[SectorHeatmapParams,
    Query()]``, so each field appears as an individual query parameter in OpenAPI and an unknown
    parameter is FastAPI's standard 422 ``extra_forbidden``.
    """

    interval: HeatmapInterval = Field(
        default="month",
        description=(
            "Period boundaries: the last market session on or before each month-end (month) or "
            "each Friday (week), with as_of replacing a month-end or Friday after it. The latest "
            "period is thus clamped to as_of and can be month-to-date or week-to-date."
        ),
    )
    periods: int = Field(
        default=DEFAULT_HEATMAP_PERIODS,
        ge=1,
        le=HEATMAP_PERIODS_MAX,
        description="Number of periods ending at as_of.",
    )
    as_of: dt.date | None = Field(
        default=None,
        description=(
            "Cutoff of the latest period (YYYY-MM-DD); defaults to the latest stored trade_date."
        ),
    )


class ValuationBandsParams(TerminalRequestModel):
    """Query parameters of ``GET /market/valuation-bands``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[ValuationBandsParams,
    Query()]``, so each field appears as an individual query parameter in OpenAPI and an unknown
    parameter is FastAPI's standard 422 ``extra_forbidden``. Giving neither or both of ``ticker``
    and ``sector`` is the service's coded 422 ``exactly_one_subject``, not a validation error here.
    """

    ticker: Ticker | None = Field(
        default=None,
        description="NSE ticker symbol, for example SCOM; an unknown ticker is a 404.",
    )
    sector: SectorName | None = Field(
        default=None,
        description="Sector name as stored, for example Banking; an unknown sector is a 404.",
    )
    metric: BandMetric = Field(default="pe", description="The banded multiple: pe or pb.")
    lookback_years: int = Field(
        default=DEFAULT_LOOKBACK_YEARS,
        ge=1,
        le=LOOKBACK_YEARS_MAX,
        description="Lookback in calendar years, ending at as_of.",
    )
    as_of: dt.date | None = Field(
        default=None,
        description="Valuation date (YYYY-MM-DD); defaults to the latest stored trade_date.",
    )


class HeatmapCell(TerminalResponseModel):
    """Capitalization-weighted return of one sector, or of the market, over one period."""

    period_end: dt.date = Field(description="Market session closing the period.")
    return_pct: Decimal | None = Field(
        description=(
            "Period return in percent; null when no constituent qualifies or their starting "
            "capitalization is zero."
        ),
    )
    relative_return_pct: Decimal | None = Field(
        description=(
            "Sector return minus market return, in percentage points; null when either is "
            "undefined, and always on the market row."
        ),
    )
    constituents: int = Field(
        description="Constituents with a close within 10 days of both period boundaries.",
    )


class SectorHeatmapRow(TerminalResponseModel):
    """One sector's row of the heatmap, one cell per period."""

    sector: str = Field(description="Sector name as stored, for example Banking.")
    cells: list[HeatmapCell] = Field(description="One cell per entry of period_ends, in order.")


class SectorHeatmapResponse(TerminalResponseModel):
    """Body of ``GET /market/sector-heatmap``."""

    as_of: dt.date = Field(
        description=(
            "Cutoff of the latest period, echoed as resolved: the requested as_of, else the latest "
            "stored trade_date, else the current date in the configured timezone "
            "(TERMINAL_TIMEZONE, Africa/Nairobi by default) when no price exists (with empty "
            "lists). It is not itself a boundary: period_ends holds observed market sessions, so "
            "an as_of that is not one (a weekend, a holiday, a date past the data) differs from "
            "period_ends[-1]."
        ),
    )
    interval: HeatmapInterval = Field(description="Period length: month or week.")
    period_ends: list[dt.date] = Field(description="Closing session of each period, ascending.")
    market: list[HeatmapCell] = Field(description="Whole-market return per period.")
    sectors: list[SectorHeatmapRow] = Field(description="One row per sector.")


class BandStats(TerminalResponseModel):
    """Statistics of the daily multiple over the lookback.

    Ratios are in x at 2 dp; ``current_percentile`` is in percent at 2 dp. Quantiles use linear
    interpolation (inclusive method) and the standard deviation is the sample one (n - 1).
    """

    observations: int = Field(description="Non-null daily values the statistics use.")
    mean: Decimal = Field(description="Mean of the daily values (x).")
    stdev: Decimal = Field(description="Sample standard deviation of the daily values (x).")
    minus_2sd: Decimal = Field(description="Mean minus two standard deviations (x).")
    minus_1sd: Decimal = Field(description="Mean minus one standard deviation (x).")
    plus_1sd: Decimal = Field(description="Mean plus one standard deviation (x).")
    plus_2sd: Decimal = Field(description="Mean plus two standard deviations (x).")
    min: Decimal = Field(description="Lowest daily value (x).")
    p25: Decimal = Field(description="25th percentile of the daily values (x).")
    median: Decimal = Field(description="Median of the daily values (x).")
    p75: Decimal = Field(description="75th percentile of the daily values (x).")
    max: Decimal = Field(description="Highest daily value (x).")
    current: Decimal = Field(description="Latest daily value (x).")
    current_percentile: Decimal = Field(
        description="Share of daily values at or below current, in percent.",
    )


class ValuationBandsResponse(TerminalResponseModel):
    """Body of ``GET /market/valuation-bands`` for one ticker or one sector."""

    subject: str = Field(description="The ticker or sector name the bands describe.")
    subject_type: SubjectType = Field(description="Whether subject is a ticker or a sector.")
    metric: BandMetric = Field(description="The banded multiple: pe or pb.")
    as_of: dt.date = Field(
        description=(
            "End of the lookback: the requested as_of, else the latest stored trade_date, else "
            "the current date in the configured timezone (TERMINAL_TIMEZONE, Africa/Nairobi by "
            "default) when no price exists (with an empty series)."
        ),
    )
    coverage: CoverageMeta = Field(
        description="Requested lookback start against the first session that has a value.",
    )
    status: BandsStatus = Field(
        description=(
            "ok, or insufficient_history (under 60 observations, including a lookback with no "
            "session with a close), no_positive_earnings (pe) or no_positive_equity (pb) when "
            "the lookback has sessions and every day is null."
        ),
    )
    flags: list[str] = Field(
        default_factory=list, description="Reason codes for null or qualified values."
    )
    series: list[SeriesPoint] = Field(
        description=(
            "Weekly as-of transport sample of the multiple (x), ascending: one point per ISO week "
            "with a non-null daily value, dated on the week's last market session on or before "
            "as_of and carrying the week's latest non-null daily value, which may come from an "
            "earlier session than that date. Weeks without a value are omitted; the statistics "
            "use the daily values."
        ),
    )
    stats: BandStats | None = Field(
        description="Band statistics; null for every status other than ok.",
    )


# ---------------------------------------------------------------------------------------------
# GET /market/anomalies and GET /market/flows
# ---------------------------------------------------------------------------------------------


class AnomaliesParams(TerminalRequestModel):
    """Query parameters of ``GET /market/anomalies``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[AnomaliesParams,
    Query()]``, so each field appears as an individual query parameter in OpenAPI and an unknown
    parameter is FastAPI's standard 422 ``extra_forbidden``. The window and threshold bounds are
    the ones :class:`ScreenerParams` declares, so the screener and the anomaly feed can never
    accept different ranges.
    """

    metric: AnomalyMetric = Field(
        default="both", description="Metric to scan: volume, turnover or both."
    )
    window: int = Field(
        default=DEFAULT_Z_WINDOW,
        ge=Z_WINDOW_MIN,
        le=Z_WINDOW_MAX,
        description="Rolling Z-score window N, in the ticker's own prior rows.",
    )
    threshold: RequestDecimal = Field(
        default=DEFAULT_THRESHOLD,
        ge=THRESHOLD_MIN,
        le=THRESHOLD_MAX,
        description=(
            "Spike threshold in standard deviations: an unrounded Z-score >= threshold is a spike."
        ),
    )
    lookback_sessions: int = Field(
        default=DEFAULT_LOOKBACK_SESSIONS,
        ge=1,
        le=LOOKBACK_SESSIONS_MAX,
        description="Market sessions scanned, ending at the latest stored trade_date.",
    )
    ticker: Ticker | None = Field(
        default=None,
        description="NSE ticker symbol, for example SCOM; an unknown ticker is a 404.",
    )
    sector: SectorName | None = Field(
        default=None,
        description="Sector name as stored, for example Banking; an unknown sector is a 404.",
    )


class FlowsParams(TerminalRequestModel):
    """Query parameters of ``GET /market/flows``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[FlowsParams,
    Query()]``, so each field appears as an individual query parameter in OpenAPI and an unknown
    parameter is FastAPI's standard 422 ``extra_forbidden``. Giving both ``ticker`` and ``sector``
    is the service's coded 422 ``conflicting_subjects``, and a ``start`` after ``end`` its coded
    422 ``start_after_end``, not validation errors here.
    """

    ticker: Ticker | None = Field(
        default=None,
        description="NSE ticker symbol, for example SCOM; an unknown ticker is a 404.",
    )
    sector: SectorName | None = Field(
        default=None,
        description="Sector name as stored, for example Banking; an unknown sector is a 404.",
    )
    as_of: dt.date | None = Field(
        default=None,
        description="Valuation date (YYYY-MM-DD); defaults to the latest stored trade_date.",
    )
    start: dt.date | None = Field(
        default=None,
        description="First date of the range (YYYY-MM-DD); defaults to end - 365 days.",
    )
    end: dt.date | None = Field(
        default=None,
        description="Last date of the range (YYYY-MM-DD); defaults to as_of.",
    )
    rolling_window: int = Field(
        default=DEFAULT_ROLLING_WINDOW,
        ge=Z_WINDOW_MIN,
        le=Z_WINDOW_MAX,
        description="Market sessions summed by rolling_foreign_net_kes.",
    )


class AnomalyItem(TerminalResponseModel):
    """One positive volume or turnover spike: an unrounded Z-score at or above the threshold."""

    ticker: str = Field(description="NSE ticker symbol, for example SCOM.")
    trade_date: dt.date = Field(description="Session of the spike.")
    metric: Literal["volume", "turnover"] = Field(description="The metric that spiked.")
    value: Decimal = Field(
        description=(
            "The metric on trade_date: shares for volume, or for turnover the value traded in the "
            "equity's listing currency (KES by default); a non-KES equity's turnover is scored "
            "and returned as stored, not converted to KES."
        ),
    )
    rolling_mean: Decimal = Field(
        description=(
            "Mean of the prior window, in value's unit (shares, or the listing currency for "
            "turnover)."
        ),
    )
    rolling_stdev: Decimal = Field(
        description="Sample standard deviation of the prior window, in value's unit.",
    )
    z_score: Decimal = Field(
        description=(
            "Z-score of value against the window, in std devs, at 2 dp. Membership tests the "
            "unrounded score, so with a threshold of more than 2 dp the value shown can be "
            "below it."
        ),
    )
    observations_used: int = Field(
        description="Prior rows of the ticker in the window; missing sessions are skipped.",
    )


class AnomaliesResponse(TerminalResponseModel):
    """Body of ``GET /market/anomalies``."""

    window: int = Field(description="Rolling Z-score window N used, in the ticker's own rows.")
    threshold: Decimal = Field(description="Spike threshold used, in standard deviations.")
    items: list[AnomalyItem] = Field(
        description=(
            "Spikes whose unrounded Z-score is >= threshold, sorted by trade_date descending, "
            "then z_score descending."
        ),
    )


class FlowPoint(TerminalResponseModel):
    """Foreign and local net flow of the subject on one market session, in KES."""

    trade_date: dt.date = Field(description="Market session.")
    foreign_net_kes: Decimal | None = Field(
        description="Net foreign buying in KES; null when no constituent has flow data.",
    )
    local_net_kes: Decimal | None = Field(
        description="Estimated net local buying in KES: the negative of foreign_net_kes.",
    )
    cumulative_foreign_net_kes: Decimal = Field(
        description="Running sum of foreign_net_kes from the range start, in KES; null days add 0.",
    )
    rolling_foreign_net_kes: Decimal | None = Field(
        description=(
            "Sum of foreign_net_kes over the last N market sessions, in KES; null until a full "
            "window and when no session in it has flow data."
        ),
    )
    turnover_kes: Decimal | None = Field(
        description="Turnover of the constituents with flow data, in KES.",
    )
    foreign_net_pct_of_turnover: Decimal | None = Field(
        description="foreign_net_kes over turnover_kes, in percent; null when turnover_kes is 0.",
    )


class FlowTotals(TerminalResponseModel):
    """Net flow totals over the whole range."""

    foreign_net_kes: Decimal = Field(description="Total net foreign buying, in KES.")
    local_net_kes: Decimal = Field(description="Total estimated net local buying, in KES.")
    foreign_net_pct_of_turnover: Decimal | None = Field(
        description=(
            "foreign_net_kes over the summed turnover of the sessions with data, in percent; "
            "null when that turnover is 0."
        ),
    )
    sessions_with_data: int = Field(description="Market sessions in the range with flow data.")
    sessions_in_range: int = Field(description="Market sessions in the range.")


class FlowsResponse(TerminalResponseModel):
    """Body of ``GET /market/flows`` for one ticker, one sector or the whole market."""

    subject: str = Field(
        description="The ticker, the sector name, or market when neither is given.",
    )
    as_of: dt.date = Field(
        description=(
            "The requested as_of, else the latest stored trade_date, else the current date in "
            "the configured timezone (TERMINAL_TIMEZONE, Africa/Nairobi by default) when no "
            "price exists (with an empty series)."
        ),
    )
    start: dt.date = Field(description="First date of the range; defaults to end minus 365 days.")
    end: dt.date = Field(description="Last date of the range; defaults to as_of.")
    status: FlowsStatus = Field(
        description="ok, or no_flow_data when no session in the range has flow data.",
    )
    flags: list[str] = Field(
        default_factory=list, description="Reason codes for null or qualified values."
    )
    coverage_pct: Decimal = Field(
        description=(
            "Sessions with flow data over market sessions in range, in percent; 0.00 for "
            "no_flow_data."
        ),
    )
    totals: FlowTotals | None = Field(
        description="Range totals; null when status is no_flow_data.",
    )
    series: list[FlowPoint] = Field(description="One point per market session, ascending.")
