"""Request and response contract of ``GET /cycles/backtest``, the event and election backtester.

Each cycle maps NSE price, turnover, valuation and foreign-flow behaviour over a window around an
event date (T) onto one event-relative axis, ``offset_days = trade_date - T`` in calendar days,
so different cycles overlay on one chart. Index levels are base 100 at the cycle's anchor, the
first market session in the window.

Index levels, P/E multiples, percentages, KES flows and macro overlays are ``Decimal`` values,
serialized as JSON strings already quantized to 2 dp by the cycle service. Offsets, counts and
day numbers are ``int``.

:class:`BacktestParams` declares the route's query parameters with their single-field bounds,
using the limits below, and forbids unknown ones. The request rules that span several parameters
(``no_event_dates``, ``conflicting_window_units`` and ``window_out_of_range``, all 422) are coded
errors raised by the service, never validators here.
"""

import datetime as dt
from decimal import Decimal
from typing import Final, Literal

from pydantic import Field

from opennse_terminal.schemas.common import (
    OptionalRequestInt,
    SectorName,
    TerminalRequestModel,
    TerminalResponseModel,
    Ticker,
)

__all__ = [
    "DEFAULT_WINDOW_DAYS",
    "MAX_BACKTEST_TICKERS",
    "MAX_EVENT_DATES",
    "MAX_WINDOW_DAYS",
    "MAX_WINDOW_MONTHS",
    "BacktestParams",
    "BacktestResponse",
    "BacktestStatus",
    "BacktestWindow",
    "CycleMarker",
    "CycleMarkers",
    "CyclePoint",
    "CycleProjection",
    "CycleProjectionPoint",
    "CycleResult",
    "CycleSummary",
    "MarkerMetric",
]

# ---------------------------------------------------------------------------------------------
# Literals and request limits
# ---------------------------------------------------------------------------------------------

BacktestStatus = Literal["complete", "partial", "awaiting_data", "no_constituents"]
"""Data state of one cycle: fully observed, partly observed, no in-window session, or no cohort."""

MarkerMetric = Literal["pe_ratio", "price_index", "turnover_index"]
"""The :class:`CyclePoint` field a :class:`CycleMarker` was selected on."""

MAX_EVENT_DATES: Final[int] = 8
"""Most ``event_dates`` a backtest request may repeat."""

MAX_BACKTEST_TICKERS: Final[int] = 100
"""Most ``tickers`` a backtest request may repeat."""

DEFAULT_WINDOW_DAYS: Final[int] = 180
"""Default ``pre_days`` and ``post_days``: the "T-180 to T+180" window."""

MAX_WINDOW_DAYS: Final[int] = 365
"""Largest accepted ``pre_days`` or ``post_days``; the smallest is 1."""

MAX_WINDOW_MONTHS: Final[int] = 12
"""Largest accepted ``pre_months`` or ``post_months``; the smallest is 1."""


# ---------------------------------------------------------------------------------------------
# Query-parameter model
# ---------------------------------------------------------------------------------------------


class BacktestParams(TerminalRequestModel):
    """Query parameters of ``GET /cycles/backtest``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[BacktestParams,
    Query()]``, so each field appears as an individual query parameter in OpenAPI, ``event_dates``
    and ``tickers`` are repeatable (``?tickers=SCOM&tickers=KCB``), and an unknown parameter is
    FastAPI's standard 422 ``extra_forbidden``. A list bound limits the number of repeats; each
    ticker is bounded by its element type, and a ticker or sector carrying a NUL character is a
    standard 422 rather than a database error.

    The four window fields default to None, never 180: the service applies the 180-day default
    only when no window parameter is given, and a day default here would make every months request
    look like both forms were supplied. Giving a day and a month field together is the service's
    coded 422 ``conflicting_window_units``, not a validation error here.
    """

    event_dates: list[dt.date] | None = Field(
        default=None,
        max_length=MAX_EVENT_DATES,
        description="Event dates T (repeatable). Defaults to every terminal_cycles row.",
    )
    pre_days: OptionalRequestInt = Field(
        default=None,
        ge=1,
        le=MAX_WINDOW_DAYS,
        description=(
            "Calendar days before each event date. Default 180 days each side when no window "
            "parameter is given; cannot be combined with pre_months or post_months."
        ),
    )
    post_days: OptionalRequestInt = Field(
        default=None,
        ge=1,
        le=MAX_WINDOW_DAYS,
        description=(
            "Calendar days after each event date. Default 180 days each side when no window "
            "parameter is given; cannot be combined with pre_months or post_months."
        ),
    )
    pre_months: OptionalRequestInt = Field(
        default=None,
        ge=1,
        le=MAX_WINDOW_MONTHS,
        description=(
            "Calendar months before each event date, the day clamped to the target month's last "
            "day (12 when only post_months is given). Without any window parameter the window is "
            "180 days each side; cannot be combined with pre_days or post_days."
        ),
    )
    post_months: OptionalRequestInt = Field(
        default=None,
        ge=1,
        le=MAX_WINDOW_MONTHS,
        description=(
            "Calendar months after each event date, the day clamped to the target month's last "
            "day (12 when only pre_months is given). Without any window parameter the window is "
            "180 days each side; cannot be combined with pre_days or post_days."
        ),
    )
    tickers: list[Ticker] | None = Field(
        default=None,
        max_length=MAX_BACKTEST_TICKERS,
        description=(
            "Candidate tickers (repeatable). Defaults to every equity, or to the equities of "
            "sector; an unknown ticker is a 404."
        ),
    )
    sector: SectorName | None = Field(
        default=None,
        description="Candidate sector as stored, for example Banking; unknown is a 404.",
    )


# ---------------------------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------------------------


class BacktestWindow(TerminalResponseModel):
    """The resolved window, echoed back in the form that produced it.

    The days form (the default, 180 days each side) leaves both month fields null; the months
    form (``pre_months``/``post_months``) leaves both day fields null.
    """

    pre_days: int | None = Field(
        description="Calendar days before the event; null when the window is given in months.",
    )
    post_days: int | None = Field(
        description="Calendar days after the event; null when the window is given in months.",
    )
    pre_months: int | None = Field(
        description=(
            "Calendar months before the event, the day clamped to the target month's last day; "
            "null when the window is given in days."
        ),
    )
    post_months: int | None = Field(
        description=(
            "Calendar months after the event, the day clamped to the target month's last day; "
            "null when the window is given in days."
        ),
    )


class CyclePoint(TerminalResponseModel):
    """One market session of a cycle on the event-relative axis.

    Every member of the cycle's cohort contributes to every point: after the anchor a member's
    missing close is carried forward from its own last actual close.
    """

    offset_days: int = Field(
        description="trade_date minus event_date, in calendar days (negative before the event).",
    )
    trade_date: dt.date = Field(description="Market session date (YYYY-MM-DD).")
    price_index: Decimal | None = Field(
        description=(
            "Cohort capitalization, base 100 at the anchor; null when the anchor "
            "capitalization is zero."
        ),
    )
    turnover_index: Decimal | None = Field(
        description=(
            "Trailing 20-session mean cohort turnover, base 100 at the anchor; null when the "
            "anchor mean is undefined or zero."
        ),
    )
    pe_ratio: Decimal | None = Field(
        description=(
            "Cohort P/E multiple over the members with available earnings on the day (a matched "
            "set); null when that set is empty or its net income is not positive."
        ),
    )
    pe_index: Decimal | None = Field(
        description=(
            "pe_ratio, base 100 at the anchor; null when the anchor P/E is unavailable or not "
            "positive."
        ),
    )
    cumulative_foreign_net_kes: Decimal | None = Field(
        description=(
            "Running sum of the cohort's foreign net flow from the anchor, in KES; days without "
            "flow data add nothing, and it is null until the first day with flow data."
        ),
    )
    inflation_yoy_pct: Decimal | None = Field(
        description=(
            "12-month inflation in percentage points as of the session; null when the latest "
            "observation is more than 62 days old."
        ),
    )
    tbill_91d_pct: Decimal | None = Field(
        description=(
            "91-day Treasury bill yield in percentage points as of the session; null when the "
            "latest observation is more than 62 days old."
        ),
    )
    tbill_364d_pct: Decimal | None = Field(
        description=(
            "364-day Treasury bill yield in percentage points as of the session; null when the "
            "latest observation is more than 62 days old."
        ),
    )


class CycleMarker(TerminalResponseModel):
    """A point singled out on the cycle chart, with the metric and value that selected it."""

    offset_days: int = Field(description="Calendar days from the event to the marked session.")
    trade_date: dt.date = Field(description="The marked market session (YYYY-MM-DD).")
    metric: MarkerMetric = Field(
        description="The point field whose minimum selected this marker.",
    )
    value: Decimal = Field(description="That field's value at the marked session.")


class CycleMarkers(TerminalResponseModel):
    """The valuation floor and volume exhaustion points of one cycle."""

    valuation_floor: CycleMarker | None = Field(
        description=(
            "The point with the minimum pe_ratio, falling back to the minimum price_index when "
            "P/E is null throughout; the earliest point wins a tie. Null when both are null "
            "throughout."
        ),
    )
    volume_exhaustion: CycleMarker | None = Field(
        description=(
            "The point with the minimum turnover_index, the earliest winning a tie; null when "
            "turnover_index is null throughout."
        ),
    )


class CycleSummary(TerminalResponseModel):
    """Cycle returns in percentage points; "index at T" is the last point on or before T."""

    pre_event_return_pct: Decimal | None = Field(
        description=(
            "(price index at T / 100 - 1) x 100; null when no point lies on or before T or the "
            "price index is null."
        ),
    )
    post_event_return_pct: Decimal | None = Field(
        description=(
            "(last price index / price index at T - 1) x 100; null when no point lies after T "
            "or none lies on or before it."
        ),
    )
    max_drawdown_pct: Decimal | None = Field(
        description=(
            "Minimum over the points of (price index / its running maximum - 1) x 100; null "
            "when the price index is null throughout."
        ),
    )
    pe_compression_pct: Decimal | None = Field(
        description=(
            "(P/E at the valuation floor / anchor P/E - 1) x 100; null when the floor came from "
            "the price-index fallback or the anchor P/E is unavailable."
        ),
    )


class CycleProjectionPoint(TerminalResponseModel):
    """The mean normalized levels of the complete cycles at one calendar offset."""

    offset_days: int = Field(description="Calendar days from the projected cycle's event.")
    price_index: Decimal | None = Field(
        description="Mean price index of the basis cycles at this offset; null when undefined.",
    )
    turnover_index: Decimal | None = Field(
        description="Mean turnover index of the basis cycles at this offset; null when undefined.",
    )
    pe_index: Decimal | None = Field(
        description="Mean P/E index of the basis cycles at this offset; null when undefined.",
    )


class CycleProjection(TerminalResponseModel):
    """A labelled projection for a partial or awaiting-data cycle.

    It is the descriptive mean of the request's complete cycles' normalized paths at each
    calendar offset of the target window, each complete cycle contributing its last value at or
    before the offset where its own window covers it. It is not a forecast and is never stored.
    """

    is_projection: Literal[True] = Field(
        default=True,
        description="Always true, so a consumer never mistakes the projection for observed data.",
    )
    basis: list[str] = Field(description="Labels of the complete cycles averaged.")
    points: list[CycleProjectionPoint] = Field(
        description="One point per calendar offset of the target window with a defined mean.",
    )


class CycleResult(TerminalResponseModel):
    """One cycle of a backtest.

    When ``status`` is ``awaiting_data`` or ``no_constituents``, ``points`` is empty and
    ``markers`` and ``summary`` are null. Otherwise both objects are present and their members
    follow the null-and-flag rules: a ``partial`` cycle with no point after T, for example, has a
    ``summary`` whose ``post_event_return_pct`` is null.
    """

    label: str = Field(
        description='Cycle label from terminal_cycles, or "Event YYYY-MM-DD" for an ad hoc date.',
    )
    event_date: dt.date = Field(description="The event day, T (YYYY-MM-DD).")
    window_start: dt.date = Field(description="First calendar day of the window.")
    window_end: dt.date = Field(description="Last calendar day of the window.")
    status: BacktestStatus = Field(
        description=(
            "complete when data reaches window_end; partial when data exists in the window but "
            "ends before window_end; awaiting_data when the window holds no observed data; "
            "no_constituents when no candidate has an actual close within 10 market sessions "
            "on or before the anchor."
        ),
    )
    anchor_date: dt.date | None = Field(
        description=(
            "First market session in the window; null when the window holds no session. Each "
            "defined normalized index (price_index, turnover_index, pe_index) is 100 at the "
            "anchor. An index that cannot be normalized (zero anchor capitalization, "
            "insufficient or zero anchor turnover, unavailable anchor P/E) stays null "
            "throughout, with the matching code in flags."
        ),
    )
    days_until_window_start: int | None = Field(
        description=(
            "Calendar days from data_available_through to window_start, set for an "
            "awaiting_data cycle whose data ends before the window opens; when no candidate has "
            "data (data_available_through is null), counted from the market's latest session of "
            "any ticker instead. Null otherwise."
        ),
    )
    constituents: list[str] = Field(
        description="Sorted tickers of the cohort, fixed at the anchor for the whole cycle.",
    )
    excluded_constituents: list[str] = Field(
        description=(
            "Sorted candidate tickers left out because they have no actual close within 10 "
            "market sessions on or before the anchor, or are listed in a currency other than KES "
            "(flag currency_mismatch)."
        ),
    )
    points: list[CyclePoint] = Field(
        description=(
            "Market sessions from the anchor to the last observed session in the window, in "
            "date order; empty for awaiting_data and no_constituents."
        ),
    )
    markers: CycleMarkers | None = Field(
        description=(
            "Valuation floor and volume exhaustion markers; null when status is awaiting_data "
            "or no_constituents, otherwise present with each marker null when undefined."
        ),
    )
    summary: CycleSummary | None = Field(
        description=(
            "Pre-event, post-event, drawdown and P/E compression figures; null when status is "
            "awaiting_data or no_constituents, otherwise present with each member null when "
            "undefined."
        ),
    )
    projection: CycleProjection | None = Field(
        description=(
            "For partial and awaiting_data cycles, the mean path of the complete cycles; null "
            "for complete and no_constituents cycles, and null with the flag "
            "insufficient_projection_basis when fewer than 2 cycles are complete."
        ),
    )
    flags: list[str] = Field(
        default_factory=list,
        description=(
            "Reason codes for null or qualified values, for example currency_mismatch, "
            "empty_cohort, zero_anchor_capitalization, insufficient_turnover_history, "
            "zero_anchor_turnover, pe_anchor_unavailable, no_pre_event_data or "
            "insufficient_projection_basis."
        ),
    )


class BacktestResponse(TerminalResponseModel):
    """Body of ``GET /cycles/backtest``.

    ``cycles`` holds one result per distinct event date, ascending by ``event_date``: a date
    requested more than once is returned once, and the stored ``terminal_cycles`` defaults are
    likewise in date order.
    """

    window: BacktestWindow = Field(description="The resolved window applied to every cycle.")
    candidates: list[str] = Field(
        description=(
            "Tickers considered for every cohort: the requested tickers, the requested sector's "
            "equities, or every equity."
        ),
    )
    data_available_through: dt.date | None = Field(
        description=(
            "Latest trade_date among the candidates' prices; null when none of them has prices."
        ),
    )
    cycles: list[CycleResult] = Field(
        description=(
            "One result per distinct event date, ascending by event_date; a date requested more "
            "than once appears once."
        ),
    )
