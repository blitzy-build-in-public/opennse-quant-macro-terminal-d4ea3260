"""Request and response contract of ``GET /macro/overview``: CBK indicators, rates and the curve.

Mirrored one-to-one by ``MacroOverviewResponse`` in ``src/components/terminal/types.ts``, so field
names are the JSON keys. Every rate, FX, reserve and tenor value is a ``Decimal`` that the service
has already quantized (2 dp) and that Pydantic serializes as a JSON string. Units follow the
``terminal_macro_indicators`` columns: ``*_pct`` in percentage points (``6.80`` means 6.80%),
``fx_kes_usd`` in KES per USD and ``fx_reserves_usd_m`` in millions of USD.
"""

import datetime as dt
from decimal import Decimal
from typing import Final, Literal

from pydantic import Field

from opennse_terminal.schemas.common import (
    CoverageMeta,
    SeriesPoint,
    TerminalRequestModel,
    TerminalResponseModel,
)

__all__ = [
    "DEFAULT_HISTORY_MONTHS",
    "HISTORY_MONTHS_MAX",
    "IndicatorKey",
    "IndicatorSnapshot",
    "IndicatorUnit",
    "Instrument",
    "MacroOverviewParams",
    "MacroOverviewResponse",
    "RatesPoint",
    "TenorLabel",
    "YieldCurve",
    "YieldCurvePoint",
    "YieldCurveSnapshot",
]


# ---------------------------------------------------------------------------------------------
# Literals and limits
# ---------------------------------------------------------------------------------------------

IndicatorKey = Literal[
    "inflation_yoy_pct",
    "cbr_rate_pct",
    "interbank_rate_pct",
    "tbill_91d_pct",
    "tbill_182d_pct",
    "tbill_364d_pct",
    "fx_kes_usd",
    "fx_reserves_usd_m",
]
"""The eight indicator columns of ``terminal_macro_indicators``, one snapshot each."""

IndicatorUnit = Literal["pct", "kes_per_usd", "usd_m"]
"""Unit of an indicator: ``pct`` for the six rates, ``kes_per_usd`` for ``fx_kes_usd`` and
``usd_m`` for ``fx_reserves_usd_m``."""

TenorLabel = Literal["91D", "182D", "364D", "2Y", "5Y", "10Y", "15Y", "20Y", "25Y"]
"""Yield-curve tenor: the T-bills at 0.25, 0.50 and 1.00 years, then the FXD bond buckets of
2, 5, 10, 15, 20 and 25 years."""

Instrument = Literal["tbill", "bond"]
"""Instrument behind a yield-curve point: a CBK Treasury bill or an FXD Treasury bond."""

HISTORY_MONTHS_MAX: Final[int] = 240
"""Largest accepted ``history_months``: twenty years of month-ends."""

DEFAULT_HISTORY_MONTHS: Final[int] = 24
"""Month-ends in the history window when the request omits ``history_months``."""


# ---------------------------------------------------------------------------------------------
# Endpoint request
# ---------------------------------------------------------------------------------------------


class MacroOverviewParams(TerminalRequestModel):
    """Query parameters of ``GET /macro/overview``.

    The route declares it as a FastAPI query-parameter model, ``Annotated[MacroOverviewParams,
    Query()]``, so its field appears as an individual query parameter in OpenAPI and an unknown
    parameter is FastAPI's standard 422 ``extra_forbidden``.
    """

    history_months: int = Field(
        default=DEFAULT_HISTORY_MONTHS,
        ge=1,
        le=HISTORY_MONTHS_MAX,
        description=(
            "Number of month-ends in the history window, ending at the latest observation month."
        ),
    )


# ---------------------------------------------------------------------------------------------
# Indicator snapshots and rate history
# ---------------------------------------------------------------------------------------------


class IndicatorSnapshot(TerminalResponseModel):
    """Latest value of one indicator column with its previous value, for a KPI tile.

    Each column is resolved on its own: ``value`` is its latest non-null observation and
    ``previous_value`` the non-null observation before it, so a newer row that leaves this
    column empty never hides it.
    """

    key: IndicatorKey = Field(description="Indicator column name in terminal_macro_indicators.")
    label: str = Field(description="Human-readable label, for example 12-month inflation.")
    value: Decimal | None = Field(
        description="Latest non-null value in the indicator's unit; null when none is stored.",
    )
    unit: IndicatorUnit = Field(
        description="Unit of value: pct (percentage points), kes_per_usd or usd_m.",
    )
    as_of: dt.date | None = Field(
        description="Observation date of value; null when value is null.",
    )
    previous_value: Decimal | None = Field(
        description="The previous non-null value of the same column; null when there is none.",
    )
    change: Decimal | None = Field(
        description="value minus previous_value, in the same unit; null when either is null.",
    )


class RatesPoint(TerminalResponseModel):
    """One stored row of the policy, interbank and T-bill rate history, in percentage points."""

    observation_date: dt.date = Field(
        description=(
            "Date (YYYY-MM-DD) the observation is stored under, returned unchanged with no "
            "monthly resampling. CBK monthly rates and the seeded history are dated at the "
            "month-end of their reference month; a CSV or Excel import keeps each row's own "
            "date, which can fall mid-month."
        ),
    )
    cbr_rate_pct: Decimal | None = Field(
        description="Central Bank Rate (%); null when not published for the date.",
    )
    interbank_rate_pct: Decimal | None = Field(
        description="Interbank rate (%); null when not published for the date.",
    )
    tbill_91d_pct: Decimal | None = Field(
        description="91-day Treasury bill yield (%); null when not published for the date.",
    )
    tbill_182d_pct: Decimal | None = Field(
        description="182-day Treasury bill yield (%); null when not published for the date.",
    )
    tbill_364d_pct: Decimal | None = Field(
        description="364-day Treasury bill yield (%); null when not published for the date.",
    )


# ---------------------------------------------------------------------------------------------
# Yield curve
# ---------------------------------------------------------------------------------------------


class YieldCurvePoint(TerminalResponseModel):
    """One tenor of the yield curve at a date.

    Tenors are resolved independently, so each point carries its own observation date and
    provenance: a T-bill tenor takes the latest non-null value of its column within 62 days, and
    a bond bucket takes the most recent real FXD observation in the preceding 365 days, falling
    back to a seeded one only when the bucket has no real observation.
    """

    tenor_label: TenorLabel = Field(description="Tenor label: 91D, 182D, 364D, or 2Y to 25Y.")
    tenor_years: Decimal = Field(
        description=(
            "Tenor in years (2 dp): 0.25, 0.50 or 1.00 for a T-bill, the chosen bond's "
            "remaining tenor for a bond bucket."
        ),
    )
    yield_pct: Decimal = Field(description="Yield in percentage points (2 dp).")
    instrument: Instrument = Field(description="tbill for a Treasury bill, bond for an FXD bond.")
    bond_code: str | None = Field(
        description=(
            "CBK issue number of the bond, for example FXD1/2018/025, or FXD-SEED-... for a "
            "seeded observation; null for a T-bill."
        ),
    )
    observation_date: dt.date = Field(
        description="Date of the observation this point uses, which can precede the curve date.",
    )
    source: str = Field(
        description=(
            "Provenance of the observation, for example seed, cbk_rates_csv or cbk_bonds; a "
            "T-bill point carries its column's source_by_field entry."
        ),
    )


class YieldCurveSnapshot(TerminalResponseModel):
    """The yield curve as it stood at one month-end."""

    as_of: dt.date = Field(description="Month-end date the curve is resolved at.")
    points: list[YieldCurvePoint] = Field(
        description="Curve points in ascending tenor; tenors without an observation are omitted.",
    )


class YieldCurve(TerminalResponseModel):
    """The current yield curve and its month-end history for comparison overlays."""

    current: list[YieldCurvePoint] = Field(
        description="Curve at the latest observation date, in ascending tenor; empty without data.",
    )
    history: list[YieldCurveSnapshot] = Field(
        description="One snapshot per month-end in the history window, oldest first.",
    )


# ---------------------------------------------------------------------------------------------
# Endpoint response
# ---------------------------------------------------------------------------------------------


class MacroOverviewResponse(TerminalResponseModel):
    """Body of ``GET /macro/overview``.

    Every list is empty, never an error, when no macro or bond data is stored. The window holds
    exactly ``history_months`` month-ends ending at the latest observation month, fewer only when
    it would reach back before January of year 1 (it then opens at 0001-01-31), and
    ``coverage.requested_start`` is the first of them: 2024-10-31 for 24 months with data through
    2026-09-30. With no data the window ends at the current month.
    """

    coverage: CoverageMeta = Field(
        description=(
            "Window coverage; is_partial_coverage is true when the first observation is more "
            "than one month after the first month-end of the full requested window: "
            "requested_start with ordinary dates, while months cut before year 1 count toward "
            "the one-month tolerance."
        ),
    )
    latest: list[IndicatorSnapshot] = Field(
        description="One snapshot per indicator column, all eight whenever a macro row exists.",
    )
    inflation_series: list[SeriesPoint] = Field(
        description="12-month inflation (%) per observation in the window, oldest first.",
    )
    rates_series: list[RatesPoint] = Field(
        description=(
            "Policy, interbank and T-bill rates per observation in the window that has any of "
            "them, oldest first."
        ),
    )
    yield_curve: YieldCurve = Field(
        description="Current yield curve and its month-end history over the window.",
    )
