"""Macro overview service: CBK indicator snapshots, rate history, coverage and the yield curve.

Serves ``GET /api/v1/terminal/macro/overview``. It reads only what the CBK pipelines, CSV imports
and the seeder stored in ``terminal_macro_indicators`` and ``terminal_bond_yields``, in at most
three statements (one returns at most two values per indicator; the other two load only rows
dated inside the requested window and the lookbacks of the curves it serves, never the stored
history between or around them), resolves every value at full ``Decimal`` precision and rounds
each output value exactly once with :func:`~opennse_terminal.quant.numeric.quantize_out` while
building the response.

Units: ``*_pct`` values are percentage points (``6.80`` means 6.80%), ``fx_kes_usd`` is KES per
USD, ``fx_reserves_usd_m`` is millions of USD and tenors are years.
"""

import logging
from bisect import bisect_left, bisect_right
from calendar import monthrange
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Final

from sqlalchemy import false, func, literal, or_, select, true, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.models import TerminalBondYield, TerminalMacroIndicator
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS
from opennse_terminal.quant.asof import (
    DEFAULT_MACRO_MAX_STALENESS_DAYS,
    AsOfValue,
    MacroColumn,
    MacroRow,
    macro_as_of,
)
from opennse_terminal.quant.numeric import QuantizeKind, quantize_out
from opennse_terminal.schemas.common import CoverageMeta, SeriesPoint
from opennse_terminal.schemas.macro import (
    IndicatorKey,
    IndicatorSnapshot,
    IndicatorUnit,
    MacroOverviewResponse,
    RatesPoint,
    TenorLabel,
    YieldCurve,
    YieldCurvePoint,
    YieldCurveSnapshot,
)

__all__ = [
    "BOND_BUCKETS_YEARS",
    "INDICATORS",
    "TBILL_TENORS",
    "BondObservation",
    "get_macro_overview",
    "yield_curve_at",
]

logger = logging.getLogger("opennse_terminal.services.macro_service")

INDICATORS: Final[tuple[tuple[IndicatorKey, str, IndicatorUnit, QuantizeKind], ...]] = (
    ("inflation_yoy_pct", "12-month inflation", "pct", "pct"),
    ("cbr_rate_pct", "Central Bank Rate", "pct", "pct"),
    ("interbank_rate_pct", "Interbank rate", "pct", "pct"),
    ("tbill_91d_pct", "91-day T-bill", "pct", "pct"),
    ("tbill_182d_pct", "182-day T-bill", "pct", "pct"),
    ("tbill_364d_pct", "364-day T-bill", "pct", "pct"),
    ("fx_kes_usd", "KES per USD", "kes_per_usd", "money"),
    ("fx_reserves_usd_m", "CBK FX reserves (USD m)", "usd_m", "money"),
)
"""The KPI snapshots in display order: ``(column, label, unit, quantize kind)``. FX values use the
2 dp ``money`` kind."""

# Each key is also the column macro_as_of resolves; a list that drifted from the model would drop
# or misname a KPI tile silently, so the mismatch fails at import instead.
if tuple(key for key, _label, _unit, _kind in INDICATORS) != MACRO_INDICATOR_COLUMNS:
    raise RuntimeError("INDICATORS keys must equal MACRO_INDICATOR_COLUMNS, in order")

TBILL_TENORS: Final[tuple[tuple[MacroColumn, TenorLabel, Decimal], ...]] = (
    ("tbill_91d_pct", "91D", Decimal("0.25")),
    ("tbill_182d_pct", "182D", Decimal("0.50")),
    ("tbill_364d_pct", "364D", Decimal("1.00")),
)
"""The T-bill legs of the curve: ``(column, tenor label, tenor in years)``."""

BOND_BUCKETS_YEARS: Final[tuple[int, ...]] = (2, 5, 10, 15, 20, 25)
"""Bond tenor buckets, in years, that FXD observations are grouped into by remaining tenor."""

_BOND_BUCKET_LABELS: Final[Mapping[int, TenorLabel]] = {
    2: "2Y",
    5: "5Y",
    10: "10Y",
    15: "15Y",
    20: "20Y",
    25: "25Y",
}

# Only fixed-coupon bonds are comparable on one curve; tax-exempt infrastructure (IFB) and special
# (SDB) bonds are stored but never read here. Seeded codes FXD-SEED-... match by design.
_FIXED_COUPON_PREFIX: Final = "FXD"
_SEED_SOURCE: Final = "seed"
_UNKNOWN_SOURCE: Final = "unknown"
_BOND_LOOKBACK: Final = timedelta(days=365)
# yield_curve_at resolves T-bill yields with macro_as_of's default staleness limit.
_TBILL_LOOKBACK: Final = timedelta(days=DEFAULT_MACRO_MAX_STALENESS_DAYS)


@dataclass(frozen=True, slots=True)
class BondObservation:
    """One fixed-coupon (``FXD``) Treasury bond observation, mirroring ``terminal_bond_yields``.

    Attributes:
        observation_date: Auction issue date.
        bond_code: CBK issue number such as ``FXD1/2018/025``, or ``FXD-SEED-...`` when seeded.
        remaining_tenor_years: Years to maturity from ``observation_date``.
        yield_pct: Redemption yield in percentage points.
        source: Provenance, ``cbk_bonds`` or ``seed``.
    """

    observation_date: date
    bond_code: str
    remaining_tenor_years: Decimal
    yield_pct: Decimal
    source: str

    @property
    def is_real(self) -> bool:
        """True for an observation from a real source rather than the seeder."""
        return self.source != _SEED_SOURCE


# ---------------------------------------------------------------------------------------------
# Month arithmetic
# ---------------------------------------------------------------------------------------------


def _shift_month(year: int, month: int, k: int) -> tuple[int, int]:
    """Return the ``(year, month)`` that lies ``k`` calendar months from ``year``-``month``."""
    shifted_year, month_index = divmod(year * 12 + (month - 1) + k, 12)
    return shifted_year, month_index + 1


def _month_end(year: int, month: int) -> date:
    """Return the last day of ``year``-``month``."""
    return date(year, month, monthrange(year, month)[1])


def _window_month_ends(end: date, history_months: int) -> list[date]:
    """Return exactly ``history_months`` month-ends, oldest first, ending at ``end``'s month.

    Month-ends are derived by month arithmetic, never by subtracting days, so the month-end before
    2026-09-30 is 2026-08-31.
    """
    return [
        _month_end(*_shift_month(end.year, end.month, offset))
        for offset in range(1 - history_months, 1)
    ]


# ---------------------------------------------------------------------------------------------
# Yield curve
# ---------------------------------------------------------------------------------------------


def _nearest_bucket(remaining_tenor_years: Decimal) -> int:
    """Return the bucket of :data:`BOND_BUCKETS_YEARS` nearest to ``remaining_tenor_years``.

    A tenor exactly between two buckets (3.5, 7.5, 12.5, 17.5 or 22.5 years) goes to the lower
    one: the assignment stays deterministic, and a seasoned bond whose remaining tenor has decayed
    to the midpoint is grouped with the shorter tenor it is approaching.
    """
    best = BOND_BUCKETS_YEARS[0]
    best_distance = abs(remaining_tenor_years - best)
    for bucket in BOND_BUCKETS_YEARS[1:]:
        distance = abs(remaining_tenor_years - bucket)
        if distance < best_distance:
            best, best_distance = bucket, distance
    return best


def _preferred(candidate: BondObservation, incumbent: BondObservation) -> bool:
    """Whether ``candidate`` should replace ``incumbent`` as its bucket's curve point.

    A real observation always beats a seeded one, so a later synthetic date never displaces real
    data; then the more recent observation wins, then the lower ``bond_code``.
    """
    if candidate.is_real != incumbent.is_real:
        return candidate.is_real
    if candidate.observation_date != incumbent.observation_date:
        return candidate.observation_date > incumbent.observation_date
    return candidate.bond_code < incumbent.bond_code


def _choose_bonds(bond_rows: Sequence[BondObservation], day: date) -> dict[int, BondObservation]:
    """Pick one observation per tenor bucket from the 365 days up to and including ``day``."""
    start = bisect_left(bond_rows, day - _BOND_LOOKBACK, key=lambda bond: bond.observation_date)
    stop = bisect_right(bond_rows, day, key=lambda bond: bond.observation_date)
    chosen: dict[int, BondObservation] = {}
    for index in range(start, stop):
        bond = bond_rows[index]
        bucket = _nearest_bucket(bond.remaining_tenor_years)
        incumbent = chosen.get(bucket)
        if incumbent is None or _preferred(bond, incumbent):
            chosen[bucket] = bond
    return chosen


def yield_curve_at(
    macro_rows: Sequence[MacroRow],
    sources_by_date: Mapping[date, Mapping[str, str]],
    bond_rows: Sequence[BondObservation],
    day: date,
) -> list[YieldCurvePoint]:
    """Resolve the yield curve as it stood on ``day``, in ascending tenor.

    * **T-bills** (91D at 0.25, 182D at 0.50 and 364D at 1.00 years): each tenor independently
      takes the latest non-null value of its column on or before ``day`` and at most 62 days old,
      through :func:`~opennse_terminal.quant.asof.macro_as_of`, so a newer row holding only other
      indicators never hides a bill yield. Its ``source`` is that row's ``source_by_field`` entry
      for the column, or ``unknown``. A tenor without such a value is omitted.
    * **Bonds**: FXD observations dated from ``day`` minus 365 days to ``day`` are bucketed to the
      nearest of 2, 5, 10, 15, 20 and 25 years by remaining tenor. Each bucket takes its most recent
      real observation (``source != "seed"``, ties by ``bond_code``), and a seeded one only when
      the bucket has no real observation in the window. ``tenor_years`` is the chosen bond's
      remaining tenor.

    Yields are percentage points and tenors years, both quantized to 2 dp.

    Args:
        macro_rows: Macro observations sorted ascending by ``observation_date``.
        sources_by_date: ``source_by_field`` of each macro row, keyed by its observation date.
        bond_rows: FXD bond observations sorted ascending by ``observation_date``.
        day: The curve date.

    Returns:
        One point per tenor that has an observation, sorted by ``tenor_years``, T-bills first on
        an equal tenor.
    """
    points: list[YieldCurvePoint] = []
    for column, label, tenor_years in TBILL_TENORS:
        resolved = macro_as_of(macro_rows, column, day)
        if resolved is None:
            continue
        row_sources = sources_by_date.get(resolved.observation_date, {})
        source = row_sources.get(column, _UNKNOWN_SOURCE)
        points.append(
            YieldCurvePoint(
                tenor_label=label,
                tenor_years=quantize_out(tenor_years, "tenor"),
                yield_pct=quantize_out(resolved.value, "pct"),
                instrument="tbill",
                bond_code=None,
                observation_date=resolved.observation_date,
                source=source,
            )
        )
    for bucket, bond in _choose_bonds(bond_rows, day).items():
        points.append(
            YieldCurvePoint(
                tenor_label=_BOND_BUCKET_LABELS[bucket],
                tenor_years=quantize_out(bond.remaining_tenor_years, "tenor"),
                yield_pct=quantize_out(bond.yield_pct, "pct"),
                instrument="bond",
                bond_code=bond.bond_code,
                observation_date=bond.observation_date,
                source=bond.source,
            )
        )
    points.sort(key=lambda point: (point.tenor_years, point.instrument != "tbill"))
    return points


# ---------------------------------------------------------------------------------------------
# Coverage, snapshots and series
# ---------------------------------------------------------------------------------------------


def _first_on_or_after(macro_rows: Sequence[MacroRow], day: date) -> int:
    """Index of the first row dated on or after ``day`` in date-sorted ``macro_rows``."""
    return bisect_left(macro_rows, day, key=lambda row: row.observation_date)


def _has_indicator(row: MacroRow) -> bool:
    return any(getattr(row, column) is not None for column in MACRO_INDICATOR_COLUMNS)


def _coverage(macro_rows: Sequence[MacroRow], requested_start: date) -> CoverageMeta:
    """Coverage of the window that opens at the month-end ``requested_start``.

    ``coverage_start`` is the first observation on or after ``requested_start`` with any non-null
    indicator. Coverage is partial when there is none, or when it falls after the month-end one
    month past ``requested_start``.
    """
    coverage_start: date | None = None
    for index in range(_first_on_or_after(macro_rows, requested_start), len(macro_rows)):
        if _has_indicator(macro_rows[index]):
            coverage_start = macro_rows[index].observation_date
            break
    one_month_later = _month_end(*_shift_month(requested_start.year, requested_start.month, 1))
    return CoverageMeta(
        requested_start=requested_start,
        coverage_start=coverage_start,
        is_partial_coverage=coverage_start is None or coverage_start > one_month_later,
    )


def _indicator_snapshots(observed: Mapping[str, Sequence[AsOfValue]]) -> list[IndicatorSnapshot]:
    """One snapshot per :data:`INDICATORS` entry, each column resolved independently.

    ``observed`` maps an indicator column to its non-null observations, newest first, however far
    back in history they lie: the first gives ``value`` and ``as_of``, the second
    ``previous_value``, and any further entry is ignored. A column that is absent or maps to no
    observation was never observed, so its value, date, previous value and change are all null.
    ``change`` is ``value - previous_value`` computed on the unrounded values, then quantized.
    """
    snapshots: list[IndicatorSnapshot] = []
    for key, label, unit, kind in INDICATORS:
        history = observed.get(key, ())
        latest = history[0] if history else None
        previous = history[1] if len(history) > 1 else None
        value = None if latest is None else latest.value
        previous_value = None if previous is None else previous.value
        change = (
            value - previous_value if value is not None and previous_value is not None else None
        )
        snapshots.append(
            IndicatorSnapshot(
                key=key,
                label=label,
                value=quantize_out(value, kind),
                unit=unit,
                as_of=None if latest is None else latest.observation_date,
                previous_value=quantize_out(previous_value, kind),
                change=quantize_out(change, kind),
            )
        )
    return snapshots


def _inflation_series(window_rows: Sequence[MacroRow]) -> list[SeriesPoint]:
    """12-month inflation (percentage points) for each window row that has it."""
    return [
        SeriesPoint(date=row.observation_date, value=quantize_out(row.inflation_yoy_pct, "pct"))
        for row in window_rows
        if row.inflation_yoy_pct is not None
    ]


def _rates_series(window_rows: Sequence[MacroRow]) -> list[RatesPoint]:
    """Policy, interbank and T-bill rates for each window row that has at least one of them."""
    points: list[RatesPoint] = []
    for row in window_rows:
        rates = (
            row.cbr_rate_pct,
            row.interbank_rate_pct,
            row.tbill_91d_pct,
            row.tbill_182d_pct,
            row.tbill_364d_pct,
        )
        if all(rate is None for rate in rates):
            continue
        points.append(
            RatesPoint(
                observation_date=row.observation_date,
                cbr_rate_pct=quantize_out(row.cbr_rate_pct, "pct"),
                interbank_rate_pct=quantize_out(row.interbank_rate_pct, "pct"),
                tbill_91d_pct=quantize_out(row.tbill_91d_pct, "pct"),
                tbill_182d_pct=quantize_out(row.tbill_182d_pct, "pct"),
                tbill_364d_pct=quantize_out(row.tbill_364d_pct, "pct"),
            )
        )
    return points


# ---------------------------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------------------------


def _lookback_start(day: date, lookback: timedelta) -> date:
    """``day - lookback``, or ``date.min`` when that would fall before the first calendar day."""
    try:
        return day - lookback
    except OverflowError:
        return date.min


def _bond_read_ranges(
    month_ends: Sequence[date], current_day: date
) -> tuple[tuple[date, date], ...]:
    """The closed ``(start, end)`` date ranges, oldest first, holding every FXD bond a curve reads.

    Each curve reads the 365 days up to its date. The history curves at the date-sorted,
    non-empty ``month_ends`` lie at most a month apart, so together they read one interval: from
    365 days before the earliest curve date to the last month-end. The earliest curve date is the
    first month-end, or ``current_day`` when that is earlier (a one-month window whose latest
    observation falls before the month-end). The current curve at ``current_day`` reads from 365
    days before it to ``current_day``. The two intervals come back as one range when they overlap
    or touch, and as two otherwise, so the bonds dated between a window and a much later current
    curve, which no curve reads, are never loaded. Starts are clamped at ``date.min``.
    """
    history_start = _lookback_start(min(month_ends[0], current_day), _BOND_LOOKBACK)
    history_end = month_ends[-1]
    current_start = _lookback_start(current_day, _BOND_LOOKBACK)
    # Subtracting the dates cannot overflow, as adding a day to a history_end of date.max would.
    if (current_start - history_end).days <= 1:
        return ((history_start, max(history_end, current_day)),)
    return ((history_start, history_end), (current_start, current_day))


async def _load_latest(
    session: AsyncSession,
) -> tuple[date | None, date | None, dict[str, list[AsOfValue]]]:
    """Read the window anchors and each indicator's latest two non-null values in one statement.

    Returns the latest macro observation date and the latest FXD bond observation date, each
    ``None`` when no such row is stored, and a map from every indicator column observed at least
    once to its latest non-null value and the non-null value before it, newest first, however far
    back they lie. The statement returns at most 16 value rows (two per indicator) plus the
    anchors. Each indicator's part is a backward scan of the unique ``observation_date`` index
    that stops at its column's second non-null value, so for a column observed on recent dates it
    touches only the newest index entries; for a sparse column it scans back as far as needed, and
    for a column never observed, the whole index. The one-row anchors are LEFT JOINed to the parts
    so they arrive even when no indicator was ever observed; that row carries a null indicator and
    contributes no value.
    """
    macro_columns = TerminalMacroIndicator.__table__.c
    anchors = select(
        select(func.max(TerminalMacroIndicator.observation_date))
        .scalar_subquery()
        .label("latest_macro_date"),
        select(func.max(TerminalBondYield.observation_date))
        .where(TerminalBondYield.bond_code.startswith(_FIXED_COUPON_PREFIX))
        .scalar_subquery()
        .label("latest_bond_date"),
    ).subquery("anchors")
    observed = union_all(
        *(
            select(
                literal(column).label("indicator"),
                TerminalMacroIndicator.observation_date,
                macro_columns[column].label("value"),
            )
            .where(macro_columns[column].is_not(None))
            .order_by(TerminalMacroIndicator.observation_date.desc())
            .limit(2)
            for column in MACRO_INDICATOR_COLUMNS
        )
    ).subquery("observed")
    result = await session.execute(
        select(
            anchors.c.latest_macro_date,
            anchors.c.latest_bond_date,
            observed.c.indicator,
            observed.c.observation_date,
            observed.c.value,
        )
        .select_from(anchors.outerjoin(observed, true()))
        .order_by(observed.c.observation_date.desc())
    )
    latest_macro_date: date | None = None
    latest_bond_date: date | None = None
    observations: dict[str, list[AsOfValue]] = {}
    for macro_date, bond_date, indicator, observation_date, value in result.tuples():
        latest_macro_date, latest_bond_date = macro_date, bond_date
        if indicator is not None:
            observations.setdefault(indicator, []).append(AsOfValue(value, observation_date))
    return latest_macro_date, latest_bond_date, observations


async def _load_macro(
    session: AsyncSession, since: date
) -> tuple[list[MacroRow], dict[date, Mapping[str, str]]]:
    """Load the macro rows dated on or after ``since``, in date order, with their provenance.

    Only the date, the indicator columns and ``source_by_field`` are read; each row's string
    ``source_by_field`` entries are returned keyed by its observation date.
    """
    records = await session.execute(
        select(
            TerminalMacroIndicator.observation_date,
            TerminalMacroIndicator.inflation_yoy_pct,
            TerminalMacroIndicator.cbr_rate_pct,
            TerminalMacroIndicator.interbank_rate_pct,
            TerminalMacroIndicator.tbill_91d_pct,
            TerminalMacroIndicator.tbill_182d_pct,
            TerminalMacroIndicator.tbill_364d_pct,
            TerminalMacroIndicator.fx_kes_usd,
            TerminalMacroIndicator.fx_reserves_usd_m,
            TerminalMacroIndicator.source_by_field,
        )
        .where(TerminalMacroIndicator.observation_date >= since)
        .order_by(TerminalMacroIndicator.observation_date)
    )
    macro_rows: list[MacroRow] = []
    sources_by_date: dict[date, Mapping[str, str]] = {}
    for record in records:
        macro_rows.append(
            MacroRow(
                observation_date=record.observation_date,
                inflation_yoy_pct=record.inflation_yoy_pct,
                cbr_rate_pct=record.cbr_rate_pct,
                interbank_rate_pct=record.interbank_rate_pct,
                tbill_91d_pct=record.tbill_91d_pct,
                tbill_182d_pct=record.tbill_182d_pct,
                tbill_364d_pct=record.tbill_364d_pct,
                fx_kes_usd=record.fx_kes_usd,
                fx_reserves_usd_m=record.fx_reserves_usd_m,
            )
        )
        sources_by_date[record.observation_date] = {
            column: source
            for column, source in record.source_by_field.items()
            if isinstance(source, str)
        }
    return macro_rows, sources_by_date


async def _load_bonds(
    session: AsyncSession, ranges: Sequence[tuple[date, date]]
) -> list[BondObservation]:
    """Load the FXD bond observations dated inside any of ``ranges``, in date order.

    Each range is a closed ``(start, end)`` pair of dates, both ends included, as
    :func:`_bond_read_ranges` returns them. The dates are bound parameters, and each range is a
    scan of the ``observation_date`` index. An empty ``ranges`` matches no observation.
    """
    statement = select(
        TerminalBondYield.observation_date,
        TerminalBondYield.bond_code,
        TerminalBondYield.remaining_tenor_years,
        TerminalBondYield.yield_pct,
        TerminalBondYield.source,
    ).where(TerminalBondYield.bond_code.startswith(_FIXED_COUPON_PREFIX))
    # false() is dropped from the OR as soon as one range is present.
    statement = statement.where(
        or_(
            false(),
            *(TerminalBondYield.observation_date.between(start, end) for start, end in ranges),
        )
    )
    statement = statement.order_by(TerminalBondYield.observation_date, TerminalBondYield.bond_code)
    result = await session.execute(statement)
    return [
        BondObservation(
            observation_date=observation_date,
            bond_code=bond_code,
            remaining_tenor_years=remaining_tenor_years,
            yield_pct=yield_pct,
            source=source,
        )
        for observation_date, bond_code, remaining_tenor_years, yield_pct, source in result.tuples()
    ]


def _empty_overview(history_months: int) -> MacroOverviewResponse:
    """The response when nothing is stored: the window ends at the current month (UTC)."""
    today = datetime.now(UTC).date()
    return MacroOverviewResponse(
        coverage=CoverageMeta(
            requested_start=_window_month_ends(today, history_months)[0],
            coverage_start=None,
            is_partial_coverage=True,
        ),
        latest=[],
        inflation_series=[],
        rates_series=[],
        yield_curve=YieldCurve(current=[], history=[]),
    )


# ---------------------------------------------------------------------------------------------
# Endpoint service
# ---------------------------------------------------------------------------------------------


async def get_macro_overview(session: AsyncSession, history_months: int) -> MacroOverviewResponse:
    """Build ``GET /macro/overview``: KPI snapshots, inflation and rates history, yield curve.

    The window holds exactly ``history_months`` month-ends ending at the month of the latest macro
    observation; ``coverage.requested_start`` is the first of them (2024-10-31 for 24 months with
    data through 2026-09-30). ``coverage.is_partial_coverage`` is true when no indicator is
    observed in the window or the first one falls more than one month after ``requested_start``.

    * ``latest``: one snapshot per indicator, all eight whenever a macro row exists. Each column
      is resolved on its own: its latest non-null value with that value's date, the non-null value
      before it, and their difference.
    * ``inflation_series`` and ``rates_series``: the window's observations, oldest first, each
      kept only when it carries a value of the series.
    * ``yield_curve.current``: :func:`yield_curve_at` on the later of the latest macro and the
      latest FXD bond observation date. ``yield_curve.history``: one snapshot per window
      month-end, oldest first.

    Units: ``*_pct`` values are percentage points (``6.80`` means 6.80%), ``fx_kes_usd`` is KES
    per USD, ``fx_reserves_usd_m`` is millions of USD and tenors are years. Rates, FX values,
    reserves and tenors are quantized to 2 dp. With only FXD bonds stored the window ends at the
    month of the latest bond observation and ``latest`` is empty; with nothing stored every list
    is empty and the window ends at the current month. This function never raises for missing
    data.

    Reads take at most three statements. Statement 1 returns at most two values per indicator;
    the rows statements 2 and 3 load are bounded by the window and the lookbacks of its curves and
    the current curve, never by the stored history. The earliest curve date is
    ``requested_start``, or the current curve's date when that is earlier (a one-month window
    whose latest observation falls before the month-end).

    1. The latest macro and FXD bond observation dates, and each indicator's latest two non-null
       values, resolved in SQL. With nothing stored the response is built from this alone.
    2. The macro rows from 62 days before the earliest curve date on, which the series, the
       coverage and the T-bill legs of every curve need. Skipped when no macro row exists.
    3. The FXD bond observations the curves read: from 365 days before the earliest curve date
       to the last window month-end, and the 365 days up to the current curve's date. The two
       intervals are read as one date range when they overlap or touch and as two otherwise, so
       bonds dated between the window and a much later current curve are never loaded.

    Args:
        session: An async session on the terminal database; it is only read.
        history_months: Month-ends in the history window, at least 1 (the route bounds it to 240).

    Raises:
        ValueError: If ``history_months`` is less than 1.
    """
    if history_months < 1:
        raise ValueError(f"history_months must be >= 1, got {history_months}")

    latest_macro_date, latest_bond_date, observed = await _load_latest(session)
    if latest_macro_date is not None:
        window_end = latest_macro_date
    elif latest_bond_date is not None:
        # Only bonds are stored (cbk_bonds ran before any rate release): the window then ends at
        # the latest bond month so the curve still renders.
        window_end = latest_bond_date
    else:
        return _empty_overview(history_months)
    month_ends = _window_month_ends(window_end, history_months)
    requested_start = month_ends[0]
    current_day = max(day for day in (latest_macro_date, latest_bond_date) if day is not None)
    # Every curve, at each window month-end and at current_day, looks back 62 days for T-bill
    # yields and 365 days for bond observations. With a one-month window current_day can fall
    # before that month's end, so the reads start from the earlier of the two dates. The macro
    # read needs no upper bound: no macro row is dated after latest_macro_date, which is never
    # after month_ends[-1]. FXD bonds can run on to a current_day long after the window, so the
    # bond read is limited to the history and current curve intervals.
    earliest_curve_day = min(requested_start, current_day)
    macro_since = _lookback_start(earliest_curve_day, _TBILL_LOOKBACK)
    bond_ranges = _bond_read_ranges(month_ends, current_day)

    macro_rows: list[MacroRow] = []
    sources_by_date: dict[date, Mapping[str, str]] = {}
    if latest_macro_date is not None:
        macro_rows, sources_by_date = await _load_macro(session, since=macro_since)
    bond_rows = await _load_bonds(session, bond_ranges)
    window_rows = macro_rows[_first_on_or_after(macro_rows, requested_start) :]

    overview = MacroOverviewResponse(
        coverage=_coverage(macro_rows, requested_start),
        latest=[] if latest_macro_date is None else _indicator_snapshots(observed),
        inflation_series=_inflation_series(window_rows),
        rates_series=_rates_series(window_rows),
        yield_curve=YieldCurve(
            current=yield_curve_at(macro_rows, sources_by_date, bond_rows, current_day),
            history=[
                YieldCurveSnapshot(
                    as_of=month_end,
                    points=yield_curve_at(macro_rows, sources_by_date, bond_rows, month_end),
                )
                for month_end in month_ends
            ],
        ),
    )
    logger.debug(
        "macro overview: window %s to %s, curve at %s; read %d macro rows from %s and %d FXD "
        "bond rows in %s",
        requested_start,
        month_ends[-1],
        current_day,
        len(macro_rows),
        macro_since,
        len(bond_rows),
        ", ".join(f"[{start}, {end}]" for start, end in bond_ranges),
    )
    return overview
