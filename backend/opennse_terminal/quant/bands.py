"""Historical valuation bands: summary statistics of a daily P/E or P/B series.

The daily series is built by the caller, one entry per market session with a close: per counter
from the single-equity multiples, per sector from the matched-set sector aggregate (each day using
the same ``shares_outstanding`` and that day's ``fundamentals_as_of``). Sessions are the observed
market calendar, the distinct ``trade_date`` values in ``terminal_daily_prices``. This module only
summarizes the series; it performs no I/O and never rounds. Services quantize every ratio statistic
with ``quantize_out(value, "ratio")`` and the percentile with ``quantize_out(value, "pct")``.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, Literal

from opennse_terminal.quant.numeric import HUNDRED, require_statistics_input

__all__ = [
    "DEFAULT_LOOKBACK_YEARS",
    "MIN_BAND_OBSERVATIONS",
    "PARTIAL_COVERAGE_DAYS",
    "BandMetric",
    "BandStatsResult",
    "BandsResult",
    "BandsStatus",
    "valuation_bands",
]

MIN_BAND_OBSERVATIONS: Final = 60
"""Fewest non-null daily values for which statistics are computed."""

PARTIAL_COVERAGE_DAYS: Final = 7
"""Calendar days after ``requested_start`` beyond which coverage counts as partial."""

DEFAULT_LOOKBACK_YEARS: Final = 5
"""Default lookback of the bands endpoint; the API enforces the 1-15 year range."""

BandsStatus = Literal["ok", "insufficient_history", "no_positive_earnings", "no_positive_equity"]
"""Outcome of :func:`valuation_bands`; every value other than ``"ok"`` is also its flag code."""

BandMetric = Literal["pe", "pb"]
"""The banded multiple: price to earnings (``"pe"``) or price to book (``"pb"``)."""


@dataclass(frozen=True, slots=True)
class BandStatsResult:
    """Statistics over the non-null daily values of the band series, unrounded.

    Every field except ``observations`` and ``current_percentile`` is a multiple (ratio, ×).

    Attributes:
        observations: Number of non-null daily values, n.
        mean: Arithmetic mean of the values.
        stdev: Sample standard deviation (n − 1 denominator).
        minus_2sd: ``mean − 2 × stdev``.
        minus_1sd: ``mean − stdev``.
        plus_1sd: ``mean + stdev``.
        plus_2sd: ``mean + 2 × stdev``.
        min: Smallest value.
        p25: First quartile, linear interpolation (inclusive method).
        median: Second quartile, linear interpolation (inclusive method).
        p75: Third quartile, linear interpolation (inclusive method).
        max: Largest value.
        current: The latest non-null daily value.
        current_percentile: Share of values at or below ``current``, in percent (ties count as
            at or below).
    """

    observations: int
    mean: Decimal
    stdev: Decimal
    minus_2sd: Decimal
    minus_1sd: Decimal
    plus_1sd: Decimal
    plus_2sd: Decimal
    min: Decimal
    p25: Decimal
    median: Decimal
    p75: Decimal
    max: Decimal
    current: Decimal
    current_percentile: Decimal


@dataclass(frozen=True, slots=True)
class BandsResult:
    """The valuation-band summary of one subject (a counter or a sector).

    Attributes:
        status: ``"ok"`` when ``stats`` is present, otherwise the reason it is ``None``.
        flags: ``[status]`` when ``status`` is not ``"ok"``, otherwise empty.
        series: Weekly transport points ``(last market session of the ISO week, value)``,
            ascending; weeks without a value are omitted.
        stats: Statistics over the daily values, or ``None`` unless ``status == "ok"``.
        requested_start: First date of the requested lookback.
        coverage_start: Date of the first non-null daily value, or ``None`` when there is none.
        is_partial_coverage: ``True`` when ``coverage_start`` is missing or more than
            :data:`PARTIAL_COVERAGE_DAYS` calendar days after ``requested_start``.
    """

    status: BandsStatus
    flags: list[str]
    series: list[tuple[date, Decimal]]
    stats: BandStatsResult | None
    requested_start: date
    coverage_start: date | None
    is_partial_coverage: bool


def valuation_bands(
    daily: Sequence[tuple[date, Decimal | None]],
    *,
    requested_start: date,
    sessions: Sequence[date],
    metric: BandMetric = "pe",
) -> BandsResult:
    """Summarize a daily valuation-multiple series into historical bands.

    ``daily`` holds one ``(session, value)`` entry per market session with a close, strictly
    ascending by date. A ``None`` value marks a day on which the multiple is undefined (losses for
    P/E, non-positive equity for P/B) and is excluded from everything below. With ``values`` the
    non-null daily values, in date order, and n their count:

    * ``mean`` = Σ values ÷ n; ``stdev`` = √(Σ (v − mean)² ÷ (n − 1)), the sample deviation;
    * ``minus_2sd``, ``minus_1sd``, ``plus_1sd``, ``plus_2sd`` = mean − 2·sd, mean − sd,
      mean + sd, mean + 2·sd, so a zero deviation puts every band on the mean;
    * ``min`` and ``max``; ``p25``, ``median``, ``p75`` are the quartiles by linear interpolation
      between closest ranks (``statistics.quantiles(values, n=4, method="inclusive")``);
    * ``current`` = the last value; ``current_percentile`` = count(v ≤ current) ÷ n × 100.

    Units: every statistic is a multiple (×) except ``current_percentile`` (%). Nothing is rounded.

    Status: ``"ok"`` needs at least :data:`MIN_BAND_OBSERVATIONS` (60) values. Fewer gives
    ``"insufficient_history"`` with ``stats`` ``None`` and the weekly series still returned. No
    value at all gives ``"no_positive_earnings"`` for ``"pe"`` or ``"no_positive_equity"`` for
    ``"pb"``, with an empty series. ``flags`` is ``[status]`` for every status but ``"ok"``.

    Coverage: ``coverage_start`` is the date of the first value, and ``is_partial_coverage`` is
    true when there is none or it falls more than :data:`PARTIAL_COVERAGE_DAYS` (7) calendar days
    after ``requested_start``. Partial coverage does not withhold statistics: they are computed
    over the available values once the minimum is met. A 15-year lookback on the seeded history,
    for example, reports ``coverage_start`` 2012-01-02, partial coverage and about 3,700
    observations.

    Transport: ``series`` is sampled to one point per ISO week of ``sessions``, dated on that
    week's last market session, carrying the latest value dated on or before it within the same
    week. Weeks without a value are omitted. Statistics always use the daily values.

    Args:
        daily: The daily multiple, one entry per market session with a close, ascending.
        requested_start: First date of the requested lookback (``as_of`` minus the lookback).
        sessions: The observed market sessions of the lookback, in any order.
        metric: The banded multiple, which names the status of an all-null series.

    Raises:
        ValueError: ``daily`` is not strictly ascending by date, ``metric`` is unknown, or a value
            is NaN, infinite or outside the exponent and digit bounds of
            :func:`~opennse_terminal.quant.numeric.require_statistics_input`. Every non-null value
            is checked, whatever their count, before any statistic is computed.
        TypeError: A value is neither ``None`` nor a ``Decimal``.
    """
    if metric == "pe":
        empty_status: BandsStatus = "no_positive_earnings"
    elif metric == "pb":
        empty_status = "no_positive_equity"
    else:
        raise ValueError(f"unknown band metric: {metric!r}")

    observed: list[tuple[date, Decimal]] = []
    previous: date | None = None
    for day, value in daily:
        if previous is not None and day <= previous:
            raise ValueError(f"daily series must be strictly ascending: {day} follows {previous}")
        previous = day
        if value is not None:
            require_statistics_input(value, f"daily value on {day}")
            observed.append((day, value))

    coverage_start = observed[0][0] if observed else None
    is_partial_coverage = (
        coverage_start is None or (coverage_start - requested_start).days > PARTIAL_COVERAGE_DAYS
    )

    stats: BandStatsResult | None = None
    status: BandsStatus
    if not observed:
        status = empty_status
    elif len(observed) < MIN_BAND_OBSERVATIONS:
        status = "insufficient_history"
    else:
        status = "ok"
        stats = _band_stats([value for _, value in observed])

    return BandsResult(
        status=status,
        flags=[] if status == "ok" else [status],
        series=_weekly_series(observed, sessions),
        stats=stats,
        requested_start=requested_start,
        coverage_start=coverage_start,
        is_partial_coverage=is_partial_coverage,
    )


def _iso_week(day: date) -> tuple[int, int]:
    """Return the ``(ISO year, ISO week)`` key of ``day``."""
    iso = day.isocalendar()
    return iso.year, iso.week


def _weekly_series(
    observed: Sequence[tuple[date, Decimal]], sessions: Sequence[date]
) -> list[tuple[date, Decimal]]:
    """Sample ascending non-null daily values to the last market session of each ISO week."""
    week_end: dict[tuple[int, int], date] = {}
    for session in sessions:
        key = _iso_week(session)
        last = week_end.get(key)
        if last is None or session > last:
            week_end[key] = session

    # ``observed`` is ascending, so a later value of a week replaces an earlier one and the keys
    # are first inserted, and therefore iterated, in ascending week order.
    sampled: dict[tuple[int, int], Decimal] = {}
    for day, value in observed:
        key = _iso_week(day)
        last = week_end.get(key)
        if last is not None and day <= last:
            sampled[key] = value
    return [(week_end[key], value) for key, value in sampled.items()]


def _band_stats(values: Sequence[Decimal]) -> BandStatsResult:
    """Compute the band statistics of at least two values, keeping ``Decimal`` throughout."""
    count = len(values)
    mean = statistics.mean(values)
    stdev = statistics.stdev(values)
    p25, median, p75 = statistics.quantiles(values, n=4, method="inclusive")
    current = values[-1]
    at_or_below = sum(1 for value in values if value <= current)
    return BandStatsResult(
        observations=count,
        mean=mean,
        stdev=stdev,
        minus_2sd=mean - 2 * stdev,
        minus_1sd=mean - stdev,
        plus_1sd=mean + stdev,
        plus_2sd=mean + 2 * stdev,
        min=min(values),
        p25=p25,
        median=median,
        p75=p75,
        max=max(values),
        current=current,
        current_percentile=Decimal(at_or_below) * HUNDRED / Decimal(count),
    )
