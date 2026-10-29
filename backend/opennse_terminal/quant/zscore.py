"""Rolling Z-scores of daily volume and turnover, and the single spike rule.

For one ticker, one metric ``x`` (volume in shares, or ``turnover_kes`` in KES) and day ``t``::

    z_t = (x_t - mean(W)) / sd(W)

``W`` holds up to ``N`` of the ticker's own observations strictly before ``t``, taken from the
previous ``2N`` market sessions, and ``sd`` is the sample standard deviation (n - 1). Market
sessions are the observed market calendar: the distinct ``trade_date`` values any ticker has in
``terminal_daily_prices``, never a holiday list.

A session on which the ticker has no row is skipped, not zero- or forward-filled, because a
scraper gap is not a no-trade day; a recorded row with a value of 0 is a real observation. A spike
is ``z >= threshold`` on the positive side only (:func:`is_spike`).

Values are ``Decimal`` throughout and nothing here rounds: services quantize ``z`` with kind
``"z"`` and the rolling mean and standard deviation in the metric's own unit.
"""

from __future__ import annotations

import statistics
from bisect import bisect_left
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, NamedTuple

from opennse_terminal.quant.numeric import safe_div

__all__ = [
    "DEFAULT_SPIKE_THRESHOLD",
    "DEFAULT_Z_WINDOW",
    "MAX_Z_WINDOW",
    "MIN_Z_WINDOW",
    "Observation",
    "ZScorePoint",
    "default_min_periods",
    "is_spike",
    "rolling_zscores",
]

DEFAULT_Z_WINDOW: Final = 20
"""Default window ``N``, in observations: the 20-day volume Z-score."""

MIN_Z_WINDOW: Final = 5
"""Smallest accepted window ``N``."""

MAX_Z_WINDOW: Final = 250
"""Largest accepted window ``N``."""

DEFAULT_SPIKE_THRESHOLD: Final = Decimal("2.00")
"""Default Z-score at or above which a day is a spike."""

# The standard deviation of fewer than two observations is undefined.
_MIN_PERIODS_FLOOR: Final = 2


class Observation(NamedTuple):
    """One ticker's metric on one day: volume in shares or turnover in KES."""

    trade_date: date
    value: Decimal


@dataclass(frozen=True, slots=True)
class ZScorePoint:
    """The rolling Z-score of one observed day.

    Attributes:
        trade_date: The day scored.
        value: The day's metric value, in the metric's unit.
        rolling_mean: Mean of the window ``W``, in the metric's unit; ``None`` when ``W`` has
            fewer than ``min_periods`` observations.
        rolling_stdev: Sample standard deviation of ``W``, in the metric's unit; ``None`` as for
            ``rolling_mean``.
        z: Dimensionless Z-score, unrounded; ``None`` with a reason in ``flags`` when undefined.
        observations_used: ``|W|``, the number of observations in the window.
        flags: Reason codes for a null ``z``: ``insufficient_observations`` or
            ``zero_variance``. Empty when ``z`` is defined.
    """

    trade_date: date
    value: Decimal
    rolling_mean: Decimal | None
    rolling_stdev: Decimal | None
    z: Decimal | None
    observations_used: int
    flags: list[str]


def _check_window(window: int) -> None:
    if not MIN_Z_WINDOW <= window <= MAX_Z_WINDOW:
        raise ValueError(f"window must be between {MIN_Z_WINDOW} and {MAX_Z_WINDOW}, got {window}")


def default_min_periods(window: int) -> int:
    """Return ``ceil(0.75 * window)``, the fewest observations a Z-score needs by default.

    Computed in integer arithmetic: 15 for the default window of 20, and 4 for a window of 5.

    Raises:
        ValueError: If ``window`` is outside ``MIN_Z_WINDOW..MAX_Z_WINDOW``.
    """
    _check_window(window)
    return -(-3 * window // 4)


def _check_inputs(rows: Sequence[tuple[date, Decimal]], sessions: Sequence[date]) -> None:
    previous: date | None = None
    for trade_date, value in rows:
        if previous is not None and trade_date <= previous:
            raise ValueError(
                f"rows must be strictly ascending by date: {trade_date} follows {previous}"
            )
        # A float or int would make statistics.mean return a float, so only Decimal may enter.
        if not isinstance(value, Decimal):
            raise TypeError(f"row values must be Decimal, got {type(value).__name__}")
        if not value.is_finite():
            raise ValueError(f"row value on {trade_date} is not finite: {value}")
        previous = trade_date
    for earlier, later in zip(sessions, sessions[1:], strict=False):
        if later <= earlier:
            raise ValueError(
                f"sessions must be strictly ascending without duplicates: {later} follows {earlier}"
            )


def rolling_zscores(
    rows: Sequence[tuple[date, Decimal]],
    sessions: Sequence[date],
    *,
    window: int = DEFAULT_Z_WINDOW,
    min_periods: int | None = None,
) -> list[ZScorePoint]:
    """Score each of one ticker's observations against its own trailing window.

    For the row on day ``t`` with value ``x_t``: ``z = (x_t - mean(W)) / sd(W)``, where ``sd`` is
    the sample standard deviation (n - 1) and ``W`` is built as follows.

    * Eligible span: the ``2N`` market sessions immediately before ``t`` (``N = window``). The cap
      stops a sparse counter comparing against months-old sessions.
    * ``W``: the ticker's rows dated inside that span and strictly before ``t``, keeping only the
      most recent ``N``. The current day is excluded, so a spike does not dampen itself.
    * A session without a row for the ticker is skipped: it is not zero- or forward-filled, so it
      only shrinks ``|W|``. A row whose value is 0 is a valid observation.

    Undefined results are ``None`` with a flag, never zero:

    * ``|W| < min_periods``: ``z``, ``rolling_mean`` and ``rolling_stdev`` are ``None``, flag
      ``insufficient_observations``;
    * ``sd(W) == 0``: ``z`` is ``None``, flag ``zero_variance``; the mean and standard deviation
      are still returned.

    A day without a row produces no point: the output has exactly one point per input row, in
    input order. ``z`` is dimensionless; ``rolling_mean`` and ``rolling_stdev`` are in the metric's
    unit (shares for volume, KES for turnover). Nothing is rounded.

    Args:
        rows: One ticker's ``(trade_date, value)`` pairs, such as :class:`Observation` records,
            strictly ascending by date. Volumes are passed as ``Decimal(int)``.
        sessions: The observed market sessions, strictly ascending. They must reach back at least
            ``2N`` sessions before the first row whose score is used, because rows dated before
            the first session are never in a window.
        window: ``N``, the most observations ``W`` may hold, within ``MIN_Z_WINDOW..MAX_Z_WINDOW``.
        min_periods: Fewest observations a defined ``z`` needs, within ``2..window``; defaults to
            :func:`default_min_periods` of ``window`` (15 for 20).

    Returns:
        One :class:`ZScorePoint` per row.

    Raises:
        ValueError: If ``window`` or ``min_periods`` is out of range, ``rows`` or ``sessions`` is
            not strictly ascending, or a value is NaN or infinite.
        TypeError: If a row value is not a ``Decimal``.
    """
    _check_window(window)
    required = default_min_periods(window) if min_periods is None else min_periods
    if not _MIN_PERIODS_FLOOR <= required <= window:
        raise ValueError(
            f"min_periods must be between {_MIN_PERIODS_FLOOR} and the window {window}, "
            f"got {required}"
        )
    _check_inputs(rows, sessions)

    dates = [trade_date for trade_date, _ in rows]
    values = [value for _, value in rows]
    span = 2 * window
    points: list[ZScorePoint] = []
    for position, (trade_date, value) in enumerate(rows):
        # Rows are strictly ascending, so rows[:position] are exactly those dated before t.
        sessions_before = bisect_left(sessions, trade_date)
        if sessions_before == 0:
            start = position
        else:
            earliest = sessions[max(0, sessions_before - span)]
            start = bisect_left(dates, earliest, 0, position)
        start = max(start, position - window)
        observed = values[start:position]

        if len(observed) < required:
            points.append(
                ZScorePoint(
                    trade_date=trade_date,
                    value=value,
                    rolling_mean=None,
                    rolling_stdev=None,
                    z=None,
                    observations_used=len(observed),
                    flags=["insufficient_observations"],
                )
            )
            continue

        mean = statistics.mean(observed)
        stdev = statistics.stdev(observed)
        z = safe_div(value - mean, stdev)
        points.append(
            ZScorePoint(
                trade_date=trade_date,
                value=value,
                rolling_mean=mean,
                rolling_stdev=stdev,
                z=z,
                observations_used=len(observed),
                flags=[] if z is not None else ["zero_variance"],
            )
        )
    return points


def is_spike(z: Decimal | None, threshold: Decimal = DEFAULT_SPIKE_THRESHOLD) -> bool:
    """Return whether a Z-score is a spike: ``z`` is defined and ``z >= threshold``.

    This is the one spike rule of the terminal. It decides the screener's ``is_volume_anomaly``
    and ``is_turnover_anomaly``, its ``anomalies_only`` filter, and membership of the anomaly
    feed. Only the positive side counts: a negative Z-score is reported as a value but is never
    an anomaly. Pass the unrounded ``z`` from :func:`rolling_zscores`; the API validates
    ``threshold`` to 0.5-10.
    """
    return z is not None and z >= threshold
