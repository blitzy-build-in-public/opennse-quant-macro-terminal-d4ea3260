"""Historical valuation bands (``opennse_terminal.quant.bands``).

Every expected statistic is a hand-computed literal, never recomputed in the test. Exact values
are compared as ``Decimal``; rounded values are compared as the strings ``quantize_out`` produces
with ROUND_HALF_UP, ``ratio`` for multiples and ``pct`` for the percentile. Sessions are explicit
weekday lists, because analytics use the observed market sessions and never a holiday list.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from typing import NoReturn

import pytest

from opennse_terminal.quant.bands import (
    DEFAULT_LOOKBACK_YEARS,
    MIN_BAND_OBSERVATIONS,
    PARTIAL_COVERAGE_DAYS,
    BandMetric,
    BandsResult,
    BandStatsResult,
    valuation_bands,
)
from opennse_terminal.quant.numeric import QuantizeKind, quantize_out

FIRST_SESSION = date(2026, 1, 5)
"""Monday of ISO week 2026-W02; the reference series starts here."""


def weekdays(start: date, count: int) -> list[date]:
    """Return ``count`` consecutive Monday-Friday dates from ``start``, with no holiday logic.

    ``start`` is the first date returned when it is a weekday; otherwise the list begins on the
    following Monday. Each call builds a new list, so a caller may change its copy freely.
    """
    days: list[date] = []
    day = start
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


def series(
    dates: Sequence[date], values: Sequence[Decimal | None]
) -> list[tuple[date, Decimal | None]]:
    """Pair each session with its daily multiple, in the ``(date, value)`` shape of the unit."""
    return list(zip(dates, values, strict=True))


def alternating(count: int) -> list[Decimal]:
    """Return ``count`` values 8, 12, 8, 12, ..., starting with 8."""
    return [Decimal("8") if index % 2 == 0 else Decimal("12") for index in range(count)]


def by_day_of_month(dates: Sequence[date]) -> list[tuple[date, Decimal | None]]:
    """Give each session the value of its day of the month, so a sampled point names its day."""
    return series(dates, [Decimal(day.day) for day in dates])


def rounded(value: Decimal, kind: QuantizeKind) -> str:
    """Return ``value`` as the response boundary renders it."""
    return str(quantize_out(value, kind))


def stats_of(result: BandsResult) -> BandStatsResult:
    """Return the statistics of ``result``, failing when they were withheld."""
    assert result.stats is not None, f"statistics withheld with status {result.status!r}"
    return result.stats


def reference_bands(requested_start: date = FIRST_SESSION) -> BandsResult:
    """Run the reference case: 60 sessions from 2026-01-05 alternating 8 and 12, ending on 12."""
    sessions = weekdays(FIRST_SESSION, 60)
    return valuation_bands(
        series(sessions, alternating(60)), requested_start=requested_start, sessions=sessions
    )


def test_band_constants() -> None:
    """Statistics need 60 values, partial coverage starts beyond 7 days, the lookback is 5 years."""
    assert MIN_BAND_OBSERVATIONS == 60
    assert PARTIAL_COVERAGE_DAYS == 7
    assert DEFAULT_LOOKBACK_YEARS == 5


def test_reference_alternating_series() -> None:
    """60 values alternating 8 and 12 (current 12), requested from the first session.

    Hand-computed: mean 10; Σ(v − mean)² = 60 × 4 = 240, so the sample sd is √(240 / 59) =
    2.016877…, 2.02 at 2 dp and 2.0169 at 4 dp. Bands: 10 − 2 × 2.016877 = 5.966… → 5.97,
    10 − 2.016877 = 7.983… → 7.98, 12.016… → 12.02 and 14.033… → 14.03. The sorted values are
    thirty 8s then thirty 12s, so the inclusive quartiles are 8, (8 + 12) ÷ 2 = 10 and 12. All 60
    values are ≤ 12, so the current percentile is 100.00. The 60 sessions span 12 ISO weeks, giving
    12 weekly points from Friday 2026-01-09 (value 8, the 5th session) to Friday 2026-03-27
    (value 12, the 60th), while the statistics use all 60 daily values.
    """
    result = reference_bands()

    assert result.status == "ok"
    assert result.flags == []
    stats = stats_of(result)
    assert stats.observations == 60
    assert stats.mean == Decimal("10")
    assert rounded(stats.stdev, "ratio") == "2.02"
    assert str(stats.stdev.quantize(Decimal("0.0001"))) == "2.0169"
    assert rounded(stats.minus_2sd, "ratio") == "5.97"
    assert rounded(stats.minus_1sd, "ratio") == "7.98"
    assert rounded(stats.plus_1sd, "ratio") == "12.02"
    assert rounded(stats.plus_2sd, "ratio") == "14.03"
    assert stats.min == Decimal("8")
    assert stats.p25 == Decimal("8")
    assert stats.median == Decimal("10")
    assert stats.p75 == Decimal("12")
    assert stats.max == Decimal("12")
    assert stats.current == Decimal("12")
    assert rounded(stats.current_percentile, "pct") == "100.00"

    assert result.requested_start == FIRST_SESSION
    assert result.coverage_start == FIRST_SESSION
    assert result.is_partial_coverage is False

    assert len(result.series) == 12
    assert result.series[0] == (date(2026, 1, 9), Decimal("8"))
    assert result.series[-1] == (date(2026, 3, 27), Decimal("12"))


def test_inclusive_quartiles_interpolate_between_ranks() -> None:
    """Values 1, 2, …, 60 in ascending order (current 60).

    Hand-computed inclusive quartiles at positions (n − 1) × k ÷ 4 = 14.75, 29.5 and 44.25 of the
    zero-based sorted values: 15 + 0.75 = 15.75, 30 + 0.5 = 30.5 and 45 + 0.25 = 45.25. Mean 30.5;
    sample sd √(n(n + 1) ÷ 12) = √305 = 17.4642… → 17.46. Current 60 is the maximum: 100.00.
    """
    sessions = weekdays(FIRST_SESSION, 60)
    values = [Decimal(rank) for rank in range(1, 61)]

    stats = stats_of(
        valuation_bands(series(sessions, values), requested_start=FIRST_SESSION, sessions=sessions)
    )

    assert stats.min == Decimal("1")
    assert stats.p25 == Decimal("15.75")
    assert stats.median == Decimal("30.5")
    assert stats.p75 == Decimal("45.25")
    assert stats.max == Decimal("60")
    assert stats.mean == Decimal("30.5")
    assert rounded(stats.stdev, "ratio") == "17.46"
    assert stats.current == Decimal("60")
    assert rounded(stats.current_percentile, "pct") == "100.00"


@pytest.mark.parametrize(
    ("values", "current", "percentile"),
    [
        pytest.param(
            [Decimal("8")] * 20 + [Decimal("12")] * 20 + [Decimal("10")] * 20,
            Decimal("10"),
            "66.67",
            id="current_ties_middle_value",
        ),
        pytest.param(
            [Decimal("12")] * 30 + [Decimal("8")] * 30,
            Decimal("8"),
            "50.00",
            id="current_ties_minimum",
        ),
    ],
)
def test_current_percentile_counts_ties_at_or_below(
    values: list[Decimal], current: Decimal, percentile: str
) -> None:
    """The percentile counts every observation at or below the current value, ties included.

    Twenty each of 8, 12 and 10 (current 10): 40 of 60 are ≤ 10, 40 ÷ 60 × 100 = 66.666… → 66.67.
    Thirty 12s then thirty 8s (current 8, the minimum): 30 of 60 are ≤ 8 → 50.00.
    """
    sessions = weekdays(FIRST_SESSION, 60)

    stats = stats_of(
        valuation_bands(series(sessions, values), requested_start=FIRST_SESSION, sessions=sessions)
    )

    assert stats.current == current
    assert rounded(stats.current_percentile, "pct") == percentile


def test_fewer_than_sixty_values_is_insufficient_history() -> None:
    """59 non-null values among 64 sessions: two leading and three trailing ``None`` days.

    59 < 60, so the status and flag are ``insufficient_history`` and no statistics are computed,
    while the weekly series is still returned: 13 ISO weeks, the last dated on its final session,
    Thursday 2026-04-02, and carrying Monday 2026-03-30's value 8 (the 59th value; odd positions
    hold 8).
    Coverage starts on the first non-null day, Wednesday 2026-01-07, 2 days after the request.
    """
    sessions = weekdays(FIRST_SESSION, 64)
    values: list[Decimal | None] = [None, None, *alternating(59), None, None, None]

    result = valuation_bands(
        series(sessions, values), requested_start=FIRST_SESSION, sessions=sessions
    )

    assert result.status == "insufficient_history"
    assert result.flags == ["insufficient_history"]
    assert result.stats is None
    assert len(result.series) == 13
    assert result.series[-1] == (date(2026, 4, 2), Decimal("8"))
    assert result.coverage_start == date(2026, 1, 7)
    assert result.is_partial_coverage is False


@pytest.mark.parametrize(
    ("metric", "status"),
    [("pe", "no_positive_earnings"), ("pb", "no_positive_equity")],
)
def test_all_null_series_reports_the_metric_status(metric: BandMetric, status: str) -> None:
    """60 sessions on which the multiple is undefined every day (losses or non-positive equity).

    P/E reports ``no_positive_earnings`` and P/B ``no_positive_equity``, with that code as the only
    flag, no statistics, an empty series, no coverage start and therefore partial coverage.
    """
    sessions = weekdays(FIRST_SESSION, 60)

    result = valuation_bands(
        series(sessions, [None] * 60),
        requested_start=FIRST_SESSION,
        sessions=sessions,
        metric=metric,
    )

    assert result.status == status
    assert result.flags == [status]
    assert result.stats is None
    assert result.series == []
    assert result.coverage_start is None
    assert result.is_partial_coverage is True


@pytest.mark.parametrize("metric", ["pe", "pb"])
def test_empty_daily_series_is_insufficient_history(metric: BandMetric) -> None:
    """A lookback without any market session with a close: ``daily`` and ``sessions`` are empty.

    0 observations is fewer than 60, so both metrics report ``insufficient_history`` as the status
    and only flag, never the all-null ``no_positive_*`` status: no day exists on which the multiple
    could be undefined. There are no statistics, an empty series, no coverage start and therefore
    partial coverage, with the requested start echoed.
    """
    result = valuation_bands([], requested_start=FIRST_SESSION, sessions=[], metric=metric)

    assert result.status == "insufficient_history"
    assert result.flags == ["insufficient_history"]
    assert result.stats is None
    assert result.series == []
    assert result.requested_start == FIRST_SESSION
    assert result.coverage_start is None
    assert result.is_partial_coverage is True


def test_zero_standard_deviation_puts_every_band_on_the_mean() -> None:
    """60 identical values of 7.5: the sample sd is 0, so all four bands equal the mean 7.5.

    The quartiles, minimum, maximum and current value are all 7.5, every value is ≤ current
    (100.00), and nothing raises.
    """
    sessions = weekdays(FIRST_SESSION, 60)

    stats = stats_of(
        valuation_bands(
            series(sessions, [Decimal("7.5")] * 60),
            requested_start=FIRST_SESSION,
            sessions=sessions,
        )
    )

    assert stats.stdev == 0
    assert stats.mean == Decimal("7.5")
    for band in (stats.minus_2sd, stats.minus_1sd, stats.plus_1sd, stats.plus_2sd):
        assert band == Decimal("7.5")
        assert rounded(band, "ratio") == "7.50"
    assert stats.min == stats.p25 == stats.median == stats.p75 == stats.max == Decimal("7.5")
    assert stats.current == Decimal("7.5")
    assert rounded(stats.current_percentile, "pct") == "100.00"


def test_null_days_are_excluded_from_statistics_and_series() -> None:
    """70 sessions alternating 8 and 12 (current 12) with 8 ``None`` days, never zero-filled.

    ``None`` falls on Monday and Tuesday 2026-01-05/06 (8, 12), the whole week of 2026-01-12
    (12, 8, 12, 8, 12) and Friday 2026-01-23 (8): four 8s and four 12s removed, leaving 31 of
    each. Hand-computed: 62 observations, mean 10, minimum 8 (not 0), sd √(62 × 4 ÷ 61) =
    2.01632… → 2.02. The null week yields no point, so 14 weeks give 13 points; the week of
    2026-01-19 is dated on its Friday and carries Thursday 2026-01-22's value 12. Coverage starts on
    Wednesday 2026-01-07.
    """
    sessions = weekdays(FIRST_SESSION, 70)
    values: list[Decimal | None] = list(alternating(70))
    for index in (0, 1, 5, 6, 7, 8, 9, 14):
        values[index] = None

    result = valuation_bands(
        series(sessions, values), requested_start=FIRST_SESSION, sessions=sessions
    )

    stats = stats_of(result)
    assert result.status == "ok"
    assert stats.observations == 62
    assert stats.mean == Decimal("10")
    assert stats.min == Decimal("8")
    assert stats.max == Decimal("12")
    assert rounded(stats.stdev, "ratio") == "2.02"
    assert stats.current == Decimal("12")
    assert result.coverage_start == date(2026, 1, 7)

    assert len(result.series) == 13
    assert result.series[0] == (date(2026, 1, 9), Decimal("8"))
    assert result.series[1] == (date(2026, 1, 23), Decimal("12"))
    assert result.series[-1] == (date(2026, 4, 10), Decimal("12"))
    assert date(2026, 1, 16) not in [day for day, _ in result.series]


def test_weekly_series_is_dated_on_the_last_session_of_each_iso_week() -> None:
    """Sessions Monday 2026-02-02 to Friday 2026-02-20 without Friday 2026-02-13.

    Each day carries its day of the month as the value. One point per ISO week, on its last
    session: (2026-02-06, 6), (2026-02-12, 12) on the Thursday, and (2026-02-20, 20). The
    statistics use the 14 daily values, not the 3 points, and 14 < 60 is insufficient history.
    """
    sessions = [day for day in weekdays(date(2026, 2, 2), 15) if day != date(2026, 2, 13)]

    result = valuation_bands(
        by_day_of_month(sessions), requested_start=date(2026, 2, 2), sessions=sessions
    )

    assert result.series == [
        (date(2026, 2, 6), Decimal("6")),
        (date(2026, 2, 12), Decimal("12")),
        (date(2026, 2, 20), Decimal("20")),
    ]
    assert result.status == "insufficient_history"
    assert result.stats is None


def test_weekly_series_ignores_values_outside_the_session_weeks() -> None:
    """Daily values on days the sessions do not reach are never sampled, whatever the order.

    The daily series runs Monday 2026-02-02 to Monday 2026-02-23 (value = day of the month); the
    sessions are the same weekdays without Friday 2026-02-13 and Monday 2026-02-23, passed in
    descending order. The week of 2026-02-09 still ends on Thursday the 12th, so the 13th's value
    is after it and is not sampled; the week of 2026-02-23 has no session and yields no point. The
    points are (2026-02-06, 6), (2026-02-12, 12) and (2026-02-20, 20).
    """
    sessions = sorted(
        (day for day in weekdays(date(2026, 2, 2), 15) if day != date(2026, 2, 13)),
        reverse=True,
    )

    result = valuation_bands(
        by_day_of_month(weekdays(date(2026, 2, 2), 16)),
        requested_start=date(2026, 2, 2),
        sessions=sessions,
    )

    assert result.series == [
        (date(2026, 2, 6), Decimal("6")),
        (date(2026, 2, 12), Decimal("12")),
        (date(2026, 2, 20), Decimal("20")),
    ]


def test_iso_week_across_the_year_boundary_yields_one_point() -> None:
    """Monday 2025-12-29 to Friday 2026-01-02 is the single ISO week 2026-W01.

    With the day of the month as the value, the one point is (2026-01-02, 2).
    """
    sessions = weekdays(date(2025, 12, 29), 5)

    result = valuation_bands(
        by_day_of_month(sessions), requested_start=date(2025, 12, 29), sessions=sessions
    )

    assert result.series == [(date(2026, 1, 2), Decimal("2"))]


@pytest.mark.parametrize(
    ("days_before_first_session", "is_partial"),
    [pytest.param(8, True, id="8_days_partial"), pytest.param(7, False, id="7_days_full")],
)
def test_partial_coverage_starts_beyond_seven_days(
    days_before_first_session: int, is_partial: bool
) -> None:
    """The reference case requested 8 or 7 calendar days before its first value, 2026-01-05.

    8 days is more than 7, so coverage is partial; 7 days is not. Either way the result echoes the
    requested start, coverage starts on 2026-01-05 and the 60 values still give ``ok`` statistics.
    """
    requested_start = FIRST_SESSION - timedelta(days=days_before_first_session)

    result = reference_bands(requested_start)

    assert result.is_partial_coverage is is_partial
    assert result.requested_start == requested_start
    assert result.coverage_start == FIRST_SESSION
    assert result.status == "ok"
    assert stats_of(result).observations == 60


@pytest.mark.parametrize(
    "daily",
    [
        pytest.param(
            [(date(2026, 1, 5), Decimal("8")), (date(2026, 1, 5), Decimal("12"))],
            id="repeated_date",
        ),
        pytest.param(
            [(date(2026, 1, 6), Decimal("8")), (date(2026, 1, 5), Decimal("12"))],
            id="descending_dates",
        ),
    ],
)
def test_daily_series_must_be_strictly_ascending(
    daily: list[tuple[date, Decimal | None]],
) -> None:
    """A repeated or out-of-order session date is a caller error and raises ``ValueError``."""
    with pytest.raises(ValueError, match="strictly ascending"):
        valuation_bands(daily, requested_start=date(2026, 1, 5), sessions=[date(2026, 1, 5)])


def test_unknown_metric_raises() -> None:
    """Only ``pe`` and ``pb`` are banded; any other metric raises ``ValueError``."""
    with pytest.raises(ValueError, match="unknown band metric"):
        valuation_bands(
            [(date(2026, 1, 5), Decimal("8"))],
            requested_start=date(2026, 1, 5),
            sessions=[date(2026, 1, 5)],
            metric="ev_ebitda",  # type: ignore[arg-type]
        )


LONG_COEFFICIENT = Decimal("1." + "1" * 200_000)
"""1.111…1: finite, adjusted exponent 0, with 200,001 significant digits."""

SIXTY_DIGITS = Decimal("1." + "1" * 59)
"""1.111…1 with exactly 60 significant digits, the most the statistics accept."""

SIXTY_DIGITS_AT_EXPONENT_40 = Decimal("9." + "9" * 59 + "E+40")
"""9.99…9E+40: 60 significant digits and adjusted exponent 40, both bounds at once."""


def reference_with(
    position: int, value: Decimal
) -> tuple[list[date], list[tuple[date, Decimal | None]]]:
    """Return 61 sessions from 2026-01-05 alternating 8 and 12, with ``value`` at ``position``."""
    sessions = weekdays(FIRST_SESSION, 61)
    values: list[Decimal | None] = list(alternating(61))
    values[position] = value
    return sessions, series(sessions, values)


@pytest.mark.parametrize("position", [30, 60], ids=["mid_series", "current"])
@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        pytest.param(Decimal("NaN"), ValueError, "is not finite: NaN", id="nan"),
        pytest.param(Decimal("sNaN"), ValueError, "is not finite: sNaN", id="snan"),
        pytest.param(Decimal("Infinity"), ValueError, r"is not finite: \+Infinity", id="inf"),
        pytest.param(Decimal("-Infinity"), ValueError, "is not finite: -Infinity", id="-inf"),
        pytest.param(Decimal("1E+999999"), ValueError, "adjusted exponent 999999,", id="1E+999999"),
        pytest.param(
            Decimal("-1E+999999"), ValueError, "adjusted exponent 999999,", id="-1E+999999"
        ),
        pytest.param(
            Decimal("1E-999999"), ValueError, "adjusted exponent -999999,", id="1E-999999"
        ),
        pytest.param(Decimal("1E+41"), ValueError, "adjusted exponent 41,", id="1E+41"),
        pytest.param(Decimal("-1E-41"), ValueError, "adjusted exponent -41,", id="-1E-41"),
        pytest.param(
            LONG_COEFFICIENT, ValueError, "has 200001 significant digits", id="200001_digits"
        ),
        pytest.param(
            Decimal("1." + "1" * 60), ValueError, "has 61 significant digits", id="61_digits"
        ),
        pytest.param(8.0, TypeError, "must be Decimal, got float", id="float"),
    ],
)
def test_unusable_daily_value_raises_before_any_statistic(
    monkeypatch: pytest.MonkeyPatch,
    position: int,
    value: Decimal,
    error: type[Exception],
    message: str,
) -> None:
    """A value the statistics cannot take is rejected before any of them runs.

    The reference series of 61 values alternating 8 and 12 carries the value mid-series (index
    30) or as the current value (index 60). Without the check NaN and ±Infinity crashed inside
    ``statistics.stdev`` with ``AttributeError``, sNaN with ``InvalidOperation``, and
    ±1E+999999 or 1E-999999 ran for about 70 s, because ``statistics.mean`` and ``stdev`` turn
    each value into an exact integer ratio of about a million digits; the 200,001-digit
    coefficient took about 8.5 s. Non-zero values with an adjusted exponent beyond ±40 or more
    than 60 significant digits are rejected, and a float is not a ``Decimal``. The statistics
    functions are replaced by one that fails the test, so a missing check fails at once instead of
    stalling. The message names the day and stays short, never listing the value's digits.
    """

    def forbidden(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("a statistic was computed over an unusable value")

    for name in ("mean", "stdev", "quantiles"):
        monkeypatch.setattr(statistics, name, forbidden)
    sessions, daily = reference_with(position, value)

    with pytest.raises(error, match=message) as raised:
        valuation_bands(daily, requested_start=FIRST_SESSION, sessions=sessions)

    assert str(raised.value).startswith(f"daily value on {sessions[position].isoformat()} ")
    assert len(str(raised.value)) < 100


@pytest.mark.parametrize(
    ("value", "minimum", "maximum"),
    [
        pytest.param(Decimal("1E+40"), Decimal("8"), Decimal("1E+40"), id="1E+40"),
        pytest.param(Decimal("-1E-40"), Decimal("-1E-40"), Decimal("12"), id="-1E-40"),
        pytest.param(SIXTY_DIGITS, SIXTY_DIGITS, Decimal("12"), id="60_digits"),
        pytest.param(
            SIXTY_DIGITS_AT_EXPONENT_40,
            Decimal("8"),
            SIXTY_DIGITS_AT_EXPONENT_40,
            id="60_digits_at_exponent_40",
        ),
        pytest.param(Decimal("0E-1000000"), Decimal("0"), Decimal("12"), id="0E-1000000"),
        pytest.param(Decimal("-0E+999999"), Decimal("0"), Decimal("12"), id="-0E+999999"),
    ],
)
def test_values_within_the_statistics_bounds_are_accepted(
    value: Decimal, minimum: Decimal, maximum: Decimal
) -> None:
    """Values at the bounds, and a zero of any exponent, enter the statistics like any other.

    The reference series of 61 values alternating 8 and 12 carries the value at index 30, so the
    statistics are ``ok`` over 61 observations and the value is the minimum or the maximum: an
    adjusted exponent of exactly ±40, exactly 60 significant digits, both at once, or a zero whose
    exponent is far beyond the bound (its exact ratio is 0/1).
    """
    sessions, daily = reference_with(30, value)

    result = valuation_bands(daily, requested_start=FIRST_SESSION, sessions=sessions)

    assert result.status == "ok"
    stats = stats_of(result)
    assert stats.observations == 61
    assert stats.min == minimum
    assert stats.max == maximum
