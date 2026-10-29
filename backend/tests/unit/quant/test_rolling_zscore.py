"""Rolling volume and turnover Z-scores, and the single spike rule.

For one ticker's metric on day ``t``: ``z = (x_t - mean(W)) / sd(W)``, where ``W`` is up to ``N``
of the ticker's own rows strictly before ``t`` taken from the previous ``2N`` market sessions and
``sd`` is the sample standard deviation (n - 1). A session without a row for the ticker is skipped,
never zero-filled; a row whose value is 0 is a real observation. A spike is ``z >= threshold`` on
the positive side only.

Every expected value is a literal computed by hand in the test's docstring; nothing here
recomputes a mean or standard deviation. Values enter as exact ``Decimal`` (volumes as
``Decimal(int)``) and Z-scores are compared after :func:`quantize_out` with kind ``"z"``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal

import pytest

from helpers.sessions import weekdays
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.quant.zscore import (
    DEFAULT_SPIKE_THRESHOLD,
    DEFAULT_Z_WINDOW,
    MAX_Z_WINDOW,
    MIN_Z_WINDOW,
    Observation,
    ZScorePoint,
    default_min_periods,
    is_spike,
    rolling_zscores,
)

THRESHOLD = Decimal("2.00")
FIRST_MONDAY = date(2026, 1, 5)
# Twenty prior volumes alternating 100 and 300: mean 200, squared deviations 20 x 100^2.
REFERENCE_HISTORY: list[int] = [100, 300] * 10


def obs(dates: Sequence[date], values: Sequence[int | Decimal]) -> list[Observation]:
    """Pair each date with its value; integer volumes enter as exact ``Decimal(int)``."""
    return [Observation(day, Decimal(value)) for day, value in zip(dates, values, strict=True)]


def point_on(points: Sequence[ZScorePoint], day: date) -> ZScorePoint | None:
    """Return the point dated ``day``, or ``None`` when no point carries that date."""
    return next((point for point in points if point.trade_date == day), None)


def score_on(
    rows: Sequence[Observation],
    sessions: Sequence[date],
    day: date,
    *,
    window: int = DEFAULT_Z_WINDOW,
    min_periods: int | None = None,
) -> ZScorePoint:
    """Run :func:`rolling_zscores` and return the point dated ``day``, which must exist."""
    point = point_on(rolling_zscores(rows, sessions, window=window, min_periods=min_periods), day)
    assert point is not None, f"no point dated {day}"
    return point


def reference_point(current: int) -> ZScorePoint:
    """Score ``current`` on the 21st of 21 sessions after the full reference history."""
    sessions = weekdays(FIRST_MONDAY, 21)
    return score_on(obs(sessions, [*REFERENCE_HISTORY, current]), sessions, sessions[-1])


def test_published_defaults() -> None:
    """The 20-row window, its 5-250 range and the 2.00 spike threshold of the API defaults."""
    assert DEFAULT_Z_WINDOW == 20
    assert MIN_Z_WINDOW == 5
    assert MAX_Z_WINDOW == 250
    assert DEFAULT_SPIKE_THRESHOLD == Decimal("2.00")


@pytest.mark.parametrize(
    ("window", "expected"),
    [(20, 15), (5, 4), (250, 188)],
    ids=["ceil(15.00)=15", "ceil(3.75)=4", "ceil(187.50)=188"],
)
def test_default_min_periods_is_ceil_of_three_quarters_of_window(
    window: int, expected: int
) -> None:
    """``min_periods = ceil(0.75 x N)``: 0.75 x 20 = 15; 0.75 x 5 = 3.75; 0.75 x 250 = 187.5."""
    assert default_min_periods(window) == expected


def test_reference_spike_matches_hand_computation() -> None:
    """Prior 100, 300, ... (20 rows), current 500.

    mean = (10 x 100 + 10 x 300) / 20 = 200; squared deviations 20 x 100^2 = 200000;
    sd = sqrt(200000 / 19) = 102.598...; z = (500 - 200) / 102.598 = 2.924... -> 2.92.
    """
    point = reference_point(500)

    assert point.value == Decimal(500)
    assert point.rolling_mean == Decimal(200)
    assert point.rolling_stdev is not None
    assert str(point.rolling_stdev.quantize(Decimal("0.001"))) == "102.598"
    assert str(quantize_out(point.z, "z")) == "2.92"
    assert point.observations_used == 20
    assert point.flags == []
    assert is_spike(point.z, THRESHOLD) is True


def test_current_day_is_excluded_from_its_own_window() -> None:
    """The reference history with current 5000 keeps mean 200 and sd 102.598.

    Including the current day would give mean (4000 + 5000) / 21 = 428.57; excluded, z is
    (5000 - 200) / 102.598 = 46.784... -> 46.78, so the spike does not dampen itself.
    """
    point = reference_point(5000)

    assert point.rolling_mean == Decimal(200)
    assert point.observations_used == 20
    assert str(quantize_out(point.z, "z")) == "46.78"


def test_missing_volume_days_are_skipped_not_zero_filled() -> None:
    """No row on 2 of the 20 prior sessions; the 18 present rows are 9 x 100 and 9 x 300.

    Skipped: mean = (900 + 2700) / 18 = 200; sd = sqrt(18 x 100^2 / 17) = 100 x sqrt(18/17)
    = 102.899...; z = 300 / 102.899 = 2.9155... -> 2.92. Zero-filling the two gaps would give
    mean 3600 / 20 = 180 over 20 observations instead.
    """
    sessions = weekdays(FIRST_MONDAY, 21)
    gaps = {4, 11}  # one 100-day and one 300-day of the alternating history
    present = [index for index in range(20) if index not in gaps]
    rows = obs(
        [*(sessions[index] for index in present), sessions[20]],
        [*(REFERENCE_HISTORY[index] for index in present), 500],
    )

    points = rolling_zscores(rows, sessions)
    point = point_on(points, sessions[20])

    assert point is not None
    assert point.observations_used == 18
    assert point.rolling_mean == Decimal(200)
    assert point.rolling_mean != Decimal(180)
    assert str(quantize_out(point.z, "z")) == "2.92"
    assert point_on(points, sessions[4]) is None
    assert point_on(points, sessions[11]) is None


def test_explicit_zero_volume_row_is_an_observation() -> None:
    """Prior rows 19 x 100 and one recorded 0; current 100.

    mean = 1900 / 20 = 95; squared deviations 19 x 5^2 + 95^2 = 475 + 9025 = 9500;
    sd = sqrt(9500 / 19) = sqrt(500) = 22.3607...; z = (100 - 95) / 22.3607 = 0.2236... -> 0.22.
    """
    sessions = weekdays(FIRST_MONDAY, 21)
    history = [100] * 20
    history[7] = 0
    point = score_on(obs(sessions, [*history, 100]), sessions, sessions[20])

    assert point.observations_used == 20
    assert point.rolling_mean == Decimal(95)
    assert point.rolling_stdev is not None
    assert str(point.rolling_stdev.quantize(Decimal("0.0001"))) == "22.3607"
    assert str(quantize_out(point.z, "z")) == "0.22"


def _sparse_history() -> tuple[list[date], list[Observation]]:
    """Return 21 sessions, none older, and rows on 14 of the 20 history sessions plus t.

    The 14 history rows alternate 100 and 300 (7 of each); the row on t is 500.
    """
    sessions = weekdays(FIRST_MONDAY, 21)
    gaps = {1, 5, 8, 12, 15, 18}
    present = [sessions[index] for index in range(20) if index not in gaps]
    return sessions, obs([*present, sessions[20]], [*([100, 300] * 7), 500])


def test_fewer_than_min_periods_observations_gives_null_z() -> None:
    """14 observations against the default min_periods of ceil(0.75 x 20) = 15.

    z, the rolling mean and the rolling sd are all null with ``insufficient_observations``.
    """
    sessions, rows = _sparse_history()
    point = score_on(rows, sessions, sessions[20])

    assert point.observations_used == 14
    assert point.z is None
    assert point.rolling_mean is None
    assert point.rolling_stdev is None
    assert point.flags == ["insufficient_observations"]
    assert is_spike(point.z, THRESHOLD) is False


def test_explicit_min_periods_scores_a_sparse_window() -> None:
    """The same 14 observations with ``min_periods=10``.

    mean = (700 + 2100) / 14 = 200; sd = sqrt(14 x 100^2 / 13) = 103.774...;
    z = 300 / 103.774 = 2.8908... -> 2.89.
    """
    sessions, rows = _sparse_history()
    point = score_on(rows, sessions, sessions[20], min_periods=10)

    assert point.observations_used == 14
    assert point.rolling_mean == Decimal(200)
    assert point.z is not None
    assert str(quantize_out(point.z, "z")) == "2.89"
    assert point.flags == []


def test_window_is_capped_at_two_n_market_sessions() -> None:
    """60 sessions with t the last; N = 20, so W draws on sessions 19-58, the 40 before t.

    Rows: 10 old rows of 1000 on sessions 9-18 (the latest just outside the span) and 14 rows
    alternating 100/300 on sessions 19-32 (the earliest on the span's edge), then 500 on t.
    Capped, |W| = 14 < 15: insufficient. Without the cap the N-row rule would take 20 rows.
    With ``min_periods=10`` the mean is (700 + 2100) / 14 = 200, so no 1000 entered W.
    """
    sessions = weekdays(FIRST_MONDAY, 60)
    rows = obs(
        [*sessions[9:19], *sessions[19:33], sessions[59]],
        [*([1000] * 10), *([100, 300] * 7), 500],
    )

    capped = score_on(rows, sessions, sessions[59])
    assert capped.observations_used == 14
    assert capped.z is None
    assert capped.flags == ["insufficient_observations"]

    relaxed = score_on(rows, sessions, sessions[59], min_periods=10)
    assert relaxed.observations_used == 14
    assert relaxed.rolling_mean == Decimal(200)


def test_window_keeps_only_the_most_recent_n_rows() -> None:
    """30 rows inside the 40 sessions before t (session 40) keep only the latest N = 20.

    Rows: 10 x 1000 on sessions 10-19, the reference history on sessions 20-39, 500 on t.
    W is the reference history: mean 200 (not (10000 + 4000) / 30 = 466.67),
    sd 102.598, z = 300 / 102.598 = 2.924... -> 2.92.
    """
    sessions = weekdays(FIRST_MONDAY, 41)
    rows = obs(sessions[10:41], [*([1000] * 10), *REFERENCE_HISTORY, 500])

    point = score_on(rows, sessions, sessions[40])

    assert point.observations_used == 20
    assert point.rolling_mean == Decimal(200)
    assert str(quantize_out(point.z, "z")) == "2.92"


def test_rows_before_the_first_session_never_enter_a_window() -> None:
    """Two rows of 1000 dated before the calendar starts, then 0, 4, 4, 4 and 7 on 5 sessions.

    The first in-calendar row has an empty window (|W| = 0, insufficient). For the last,
    window N = 5 and W = 0, 4, 4, 4: mean 12 / 4 = 3; squared deviations 9 + 1 + 1 + 1 = 12;
    sd = sqrt(12 / 3) = 2; z = (7 - 3) / 2 = 2.
    """
    days = weekdays(FIRST_MONDAY, 7)
    sessions = days[2:]
    rows = obs(days, [1000, 1000, 0, 4, 4, 4, 7])

    points = rolling_zscores(rows, sessions, window=5)
    first_in_calendar = point_on(points, sessions[0])
    last = point_on(points, sessions[4])

    assert first_in_calendar is not None
    assert first_in_calendar.observations_used == 0
    assert first_in_calendar.flags == ["insufficient_observations"]
    assert last is not None
    assert last.observations_used == 4
    assert last.rolling_mean == Decimal(3)
    assert last.rolling_stdev == Decimal(2)
    assert last.z == Decimal(2)


def test_zero_variance_gives_null_z_with_flag() -> None:
    """Prior rows 20 x 100, current 150: mean 100, sd 0, so z = 50 / 0 is undefined.

    The mean and the zero sd are still returned; z is null with ``zero_variance``.
    """
    sessions = weekdays(FIRST_MONDAY, 21)
    point = score_on(obs(sessions, [*([100] * 20), 150]), sessions, sessions[20])

    assert point.observations_used == 20
    assert point.rolling_mean == Decimal(100)
    assert point.rolling_stdev == Decimal(0)
    assert point.z is None
    assert point.flags == ["zero_variance"]
    assert is_spike(point.z, THRESHOLD) is False


def test_negative_z_is_returned_but_never_a_spike() -> None:
    """The reference history with current 0: z = (0 - 200) / 102.598 = -1.9493... -> -1.95.

    The value is reported, not nulled, and the positive-side rule never classifies it.
    """
    point = reference_point(0)

    assert point.z is not None
    assert str(quantize_out(point.z, "z")) == "-1.95"
    assert point.flags == []
    assert is_spike(point.z, THRESHOLD) is False


def test_z_exactly_at_threshold_is_a_spike() -> None:
    """window = 5; prior rows 0, 4, 0, 4, 2 and current 6 on 6 sessions.

    mean = 10 / 5 = 2; squared deviations 4 + 4 + 4 + 4 + 0 = 16; sd = sqrt(16 / 4) = 2;
    z = (6 - 2) / 2 = 2 exactly, which meets the 2.00 threshold.
    """
    sessions = weekdays(FIRST_MONDAY, 6)
    point = score_on(obs(sessions, [0, 4, 0, 4, 2, 6]), sessions, sessions[5], window=5)

    assert point.observations_used == 5
    assert point.rolling_mean == Decimal(2)
    assert point.rolling_stdev == Decimal(2)
    assert point.z == Decimal(2)
    assert is_spike(point.z, THRESHOLD) is True


@pytest.mark.parametrize(
    ("z", "threshold", "expected"),
    [
        pytest.param(Decimal("2.00"), THRESHOLD, True, id="at-threshold"),
        pytest.param(Decimal("2.01"), THRESHOLD, True, id="above-threshold"),
        pytest.param(Decimal("1.99"), THRESHOLD, False, id="below-threshold"),
        pytest.param(Decimal("-2.00"), THRESHOLD, False, id="negative-mirror"),
        pytest.param(Decimal("-9.50"), THRESHOLD, False, id="large-negative"),
        pytest.param(None, THRESHOLD, False, id="undefined-z"),
        pytest.param(Decimal("0.50"), Decimal("0.5"), True, id="lowest-api-threshold"),
    ],
)
def test_spike_rule_is_z_at_or_above_threshold_on_the_positive_side(
    z: Decimal | None, threshold: Decimal, expected: bool
) -> None:
    """One rule for the screener, its ``anomalies_only`` filter and the anomaly feed."""
    assert is_spike(z, threshold) is expected


def test_spike_rule_defaults_to_the_published_threshold() -> None:
    """Without a threshold, 2.00 applies: 2 is a spike and 1.999 is not."""
    assert is_spike(Decimal(2)) is True
    assert is_spike(Decimal("1.999")) is False


def test_session_without_a_row_produces_no_point() -> None:
    """The calendar includes t, but the ticker has no row on t: no point is dated t.

    The output has exactly one point per row, in row order.
    """
    sessions = weekdays(FIRST_MONDAY, 21)
    rows = obs(sessions[:20], REFERENCE_HISTORY)

    points = rolling_zscores(rows, sessions)

    assert point_on(points, sessions[20]) is None
    assert [point.trade_date for point in points] == sessions[:20]


def test_turnover_values_keep_exact_decimal_arithmetic() -> None:
    """Turnover in KES, window = 5: prior 1000.50, 1002.50, 1000.50, 1002.50, 1001.50, then 1004.00.

    mean = 5007.50 / 5 = 1001.50 KES; deviations -1, 1, -1, 1, 0 give squared sum 4;
    sd = sqrt(4 / 4) = 1.00 KES; z = (1004.00 - 1001.50) / 1 = 2.5 exactly.
    """
    sessions = weekdays(FIRST_MONDAY, 6)
    turnover = [Decimal(text) for text in ("1000.50", "1002.50", "1000.50", "1002.50", "1001.50")]
    point = score_on(
        obs(sessions, [*turnover, Decimal("1004.00")]), sessions, sessions[5], window=5
    )

    assert point.value == Decimal("1004.00")
    assert point.rolling_mean == Decimal("1001.50")
    assert str(quantize_out(point.rolling_mean, "money")) == "1001.50"
    assert point.rolling_stdev == Decimal(1)
    assert point.z == Decimal("2.5")
    assert str(quantize_out(point.z, "z")) == "2.50"


@pytest.mark.parametrize("window", [MIN_Z_WINDOW - 1, MAX_Z_WINDOW + 1])
def test_default_min_periods_rejects_window_outside_range(window: int) -> None:
    """Windows outside 5-250 have no default minimum."""
    with pytest.raises(ValueError, match="window must be between 5 and 250"):
        default_min_periods(window)


_DAYS = weekdays(FIRST_MONDAY, 3)
_ROWS = obs(_DAYS, [1, 2, 3])


@pytest.mark.parametrize(
    ("rows", "sessions", "window", "min_periods", "error", "message"),
    [
        pytest.param(_ROWS, _DAYS, 4, None, ValueError, "window must be", id="window-below-5"),
        pytest.param(_ROWS, _DAYS, 251, None, ValueError, "window must be", id="window-above-250"),
        pytest.param(_ROWS, _DAYS, 5, 1, ValueError, "min_periods", id="min-periods-below-2"),
        pytest.param(_ROWS, _DAYS, 5, 6, ValueError, "min_periods", id="min-periods-above-window"),
        pytest.param(
            _ROWS[::-1], _DAYS, 5, None, ValueError, "ascending by date", id="rows-descending"
        ),
        pytest.param(
            [_ROWS[0], _ROWS[0]], _DAYS, 5, None, ValueError, "ascending by date", id="rows-repeat"
        ),
        pytest.param(
            [(_DAYS[0], 100)], _DAYS, 5, None, TypeError, "must be Decimal", id="int-volume"
        ),
        pytest.param(
            [Observation(_DAYS[0], Decimal("NaN"))],
            _DAYS,
            5,
            None,
            ValueError,
            "not finite",
            id="nan-value",
        ),
        pytest.param(
            obs(_DAYS, [1, Decimal("Infinity"), 3]),
            _DAYS,
            5,
            2,
            ValueError,
            "not finite",
            id="infinity-in-window",
        ),
        pytest.param(
            obs(_DAYS, [1, Decimal("sNaN"), 3]),
            _DAYS,
            5,
            2,
            ValueError,
            "not finite",
            id="snan-in-window",
        ),
        pytest.param(
            obs(_DAYS, [1, Decimal("1E+999999"), 3]),
            _DAYS,
            5,
            2,
            ValueError,
            "adjusted exponent 999999,",
            id="huge-exponent-in-window",
        ),
        pytest.param(
            obs(_DAYS, [1, Decimal("1." + "1" * 200_000), 3]),
            _DAYS,
            5,
            2,
            ValueError,
            "has 200001 significant digits",
            id="200001-digits-in-window",
        ),
        pytest.param(
            obs(_DAYS, [1, 2, Decimal("1E+999999")]),
            _DAYS,
            5,
            2,
            ValueError,
            "adjusted exponent 999999,",
            id="huge-exponent-current-row",
        ),
        pytest.param(
            _ROWS, _DAYS[::-1], 5, None, ValueError, "sessions must be", id="sessions-descending"
        ),
        pytest.param(
            _ROWS,
            [_DAYS[0], _DAYS[0]],
            5,
            None,
            ValueError,
            "sessions must be",
            id="sessions-repeat",
        ),
    ],
)
def test_invalid_arguments_raise(
    rows: Sequence[tuple[date, Decimal]],
    sessions: Sequence[date],
    window: int,
    min_periods: int | None,
    error: type[Exception],
    message: str,
) -> None:
    """Out-of-range windows, unordered input and non-``Decimal`` or unusable values raise.

    An integer volume is rejected because it would turn the mean into a non-``Decimal``. With
    ``min_periods=2`` the third row is scored over a window holding the second, so an Infinity,
    sNaN, 1E+999999 or 200,001-digit value there, or 1E+999999 as the scored row itself, must be
    rejected before ``statistics.mean`` turns it into an exact integer ratio: unchecked, 1E+999999
    in the window ran for about 70 s and as the scored row gave a finite z near 1E+999999.
    """
    with pytest.raises(error, match=message):
        rolling_zscores(rows, sessions, window=window, min_periods=min_periods)
