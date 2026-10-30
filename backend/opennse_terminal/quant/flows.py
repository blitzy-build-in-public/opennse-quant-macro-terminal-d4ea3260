"""Foreign versus local net capital flow estimator for a subject set of NSE counters.

The subject set is one ticker, one sector or the whole market. The service passes that set's
``terminal_daily_prices`` rows and the observed market sessions of the requested range, and
resolves the range defaults itself (``end = as_of``, ``start = end - 365 days``).

For each market session *t*:

* ``foreign_net_kes_t`` = sum of ``foreign_net_flow_kes`` over the constituents whose value is
  non-null that day;
* ``local_net_kes_t`` = -``foreign_net_kes_t``. Every share bought is sold by someone, so in
  aggregate foreign net buying equals local net selling: the domestic investors' net position is
  the opposite of the foreign investors' (the zero-sum identity);
* ``turnover_kes_t`` = sum of ``turnover_kes`` over the same constituents;
* ``foreign_net_pct_of_turnover_t`` = ``foreign_net_kes_t`` / ``turnover_kes_t`` x 100.

Per-ticker foreign flow is not published on the public NSE pages, so the flow column is nullable
and the estimate is only as complete as ``coverage_pct``, the share of market sessions in the range
that carry flow data. Amounts are KES and percentages are percentage points, both unrounded:
services quantize them with ``"money"`` and ``"pct"``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from itertools import pairwise
from typing import Final, Literal

from opennse_terminal.quant.asof import PriceRow
from opennse_terminal.quant.numeric import HUNDRED, ZERO, safe_div

__all__ = [
    "DEFAULT_FLOW_ROLLING_WINDOW",
    "MAX_FLOW_WINDOW",
    "MIN_FLOW_WINDOW",
    "FlowPointResult",
    "FlowSeriesResult",
    "FlowStatus",
    "FlowTotalsResult",
    "net_flow_series",
]

DEFAULT_FLOW_ROLLING_WINDOW: Final = 20
"""Market sessions summed by ``rolling_foreign_net_kes`` unless the caller overrides it."""

MIN_FLOW_WINDOW: Final = 5
"""Smallest accepted rolling window, in market sessions."""

MAX_FLOW_WINDOW: Final = 250
"""Largest accepted rolling window, in market sessions."""

FlowStatus = Literal["ok", "no_flow_data"]
"""``no_flow_data`` when no market session in the range carries flow data, otherwise ``ok``."""


@dataclass(frozen=True, slots=True)
class FlowPointResult:
    """Net flows of the subject set on one market session.

    Field names match ``schemas.market.FlowPoint``.

    Attributes:
        trade_date: The market session.
        foreign_net_kes: Foreign investors' net purchases in KES, summed over the constituents
            with flow data; ``None`` when no constituent has flow data that day.
        local_net_kes: Local investors' net purchases in KES, ``-foreign_net_kes``; ``None``
            with it.
        cumulative_foreign_net_kes: Running sum of ``foreign_net_kes`` in KES from the first
            session of the range; days without flow data add nothing.
        rolling_foreign_net_kes: Sum in KES of the non-null ``foreign_net_kes`` over the last
            ``rolling_window`` sessions including this one; ``None`` until the series holds a
            full window, and when no session in the window has flow data.
        turnover_kes: Turnover in KES of the constituents with flow data; ``None`` with
            ``foreign_net_kes``.
        foreign_net_pct_of_turnover: ``foreign_net_kes`` as a percentage of ``turnover_kes``;
            ``None`` when there is no flow data or that turnover is zero.
    """

    trade_date: date
    foreign_net_kes: Decimal | None
    local_net_kes: Decimal | None
    cumulative_foreign_net_kes: Decimal
    rolling_foreign_net_kes: Decimal | None
    turnover_kes: Decimal | None
    foreign_net_pct_of_turnover: Decimal | None


@dataclass(frozen=True, slots=True)
class FlowTotalsResult:
    """Net flows of the subject set over the whole range.

    Field names match ``schemas.market.FlowTotals``.

    Attributes:
        foreign_net_kes: Sum in KES of the daily ``foreign_net_kes`` over sessions with data.
        local_net_kes: ``-foreign_net_kes``, in KES.
        foreign_net_pct_of_turnover: ``foreign_net_kes`` as a percentage of the summed daily
            ``turnover_kes`` of the sessions with data; ``None`` when that turnover is zero.
        sessions_with_data: Market sessions whose ``foreign_net_kes`` is non-null.
        sessions_in_range: Market sessions in the range.
    """

    foreign_net_kes: Decimal
    local_net_kes: Decimal
    foreign_net_pct_of_turnover: Decimal | None
    sessions_with_data: int
    sessions_in_range: int


@dataclass(frozen=True, slots=True)
class FlowSeriesResult:
    """Result of :func:`net_flow_series`.

    Attributes:
        status: ``no_flow_data`` when no session in the range has flow data, otherwise ``ok``.
        flags: ``["no_flow_data"]`` with that status, otherwise empty.
        coverage_pct: Sessions with flow data as a percentage of the sessions in the range;
            zero when the range has no session.
        totals: Range totals, or ``None`` with ``no_flow_data``.
        series: One point per market session, ascending, returned with either status.
    """

    status: FlowStatus
    flags: list[str]
    coverage_pct: Decimal
    totals: FlowTotalsResult | None
    series: list[FlowPointResult]


def net_flow_series(
    rows: Sequence[PriceRow],
    sessions: Sequence[date],
    *,
    rolling_window: int = DEFAULT_FLOW_ROLLING_WINDOW,
) -> FlowSeriesResult:
    """Estimate daily foreign and local net flows of a subject set over a range of sessions.

    Formulas, for each market session *t* in ``sessions`` (KES unless stated):

    * ``foreign_net_kes_t`` = sum of ``foreign_net_flow_kes`` over the constituents whose value
      is non-null on *t*, or ``None`` when every constituent is null or absent;
    * ``local_net_kes_t`` = -``foreign_net_kes_t``, by the zero-sum identity: aggregate foreign
      net buying is matched by local net selling;
    * ``turnover_kes_t`` = sum of ``turnover_kes`` over those same constituents;
    * ``foreign_net_pct_of_turnover_t`` = ``foreign_net_kes_t`` / ``turnover_kes_t`` x 100 (%),
      ``None`` when that turnover is zero;
    * ``cumulative_foreign_net_kes_t`` = running sum of ``foreign_net_kes`` from the first
      session, where a null day adds nothing;
    * ``rolling_foreign_net_kes_t`` = sum of the non-null ``foreign_net_kes`` over the last
      ``rolling_window`` sessions including *t*.

    ``coverage_pct`` = sessions with flow data / sessions in range x 100 (%). The totals sum the
    daily values of the sessions with data, and their percentage of turnover divides by the
    summed turnover of those sessions. The estimate is only as complete as ``coverage_pct``:
    sessions and constituents without published flow are left out, never treated as zero.

    Args:
        rows: The subject set's price rows in the range, in any order. Every row's flow and
            turnover must be in one currency, KES, because they are summed as they are: the
            service passes the rows of KES-listed constituents only. Rows dated on a day that
            is not in ``sessions`` are ignored, as are rows whose ``foreign_net_flow_kes`` is
            ``None`` (their turnover is left out with them).
        sessions: The observed market sessions of the range, strictly ascending.
        rolling_window: Sessions in the rolling sum, from ``MIN_FLOW_WINDOW`` to
            ``MAX_FLOW_WINDOW``.

    Returns:
        The daily series with coverage and range totals. With no flow data on any session, the
        status is ``no_flow_data``, the totals are ``None``, the coverage is zero and the series
        still holds one point per session, with null flows.

    Raises:
        ValueError: If ``rolling_window`` is outside its range or ``sessions`` is not strictly
            ascending.
    """
    if not MIN_FLOW_WINDOW <= rolling_window <= MAX_FLOW_WINDOW:
        raise ValueError(
            f"rolling_window must be between {MIN_FLOW_WINDOW} and {MAX_FLOW_WINDOW}, "
            f"got {rolling_window}"
        )
    for previous, current in pairwise(sessions):
        if current <= previous:
            raise ValueError(f"sessions must be strictly ascending, got {current} after {previous}")

    # (foreign net, turnover) per session, over the constituents with non-null flow only.
    session_days = frozenset(sessions)
    daily: dict[date, tuple[Decimal, Decimal]] = {}
    for row in rows:
        flow = row.foreign_net_flow_kes
        if flow is None or row.trade_date not in session_days:
            continue
        foreign_sum, turnover_sum = daily.get(row.trade_date, (ZERO, ZERO))
        daily[row.trade_date] = (foreign_sum + flow, turnover_sum + row.turnover_kes)

    series: list[FlowPointResult] = []
    cumulative = ZERO
    sessions_with_data = 0
    total_turnover = ZERO
    # Running sum and running count of sessions with data after each point. A rolling window's
    # sum is the difference of two running sums, which keeps the series linear in length. That
    # difference is the exact window sum while every running sum and window sum fits the
    # 28-significant-digit context, which for amounts of at most 2 dp means magnitudes below
    # 10^26. Beyond that, each addition and the subtraction round half-even to 28 digits like
    # any Decimal sum, and the difference can then differ from re-summing the window.
    cumulative_path: list[Decimal] = []
    data_count_path: list[int] = []
    for index, day in enumerate(sessions):
        foreign: Decimal | None = None
        local: Decimal | None = None
        turnover: Decimal | None = None
        pct_of_turnover: Decimal | None = None
        day_totals = daily.get(day)
        if day_totals is not None:
            foreign, turnover = day_totals
            local = -foreign
            pct_of_turnover = safe_div(foreign * HUNDRED, turnover)
            cumulative += foreign
            sessions_with_data += 1
            total_turnover += turnover
        cumulative_path.append(cumulative)
        data_count_path.append(sessions_with_data)

        rolling: Decimal | None = None
        if index >= rolling_window - 1:
            before = index - rolling_window
            sum_before = cumulative_path[before] if before >= 0 else ZERO
            count_before = data_count_path[before] if before >= 0 else 0
            if sessions_with_data > count_before:
                rolling = cumulative - sum_before

        series.append(
            FlowPointResult(
                trade_date=day,
                foreign_net_kes=foreign,
                local_net_kes=local,
                cumulative_foreign_net_kes=cumulative,
                rolling_foreign_net_kes=rolling,
                turnover_kes=turnover,
                foreign_net_pct_of_turnover=pct_of_turnover,
            )
        )

    if sessions_with_data == 0:
        return FlowSeriesResult(
            status="no_flow_data",
            flags=["no_flow_data"],
            coverage_pct=ZERO,
            totals=None,
            series=series,
        )

    totals = FlowTotalsResult(
        foreign_net_kes=cumulative,
        local_net_kes=-cumulative,
        foreign_net_pct_of_turnover=safe_div(cumulative * HUNDRED, total_turnover),
        sessions_with_data=sessions_with_data,
        sessions_in_range=len(sessions),
    )
    return FlowSeriesResult(
        status="ok",
        flags=[],
        coverage_pct=Decimal(sessions_with_data) * HUNDRED / len(sessions),
        totals=totals,
        series=series,
    )
