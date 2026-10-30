"""Deterministic generators of the fallback seed: the rows ``seed_terminal_db.py`` writes.

This is a pure module. It performs no I/O and imports nothing from the database, model, API or
service layers: each ``generate_*`` function returns immutable row records whose field names are
the ``terminal_`` ORM column names (without ``id``), and the seeder maps them onto the models
through :func:`to_row`.

Determinism. Output is byte-identical on every platform and every run, which
:func:`canonical_bytes` and :func:`dataset_fingerprint` make checkable:

* every random draw comes from a ``random.Random`` seeded with a fixed string, so Python's
  SHA-512 string seeding fixes the Mersenne Twister state. Each (subject, purpose) pair has its own
  independent stream, named ``opennse-seed:<TICKER>:prices``, ``:volumes``, ``:ohlc``, ``:flows``
  and ``:fundamentals``, ``opennse-seed:macro:<column>`` and ``opennse-seed:bonds``, so a change to
  one series never shifts the draws of another;
* each uniform float from ``rng.random()`` is converted exactly with ``Decimal(float)`` before any
  arithmetic. Normal variates use the Irwin-Hall approximation (the sum of 12 uniforms minus 6);
* all arithmetic after a draw is ``Decimal`` in a fixed 28-digit context (:data:`SEED_CONTEXT`,
  the decimal module's defaults), entered by every ``generate_*`` function so that a caller's
  modified context cannot change the output. Compounding uses ``Decimal.exp`` and roots
  ``Decimal.sqrt``; no float, ``math`` function, set, hash order, clock or environment value is
  involved;
* values are rounded once, to their column's scale, with ``ROUND_HALF_UP``.

Units follow the columns: prices and dividends are KES per share, ``*_kes`` totals are whole KES,
``*_pct`` values are percentage points, ``fx_kes_usd`` is KES per USD, ``fx_reserves_usd_m`` is
millions of USD, and volumes and share counts are ``int``. Currency is KES throughout.

Calendar. Price rows are dated on :func:`synthetic_sessions`, a simplified mock calendar
(weekdays minus eight fixed-date holidays). It is not an exchange calendar and never dates real
data: analytics use the observed market sessions, and live NSE runs use ``ingestion/calendar.py``.

No future history. Price trade dates, fundamentals period ends and macro and bond observation
dates never pass :data:`~opennse_terminal.seed.reference_data.ANCHOR_DATE` (2026-09-30):
:func:`generate_prices`, :func:`generate_fundamentals`, :func:`generate_macro` and
:func:`generate_bonds` raise ``RuntimeError`` rather than return such a row. The only later dates
are forward reference dates, never observations: bond maturity dates (up to 2051-09-30) and the
2027-08-10 event date. The 2027 election is seeded only as an event definition in
``terminal_cycles``: rows dated after the present would be read as observed history, so its
trajectory is the backtester's labelled projection from the completed cycles instead.
"""

import functools
import hashlib
import json
import random
from bisect import bisect_left
from calendar import monthrange
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, fields, replace
from datetime import date, timedelta
from decimal import (
    ROUND_CEILING,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Context,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    localcontext,
)
from typing import Final, ParamSpec, TypeVar

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.quant.zscore import Observation, ZScorePoint, rolling_zscores
from opennse_terminal.seed import reference_data as ref

__all__ = [
    "EXPECTED_BOND_ROWS",
    "EXPECTED_FUNDAMENTAL_ROWS",
    "EXPECTED_MACRO_ROWS",
    "EXPECTED_PRICE_ROWS",
    "EXPECTED_SESSIONS",
    "SEED_CONTEXT",
    "SeedBondRow",
    "SeedCycleRow",
    "SeedDataset",
    "SeedEquityRow",
    "SeedFundamentalRow",
    "SeedIpoScenarioRow",
    "SeedMacroRow",
    "SeedPriceRow",
    "SeedRecord",
    "SeedWatchlist",
    "add_years",
    "canonical_bytes",
    "dataset_fingerprint",
    "flow_fraction",
    "flow_fraction_overlay",
    "generate_all",
    "generate_bonds",
    "generate_cycles",
    "generate_equities",
    "generate_fundamentals",
    "generate_ipo_scenarios",
    "generate_macro",
    "generate_prices",
    "generate_watchlist",
    "month_ends",
    "price_multiplier",
    "price_overlay",
    "quantize_half_up",
    "standard_normal",
    "synthetic_sessions",
    "to_row",
    "uniform",
    "volume_multiplier",
    "volume_overlay",
]

EXPECTED_SESSIONS: Final = 3_762
"""Sessions of the synthetic calendar from 2012-01-02 to 2026-09-30."""
EXPECTED_PRICE_ROWS: Final = EXPECTED_SESSIONS * len(ref.EQUITIES)
"""One price row per counter per session: 30,096."""
EXPECTED_FUNDAMENTAL_ROWS: Final = 131
"""FY rows: five banks x 16 years, EABL and KEGN x 17, and SCOM x 17."""
EXPECTED_MACRO_ROWS: Final = 177
"""Month-ends from 2012-01-31 to 2026-09-30."""
EXPECTED_BOND_ROWS: Final = EXPECTED_MACRO_ROWS * len(ref.BOND_TENOR_SPREADS)
"""One bond observation per tenor per month-end: 1,062."""

SEED_CONTEXT: Final = Context(
    prec=28,
    rounding=ROUND_HALF_EVEN,
    Emin=-999_999,
    Emax=999_999,
    capitals=1,
    clamp=0,
    flags=[],
    traps=[InvalidOperation, DivisionByZero, Overflow],
)
"""The decimal module's default context, spelled out: every generator computes in it."""

_STREAM_PREFIX: Final = "opennse-seed"
_CENT: Final = Decimal("0.01")
_DPS_STEP: Final = Decimal("0.0001")
_UNIT: Final = Decimal("1")
_HUNDRED: Final = Decimal("100")
_ZERO: Final = Decimal("0")
_IRWIN_HALL_TERMS: Final = 12
# Bounds of the opening gap and high/low extension draws, as fractions of price.
_OHLC_LOW: Final = -ref.OHLC_MAX_EPSILON
_OHLC_HIGH: Final = ref.OHLC_MAX_EPSILON

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _in_seed_context(function: Callable[_P, _R]) -> Callable[_P, _R]:
    """Run ``function`` inside a private copy of :data:`SEED_CONTEXT`."""

    @functools.wraps(function)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with localcontext(SEED_CONTEXT):
            return function(*args, **kwargs)

    return wrapper


# Row records ------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SeedEquityRow:
    """One ``terminal_equities`` row. ``free_float_pct`` is in percentage points."""

    ticker: str
    isin: str
    company_name: str
    sector: str
    shares_outstanding: int
    free_float_pct: Decimal
    currency: str
    source: str


@dataclass(frozen=True, slots=True)
class SeedPriceRow:
    """One ``terminal_daily_prices`` session; the seeder adds ``ingestion_run_id``.

    Prices are KES per share at 2 dp, ``volume`` is shares, ``turnover_kes`` and
    ``foreign_net_flow_kes`` are whole KES at 2 dp. The flow is populated on every row, so
    ``flow_source`` is always set with it.
    """

    ticker: str
    trade_date: date
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    turnover_kes: Decimal
    turnover_estimated: bool
    foreign_net_flow_kes: Decimal
    source: str
    flow_source: str


@dataclass(frozen=True, slots=True)
class SeedFundamentalRow:
    """One ``terminal_fundamentals`` FY row; ``*_kes`` are whole KES, ``dps_kes`` KES per share.

    Banks carry ``None`` for ``ebitda_kes``, ``total_debt_kes``, ``cash_and_equivalents_kes`` and
    ``operating_cash_flow_kes``. ``source_by_field`` maps each non-null value column to ``seed``.
    """

    ticker: str
    period_end_date: date
    period_type: str
    revenue_kes: Decimal
    ebitda_kes: Decimal | None
    net_income_kes: Decimal
    total_assets_kes: Decimal
    total_equity_kes: Decimal
    total_debt_kes: Decimal | None
    cash_and_equivalents_kes: Decimal | None
    operating_cash_flow_kes: Decimal | None
    dps_kes: Decimal
    shares_outstanding: int
    currency: str
    source_by_field: dict[str, str]


@dataclass(frozen=True, slots=True)
class SeedMacroRow:
    """One ``terminal_macro_indicators`` month-end, every indicator populated at 2 dp."""

    observation_date: date
    inflation_yoy_pct: Decimal
    cbr_rate_pct: Decimal
    interbank_rate_pct: Decimal
    tbill_91d_pct: Decimal
    tbill_182d_pct: Decimal
    tbill_364d_pct: Decimal
    fx_kes_usd: Decimal
    fx_reserves_usd_m: Decimal
    source_by_field: dict[str, str]


@dataclass(frozen=True, slots=True)
class SeedBondRow:
    """One ``terminal_bond_yields`` observation of a mock fixed-coupon (``FXD``) bond.

    Tenors are years at 2 dp and ``yield_pct`` is percentage points. The plan specifies no ISIN or
    coupon for mock bonds, so both are ``None``.
    """

    observation_date: date
    bond_code: str
    isin: str | None
    original_tenor_years: Decimal
    remaining_tenor_years: Decimal
    maturity_date: date
    coupon_rate_pct: Decimal | None
    yield_pct: Decimal
    source: str


@dataclass(frozen=True, slots=True)
class SeedCycleRow:
    """One ``terminal_cycles`` event definition."""

    label: str
    event_date: date
    event_type: str
    source: str


@dataclass(frozen=True, slots=True)
class SeedIpoScenarioRow:
    """One ``terminal_ipo_scenarios`` preset: the reference preset plus its stored extras.

    ``risk_scenarios`` holds ``{"title", "description"}`` objects. Money is whole KES, rates and
    weights are percentage points, and ``peer_ev_ebitda_multiple`` is a multiple (x).
    """

    company_name: str
    sector: str
    offer_price_kes: Decimal
    shares_offered: int
    shares_outstanding_post_offer: int
    pct_stake_sold: Decimal
    primary_proceeds_kes: Decimal
    base_revenue_kes: Decimal
    base_ebitda_kes: Decimal
    net_debt_kes: Decimal
    assumed_wacc_pct: Decimal
    assumed_growth_pct: Decimal
    terminal_growth_pct: Decimal
    margin_expansion_pct: Decimal
    tax_rate_pct: Decimal
    capex_pct_revenue: Decimal
    da_pct_revenue: Decimal
    nwc_pct_revenue: Decimal
    peer_ev_ebitda_multiple: Decimal
    dcf_weight_pct: Decimal
    forecast_years: int
    risk_scenarios: list[dict[str, str]]
    currency: str
    source: str
    is_preset: bool = True


@dataclass(frozen=True, slots=True)
class SeedWatchlist:
    """The seeded watchlist; the seeder assigns ``owner_key`` (``default_owner_key``)."""

    name: str
    tickers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SeedDataset:
    """Every seeded record, in the seeder's foreign-key order."""

    equities: tuple[SeedEquityRow, ...]
    prices: tuple[SeedPriceRow, ...]
    fundamentals: tuple[SeedFundamentalRow, ...]
    macro: tuple[SeedMacroRow, ...]
    bonds: tuple[SeedBondRow, ...]
    cycles: tuple[SeedCycleRow, ...]
    ipo_scenarios: tuple[SeedIpoScenarioRow, ...]
    watchlist: SeedWatchlist


SeedRecord = (
    SeedEquityRow
    | SeedPriceRow
    | SeedFundamentalRow
    | SeedMacroRow
    | SeedBondRow
    | SeedCycleRow
    | SeedIpoScenarioRow
    | SeedWatchlist
)
"""Any one seeded row record."""


def to_row(record: SeedRecord) -> dict[str, object]:
    """Return ``record`` as a ``{column: value}`` dict for an upsert.

    ``dataclasses.asdict`` deep-copies the ``dict`` and ``list`` fields, so the caller may extend
    or change the row (for example to add ``id``) without touching the frozen record.
    """
    return asdict(record)


# Rounding, calendar and random-draw helpers -----------------------------------------------------


def quantize_half_up(value: Decimal, exponent: Decimal) -> Decimal:
    """Round ``value`` to the scale of ``exponent`` (such as ``Decimal("0.01")``), half up.

    This is the seed's only rounding step: prices, KES totals, percentages and yields use
    ``0.01``, dividends per share ``0.0001`` and volumes ``1``. A result that rounds to zero is
    returned as positive zero, so a tiny negative flow is stored and serialized as ``0.00``.
    """
    rounded = value.quantize(exponent, rounding=ROUND_HALF_UP)
    return rounded.copy_abs() if rounded.is_zero() else rounded


def _require_not_after_anchor(day: date, what: str) -> None:
    if day > ref.ANCHOR_DATE:
        raise RuntimeError(
            f"{what} is dated {day.isoformat()}, after the seed anchor "
            f"{ref.ANCHOR_DATE.isoformat()}; no future rows may be seeded"
        )


@functools.cache
def synthetic_sessions() -> tuple[date, ...]:
    """Return the synthetic seed calendar: 3,762 sessions from 2012-01-02 to 2026-09-30.

    A session is a weekday from ``SERIES_START`` to ``ANCHOR_DATE`` inclusive whose
    ``(month, day)`` is not one of the eight fixed dates in ``SESSION_EXCLUDED_MONTH_DAYS``
    (1 Jan, 1 May, 1 Jun, 10 Oct, 20 Oct, 12 Dec, 25 Dec, 26 Dec).

    This is a simplified mock calendar, not an exchange calendar: it omits Easter, Idd-ul-Fitr,
    Sunday substitution and gazetted closures. It never dates real data; analytics use the
    observed market sessions and live NSE runs use ``ingestion/calendar.py``.

    Raises:
        RuntimeError: If the calendar does not hold exactly 3,762 sessions.
    """
    excluded = ref.SESSION_EXCLUDED_MONTH_DAYS
    sessions: list[date] = []
    day = ref.SERIES_START
    one_day = timedelta(days=1)
    while day <= ref.ANCHOR_DATE:
        if day.weekday() < 5 and (day.month, day.day) not in excluded:
            sessions.append(day)
        day += one_day
    if len(sessions) != EXPECTED_SESSIONS:
        raise RuntimeError(
            f"synthetic calendar has {len(sessions)} sessions, expected {EXPECTED_SESSIONS}"
        )
    return tuple(sessions)


def month_ends(start: date = ref.MACRO_START, end: date = ref.ANCHOR_DATE) -> tuple[date, ...]:
    """Return every calendar month-end ``d`` with ``start <= d <= end``, ascending.

    With the defaults these are the 177 month-ends from 2012-01-31 to 2026-09-30.
    """
    ends: list[date] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        month_end = date(year, month, monthrange(year, month)[1])
        if start <= month_end <= end:
            ends.append(month_end)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return tuple(ends)


def add_years(day: date, years: int) -> date:
    """Return the same month and day ``years`` later, with 29 February clamped to 28 February."""
    try:
        return day.replace(year=day.year + years)
    except ValueError:
        return date(day.year + years, 2, 28)


def _stream(*parts: str) -> random.Random:
    """Return the independent random stream ``opennse-seed:<part>:<part>...``."""
    # Mock-data noise, not security: a reproducible non-cryptographic generator is the point.
    return random.Random(":".join((_STREAM_PREFIX, *parts)))  # noqa: S311


def uniform(rng: random.Random, low: Decimal, high: Decimal) -> Decimal:
    """Return a uniform draw in ``[low, high)``: ``low + (high - low) * u``.

    ``u`` is ``rng.random()`` converted exactly with ``Decimal(float)``, so no float arithmetic
    takes place. The result is in the unit of ``low`` and ``high``.
    """
    return low + (high - low) * Decimal(rng.random())


def standard_normal(rng: random.Random) -> Decimal:
    """Return an approximately N(0, 1) draw: the sum of 12 exact-``Decimal`` uniforms minus 6.

    This is the Irwin-Hall approximation, bounded to ``[-6, 6)`` and fully reproducible.
    """
    total = sum((Decimal(rng.random()) for _ in range(_IRWIN_HALL_TERMS)), _ZERO)
    return total - 6


# Election-cycle overlays ------------------------------------------------------------------------


def _lerp(offset: int, start: int, start_value: Decimal, end: int, end_value: Decimal) -> Decimal:
    """Interpolate linearly at ``offset`` from ``start_value`` at ``start`` to ``end_value``."""
    return start_value + (end_value - start_value) * (offset - start) / (end - start)


def price_overlay(offset: int) -> Decimal:
    """Return one cycle's price multiplier at ``offset`` calendar days from its event.

    1.00 up to -150; linear from 1.00 at -150 to 0.88 at -5; 0.85 from -5 to +5 (the valuation
    floor); linear from 0.85 at +5 to 1.05 at +150; 1.05 held from +150 on.
    """
    if offset <= ref.PRICE_OVERLAY_DECLINE_START:
        return ref.OVERLAY_NEUTRAL
    if offset < ref.PRICE_OVERLAY_FLOOR_START:
        return _lerp(
            offset,
            ref.PRICE_OVERLAY_DECLINE_START,
            ref.OVERLAY_NEUTRAL,
            ref.PRICE_OVERLAY_FLOOR_START,
            ref.PRICE_OVERLAY_PRE_FLOOR,
        )
    if offset <= ref.PRICE_OVERLAY_FLOOR_END:
        return ref.PRICE_OVERLAY_FLOOR
    if offset < ref.PRICE_OVERLAY_RECOVERY_END:
        return _lerp(
            offset,
            ref.PRICE_OVERLAY_FLOOR_END,
            ref.PRICE_OVERLAY_FLOOR,
            ref.PRICE_OVERLAY_RECOVERY_END,
            ref.PRICE_OVERLAY_POST,
        )
    return ref.PRICE_OVERLAY_POST


def volume_overlay(offset: int) -> Decimal:
    """Return one cycle's volume multiplier at ``offset`` calendar days from its event.

    1.00 before -60 and after +60; linear from 1.00 at -60 to 0.60 at -10; 0.45 from -10 to +5
    (volume exhaustion); 1.80 from +5 to +15; linear from 1.80 at +15 back to 1.00 at +60.
    """
    if offset < ref.VOLUME_OVERLAY_START or offset > ref.VOLUME_OVERLAY_END:
        return ref.OVERLAY_NEUTRAL
    if offset < ref.VOLUME_OVERLAY_TROUGH_START:
        return _lerp(
            offset,
            ref.VOLUME_OVERLAY_START,
            ref.OVERLAY_NEUTRAL,
            ref.VOLUME_OVERLAY_TROUGH_START,
            ref.VOLUME_OVERLAY_PRE_TROUGH,
        )
    if offset < ref.VOLUME_OVERLAY_SURGE_START:
        return ref.VOLUME_OVERLAY_EXHAUSTION
    if offset < ref.VOLUME_OVERLAY_SURGE_END:
        return ref.VOLUME_OVERLAY_SURGE
    return _lerp(
        offset,
        ref.VOLUME_OVERLAY_SURGE_END,
        ref.VOLUME_OVERLAY_SURGE,
        ref.VOLUME_OVERLAY_END,
        ref.OVERLAY_NEUTRAL,
    )


def flow_fraction_overlay(offset: int) -> Decimal:
    """Return one cycle's foreign net flow as a fraction of turnover, before noise.

    -0.20 from -120 to -1 (pre-election outflow), +0.12 from +10 to +120, and 0 otherwise.
    """
    if ref.FLOW_PRE_START <= offset <= ref.FLOW_PRE_END:
        return ref.FLOW_PRE_FRACTION
    if ref.FLOW_POST_START <= offset <= ref.FLOW_POST_END:
        return ref.FLOW_POST_FRACTION
    return _ZERO


def price_multiplier(day: date) -> Decimal:
    """Return the product of every cycle's :func:`price_overlay` on ``day``.

    Successive cycles compound, so each completed cycle's +5% recovery level carries forward. The
    2027 event contributes 1.00 to every seeded day, whose offsets all precede its window.
    """
    multiplier = _UNIT
    for cycle in ref.CYCLES:
        multiplier *= price_overlay((day - cycle.event_date).days)
    return multiplier


def volume_multiplier(day: date) -> Decimal:
    """Return the product of every cycle's :func:`volume_overlay` on ``day``."""
    multiplier = _UNIT
    for cycle in ref.CYCLES:
        multiplier *= volume_overlay((day - cycle.event_date).days)
    return multiplier


def flow_fraction(day: date) -> Decimal:
    """Return the sum of every cycle's :func:`flow_fraction_overlay` on ``day``.

    The cycles' flow windows never overlap, so at most one term is non-zero.
    """
    return sum(
        (flow_fraction_overlay((day - cycle.event_date).days) for cycle in ref.CYCLES), _ZERO
    )


# Daily prices, volumes, turnover and foreign flows ----------------------------------------------


def _close_path(ticker: str, sessions: Sequence[date]) -> list[Decimal]:
    """Closes from a log-space GBM times the cycle overlays, rescaled to end at the anchor price.

    ``L_0 = 0`` and ``L_i = L_(i-1) + (mu - sigma^2 / 2) dt + sigma sqrt(dt) Z_i`` with
    ``dt = 1/252``; ``raw_i = exp(L_i) * price_multiplier(day_i)`` and
    ``close_i = max(round(raw_i * anchor / raw_last, 0.01), 0.01)``. The last close is set to the
    anchor price exactly.
    """
    rng = _stream(ticker, "prices")
    sigma = ref.VOLATILITY[ticker]
    dt = _UNIT / ref.TRADING_DAYS_PER_YEAR
    drift_step = (ref.ANNUAL_DRIFT - sigma * sigma / 2) * dt
    shock_scale = sigma * dt.sqrt()
    log_price = _ZERO
    raw: list[Decimal] = []
    for index, day in enumerate(sessions):
        if index:
            log_price += drift_step + shock_scale * standard_normal(rng)
        raw.append(log_price.exp() * price_multiplier(day))
    anchor = ref.PRICE_ANCHORS[ticker]
    scale = anchor / raw[-1]
    closes = [max(quantize_half_up(value * scale, _CENT), _CENT) for value in raw]
    closes[-1] = anchor
    return closes


def _turnover(volume: int, high: Decimal, low: Decimal, close: Decimal) -> Decimal:
    """``volume * (high + low + close) / 3`` in KES at 2 dp (the typical-price turnover)."""
    return quantize_half_up(volume * (high + low + close) / 3, _CENT)


def _foreign_flow(turnover: Decimal, fraction: Decimal) -> Decimal:
    """``turnover * fraction`` in KES at 2 dp."""
    return quantize_half_up(turnover * fraction, _CENT)


def _clip_flow_fraction(value: Decimal) -> Decimal:
    return max(-ref.FLOW_FRACTION_CLIP, min(ref.FLOW_FRACTION_CLIP, value))


def _spike_point(rows: Sequence[SeedPriceRow], sessions: Sequence[date], day: date) -> ZScorePoint:
    """The volume Z-score of ``day``, scored by :func:`rolling_zscores` over the whole series.

    Using the analytic's own window rule keeps the planted spikes consistent with the screener
    and the anomaly feed.
    """
    observations = [
        Observation(trade_date=row.trade_date, value=Decimal(row.volume)) for row in rows
    ]
    for point in rolling_zscores(observations, sessions, window=ref.SPIKE_Z_WINDOW):
        if point.trade_date == day:
            return point
    raise RuntimeError(f"{rows[0].ticker} has no seeded row on {day.isoformat()}")


def _plant_spikes(
    ticker: str,
    rows: list[SeedPriceRow],
    fractions: Sequence[Decimal],
    sessions: Sequence[date],
) -> None:
    """Raise each planted day's volume to a guaranteed spike, in place, then verify it.

    The new volume is ``max(4 * volume, ceil(mean(W) + 4 * sd(W)))``, with ``W`` the 20 preceding
    rows under the Z-score window rule; turnover and the flow are recomputed with the same OHLC
    and flow fraction. Each planted day must then score ``z >= 4.00``.
    """
    spike_days = sorted(day for spike_ticker, day in ref.PLANTED_SPIKES if spike_ticker == ticker)
    for day in spike_days:
        session_index = bisect_left(sessions, day)
        if session_index == len(sessions) or sessions[session_index] != day:
            raise RuntimeError(
                f"planted spike {ticker} {day.isoformat()} is not a synthetic session"
            )
        point = _spike_point(rows, sessions, day)
        mean, stdev = point.rolling_mean, point.rolling_stdev
        if mean is None or stdev is None or stdev.is_zero():
            raise RuntimeError(
                f"planted spike {ticker} {day.isoformat()} has no usable Z-score window: "
                f"flags {point.flags}"
            )
        position = bisect_left([row.trade_date for row in rows], day)
        row = rows[position]
        floor_volume = (mean + ref.SPIKE_SD_MULTIPLE * stdev).to_integral_value(
            rounding=ROUND_CEILING
        )
        volume = max(ref.SPIKE_VOLUME_MULTIPLE * row.volume, int(floor_volume))
        turnover = _turnover(volume, row.high_price, row.low_price, row.close_price)
        rows[position] = replace(
            row,
            volume=volume,
            turnover_kes=turnover,
            foreign_net_flow_kes=_foreign_flow(turnover, fractions[position]),
        )
    for day in spike_days:
        z = _spike_point(rows, sessions, day).z
        if z is None or z < ref.SPIKE_MIN_Z:
            raise RuntimeError(
                f"planted spike {ticker} {day.isoformat()} has volume Z-score {z}, "
                f"below the required {ref.SPIKE_MIN_Z}"
            )


def _ticker_prices(ticker: str, sessions: Sequence[date]) -> list[SeedPriceRow]:
    """One counter's full seeded series, planted spike included."""
    closes = _close_path(ticker, sessions)
    ohlc_rng = _stream(ticker, "ohlc")
    volume_rng = _stream(ticker, "volumes")
    flow_rng = _stream(ticker, "flows")
    base_volume = ref.BASE_VOLUMES[ticker]
    rows: list[SeedPriceRow] = []
    fractions: list[Decimal] = []
    previous_close = closes[0]
    for day, close in zip(sessions, closes, strict=True):
        open_gap = uniform(ohlc_rng, _OHLC_LOW, _OHLC_HIGH)
        high_extension = abs(uniform(ohlc_rng, _OHLC_LOW, _OHLC_HIGH))
        low_extension = abs(uniform(ohlc_rng, _OHLC_LOW, _OHLC_HIGH))
        open_price = max(quantize_half_up(previous_close * (1 + open_gap), _CENT), _CENT)
        # Rounding a value at or beyond a cent-grid price never crosses it, so
        # low <= min(open, close) <= max(open, close) <= high holds after rounding.
        high_price = quantize_half_up(max(open_price, close) * (1 + high_extension), _CENT)
        low_price = max(
            quantize_half_up(min(open_price, close) * (1 - low_extension), _CENT), _CENT
        )
        noise = (ref.VOLUME_LOG_SIGMA * standard_normal(volume_rng)).exp()
        volume = max(int(quantize_half_up(base_volume * noise * volume_multiplier(day), _UNIT)), 1)
        turnover = _turnover(volume, high_price, low_price, close)
        fraction = _clip_flow_fraction(
            flow_fraction(day) + ref.FLOW_NOISE_SIGMA * standard_normal(flow_rng)
        )
        fractions.append(fraction)
        rows.append(
            SeedPriceRow(
                ticker=ticker,
                trade_date=day,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close,
                volume=volume,
                turnover_kes=turnover,
                turnover_estimated=False,
                foreign_net_flow_kes=_foreign_flow(turnover, fraction),
                source=ref.SEED_SOURCE,
                flow_source=ref.SEED_SOURCE,
            )
        )
        previous_close = close
    _plant_spikes(ticker, rows, fractions, sessions)
    return rows


@_in_seed_context
def generate_prices() -> tuple[SeedPriceRow, ...]:
    """Generate the 30,096 seeded price rows, ordered by ``EQUITIES`` then by date.

    Per counter, over :func:`synthetic_sessions`:

    * close: a log-space geometric Brownian motion (annual drift 6%, the counter's volatility,
      ``dt = 1/252``) times :func:`price_multiplier`, rescaled so the 2026-09-30 close equals the
      observed NSE last traded price; KES per share at 2 dp, at least 0.01;
    * open: the previous close times ``1 + e`` and high/low: ``max(open, close) * (1 + |e|)`` and
      ``min(open, close) * (1 - |e|)``, with ``e`` uniform in ``[-0.01, 0.01)``;
    * volume: the counter's NSE-observed base volume times ``exp(0.5 Z)`` times
      :func:`volume_multiplier`, rounded to whole shares and at least 1;
    * turnover: ``volume * (high + low + close) / 3`` in KES at 2 dp, ``turnover_estimated`` false;
    * foreign net flow: turnover times ``clip(flow_fraction + 0.05 Z, -0.5, 0.5)`` in KES at 2 dp,
      on every row (100% coverage);
    * the three planted spikes of ``PLANTED_SPIKES``, each verified to score ``z >= 4.00``.

    Raises:
        RuntimeError: If a planted spike is not a session or does not reach ``z >= 4.00``, a
            counter does not end on the anchor price, a row is dated after 2026-09-30, a volume is
            out of range, or the row count is not 30,096.
    """
    sessions = synthetic_sessions()
    for spike_ticker, day in ref.PLANTED_SPIKES:
        if spike_ticker not in ref.TICKERS:
            raise RuntimeError(f"planted spike {spike_ticker} {day.isoformat()} is not seeded")
    rows: list[SeedPriceRow] = []
    for equity in ref.EQUITIES:
        ticker_rows = _ticker_prices(equity.ticker, sessions)
        last = ticker_rows[-1]
        anchor_price = ref.PRICE_ANCHORS[equity.ticker]
        if last.trade_date != ref.ANCHOR_DATE or last.close_price != anchor_price:
            raise RuntimeError(
                f"{equity.ticker} ends on {last.trade_date.isoformat()} at {last.close_price}, "
                f"expected {ref.ANCHOR_DATE.isoformat()} at {anchor_price}"
            )
        rows.extend(ticker_rows)
    for row in rows:
        _require_not_after_anchor(row.trade_date, f"price row {row.ticker}")
        if not 0 <= row.volume <= MAX_SHARE_COUNT:
            raise RuntimeError(
                f"price row {row.ticker} {row.trade_date.isoformat()} has volume {row.volume} "
                f"outside 0..{MAX_SHARE_COUNT}"
            )
    if len(rows) != EXPECTED_PRICE_ROWS:
        raise RuntimeError(f"generated {len(rows)} price rows, expected {EXPECTED_PRICE_ROWS}")
    return tuple(rows)


# Fundamentals -----------------------------------------------------------------------------------

_FUNDAMENTAL_NON_VALUE_FIELDS: Final[tuple[str, ...]] = (
    "ticker",
    "period_end_date",
    "period_type",
    "currency",
    "source_by_field",
)
"""Fields of :class:`SeedFundamentalRow` that are not value columns: the conflict key, the
currency and the provenance mapping itself."""

_FUNDAMENTAL_VALUE_COLUMNS: Final[tuple[str, ...]] = tuple(
    field.name
    for field in fields(SeedFundamentalRow)
    if field.name not in _FUNDAMENTAL_NON_VALUE_FIELDS
)
"""Value columns whose provenance ``source_by_field`` records, in table order."""


def _scaled(value: Decimal | None, factor: Decimal) -> Decimal | None:
    return None if value is None else quantize_half_up(value * factor, _CENT)


def _ticker_fundamentals(
    target: ref.FundamentalTarget, equity: ref.EquityReference
) -> list[SeedFundamentalRow]:
    """One counter's FY rows, ascending by period end, derived from its target ratios."""
    shares = equity.shares_outstanding
    if shares <= 0:
        raise RuntimeError(f"{target.ticker} has {shares} shares; fundamentals need shares > 0")
    market_cap = ref.PRICE_ANCHORS[target.ticker] * shares
    net_income = market_cap / target.pe
    total_equity = market_cap / target.pb
    revenue = net_income / (target.net_margin_pct / _HUNDRED)
    dps = net_income * (target.payout_pct / _HUNDRED) / shares
    ebitda: Decimal | None = None
    total_debt: Decimal | None = None
    cash: Decimal | None = None
    operating_cash_flow: Decimal | None = None
    if equity.sector == ref.BANKING_SECTOR:
        total_assets = total_equity * ref.BANK_ASSETS_TO_EQUITY
    else:
        if target.ebitda_margin_pct is None or target.net_debt_to_ebitda is None:
            raise RuntimeError(
                f"non-bank {target.ticker} needs an EBITDA margin and a net debt / EBITDA target"
            )
        ebitda = revenue * target.ebitda_margin_pct / _HUNDRED
        cash = revenue * ref.NON_BANK_CASH_PCT_REVENUE / _HUNDRED
        total_debt = target.net_debt_to_ebitda * ebitda + cash
        total_assets = total_equity * ref.NON_BANK_ASSETS_TO_EQUITY
        operating_cash_flow = net_income * ref.OCF_TO_NET_INCOME

    # factors[k] scales the latest year back k years: (1 + u_k) / 1.08^k, u_k drawn in ascending k.
    # One factor per year scales every column alike, so that year's margins and ratios hold.
    rng = _stream(target.ticker, "fundamentals")
    factors = [_UNIT]
    for years_back in range(1, target.last_fy - target.first_fy + 1):
        noise = uniform(rng, -ref.FUNDAMENTALS_HISTORY_NOISE, ref.FUNDAMENTALS_HISTORY_NOISE)
        factors.append((1 + noise) / ref.FUNDAMENTALS_HISTORY_GROWTH**years_back)

    rows: list[SeedFundamentalRow] = []
    for fiscal_year in range(target.first_fy, target.last_fy + 1):
        period_end = date(fiscal_year, target.fy_end_month, target.fy_end_day)
        _require_not_after_anchor(period_end, f"fundamentals row {target.ticker}")
        factor = factors[target.last_fy - fiscal_year]
        draft = SeedFundamentalRow(
            ticker=target.ticker,
            period_end_date=period_end,
            period_type="FY",
            revenue_kes=quantize_half_up(revenue * factor, _CENT),
            ebitda_kes=_scaled(ebitda, factor),
            net_income_kes=quantize_half_up(net_income * factor, _CENT),
            total_assets_kes=quantize_half_up(total_assets * factor, _CENT),
            total_equity_kes=quantize_half_up(total_equity * factor, _CENT),
            total_debt_kes=_scaled(total_debt, factor),
            cash_and_equivalents_kes=_scaled(cash, factor),
            operating_cash_flow_kes=_scaled(operating_cash_flow, factor),
            dps_kes=quantize_half_up(dps * factor, _DPS_STEP),
            shares_outstanding=shares,
            currency=DEFAULT_CURRENCY,
            source_by_field={},
        )
        provenance = {
            column: ref.SEED_SOURCE
            for column in _FUNDAMENTAL_VALUE_COLUMNS
            if getattr(draft, column) is not None
        }
        rows.append(replace(draft, source_by_field=provenance))
    return rows


@_in_seed_context
def generate_fundamentals() -> tuple[SeedFundamentalRow, ...]:
    """Generate the 131 seeded FY fundamentals rows, by ``FUNDAMENTAL_TARGETS`` then period end.

    The latest fiscal year is set from the counter's target ratios at its anchor market cap
    (``mcap = anchor close * shares``):

    * net income ``= mcap / P/E``; total equity ``= mcap / P/B``;
      revenue ``= net income / net margin``; dps ``= net income * payout / shares``;
    * non-banks: EBITDA ``= revenue * EBITDA margin``; cash ``= 10% of revenue``; total debt
      ``= net debt / EBITDA target * EBITDA + cash``; total assets ``= 2.0 * equity``; operating
      cash flow ``= 1.2 * net income``;
    * banks: EBITDA, total debt, cash and operating cash flow are ``None``; total assets
      ``= 7.5 * equity``.

    Year ``k`` before the latest is every column times ``(1 + u_k) / 1.08^k`` with ``u_k``
    uniform in ``[-0.05, 0.05)``. KES columns are whole KES at 2 dp, ``dps_kes`` KES per share at
    4 dp; ``shares_outstanding`` is the attachment share count in every year; currency KES.
    ``source_by_field`` maps every non-null value column to ``seed``.

    Raises:
        RuntimeError: If a target names an unseeded counter, a non-bank lacks its EBITDA targets,
            a period ends after 2026-09-30, or the row count is not 131.
    """
    equities = {equity.ticker: equity for equity in ref.EQUITIES}
    rows: list[SeedFundamentalRow] = []
    for target in ref.FUNDAMENTAL_TARGETS:
        equity = equities.get(target.ticker)
        if equity is None:
            raise RuntimeError(f"fundamental target {target.ticker} is not a seeded equity")
        rows.extend(_ticker_fundamentals(target, equity))
    if len(rows) != EXPECTED_FUNDAMENTAL_ROWS:
        raise RuntimeError(
            f"generated {len(rows)} fundamentals rows, expected {EXPECTED_FUNDAMENTAL_ROWS}"
        )
    return tuple(rows)


# Macro indicators -------------------------------------------------------------------------------


def _exact_macro_points() -> dict[str, dict[date, Decimal]]:
    """Per column, the values applied exactly: its ``MACRO_FIXED_POINTS`` and the anchor row."""
    exact: dict[str, dict[date, Decimal]] = {column: {} for column in ref.MACRO_COLUMNS}
    for column, day, value in ref.MACRO_FIXED_POINTS:
        exact[column][day] = value
    for column in ref.MACRO_COLUMNS:
        exact[column][ref.ANCHOR_DATE] = ref.MACRO_ANCHOR[column]
    return exact


def _interpolate(knots: Sequence[tuple[date, Decimal]], day: date) -> Decimal:
    """Linear interpolation by calendar days between the knots bracketing ``day``."""
    dates = [knot_date for knot_date, _ in knots]
    if day <= dates[0]:
        return knots[0][1]
    if day >= dates[-1]:
        return knots[-1][1]
    index = bisect_left(dates, day)
    end_date, end_value = knots[index]
    if end_date == day:
        return end_value
    start_date, start_value = knots[index - 1]
    elapsed = (day - start_date).days
    return start_value + (end_value - start_value) * elapsed / (end_date - start_date).days


def _macro_series(
    column: str, days: Sequence[date], exact: Mapping[date, Decimal]
) -> list[Decimal]:
    """One indicator's month-end path: knots, noise, 2 dp, clamped to its bounds."""
    # A fixed point or the anchor value replaces any yearly knot on the same date.
    knots = sorted({**dict(ref.MACRO_KNOTS[column]), **exact}.items())
    rng = _stream("macro", column)
    low, high = ref.MACRO_BOUNDS[column]
    values: list[Decimal] = []
    for day in days:
        # Drawn on every month-end, exact ones included, so each draw stays tied to its month.
        noise = uniform(rng, -ref.MACRO_NOISE, ref.MACRO_NOISE)
        value = _interpolate(knots, day)
        if day not in exact:
            value += noise
        values.append(min(max(quantize_half_up(value, _CENT), low), high))
    return values


def _check_macro(rows: Sequence[SeedMacroRow], exact: Mapping[str, Mapping[date, Decimal]]) -> None:
    """Raise ``RuntimeError`` unless the macro rows meet every seeded-history constraint."""
    if len(rows) != EXPECTED_MACRO_ROWS:
        raise RuntimeError(f"generated {len(rows)} macro rows, expected {EXPECTED_MACRO_ROWS}")
    if rows[0].observation_date != ref.MACRO_START or rows[-1].observation_date != ref.ANCHOR_DATE:
        raise RuntimeError(
            f"macro rows run {rows[0].observation_date} to {rows[-1].observation_date}, expected "
            f"{ref.MACRO_START} to {ref.ANCHOR_DATE}"
        )
    by_date = {row.observation_date: row for row in rows}
    for column, points in exact.items():
        for day, value in points.items():
            row = by_date.get(day)
            if row is None or getattr(row, column) != value:
                raise RuntimeError(f"macro {column} on {day} is not the exact seeded {value}")
    anchor_inflation = ref.MACRO_ANCHOR["inflation_yoy_pct"]
    for row in rows:
        day = row.observation_date
        _require_not_after_anchor(day, "macro row")
        for column in ref.MACRO_COLUMNS:
            low, high = ref.MACRO_BOUNDS[column]
            value = getattr(row, column)
            if not low <= value <= high:
                raise RuntimeError(f"macro {column} on {day} is {value}, outside {low}..{high}")
        if not row.tbill_91d_pct <= row.tbill_182d_pct <= row.tbill_364d_pct:
            raise RuntimeError(f"macro T-bill yields on {day} are not ordered 91d <= 182d <= 364d")
        # The attachment calls the anchor's inflation the highest since January 2024.
        in_cap = ref.INFLATION_CAP_START <= day <= ref.INFLATION_CAP_END
        if in_cap and row.inflation_yoy_pct >= anchor_inflation:
            raise RuntimeError(
                f"macro inflation on {day} is {row.inflation_yoy_pct}, not below the "
                f"{ref.ANCHOR_DATE} value {anchor_inflation}"
            )


@_in_seed_context
def generate_macro() -> tuple[SeedMacroRow, ...]:
    """Generate the 177 seeded month-end macro rows, 2012-01-31 to 2026-09-30.

    Each indicator interpolates linearly by calendar days between its yearly ``MACRO_KNOTS``, its
    ``MACRO_FIXED_POINTS`` and its 2026-09-30 ``MACRO_ANCHOR`` value (the attachment's figure, or
    CBK's published September 2026 Central Bank Rate for ``cbr_rate_pct``), adds uniform noise in
    ``[-0.15, 0.15)`` except on exact points, rounds to 2 dp and is clamped to ``MACRO_BOUNDS``.
    Then, in order: inflation from 2024-02-29 to 2026-07-31 is capped at 6.70; T-bills are ordered
    (``182d = max(182d, 91d)``, ``364d = max(364d, 182d)``); and the fixed points and the anchor
    row are set to their exact values. Rates are percentage points, ``fx_kes_usd`` KES per USD and
    ``fx_reserves_usd_m`` millions of USD; ``source_by_field`` maps all eight columns to ``seed``.

    Raises:
        RuntimeError: If the result breaks a seeded-history constraint: row count and span, exact
            values, bounds, T-bill ordering, or 6.80 being the highest inflation since January
            2024.
    """
    days = month_ends()
    exact = _exact_macro_points()
    for column, points in exact.items():
        for day in points:
            if day not in days:
                raise RuntimeError(f"exact macro point {column} {day} is not a seeded month-end")
    series = {column: _macro_series(column, days, exact[column]) for column in ref.MACRO_COLUMNS}

    inflation = series["inflation_yoy_pct"]
    for index, day in enumerate(days):
        if ref.INFLATION_CAP_START <= day <= ref.INFLATION_CAP_END:
            inflation[index] = min(inflation[index], ref.INFLATION_CAP_VALUE)
    bill_91d, bill_182d, bill_364d = (
        series["tbill_91d_pct"],
        series["tbill_182d_pct"],
        series["tbill_364d_pct"],
    )
    for index in range(len(days)):
        bill_182d[index] = max(bill_182d[index], bill_91d[index])
        bill_364d[index] = max(bill_364d[index], bill_182d[index])
    for column, points in exact.items():
        for day, value in points.items():
            series[column][days.index(day)] = value

    rows = tuple(
        SeedMacroRow(
            observation_date=day,
            inflation_yoy_pct=series["inflation_yoy_pct"][index],
            cbr_rate_pct=series["cbr_rate_pct"][index],
            interbank_rate_pct=series["interbank_rate_pct"][index],
            tbill_91d_pct=series["tbill_91d_pct"][index],
            tbill_182d_pct=series["tbill_182d_pct"][index],
            tbill_364d_pct=series["tbill_364d_pct"][index],
            fx_kes_usd=series["fx_kes_usd"][index],
            fx_reserves_usd_m=series["fx_reserves_usd_m"][index],
            source_by_field={column: ref.SEED_SOURCE for column in ref.MACRO_COLUMNS},
        )
        for index, day in enumerate(days)
    )
    _check_macro(rows, exact)
    return rows


# Bond yields ------------------------------------------------------------------------------------


@_in_seed_context
def generate_bonds(macro: Sequence[SeedMacroRow]) -> tuple[SeedBondRow, ...]:
    """Generate one mock ``FXD`` bond observation per tenor for each macro month-end.

    ``yield_pct = round(tbill_364d_pct + spread + noise, 0.01)`` in percentage points, with the
    tenor's spread from ``BOND_TENOR_SPREADS`` and ``noise`` uniform in ``[-0.10, 0.10)``, drawn
    for every observation but not applied on 2026-09-30. Codes read
    ``FXD-SEED-<tenor>Y-<YYYYMM>``; original and remaining tenor both equal the nominal tenor in
    years (2 dp), and the maturity is the observation date plus that many years. With the seeded
    macro rows this gives 177 x 6 = 1,062 rows.

    Raises:
        RuntimeError: If the macro rows are not strictly ascending, one is dated after 2026-09-30,
            or a yield is not positive.
    """
    rng = _stream("bonds")
    rows: list[SeedBondRow] = []
    previous: date | None = None
    for month in macro:
        day = month.observation_date
        if previous is not None and day <= previous:
            raise RuntimeError(f"macro rows must ascend strictly: {day} follows {previous}")
        _require_not_after_anchor(day, "bond observation")
        previous = day
        for tenor, spread in ref.BOND_TENOR_SPREADS:
            noise = uniform(rng, -ref.BOND_NOISE, ref.BOND_NOISE)
            if day == ref.ANCHOR_DATE:
                noise = _ZERO
            yield_pct = quantize_half_up(month.tbill_364d_pct + spread + noise, _CENT)
            if yield_pct <= 0:
                raise RuntimeError(f"bond yield for {tenor}Y on {day} is {yield_pct}, not > 0")
            tenor_years = quantize_half_up(Decimal(tenor), _CENT)
            rows.append(
                SeedBondRow(
                    observation_date=day,
                    bond_code=f"{ref.BOND_CODE_PREFIX}{tenor}Y-{day:%Y%m}",
                    isin=None,
                    original_tenor_years=tenor_years,
                    remaining_tenor_years=tenor_years,
                    maturity_date=add_years(day, tenor),
                    coupon_rate_pct=None,
                    yield_pct=yield_pct,
                    source=ref.SEED_SOURCE,
                )
            )
    return tuple(rows)


# Reference rows ---------------------------------------------------------------------------------


@_in_seed_context
def generate_equities() -> tuple[SeedEquityRow, ...]:
    """Return the eight seeded counters of ``EQUITIES``, in order, in KES with source ``seed``."""
    return tuple(
        SeedEquityRow(
            ticker=equity.ticker,
            isin=equity.isin,
            company_name=equity.company_name,
            sector=equity.sector,
            shares_outstanding=equity.shares_outstanding,
            free_float_pct=equity.free_float_pct,
            currency=DEFAULT_CURRENCY,
            source=ref.SEED_SOURCE,
        )
        for equity in ref.EQUITIES
    )


@_in_seed_context
def generate_cycles() -> tuple[SeedCycleRow, ...]:
    """Return the four election-cycle event definitions of ``CYCLES``, with source ``seed``.

    The 2027-08-10 event is dated after the anchor on purpose: it defines an event, not observed
    history, and no price, macro or bond row is seeded for it.
    """
    return tuple(
        SeedCycleRow(
            label=cycle.label,
            event_date=cycle.event_date,
            event_type=cycle.event_type,
            source=ref.SEED_SOURCE,
        )
        for cycle in ref.CYCLES
    )


@_in_seed_context
def generate_ipo_scenarios() -> tuple[SeedIpoScenarioRow, ...]:
    """Return the Quickmart Supermarket preset, its three risk scenarios stored as plain dicts."""
    preset = ref.QUICKMART_PRESET
    return (
        SeedIpoScenarioRow(
            company_name=preset.company_name,
            sector=preset.sector,
            offer_price_kes=preset.offer_price_kes,
            shares_offered=preset.shares_offered,
            shares_outstanding_post_offer=preset.shares_outstanding_post_offer,
            pct_stake_sold=preset.pct_stake_sold,
            primary_proceeds_kes=preset.primary_proceeds_kes,
            base_revenue_kes=preset.base_revenue_kes,
            base_ebitda_kes=preset.base_ebitda_kes,
            net_debt_kes=preset.net_debt_kes,
            assumed_wacc_pct=preset.assumed_wacc_pct,
            assumed_growth_pct=preset.assumed_growth_pct,
            terminal_growth_pct=preset.terminal_growth_pct,
            margin_expansion_pct=preset.margin_expansion_pct,
            tax_rate_pct=preset.tax_rate_pct,
            capex_pct_revenue=preset.capex_pct_revenue,
            da_pct_revenue=preset.da_pct_revenue,
            nwc_pct_revenue=preset.nwc_pct_revenue,
            peer_ev_ebitda_multiple=preset.peer_ev_ebitda_multiple,
            dcf_weight_pct=preset.dcf_weight_pct,
            forecast_years=preset.forecast_years,
            risk_scenarios=[dict(scenario) for scenario in ref.QUICKMART_RISK_SCENARIOS],
            currency=DEFAULT_CURRENCY,
            source=ref.SEED_SOURCE,
            is_preset=True,
        ),
    )


def generate_watchlist() -> SeedWatchlist:
    """Return the "NSE Core 8" watchlist holding every seeded counter.

    Raises:
        RuntimeError: If it names a ticker that is not seeded.
    """
    unknown = [ticker for ticker in ref.SEED_WATCHLIST_TICKERS if ticker not in ref.TICKERS]
    if unknown:
        raise RuntimeError(f"seed watchlist names unseeded tickers: {', '.join(unknown)}")
    return SeedWatchlist(name=ref.SEED_WATCHLIST_NAME, tickers=tuple(ref.SEED_WATCHLIST_TICKERS))


@_in_seed_context
def generate_all() -> SeedDataset:
    """Generate the whole seed, ready for the seeder; every invariant is checked before return.

    Raises:
        RuntimeError: If any generator's check fails, or the bond count is not 1,062.
    """
    macro = generate_macro()
    dataset = SeedDataset(
        equities=generate_equities(),
        prices=generate_prices(),
        fundamentals=generate_fundamentals(),
        macro=macro,
        bonds=generate_bonds(macro),
        cycles=generate_cycles(),
        ipo_scenarios=generate_ipo_scenarios(),
        watchlist=generate_watchlist(),
    )
    if len(dataset.bonds) != EXPECTED_BOND_ROWS:
        raise RuntimeError(
            f"generated {len(dataset.bonds)} bond rows, expected {EXPECTED_BOND_ROWS}"
        )
    return dataset


# Canonical serialization ------------------------------------------------------------------------


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not part of the seed's canonical form")


def canonical_bytes(dataset: SeedDataset) -> bytes:
    """Return the dataset as canonical UTF-8 JSON, identical whenever the data is identical.

    Records become their :func:`to_row` dicts with sorted keys and no whitespace; ``Decimal``
    values are written as their exact strings (``"7.50"``) and dates in ISO form.
    """
    payload = {
        "equities": [to_row(row) for row in dataset.equities],
        "prices": [to_row(row) for row in dataset.prices],
        "fundamentals": [to_row(row) for row in dataset.fundamentals],
        "macro": [to_row(row) for row in dataset.macro],
        "bonds": [to_row(row) for row in dataset.bonds],
        "cycles": [to_row(row) for row in dataset.cycles],
        "ipo_scenarios": [to_row(row) for row in dataset.ipo_scenarios],
        "watchlist": to_row(dataset.watchlist),
    }
    text = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )
    return text.encode("utf-8")


def dataset_fingerprint(dataset: SeedDataset) -> str:
    """Return the SHA-256 hex digest of :func:`canonical_bytes`."""
    return hashlib.sha256(canonical_bytes(dataset)).hexdigest()
