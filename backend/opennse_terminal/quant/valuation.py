"""Valuation multiples, dividend yield and sector aggregates (P/E, P/B, EV/EBITDA, yield).

Per-counter metrics combine three inputs that services resolve before calling in:

* ``shares_outstanding`` from ``terminal_equities``;
* the latest close on or before ``as_of`` from ``terminal_daily_prices``;
* the fiscal-year fundamentals available on ``as_of``, chosen by
  :func:`opennse_terminal.quant.asof.fundamentals_as_of` (the 90-day publication lag).

Units: ``*_kes`` values are whole Kenyan shillings (KES, the default currency), multiples are
dimensionless ratios (×) and yields are percentage points. Every value is an unrounded ``Decimal``;
services round once with :func:`opennse_terminal.quant.numeric.quantize_out` (``money`` for KES
totals, ``per_share`` for EPS, ``ratio`` for multiples, ``pct`` for yields).

Only KES inputs are valued (:data:`~opennse_terminal.quant.numeric.ANALYTICS_CURRENCY`). An
equity in another currency, or fundamentals in another currency, is refused with
``currency_mismatch`` and never converted; a refused equity has no market cap and enters no
sector total or eligible set.

An undefined metric is ``None`` together with a reason code in ``flags``, never zero and never an
exception. Each primitive returns a :class:`Metric` carrying both. A NaN or infinite input is not
an undefined metric but a caller error: ingestion rejects non-finite figures and the bounded
``NUMERIC`` columns cannot hold infinities, so every public function raises ``ValueError`` for one
rather than return an infinite metric or a wrong finite one such as a zero P/B.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Final, NamedTuple

from opennse_terminal.quant.asof import FundamentalsRow
from opennse_terminal.quant.numeric import ANALYTICS_CURRENCY, HUNDRED, ZERO, safe_div

__all__ = [
    "EV_EXCLUDED_SECTORS",
    "STALE_PRICE_DAYS",
    "Metric",
    "SectorAggregateResult",
    "ValuationInputs",
    "ValuationResult",
    "compute_valuation",
    "dividend_yield_pct",
    "enterprise_value",
    "eps",
    "ev_ebitda",
    "market_cap",
    "pb_ratio",
    "pe_ratio",
    "price_change_pct",
    "sector_aggregates",
]

EV_EXCLUDED_SECTORS: Final[frozenset[str]] = frozenset({"Banking"})
"""Sectors whose enterprise value and EV/EBITDA are always ``None``.

Banks do not report EBITDA, and enterprise value is not meaningful for a deposit-funded balance
sheet, so the policy applies whatever debt, cash or EBITDA figures were imported.
"""

STALE_PRICE_DAYS: Final = 10
"""A close more than this many calendar days before ``as_of`` adds the ``stale_price`` flag."""


class Metric(NamedTuple):
    """One metric value with the reason code that explains a ``None`` (or qualifies a value)."""

    value: Decimal | None
    flag: str | None


@dataclass(frozen=True, slots=True)
class ValuationInputs:
    """Point-in-time inputs for one counter.

    Attributes:
        ticker: Exchange ticker, for example ``"SCOM"``.
        sector: Sector exactly as stored in ``terminal_equities``, for example ``"Banking"``.
        shares_outstanding: Shares in issue (``terminal_equities.shares_outstanding``), >= 0.
        close_price: Latest close on or before ``as_of``, per share in ``currency`` (KES for
            every equity that is valued).
        price_date: ``trade_date`` of that close.
        as_of: Day on which the valuation stands.
        currency: The equity's ISO 4217 trading currency. Only KES is valued: an equity in any
            other currency is refused outright, and a KES equity's fundamentals in any other
            currency are refused, both with ``currency_mismatch`` and without conversion.
        fundamentals: The fiscal-year row available on ``as_of``, or ``None`` when there is none.
    """

    ticker: str
    sector: str
    shares_outstanding: int
    close_price: Decimal
    price_date: date
    as_of: date
    currency: str = "KES"
    fundamentals: FundamentalsRow | None = None


@dataclass(frozen=True, slots=True)
class ValuationResult:
    """Unrounded valuation of one counter, with the effective inputs sector aggregates reuse.

    ``market_cap_kes`` is ``None`` only for an equity refused because its currency is not KES
    (flag ``currency_mismatch``), which keeps it out of every sector total and eligible set.
    ``net_income_kes``, ``total_equity_kes``, ``ebitda_kes`` and ``dps_kes`` are the fundamentals
    values actually used, in KES: all ``None`` when fundamentals are absent, the equity is refused
    or the fundamentals are not in KES, so a member is eligible for a sector aggregate under
    exactly the rules of its own metrics. ``flags`` holds unique reason codes in evaluation order.
    """

    ticker: str
    sector: str
    shares_outstanding: int
    close_price: Decimal
    market_cap_kes: Decimal | None
    eps_kes: Decimal | None
    pe: Decimal | None
    pb: Decimal | None
    enterprise_value_kes: Decimal | None
    ev_ebitda: Decimal | None
    dividend_yield_pct: Decimal | None
    fundamentals_period_end: date | None
    net_income_kes: Decimal | None
    total_equity_kes: Decimal | None
    ebitda_kes: Decimal | None
    dps_kes: Decimal | None
    flags: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SectorAggregateResult:
    """Unrounded sector aggregates, each over its own matched eligible set of constituents.

    ``market_cap_kes`` is the sum, in KES, over the constituents that have a market cap, so a
    constituent refused for a non-KES currency is left out; it is ``None`` when no constituent has
    one, an empty sector included. ``constituent_count`` counts every constituent, refused ones
    included, and each ``*_eligible_count`` is the size of the set its aggregate was computed over,
    so partial coverage is visible.
    """

    sector: str
    constituent_count: int
    market_cap_kes: Decimal | None
    pe: Decimal | None
    pb: Decimal | None
    ev_ebitda: Decimal | None
    dividend_yield_pct: Decimal | None
    pe_eligible_count: int
    pb_eligible_count: int
    ev_ebitda_eligible_count: int
    dy_eligible_count: int
    flags: list[str] = field(default_factory=list)


def _require_non_negative_shares(shares: int) -> None:
    # The database CHECK keeps share counts in [0, 10^12]; a negative count is a caller error.
    if shares < 0:
        raise ValueError(f"shares must be >= 0, got {shares}")


def _require_finite(**values: Decimal | None) -> None:
    """Raise ``ValueError`` for the first value that is NaN (quiet or signalling) or infinite.

    ``None`` passes: a missing figure is an undefined metric, flagged by the caller.
    """
    for name, value in values.items():
        # is_finite() never signals, so a signalling NaN reaches the ValueError too.
        if value is not None and not value.is_finite():
            raise ValueError(f"{name} must be finite, got {value}")


def _add_flags(flags: list[str], codes: Iterable[str | None]) -> None:
    """Append each non-``None`` code not already present, keeping evaluation order."""
    for code in codes:
        if code is not None and code not in flags:
            flags.append(code)


def market_cap(close: Decimal, shares: int) -> Decimal:
    """Market capitalisation = close × shares, in KES.

    Raises:
        ValueError: If ``close`` is NaN or infinite, or ``shares`` is negative.
    """
    _require_finite(close=close)
    _require_non_negative_shares(shares)
    return close * Decimal(shares)


def eps(net_income: Decimal | None, shares: int) -> Metric:
    """Earnings per share = net income ÷ shares, in KES per share.

    A loss gives a negative EPS and zero earnings give zero; both are defined values.

    Flags: ``pe_missing_earnings`` when net income is ``None``; ``zero_shares`` when ``shares``
    is 0.

    Raises:
        ValueError: If ``net_income`` is NaN or infinite, or ``shares`` is negative.
    """
    _require_finite(net_income=net_income)
    _require_non_negative_shares(shares)
    if net_income is None:
        return Metric(None, "pe_missing_earnings")
    per_share = safe_div(net_income, Decimal(shares))
    if per_share is None:
        return Metric(None, "zero_shares")
    return Metric(per_share, None)


def pe_ratio(market_cap_kes: Decimal, net_income: Decimal | None) -> Metric:
    """Price-to-earnings = market cap ÷ net income, a ratio (×).

    Flags: ``pe_missing_earnings`` (net income ``None``), ``pe_zero_earnings`` (net income 0) and
    ``pe_negative_earnings`` (a loss); each leaves the ratio ``None``.

    Raises:
        ValueError: If ``market_cap_kes`` or ``net_income`` is NaN or infinite.
    """
    _require_finite(market_cap_kes=market_cap_kes, net_income=net_income)
    if net_income is None:
        return Metric(None, "pe_missing_earnings")
    if net_income.is_zero():
        return Metric(None, "pe_zero_earnings")
    if net_income < ZERO:
        return Metric(None, "pe_negative_earnings")
    return Metric(market_cap_kes / net_income, None)


def pb_ratio(market_cap_kes: Decimal, equity: Decimal | None) -> Metric:
    """Price-to-book = market cap ÷ total equity, a ratio (×).

    Flags: ``pb_missing_equity`` (equity ``None``) and ``pb_non_positive_equity`` (equity <= 0);
    each leaves the ratio ``None``.

    Raises:
        ValueError: If ``market_cap_kes`` or ``equity`` is NaN or infinite.
    """
    _require_finite(market_cap_kes=market_cap_kes, equity=equity)
    if equity is None:
        return Metric(None, "pb_missing_equity")
    if equity <= ZERO:
        return Metric(None, "pb_non_positive_equity")
    return Metric(market_cap_kes / equity, None)


def enterprise_value(
    market_cap_kes: Decimal,
    debt: Decimal | None,
    cash: Decimal | None,
    *,
    sector: str,
) -> Metric:
    """Enterprise value = market cap + total debt − cash and equivalents, in KES.

    The sector policy is checked first: a sector in :data:`EV_EXCLUDED_SECTORS` gives ``None``
    with ``ev_not_meaningful_for_banks`` whatever debt and cash are supplied. Otherwise a missing
    debt or cash figure gives ``None`` with ``ev_missing_inputs``. An enterprise value <= 0 is
    returned as computed, flagged ``negative_enterprise_value``.

    Raises:
        ValueError: If ``market_cap_kes``, ``debt`` or ``cash`` is NaN or infinite, whatever the
            sector.
    """
    _require_finite(market_cap_kes=market_cap_kes, debt=debt, cash=cash)
    if sector in EV_EXCLUDED_SECTORS:
        return Metric(None, "ev_not_meaningful_for_banks")
    if debt is None or cash is None:
        return Metric(None, "ev_missing_inputs")
    ev = market_cap_kes + debt - cash
    if ev <= ZERO:
        return Metric(ev, "negative_enterprise_value")
    return Metric(ev, None)


def ev_ebitda(ev: Decimal | None, ebitda: Decimal | None) -> Metric:
    """EV/EBITDA = enterprise value ÷ EBITDA, a ratio (×).

    Flags: ``ev_ebitda_missing_ebitda`` (EBITDA ``None``) and ``ev_ebitda_non_positive_ebitda``
    (EBITDA <= 0). A ``None`` enterprise value gives ``Metric(None, None)``, because the flag of
    :func:`enterprise_value` already explains it. A negative enterprise value over a positive
    EBITDA returns the negative ratio as computed.

    Raises:
        ValueError: If ``ev`` or ``ebitda`` is NaN or infinite.
    """
    _require_finite(ev=ev, ebitda=ebitda)
    if ebitda is None:
        return Metric(None, "ev_ebitda_missing_ebitda")
    if ebitda <= ZERO:
        return Metric(None, "ev_ebitda_non_positive_ebitda")
    if ev is None:
        return Metric(None, None)
    return Metric(ev / ebitda, None)


def dividend_yield_pct(dps: Decimal | None, close: Decimal) -> Metric:
    """Dividend yield = dividend per share ÷ close × 100, in percentage points.

    A zero dividend gives a defined 0% yield. Flags: ``dy_missing_dps`` (dps ``None``) and
    ``dy_non_positive_price`` (close <= 0, a guard the ``close_price > 0`` database check normally
    makes unreachable); each leaves the yield ``None``.

    Raises:
        ValueError: If ``dps`` or ``close`` is NaN or infinite.
    """
    _require_finite(dps=dps, close=close)
    if dps is None:
        return Metric(None, "dy_missing_dps")
    if close <= ZERO:
        return Metric(None, "dy_non_positive_price")
    return Metric(dps / close * HUNDRED, None)


def price_change_pct(close: Decimal, previous_close: Decimal | None) -> Metric:
    """Session change = (close − previous close) ÷ previous close × 100, in percentage points.

    Flags: ``no_previous_close`` (previous close ``None``, as on a counter's first session) and
    ``non_positive_previous_close`` (previous close <= 0, a guard the ``close_price > 0`` database
    check normally makes unreachable); each leaves the change ``None``.

    Raises:
        ValueError: If ``close`` or ``previous_close`` is NaN or infinite.
    """
    _require_finite(close=close, previous_close=previous_close)
    if previous_close is None:
        return Metric(None, "no_previous_close")
    if previous_close <= ZERO:
        return Metric(None, "non_positive_previous_close")
    return Metric((close - previous_close) / previous_close * HUNDRED, None)


def compute_valuation(inputs: ValuationInputs) -> ValuationResult:
    """Value one counter as of ``inputs.as_of``.

    Evaluation order, which is also the order of ``flags``:

    1. Market cap = close × shares (KES), computed for every KES equity. An equity whose
       ``currency`` is not KES is refused: its market cap is ``None``, because close × shares is
       then not a KES amount and is never converted.
    2. ``stale_price`` when the close is more than :data:`STALE_PRICE_DAYS` calendar days before
       ``as_of``; the metrics are still computed.
    3. A refused (non-KES) equity has every fundamentals-based metric (EPS, P/E, P/B, EV,
       EV/EBITDA and dividend yield), every effective input and ``fundamentals_period_end``
       ``None`` with ``currency_mismatch``, whatever fundamentals it carries. For a KES equity the
       same metrics are ``None`` with ``no_fundamentals`` when there are no fundamentals, and with
       ``currency_mismatch`` when the fundamentals are not in KES (its market cap stands). A
       sector in :data:`EV_EXCLUDED_SECTORS` also gets ``ev_not_meaningful_for_banks``, because
       that policy does not depend on data.
    4. With zero shares, EPS, P/E, P/B and EV/EBITDA are ``None`` with ``zero_shares``; enterprise
       value (debt − cash) and dividend yield, which do not divide by shares, are still computed.
    5. Otherwise EPS, P/E, P/B, enterprise value, EV/EBITDA (skipped for excluded sectors, whose
       EV flag already explains it) and dividend yield, each from its primitive.

    Raises:
        ValueError: If ``inputs.shares_outstanding`` is negative, ``inputs.close_price`` is NaN or
            infinite, or a KES fundamentals figure the valuation uses (net income, equity,
            EBITDA, debt, cash or dps) is NaN or infinite, on the zero-shares and Banking paths
            too. Refused fundamentals are not used and not checked.
    """
    shares = inputs.shares_outstanding
    mc = market_cap(inputs.close_price, shares)
    flags: list[str] = []
    if (inputs.as_of - inputs.price_date).days > STALE_PRICE_DAYS:
        flags.append("stale_price")
    ev_excluded = inputs.sector in EV_EXCLUDED_SECTORS

    kes_equity = inputs.currency == ANALYTICS_CURRENCY
    fundamentals = inputs.fundamentals
    if not kes_equity or fundamentals is None or fundamentals.currency != ANALYTICS_CURRENCY:
        reason = "no_fundamentals" if kes_equity and fundamentals is None else "currency_mismatch"
        _add_flags(flags, (reason, "ev_not_meaningful_for_banks" if ev_excluded else None))
        return ValuationResult(
            ticker=inputs.ticker,
            sector=inputs.sector,
            shares_outstanding=shares,
            close_price=inputs.close_price,
            market_cap_kes=mc if kes_equity else None,
            eps_kes=None,
            pe=None,
            pb=None,
            enterprise_value_kes=None,
            ev_ebitda=None,
            dividend_yield_pct=None,
            fundamentals_period_end=None,
            net_income_kes=None,
            total_equity_kes=None,
            ebitda_kes=None,
            dps_kes=None,
            flags=flags,
        )

    # Checked up front: the zero-shares and Banking paths skip primitives that would check some.
    _require_finite(
        net_income_kes=fundamentals.net_income_kes,
        total_equity_kes=fundamentals.total_equity_kes,
        ebitda_kes=fundamentals.ebitda_kes,
        total_debt_kes=fundamentals.total_debt_kes,
        cash_and_equivalents_kes=fundamentals.cash_and_equivalents_kes,
        dps_kes=fundamentals.dps_kes,
    )
    net_income = fundamentals.net_income_kes
    zero_shares = Metric(None, "zero_shares")
    if shares == 0:
        eps_m = pe_m = pb_m = zero_shares
    else:
        eps_m = eps(net_income, shares)
        pe_m = pe_ratio(mc, net_income)
        pb_m = pb_ratio(mc, fundamentals.total_equity_kes)
    ev_m = enterprise_value(
        mc,
        fundamentals.total_debt_kes,
        fundamentals.cash_and_equivalents_kes,
        sector=inputs.sector,
    )
    if ev_excluded:
        ev_ebitda_m = Metric(None, None)
    elif shares == 0:
        ev_ebitda_m = zero_shares
    else:
        ev_ebitda_m = ev_ebitda(ev_m.value, fundamentals.ebitda_kes)
    dy_m = dividend_yield_pct(fundamentals.dps_kes, inputs.close_price)
    _add_flags(flags, (m.flag for m in (eps_m, pe_m, pb_m, ev_m, ev_ebitda_m, dy_m)))

    return ValuationResult(
        ticker=inputs.ticker,
        sector=inputs.sector,
        shares_outstanding=shares,
        close_price=inputs.close_price,
        market_cap_kes=mc,
        eps_kes=eps_m.value,
        pe=pe_m.value,
        pb=pb_m.value,
        enterprise_value_kes=ev_m.value,
        ev_ebitda=ev_ebitda_m.value,
        dividend_yield_pct=dy_m.value,
        fundamentals_period_end=fundamentals.period_end_date,
        net_income_kes=net_income,
        total_equity_kes=fundamentals.total_equity_kes,
        ebitda_kes=fundamentals.ebitda_kes,
        dps_kes=fundamentals.dps_kes,
        flags=flags,
    )


def _ratio_of_sums(pairs: Sequence[tuple[Decimal, Decimal]]) -> Decimal | None:
    """Σ numerators ÷ Σ denominators over one matched set.

    ``None`` when the set is empty or Σ denominators <= 0.
    """
    if not pairs:
        return None
    denominator = sum((pair[1] for pair in pairs), ZERO)
    if denominator <= ZERO:
        return None
    return sum((pair[0] for pair in pairs), ZERO) / denominator


def sector_aggregates(sector: str, members: Sequence[ValuationResult]) -> SectorAggregateResult:
    """Aggregate one sector's constituents, each metric over a matched eligible set.

    Numerator and denominator of every aggregate are summed over the same constituents. Only
    constituents with a market cap take part: one refused for a non-KES currency is left out of
    the market-cap total and of every eligible set (numerator and denominator alike), still counts
    in ``constituent_count``, and gives the sector the flag ``currency_mismatch``, listed first. A
    KES constituent whose fundamentals were refused (not in KES) keeps its market cap in the
    total, has no fundamentals to make it eligible, and also gives the sector
    ``currency_mismatch``. No amount is converted.

    * Market cap (KES) = Σ market cap over the constituents that have one; ``None`` when none
      does.
    * P/E (×) = Σ market cap ÷ Σ net income over members with net income; ``None`` with
      ``sector_pe_unavailable`` when that set is empty or Σ net income <= 0.
    * P/B (×) = Σ market cap ÷ Σ equity over members with equity; ``None`` with
      ``sector_pb_unavailable`` when empty or Σ equity <= 0.
    * EV/EBITDA (×) = Σ EV ÷ Σ EBITDA over members with both; ``None`` with
      ``sector_ev_ebitda_unavailable`` when empty or Σ EBITDA <= 0. A sector in
      :data:`EV_EXCLUDED_SECTORS` is always ``None`` with ``ev_not_meaningful_for_banks`` and an
      eligible count of 0.
    * Dividend yield (%, capitalisation-weighted) = Σ (dps × shares) ÷ Σ market cap × 100 over
      members with a dps; ``None`` with ``sector_dy_unavailable`` when empty or that set's market
      cap sums to 0 (every eligible member with zero shares, for example).

    The aggregates are point-in-time. Services pass every equity of the sector, valued as of one
    day: for the screener, as of its ``as_of`` and independent of its filters; for sector
    valuation bands, once per market session on which at least one member has a close.

    Args:
        sector: The sector name, which decides the EV/EBITDA policy.
        members: :func:`compute_valuation` results of the sector's constituents.

    Raises:
        ValueError: If a member's market cap, net income, equity, enterprise value, EBITDA or dps
            is NaN or infinite, which only a hand-built :class:`ValuationResult` can carry.
    """
    for m in members:
        _require_finite(
            market_cap_kes=m.market_cap_kes,
            net_income_kes=m.net_income_kes,
            total_equity_kes=m.total_equity_kes,
            enterprise_value_kes=m.enterprise_value_kes,
            ebitda_kes=m.ebitda_kes,
            dps_kes=m.dps_kes,
        )
    flags: list[str] = []
    if any(m.market_cap_kes is None or "currency_mismatch" in m.flags for m in members):
        flags.append("currency_mismatch")
    priced = [(m, m.market_cap_kes) for m in members if m.market_cap_kes is not None]
    market_cap_total = sum((cap for _, cap in priced), ZERO) if priced else None

    pe_pairs = [(cap, m.net_income_kes) for m, cap in priced if m.net_income_kes is not None]
    pe = _ratio_of_sums(pe_pairs)
    if pe is None:
        flags.append("sector_pe_unavailable")

    pb_pairs = [(cap, m.total_equity_kes) for m, cap in priced if m.total_equity_kes is not None]
    pb = _ratio_of_sums(pb_pairs)
    if pb is None:
        flags.append("sector_pb_unavailable")

    ev_pairs: list[tuple[Decimal, Decimal]] = []
    if sector in EV_EXCLUDED_SECTORS:
        ev_multiple: Decimal | None = None
        flags.append("ev_not_meaningful_for_banks")
    else:
        ev_pairs = [
            (m.enterprise_value_kes, m.ebitda_kes)
            for m, _ in priced
            if m.enterprise_value_kes is not None and m.ebitda_kes is not None
        ]
        ev_multiple = _ratio_of_sums(ev_pairs)
        if ev_multiple is None:
            flags.append("sector_ev_ebitda_unavailable")

    dy_members = [(m, cap, m.dps_kes) for m, cap in priced if m.dps_kes is not None]
    dividends = sum((dps * Decimal(m.shares_outstanding) for m, _, dps in dy_members), ZERO)
    dy_market_cap = sum((cap for _, cap, _ in dy_members), ZERO)
    dy_ratio = safe_div(dividends, dy_market_cap) if dy_members else None
    dividend_yield = None if dy_ratio is None else dy_ratio * HUNDRED
    if dividend_yield is None:
        flags.append("sector_dy_unavailable")

    return SectorAggregateResult(
        sector=sector,
        constituent_count=len(members),
        market_cap_kes=market_cap_total,
        pe=pe,
        pb=pb,
        ev_ebitda=ev_multiple,
        dividend_yield_pct=dividend_yield,
        pe_eligible_count=len(pe_pairs),
        pb_eligible_count=len(pb_pairs),
        ev_ebitda_eligible_count=len(ev_pairs),
        dy_eligible_count=len(dy_members),
        flags=flags,
    )
