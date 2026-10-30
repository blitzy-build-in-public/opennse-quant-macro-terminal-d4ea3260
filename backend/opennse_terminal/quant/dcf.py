"""IPO valuation sandbox: discounted cash flow, comparable multiples, blend and sensitivity matrix.

This is the only implementation of the sandbox valuation: every slider change in the terminal
posts its assumptions to the server, which recomputes here. Rates are entered in percentage points
(``Decimal("18.00")`` means 18%) and divided by 100 internally. KES amounts are whole shillings.

For year ``t`` in ``1..N`` (``forecast_years``), with ``Rev_0 = base_revenue_kes``:

===============  ==============================================================================
Revenue          ``Rev_t = Rev_{t-1} * (1 + g)``
Margin path      ``m_t = (ebitda_margin_pct + margin_expansion_pct * t / N) / 100``
EBITDA, D&A      ``EBITDA_t = Rev_t * m_t``; ``DA_t = Rev_t * da``; ``EBIT_t = EBITDA_t - DA_t``
Tax, NOPAT       ``Tax_t = max(EBIT_t, 0) * tax``; ``NOPAT_t = EBIT_t - Tax_t``
Capex, NWC       ``Capex_t = Rev_t * capex``; ``dNWC_t = nwc * (Rev_t - Rev_{t-1})``
Free cash flow   ``FCF_t = NOPAT_t + DA_t - Capex_t - dNWC_t``
Discounting      ``DF_t = 1 / (1 + WACC)^t`` (end of year); ``PV_t = FCF_t * DF_t``
Terminal value   ``TV = FCF_N * (1 + g_T) / (WACC - g_T)`` (Gordon growth); ``PV_TV = TV * DF_N``
Equity           ``EV = sum(PV_t) + PV_TV``; ``Equity = EV - net_debt_kes + primary_proceeds_kes``
Fair value       ``FV_dcf = Equity / shares_outstanding``
Comparables      ``FV_comps = (multiple * Rev_0 * ebitda_margin_pct / 100 - net debt + primary)
                 / shares``
Blend            ``FV = w * FV_dcf + (1 - w) * FV_comps`` with ``w = dcf_weight_pct / 100``
===============  ==============================================================================

Formulas run under the default ``decimal`` context, use integer exponents only and never round;
services quantize each output once with :func:`opennse_terminal.quant.numeric.quantize_out`.

Single-field bounds are Pydantic constraints on the request schema. Rules relating two fields are
checked by :func:`assumption_violations`, never by a Pydantic validator, so the route can raise the
first one as a coded 422.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Final

from opennse_terminal.quant.numeric import HUNDRED, ONE, ZERO
from opennse_terminal.quant.numeric import Violation as Violation

__all__ = [
    "DEFAULT_SENSITIVITY_MARGIN",
    "DEFAULT_SENSITIVITY_WACC",
    "MAX_SENSITIVITY_AXIS",
    "ComparablesOutput",
    "DcfAssumptions",
    "DcfOutput",
    "IpoValuationOutput",
    "ProjectionYearOutput",
    "SensitivityCellOutput",
    "SensitivityOutput",
    "Violation",
    "assumption_violations",
    "blend",
    "run_comparables",
    "run_dcf",
    "sensitivity_matrix",
    "value_ipo",
]

DEFAULT_SENSITIVITY_WACC: Final[tuple[Decimal, ...]] = tuple(
    Decimal(x) for x in ("16", "17", "18", "19", "20")
)
"""Default matrix rows: WACC in %, spanning the 16-20% range of the Quickmart valuation brief."""

DEFAULT_SENSITIVITY_MARGIN: Final[tuple[Decimal, ...]] = tuple(
    Decimal(x) for x in ("3.0", "3.5", "4.0", "4.5", "5.0", "5.5", "6.0")
)
"""Default matrix columns: base EBITDA margin in %, spanning thin supermarket margins of 3-6%."""

MAX_SENSITIVITY_AXIS: Final = 9
"""Most values a caller may supply for one sensitivity axis; at least one is required."""

_FLAG_NEGATIVE_TERMINAL_FCF: Final = "negative_terminal_fcf"
_FLAG_NEGATIVE_EQUITY_VALUE: Final = "negative_equity_value"
_FLAG_COMPS_NON_POSITIVE_EBITDA: Final = "comps_non_positive_ebitda"
_FLAG_WACC_NOT_ABOVE_TERMINAL_GROWTH: Final = "wacc_not_above_terminal_growth"


@dataclass(frozen=True)
class DcfAssumptions:
    """The sandbox inputs. Field names equal those of the ``IpoValuationRequest`` schema."""

    offer_price_kes: Decimal
    """IPO offer price, KES per share."""
    revenue_growth_pct: Decimal
    """Annual revenue growth ``g``, %."""
    ebitda_margin_pct: Decimal
    """Base EBITDA margin, %; also the margin of ``EBITDA_0`` for the comparables."""
    margin_expansion_pct: Decimal
    """Margin change reached by year N, percentage points, applied linearly."""
    wacc_pct: Decimal
    """Weighted average cost of capital, %."""
    terminal_growth_pct: Decimal
    """Perpetual growth ``g_T`` after year N, %."""
    net_debt_kes: Decimal
    """Debt less cash, KES; negative for a net cash position."""
    nwc_pct_revenue: Decimal
    """Net working capital as % of revenue; negative for supplier-financed retailers."""
    peer_ev_ebitda_multiple: Decimal
    """Peer EV/EBITDA multiple applied to ``EBITDA_0``, times."""
    dcf_weight_pct: Decimal
    """Weight of the DCF value in the blend, %; the comparables take the remainder."""
    capex_pct_revenue: Decimal
    """Capital expenditure as % of revenue."""
    da_pct_revenue: Decimal
    """Depreciation and amortisation as % of revenue."""
    tax_rate_pct: Decimal
    """Corporate tax rate on positive EBIT, %."""
    forecast_years: int
    """Explicit forecast years N."""
    base_revenue_kes: Decimal
    """Revenue of the base year ``Rev_0``, KES."""
    shares_outstanding: int
    """Shares in issue after the offer."""
    shares_offered: int
    """Shares sold in the offer, primary and secondary together."""
    primary_proceeds_kes: Decimal
    """New cash raised by the issuer in the offer, KES."""


@dataclass(frozen=True)
class ProjectionYearOutput:
    """One explicit forecast year. KES amounts unrounded; fields match ``ProjectionYear``."""

    year: int
    revenue_kes: Decimal
    ebitda_margin_pct: Decimal
    """``m_t * 100``, %."""
    ebitda_kes: Decimal
    da_kes: Decimal
    ebit_kes: Decimal
    tax_kes: Decimal
    nopat_kes: Decimal
    capex_kes: Decimal
    delta_nwc_kes: Decimal
    fcf_kes: Decimal
    discount_factor: Decimal
    """``1 / (1 + WACC)^t``, dimensionless."""
    pv_fcf_kes: Decimal


@dataclass(frozen=True)
class DcfOutput:
    """DCF result; every value is defined. Fields match the ``DcfResult`` schema."""

    projections: list[ProjectionYearOutput]
    pv_explicit_fcf_kes: Decimal
    """``sum(PV_t)``, KES."""
    terminal_value_kes: Decimal
    """Gordon terminal value ``TV`` at year N, KES."""
    pv_terminal_value_kes: Decimal
    """``TV * DF_N``, KES."""
    enterprise_value_kes: Decimal
    equity_value_kes: Decimal
    fair_value_per_share_kes: Decimal
    """KES per share."""
    flags: list[str]


@dataclass(frozen=True)
class ComparablesOutput:
    """Comparable-multiples result; values are ``None`` when ``EBITDA_0 <= 0``."""

    ev_ebitda_multiple: Decimal
    base_ebitda_kes: Decimal
    """``EBITDA_0 = Rev_0 * margin / 100``, KES."""
    enterprise_value_kes: Decimal | None
    equity_value_kes: Decimal | None
    fair_value_per_share_kes: Decimal | None
    """KES per share."""
    flags: list[str]


@dataclass(frozen=True)
class SensitivityCellOutput:
    """Blended fair value for one (WACC, base margin) pair, with every other input unchanged."""

    wacc_pct: Decimal
    ebitda_margin_pct: Decimal
    fair_value_per_share_kes: Decimal | None
    """KES per share; ``None`` when the cell's WACC is not above terminal growth."""
    upside_pct: Decimal | None
    """``(FV - offer) / offer * 100``, %."""
    flags: list[str]


@dataclass(frozen=True)
class SensitivityOutput:
    """The WACC x base-margin matrix: ``cells[i][j]`` is ``rows[i]`` by ``cols[j]``."""

    rows: list[Decimal]
    """WACC values, %."""
    cols: list[Decimal]
    """Base EBITDA margin values, %."""
    cells: list[list[SensitivityCellOutput]]


@dataclass(frozen=True)
class IpoValuationOutput:
    """The full sandbox valuation, unrounded. Fields match the ``IpoValuationResponse`` schema."""

    dcf: DcfOutput
    comparables: ComparablesOutput
    blended_fair_value_per_share_kes: Decimal
    offer_price_kes: Decimal
    upside_pct: Decimal
    """``(blended - offer) / offer * 100``, %."""
    implied_market_cap_at_offer_kes: Decimal
    implied_ev_ebitda_at_offer: Decimal | None
    """``(market cap at offer + net debt - primary) / EBITDA_0``; ``None`` if ``EBITDA_0 <= 0``."""
    offer_size_kes: Decimal
    primary_proceeds_kes: Decimal
    secondary_proceeds_kes: Decimal
    sensitivity: SensitivityOutput
    flags: list[str]
    """Union of the DCF, comparables and cell flags, in first-seen order."""


def assumption_violations(a: DcfAssumptions) -> list[Violation]:
    """Return every cross-field rule ``a`` breaks, in reporting order; empty when all hold.

    1. ``wacc_pct`` must exceed ``terminal_growth_pct``, or the Gordon terminal value is undefined
       (``wacc_not_above_terminal_growth`` on ``wacc_pct``).
    2. ``shares_offered`` must not exceed ``shares_outstanding``
       (``shares_offered_exceeds_outstanding`` on ``shares_offered``).
    3. ``primary_proceeds_kes`` must not exceed the offer size ``offer_price_kes * shares_offered``,
       since the excess would be negative secondary proceeds
       (``primary_exceeds_offer_size`` on ``primary_proceeds_kes``). The offer size is the exact
       product, not one rounded to the default 28-digit context, so an excess smaller than that
       context's rounding unit is still reported, and the message states the exact offer size.

    Single-field bounds are not rechecked here; the request schema enforces them.
    """
    violations: list[Violation] = []
    if a.wacc_pct <= a.terminal_growth_pct:
        violations.append(
            Violation(
                "wacc_not_above_terminal_growth",
                "wacc_pct",
                f"WACC ({a.wacc_pct:f}%) must be above the terminal growth rate "
                f"({a.terminal_growth_pct:f}%).",
            )
        )
    if a.shares_offered > a.shares_outstanding:
        violations.append(
            Violation(
                "shares_offered_exceeds_outstanding",
                "shares_offered",
                f"Shares offered ({a.shares_offered:,}) cannot exceed shares outstanding "
                f"({a.shares_outstanding:,}).",
            )
        )
    offer_size = _exact_offer_size(a)
    if a.primary_proceeds_kes > offer_size:
        violations.append(
            Violation(
                "primary_exceeds_offer_size",
                "primary_proceeds_kes",
                f"Primary proceeds (KES {a.primary_proceeds_kes:,f}) cannot exceed the offer size "
                f"(KES {offer_size:,f}, offer price times shares offered): the difference would "
                "be negative secondary proceeds.",
            )
        )
    return violations


def _exact_offer_size(a: DcfAssumptions) -> Decimal:
    """``offer_price_kes * shares_offered``, KES, computed exactly for the cross-field check.

    The default 28-digit context can round this product up, hiding a smaller excess of primary
    proceeds. A product of a ``p``-digit coefficient and a ``c``-digit integer has at most
    ``p + c`` digits, so a context of that precision holds it exactly. The valuation formulas,
    ``offer_size_kes`` included, still run under the default context.
    """
    precision = len(a.offer_price_kes.as_tuple().digits) + len(str(abs(a.shares_offered)))
    with localcontext(prec=precision):
        return a.offer_price_kes * a.shares_offered


def _merge_flags(*groups: Sequence[str]) -> list[str]:
    """Concatenate flag groups, dropping repeats and keeping first-seen order."""
    return list(dict.fromkeys(flag for group in groups for flag in group))


def _require_shares(a: DcfAssumptions) -> Decimal:
    """Return ``shares_outstanding`` as a divisor, rejecting a count below 1 as a caller error."""
    if a.shares_outstanding < 1:
        raise ValueError(f"shares_outstanding must be at least 1, got {a.shares_outstanding}")
    return Decimal(a.shares_outstanding)


def _upside_pct(fair_value: Decimal, offer_price: Decimal) -> Decimal:
    """``(fair_value - offer) / offer * 100``, %; a non-positive offer is a caller error."""
    if offer_price <= ZERO:
        raise ValueError(f"offer_price_kes must be positive, got {offer_price}")
    return (fair_value - offer_price) / offer_price * HUNDRED


def _dcf(a: DcfAssumptions, *, wacc_pct: Decimal, base_margin_pct: Decimal) -> DcfOutput:
    """Run the DCF of the module formulas with ``wacc_pct`` and ``base_margin_pct`` substituted.

    The sensitivity matrix reuses this with each axis value; every other input comes from ``a``,
    and the margin expansion is still applied on top of ``base_margin_pct``.

    Raises:
        ValueError: ``wacc_pct <= a.terminal_growth_pct``, ``a.forecast_years < 1`` or
            ``a.shares_outstanding < 1``. Callers prevent these: the route through
            :func:`assumption_violations` and the schema bounds, the matrix by nulling the cell.
    """
    if wacc_pct <= a.terminal_growth_pct:
        raise ValueError(
            f"wacc_pct ({wacc_pct}) must exceed terminal_growth_pct ({a.terminal_growth_pct})"
        )
    if a.forecast_years < 1:
        raise ValueError(f"forecast_years must be at least 1, got {a.forecast_years}")
    shares = _require_shares(a)

    growth = a.revenue_growth_pct / HUNDRED
    wacc = wacc_pct / HUNDRED
    terminal_growth = a.terminal_growth_pct / HUNDRED
    nwc = a.nwc_pct_revenue / HUNDRED
    da = a.da_pct_revenue / HUNDRED
    capex = a.capex_pct_revenue / HUNDRED
    tax = a.tax_rate_pct / HUNDRED
    n = a.forecast_years

    projections: list[ProjectionYearOutput] = []
    pv_explicit = ZERO
    rev_prev = a.base_revenue_kes
    fcf_t = ZERO
    df_t = ONE
    for t in range(1, n + 1):
        rev_t = rev_prev * (ONE + growth)
        margin_pct_t = base_margin_pct + a.margin_expansion_pct * t / n
        ebitda_t = rev_t * (margin_pct_t / HUNDRED)
        da_t = rev_t * da
        ebit_t = ebitda_t - da_t
        tax_t = max(ebit_t, ZERO) * tax
        nopat_t = ebit_t - tax_t
        capex_t = rev_t * capex
        delta_nwc_t = nwc * (rev_t - rev_prev)
        fcf_t = nopat_t + da_t - capex_t - delta_nwc_t
        df_t = ONE / (ONE + wacc) ** t
        pv_t = fcf_t * df_t
        pv_explicit += pv_t
        projections.append(
            ProjectionYearOutput(
                year=t,
                revenue_kes=rev_t,
                ebitda_margin_pct=margin_pct_t,
                ebitda_kes=ebitda_t,
                da_kes=da_t,
                ebit_kes=ebit_t,
                tax_kes=tax_t,
                nopat_kes=nopat_t,
                capex_kes=capex_t,
                delta_nwc_kes=delta_nwc_t,
                fcf_kes=fcf_t,
                discount_factor=df_t,
                pv_fcf_kes=pv_t,
            )
        )
        rev_prev = rev_t

    # After the loop fcf_t and df_t hold year N's values.
    terminal_value = fcf_t * (ONE + terminal_growth) / (wacc - terminal_growth)
    pv_tv = terminal_value * df_t
    enterprise_value = pv_explicit + pv_tv
    equity_value = enterprise_value - a.net_debt_kes + a.primary_proceeds_kes

    flags: list[str] = []
    if fcf_t <= ZERO:
        flags.append(_FLAG_NEGATIVE_TERMINAL_FCF)
    if equity_value < ZERO:
        flags.append(_FLAG_NEGATIVE_EQUITY_VALUE)

    return DcfOutput(
        projections=projections,
        pv_explicit_fcf_kes=pv_explicit,
        terminal_value_kes=terminal_value,
        pv_terminal_value_kes=pv_tv,
        enterprise_value_kes=enterprise_value,
        equity_value_kes=equity_value,
        fair_value_per_share_kes=equity_value / shares,
        flags=flags,
    )


def run_dcf(a: DcfAssumptions) -> DcfOutput:
    """Value the issuer by DCF at its own WACC and base EBITDA margin.

    KES amounts are whole shillings, ``fair_value_per_share_kes`` is KES per share and the
    projection margins are %. Flags: ``negative_terminal_fcf`` when ``FCF_N <= 0`` and
    ``negative_equity_value`` when equity is below zero; the values are returned regardless.

    Raises:
        ValueError: as :func:`_dcf`, for ``wacc_pct <= terminal_growth_pct``, ``forecast_years < 1``
            or ``shares_outstanding < 1``.
    """
    return _dcf(a, wacc_pct=a.wacc_pct, base_margin_pct=a.ebitda_margin_pct)


def run_comparables(
    a: DcfAssumptions, *, ebitda_margin_pct: Decimal | None = None
) -> ComparablesOutput:
    """Value the issuer at the peer EV/EBITDA multiple of its base-year EBITDA.

    ``EBITDA_0 = base_revenue_kes * margin / 100``, where ``margin`` is ``ebitda_margin_pct``
    (%) when given and ``a.ebitda_margin_pct`` otherwise. ``EV = peer_ev_ebitda_multiple *
    EBITDA_0``; ``Equity = EV - net_debt_kes + primary_proceeds_kes``; ``FV = Equity /
    shares_outstanding`` in KES per share. When ``EBITDA_0 <= 0`` the multiple is meaningless, so
    EV, equity and FV are ``None`` with ``comps_non_positive_ebitda``.

    Raises:
        ValueError: ``a.shares_outstanding < 1``.
    """
    shares = _require_shares(a)
    margin_pct = a.ebitda_margin_pct if ebitda_margin_pct is None else ebitda_margin_pct
    base_ebitda = a.base_revenue_kes * margin_pct / HUNDRED
    if base_ebitda <= ZERO:
        return ComparablesOutput(
            ev_ebitda_multiple=a.peer_ev_ebitda_multiple,
            base_ebitda_kes=base_ebitda,
            enterprise_value_kes=None,
            equity_value_kes=None,
            fair_value_per_share_kes=None,
            flags=[_FLAG_COMPS_NON_POSITIVE_EBITDA],
        )
    enterprise_value = a.peer_ev_ebitda_multiple * base_ebitda
    equity_value = enterprise_value - a.net_debt_kes + a.primary_proceeds_kes
    return ComparablesOutput(
        ev_ebitda_multiple=a.peer_ev_ebitda_multiple,
        base_ebitda_kes=base_ebitda,
        enterprise_value_kes=enterprise_value,
        equity_value_kes=equity_value,
        fair_value_per_share_kes=equity_value / shares,
        flags=[],
    )


def blend(dcf_fv: Decimal, comps_fv: Decimal | None, weight_pct: Decimal) -> Decimal:
    """Weight the two fair values per share: ``w * dcf_fv + (1 - w) * comps_fv``, KES per share.

    ``w = weight_pct / 100``. Without a comparables value (``comps_fv is None``) the DCF value is
    returned unweighted.
    """
    if comps_fv is None:
        return dcf_fv
    weight = weight_pct / HUNDRED
    return weight * dcf_fv + (ONE - weight) * comps_fv


def _check_axis(name: str, values: Sequence[Decimal]) -> None:
    if not 1 <= len(values) <= MAX_SENSITIVITY_AXIS:
        raise ValueError(f"{name} needs 1 to {MAX_SENSITIVITY_AXIS} values, got {len(values)}")


def sensitivity_matrix(
    a: DcfAssumptions,
    wacc_values: Sequence[Decimal] = DEFAULT_SENSITIVITY_WACC,
    margin_values: Sequence[Decimal] = DEFAULT_SENSITIVITY_MARGIN,
) -> list[list[SensitivityCellOutput]]:
    """Blended fair value per share for each WACC (row, %) by base EBITDA margin (column, %).

    Each cell is ``blend(DCF FV at (w, m), comparables FV at m, dcf_weight_pct)`` with every other
    input of ``a`` unchanged and the margin expansion still applied, plus ``upside_pct`` against
    the offer price. Cell flags are the DCF and comparables flags, de-duplicated. A cell whose WACC
    is not above terminal growth has no Gordon value, so its fair value and upside are ``None``
    with ``wacc_not_above_terminal_growth`` instead of an error.

    Raises:
        ValueError: an axis holds fewer than 1 or more than :data:`MAX_SENSITIVITY_AXIS` values,
            ``a.forecast_years < 1``, ``a.shares_outstanding < 1`` or ``a.offer_price_kes <= 0``.
    """
    _check_axis("wacc_values", wacc_values)
    _check_axis("margin_values", margin_values)
    comps_by_margin = [run_comparables(a, ebitda_margin_pct=m) for m in margin_values]
    matrix: list[list[SensitivityCellOutput]] = []
    for wacc_pct in wacc_values:
        row: list[SensitivityCellOutput] = []
        for margin_pct, comps in zip(margin_values, comps_by_margin, strict=True):
            if wacc_pct <= a.terminal_growth_pct:
                row.append(
                    SensitivityCellOutput(
                        wacc_pct=wacc_pct,
                        ebitda_margin_pct=margin_pct,
                        fair_value_per_share_kes=None,
                        upside_pct=None,
                        flags=[_FLAG_WACC_NOT_ABOVE_TERMINAL_GROWTH],
                    )
                )
                continue
            dcf = _dcf(a, wacc_pct=wacc_pct, base_margin_pct=margin_pct)
            fair_value = blend(
                dcf.fair_value_per_share_kes, comps.fair_value_per_share_kes, a.dcf_weight_pct
            )
            row.append(
                SensitivityCellOutput(
                    wacc_pct=wacc_pct,
                    ebitda_margin_pct=margin_pct,
                    fair_value_per_share_kes=fair_value,
                    upside_pct=_upside_pct(fair_value, a.offer_price_kes),
                    flags=_merge_flags(dcf.flags, comps.flags),
                )
            )
        matrix.append(row)
    return matrix


def value_ipo(
    a: DcfAssumptions,
    *,
    sensitivity_wacc: Sequence[Decimal] | None = None,
    sensitivity_margin: Sequence[Decimal] | None = None,
) -> IpoValuationOutput:
    """Value an IPO offer: DCF, comparables, their blend, offer metrics and the sensitivity matrix.

    ``upside_pct = (blended - offer) / offer * 100`` (%);
    ``implied_market_cap_at_offer_kes = offer * shares_outstanding``;
    ``implied_ev_ebitda_at_offer = (implied market cap + net debt - primary) / EBITDA_0``, or
    ``None`` when ``EBITDA_0 <= 0``; ``offer_size_kes = offer * shares_offered``;
    ``secondary_proceeds_kes = offer size - primary``. A ``None`` axis takes its default
    (:data:`DEFAULT_SENSITIVITY_WACC`, :data:`DEFAULT_SENSITIVITY_MARGIN`). ``flags`` is the union
    of the DCF, comparables and every cell's flags in first-seen order.

    Raises:
        ValueError: :func:`assumption_violations` reports a violation (callers check and report it
            first), or any precondition of :func:`sensitivity_matrix` fails.
    """
    violations = assumption_violations(a)
    if violations:
        first = violations[0]
        raise ValueError(f"{first.code} ({first.field}): {first.message}")

    dcf = run_dcf(a)
    comparables = run_comparables(a)
    blended = blend(
        dcf.fair_value_per_share_kes, comparables.fair_value_per_share_kes, a.dcf_weight_pct
    )

    wacc_axis = DEFAULT_SENSITIVITY_WACC if sensitivity_wacc is None else sensitivity_wacc
    margin_axis = DEFAULT_SENSITIVITY_MARGIN if sensitivity_margin is None else sensitivity_margin
    cells = sensitivity_matrix(a, wacc_axis, margin_axis)

    market_cap_at_offer = a.offer_price_kes * a.shares_outstanding
    base_ebitda = comparables.base_ebitda_kes
    implied_ev_ebitda = (
        (market_cap_at_offer + a.net_debt_kes - a.primary_proceeds_kes) / base_ebitda
        if base_ebitda > ZERO
        else None
    )
    offer_size = a.offer_price_kes * a.shares_offered

    return IpoValuationOutput(
        dcf=dcf,
        comparables=comparables,
        blended_fair_value_per_share_kes=blended,
        offer_price_kes=a.offer_price_kes,
        upside_pct=_upside_pct(blended, a.offer_price_kes),
        implied_market_cap_at_offer_kes=market_cap_at_offer,
        implied_ev_ebitda_at_offer=implied_ev_ebitda,
        offer_size_kes=offer_size,
        primary_proceeds_kes=a.primary_proceeds_kes,
        secondary_proceeds_kes=offer_size - a.primary_proceeds_kes,
        sensitivity=SensitivityOutput(rows=list(wacc_axis), cols=list(margin_axis), cells=cells),
        flags=_merge_flags(
            dcf.flags, comparables.flags, *(cell.flags for row in cells for cell in row)
        ),
    )
