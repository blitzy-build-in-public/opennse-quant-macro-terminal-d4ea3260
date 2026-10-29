"""IPO sandbox valuation: DCF, comparables, blend, sensitivity matrix and cross-field rules.

Formulas under test (``quant/dcf.py``), year ``t`` in ``1..N``:

- ``Rev_t = Rev_{t-1} * (1 + g)``; ``m_t = (margin + expansion * t / N) / 100``;
- ``EBITDA_t = Rev_t * m_t``; ``DA_t = Rev_t * da``; ``EBIT_t = EBITDA_t - DA_t``;
- ``Tax_t = max(EBIT_t, 0) * tax``; ``NOPAT_t = EBIT_t - Tax_t``;
- ``FCF_t = NOPAT_t + DA_t - Rev_t * capex - nwc * (Rev_t - Rev_{t-1})``;
- ``DF_t = 1 / (1 + WACC)^t``; ``TV = FCF_N * (1 + g_T) / (WACC - g_T)``; ``PV_TV = TV * DF_N``;
- ``EV = sum(FCF_t * DF_t) + PV_TV``; ``Equity = EV - net debt + primary``;
  ``FV = Equity / shares``;
- comparables ``FV = (multiple * Rev_0 * margin / 100 - net debt + primary) / shares``;
- blend ``w * FV_dcf + (1 - w) * FV_comps``; upside ``(blend - offer) / offer * 100``.

Two bases are used. The toy basis (revenue 1,000, growth 0, margin 20%, WACC 10%, g_T 0, N = 1,
no D&A, capex, NWC or tax, net debt 500, 100 shares, peer 8x, weight 50%, offer 10.00) is small
enough to compute by hand in each docstring. The Quickmart preset is the seeded sandbox default,
whose published reference results are asserted exactly.

Every input is a ``Decimal`` built from a string literal and every expected value is a
hand-computed literal. Formulas never round, so values are compared after a single
:func:`quantize_out` (ROUND_HALF_UP) or, where the hand computation is exact, unrounded.

Validation has two layers. Single-field bounds (zero shares, revenue above 10^14, unknown fields,
oversized sensitivity axes) are Pydantic constraints of ``IpoValuationRequest``, checked here on
the request model alone. Rules relating two fields pass that model and are reported by
``assumption_violations`` instead. The quant layer's own guards (zero shares, zero forecast years,
WACC <= g_T and a zero offer raise as caller errors) are checked as well. No database, network or
filesystem is touched.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, replace
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from opennse_terminal.quant.dcf import (
    DEFAULT_SENSITIVITY_MARGIN,
    DEFAULT_SENSITIVITY_WACC,
    MAX_SENSITIVITY_AXIS,
    DcfAssumptions,
    DcfOutput,
    ProjectionYearOutput,
    Violation,
    assumption_violations,
    blend,
    run_comparables,
    run_dcf,
    sensitivity_matrix,
    value_ipo,
)
from opennse_terminal.quant.numeric import QuantizeKind, quantize_out
from opennse_terminal.schemas.ipo import IpoValuationRequest

SENSITIVITY_AXIS_FIELDS = frozenset({"sensitivity_wacc_pct", "sensitivity_margin_pct"})

NEGATIVE_TERMINAL_FCF = "negative_terminal_fcf"
NEGATIVE_EQUITY_VALUE = "negative_equity_value"
COMPS_NON_POSITIVE_EBITDA = "comps_non_positive_ebitda"
WACC_NOT_ABOVE_TERMINAL_GROWTH = "wacc_not_above_terminal_growth"


def quickmart(**overrides: Any) -> DcfAssumptions:
    """The seeded Quickmart Supermarket preset, with some assumptions replaced."""
    base = DcfAssumptions(
        offer_price_kes=Decimal("7.50"),
        revenue_growth_pct=Decimal("10.00"),
        ebitda_margin_pct=Decimal("4.50"),
        margin_expansion_pct=Decimal("0.50"),
        wacc_pct=Decimal("18.00"),
        terminal_growth_pct=Decimal("5.00"),
        net_debt_kes=Decimal("4000000000"),
        nwc_pct_revenue=Decimal("-3.00"),
        peer_ev_ebitda_multiple=Decimal("6.00"),
        dcf_weight_pct=Decimal("50"),
        capex_pct_revenue=Decimal("2.00"),
        da_pct_revenue=Decimal("1.50"),
        tax_rate_pct=Decimal("30.00"),
        forecast_years=5,
        base_revenue_kes=Decimal("45000000000"),
        shares_outstanding=1_000_000_000,
        shares_offered=500_000_000,
        primary_proceeds_kes=Decimal("0.00"),
    )
    return replace(base, **overrides)


def toy(**overrides: Any) -> DcfAssumptions:
    """The hand-computable basis: one year, FCF = EBITDA = 1,000 x 20% = 200, with no leakage."""
    base = DcfAssumptions(
        offer_price_kes=Decimal("10.00"),
        revenue_growth_pct=Decimal("0"),
        ebitda_margin_pct=Decimal("20"),
        margin_expansion_pct=Decimal("0"),
        wacc_pct=Decimal("10"),
        terminal_growth_pct=Decimal("0"),
        net_debt_kes=Decimal("500"),
        nwc_pct_revenue=Decimal("0"),
        peer_ev_ebitda_multiple=Decimal("8"),
        dcf_weight_pct=Decimal("50"),
        capex_pct_revenue=Decimal("0"),
        da_pct_revenue=Decimal("0"),
        tax_rate_pct=Decimal("0"),
        forecast_years=1,
        base_revenue_kes=Decimal("1000"),
        shares_outstanding=100,
        shares_offered=50,
        primary_proceeds_kes=Decimal("0"),
    )
    return replace(base, **overrides)


def request_payload(**overrides: object) -> dict[str, object]:
    """The Quickmart preset as an ``IpoValuationRequest`` JSON body: decimal strings, int counts."""
    payload: dict[str, object] = {
        "offer_price_kes": "7.50",
        "revenue_growth_pct": "10.00",
        "ebitda_margin_pct": "4.50",
        "margin_expansion_pct": "0.50",
        "wacc_pct": "18.00",
        "terminal_growth_pct": "5.00",
        "net_debt_kes": "4000000000",
        "nwc_pct_revenue": "-3.00",
        "peer_ev_ebitda_multiple": "6.00",
        "dcf_weight_pct": "50",
        "capex_pct_revenue": "2.00",
        "da_pct_revenue": "1.50",
        "tax_rate_pct": "30.00",
        "forecast_years": 5,
        "base_revenue_kes": "45000000000",
        "shares_outstanding": 1_000_000_000,
        "shares_offered": 500_000_000,
        "primary_proceeds_kes": "0.00",
    }
    payload.update(overrides)
    return payload


def to_assumptions(request: IpoValuationRequest) -> DcfAssumptions:
    """The request's 18 assumption fields, one-to-one, without the optional sensitivity axes."""
    return DcfAssumptions(**request.model_dump(exclude=set(SENSITIVITY_AXIS_FIELDS)))


def rounded(value: Decimal | None, kind: QuantizeKind) -> Decimal | None:
    """The value as a response would carry it: quantized once, ROUND_HALF_UP."""
    return quantize_out(value, kind)


# ---------------------------------------------------------------------------------------------
# Toy basis: discount factor, terminal value, comparables, blend and offer metrics
# ---------------------------------------------------------------------------------------------


def test_toy_dcf_discount_factor_and_terminal_value() -> None:
    """Revenue 1,000, margin 20%, WACC 10%, g_T 0, N = 1, no D&A, capex, NWC or tax.

    FCF_1 = 200. DF_1 = 1 / 1.1 = 0.9090909090909090909090909091 (28 digits). PV FCF = 200 / 1.1
    = 181.82. TV = 200 x 1 / (0.10 - 0) = 2,000 exactly; PV TV = 2,000 / 1.1 = 1,818.18;
    EV = 2,200 / 1.1 = 2,000.00. Net debt 500: equity 1,500.00; 100 shares: DCF FV 15.00.
    """
    result = run_dcf(toy())

    assert len(result.projections) == 1
    year = result.projections[0]
    assert year.year == 1
    assert year.revenue_kes == Decimal("1000")
    assert year.ebitda_margin_pct == Decimal("20")
    assert year.ebitda_kes == Decimal("200")
    assert year.da_kes == Decimal("0")
    assert year.ebit_kes == Decimal("200")
    assert year.tax_kes == Decimal("0")
    assert year.nopat_kes == Decimal("200")
    assert year.capex_kes == Decimal("0")
    assert year.delta_nwc_kes == Decimal("0")
    assert year.fcf_kes == Decimal("200")
    assert year.discount_factor == Decimal("0.9090909090909090909090909091")
    assert rounded(year.discount_factor, "discount_factor") == Decimal("0.909091")
    assert rounded(year.pv_fcf_kes, "money") == Decimal("181.82")

    assert rounded(result.pv_explicit_fcf_kes, "money") == Decimal("181.82")
    assert result.terminal_value_kes == Decimal("2000")
    assert rounded(result.terminal_value_kes, "money") == Decimal("2000.00")
    assert rounded(result.pv_terminal_value_kes, "money") == Decimal("1818.18")
    assert rounded(result.enterprise_value_kes, "money") == Decimal("2000.00")
    assert rounded(result.equity_value_kes, "money") == Decimal("1500.00")
    assert rounded(result.fair_value_per_share_kes, "per_share") == Decimal("15.00")
    assert result.flags == []


def test_terminal_value_applies_gordon_growth() -> None:
    """g_T 2% on the toy basis: TV = 200 x 1.02 / (0.10 - 0.02) = 204 / 0.08 = 2,550 exactly.

    PV TV = 2,550 / 1.1 = 2,318.18; EV = (200 + 2,550) / 1.1 = 2,500.00.
    """
    result = run_dcf(toy(terminal_growth_pct=Decimal("2")))

    assert result.terminal_value_kes == Decimal("2550")
    assert rounded(result.pv_terminal_value_kes, "money") == Decimal("2318.18")
    assert rounded(result.enterprise_value_kes, "money") == Decimal("2500.00")


def test_revenue_growth_and_working_capital_change() -> None:
    """Growth 10% and NWC 10% of revenue on the toy basis.

    Rev_1 = 1,100; EBITDA = 220; dNWC = 0.10 x (1,100 - 1,000) = 10; FCF = 220 - 10 = 210.
    """
    year = run_dcf(
        toy(revenue_growth_pct=Decimal("10"), nwc_pct_revenue=Decimal("10"))
    ).projections[0]

    assert year.revenue_kes == Decimal("1100")
    assert year.ebitda_kes == Decimal("220")
    assert year.delta_nwc_kes == Decimal("10")
    assert year.fcf_kes == Decimal("210")


def test_toy_comparables() -> None:
    """EBITDA_0 = 1,000 x 20% = 200; EV = 8 x 200 = 1,600; equity 1,100; 100 shares: FV 11.00."""
    result = run_comparables(toy())

    assert result.ev_ebitda_multiple == Decimal("8")
    assert result.base_ebitda_kes == Decimal("200")
    assert result.enterprise_value_kes == Decimal("1600")
    assert result.equity_value_kes == Decimal("1100")
    assert result.fair_value_per_share_kes == Decimal("11")
    assert result.flags == []


def test_comparables_margin_override() -> None:
    """A 6.0% margin override on Quickmart: EBITDA_0 2,700,000,000; EV 16,200,000,000.

    Equity = 16,200,000,000 - 4,000,000,000 = 12,200,000,000; 1,000,000,000 shares: FV 12.20.
    """
    result = run_comparables(quickmart(), ebitda_margin_pct=Decimal("6.0"))

    assert result.base_ebitda_kes == Decimal("2700000000")
    assert result.enterprise_value_kes == Decimal("16200000000")
    assert result.equity_value_kes == Decimal("12200000000")
    assert result.fair_value_per_share_kes == Decimal("12.20")
    assert result.flags == []


@pytest.mark.parametrize(
    ("dcf_fv", "comps_fv", "weight_pct", "expected"),
    [
        pytest.param("15", "11", "50", "13", id="50pct:(15+11)/2=13"),
        pytest.param("15", "11", "25", "12", id="25pct:3.75+8.25=12"),
        pytest.param("15", "11", "100", "15", id="100pct:dcf_only"),
        pytest.param("15", "11", "0", "11", id="0pct:comps_only"),
        pytest.param("15", None, "50", "15", id="no_comps:dcf_unweighted"),
    ],
)
def test_blend(dcf_fv: str, comps_fv: str | None, weight_pct: str, expected: str) -> None:
    """``w x FV_dcf + (1 - w) x FV_comps`` with ``w = weight / 100``; no comps FV gives FV_dcf."""
    comps = None if comps_fv is None else Decimal(comps_fv)

    assert blend(Decimal(dcf_fv), comps, Decimal(weight_pct)) == Decimal(expected)


@pytest.mark.parametrize(
    ("primary", "dcf_fv", "comps_fv", "blended", "upside", "implied_ev_ebitda", "secondary"),
    [
        # Equity 2,000 - 500 = 1,500 and 1,600 - 500 = 1,100; implied (1,000 + 500) / 200.
        pytest.param("0", "15.00", "11.00", "13.00", "30.00", "7.50", "500.00", id="no_primary"),
        # Equity 2,000 - 500 + 100 = 1,600 and 1,200; implied (1,000 + 500 - 100) / 200 = 7.00.
        pytest.param("100", "16.00", "12.00", "14.00", "40.00", "7.00", "400.00", id="primary_100"),
    ],
)
def test_toy_value_ipo(
    primary: str,
    dcf_fv: str,
    comps_fv: str,
    blended: str,
    upside: str,
    implied_ev_ebitda: str,
    secondary: str,
) -> None:
    """Offer 10.00 for 100 shares outstanding and 50 offered; weight 50%.

    Market cap at offer 1,000.00; offer size 500.00, split into primary and the secondary rest.
    Without primary: blended (15 + 11) / 2 = 13.00 and upside (13 - 10) / 10 x 100 = 30.00%.
    The one-cell matrix at the toy's own WACC and margin repeats the headline blend.
    """
    result = value_ipo(
        toy(primary_proceeds_kes=Decimal(primary)),
        sensitivity_wacc=[Decimal("10")],
        sensitivity_margin=[Decimal("20")],
    )

    assert rounded(result.dcf.fair_value_per_share_kes, "per_share") == Decimal(dcf_fv)
    assert rounded(result.comparables.fair_value_per_share_kes, "per_share") == Decimal(comps_fv)
    assert rounded(result.blended_fair_value_per_share_kes, "per_share") == Decimal(blended)
    assert result.offer_price_kes == Decimal("10.00")
    assert rounded(result.upside_pct, "pct") == Decimal(upside)
    assert result.implied_market_cap_at_offer_kes == Decimal("1000.00")
    assert result.implied_ev_ebitda_at_offer == Decimal(implied_ev_ebitda)
    assert result.offer_size_kes == Decimal("500.00")
    assert result.primary_proceeds_kes == Decimal(primary)
    assert result.secondary_proceeds_kes == Decimal(secondary)
    [[cell]] = result.sensitivity.cells
    assert cell.fair_value_per_share_kes == result.blended_fair_value_per_share_kes
    assert cell.upside_pct == result.upside_pct
    assert result.flags == []


@pytest.mark.parametrize(
    "assumptions",
    [
        pytest.param(toy(), id="toy"),
        pytest.param(toy(primary_proceeds_kes=Decimal("100")), id="toy_primary_100"),
        pytest.param(quickmart(), id="quickmart"),
        pytest.param(quickmart(primary_proceeds_kes=Decimal("1000000000")), id="quickmart_primary"),
    ],
)
def test_valuation_output_identities(assumptions: DcfAssumptions) -> None:
    """The unit's own unrounded outputs reconcile exactly, with and without primary proceeds.

    ``EV = sum(PV_t) + PV_TV``; ``PV_TV = TV x DF_N``; ``PV_t = FCF_t x DF_t``;
    ``Equity = EV - net debt + primary`` for both the DCF and the comparables; and
    ``secondary = offer size - primary``.
    """
    result = value_ipo(assumptions)
    dcf = result.dcf
    comps = result.comparables

    assert dcf.enterprise_value_kes == dcf.pv_explicit_fcf_kes + dcf.pv_terminal_value_kes
    assert dcf.pv_explicit_fcf_kes == sum(
        (year.pv_fcf_kes for year in dcf.projections), start=Decimal("0")
    )
    assert dcf.pv_terminal_value_kes == dcf.terminal_value_kes * dcf.projections[-1].discount_factor
    for year in dcf.projections:
        assert year.pv_fcf_kes == year.fcf_kes * year.discount_factor
    assert dcf.equity_value_kes == (
        dcf.enterprise_value_kes - assumptions.net_debt_kes + assumptions.primary_proceeds_kes
    )
    assert comps.enterprise_value_kes is not None
    assert comps.equity_value_kes == (
        comps.enterprise_value_kes - assumptions.net_debt_kes + assumptions.primary_proceeds_kes
    )
    assert result.secondary_proceeds_kes == result.offer_size_kes - result.primary_proceeds_kes


# ---------------------------------------------------------------------------------------------
# Quickmart preset: published reference results
# ---------------------------------------------------------------------------------------------


def test_quickmart_reference_results() -> None:
    """Quickmart defaults: EV 9,484,146,704.50; DCF 5.48; comps 8.15; blended 6.82; upside -9.11%.

    EV = PV explicit 3,796,952,900.12 + PV TV 5,687,193,804.38, where TV = 1,610,876,025 x 1.05 /
    0.13 = 13,010,921,740.38. Equity = EV - 4,000,000,000. Comparables: EBITDA_0 = 45,000,000,000 x
    4.5% = 2,025,000,000; EV 12,150,000,000; equity 8,150,000,000. The upside comes from the
    unrounded blend 6.81707...: (6.81707 - 7.50) / 7.50 x 100 = -9.11, whereas the rounded 6.82
    would give -9.07. Market cap at offer 7,500,000,000; implied EV/EBITDA 11,500,000,000 /
    2,025,000,000 = 5.68; offer size 7.50 x 500,000,000 = 3,750,000,000, all secondary.
    """
    result = value_ipo(quickmart())
    dcf = result.dcf
    comps = result.comparables

    assert rounded(dcf.pv_explicit_fcf_kes, "money") == Decimal("3796952900.12")
    assert rounded(dcf.terminal_value_kes, "money") == Decimal("13010921740.38")
    assert rounded(dcf.pv_terminal_value_kes, "money") == Decimal("5687193804.38")
    assert rounded(dcf.enterprise_value_kes, "money") == Decimal("9484146704.50")
    assert rounded(dcf.equity_value_kes, "money") == Decimal("5484146704.50")
    assert rounded(dcf.fair_value_per_share_kes, "per_share") == Decimal("5.48")
    assert dcf.flags == []

    assert comps.ev_ebitda_multiple == Decimal("6.00")
    assert comps.base_ebitda_kes == Decimal("2025000000")
    assert comps.enterprise_value_kes == Decimal("12150000000")
    assert comps.equity_value_kes == Decimal("8150000000")
    assert rounded(comps.fair_value_per_share_kes, "per_share") == Decimal("8.15")
    assert comps.flags == []

    assert result.blended_fair_value_per_share_kes != Decimal("6.82")
    assert rounded(result.blended_fair_value_per_share_kes, "per_share") == Decimal("6.82")
    assert result.offer_price_kes == Decimal("7.50")
    assert rounded(result.upside_pct, "pct") == Decimal("-9.11")
    assert result.implied_market_cap_at_offer_kes == Decimal("7500000000")
    assert rounded(result.implied_ev_ebitda_at_offer, "ratio") == Decimal("5.68")
    assert result.offer_size_kes == Decimal("3750000000")
    assert result.primary_proceeds_kes == Decimal("0")
    assert result.secondary_proceeds_kes == Decimal("3750000000")
    assert result.flags == []


@pytest.mark.parametrize(
    (
        "year",
        "revenue",
        "margin",
        "ebitda",
        "da",
        "ebit",
        "tax",
        "nopat",
        "capex",
        "delta_nwc",
        "fcf",
        "discount_factor",
        "pv_fcf",
    ),
    [
        pytest.param(
            1,
            "49500000000",
            "4.60",
            "2277000000",
            "742500000",
            "1534500000",
            "460350000",
            "1074150000",
            "990000000",
            "-135000000",
            "961650000",
            "0.847458",
            "814957627.12",
            id="year1",
        ),
        pytest.param(
            2,
            "54450000000",
            "4.70",
            "2559150000",
            "816750000",
            "1742400000",
            "522720000",
            "1219680000",
            "1089000000",
            "-148500000",
            "1095930000",
            "0.718184",
            "787079862.11",
            id="year2",
        ),
        pytest.param(
            3,
            "59895000000",
            "4.80",
            "2874960000",
            "898425000",
            "1976535000",
            "592960500",
            "1383574500",
            "1197900000",
            "-163350000",
            "1247449500",
            "0.608631",
            "759236277.81",
            id="year3",
        ),
        pytest.param(
            4,
            "65884500000",
            "4.90",
            "3228340500",
            "988267500",
            "2240073000",
            "672021900",
            "1568051100",
            "1317690000",
            "-179685000",
            "1418313600",
            "0.515789",
            "731550376.36",
            id="year4",
        ),
        pytest.param(
            5,
            "72472950000",
            "5.00",
            "3623647500",
            "1087094250",
            "2536553250",
            "760965975",
            "1775587275",
            "1449459000",
            "-197653500",
            "1610876025",
            "0.437109",
            "704128756.73",
            id="year5",
        ),
    ],
)
def test_quickmart_projection_year(
    year: int,
    revenue: str,
    margin: str,
    ebitda: str,
    da: str,
    ebit: str,
    tax: str,
    nopat: str,
    capex: str,
    delta_nwc: str,
    fcf: str,
    discount_factor: str,
    pv_fcf: str,
) -> None:
    """Each forecast year of the preset, hand-computed from the formula table.

    Revenue grows 10% a year from 45,000,000,000; the margin climbs linearly from 4.5% by 0.5 / 5
    = 0.1 point a year; D&A 1.5% and capex 2% of revenue; tax 30% of EBIT; dNWC = -3% of the
    revenue increase (supplier credit releases cash). DF_t = 1 / 1.18^t at 6 dp; for year 1,
    FCF = 1,074,150,000 + 742,500,000 - 990,000,000 + 135,000,000 = 961,650,000 and
    PV = 961,650,000 / 1.18 = 814,957,627.12.
    """
    projection = run_dcf(quickmart()).projections[year - 1]

    assert projection.year == year
    assert projection.revenue_kes == Decimal(revenue)
    assert projection.ebitda_margin_pct == Decimal(margin)
    assert projection.ebitda_kes == Decimal(ebitda)
    assert projection.da_kes == Decimal(da)
    assert projection.ebit_kes == Decimal(ebit)
    assert projection.tax_kes == Decimal(tax)
    assert projection.nopat_kes == Decimal(nopat)
    assert projection.capex_kes == Decimal(capex)
    assert projection.delta_nwc_kes == Decimal(delta_nwc)
    assert projection.fcf_kes == Decimal(fcf)
    assert rounded(projection.discount_factor, "discount_factor") == Decimal(discount_factor)
    assert rounded(projection.pv_fcf_kes, "money") == Decimal(pv_fcf)


# ---------------------------------------------------------------------------------------------
# Sensitivity matrix: default axes, corners, custom axes and dimensions
# ---------------------------------------------------------------------------------------------


def test_default_sensitivity_matrix_axes_and_dimensions() -> None:
    """Rows WACC 16-20%, columns base margin 3.0-6.0% in 0.5 steps: 5 x 7 cells, row-major."""
    sensitivity = value_ipo(quickmart()).sensitivity

    assert sensitivity.rows == [Decimal(x) for x in ("16", "17", "18", "19", "20")]
    assert sensitivity.cols == [
        Decimal(x) for x in ("3.0", "3.5", "4.0", "4.5", "5.0", "5.5", "6.0")
    ]
    assert list(DEFAULT_SENSITIVITY_WACC) == sensitivity.rows
    assert list(DEFAULT_SENSITIVITY_MARGIN) == sensitivity.cols
    assert len(sensitivity.cells) == 5
    for wacc_pct, row in zip(sensitivity.rows, sensitivity.cells, strict=True):
        assert len(row) == 7
        for margin_pct, cell in zip(sensitivity.cols, row, strict=True):
            assert cell.wacc_pct == wacc_pct
            assert cell.ebitda_margin_pct == margin_pct
            assert cell.fair_value_per_share_kes is not None
            assert cell.upside_pct is not None
            assert cell.flags == []


@pytest.mark.parametrize(
    ("row", "col", "wacc", "margin", "fair_value", "upside"),
    [
        pytest.param(0, 0, "16", "3.0", "2.97", "-60.43", id="16pct_x_3.0pct"),
        pytest.param(0, 6, "16", "6.0", "12.50", "66.62", id="16pct_x_6.0pct"),
        pytest.param(4, 0, "20", "3.0", "2.14", "-71.51", id="20pct_x_3.0pct"),
        pytest.param(4, 6, "20", "6.0", "10.16", "35.48", id="20pct_x_6.0pct"),
        # The centre cell is the preset itself, so it repeats the headline blend and upside.
        pytest.param(2, 3, "18", "4.5", "6.82", "-9.11", id="18pct_x_4.5pct_headline"),
    ],
)
def test_default_sensitivity_matrix_cells(
    row: int, col: int, wacc: str, margin: str, fair_value: str, upside: str
) -> None:
    """Quickmart corners 2.97 / 12.50 / 2.14 / 10.16, with upside against KES 7.50.

    Each cell equals a full revaluation with that WACC and base margin and every other input
    unchanged, the 0.5-point margin expansion included: at (20%, 3.0%) the margin runs 3.1%-3.5%,
    FCF_5 = 849,910,050 and TV = 849,910,050 x 1.05 / 0.15 = 5,949,370,350, so EV is
    4,173,087,456.60 and DCF FV (EV - 4,000,000,000) / 10^9 = 0.1731; comps (6 x 1,350,000,000
    - 4,000,000,000) / 10^9 = 4.10; blended 2.1365 -> 2.14 and (2.1365 - 7.50) / 7.50 x 100 =
    -71.51%.
    """
    cell = value_ipo(quickmart()).sensitivity.cells[row][col]
    revalued = value_ipo(quickmart(wacc_pct=Decimal(wacc), ebitda_margin_pct=Decimal(margin)))

    assert cell.wacc_pct == Decimal(wacc)
    assert cell.ebitda_margin_pct == Decimal(margin)
    assert rounded(cell.fair_value_per_share_kes, "per_share") == Decimal(fair_value)
    assert rounded(cell.upside_pct, "pct") == Decimal(upside)
    assert cell.fair_value_per_share_kes == revalued.blended_fair_value_per_share_kes
    assert cell.upside_pct == revalued.upside_pct


def test_custom_sensitivity_axes() -> None:
    """Caller axes replace the defaults: one WACC (18%) by margins 4.5% and 6.0% gives 1 x 2.

    (18%, 4.5%) is the headline 6.82 / -9.11%; (18%, 6.0%) is 11.15 / +48.63%.
    """
    result = value_ipo(
        quickmart(),
        sensitivity_wacc=[Decimal("18")],
        sensitivity_margin=[Decimal("4.5"), Decimal("6.0")],
    )

    assert result.sensitivity.rows == [Decimal("18")]
    assert result.sensitivity.cols == [Decimal("4.5"), Decimal("6.0")]
    assert len(result.sensitivity.cells) == 1
    first, second = result.sensitivity.cells[0]
    assert rounded(first.fair_value_per_share_kes, "per_share") == Decimal("6.82")
    assert rounded(first.upside_pct, "pct") == Decimal("-9.11")
    assert rounded(second.fair_value_per_share_kes, "per_share") == Decimal("11.15")
    assert rounded(second.upside_pct, "pct") == Decimal("48.63")


@pytest.mark.parametrize(
    ("wacc_count", "margin_count"),
    [
        pytest.param(1, 1, id="1x1"),
        pytest.param(1, MAX_SENSITIVITY_AXIS, id="1x9"),
        pytest.param(MAX_SENSITIVITY_AXIS, 1, id="9x1"),
        pytest.param(MAX_SENSITIVITY_AXIS, MAX_SENSITIVITY_AXIS, id="9x9"),
    ],
)
def test_sensitivity_matrix_dimensions_follow_the_axes(wacc_count: int, margin_count: int) -> None:
    """Axes of 1 to 9 values give ``len(wacc) x len(margin)`` cells in axis order."""
    wacc_axis = [Decimal(str(12 + i)) for i in range(wacc_count)]
    margin_axis = [Decimal(str(3 + i)) for i in range(margin_count)]

    cells = sensitivity_matrix(quickmart(), wacc_axis, margin_axis)

    assert MAX_SENSITIVITY_AXIS == 9
    assert [[cell.wacc_pct for cell in row] for row in cells] == [
        [wacc] * margin_count for wacc in wacc_axis
    ]
    assert [[cell.ebitda_margin_pct for cell in row] for row in cells] == [
        margin_axis for _ in wacc_axis
    ]


@pytest.mark.parametrize(
    ("wacc_count", "margin_count", "message"),
    [
        pytest.param(0, 7, "wacc_values needs 1 to 9 values, got 0", id="empty_wacc"),
        pytest.param(10, 7, "wacc_values needs 1 to 9 values, got 10", id="ten_wacc"),
        pytest.param(5, 0, "margin_values needs 1 to 9 values, got 0", id="empty_margin"),
        pytest.param(5, 10, "margin_values needs 1 to 9 values, got 10", id="ten_margin"),
    ],
)
def test_sensitivity_axis_length_is_bounded(
    wacc_count: int, margin_count: int, message: str
) -> None:
    """An axis outside 1-9 values is a caller error, raised before any cell is computed."""
    wacc_axis = [Decimal(str(12 + i)) for i in range(wacc_count)]
    margin_axis = [Decimal(str(3 + i)) for i in range(margin_count)]

    with pytest.raises(ValueError, match=f"^{message}$"):
        sensitivity_matrix(quickmart(), wacc_axis, margin_axis)
    with pytest.raises(ValueError, match=f"^{message}$"):
        value_ipo(quickmart(), sensitivity_wacc=wacc_axis, sensitivity_margin=margin_axis)


def test_matrix_cell_with_wacc_not_above_terminal_growth_is_null() -> None:
    """g_T 5%: WACC 4% and 5% cells are null with ``wacc_not_above_terminal_growth``, not errors.

    The 18% row stays defined (18%, 4.5% is the headline 6.82), and the response flags carry the
    cell flag through the union.
    """
    result = value_ipo(
        quickmart(),
        sensitivity_wacc=[Decimal("4"), Decimal("5"), Decimal("18")],
        sensitivity_margin=[Decimal("4.5")],
    )
    below, equal, defined = (row[0] for row in result.sensitivity.cells)

    for cell, wacc in ((below, Decimal("4")), (equal, Decimal("5"))):
        assert cell.wacc_pct == wacc
        assert cell.ebitda_margin_pct == Decimal("4.5")
        assert cell.fair_value_per_share_kes is None
        assert cell.upside_pct is None
        assert cell.flags == [WACC_NOT_ABOVE_TERMINAL_GROWTH]
    assert rounded(defined.fair_value_per_share_kes, "per_share") == Decimal("6.82")
    assert defined.flags == []
    assert rounded(result.blended_fair_value_per_share_kes, "per_share") == Decimal("6.82")
    assert result.flags == [WACC_NOT_ABOVE_TERMINAL_GROWTH]


# ---------------------------------------------------------------------------------------------
# Cross-field rules: assumption_violations
# ---------------------------------------------------------------------------------------------


def test_quickmart_preset_has_no_violations() -> None:
    """WACC 18 > g_T 5; 500,000,000 <= 1,000,000,000 shares; primary 0 <= offer size."""
    assert assumption_violations(quickmart()) == []


@pytest.mark.parametrize(
    ("wacc", "terminal_growth", "reported"),
    [
        pytest.param("4.00", "5.00", True, id="well_below"),
        pytest.param("4.99", "5.00", True, id="below"),
        pytest.param("5.00", "5.00", True, id="equal"),
        pytest.param("5.01", "5.00", False, id="just_above"),
    ],
)
def test_wacc_not_above_terminal_growth(wacc: str, terminal_growth: str, reported: bool) -> None:
    """WACC <= g_T leaves the Gordon value undefined and is reported on ``wacc_pct``."""
    violations = assumption_violations(
        quickmart(wacc_pct=Decimal(wacc), terminal_growth_pct=Decimal(terminal_growth))
    )

    if reported:
        assert violations == [
            Violation(
                WACC_NOT_ABOVE_TERMINAL_GROWTH,
                "wacc_pct",
                f"WACC ({wacc}%) must be above the terminal growth rate ({terminal_growth}%).",
            )
        ]
    else:
        assert violations == []


@pytest.mark.parametrize(
    ("shares_offered", "reported"),
    [
        pytest.param(1_000_000_000, False, id="all_shares_offered"),
        pytest.param(1_000_000_001, True, id="one_more_than_outstanding"),
    ],
)
def test_shares_offered_exceeds_outstanding(shares_offered: int, reported: bool) -> None:
    """Offering more shares than exist is reported on ``shares_offered``; all of them is allowed.

    Primary proceeds stay 0, so the offer-size rule cannot fire alongside.
    """
    violations = assumption_violations(quickmart(shares_offered=shares_offered))

    if reported:
        assert violations == [
            Violation(
                "shares_offered_exceeds_outstanding",
                "shares_offered",
                "Shares offered (1,000,000,001) cannot exceed shares outstanding (1,000,000,000).",
            )
        ]
    else:
        assert violations == []


@pytest.mark.parametrize(
    ("offer_price", "primary", "message"),
    [
        pytest.param("7.50", "3750000000.00", None, id="exactly_offer_size"),
        pytest.param(
            "7.50",
            "3750000000.01",
            "Primary proceeds (KES 3,750,000,000.01) cannot exceed the offer size "
            "(KES 3,750,000,000.00, offer price times shares offered): the difference would "
            "be negative secondary proceeds.",
            id="one_cent_over",
        ),
        pytest.param(
            "7.123456789012345678901234567",
            "3561728394.5061728394506172835",
            None,
            id="exactly_unrounded_offer_size",
        ),
        pytest.param(
            "7.123456789012345678901234567",
            "3561728394.506172839450617284",
            "Primary proceeds (KES 3,561,728,394.506172839450617284) cannot exceed the offer size "
            "(KES 3,561,728,394.506172839450617283500000000, offer price times shares offered): "
            "the difference would be negative secondary proceeds.",
            id="5e-19_over_unrounded_offer_size",
        ),
    ],
)
def test_primary_exceeds_offer_size(offer_price: str, primary: str, message: str | None) -> None:
    """Primary proceeds above offer x 500,000,000 shares are reported; exactly equal is clear.

    Offer 7.50 gives 3,750,000,000.00, so one cent more is reported. Offer
    7.123456789012345678901234567 (28 digits) gives exactly 3,561,728,394.5061728394506172835,
    which the default 28-digit context rounds up to ...617284. Primary ...617284 therefore
    exceeds the exact offer size by 5E-19 (negative secondary proceeds) and is still reported,
    with the exact offer size in the message; primary equal to the exact product is clear.
    """
    violations = assumption_violations(
        quickmart(offer_price_kes=Decimal(offer_price), primary_proceeds_kes=Decimal(primary))
    )

    if message is None:
        assert violations == []
    else:
        assert violations == [
            Violation("primary_exceeds_offer_size", "primary_proceeds_kes", message)
        ]


def test_violations_are_reported_in_rule_order() -> None:
    """All three rules broken at once come back WACC, shares, then primary proceeds."""
    violations = assumption_violations(
        quickmart(
            wacc_pct=Decimal("5.00"),
            shares_offered=1_000_000_001,
            primary_proceeds_kes=Decimal("8000000000"),
        )
    )

    assert [(v.code, v.field) for v in violations] == [
        (WACC_NOT_ABOVE_TERMINAL_GROWTH, "wacc_pct"),
        ("shares_offered_exceeds_outstanding", "shares_offered"),
        ("primary_exceeds_offer_size", "primary_proceeds_kes"),
    ]


def test_value_ipo_raises_the_first_violation() -> None:
    """``value_ipo`` refuses assumptions that break a cross-field rule, naming the first one."""
    with pytest.raises(ValueError, match=r"^wacc_not_above_terminal_growth \(wacc_pct\): WACC"):
        value_ipo(quickmart(wacc_pct=Decimal("5.00"), shares_offered=1_000_000_001))
    with pytest.raises(ValueError, match=r"^primary_exceeds_offer_size \(primary_proceeds_kes\)"):
        value_ipo(quickmart(primary_proceeds_kes=Decimal("3750000000.01")))


# ---------------------------------------------------------------------------------------------
# Request schema: single-field bounds are Pydantic constraints, cross-field rules are not
# ---------------------------------------------------------------------------------------------


def test_request_fields_map_one_to_one_onto_the_assumptions() -> None:
    """The preset body validates, and its 18 fields are exactly those of ``DcfAssumptions``.

    Apart from the two optional sensitivity axes (null by default), the request converts
    field-for-field into the preset assumptions, so the services need no renaming layer.
    """
    request = IpoValuationRequest.model_validate(request_payload())

    assert set(IpoValuationRequest.model_fields) - SENSITIVITY_AXIS_FIELDS == {
        field.name for field in fields(DcfAssumptions)
    }
    assert set(request_payload()) == {field.name for field in fields(DcfAssumptions)}
    assert request.sensitivity_wacc_pct is None
    assert request.sensitivity_margin_pct is None
    assert to_assumptions(request) == quickmart()


@pytest.mark.parametrize(
    ("overrides", "loc", "error_type"),
    [
        pytest.param(
            {"shares_outstanding": 0},
            ("shares_outstanding",),
            "greater_than_equal",
            id="zero_shares_outstanding",
        ),
        pytest.param(
            {"shares_outstanding": 10**12 + 1},
            ("shares_outstanding",),
            "less_than_equal",
            id="shares_outstanding_above_10^12",
        ),
        pytest.param(
            {"base_revenue_kes": "0"},
            ("base_revenue_kes",),
            "greater_than",
            id="zero_base_revenue",
        ),
        pytest.param(
            {"base_revenue_kes": "100000000000000.01"},
            ("base_revenue_kes",),
            "less_than_equal",
            id="base_revenue_above_10^14",
        ),
        pytest.param(
            {"unexpected_field": "1"},
            ("unexpected_field",),
            "extra_forbidden",
            id="unknown_field",
        ),
        pytest.param(
            {"sensitivity_wacc_pct": [str(10 + i) for i in range(10)]},
            ("sensitivity_wacc_pct",),
            "too_long",
            id="ten_wacc_axis_values",
        ),
    ],
)
def test_single_field_bounds_are_request_constraints(
    overrides: dict[str, object], loc: tuple[str, ...], error_type: str
) -> None:
    """Zero or more than 10^12 shares, revenue of 0 or above 10^14 KES, an unknown field and a
    10-value sensitivity axis (1 to 9 allowed) fail on that one field in FastAPI's standard list.
    """
    with pytest.raises(ValidationError) as excinfo:
        IpoValuationRequest.model_validate(request_payload(**overrides))

    assert [(error["loc"], error["type"]) for error in excinfo.value.errors()] == [
        (loc, error_type)
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"shares_outstanding": 10**12}, id="shares_outstanding_10^12"),
        pytest.param({"base_revenue_kes": "100000000000000"}, id="base_revenue_10^14"),
        pytest.param({"shares_offered": 0}, id="nothing_offered"),
        pytest.param(
            {"sensitivity_wacc_pct": [str(10 + i) for i in range(9)]}, id="nine_wacc_axis_values"
        ),
    ],
)
def test_single_field_bounds_are_inclusive(overrides: dict[str, object]) -> None:
    """10^12 shares, revenue of exactly 10^14 KES, no shares offered and 9 axis values validate."""
    request = IpoValuationRequest.model_validate(request_payload(**overrides))

    assert assumption_violations(to_assumptions(request)) == []


@pytest.mark.parametrize(
    ("overrides", "code", "field"),
    [
        pytest.param(
            {"primary_proceeds_kes": "3750000000.01"},
            "primary_exceeds_offer_size",
            "primary_proceeds_kes",
            id="primary_one_cent_over_offer_size",
        ),
        # WACC must be at least 8.00, so equality with g_T is tested at 8.00 = 8.00.
        pytest.param(
            {"wacc_pct": "8.00", "terminal_growth_pct": "8.00"},
            WACC_NOT_ABOVE_TERMINAL_GROWTH,
            "wacc_pct",
            id="wacc_equal_to_terminal_growth",
        ),
        pytest.param(
            {"shares_offered": 1_000_000_001},
            "shares_offered_exceeds_outstanding",
            "shares_offered",
            id="shares_offered_over_outstanding",
        ),
    ],
)
def test_cross_field_rules_pass_the_request_model(
    overrides: dict[str, object], code: str, field: str
) -> None:
    """Every field is within its own bounds, so Pydantic accepts the body; the rule relating two
    fields is reported by ``assumption_violations``, which the route raises as a coded 422.
    """
    request = IpoValuationRequest.model_validate(request_payload(**overrides))

    assert [(v.code, v.field) for v in assumption_violations(to_assumptions(request))] == [
        (code, field)
    ]


# ---------------------------------------------------------------------------------------------
# Edge cases: terminal FCF, equity, comparables fallback, tax floor and the flag union
# ---------------------------------------------------------------------------------------------


def test_zero_fcf_every_year_still_defines_every_field() -> None:
    """Constant revenue, margin, D&A and capex all 1%, no expansion or NWC, no net debt.

    EBITDA_t = DA_t = Capex_t = 450,000,000, so EBIT 0, tax 0, NOPAT 0 and FCF 0.00 every year;
    TV 0, EV 0.00, equity 0.00 and FV 0.00, flagged ``negative_terminal_fcf`` (FCF_N <= 0) with
    every ``DcfResult`` field defined.
    """
    result = run_dcf(
        quickmart(
            revenue_growth_pct=Decimal("0"),
            ebitda_margin_pct=Decimal("1.00"),
            margin_expansion_pct=Decimal("0"),
            da_pct_revenue=Decimal("1.00"),
            capex_pct_revenue=Decimal("1.00"),
            nwc_pct_revenue=Decimal("0"),
            net_debt_kes=Decimal("0"),
        )
    )

    assert [p.year for p in result.projections] == [1, 2, 3, 4, 5]
    for projection in result.projections:
        assert projection.revenue_kes == Decimal("45000000000")
        assert projection.ebitda_kes == Decimal("450000000")
        assert projection.ebit_kes == Decimal("0")
        assert projection.tax_kes == Decimal("0")
        assert projection.delta_nwc_kes == Decimal("0")
        assert rounded(projection.fcf_kes, "money") == Decimal("0.00")
        assert rounded(projection.pv_fcf_kes, "money") == Decimal("0.00")
        for field in fields(ProjectionYearOutput):
            assert getattr(projection, field.name) is not None
    assert result.terminal_value_kes == Decimal("0")
    assert rounded(result.pv_explicit_fcf_kes, "money") == Decimal("0.00")
    assert rounded(result.pv_terminal_value_kes, "money") == Decimal("0.00")
    assert rounded(result.enterprise_value_kes, "money") == Decimal("0.00")
    assert rounded(result.equity_value_kes, "money") == Decimal("0.00")
    assert rounded(result.fair_value_per_share_kes, "per_share") == Decimal("0.00")
    assert result.flags == [NEGATIVE_TERMINAL_FCF]
    for field in fields(DcfOutput):
        assert getattr(result, field.name) is not None


@pytest.mark.parametrize(
    ("capex", "fcf", "terminal_value", "pv_tv", "ev", "equity", "fair_value"),
    [
        # FCF = 200 - 200 = 0: TV 0 and EV 0, equity 0 - 500.
        pytest.param("20", "0", "0", "0.00", "0.00", "-500.00", "-5.00", id="zero_terminal_fcf"),
        # FCF = 200 - 300 = -100: TV -100 / 0.10 = -1,000; EV -1,100 / 1.1 = -1,000.
        pytest.param(
            "30", "-100", "-1000", "-909.09", "-1000.00", "-1500.00", "-15.00", id="negative_fcf"
        ),
    ],
)
def test_non_positive_terminal_fcf_is_returned_with_flags(
    capex: str,
    fcf: str,
    terminal_value: str,
    pv_tv: str,
    ev: str,
    equity: str,
    fair_value: str,
) -> None:
    """FCF_N <= 0 is valued anyway: ``negative_terminal_fcf``, plus ``negative_equity_value``."""
    result = run_dcf(toy(capex_pct_revenue=Decimal(capex)))

    assert result.projections[0].fcf_kes == Decimal(fcf)
    assert result.terminal_value_kes == Decimal(terminal_value)
    assert rounded(result.pv_terminal_value_kes, "money") == Decimal(pv_tv)
    assert rounded(result.enterprise_value_kes, "money") == Decimal(ev)
    assert rounded(result.equity_value_kes, "money") == Decimal(equity)
    assert rounded(result.fair_value_per_share_kes, "per_share") == Decimal(fair_value)
    assert result.flags == [NEGATIVE_TERMINAL_FCF, NEGATIVE_EQUITY_VALUE]


def test_negative_equity_is_returned_with_flag() -> None:
    """Net debt 2,500 on the toy basis: DCF equity 2,000 - 2,500 = -500, FV -5.00.

    Comparables equity 1,600 - 2,500 = -900, FV -9.00; blended -7.00; upside (-7 - 10) / 10 x
    100 = -170.00%. Every default matrix cell also has negative equity, so the union is one flag.
    """
    result = value_ipo(toy(net_debt_kes=Decimal("2500")))

    assert rounded(result.dcf.equity_value_kes, "money") == Decimal("-500.00")
    assert rounded(result.dcf.fair_value_per_share_kes, "per_share") == Decimal("-5.00")
    assert result.dcf.flags == [NEGATIVE_EQUITY_VALUE]
    assert result.comparables.fair_value_per_share_kes == Decimal("-9")
    assert result.comparables.flags == []
    assert rounded(result.blended_fair_value_per_share_kes, "per_share") == Decimal("-7.00")
    assert rounded(result.upside_pct, "pct") == Decimal("-170.00")
    assert result.flags == [NEGATIVE_EQUITY_VALUE]


def test_quickmart_negative_equity_at_maximum_net_debt() -> None:
    """Net debt 40,000,000,000, the request maximum, on the Quickmart preset.

    DCF equity = 9,484,146,704.50 - 40,000,000,000 = -30,515,853,295.50, FV -30.52. Comparables
    equity = 12,150,000,000 - 40,000,000,000 = -27,850,000,000, FV -27.85. Blended
    (-30.5159 - 27.85) / 2 = -29.18; upside (-29.1829 - 7.50) / 7.50 x 100 = -489.11%. Implied
    EV/EBITDA at the offer (7,500,000,000 + 40,000,000,000) / 2,025,000,000 = 23.46. FCF_N stays
    positive, so ``negative_equity_value`` is the only flag, and every matrix cell repeats it.
    """
    result = value_ipo(quickmart(net_debt_kes=Decimal("40000000000")))

    assert result.dcf.equity_value_kes < Decimal("0")
    assert rounded(result.dcf.equity_value_kes, "money") == Decimal("-30515853295.50")
    assert rounded(result.dcf.fair_value_per_share_kes, "per_share") == Decimal("-30.52")
    assert result.dcf.flags == [NEGATIVE_EQUITY_VALUE]
    assert result.comparables.equity_value_kes == Decimal("-27850000000")
    assert rounded(result.comparables.fair_value_per_share_kes, "per_share") == Decimal("-27.85")
    assert rounded(result.blended_fair_value_per_share_kes, "per_share") == Decimal("-29.18")
    assert rounded(result.upside_pct, "pct") == Decimal("-489.11")
    assert rounded(result.implied_ev_ebitda_at_offer, "ratio") == Decimal("23.46")
    assert all(
        cell.flags == [NEGATIVE_EQUITY_VALUE] for row in result.sensitivity.cells for cell in row
    )
    assert result.flags == [NEGATIVE_EQUITY_VALUE]


@pytest.mark.parametrize(
    ("margin", "base_ebitda", "dcf_fv", "upside"),
    [
        # Year-1 margin 0 + 10 = 10%: FCF 100, TV 1,000, EV 1,000, equity 500.
        pytest.param("0", "0", "5.00", "-50.00", id="zero_ebitda_0"),
        # Year-1 margin -5 + 10 = 5%: FCF 50, TV 500, EV 500, equity 0.
        pytest.param("-5", "-50", "0.00", "-100.00", id="negative_ebitda_0"),
    ],
)
def test_non_positive_base_ebitda_falls_back_to_dcf(
    margin: str, base_ebitda: str, dcf_fv: str, upside: str
) -> None:
    """EBITDA_0 <= 0: comparables EV, equity and FV are null with ``comps_non_positive_ebitda``.

    A 10-point margin expansion lifts the year-1 margin above zero, so the DCF stays defined; the
    blend returns FV_dcf unweighted and the implied EV/EBITDA at the offer is null.
    """
    result = value_ipo(toy(ebitda_margin_pct=Decimal(margin), margin_expansion_pct=Decimal("10")))
    comps = result.comparables

    assert comps.base_ebitda_kes == Decimal(base_ebitda)
    assert comps.ev_ebitda_multiple == Decimal("8")
    assert comps.enterprise_value_kes is None
    assert comps.equity_value_kes is None
    assert comps.fair_value_per_share_kes is None
    assert comps.flags == [COMPS_NON_POSITIVE_EBITDA]
    assert rounded(result.dcf.fair_value_per_share_kes, "per_share") == Decimal(dcf_fv)
    assert result.blended_fair_value_per_share_kes == result.dcf.fair_value_per_share_kes
    assert rounded(result.upside_pct, "pct") == Decimal(upside)
    assert result.implied_ev_ebitda_at_offer is None
    assert result.flags == [COMPS_NON_POSITIVE_EBITDA]


@pytest.mark.parametrize(
    ("da", "ebit", "tax", "nopat", "fcf"),
    [
        # EBIT 200 - 0 = 200: tax 30% = 60, NOPAT 140, FCF 140.
        pytest.param("0", "200", "60", "140", "140", id="positive_ebit_taxed"),
        # EBIT 200 - 200 = 0: no tax, FCF = 0 + 200 = 200.
        pytest.param("20", "0", "0", "0", "200", id="zero_ebit"),
        # EBIT 200 - 300 = -100: tax floored at 0 (no credit of 30), NOPAT -100, FCF 200.
        pytest.param("30", "-100", "0", "-100", "200", id="negative_ebit_floored"),
    ],
)
def test_tax_is_floored_at_zero_on_negative_ebit(
    da: str, ebit: str, tax: str, nopat: str, fcf: str
) -> None:
    """``Tax_t = max(EBIT_t, 0) x 30%`` on the toy basis with D&A as a share of revenue."""
    year = run_dcf(toy(da_pct_revenue=Decimal(da), tax_rate_pct=Decimal("30"))).projections[0]

    assert year.ebit_kes == Decimal(ebit)
    assert year.tax_kes == Decimal(tax)
    assert year.nopat_kes == Decimal(nopat)
    assert year.fcf_kes == Decimal(fcf)


def test_flags_are_the_union_of_dcf_comparables_and_cells() -> None:
    """Capex 30% on the toy basis with WACC axis [0, 10] and margin axis [0, 20].

    DCF: FCF -100 and equity -1,500 give ``negative_terminal_fcf`` and ``negative_equity_value``.
    The WACC 0 row (not above g_T 0) is null with ``wacc_not_above_terminal_growth``. Cell (10%,
    0%) has no comparables (``comps_non_positive_ebitda``) and falls back to its DCF: FCF -300,
    EV -3,000, equity -3,500, FV -35.00 and upside -450.00%. The union keeps first-seen order
    without repeats.
    """
    result = value_ipo(
        toy(capex_pct_revenue=Decimal("30")),
        sensitivity_wacc=[Decimal("0"), Decimal("10")],
        sensitivity_margin=[Decimal("0"), Decimal("20")],
    )
    (null_zero, null_twenty), (no_comps, base) = result.sensitivity.cells

    assert result.dcf.flags == [NEGATIVE_TERMINAL_FCF, NEGATIVE_EQUITY_VALUE]
    assert result.comparables.flags == []
    assert null_zero.flags == [WACC_NOT_ABOVE_TERMINAL_GROWTH]
    assert null_twenty.flags == [WACC_NOT_ABOVE_TERMINAL_GROWTH]
    assert no_comps.flags == [
        NEGATIVE_TERMINAL_FCF,
        NEGATIVE_EQUITY_VALUE,
        COMPS_NON_POSITIVE_EBITDA,
    ]
    assert rounded(no_comps.fair_value_per_share_kes, "per_share") == Decimal("-35.00")
    assert rounded(no_comps.upside_pct, "pct") == Decimal("-450.00")
    assert base.flags == [NEGATIVE_TERMINAL_FCF, NEGATIVE_EQUITY_VALUE]
    assert base.fair_value_per_share_kes == result.blended_fair_value_per_share_kes
    assert result.flags == [
        NEGATIVE_TERMINAL_FCF,
        NEGATIVE_EQUITY_VALUE,
        WACC_NOT_ABOVE_TERMINAL_GROWTH,
        COMPS_NON_POSITIVE_EBITDA,
    ]


# ---------------------------------------------------------------------------------------------
# Caller errors: preconditions the request schema and assumption_violations normally guarantee
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("call", "message"),
    [
        pytest.param(
            lambda: run_dcf(toy(shares_outstanding=0)),
            "shares_outstanding must be at least 1, got 0",
            id="dcf_zero_shares",
        ),
        pytest.param(
            lambda: run_comparables(toy(shares_outstanding=0)),
            "shares_outstanding must be at least 1, got 0",
            id="comparables_zero_shares",
        ),
        pytest.param(
            lambda: run_dcf(toy(forecast_years=0)),
            "forecast_years must be at least 1, got 0",
            id="dcf_zero_forecast_years",
        ),
        pytest.param(
            lambda: run_dcf(toy(wacc_pct=Decimal("0"))),
            r"wacc_pct \(0\) must exceed terminal_growth_pct \(0\)",
            id="dcf_wacc_equal_terminal_growth",
        ),
        pytest.param(
            lambda: sensitivity_matrix(toy(offer_price_kes=Decimal("0"))),
            "offer_price_kes must be positive, got 0",
            id="matrix_zero_offer",
        ),
        pytest.param(
            lambda: value_ipo(toy(offer_price_kes=Decimal("0"))),
            "offer_price_kes must be positive, got 0",
            id="value_ipo_zero_offer",
        ),
    ],
)
def test_caller_errors_raise(call: Callable[[], object], message: str) -> None:
    """Zero shares, zero forecast years, WACC <= g_T and a zero offer are programmer errors."""
    with pytest.raises(ValueError, match=f"^{message}$"):
        call()
