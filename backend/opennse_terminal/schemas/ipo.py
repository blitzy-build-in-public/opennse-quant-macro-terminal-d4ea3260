"""Request and response schemas of the IPO valuation sandbox.

``GET /ipo/scenarios`` returns :class:`IpoScenario` presets and ``POST /ipo/valuation`` takes an
:class:`IpoValuationRequest` and returns an :class:`IpoValuationResponse`.

The request's 18 assumption fields carry the same names as ``quant.dcf.DcfAssumptions``, to
which ``services/ipo_service.py`` converts the request one-to-one. Rates are percentage points
(``"18.00"`` means 18%), ``*_kes`` amounts are whole Kenyan shillings and share counts are
integers. Requests accept each decimal as a JSON number or string; the frontend sends
fixed-decimal strings. Responses serialize every ``Decimal`` as a JSON string, already quantized
by the service: KES and per-share KES to 2 dp, percentages and multiples to 2 dp, discount factors
to 6 dp.

Only single-field bounds are declared here, so their failures use FastAPI's standard 422 list.
Rules that relate fields are checked by ``quant.dcf.assumption_violations`` and raised by the
service as a coded 422, never by a Pydantic validator:

* ``wacc_pct <= terminal_growth_pct`` is ``wacc_not_above_terminal_growth`` on ``wacc_pct``;
* ``shares_offered > shares_outstanding`` is ``shares_offered_exceeds_outstanding`` on
  ``shares_offered``;
* ``primary_proceeds_kes > offer_price_kes * shares_offered`` is ``primary_exceeds_offer_size``
  on ``primary_proceeds_kes``.
"""

import uuid
from decimal import Decimal
from typing import Annotated, Final

from pydantic import ConfigDict, Field

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.schemas.common import (
    PositiveShareCount,
    RequestDecimal,
    ShareCount,
    TerminalRequestModel,
    TerminalResponseModel,
)

__all__ = [
    "ComparablesResult",
    "DcfResult",
    "IpoScenario",
    "IpoValuationRequest",
    "IpoValuationResponse",
    "MarginAxisValue",
    "ProjectionYear",
    "RiskScenario",
    "SensitivityCell",
    "SensitivityMatrixData",
    "WaccAxisValue",
]

_MIN_OFFER_PRICE_KES: Final = Decimal("0.50")
_MAX_OFFER_PRICE_KES: Final = Decimal("50.00")
_MIN_WACC_PCT: Final = Decimal("8.00")
_MAX_WACC_PCT: Final = Decimal("30.00")
_MIN_EBITDA_MARGIN_PCT: Final = Decimal("0.50")
_MAX_EBITDA_MARGIN_PCT: Final = Decimal("15.00")
_MAX_BASE_REVENUE_KES: Final = Decimal("100000000000000")

# The largest offer size the single-field bounds allow (maximum offer price times the maximum
# share count, 5 x 10^13 KES). It only bounds magnitude: any primary proceeds the cross-field
# rule primary_exceeds_offer_size would accept also pass this bound.
_MAX_PRIMARY_PROCEEDS_KES: Final = _MAX_OFFER_PRICE_KES * MAX_SHARE_COUNT

_MAX_SENSITIVITY_AXIS: Final = 9

# Axis elements reuse the scalar ranges of wacc_pct and ebitda_margin_pct. Without them a WACC
# element such as -100 would make the discount base (1 + WACC)^t zero.
WaccAxisValue = Annotated[RequestDecimal, Field(ge=_MIN_WACC_PCT, le=_MAX_WACC_PCT)]
"""One sensitivity-matrix row: a WACC in %, 8.00 to 30.00."""

MarginAxisValue = Annotated[
    RequestDecimal, Field(ge=_MIN_EBITDA_MARGIN_PCT, le=_MAX_EBITDA_MARGIN_PCT)
]
"""One sensitivity-matrix column: a base EBITDA margin in %, 0.50 to 15.00."""


# ---------------------------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------------------------


class IpoValuationRequest(TerminalRequestModel):
    """Body of ``POST /ipo/valuation``: every sandbox assumption, all required.

    Each field's range is a single-field bound. The schema example is the seeded Quickmart
    Supermarket preset; it documents the request and is never applied as a default.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "offer_price_kes": "7.50",
                    "revenue_growth_pct": "10.00",
                    "ebitda_margin_pct": "4.50",
                    "margin_expansion_pct": "0.50",
                    "wacc_pct": "18.00",
                    "terminal_growth_pct": "5.00",
                    "net_debt_kes": "4000000000.00",
                    "nwc_pct_revenue": "-3.00",
                    "peer_ev_ebitda_multiple": "6.00",
                    "dcf_weight_pct": "50",
                    "capex_pct_revenue": "2.00",
                    "da_pct_revenue": "1.50",
                    "tax_rate_pct": "30.00",
                    "forecast_years": 5,
                    "base_revenue_kes": "45000000000.00",
                    "shares_outstanding": 1000000000,
                    "shares_offered": 500000000,
                    "primary_proceeds_kes": "0.00",
                }
            ]
        },
    )

    offer_price_kes: RequestDecimal = Field(
        ge=_MIN_OFFER_PRICE_KES,
        le=_MAX_OFFER_PRICE_KES,
        description="IPO offer price, KES per share, 0.50 to 50.00.",
    )
    revenue_growth_pct: RequestDecimal = Field(
        ge=Decimal("-20.00"),
        le=Decimal("40.00"),
        description="Annual revenue growth g over the forecast, % per year, -20.00 to 40.00.",
    )
    ebitda_margin_pct: RequestDecimal = Field(
        ge=_MIN_EBITDA_MARGIN_PCT,
        le=_MAX_EBITDA_MARGIN_PCT,
        description=(
            "Base-year EBITDA margin, % of revenue, 0.50 to 15.00; also sets EBITDA_0 for the "
            "comparables."
        ),
    )
    margin_expansion_pct: RequestDecimal = Field(
        ge=Decimal("-3.00"),
        le=Decimal("3.00"),
        description=(
            "Change in EBITDA margin reached by forecast year N, applied linearly, percentage "
            "points, -3.00 to 3.00."
        ),
    )
    wacc_pct: RequestDecimal = Field(
        ge=_MIN_WACC_PCT,
        le=_MAX_WACC_PCT,
        description=(
            "Weighted average cost of capital used to discount free cash flows, %, 8.00 to "
            "30.00; must exceed terminal_growth_pct (coded 422 wacc_not_above_terminal_growth)."
        ),
    )
    terminal_growth_pct: RequestDecimal = Field(
        ge=Decimal("0.00"),
        le=Decimal("10.00"),
        description=(
            "Perpetual growth g_T after year N in the Gordon terminal value, %, 0.00 to 10.00."
        ),
    )
    net_debt_kes: RequestDecimal = Field(
        ge=Decimal("-20000000000"),
        le=Decimal("40000000000"),
        description=(
            "Debt less cash before the offer, KES, -20,000,000,000 to 40,000,000,000; negative "
            "for a net cash position."
        ),
    )
    nwc_pct_revenue: RequestDecimal = Field(
        ge=Decimal("-15.00"),
        le=Decimal("15.00"),
        description=(
            "Net working capital invested per shilling of incremental revenue, %, -15.00 to "
            "15.00; negative for supplier-financed working capital."
        ),
    )
    peer_ev_ebitda_multiple: RequestDecimal = Field(
        ge=Decimal("1.00"),
        le=Decimal("20.00"),
        description="Peer EV/EBITDA multiple applied to EBITDA_0, times (x), 1.00 to 20.00.",
    )
    dcf_weight_pct: RequestDecimal = Field(
        ge=Decimal("0"),
        le=Decimal("100"),
        description="Weight of the DCF value in the blended fair value, %, 0 to 100.",
    )
    capex_pct_revenue: RequestDecimal = Field(
        ge=Decimal("0.00"),
        le=Decimal("10.00"),
        description="Capital expenditure, % of revenue, 0.00 to 10.00.",
    )
    da_pct_revenue: RequestDecimal = Field(
        ge=Decimal("0.00"),
        le=Decimal("10.00"),
        description="Depreciation and amortisation, % of revenue, 0.00 to 10.00.",
    )
    tax_rate_pct: RequestDecimal = Field(
        ge=Decimal("0.00"),
        le=Decimal("50.00"),
        description="Corporate tax rate on positive EBIT, %, 0.00 to 50.00 (Kenya: 30.00).",
    )
    forecast_years: int = Field(
        ge=3,
        le=10,
        description="Explicit forecast horizon N, whole years, 3 to 10.",
    )
    base_revenue_kes: RequestDecimal = Field(
        gt=Decimal("0"),
        le=_MAX_BASE_REVENUE_KES,
        description="Base-year revenue Rev_0, KES, greater than 0 and at most 100,000,000,000,000.",
    )
    shares_outstanding: PositiveShareCount = Field(
        description=f"Shares in issue after the offer, shares, 1 to {MAX_SHARE_COUNT:,}.",
    )
    # The lower bound is 0, the sandbox range "0 to shares_outstanding". The >= 1 CHECK on
    # terminal_ipo_scenarios.shares_offered applies to stored presets, not to this request.
    shares_offered: ShareCount = Field(
        description=(
            f"Shares sold in the offer, primary and secondary together, shares, 0 to "
            f"{MAX_SHARE_COUNT:,}; must not exceed shares_outstanding (coded 422 "
            "shares_offered_exceeds_outstanding)."
        ),
    )
    primary_proceeds_kes: RequestDecimal = Field(
        ge=Decimal("0"),
        le=_MAX_PRIMARY_PROCEEDS_KES,
        description=(
            "New cash raised by the issuer in the offer, KES, 0 to 50,000,000,000,000; must not "
            "exceed offer_price_kes x shares_offered (coded 422 primary_exceeds_offer_size)."
        ),
    )
    sensitivity_wacc_pct: (
        Annotated[
            list[WaccAxisValue],
            Field(min_length=1, max_length=_MAX_SENSITIVITY_AXIS),
        ]
        | None
    ) = Field(
        default=None,
        description=(
            "Sensitivity-matrix rows: 1 to 9 WACC values in %, each 8.00 to 30.00; null uses "
            "16, 17, 18, 19 and 20."
        ),
    )
    sensitivity_margin_pct: (
        Annotated[
            list[MarginAxisValue],
            Field(min_length=1, max_length=_MAX_SENSITIVITY_AXIS),
        ]
        | None
    ) = Field(
        default=None,
        description=(
            "Sensitivity-matrix columns: 1 to 9 base EBITDA margins in %, each 0.50 to 15.00; "
            "null uses 3.0 to 6.0 in steps of 0.5."
        ),
    )


# ---------------------------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------------------------


class RiskScenario(TerminalResponseModel):
    """One qualitative risk note of a preset, as stored in
    ``terminal_ipo_scenarios.risk_scenarios``."""

    title: str = Field(description="Short heading of the risk, for example Valuation Floor.")
    description: str = Field(description="The risk note, verbatim.")


class IpoScenario(TerminalResponseModel):
    """One read-only sandbox preset, an item of ``GET /ipo/scenarios``.

    ``assumptions`` is ready to post back to ``POST /ipo/valuation`` unchanged. The service builds
    it from the stored preset: ``ebitda_margin_pct`` is derived as
    ``base_ebitda_kes / base_revenue_kes * 100``, ``assumed_growth_pct`` becomes
    ``revenue_growth_pct``, ``assumed_wacc_pct`` becomes ``wacc_pct`` and
    ``shares_outstanding_post_offer`` becomes ``shares_outstanding``. The stored ``NUMERIC(..., 4)``
    values are quantized first, because Pydantic keeps a Decimal's exponent: the preset reads
    ``"offer_price_kes": "7.50"``, not ``"7.5000"``. Both sensitivity axes are null.
    """

    id: uuid.UUID = Field(description="Identifier of the preset row.")
    company_name: str = Field(description="Issuer name, for example Quickmart Supermarket.")
    sector: str | None = Field(description="Issuer sector; null when not recorded.")
    currency: str = Field(
        default=DEFAULT_CURRENCY,
        description="ISO 4217 currency of every KES amount in the preset; KES by default.",
    )
    risk_scenarios: list[RiskScenario] = Field(
        description="Qualitative risk notes shown beside the sandbox, in stored order.",
    )
    assumptions: IpoValuationRequest = Field(
        description="The preset's assumptions, in the request shape of POST /ipo/valuation.",
    )


# ---------------------------------------------------------------------------------------------
# Valuation result
# ---------------------------------------------------------------------------------------------


class ProjectionYear(TerminalResponseModel):
    """One explicit forecast year of the DCF. Amounts are KES at 2 dp."""

    year: int = Field(description="Forecast year index t, 1 to forecast_years.")
    revenue_kes: Decimal = Field(description="Revenue Rev_t = Rev_{t-1} x (1 + g), KES.")
    ebitda_margin_pct: Decimal = Field(
        description="EBITDA margin m_t on the linear expansion path, %.",
    )
    ebitda_kes: Decimal = Field(description="EBITDA_t = Rev_t x m_t, KES.")
    da_kes: Decimal = Field(description="Depreciation and amortisation DA_t, KES.")
    ebit_kes: Decimal = Field(description="EBIT_t = EBITDA_t - DA_t, KES.")
    tax_kes: Decimal = Field(description="Tax_t = max(EBIT_t, 0) x tax rate, KES.")
    nopat_kes: Decimal = Field(description="NOPAT_t = EBIT_t - Tax_t, KES.")
    capex_kes: Decimal = Field(description="Capital expenditure Capex_t, KES.")
    delta_nwc_kes: Decimal = Field(
        description="Working-capital investment dNWC_t = nwc x (Rev_t - Rev_{t-1}), KES.",
    )
    fcf_kes: Decimal = Field(
        description="Free cash flow FCF_t = NOPAT_t + DA_t - Capex_t - dNWC_t, KES.",
    )
    discount_factor: Decimal = Field(
        description="End-of-year discount factor DF_t = 1 / (1 + WACC)^t, 6 dp.",
    )
    pv_fcf_kes: Decimal = Field(description="Present value PV_t = FCF_t x DF_t, KES.")


class DcfResult(TerminalResponseModel):
    """Discounted cash flow valuation at the request's WACC and base margin."""

    projections: list[ProjectionYear] = Field(description="One entry per forecast year, 1 to N.")
    pv_explicit_fcf_kes: Decimal = Field(
        description="Sum of the present values of the explicit free cash flows, KES.",
    )
    terminal_value_kes: Decimal = Field(
        description="Gordon terminal value TV = FCF_N x (1 + g_T) / (WACC - g_T), KES.",
    )
    pv_terminal_value_kes: Decimal = Field(
        description="Present value of the terminal value PV_TV = TV x DF_N, KES.",
    )
    enterprise_value_kes: Decimal = Field(
        description="Enterprise value EV = sum of PV_t + PV_TV, KES.",
    )
    equity_value_kes: Decimal = Field(
        description="Equity value = EV - net debt + primary proceeds, KES.",
    )
    fair_value_per_share_kes: Decimal = Field(
        description="DCF fair value = equity value / shares outstanding, KES per share.",
    )
    flags: list[str] = Field(
        default_factory=list,
        description="Reason codes: negative_terminal_fcf, negative_equity_value.",
    )


class ComparablesResult(TerminalResponseModel):
    """Peer EV/EBITDA multiple valuation on base-year EBITDA."""

    ev_ebitda_multiple: Decimal = Field(description="Peer EV/EBITDA multiple applied, x.")
    base_ebitda_kes: Decimal = Field(
        description="EBITDA_0 = base revenue x base EBITDA margin / 100, KES.",
    )
    enterprise_value_kes: Decimal | None = Field(
        description="EV = multiple x EBITDA_0, KES; null with comps_non_positive_ebitda.",
    )
    equity_value_kes: Decimal | None = Field(
        description=(
            "Equity value = EV - net debt + primary proceeds, KES; null with "
            "comps_non_positive_ebitda."
        ),
    )
    fair_value_per_share_kes: Decimal | None = Field(
        description=(
            "Comparables fair value = equity value / shares outstanding, KES per share; null "
            "with comps_non_positive_ebitda."
        ),
    )
    flags: list[str] = Field(
        default_factory=list,
        description="Reason codes: comps_non_positive_ebitda.",
    )


class SensitivityCell(TerminalResponseModel):
    """The blended valuation recomputed at one WACC and one base EBITDA margin, every other
    assumption, margin expansion included, unchanged."""

    wacc_pct: Decimal = Field(description="WACC of this row, %.")
    ebitda_margin_pct: Decimal = Field(description="Base EBITDA margin of this column, %.")
    fair_value_per_share_kes: Decimal | None = Field(
        description=(
            "Blended fair value, KES per share; null with wacc_not_above_terminal_growth when "
            "the row's WACC does not exceed terminal growth."
        ),
    )
    upside_pct: Decimal | None = Field(
        description=(
            "(fair value - offer price) / offer price x 100, %; null when the fair value is null."
        ),
    )
    flags: list[str] = Field(
        default_factory=list,
        description="Reason codes for null or qualified values in this cell.",
    )


class SensitivityMatrixData(TerminalResponseModel):
    """WACC x base EBITDA margin grid of blended fair values, also the ``data`` prop of the
    ``SensitivityMatrix`` widget."""

    rows: list[Decimal] = Field(description="Row axis: WACC values, %.")
    cols: list[Decimal] = Field(description="Column axis: base EBITDA margins, %.")
    cells: list[list[SensitivityCell]] = Field(
        description="One list per row, each holding one cell per column (len(rows) x len(cols)).",
    )


class IpoValuationResponse(TerminalResponseModel):
    """Body of a successful ``POST /ipo/valuation``."""

    dcf: DcfResult = Field(description="Discounted cash flow valuation.")
    comparables: ComparablesResult = Field(description="Peer multiple valuation.")
    blended_fair_value_per_share_kes: Decimal = Field(
        description=(
            "w x DCF value + (1 - w) x comparables value with w = dcf_weight_pct / 100, KES per "
            "share; the DCF value alone when the comparables value is null."
        ),
    )
    offer_price_kes: Decimal = Field(description="Offer price of the request, KES per share.")
    upside_pct: Decimal = Field(
        description="(blended fair value - offer price) / offer price x 100, %.",
    )
    implied_market_cap_at_offer_kes: Decimal = Field(
        description="Offer price x shares outstanding, KES.",
    )
    implied_ev_ebitda_at_offer: Decimal | None = Field(
        description=(
            "(implied market cap + net debt - primary proceeds) / EBITDA_0, x; null when "
            "EBITDA_0 is not positive."
        ),
    )
    offer_size_kes: Decimal = Field(description="Offer price x shares offered, KES.")
    primary_proceeds_kes: Decimal = Field(
        description="New cash raised by the issuer, KES.",
    )
    secondary_proceeds_kes: Decimal = Field(
        description="Offer size - primary proceeds, KES, paid to selling shareholders.",
    )
    sensitivity: SensitivityMatrixData = Field(
        description="Blended fair value across the WACC and base EBITDA margin axes.",
    )
    flags: list[str] = Field(
        default_factory=list,
        description="Union of the DCF, comparables and sensitivity-cell reason codes.",
    )
