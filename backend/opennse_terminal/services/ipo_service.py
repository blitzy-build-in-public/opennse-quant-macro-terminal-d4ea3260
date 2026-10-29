"""IPO valuation sandbox service: read-only presets and the stateless DCF/comparables valuation.

``GET /ipo/scenarios`` awaits :func:`list_scenarios` and ``POST /ipo/valuation`` awaits
:func:`value_ipo`. Every formula lives in :mod:`opennse_terminal.quant.dcf`, the single
server-side implementation of the sandbox; this module converts the request to
``DcfAssumptions``, raises the first cross-field violation as a coded 422, and rounds each output
exactly once with :func:`~opennse_terminal.quant.numeric.quantize_out` (ROUND_HALF_UP) while
building the response.

Units: ``*_kes`` amounts are whole KES, fair values and the offer price are KES per share,
``*_pct`` values are percentage points and multiples are times (x). Values stay ``Decimal`` from
the request to the response; no ``float`` is involved. Presets are read-only and a valuation
writes nothing, so the module has no create, update or delete function.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.models import TerminalIpoScenario
from opennse_terminal.quant.dcf import (
    ComparablesOutput,
    DcfAssumptions,
    DcfOutput,
    IpoValuationOutput,
    ProjectionYearOutput,
    SensitivityCellOutput,
    SensitivityOutput,
    assumption_violations,
)
from opennse_terminal.quant.dcf import value_ipo as quant_value_ipo
from opennse_terminal.quant.numeric import HUNDRED, ZERO, quantize_out
from opennse_terminal.schemas.common import ErrorDetail
from opennse_terminal.schemas.ipo import (
    ComparablesResult,
    DcfResult,
    IpoScenario,
    IpoValuationRequest,
    IpoValuationResponse,
    ProjectionYear,
    RiskScenario,
    SensitivityCell,
    SensitivityMatrixData,
)

__all__ = ["compute_ipo_valuation", "list_scenarios", "to_dcf_assumptions", "value_ipo"]

logger = logging.getLogger("opennse_terminal.services.ipo_service")


# ---------------------------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------------------------


async def list_scenarios(session: AsyncSession) -> list[IpoScenario]:
    """Return every stored sandbox preset, presets first, then by company name.

    Each preset's ``assumptions`` is an :class:`IpoValuationRequest` ready to post back to
    ``POST /ipo/valuation`` unchanged. Stored ``NUMERIC(..., 4)`` values are quantized to the
    2 dp strings the sliders use (``7.5000`` becomes ``"7.50"``): KES per share for the offer
    price, KES for ``net_debt_kes``, ``base_revenue_kes`` and ``primary_proceeds_kes``, % for the
    rates and x for the peer multiple. The column-to-field renames are ``assumed_growth_pct`` to
    ``revenue_growth_pct``, ``assumed_wacc_pct`` to ``wacc_pct`` and
    ``shares_outstanding_post_offer`` to ``shares_outstanding``; ``ebitda_margin_pct`` is not
    stored and is derived as ``base_ebitda_kes / base_revenue_kes * 100``. Both sensitivity axes
    are left null so the defaults apply.

    Returns ``[]`` when no preset exists; the endpoint has no error codes.
    """
    rows = (
        await session.scalars(
            select(TerminalIpoScenario).order_by(
                TerminalIpoScenario.is_preset.desc(), TerminalIpoScenario.company_name
            )
        )
    ).all()
    scenarios: list[IpoScenario] = []
    for row in rows:
        scenario = _scenario_from_row(row)
        if scenario is not None:
            scenarios.append(scenario)
    return scenarios


def _scenario_from_row(row: TerminalIpoScenario) -> IpoScenario | None:
    """Build the :class:`IpoScenario` of one stored preset, or ``None`` if it cannot form one.

    Presets are seeded reference data with no write endpoint, so a row that cannot form a valid
    request (a non-positive base revenue, a value outside its slider range, a ``risk_scenarios``
    value that is not a JSON array, or a malformed risk note) is a data error. It is logged once
    at WARNING, naming the preset and the fields but never the stored values, and left out, so
    one bad row does not fail the whole preset list.
    """
    if row.base_revenue_kes <= ZERO:
        logger.warning(
            "skipping IPO preset %r: base_revenue_kes must be positive to derive the base "
            "EBITDA margin",
            row.company_name,
        )
        return None
    # The JSONB column constrains only NOT NULL, so a stored JSON null, scalar or object reaches
    # here despite the column's list annotation; it is checked as untyped data before iterating.
    stored_risks: object = row.risk_scenarios
    if not isinstance(stored_risks, list):
        logger.warning(
            "skipping IPO preset %r: risk_scenarios must be a JSON array of risk notes",
            row.company_name,
        )
        return None
    base_margin_pct = row.base_ebitda_kes / row.base_revenue_kes * HUNDRED
    try:
        assumptions = IpoValuationRequest(
            offer_price_kes=quantize_out(row.offer_price_kes, "per_share"),
            revenue_growth_pct=quantize_out(row.assumed_growth_pct, "pct"),
            ebitda_margin_pct=quantize_out(base_margin_pct, "pct"),
            margin_expansion_pct=quantize_out(row.margin_expansion_pct, "pct"),
            wacc_pct=quantize_out(row.assumed_wacc_pct, "pct"),
            terminal_growth_pct=quantize_out(row.terminal_growth_pct, "pct"),
            net_debt_kes=quantize_out(row.net_debt_kes, "money"),
            nwc_pct_revenue=quantize_out(row.nwc_pct_revenue, "pct"),
            peer_ev_ebitda_multiple=quantize_out(row.peer_ev_ebitda_multiple, "ratio"),
            dcf_weight_pct=quantize_out(row.dcf_weight_pct, "pct"),
            capex_pct_revenue=quantize_out(row.capex_pct_revenue, "pct"),
            da_pct_revenue=quantize_out(row.da_pct_revenue, "pct"),
            tax_rate_pct=quantize_out(row.tax_rate_pct, "pct"),
            forecast_years=row.forecast_years,
            base_revenue_kes=quantize_out(row.base_revenue_kes, "money"),
            shares_outstanding=row.shares_outstanding_post_offer,
            shares_offered=row.shares_offered,
            primary_proceeds_kes=quantize_out(row.primary_proceeds_kes, "money"),
        )
        risk_scenarios = [RiskScenario.model_validate(entry) for entry in stored_risks]
    except ValidationError as exc:
        # Only the field names are logged, never the stored values.
        invalid_fields = sorted(
            {".".join(str(part) for part in err["loc"]) for err in exc.errors()}
        )
        logger.warning(
            "skipping IPO preset %r: stored values do not form a valid %s (%s)",
            row.company_name,
            exc.title,
            ", ".join(invalid_fields),
        )
        return None
    return IpoScenario(
        id=row.id,
        company_name=row.company_name,
        sector=row.sector,
        currency=row.currency,
        risk_scenarios=risk_scenarios,
        assumptions=assumptions,
    )


# ---------------------------------------------------------------------------------------------
# Valuation
# ---------------------------------------------------------------------------------------------


def to_dcf_assumptions(request: IpoValuationRequest) -> DcfAssumptions:
    """Convert a validated request to the quant engine's ``DcfAssumptions``, one-to-one.

    The 18 assumption fields share their names with ``DcfAssumptions``; the sensitivity axes are
    passed to the engine separately. Python-mode ``model_dump`` keeps each ``Decimal`` and
    ``int`` exactly as validated, so no value is converted or rounded.
    """
    return DcfAssumptions(
        **request.model_dump(exclude={"sensitivity_wacc_pct", "sensitivity_margin_pct"})
    )


def _raise_for_violations(assumptions: DcfAssumptions) -> None:
    """Raise the first cross-field rule ``assumptions`` breaks as a coded 422.

    The body is ``{"detail": {"code", "message", "field"}}``, an object rather than FastAPI's
    validation list, with the codes ``wacc_not_above_terminal_growth`` (``wacc_pct``),
    ``shares_offered_exceeds_outstanding`` (``shares_offered``) and
    ``primary_exceeds_offer_size`` (``primary_proceeds_kes``), in that reporting order.
    """
    violations = assumption_violations(assumptions)
    if not violations:
        return
    first = violations[0]
    logger.debug("IPO valuation rejected: %s on %s", first.code, first.field)
    raise HTTPException(
        status_code=422,
        detail=ErrorDetail(code=first.code, message=first.message, field=first.field).model_dump(),
    )


def compute_ipo_valuation(request: IpoValuationRequest) -> IpoValuationResponse:
    """Value the IPO offer described by ``request``; pure computation with no I/O.

    Runs ``quant.dcf.value_ipo``: the DCF, the peer EV/EBITDA comparables, their blend weighted
    by ``dcf_weight_pct``, the offer metrics and the WACC (%) x base EBITDA margin (%) sensitivity
    matrix. Null axes select the defaults 16-20% x 3.0-6.0%, a 5 x 7 matrix. Each output is
    rounded once: KES to 2 dp, KES per share to 2 dp, % to 2 dp, multiples (x) to 2 dp and
    discount factors to 6 dp. ``flags`` is the de-duplicated union, in first-seen order, of the
    DCF, comparables and cell flags.

    Raises:
        HTTPException: 422 with a coded ``detail`` object for the first cross-field violation.
    """
    assumptions = to_dcf_assumptions(request)
    _raise_for_violations(assumptions)
    output = quant_value_ipo(
        assumptions,
        sensitivity_wacc=request.sensitivity_wacc_pct,
        sensitivity_margin=request.sensitivity_margin_pct,
    )
    response = _valuation_response(output)
    logger.debug(
        "IPO valuation: blended %s KES/share, upside %s%%, %d x %d sensitivity matrix",
        response.blended_fair_value_per_share_kes,
        response.upside_pct,
        len(response.sensitivity.rows),
        len(response.sensitivity.cols),
    )
    return response


async def value_ipo(request: IpoValuationRequest) -> IpoValuationResponse:
    """Endpoint function of ``POST /ipo/valuation``; see :func:`compute_ipo_valuation`.

    Asynchronous for uniformity with the other service entry points. It reads and writes no
    database: the valuation is stateless and bounded (at most 10 forecast years and 81 matrix
    cells), so it runs inline without a worker thread.

    Raises:
        HTTPException: 422 with a coded ``detail`` object for the first cross-field violation.
    """
    return compute_ipo_valuation(request)


def _valuation_response(output: IpoValuationOutput) -> IpoValuationResponse:
    """Quantize the unrounded engine output into the response model."""
    return IpoValuationResponse(
        dcf=_dcf_result(output.dcf),
        comparables=_comparables_result(output.comparables),
        blended_fair_value_per_share_kes=quantize_out(
            output.blended_fair_value_per_share_kes, "per_share"
        ),
        offer_price_kes=quantize_out(output.offer_price_kes, "per_share"),
        upside_pct=quantize_out(output.upside_pct, "pct"),
        implied_market_cap_at_offer_kes=quantize_out(
            output.implied_market_cap_at_offer_kes, "money"
        ),
        implied_ev_ebitda_at_offer=quantize_out(output.implied_ev_ebitda_at_offer, "ratio"),
        offer_size_kes=quantize_out(output.offer_size_kes, "money"),
        primary_proceeds_kes=quantize_out(output.primary_proceeds_kes, "money"),
        secondary_proceeds_kes=quantize_out(output.secondary_proceeds_kes, "money"),
        sensitivity=_sensitivity_matrix(output.sensitivity),
        flags=output.flags,
    )


def _projection_year(year: ProjectionYearOutput) -> ProjectionYear:
    """One forecast year: KES to 2 dp, the margin in % to 2 dp, the discount factor to 6 dp."""
    return ProjectionYear(
        year=year.year,
        revenue_kes=quantize_out(year.revenue_kes, "money"),
        ebitda_margin_pct=quantize_out(year.ebitda_margin_pct, "pct"),
        ebitda_kes=quantize_out(year.ebitda_kes, "money"),
        da_kes=quantize_out(year.da_kes, "money"),
        ebit_kes=quantize_out(year.ebit_kes, "money"),
        tax_kes=quantize_out(year.tax_kes, "money"),
        nopat_kes=quantize_out(year.nopat_kes, "money"),
        capex_kes=quantize_out(year.capex_kes, "money"),
        delta_nwc_kes=quantize_out(year.delta_nwc_kes, "money"),
        fcf_kes=quantize_out(year.fcf_kes, "money"),
        discount_factor=quantize_out(year.discount_factor, "discount_factor"),
        pv_fcf_kes=quantize_out(year.pv_fcf_kes, "money"),
    )


def _dcf_result(dcf: DcfOutput) -> DcfResult:
    """The DCF block: KES totals to 2 dp and the fair value in KES per share to 2 dp."""
    return DcfResult(
        projections=[_projection_year(year) for year in dcf.projections],
        pv_explicit_fcf_kes=quantize_out(dcf.pv_explicit_fcf_kes, "money"),
        terminal_value_kes=quantize_out(dcf.terminal_value_kes, "money"),
        pv_terminal_value_kes=quantize_out(dcf.pv_terminal_value_kes, "money"),
        enterprise_value_kes=quantize_out(dcf.enterprise_value_kes, "money"),
        equity_value_kes=quantize_out(dcf.equity_value_kes, "money"),
        fair_value_per_share_kes=quantize_out(dcf.fair_value_per_share_kes, "per_share"),
        flags=dcf.flags,
    )


def _comparables_result(comparables: ComparablesOutput) -> ComparablesResult:
    """The comparables block; values null with ``comps_non_positive_ebitda`` stay null."""
    return ComparablesResult(
        ev_ebitda_multiple=quantize_out(comparables.ev_ebitda_multiple, "ratio"),
        base_ebitda_kes=quantize_out(comparables.base_ebitda_kes, "money"),
        enterprise_value_kes=quantize_out(comparables.enterprise_value_kes, "money"),
        equity_value_kes=quantize_out(comparables.equity_value_kes, "money"),
        fair_value_per_share_kes=quantize_out(comparables.fair_value_per_share_kes, "per_share"),
        flags=comparables.flags,
    )


def _sensitivity_cell(cell: SensitivityCellOutput) -> SensitivityCell:
    """One matrix cell; a cell null with ``wacc_not_above_terminal_growth`` stays null."""
    return SensitivityCell(
        wacc_pct=quantize_out(cell.wacc_pct, "pct"),
        ebitda_margin_pct=quantize_out(cell.ebitda_margin_pct, "pct"),
        fair_value_per_share_kes=quantize_out(cell.fair_value_per_share_kes, "per_share"),
        upside_pct=quantize_out(cell.upside_pct, "pct"),
        flags=cell.flags,
    )


def _sensitivity_matrix(sensitivity: SensitivityOutput) -> SensitivityMatrixData:
    """The WACC (%) x base EBITDA margin (%) matrix, with both axes in % to 2 dp."""
    return SensitivityMatrixData(
        rows=[quantize_out(wacc, "pct") for wacc in sensitivity.rows],
        cols=[quantize_out(margin, "pct") for margin in sensitivity.cols],
        cells=[[_sensitivity_cell(cell) for cell in row] for row in sensitivity.cells],
    )
