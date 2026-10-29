"""IPO valuation sandbox endpoints: ``GET /ipo/scenarios`` and ``POST /ipo/valuation``.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint.

Presets are read-only seeded reference data (the Quickmart Supermarket scenario), so there is no
route that creates, updates or deletes one. Valuation is stateless: every slider change posts the
full assumptions and the server recalculates them with the single ``Decimal`` implementation in
``quant.dcf``, which has no client-side copy. Rates are percentage points (``"18.00"`` means
18%), ``*_kes`` amounts are whole KES, fair values and the offer price are KES per share. The
route neither rounds nor converts: ``ipo_service`` quantizes each output once and the response
serializes every ``Decimal`` as a JSON string (``"6.82"``, ``"-9.11"``).

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` dependency metadata of ``SessionDep`` from the route signature.
"""

from fastapi import APIRouter

from opennse_terminal.api.deps import SessionDep, TerminalJsonRoute
from opennse_terminal.schemas import (
    ErrorResponse,
    IpoScenario,
    IpoValuationRequest,
    IpoValuationResponse,
)
from opennse_terminal.services import ipo_service

__all__ = ["list_ipo_scenarios", "router", "value_ipo"]

# TerminalJsonRoute rather than the default APIRoute: POST /ipo/valuation binds a JSON body, and
# this route class answers every undecodable body (invalid UTF-8, nesting over 200 levels, an
# over-long integer literal, an unpaired surrogate) with FastAPI's standard 422 list instead of a
# 400 with a string detail, so the endpoint keeps its two documented 422 shapes.
# include_router preserves the class, so it travels with terminal_router into a host.
router = APIRouter(route_class=TerminalJsonRoute)


@router.get(
    "/ipo/scenarios",
    response_model=list[IpoScenario],
    summary="Read-only IPO sandbox presets",
)
async def list_ipo_scenarios(session: SessionDep) -> list[IpoScenario]:
    """List the stored sandbox presets, presets first and then by company name.

    The seeded preset is the Quickmart Supermarket scenario: offer KES 7.50 per share, a 50%
    secondary stake and three risk notes. Each item's ``assumptions`` is an
    ``IpoValuationRequest`` that can be posted to ``POST /ipo/valuation`` unchanged, with amounts
    as 2 dp strings (KES, KES per share, rates in %) and both sensitivity axes null so the default
    16-20% WACC by 3.0-6.0% margin matrix applies.

    Only KES presets are listed; a preset stored in another currency, or one that cannot form a
    valid request, is left out rather than converted. Returns ``[]`` when there is none; the
    endpoint has no error codes.
    """
    return await ipo_service.list_scenarios(session)


@router.post(
    "/ipo/valuation",
    response_model=IpoValuationResponse,
    summary="DCF, comparable multiples, blended fair value and sensitivity matrix",
    responses={
        422: {
            "model": ErrorResponse,
            "description": (
                "Coded cross-field rule, a detail object with code, message and field "
                "(wacc_not_above_terminal_growth, shares_offered_exceeds_outstanding or "
                "primary_exceeds_offer_size), or FastAPI's standard validation list for a "
                "single-field bound"
            ),
        }
    },
)
async def value_ipo(body: IpoValuationRequest) -> IpoValuationResponse:
    """Value the IPO offer: DCF, peer EV/EBITDA comparables, their blend and the sensitivity matrix.

    The body holds every sandbox assumption, all required: rates in %, ``*_kes`` amounts in KES,
    the offer price in KES per share, share counts as integers. Decimals may be JSON strings or
    numbers. The response reports the DCF and comparables fair values and their blend weighted by
    ``dcf_weight_pct``, in KES per share, the upside against the offer price in %, the offer
    metrics in KES, and the WACC (%) by base EBITDA margin (%) matrix: 5 x 7 by default, or the
    optional ``sensitivity_wacc_pct`` and ``sensitivity_margin_pct`` axes of 1-9 values each.
    Undefined values are null with reason codes in ``flags``.

    The computation is pure and bounded (at most 10 forecast years and 81 matrix cells), so the
    endpoint takes no database session and answers each debounced slider change directly.

    Errors are 422 in one of two shapes. A single-field bound or an unknown key fails with
    FastAPI's standard list. The first cross-field rule broken fails with the coded object
    ``{"detail": {"code", "message", "field"}}``: ``wacc_not_above_terminal_growth`` on
    ``wacc_pct``, ``shares_offered_exceeds_outstanding`` on ``shares_offered`` or
    ``primary_exceeds_offer_size`` on ``primary_proceeds_kes``. An error body carries no valuation.
    """
    return await ipo_service.value_ipo(body)
