"""``GET /macro/overview``: CBK indicator snapshots, inflation and rates history, and the curve.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint.

The route only binds and bounds ``history_months``, through the query-parameter model
``MacroOverviewParams``, which forbids unknown keys; ``macro_service`` reads, resolves and rounds
every value once, and ``MacroOverviewResponse`` serializes each ``Decimal`` as a JSON string. The
service is called through its module so that tests can replace it.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of the route signature.
"""

from typing import Annotated, Final

from fastapi import APIRouter, Query

from opennse_terminal.api.deps import SessionDep
from opennse_terminal.schemas import (
    DEFAULT_HISTORY_MONTHS,
    HISTORY_MONTHS_MAX,
    MacroOverviewParams,
    MacroOverviewResponse,
)
from opennse_terminal.services import macro_service

__all__ = ["DEFAULT_HISTORY_MONTHS", "MAX_HISTORY_MONTHS", "get_macro_overview", "router"]

# DEFAULT_HISTORY_MONTHS (24, the month-ends in the history window when the request omits
# history_months) is re-exported from the schemas as it is; MAX_HISTORY_MONTHS aliases the
# bound MacroOverviewParams declares, so the two can never differ.
MAX_HISTORY_MONTHS: Final[int] = HISTORY_MONTHS_MAX
"""Largest accepted ``history_months``: twenty years of month-ends (240)."""

router = APIRouter()


@router.get(
    "/macro/overview",
    response_model=MacroOverviewResponse,
    summary="Macro snapshot, inflation and rates history, and the yield curve",
)
async def get_macro_overview(
    session: SessionDep,
    params: Annotated[MacroOverviewParams, Query()],
) -> MacroOverviewResponse:
    """Return the macro overview over ``history_months`` month-ends (default 24, range 1-240).

    The window holds exactly ``history_months`` month-ends ending at the month of the latest
    observation, fewer only when it would reach back before January of year 1 (it then opens at
    0001-01-31), and ``coverage.requested_start`` is the first of them (2024-10-31 for 24 months
    with data through 2026-09-30). ``coverage.is_partial_coverage`` is true when the data starts
    more than one month after the first month-end of the full requested window: with ordinary
    dates that is ``requested_start``, and months cut before year 1 count toward the one-month
    tolerance.

    Values are Decimal strings rounded once by the service: rates in percentage points (``"6.80"``
    means 6.80%), ``fx_kes_usd`` in KES per USD and ``fx_reserves_usd_m`` in millions of USD.
    Missing values are ``null``. The endpoint defines no coded error: with no data stored every
    list is empty. A ``history_months`` outside 1-240, and any unknown query parameter, fails
    FastAPI's own validation with its standard 422 list.
    """
    return await macro_service.get_macro_overview(session, params.history_months)
