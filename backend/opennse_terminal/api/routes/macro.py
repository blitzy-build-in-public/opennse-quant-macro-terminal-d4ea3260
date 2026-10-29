"""``GET /macro/overview``: CBK indicator snapshots, inflation and rates history, and the curve.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint.

The route only binds and bounds ``history_months``; ``macro_service`` reads, resolves and rounds
every value once, and ``MacroOverviewResponse`` serializes each ``Decimal`` as a JSON string. The
service is called through its module so that tests can replace it.
"""

from typing import Annotated, Final

from fastapi import APIRouter, Query

from opennse_terminal.api.deps import SessionDep
from opennse_terminal.schemas import MacroOverviewResponse
from opennse_terminal.services import macro_service

__all__ = ["DEFAULT_HISTORY_MONTHS", "MAX_HISTORY_MONTHS", "get_macro_overview", "router"]

DEFAULT_HISTORY_MONTHS: Final = 24
"""Month-ends in the history window when the request omits ``history_months``."""

MAX_HISTORY_MONTHS: Final = 240
"""Largest accepted ``history_months``: twenty years of month-ends."""

router = APIRouter()


@router.get(
    "/macro/overview",
    response_model=MacroOverviewResponse,
    summary="Macro snapshot, inflation and rates history, and the yield curve",
)
async def get_macro_overview(
    session: SessionDep,
    history_months: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_HISTORY_MONTHS,
            description=(
                "Number of month-ends in the history window, ending at the latest observation "
                "month."
            ),
        ),
    ] = DEFAULT_HISTORY_MONTHS,
) -> MacroOverviewResponse:
    """Return the macro overview over ``history_months`` month-ends (default 24, range 1-240).

    The window holds exactly ``history_months`` month-ends ending at the month of the latest
    observation, and ``coverage.requested_start`` is the first of them (2024-10-31 for 24 months
    with data through 2026-09-30). ``coverage.is_partial_coverage`` is true when the data starts
    more than one month after ``requested_start``.

    Values are Decimal strings rounded once by the service: rates in percentage points (``"6.80"``
    means 6.80%), ``fx_kes_usd`` in KES per USD and ``fx_reserves_usd_m`` in millions of USD.
    Missing values are ``null``. The endpoint defines no coded error: with no data stored every
    list is empty. A ``history_months`` outside 1-240 fails FastAPI's own validation with its
    standard 422 list.
    """
    return await macro_service.get_macro_overview(session, history_months)
