"""Liveness/readiness endpoint for the terminal router; checks database connectivity.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint, so a host's authentication override also guards ``/health``.

The route only resolves the request session and binds the empty query-parameter model
``NoQueryParams``, so any query parameter is FastAPI's standard 422 list; ``health_service`` runs
the bounded ``SELECT 1`` probe and translates its failure into the coded 503. The service is
called through its module so that tests can replace it.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` metadata of the route signature.
"""

from typing import Annotated, Final

from fastapi import APIRouter, Query

from opennse_terminal.api.deps import SessionDep
from opennse_terminal.schemas import ErrorResponse, HealthResponse, NoQueryParams
from opennse_terminal.services import health_service

__all__ = ["get_health", "router"]

# FastAPI validates the query string into a fresh NoQueryParams on every request, so this default
# is used only when get_health is awaited directly with a session alone. The model has no fields,
# so the shared instance holds no state.
_NO_QUERY_PARAMS: Final = NoQueryParams()

router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Terminal health and database connectivity",
    responses={503: {"model": ErrorResponse, "description": "Database unavailable"}},
)
async def get_health(
    session: SessionDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> HealthResponse:
    """Report the terminal's health after a ``SELECT 1`` round trip to its database.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and the probe does not run.

    Returns 200 with ``status="ok"``, ``database="ok"`` and the installed ``opennse_terminal``
    version. When the database cannot be reached, or the round trip (connection checkout and pool
    pre-ping included) takes longer than ``health_service.HEALTH_CHECK_TIMEOUT_SECONDS`` (5 s),
    raises 503, within about 7 s at worst once a hung pooled connection is discarded, with the
    coded body ``{"detail": {"code": "database_unavailable", "message": ..., "field": null}}``.

    The bound is applied by the service to the session it is given rather than on the engine, so
    it also covers a session supplied by a host's ``get_terminal_session`` override. The session
    is only read; its provider closes it after the request.
    """
    return await health_service.get_health(session)
