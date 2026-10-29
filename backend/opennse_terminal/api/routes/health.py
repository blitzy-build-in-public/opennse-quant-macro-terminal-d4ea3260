"""Liveness/readiness endpoint for the terminal router; checks database connectivity.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint, so a host's authentication override also guards ``/health``.
"""

import logging

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text

from opennse_terminal import __version__
from opennse_terminal.api.deps import SessionDep
from opennse_terminal.schemas import ErrorDetail, ErrorResponse, HealthResponse

__all__ = ["get_health", "router"]

logger = logging.getLogger("opennse_terminal.api.routes.health")

router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Terminal health and database connectivity",
    responses={503: {"model": ErrorResponse, "description": "Database unavailable"}},
)
async def get_health(session: SessionDep) -> HealthResponse:
    """Report the terminal's health after a ``SELECT 1`` round trip to its database.

    Returns 200 with ``status="ok"``, ``database="ok"`` and the installed ``opennse_terminal``
    version. When the database cannot be reached, raises 503 with the coded body
    ``{"detail": {"code": "database_unavailable", "message": ..., "field": null}}``.

    The session is only read; ``get_terminal_session`` closes it after the request.
    """
    try:
        await session.execute(text("SELECT 1"))
    # Deliberately wider than SQLAlchemyError: any failure of this constant query means the
    # database is unavailable. SQLAlchemy does not wrap asyncpg's connect-time errors, so a refused
    # connection or timeout arrives as a raw OSError, and bad credentials, a missing database or
    # too many connections as asyncpg.PostgresError, which the package cannot name because it never
    # imports the untyped asyncpg. CancelledError is a BaseException and still propagates.
    except Exception as exc:
        # Only the exception type is logged: its message can embed the database URL and password.
        logger.warning("terminal health check failed: %s", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ErrorDetail(
                code="database_unavailable",
                message="The terminal database is unavailable.",
                field=None,
            ).model_dump(),
        ) from exc
    return HealthResponse(status="ok", database="ok", version=__version__)
