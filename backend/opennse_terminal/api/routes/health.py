"""Liveness/readiness endpoint for the terminal router; checks database connectivity.

The sub-router carries no prefix, tags or dependencies of its own: ``api/router.py`` mounts it
under ``/api/v1/terminal``, and the router-level principal dependency applies here as it does to
every terminal endpoint, so a host's authentication override also guards ``/health``.
"""

import asyncio
import logging
from typing import Final

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import text

from opennse_terminal import __version__
from opennse_terminal.api.deps import SessionDep
from opennse_terminal.schemas import ErrorDetail, ErrorResponse, HealthResponse

__all__ = ["HEALTH_CHECK_TIMEOUT_SECONDS", "get_health", "router"]

HEALTH_CHECK_TIMEOUT_SECONDS: Final[float] = 5.0
"""Bound, in seconds, on the health check's whole ``SELECT 1`` round trip: connection checkout,
connect, pool pre-ping and the query itself. When it expires the check answers the coded 503
``database_unavailable``. If the bound cancelled work on a hung pooled connection, the pool first
discards that connection, and SQLAlchemy's graceful close of it can add up to 2 s, so the 503
arrives within about 7 s at worst and 5 s otherwise: a hung database still fails a readiness
probe far sooner than asyncpg's 60 s connect timeout. ``get_health`` reads it at call time."""

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
    version. When the database cannot be reached, or the round trip (connection checkout and pool
    pre-ping included) takes longer than ``HEALTH_CHECK_TIMEOUT_SECONDS`` (5 s), raises 503, within
    about 7 s at worst once a hung pooled connection is discarded, with the coded body
    ``{"detail": {"code": "database_unavailable", "message": ..., "field": null}}``.

    The bound is applied here rather than on the engine, so it also covers a session supplied by a
    host's ``get_terminal_session`` override. The session is only read; its provider closes it
    after the request.
    """
    try:
        async with asyncio.timeout(HEALTH_CHECK_TIMEOUT_SECONDS):
            await session.execute(text("SELECT 1"))
    # Deliberately wider than SQLAlchemyError: any failure of this constant query means the
    # database is unavailable. SQLAlchemy does not wrap asyncpg's connect-time errors, so a refused
    # connection arrives as a raw OSError, and bad credentials, a missing database or too many
    # connections as asyncpg.PostgresError, which the package cannot name because it never imports
    # the untyped asyncpg. An expired bound arrives as the TimeoutError of asyncio.timeout(), which
    # cancels the hung round trip first. A CancelledError from outside (a client disconnect) is a
    # BaseException that asyncio.timeout() passes through, so it still propagates.
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
