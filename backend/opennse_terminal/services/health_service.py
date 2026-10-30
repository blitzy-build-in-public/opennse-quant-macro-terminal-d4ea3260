"""Health service: a bounded ``SELECT 1`` round trip to the terminal database.

Serves ``GET /api/v1/terminal/health``. It reads no table: the constant ``SELECT 1`` proves that a
connection can be checked out, connected, pre-pinged and queried within
``HEALTH_CHECK_TIMEOUT_SECONDS`` (seconds) on the session it is given, which is the request
session of ``get_terminal_session`` or a host's override of it.

Errors: any failure of the round trip, an expired bound included, is the coded 503
``{"detail": {"code": "database_unavailable", "message": ..., "field": null}}`` (``ErrorDetail``),
and the module registers no exception handlers. Only the exception's type name is logged, at
WARNING on ``opennse_terminal.services.health_service``, which adds no handlers. An external
``asyncio.CancelledError`` propagates unchanged.

This module never imports ``opennse_terminal.api``: the routes import the services, so the reverse
import would be a cycle.
"""

import asyncio
import logging
from typing import Final

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal import __version__
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.schemas.common import ErrorDetail, HealthResponse

__all__ = ["HEALTH_CHECK_TIMEOUT_SECONDS", "get_health"]

HEALTH_CHECK_TIMEOUT_SECONDS: Final[float] = 5.0
"""Bound, in seconds, on the health check's whole ``SELECT 1`` round trip: connection checkout,
connect, pool pre-ping and the query itself. When it expires the check answers the coded 503
``database_unavailable``. If the bound cancelled work on a hung pooled connection, the pool first
discards that connection, and SQLAlchemy's graceful close of it can add up to 2 s, so the 503
arrives within about 7 s at worst and 5 s otherwise: a hung database still fails a readiness
probe far sooner than asyncpg's 60 s connect timeout. :func:`get_health` reads it at call time."""

logger = logging.getLogger("opennse_terminal.services.health_service")


async def get_health(session: AsyncSession) -> HealthResponse:
    """Check the database with a ``SELECT 1`` round trip on ``session`` and report the health.

    Returns ``HealthResponse(status="ok", database="ok", version=opennse_terminal.__version__)``
    once the round trip completes. When the database cannot be reached, or the round trip
    (connection checkout and pool pre-ping included) takes longer than
    ``HEALTH_CHECK_TIMEOUT_SECONDS`` (5 s), raises ``HTTPException`` 503 with the coded detail
    ``{"code": "database_unavailable", "message": "The terminal database is unavailable.",
    "field": None}``, chained to the original exception; it arrives within about 7 s at worst once
    a hung pooled connection is discarded. The failure is logged at WARNING with its exception type
    name only.

    The bound is applied to the given session rather than on the engine, so it also covers a
    session supplied by a host's ``get_terminal_session`` override. The round trip runs on the bind
    the session gives the terminal's models, so a host session with no default bind, configured
    only through ``binds`` or a custom ``get_bind``, is checked like one with a default bind. The
    session is only read,
    never committed or closed: its provider closes it after the request. An external cancellation
    (``asyncio.CancelledError``) propagates instead of becoming the 503.
    """
    try:
        async with asyncio.timeout(HEALTH_CHECK_TIMEOUT_SECONDS):
            # A text clause names no mapper or table, so a session with no default bind, configured
            # only through ``binds``, finds no bind for it. The probe is therefore resolved as a
            # query of TerminalIngestionRun, the model filings_service resolves the terminal's bind
            # through: its mapper and table match ``binds`` keyed by TerminalBase, a mapped class or
            # a table, and reach a custom ``get_bind`` as an ORM query's would. A session with only
            # a default bind still uses that bind.
            await session.execute(
                text("SELECT 1"),
                bind_arguments={
                    "mapper": TerminalIngestionRun.__mapper__,
                    "clause": TerminalIngestionRun.__table__,
                },
            )
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
