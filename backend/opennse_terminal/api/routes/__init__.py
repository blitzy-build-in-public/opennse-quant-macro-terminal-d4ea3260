"""Sub-routers of the terminal API; ``api/router.py`` mounts them under ``/api/v1/terminal``.

``api/router.py`` includes ``ALL_ROUTERS`` in order. Each sub-router is re-exported exactly as
its module defines it, with no prefix, tags or dependencies added here, so ``include_router``
keeps every relative path and each route's class, including the ingest router's
``TerminalUploadRoute`` request-size guard.

This module must not import ``opennse_terminal.api`` or ``opennse_terminal.api.router``: the
router module imports this one, so either import would be circular.
"""

from fastapi import APIRouter

from opennse_terminal.api.routes.cycles import router as cycles_router
from opennse_terminal.api.routes.health import router as health_router
from opennse_terminal.api.routes.ingest import router as ingest_router
from opennse_terminal.api.routes.ipo import router as ipo_router
from opennse_terminal.api.routes.macro import router as macro_router
from opennse_terminal.api.routes.market import router as market_router
from opennse_terminal.api.routes.watchlists import router as watchlists_router

__all__ = [
    "ALL_ROUTERS",
    "cycles_router",
    "health_router",
    "ingest_router",
    "ipo_router",
    "macro_router",
    "market_router",
    "watchlists_router",
]

ALL_ROUTERS: tuple[APIRouter, ...] = (
    health_router,
    macro_router,
    market_router,
    cycles_router,
    ipo_router,
    ingest_router,
    watchlists_router,
)
"""Every sub-router, in mounting order; this order fixes the order of paths in OpenAPI."""
