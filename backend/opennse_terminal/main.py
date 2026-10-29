"""Standalone FastAPI application of the OpenNSE Quant & Macro Terminal.

Run it with ``uvicorn opennse_terminal.main:app --port 8000``. The app mounts
``terminal_router`` with the same single ``include_router`` line a host adds, and adds nothing a
router cannot carry into a host: no middleware (no CORS, because the browser reaches the API on
the same origin through the Next.js rewrites), no exception handlers, no extra routes and no
dependency overrides. Standalone and mounted behaviour are therefore identical.

The only app-level addition is the lifespan, which releases the default session provider's lazily
created engine on shutdown. Importing this module creates no engine and opens no connection.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from opennse_terminal import __version__
from opennse_terminal.api import dispose_terminal_engine, terminal_router

__all__ = ["app", "create_app", "lifespan"]

logger = logging.getLogger("opennse_terminal.main")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Log startup, then dispose the default engine when the app shuts down.

    Startup deliberately does not connect to the database: ``GET /api/v1/terminal/health``
    reports its availability, so the API starts and answers 503 while the database is down.
    """
    logger.info("OpenNSE terminal API starting (version %s)", __version__)
    try:
        yield
    finally:
        await dispose_terminal_engine()


def create_app() -> FastAPI:
    """Build the standalone app exactly as a host mounts the terminal router.

    Every call returns a new app, so tests may build one per module. Configures INFO logging on
    the root logger unless it already has handlers; uvicorn configures only its own loggers, so
    the terminal's records are formatted here.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    application = FastAPI(
        title="OpenNSE Quant & Macro Terminal",
        version=__version__,
        lifespan=lifespan,
    )
    application.include_router(terminal_router)
    return application


app = create_app()
