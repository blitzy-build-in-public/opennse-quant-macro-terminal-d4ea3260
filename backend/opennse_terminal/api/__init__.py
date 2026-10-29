"""Public mounting surface of the terminal API: the router and every host override target.

A host mounts the terminal in four steps, of which only the first is required::

    from opennse_terminal.api import (
        dispose_terminal_engine,
        get_terminal_principal,
        get_terminal_session,
        terminal_router,
    )

    # 1. Mount every endpoint under /api/v1/terminal.
    app.include_router(terminal_router)

    # 2. Optional: serve terminal requests from the host's own sessions. The provider must yield
    #    a SQLAlchemy AsyncSession on the same PostgreSQL database through asyncpg.
    app.dependency_overrides[get_terminal_session] = host_get_async_session

    # 3. Optional: map the host's user to a TerminalPrincipal subject, or raise
    #    HTTPException(401), to protect every terminal endpoint. get_terminal_settings and
    #    require_ingest_permission are overridden the same way.
    app.dependency_overrides[get_terminal_principal] = host_get_terminal_principal

    # 4. Only when the default session provider is kept: release its engine on shutdown.
    await dispose_terminal_engine()

Overrides are keyed by function identity. These names are the same objects as those of
``opennse_terminal.api.deps`` and ``opennse_terminal.db.session``, so an override registered
through either import path applies. Importing this package builds no app, engine or settings.
"""

from opennse_terminal.api.deps import (
    TerminalPrincipal,
    get_terminal_principal,
    get_terminal_session,
    get_terminal_settings,
    require_ingest_permission,
)
from opennse_terminal.api.router import terminal_router
from opennse_terminal.db.session import dispose_terminal_engine

__all__ = [
    "TerminalPrincipal",
    "dispose_terminal_engine",
    "get_terminal_principal",
    "get_terminal_session",
    "get_terminal_settings",
    "require_ingest_permission",
    "terminal_router",
]
