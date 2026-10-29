"""Async engine and session factory of the OpenNSE terminal (SQLAlchemy 2.0 on asyncpg).

Two entry points serve two ownership models:

* :func:`build_engine` returns a new engine that the caller owns and disposes. Ingestion runs, the
  seeder, the CSV importer and the test fixtures use it, because they need dedicated connections
  (a transaction-scoped advisory lock on one, run-record writes on another) for a bounded job.
* :func:`session_factory` returns the process-wide ``async_sessionmaker`` behind the API's default
  ``get_terminal_session`` provider. Its engine is created lazily on the first call, so importing
  this module, or mounting the router in a host that overrides the session provider, never
  creates a terminal engine. :func:`dispose_terminal_engine` releases it at shutdown.

``database_url`` carries the database password, so URLs are only ever logged or quoted in errors
through ``render_as_string(hide_password=True)``.
"""

import logging
from typing import Any

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from opennse_terminal.config import TerminalSettings

__all__ = ["build_engine", "dispose_terminal_engine", "session_factory"]

logger = logging.getLogger("opennse_terminal.db.session")

# The only module-level state of the database layer: one engine and sessionmaker per raw
# database_url, so tests and tools that address several databases in one process each get their
# own pool. An asyncpg pool is bound to the event loop that created it, so a cached engine must be
# used from a single loop (uvicorn's). Code that switches loops, such as tests, calls
# dispose_terminal_engine() in between.
_CACHE: dict[str, tuple[AsyncEngine, async_sessionmaker[AsyncSession]]] = {}


def build_engine(settings: TerminalSettings, **engine_options: Any) -> AsyncEngine:
    """Create a new async engine for ``settings.database_url``.

    The URL must use the ``postgresql+asyncpg://`` scheme; anything else raises ``ValueError``
    naming the scheme requirement, with the password masked, rather than SQLAlchemy's later
    async-driver error. The engine is created with ``pool_pre_ping=True``; ``engine_options`` are
    passed through to ``create_async_engine`` unchanged (for example ``poolclass=NullPool`` in
    tests).

    Every call returns a new engine, which the caller owns and must release with
    ``await engine.dispose()``. No connection is opened until the engine is first used.
    """
    try:
        url = make_url(settings.database_url)
    except (ArgumentError, ValueError):
        # make_url raises ArgumentError for an unparseable string and ValueError (quoting the
        # port) for a non-numeric port. Both become one ValueError naming the expected form; the
        # original is not chained, so no fragment of the URL reaches a traceback.
        raise ValueError(
            "TERMINAL_DATABASE_URL is not a valid database URL; "
            "expected postgresql+asyncpg://user:password@host:port/database"
        ) from None
    masked = url.render_as_string(hide_password=True)
    # get_backend_name() is checked first: get_driver_name() loads the dialect for a URL without
    # an explicit driver, which fails for an unknown backend.
    if url.get_backend_name() != "postgresql" or url.get_driver_name() != "asyncpg":
        raise ValueError(
            f"TERMINAL_DATABASE_URL must use the postgresql+asyncpg:// scheme; got {masked}"
        )
    engine = create_async_engine(url, pool_pre_ping=True, **engine_options)
    logger.debug("created engine for %s", masked)
    return engine


def session_factory(settings: TerminalSettings | None = None) -> async_sessionmaker[AsyncSession]:
    """Return the process-wide sessionmaker for ``settings.database_url``.

    ``settings`` defaults to a fresh ``TerminalSettings()`` read from the environment. The first
    call for a URL builds its engine with :func:`build_engine` and an
    ``async_sessionmaker(engine, expire_on_commit=False)``; later calls return the same
    sessionmaker. Creation involves no ``await``, so calls within one event loop cannot race.

    Usage contract: ``api.deps.get_terminal_session`` runs
    ``async with session_factory(settings)() as session: yield session``, one ``AsyncSession`` per
    request. Nothing commits automatically: write routes and services call
    ``await session.commit()`` themselves, and closing the session rolls back anything left
    uncommitted. Release the cached engines with :func:`dispose_terminal_engine`.
    """
    settings = settings if settings is not None else TerminalSettings()
    cached = _CACHE.get(settings.database_url)
    if cached is not None:
        return cached[1]
    engine = build_engine(settings)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    _CACHE[settings.database_url] = (engine, factory)
    logger.info(
        "terminal database engine initialised for %s",
        engine.url.render_as_string(hide_password=True),
    )
    return factory


async def dispose_terminal_engine() -> None:
    """Dispose every engine cached by :func:`session_factory` and empty the cache.

    This is the shutdown hook of the standalone app's lifespan, re-exported by
    ``opennse_terminal.api`` for hosts that keep the default session provider; hosts that override
    the session never create an engine and need not call it. It is idempotent: with an empty cache
    it does nothing.

    The cache is emptied before any ``await``, so a failing dispose leaves no half-disposed entry
    behind. Every engine is still disposed when one fails; the first failure is then re-raised.
    """
    entries = list(_CACHE.values())
    _CACHE.clear()
    first_error: Exception | None = None
    for engine, _factory in entries:
        try:
            await engine.dispose()
        except Exception as exc:
            logger.exception(
                "failed to dispose terminal database engine for %s",
                engine.url.render_as_string(hide_password=True),
            )
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error
