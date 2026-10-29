"""Async engine and session factory of the OpenNSE terminal (SQLAlchemy 2.0 on asyncpg).

Two entry points serve two ownership models:

* :func:`build_engine` returns a new engine that the caller owns and disposes. Ingestion runs, the
  seeder, the CSV importer and the test fixtures use it, because they need dedicated connections
  (a transaction-scoped advisory lock on one, run-record writes on another) for a bounded job.
  The seeder opens its session on such an engine through :func:`build_session_factory`, the one
  place the terminal's ``async_sessionmaker`` configuration lives.
* :func:`session_factory` returns the process-wide ``async_sessionmaker`` behind the API's default
  ``get_terminal_session`` provider. Its engine is created lazily on the first call, so importing
  this module, or mounting the router in a host that overrides the session provider, never
  creates a terminal engine. :func:`dispose_terminal_engine` releases it at shutdown.

``database_url`` carries the database password, so no URL or other connection string is ever
logged or quoted in errors: log records and messages name the database only through
:func:`database_log_label`, a prose label such as ``database "opennse" on localhost:5432``.
``render_as_string(hide_password=True)`` is not enough: it masks only the password before ``@``
and prints every query value, and asyncpg accepts the password as a query value too
(``?password=...``).
"""

import json
import logging
from typing import Any

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from opennse_terminal.config import TerminalSettings

__all__ = [
    "build_engine",
    "build_session_factory",
    "database_log_label",
    "dispose_terminal_engine",
    "session_factory",
]

logger = logging.getLogger("opennse_terminal.db.session")

_UNPARSEABLE_URL_LABEL = "<unparseable database URL>"

# The only module-level state of the database layer: one engine and sessionmaker per raw
# database_url, so tests and tools that address several databases in one process each get their
# own pool. An asyncpg pool is bound to the event loop that created it, so a cached engine must be
# used from a single loop (uvicorn's). Code that switches loops, such as tests, calls
# dispose_terminal_engine() in between.
_CACHE: dict[str, tuple[AsyncEngine, async_sessionmaker[AsyncSession]]] = {}


def _escaped(text: str) -> str:
    """``text`` with quotes, backslashes and control characters escaped as in a JSON string, so a
    crafted host or database name can neither end its quotes nor break or forge a log line."""
    return json.dumps(text, ensure_ascii=False)[1:-1]


def database_log_label(url: str | URL) -> str:
    """Return a credential-free prose label of ``url`` for log records and error messages.

    The label names only the database, host and port, and is neither a URL nor a libpq
    ``key=value`` string, so it cannot be used to connect:

    * ``database "opennse" on localhost:5432``; without a port, ``database "opennse" on
      localhost``; an IPv6 host is bracketed, ``database "opennse" on [::1]:5433``;
    * ``database "opennse_c1"`` when the URL has no host part, as when the host is given only in
      the query (a Unix-socket directory or a multi-host list);
    * ``default database on localhost:5432`` when the URL names no database.

    The driver, the username, the password and the whole query string are dropped: asyncpg takes
    connection arguments, the password included, from query values, which
    ``URL.render_as_string(hide_password=True)`` prints unchanged. Quotes, backslashes and control
    characters in the host or database are escaped as in a JSON string.

    It never raises, and never echoes a URL it cannot take apart reliably: a string that
    ``make_url`` cannot parse, and a URL whose host or database contains ``@`` (the parser's
    reading of a username or password with an unescaped ``@`` or ``/``, so that part may hold the
    credential), both give the fixed label ``<unparseable database URL>``. The URL used to connect
    is never changed by this function.
    """
    try:
        parsed = make_url(url)
    except (ArgumentError, ValueError):
        # ArgumentError: not a URL at all; ValueError: a non-numeric port, which make_url quotes.
        return _UNPARSEABLE_URL_LABEL
    host = parsed.host
    database = parsed.database
    if "@" in (host or "") or "@" in (database or ""):
        return _UNPARSEABLE_URL_LABEL
    label = f'database "{_escaped(database)}"' if database else "default database"
    if not host:
        return label
    # make_url strips the brackets of an IPv6 literal; they are restored so the port stays
    # distinguishable from the address.
    location = f"[{_escaped(host)}]" if ":" in host else _escaped(host)
    if parsed.port is not None:
        location = f"{location}:{parsed.port}"
    return f"{label} on {location}"


def build_engine(settings: TerminalSettings, **engine_options: Any) -> AsyncEngine:
    """Create a new async engine for ``settings.database_url``.

    The URL must parse, use the ``postgresql+asyncpg://`` scheme and carry query values of the
    types the dialect expects (an integer ``prepared_statement_cache_size``, for example);
    otherwise ``ValueError`` is raised instead of SQLAlchemy's later async-driver error. Its
    message names the expected form, or the URL's :func:`database_log_label` (and for a wrong
    scheme its driver), and nothing else of the URL. Every message begins with ``database URL``
    and does not say where the URL came from (``TERMINAL_DATABASE_URL``, ``.env``, a command-line
    flag or a host's own ``TerminalSettings``), so a caller that knows the source adds it. The
    engine is created with ``pool_pre_ping=True``; ``engine_options`` are passed through to
    ``create_async_engine`` unchanged (for example ``poolclass=NullPool`` in tests).

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
            "database URL is not valid; "
            "expected postgresql+asyncpg://user:password@host:port/database"
        ) from None
    label = database_log_label(url)
    # get_backend_name() is checked first: get_driver_name() loads the dialect for a URL without
    # an explicit driver, which fails for an unknown backend.
    if url.get_backend_name() != "postgresql" or url.get_driver_name() != "asyncpg":
        # make_url accepts only word characters and "+" in the driver name, so quoting it echoes
        # no credential.
        raise ValueError(
            "database URL must use the postgresql+asyncpg:// scheme; "
            f'got driver "{url.drivername}" for {label}'
        )
    try:
        engine = create_async_engine(url, pool_pre_ping=True, **engine_options)
    except ValueError:
        # The asyncpg dialect converts typed query values (port, prepared_statement_cache_size,
        # async_fallback) while the engine is created, and its ValueError quotes the value. It is
        # replaced, unchained, by one that names only the label.
        raise ValueError(f"database URL has an invalid query parameter value for {label}") from None
    logger.debug("created engine for %s", label)
    return engine


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Return a new sessionmaker of the terminal's configuration, bound to ``engine``.

    This is the single definition of how the terminal opens sessions:
    ``async_sessionmaker(engine, expire_on_commit=False)``, so objects stay readable after
    ``commit()`` without another round trip. :func:`session_factory` builds its cached
    sessionmaker with it, and the seeder its one-shot session on an engine from
    :func:`build_engine`.

    Nothing is cached and no connection is opened. The caller keeps ownership of ``engine`` and
    releases it with ``await engine.dispose()`` once its sessions are closed.
    """
    return async_sessionmaker(engine, expire_on_commit=False)


def session_factory(settings: TerminalSettings | None = None) -> async_sessionmaker[AsyncSession]:
    """Return the process-wide sessionmaker for ``settings.database_url``.

    ``settings`` defaults to a fresh ``TerminalSettings()`` read from the environment. The first
    call for a URL builds its engine with :func:`build_engine` and its sessionmaker with
    :func:`build_session_factory`; later calls return the same sessionmaker. Creation involves no
    ``await``, so calls within one event loop cannot race.

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
    factory = build_session_factory(engine)
    _CACHE[settings.database_url] = (engine, factory)
    logger.info("terminal database engine initialised for %s", database_log_label(engine.url))
    return factory


async def dispose_terminal_engine() -> None:
    """Dispose every engine cached by :func:`session_factory` and empty the cache.

    This is the shutdown hook of the standalone app's lifespan, re-exported by
    ``opennse_terminal.api`` for hosts that keep the default session provider; hosts that override
    the session never create an engine and need not call it. It is idempotent: with an empty cache
    it does nothing.

    The cache is emptied before any ``await``, so a failing dispose leaves no half-disposed entry
    behind. Every engine is still disposed when one fails. Each failure is logged at ERROR with the
    engine's :func:`database_log_label` and the exception's type name only; once every engine has
    been tried, a ``RuntimeError`` naming the failed engines' labels and exception type names is
    raised, for example ``failed to dispose 1 terminal database engine: database "opennse" on
    localhost:5432 (OSError)``. The original exception is never logged, with or without its
    traceback, and never chained: a driver, pool or network error's text and cause chain can quote
    the connection URL, password included.
    """
    entries = list(_CACHE.values())
    _CACHE.clear()
    failures: list[str] = []
    for engine, _factory in entries:
        try:
            await engine.dispose()
        except Exception as exc:
            label = database_log_label(engine.url)
            error_type = type(exc).__name__
            # No exc_info and no exception text: either can carry connection details.
            logger.error("failed to dispose terminal database engine for %s: %s", label, error_type)
            failures.append(f"{label} ({error_type})")
    if failures:
        noun = "engine" if len(failures) == 1 else "engines"
        detail = "; ".join(failures)
        # Raised outside the except clause and from None, so no original exception is chained or
        # kept as __cause__ or __context__.
        raise RuntimeError(
            f"failed to dispose {len(failures)} terminal database {noun}: {detail}"
        ) from None
