"""Isolated Alembic history of the OpenNSE terminal.

This package holds the terminal's migration environment (``env.py``), revision template
(``script.py.mako``) and revisions (``versions/terminal_*.py``). It is a Python package so the
whole history ships in the wheel, and Alembic locates it through the package resource
:data:`SCRIPT_LOCATION` rather than a filesystem path.

There is no ``alembic.ini``. Every Alembic ``Config`` is built in code by
:func:`build_alembic_config`; ``opennse-terminal migrate`` (through :func:`run_upgrade` and
:func:`run_downgrade`), the seeder's schema check and the test fixtures all go through it. The
history is recorded in its own version table, :data:`VERSION_TABLE`, and
:func:`include_terminal_only` limits autogenerate to ``terminal_`` tables, so a host database's
``alembic_version`` and its tables are never read, written or proposed for change.

Connecting is bounded by :data:`CONNECT_TIMEOUT_SECONDS`. A connect failure or timeout raises
``ConnectionError`` naming the migration and the database, host and port through
``opennse_terminal.db.session.database_log_label``, never the driver URL, the user, the password
or the query string.

Never import ``opennse_terminal.migrations.env``: Alembic loads it by path, and importing it runs
migrations. Importing this package itself has no side effects: it opens no connection, reads no
settings and registers no models.
"""

import asyncio
from typing import Final

from alembic import command
from alembic.config import Config
from alembic.runtime.environment import NameFilterParentNames, NameFilterType
from alembic.script import ScriptDirectory
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import ArgumentError

from opennse_terminal.db.base import TABLE_PREFIX

__all__ = [
    "CONNECT_TIMEOUT_SECONDS",
    "SCRIPT_LOCATION",
    "VERSION_TABLE",
    "build_alembic_config",
    "head_revision",
    "include_terminal_only",
    "run_downgrade",
    "run_upgrade",
]

SCRIPT_LOCATION: Final[str] = "opennse_terminal:migrations"
"""Alembic ``package:path`` resource form of this directory. It resolves against the installed
``opennse_terminal`` package, so it works from an editable install and from a wheel alike."""

VERSION_TABLE: Final[str] = "terminal_alembic_version"
"""The terminal's own Alembic version table. A host's ``alembic_version`` is never touched."""

CONNECT_TIMEOUT_SECONDS: Final[float] = 10.0
"""Seconds ``env.py`` allows asyncpg to establish the migration connection (TCP, TLS and
authentication) before raising ``ConnectionError``. asyncpg's own default is 60 s; 10 s matches the
scrapers' connect timeout."""

_ASYNC_DRIVERNAME: Final[str] = "postgresql+asyncpg"
"""The only driver ``env.py`` can migrate with: it builds an async engine."""


def include_terminal_only(
    name: str | None,
    type_: NameFilterType,
    parent_names: NameFilterParentNames,
) -> bool:
    """Alembic ``include_name`` hook that admits only the terminal's own tables.

    A ``table`` is admitted only when its name starts with ``TABLE_PREFIX`` (``terminal_``).
    Every other object type (schema, column, index, unique, foreign-key or check constraint) is
    admitted, because Alembic offers those only for a managed schema or for a table this hook
    has already admitted.

    Used as ``include_name`` by ``env.py`` and by ``alembic.autogenerate.compare_metadata`` in the
    isolation tests, so autogenerate never reflects a host's tables, such as ``users`` or
    ``alembic_version``, and never proposes altering or dropping them.
    """
    if type_ == "table":
        return name is not None and name.startswith(TABLE_PREFIX)
    return True


def _require_async_postgres_url(database_url: str) -> None:
    """Raise ``ValueError`` unless ``database_url`` is a ``postgresql+asyncpg://`` URL.

    Messages never repeat the URL, so a password cannot leak into logs or tracebacks.
    """
    try:
        drivername = make_url(database_url).drivername
    except (ArgumentError, ValueError):
        # SQLAlchemy's parse error quotes the whole string, password included; drop it.
        raise ValueError("database_url is not a valid SQLAlchemy database URL") from None
    if drivername != _ASYNC_DRIVERNAME:
        raise ValueError(
            f"database_url must use the {_ASYNC_DRIVERNAME!r} driver, because the migration "
            f"environment runs on an async engine; got driver {drivername!r}"
        )


def build_alembic_config(
    database_url: str | None = None,
    *,
    connection: Connection | None = None,
) -> Config:
    """Build the in-code Alembic ``Config`` for the terminal's history.

    The config holds ``script_location`` (:data:`SCRIPT_LOCATION`), plus:

    - ``sqlalchemy.url`` when ``database_url`` is given. It must be a ``postgresql+asyncpg://``
      URL, otherwise ``ValueError`` is raised. Without a URL or connection the config can still
      read the script directory, as :func:`head_revision` does, but cannot migrate.
    - ``attributes["connection"]`` when ``connection`` is given. It must be a synchronous
      SQLAlchemy ``Connection``, such as the argument ``await async_conn.run_sync(fn)`` passes to
      ``fn``; ``env.py`` then migrates on it instead of opening its own engine, and joins any
      transaction already open on it, leaving the commit to the caller. Anything else raises
      ``TypeError``.

    No ini file is read and no logging is configured.
    """
    config = Config()
    config.set_main_option("script_location", SCRIPT_LOCATION)
    if database_url is not None:
        _require_async_postgres_url(database_url)
        # Option values go through ConfigParser interpolation; doubling "%" keeps a
        # percent-encoded password such as "p%40ss" intact when env.py reads it back.
        config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    if connection is not None:
        if not isinstance(connection, Connection):
            # env.py only shares a sync Connection; an AsyncConnection would be silently ignored.
            raise TypeError(
                "connection must be a synchronous sqlalchemy.engine.Connection, such as the "
                "argument AsyncConnection.run_sync() passes to its function"
            )
        config.attributes["connection"] = connection
    return config


def head_revision() -> str:
    """Return the single head revision of the packaged history (``"terminal_0002"`` today).

    Reads only the script directory and never touches a database. Raises ``RuntimeError`` when
    the history has no head or several heads. The seeder compares
    ``terminal_alembic_version.version_num`` with this value before writing anything.
    """
    heads = ScriptDirectory.from_config(build_alembic_config()).get_heads()
    if len(heads) != 1:
        found = ", ".join(sorted(heads)) or "none"
        raise RuntimeError(f"expected exactly one terminal migration head; found {found}")
    return heads[0]


def _require_no_running_loop(caller: str) -> None:
    """Raise ``RuntimeError`` when called from a thread that is running an event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        f"{caller}() must be called outside a running event loop, because env.py starts its own "
        f"with asyncio.run(); from async code use `await asyncio.to_thread({caller}, ...)`"
    )


def run_upgrade(database_url: str, revision: str = "head") -> None:
    """Upgrade the terminal schema at ``database_url`` to ``revision`` (default ``head``).

    Synchronous, and must be called outside a running event loop: ``env.py`` drives its async
    engine with ``asyncio.run``, so a call from inside a loop raises ``RuntimeError``. Async
    callers use ``await asyncio.to_thread(run_upgrade, url)``, or migrate on a shared connection
    with ``build_alembic_config(connection=...)``. ``database_url`` must be a
    ``postgresql+asyncpg://`` URL (``ValueError`` otherwise). Only ``terminal_`` tables and
    :data:`VERSION_TABLE` are created or changed; upgrading an already current database is a
    no-op.

    Connecting is bounded by :data:`CONNECT_TIMEOUT_SECONDS`. A connect failure or timeout raises
    ``ConnectionError`` naming the migration, its target revision and the database, host and port
    through ``database_log_label`` (for example ``database "opennse" on localhost:5432``), never
    the driver URL, the user, the password or the query string; the original ``OSError`` is its
    ``__cause__``. Errors the server reports, such as a rejected password or a missing database,
    propagate unchanged from asyncpg.
    """
    _require_no_running_loop("run_upgrade")
    command.upgrade(build_alembic_config(database_url), revision)


def run_downgrade(database_url: str, revision: str) -> None:
    """Downgrade the terminal schema at ``database_url`` to ``revision``, e.g. ``"base"``.

    Same calling rules as :func:`run_upgrade`: synchronous, outside a running event loop, with a
    ``postgresql+asyncpg://`` URL. Only the revisions' own ``downgrade()`` steps run, newest
    first, so ``downgrade base`` restores the two ticker foreign keys ``terminal_0002`` recreated
    to their ``terminal_0001`` definitions, then drops the 11 tables ``terminal_0001`` created,
    and nothing else. Alembic keeps :data:`VERSION_TABLE`, now empty; that is intended, since
    only what the revisions created is dropped. Checks that every ``terminal_`` table is gone
    must therefore exclude :data:`VERSION_TABLE` or assert that it holds no rows.

    A connect failure or timeout raises the same credential-free ``ConnectionError`` as
    :func:`run_upgrade`, within :data:`CONNECT_TIMEOUT_SECONDS`.
    """
    _require_no_running_loop("run_downgrade")
    command.downgrade(build_alembic_config(database_url), revision)
