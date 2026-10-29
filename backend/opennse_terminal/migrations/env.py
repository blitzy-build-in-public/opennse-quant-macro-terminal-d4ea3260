"""Alembic environment of the terminal's isolated migration history.

Alembic loads this file by path for every ``upgrade``, ``downgrade``, ``revision`` or offline SQL
command, and loading it runs that command. Never import it directly: tests and tooling use
:mod:`opennse_terminal.migrations` for the filter, the version table and the config builder.

Isolation contract with a host database:

- ``target_metadata`` is ``TerminalBase.metadata``, the terminal's own collection holding only
  the 11 ``terminal_`` tables, never a host's ``MetaData``.
- The history is recorded in ``terminal_alembic_version``, so a host's ``alembic_version`` is
  never read or written.
- ``include_name=include_terminal_only`` keeps autogenerate from reflecting, altering or
  proposing to drop any table whose name lacks the ``terminal_`` prefix.
- Online migrations run on a SQLAlchemy async engine over asyncpg; the synchronous
  ``Connection`` Alembic needs exists only inside ``AsyncConnection.run_sync``.
- There is no ``alembic.ini`` and no ``fileConfig``: the ``Config`` comes from
  ``opennse_terminal.migrations.build_alembic_config``, and logging is configured by the caller.
"""

import asyncio
from contextlib import AsyncExitStack
from typing import Any, Final

from alembic import context
from alembic.util import CommandError
from sqlalchemy import pool
from sqlalchemy.engine import URL, Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Imported for its side effect: it registers all 11 terminal_ tables on TerminalBase.metadata, so
# autogenerate compares the complete schema instead of proposing to drop unregistered tables.
import opennse_terminal.models  # noqa: F401
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.db.session import database_log_label
from opennse_terminal.migrations import (
    CONNECT_TIMEOUT_SECONDS,
    VERSION_TABLE,
    include_terminal_only,
)

config = context.config
target_metadata = TerminalBase.metadata

_MISSING_URL_MESSAGE: Final[str] = (
    "sqlalchemy.url is not set; use opennse_terminal.migrations.build_alembic_config"
)


def _configure_kwargs() -> dict[str, Any]:
    """Return the ``context.configure`` options shared by offline and online mode.

    ``version_table_pk`` keeps its default, so Alembic names the version table's primary key
    ``terminal_alembic_version_pkc`` and every constraint name still contains ``terminal_``.
    ``include_schemas`` stays off: only the connection's default schema is managed.
    """
    return {
        "target_metadata": target_metadata,
        "version_table": VERSION_TABLE,
        "include_name": include_terminal_only,
        "compare_type": True,
    }


def run_migrations_offline() -> None:
    """Emit the migration SQL for ``sqlalchemy.url``'s dialect without connecting.

    Bound values are rendered inline, and the script records its revisions in
    ``terminal_alembic_version``.
    """
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError(_MISSING_URL_MESSAGE)
    context.configure(
        url=url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_configure_kwargs(),
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run the pending migrations on a synchronous ``connection``.

    Alembic begins and commits its own transaction when ``connection`` has none open. When the
    caller already holds a transaction on it, Alembic joins that transaction and leaves the commit
    or rollback to the caller.
    """
    context.configure(connection=connection, **_configure_kwargs())
    with context.begin_transaction():
        context.run_migrations()


def _target_revision_label() -> str:
    """Return `` to revision '<id>'`` for the running command's destination, else ``""``.

    Alembic resolves ``head`` to the revision id and ``base`` to ``None``. Commands without a
    destination (``current``, ``check``, ``revision``) raise ``KeyError`` here and an
    unresolvable destination raises ``CommandError``; neither may replace the connect error being
    reported, so both give no label.
    """
    try:
        revision = context.get_revision_argument()
    except (KeyError, CommandError):
        return ""
    if revision is None:
        return " to revision 'base'"
    if isinstance(revision, str):
        return f" to revision {revision!r}"
    return ""


def _connect_failure_message(url: URL, exc: OSError) -> str:
    """Describe a failed migration connect to ``url`` without any credential.

    The message names the operation and its target revision, the database through
    ``database_log_label`` (database, host and port in prose; never the driver URL, the user, the
    password or the query string) and the cause: the deadline when asyncpg's own timeout expired,
    otherwise the error type and its ``strerror`` when set.
    """
    if isinstance(exc, TimeoutError) and exc.errno is None:
        # asyncpg's deadline expired; a TimeoutError carrying an errno is the OS's ETIMEDOUT.
        cause = f"timed out after {CONNECT_TIMEOUT_SECONDS:g} s"
    elif exc.strerror:
        cause = f"{type(exc).__name__}: {exc.strerror}"
    else:
        cause = type(exc).__name__
    return (
        f"terminal migration{_target_revision_label()} could not connect to "
        f"{database_log_label(url)}: {cause}"
    )


async def run_async_migrations() -> None:
    """Open a single-use async engine on ``sqlalchemy.url`` and migrate through ``run_sync``.

    ``NullPool`` keeps no connection alive after the run, and the engine is always disposed.
    asyncpg gets ``CONNECT_TIMEOUT_SECONDS`` (from :mod:`opennse_terminal.migrations`) to
    establish the connection, TCP, TLS and authentication included, instead of its 60 s default.
    An ``OSError`` while connecting (the timeout, a refused connection, an unresolvable host) is
    raised as ``ConnectionError`` whose message names the operation, the target revision and the
    database, host and port through ``database_log_label``, never the driver URL, the user, the
    password or the query string, with the original error as ``__cause__``. Errors the server
    reports, such as a rejected password or a missing database, and every error raised while
    migrating propagate unchanged.
    """
    engine_options = config.get_section(config.config_ini_section, {})
    if not engine_options.get("sqlalchemy.url"):
        raise RuntimeError(_MISSING_URL_MESSAGE)
    engine = async_engine_from_config(
        engine_options,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args={"timeout": CONNECT_TIMEOUT_SECONDS},
    )
    try:
        # The exit stack closes the connection exactly as ``async with engine.connect()`` does,
        # while only the connect step sits inside the translating ``try``.
        async with AsyncExitStack() as stack:
            try:
                connection = await stack.enter_async_context(engine.connect())
            except OSError as exc:
                raise ConnectionError(_connect_failure_message(engine.url, exc)) from exc
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    """Migrate on the caller's shared connection, or else on an engine of the terminal's own.

    A synchronous ``Connection`` in ``config.attributes["connection"]`` (Alembic's
    connection-sharing pattern, set by ``build_alembic_config(connection=...)`` from inside
    ``await async_conn.run_sync(...)``) is migrated on directly. Without one, an engine is built
    from ``sqlalchemy.url`` and driven with ``asyncio.run``, so this path must not be reached from
    a thread already running an event loop; async callers use ``asyncio.to_thread``.
    """
    connection = config.attributes.get("connection")
    if connection is None:
        asyncio.run(run_async_migrations())
    elif isinstance(connection, Connection):
        do_run_migrations(connection)
    else:
        # Ignoring it would silently migrate whatever sqlalchemy.url names instead.
        raise TypeError(
            "config.attributes['connection'] must be a synchronous sqlalchemy.engine.Connection, "
            f"not {type(connection).__name__}"
        )


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
