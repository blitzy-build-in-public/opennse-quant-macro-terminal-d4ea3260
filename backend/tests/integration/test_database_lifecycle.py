"""Lifecycle of the suite's own databases: where generated URLs connect, and what gets dropped.

The root conftest's ``DatabaseFactory`` creates every database the suite uses on the server named
by ``TERMINAL_TEST_DATABASE_URL``. That URL's database is the administrative connection only and
never holds test data, and every database the factory creates is dropped again. These tests prove
the guarantees that depend on the URL and on failure timing:

- every URL the factory generates connects to the database it names, even when the
  administrative URL carries a ``database`` query option, which asyncpg applies after the path;
- a database whose ``CREATE DATABASE`` committed is dropped by ``close()`` even when the creating
  call then fails or is cancelled;
- a caller cancelled while its ``CREATE DATABASE`` is in flight raises ``CancelledError`` only
  once that statement has settled, so the database it leaves behind is tracked and dropped.

The factory class is reached as ``type(database_factory)``, because test modules never import a
conftest. Each test builds private factories over AUTOCOMMIT engines of its own and closes them
in ``finally``. A private factory only ever creates empty databases: the seeded template's name
is fixed, so a private factory that built it would destroy the session's template.

Only catalogue and server-function queries are written as SQL here; no table is read or written.
"""

import asyncio
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Final, Protocol

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine
from opennse_terminal.migrations import VERSION_TABLE, run_upgrade

WAIT_SECONDS: Final[float] = 120.0
"""Upper bound of every wait on database work (a CREATE DATABASE or a migration to head)."""

TERMINAL_TABLE_COUNT: Final[int] = 11
"""Tables of the terminal schema at head, besides the version table."""

_STAND_IN_PATH_DATABASE: Final[str] = "postgres"
"""Path database of the stand-in administrative URL; its ``database`` query option overrides it."""

PENDING_SECONDS: Final[float] = 0.5
"""How long a cancelled caller must stay pending while its ``CREATE DATABASE`` is held."""

_CREATE_DATABASE: Final = re.compile(r'CREATE DATABASE "?(?P<name>[a-z0-9_]{1,63})"? ')
"""A factory's ``CREATE DATABASE`` statement, capturing the generated database name."""

_EXISTS_SQL: Final[str] = "SELECT 1 FROM pg_database WHERE datname = :name"

_Execute = Callable[..., Awaitable[None]]
"""Signature of the factory's ``_execute(statement, **params)`` statement runner."""


class _Handle(Protocol):
    """The part of the root conftest's ``DatabaseHandle`` these tests use."""

    @property
    def name(self) -> str:
        """Name of the database."""

    @property
    def url(self) -> str:
        """Connection URL of the database, password included."""

    @property
    def engine(self) -> AsyncEngine:
        """Engine on the database, owned and disposed by the factory."""


class _Factory(Protocol):
    """The part of the root conftest's ``DatabaseFactory`` these tests use.

    ``_live`` (database name to handle engine) is what ``close()`` drops, and ``_execute`` runs
    each administrative statement; the cancellation tests read the first and wrap the second.
    """

    _live: dict[str, AsyncEngine | None]
    _execute: _Execute

    def __init__(self, admin_engine: AsyncEngine) -> None:
        """Create a factory working through the AUTOCOMMIT ``admin_engine``."""

    def url_for(self, name: str) -> str:
        """The administrative URL pointed at database ``name``, password included."""

    async def create_empty(self, label: str) -> _Handle:
        """Create the empty database ``opennse_iso_<label>_<uuid8>`` from ``template0``."""

    async def drop(self, handle: _Handle) -> None:
        """Dispose the handle's engine and drop its database."""

    async def close(self) -> None:
        """Drop every database still tracked."""


@asynccontextmanager
async def _autocommit_engine(settings: TerminalSettings) -> AsyncIterator[AsyncEngine]:
    """A new AUTOCOMMIT engine on ``settings.database_url``, disposed on exit."""
    engine = build_engine(settings, isolation_level="AUTOCOMMIT")
    try:
        yield engine
    finally:
        await engine.dispose()


@asynccontextmanager
async def _private_factory(
    factory_class: type[_Factory], admin_engine: AsyncEngine
) -> AsyncIterator[_Factory]:
    """A factory of ``factory_class`` over ``admin_engine``, closed on exit.

    Closing drops whatever the factory still tracks, so a failing test leaves nothing behind.
    """
    factory = factory_class(admin_engine)
    try:
        yield factory
    finally:
        await factory.close()


async def _scalar(engine: AsyncEngine, sql: str, **params: object) -> Any:
    """Run the static query ``sql`` with bound ``params`` and return its single value."""
    async with engine.connect() as connection:
        result = await connection.execute(text(sql), params)
        return result.scalar_one()


async def _exists(engine: AsyncEngine, name: str) -> bool:
    """Whether the server ``engine`` connects to has a database called ``name``."""
    async with engine.connect() as connection:
        result = await connection.execute(text(_EXISTS_SQL), {"name": name})
        return result.scalar_one_or_none() is not None


async def _drop_leftovers(factory: _Factory, names: list[str]) -> None:
    """Drop through ``factory`` whatever of ``names`` a failed assertion left on the server.

    Each name is re-tracked and the factory closed again, so a regression of the cleanup under
    test still leaves the shared server clean.
    """
    for name in names:
        factory._live.setdefault(name, None)
    await factory.close()


async def _public_tables(engine: AsyncEngine) -> set[str]:
    """Names of the tables in the ``public`` schema of ``engine``'s database."""
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        )
        return {name for (name,) in result}


@pytest.mark.parametrize(
    ("admin_url", "kept_query"),
    [
        pytest.param(
            "postgresql+asyncpg://tester@db.invalid:5432/postgres?database=opennse_admin",
            {},
            id="database-query-option",
        ),
        pytest.param(
            "postgresql+asyncpg://tester@db.invalid:5432/postgres"
            "?database=opennse_admin&host=/some/socket/dir",
            {"host": "/some/socket/dir"},
            id="database-and-socket-host-query-options",
        ),
        pytest.param(
            "postgresql+asyncpg://tester@db.invalid:5432/opennse_admin",
            {},
            id="plain",
        ),
    ],
)
async def test_generated_url_connects_to_the_named_database(
    database_factory: _Factory,
    make_settings: Callable[..., TerminalSettings],
    admin_url: str,
    kept_query: dict[str, str],
) -> None:
    """``url_for(name)`` names ``name`` as the database asyncpg connects to, whatever the query.

    No connection is opened: the URLs name an unresolvable host, and only the URL and the
    dialect's translation of it into asyncpg connect arguments are examined.
    """
    name = "opennse_iso_url_case"
    admin = make_url(admin_url)
    async with (
        _autocommit_engine(make_settings(database_url=admin_url)) as engine,
        _private_factory(type(database_factory), engine) as factory,
    ):
        generated = make_url(factory.url_for(name))
        _args, connect_kwargs = engine.dialect.create_connect_args(generated)
    assert generated.database == name
    assert "database" not in generated.query
    assert dict(generated.query) == kept_query
    assert (generated.drivername, generated.username, generated.host, generated.port) == (
        admin.drivername,
        admin.username,
        admin.host,
        admin.port,
    )
    assert connect_kwargs["database"] == name


async def test_admin_url_database_option_does_not_redirect_created_databases(
    database_factory: _Factory,
    make_settings: Callable[..., TerminalSettings],
) -> None:
    """A database created and migrated through a ``?database=`` admin URL holds the schema itself.

    An empty database from the session factory stands in for the administrative database, and a
    private factory reaches it only through the ``database`` query option of its URL, as with an
    administrative URL like ``.../postgres?database=opennse_test``. A database that factory
    creates is migrated to head through its generated URL. The tables must appear in that
    database, and the stand-in must still hold no table at all.
    """
    stand_in = await asyncio.wait_for(database_factory.create_empty("admin_stand_in"), WAIT_SECONDS)
    try:
        admin_url = (
            make_url(stand_in.url)
            .set(database=_STAND_IN_PATH_DATABASE)
            .update_query_dict({"database": stand_in.name})
            .render_as_string(hide_password=False)
        )
        async with (
            _autocommit_engine(make_settings(database_url=admin_url)) as engine,
            _private_factory(type(database_factory), engine) as factory,
        ):
            # Precondition: the query option, not the path, selects the administrative database.
            assert await _scalar(engine, "SELECT current_database()") == stand_in.name
            handle = await asyncio.wait_for(factory.create_empty("redirect"), WAIT_SECONDS)
            await asyncio.wait_for(asyncio.to_thread(run_upgrade, handle.url, "head"), WAIT_SECONDS)
            assert await _scalar(handle.engine, "SELECT current_database()") == handle.name
            tables = await _public_tables(handle.engine)
        stand_in_tables = await _scalar(
            stand_in.engine,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
        )
    finally:
        await database_factory.drop(stand_in)
    assert VERSION_TABLE in tables
    assert len(tables - {VERSION_TABLE}) == TERMINAL_TABLE_COUNT
    assert all(table.startswith("terminal_") for table in tables)
    assert stand_in_tables == 0


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(RuntimeError, id="connection-close-error"),
        pytest.param(asyncio.CancelledError, id="cancelled"),
    ],
)
async def test_database_committed_before_a_failure_is_dropped_by_close(
    database_factory: _Factory,
    make_settings: Callable[..., TerminalSettings],
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    """A failure right after ``CREATE DATABASE`` committed leaves the database to ``close()``.

    The factory's statement runner executes the real statement, then raises ``failure``, as a
    failing connection release or a cancellation arriving after the commit would. The creating
    call raises that exception, the database exists and is tracked, and ``close()`` drops it.
    """
    created: list[str] = []
    async with (
        _autocommit_engine(make_settings()) as engine,
        _private_factory(type(database_factory), engine) as factory,
    ):
        original = factory._execute

        async def fail_after_create(statement: str, **params: object) -> None:
            await original(statement, **params)
            match = _CREATE_DATABASE.match(statement)
            if match is not None:
                created.append(match["name"])
                raise failure("injected after CREATE DATABASE committed")

        monkeypatch.setattr(factory, "_execute", fail_after_create)
        try:
            with pytest.raises(failure):
                await asyncio.wait_for(factory.create_empty("post_commit"), WAIT_SECONDS)
            (name,) = created
            assert await _exists(engine, name)
            assert name in factory._live
            await factory.close()
            assert not await _exists(engine, name)
        finally:
            await _drop_leftovers(factory, created)


async def test_caller_cancelled_during_create_waits_for_it_and_leaves_it_to_close(
    database_factory: _Factory,
    make_settings: Callable[..., TerminalSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling a caller whose ``CREATE DATABASE`` is in flight neither aborts nor leaks it.

    The factory's statement runner holds the ``CREATE DATABASE`` on an event. The caller is
    cancelled while it is held and must stay pending; once the statement is released and has
    committed, the caller raises ``CancelledError``, and the database exists, is tracked, and is
    dropped by ``close()``.
    """
    held: list[str] = []
    created: list[str] = []
    finished_after_create: list[bool] = []
    entered = asyncio.Event()
    release = asyncio.Event()
    caller_finished = asyncio.Event()

    def on_caller_done(_task: asyncio.Task[Any]) -> None:
        finished_after_create.append(bool(created))
        caller_finished.set()

    async with (
        _autocommit_engine(make_settings()) as engine,
        _private_factory(type(database_factory), engine) as factory,
    ):
        original = factory._execute

        async def hold_create(statement: str, **params: object) -> None:
            match = _CREATE_DATABASE.match(statement)
            if match is None:
                await original(statement, **params)
                return
            held.append(match["name"])
            entered.set()
            await release.wait()
            await original(statement, **params)
            created.append(match["name"])

        monkeypatch.setattr(factory, "_execute", hold_create)
        caller = asyncio.create_task(factory.create_empty("cancelled"))
        caller.add_done_callback(on_caller_done)
        try:
            await asyncio.wait_for(entered.wait(), WAIT_SECONDS)
            (name,) = held
            caller.cancel()
            # The caller must still be pending after PENDING_SECONDS: its CREATE is held.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(caller_finished.wait(), PENDING_SECONDS)
            assert not caller.done()
            assert not await _exists(engine, name)
            assert name in factory._live
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(caller, WAIT_SECONDS)
            assert caller.cancelled()
            assert created == [name]
            assert finished_after_create == [True]
            assert await _exists(engine, name)
            assert name in factory._live
            await factory.close()
            assert not await _exists(engine, name)
        finally:
            release.set()
            try:
                await asyncio.wait_for(caller_finished.wait(), WAIT_SECONDS)
            finally:
                # Retrieved so that a failure here never ends as an unretrieved task exception.
                if caller.done() and not caller.cancelled():
                    caller.exception()
                await _drop_leftovers(factory, held)
