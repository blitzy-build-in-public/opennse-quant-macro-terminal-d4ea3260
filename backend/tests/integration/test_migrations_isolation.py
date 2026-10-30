"""Schema isolation between the terminal's migration history and a host database.

The terminal must be safe to migrate into a host's PostgreSQL database: every table, index and
constraint it creates is ``terminal_``-prefixed, it creates no ``users``/``auth`` lookalike, its
history lives in ``terminal_alembic_version``, and neither upgrading nor downgrading reads or
changes the host's own tables or the host's ``alembic_version``. The models must also equal the
migration, so autogenerate has nothing to propose.

Migrations run through the product's own ``opennse-terminal migrate`` command, in-process, on its
async (asyncpg) engine, exactly as a host operator runs it.

A database already stamped with the first shipped revision, ``terminal_0001``, must reach the
same schema on ``upgrade head`` as a fresh database does, so the upgrade path is checked too.

This is the only test module that creates objects without the ``terminal_`` prefix: the simulated
host tables ``users`` and ``alembic_version``. They stand in for a host's schema, so the "no
generic users or auth tables" requirement of the Database Schema Isolation rule is about the
terminal's schema, which these tests prove creates none. They are created only inside the
``empty_database`` fixture, a fresh database copied from ``template0`` for each test and dropped
afterwards, never the seeded template or a clone of it.

Every query helper opens and closes its own connection and leaves no transaction open, and no
connection is held across a CLI call: a lingering transaction that touched a ``terminal_`` table
would block the ``DROP TABLE`` statements of ``migrate downgrade base``.

Two further sections need no database and request no database fixture: the connect failures of
the migration environment (``migrations/env.py``), and the referential actions of every foreign
key to ``terminal_equities.ticker`` in the models and in the offline DDL of the migration
history, at the head and at ``terminal_0001``. A last section, on the root conftest's
``database_factory`` and creating no host object, covers the lifecycle of the suite's own
databases: the URLs the factory generates connect to the database they name, a database whose
``CREATE DATABASE`` committed is dropped even when its creating call fails or is cancelled, and
private factories create only empty databases, which migrate to head through those URLs. Each
section opens with its own contract.
"""

from __future__ import annotations

import asyncio
import errno
import logging
import pprint
import re
import socket
import time
import traceback
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager
from typing import Any, Final, Protocol

import asyncpg
import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import ForeignKeyConstraint, insert, select, text, update
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

# Imported for its side effect: it registers all 11 terminal_ tables on TerminalBase.metadata.
import opennse_terminal.models  # noqa: F401
from opennse_terminal import cli, migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.base import TABLE_PREFIX, TerminalBase
from opennse_terminal.db.session import build_engine
from opennse_terminal.migrations import (
    VERSION_TABLE,
    build_alembic_config,
    head_revision,
    include_terminal_only,
    run_downgrade,
    run_upgrade,
)
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalEquity,
    TerminalWatchlist,
    TerminalWatchlistItem,
)

TERMINAL_TABLES: Final[frozenset[str]] = frozenset(
    {
        "terminal_equities",
        "terminal_daily_prices",
        "terminal_macro_indicators",
        "terminal_bond_yields",
        "terminal_fundamentals",
        "terminal_corporate_filings",
        "terminal_ipo_scenarios",
        "terminal_cycles",
        "terminal_watchlists",
        "terminal_watchlist_items",
        "terminal_ingestion_runs",
    }
)
"""The 11 product tables; the migration creates exactly these plus the version table."""

FORBIDDEN_NAME: Final = re.compile(r"(^|_)(users?|auth\w*|accounts?|sessions?|roles?)(_|$)")
"""A user, auth, account, session or role word in a table name with its prefix removed."""

PG_IDENTIFIER_MAX: Final[int] = 63
"""PostgreSQL's identifier length limit (NAMEDATALEN - 1)."""

HOST_REVISION: Final[str] = "host_0001"
HOST_EMAIL: Final[str] = "host@example.invalid"

HOST_STATE: Final[dict[str, list[tuple[Any, ...]]]] = {
    "alembic_version": [(HOST_REVISION,)],
    "users": [(1, HOST_EMAIL)],
    "users_columns": [("id",), ("email",)],
}
"""What :func:`_host_state` reads while the simulated host objects are untouched."""

BOUNDARY_REVISION: Final[str] = "terminal_0001"
"""The first shipped revision; databases stamped with it must still upgrade to the head schema."""

TICKER_FK_ACTIONS_AT_BOUNDARY: Final[dict[str, tuple[str, str]]] = {
    "fk_terminal_corporate_filings_ticker_terminal_equities": ("a", "n"),
    "fk_terminal_daily_prices_ticker_terminal_equities": ("c", "c"),
    "fk_terminal_fundamentals_ticker_terminal_equities": ("c", "c"),
    "fk_terminal_watchlist_items_ticker_terminal_equities": ("a", "c"),
}
"""``(confupdtype, confdeltype)`` of each foreign key to ``terminal_equities`` at
``terminal_0001``, in ``pg_constraint`` codes: ``a`` no action, ``c`` cascade, ``n`` set null."""

TICKER_FK_ACTIONS_AT_HEAD: Final[dict[str, tuple[str, str]]] = {
    "fk_terminal_corporate_filings_ticker_terminal_equities": ("c", "n"),
    "fk_terminal_daily_prices_ticker_terminal_equities": ("c", "c"),
    "fk_terminal_fundamentals_ticker_terminal_equities": ("c", "c"),
    "fk_terminal_watchlist_items_ticker_terminal_equities": ("c", "c"),
}
"""The same foreign keys at the head: every one cascades on update; delete actions unchanged."""

OLD_TICKER: Final[str] = "RNMOLD"
NEW_TICKER: Final[str] = "RNMNEW"
"""An equity's ticker before and after the rename the upgrade-path test performs."""


class _DatabaseHandle(Protocol):
    """The part of the ``empty_database`` fixture's handle these tests use."""

    @property
    def url(self) -> str:
        """Connection URL of the database, password included."""

    @property
    def engine(self) -> AsyncEngine:
        """Engine on the database, owned and disposed by the fixture."""


async def _fetch(engine: AsyncEngine, sql: str) -> list[tuple[Any, ...]]:
    """Run the static query ``sql`` on a connection of its own and return every row."""
    async with engine.connect() as connection:
        result = await connection.execute(text(sql))
        return [tuple(row) for row in result]


async def _tables(engine: AsyncEngine) -> set[str]:
    """Names of the tables in the ``public`` schema."""
    rows = await _fetch(
        engine,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    return {name for (name,) in rows}


async def _indexes(engine: AsyncEngine) -> set[tuple[str, str]]:
    """``(index name, table name)`` of every index in the ``public`` schema."""
    rows = await _fetch(
        engine, "SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public'"
    )
    return {(name, table) for name, table in rows}


async def _constraints(engine: AsyncEngine) -> set[tuple[str, str]]:
    """``(constraint name, table name)`` of every table constraint in the ``public`` schema."""
    rows = await _fetch(
        engine,
        "SELECT c.conname, t.relname FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = 'public'",
    )
    return {(name, table) for name, table in rows}


async def _host_state(engine: AsyncEngine) -> dict[str, list[tuple[Any, ...]]]:
    """The simulated host's history, its ``users`` rows and the ``users`` column names."""
    return {
        "alembic_version": await _fetch(engine, "SELECT version_num FROM alembic_version"),
        "users": await _fetch(engine, "SELECT count(*), min(email) FROM users"),
        "users_columns": await _fetch(
            engine,
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'users' "
            "ORDER BY ordinal_position",
        ),
    }


async def _terminal_versions(engine: AsyncEngine) -> list[tuple[Any, ...]]:
    """Rows of the terminal's own version table."""
    return await _fetch(engine, "SELECT version_num FROM terminal_alembic_version")


async def _run_cli(argv: Sequence[str]) -> int:
    """Run ``opennse-terminal <argv>`` in-process and return its exit code.

    The migrate handler drives its engine with ``asyncio.run``, so it runs in a worker thread
    that has no event loop of its own.
    """
    return await asyncio.to_thread(cli.main, list(argv))


def _compare(sync_connection: Connection) -> list[Any]:
    """Autogenerate's diff between the live ``terminal_`` schema and ``TerminalBase.metadata``.

    The options mirror ``env.py``: ``include_name`` hides every host table, and
    ``version_table`` keeps the terminal's own version table out of the comparison.
    """
    context = MigrationContext.configure(
        sync_connection,
        opts={
            "include_name": include_terminal_only,
            "compare_type": True,
            "version_table": VERSION_TABLE,
        },
    )
    diff: list[Any] = compare_metadata(context, TerminalBase.metadata)
    return diff


def _describe_tables(found: set[str], expected: frozenset[str]) -> str:
    """Failure text naming the unexpected and the missing tables."""
    return (
        f"unexpected tables: {sorted(found - expected)}; missing tables: {sorted(expected - found)}"
    )


async def _create_host_objects(engine: AsyncEngine) -> None:
    """Create the simulated host schema: its own ``users`` table and its own Alembic history.

    The Database Schema Isolation rule keeps ``users`` and every other non-``terminal_`` table
    out of the terminal's schema; these two stand in for a host's and exist only in the
    throwaway ``empty_database``.
    """
    async with engine.begin() as connection:
        await connection.execute(
            text("CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT NOT NULL)")
        )
        await connection.execute(
            text("INSERT INTO users (email) VALUES (:email)"), {"email": HOST_EMAIL}
        )
        await connection.execute(
            text(
                "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL, "
                "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
            )
        )
        await connection.execute(
            text("INSERT INTO alembic_version (version_num) VALUES (:revision)"),
            {"revision": HOST_REVISION},
        )


async def _ticker_fk_actions(engine: AsyncEngine) -> dict[str, tuple[str, str]]:
    """``name -> (on update, on delete)`` codes of every foreign key to ``terminal_equities``."""
    rows = await _fetch(
        engine,
        "SELECT conname, confupdtype::text, confdeltype::text FROM pg_constraint "
        "WHERE contype = 'f' AND confrelid = 'terminal_equities'::regclass",
    )
    return {name: (on_update, on_delete) for name, on_update, on_delete in rows}


async def _insert_ticker_references(engine: AsyncEngine) -> None:
    """Commit one equity at :data:`OLD_TICKER`, a watchlist item and a filing that reference it."""
    watchlist_id = uuid.uuid4()
    async with engine.begin() as connection:
        await connection.execute(
            insert(TerminalEquity).values(
                ticker=OLD_TICKER,
                company_name="Rename Check PLC",
                sector="Banking",
                shares_outstanding=1_000_000,
                source="seed",
            )
        )
        await connection.execute(
            insert(TerminalWatchlist).values(id=watchlist_id, owner_key="local", name="Renames")
        )
        await connection.execute(
            insert(TerminalWatchlistItem).values(watchlist_id=watchlist_id, ticker=OLD_TICKER)
        )
        await connection.execute(
            insert(TerminalCorporateFiling).values(
                ticker=OLD_TICKER,
                document_type="annual_report",
                file_name="rename-check.pdf",
                file_sha256="0" * 64,
                file_size_bytes=1024,
                page_count=1,
                extraction=[],
                status="no_items",
            )
        )


async def _rename_ticker(engine: AsyncEngine, old: str, new: str) -> None:
    """Rename the equity ``old`` to ``new`` in a committed transaction of its own."""
    async with engine.begin() as connection:
        await connection.execute(
            update(TerminalEquity).where(TerminalEquity.ticker == old).values(ticker=new)
        )


async def _stored_tickers(engine: AsyncEngine) -> dict[str, list[str | None]]:
    """The tickers stored on equities, watchlist items and filings."""
    async with engine.connect() as connection:
        return {
            "equities": list((await connection.scalars(select(TerminalEquity.ticker))).all()),
            "items": list((await connection.scalars(select(TerminalWatchlistItem.ticker))).all()),
            "filings": list(
                (await connection.scalars(select(TerminalCorporateFiling.ticker))).all()
            ),
        }


def test_metadata_contains_only_terminal_tables() -> None:
    """The ORM metadata holds exactly the 11 ``terminal_`` tables and nothing a host owns."""
    tables = set(TerminalBase.metadata.tables)

    assert tables == TERMINAL_TABLES, _describe_tables(tables, TERMINAL_TABLES)
    assert all(name.startswith(TABLE_PREFIX) for name in tables)
    assert TABLE_PREFIX == "terminal_"
    assert VERSION_TABLE == "terminal_alembic_version"


async def test_migration_history_is_isolated_from_host(
    empty_database: _DatabaseHandle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Upgrade, compare, downgrade and re-upgrade beside a host schema that stays untouched.

    1. Create the host's ``users`` table and ``alembic_version`` (at ``host_0001``).
    2. Run ``opennse-terminal migrate upgrade head``.
    3. The new tables are exactly the 11 terminal tables plus ``terminal_alembic_version``,
       which records the packaged head.
    4. No new table, prefix removed, is named like a user, auth, account, session or role table.
    5. Every new index and constraint name contains ``terminal_``, fits PostgreSQL's identifier
       limit and belongs to a ``terminal_`` table.
    6. The host's version row, ``users`` row and ``users`` columns are unchanged.
    7. Autogenerate finds no difference between the migrated schema and the models, and the
       host's tables are invisible to it.
    8. ``migrate downgrade base`` drops every terminal table and leaves the host's intact.

    A final ``upgrade head`` proves the history is reversible: the 11 tables come back.
    """
    engine = empty_database.engine

    # Step 1: host objects.
    await _create_host_objects(engine)
    before_tables = await _tables(engine)
    before_indexes = await _indexes(engine)
    before_constraints = await _constraints(engine)
    assert await _host_state(engine) == HOST_STATE

    # Step 2: migrate as a host operator would. The environment outranks any .env file.
    monkeypatch.setenv("TERMINAL_DATABASE_URL", empty_database.url)
    assert await _run_cli(["migrate", "upgrade", "head"]) == cli.EXIT_OK

    # Step 3: exactly the terminal tables, at the packaged head.
    expected_tables = TERMINAL_TABLES | {VERSION_TABLE}
    new_tables = await _tables(engine) - before_tables
    assert new_tables == expected_tables, _describe_tables(new_tables, expected_tables)
    assert head_revision() == "terminal_0002"
    assert await _terminal_versions(engine) == [(head_revision(),)]

    # Step 4: no users/auth lookalike under the prefix.
    lookalikes = sorted(
        name for name in new_tables if FORBIDDEN_NAME.search(name.removeprefix(TABLE_PREFIX))
    )
    assert lookalikes == [], f"tables named like user or auth tables: {lookalikes}"

    # Step 5: every new index and constraint is recognisably the terminal's.
    new_indexes = await _indexes(engine) - before_indexes
    new_constraints = await _constraints(engine) - before_constraints
    assert new_indexes, "the migration created no index"
    assert new_constraints, "the migration created no constraint"
    misnamed = sorted(
        f"{kind} {name!r} on {table!r}"
        for kind, objects in (("index", new_indexes), ("constraint", new_constraints))
        for name, table in objects
        if TABLE_PREFIX not in name
        or len(name) > PG_IDENTIFIER_MAX
        or not table.startswith(TABLE_PREFIX)
    )
    assert misnamed == [], "names outside the terminal_ namespace:\n" + "\n".join(misnamed)

    # Step 6: the host's history, rows and columns are untouched.
    assert await _host_state(engine) == HOST_STATE

    # Step 7: the models equal the migration; include_name keeps the host tables out.
    async with engine.connect() as connection:
        diff = await connection.run_sync(_compare)
    assert diff == [], "models and migration differ:\n" + pprint.pformat(diff)

    # Step 8: downgrade drops only what the history created. Alembic keeps its version table,
    # emptied, because no revision created it.
    assert await _run_cli(["migrate", "downgrade", "base"]) == cli.EXIT_OK
    after_downgrade = await _tables(engine)
    leftover = {name for name in after_downgrade if name.startswith(TABLE_PREFIX)}
    assert leftover <= {VERSION_TABLE}, f"terminal tables left after downgrade: {sorted(leftover)}"
    if VERSION_TABLE in leftover:
        assert await _terminal_versions(engine) == []
    assert after_downgrade - leftover == before_tables
    assert await _host_state(engine) == HOST_STATE

    # The history is reversible: upgrading again restores every table at the head.
    assert await _run_cli(["migrate", "upgrade", "head"]) == cli.EXIT_OK
    restored = await _tables(engine) - before_tables
    assert restored == expected_tables, _describe_tables(restored, expected_tables)
    assert await _terminal_versions(engine) == [(head_revision(),)]
    assert await _host_state(engine) == HOST_STATE


async def test_upgrade_from_terminal_0001_cascades_ticker_renames(
    empty_database: _DatabaseHandle, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A database stamped ``terminal_0001`` upgrades to the head schema, rows included.

    1. ``migrate upgrade terminal_0001`` gives the first shipped schema: the watchlist-item and
       filing ticker foreign keys take no action on update, so renaming a referenced ticker is
       rejected.
    2. ``migrate upgrade head`` makes all four ticker foreign keys cascade on update, keeps their
       delete actions, leaves nothing for autogenerate to propose, and the rename then carries
       over to the watchlist item and the filing.
    3. ``migrate downgrade terminal_0001`` restores the first schema's actions, with the rows in
       place, and a further ``upgrade head`` cascades again.
    """
    engine = empty_database.engine
    monkeypatch.setenv("TERMINAL_DATABASE_URL", empty_database.url)

    # Step 1: the first shipped revision, with rows that reference a ticker.
    assert await _run_cli(["migrate", "upgrade", BOUNDARY_REVISION]) == cli.EXIT_OK
    assert await _terminal_versions(engine) == [(BOUNDARY_REVISION,)]
    assert await _ticker_fk_actions(engine) == TICKER_FK_ACTIONS_AT_BOUNDARY
    await _insert_ticker_references(engine)
    stored_before = {"equities": [OLD_TICKER], "items": [OLD_TICKER], "filings": [OLD_TICKER]}
    assert await _stored_tickers(engine) == stored_before
    with pytest.raises(IntegrityError):
        await _rename_ticker(engine, OLD_TICKER, NEW_TICKER)
    assert await _stored_tickers(engine) == stored_before

    # Step 2: the upgrade path reaches the same schema as a fresh head, and renames cascade.
    assert await _run_cli(["migrate", "upgrade", "head"]) == cli.EXIT_OK
    assert await _terminal_versions(engine) == [(head_revision(),)]
    assert await _ticker_fk_actions(engine) == TICKER_FK_ACTIONS_AT_HEAD
    async with engine.connect() as connection:
        diff = await connection.run_sync(_compare)
    assert diff == [], "models and upgraded schema differ:\n" + pprint.pformat(diff)
    await _rename_ticker(engine, OLD_TICKER, NEW_TICKER)
    assert await _stored_tickers(engine) == {
        "equities": [NEW_TICKER],
        "items": [NEW_TICKER],
        "filings": [NEW_TICKER],
    }

    # Step 3: the successor revision is reversible on a database holding rows.
    assert await _run_cli(["migrate", "downgrade", BOUNDARY_REVISION]) == cli.EXIT_OK
    assert await _terminal_versions(engine) == [(BOUNDARY_REVISION,)]
    assert await _ticker_fk_actions(engine) == TICKER_FK_ACTIONS_AT_BOUNDARY
    assert await _run_cli(["migrate", "upgrade", "head"]) == cli.EXIT_OK
    assert await _terminal_versions(engine) == [(head_revision(),)]
    assert await _ticker_fk_actions(engine) == TICKER_FK_ACTIONS_AT_HEAD


async def test_migrate_names_a_revision_alembic_cannot_resolve_as_invalid(
    empty_database: _DatabaseHandle,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A revision Alembic cannot resolve from the database's current revision exits 1 with one
    ERROR line naming it invalid, and the recorded revision stays where it was.

    After connecting, Alembic's revision map raises an ``AssertionError`` without a message for
    ``upgrade head+1`` at the head, a step past it, and for ``downgrade head@-1`` at base, where
    there is nothing to step down from.

    1. ``migrate upgrade head``, then ``migrate upgrade head+1``.
    2. ``migrate downgrade base``, then ``migrate downgrade head@-1``.
    """
    engine = empty_database.engine
    monkeypatch.setenv("TERMINAL_DATABASE_URL", empty_database.url)
    caplog.set_level(logging.INFO, logger=cli.logger.name)
    unresolved = "Alembic could not resolve it from the database's current revision"

    for setup, direction, revision, versions in (
        ("upgrade head", "upgrade", "head+1", [(head_revision(),)]),
        ("downgrade base", "downgrade", "head@-1", []),
    ):
        assert await _run_cli(["migrate", *setup.split()]) == cli.EXIT_OK
        caplog.clear()

        assert await _run_cli(["migrate", direction, revision]) == cli.EXIT_FAILED

        assert await _terminal_versions(engine) == versions
        lines = [
            (record.levelno, record.getMessage())
            for record in caplog.records
            if record.name == cli.logger.name
        ]
        assert len(lines) == 1, lines
        level, message = lines[0]
        assert level == logging.ERROR
        assert message.startswith(f"migrate {direction} to {revision} on database ")
        assert message.endswith(f" failed: invalid revision '{revision}': {unresolved}")


# ---------------------------------------------------------------------------------------------
# Connect failures of the terminal's migration environment (``migrations/env.py``)
#
# ``run_upgrade`` and ``run_downgrade`` must turn an unreachable database into a
# ``ConnectionError`` that names the migration, its target revision and the database (name, host
# and port, in the prose form of ``database_log_label``), within ``CONNECT_TIMEOUT_SECONDS``
# rather than asyncpg's 60 s default. The message is never a connection string: it quotes no
# driver URL and no credential, neither the username, the password before ``@`` nor the
# ``?password=`` query value.
#
# No PostgreSQL is needed. The socket cases bind their own loopback socket on port 0: one that is
# closed again before connecting, so the connect is refused, and one that listens but never
# accepts or replies, so asyncpg's deadline expires. The remaining cases replace
# ``asyncpg.connect`` with one that raises a given error, for causes a loopback socket cannot
# produce and for a server-reported error, which must propagate unwrapped. The credentials are
# synthetic markers, so a leak shows as the marker in a message or traceback. The tests are
# synchronous because ``run_upgrade`` refuses to run inside an event loop.
# ---------------------------------------------------------------------------------------------

USER_MARKER = "migrateuserMARK"
USERINFO_PW_MARKER = "migratepwMARK"
QUERY_PW_MARKER = "migrateqpwMARK"
CREDENTIAL_MARKERS = (USER_MARKER, USERINFO_PW_MARKER, QUERY_PW_MARKER)
DATABASE = "terminal_connect_check"
LOOPBACK = "127.0.0.1"

MigrateCommand = Callable[[str, str], None]

COMMANDS = [
    pytest.param(run_upgrade, "head", head_revision(), id="upgrade-head"),
    pytest.param(run_downgrade, "base", "base", id="downgrade-base"),
]


def _marked_url(port: int) -> str:
    return (
        f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@{LOOPBACK}:{port}/{DATABASE}"
        f"?password={QUERY_PW_MARKER}"
    )


def _expected_prefix(target_revision: str, port: int) -> str:
    return (
        f"terminal migration to revision {target_revision!r} could not connect to "
        f'database "{DATABASE}" on {LOOPBACK}:{port}: '
    )


def _assert_no_credential(error: BaseException) -> None:
    """Check the message and the whole chained traceback, as a log handler would print it."""
    rendered = "".join(traceback.format_exception(error))
    for marker in CREDENTIAL_MARKERS:
        assert marker not in rendered


def _assert_no_connection_string(error: BaseException) -> None:
    """Check that the message names the database in prose, never as a ``driver://`` URL."""
    assert "://" not in str(error)


@pytest.fixture
def closed_port() -> int:
    """A loopback port that nothing listens on: bound once by the OS, then released."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((LOOPBACK, 0))
        port: int = probe.getsockname()[1]
    return port


@pytest.fixture
def silent_server_port() -> Iterator[int]:
    """A loopback port whose listener completes TCP handshakes but never accepts or replies."""
    with socket.create_server((LOOPBACK, 0)) as listener:
        port: int = listener.getsockname()[1]
        yield port


@pytest.mark.parametrize(("command", "revision", "target_revision"), COMMANDS)
def test_refused_connect_raises_connection_error_naming_the_target(
    command: MigrateCommand, revision: str, target_revision: str, closed_port: int
) -> None:
    with pytest.raises(ConnectionError) as raised:
        command(_marked_url(closed_port), revision)

    message = str(raised.value)
    assert message.startswith(_expected_prefix(target_revision, closed_port))
    assert "ConnectionRefusedError" in message
    assert isinstance(raised.value.__cause__, OSError)
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


@pytest.mark.parametrize(("command", "revision", "target_revision"), COMMANDS)
def test_unanswered_connect_times_out_within_the_connect_timeout(
    command: MigrateCommand,
    revision: str,
    target_revision: str,
    silent_server_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # env.py is executed afresh by every Alembic command, so it reads the patched value.
    monkeypatch.setattr(migrations, "CONNECT_TIMEOUT_SECONDS", 0.5)
    started = time.monotonic()

    with pytest.raises(ConnectionError) as raised:
        command(_marked_url(silent_server_port), revision)

    elapsed = time.monotonic() - started
    assert elapsed < 10, f"connect took {elapsed:.1f} s; asyncpg's 60 s default applied"
    expected = f"{_expected_prefix(target_revision, silent_server_port)}timed out after 0.5 s"
    assert str(raised.value) == expected
    assert isinstance(raised.value.__cause__, TimeoutError)
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


def _patch_asyncpg_connect(monkeypatch: pytest.MonkeyPatch, error: Exception) -> dict[str, Any]:
    """Make every asyncpg connect raise ``error``; return the keyword arguments it received."""
    received: dict[str, Any] = {}

    async def failing_connect(*args: Any, **kwargs: Any) -> None:
        received.update(kwargs)
        raise error

    # SQLAlchemy's asyncpg adapter looks asyncpg.connect up on the module at connect time.
    monkeypatch.setattr(asyncpg, "connect", failing_connect)
    return received


@pytest.mark.parametrize(
    ("error", "expected_cause"),
    [
        pytest.param(
            TimeoutError(errno.ETIMEDOUT, f"Connect call failed ('{LOOPBACK}', 5432)"),
            f"TimeoutError: Connect call failed ('{LOOPBACK}', 5432)",
            id="os-etimedout-is-not-the-deadline",
        ),
        pytest.param(
            socket.gaierror(socket.EAI_NONAME, "Name or service not known"),
            "gaierror: Name or service not known",
            id="unresolvable-host",
        ),
        pytest.param(OSError("Multiple exceptions: ..."), "OSError", id="no-strerror"),
    ],
)
def test_connect_oserror_cause_names_its_type_and_strerror(
    error: OSError, expected_cause: str, closed_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A closed port, so a patch that failed to apply could never reach a real server.
    received = _patch_asyncpg_connect(monkeypatch, error)

    with pytest.raises(ConnectionError) as raised:
        run_upgrade(_marked_url(closed_port))

    expected = f"{_expected_prefix(head_revision(), closed_port)}{expected_cause}"
    assert str(raised.value) == expected
    assert raised.value.__cause__ is error
    assert received["timeout"] == migrations.CONNECT_TIMEOUT_SECONDS == 10.0
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


def test_server_reported_connect_error_propagates_unwrapped(
    closed_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The server's own refusal already names the user and never the password.
    refusal = asyncpg.exceptions.InvalidPasswordError(
        f'password authentication failed for user "{USER_MARKER}"'
    )
    _patch_asyncpg_connect(monkeypatch, refusal)

    with pytest.raises(asyncpg.exceptions.InvalidPasswordError) as raised:
        run_upgrade(_marked_url(closed_port))

    assert raised.value is refusal


# ---------------------------------------------------------------------------------------------
# Referential actions of every foreign key to ``terminal_equities.ticker``
#
# Four tables reference a counter by ticker. Each foreign key cascades on update, so renaming an
# equity's ticker carries over to its prices, fundamentals, watchlist items and filings instead
# of being rejected while any of them still references the old ticker. On delete, prices,
# fundamentals and watchlist items cascade, while a filing keeps its extraction and has its
# ticker set to null.
#
# Both schema sources are checked: the ORM models on ``TerminalBase.metadata`` and the DDL that
# the migration history emits in Alembic's offline mode, which renders SQL without connecting to a
# database. ``terminal_0001`` defines all four keys inline in ``CREATE TABLE``, the watchlist-item
# and filing keys without an ON UPDATE action; ``terminal_0002`` drops those two and adds them
# back with ON UPDATE CASCADE. The head DDL's last statement that defines or drops each key must
# therefore define it with the models' actions, and the DDL up to ``terminal_0001`` must define
# each key once with the actions that revision shipped with, so the shipped revision is never
# edited in place.
#
# Expected names and actions are literals, never derived from the code under test. The tests are
# pure: no database or network.
# ---------------------------------------------------------------------------------------------

# (table, foreign key name, ON UPDATE action, ON DELETE action)
EXPECTED_TICKER_FKS: list[tuple[str, str, str, str]] = [
    (
        "terminal_daily_prices",
        "fk_terminal_daily_prices_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_fundamentals",
        "fk_terminal_fundamentals_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_watchlist_items",
        "fk_terminal_watchlist_items_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_corporate_filings",
        "fk_terminal_corporate_filings_ticker_terminal_equities",
        "CASCADE",
        "SET NULL",
    ),
]
# (table, foreign key name, referential actions the DDL of terminal_0001 renders)
BOUNDARY_TICKER_FKS: list[tuple[str, str, str]] = [
    (
        "terminal_daily_prices",
        "fk_terminal_daily_prices_ticker_terminal_equities",
        "ON DELETE CASCADE ON UPDATE CASCADE",
    ),
    (
        "terminal_fundamentals",
        "fk_terminal_fundamentals_ticker_terminal_equities",
        "ON DELETE CASCADE ON UPDATE CASCADE",
    ),
    (
        "terminal_watchlist_items",
        "fk_terminal_watchlist_items_ticker_terminal_equities",
        "ON DELETE CASCADE",
    ),
    (
        "terminal_corporate_filings",
        "fk_terminal_corporate_filings_ticker_terminal_equities",
        "ON DELETE SET NULL",
    ),
]
EQUITIES_TABLE = "terminal_equities"
# Offline mode needs only the dialect of the URL; nothing connects to this address.
OFFLINE_URL = "postgresql+asyncpg://localhost/x"
DROPPED = "DROP"


def _offline_upgrade_ddl(capsys: pytest.CaptureFixture[str], revision: str) -> str:
    """The SQL ``upgrade`` to ``revision`` emits from an empty database, without connecting."""
    # In offline mode Alembic writes the migration SQL to stdout instead of executing it.
    command.upgrade(build_alembic_config(OFFLINE_URL), revision, sql=True)
    return capsys.readouterr().out


def _fk_statements(ddl: str, table_name: str, fk_name: str) -> list[str]:
    """Each statement of ``ddl`` that defines or drops ``fk_name``, in order.

    A definition, inline in its table's ``CREATE TABLE`` or through ``ALTER TABLE ... ADD
    CONSTRAINT``, is given as everything after its ``REFERENCES`` target, for example
    ``"ON DELETE CASCADE ON UPDATE CASCADE"``; a drop is given as :data:`DROPPED`.
    """
    clause = f"CONSTRAINT {fk_name} FOREIGN KEY(ticker) REFERENCES {EQUITIES_TABLE} (ticker)"
    found: list[str] = []
    for raw_statement in ddl.split(";\n"):
        # Alembic precedes each revision's first statement with a "-- Running upgrade" comment.
        sql_lines = [line for line in raw_statement.splitlines() if not line.startswith("--")]
        statement = "\n".join(sql_lines).strip()
        if fk_name not in statement:
            continue
        if statement == f"ALTER TABLE {table_name} DROP CONSTRAINT {fk_name}":
            found.append(DROPPED)
            continue
        assert statement.startswith(
            (f"CREATE TABLE {table_name} (", f"ALTER TABLE {table_name} ADD {clause}")
        ), statement
        lines = [line for line in statement.splitlines() if clause in line]
        assert len(lines) == 1, statement
        # An inline constraint ends with ", " unless it is the table's last element.
        found.append(lines[0].split(clause, 1)[1].strip().removesuffix(",").rstrip())
    return found


def _ticker_fk(table_name: str) -> ForeignKeyConstraint:
    """Return the single foreign key of ``table_name`` that references ``terminal_equities``."""
    table = TerminalBase.metadata.tables[table_name]
    matches = [
        constraint
        for constraint in table.foreign_key_constraints
        if constraint.referred_table.name == EQUITIES_TABLE
    ]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.parametrize(("table_name", "fk_name", "onupdate", "ondelete"), EXPECTED_TICKER_FKS)
def test_model_ticker_fk_has_its_name_and_actions(
    table_name: str, fk_name: str, onupdate: str, ondelete: str
) -> None:
    constraint = _ticker_fk(table_name)
    assert constraint.name == fk_name
    assert constraint.column_keys == ["ticker"]
    assert [element.column.name for element in constraint.elements] == ["ticker"]
    assert constraint.onupdate == onupdate
    assert constraint.ondelete == ondelete


def test_model_fks_to_equities_are_exactly_the_four_ticker_fks() -> None:
    referencing = {
        (table.name, constraint.name)
        for table in TerminalBase.metadata.tables.values()
        for constraint in table.foreign_key_constraints
        if constraint.referred_table.name == EQUITIES_TABLE
    }
    assert referencing == {
        (table_name, fk_name) for table_name, fk_name, _, _ in EXPECTED_TICKER_FKS
    }


@pytest.mark.parametrize(("table_name", "fk_name", "onupdate", "ondelete"), EXPECTED_TICKER_FKS)
def test_migration_ddl_renders_the_ticker_fk_actions(
    capsys: pytest.CaptureFixture[str],
    table_name: str,
    fk_name: str,
    onupdate: str,
    ondelete: str,
) -> None:
    statements = _fk_statements(_offline_upgrade_ddl(capsys, "head"), table_name, fk_name)
    # The last statement decides the head schema; a drop there would leave no key at all.
    assert statements, f"the head DDL never defines {fk_name}"
    assert statements[-1] == f"ON DELETE {ondelete} ON UPDATE {onupdate}"


@pytest.mark.parametrize(("table_name", "fk_name", "actions"), BOUNDARY_TICKER_FKS)
def test_first_revision_ddl_keeps_its_shipped_ticker_fk_actions(
    capsys: pytest.CaptureFixture[str], table_name: str, fk_name: str, actions: str
) -> None:
    ddl = _offline_upgrade_ddl(capsys, BOUNDARY_REVISION)
    assert _fk_statements(ddl, table_name, fk_name) == [actions]


# ---------------------------------------------------------------------------------------------
# Lifecycle of the suite's own databases: where generated URLs connect, and what gets dropped
#
# The root conftest's ``DatabaseFactory`` creates every database the suite uses on the server
# named by ``TERMINAL_TEST_DATABASE_URL``. That URL's database is the administrative connection
# only and never holds test data, and every database the factory creates is dropped again. These
# tests prove the guarantees that depend on the URL and on failure timing:
#
# - every URL the factory generates connects to the database it names, even when the
#   administrative URL carries a ``database`` query option, which asyncpg applies after the path;
# - a database whose ``CREATE DATABASE`` committed is dropped by ``close()`` even when the
#   creating call then fails or is cancelled;
# - a caller cancelled while its ``CREATE DATABASE`` is in flight raises ``CancelledError`` only
#   once that statement has settled, so the database it leaves behind is tracked and dropped.
#
# The factory class is reached as ``type(database_factory)``, because test modules never import
# a conftest. Each test builds private factories over AUTOCOMMIT engines of its own and closes
# them in ``finally``. A private factory only ever creates empty databases: the seeded
# template's name is fixed, so a private factory that built it would destroy the session's
# template.
#
# Only catalogue and server-function queries are written as SQL in this section; no table is
# read or written.
# ---------------------------------------------------------------------------------------------

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
