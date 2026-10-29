"""Schema isolation between the terminal's migration history and a host database.

The terminal must be safe to migrate into a host's PostgreSQL database: every table, index and
constraint it creates is ``terminal_``-prefixed, it creates no ``users``/``auth`` lookalike, its
history lives in ``terminal_alembic_version``, and neither upgrading nor downgrading reads or
changes the host's own tables or the host's ``alembic_version``. The models must also equal the
migration, so autogenerate has nothing to propose.

Migrations run through the product's own ``opennse-terminal migrate`` command, in-process, on its
async (asyncpg) engine, exactly as a host operator runs it.

This is the only test module that creates objects without the ``terminal_`` prefix: the simulated
host tables ``users`` and ``alembic_version``. They stand in for a host's schema, so the "no
generic users or auth tables" requirement of the Database Schema Isolation rule is about the
terminal's schema, which these tests prove creates none. They are created only inside the
``empty_database`` fixture, a fresh database copied from ``template0`` for each test and dropped
afterwards, never the seeded template or a clone of it.

Every query helper opens and closes its own connection and leaves no transaction open, and no
connection is held across a CLI call: a lingering transaction that touched a ``terminal_`` table
would block the ``DROP TABLE`` statements of ``migrate downgrade base``.
"""

import asyncio
import pprint
import re
from collections.abc import Sequence
from typing import Any, Final, Protocol

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

# Imported for its side effect: it registers all 11 terminal_ tables on TerminalBase.metadata.
import opennse_terminal.models  # noqa: F401
from opennse_terminal import cli
from opennse_terminal.db.base import TABLE_PREFIX, TerminalBase
from opennse_terminal.migrations import VERSION_TABLE, head_revision, include_terminal_only

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
    assert head_revision() == "terminal_0001"
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
