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
"""

import asyncio
import pprint
import re
import uuid
from collections.abc import Sequence
from typing import Any, Final, Protocol

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import insert, select, text, update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

# Imported for its side effect: it registers all 11 terminal_ tables on TerminalBase.metadata.
import opennse_terminal.models  # noqa: F401
from opennse_terminal import cli
from opennse_terminal.db.base import TABLE_PREFIX, TerminalBase
from opennse_terminal.migrations import VERSION_TABLE, head_revision, include_terminal_only
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
