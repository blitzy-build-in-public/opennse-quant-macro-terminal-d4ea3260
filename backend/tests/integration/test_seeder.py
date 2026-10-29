"""Integration tests of the fallback seeder (``opennse_terminal.seed.seed_terminal_db``).

What they prove, against PostgreSQL:

* idempotency: re-seeding through :func:`seed_terminal_db.seed`, ``python -m
  opennse_terminal.seed.seed_terminal_db`` (its ``main``) or ``opennse-terminal seed`` inserts no
  data row and adds only its own ``terminal_ingestion_runs`` record;
* the migrate-first precondition: on an unmigrated database, or one whose
  ``terminal_alembic_version`` is not at the packaged head, both entry points exit 2 with the
  "run `opennse-terminal migrate upgrade head` first" instruction and write nothing; probed
  directly on an ``AsyncConnection``, the schema check finds no revision on an empty database
  without aborting the transaction, and the head on a migrated one;
* content and provenance: the seeded template holds every attachment value and the stated row
  counts, and every seeded row or column is marked ``seed`` so real data can supersede it;
* precedence: rows written by the CSV/Excel importer survive a re-seed untouched.

Seeded tests request the root conftest's ``seeded_database``, a fresh function-scoped clone of the
migrated and seeded template: the seeder and the importer open their own connections, which an
outer rollback transaction cannot wrap. The precondition tests use ``empty_database``, an empty
copy of ``template0`` in which only ``terminal_alembic_version`` is ever created. Every assertion
reads committed state on a separate connection, through Core ``select()`` statements over the
``opennse_terminal.models`` columns with every value bound, and every stored amount is compared as
``Decimal`` (``Decimal("6.8000") == Decimal("6.80")``), never as a float. Only the catalogue query
and the Alembic-owned ``terminal_alembic_version`` table, which have no model, use ``text()``.

Expected values are the seeder's reference constants (``seed/reference_data.py``), which hold the
attachment values verbatim, plus the row counts AAP 0.8.3 states.

The last two sections need no database and request no database fixture. One covers the seeder's
failure paths: engine creation, connection, usage and interrupt errors, credential-safe output,
single reporting of each failure, the recording of a failed run and the shared session
configuration. The other covers the natural-key write order of the seeder's table writer, with
``upsert_rows`` replaced by a recorder. Each section's contract opens it.
"""

import asyncio
import csv
import logging
import socket
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, date, datetime
from decimal import Decimal
from math import ceil
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Protocol

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import Executable, Select, func, or_, select, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal import cli, migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db import session as db_session
from opennse_terminal.db.session import (
    build_engine,
    build_session_factory,
    dispose_terminal_engine,
    session_factory,
)
from opennse_terminal.ingestion.files.tabular_import import import_file
from opennse_terminal.models import (
    TerminalBondYield,
    TerminalCycle,
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
    TerminalIpoScenario,
    TerminalMacroIndicator,
    TerminalWatchlist,
    TerminalWatchlistItem,
)
from opennse_terminal.seed import seed_terminal_db, synthetic
from opennse_terminal.seed.reference_data import (
    ANCHOR_DATE,
    BANKING_SECTOR,
    BOND_CODE_PREFIX,
    BOND_TENOR_SPREADS,
    CYCLES,
    EQUITIES,
    FUNDAMENTAL_TARGETS,
    MACRO_ANCHOR,
    MACRO_COLUMNS,
    MACRO_FIXED_POINTS,
    MACRO_START,
    PRICE_ANCHORS,
    QUICKMART_PRESET,
    QUICKMART_RISK_SCENARIOS,
    SEED_SOURCE,
    SEED_WATCHLIST_NAME,
    SERIES_START,
    TICKERS,
)
from opennse_terminal.seed.seed_terminal_db import (
    SCHEMA_HINT,
    SEED_BATCH_SIZE,
    SchemaNotAtHeadError,
    run_seed,
)


class _Database(Protocol):
    """The part of a conftest database handle these tests use."""

    @property
    def url(self) -> str:
        """Connection URL of the database, password included."""

    @property
    def engine(self) -> AsyncEngine:
        """Engine on the database, owned and disposed by the conftest factory."""


class _SeededDatabase(_Database, Protocol):
    """The part of the root conftest's ``seeded_database`` handle these tests use."""

    @property
    def settings(self) -> TerminalSettings:
        """``TerminalSettings`` addressing the clone (owner ``local``)."""


# ---------------------------------------------------------------------------------------------
# Expectations
# ---------------------------------------------------------------------------------------------

DATA_TABLES: Final[tuple[str, ...]] = (
    "terminal_equities",
    "terminal_daily_prices",
    "terminal_fundamentals",
    "terminal_macro_indicators",
    "terminal_bond_yields",
    "terminal_cycles",
    "terminal_ipo_scenarios",
    "terminal_watchlists",
    "terminal_watchlist_items",
)
"""The nine tables the seeder writes, in its foreign-key write order."""

EXPECTED_COUNTS: Final[Mapping[str, int]] = MappingProxyType(
    {
        "terminal_equities": 8,
        "terminal_daily_prices": 30_096,
        "terminal_fundamentals": 131,
        "terminal_macro_indicators": 177,
        "terminal_bond_yields": 1_062,
        "terminal_cycles": 4,
        "terminal_ipo_scenarios": 1,
        "terminal_watchlists": 1,
        "terminal_watchlist_items": 8,
    }
)
"""Rows of each table after seeding an empty schema (AAP 0.8.2 and 0.8.3)."""

SEED_ROW_TOTAL: Final[int] = sum(EXPECTED_COUNTS.values())
"""31,488 candidate rows per seeder run."""

SEED_SESSIONS: Final[int] = 3_762
"""Sessions of the synthetic seed calendar, 2012-01-02 to 2026-09-30."""

SEED_OWNER: Final[str] = "local"
"""``TerminalSettings.default_owner_key`` of the template's seed run, the watchlist owner."""

MIGRATE_HINT: Final[str] = "opennse-terminal migrate upgrade head"
"""The operator instruction an exit code 2 must carry."""

STALE_REVISION: Final[str] = "terminal_0000"
"""A ``terminal_alembic_version`` value that is not the packaged head."""

CSV_IMPORT_SOURCE: Final[str] = "csv_import"
"""Provenance label of every value the CSV/Excel importer writes."""

NEW_SESSION: Final[date] = date(2026, 10, 27)
"""An imported session after the seeded history, which ends on ``ANCHOR_DATE``."""

FUNDAMENTAL_VALUE_COLUMNS: Final[tuple[str, ...]] = (
    "revenue_kes",
    "ebitda_kes",
    "net_income_kes",
    "total_assets_kes",
    "total_equity_kes",
    "total_debt_kes",
    "cash_and_equivalents_kes",
    "operating_cash_flow_kes",
    "dps_kes",
    "shares_outstanding",
)
"""The ``terminal_fundamentals`` value columns whose provenance ``source_by_field`` records."""

BANK_UNSEEDED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"ebitda_kes", "total_debt_kes", "cash_and_equivalents_kes", "operating_cash_flow_kes"}
)
"""Fundamentals columns left NULL for Banking-sector counters (AAP 0.8.3)."""

BANK_TICKERS: Final[frozenset[str]] = frozenset(
    equity.ticker for equity in EQUITIES if equity.sector == BANKING_SECTOR
)

# ---------------------------------------------------------------------------------------------
# Statements. Mapped tables are read with Core ``select()`` over the model columns, every value
# bound. The catalogue query and ``terminal_alembic_version``, which Alembic owns and no model
# maps, are fixed ``text()`` literals with bound values only.
# ---------------------------------------------------------------------------------------------

_COUNT_STATEMENTS: Final[Mapping[str, Select[tuple[int]]]] = MappingProxyType(
    {
        model.__tablename__: select(func.count()).select_from(model)
        for model in (
            TerminalEquity,
            TerminalDailyPrice,
            TerminalFundamental,
            TerminalMacroIndicator,
            TerminalBondYield,
            TerminalCycle,
            TerminalIpoScenario,
            TerminalWatchlist,
            TerminalWatchlistItem,
        )
    }
)
"""``SELECT count(*)`` of each :data:`DATA_TABLES` table, keyed by its table name."""

_SEED_RUNS: Final = (
    select(
        TerminalIngestionRun.id,
        TerminalIngestionRun.pipeline,
        TerminalIngestionRun.trigger,
        TerminalIngestionRun.status,
        TerminalIngestionRun.started_at,
        TerminalIngestionRun.finished_at,
        TerminalIngestionRun.rows_fetched,
        TerminalIngestionRun.rows_written,
        TerminalIngestionRun.rows_rejected,
        TerminalIngestionRun.error_message,
        TerminalIngestionRun.details,
    )
    .where(TerminalIngestionRun.pipeline == seed_terminal_db.SEED_PIPELINE)
    .order_by(TerminalIngestionRun.started_at, TerminalIngestionRun.id)
)

_TERMINAL_TABLES_SQL: Final = text(
    "SELECT table_name::text AS table_name FROM information_schema.tables "
    "WHERE starts_with(table_name::text, :prefix) ORDER BY table_name"
).bindparams(prefix="terminal_")

_VERSION_SQL: Final = text("SELECT version_num FROM terminal_alembic_version")


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


async def _counts(engine: AsyncEngine) -> dict[str, int]:
    """Row count of every table in :data:`DATA_TABLES`."""
    async with engine.connect() as connection:
        return {
            table: int((await connection.execute(_COUNT_STATEMENTS[table])).scalar_one())
            for table in DATA_TABLES
        }


async def _fetch(engine: AsyncEngine, statement: Executable) -> list[RowMapping]:
    """Every row of ``statement``, whose values are bound inside it, read on a fresh connection.

    Rows are keyed by the selected column names or labels.
    """
    async with engine.connect() as connection:
        return list((await connection.execute(statement)).mappings().all())


async def _seed_runs(engine: AsyncEngine) -> list[RowMapping]:
    """The seeder's ``terminal_ingestion_runs`` rows, oldest first."""
    return await _fetch(engine, _SEED_RUNS)


async def _terminal_tables(engine: AsyncEngine) -> list[str]:
    """Names of every table or view whose name starts with ``terminal_``, sorted."""
    rows = await _fetch(engine, _TERMINAL_TABLES_SQL)
    return [str(row["table_name"]) for row in rows]


def _write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence[str]]) -> Path:
    """Write ``header`` and ``rows`` to ``path`` as UTF-8 CSV and return ``path``."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


async def _reseed(database: _SeededDatabase) -> seed_terminal_db.SeedReport:
    """Run :func:`seed_terminal_db.seed` once more on its own session.

    No commit follows: ``seed()`` owns and commits its transaction, which the assertions confirm
    by reading on separate connections.
    """
    async with AsyncSession(database.engine, expire_on_commit=False) as session:
        return await seed_terminal_db.seed(session, settings=database.settings)


def _new_runs(before: Sequence[RowMapping], after: Sequence[RowMapping]) -> list[RowMapping]:
    """The rows of ``after`` whose id is not in ``before``, in ``after``'s order."""
    known = {str(row["id"]) for row in before}
    return [row for row in after if str(row["id"]) not in known]


def _assert_idle_seed_run(run: RowMapping) -> None:
    """``run`` is a succeeded re-seed record: every candidate offered, nothing inserted."""
    assert run["trigger"] == seed_terminal_db.SEED_TRIGGER
    assert run["status"] == "succeeded"
    assert run["finished_at"] is not None
    assert run["finished_at"] >= run["started_at"]
    assert run["error_message"] is None
    assert (run["rows_fetched"], run["rows_written"], run["rows_rejected"]) == (
        SEED_ROW_TOTAL,
        0,
        0,
    )
    details = run["details"]
    assert details["inserted"] == dict.fromkeys(DATA_TABLES, 0)
    assert details["candidates"] == dict(EXPECTED_COUNTS)
    assert details["data_through"] == ANCHOR_DATE.isoformat()


# ---------------------------------------------------------------------------------------------
# Idempotency (AAP 0.8.1, 0.16.1: a second run reports 0 rows inserted)
# ---------------------------------------------------------------------------------------------


async def test_reseed_inserts_no_data_rows(seeded_database: _SeededDatabase) -> None:
    engine = seeded_database.engine
    counts_before = await _counts(engine)
    runs_before = await _seed_runs(engine)

    report = await _reseed(seeded_database)

    assert tuple(report.inserted) == DATA_TABLES
    assert tuple(report.candidates) == DATA_TABLES
    assert report.total_inserted == 0
    assert dict(report.inserted) == dict.fromkeys(DATA_TABLES, 0)
    assert dict(report.candidates) == dict(EXPECTED_COUNTS)
    assert counts_before == dict(EXPECTED_COUNTS)
    assert await _counts(engine) == counts_before

    new_runs = _new_runs(runs_before, await _seed_runs(engine))
    assert len(new_runs) == 1
    assert str(new_runs[0]["id"]) == str(report.run_id)
    _assert_idle_seed_run(new_runs[0])


async def test_seed_entry_points_are_idempotent(
    seeded_database: _SeededDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Both entry points read every setting except the URL from the environment, so a developer's
    # TERMINAL_DEFAULT_OWNER_KEY would seed a second watchlist; the owner is pinned to the clone's.
    monkeypatch.setenv("TERMINAL_DEFAULT_OWNER_KEY", seeded_database.settings.default_owner_key)
    engine = seeded_database.engine
    counts_before = await _counts(engine)
    runs_before = await _seed_runs(engine)
    url_args = ["--database-url", seeded_database.url]

    module_exit = await asyncio.to_thread(seed_terminal_db.main, url_args)
    cli_exit = await asyncio.to_thread(cli.main, ["seed", *url_args])

    assert (module_exit, cli_exit) == (0, cli.EXIT_OK)
    assert await _counts(engine) == counts_before == dict(EXPECTED_COUNTS)
    new_runs = _new_runs(runs_before, await _seed_runs(engine))
    assert len(new_runs) == 2
    for run in new_runs:
        _assert_idle_seed_run(run)


# ---------------------------------------------------------------------------------------------
# Precondition (AAP 0.8.1: exit 2 before any write when the schema is not at head)
# ---------------------------------------------------------------------------------------------

_EntryPoint = Callable[[list[str]], int]

SEED_ENTRY_POINTS: Final[tuple[tuple[_EntryPoint, tuple[str, ...]], ...]] = (
    (seed_terminal_db.main, ()),
    (cli.main, ("seed",)),
)
"""``(main, leading argv)`` of ``python -m opennse_terminal.seed.seed_terminal_db`` and
``opennse-terminal seed``."""

_ENTRY_POINT_IDS: Final[tuple[str, ...]] = ("seed_terminal_db", "cli_seed")


async def _run_entry_point(
    entry_point: _EntryPoint, leading: Sequence[str], database: _Database
) -> int:
    """Run a synchronous seed entry point off the event loop (it calls ``asyncio.run``)."""
    return await asyncio.to_thread(entry_point, [*leading, "--database-url", database.url])


def _captured_output(capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> str:
    """Everything the run printed to stdout or stderr, or logged."""
    captured = capsys.readouterr()
    return "\n".join((captured.out, captured.err, caplog.text))


@pytest.mark.parametrize(("entry_point", "leading"), SEED_ENTRY_POINTS, ids=_ENTRY_POINT_IDS)
async def test_seed_refuses_unmigrated_database(
    empty_database: _Database,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    entry_point: _EntryPoint,
    leading: tuple[str, ...],
) -> None:
    exit_code = await _run_entry_point(entry_point, leading, empty_database)

    assert exit_code == cli.EXIT_DB_NOT_AT_HEAD == 2
    assert MIGRATE_HINT in _captured_output(capsys, caplog)
    assert await _terminal_tables(empty_database.engine) == []


@pytest.mark.parametrize(("entry_point", "leading"), SEED_ENTRY_POINTS, ids=_ENTRY_POINT_IDS)
async def test_seed_refuses_database_not_at_head(
    empty_database: _Database,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    entry_point: _EntryPoint,
    leading: tuple[str, ...],
) -> None:
    assert migrations.head_revision() != STALE_REVISION
    async with empty_database.engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE TABLE terminal_alembic_version "
                "(version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
            )
        )
        await connection.execute(
            text("INSERT INTO terminal_alembic_version (version_num) VALUES (:revision)"),
            {"revision": STALE_REVISION},
        )

    exit_code = await _run_entry_point(entry_point, leading, empty_database)

    assert exit_code == cli.EXIT_DB_NOT_AT_HEAD == 2
    output = _captured_output(capsys, caplog)
    assert MIGRATE_HINT in output
    assert STALE_REVISION in output
    assert await _terminal_tables(empty_database.engine) == [migrations.VERSION_TABLE]
    versions = await _fetch(empty_database.engine, _VERSION_SQL)
    assert [row["version_num"] for row in versions] == [STALE_REVISION]


async def test_schema_probe_reports_no_revision_on_unmigrated_database(
    empty_database: _Database,
) -> None:
    # Probed on an AsyncConnection; the seeder itself passes an AsyncSession. to_regclass yields
    # NULL for the missing version table instead of an error, so the transaction stays usable.
    async with empty_database.engine.connect() as connection:
        assert await seed_terminal_db.current_schema_revision(connection) is None
        with pytest.raises(seed_terminal_db.SchemaNotAtHeadError) as raised:
            await seed_terminal_db.ensure_schema_at_head(connection)
        assert connection.in_transaction()
        assert await connection.scalar(text("SELECT 1")) == 1

    assert raised.value.found is None
    assert raised.value.expected == migrations.head_revision()
    assert MIGRATE_HINT in str(raised.value)
    assert await _terminal_tables(empty_database.engine) == []


async def test_schema_probe_reads_head_on_migrated_database(
    seeded_database: _SeededDatabase,
) -> None:
    head = migrations.head_revision()
    async with seeded_database.engine.connect() as connection:
        assert await seed_terminal_db.current_schema_revision(connection) == head
        assert await seed_terminal_db.ensure_schema_at_head(connection) == head


# ---------------------------------------------------------------------------------------------
# Content and provenance (AAP 0.8.1-0.8.3)
# ---------------------------------------------------------------------------------------------


async def _assert_seed_run_history(engine: AsyncEngine) -> None:
    """The template's first seed run inserted every row; any later run inserted none."""
    runs = await _seed_runs(engine)
    assert runs, "the seeded template records its seed run"
    first, *later = runs
    assert first["status"] == "succeeded"
    assert first["trigger"] == seed_terminal_db.SEED_TRIGGER
    assert first["details"]["inserted"] == dict(EXPECTED_COUNTS)
    assert first["rows_written"] == SEED_ROW_TOTAL
    for run in later:
        _assert_idle_seed_run(run)


async def _assert_prices(engine: AsyncEngine) -> None:
    price = TerminalDailyPrice
    unmarked = await _fetch(
        engine,
        select(func.count().label("n"))
        .select_from(price)
        .where(
            or_(
                price.source != SEED_SOURCE,
                price.flow_source.is_distinct_from(SEED_SOURCE),
                price.foreign_net_flow_kes.is_(None),
            )
        ),
    )
    assert unmarked[0]["n"] == 0, "every seeded price row is marked seed, with a seeded flow"

    (span,) = await _fetch(
        engine,
        select(
            func.min(price.trade_date).label("first"),
            func.max(price.trade_date).label("last"),
            func.count(price.trade_date.distinct()).label("sessions"),
        ),
    )
    # Nothing is dated after the anchor: future rows would read as observed history.
    assert (span["first"], span["last"], span["sessions"]) == (
        SERIES_START,
        ANCHOR_DATE,
        SEED_SESSIONS,
    )
    per_ticker = await _fetch(
        engine, select(price.ticker, func.count().label("n")).group_by(price.ticker)
    )
    assert {row["ticker"]: row["n"] for row in per_ticker} == dict.fromkeys(TICKERS, SEED_SESSIONS)

    closes = await _fetch(
        engine,
        select(price.ticker, price.close_price).where(price.trade_date == ANCHOR_DATE),
    )
    anchor_closes = {row["ticker"]: row["close_price"] for row in closes}
    assert anchor_closes == dict(PRICE_ANCHORS)
    assert anchor_closes["SCOM"] == Decimal("33.25")


async def _assert_equities(engine: AsyncEngine) -> None:
    rows = await _fetch(engine, select(TerminalEquity.__table__))
    by_ticker = {row["ticker"]: row for row in rows}
    assert set(by_ticker) == set(TICKERS)
    for equity in EQUITIES:
        expected = asdict(equity)
        row = by_ticker[equity.ticker]
        assert {name: row[name] for name in expected} == expected, equity.ticker
        assert (row["currency"], row["source"]) == ("KES", SEED_SOURCE), equity.ticker
    scom = by_ticker["SCOM"]
    assert (scom["shares_outstanding"], scom["isin"]) == (40_065_428_000, "KE1000001402")
    assert sum(row["sector"] == BANKING_SECTOR for row in rows) == 5


async def _assert_macro(engine: AsyncEngine) -> None:
    rows = await _fetch(
        engine,
        select(TerminalMacroIndicator.__table__).order_by(TerminalMacroIndicator.observation_date),
    )
    assert len(rows) == EXPECTED_COUNTS["terminal_macro_indicators"]
    assert (rows[0]["observation_date"], rows[-1]["observation_date"]) == (
        MACRO_START,
        ANCHOR_DATE,
    )
    unmarked = []
    for row in rows:
        filled = {column for column in MACRO_COLUMNS if row[column] is not None}
        provenance = row["source_by_field"]
        if filled != set(MACRO_COLUMNS) or provenance != dict.fromkeys(filled, SEED_SOURCE):
            unmarked.append(row["observation_date"])
    assert unmarked == [], "every seeded month holds all eight indicators, each marked seed"

    by_date = {row["observation_date"]: row for row in rows}
    anchor = by_date[ANCHOR_DATE]
    assert {column: anchor[column] for column in MACRO_COLUMNS} == dict(MACRO_ANCHOR)
    assert anchor["inflation_yoy_pct"] == Decimal("6.80")
    assert anchor["cbr_rate_pct"] == Decimal("8.75")
    for column, day, value in MACRO_FIXED_POINTS:
        assert by_date[day][column] == value, (column, day)
    assert by_date[date(2026, 8, 31)]["inflation_yoy_pct"] == Decimal("6.60")
    assert by_date[date(2024, 1, 31)]["inflation_yoy_pct"] == Decimal("6.90")


async def _assert_fundamentals(engine: AsyncEngine) -> None:
    rows = await _fetch(engine, select(TerminalFundamental.__table__))
    assert len(rows) == EXPECTED_COUNTS["terminal_fundamentals"]
    period_ends: dict[str, set[date]] = {}
    for row in rows:
        ticker = row["ticker"]
        key = (ticker, row["period_end_date"])
        period_ends.setdefault(ticker, set()).add(row["period_end_date"])
        filled = {column for column in FUNDAMENTAL_VALUE_COLUMNS if row[column] is not None}
        expected_filled = set(FUNDAMENTAL_VALUE_COLUMNS)
        if ticker in BANK_TICKERS:
            expected_filled -= BANK_UNSEEDED_COLUMNS
        assert filled == expected_filled, key
        assert row["source_by_field"] == dict.fromkeys(filled, SEED_SOURCE), key
        assert (row["currency"], row["period_type"], row["filing_id"]) == ("KES", "FY", None), key

    assert period_ends == {
        target.ticker: {
            date(year, target.fy_end_month, target.fy_end_day)
            for year in range(target.first_fy, target.last_fy + 1)
        }
        for target in FUNDAMENTAL_TARGETS
    }
    assert date(2026, 3, 31) in period_ends["SCOM"]


async def _assert_bonds(engine: AsyncEngine) -> None:
    bond = TerminalBondYield
    unmarked = await _fetch(
        engine,
        select(func.count().label("n"))
        .select_from(bond)
        .where(
            or_(
                ~bond.bond_code.startswith(BOND_CODE_PREFIX, autoescape=True),
                bond.source != SEED_SOURCE,
            )
        ),
    )
    assert unmarked[0]["n"] == 0, "every seeded bond carries an FXD-SEED- code and source seed"

    anchor_rows = await _fetch(
        engine,
        select(
            bond.bond_code, bond.original_tenor_years, bond.remaining_tenor_years, bond.yield_pct
        )
        .where(bond.observation_date == ANCHOR_DATE)
        .order_by(bond.original_tenor_years),
    )
    # The anchor month carries no noise: each yield is the 364-day bill plus the tenor's spread.
    assert [dict(row) for row in anchor_rows] == [
        {
            "bond_code": f"{BOND_CODE_PREFIX}{tenor}Y-{ANCHOR_DATE:%Y%m}",
            "original_tenor_years": Decimal(tenor),
            "remaining_tenor_years": Decimal(tenor),
            "yield_pct": MACRO_ANCHOR["tbill_364d_pct"] + spread,
        }
        for tenor, spread in BOND_TENOR_SPREADS
    ]
    assert len(anchor_rows) == 6


async def _assert_cycles(engine: AsyncEngine) -> None:
    cycle = TerminalCycle
    rows = await _fetch(
        engine,
        select(cycle.label, cycle.event_date, cycle.event_type, cycle.source).order_by(
            cycle.event_date
        ),
    )
    assert [
        (row["label"], row["event_date"], row["event_type"], row["source"]) for row in rows
    ] == [
        ("2013 Cycle", date(2013, 3, 4), "general_election", SEED_SOURCE),
        ("2017 Cycle", date(2017, 8, 8), "general_election", SEED_SOURCE),
        ("2022 Cycle", date(2022, 8, 9), "general_election", SEED_SOURCE),
        ("2027 Election Watch", date(2027, 8, 10), "general_election", SEED_SOURCE),
    ]
    assert [(cycle.label, cycle.event_date) for cycle in CYCLES] == [
        (row["label"], row["event_date"]) for row in rows
    ]


async def _assert_ipo_preset(engine: AsyncEngine) -> None:
    (row,) = await _fetch(engine, select(TerminalIpoScenario.__table__))
    for preset_field in fields(QUICKMART_PRESET):
        name = preset_field.name
        assert row[name] == getattr(QUICKMART_PRESET, name), name
    assert (row["company_name"], row["sector"]) == (
        "Quickmart Supermarket",
        "Retail & Consumer Goods",
    )
    assert (row["offer_price_kes"], row["pct_stake_sold"], row["primary_proceeds_kes"]) == (
        Decimal("7.50"),
        Decimal("50.0"),
        Decimal("0"),
    )
    assert (row["shares_offered"], row["shares_outstanding_post_offer"]) == (
        500_000_000,
        1_000_000_000,
    )
    assert row["risk_scenarios"] == [dict(scenario) for scenario in QUICKMART_RISK_SCENARIOS]
    assert [scenario["title"] for scenario in row["risk_scenarios"]] == [
        "Ownership & Motive",
        "Growth Financing",
        "Valuation Floor",
    ]
    assert (row["currency"], row["source"], row["is_preset"]) == ("KES", SEED_SOURCE, True)


async def _assert_watchlist(engine: AsyncEngine) -> None:
    rows = await _fetch(
        engine,
        select(TerminalWatchlist.owner_key, TerminalWatchlist.name, TerminalWatchlistItem.ticker)
        .select_from(TerminalWatchlist)
        .join(TerminalWatchlistItem, TerminalWatchlistItem.watchlist_id == TerminalWatchlist.id),
    )
    assert {(row["owner_key"], row["name"]) for row in rows} == {(SEED_OWNER, SEED_WATCHLIST_NAME)}
    assert sorted(row["ticker"] for row in rows) == sorted(TICKERS)
    assert rows[0]["name"] == "NSE Core 8"


async def test_seeded_rows_are_complete_and_marked(seeded_database: _SeededDatabase) -> None:
    engine = seeded_database.engine

    assert await _counts(engine) == dict(EXPECTED_COUNTS)
    await _assert_seed_run_history(engine)
    await _assert_prices(engine)
    await _assert_equities(engine)
    await _assert_macro(engine)
    await _assert_fundamentals(engine)
    await _assert_bonds(engine)
    await _assert_cycles(engine)
    await _assert_ipo_preset(engine)
    await _assert_watchlist(engine)


# ---------------------------------------------------------------------------------------------
# Real data is never touched by a re-seed (AAP 0.8.1: ON CONFLICT DO NOTHING on natural keys)
# ---------------------------------------------------------------------------------------------


async def _price_rows(
    engine: AsyncEngine, ticker: str, days: Sequence[date]
) -> dict[date, dict[str, Any]]:
    """Every column of ``ticker``'s price row on each of ``days``; a missing row fails."""
    rows: dict[date, dict[str, Any]] = {}
    for day in days:
        (row,) = await _fetch(
            engine,
            select(TerminalDailyPrice.__table__).where(
                TerminalDailyPrice.ticker == ticker, TerminalDailyPrice.trade_date == day
            ),
        )
        rows[day] = dict(row)
    return rows


async def _macro_row(engine: AsyncEngine, day: date) -> dict[str, Any]:
    """Every column of the macro row dated ``day``; a missing row fails."""
    (row,) = await _fetch(
        engine,
        select(TerminalMacroIndicator.__table__).where(
            TerminalMacroIndicator.observation_date == day
        ),
    )
    return dict(row)


async def test_reseed_preserves_imported_rows(
    seeded_database: _SeededDatabase, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    settings = seeded_database.settings
    seeded_price = (await _price_rows(engine, "SCOM", [ANCHOR_DATE]))[ANCHOR_DATE]

    prices_csv = _write_csv(
        tmp_path / "daily_prices.csv",
        ("ticker", "trade_date", "close_price", "volume"),
        [
            ("SCOM", ANCHOR_DATE.isoformat(), "34.00", "1000"),
            ("SCOM", NEW_SESSION.isoformat(), "34.10", "2000"),
        ],
    )
    prices_run = await import_file("daily_prices", prices_csv, settings=settings)
    assert (prices_run.status, prices_run.rows_written) == ("succeeded", 2)

    macro_csv = _write_csv(
        tmp_path / "macro.csv",
        ("observation_date", "inflation_yoy_pct"),
        [(ANCHOR_DATE.isoformat(), "6.76")],
    )
    macro_run = await import_file("macro_indicators", macro_csv, settings=settings)
    assert (macro_run.status, macro_run.rows_written) == ("succeeded", 1)

    imported_prices = await _price_rows(engine, "SCOM", [ANCHOR_DATE, NEW_SESSION])
    imported_macro = await _macro_row(engine, ANCHOR_DATE)
    counts_before = await _counts(engine)
    assert counts_before == {
        **EXPECTED_COUNTS,
        "terminal_daily_prices": EXPECTED_COUNTS["terminal_daily_prices"] + 1,
    }

    report = await _reseed(seeded_database)

    assert report.total_inserted == 0
    assert await _counts(engine) == counts_before
    # Every column, ids and timestamps included, is exactly as the importer left it.
    assert await _price_rows(engine, "SCOM", [ANCHOR_DATE, NEW_SESSION]) == imported_prices
    assert await _macro_row(engine, ANCHOR_DATE) == imported_macro

    replaced = imported_prices[ANCHOR_DATE]
    assert (replaced["source"], replaced["close_price"], replaced["volume"]) == (
        CSV_IMPORT_SOURCE,
        Decimal("34.00"),
        1000,
    )
    # The file had no flow column, so the seeded flow and its provenance stay.
    assert replaced["foreign_net_flow_kes"] is not None
    assert (replaced["foreign_net_flow_kes"], replaced["flow_source"]) == (
        seeded_price["foreign_net_flow_kes"],
        SEED_SOURCE,
    )
    added = imported_prices[NEW_SESSION]
    assert (added["source"], added["close_price"], added["volume"]) == (
        CSV_IMPORT_SOURCE,
        Decimal("34.10"),
        2000,
    )

    assert imported_macro["inflation_yoy_pct"] == Decimal("6.76")
    assert imported_macro["source_by_field"] == {
        **dict.fromkeys(MACRO_COLUMNS, SEED_SOURCE),
        "inflation_yoy_pct": CSV_IMPORT_SOURCE,
    }
    untouched = [column for column in MACRO_COLUMNS if column != "inflation_yoy_pct"]
    assert {column: imported_macro[column] for column in untouched} == {
        column: MACRO_ANCHOR[column] for column in untouched
    }


# ---------------------------------------------------------------------------------------------
# Failure paths of the seeder (``opennse_terminal.seed.seed_terminal_db``) that need no database
#
# * ``run_seed`` prints nothing: each of its diagnostics is one ERROR record on the
#   ``opennse_terminal.seed`` logger, with no traceback, so a host's logging routes it.
# * An engine that cannot be created gives exit code 1 and one credential-safe record naming
#   where the URL came from.
# * A database that cannot be reached gets one readable ``could not connect to <label>: <reason>``
#   record: the OS reason for an ``OSError``, the type name for anything else.
# * The not-at-head message is logged once, and the ``seeding <label>`` line is never a URL.
# * ``main`` returns its exit code and never raises ``SystemExit``: 1 for a usage error, 0 for
#   ``--help`` (which documents the precondition, examples and exit codes), 130 for Ctrl-C.
# * A failure once connected is logged by exception type only. SQLAlchemy errors embed the
#   failed statement's parameters, such as ``default_owner_key``, so neither their text nor a
#   traceback may reach stderr, a log record or the failed run's ``error_message``.
# * Each failure is reported exactly once. ``seed`` reports what it raises: a failed run by its
#   run id, the only record of it, and a failure before the run starts (the schema check, invalid
#   default settings) by type. ``run_seed`` adds a record only for a failure outside ``seed``,
#   such as closing the session.
# * ``seed`` generates its dataset inside the recorded operation, after the schema check and
#   before the run row and any data write, so a generator or invariant failure leaves a failed
#   run row.
# * Sessions come from the shared ``db/session.py`` configuration: ``build_session_factory``
#   binds ``async_sessionmaker(expire_on_commit=False)`` to the caller's engine, and both
#   ``session_factory`` and ``run_seed`` build their sessionmaker through it.
#
# Secrets are synthetic markers, so a leak shows as the marker in stderr or a log record. The
# session is a recording double; writing rows and the run record for real needs PostgreSQL.
# ---------------------------------------------------------------------------------------------

URL_PW_MARKER = "urlpwMARK"
OWNER_MARKER = "OWNERMARK123"
USER_MARKER = "userMARK77"
# Templates of the two engine-creation lines; {source} is where the URL came from.
INVALID_URL_LINE = (
    "cannot seed: database URL is not valid; "
    "expected postgresql+asyncpg://user:password@host:port/database (from {source})"
)
ENGINE_FAILURE_LINE = (
    "cannot seed: could not create a database engine from {source} (ArgumentError)"
)
# Passes the settings prefix check; SQLAlchemy's asyncpg dialect raises ArgumentError (not a
# ValueError) because the second host has no port.
MULTIHOST_MISSING_PORT_URL = (
    f"postgresql+asyncpg://user:{URL_PW_MARKER}@/db?host=host-a:5432&host=host-b"
)
# build_engine's own ValueError.
NON_NUMERIC_PORT_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:notaport/db"
VALID_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:5432/opennse"
VALID_URL_LABEL = 'database "opennse" on db.example:5432'
# Nothing listens on port 1, so connecting is refused at once, with no database or network.
REFUSED_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@127.0.0.1:1/opennse"
URL_PREFIX = "postgresql+asyncpg://"
DEFAULT_PROG = "python -m opennse_terminal.seed.seed_terminal_db"
SEED_LOGGER = "opennse_terminal.seed"


class InvalidPasswordError(Exception):
    """Stands in for asyncpg's server error of the same name, whose text quotes the user."""


def _watchlist_insert_error() -> sa.exc.IntegrityError:
    """The error a failed watchlist INSERT raises: its text lists the bound parameters."""
    return sa.exc.IntegrityError(
        "INSERT INTO terminal_watchlists (id, owner_key, name) VALUES ($1, $2, $3)",
        {"id": "00000000-0000-0000-0000-000000000001", "owner_key": OWNER_MARKER},
        Exception(f"duplicate key value (owner_key)=({OWNER_MARKER})"),
    )


class _SessionDouble:
    """Records the calls the seeder makes on its ``AsyncSession``.

    ``commit_error`` is raised by every ``commit``, and ``close_error`` when ``async with`` closes
    the session. ``execute`` and ``scalar`` are the calls a data write or a query would make; they
    record and then fail, so an unexpected one is seen.
    """

    def __init__(
        self, commit_error: Exception | None = None, close_error: Exception | None = None
    ) -> None:
        self.calls: list[str] = []
        self.added: list[object] = []
        self.commit_error = commit_error
        self.close_error = close_error

    async def __aenter__(self) -> "_SessionDouble":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error

    async def rollback(self) -> None:
        self.calls.append("rollback")

    def add(self, instance: object) -> None:
        self.calls.append("add")
        self.added.append(instance)

    async def flush(self) -> None:
        self.calls.append("flush")

    async def commit(self) -> None:
        self.calls.append("commit")
        if self.commit_error is not None:
            raise self.commit_error

    async def execute(self, *args: object, **kwargs: object) -> None:
        self.calls.append("execute")
        raise AssertionError("unexpected execute")

    async def scalar(self, *args: object, **kwargs: object) -> None:
        self.calls.append("scalar")
        raise AssertionError("unexpected scalar")


def _assert_type_only(caplog: pytest.LogCaptureFixture, *texts: str) -> None:
    """No record carries a traceback, and no text holds the owner or URL marker."""
    assert all(record.exc_info is None for record in caplog.records)
    # The loop variable is not named text: that would shadow sqlalchemy's text() import.
    for output in (caplog.text, *texts):
        assert "Traceback" not in output
        assert OWNER_MARKER not in output
        assert URL_PW_MARKER not in output


def _seed_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Messages of the ERROR records, each of which must come from the seeder's logger."""
    records = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert [record.name for record in records] == [SEED_LOGGER] * len(records)
    return [record.getMessage() for record in records]


async def _connected(engine: AsyncEngine) -> None:
    """Replaces ``_check_connection`` where the database is not reached: it always succeeds."""


def _record_engines(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[AsyncEngine], list[AsyncEngine]]:
    """Record every engine ``run_seed`` builds and every engine disposed, in that order."""
    built: list[AsyncEngine] = []
    disposed: list[AsyncEngine] = []
    original_dispose = AsyncEngine.dispose

    def recording_build_engine(settings: TerminalSettings) -> AsyncEngine:
        engine = build_engine(settings)
        built.append(engine)
        return engine

    async def recording_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)
        await original_dispose(self, close)

    monkeypatch.setattr(seed_terminal_db, "build_engine", recording_build_engine)
    # AsyncEngine has __slots__, so disposal is recorded on the class for this test only.
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)
    return built, disposed


def _use_session_double(
    monkeypatch: pytest.MonkeyPatch, session: _SessionDouble
) -> list[AsyncEngine]:
    """Make ``run_seed`` open ``session`` through ``build_session_factory``; return the engines
    the factory was built on."""
    bound: list[AsyncEngine] = []

    def session_double_factory(engine: AsyncEngine) -> Callable[[], _SessionDouble]:
        bound.append(engine)
        return lambda: session

    monkeypatch.setattr(seed_terminal_db, "build_session_factory", session_double_factory)
    return bound


ENGINE_CREATION_FAILURES = [
    pytest.param(MULTIHOST_MISSING_PORT_URL, ENGINE_FAILURE_LINE, id="multihost-missing-port"),
    pytest.param(NON_NUMERIC_PORT_URL, INVALID_URL_LINE, id="non-numeric-port"),
]


@pytest.mark.parametrize(("database_url", "expected_line"), ENGINE_CREATION_FAILURES)
async def test_run_seed_engine_creation_failure_returns_1(
    database_url: str,
    expected_line: str,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=database_url, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    # Logged once at ERROR and never printed, so the host's logging routes it.
    assert stderr == ""
    assert _seed_errors(caplog) == [expected_line.format(source="settings.database_url")]
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(("database_url", "expected_line"), ENGINE_CREATION_FAILURES)
@pytest.mark.parametrize("source", ["--database-url", "TERMINAL_DATABASE_URL"])
def test_main_names_the_url_source_in_engine_creation_failures(
    database_url: str,
    expected_line: str,
    source: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # No .env is read from tmp_path, and the environment's own URL is never used.
    monkeypatch.chdir(tmp_path)
    if source == "--database-url":
        monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)
        argv = ["--database-url", database_url]
    else:
        monkeypatch.setenv("TERMINAL_DATABASE_URL", database_url)
        argv = []
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = seed_terminal_db.main(argv)

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [expected_line.format(source=source)]
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(
    ("argv", "error"),
    [
        pytest.param(["--bogus"], "unrecognized arguments: --bogus", id="unknown-option"),
        pytest.param(["extra"], "unrecognized arguments: extra", id="extra-positional"),
        pytest.param(
            ["--database-url"],
            "argument --database-url: expected one argument",
            id="missing-value",
        ),
    ],
)
def test_main_returns_1_for_a_usage_error(
    argv: list[str],
    error: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)

    # Returned, never raised as SystemExit; 2 would read as "schema not at head".
    exit_code = seed_terminal_db.main(argv)

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith(f"usage: {DEFAULT_PROG} ")
    assert captured.err.endswith(f"\n{DEFAULT_PROG}: error: {error}\n")


@pytest.mark.parametrize(
    ("prog", "shown"),
    [
        pytest.param(None, DEFAULT_PROG, id="default-prog"),
        pytest.param("opennse-terminal seed", "opennse-terminal seed", id="cli-prog"),
    ],
)
def test_main_help_documents_the_precondition_examples_and_exit_codes(
    prog: str | None,
    shown: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)

    exit_code = seed_terminal_db.main(["--help"], prog=prog)

    assert exit_code == 0
    help_text = capsys.readouterr().out
    assert help_text.startswith(f"usage: {shown} [-h]")
    assert "TERMINAL_DATABASE_URL (or .env" in help_text
    assert "run `opennse-terminal migrate upgrade head` against the same" in help_text
    assert f"\n  opennse-terminal migrate upgrade head && {shown}\n" in help_text
    assert f"\n  {shown} --database-url 'postgresql+asyncpg://USER:PASSWORD@" in help_text
    for exit_line in (
        "\n  0    seeded, including a re-run that inserts nothing\n",
        "\n  1    failed and rolled back, or invalid arguments or settings\n",
        "\n  2    schema not at migration head: run `opennse-terminal migrate upgrade head` "
        "first\n",
        "\n  130  interrupted (Ctrl-C); any uncommitted seed transaction was rolled back\n",
    ):
        assert exit_line in help_text
    assert "opennse:opennse@" not in help_text


def test_main_reports_an_interrupt_in_one_line_and_returns_130(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sources: list[str] = []

    # A plain function, so no coroutine or event loop is created: the interrupt arrives where
    # asyncio.run would raise it.
    def interrupted_run_seed(settings: TerminalSettings, *, url_source: str) -> int:
        sources.append(url_source)
        raise KeyboardInterrupt

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)
    monkeypatch.setattr(seed_terminal_db, "run_seed", interrupted_run_seed)

    exit_code = seed_terminal_db.main(["--database-url", VALID_URL])

    assert exit_code == 130
    assert sources == ["--database-url"]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "seeding interrupted; any uncommitted seed transaction was rolled back, "
        "and re-running the seeder is safe\n"
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(_watchlist_insert_error(), id="integrity-error-with-parameters"),
        pytest.param(
            RuntimeError(f"seeded watchlist 'NSE Core 8' for owner {OWNER_MARKER!r} is missing"),
            id="runtime-error-naming-owner",
        ),
    ],
)
async def test_run_seed_reports_a_failed_run_once_by_its_run_id(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    dataset = object()
    write_run_ids: list[uuid.UUID] = []
    built, disposed = _record_engines(monkeypatch)
    bound = _use_session_double(monkeypatch, session)

    async def schema_at_head(executor: object) -> str:
        assert executor is session
        return "terminal_0001"

    def generate_all() -> object:
        return dataset

    async def failing_write(
        write_session: object, written: object, *, owner_key: str, run_id: uuid.UUID
    ) -> None:
        assert write_session is session
        assert written is dataset
        assert owner_key == OWNER_MARKER
        session.calls.append("write")
        write_run_ids.append(run_id)
        raise error

    # db.example is never reached: the connection check passes, and the real seed() runs on the
    # session double up to the failing data write.
    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", schema_at_head)
    monkeypatch.setattr(seed_terminal_db.synthetic, "generate_all", generate_all)
    monkeypatch.setattr(seed_terminal_db, "_write_dataset", failing_write)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")
    settings = TerminalSettings(
        database_url=f"{VALID_URL}?password={URL_PW_MARKER}",
        default_owner_key=OWNER_MARKER,
        _env_file=None,
    )

    exit_code = await run_seed(settings)

    assert exit_code == 1
    assert len(built) == 1
    assert bound == built
    assert disposed == built
    # The running row and the failed write, then the failure recorded in a fresh transaction;
    # returning the exit code still closes the session.
    assert session.calls == ["add", "flush", "write", "rollback", "add", "commit", "close"]
    running, failed = session.added
    assert isinstance(running, TerminalIngestionRun)
    assert isinstance(failed, TerminalIngestionRun)
    assert running.id == failed.id == write_run_ids[0]
    assert failed.status == "failed"
    assert failed.error_message == type(error).__name__
    messages = [record.getMessage() for record in caplog.records]
    assert f"seeding {VALID_URL_LABEL}" in messages
    # seed()'s record, naming the failed run row, is the only one: run_seed adds none.
    assert _seed_errors(caplog) == [
        f"seed run {failed.id} failed and was rolled back: {type(error).__name__}"
    ]
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_failed_schema_check_once_by_type(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    query_error = sa.exc.OperationalError(
        "SELECT version_num FROM terminal_alembic_version WHERE owner_key = $1",
        {"owner_key": OWNER_MARKER},
        Exception(f"server closed the connection while serving {OWNER_MARKER}"),
    )
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def failing_schema_check(executor: object) -> str:
        assert executor is session
        raise query_error

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", failing_schema_check)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    assert disposed == built
    assert len(built) == 1
    # No run had started, so nothing was added; the one record names the failure by type.
    assert session.calls == ["close"]
    assert _seed_errors(caplog) == ["seeding failed before the run started: OperationalError"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_failure_outside_seed_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    close_error = sa.exc.OperationalError(
        "ROLLBACK",
        {"owner_key": OWNER_MARKER},
        Exception(f"connection lost while closing the session of {OWNER_MARKER}"),
    )
    session = _SessionDouble(close_error=close_error)
    seeded: list[object] = []
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def succeeding_seed(seed_session: object, *, settings: TerminalSettings) -> None:
        seeded.append(seed_session)

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "seed", succeeding_seed)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    assert seeded == [session]
    assert session.calls == ["close"]
    assert disposed == built
    assert len(built) == 1
    assert _seed_errors(caplog) == ["seeding failed: OperationalError"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    _assert_type_only(caplog, stderr)


async def test_run_seed_logs_the_not_at_head_message_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def no_stored_revision(executor: object) -> None:
        assert executor is session

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    # The real ensure_schema_at_head finds no stored revision, as on an unmigrated database.
    monkeypatch.setattr(seed_terminal_db, "current_schema_revision", no_stored_revision)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    not_at_head = str(SchemaNotAtHeadError(None, migrations.head_revision()))
    assert exit_code == 2
    assert disposed == built
    assert len(built) == 1
    # Nothing is written, not even a run row, and the session is closed.
    assert session.calls == ["close"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [not_at_head]
    assert not_at_head.endswith(SCHEMA_HINT)
    assert f"seeding {VALID_URL_LABEL}" in [record.getMessage() for record in caplog.records]
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_refused_connection_readably(
    capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=REFUSED_URL, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [
        'cannot seed: could not connect to database "opennse" on 127.0.0.1:1: '
        "[Errno 111] Connect call failed ('127.0.0.1', 1)"
    ]
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        pytest.param(
            socket.gaierror(-2, "Name or service not known"),
            "[Errno -2] Name or service not known",
            id="unresolvable-host",
        ),
        pytest.param(
            OSError(
                "Multiple exceptions: [Errno 111] Connect call failed ('127.0.0.1', 5999), "
                "[Errno 99] Cannot assign requested address"
            ),
            "Multiple exceptions: [Errno 111] Connect call failed ('127.0.0.1', 5999), "
            "[Errno 99] Cannot assign requested address",
            id="several-addresses",
        ),
        pytest.param(TimeoutError(), "timed out", id="connect-timeout"),
        pytest.param(OSError(), "OSError", id="os-error-without-text"),
        pytest.param(
            InvalidPasswordError(f'password authentication failed for user "{USER_MARKER}"'),
            "InvalidPasswordError",
            id="server-error-type-only",
        ),
    ],
)
async def test_run_seed_connection_failure_reason(
    error: Exception,
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failing_connection(engine: AsyncEngine) -> None:
        raise error

    async def unreachable_seed(session: object, *, settings: TerminalSettings) -> None:
        raise AssertionError("seed must not run after a failed connection")

    monkeypatch.setattr(seed_terminal_db, "_check_connection", failing_connection)
    monkeypatch.setattr(seed_terminal_db, "seed", unreachable_seed)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [
        f"cannot seed: could not connect to {VALID_URL_LABEL}: {reason}"
    ]
    assert USER_MARKER not in stderr
    assert USER_MARKER not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_lets_a_cancelled_connection_attempt_propagate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    built: list[AsyncEngine] = []
    disposed: list[AsyncEngine] = []
    original_dispose = AsyncEngine.dispose

    def recording_build_engine(settings: TerminalSettings) -> AsyncEngine:
        engine = build_engine(settings)
        built.append(engine)
        return engine

    async def recording_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)
        await original_dispose(self, close)

    async def cancelled_connection(engine: AsyncEngine) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(seed_terminal_db, "build_engine", recording_build_engine)
    monkeypatch.setattr(seed_terminal_db, "_check_connection", cancelled_connection)
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)

    # An interrupt is not a connection failure: it propagates, after the engine is disposed.
    with pytest.raises(asyncio.CancelledError):
        await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert len(built) == 1
    assert disposed == built
    assert capsys.readouterr().err == ""


async def test_seed_records_a_generation_failure_as_a_failed_run(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    session = _SessionDouble()
    invariant_error = RuntimeError(
        "planted spike SCOM 2026-09-15 has volume Z-score 3.10, below the required 4.00"
    )

    async def schema_at_head(executor: object) -> str:
        assert executor is session
        session.calls.append("schema_check")
        return "terminal_0001"

    def failing_generate_all() -> None:
        session.calls.append("generate_all")
        raise invariant_error

    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", schema_at_head)
    monkeypatch.setattr(seed_terminal_db.synthetic, "generate_all", failing_generate_all)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")
    settings = TerminalSettings(default_owner_key=OWNER_MARKER, _env_file=None)
    before = datetime.now(UTC)

    with pytest.raises(RuntimeError) as raised:
        await seed_terminal_db.seed(session, settings=settings)  # type: ignore[arg-type]

    assert raised.value is invariant_error
    # The schema check comes first; generation fails before the run row or any data write, and
    # the failure is then recorded in a fresh transaction.
    assert session.calls == ["schema_check", "generate_all", "rollback", "add", "commit"]
    (run,) = session.added
    assert isinstance(run, TerminalIngestionRun)
    assert run.pipeline == "seed"
    assert run.trigger == "seed"
    assert run.status == "failed"
    assert run.error_message == "RuntimeError"
    assert (run.rows_fetched, run.rows_written, run.rows_rejected) == (0, 0, 0)
    assert run.details == {}
    assert isinstance(run.id, uuid.UUID)
    assert run.finished_at is not None
    assert before <= run.started_at <= run.finished_at
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"seed run {run.id} failed and was rolled back: RuntimeError"]
    assert "Z-score" not in caplog.text
    _assert_type_only(caplog)


async def test_seed_reports_invalid_default_settings_once_by_type(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    session = _SessionDouble()
    # No .env is read from tmp_path; the environment's URL lacks the asyncpg driver.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(
        "TERMINAL_DATABASE_URL", f"postgresql://user:{URL_PW_MARKER}@db.example:5432/opennse"
    )
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    with pytest.raises(ValidationError):
        await seed_terminal_db.seed(session)  # type: ignore[arg-type]

    # The settings are read before the schema check, so the session is never used.
    assert session.calls == []
    assert _seed_errors(caplog) == ["seeding failed before the run started: ValidationError"]
    _assert_type_only(caplog)


async def test_record_failure_stores_the_type_and_logs_its_own_failure_by_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    run_id = uuid.uuid4()
    commit_error = sa.exc.OperationalError(
        "INSERT INTO terminal_ingestion_runs (error_message) VALUES ($1)",
        {"error_message": OWNER_MARKER},
        Exception(f"connection lost while writing {OWNER_MARKER}"),
    )
    session = _SessionDouble(commit_error=commit_error)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    await seed_terminal_db._record_failure(
        session,  # type: ignore[arg-type]
        run_id=run_id,
        started_at=datetime(2026, 9, 30, 12, 0, tzinfo=UTC),
        exc=_watchlist_insert_error(),
    )

    assert session.calls == ["rollback", "add", "commit"]
    (run,) = session.added
    assert isinstance(run, TerminalIngestionRun)
    assert run.error_message == "IntegrityError"
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"could not record seed run {run_id} as failed: OperationalError"]
    _assert_type_only(caplog)


async def test_build_session_factory_is_the_shared_session_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = TerminalSettings(database_url=VALID_URL, _env_file=None)
    engine = build_engine(settings)
    built: list[async_sessionmaker[AsyncSession]] = []
    original_build_session_factory = db_session.build_session_factory

    def recording_build_session_factory(bound: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        factory = original_build_session_factory(bound)
        built.append(factory)
        return factory

    await dispose_terminal_engine()
    try:
        factory = build_session_factory(engine)
        assert isinstance(factory, async_sessionmaker)
        assert factory.kw["bind"] is engine
        assert factory.kw["expire_on_commit"] is False
        # Opening and closing a session connects to nothing, so db.example is never reached.
        async with factory() as session:
            assert session.bind is engine

        # The API's cached sessionmaker is built by the same function, on its own engine.
        monkeypatch.setattr(db_session, "build_session_factory", recording_build_session_factory)
        cached = session_factory(settings)
        assert built == [cached]
        assert cached.kw["bind"] is db_session._CACHE[VALID_URL][0]
        assert cached.kw["expire_on_commit"] is False
        assert session_factory(settings) is cached
        assert len(built) == 1
    finally:
        await dispose_terminal_engine()
        await engine.dispose()


# ---------------------------------------------------------------------------------------------
# Natural-key write order of the seeder's table writer (``seed_terminal_db._TableWriter``)
#
# * ``write`` de-duplicates and sorts a table's complete input by its natural key before slicing
#   it into ``SEED_BATCH_SIZE`` batches, so the batches, concatenated, are in one strictly
#   ascending key order. Concurrent writers such as the CSV importer lock rows in that order too,
#   so a seed and an import inserting the same new keys cannot deadlock.
# * The 30,096 generated price rows, which ``synthetic.generate_prices`` yields per counter with
#   SCOM first, reach ``upsert_rows`` in ``(ticker, trade_date)`` order, at most
#   ``SEED_BATCH_SIZE`` rows per call, each call ``DO NOTHING`` on the natural key with the
#   caller's own mappings; the tallies are 30,096 offered and 30,096 inserted.
# * An input spanning several batches, in reverse or per-counter order with a repeated key: the
#   last occurrence of the key is the one written, ``candidates`` counts every row offered and
#   ``inserted`` sums what each call reports.
# * A natural-key column that is missing or ``None`` raises ``ValueError`` naming the table, row
#   and column before any batch is written.
#
# ``upsert_rows`` is replaced by a recorder that keeps every batch, so no database is needed;
# writing the seed for real is covered by the seeded tests above.
# ---------------------------------------------------------------------------------------------

PRICE_KEY = ("ticker", "trade_date")
EQUITY_KEY = ("ticker",)
SEEDED_PRICE_ROWS = 30_096
# Small enough that the short inputs below span several batches.
SMALL_BATCH_SIZE = 2
# Stands in for the AsyncSession: the recorder only checks that each call receives it.
SESSION = object()


@dataclass
class _UpsertCall:
    executor: object
    model: type
    rows: list[Mapping[str, object]]
    key: tuple[str, ...]
    mode: str


@dataclass
class _UpsertRecorder:
    """Stands in for ``upsert_rows``: keeps each batch and reports its rows inserted, except rows
    whose natural key is in ``existing``, which ``DO NOTHING`` would skip."""

    existing: frozenset[tuple[object, ...]] = frozenset()
    calls: list[_UpsertCall] = field(default_factory=list)

    async def __call__(
        self,
        executor: object,
        model: type,
        rows: Sequence[Mapping[str, object]],
        key: Sequence[str],
        mode: str,
    ) -> int:
        self.calls.append(_UpsertCall(executor, model, list(rows), tuple(key), mode))
        return sum(1 for row in rows if _key_of(row, key) not in self.existing)

    @property
    def written(self) -> list[Mapping[str, object]]:
        """Every row offered to ``upsert_rows``, in call order."""
        return [row for call in self.calls for row in call.rows]


def _key_of(row: Mapping[str, object], key: Sequence[str]) -> tuple[object, ...]:
    return tuple(row[name] for name in key)


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> _UpsertRecorder:
    upsert = _UpsertRecorder()
    monkeypatch.setattr(seed_terminal_db, "upsert_rows", upsert)
    return upsert


def _price(ticker: str, day: int, close: str) -> dict[str, object]:
    return {"ticker": ticker, "trade_date": date(2026, 9, day), "close_price": Decimal(close)}


def _equity(ticker: str, name: str) -> dict[str, object]:
    return {"ticker": ticker, "company_name": name}


def _assert_strictly_ascending(keys: Sequence[tuple[object, ...]]) -> None:
    out_of_order = [(a, b) for a, b in zip(keys, keys[1:], strict=False) if not a < b]
    assert out_of_order == []


async def test_generated_prices_are_written_in_natural_key_order(
    recorder: _UpsertRecorder,
) -> None:
    run_id = uuid.uuid4()
    rows = [
        seed_terminal_db._row_with_id(row, ingestion_run_id=run_id)
        for row in synthetic.generate_prices()
    ]
    offered_keys = [_key_of(row, PRICE_KEY) for row in rows]
    # The generator's per-counter order is not key order, so the writer has something to sort.
    assert rows[0]["ticker"] == "SCOM"
    assert offered_keys != sorted(offered_keys)

    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]
    await writer.write(TerminalDailyPrice, PRICE_KEY, rows)

    written_keys = [_key_of(row, PRICE_KEY) for row in recorder.written]
    assert written_keys == sorted(offered_keys)
    _assert_strictly_ascending(written_keys)
    assert len(recorder.calls) == ceil(SEEDED_PRICE_ROWS / SEED_BATCH_SIZE)
    assert all(len(call.rows) <= SEED_BATCH_SIZE for call in recorder.calls)
    assert {(call.executor, call.model, call.key, call.mode) for call in recorder.calls} == {
        (SESSION, TerminalDailyPrice, PRICE_KEY, "do_nothing")
    }
    assert sorted(map(id, recorder.written)) == sorted(map(id, rows))
    assert writer.candidates == {"terminal_daily_prices": SEEDED_PRICE_ROWS}
    assert writer.inserted == {"terminal_daily_prices": SEEDED_PRICE_ROWS}


@pytest.mark.parametrize(
    ("model", "key", "rows", "existing"),
    [
        pytest.param(
            TerminalDailyPrice,
            PRICE_KEY,
            [
                _price("SCOM", 30, "28.10"),
                _price("SCOM", 29, "28.00"),
                _price("KCB", 30, "61.25"),
                _price("SCOM", 30, "28.15"),
                _price("EQTY", 29, "60.00"),
                _price("ABSA", 30, "24.80"),
                _price("ABSA", 29, "24.70"),
            ],
            frozenset({("KCB", date(2026, 9, 30))}),
            id="prices-reverse-order",
        ),
        pytest.param(
            TerminalDailyPrice,
            PRICE_KEY,
            [
                _price("SCOM", 29, "28.00"),
                _price("SCOM", 30, "28.10"),
                _price("EQTY", 29, "60.00"),
                _price("EQTY", 30, "60.50"),
                _price("ABSA", 29, "24.70"),
                _price("EQTY", 29, "60.05"),
                _price("ABSA", 30, "24.80"),
            ],
            frozenset(),
            id="prices-per-counter-order",
        ),
        pytest.param(
            TerminalEquity,
            EQUITY_KEY,
            [
                _equity("SCOM", "Safaricom PLC"),
                _equity("KEGN", "KenGen"),
                _equity("EQTY", "Equity Group Holdings PLC"),
                _equity("KEGN", "Kenya Electricity Generating Company PLC"),
                _equity("ABSA", "Absa Bank Kenya PLC"),
            ],
            frozenset({("EQTY",)}),
            id="equities-single-column-key",
        ),
    ],
)
async def test_batches_follow_the_sorted_unique_keys(
    recorder: _UpsertRecorder,
    monkeypatch: pytest.MonkeyPatch,
    model: type[TerminalDailyPrice] | type[TerminalEquity],
    key: tuple[str, ...],
    rows: list[dict[str, object]],
    existing: frozenset[tuple[object, ...]],
) -> None:
    monkeypatch.setattr(seed_terminal_db, "SEED_BATCH_SIZE", SMALL_BATCH_SIZE)
    recorder.existing = existing
    last_by_key = {_key_of(row, key): row for row in rows}
    expected = [last_by_key[row_key] for row_key in sorted(last_by_key)]
    assert len(expected) > SMALL_BATCH_SIZE

    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]
    await writer.write(model, key, rows)

    # Identity, not equality: each repeated key is written as its last occurrence, as given.
    assert [id(row) for row in recorder.written] == [id(row) for row in expected]
    _assert_strictly_ascending([_key_of(row, key) for row in recorder.written])
    assert [len(call.rows) for call in recorder.calls] == [
        min(SMALL_BATCH_SIZE, len(expected) - start)
        for start in range(0, len(expected), SMALL_BATCH_SIZE)
    ]
    table = model.__tablename__
    assert writer.candidates == {table: len(rows)}
    assert writer.inserted == {table: len(expected) - len(existing)}


@pytest.mark.parametrize(
    ("bad_row", "message"),
    [
        pytest.param(
            {"ticker": "EQTY", "trade_date": None},
            "terminal_daily_prices row 1: natural-key column 'trade_date' is None",
            id="none-value",
        ),
        pytest.param(
            {"trade_date": date(2026, 9, 30)},
            "terminal_daily_prices row 1: natural-key column 'ticker' is missing",
            id="missing-column",
        ),
    ],
)
async def test_unusable_natural_key_raises_before_any_batch(
    recorder: _UpsertRecorder, bad_row: dict[str, object], message: str
) -> None:
    rows = [_price("SCOM", 30, "28.10"), bad_row, _price("ABSA", 30, "24.80")]
    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]

    with pytest.raises(ValueError) as raised:
        await writer.write(TerminalDailyPrice, PRICE_KEY, rows)

    assert str(raised.value) == message
    assert recorder.calls == []
    assert writer.inserted == {}
    assert writer.candidates == {}
