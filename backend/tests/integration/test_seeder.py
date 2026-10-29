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
"""

import asyncio
import csv
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, fields
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Protocol

import pytest
from sqlalchemy import Executable, Select, func, or_, select, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from opennse_terminal import cli, migrations
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
from opennse_terminal.seed import seed_terminal_db
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
    def settings(self) -> Any:
        """``TerminalSettings`` addressing the clone (owner ``local``).

        Typed ``Any``: the settings module is not among this file's dependencies, and the value is
        only passed through to ``seed()`` and ``import_file()``.
        """


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
