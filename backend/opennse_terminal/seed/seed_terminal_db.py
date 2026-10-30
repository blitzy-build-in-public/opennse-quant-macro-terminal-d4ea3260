"""Idempotent fallback seeder: deterministic Kenyan market data for nine ``terminal_`` tables.

Run it as ``python -m opennse_terminal.seed.seed_terminal_db [--database-url URL]`` or
``opennse-terminal seed [--database-url URL]``; both call :func:`main`, and ``--help`` lists the
examples and exit codes. The database comes from ``TERMINAL_DATABASE_URL`` (or ``.env``), and
``--database-url`` overrides it. Exit codes: ``0`` seeded (including a re-run that inserts
nothing); ``1`` failed and rolled back, or invalid arguments or settings; ``2`` the schema is not
at the packaged migration head ("run `opennse-terminal migrate upgrade head` first"); ``130``
interrupted by Ctrl-C, or ``143`` terminated by SIGTERM, with any uncommitted seed transaction
rolled back. A started run that the cancellation reached before its commit is then recorded
``failed`` ("cancelled"); one whose commit had reached the server keeps its ``succeeded`` run row
and its data (see :func:`seed`).

What is written. The records of :func:`opennse_terminal.seed.synthetic.generate_all`, in nine
reference, history and watchlist tables: the eight attachment counters, their synthetic daily
prices and fundamentals, the macro and bond history, the four election cycles, the Quickmart IPO
preset, and the "NSE Core 8" watchlist owned by ``TerminalSettings.default_owner_key``. Each run
also adds its own ``terminal_ingestion_runs`` row. No ``terminal_corporate_filings`` row is
written; only ``POST /ingest/pdf`` writes those. Only ``terminal_`` tables are touched, through the
ORM models and :func:`opennse_terminal.db.upsert.upsert_rows`; no user or auth row is created.

Idempotency. Every insert is ``INSERT ... ON CONFLICT (<natural key>) DO NOTHING``, and generation
is deterministic, so a re-run inserts no data rows and adds only its own
``terminal_ingestion_runs`` row (``pipeline='seed'``, ``trigger='seed'``). Rows written by the
scrapers, CBK pipelines, PDF commits or CSV imports are never updated.

Provenance. Seeded rows carry ``source = 'seed'`` (and ``flow_source = 'seed'`` on prices), and
``source_by_field`` maps every seeded macro and fundamentals column to ``"seed"``. That makes the
seed removable once real data exists, for example::

    DELETE FROM terminal_daily_prices WHERE source = 'seed';
    UPDATE terminal_macro_indicators SET tbill_91d_pct = NULL
        WHERE source_by_field ->> 'tbill_91d_pct' = 'seed';

The same ``DELETE`` applies to ``terminal_bond_yields``, ``terminal_cycles`` and
``terminal_ipo_scenarios``, and the same per-column ``UPDATE`` to ``terminal_fundamentals``.
``terminal_equities`` is the exception. An equity is a shared reference row: it keeps
``source = 'seed'`` after real prices, fundamentals or watchlist items attach to it, and deleting
it cascades to those rows and clears the ``ticker`` of its corporate filings. Keep every
referenced equity and remove only unreferenced seed equities::

    DELETE FROM terminal_equities e WHERE e.source = 'seed'
        AND NOT EXISTS (SELECT 1 FROM terminal_daily_prices p WHERE p.ticker = e.ticker)
        AND NOT EXISTS (SELECT 1 FROM terminal_fundamentals f WHERE f.ticker = e.ticker)
        AND NOT EXISTS (SELECT 1 FROM terminal_watchlist_items w WHERE w.ticker = e.ticker)
        AND NOT EXISTS (SELECT 1 FROM terminal_corporate_filings c WHERE c.ticker = e.ticker);

The seeded fundamentals rows (the per-column ``UPDATE`` clears their values but keeps the rows)
and the "NSE Core 8" watchlist reference all eight seeded tickers, so this statement keeps those
equities until those rows are removed as well.
"""

import argparse
import asyncio
import logging
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from types import MappingProxyType
from typing import Final, NoReturn, TypeAlias

import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from opennse_terminal import migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine, build_session_factory, database_log_label
from opennse_terminal.db.upsert import Executor, upsert_rows
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
from opennse_terminal.seed import synthetic
from opennse_terminal.seed.synthetic import SeedDataset, SeedRecord

__all__ = [
    "SCHEMA_HINT",
    "SEED_BATCH_SIZE",
    "SEED_PIPELINE",
    "SEED_TRIGGER",
    "SchemaNotAtHeadError",
    "SeedReport",
    "build_parser",
    "current_schema_revision",
    "ensure_schema_at_head",
    "main",
    "run_seed",
    "seed",
]

logger = logging.getLogger("opennse_terminal.seed")

SEED_PIPELINE: Final = "seed"
"""``terminal_ingestion_runs.pipeline`` of every seeder run."""

SEED_TRIGGER: Final = "seed"
"""``terminal_ingestion_runs.trigger`` of every seeder run."""

SEED_BATCH_SIZE: Final = 1000
"""Rows per ``upsert_rows`` call, sliced from a table's rows only after they are sorted by natural
key, so the batches follow one key order; keeps each price statement far below asyncpg's 32,767
bind-parameter limit."""

SCHEMA_HINT: Final = "run `opennse-terminal migrate upgrade head` first"
"""Operator instruction carried by :class:`SchemaNotAtHeadError` and logged at ERROR on exit code
2."""

_EXIT_OK: Final = 0
_EXIT_FAILED: Final = 1
_EXIT_SCHEMA_NOT_AT_HEAD: Final = 2
# 128 + SIGINT, the shell's code for a command stopped by Ctrl-C.
_EXIT_INTERRUPTED: Final = 130
_INTERRUPTED_MESSAGE: Final = (
    "seeding interrupted; any uncommitted seed transaction was rolled back, "
    "and re-running the seeder is safe"
)
# 128 + SIGTERM, the shell's code for a command stopped by SIGTERM.
_EXIT_TERMINATED: Final = 143
_TERMINATED_MESSAGE: Final = (
    "seeding terminated (SIGTERM); any uncommitted seed transaction was rolled back, "
    "and re-running the seeder is safe"
)
# error_message of a seed run stopped by Ctrl-C or SIGTERM, as for a cancelled ingestion run.
_CANCELLED_MESSAGE: Final = "cancelled"
_SEED_CALENDAR: Final = "synthetic"
# Where main() took the database URL from, named in the messages about the URL itself.
_URL_SOURCE_FLAG: Final = "--database-url"
_URL_SOURCE_ENV: Final = "TERMINAL_DATABASE_URL"
_DEFAULT_PROG: Final = "python -m opennse_terminal.seed.seed_terminal_db"
# argparse substitutes %(prog)s; a literal percent sign here would have to be written %%.
_HELP_EPILOG: Final = """\
The database URL comes from TERMINAL_DATABASE_URL (or .env in the working
directory), and --database-url overrides it. The schema must be at the packaged
migration head, so run `opennse-terminal migrate upgrade head` against the same
database first. Seeding is idempotent: a re-run inserts no data rows and
records only its own run.

examples:
  opennse-terminal migrate upgrade head && %(prog)s
  %(prog)s --database-url 'postgresql+asyncpg://USER:PASSWORD@HOST:PORT/DATABASE'

exit codes:
  0    seeded, including a re-run that inserts nothing
  1    failed and rolled back, or invalid arguments or settings
  2    schema not at migration head: run `opennse-terminal migrate upgrade head` first
  130  interrupted (Ctrl-C); any uncommitted seed transaction was rolled back
  143  terminated (SIGTERM); any uncommitted seed transaction was rolled back
"""

# to_regclass returns NULL instead of raising for a missing relation, so an unmigrated database
# is detected without aborting the transaction. The table name is the bound parameter ``name``.
_VERSION_TABLE_EXISTS: Final = sa.select(
    sa.func.to_regclass(sa.bindparam("name", type_=sa.String())).is_not(None)
)
_VERSION_TABLE: Final = sa.table(migrations.VERSION_TABLE, sa.column("version_num", sa.String))

_SeedModel: TypeAlias = (
    type[TerminalEquity]
    | type[TerminalDailyPrice]
    | type[TerminalFundamental]
    | type[TerminalMacroIndicator]
    | type[TerminalBondYield]
    | type[TerminalCycle]
    | type[TerminalIpoScenario]
    | type[TerminalWatchlist]
    | type[TerminalWatchlistItem]
)
"""The nine tables the seeder writes, in foreign-key order."""


class SchemaNotAtHeadError(RuntimeError):
    """The database's ``terminal_alembic_version`` is missing or not at the packaged head.

    ``found`` is the stored revision (several are joined by ``","``), or ``None`` when the version
    table is absent or empty; ``expected`` is :func:`opennse_terminal.migrations.head_revision`.
    The message ends with :data:`SCHEMA_HINT`.
    """

    def __init__(self, found: str | None, expected: str) -> None:
        self.found = found
        self.expected = expected
        stored = found if found is not None else f"no {migrations.VERSION_TABLE} revision"
        super().__init__(
            f"terminal schema is not at migration head (found {stored}, expected {expected}); "
            f"{SCHEMA_HINT}"
        )


@dataclass(frozen=True)
class SeedReport:
    """Outcome of one :func:`seed` call.

    ``inserted`` and ``candidates`` are read-only mappings keyed by table name, in write order:
    the rows actually inserted and the rows offered. A re-run reports ``inserted`` all zero.
    ``run_id`` is the ``terminal_ingestion_runs.id`` of this run.
    """

    run_id: uuid.UUID
    inserted: Mapping[str, int]
    candidates: Mapping[str, int]

    @property
    def total_inserted(self) -> int:
        """Data rows inserted across every table (the run record is not counted)."""
        return sum(self.inserted.values())


async def current_schema_revision(executor: Executor) -> str | None:
    """Return the revision stored in ``terminal_alembic_version``, or ``None``.

    ``None`` means the version table does not exist (the database was never migrated) or holds
    no row. Several rows, which only a corrupted history produces, are returned sorted and joined
    by ``","`` so the value can never equal a head revision. Read-only: on a session this joins
    or begins its transaction and writes nothing.
    """
    exists = await executor.scalar(_VERSION_TABLE_EXISTS, {"name": migrations.VERSION_TABLE})
    if not exists:
        return None
    result = await executor.execute(sa.select(_VERSION_TABLE.c.version_num))
    revisions = sorted(str(value) for value in result.scalars().all())
    if not revisions:
        return None
    return ",".join(revisions)


async def ensure_schema_at_head(executor: Executor) -> str:
    """Return the stored revision when it equals the packaged migration head.

    Raises :class:`SchemaNotAtHeadError`, whose message includes :data:`SCHEMA_HINT`, when the
    version table is missing, empty or at another revision. The seeder calls this before it
    writes anything, including its run record: an unmigrated database has no
    ``terminal_ingestion_runs`` table to record into.
    """
    expected = migrations.head_revision()
    found = await current_schema_revision(executor)
    if found is None or found != expected:
        raise SchemaNotAtHeadError(found, expected)
    return found


class _TableWriter:
    """Writes the seed table by table, each in natural-key order with ``ON CONFLICT DO NOTHING``,
    and tallies the counts."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.inserted: dict[str, int] = {}
        self.candidates: dict[str, int] = {}

    async def write(
        self, model: _SeedModel, key: tuple[str, ...], rows: Sequence[Mapping[str, object]]
    ) -> None:
        """Insert ``rows`` into ``model``'s table in natural-key order, in batches of
        :data:`SEED_BATCH_SIZE`.

        The complete input is de-duplicated by the natural ``key``, the last occurrence winning,
        and sorted by it before it is sliced, so each batch covers a key range above the previous
        one's and the whole table is written in one key order, whatever order the generator
        produced (prices come per counter, SCOM first). ``upsert_rows`` sorts each call the same
        way, so concurrent writers such as a CSV import of the same table take their row locks in
        that one order: when a seed and an import insert the same new keys, the later one waits
        for the earlier instead of both deadlocking. Batches hold the caller's own mappings, never
        copies.

        Existing rows with the same natural ``key`` are left untouched, so only new rows count
        as inserted; ``candidates`` counts every row offered, duplicates included. Nothing is
        committed or begun here: every batch joins the caller's seed transaction.

        Raises:
            ValueError: If a row lacks a ``key`` column or holds ``None`` in one, before any batch
                is written. Such a key can be neither ordered nor matched to the conflict target.
        """
        table = model.__tablename__
        by_key: dict[tuple[object, ...], Mapping[str, object]] = {}
        for index, row in enumerate(rows):
            for name in key:
                if row.get(name) is None:
                    reason = "is None" if name in row else "is missing"
                    raise ValueError(f"{table} row {index}: natural-key column {name!r} {reason}")
            by_key[tuple(row[name] for name in key)] = row
        ordered = [by_key[row_key] for row_key in sorted(by_key)]
        inserted = 0
        for start in range(0, len(ordered), SEED_BATCH_SIZE):
            batch = ordered[start : start + SEED_BATCH_SIZE]
            inserted += await upsert_rows(self._session, model, batch, key, "do_nothing")
        self.inserted[table] = inserted
        self.candidates[table] = len(rows)


def _row_with_id(record: SeedRecord, **extra: object) -> dict[str, object]:
    """Return ``record``'s column mapping with a new UUID ``id`` and ``extra`` columns.

    The seeder allocates each row's ``uuid4`` id explicitly here, when it builds the record and
    before the rows are batched, so every row offered to ``upsert_rows`` is complete. On a conflict
    the stored row keeps its own id.
    """
    row = synthetic.to_row(record)
    row["id"] = uuid.uuid4()
    row.update(extra)
    return row


async def _seeded_watchlist_id(session: AsyncSession, owner_key: str, name: str) -> uuid.UUID:
    """Return the id of the watchlist ``(owner_key, name)``: the new row, or the existing one
    when the insert conflicted."""
    watchlist_id = await session.scalar(
        sa.select(TerminalWatchlist.id).where(
            TerminalWatchlist.owner_key == owner_key, TerminalWatchlist.name == name
        )
    )
    if watchlist_id is None:
        raise RuntimeError(f"seeded watchlist {name!r} for owner {owner_key!r} is missing")
    return watchlist_id


async def _write_dataset(
    session: AsyncSession, dataset: SeedDataset, *, owner_key: str, run_id: uuid.UUID
) -> _TableWriter:
    """Insert every seed record in foreign-key order and return the tallies.

    Natural keys: equities ``ticker``; prices ``(ticker, trade_date)``; fundamentals ``(ticker,
    period_end_date, period_type)``; macro ``observation_date``; bonds ``(observation_date,
    bond_code)``; cycles ``event_date``; IPO scenarios ``company_name``; watchlists ``(owner_key,
    name)``; watchlist items ``(watchlist_id, ticker)``.
    """
    writer = _TableWriter(session)
    await writer.write(TerminalEquity, ("ticker",), [_row_with_id(row) for row in dataset.equities])
    await writer.write(
        TerminalDailyPrice,
        ("ticker", "trade_date"),
        [_row_with_id(row, ingestion_run_id=run_id) for row in dataset.prices],
    )
    await writer.write(
        TerminalFundamental,
        ("ticker", "period_end_date", "period_type"),
        [_row_with_id(row) for row in dataset.fundamentals],
    )
    await writer.write(
        TerminalMacroIndicator,
        ("observation_date",),
        [_row_with_id(row) for row in dataset.macro],
    )
    await writer.write(
        TerminalBondYield,
        ("observation_date", "bond_code"),
        [_row_with_id(row) for row in dataset.bonds],
    )
    await writer.write(
        TerminalCycle, ("event_date",), [_row_with_id(row) for row in dataset.cycles]
    )
    await writer.write(
        TerminalIpoScenario,
        ("company_name",),
        [_row_with_id(row) for row in dataset.ipo_scenarios],
    )

    name = dataset.watchlist.name
    await writer.write(
        TerminalWatchlist,
        ("owner_key", "name"),
        [{"id": uuid.uuid4(), "owner_key": owner_key, "name": name}],
    )
    watchlist_id = await _seeded_watchlist_id(session, owner_key, name)
    await writer.write(
        TerminalWatchlistItem,
        ("watchlist_id", "ticker"),
        [{"watchlist_id": watchlist_id, "ticker": ticker} for ticker in dataset.watchlist.tickers],
    )
    return writer


def _data_through(dataset: SeedDataset) -> str | None:
    """ISO date of the last seeded price session, recorded in the run's ``details``."""
    last: date | None = max((row.trade_date for row in dataset.prices), default=None)
    return last.isoformat() if last is not None else None


async def _record_failure(
    session: AsyncSession, *, run_id: uuid.UUID, started_at: datetime, error_message: str
) -> None:
    """Roll back the seed transaction and record the run as ``failed`` in a fresh transaction.

    ``error_message`` is the failed run's exception type name only, or ``"cancelled"``.
    SQLAlchemy errors embed the failed statement's parameters, such as the watchlist's
    ``default_owner_key``, so their text is never stored or logged. Best effort: an error here is
    logged with the run id and its type name, then suppressed, so the caller re-raises the
    original. The session stays usable after a cancellation that interrupted one of its
    statements: SQLAlchemy discards that connection, and the rollback and the insert run on a
    fresh one from the pool.
    """
    try:
        await session.rollback()
        session.add(
            TerminalIngestionRun(
                id=run_id,
                pipeline=SEED_PIPELINE,
                trigger=SEED_TRIGGER,
                source_url=None,
                started_at=started_at,
                finished_at=datetime.now(UTC),
                status="failed",
                rows_fetched=0,
                rows_written=0,
                rows_rejected=0,
                error_message=error_message,
                details={},
            )
        )
        await session.commit()
    except Exception as record_exc:
        logger.error(
            "could not record seed run %s as failed: %s", run_id, type(record_exc).__name__
        )


async def _record_cancellation(
    session: AsyncSession, *, run_id: uuid.UUID, started_at: datetime
) -> None:
    """Record a cancelled seed run as ``failed`` with ``"cancelled"`` (:func:`_record_failure`).

    The recording runs in a task of its own to completion, however often the calling task is
    cancelled meanwhile (a repeated Ctrl-C or SIGTERM), so the run row is never lost midway; the
    caller re-raises the cancellation it is handling once this returns.
    """
    recording = asyncio.create_task(
        _record_failure(
            session, run_id=run_id, started_at=started_at, error_message=_CANCELLED_MESSAGE
        )
    )
    while not recording.done():
        try:
            # asyncio.wait neither cancels the task it waits for nor raises its exception.
            await asyncio.wait((recording,))
        except asyncio.CancelledError:
            # The seed is already being cancelled; the caller re-raises once this is recorded.
            continue
    # Retrieved, so asyncio never reports an outcome as unretrieved; _record_failure suppresses
    # every error of its own.
    recording.result()


async def seed(
    session: AsyncSession,
    *,
    settings: TerminalSettings | None = None,
    dataset: SeedDataset | None = None,
) -> SeedReport:
    """Seed the ``terminal_`` tables on ``session`` and commit; return the per-table counts.

    ``settings`` defaults to ``TerminalSettings()`` and supplies the watchlist owner
    (``default_owner_key``). ``dataset`` defaults to :func:`synthetic.generate_all`, whose
    invariant checks (including each planted volume spike's Z-score of at least 4.00) run before
    anything is written.

    Steps: :func:`ensure_schema_at_head` (raises :class:`SchemaNotAtHeadError`, nothing written);
    the run's id and start time; the dataset generated, when none is given; a
    ``terminal_ingestion_runs`` row with status ``running``; every table inserted with
    ``ON CONFLICT DO NOTHING`` on its natural key, so existing rows, seeded or real, are never
    changed; the run row finished as ``succeeded`` with the counts in ``details``; then
    ``session.commit()``. The seeder owns this transaction, so the session should hold no other
    pending work.

    On any failure after the schema check, generation and its invariant checks included, the
    transaction is rolled back, the run is recorded as ``failed`` in a fresh transaction (best
    effort, with the exception's type name as ``error_message``) and the exception is re-raised.
    ``asyncio.CancelledError`` (Ctrl-C, or SIGTERM through ``opennse-terminal seed`` and
    :func:`main`) is handled the same way, with ``"cancelled"`` as ``error_message``: that
    recording runs to completion even if the task is cancelled again, one WARNING ``seed run
    <run_id> was cancelled; its uncommitted changes were rolled back`` names the run, and the
    cancellation is re-raised. A cancellation that arrives once the commit has reached the server
    leaves that run's ``succeeded`` row in place; recording the failure then conflicts with it
    and is logged at ERROR by type.

    Every failure it raises is logged exactly once at ERROR on ``opennse_terminal.seed``, with no
    traceback, and then re-raised unchanged, so callers receive the exception itself and need not
    log it again. A :class:`SchemaNotAtHeadError` is logged as its message, which names only
    revisions and ends with :data:`SCHEMA_HINT`; any other failure before the run starts (the
    schema check's query, or reading the default settings) as ``seeding failed before the run
    started: <type>``; a failure once the run has started as ``seed run <run_id> failed and was
    rolled back: <type>``, naming the failed ``terminal_ingestion_runs`` row. Only type names are
    logged because exception text can quote SQL parameters such as ``default_owner_key``.
    """
    # The cheap precondition comes first, so an unmigrated database, which has no
    # terminal_ingestion_runs table to record a failure in, fails fast. With no run started there
    # is no run id to name, but the failure is still reported here, once.
    try:
        settings = settings if settings is not None else TerminalSettings()
        await ensure_schema_at_head(session)
    except SchemaNotAtHeadError as exc:
        logger.error("%s", exc)
        raise
    except Exception as exc:
        # Type name only: a settings ValidationError quotes the invalid values, the database URL
        # among them, and a database error its statement's parameters.
        logger.error("seeding failed before the run started: %s", type(exc).__name__)
        raise

    run_id = uuid.uuid4()
    started_at = datetime.now(UTC)
    try:
        # Generation, with its invariant checks, runs inside the recorded operation, so its
        # failure leaves a failed run row; it still precedes the run row and every data write.
        dataset = dataset if dataset is not None else synthetic.generate_all()
        run = TerminalIngestionRun(
            id=run_id,
            pipeline=SEED_PIPELINE,
            trigger=SEED_TRIGGER,
            source_url=None,
            started_at=started_at,
            status="running",
            details={},
        )
        session.add(run)
        await session.flush()

        writer = await _write_dataset(
            session, dataset, owner_key=settings.default_owner_key, run_id=run_id
        )
        inserted = dict(writer.inserted)
        candidates = dict(writer.candidates)
        run.status = "succeeded"
        run.finished_at = datetime.now(UTC)
        run.rows_fetched = sum(candidates.values())
        run.rows_written = sum(inserted.values())
        run.rows_rejected = 0
        run.details = {
            "inserted": inserted,
            "candidates": candidates,
            "data_through": _data_through(dataset),
            "calendar": _SEED_CALENDAR,
        }
        await session.flush()
        await session.commit()
    except asyncio.CancelledError:
        # Ctrl-C (asyncio.run) or SIGTERM (cli.run_terminable) cancelled the seed. Unless the
        # commit had reached the server, the running row was part of the rolled-back transaction,
        # so the run is recorded afresh; a committed succeeded row stays, and recording over it
        # conflicts and is logged at ERROR. Either way the cancellation then propagates.
        await _record_cancellation(session, run_id=run_id, started_at=started_at)
        logger.warning(
            "seed run %s was cancelled; its uncommitted changes were rolled back", run_id
        )
        raise
    except Exception as exc:
        await _record_failure(
            session, run_id=run_id, started_at=started_at, error_message=type(exc).__name__
        )
        # The one record of this failure: callers, run_seed included, do not log it again. Only
        # the type name is logged: the exception's text, and so its traceback, can carry SQL
        # parameters such as default_owner_key. The caller receives the exception itself.
        logger.error("seed run %s failed and was rolled back: %s", run_id, type(exc).__name__)
        raise

    logger.info(
        "seed run %s succeeded: %d of %d candidate rows inserted (%s)",
        run_id,
        sum(inserted.values()),
        sum(candidates.values()),
        ", ".join(f"{table}={inserted[table]}/{candidates[table]}" for table in candidates),
    )
    return SeedReport(
        run_id=run_id,
        inserted=MappingProxyType(inserted),
        candidates=MappingProxyType(candidates),
    )


async def _check_connection(engine: AsyncEngine) -> None:
    """Open and release one connection of ``engine``; whatever connecting raises propagates.

    SQLAlchemy does not wrap asyncpg's connect errors (``OSError`` subclasses such as
    ``ConnectionRefusedError`` and ``socket.gaierror``, ``TimeoutError``, and asyncpg's own
    server errors), so they look like any statement failure. Connecting once before the first
    statement separates a database that cannot be reached from a seed that fails. The pool keeps
    the connection for the seed session that follows.
    """
    async with engine.connect():
        pass


def _connect_failure_reason(exc: Exception) -> str:
    """Return the reason logged for a failed connection attempt.

    An ``OSError`` (a refused connection, an unresolvable host, a missing socket file, or asyncio's
    ``Multiple exceptions: ...`` for a host with several addresses) carries only the OS reason and
    the address, so its text is used; an empty ``TimeoutError``, asyncpg's connect timeout, reads
    ``timed out``. Anything else, asyncpg's server errors such as ``InvalidPasswordError`` and
    ``InvalidCatalogNameError`` included, is named by type only: the server's message can quote
    the user name.
    """
    if isinstance(exc, OSError):
        text = str(exc)
        if text:
            return text
        if isinstance(exc, TimeoutError):
            return "timed out"
    return type(exc).__name__


async def run_seed(settings: TerminalSettings, *, url_source: str = "settings.database_url") -> int:
    """Seed the database of ``settings`` on an engine of its own and return an exit code.

    ``url_source`` names where ``settings.database_url`` came from, for the messages about the URL
    itself; :func:`main` passes ``--database-url`` or ``TERMINAL_DATABASE_URL``. The INFO line
    ``seeding <label>`` names the database through :func:`database_log_label` only. Nothing is
    printed: every outcome below is reported exactly once, as a record on the
    ``opennse_terminal.seed`` logger, so a host's logging configuration routes it. No record
    carries a traceback.

    * ``0``: seeded, a re-run that inserts nothing included; :func:`seed` logs the summary.
    * ``2``: the schema is not at head. :func:`seed` logs the :class:`SchemaNotAtHeadError`
      message, which ends with :data:`SCHEMA_HINT`, at ERROR.
    * ``1``, logged at ERROR here, before :func:`seed` runs: an invalid URL, as ``cannot seed:
      <build_engine's message> (from <url_source>)``, a message that quotes the URL only through
      its label; any other error creating the engine (such as SQLAlchemy's ``ArgumentError`` for
      a multi-host URL without every port), as ``cannot seed: could not create a database engine
      from <url_source> (<type>)``, by type only because its text can quote parts of the URL; a
      database that cannot be reached, as ``cannot seed: could not connect to <label>: <reason>``
      (see :func:`_connect_failure_reason`).
    * ``1``, logged at ERROR by :func:`seed`: a failure once connected, which :func:`seed`
      reports itself, with the failed run's id once the run has started; no second record is
      added here.
    * ``1``, logged at ERROR here: a failure outside :func:`seed`, such as closing its session, as
      ``seeding failed: <type>``.

    Failures once connected are named by type only, never by their text or traceback, which can
    carry SQL parameters such as ``default_owner_key``.

    The engine is disposed whenever it was created. ``asyncio.CancelledError`` and
    ``KeyboardInterrupt`` are not caught: they propagate after that disposal, with a started run
    as :func:`seed` left it: ``failed`` ("cancelled") when the cancellation came before its
    commit, otherwise ``succeeded`` with its data committed. A cancellation after :func:`seed`
    returned, while its session closes or the engine is disposed, leaves that committed run as it
    is and records nothing more. A coroutine, so code that already runs an event loop (such as
    test fixtures) awaits it.
    """
    try:
        engine = build_engine(settings)
    except ValueError as exc:
        # build_engine's own messages quote the URL only through database_log_label.
        logger.error("cannot seed: %s (from %s)", exc, url_source)
        return _EXIT_FAILED
    except Exception as exc:
        # Other construction errors are reported by type only: their text can quote parts of the
        # URL, such as the ports of a multi-host query.
        logger.error(
            "cannot seed: could not create a database engine from %s (%s)",
            url_source,
            type(exc).__name__,
        )
        return _EXIT_FAILED
    try:
        label = database_log_label(engine.url)
        logger.info("seeding %s", label)
        try:
            await _check_connection(engine)
        except Exception as exc:
            logger.error(
                "cannot seed: could not connect to %s: %s", label, _connect_failure_reason(exc)
            )
            return _EXIT_FAILED
        async with build_session_factory(engine)() as session:
            try:
                await seed(session, settings=settings)
            except SchemaNotAtHeadError:
                # seed() has logged the message, SCHEMA_HINT included.
                return _EXIT_SCHEMA_NOT_AT_HEAD
            except Exception:
                # seed() has logged this failure once, with the failed run's id once the run had
                # started; a second record here would count one failure twice.
                return _EXIT_FAILED
    except Exception as exc:
        # Only a failure outside seed(), such as closing its session, reaches this handler. Type
        # name only, with no traceback: SQLAlchemy errors embed the statement's parameters, such
        # as default_owner_key.
        logger.error("seeding failed: %s", type(exc).__name__)
        return _EXIT_FAILED
    finally:
        await engine.dispose()
    return _EXIT_OK


class _SeedArgumentParser(argparse.ArgumentParser):
    """An ``ArgumentParser`` whose usage errors exit 1 instead of argparse's 2.

    The seeder's exit code 2 means the schema is not at head, so a mistyped option or a missing
    value must not read as "run the migrations first".
    """

    def error(self, message: str) -> NoReturn:
        """Print the usage and ``<prog>: error: <message>`` to stderr, then exit with 1."""
        self.print_usage(sys.stderr)
        self.exit(_EXIT_FAILED, f"{self.prog}: error: {message}\n")


def build_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    """Return the seeder's argument parser; ``opennse-terminal seed`` forwards its argv here.

    ``prog`` is the command shown in the usage line, the help and the error messages. It defaults
    to ``python -m opennse_terminal.seed.seed_terminal_db``; ``opennse-terminal seed`` passes its
    own. The help ends with the URL's source, the migrate-first precondition, examples and the
    exit codes. A usage error exits 1, never 2 (see :class:`_SeedArgumentParser`).
    """
    parser = _SeedArgumentParser(
        prog=prog if prog is not None else _DEFAULT_PROG,
        description="Seed the terminal_ tables with deterministic fallback data.",
        epilog=_HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database-url",
        metavar="URL",
        default=None,
        help=(
            "database to seed; overrides TERMINAL_DATABASE_URL. "
            "Must be a postgresql+asyncpg:// URL."
        ),
    )
    return parser


def main(argv: list[str] | None = None, *, prog: str | None = None) -> int:
    """Parse ``argv`` (default ``sys.argv[1:]``), seed, and return the exit code.

    ``0`` seeded, and after ``-h``/``--help``; ``1`` failed and rolled back, or invalid arguments
    or settings; ``2`` the schema is not at head; ``130`` interrupted by Ctrl-C and ``143``
    terminated by SIGTERM, each reported in one line on stderr rather than a traceback; a started
    run is then ``failed`` ("cancelled") when the cancellation came before its commit, and
    otherwise stays ``succeeded`` with its data committed (see :func:`seed`). It never raises
    ``SystemExit``: argparse's exits for ``--help`` and usage errors become the returned code, so
    ``opennse-terminal seed`` and other in-process callers always receive an int. ``prog`` is
    passed to :func:`build_parser`.

    Runs :func:`run_seed` through ``opennse_terminal.cli.run_terminable`` (``asyncio.run``, with
    SIGTERM cancelling the seed as Ctrl-C does when called from the main thread with SIGTERM's
    default disposition), so it must be called outside a running event loop; async callers
    await :func:`run_seed` or :func:`seed` instead, or call this through ``asyncio.to_thread``.
    The CLI module is imported only here, so importing the seeder as a library never imports it.
    Invalid settings (for example a non-asyncpg ``--database-url``) print the validation
    message, which never repeats the URL, and return 1.
    """
    try:
        args = build_parser(prog=prog).parse_args(argv)
    except SystemExit as exc:
        # argparse has already printed the help (code 0) or the usage error (code 1, see
        # _SeedArgumentParser); None is sys.exit()'s success.
        if exc.code is None:
            return _EXIT_OK
        return exc.code if isinstance(exc.code, int) else _EXIT_FAILED
    database_url: str | None = args.database_url
    url_source = _URL_SOURCE_FLAG if database_url is not None else _URL_SOURCE_ENV
    try:
        settings = (
            TerminalSettings(database_url=database_url)
            if database_url is not None
            else TerminalSettings()
        )
    except ValidationError as exc:
        print(f"invalid terminal settings: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    # The CLI owns the console scripts' signal handling as it owns their logging; imported on
    # this call only, so importing the seeder as a library never imports the CLI.
    from opennse_terminal.cli import TerminatedError, run_terminable

    try:
        return run_terminable(run_seed(settings, url_source=url_source))
    except KeyboardInterrupt:
        # asyncio.run cancels the seed on Ctrl-C and then raises KeyboardInterrupt. The seed
        # transaction commits only at the very end, so nothing partial is kept: seed() has
        # recorded the run failed ("cancelled") unless its commit had already reached the server,
        # which leaves the run succeeded with all its data.
        print(_INTERRUPTED_MESSAGE, file=sys.stderr)
        return _EXIT_INTERRUPTED
    except TerminatedError:
        # run_terminable cancelled the seed on SIGTERM, exactly as Ctrl-C does.
        print(_TERMINATED_MESSAGE, file=sys.stderr)
        return _EXIT_TERMINATED


if __name__ == "__main__":
    # The CLI owns logging configuration. It is imported only on this entry and in main(), so
    # importing the seeder as a library neither imports the CLI nor adds a handler.
    from opennse_terminal.cli import configure_logging

    configure_logging()
    raise SystemExit(main())
