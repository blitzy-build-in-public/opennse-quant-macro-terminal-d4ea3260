"""Idempotent fallback seeder: fills the ``terminal_`` tables with deterministic Kenyan market data.

Run it as ``python -m opennse_terminal.seed.seed_terminal_db [--database-url URL]`` or
``opennse-terminal seed [--database-url URL]``; both call :func:`main`. The database comes from
``TERMINAL_DATABASE_URL`` (or ``.env``), and ``--database-url`` overrides it. Exit codes: ``0``
seeded (including a re-run that inserts nothing), ``1`` failed and rolled back, ``2`` the schema
is not at the packaged migration head ("run `opennse-terminal migrate upgrade head` first").

What is written. The records of :func:`opennse_terminal.seed.synthetic.generate_all`: the eight
attachment counters, their synthetic daily prices and fundamentals, the macro and bond history,
the four election cycles, the Quickmart IPO preset, and the "NSE Core 8" watchlist owned by
``TerminalSettings.default_owner_key``. Only ``terminal_`` tables are touched, through the ORM
models and :func:`opennse_terminal.db.upsert.upsert_rows`; no user or auth row is created.

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

The same ``DELETE`` applies to the other row-level-source tables (``terminal_equities``,
``terminal_bond_yields``, ``terminal_cycles``, ``terminal_ipo_scenarios``), and the same per-column
``UPDATE`` to ``terminal_fundamentals``.
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
from typing import Final, TypeAlias

import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from opennse_terminal import migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine
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
"""Rows per ``upsert_rows`` call; keeps each price statement far below asyncpg's 32,767
bind-parameter limit."""

SCHEMA_HINT: Final = "run `opennse-terminal migrate upgrade head` first"
"""Operator instruction carried by :class:`SchemaNotAtHeadError` and printed on exit code 2."""

_EXIT_OK: Final = 0
_EXIT_FAILED: Final = 1
_EXIT_SCHEMA_NOT_AT_HEAD: Final = 2
_MAX_ERROR_MESSAGE_CHARS: Final = 2000
_SEED_CALENDAR: Final = "synthetic"

# to_regclass returns NULL instead of raising for a missing relation, so an unmigrated database
# is detected without aborting the transaction. The table name is a bound parameter.
_VERSION_TABLE_EXISTS: Final = sa.text("SELECT to_regclass(:name) IS NOT NULL")
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
    """Writes the seed table by table with ``ON CONFLICT DO NOTHING`` and tallies the counts."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.inserted: dict[str, int] = {}
        self.candidates: dict[str, int] = {}

    async def write(
        self, model: _SeedModel, key: tuple[str, ...], rows: Sequence[Mapping[str, object]]
    ) -> None:
        """Insert ``rows`` into ``model``'s table in batches of :data:`SEED_BATCH_SIZE`.

        Existing rows with the same natural ``key`` are left untouched, so only new rows count
        as inserted.
        """
        inserted = 0
        for start in range(0, len(rows), SEED_BATCH_SIZE):
            batch = rows[start : start + SEED_BATCH_SIZE]
            inserted += await upsert_rows(self._session, model, batch, key, "do_nothing")
        table = model.__tablename__
        self.inserted[table] = inserted
        self.candidates[table] = len(rows)


def _row_with_id(record: SeedRecord, **extra: object) -> dict[str, object]:
    """Return ``record``'s column mapping with a new UUID ``id`` and ``extra`` columns.

    The id is set here rather than left to the model's Python-side default, which multi-row Core
    inserts do not apply; on a conflict the stored row keeps its own id.
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
    session: AsyncSession, *, run_id: uuid.UUID, started_at: datetime, exc: Exception
) -> None:
    """Roll back the seed transaction and record the run as ``failed`` in a fresh transaction.

    Best effort: an error here is logged and suppressed, so the caller re-raises the original.
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
                error_message=f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_MESSAGE_CHARS],
                details={},
            )
        )
        await session.commit()
    except Exception:
        logger.exception("could not record seed run %s as failed", run_id)


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
    a ``terminal_ingestion_runs`` row with status ``running``; every table inserted with
    ``ON CONFLICT DO NOTHING`` on its natural key, so existing rows, seeded or real, are never
    changed; the run row finished as ``succeeded`` with the counts in ``details``; then
    ``session.commit()``. The seeder owns this transaction, so the session should hold no other
    pending work.

    On any later failure the transaction is rolled back, the run is recorded as ``failed`` in a
    fresh transaction (best effort) and the exception is re-raised.
    """
    settings = settings if settings is not None else TerminalSettings()
    # The cheap precondition comes before the generation, so an unmigrated database fails fast.
    await ensure_schema_at_head(session)
    dataset = dataset if dataset is not None else synthetic.generate_all()

    run_id = uuid.uuid4()
    started_at = datetime.now(UTC)
    try:
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
    except Exception as exc:
        await _record_failure(session, run_id=run_id, started_at=started_at, exc=exc)
        # The traceback travels with the re-raised exception; run_seed logs it once.
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


async def run_seed(settings: TerminalSettings) -> int:
    """Seed the database of ``settings`` on an engine of its own and return an exit code.

    ``0`` seeded (a re-run that inserts nothing included); ``2`` the schema is not at head, with
    the :class:`SchemaNotAtHeadError` message, which contains :data:`SCHEMA_HINT`, printed to
    stderr; ``1`` any other failure, logged with its traceback. The engine is always disposed.
    A coroutine, so code that already runs an event loop (such as test fixtures) awaits it.
    """
    try:
        engine = build_engine(settings)
    except ValueError as exc:
        # build_engine masks the password in this message.
        print(str(exc), file=sys.stderr)
        logger.error("cannot seed: %s", exc)
        return _EXIT_FAILED
    logger.info("seeding %s", engine.url.render_as_string(hide_password=True))
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            await seed(session, settings=settings)
    except SchemaNotAtHeadError as exc:
        print(str(exc), file=sys.stderr)
        logger.error("%s", exc)
        return _EXIT_SCHEMA_NOT_AT_HEAD
    except Exception:
        logger.exception("seeding failed")
        return _EXIT_FAILED
    finally:
        await engine.dispose()
    return _EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    """Return the seeder's argument parser; ``opennse-terminal seed`` forwards its argv here."""
    parser = argparse.ArgumentParser(
        description="Seed the terminal_ tables with deterministic fallback data."
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


def main(argv: list[str] | None = None) -> int:
    """Parse ``argv`` (default ``sys.argv[1:]``), seed, and return the exit code (0, 1 or 2).

    Runs :func:`run_seed` with ``asyncio.run``, so it must be called outside a running event
    loop; async callers await :func:`run_seed` or :func:`seed` instead, or call this through
    ``asyncio.to_thread``. Invalid settings (for example a non-asyncpg ``--database-url``) print
    the validation message, which never repeats the URL, and return 1.
    """
    args = build_parser().parse_args(argv)
    database_url: str | None = args.database_url
    try:
        settings = (
            TerminalSettings(database_url=database_url)
            if database_url is not None
            else TerminalSettings()
        )
    except ValidationError as exc:
        print(f"invalid terminal settings: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    return asyncio.run(run_seed(settings))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    raise SystemExit(main())
