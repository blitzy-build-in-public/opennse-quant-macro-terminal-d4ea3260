"""PostgreSQL write helpers for the ``terminal_`` tables: precedence-aware, provenance-tracking
upserts.

The NSE market-statistics pipeline, the CBK pipelines, the CSV/Excel importer, PDF filing commits
and the fallback seeder all write through this module, using SQLAlchemy 2.0 async Core
(``INSERT ... ON CONFLICT`` and ``UPDATE ... FROM (VALUES ...)``) on the ORM tables.

Contract shared by every public function:

1. **Executor.** The first argument is an ``AsyncSession`` or an ``AsyncConnection``. A session
   contributes ``await session.connection()``, which joins the session's transaction.
2. **The caller owns the transaction.** Nothing here calls ``begin()``, ``commit()`` or
   ``rollback()``. Ingestion writes inside the run's single data transaction, which holds the
   transaction-scoped advisory lock; the filings service commits the filing and fundamentals
   upserts together or rolls both back; the seeder commits its own session. Database errors (FK,
   CHECK, NOT NULL) propagate unchanged.
3. **Rows** are mappings keyed by column name and are validated before any SQL is built. A key
   that is not a column, or not one this writer accepts, raises ``ValueError``; so does a missing
   required key. A ``float`` value raises ``TypeError``: monetary and ratio values are ``Decimal``,
   counts are ``int``. Nothing is rounded here; callers quantize to the column scale.
4. **Return value.** The number of rows inserted or updated, counted from ``RETURNING`` the
   conflict-key columns. Rows skipped by ``DO NOTHING``, by the price precedence guard or by the
   fundamentals currency guard are not counted. ``update_flows`` returns a ``FlowUpdateResult``.
   Callers that need a row's ``id`` select it by natural key afterwards, in the same transaction.
5. **Batching.** Rows are validated, de-duplicated by conflict key with the last occurrence
   winning (one ``INSERT ... ON CONFLICT DO UPDATE`` cannot affect a row twice, so callers order
   rows with the preferred value last) and sorted by key, once per call. Statements then follow
   that key order, so concurrent writers lock rows in one order: each statement covers a key range
   above the previous one's, and ``upsert_rows`` writes rows of different column sets as
   contiguous same-column runs of the sorted rows. A statement's rows are normalized to one column
   set only as its chunk is built, at most 1,000 rows and 32,000 bind parameters (asyncpg's limit
   is 32,767), so a call holds references to the caller's rows plus one chunk of new rows; the
   caller's rows are never copied wholesale or mutated.
6. **Identity and timestamps.** A UUID ``id`` is generated for each row that lacks one and is
   never updated. ``ON CONFLICT DO UPDATE`` does not apply a column's ``onupdate``, so updates set
   ``updated_at`` (``ingested_at`` for prices) to ``now()`` explicitly. ``created_at`` is never
   updated.
7. **An update sets only the columns the writer supplied.** ``EXCLUDED`` of an absent column holds
   that column's default (``currency = 'KES'``, or NULL), so setting it would overwrite stored
   data.
8. **Bound parameters only.** Every statement is a Core construct; no SQL text is built from
   values.

This module imports ``opennse_terminal.models``, whose modules import only ``db.base``, and
``db/__init__.py`` does not re-export it, so importing it is acyclic.
"""

import logging
import uuid
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import chain, groupby, islice
from typing import Any, Final, Literal, TypeAlias, TypeVar, cast

from sqlalchemy import (
    ColumnElement,
    Date,
    Executable,
    Numeric,
    String,
    Table,
    case,
    column,
    func,
    update,
    values,
)
from sqlalchemy.dialects.postgresql import JSONB, Insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from opennse_terminal.config import DEFAULT_CURRENCY
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.models import TerminalDailyPrice, TerminalFundamental, TerminalMacroIndicator
from opennse_terminal.models.fundamentals import FUNDAMENTAL_LINE_ITEM_COLUMNS
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "FLOW_COLUMN",
    "FUNDAMENTAL_VALUE_COLUMNS",
    "MACRO_INDICATOR_COLUMNS",
    "PRICE_COLUMNS",
    "PRICE_SOURCE_PRECEDENCE",
    "Executor",
    "FlowUpdateResult",
    "UpsertMode",
    "update_flows",
    "upsert_fundamentals",
    "upsert_macro",
    "upsert_prices",
    "upsert_rows",
]

logger = logging.getLogger("opennse_terminal.db.upsert")

Executor: TypeAlias = AsyncSession | AsyncConnection
"""What every write helper accepts: a session (its transaction is joined) or a connection."""

UpsertMode: TypeAlias = Literal["do_nothing", "do_update"]
"""Conflict behaviour of ``upsert_rows``."""

PRICE_SOURCE_PRECEDENCE: Final[tuple[str, ...]] = ("seed", "nse_market_statistics", "csv_import")
"""Writers of the ``terminal_daily_prices`` price columns, lowest rank first. ``csv_import``, the
highest rank, overwrites a row whatever its stored ``source``, including a value outside this
tuple; a lower writer overwrites only rows whose ``source`` is listed at or below its own rank."""

PRICE_COLUMNS: Final[tuple[str, ...]] = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "turnover_kes",
    "turnover_estimated",
)
"""The price columns of ``terminal_daily_prices``, whose provenance is the row's ``source``."""

FLOW_COLUMN: Final[str] = "foreign_net_flow_kes"
"""The flow column of ``terminal_daily_prices``, whose provenance is ``flow_source``."""

# One source of truth with the model: the importer and the models module read the same tuple.
FUNDAMENTAL_VALUE_COLUMNS: Final[tuple[str, ...]] = FUNDAMENTAL_LINE_ITEM_COLUMNS
"""The value columns of ``terminal_fundamentals`` that are COALESCE-merged and whose provenance is
recorded in ``source_by_field``, in table order."""

_Row: TypeAlias = dict[str, object]
_InputRow: TypeAlias = Mapping[str, object]
_Key: TypeAlias = tuple[Any, ...]
_T = TypeVar("_T")

_MAX_ROWS_PER_STATEMENT: Final[int] = 1_000
_MAX_BIND_PARAMS: Final[int] = 32_000
_NEVER_UPDATED: Final[frozenset[str]] = frozenset({"id", "created_at"})
_FLOW_SOURCE_COLUMN: Final[str] = "flow_source"
_FLOW_WRITER: Final[str] = "csv_import"
_PRICE_KEY: Final[tuple[str, ...]] = ("ticker", "trade_date")
_PRICE_REQUIRED: Final[tuple[str, ...]] = (*_PRICE_KEY, "close_price", "volume", "turnover_kes")
_FLOW_ROW_COLUMNS: Final[tuple[str, ...]] = (*_PRICE_KEY, FLOW_COLUMN)
_MACRO_KEY: Final[tuple[str, ...]] = ("observation_date",)
_FUNDAMENTALS_KEY: Final[tuple[str, ...]] = ("ticker", "period_end_date", "period_type")
# Matches the server and ORM default of terminal_fundamentals.period_type.
_DEFAULT_PERIOD_TYPE: Final[str] = "FY"


@dataclass(frozen=True)
class FlowUpdateResult:
    """Outcome of ``update_flows``.

    ``rows_updated`` counts the ``terminal_daily_prices`` rows whose flow was set.
    ``missing_keys`` lists, sorted, the requested ``(ticker, trade_date)`` keys that have no price
    row, so the importer can reject them.
    """

    rows_updated: int
    missing_keys: tuple[tuple[str, date], ...]


def _table(model: type[TerminalBase]) -> Table:
    """Return the ``Table`` behind ``model``; ``__table__`` is typed as a ``FromClause``."""
    table = model.__table__
    if not isinstance(table, Table):
        raise TypeError(f"{model.__name__}.__table__ is not a Table")
    return table


def _require_columns(model: type[TerminalBase], names: Collection[str]) -> None:
    """Fail at import time when a column constant here no longer matches its model."""
    table = _table(model)
    absent = [name for name in names if name not in table.c]
    if absent:
        raise RuntimeError(f"db.upsert names columns absent from {table.name}: {absent}")


_require_columns(
    TerminalDailyPrice,
    (
        *_PRICE_KEY,
        *PRICE_COLUMNS,
        FLOW_COLUMN,
        _FLOW_SOURCE_COLUMN,
        "source",
        "ingestion_run_id",
        "ingested_at",
    ),
)
_require_columns(TerminalMacroIndicator, (*_MACRO_KEY, *MACRO_INDICATOR_COLUMNS, "source_by_field"))
_require_columns(
    TerminalFundamental,
    (*_FUNDAMENTALS_KEY, *FUNDAMENTAL_VALUE_COLUMNS, "currency", "filing_id", "source_by_field"),
)


async def _connection(executor: Executor) -> AsyncConnection:
    """Return the connection to write on; a session's connection joins its transaction."""
    if isinstance(executor, AsyncSession):
        return await executor.connection()
    return executor


def _validate_rows(
    table: Table,
    rows: Sequence[_InputRow],
    *,
    required: Collection[str],
    allowed: Collection[str] | None,
) -> None:
    """Check every row in place; nothing is copied or changed.

    ``allowed`` restricts the accepted keys to a subset of the table's columns; ``None`` accepts
    every column. Raises ``ValueError`` for an unknown, unaccepted or missing key and
    ``TypeError`` for a ``float`` value (``bool`` and ``int`` are fine).
    """
    accepted = frozenset(table.c.keys()) if allowed is None else frozenset(allowed)
    for index, row in enumerate(rows):
        for name, value in row.items():
            if name not in table.c:
                raise ValueError(f"{table.name} row {index}: {name!r} is not a column")
            if name not in accepted:
                raise ValueError(f"{table.name} row {index}: column {name!r} is not accepted here")
            if isinstance(value, float):
                raise TypeError(
                    f"{table.name} row {index}: column {name!r} holds a float; pass a Decimal"
                )
        missing = [name for name in required if name not in row]
        if missing:
            raise ValueError(f"{table.name} row {index}: missing required columns {missing}")


def _dedupe_sorted(
    rows: Sequence[_InputRow],
    key: Sequence[str],
    key_of: Callable[[_InputRow], _Key] | None = None,
) -> tuple[list[_InputRow], int]:
    """Keep the last row per conflict key, sorted by key; also return how many were dropped.

    The result holds the caller's mappings themselves, never copies. ``key_of`` returns a row's
    key values in ``key`` order, for a writer that defaults a key column without changing the
    row; by default they are the row's own values. A ``None`` key value raises ``ValueError``: it
    can never match a conflict target.
    """
    by_key: dict[_Key, _InputRow] = {}
    for row in rows:
        row_key = key_of(row) if key_of is not None else tuple(row[name] for name in key)
        for name, value in zip(key, row_key, strict=True):
            if value is None:
                raise ValueError(f"conflict-key column {name!r} is None")
        by_key[row_key] = row
    unique = [by_key[row_key] for row_key in sorted(by_key)]
    return unique, len(rows) - len(unique)


def _generates_ids(table: Table) -> bool:
    """True where the table's primary key is ``id``: each written row then gets a new UUID ``id``
    unless it supplies one, set on the statement's own copy of the row.

    ORM-side defaults are not relied on in multi-row Core inserts.
    """
    return "id" in table.c and bool(table.c["id"].primary_key)


def _params_per_row(table: Table, columns: Collection[str]) -> int:
    """Bind parameters one row adds to a multi-row INSERT.

    SQLAlchemy renders a per-row parameter for every absent column with a Python-side default,
    so those count as well.
    """
    defaulted = sum(
        1 for col in table.columns if col.key not in columns and col.default is not None
    )
    return len(columns) + defaulted


def _chunks(rows: Iterable[_T], n_columns: int) -> Iterator[list[_T]]:
    """Yield lists of at most 1,000 rows and 32,000 bind parameters each, drawing ``rows`` one
    chunk at a time, so a lazy ``rows`` is never materialized beyond the current chunk.

    ``itertools.batched`` would do, but it needs Python 3.12 and the package supports 3.11.
    """
    size = max(1, min(_MAX_ROWS_PER_STATEMENT, _MAX_BIND_PARAMS // max(1, n_columns)))
    iterator = iter(rows)
    while chunk := list(islice(iterator, size)):
        yield chunk


async def _count(conn: AsyncConnection, stmt: Executable) -> int:
    """Execute a ``RETURNING`` statement and return how many rows it affected."""
    result = await conn.execute(stmt)
    return len(result.all())


async def _insert_all(
    executor: Executor,
    table: Table,
    rows: Iterable[_Row],
    key: Sequence[str],
    on_conflict: Callable[[Insert], Insert],
) -> int:
    """Insert ``rows`` (normalized rows sharing one column tuple, in key order) chunk by chunk,
    applying ``on_conflict`` to each statement, and return the number of rows ``RETURNING``
    reports.

    ``rows`` is consumed lazily, one chunk at a time, so a generator that normalizes the caller's
    rows holds only the current chunk's new rows. The first row's columns size the chunks.
    ``on_conflict`` receives the multi-row ``INSERT`` and returns it with its ``ON CONFLICT``
    clause; it reads ``EXCLUDED`` from the statement it is given.
    """
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return 0
    columns = tuple(first)
    key_columns = [table.c[name] for name in key]
    conn = await _connection(executor)
    written = 0
    for chunk in _chunks(chain((first,), iterator), _params_per_row(table, columns)):
        stmt = on_conflict(pg_insert(table).values(chunk))
        written += await _count(conn, stmt.returning(*key_columns))
    return written


def _normalized_rows(
    rows: Iterable[_InputRow], columns: Sequence[str], *, with_ids: bool
) -> Iterator[_Row]:
    """Yield each row as a new dict of exactly ``columns``, in that order.

    With ``with_ids``, an ``id`` the row lacks or holds as ``None`` is a new UUID. The input rows
    are only read.
    """
    for row in rows:
        out: _Row = {name: row.get(name) for name in columns}
        if with_ids and out.get("id") is None:
            out["id"] = uuid.uuid4()
        yield out


def _provenance_rows(
    table: Table,
    rows: Iterable[_InputRow],
    *,
    carried: Sequence[str],
    written: Sequence[str],
    source: str,
    defaults: Mapping[str, object] | None = None,
    extra: Mapping[str, object] | None = None,
) -> Iterator[_Row]:
    """Yield COALESCE-merge rows normalized to one column set, with per-row provenance.

    Each output row is a new dict holding the ``carried`` columns; each ``defaults`` column, with
    the row's value or, where the row lacks it or holds ``None``, the default; every ``written``
    column (``None`` where the row did not supply a value); ``source_by_field``, which maps
    exactly the row's non-null written columns to ``source``; the ``extra`` columns; and a new
    UUID ``id`` where the table's primary key is ``id``. Rows whose written values are all
    ``None`` would change nothing and are skipped. The input rows are only read.
    """
    with_ids = _generates_ids(table)
    for row in rows:
        supplied = {name: row.get(name) for name in written}
        if all(value is None for value in supplied.values()):
            continue
        out: _Row = {name: row[name] for name in carried}
        for name, default in (defaults or {}).items():
            value = row.get(name)
            out[name] = default if value is None else value
        out.update(supplied)
        out["source_by_field"] = {
            name: source for name, value in supplied.items() if value is not None
        }
        if extra:
            out.update(extra)
        if with_ids:
            out["id"] = uuid.uuid4()
        yield out


def _generic_conflict(
    table: Table, key: Sequence[str], update_names: Sequence[str]
) -> Callable[[Insert], Insert]:
    """Build ``upsert_rows``'s ``ON CONFLICT`` clause.

    With no ``update_names`` (``do_nothing`` mode, or a run carrying only key columns) the
    clause is ``DO NOTHING``; otherwise ``DO UPDATE`` sets those columns from ``EXCLUDED``, plus
    ``updated_at = now()`` where the table has it and the rows do not supply it.
    """

    def apply(stmt: Insert) -> Insert:
        if not update_names:
            return stmt.on_conflict_do_nothing(index_elements=list(key))
        set_: dict[str, ColumnElement[Any]] = {name: stmt.excluded[name] for name in update_names}
        if "updated_at" in table.c and "updated_at" not in set_:
            set_["updated_at"] = func.now()
        return stmt.on_conflict_do_update(index_elements=list(key), set_=set_)

    return apply


def _merge_set(table: Table, stmt: Insert, written: Sequence[str]) -> dict[str, ColumnElement[Any]]:
    """``SET`` list of the COALESCE merges: each written column keeps its stored value unless the
    new one is non-null, provenance is merged key by key, and ``updated_at`` is refreshed."""
    excluded = stmt.excluded
    set_: dict[str, ColumnElement[Any]] = {
        name: func.coalesce(excluded[name], table.c[name]) for name in written
    }
    set_["source_by_field"] = table.c["source_by_field"].op("||", return_type=JSONB())(
        excluded["source_by_field"]
    )
    set_["updated_at"] = func.now()
    return set_


def _require_source(source: str, writer: str) -> None:
    if not source:
        raise ValueError(f"{writer}: source must be a non-empty string")


def _fundamentals_key(row: _InputRow) -> _Key:
    """A fundamentals row's conflict key, with an absent or ``None`` ``period_type`` read as
    ``"FY"``, the value its written copy carries."""
    period_type = row.get("period_type")
    return (
        row["ticker"],
        row["period_end_date"],
        _DEFAULT_PERIOD_TYPE if period_type is None else period_type,
    )


async def upsert_rows(
    executor: Executor,
    model: type[TerminalBase],
    rows: Sequence[Mapping[str, object]],
    key: Sequence[str],
    mode: UpsertMode,
) -> int:
    """Generic upsert of ``rows`` into ``model``'s table on the conflict key ``key``.

    Conflict key: the column list ``key`` (column-list inference, never a constraint name), which
    every row must contain with a non-null value. Rows may carry any column of the table.

    Columns written: on insert, the columns each row supplies; columns a row omits take their
    defaults, and a UUID ``id`` is generated when absent. Rows are validated, de-duplicated and
    sorted by key once; the sorted rows are then written as contiguous runs that share a column
    set (a generated ``id`` counts as supplied), each run by its own statements in key order, so
    callers need not normalize rows and every statement covers a key range above the previous
    one's: concurrent callers lock rows in one order whatever their column sets. Each statement's
    rows are copied only as its chunk is built; the caller's rows are never mutated.

    Precedence:

    - ``"do_nothing"``: an existing row is never touched. The seeder writes every table this way,
      so a re-seed inserts no rows and never alters scraped or imported data.
    - ``"do_update"``: an existing row takes every column its run supplies except the key,
      ``id`` and ``created_at``, and ``updated_at = now()`` where the table has that column and the
      rows do not supply it. Unsupplied columns keep their stored values. There is no
      change-detection predicate: re-writing identical values counts the row. Used for
      ``terminal_bond_yields`` on ``(observation_date, bond_code)``, ``terminal_equities`` on
      ``(ticker,)`` and ``terminal_corporate_filings`` on ``(file_sha256,)``. A run that carries
      only key columns has nothing to update and falls back to ``DO NOTHING``.

    Works for composite-key tables without ``id``, such as ``terminal_watchlist_items`` on
    ``(watchlist_id, ticker)``. Returns the number of rows inserted or updated.
    """
    table = _table(model)
    key_names = tuple(key)
    if not key_names:
        raise ValueError(f"upsert_rows({table.name}): key must name at least one column")
    absent = [name for name in key_names if name not in table.c]
    if absent:
        raise ValueError(f"upsert_rows({table.name}): key columns {absent} are not columns")
    if mode not in ("do_nothing", "do_update"):
        raise ValueError(f"upsert_rows({table.name}): unknown mode {mode!r}")

    _validate_rows(table, rows, required=key_names, allowed=None)
    unique, duplicates = _dedupe_sorted(rows, key_names)
    with_ids = _generates_ids(table)

    def column_set(row: _InputRow) -> frozenset[str]:
        # A generated id is written like a supplied one, so it belongs to the row's column set.
        return frozenset(row) | {"id"} if with_ids else frozenset(row)

    written = 0
    # Contiguous runs in key order, never a partition by column set: statements must follow the
    # global key order, or two callers with mixed column sets can lock rows in opposite orders.
    for names, run in groupby(unique, key=column_set):
        # One column order per run: a multi-row VALUES binds every row by the first row's keys.
        ordered = [col.key for col in table.columns if col.key in names]
        update_names = tuple(
            name for name in ordered if name not in key_names and name not in _NEVER_UPDATED
        )
        on_conflict = _generic_conflict(
            table, key_names, update_names if mode == "do_update" else ()
        )
        normalized = _normalized_rows(run, ordered, with_ids=with_ids)
        written += await _insert_all(executor, table, normalized, key_names, on_conflict)

    logger.debug(
        "upsert_rows %s (%s): %d rows in, %d duplicates dropped, %d written",
        table.name,
        mode,
        len(rows),
        duplicates,
        written,
    )
    return written


async def upsert_prices(
    executor: Executor,
    rows: Sequence[Mapping[str, object]],
    *,
    source: str,
    ingestion_run_id: uuid.UUID | None,
) -> int:
    """Write daily sessions into ``terminal_daily_prices`` with source precedence.

    Conflict key: ``(ticker, trade_date)``. Callers write only tickers present in
    ``terminal_equities`` (the NSE pipeline skips unmapped ISINs; the importer rejects unknown
    tickers), because an unknown ticker violates the foreign key.

    Rows carry ``ticker``, ``trade_date``, ``close_price``, ``volume`` and ``turnover_kes``, and
    optionally ``open_price``, ``high_price``, ``low_price`` and ``turnover_estimated``.
    ``foreign_net_flow_kes`` is accepted only from ``source="csv_import"``: the NSE pipeline never
    writes flows.

    Columns written: every price column (``PRICE_COLUMNS``), with absent OHLC as NULL and an absent
    ``turnover_estimated`` as false, because the row's ``source`` describes all price columns
    together: a scrape replacing a seeded row leaves OHLC NULL rather than seeded OHLC beside an
    NSE close. Also ``source``, ``ingestion_run_id`` and, on update, ``ingested_at = now()``.

    Flow columns:

    - ``csv_import``: a non-null ``foreign_net_flow_kes`` is written with
      ``flow_source = 'csv_import'``; a null one keeps the stored flow and ``flow_source``
      (COALESCE and a matching CASE, which keeps the CHECK that ``flow_source`` is non-null exactly
      when the flow is).
    - Any other writer: the flow columns are not written, so a new row has both NULL and an existing
      row keeps its flow and ``flow_source``.

    Precedence: ``source`` must be in ``PRICE_SOURCE_PRECEDENCE``. ``csv_import``, the highest
    rank, replaces an existing row whatever its stored ``source``, including a value outside that
    tuple, so its ``ON CONFLICT DO UPDATE`` has no ``WHERE``. A lower writer updates an existing
    row only when its stored ``source`` is listed at or below the writer's rank
    (``WHERE source IN (...)``): ``nse_market_statistics`` replaces ``seed`` and
    ``nse_market_statistics`` rows but never ``csv_import`` rows or a source the tuple does not
    list, and ``seed`` replaces only ``seed`` rows. Guarded-out rows are not counted. Identical
    re-runs rewrite identical values and are counted. Returns the number of rows inserted or
    updated.
    """
    if source not in PRICE_SOURCE_PRECEDENCE:
        raise ValueError(
            f"upsert_prices: source {source!r} is not one of {list(PRICE_SOURCE_PRECEDENCE)}"
        )
    table = _table(TerminalDailyPrice)
    writes_flows = source == _FLOW_WRITER
    accepted = (*_PRICE_KEY, *PRICE_COLUMNS, *((FLOW_COLUMN,) if writes_flows else ()))

    _validate_rows(table, rows, required=_PRICE_REQUIRED, allowed=accepted)
    unique, duplicates = _dedupe_sorted(rows, _PRICE_KEY)

    def normalized() -> Iterator[_Row]:
        # Built lazily, so only the chunk being written exists as new dicts.
        for row in unique:
            out: _Row = {
                "id": uuid.uuid4(),
                "ticker": row["ticker"],
                "trade_date": row["trade_date"],
            }
            out.update({name: row.get(name) for name in PRICE_COLUMNS})
            if out["turnover_estimated"] is None:
                out["turnover_estimated"] = False
            out["source"] = source
            out["ingestion_run_id"] = ingestion_run_id
            if writes_flows:
                flow = row.get(FLOW_COLUMN)
                out[FLOW_COLUMN] = flow
                out[_FLOW_SOURCE_COLUMN] = source if flow is not None else None
            yield out

    rank = PRICE_SOURCE_PRECEDENCE.index(source)
    # The top-ranked writer replaces a row whatever its stored source, including one outside the
    # tuple; a lower writer updates only rows whose source is listed at or below its rank.
    guard = (
        None
        if rank == len(PRICE_SOURCE_PRECEDENCE) - 1
        else table.c["source"].in_(PRICE_SOURCE_PRECEDENCE[: rank + 1])
    )

    def on_conflict(stmt: Insert) -> Insert:
        excluded = stmt.excluded
        set_: dict[str, ColumnElement[Any]] = {name: excluded[name] for name in PRICE_COLUMNS}
        set_["source"] = excluded["source"]
        set_["ingestion_run_id"] = excluded["ingestion_run_id"]
        set_["ingested_at"] = func.now()
        if writes_flows:
            set_[FLOW_COLUMN] = func.coalesce(excluded[FLOW_COLUMN], table.c[FLOW_COLUMN])
            set_[_FLOW_SOURCE_COLUMN] = case(
                (excluded[FLOW_COLUMN].is_not(None), excluded[_FLOW_SOURCE_COLUMN]),
                else_=table.c[_FLOW_SOURCE_COLUMN],
            )
        return stmt.on_conflict_do_update(
            index_elements=list(_PRICE_KEY),
            set_=set_,
            where=guard,
        )

    written = await _insert_all(executor, table, normalized(), _PRICE_KEY, on_conflict)
    logger.debug(
        "upsert_prices %s (%s): %d rows in, %d duplicates dropped, %d written",
        table.name,
        source,
        len(rows),
        duplicates,
        written,
    )
    return written


async def update_flows(
    executor: Executor,
    rows: Sequence[Mapping[str, object]],
    *,
    flow_source: str,
) -> FlowUpdateResult:
    """Set foreign net flows on existing ``terminal_daily_prices`` rows (the ``foreign_flows``
    import).

    Key: ``(ticker, trade_date)``. Each row carries exactly ``ticker`` (``str``), ``trade_date``
    (``date``) and a non-null ``foreign_net_flow_kes`` (``Decimal``, whole KES); a null flow
    raises ``ValueError`` because it would break the CHECK pairing it with ``flow_source``.

    Columns written: ``foreign_net_flow_kes`` and ``flow_source`` only. ``source``, the price
    columns, ``ingestion_run_id`` and ``ingested_at`` are never touched, and no row is inserted.

    Precedence: a flow writer always overwrites the stored flow; flows have no rank. One
    ``UPDATE ... FROM (VALUES ...)`` runs per chunk of up to 1,000 rows. Returns the updated count
    and the requested keys that have no price row.
    """
    _require_source(flow_source, "update_flows")
    table = _table(TerminalDailyPrice)
    _validate_rows(table, rows, required=_FLOW_ROW_COLUMNS, allowed=_FLOW_ROW_COLUMNS)
    for index, row in enumerate(rows):
        ticker, trade_date = row["ticker"], row["trade_date"]
        if not isinstance(ticker, str) or not isinstance(trade_date, date):
            raise TypeError(f"update_flows row {index}: ticker must be str and trade_date a date")
        if row[FLOW_COLUMN] is None:
            raise ValueError(f"update_flows row {index}: {FLOW_COLUMN} must not be None")
    unique, duplicates = _dedupe_sorted(rows, _PRICE_KEY)

    # The unique rows are the requested keys, sorted; each chunk's keys its UPDATE does not return
    # have no price row. Chunks hold disjoint keys, so the per-chunk counts add up.
    rows_updated = 0
    missing: list[tuple[str, date]] = []
    if unique:
        conn = await _connection(executor)
        for chunk in _chunks(unique, len(_FLOW_ROW_COLUMNS)):
            # Typed columns make the driver bind DATE and NUMERIC, matching the table's columns.
            source_rows = values(
                column("ticker", String(16)),
                column("trade_date", Date()),
                column(FLOW_COLUMN, Numeric(24, 2)),
                name="v",
            ).data([(row["ticker"], row["trade_date"], row[FLOW_COLUMN]) for row in chunk])
            stmt = (
                update(table)
                .where(
                    table.c["ticker"] == source_rows.c["ticker"],
                    table.c["trade_date"] == source_rows.c["trade_date"],
                )
                .values(
                    {
                        FLOW_COLUMN: source_rows.c[FLOW_COLUMN],
                        _FLOW_SOURCE_COLUMN: flow_source,
                    }
                )
                .returning(table.c["ticker"], table.c["trade_date"])
            )
            result = await conn.execute(stmt)
            returned = {(ticker, trade_date) for ticker, trade_date in result.all()}
            rows_updated += len(returned)
            for row in chunk:
                # The loop above checked that ticker is a str and trade_date a date.
                row_key = cast(tuple[str, date], (row["ticker"], row["trade_date"]))
                if row_key not in returned:
                    missing.append(row_key)

    outcome = FlowUpdateResult(rows_updated=rows_updated, missing_keys=tuple(missing))
    logger.debug(
        "update_flows %s (%s): %d rows in, %d duplicates dropped, %d written, %d without price row",
        table.name,
        flow_source,
        len(rows),
        duplicates,
        outcome.rows_updated,
        len(missing),
    )
    return outcome


async def upsert_macro(
    executor: Executor,
    rows: Sequence[Mapping[str, object]],
    *,
    source: str,
) -> int:
    """COALESCE-merge macro indicators into ``terminal_macro_indicators``.

    Conflict key: ``observation_date`` (required). Writers are the CBK pipelines (``source`` is
    the pipeline name, such as ``"cbk_rates_csv"``) and the ``macro_indicators`` import
    (``"csv_import"``).

    Columns written: the written set W is every ``MACRO_INDICATOR_COLUMNS`` name that any row
    supplies; each row is normalized to W, with ``None`` where it has no value. Rows whose W values
    are all ``None`` change nothing and are dropped uncounted. ``source_by_field`` of each row maps
    exactly its non-null W columns to ``source``.

    Precedence: on conflict each W column becomes ``COALESCE(EXCLUDED.c, stored c)``, so a value is
    replaced only by a non-null one and never nulled, and columns outside W are untouched.
    ``source_by_field`` is merged with ``||``, so only the written columns change provenance (for
    example from ``"seed"`` to ``"cbk_inflation"``); ``updated_at = now()``. When several rows
    share a date the last one wins, so ``cbk_reserves`` passes bulletins oldest first and the
    newest bulletin's value for a header date is kept. Returns the number of rows inserted or
    updated.
    """
    _require_source(source, "upsert_macro")
    table = _table(TerminalMacroIndicator)
    _validate_rows(
        table, rows, required=_MACRO_KEY, allowed=(*_MACRO_KEY, *MACRO_INDICATOR_COLUMNS)
    )
    written_names = tuple(
        name for name in MACRO_INDICATOR_COLUMNS if any(name in row for row in rows)
    )
    unique, duplicates = _dedupe_sorted(rows, _MACRO_KEY)
    normalized = _provenance_rows(
        table, unique, carried=_MACRO_KEY, written=written_names, source=source
    )

    def on_conflict(stmt: Insert) -> Insert:
        return stmt.on_conflict_do_update(
            index_elements=list(_MACRO_KEY),
            set_=_merge_set(table, stmt, written_names),
        )

    written = await _insert_all(executor, table, normalized, _MACRO_KEY, on_conflict)
    logger.debug(
        "upsert_macro %s (%s): %d rows in, %d duplicates dropped, %d written",
        table.name,
        source,
        len(rows),
        duplicates,
        written,
    )
    return written


async def upsert_fundamentals(
    executor: Executor,
    rows: Sequence[Mapping[str, object]],
    *,
    source: str,
    filing_id: uuid.UUID | None = None,
) -> int:
    """COALESCE-merge reported line items into ``terminal_fundamentals``.

    Conflict key: ``(ticker, period_end_date, period_type)``. ``ticker`` and ``period_end_date``
    are required; an absent or ``None`` ``period_type`` becomes ``"FY"`` and an absent or ``None``
    ``currency`` becomes ``DEFAULT_CURRENCY`` (KES), so the key and the currency compared below
    are always explicit. Writers are PDF commits (``source="pdf_filing"``, with ``filing_id``) and
    the ``fundamentals`` import (``"csv_import"``); callers pass only committable values (the
    filings service drops ``ambiguous`` items).

    Columns written: the written set W is every ``FUNDAMENTAL_VALUE_COLUMNS`` name any row
    supplies; rows whose W values are all ``None`` are dropped uncounted. ``source_by_field`` of
    each row maps exactly its non-null W columns to ``source``. ``filing_id``, when given, is
    written on every row. ``currency`` is written on insert only.

    Precedence: on conflict each W column becomes ``COALESCE(EXCLUDED.c, stored c)``, so existing
    values are never nulled; ``source_by_field`` is merged with ``||``, so a partial PDF commit into
    a seeded row re-labels only the columns it wrote and every other column stays ``"seed"``;
    ``filing_id`` is replaced only when given; ``updated_at = now()``. A row whose currency differs
    from the stored row's is skipped and not counted, so values are never merged across
    currencies; callers report that case as ``currency_conflict`` before calling. Returns the
    number of rows inserted or updated.
    """
    _require_source(source, "upsert_fundamentals")
    table = _table(TerminalFundamental)
    _validate_rows(
        table,
        rows,
        required=("ticker", "period_end_date"),
        allowed=(*_FUNDAMENTALS_KEY, "currency", *FUNDAMENTAL_VALUE_COLUMNS),
    )
    written_names = tuple(
        name for name in FUNDAMENTAL_VALUE_COLUMNS if any(name in row for row in rows)
    )
    # The defaults are applied to the key and to each written copy, never to the caller's rows.
    unique, duplicates = _dedupe_sorted(rows, _FUNDAMENTALS_KEY, key_of=_fundamentals_key)
    normalized = _provenance_rows(
        table,
        unique,
        carried=("ticker", "period_end_date"),
        written=written_names,
        source=source,
        defaults={"period_type": _DEFAULT_PERIOD_TYPE, "currency": DEFAULT_CURRENCY},
        extra={"filing_id": filing_id} if filing_id is not None else None,
    )

    def on_conflict(stmt: Insert) -> Insert:
        set_ = _merge_set(table, stmt, written_names)
        if filing_id is not None:
            set_["filing_id"] = stmt.excluded["filing_id"]
        return stmt.on_conflict_do_update(
            index_elements=list(_FUNDAMENTALS_KEY),
            set_=set_,
            where=table.c["currency"] == stmt.excluded["currency"],
        )

    written = await _insert_all(executor, table, normalized, _FUNDAMENTALS_KEY, on_conflict)
    logger.debug(
        "upsert_fundamentals %s (%s): %d rows in, %d duplicates dropped, %d written",
        table.name,
        source,
        len(rows),
        duplicates,
        written,
    )
    return written
