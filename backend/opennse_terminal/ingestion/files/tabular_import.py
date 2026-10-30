"""CSV/Excel importer: operator-supplied files into the ``terminal_`` tables.

``opennse-terminal import <kind> <path> [--sheet NAME]`` calls :func:`import_file`, which runs
:class:`TabularImportPipeline` through the shared framework (``ingestion.base``): one data
transaction holding the advisory lock ``terminal:csv_import:<kind>``, the shared row thresholds
and a ``terminal_ingestion_runs`` row with ``pipeline = 'csv_import:<kind>'``. There is no HTTP
endpoint; this importer is how price histories and other operator data are backfilled.

Kinds, conflict keys and targets:

====================  ====================================  ===================================
Kind                  Conflict key                          Target and precedence
====================  ====================================  ===================================
``equities``          ``ticker``                            ``terminal_equities``, every column
                                                            the file supplies; others kept
``daily_prices``      ``(ticker, trade_date)``              ``terminal_daily_prices``; price
                                                            columns overwrite any source
                                                            (``source = 'csv_import'``); a
                                                            non-empty flow cell sets the flow
                                                            and ``flow_source = 'csv_import'``
``fundamentals``      ``(ticker, period_end_date,           ``terminal_fundamentals``; COALESCE
                      period_type)``                        merge, ``source_by_field`` entries
                                                            ``csv_import``; never across
                                                            currencies
``macro_indicators``  ``observation_date``                  ``terminal_macro_indicators``;
                                                            COALESCE merge, ``source_by_field``
``foreign_flows``     ``(ticker, trade_date)``              flow and ``flow_source`` of
                                                            existing price rows only
====================  ====================================  ===================================

Value rules (Rule 4): pandas reads every cell as text (``dtype=str``) and each cell goes straight
from ``str`` to ``Decimal``, ``int`` or ``date``, never through ``float``. Thousands separators are
stripped, parentheses mean negative, and an empty cell (or ``-``, ``—``, ``–``, ``n/a``) is null.
Dates are ``YYYY-MM-DD`` or ``DD/MM/YYYY``. Decimals are quantized ``ROUND_HALF_UP`` to the target
column's scale. A cell holding a NUL character is rejected whole (``invalid_character:<column>``),
never read up to the NUL, and recorded text (headers, a rejected row's raw cells) shows each NUL
as U+2400, because PostgreSQL stores no U+0000 in the run row. Units: ``*_kes`` whole KES (never
thousands), ``*_pct`` percentage points, ``fx_kes_usd`` KES per USD, ``fx_reserves_usd_m`` USD
millions, share counts and volumes integers in 0..``MAX_SHARE_COUNT``. Currency defaults to KES.

Column types, scales, digit limits and text lengths are read from the ORM tables, so the
pre-write checks mirror the database exactly and a bad row is rejected rather than aborting the
whole transaction with a constraint violation.

pandas is imported inside the reader only: ``cli.py`` imports :data:`IMPORT_KINDS` for its
argparse choices, and that import must stay cheap.
"""

import asyncio
import io
import logging
import os
import posixpath
import re
import stat
import uuid
import zipfile
import zlib
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import (
    MAX_EMAX,
    MAX_PREC,
    MIN_EMIN,
    ROUND_HALF_UP,
    Context,
    Decimal,
    Inexact,
    InvalidOperation,
    Overflow,
    Rounded,
)
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Final, Literal, TypeGuard, TypeVar, get_args, overload

# ``column`` and ``values`` are aliased: this module's loop variables named ``column`` would
# shadow the bare names (F402).
from sqlalchemy import Date, FromClause, Integer, Numeric, String, Table, Values, and_, select
from sqlalchemy import column as sql_column
from sqlalchemy import values as sql_values
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.types import TypeEngine

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT, TerminalSettings
from opennse_terminal.db.session import build_engine
from opennse_terminal.db.upsert import (
    FLOW_COLUMN,
    PRICE_COLUMNS,
    update_flows,
    upsert_fundamentals,
    upsert_macro,
    upsert_prices,
    upsert_rows,
)
from opennse_terminal.ingestion.base import (
    FetchFailed,
    IngestionError,
    Pipeline,
    RejectedRow,
    RunContext,
    RunResult,
    RunTrigger,
    SourceLayoutChanged,
    ValidationOutcome,
    execute_pipeline,
    normalize_header,
    parse_decimal_text,
)
from opennse_terminal.models import (
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalMacroIndicator,
)
from opennse_terminal.models.fundamentals import FUNDAMENTAL_LINE_ITEM_COLUMNS, PERIOD_TYPES
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "COLUMN_INTEGER_DIGITS",
    "COLUMN_SCALES",
    "IMPORT_KINDS",
    "KIND_SPECS",
    "MAX_FILE_BYTES",
    "MAX_ROWS",
    "MAX_XLSX_EXPANDED_BYTES",
    "MAX_XLSX_MEMBERS",
    "MAX_XLSX_PARTS_BYTES",
    "MAX_XLSX_WORKSHEET_BYTES",
    "SUPPORTED_SUFFIXES",
    "CellValue",
    "ImportKind",
    "KindSpec",
    "TabularImportPipeline",
    "TabularRecord",
    "import_file",
    "normalize_column_name",
    "parse_date_cell",
    "parse_decimal_cell",
    "parse_int_cell",
]

logger = logging.getLogger("opennse_terminal.ingestion.files.tabular_import")

# ---------------------------------------------------------------------------------------------
# Public declarations
# ---------------------------------------------------------------------------------------------

ImportKind = Literal[
    "equities", "daily_prices", "fundamentals", "macro_indicators", "foreign_flows"
]
"""What an import file holds; selects its columns, conflict key and target table."""

IMPORT_KINDS: Final[tuple[ImportKind, ...]] = (
    "equities",
    "daily_prices",
    "fundamentals",
    "macro_indicators",
    "foreign_flows",
)
"""The import kinds, in CLI order (``opennse-terminal import <kind> ...``)."""

if get_args(ImportKind) != IMPORT_KINDS:
    raise RuntimeError(f"ImportKind {get_args(ImportKind)!r} does not match {IMPORT_KINDS!r}")

MAX_FILE_BYTES: Final = 50 * 1024 * 1024
"""Largest accepted file, in bytes (50 MiB); checked before the file is read."""

MAX_ROWS: Final = 1_000_000
"""Most rows accepted after the header line, blank rows included; a longer file or sheet fails
with ``too_many_rows`` and is never imported in part."""

SUPPORTED_SUFFIXES: Final = (".csv", ".xlsx")
"""File extensions the importer reads (compared case-insensitively)."""

# An .xlsx file is a ZIP archive, so MAX_FILE_BYTES bounds only its compressed size. These limits
# bound what it expands to; they are checked on the archive's central directory, its manifest
# and its workbook relationships before the workbook is opened, every member openpyxl may read
# whole is first inflated in bounded chunks to prove it expands to exactly its declared size,
# and an archive beyond a limit fails the run with its code.

MAX_XLSX_MEMBERS: Final = 1_000
"""Most members an ``.xlsx`` archive may hold; more fails with ``xlsx_too_many_members``."""

MAX_XLSX_PARTS_BYTES: Final = 64 * 1024 * 1024
"""Largest total expanded size, in bytes (64 MiB), of the ``.xlsx`` members that are not streamed
worksheets (see :data:`MAX_XLSX_WORKSHEET_BYTES`); a larger total fails with
``xlsx_parts_too_large``, as does a ``[Content_Types].xml`` larger than 1 MiB, before it is read.

openpyxl parses the manifest, the shared strings, the workbook and its relationships, the
properties, theme and styles, each sheet's relationships and any chartsheet whole when the
workbook opens, before any row limit applies, and the manifest and relationships, not the part
names, decide which member plays which role. So every other member counts toward this one
budget, whatever its name, media and unused parts included. The trade-off: a workbook whose
non-worksheet parts exceed 64 MiB in total is rejected even when its sheet is small."""

MAX_XLSX_WORKSHEET_BYTES: Final = 512 * 1024 * 1024
"""Largest expanded size, in bytes (512 MiB), of one streamed worksheet; a larger one fails with
``xlsx_worksheet_too_large``. openpyxl reads a worksheet as a stream, and a sheet of
:data:`MAX_ROWS` rows is about 400 MB of XML.

A member is a streamed worksheet only when it is named ``xl/worksheets/<name>.xml`` and
``[Content_Types].xml`` declares it with the worksheet content type and no other, in a workbook
whose manifest declares no DOCTYPE and whose workbook relationships part is readable (at most
1 MiB, well-formed, no DOCTYPE) and holds no chartsheet, which openpyxl parses whole. Otherwise
it counts toward :data:`MAX_XLSX_PARTS_BYTES`."""

MAX_XLSX_EXPANDED_BYTES: Final = 1024 * 1024 * 1024
"""Largest total expanded size, in bytes (1 GiB), of every member of an ``.xlsx`` archive; a larger
total fails with ``xlsx_expanded_too_large``."""

CellValue = Decimal | int | date | str | bool | None
"""A converted cell: ``Decimal`` amounts, ``int`` share counts and volumes, ``date``s, trimmed
text, the derived ``turnover_estimated`` flag, or ``None`` for an empty cell."""


@dataclass(frozen=True, slots=True)
class KindSpec:
    """The columns of one import kind.

    Attributes:
        kind: The import kind.
        required: Normalized headers the file must have; their cells must not be empty.
        optional: Normalized headers read when present; an absent column or empty cell is null.
        key: The conflict key; rows sharing it are collapsed, the last occurrence winning.
    """

    kind: ImportKind
    required: tuple[str, ...]
    optional: tuple[str, ...]
    key: tuple[str, ...]

    @property
    def columns(self) -> tuple[str, ...]:
        """Every column the kind reads: the required ones, then the optional ones."""
        return (*self.required, *self.optional)


KIND_SPECS: Final[Mapping[ImportKind, KindSpec]] = MappingProxyType(
    {
        "equities": KindSpec(
            kind="equities",
            required=("ticker", "company_name", "sector", "shares_outstanding"),
            optional=("isin", "free_float_pct", "currency"),
            key=("ticker",),
        ),
        "daily_prices": KindSpec(
            kind="daily_prices",
            required=("ticker", "trade_date", "close_price", "volume"),
            optional=(
                "open_price",
                "high_price",
                "low_price",
                "turnover_kes",
                "foreign_net_flow_kes",
            ),
            key=("ticker", "trade_date"),
        ),
        "fundamentals": KindSpec(
            kind="fundamentals",
            required=("ticker", "period_end_date"),
            # All ten value columns of the table, dps_kes and shares_outstanding included.
            optional=("period_type", "currency", *FUNDAMENTAL_LINE_ITEM_COLUMNS),
            key=("ticker", "period_end_date", "period_type"),
        ),
        "macro_indicators": KindSpec(
            kind="macro_indicators",
            required=("observation_date",),
            optional=MACRO_INDICATOR_COLUMNS,
            key=("observation_date",),
        ),
        "foreign_flows": KindSpec(
            kind="foreign_flows",
            required=("ticker", "trade_date", "foreign_net_flow_kes"),
            optional=(),
            key=("ticker", "trade_date"),
        ),
    }
)
"""Required and optional columns and the conflict key of each kind (read-only)."""


def _table(clause: FromClause) -> Table:
    """Return a model's ``Table``; ``__table__`` is typed as a ``FromClause``."""
    if not isinstance(clause, Table):
        raise TypeError(f"{clause!r} is not a Table")
    return clause


_KIND_TABLES: Final[Mapping[ImportKind, Table]] = MappingProxyType(
    {
        "equities": _table(TerminalEquity.__table__),
        "daily_prices": _table(TerminalDailyPrice.__table__),
        "fundamentals": _table(TerminalFundamental.__table__),
        "macro_indicators": _table(TerminalMacroIndicator.__table__),
        "foreign_flows": _table(TerminalDailyPrice.__table__),
    }
)

_ColumnType = Literal["text", "integer", "decimal", "date"]


@dataclass(frozen=True, slots=True)
class _ColumnRule:
    """How one file column converts, derived from its database column type."""

    type: _ColumnType
    scale: int = 0
    integer_digits: int = 0
    max_length: int | None = None


def _column_rule(table: Table, name: str) -> _ColumnRule:
    """Derive the conversion rule of ``name`` from ``table``'s column type."""
    if name not in table.c:
        raise RuntimeError(f"tabular_import reads {name!r}, which is not a column of {table.name}")
    column_type = table.c[name].type
    if isinstance(column_type, Numeric):
        if column_type.precision is None or column_type.scale is None:
            raise RuntimeError(f"{table.name}.{name} needs an explicit NUMERIC(precision, scale)")
        return _ColumnRule(
            type="decimal",
            scale=column_type.scale,
            integer_digits=column_type.precision - column_type.scale,
        )
    if isinstance(column_type, Integer):
        return _ColumnRule(type="integer")
    if isinstance(column_type, Date):
        return _ColumnRule(type="date")
    if isinstance(column_type, String):
        return _ColumnRule(type="text", max_length=column_type.length)
    raise RuntimeError(f"{table.name}.{name} has an unsupported type {column_type!r}")


def _build_column_rules() -> dict[str, _ColumnRule]:
    """One rule per column name across every kind; a name typed differently in two target
    tables would make one rule wrong for the other, so that fails at import."""
    rules: dict[str, _ColumnRule] = {}
    for kind, spec in KIND_SPECS.items():
        for name in spec.columns:
            rule = _column_rule(_KIND_TABLES[kind], name)
            if rules.setdefault(name, rule) != rule:
                raise RuntimeError(f"column {name!r} has different types across import targets")
    return rules


_COLUMN_RULES: Final[Mapping[str, _ColumnRule]] = MappingProxyType(_build_column_rules())

COLUMN_SCALES: Final[Mapping[str, int]] = MappingProxyType(
    {name: rule.scale for name, rule in _COLUMN_RULES.items() if rule.type == "decimal"}
)
"""Decimal places of every decimal column, from its ``NUMERIC(p, s)`` type; cells are quantized
to it with ``ROUND_HALF_UP`` (prices and ``dps_kes`` 4, KES totals 2, ``*_pct`` 4,
``fx_kes_usd`` 4, ``fx_reserves_usd_m`` 2, ``free_float_pct`` 4)."""

COLUMN_INTEGER_DIGITS: Final[Mapping[str, int]] = MappingProxyType(
    {name: rule.integer_digits for name, rule in _COLUMN_RULES.items() if rule.type == "decimal"}
)
"""Integer digits ``p - s`` of every decimal column; a value with ``|x| >= 10**digits`` (for
example 10^14 for a price, 10^22 for a KES total) overflows the column and is rejected."""

_QUANTA: Final[Mapping[str, Decimal]] = MappingProxyType(
    {name: Decimal(1).scaleb(-scale) for name, scale in COLUMN_SCALES.items()}
)
_LIMITS: Final[Mapping[str, Decimal]] = MappingProxyType(
    {name: Decimal(10) ** digits for name, digits in COLUMN_INTEGER_DIGITS.items()}
)

_PCT_SUFFIX: Final = "_pct"
# period_type is a code like the other three, so "fy" is read as FY rather than rejected.
_UPPERCASE_COLUMNS: Final = frozenset({"ticker", "isin", "currency", "period_type"})
_NULL_TOKENS: Final = frozenset({"", "-", "—", "–", "n/a"})
_NUL: Final = "\x00"
_NUL_EVIDENCE: Final = "\u2400"
"""U+2400 SYMBOL FOR NULL: how a NUL character (U+0000) of the file is shown wherever its text is
recorded, in normalized headers and in a rejected row's raw cells. PostgreSQL stores no U+0000 in
TEXT or JSONB, so the run row's ``error_message`` and ``details`` must never carry one."""
_ISIN_RE: Final = re.compile(r"KE[0-9A-Z]{9}[0-9]")
_CURRENCY_RE: Final = re.compile(r"[A-Z]{3}")
_ISO_DATE_RE: Final = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?: 00:00:00)?")
_DMY_DATE_RE: Final = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
# Package part names compare case-insensitively (ECMA-376 Part 2, part name equivalence).
_XLSX_WORKSHEET_RE: Final = re.compile(r"xl/worksheets/[^/]+\.xml", re.IGNORECASE)
_HUNDRED: Final = Decimal(100)
_DEFAULT_PERIOD_TYPE: Final = "FY"
_SESSION_KINDS: Final[frozenset[ImportKind]] = frozenset({"daily_prices", "foreign_flows"})
"""Kinds whose ``trade_date`` names an observed session, so it may not be after today: a future
row would be read as history and move every market endpoint's default ``as_of``."""
_IN_CHUNK: Final = 5_000
_MISSING_KEYS_SAMPLE: Final = 50

_WRITE_BATCH_ROWS: Final = 1_000
"""Accepted rows per ``db.upsert`` writer call. Only one batch of write rows exists at a time,
and every batch runs on the run's connection, inside its one data transaction."""


@dataclass(frozen=True, slots=True)
class TabularRecord:
    """One data row of an import file, converted.

    Attributes:
        row_number: The 1-based line of the row in the file or sheet; the header is line 1, so
            the first data row is 2. Reported as ``RejectedRow.index``.
        values: Converted cells of the kind's columns present in the file (``None`` for empty
            cells), plus values derived during validation and reconciliation, in that order. The
            records this module builds hold a read-only compact mapping (:class:`_ValuesRow`) that
            equals the ``dict`` of the same items and returns every ``Decimal`` with its exact
            value and exponent.
        errors: Conversion error codes, for example ``invalid_decimal:close_price``,
            ``out_of_range:volume`` or ``invalid_character:ticker`` (a cell holding NUL); a record
            with any is rejected.
        raw: The original cell text of the kind's columns, in file order, recorded for rejected
            rows, with each NUL character shown as U+2400. The records this module builds hold a
            read-only mapping over one string per row (:class:`_RawCells`), never the parsed
            file's own cell strings, so the parsed frame is freed once :meth:`parse` returns.
    """

    row_number: int
    values: Mapping[str, CellValue]
    errors: tuple[str, ...]
    raw: Mapping[str, str]


# ---------------------------------------------------------------------------------------------
# Compact rows
# ---------------------------------------------------------------------------------------------
#
# A file of MAX_ROWS rows becomes as many records, held from parse until the last write. As dicts,
# with every amount a 104-byte Decimal and the raw cells a second dict keeping the parsed frame's
# strings alive, they took about 1.7 KB per row (a 1,000,000-row price file peaked near 1.8 GiB).
# A compact row is one tuple of cells under a layout shared by every row with the same columns:
# a decimal is stored as its exact scaled integer, a value recurring in a column is one shared
# object (_SHARED_VALUES_LIMIT), and the raw cells are one string per row.

_EXACT: Final = Context(
    prec=MAX_PREC,
    Emax=MAX_EMAX,
    Emin=MIN_EMIN,
    traps=[InvalidOperation, Inexact, Rounded, Overflow],
)
"""The context compact rows rescale decimals in: wide enough that ``scaleb`` never rounds, and
trapping any inexact result, so the caller's thread context cannot change a value."""

_SHARED_VALUES_LIMIT: Final = 10_000
"""Most cell texts per column whose converted value one parse keeps for sharing. Conversion is a
pure function of the column and the text, so rows holding the same text in a column (a ticker,
a session date, a price that recurs) share one immutable value, converted once. The table of a
column is emptied when full and refills from the rows that follow, so it stays bounded whatever
the file holds and follows the file's order."""

_ABSENT: Final = object()

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True, eq=False)
class _RowLayout:
    """The columns of compact rows, shared by every row with the same columns.

    Attributes:
        columns: The column names, in mapping order.
        positions: The index of each column in ``columns`` (read-only).
        scales: Per column, its ``COLUMN_SCALES`` entry for a decimal column, else ``None``.
    """

    columns: tuple[str, ...]
    positions: Mapping[str, int]
    scales: tuple[int | None, ...]


def _row_layout(columns: tuple[str, ...]) -> _RowLayout:
    """The layout of ``columns``; a repeated name raises ``ValueError``."""
    positions = {column: index for index, column in enumerate(columns)}
    if len(positions) != len(columns):
        raise ValueError(f"a row layout repeats a column: {columns!r}")
    return _RowLayout(
        columns=columns,
        positions=MappingProxyType(positions),
        scales=tuple(COLUMN_SCALES.get(column) for column in columns),
    )


def _compact_cell(column: str, value: CellValue, scale: int | None) -> CellValue:
    """``value`` as a compact row stores it in ``column``: for a decimal column (``scale`` set), a
    ``Decimal`` whose exponent is ``-scale``, as every value this module quantizes is, becomes the
    exact integer ``value * 10**scale``; any other value is stored as it is.

    A negative zero stays a ``Decimal``, because the integer 0 would read back positive.

    Raises:
        TypeError: A decimal column is given something other than a ``Decimal`` or ``None``.
    """
    if scale is None or value is None:
        return value
    if not isinstance(value, Decimal):
        raise TypeError(f"{column} holds {type(value).__name__}, expected Decimal")
    if value.as_tuple().exponent == -scale and not (value.is_zero() and value.is_signed()):
        return int(value.scaleb(scale, _EXACT))
    return value


def _expanded_cell(cell: CellValue, scale: int | None) -> CellValue:
    """The value of a compact ``cell`` (:func:`_compact_cell`): a decimal column's integer back as
    the identical ``Decimal``, its value and exponent both (``284500`` at scale 4 is
    ``Decimal("28.4500")``); any other cell as it is."""
    if scale is not None and type(cell) is int:
        return Decimal(cell).scaleb(-scale, _EXACT)
    return cell


class _ValuesRow(Mapping[str, CellValue]):
    """A record's converted values: a read-only mapping of its columns, in order, to their values,
    equal to any mapping with the same items, a ``dict`` included.

    The cells are one tuple under a shared :class:`_RowLayout`. A decimal cell is held as a scaled
    integer and read as the identical ``Decimal`` (:func:`_expanded_cell`), whatever the caller's
    ``decimal`` context.

    Raises:
        ValueError: ``cells`` does not hold one cell per column of ``layout``.
    """

    __slots__ = ("_cells", "_layout")

    def __init__(self, layout: _RowLayout, cells: tuple[CellValue, ...]) -> None:
        if len(cells) != len(layout.columns):
            raise ValueError(f"{len(cells)} cells for {len(layout.columns)} columns")
        self._layout = layout
        self._cells = cells

    @property
    def layout(self) -> _RowLayout:
        """The shared layout of the row's columns."""
        return self._layout

    @property
    def cells(self) -> tuple[CellValue, ...]:
        """The compact cells, in column order."""
        return self._cells

    def __getitem__(self, key: str) -> CellValue:
        position = self._layout.positions[key]
        return _expanded_cell(self._cells[position], self._layout.scales[position])

    def __iter__(self) -> Iterator[str]:
        return iter(self._layout.columns)

    def __len__(self) -> int:
        return len(self._cells)

    def __contains__(self, key: object) -> bool:
        return key in self._layout.positions

    @overload
    def get(self, key: str, /) -> CellValue: ...

    @overload
    def get(self, key: str, default: CellValue | _T, /) -> CellValue | _T: ...

    def get(self, key: str, default: object = None, /) -> object:
        """The value of ``key``, or ``default`` when the row has no such column."""
        position = self._layout.positions.get(key)
        if position is None:
            return default
        return _expanded_cell(self._cells[position], self._layout.scales[position])

    def as_dict(self) -> dict[str, CellValue]:
        """A new ``dict`` of the row's items, in column order."""
        layout = self._layout
        # _expanded_cell, inlined: this runs once per row in validate.
        return {
            column: Decimal(cell).scaleb(-scale, _EXACT)
            if scale is not None and type(cell) is int
            else cell
            for column, cell, scale in zip(layout.columns, self._cells, layout.scales, strict=True)
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.as_dict()!r})"


class _RawCells(Mapping[str, str]):
    """A record's raw cells: a read-only mapping of the file's columns of the kind, in file order,
    to their text, each NUL shown as U+2400; equal to any mapping with the same items.

    The texts are held as one string joined by NUL, which none of them can contain, and are split
    only when read, which happens for rejected rows alone.

    Raises:
        ValueError: ``joined`` does not hold exactly one text per column of ``layout``, as a text
            holding NUL would make it.
    """

    __slots__ = ("_joined", "_layout")

    def __init__(self, layout: _RowLayout, joined: str) -> None:
        if joined.count(_NUL) != max(len(layout.columns) - 1, 0):
            raise ValueError(f"raw cells for {len(layout.columns)} columns must not hold NUL")
        self._layout = layout
        self._joined = joined

    def __getitem__(self, key: str) -> str:
        position = self._layout.positions[key]
        return self._joined.split(_NUL)[position]

    def __iter__(self) -> Iterator[str]:
        return iter(self._layout.columns)

    def __len__(self) -> int:
        return len(self._layout.columns)

    def __contains__(self, key: object) -> bool:
        return key in self._layout.positions

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self)!r})"


class _RowLayouts:
    """The layouts of one pipeline's compact rows, one per distinct column tuple, so that rows with
    the same columns share one. A run builds a handful: the file's columns, and those the values
    derived by validation or reconciliation add."""

    __slots__ = ("_extended", "_layouts")

    def __init__(self) -> None:
        self._layouts: dict[tuple[str, ...], _RowLayout] = {}
        # The layout of a row of each layout updated with each sequence of changed columns.
        self._extended: dict[tuple[_RowLayout, tuple[str, ...]], _RowLayout] = {}

    def layout(self, columns: tuple[str, ...]) -> _RowLayout:
        """The shared layout of ``columns``."""
        layout = self._layouts.get(columns)
        if layout is None:
            layout = self._layouts[columns] = _row_layout(columns)
        return layout

    def row(self, values: Mapping[str, CellValue]) -> _ValuesRow:
        """A compact row of ``values``' items, in their order."""
        layout = self.layout(tuple(values))
        return _ValuesRow(
            layout,
            tuple(
                _compact_cell(column, values[column], scale)
                for column, scale in zip(layout.columns, layout.scales, strict=True)
            ),
        )

    def updated(
        self, values: Mapping[str, CellValue], changes: Mapping[str, CellValue]
    ) -> _ValuesRow:
        """A compact row of ``values`` updated with ``changes`` as ``dict.update`` would: a column
        already present keeps its place and a new one is appended, in ``changes`` order. The
        unchanged cells of a compact ``values`` are shared, never converted again."""
        if not isinstance(values, _ValuesRow):
            return self.row({**values, **changes})
        base = values.layout
        key = (base, tuple(changes))
        layout = self._extended.get(key)
        if layout is None:
            added = tuple(column for column in changes if column not in base.positions)
            layout = self._extended[key] = self.layout(base.columns + added)
        cells = list(values.cells)
        cells.extend(None for _ in range(len(layout.columns) - len(cells)))
        positions = layout.positions
        scales = layout.scales
        for column, value in changes.items():
            position = positions[column]
            cells[position] = _compact_cell(column, value, scales[position])
        return _ValuesRow(layout, tuple(cells))


def _plain_values(values: Mapping[str, CellValue]) -> dict[str, CellValue]:
    """A new ``dict`` of ``values``' items, in order."""
    return values.as_dict() if isinstance(values, _ValuesRow) else dict(values)


# ---------------------------------------------------------------------------------------------
# Pure cell helpers
# ---------------------------------------------------------------------------------------------


class _OutOfRangeError(ValueError):
    """A cell that parses but does not fit its column; reported as ``out_of_range:<column>``."""


def normalize_column_name(name: object) -> str:
    """Normalize a header cell to a column name: BOM and zero-width spaces stripped, trimmed,
    lowercased and every run of whitespace replaced by one ``_`` (``"Close Price"`` gives
    ``"close_price"``). A NUL character is shown as U+2400 (``"Close\\x00 Price"`` gives
    ``"close␀_price"``), so a header holding one never matches a column name and its name can be
    recorded."""
    return normalize_header(str(name).replace(_NUL, _NUL_EVIDENCE)).replace(" ", "_")


def _is_null(text: str | None) -> bool:
    """True for ``None``, a blank cell and the no-value markers ``-``, ``—``, ``–`` and ``n/a``,
    which ``parse_decimal_text`` also reads as no number."""
    return text is None or text.strip().lower() in _NULL_TOKENS


def parse_date_cell(text: str | None) -> date | None:
    """Parse a date cell: ``YYYY-MM-DD``, ``DD/MM/YYYY`` (day and month may have one digit) or
    ``YYYY-MM-DD 00:00:00``, the form an Excel date cell takes when read as text.

    Returns ``None`` for an empty cell. Raises ``ValueError`` for any other text, including an
    impossible date such as ``2026-02-30`` or a time other than midnight.
    """
    if text is None or _is_null(text):
        return None
    cleaned = text.strip()
    iso = _ISO_DATE_RE.fullmatch(cleaned)
    if iso is not None:
        year, month, day = (int(part) for part in iso.groups())
        return date(year, month, day)
    dmy = _DMY_DATE_RE.fullmatch(cleaned)
    if dmy is not None:
        day, month, year = (int(part) for part in dmy.groups())
        return date(year, month, day)
    raise ValueError(f"not a YYYY-MM-DD or DD/MM/YYYY date: {cleaned!r}")


def parse_decimal_cell(text: str | None, column: str) -> Decimal | None:
    """Parse an amount, price, rate or percentage cell for ``column`` into an exact ``Decimal``.

    Whitespace and ``,`` thousands separators are stripped and ``(1,234.5)`` reads as
    ``-1234.5`` (:func:`~opennse_terminal.ingestion.base.parse_decimal_text`, never ``float``). A
    trailing ``%`` is accepted on ``*_pct`` columns only; the value stays in percentage points.
    The result is quantized ``ROUND_HALF_UP`` to ``COLUMN_SCALES[column]``.

    Returns ``None`` for an empty cell. Raises ``ValueError`` for text that is not a finite
    number, and for a value whose magnitude reaches ``10 ** COLUMN_INTEGER_DIGITS[column]``,
    before or after rounding, because it would overflow the column. The magnitude checks are
    exact, so an extreme exponent such as ``1e1000000`` or ``(1e1000000)`` is rejected as out of
    range (``_OutOfRangeError``, a ``ValueError``) rather than raising a ``decimal`` signal. An
    unknown ``column`` also raises ``ValueError``.
    """
    scale = COLUMN_SCALES.get(column)
    if scale is None:
        raise ValueError(f"{column!r} is not a decimal import column")
    if text is None or _is_null(text):
        return None
    cleaned = text.strip()
    if cleaned.endswith("%"):
        if not column.endswith(_PCT_SUFFIX):
            raise ValueError(f"a percent sign is accepted only on *_pct columns, not {column!r}")
        cleaned = cleaned[:-1].rstrip()
    value = parse_decimal_text(cleaned)
    if value is None:
        raise ValueError(f"not a number: {text.strip()!r}")
    limit = _LIMITS[column]
    overflow = f"{column} must be below 10^{COLUMN_INTEGER_DIGITS[column]} in magnitude"
    # Checked before quantizing as well: quantizing a huge value to the column scale would need
    # more than the context's 28 digits and raise InvalidOperation instead. copy_abs is exact,
    # whereas abs() applies the context and raises Overflow beyond its exponent range (1e1000000).
    if value.copy_abs() >= limit:
        raise _OutOfRangeError(overflow)
    quantized = value.quantize(_QUANTA[column], rounding=ROUND_HALF_UP)
    if quantized.copy_abs() >= limit:
        raise _OutOfRangeError(overflow)
    # "(0)" and "-0.001" would otherwise keep a negative zero.
    return quantized.copy_abs() if quantized.is_zero() else quantized


def parse_int_cell(text: str | None) -> int | None:
    """Parse a share-count or volume cell into an ``int`` in 0..``MAX_SHARE_COUNT`` (10^12).

    Separators are stripped (``"1,888,186"`` gives 1888186). A decimal form is accepted only
    when its fractional part is zero (``"1234.0"``, as spreadsheets export integers).

    Returns ``None`` for an empty cell. Raises ``ValueError`` for text that is not an integer, and
    for a negative value or one above 10^12.
    """
    if text is None or _is_null(text):
        return None
    value = parse_decimal_text(text.strip())
    if value is None:
        raise ValueError(f"not an integer: {text.strip()!r}")
    if value < 0 or value > MAX_SHARE_COUNT:
        raise _OutOfRangeError(f"{value} is outside 0..{MAX_SHARE_COUNT}")
    if value != value.to_integral_value():
        raise ValueError(f"not a whole number: {text.strip()!r}")
    return int(value)


def _convert_cell(column: str, text: str) -> CellValue:
    """Convert one cell by its column's rule; ``ValueError`` propagates to the caller."""
    rule = _COLUMN_RULES[column]
    if rule.type == "decimal":
        return parse_decimal_cell(text, column)
    if rule.type == "integer":
        return parse_int_cell(text)
    if rule.type == "date":
        return parse_date_cell(text)
    if _is_null(text):
        return None
    cleaned = text.strip()
    return cleaned.upper() if column in _UPPERCASE_COLUMNS else cleaned


def _conversion_error(column: str, exc: ValueError) -> str:
    """The error code of a cell that failed to convert."""
    if isinstance(exc, _OutOfRangeError):
        return f"out_of_range:{column}"
    return f"invalid_{_COLUMN_RULES[column].type}:{column}"


def _convert_row(
    row_number: int,
    layout: _RowLayout,
    texts: Sequence[str],
    shared: Sequence[dict[str, CellValue]],
) -> TabularRecord:
    """Convert one row's cells of the kind's columns, ``texts`` in ``layout`` order, into a record
    of compact rows; a failed cell becomes ``None`` plus an error code.

    A cell holding a NUL character fails whatever its column (``invalid_character:<column>``):
    no prefix or remainder of its text is read as the value. The row's raw cells show each NUL
    as U+2400, so the evidence of a rejected row stays storable.

    ``shared`` holds, per column, compact values already converted from a text in this parse
    (:data:`_SHARED_VALUES_LIMIT`); a text found there takes that value instead of a new one.
    Only cells that convert are kept there, so every failure is converted, and reported, anew.
    """
    joined = _NUL.join(texts)
    holds_nul = joined.count(_NUL) > len(texts) - 1
    if holds_nul:
        joined = _NUL.join(text.replace(_NUL, _NUL_EVIDENCE) for text in texts)
    cells: list[CellValue] = []
    errors: list[str] = []
    for column, text, scale, known in zip(
        layout.columns, texts, layout.scales, shared, strict=True
    ):
        if holds_nul and _NUL in text:
            cells.append(None)
            errors.append(f"invalid_character:{column}")
            continue
        if text in known:
            cells.append(known[text])
            continue
        try:
            value = _convert_cell(column, text)
        except ValueError as exc:
            cells.append(None)
            errors.append(_conversion_error(column, exc))
            continue
        cell = _compact_cell(column, value, scale)
        if len(known) >= _SHARED_VALUES_LIMIT:
            known.clear()
        known[text] = cell
        cells.append(cell)
    return TabularRecord(
        row_number=row_number,
        values=_ValuesRow(layout, tuple(cells)),
        errors=tuple(errors),
        raw=_RawCells(layout, joined),
    )


# Typed accessors: record values are a union, and the rules below need the concrete type. A
# mismatch can only come from this module's own conversion, so it is a programmer error.


def _decimal(values: Mapping[str, CellValue], column: str) -> Decimal | None:
    value = values.get(column)
    if value is None or isinstance(value, Decimal):
        return value
    raise TypeError(f"{column} holds {type(value).__name__}, expected Decimal")


def _integer(values: Mapping[str, CellValue], column: str) -> int | None:
    value = values.get(column)
    if value is None or (isinstance(value, int) and not isinstance(value, bool)):
        return value
    raise TypeError(f"{column} holds {type(value).__name__}, expected int")


def _date(values: Mapping[str, CellValue], column: str) -> date | None:
    value = values.get(column)
    if value is None or isinstance(value, date):
        return value
    raise TypeError(f"{column} holds {type(value).__name__}, expected date")


def _text(values: Mapping[str, CellValue], column: str) -> str | None:
    value = values.get(column)
    if value is None or isinstance(value, str):
        return value
    raise TypeError(f"{column} holds {type(value).__name__}, expected str")


def _chunks(items: Sequence[Any], size: int = _IN_CHUNK) -> Iterator[Sequence[Any]]:
    """Yield slices of at most ``size`` items, keeping every statement far below asyncpg's
    32,767 bind parameters: at the default 5,000, an ``IN`` list binds at most 5,000 and a
    ``VALUES`` list of composite keys, at most three parts each, at most 15,000."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _key_batches(keys: Sequence[tuple[Any, ...]], **parts: TypeEngine[Any]) -> Iterator[Values]:
    """Yield ``keys`` as typed ``VALUES`` lists named ``requested``, one per :func:`_chunks`
    slice; each column is named by a keyword of ``parts``, in order, and bound as its type.

    A ``select()`` joined to one on ``=`` of every key column reads only the stored rows whose
    composite key is requested, through the table's unique key index, never a ticker's other
    periods or sessions.
    """
    for chunk in _chunks(keys):
        yield sql_values(
            *(sql_column(name, type_) for name, type_ in parts.items()), name="requested"
        ).data(chunk)


# ---------------------------------------------------------------------------------------------
# File reading (pandas is imported here only)
# ---------------------------------------------------------------------------------------------

_OPEN_FLAGS: Final[int] = (
    os.O_RDONLY
    | getattr(os, "O_NONBLOCK", 0)
    | getattr(os, "O_NOCTTY", 0)
    | getattr(os, "O_BINARY", 0)
)
"""Flags of the one descriptor :meth:`TabularImportPipeline.fetch` checks and reads. Non-blocking,
so a FIFO or device at the path opens at once instead of waiting for a writer and ``fstat`` then
rejects it; a terminal never becomes the controlling one. A flag the platform lacks counts as 0."""


# The package parts _preflight_xlsx reads, and the content types openpyxl (3.1.5) routes by.
_XLSX_MANIFEST: Final = "[Content_Types].xml"
_XLSX_DEFAULT_WORKBOOK: Final = "xl/workbook.xml"
_XLSX_WORKSHEET_TYPE: Final = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
)
# In the order openpyxl tries them: the workbook it opens is the first Override of the first of
# these types the manifest declares, else xl/workbook.xml.
_XLSX_WORKBOOK_TYPES: Final = (
    "application/vnd.ms-excel.template.macroEnabled.main+xml",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml",
    "application/vnd.ms-excel.sheet.macroEnabled.main+xml",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml",
)
_XLSX_PACKAGE_XML_BYTES: Final = 1024 * 1024
"""Largest declared size, in bytes (1 MiB), of the manifest or workbook relationships part that
:func:`_preflight_xlsx` parses. A manifest is about 1 KB plus 150 bytes per part, so one of
:data:`MAX_XLSX_MEMBERS` parts stays near 150 KB."""

# ECMA-376 Part 2 (Open Packaging Conventions) allows only stored and deflated, unencrypted
# members; zipfile flags an encrypted member with bit 0 of its general purpose flags.
_XLSX_COMPRESS_TYPES: Final = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
_ZIP_ENCRYPTED_FLAG: Final = 0x1
# A ZIP local file header: its signature, then 26 bytes whose last four are the lengths of the
# member name and of the extra field that follow it, before the member's data (APPNOTE 4.3.7).
_ZIP_LOCAL_HEADER_SIGNATURE: Final = b"PK\x03\x04"
_ZIP_LOCAL_HEADER_BYTES: Final = 30
_ZIP_LOCAL_NAME_LENGTH_AT: Final = 26
_XLSX_VERIFY_INPUT_BYTES: Final = 64 * 1024
"""Most compressed bytes (64 KiB) :func:`_inflated` hands zlib at a time."""
_XLSX_VERIFY_OUTPUT_BYTES: Final = 1024 * 1024
"""Most expanded bytes (1 MiB) :func:`_inflated` takes from zlib at a time."""


def _member_data(raw: bytes, member: zipfile.ZipInfo, source: str) -> memoryview:
    """The compressed data of ``member`` within the archive ``raw``, where zipfile reads it:
    ``compress_size`` bytes (from the central directory) after the local file header at
    ``member.header_offset``, which is 30 bytes plus the name and extra-field lengths that local
    header records. Nothing is copied.

    Raises:
        FetchFailed: ``xlsx_size_mismatch`` when no local file header is at that offset or the
            data runs past the end of ``raw``.
    """
    header = member.header_offset
    if (
        header < 0
        or header + _ZIP_LOCAL_HEADER_BYTES > len(raw)
        or raw[header : header + len(_ZIP_LOCAL_HEADER_SIGNATURE)] != _ZIP_LOCAL_HEADER_SIGNATURE
    ):
        raise FetchFailed(source, "xlsx_size_mismatch")
    lengths = header + _ZIP_LOCAL_NAME_LENGTH_AT
    name_length = int.from_bytes(raw[lengths : lengths + 2], "little")
    extra_length = int.from_bytes(raw[lengths + 2 : lengths + 4], "little")
    start = header + _ZIP_LOCAL_HEADER_BYTES + name_length + extra_length
    end = start + member.compress_size
    if end > len(raw):
        raise FetchFailed(source, "xlsx_size_mismatch")
    return memoryview(raw)[start:end]


def _inflated(data: memoryview) -> Iterator[bytes]:
    """Decompress the raw DEFLATE stream ``data`` as zipfile does (to the end of the stream or
    of ``data``, whichever comes first), yielding at most :data:`_XLSX_VERIFY_OUTPUT_BYTES` at a
    time from at most :data:`_XLSX_VERIFY_INPUT_BYTES` of input.

    Raises:
        zlib.error: ``data`` is not a valid DEFLATE stream.
    """
    decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
    for start in range(0, len(data), _XLSX_VERIFY_INPUT_BYTES):
        pending: bytes | memoryview = data[start : start + _XLSX_VERIFY_INPUT_BYTES]
        while pending and not decompressor.eof:
            yield decompressor.decompress(pending, _XLSX_VERIFY_OUTPUT_BYTES)
            pending = decompressor.unconsumed_tail
        if decompressor.eof:
            return
    # zlib can still hold output once every input byte is consumed; zipfile's final flush()
    # returns it, so it is drained here too, as bounded as the rest.
    while not decompressor.eof:
        chunk = decompressor.decompress(b"", _XLSX_VERIFY_OUTPUT_BYTES)
        if not chunk:
            return
        yield chunk


def _member_chunks(
    raw: bytes, member: zipfile.ZipInfo, source: str
) -> Iterator[bytes | memoryview]:
    """Yield, in order, what a whole read of the stored or deflated ``member`` of ``raw``
    decompresses, at most :data:`_XLSX_VERIFY_OUTPUT_BYTES` at a time, and fail as soon as that
    exceeds the member's declared ``file_size``. Once the data ends, it must have expanded to
    exactly ``file_size`` bytes with the declared CRC-32; a stored member must also declare a
    ``compress_size`` equal to its ``file_size``. Only after the last chunk is everything
    yielded verified.

    zipfile reads the same data with the same lengths, so a member that passes expands to no
    more than ``file_size`` however it is read later.

    Raises:
        FetchFailed: ``xlsx_size_mismatch`` when the member's data does not match its
            central-directory entry: no local file header at its offset, data past the end of
            ``raw``, an invalid DEFLATE stream, another expanded size or another CRC-32.
    """
    data = _member_data(raw, member, source)
    chunks: Iterator[bytes | memoryview]
    if member.compress_type == zipfile.ZIP_STORED:
        if member.compress_size != member.file_size:
            raise FetchFailed(source, "xlsx_size_mismatch")
        chunks = iter((data,))
    else:
        chunks = _inflated(data)
    expanded = 0
    crc = 0
    try:
        for chunk in chunks:
            expanded += len(chunk)
            if expanded > member.file_size:
                raise FetchFailed(source, "xlsx_size_mismatch")
            crc = zlib.crc32(chunk, crc)
            yield chunk
    except zlib.error as exc:
        raise FetchFailed(source, "xlsx_size_mismatch") from exc
    if expanded != member.file_size or crc != member.CRC:
        raise FetchFailed(source, "xlsx_size_mismatch")


def _verify_member(raw: bytes, member: zipfile.ZipInfo, source: str) -> None:
    """Check that ``member`` expands to exactly its declared size and CRC-32
    (:func:`_member_chunks`), holding one chunk at a time.

    Raises:
        FetchFailed: ``xlsx_size_mismatch``.
    """
    for _chunk in _member_chunks(raw, member, source):
        pass


def _verified_member_bytes(raw: bytes, member: zipfile.ZipInfo, source: str) -> bytes:
    """The expanded data of ``member``, verified by :func:`_member_chunks`; the caller bounds its
    declared size first.

    Raises:
        FetchFailed: ``xlsx_size_mismatch``.
    """
    return b"".join(_member_chunks(raw, member, source))


class _DoctypeDeclaredError(Exception):
    """Raised by :class:`_PackagePartTarget` to stop parsing at a DOCTYPE declaration."""


class _PackagePartTarget:
    """``xml.etree.ElementTree.XMLParser`` target that keeps, for each child of the root
    element, its local name and its attributes, the way openpyxl reads a manifest or a
    relationships part, and builds no tree.

    A DOCTYPE stops the parse (:class:`_DoctypeDeclaredError`) before its declarations are
    read: DTD entities and attribute defaults are where two XML parsers can disagree, so a part
    declaring one is not trusted to describe the package.
    """

    def __init__(self) -> None:
        self.children: list[tuple[str, dict[str, str]]] = []
        self._depth = 0

    def start(self, tag: str, attrib: dict[str, str]) -> None:
        if self._depth == 1:
            self.children.append((tag.rpartition("}")[2], attrib))
        self._depth += 1

    def end(self, tag: str) -> None:
        self._depth -= 1

    def doctype(self, name: str, pubid: str | None, system: str | None) -> None:
        raise _DoctypeDeclaredError(name)


def _package_part_children(part: bytes) -> list[tuple[str, dict[str, str]]] | None:
    """The children of the root element of the package part ``part`` (its expanded data), each
    as its local name and its unprefixed and ``{namespace}``-prefixed attributes; ``None`` when
    the part declares a DOCTYPE. The caller bounds and verifies the part's size first.

    Raises:
        xml.etree.ElementTree.ParseError: The part is not well-formed XML.
        LookupError: The part declares an encoding Python does not know.
    """
    import xml.etree.ElementTree as ElementTree

    target = _PackagePartTarget()
    # S314 asks for defusedxml. The stdlib parser is safe here: the caller caps the part's
    # declared size at _XLSX_PACKAGE_XML_BYTES and verifies it expands to exactly that, openpyxl
    # parses the same part whole right after, the target stops at a DOCTYPE so no entity is
    # declared, and the bundled expat (2.9.0) limits entity amplification.
    parser = ElementTree.XMLParser(target=target)  # noqa: S314
    try:
        parser.feed(part)
        parser.close()
    except _DoctypeDeclaredError:
        return None
    return target.children


def _xlsx_worksheet_grants(raw: bytes, archive: zipfile.ZipFile, source: str) -> frozenset[str]:
    """The members of ``archive`` (opened on ``raw``, every member stored or deflated) that
    openpyxl reads only as streamed worksheets.

    A member qualifies when it is named ``xl/worksheets/<name>.xml`` and the manifest has an
    ``Override`` whose ``PartName`` minus its leading ``/`` equals the name exactly (zipfile
    lookups are exact) with the worksheet content type, and no ``Override`` giving it another
    type: openpyxl opens the shared strings and the workbook by their ``Override``, so a member
    with any other type may be parsed whole. Nothing qualifies when the manifest declares a
    DOCTYPE, or when the relationships part of the workbook openpyxl opens is absent, larger
    than :data:`_XLSX_PACKAGE_XML_BYTES`, not well-formed, declares a DOCTYPE or has any child
    whose ``Type`` contains ``chartsheet``: openpyxl parses a chartsheet's target whole,
    whatever its name, and reads every child of that part as a relationship.

    The manifest and the relationships part are parsed from data :func:`_member_chunks` has
    verified, never read through zipfile, which inflates a whole read in one unbounded step.

    Raises:
        FetchFailed: ``xlsx_parts_too_large`` when the manifest is larger than
            :data:`_XLSX_PACKAGE_XML_BYTES`, and ``xlsx_size_mismatch`` when the manifest or the
            relationships part does not expand to its declared size and CRC-32; openpyxl reads
            both whole when the workbook opens.
        KeyError, xml.etree.ElementTree.ParseError, LookupError: The manifest is absent or not
            well-formed; openpyxl fails on it too.
    """
    manifest_member = archive.getinfo(_XLSX_MANIFEST)
    if manifest_member.file_size > _XLSX_PACKAGE_XML_BYTES:
        raise FetchFailed(source, "xlsx_parts_too_large")
    manifest = _package_part_children(_verified_member_bytes(raw, manifest_member, source))
    if manifest is None:
        return frozenset()
    # (member, content type) of each Override, in document order; Default entries name no part.
    overrides = [
        (attrib["PartName"][1:], attrib["ContentType"])
        for local_name, attrib in manifest
        if local_name == "Override" and "PartName" in attrib and "ContentType" in attrib
    ]
    workbook = next(
        (
            member
            for workbook_type in _XLSX_WORKBOOK_TYPES
            for member, content_type in overrides
            if content_type == workbook_type
        ),
        _XLSX_DEFAULT_WORKBOOK,
    )
    folder, workbook_name = posixpath.split(workbook)
    relationships = posixpath.join(folder, "_rels", f"{workbook_name}.rels")
    try:
        relationships_member = archive.getinfo(relationships)
    except KeyError:
        return frozenset()
    if relationships_member.file_size > _XLSX_PACKAGE_XML_BYTES:
        return frozenset()
    # A mismatch fails the run here rather than withholding the grants: openpyxl reads this
    # part whole next, and that read would inflate all of it.
    part = _verified_member_bytes(raw, relationships_member, source)
    try:
        children = _package_part_children(part)
    except Exception:
        # Not well-formed, or in an unknown encoding: openpyxl reads this part next and fails
        # the run on it, so nothing is trusted to be a worksheet meanwhile.
        return frozenset()
    if children is None or any(
        "chartsheet" in attrib.get("Type", "") for _local_name, attrib in children
    ):
        return frozenset()
    declared: dict[str, set[str]] = {}
    for member, content_type in overrides:
        declared.setdefault(member, set()).add(content_type)
    return frozenset(
        member
        for member, content_types in declared.items()
        if content_types == {_XLSX_WORKSHEET_TYPE} and _XLSX_WORKSHEET_RE.fullmatch(member)
    )


def _preflight_xlsx(raw: bytes, source: str) -> None:
    """Bound what the ``.xlsx`` archive ``raw`` can expand to, before openpyxl opens it.

    Checks, in order: the member count against :data:`MAX_XLSX_MEMBERS` and the total declared
    expanded size against :data:`MAX_XLSX_EXPANDED_BYTES`, from the central directory; that
    every member is stored or deflated and unencrypted; then which members are streamed
    worksheets (:func:`_xlsx_worksheet_grants`, which verifies and parses only the manifest and
    the workbook relationships, each at most 1 MiB); then each streamed worksheet against
    :data:`MAX_XLSX_WORKSHEET_BYTES` and the sum of every other member against
    :data:`MAX_XLSX_PARTS_BYTES`; last, that every member other than a streamed worksheet
    expands to exactly its declared size and CRC-32 (:func:`_member_chunks`). Nothing is ever
    decompressed more than 1 MiB at a time.

    Raises:
        FetchFailed: ``xlsx_too_many_members``, ``xlsx_expanded_too_large``,
            ``xlsx_unsupported_member`` (a member compressed other than stored or deflated, or
            encrypted), ``xlsx_worksheet_too_large``, ``xlsx_parts_too_large`` or
            ``xlsx_size_mismatch`` (a member's data does not match its central-directory entry).
        zipfile.BadZipFile: ``raw`` is not a ZIP archive.
        KeyError, xml.etree.ElementTree.ParseError, LookupError: The manifest is absent or not
            well-formed.
    """
    # A declared size bounds what zipfile returns, not what it decompresses: a whole read
    # (ZipFile.read, or ZipExtFile.read() with no size) inflates the member's entire compressed
    # data in one call and only then cuts it at file_size and checks the CRC-32, so a member
    # that understates its size expands without bound before BadZipFile is raised. Hence every
    # member openpyxl may read whole is inflated here first, 1 MiB at a time, and must expand to
    # exactly its declared size.
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) > MAX_XLSX_MEMBERS:
            raise FetchFailed(source, "xlsx_too_many_members")
        if sum(member.file_size for member in members) > MAX_XLSX_EXPANDED_BYTES:
            raise FetchFailed(source, "xlsx_expanded_too_large")
        if any(
            member.compress_type not in _XLSX_COMPRESS_TYPES
            or member.flag_bits & _ZIP_ENCRYPTED_FLAG
            for member in members
        ):
            raise FetchFailed(source, "xlsx_unsupported_member")
        worksheets = _xlsx_worksheet_grants(raw, archive, source)
    parts = 0
    for member in members:
        if member.filename not in worksheets:
            parts += member.file_size
        elif member.file_size > MAX_XLSX_WORKSHEET_BYTES:
            raise FetchFailed(source, "xlsx_worksheet_too_large")
    if parts > MAX_XLSX_PARTS_BYTES:
        raise FetchFailed(source, "xlsx_parts_too_large")
    # openpyxl reads a streamed worksheet in reads of a bounded size, each of which zipfile
    # inflates at most that much at a time and cuts at the declared size, capped above; so the
    # worksheets need no verification. Every other member may be read whole, and their declared
    # sizes total at most MAX_XLSX_PARTS_BYTES, which bounds this pass.
    for member in members:
        if member.filename not in worksheets:
            _verify_member(raw, member, source)


# pandas' C parser ends every cell at its first NUL byte, silently dropping the rest of the text
# ("12\x0034.56" reads as "12"). So each NUL of a CSV file is parsed as a character the file does
# not otherwise contain, drawn from the Basic Multilingual Plane's private-use area, and turned
# back into NUL in every cell read.
_PRIVATE_USE_FIRST: Final = 0xE000
_PRIVATE_USE_LAST: Final = 0xF8FF
# The UTF-8 forms of U+E000..U+F8FF: EE 80 80 .. EE BF BF, then EF 80 80 .. EF A3 BF. In valid
# UTF-8 an EE or EF byte only ever starts such a three-byte sequence.
_PRIVATE_USE_UTF8_RE: Final = re.compile(rb"\xee[\x80-\xbf][\x80-\xbf]|\xef[\x80-\xa3][\x80-\xbf]")


def _nul_stand_in(raw: bytes, source: str) -> str:
    """The lowest private-use character (U+E000..U+F8FF) the file ``raw`` does not contain.

    One pass over ``raw`` collects the private-use characters it holds, each once, so the cost is
    linear in the file's size whatever it holds. Replacing every NUL byte with the character's
    UTF-8 form keeps the file exactly as valid or invalid UTF-8 as it was: a NUL is never part of
    a multi-byte sequence, and the replacement is a whole one.

    Raises:
        FetchFailed: ``unreadable_file:nul_byte`` when the file holds every private-use character,
            so none can stand in for NUL.
    """
    present = {match.group() for match in _PRIVATE_USE_UTF8_RE.finditer(raw)}
    for code_point in range(_PRIVATE_USE_FIRST, _PRIVATE_USE_LAST + 1):
        candidate = chr(code_point)
        if candidate.encode() not in present:
            return candidate
    raise FetchFailed(source, "unreadable_file:nul_byte")


def _read_grid(
    raw: bytes, *, suffix: str, sheet: str | None, source: str
) -> tuple[int, Iterator[list[str]]]:
    """Read a CSV or ``.xlsx`` file as rows of cell texts, the header line first.

    Every cell is read as text (``dtype=str``, no NA inference), so nothing passes through
    ``float``; Excel numeric cells arrive as pandas' shortest round-trip string (``95.75``) and
    date cells as ``YYYY-MM-DD 00:00:00``. Empty cells become ``""``. The header is read as a
    data row (``header=None``) because pandas would otherwise rename a repeated header to
    ``name.1``, hiding the duplicate. CSV blank lines are kept, so row index + 1 is the line.

    pandas' C parser would cut a CSV cell at its first NUL byte, so a file holding one is parsed
    with each NUL replaced by a private-use character it does not otherwise contain
    (:func:`_nul_stand_in`), which every cell read turns back into NUL: each cell text is the
    file's exactly, NULs included, for :func:`_convert_row` to reject.

    The file is parsed as one frame, never in pandas chunks: the C parser widens a short row to
    the table's width only within a chunk, so a chunk starting with a short or blank line fails
    the full rows after it, and one starting with an over-long line silently drops its extra
    cells. The whole file is read, and every limit and read error raised, before this returns.

    Returns:
        The number of rows, the header line included, and an iterator that converts the frame
        to a list of cell texts one row at a time, in file order. No grid of the whole file is
        built: the frame is the only copy of the file's cells, and it is released once the
        iterator is exhausted or dropped. A CSV file in which no line holds a cell (0 bytes, a
        BOM alone, only line breaks) has 0 rows, like an empty worksheet.

    Raises:
        SourceLayoutChanged: The named sheet is not in the workbook.
        FetchFailed: The file cannot be decoded or parsed (``unreadable_file:<error>``), a CSV
            file holding NUL also holds every private-use character
            (``unreadable_file:nul_byte``), it has more than :data:`MAX_ROWS` rows after the
            header (``too_many_rows``), or an
            ``.xlsx`` archive fails a check of :func:`_preflight_xlsx`
            (``xlsx_too_many_members``, ``xlsx_expanded_too_large``,
            ``xlsx_unsupported_member``, ``xlsx_worksheet_too_large``,
            ``xlsx_parts_too_large``, ``xlsx_size_mismatch``).
    """
    import pandas as pd

    # One row more than allowed after the header, so an oversize file is detected, not cut.
    row_cap = MAX_ROWS + 2
    stand_in: str | None = None
    try:
        if suffix == ".csv":
            if _NUL.encode() in raw:
                stand_in = _nul_stand_in(raw, source)
                raw = raw.replace(_NUL.encode(), stand_in.encode())
            try:
                frame = pd.read_csv(
                    io.BytesIO(raw),
                    header=None,
                    dtype=str,
                    keep_default_na=False,
                    skip_blank_lines=False,
                    encoding="utf-8-sig",
                    nrows=row_cap,
                )
            except pd.errors.EmptyDataError:
                # No line holds a cell (0 bytes, a BOM alone, only line breaks): the file has no
                # rows, as an empty worksheet has none, and parse reports it as a layout change.
                return 0, iter(())
        else:
            # openpyxl parses the shared strings and the other non-worksheet parts whole while
            # the workbook opens, before nrows applies, so the archive is bounded first.
            _preflight_xlsx(raw, source)
            with pd.ExcelFile(io.BytesIO(raw), engine="openpyxl") as workbook:
                sheet_names = [str(name) for name in workbook.sheet_names]
                if sheet is not None and sheet not in sheet_names:
                    raise SourceLayoutChanged(
                        expected=[sheet],
                        found=sheet_names,
                        message=f"sheet {sheet!r} not found in the workbook",
                    )
                frame = pd.read_excel(
                    workbook,
                    sheet_name=sheet if sheet is not None else 0,
                    header=None,
                    dtype=str,
                    keep_default_na=False,
                    nrows=row_cap,
                )
    except IngestionError:
        raise
    except Exception as exc:
        # Untrusted input: any decoder or parser failure (ParserError, UnicodeDecodeError,
        # BadZipFile, a corrupt workbook) ends the run failed, named by type.
        raise FetchFailed(source, f"unreadable_file:{type(exc).__name__}") from exc
    if len(frame) > MAX_ROWS + 1:
        raise FetchFailed(source, "too_many_rows")

    def rows() -> Iterator[list[str]]:
        # dtype=str cells are text, or NA for an empty cell or a short row. NA becomes "" here,
        # tested only on cells that are not text, so the frame is never copied to fill it.
        for row in frame.itertuples(index=False, name=None):
            cells = [
                cell if isinstance(cell, str) else "" if pd.isna(cell) else str(cell)
                for cell in row
            ]
            if stand_in is None:
                yield cells
            else:
                yield [cell.replace(stand_in, _NUL) for cell in cells]

    return len(frame), rows()


# ---------------------------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------------------------

_SOURCE: Final = "csv_import"
"""Provenance label of every value this importer writes."""

_WRITE_COLUMNS: Final[Mapping[ImportKind, tuple[str, ...]]] = MappingProxyType(
    {
        "equities": (
            "ticker",
            "isin",
            "company_name",
            "sector",
            "shares_outstanding",
            "free_float_pct",
            "currency",
        ),
        "daily_prices": (*KIND_SPECS["daily_prices"].key, *PRICE_COLUMNS, FLOW_COLUMN),
        "fundamentals": (
            "ticker",
            "period_end_date",
            "period_type",
            "currency",
            *FUNDAMENTAL_LINE_ITEM_COLUMNS,
        ),
        "macro_indicators": ("observation_date", *MACRO_INDICATOR_COLUMNS),
        "foreign_flows": (*KIND_SPECS["foreign_flows"].key, FLOW_COLUMN),
    }
)
"""The columns each kind passes to its ``db.upsert`` writer: one uniform key set per kind, with
``None`` for an empty cell. What a ``None`` does depends on the kind:

- ``daily_prices``: the price columns are replaced as a set, because the row's ``source``
  describes them all, so an empty ``open_price``, ``high_price`` or ``low_price`` stores NULL and
  clears a stored value. An empty ``turnover_kes`` never reaches the writer:
  :meth:`TabularImportPipeline.validate` derives it as volume x close with
  ``turnover_estimated = True``. An empty ``foreign_net_flow_kes`` keeps the stored flow and
  ``flow_source`` (COALESCE).
- ``fundamentals`` and ``macro_indicators``: COALESCE merges, so an empty value cell keeps the
  stored value and its ``source_by_field`` entry.
- ``equities``: :meth:`TabularImportPipeline.reconcile` has already filled empty ``isin``,
  ``free_float_pct`` and ``currency`` cells with the stored values (``currency`` defaulting to
  KES), so the full-row update keeps them.
- ``foreign_flows``: the flow is required, so no ``None`` is passed."""

_FORMAT_CHECKED: Final = frozenset({"isin", "currency", "period_type"})
_PRICE_BOUNDS: Final = ("open_price", "high_price", "low_price", "turnover_kes")


def _is_import_kind(kind: str) -> TypeGuard[ImportKind]:
    return kind in IMPORT_KINDS


def _require_kind(kind: str) -> ImportKind:
    """Return ``kind`` as an :data:`ImportKind`, or raise ``ValueError``."""
    if not _is_import_kind(kind):
        raise ValueError(f"unknown import kind {kind!r}; expected one of {list(IMPORT_KINDS)}")
    return kind


def _require_suffix(path: Path) -> str:
    """Return the lowercased extension of ``path``, or raise ``ValueError`` when it is not one of
    :data:`SUPPORTED_SUFFIXES`."""
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise ValueError(
            f"unsupported file type {path.suffix or '(none)'!r} for {path.name!r}; "
            f"expected one of {list(SUPPORTED_SUFFIXES)}"
        )
    return suffix


def _required_text(values: Mapping[str, CellValue], column: str) -> str:
    value = _text(values, column)
    if value is None:
        raise TypeError(f"validated record has no {column}")
    return value


def _required_date(values: Mapping[str, CellValue], column: str) -> date:
    value = _date(values, column)
    if value is None:
        raise TypeError(f"validated record has no {column}")
    return value


class TabularImportPipeline(Pipeline[bytes, TabularRecord]):
    """One CSV or ``.xlsx`` file of one :data:`ImportKind`, imported in one run.

    The run is recorded as ``pipeline = 'csv_import:<kind>'`` (:attr:`run_name`), which is also
    its advisory-lock key, so two imports of the same kind never overlap while imports of
    different kinds may. ``source_url`` is the file's absolute path. The pipeline makes no
    network request; the framework's HTTP client is never used.

    Stages: :meth:`fetch` reads the file (at most :data:`MAX_FILE_BYTES`); :meth:`parse` reads
    the rows, checks the headers and converts the kind's columns; :meth:`validate` mirrors every
    CHECK and length limit of the target table and rejects a ``daily_prices`` or
    ``foreign_flows`` row dated after today (``future_date:trade_date``); :meth:`reconcile`
    collapses repeated keys and checks stored data (unknown tickers, ISIN owners, currency
    conflicts, missing price rows); :meth:`upsert` writes through ``opennse_terminal.db.upsert``
    and reports, through :meth:`write_rejections`, each fundamentals row whose period a
    concurrent writer stored in another currency after :meth:`reconcile` (``currency_conflict``)
    and each foreign_flows row whose price row was deleted after :meth:`reconcile`
    (``no_price_row``). A key rejected by :meth:`reconcile` or :meth:`upsert` rejects every row
    collapsed into it as well. Rejections at any of these stages count toward the shared
    thresholds: up to 10% gives ``partial`` (valid rows written), more gives ``layout_changed``
    (nothing written); a missing required header or a file without data rows is
    ``layout_changed`` too.

    Instances are single-use. Evidence found during the run (``kind``, ``file_name``, ``sheet``,
    ``file_size_bytes``, ``ignored_columns``, ``blank_rows_skipped``, ``duplicates_collapsed``,
    ``flow_missing_keys``) goes into the run's ``details``.

    Args:
        settings: The run's settings (database, timezone).
        kind: The import kind.
        path: The CSV or ``.xlsx`` file.
        sheet: The worksheet of an ``.xlsx`` file; the first one when ``None`` or empty.
        now: Returns the current timezone-aware time; defaults to now in ``settings.tzinfo``.
            :meth:`validate` calls it once and rejects a ``daily_prices`` or ``foreign_flows`` row
            dated after that day in ``settings.tzinfo`` (``future_date:trade_date``).

    Raises:
        ValueError: ``kind`` is not an import kind, the extension is not ``.csv`` or ``.xlsx``,
            or ``sheet`` is given for a CSV file.
    """

    name: ClassVar[str] = "csv_import"
    min_rows: ClassVar[int] = 1

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        kind: ImportKind,
        path: Path,
        sheet: str | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(settings)
        self._now: Callable[[], datetime] = (
            now if now is not None else (lambda: datetime.now(settings.tzinfo))
        )
        self.kind: ImportKind = _require_kind(kind)
        self.spec = KIND_SPECS[self.kind]
        self.path = Path(path)
        self._suffix = _require_suffix(self.path)
        self.sheet = sheet or None
        if self.sheet is not None and self._suffix != ".xlsx":
            raise ValueError(f"a sheet name applies only to .xlsx files, not {self.path.name!r}")
        self.source_url: str = str(self.path.resolve())
        # Bound to the run's ctx.details and ctx.run_id by fetch(); parse() and upsert() have no
        # context, and their log lines name the run by its id, never by the file's name.
        self._details: dict[str, Any] = {}
        self._run_id: uuid.UUID | None = None
        # The layouts of this run's compact rows, shared by every record with the same columns.
        self._layouts = _RowLayouts()
        # The rows upsert() found it could not write, reported by write_rejections().
        self._write_rejected: list[RejectedRow] = []
        # By the line of each fundamentals or foreign_flows row reconcile() accepted, the rows of
        # its key it collapsed, in line order; upsert() rejects them with that row.
        self._superseded: dict[int, list[TabularRecord]] = {}

    @property
    def run_name(self) -> str:
        """``csv_import:<kind>``: the run row's ``pipeline`` and the advisory-lock key."""
        return f"{self.name}:{self.kind}"

    # --- fetch ---------------------------------------------------------------------------------

    async def fetch(self, ctx: RunContext) -> bytes:
        """Read the file's bytes through one descriptor, in one worker thread.

        The path is opened once, non-blocking (:data:`_OPEN_FLAGS`), so a FIFO or device opens
        at once instead of waiting for a writer. ``fstat`` on that descriptor checks that it is a
        regular file of at most :data:`MAX_FILE_BYTES`, and the bytes are read from the same
        descriptor, one byte past the limit so a file that grew after the check is caught. The
        path is never reopened, so replacing it after the check cannot block or change the read.
        ``details.file_size_bytes`` holds the checked size, then the length read.

        Raises :class:`FetchFailed` with ``file_not_found``, ``not_a_file``, ``file_too_large``
        (above :data:`MAX_FILE_BYTES`, checked before reading) or ``unreadable_file:<error>``.
        """
        self._details = ctx.details
        self._run_id = ctx.run_id
        ctx.details.update({"kind": self.kind, "file_name": self.path.name, "sheet": self.sheet})
        size, raw = await asyncio.to_thread(self._read_file)
        ctx.details["file_size_bytes"] = size
        if raw is None or len(raw) > MAX_FILE_BYTES:
            raise FetchFailed(self.source_url, "file_too_large")
        ctx.details["file_size_bytes"] = len(raw)
        return raw

    def _read_file(self) -> tuple[int, bytes | None]:
        """The checked size and the bytes read, or ``None`` for a file above the limit."""
        try:
            fd = os.open(self.path, _OPEN_FLAGS)
        except FileNotFoundError:
            raise FetchFailed(self.source_url, "file_not_found") from None
        except IsADirectoryError:
            raise FetchFailed(self.source_url, "not_a_file") from None
        except OSError as exc:
            raise FetchFailed(self.source_url, f"unreadable_file:{type(exc).__name__}") from exc
        try:
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode):
                    raise FetchFailed(self.source_url, "not_a_file")
                if info.st_size > MAX_FILE_BYTES:
                    return info.st_size, None
                chunks: list[bytes] = []
                remaining = MAX_FILE_BYTES + 1
                # os.read may return fewer bytes than asked; an empty chunk ends the file.
                while remaining > 0:
                    chunk = os.read(fd, remaining)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
            finally:
                os.close(fd)
        except OSError as exc:
            raise FetchFailed(self.source_url, f"unreadable_file:{type(exc).__name__}") from exc
        return info.st_size, b"".join(chunks)

    # --- parse ---------------------------------------------------------------------------------

    def parse(self, raw: bytes) -> list[TabularRecord]:
        """Read the file, check the headers and convert the kind's columns of every row.

        The rows are taken one at a time from :func:`_read_grid`, the first as the header and the
        rest as data rows numbered from line 2, so only the parsed frame and the records exist
        at once, never a grid of the whole file. Each record holds compact rows
        (:class:`TabularRecord`) whose raw cells are one new string per row, so nothing of the
        frame but the text values themselves outlives this call; a value recurring in a column is
        converted once and shared (:data:`_SHARED_VALUES_LIMIT`). Headers are normalized with
        :func:`normalize_column_name`. Columns the kind does not read are ignored and named in
        ``details.ignored_columns``; the WARNING logged for them carries only the run id, the kind
        and their count, never the file's name or its headers. A NUL character in a header is
        shown as U+2400, so that header matches no column. Rows whose kind columns are all empty
        are skipped (``details.blank_rows_skipped``); a cell holding NUL is not empty.
        Conversion failures, ``invalid_character:<col>`` for a cell holding NUL included, are
        collected on each record for :meth:`validate`; nothing is raised per row.

        Raises:
            SourceLayoutChanged: No header row (``the file is empty``, with the required headers
                expected and none found, for a CSV file without a cell as for an empty
                worksheet), a repeated header, a missing required header, or (``.xlsx``) a
                missing sheet.
            FetchFailed: The file cannot be read, it has more than :data:`MAX_ROWS` rows, or an
                ``.xlsx`` archive would expand beyond the ``MAX_XLSX_*`` limits.
        """
        row_count, rows = _read_grid(
            raw, suffix=self._suffix, sheet=self.sheet, source=self.source_url
        )
        required = list(self.spec.required)
        if row_count == 0:
            raise SourceLayoutChanged(expected=required, found=[], message="the file is empty")
        headers = [normalize_column_name(cell) for cell in next(rows)]
        named = [header for header in headers if header]
        repeated = sorted(header for header, count in Counter(named).items() if count > 1)
        if repeated:
            raise SourceLayoutChanged(
                expected=required,
                found=named,
                message=f"duplicate headers: {', '.join(repeated)}",
            )
        missing = [column for column in required if column not in named]
        if missing:
            raise SourceLayoutChanged(
                expected=required,
                found=named,
                message=f"missing required columns for {self.kind}: {', '.join(missing)}",
            )

        wanted = frozenset(self.spec.columns)
        positions = {header: index for index, header in enumerate(headers) if header in wanted}
        ignored = [header for header in named if header not in wanted]
        if ignored:
            self._details["ignored_columns"] = ignored
            logger.warning(
                "run %s: %s import ignores %d columns it does not read (details.ignored_columns)",
                self._run_id,
                self.kind,
                len(ignored),
            )

        layout = self._layouts.layout(tuple(positions))
        indexes = tuple(positions.values())
        shared: list[dict[str, CellValue]] = [{} for _ in indexes]
        records: list[TabularRecord] = []
        blank = 0
        # The header is line 1 and has been taken, so the first data row is line 2.
        for line, row in enumerate(rows, start=2):
            texts = [row[index] if index < len(row) else "" for index in indexes]
            if all(not text.strip() for text in texts):
                blank += 1
                continue
            records.append(_convert_row(line, layout, texts, shared))
        if blank:
            self._details["blank_rows_skipped"] = blank
        return records

    # --- validate ------------------------------------------------------------------------------

    def validate(self, records: Sequence[TabularRecord]) -> ValidationOutcome[TabularRecord]:
        """Reject rows that would break a rule or a database constraint, and derive defaults.

        Every kind: a conversion error (``invalid_decimal:<col>``, ``invalid_integer:<col>``,
        ``invalid_date:<col>``, ``out_of_range:<col>``, or ``invalid_character:<col>`` for a cell
        of any type holding a NUL character), an empty required cell
        (``missing_value:<col>``) or text longer than its column (``too_long:<col>``); an
        ``isin`` not matching ``KE`` + 9 alphanumerics + a digit (``invalid_isin``); a
        ``currency`` that is not three letters (``invalid_currency``).

        - ``equities``: ``free_float_pct`` outside 0..100 (``out_of_range:free_float_pct``).
        - ``daily_prices``: ``close_price`` <= 0 (``non_positive:close_price``); a negative open,
          high, low or turnover (``negative:<col>``); high below low (``high_below_low``). An
          empty turnover is derived as volume x close, rounded ``ROUND_HALF_UP`` to 0.01, with
          ``turnover_estimated = True``, and an estimate that reaches 10^22 after rounding is
          rejected (``out_of_range:turnover_kes``); a supplied one gives
          ``turnover_estimated = False``.
        - ``fundamentals``: ``period_type`` (default ``FY``) not ``FY`` or ``H1``
          (``invalid_period_type``); ``currency`` defaults to KES; a row with no value column
          (``no_values``).
        - ``macro_indicators``: a row with no indicator value (``no_values``).
        - ``daily_prices`` and ``foreign_flows``: a ``trade_date`` after today in
          ``settings.tzinfo`` (``future_date:trade_date``), because a future row would be read
          as an observed session; today itself is accepted. Today is read once per call from the
          pipeline's ``now``.

        A cell that failed to convert is reported by its conversion code only, never also as
        ``missing_value``, ``no_values`` or ``future_date``. Rejected rows are reported with their
        line number, the codes joined by ``;`` and their raw cells (NUL shown as U+2400). An
        accepted row whose values gained defaults or derived values is a new record whose compact
        row shares every unchanged cell; any other accepted row is returned as it is.

        Raises:
            ValueError: ``now`` returned a naive datetime.
        """
        today = self._today()
        valid: list[TabularRecord] = []
        rejected: list[RejectedRow] = []
        for record in records:
            # The checks read and fill a plain dict, one row at a time; an accepted row keeps a
            # compact row again, sharing every cell the checks left unchanged.
            original = _plain_values(record.values)
            values = dict(original)
            codes = list(record.errors)
            failed = {code.partition(":")[2] for code in record.errors}
            codes.extend(
                f"missing_value:{column}"
                for column in self.spec.required
                if column not in failed and values.get(column) is None
            )
            codes.extend(_check_text(values))
            codes.extend(self._check_kind(values, failed, today))
            if codes:
                rejected.append(
                    RejectedRow(index=record.row_number, reason=";".join(codes), row=record.raw)
                )
                continue
            changes = {
                column: value
                for column, value in values.items()
                if original.get(column, _ABSENT) is not value
            }
            valid.append(
                replace(record, values=self._layouts.updated(record.values, changes))
                if changes
                else record
            )
        return ValidationOutcome(valid=valid, rejected=rejected)

    def _today(self) -> date:
        """Today in ``settings.tzinfo``, from ``now``; raises ``ValueError`` for a naive time."""
        current = self._now()
        if current.tzinfo is None:
            raise ValueError("now() must return a timezone-aware datetime")
        return current.astimezone(self.settings.tzinfo).date()

    def _check_kind(self, values: dict[str, CellValue], failed: set[str], today: date) -> list[str]:
        """The kind's own rules; fills defaults and derived values into ``values``.

        ``failed`` names the columns whose cells did not convert: a row with an unconvertible
        value is reported for that cell, not as ``no_values``. ``today`` is the latest
        ``trade_date`` a session kind accepts; an unconvertible date is ``None`` here, so it is
        never also reported as ``future_date``.
        """
        codes: list[str] = []
        if self.kind in _SESSION_KINDS:
            trade_date = _date(values, "trade_date")
            if trade_date is not None and trade_date > today:
                codes.append("future_date:trade_date")
        if self.kind == "equities":
            free_float = _decimal(values, "free_float_pct")
            if free_float is not None and not 0 <= free_float <= _HUNDRED:
                codes.append("out_of_range:free_float_pct")
        elif self.kind == "daily_prices":
            codes.extend(_check_prices(values))
        elif self.kind == "fundamentals":
            if values.get("period_type") is None:
                values["period_type"] = _DEFAULT_PERIOD_TYPE
            if values.get("currency") is None:
                values["currency"] = DEFAULT_CURRENCY
            if values["period_type"] not in PERIOD_TYPES:
                codes.append("invalid_period_type")
            # upsert_fundamentals would drop a row without values uncounted; reporting it as a
            # rejection, as for macro rows, keeps every file row accounted for.
            if _no_values(values, failed, FUNDAMENTAL_LINE_ITEM_COLUMNS):
                codes.append("no_values")
        elif self.kind == "macro_indicators":
            if _no_values(values, failed, MACRO_INDICATOR_COLUMNS):
                codes.append("no_values")
        return codes

    # --- reconcile -----------------------------------------------------------------------------

    async def reconcile(
        self, ctx: RunContext, records: list[TabularRecord]
    ) -> ValidationOutcome[TabularRecord]:
        """Collapse repeated keys, then check the rows against stored data on ``ctx.conn``.

        Rows sharing the kind's conflict key are collapsed, the last occurrence winning
        (``details.duplicates_collapsed``): one ``INSERT ... ON CONFLICT DO UPDATE`` cannot touch
        a row twice. Then, with bound ``select()`` statements in batches of at most 5,000 keys,
        either an ``IN`` list of tickers or ISINs or a join to a typed ``VALUES`` list of the
        file's exact composite keys, so no stored row outside the file's keys is read:

        - ``equities``: an ``isin`` held by another ticker, stored or claimed by an earlier row,
          is rejected (``isin_conflict``). Each accepted row is completed with the stored
          ``isin``, ``free_float_pct`` and ``currency`` where its cells are empty (``currency``
          finally defaulting to KES), so the upsert never nulls stored data.
        - ``daily_prices``: a ticker absent from ``terminal_equities`` (``unknown_ticker``).
        - ``fundamentals``: an unknown ticker (``unknown_ticker``), or a currency different from
          the stored period's (``currency_conflict``), so values never merge across currencies;
          a period another writer stores after this check is caught by :meth:`upsert`.
        - ``foreign_flows``: a ``(ticker, trade_date)`` without a price row (``no_price_row``).

        A collapsed row is neither written nor rejected while its key's surviving row is
        accepted. When the surviving row is rejected, every row it superseded is rejected too,
        with the same reason, its own line number and its raw cells, so each file row stating a
        rejected key counts toward the same thresholds as :meth:`validate`'s rejections; the
        rejected rows are returned in line order. The superseded rows of each accepted
        ``fundamentals`` or ``foreign_flows`` key are kept for :meth:`upsert`, which rejects them
        with their key's row should it reject that row at write time.
        """
        unique, superseded = _collapse_repeated_keys(records, self._key_of)
        collapsed = len(records) - len(unique)
        ctx.details["duplicates_collapsed"] = collapsed
        if collapsed:
            logger.info("%s import collapsed %d rows that repeat a key", self.kind, collapsed)
        if not unique:
            return ValidationOutcome(valid=[], rejected=[])
        if self.kind == "equities":
            checked = await _reconcile_equities(ctx.conn, unique, self._layouts)
        elif self.kind in ("daily_prices", "fundamentals"):
            checked = await self._reconcile_tickers(ctx.conn, unique)
        elif self.kind == "foreign_flows":
            checked = await _reconcile_flows(ctx.conn, unique)
        else:
            checked = ValidationOutcome(valid=unique, rejected=[])
        # Only these kinds' upsert() can still reject an accepted key; for the others, the rows
        # collapsed into an accepted key are released with the records.
        if self.kind in ("fundamentals", "foreign_flows"):
            self._superseded = {
                record.row_number: superseded[record.row_number]
                for record in checked.valid
                if record.row_number in superseded
            }
        rejected = sorted(_with_superseded(checked.rejected, superseded), key=lambda row: row.index)
        return ValidationOutcome(valid=checked.valid, rejected=rejected)

    def _key_of(self, record: TabularRecord) -> tuple[CellValue, ...]:
        return tuple(record.values.get(column) for column in self.spec.key)

    async def _reconcile_tickers(
        self, conn: AsyncConnection, records: list[TabularRecord]
    ) -> ValidationOutcome[TabularRecord]:
        """Unknown tickers (prices, fundamentals) and fundamentals currency conflicts."""
        tickers = sorted({_required_text(record.values, "ticker") for record in records})
        known: set[str] = set()
        for chunk in _chunks(tickers):
            result = await conn.execute(
                select(TerminalEquity.ticker).where(TerminalEquity.ticker.in_(chunk))
            )
            known.update(result.scalars())
        stored_currency: dict[_Period, str] = {}
        if self.kind == "fundamentals":
            periods = sorted(
                {
                    _period_of(record)
                    for record in records
                    if _required_text(record.values, "ticker") in known
                }
            )
            stored_currency = await _stored_currencies(conn, periods)
        valid: list[TabularRecord] = []
        rejected: list[RejectedRow] = []
        for record in records:
            values = record.values
            ticker = _required_text(values, "ticker")
            reason: str | None = None
            if ticker not in known:
                reason = "unknown_ticker"
            elif self.kind == "fundamentals" and _conflicts_with(record, stored_currency):
                reason = "currency_conflict"
            if reason is None:
                valid.append(record)
            else:
                rejected.append(RejectedRow(index=record.row_number, reason=reason, row=record.raw))
        return ValidationOutcome(valid=valid, rejected=rejected)

    # --- upsert --------------------------------------------------------------------------------

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[TabularRecord], run_id: uuid.UUID
    ) -> int:
        """Write the accepted rows through ``opennse_terminal.db.upsert`` on ``conn``; never
        commits.

        The rows are sorted by the kind's conflict key and handed to the writer in batches of at
        most :data:`_WRITE_BATCH_ROWS` (1,000), each batch's write rows built only when it is
        written. Every batch runs on ``conn``, inside the run's one data transaction, and covers
        a key range above the previous batch's, so the import locks rows in key order;
        :meth:`reconcile` has already collapsed repeated keys, so no key spans two batches.

        - ``equities``: ``upsert_rows(..., key=("ticker",), mode="do_update")`` with complete
          rows and ``source = 'csv_import'``.
        - ``daily_prices``: ``upsert_prices(source="csv_import", ingestion_run_id=run_id)``: the
          price columns overwrite any stored source; a non-empty flow sets
          ``foreign_net_flow_kes`` and ``flow_source = 'csv_import'``, an empty one keeps the
          stored flow.
        - ``fundamentals`` and ``macro_indicators``: COALESCE merges recording ``csv_import`` in
          ``source_by_field`` for each non-empty cell. ``upsert_fundamentals`` skips, uncounted,
          a row whose period is stored in another currency; :meth:`reconcile` rejected every
          such period it saw, so a skipped row's period was stored by a concurrent writer (a PDF
          commit) since. When a batch writes fewer rows than it holds, the stored currencies of
          its periods are read on ``conn`` (a bound ``select()`` joined to a typed ``VALUES``
          list), and each row whose period is stored in another currency becomes a
          ``currency_conflict`` rejection reported by :meth:`write_rejections`, so it counts
          toward the shared thresholds; their count is logged once at WARNING with the run id.
        - ``foreign_flows``: ``update_flows(flow_source="csv_import")``, touching only the flow
          columns. A key whose price row was deleted since :meth:`reconcile` (for example by an
          equity cascade) updates nothing; its row becomes a ``no_price_row`` rejection reported
          by :meth:`write_rejections`, with its line number and raw cells, so it counts toward
          the shared thresholds. Their count is logged once at WARNING with the run id and
          recorded in ``details.flow_missing_keys`` (the count of keys and the first 50 keys, in
          key order).

        Each row :meth:`reconcile` collapsed into a key rejected here is rejected with it, for the
        same reason, with its own line number and raw cells, and counted in the WARNING line.

        Returns the number of rows inserted or updated, summed over the batches. Re-importing a
        file rewrites identical values, so the stored state is unchanged.

        Raises:
            RuntimeError: A fundamentals batch wrote fewer rows than it holds and currency
                conflicts do not account for every row it left out, or ``update_flows`` reported
                a key without a price row that its foreign_flows batch does not hold; the run
                ends ``failed`` and rolls back rather than report a short write as success.
        """
        columns = _WRITE_COLUMNS[self.kind]
        ordered = sorted(records, key=self._key_of)
        written = 0
        missing_sample: list[tuple[str, date]] = []
        unwritten: list[RejectedRow] = []
        no_price_rows: list[RejectedRow] = []
        for start in range(0, len(ordered), _WRITE_BATCH_ROWS):
            batch = ordered[start : start + _WRITE_BATCH_ROWS]
            rows = [{column: record.values.get(column) for column in columns} for record in batch]
            if self.kind == "equities":
                for row in rows:
                    row["source"] = _SOURCE
                written += await upsert_rows(
                    conn, TerminalEquity, rows, key=("ticker",), mode="do_update"
                )
            elif self.kind == "daily_prices":
                written += await upsert_prices(conn, rows, source=_SOURCE, ingestion_run_id=run_id)
            elif self.kind == "fundamentals":
                count = await upsert_fundamentals(conn, rows, source=_SOURCE)
                if count < len(rows):
                    unwritten.extend(await _currency_conflicts(conn, batch, len(rows) - count))
                written += count
            elif self.kind == "macro_indicators":
                written += await upsert_macro(conn, rows, source=_SOURCE)
            else:
                outcome = await update_flows(conn, rows, flow_source=_SOURCE)
                written += outcome.rows_updated
                if outcome.missing_keys:
                    # reconcile() collapsed repeated keys, so each key names one batch record.
                    by_key = {
                        (
                            _required_text(record.values, "ticker"),
                            _required_date(record.values, "trade_date"),
                        ): record
                        for record in batch
                    }
                    unknown = sum(1 for key in outcome.missing_keys if key not in by_key)
                    if unknown:
                        raise RuntimeError(
                            f"update_flows reported {unknown} keys without a price row that its "
                            f"batch of {len(batch)} foreign_flows rows does not hold"
                        )
                    no_price_rows.extend(
                        RejectedRow(
                            index=by_key[key].row_number,
                            reason="no_price_row",
                            row=by_key[key].raw,
                        )
                        for key in outcome.missing_keys
                    )
                # Batches ascend by key and each batch's keys are sorted, so the rejections and
                # this sample keep the key order overall.
                room = _MISSING_KEYS_SAMPLE - len(missing_sample)
                missing_sample.extend(outcome.missing_keys[:room])
        if no_price_rows:
            flow_rejections = _with_superseded(no_price_rows, self._superseded)
            logger.warning(
                "foreign_flows import run %s rejected %d rows at write time: no_price_row",
                self._run_id,
                len(flow_rejections),
            )
            self._details["flow_missing_keys"] = {
                "count": len(no_price_rows),
                "sample": [list(key) for key in missing_sample],
            }
            self._write_rejected.extend(flow_rejections)
        if unwritten:
            conflicts = _with_superseded(unwritten, self._superseded)
            logger.warning(
                "fundamentals import run %s rejected %d rows at write time: currency_conflict",
                self._run_id,
                len(conflicts),
            )
            self._write_rejected.extend(conflicts)
        return written

    def write_rejections(self) -> Sequence[RejectedRow]:
        """The rows :meth:`upsert` could not write, in key order, each with its line number and
        raw cells: fundamentals rows whose period a concurrent writer stored in another currency
        after :meth:`reconcile` (``currency_conflict``), and foreign_flows rows whose price row
        was deleted after :meth:`reconcile` (``no_price_row``). A key's rows that
        :meth:`reconcile` collapsed are reported with it, for the same reason, the key's rows in
        line order. They count toward the shared thresholds with the earlier stages' rejections.
        Empty for the other kinds."""
        return tuple(self._write_rejected)


def _collapse_repeated_keys(
    records: Sequence[TabularRecord], key: Callable[[TabularRecord], tuple[CellValue, ...]]
) -> tuple[list[TabularRecord], dict[int, list[TabularRecord]]]:
    """Collapse records that share a key as ``base.dedupe_by_key`` does: the **last** occurrence
    of each key wins, in the position where the key was first seen.

    Returns the surviving records and, by each survivor's line number, the earlier records of its
    key that it superseded, in file order; a key that does not repeat has no entry.
    """
    latest: dict[tuple[CellValue, ...], TabularRecord] = {}
    earlier: dict[tuple[CellValue, ...], list[TabularRecord]] = {}
    for record in records:
        record_key = key(record)
        previous = latest.get(record_key)
        if previous is not None:
            earlier.setdefault(record_key, []).append(previous)
        # Re-assigning an existing key keeps its original insertion position.
        latest[record_key] = record
    superseded = {latest[record_key].row_number: rows for record_key, rows in earlier.items()}
    return list(latest.values()), superseded


def _with_superseded(
    rejected: Sequence[RejectedRow], superseded: Mapping[int, Sequence[TabularRecord]]
) -> list[RejectedRow]:
    """``rejected``, each surviving row preceded by the rows of its key it superseded
    (``superseded``, by the survivor's line number), rejected for the same reason with their own
    line numbers and raw cells, so every file row of a rejected key counts toward the shared
    thresholds."""
    expanded: list[RejectedRow] = []
    for row in rejected:
        expanded.extend(
            RejectedRow(index=record.row_number, reason=row.reason, row=record.raw)
            for record in superseded.get(row.index, ())
        )
        expanded.append(row)
    return expanded


_Period = tuple[str, date, str]
"""A fundamentals period: ``(ticker, period_end_date, period_type)``, the conflict key."""


def _period_of(record: TabularRecord) -> _Period:
    """The period of a validated fundamentals record (``period_type`` already defaulted)."""
    values = record.values
    return (
        _required_text(values, "ticker"),
        _required_date(values, "period_end_date"),
        _required_text(values, "period_type"),
    )


def _conflicts_with(record: TabularRecord, stored_currency: Mapping[_Period, str]) -> bool:
    """True when the record's period is stored in a currency other than the record's own
    (``currency`` already defaulted to KES)."""
    stored = stored_currency.get(_period_of(record))
    return stored is not None and stored != _required_text(record.values, "currency")


async def _stored_currencies(
    conn: AsyncConnection, periods: Sequence[_Period]
) -> dict[_Period, str]:
    """The stored ``currency`` of each of ``periods`` that ``terminal_fundamentals`` holds, read
    on ``conn`` with bound ``select()`` statements joined to typed ``VALUES`` lists of the exact
    periods (:func:`_key_batches`), so no other stored period is read."""
    stored: dict[_Period, str] = {}
    for requested in _key_batches(
        periods, ticker=String(), period_end_date=Date(), period_type=String()
    ):
        result_rows = await conn.execute(
            select(
                TerminalFundamental.ticker,
                TerminalFundamental.period_end_date,
                TerminalFundamental.period_type,
                TerminalFundamental.currency,
            ).join(
                requested,
                and_(
                    TerminalFundamental.ticker == requested.c.ticker,
                    TerminalFundamental.period_end_date == requested.c.period_end_date,
                    TerminalFundamental.period_type == requested.c.period_type,
                ),
            )
        )
        for ticker, period_end, period_type, currency in result_rows.tuples():
            stored[(ticker, period_end, period_type)] = currency
    return stored


async def _currency_conflicts(
    conn: AsyncConnection, batch: Sequence[TabularRecord], unwritten: int
) -> list[RejectedRow]:
    """The ``currency_conflict`` rejections of the ``unwritten`` rows ``upsert_fundamentals``
    left out of a written ``batch`` (unique periods, in key order).

    The batch's periods are read on the run's connection, inside its transaction, after the
    write, so a period a concurrent writer committed meanwhile is read in the currency the
    ``upsert_fundamentals`` guard compared against (a stored currency is written on insert
    only).

    Raises:
        RuntimeError: Currency conflicts do not account for exactly ``unwritten`` rows.
    """
    stored_currency = await _stored_currencies(conn, [_period_of(record) for record in batch])
    conflicts = [
        RejectedRow(index=record.row_number, reason="currency_conflict", row=record.raw)
        for record in batch
        if _conflicts_with(record, stored_currency)
    ]
    if len(conflicts) != unwritten:
        raise RuntimeError(
            f"fundamentals upsert left {unwritten} of {len(batch)} batch rows unwritten, "
            f"of which currency conflicts explain {len(conflicts)}"
        )
    return conflicts


def _no_values(values: Mapping[str, CellValue], failed: set[str], columns: Sequence[str]) -> bool:
    """True when none of ``columns`` holds a value or an unconvertible cell."""
    return all(values.get(column) is None and column not in failed for column in columns)


def _check_text(values: Mapping[str, CellValue]) -> list[str]:
    """Format checks of ``isin`` and ``currency``, and length limits of the other text columns
    (``period_type`` is checked with its default, by kind)."""
    codes: list[str] = []
    for column, value in values.items():
        if not isinstance(value, str):
            continue
        if column == "isin":
            if _ISIN_RE.fullmatch(value) is None:
                codes.append("invalid_isin")
        elif column == "currency":
            if _CURRENCY_RE.fullmatch(value) is None:
                codes.append("invalid_currency")
        elif column not in _FORMAT_CHECKED:
            max_length = _COLUMN_RULES[column].max_length
            if max_length is not None and len(value) > max_length:
                codes.append(f"too_long:{column}")
    return codes


def _check_prices(values: dict[str, CellValue]) -> list[str]:
    """The ``terminal_daily_prices`` CHECKs, then the turnover estimate for an empty cell."""
    codes: list[str] = []
    close = _decimal(values, "close_price")
    if close is not None and close <= 0:
        codes.append("non_positive:close_price")
    for column in _PRICE_BOUNDS:
        bound = _decimal(values, column)
        if bound is not None and bound < 0:
            codes.append(f"negative:{column}")
    high = _decimal(values, "high_price")
    low = _decimal(values, "low_price")
    if high is not None and low is not None and high < low:
        codes.append("high_below_low")
    volume = _integer(values, "volume")
    if _decimal(values, "turnover_kes") is not None:
        values["turnover_estimated"] = False
    elif close is not None and close > 0 and volume is not None:
        limit = _LIMITS["turnover_kes"]
        # Exact: below the 10^22 limit the product has at most 26 significant digits, and only
        # there is it rounded, as in parse_decimal_cell. The rounded value is what is checked: a
        # product within 0.005 of 10^22 rounds HALF_UP to 10^22, which the column cannot hold.
        estimate = Decimal(volume) * close
        rounded = (
            estimate.quantize(_QUANTA["turnover_kes"], rounding=ROUND_HALF_UP)
            if estimate < limit
            else estimate
        )
        if rounded.copy_abs() >= limit:
            codes.append("out_of_range:turnover_kes")
        else:
            values["turnover_kes"] = rounded
            values["turnover_estimated"] = True
    return codes


async def _reconcile_equities(
    conn: AsyncConnection, records: list[TabularRecord], layouts: _RowLayouts
) -> ValidationOutcome[TabularRecord]:
    """Reject ISIN conflicts and complete each row with the stored values it leaves empty; each
    completed row is a compact row of ``layouts``."""
    tickers = sorted({_required_text(record.values, "ticker") for record in records})
    stored: dict[str, tuple[str | None, Decimal | None, str]] = {}
    for chunk in _chunks(tickers):
        result = await conn.execute(
            select(
                TerminalEquity.ticker,
                TerminalEquity.isin,
                TerminalEquity.free_float_pct,
                TerminalEquity.currency,
            ).where(TerminalEquity.ticker.in_(chunk))
        )
        for row_ticker, row_isin, row_free_float, row_currency in result.tuples():
            stored[row_ticker] = (row_isin, row_free_float, row_currency)

    isins = sorted(
        {code for record in records if (code := _text(record.values, "isin")) is not None}
    )
    owners: dict[str, str] = {}
    for chunk in _chunks(isins):
        owner_rows = await conn.execute(
            select(TerminalEquity.isin, TerminalEquity.ticker).where(TerminalEquity.isin.in_(chunk))
        )
        for owned_isin, owner in owner_rows.tuples():
            if owned_isin is not None:
                owners[owned_isin] = owner

    claimed: dict[str, str] = {}
    valid: list[TabularRecord] = []
    rejected: list[RejectedRow] = []
    for record in records:
        values = record.values
        ticker = _required_text(values, "ticker")
        isin = _text(values, "isin")
        if isin is not None:
            if owners.get(isin, ticker) != ticker or claimed.get(isin, ticker) != ticker:
                rejected.append(
                    RejectedRow(index=record.row_number, reason="isin_conflict", row=record.raw)
                )
                continue
            claimed[isin] = ticker
        stored_isin, stored_free_float, stored_currency = stored.get(
            ticker, (None, None, DEFAULT_CURRENCY)
        )
        free_float = _decimal(values, "free_float_pct")
        currency = _text(values, "currency")
        completed = layouts.updated(
            values,
            {
                "isin": isin if isin is not None else stored_isin,
                "free_float_pct": free_float if free_float is not None else stored_free_float,
                "currency": currency if currency is not None else stored_currency,
            },
        )
        valid.append(replace(record, values=completed))
    return ValidationOutcome(valid=valid, rejected=rejected)


async def _reconcile_flows(
    conn: AsyncConnection, records: list[TabularRecord]
) -> ValidationOutcome[TabularRecord]:
    """Reject flows for ``(ticker, trade_date)`` keys that have no price row.

    Exactly the file's keys are looked up, joined as typed ``VALUES`` lists of at most 5,000
    keys, so a file with widely separated dates never reads the sessions between them.
    """
    keys = [
        (_required_text(record.values, "ticker"), _required_date(record.values, "trade_date"))
        for record in records
    ]
    existing: set[tuple[str, date]] = set()
    for requested in _key_batches(sorted(set(keys)), ticker=String(), trade_date=Date()):
        result = await conn.execute(
            select(TerminalDailyPrice.ticker, TerminalDailyPrice.trade_date).join(
                requested,
                and_(
                    TerminalDailyPrice.ticker == requested.c.ticker,
                    TerminalDailyPrice.trade_date == requested.c.trade_date,
                ),
            )
        )
        existing.update((ticker, trade_date) for ticker, trade_date in result.tuples())
    valid: list[TabularRecord] = []
    rejected: list[RejectedRow] = []
    for record, key in zip(records, keys, strict=True):
        if key in existing:
            valid.append(record)
        else:
            rejected.append(
                RejectedRow(index=record.row_number, reason="no_price_row", row=record.raw)
            )
    return ValidationOutcome(valid=valid, rejected=rejected)


# ---------------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------------


async def import_file(
    kind: str,
    path: str | Path,
    sheet: str | None = None,
    *,
    settings: TerminalSettings | None = None,
    trigger: RunTrigger = "cli",
) -> RunResult:
    """Import one operator-supplied CSV or ``.xlsx`` file and return its recorded run.

    Kinds (required columns; optional columns), conflict key and precedence:

    - ``equities`` (ticker, company_name, sector, shares_outstanding; isin, free_float_pct,
      currency): ``terminal_equities`` on ``ticker``. Supplied columns overwrite; empty optional
      cells keep the stored value; currency defaults to KES.
    - ``daily_prices`` (ticker, trade_date, close_price, volume; open_price, high_price,
      low_price, turnover_kes, foreign_net_flow_kes): ``terminal_daily_prices`` on
      ``(ticker, trade_date)``. The price columns overwrite every source (``csv_import`` >
      ``nse_market_statistics`` > ``seed``) and set ``source = 'csv_import'``; an empty turnover
      is estimated as volume x close (``turnover_estimated = True``). A non-empty flow sets
      ``foreign_net_flow_kes`` with ``flow_source = 'csv_import'``; an empty one keeps the stored
      flow. A row dated after today in ``settings.tzinfo`` is rejected
      (``future_date:trade_date``).
    - ``fundamentals`` (ticker, period_end_date; period_type, currency and the ten value columns
      of ``terminal_fundamentals``): on ``(ticker, period_end_date, period_type)``; each non-empty
      cell is COALESCE-merged and recorded as ``csv_import`` in ``source_by_field``; a row whose
      currency differs from the stored row's is rejected (``currency_conflict``).
    - ``macro_indicators`` (observation_date; the eight indicator columns):
      ``terminal_macro_indicators`` on ``observation_date``, COALESCE-merged with
      ``source_by_field`` entries ``csv_import``.
    - ``foreign_flows`` (ticker, trade_date, foreign_net_flow_kes): sets only the flow and
      ``flow_source = 'csv_import'`` of existing price rows; rows without one, and rows dated
      after today in ``settings.tzinfo`` (``future_date:trade_date``), are rejected.

    Units: ``*_kes`` whole KES (never thousands), ``*_pct`` percentage points, ``fx_kes_usd`` KES
    per USD, ``fx_reserves_usd_m`` USD millions, share counts and volumes integers up to 10^12.

    The run is recorded in ``terminal_ingestion_runs`` as ``csv_import:<kind>`` with
    ``trigger``; statuses follow the shared thresholds (``succeeded``, ``partial`` when up to 10%
    of rows are rejected, ``layout_changed`` beyond that or for a missing required header,
    ``failed`` for an unreadable or oversize file or a held lock). ``settings`` defaults to
    ``TerminalSettings()``; the engine is built from its ``database_url`` and disposed afterwards.

    Raises:
        ValueError: ``kind`` is not one of :data:`IMPORT_KINDS`, the extension is not ``.csv`` or
            ``.xlsx``, or ``sheet`` is given for a CSV file. Raised before any engine or run row
            exists.
    """
    import_kind = _require_kind(kind)
    file_path = Path(path)
    _require_suffix(file_path)
    resolved = settings if settings is not None else TerminalSettings()
    pipeline = TabularImportPipeline(resolved, kind=import_kind, path=file_path, sheet=sheet)
    engine = build_engine(resolved)
    try:
        return await execute_pipeline(pipeline, engine, trigger=trigger)
    finally:
        await engine.dispose()
