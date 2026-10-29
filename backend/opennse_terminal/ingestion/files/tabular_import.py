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
column's scale. Units: ``*_kes`` whole KES (never thousands), ``*_pct`` percentage points,
``fx_kes_usd`` KES per USD, ``fx_reserves_usd_m`` USD millions, share counts and volumes integers
in 0..``MAX_SHARE_COUNT``. Currency defaults to KES.

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
import re
import stat
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from itertools import islice
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Final, Literal, TypeGuard, get_args

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
    dedupe_by_key,
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
_ISIN_RE: Final = re.compile(r"KE[0-9A-Z]{9}[0-9]")
_CURRENCY_RE: Final = re.compile(r"[A-Z]{3}")
_ISO_DATE_RE: Final = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?: 00:00:00)?")
_DMY_DATE_RE: Final = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_HUNDRED: Final = Decimal(100)
_DEFAULT_PERIOD_TYPE: Final = "FY"
_IN_CHUNK: Final = 5_000
_MISSING_KEYS_SAMPLE: Final = 50


@dataclass(frozen=True, slots=True)
class TabularRecord:
    """One data row of an import file, converted.

    Attributes:
        row_number: The 1-based line of the row in the file or sheet; the header is line 1, so
            the first data row is 2. Reported as ``RejectedRow.index``.
        values: Converted cells of the kind's columns present in the file (``None`` for empty
            cells), plus values derived during validation and reconciliation.
        errors: Conversion error codes, for example ``invalid_decimal:close_price`` or
            ``out_of_range:volume``; a record with any is rejected.
        raw: The original cell text of the kind's columns, recorded for rejected rows.
    """

    row_number: int
    values: Mapping[str, CellValue]
    errors: tuple[str, ...]
    raw: Mapping[str, str]


# ---------------------------------------------------------------------------------------------
# Pure cell helpers
# ---------------------------------------------------------------------------------------------


class _OutOfRangeError(ValueError):
    """A cell that parses but does not fit its column; reported as ``out_of_range:<column>``."""


def normalize_column_name(name: object) -> str:
    """Normalize a header cell to a column name: BOM and zero-width spaces stripped, trimmed,
    lowercased and every run of whitespace replaced by one ``_`` (``"Close Price"`` gives
    ``"close_price"``)."""
    return normalize_header(str(name)).replace(" ", "_")


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


def _convert_row(row_number: int, cells: Mapping[str, str]) -> TabularRecord:
    """Convert the kind's cells of one row; a failed cell becomes ``None`` plus an error code."""
    values: dict[str, CellValue] = {}
    errors: list[str] = []
    for column, text in cells.items():
        try:
            values[column] = _convert_cell(column, text)
        except ValueError as exc:
            values[column] = None
            errors.append(_conversion_error(column, exc))
    return TabularRecord(row_number=row_number, values=values, errors=tuple(errors), raw=cells)


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


def _read_grid(raw: bytes, *, suffix: str, sheet: str | None, source: str) -> list[list[str]]:
    """Read a CSV or ``.xlsx`` file as a grid of cell texts, the header line first.

    Every cell is read as text (``dtype=str``, no NA inference), so nothing passes through
    ``float``; Excel numeric cells arrive as pandas' shortest round-trip string (``95.75``) and
    date cells as ``YYYY-MM-DD 00:00:00``. Empty cells become ``""``. The header is read as a
    data row (``header=None``) because pandas would otherwise rename a repeated header to
    ``name.1``, hiding the duplicate. CSV blank lines are kept, so grid index + 1 is the line.

    Raises:
        SourceLayoutChanged: The named sheet is not in the workbook.
        FetchFailed: The file cannot be decoded or parsed (``unreadable_file:<error>``), or it
            has more than :data:`MAX_ROWS` rows after the header (``too_many_rows``).
    """
    import pandas as pd

    # One row more than allowed after the header, so an oversize file is detected, not cut.
    row_cap = MAX_ROWS + 2
    try:
        if suffix == ".csv":
            frame = pd.read_csv(
                io.BytesIO(raw),
                header=None,
                dtype=str,
                keep_default_na=False,
                skip_blank_lines=False,
                encoding="utf-8-sig",
                nrows=row_cap,
            )
        else:
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
        frame = frame.fillna("")
        grid = [[str(cell) for cell in row] for row in frame.itertuples(index=False, name=None)]
    except IngestionError:
        raise
    except Exception as exc:
        # Untrusted input: any decoder or parser failure (ParserError, EmptyDataError,
        # UnicodeDecodeError, BadZipFile, a corrupt workbook) ends the run failed, named by type.
        raise FetchFailed(source, f"unreadable_file:{type(exc).__name__}") from exc
    if len(grid) > MAX_ROWS + 1:
        raise FetchFailed(source, "too_many_rows")
    return grid


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
        "daily_prices": (
            "ticker",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover_kes",
            "turnover_estimated",
            "foreign_net_flow_kes",
        ),
        "fundamentals": (
            "ticker",
            "period_end_date",
            "period_type",
            "currency",
            *FUNDAMENTAL_LINE_ITEM_COLUMNS,
        ),
        "macro_indicators": ("observation_date", *MACRO_INDICATOR_COLUMNS),
        "foreign_flows": ("ticker", "trade_date", "foreign_net_flow_kes"),
    }
)
"""The columns each kind passes to its ``db.upsert`` writer: one uniform key set per kind, with
``None`` for an empty cell, which the writers treat as "not supplied"."""

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
    the grid, checks the headers and converts the kind's columns; :meth:`validate` mirrors every
    CHECK and length limit of the target table; :meth:`reconcile` collapses repeated keys and
    checks stored data (unknown tickers, ISIN owners, currency conflicts, missing price rows);
    :meth:`upsert` writes through ``opennse_terminal.db.upsert``. Rejections at either check count
    toward the shared thresholds: up to 10% gives ``partial`` (valid rows written), more gives
    ``layout_changed`` (nothing written); a missing required header or a file without data rows
    is ``layout_changed`` too.

    Instances are single-use. Evidence found during the run (``kind``, ``file_name``, ``sheet``,
    ``file_size_bytes``, ``ignored_columns``, ``blank_rows_skipped``, ``duplicates_collapsed``,
    ``flow_missing_keys``) goes into the run's ``details``.

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
    ) -> None:
        super().__init__(settings)
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
        """Read the grid, check the headers and convert the kind's columns of every row.

        Headers are normalized with :func:`normalize_column_name`. Columns the kind does not read
        are ignored and named in ``details.ignored_columns``; the WARNING logged for them carries
        only the run id, the kind and their count, never the file's name or its headers. Rows
        whose kind columns are all empty are skipped (``details.blank_rows_skipped``). Conversion
        failures are collected on each record for :meth:`validate`; nothing is raised per row.

        Raises:
            SourceLayoutChanged: No header row, a repeated header, a missing required header, or
                (``.xlsx``) a missing sheet.
            FetchFailed: The file cannot be read, or it has more than :data:`MAX_ROWS` rows.
        """
        grid = _read_grid(raw, suffix=self._suffix, sheet=self.sheet, source=self.source_url)
        required = list(self.spec.required)
        if not grid:
            raise SourceLayoutChanged(expected=required, found=[], message="the file is empty")
        headers = [normalize_column_name(cell) for cell in grid[0]]
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

        records: list[TabularRecord] = []
        blank = 0
        # The header is line 1, so the first data row is line 2.
        for line, row in enumerate(islice(grid, 1, None), start=2):
            cells = {
                column: row[index] if index < len(row) else ""
                for column, index in positions.items()
            }
            if all(not text.strip() for text in cells.values()):
                blank += 1
                continue
            records.append(_convert_row(line, cells))
        if blank:
            self._details["blank_rows_skipped"] = blank
        return records

    # --- validate ------------------------------------------------------------------------------

    def validate(self, records: Sequence[TabularRecord]) -> ValidationOutcome[TabularRecord]:
        """Reject rows that would break a rule or a database constraint, and derive defaults.

        Every kind: a conversion error (``invalid_decimal:<col>``, ``invalid_integer:<col>``,
        ``invalid_date:<col>``, ``out_of_range:<col>``), an empty required cell
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

        Rejected rows are reported with their line number, the codes joined by ``;`` and their
        raw cells.
        """
        valid: list[TabularRecord] = []
        rejected: list[RejectedRow] = []
        for record in records:
            values = dict(record.values)
            codes = list(record.errors)
            failed = {code.partition(":")[2] for code in record.errors}
            codes.extend(
                f"missing_value:{column}"
                for column in self.spec.required
                if column not in failed and values.get(column) is None
            )
            codes.extend(_check_text(values))
            codes.extend(self._check_kind(values, failed))
            if codes:
                rejected.append(
                    RejectedRow(index=record.row_number, reason=";".join(codes), row=record.raw)
                )
            else:
                valid.append(replace(record, values=values))
        return ValidationOutcome(valid=valid, rejected=rejected)

    def _check_kind(self, values: dict[str, CellValue], failed: set[str]) -> list[str]:
        """The kind's own rules; fills defaults and derived values into ``values``.

        ``failed`` names the columns whose cells did not convert: a row with an unconvertible
        value is reported for that cell, not as ``no_values``.
        """
        codes: list[str] = []
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
          the stored period's (``currency_conflict``), so values never merge across currencies.
        - ``foreign_flows``: a ``(ticker, trade_date)`` without a price row (``no_price_row``).

        These rejections count toward the same thresholds as :meth:`validate`'s.
        """
        unique, collapsed = dedupe_by_key(records, self._key_of)
        ctx.details["duplicates_collapsed"] = collapsed
        if collapsed:
            logger.info("%s import collapsed %d rows that repeat a key", self.kind, collapsed)
        if not unique:
            return ValidationOutcome(valid=[], rejected=[])
        if self.kind == "equities":
            return await _reconcile_equities(ctx.conn, unique)
        if self.kind in ("daily_prices", "fundamentals"):
            return await self._reconcile_tickers(ctx.conn, unique)
        if self.kind == "foreign_flows":
            return await _reconcile_flows(ctx.conn, unique)
        return ValidationOutcome(valid=unique, rejected=[])

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
        stored_currency: dict[tuple[str, date, str], str] = {}
        if self.kind == "fundamentals":
            periods = sorted(
                {
                    (
                        ticker,
                        _required_date(record.values, "period_end_date"),
                        _required_text(record.values, "period_type"),
                    )
                    for record in records
                    if (ticker := _required_text(record.values, "ticker")) in known
                }
            )
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
                    stored_currency[(ticker, period_end, period_type)] = currency
        valid: list[TabularRecord] = []
        rejected: list[RejectedRow] = []
        for record in records:
            values = record.values
            ticker = _required_text(values, "ticker")
            reason: str | None = None
            if ticker not in known:
                reason = "unknown_ticker"
            elif self.kind == "fundamentals":
                key = (
                    ticker,
                    _required_date(values, "period_end_date"),
                    _required_text(values, "period_type"),
                )
                stored = stored_currency.get(key)
                if stored is not None and stored != _required_text(values, "currency"):
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
        commits. The writers batch at most 1,000 rows per statement themselves.

        - ``equities``: ``upsert_rows(..., key=("ticker",), mode="do_update")`` with complete
          rows and ``source = 'csv_import'``.
        - ``daily_prices``: ``upsert_prices(source="csv_import", ingestion_run_id=run_id)``: the
          price columns overwrite any stored source; a non-empty flow sets
          ``foreign_net_flow_kes`` and ``flow_source = 'csv_import'``, an empty one keeps the
          stored flow.
        - ``fundamentals`` and ``macro_indicators``: COALESCE merges recording ``csv_import`` in
          ``source_by_field`` for each non-empty cell.
        - ``foreign_flows``: ``update_flows(flow_source="csv_import")``, touching only the flow
          columns; keys that lost their price row since :meth:`reconcile` are logged and
          recorded in ``details.flow_missing_keys``.

        Returns the number of rows inserted or updated. Re-importing a file rewrites identical
        values, so the stored state is unchanged.
        """
        columns = _WRITE_COLUMNS[self.kind]
        rows = [{column: record.values.get(column) for column in columns} for record in records]
        if self.kind == "equities":
            for row in rows:
                row["source"] = _SOURCE
            return await upsert_rows(conn, TerminalEquity, rows, key=("ticker",), mode="do_update")
        if self.kind == "daily_prices":
            return await upsert_prices(conn, rows, source=_SOURCE, ingestion_run_id=run_id)
        if self.kind == "fundamentals":
            return await upsert_fundamentals(conn, rows, source=_SOURCE)
        if self.kind == "macro_indicators":
            return await upsert_macro(conn, rows, source=_SOURCE)
        outcome = await update_flows(conn, rows, flow_source=_SOURCE)
        if outcome.missing_keys:
            logger.warning(
                "foreign_flows import found %d keys without a price row at write time",
                len(outcome.missing_keys),
            )
            self._details["flow_missing_keys"] = {
                "count": len(outcome.missing_keys),
                "sample": [list(key) for key in outcome.missing_keys[:_MISSING_KEYS_SAMPLE]],
            }
        return outcome.rows_updated


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
    conn: AsyncConnection, records: list[TabularRecord]
) -> ValidationOutcome[TabularRecord]:
    """Reject ISIN conflicts and complete each row with the stored values it leaves empty."""
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
        completed = dict(values)
        completed["isin"] = isin if isin is not None else stored_isin
        completed["free_float_pct"] = free_float if free_float is not None else stored_free_float
        completed["currency"] = currency if currency is not None else stored_currency
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
      flow.
    - ``fundamentals`` (ticker, period_end_date; period_type, currency and the ten value columns
      of ``terminal_fundamentals``): on ``(ticker, period_end_date, period_type)``; each non-empty
      cell is COALESCE-merged and recorded as ``csv_import`` in ``source_by_field``; a row whose
      currency differs from the stored row's is rejected (``currency_conflict``).
    - ``macro_indicators`` (observation_date; the eight indicator columns):
      ``terminal_macro_indicators`` on ``observation_date``, COALESCE-merged with
      ``source_by_field`` entries ``csv_import``.
    - ``foreign_flows`` (ticker, trade_date, foreign_net_flow_kes): sets only the flow and
      ``flow_source = 'csv_import'`` of existing price rows; rows without one are rejected.

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
