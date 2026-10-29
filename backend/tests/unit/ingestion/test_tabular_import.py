"""Tests for the CSV/Excel importer (``opennse_terminal.ingestion.files.tabular_import``).

Every case runs without PostgreSQL or a network. Operator-style CSV and ``.xlsx`` files are written
into ``tmp_path`` (the workbooks with openpyxl) and the importer runs stage by stage:

* ``fetch`` with a ``RunContext`` whose HTTP client fails the test if it is touched, then
  ``parse`` and ``validate``;
* ``reconcile`` on a read-only connection double that answers each ``select()`` with scripted
  rows and keeps the statement, so its tables and bound parameters can be checked;
* ``upsert`` with the ``db.upsert`` writers replaced by recorders that keep their signatures;
* ``import_file`` with the engine factory and ``execute_pipeline`` replaced.

The shared row thresholds are applied to the counts the stages produce. Writing rows, the run row,
the advisory lock and precedence over stored rows need PostgreSQL and are not exercised here.

Expected values are literals from the importer's contract, never recomputed with the code under
test. Amounts are ``Decimal`` and volumes and share counts ``int``.
"""

import asyncio
import contextlib
import csv
import io
import logging
import os
import re
import uuid
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from openpyxl import Workbook
from sqlalchemy import Join
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.upsert import FlowUpdateResult
from opennse_terminal.ingestion.base import (
    FetchFailed,
    RejectedRow,
    RunContext,
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    ValidationOutcome,
    classify_exception,
    evaluate_thresholds,
)
from opennse_terminal.ingestion.files import tabular_import
from opennse_terminal.ingestion.files.tabular_import import (
    IMPORT_KINDS,
    KIND_SPECS,
    MAX_FILE_BYTES,
    MAX_ROWS,
    SUPPORTED_SUFFIXES,
    ImportKind,
    TabularImportPipeline,
    TabularRecord,
    import_file,
    normalize_column_name,
    parse_date_cell,
    parse_decimal_cell,
    parse_int_cell,
)
from opennse_terminal.models import TerminalEquity

LOGGER = "opennse_terminal.ingestion.files.tabular_import"
UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://nobody:x@127.0.0.1:1/none"
RUN_ID = uuid.UUID("2b7e1516-28ae-4d2a-a6f7-15880cf4f3c1")
SESSION = date(2026, 9, 28)
NEXT_SESSION = date(2026, 9, 29)
PRICE_HEADER = ("Ticker", "Trade Date", "Close Price", "Volume")

REQUIRED_COLUMNS: Mapping[ImportKind, tuple[str, ...]] = {
    "equities": ("ticker", "company_name", "sector", "shares_outstanding"),
    "daily_prices": ("ticker", "trade_date", "close_price", "volume"),
    "fundamentals": ("ticker", "period_end_date"),
    "macro_indicators": ("observation_date",),
    "foreign_flows": ("ticker", "trade_date", "foreign_net_flow_kes"),
}
FUNDAMENTAL_VALUE_COLUMNS = (
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
MACRO_COLUMNS = (
    "inflation_yoy_pct",
    "cbr_rate_pct",
    "interbank_rate_pct",
    "tbill_91d_pct",
    "tbill_182d_pct",
    "tbill_364d_pct",
    "fx_kes_usd",
    "fx_reserves_usd_m",
)
OPTIONAL_COLUMNS: Mapping[ImportKind, tuple[str, ...]] = {
    "equities": ("isin", "free_float_pct", "currency"),
    "daily_prices": (
        "open_price",
        "high_price",
        "low_price",
        "turnover_kes",
        "foreign_net_flow_kes",
    ),
    "fundamentals": ("period_type", "currency", *FUNDAMENTAL_VALUE_COLUMNS),
    "macro_indicators": MACRO_COLUMNS,
    "foreign_flows": (),
}
CONFLICT_KEYS: Mapping[ImportKind, tuple[str, ...]] = {
    "equities": ("ticker",),
    "daily_prices": ("ticker", "trade_date"),
    "fundamentals": ("ticker", "period_end_date", "period_type"),
    "macro_indicators": ("observation_date",),
    "foreign_flows": ("ticker", "trade_date"),
}
KINDS: tuple[ImportKind, ...] = tuple(REQUIRED_COLUMNS)
INTEGER_COLUMNS = frozenset({"volume", "shares_outstanding"})
WRITE_COLUMNS: Mapping[ImportKind, tuple[str, ...]] = {
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
        *FUNDAMENTAL_VALUE_COLUMNS,
    ),
    "macro_indicators": ("observation_date", *MACRO_COLUMNS),
    "foreign_flows": ("ticker", "trade_date", "foreign_net_flow_kes"),
}


# --- Settings, files and stages -------------------------------------------------------------


def _hermetic_settings(**overrides: Any) -> TerminalSettings:
    """Settings with no ``.env`` file and every field explicit, so the ``TERMINAL_*`` variables of
    the process environment cannot leak in; the database URL names a closed port."""
    fields: dict[str, Any] = {
        "database_url": UNREACHABLE_DATABASE_URL,
        "default_owner_key": "local",
        "timezone": "Africa/Nairobi",
        "fundamentals_lag_days": 90,
        "scraper_contact": "tests@example.invalid",
        "scraper_min_interval_seconds": 2.0,
        "scraper_timeout_seconds": 30,
        "scraper_max_retries": 3,
        "pdf_max_bytes": 20971520,
        "pdf_max_pages": 400,
        "pdf_max_concurrency": 2,
    }
    fields.update(overrides)
    return TerminalSettings(_env_file=None, **fields)


def _csv_cell(value: object) -> str:
    """A cell as an operator's CSV export writes it: ISO dates, ``None`` as an empty cell."""
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _write_csv(path: Path, rows: Sequence[Sequence[object]], *, encoding: str = "utf-8") -> Path:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow([_csv_cell(value) for value in row])
    path.write_text(buffer.getvalue(), encoding=encoding)
    return path


def _write_xlsx(path: Path, sheets: Mapping[str, Sequence[Sequence[object]]]) -> Path:
    """A workbook with one worksheet per entry, in order; ``None`` leaves a cell empty."""
    workbook = Workbook()
    workbook.remove(workbook.active)
    for title, rows in sheets.items():
        worksheet = workbook.create_sheet(title)
        for row in rows:
            worksheet.append(list(row))
    workbook.save(path)
    return path


def _write_table(path: Path, rows: Sequence[Sequence[object]]) -> Path:
    """Write ``rows`` as CSV or as the single sheet of a workbook, by ``path``'s extension."""
    if path.suffix == ".xlsx":
        return _write_xlsx(path, {"Data": rows})
    return _write_csv(path, rows)


class _NoNetwork:
    """The run's HTTP client: the importer reads local files only, so any use fails the test."""

    def __getattr__(self, name: str) -> object:
        raise AssertionError(f"the importer used the HTTP client: {name}")


@dataclass(frozen=True, slots=True)
class _Rows:
    """The two result accessors ``reconcile`` reads: ``scalars()`` and ``tuples()``."""

    rows: tuple[tuple[object, ...], ...]

    def scalars(self) -> Iterator[object]:
        return (row[0] for row in self.rows)

    def tuples(self) -> Iterator[tuple[object, ...]]:
        return iter(self.rows)


class _ScriptedConnection:
    """A read-only data connection: each ``execute`` is answered with the next scripted rows and
    its statement kept; a query beyond the script fails the test."""

    def __init__(self, *results: Sequence[tuple[object, ...]]) -> None:
        self._results = deque(tuple(rows) for rows in results)
        self.statements: list[Any] = []

    async def execute(self, statement: Any) -> _Rows:
        self.statements.append(statement)
        if not self._results:
            raise AssertionError(f"unscripted query: {statement}")
        return _Rows(self._results.popleft())

    @property
    def unanswered(self) -> int:
        """Scripted results no query asked for."""
        return len(self._results)


def _from_names(froms: Sequence[Any]) -> list[str]:
    """The names of the tables and ``VALUES`` lists in ``froms``, each join expanded in order."""
    names: list[str] = []
    for source in froms:
        if isinstance(source, Join):
            names.extend(_from_names([source.left, source.right]))
        else:
            names.append(source.name)
    return names


def _query(statement: Any) -> tuple[list[str], list[object]]:
    """The tables and ``VALUES`` lists a ``select()`` reads and its bound parameter values,
    compiled for PostgreSQL."""
    compiled = statement.compile(dialect=postgresql.dialect())
    return _from_names(statement.get_final_froms()), list(compiled.params.values())


def _open(
    kind: ImportKind,
    path: Path,
    *,
    sheet: str | None = None,
    conn: _ScriptedConnection | None = None,
) -> tuple[TabularImportPipeline, RunContext]:
    """A fresh pipeline and the run context ``execute_pipeline`` would hand it."""
    pipeline = TabularImportPipeline(_hermetic_settings(), kind=kind, path=path, sheet=sheet)
    ctx = RunContext(
        run_id=RUN_ID,
        pipeline=pipeline.run_name,
        trigger="cli",
        settings=pipeline.settings,
        conn=cast(AsyncConnection, conn if conn is not None else _ScriptedConnection()),
        http=cast(Any, _NoNetwork()),
    )
    return pipeline, ctx


@dataclass(frozen=True, slots=True)
class _Staged:
    """A pipeline after ``fetch``, ``parse`` and ``validate``."""

    pipeline: TabularImportPipeline
    ctx: RunContext
    records: list[TabularRecord]
    outcome: ValidationOutcome[TabularRecord]


async def _stage(
    kind: ImportKind,
    path: Path,
    *,
    sheet: str | None = None,
    conn: _ScriptedConnection | None = None,
) -> _Staged:
    pipeline, ctx = _open(kind, path, sheet=sheet, conn=conn)
    records = pipeline.parse(await pipeline.fetch(ctx))
    return _Staged(pipeline=pipeline, ctx=ctx, records=records, outcome=pipeline.validate(records))


def _values(records: Sequence[TabularRecord]) -> list[dict[str, object]]:
    return [dict(record.values) for record in records]


def _assert_exact_types(records: Sequence[TabularRecord]) -> None:
    """Amounts are ``Decimal`` and counts ``int``: never ``float``, and never a ``Decimal`` count,
    which would compare equal to the expected ``int``."""
    for record in records:
        for column, value in record.values.items():
            assert not isinstance(value, float), column
            if value is None:
                continue
            if column in INTEGER_COLUMNS:
                assert type(value) is int, column
            elif column.endswith(("_kes", "_price", "_pct")) or column.startswith("fx_"):
                assert type(value) is Decimal, column


# --- One small operator file per kind --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Sample:
    """A kind's header and two rows as spreadsheet cells (``date`` cells, ``int`` counts, text
    amounts, ``None`` empty), and the converted values ``validate`` accepts for each row."""

    header: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    expected: tuple[Mapping[str, object], ...]


SAMPLES: Mapping[ImportKind, _Sample] = {
    "equities": _Sample(
        header=(
            "Ticker",
            "Company Name",
            "Sector",
            "Shares Outstanding",
            "ISIN",
            "Free Float Pct",
            "Currency",
        ),
        rows=(
            (
                "scom",
                "Safaricom PLC",
                "Telecommunication",
                40_065_428_000,
                "ke1000001402",
                "35.0",
                "kes",
            ),
            ("EQTY", "Equity Group Holdings PLC", "Banking", 3_773_674_802, None, None, None),
        ),
        expected=(
            {
                "ticker": "SCOM",
                "company_name": "Safaricom PLC",
                "sector": "Telecommunication",
                "shares_outstanding": 40_065_428_000,
                "isin": "KE1000001402",
                "free_float_pct": Decimal("35.0"),
                "currency": "KES",
            },
            {
                "ticker": "EQTY",
                "company_name": "Equity Group Holdings PLC",
                "sector": "Banking",
                "shares_outstanding": 3_773_674_802,
                "isin": None,
                "free_float_pct": None,
                "currency": None,
            },
        ),
    ),
    "daily_prices": _Sample(
        header=(
            "Ticker",
            "Trade Date",
            "Open Price",
            "High Price",
            "Low Price",
            "Close Price",
            "Volume",
            "Turnover KES",
            "Foreign Net Flow KES",
        ),
        rows=(
            (
                "SCOM",
                SESSION,
                "28.10",
                "28.60",
                "27.95",
                "28.45",
                12_345_678,
                "351,234,539.10",
                "(1,250,000.00)",
            ),
            # No turnover: volume x close, 987,241 x 10.605 = 10,469,690.805, rounds half up.
            ("EQTY", SESSION, None, None, None, "10.605", 987_241, None, None),
        ),
        expected=(
            {
                "ticker": "SCOM",
                "trade_date": SESSION,
                "open_price": Decimal("28.10"),
                "high_price": Decimal("28.60"),
                "low_price": Decimal("27.95"),
                "close_price": Decimal("28.45"),
                "volume": 12_345_678,
                "turnover_kes": Decimal("351234539.10"),
                "foreign_net_flow_kes": Decimal("-1250000.00"),
                "turnover_estimated": False,
            },
            {
                "ticker": "EQTY",
                "trade_date": SESSION,
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "close_price": Decimal("10.605"),
                "volume": 987_241,
                "turnover_kes": Decimal("10469690.81"),
                "foreign_net_flow_kes": None,
                "turnover_estimated": True,
            },
        ),
    ),
    "fundamentals": _Sample(
        header=(
            "Ticker",
            "Period End Date",
            "Period Type",
            "Currency",
            "Revenue KES",
            "EBITDA KES",
            "Net Income KES",
            "Total Assets KES",
            "Total Equity KES",
            "Total Debt KES",
            "Cash And Equivalents KES",
            "Operating Cash Flow KES",
            "DPS KES",
            "Shares Outstanding",
        ),
        rows=(
            (
                "SCOM",
                date(2026, 3, 31),
                "fy",
                None,
                "388,690,000,000",
                "190,000,000,000",
                "69,800,000,000",
                None,
                None,
                None,
                None,
                None,
                "1.2",
                40_065_428_000,
            ),
            (
                "EQTY",
                date(2025, 12, 31),
                None,
                "kes",
                "1,234,567.885",
                None,
                "(2,500)",
                None,
                None,
                None,
                None,
                None,
                "0.33345",
                None,
            ),
        ),
        expected=(
            {
                "ticker": "SCOM",
                "period_end_date": date(2026, 3, 31),
                "period_type": "FY",
                "currency": "KES",
                "revenue_kes": Decimal("388690000000"),
                "ebitda_kes": Decimal("190000000000"),
                "net_income_kes": Decimal("69800000000"),
                "total_assets_kes": None,
                "total_equity_kes": None,
                "total_debt_kes": None,
                "cash_and_equivalents_kes": None,
                "operating_cash_flow_kes": None,
                "dps_kes": Decimal("1.2"),
                "shares_outstanding": 40_065_428_000,
            },
            {
                "ticker": "EQTY",
                "period_end_date": date(2025, 12, 31),
                "period_type": "FY",
                "currency": "KES",
                "revenue_kes": Decimal("1234567.89"),
                "ebitda_kes": None,
                "net_income_kes": Decimal("-2500.00"),
                "total_assets_kes": None,
                "total_equity_kes": None,
                "total_debt_kes": None,
                "cash_and_equivalents_kes": None,
                "operating_cash_flow_kes": None,
                "dps_kes": Decimal("0.3335"),
                "shares_outstanding": None,
            },
        ),
    ),
    "macro_indicators": _Sample(
        header=(
            "Observation Date",
            "Inflation YoY Pct",
            "CBR Rate Pct",
            "Interbank Rate Pct",
            "TBill 91D Pct",
            "TBill 182D Pct",
            "TBill 364D Pct",
            "FX KES USD",
            "FX Reserves USD M",
        ),
        rows=(
            (
                date(2026, 9, 30),
                "6.8%",
                None,
                "12.85",
                "15.72",
                "16.58",
                "16.84",
                "129.20",
                "7,850.00",
            ),
            (date(2026, 8, 31), "6.6", None, None, None, None, None, None, None),
        ),
        expected=(
            {
                "observation_date": date(2026, 9, 30),
                "inflation_yoy_pct": Decimal("6.8"),
                "cbr_rate_pct": None,
                "interbank_rate_pct": Decimal("12.85"),
                "tbill_91d_pct": Decimal("15.72"),
                "tbill_182d_pct": Decimal("16.58"),
                "tbill_364d_pct": Decimal("16.84"),
                "fx_kes_usd": Decimal("129.20"),
                "fx_reserves_usd_m": Decimal("7850.00"),
            },
            {
                "observation_date": date(2026, 8, 31),
                "inflation_yoy_pct": Decimal("6.6"),
                "cbr_rate_pct": None,
                "interbank_rate_pct": None,
                "tbill_91d_pct": None,
                "tbill_182d_pct": None,
                "tbill_364d_pct": None,
                "fx_kes_usd": None,
                "fx_reserves_usd_m": None,
            },
        ),
    ),
    "foreign_flows": _Sample(
        header=("Ticker", "Trade Date", "Foreign Net Flow KES"),
        rows=(
            ("SCOM", SESSION, "(1,250,000.00)"),
            ("EQTY", SESSION, "3,400,500.565"),
        ),
        expected=(
            {
                "ticker": "SCOM",
                "trade_date": SESSION,
                "foreign_net_flow_kes": Decimal("-1250000.00"),
            },
            {
                "ticker": "EQTY",
                "trade_date": SESSION,
                "foreign_net_flow_kes": Decimal("3400500.57"),
            },
        ),
    ),
}


def _write_sample(tmp_path: Path, kind: ImportKind, suffix: str = ".csv") -> Path:
    sample = SAMPLES[kind]
    return _write_table(tmp_path / f"{kind}{suffix}", [sample.header, *sample.rows])


# --- Declarations ----------------------------------------------------------------------------


def test_import_kinds_are_the_five_kinds_in_cli_order() -> None:
    assert IMPORT_KINDS == (
        "equities",
        "daily_prices",
        "fundamentals",
        "macro_indicators",
        "foreign_flows",
    )


@pytest.mark.parametrize("kind", KINDS)
def test_each_kind_declares_its_required_and_optional_columns_and_key(kind: ImportKind) -> None:
    spec = KIND_SPECS[kind]
    assert spec.kind == kind
    assert spec.required == REQUIRED_COLUMNS[kind]
    assert spec.optional == OPTIONAL_COLUMNS[kind]
    assert spec.key == CONFLICT_KEYS[kind]
    assert spec.columns == REQUIRED_COLUMNS[kind] + OPTIONAL_COLUMNS[kind]


def test_file_limits_and_formats() -> None:
    assert MAX_FILE_BYTES == 52_428_800
    assert MAX_ROWS == 1_000_000
    assert SUPPORTED_SUFFIXES == (".csv", ".xlsx")


# --- Cell helpers ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "column"),
    [
        ("Close Price", "close_price"),
        ("  Trade Date ", "trade_date"),
        ("VOLUME", "volume"),
        ("\ufeffTicker", "ticker"),
        ("Foreign\u00a0 Net\nFlow KES", "foreign_net_flow_kes"),
        (95, "95"),
    ],
)
def test_normalize_column_name(header: object, column: str) -> None:
    assert normalize_column_name(header) == column


@pytest.mark.parametrize(
    ("text", "column", "expected", "scale"),
    [
        ("95.75", "close_price", Decimal("95.75"), 4),
        ("(1,250,000.00)", "foreign_net_flow_kes", Decimal("-1250000.00"), 2),
        ("1,234.5", "revenue_kes", Decimal("1234.5"), 2),
        (" 6.80 % ", "inflation_yoy_pct", Decimal("6.80"), 4),
        ("10.60505", "close_price", Decimal("10.6051"), 4),
        ("1234.565", "turnover_kes", Decimal("1234.57"), 2),
        ("7,850.005", "fx_reserves_usd_m", Decimal("7850.01"), 2),
        ("0.33345", "dps_kes", Decimal("0.3335"), 4),
        ("129.20", "fx_kes_usd", Decimal("129.2"), 4),
        ("99999999999999.99994", "close_price", Decimal("99999999999999.9999"), 4),
        # 33 significant digits in parentheses: rounded once, HALF_UP from the exact value, not
        # first to 28 digits (which would give -1.01).
        ("(1.00499999999999999999999999999999)", "foreign_net_flow_kes", Decimal("-1.00"), 2),
    ],
)
def test_parse_decimal_cell_is_exact_and_rounds_half_up_to_the_column_scale(
    text: str, column: str, expected: Decimal, scale: int
) -> None:
    value = parse_decimal_cell(text, column)
    assert value == expected
    assert type(value) is Decimal
    assert value.as_tuple().exponent == -scale


@pytest.mark.parametrize(
    ("text", "column"),
    [
        ("", "close_price"),
        ("   ", "turnover_kes"),
        ("-", "revenue_kes"),
        ("N/A", "fx_kes_usd"),
        ("—", "dps_kes"),
        (None, "close_price"),
    ],
)
def test_parse_decimal_cell_reads_empty_cells_as_null(text: str | None, column: str) -> None:
    assert parse_decimal_cell(text, column) is None


@pytest.mark.parametrize(
    ("text", "column", "expected"),
    [
        ("(0.001)", "turnover_kes", Decimal("0.00")),
        ("-0.00004", "close_price", Decimal("0.0000")),
        # Exponents below the decimal context's range round to zero without a decimal signal.
        ("(1e-1000000)", "close_price", Decimal("0.0000")),
        ("-1e-999999999999999999", "turnover_kes", Decimal("0.00")),
    ],
)
def test_parse_decimal_cell_never_keeps_a_negative_zero(
    text: str, column: str, expected: Decimal
) -> None:
    value = parse_decimal_cell(text, column)
    assert value == expected
    assert value is not None
    assert not value.is_signed()


@pytest.mark.parametrize(
    ("text", "column", "message"),
    [
        ("abc", "close_price", "not a number: 'abc'"),
        ("NaN", "close_price", "not a number: 'NaN'"),
        ("12.5%", "close_price", "a percent sign is accepted only on *_pct columns"),
        ("100000000000000", "close_price", "close_price must be below 10^14 in magnitude"),
        ("99999999999999.99995", "close_price", "close_price must be below 10^14 in magnitude"),
        ("1e30", "turnover_kes", "turnover_kes must be below 10^22 in magnitude"),
        ("100000", "inflation_yoy_pct", "inflation_yoy_pct must be below 10^5 in magnitude"),
        # Exponents beyond the decimal context's range are out of range, not a decimal signal.
        ("1e1000000", "close_price", "close_price must be below 10^14 in magnitude"),
        ("(1e1000000)", "turnover_kes", "turnover_kes must be below 10^22 in magnitude"),
        ("-1e1000000", "foreign_net_flow_kes", "foreign_net_flow_kes must be below 10^22"),
        ("1e999999999999999999", "inflation_yoy_pct", "inflation_yoy_pct must be below 10^5"),
        ("(" + "9" * 35 + ")", "revenue_kes", "revenue_kes must be below 10^22 in magnitude"),
        ("12", "ticker", "'ticker' is not a decimal import column"),
    ],
)
def test_parse_decimal_cell_rejects_non_numbers_and_column_overflow(
    text: str, column: str, message: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        parse_decimal_cell(text, column)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1,888,186", 1_888_186),
        (" 987241 ", 987_241),
        ("1000000000000", 1_000_000_000_000),
        ("1234.0", 1_234),
        ("0", 0),
    ],
)
def test_parse_int_cell_reads_whole_counts_up_to_ten_to_the_twelfth(
    text: str, expected: int
) -> None:
    value = parse_int_cell(text)
    assert value == expected
    assert type(value) is int


@pytest.mark.parametrize("text", ["", "-", None])
def test_parse_int_cell_reads_empty_cells_as_null(text: str | None) -> None:
    assert parse_int_cell(text) is None


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("1000000000001", "1000000000001 is outside 0..1000000000000"),
        ("-5", "-5 is outside 0..1000000000000"),
        ("(100)", "-100 is outside 0..1000000000000"),
        ("12.5", "not a whole number: '12.5'"),
        ("abc", "not an integer: 'abc'"),
        # Exponents beyond the decimal context's range are out of range, not a decimal signal.
        ("1e1000000", "1E+1000000 is outside 0..1000000000000"),
        ("(1e1000000)", "-1E+1000000 is outside 0..1000000000000"),
        ("(1e-1000000)", "-1E-1000000 is outside 0..1000000000000"),
        ("1e-999999999999999999", "not a whole number: '1e-999999999999999999'"),
        ("(" + "1" * 35 + ")", "-11111111111111111111111111111111111 is outside 0..1000000000000"),
    ],
)
def test_parse_int_cell_rejects_fractions_negatives_and_counts_above_the_cap(
    text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        parse_int_cell(text)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-09-28", date(2026, 9, 28)),
        ("28/09/2026", date(2026, 9, 28)),
        ("2026-09-28 00:00:00", date(2026, 9, 28)),
        (" 8/9/2026 ", date(2026, 9, 8)),
    ],
)
def test_parse_date_cell_reads_iso_day_month_year_and_excel_midnight(
    text: str, expected: date
) -> None:
    assert parse_date_cell(text) == expected


@pytest.mark.parametrize("text", ["", "n/a", None])
def test_parse_date_cell_reads_empty_cells_as_null(text: str | None) -> None:
    assert parse_date_cell(text) is None


@pytest.mark.parametrize(
    "text",
    ["2026/28/09", "2026-02-30", "31/09/2026", "2026-09-28 13:45:00", "28-09-2026", "20260928"],
)
def test_parse_date_cell_rejects_other_forms_and_impossible_dates(text: str) -> None:
    with pytest.raises(ValueError):
        parse_date_cell(text)


# --- Construction and the entry point --------------------------------------------------------

BAD_ARGUMENTS = [
    pytest.param("prices", "prices.csv", None, "unknown import kind 'prices'", id="unknown-kind"),
    pytest.param(
        "daily_prices", "prices.txt", None, "unsupported file type '.txt'", id="text-file"
    ),
    pytest.param(
        "daily_prices", "prices", None, "unsupported file type '(none)'", id="no-extension"
    ),
    pytest.param(
        "daily_prices",
        "prices.csv",
        "Prices",
        "a sheet name applies only to .xlsx files, not 'prices.csv'",
        id="sheet-for-csv",
    ),
]


@pytest.mark.parametrize(("kind", "file_name", "sheet", "message"), BAD_ARGUMENTS)
def test_pipeline_rejects_bad_arguments(
    tmp_path: Path, kind: Any, file_name: str, sheet: str | None, message: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        TabularImportPipeline(
            _hermetic_settings(), kind=kind, path=tmp_path / file_name, sheet=sheet
        )


@pytest.mark.parametrize(("kind", "file_name", "sheet", "message"), BAD_ARGUMENTS)
async def test_import_file_rejects_bad_arguments_before_building_an_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kind: str,
    file_name: str,
    sheet: str | None,
    message: str,
) -> None:
    built: list[TerminalSettings] = []
    monkeypatch.setattr(tabular_import, "build_engine", built.append)
    with pytest.raises(ValueError, match=re.escape(message)):
        await import_file(kind, tmp_path / file_name, sheet, settings=_hermetic_settings())
    assert built == []


class _EngineDouble:
    """The engine ``import_file`` builds; only its disposal is observable here."""

    def __init__(self) -> None:
        self.disposals = 0

    async def dispose(self) -> None:
        self.disposals += 1


@dataclass(slots=True)
class _EntryPoint:
    """Recorders standing in for ``build_engine`` and ``execute_pipeline`` in ``import_file``."""

    engine: _EngineDouble = field(default_factory=_EngineDouble)
    built: list[TerminalSettings] = field(default_factory=list)
    runs: list[tuple[Any, Any, str]] = field(default_factory=list)
    failure: Exception | None = None


RESULT = RunResult(
    run_id=RUN_ID,
    pipeline="csv_import:daily_prices",
    status=RunStatus.SUCCEEDED,
    rows_fetched=2,
    rows_written=2,
    rows_rejected=0,
    error_message=None,
    details={},
)


@pytest.fixture
def entry_point(monkeypatch: pytest.MonkeyPatch) -> _EntryPoint:
    recorded = _EntryPoint()

    def build_engine(settings: TerminalSettings) -> _EngineDouble:
        recorded.built.append(settings)
        return recorded.engine

    async def execute_pipeline(pipeline: Any, engine: Any, *, trigger: str) -> RunResult:
        recorded.runs.append((pipeline, engine, trigger))
        if recorded.failure is not None:
            raise recorded.failure
        return RESULT

    monkeypatch.setattr(tabular_import, "build_engine", build_engine)
    monkeypatch.setattr(tabular_import, "execute_pipeline", execute_pipeline)
    return recorded


@pytest.mark.parametrize(
    ("trigger_option", "trigger"),
    [pytest.param({}, "cli", id="default-cli"), pytest.param({"trigger": "api"}, "api", id="api")],
)
async def test_import_file_runs_one_pipeline_and_disposes_its_engine(
    tmp_path: Path,
    entry_point: _EntryPoint,
    trigger_option: dict[str, Any],
    trigger: str,
) -> None:
    settings = _hermetic_settings()
    path = tmp_path / "prices.xlsx"
    result = await import_file(
        "daily_prices", str(path), "Prices", settings=settings, **trigger_option
    )
    assert result is RESULT
    assert len(entry_point.built) == 1
    assert entry_point.built[0] is settings
    [(pipeline, engine, run_trigger)] = entry_point.runs
    assert isinstance(pipeline, TabularImportPipeline)
    assert (pipeline.kind, pipeline.path, pipeline.sheet, pipeline.run_name) == (
        "daily_prices",
        path,
        "Prices",
        "csv_import:daily_prices",
    )
    assert pipeline.settings is settings
    assert engine is entry_point.engine
    assert run_trigger == trigger
    assert entry_point.engine.disposals == 1


async def test_import_file_disposes_its_engine_when_the_run_raises(
    tmp_path: Path, entry_point: _EntryPoint
) -> None:
    entry_point.failure = RuntimeError("run row could not be written")
    with pytest.raises(RuntimeError, match="run row could not be written"):
        await import_file("foreign_flows", tmp_path / "flows.csv", settings=_hermetic_settings())
    assert len(entry_point.runs) == 1
    assert entry_point.engine.disposals == 1


@pytest.mark.parametrize(
    ("kind", "run_name"),
    [
        ("equities", "csv_import:equities"),
        ("daily_prices", "csv_import:daily_prices"),
        ("fundamentals", "csv_import:fundamentals"),
        ("macro_indicators", "csv_import:macro_indicators"),
        ("foreign_flows", "csv_import:foreign_flows"),
    ],
)
def test_run_name_is_csv_import_and_the_kind(
    tmp_path: Path, kind: ImportKind, run_name: str
) -> None:
    pipeline = TabularImportPipeline(_hermetic_settings(), kind=kind, path=tmp_path / "file.csv")
    assert pipeline.run_name == run_name
    assert pipeline.name == "csv_import"
    assert pipeline.min_rows == 1


def test_pipeline_records_the_absolute_path_and_treats_an_empty_sheet_as_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    pipeline = TabularImportPipeline(
        _hermetic_settings(), kind="daily_prices", path=Path("Prices.XLSX"), sheet=""
    )
    assert pipeline.source_url == str(tmp_path.resolve() / "Prices.XLSX")
    assert pipeline.sheet is None


# --- Fetch -----------------------------------------------------------------------------------


async def test_fetch_reads_the_file_and_records_its_evidence(tmp_path: Path) -> None:
    path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, ("SCOM", SESSION, "28.45", 1_000)])
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    assert raw == path.read_bytes()
    assert ctx.details == {
        "kind": "daily_prices",
        "file_name": "prices.csv",
        "sheet": None,
        "file_size_bytes": path.stat().st_size,
    }


NO_FIFOS = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="the platform has no FIFOs")


def _release_fifo_readers(path: Path) -> None:
    """Open the FIFO at ``path`` for writing and close it, so a reader blocked opening it returns.

    A regression that opened the path blocking would otherwise leave its worker thread waiting
    for a writer forever, and the test process could not exit after the timeout failed the test.
    """
    if not path.is_fifo():
        return
    with contextlib.suppress(OSError):  # ENXIO: no reader is waiting
        os.close(os.open(path, os.O_WRONLY | os.O_NONBLOCK))


class _OsView:
    """``os`` as the importer sees it, with some functions replaced.

    Patched in as ``tabular_import.os`` only, so no other caller of ``os`` meets the hooks.
    """

    def __init__(self, **hooks: Callable[..., Any]) -> None:
        self._hooks = hooks

    def __getattr__(self, name: str) -> Any:
        return self._hooks[name] if name in self._hooks else getattr(os, name)


def _after_fstat(
    monkeypatch: pytest.MonkeyPatch, action: Callable[[], None]
) -> list[os.stat_result]:
    """Run ``action`` once, right after the importer's first ``os.fstat``.

    Returns the checks seen; the check returned to the importer is the one taken before
    ``action`` ran.
    """
    seen: list[os.stat_result] = []

    def fstat(fd: int) -> os.stat_result:
        info = os.fstat(fd)
        if not seen:
            seen.append(info)
            action()
        return info

    monkeypatch.setattr(tabular_import, "os", _OsView(fstat=fstat))
    return seen


@pytest.mark.parametrize(
    ("setup", "reason"),
    [
        pytest.param("missing", "file_not_found", id="missing"),
        pytest.param("directory", "not_a_file", id="directory"),
        pytest.param("fifo", "not_a_file", id="fifo", marks=NO_FIFOS),
        pytest.param(
            "character_device",
            "not_a_file",
            id="character-device",
            marks=pytest.mark.skipif(
                not Path(os.devnull).is_char_device(), reason="no character device to link"
            ),
        ),
    ],
)
async def test_fetch_fails_for_a_path_that_is_not_a_readable_file(
    tmp_path: Path, setup: str, reason: str
) -> None:
    path = tmp_path / "prices.csv"
    if setup == "directory":
        path.mkdir()
    elif setup == "fifo":
        os.mkfifo(path)
    elif setup == "character_device":
        path.symlink_to(os.devnull)
    pipeline, ctx = _open("daily_prices", path)
    try:
        with pytest.raises(FetchFailed) as failure:
            # Bounded: a FIFO or device opened blocking would wait for a writer forever.
            await asyncio.wait_for(pipeline.fetch(ctx), timeout=5)
    finally:
        _release_fifo_readers(path)
    assert (failure.value.url, failure.value.reason) == (str(path.resolve()), reason)


@NO_FIFOS
async def test_fetch_reads_the_checked_descriptor_when_the_path_is_replaced_by_a_fifo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, ("SCOM", SESSION, "28.45", 1_000)])
    original = path.read_bytes()

    def replace_with_fifo() -> None:
        path.rename(tmp_path / "moved.csv")
        os.mkfifo(path)

    checks = _after_fstat(monkeypatch, replace_with_fifo)
    pipeline, ctx = _open("daily_prices", path)
    try:
        raw = await asyncio.wait_for(pipeline.fetch(ctx), timeout=5)
    finally:
        _release_fifo_readers(path)
    assert len(checks) == 1
    assert path.is_fifo()
    assert raw == original
    assert ctx.details["file_size_bytes"] == len(original)


async def test_fetch_rejects_a_file_that_grew_above_the_limit_after_its_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "prices.csv"
    with path.open("wb") as handle:
        handle.truncate(52_428_800)

    def grow() -> None:
        with path.open("r+b") as handle:
            handle.truncate(52_428_801)

    checks = _after_fstat(monkeypatch, grow)
    pipeline, ctx = _open("daily_prices", path)
    with pytest.raises(FetchFailed) as failure:
        await pipeline.fetch(ctx)
    assert len(checks) == 1
    assert failure.value.reason == "file_too_large"
    assert ctx.details["file_size_bytes"] == 52_428_800


@pytest.mark.parametrize("stage", ["open", "fstat", "read", "close"])
async def test_fetch_names_an_os_error_and_closes_the_descriptor_it_opened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, ("SCOM", SESSION, "28.45", 1_000)])
    opened: list[int] = []
    closed: list[int] = []

    def tracked_open(file: Path, flags: int) -> int:
        fd = os.open(file, flags)
        opened.append(fd)
        return fd

    def tracked_close(fd: int) -> None:
        os.close(fd)
        closed.append(fd)
        if stage == "close":
            raise OSError(5, "Input/output error")

    def denied(*args: object) -> object:
        raise PermissionError(13, "Permission denied")

    hooks: dict[str, Callable[..., Any]] = {"open": tracked_open, "close": tracked_close}
    if stage in ("open", "fstat", "read"):
        hooks[stage] = denied
    monkeypatch.setattr(tabular_import, "os", _OsView(**hooks))
    pipeline, ctx = _open("daily_prices", path)
    with pytest.raises(FetchFailed) as failure:
        await pipeline.fetch(ctx)
    expected = "unreadable_file:OSError" if stage == "close" else "unreadable_file:PermissionError"
    assert failure.value.reason == expected
    assert closed == opened
    assert len(opened) == (0 if stage == "open" else 1)


async def test_fetch_rejects_a_file_above_the_size_limit(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    with path.open("wb") as handle:
        handle.truncate(52_428_801)
    pipeline, ctx = _open("daily_prices", path)
    with pytest.raises(FetchFailed) as failure:
        await pipeline.fetch(ctx)
    assert failure.value.reason == "file_too_large"
    assert classify_exception(failure.value)[0] is RunStatus.FAILED
    assert ctx.details["file_size_bytes"] == 52_428_801


async def test_fetch_accepts_a_file_of_exactly_the_size_limit(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    with path.open("wb") as handle:
        handle.truncate(52_428_800)
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    assert len(raw) == 52_428_800
    assert ctx.details["file_size_bytes"] == 52_428_800


# --- Parse -----------------------------------------------------------------------------------


@pytest.mark.parametrize("suffix", [".csv", ".xlsx"])
@pytest.mark.parametrize("kind", KINDS)
async def test_every_kind_converts_the_same_from_csv_and_xlsx(
    tmp_path: Path, kind: ImportKind, suffix: str
) -> None:
    staged = await _stage(kind, _write_sample(tmp_path, kind, suffix))
    assert staged.outcome.rejected == []
    assert [record.row_number for record in staged.outcome.valid] == [2, 3]
    assert _values(staged.outcome.valid) == list(SAMPLES[kind].expected)
    _assert_exact_types(staged.outcome.valid)


async def test_excel_numeric_cells_convert_exactly(tmp_path: Path) -> None:
    path = _write_xlsx(
        tmp_path / "prices.xlsx",
        {
            "Prices": [
                PRICE_HEADER,
                # The one deliberate float: Excel keeps every number as a binary double, so a typed
                # 95.75 reaches the importer through openpyxl as a float and must read exactly.
                ("SCOM", SESSION, 95.75, 987_241),
            ]
        },
    )
    staged = await _stage("daily_prices", path)
    assert _values(staged.outcome.valid) == [
        {
            "ticker": "SCOM",
            "trade_date": SESSION,
            "close_price": Decimal("95.75"),
            "volume": 987_241,
            "turnover_kes": Decimal("94528325.75"),
            "turnover_estimated": True,
        }
    ]
    _assert_exact_types(staged.outcome.valid)


async def test_parse_reads_an_operator_csv_export(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    path = _write_csv(
        tmp_path / "Prices Export.CSV",
        [
            ("TICKER", " Trade\u00a0Date ", "Close  Price", "Volume", "Notes"),
            ("scom", "28/09/2026", "28.45", "12,345,678", "checked"),
            (None, None, None, None, "end of day summary"),
            ("eqty", "2026-09-28", "10.605", "987,241", None),
        ],
        encoding="utf-8-sig",
    )
    staged = await _stage("daily_prices", path)
    assert [record.row_number for record in staged.records] == [2, 4]
    assert _values(staged.records) == [
        {
            "ticker": "SCOM",
            "trade_date": SESSION,
            "close_price": Decimal("28.45"),
            "volume": 12_345_678,
        },
        {
            "ticker": "EQTY",
            "trade_date": SESSION,
            "close_price": Decimal("10.605"),
            "volume": 987_241,
        },
    ]
    assert staged.records[0].raw == {
        "ticker": "scom",
        "trade_date": "28/09/2026",
        "close_price": "28.45",
        "volume": "12,345,678",
    }
    assert staged.ctx.details["ignored_columns"] == ["notes"]
    assert staged.ctx.details["blank_rows_skipped"] == 1
    assert caplog.messages == [
        "run 2b7e1516-28ae-4d2a-a6f7-15880cf4f3c1: daily_prices import ignores 1 columns it does"
        " not read (details.ignored_columns)"
    ]


async def test_parse_logs_neither_the_file_name_nor_a_forged_line(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    name = "prices\r\nFORGED ERROR line.csv"
    try:
        path = _write_csv(
            tmp_path / name,
            [(*PRICE_HEADER, "Notes"), ("SCOM", "2026-09-28", "28.45", "1,000", "checked")],
        )
    except OSError as exc:
        pytest.skip(f"the filesystem rejects a CR/LF file name: {type(exc).__name__}")
    staged = await _stage("daily_prices", path)
    assert [record.row_number for record in staged.outcome.valid] == [2]
    # Details are run evidence stored on the run row, not log output: the name stays there.
    assert staged.ctx.details["file_name"] == name
    assert staged.ctx.details["ignored_columns"] == ["notes"]
    assert caplog.messages == [
        "run 2b7e1516-28ae-4d2a-a6f7-15880cf4f3c1: daily_prices import ignores 1 columns it does"
        " not read (details.ignored_columns)"
    ]
    for record in caplog.records:
        message = record.getMessage()
        assert name not in message
        assert "FORGED" not in message
        assert "\r" not in message
        assert "\n" not in message


MISSING_HEADER_CASES = [
    pytest.param(kind, column, id=f"{kind}-{column}")
    for kind, columns in REQUIRED_COLUMNS.items()
    for column in columns
]


@pytest.mark.parametrize(("kind", "missing"), MISSING_HEADER_CASES)
async def test_parse_rejects_a_missing_required_header(
    tmp_path: Path, kind: ImportKind, missing: str
) -> None:
    header = [
        column for column in REQUIRED_COLUMNS[kind] + OPTIONAL_COLUMNS[kind] if column != missing
    ]
    pipeline, ctx = _open(kind, _write_csv(tmp_path / f"{kind}.csv", [header]))
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert failure.value.expected == list(REQUIRED_COLUMNS[kind])
    assert failure.value.found == header
    message = f"missing required columns for {kind}: {missing}"
    assert failure.value.message == message
    assert classify_exception(failure.value) == (RunStatus.LAYOUT_CHANGED, message)


async def test_parse_rejects_a_repeated_header(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path / "prices.csv", [("Ticker", " ticker ", "Trade Date", "Close Price", "Volume")]
    )
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert failure.value.message == "duplicate headers: ticker"
    assert failure.value.found == ["ticker", "ticker", "trade_date", "close_price", "volume"]


async def test_parse_rejects_an_empty_worksheet(tmp_path: Path) -> None:
    pipeline, ctx = _open("equities", _write_xlsx(tmp_path / "equities.xlsx", {"Empty": []}))
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert failure.value.message == "the file is empty"
    assert failure.value.expected == ["ticker", "company_name", "sector", "shares_outstanding"]
    assert failure.value.found == []


async def test_a_header_without_data_rows_is_below_the_minimum(tmp_path: Path) -> None:
    staged = await _stage("daily_prices", _write_csv(tmp_path / "prices.csv", [PRICE_HEADER]))
    assert staged.records == []
    assert evaluate_thresholds(0, 0, TabularImportPipeline.min_rows) is RunStatus.LAYOUT_CHANGED


@pytest.mark.parametrize(
    ("file_name", "content", "reason"),
    [
        pytest.param("prices.csv", b"", "unreadable_file:EmptyDataError", id="empty-csv"),
        pytest.param(
            "prices.csv",
            b"\xff\xfe\x00ticker",
            "unreadable_file:UnicodeDecodeError",
            id="not-utf-8",
        ),
        pytest.param(
            "prices.csv",
            b"ticker,trade_date\nSCOM,2026-09-28,extra\n",
            "unreadable_file:ParserError",
            id="ragged-csv",
        ),
        pytest.param(
            "prices.xlsx", b"not a workbook", "unreadable_file:BadZipFile", id="not-a-workbook"
        ),
    ],
)
async def test_parse_fails_for_an_unreadable_file_by_error_type(
    tmp_path: Path, file_name: str, content: bytes, reason: str
) -> None:
    path = tmp_path / file_name
    path.write_bytes(content)
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    with pytest.raises(FetchFailed) as failure:
        pipeline.parse(raw)
    assert (failure.value.url, failure.value.reason) == (str(path.resolve()), reason)


def _workbook_with_notes_first(tmp_path: Path) -> Path:
    return _write_xlsx(
        tmp_path / "prices.xlsx",
        {
            "Notes": [("Prepared By", "Operations Desk")],
            "Prices": [PRICE_HEADER, ("SCOM", SESSION, "28.45", 12_345_678)],
        },
    )


async def test_sheet_option_selects_the_named_worksheet(tmp_path: Path) -> None:
    staged = await _stage("daily_prices", _workbook_with_notes_first(tmp_path), sheet="Prices")
    assert staged.ctx.details["sheet"] == "Prices"
    assert _values(staged.outcome.valid) == [
        {
            "ticker": "SCOM",
            "trade_date": SESSION,
            "close_price": Decimal("28.45"),
            "volume": 12_345_678,
            "turnover_kes": Decimal("351234539.10"),
            "turnover_estimated": True,
        }
    ]


@pytest.mark.parametrize(
    ("sheet", "expected", "found", "message"),
    [
        pytest.param(
            None,
            ["ticker", "trade_date", "close_price", "volume"],
            ["prepared_by", "operations_desk"],
            "missing required columns for daily_prices: ticker, trade_date, close_price, volume",
            id="first-sheet-by-default",
        ),
        pytest.param(
            "Missing",
            ["Missing"],
            ["Notes", "Prices"],
            "sheet 'Missing' not found in the workbook",
            id="unknown-sheet",
        ),
    ],
)
async def test_sheet_selection_layout_changes(
    tmp_path: Path, sheet: str | None, expected: list[str], found: list[str], message: str
) -> None:
    pipeline, ctx = _open("daily_prices", _workbook_with_notes_first(tmp_path), sheet=sheet)
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert (failure.value.expected, failure.value.found, failure.value.message) == (
        expected,
        found,
        message,
    )


def _price_rows(count: int) -> list[tuple[object, ...]]:
    return [("SCOM", date(2026, 9, day), "28.45", 1_000) for day in range(1, count + 1)]


@pytest.mark.parametrize("suffix", [".csv", ".xlsx"])
async def test_row_limit_accepts_a_file_at_the_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, suffix: str
) -> None:
    monkeypatch.setattr(tabular_import, "MAX_ROWS", 3)
    path = _write_table(tmp_path / f"prices{suffix}", [PRICE_HEADER, *_price_rows(3)])
    staged = await _stage("daily_prices", path)
    assert len(staged.records) == 3


@pytest.mark.parametrize(
    ("suffix", "rows"),
    [
        pytest.param(".csv", [PRICE_HEADER, *_price_rows(4)], id="csv"),
        pytest.param(".xlsx", [PRICE_HEADER, *_price_rows(4)], id="xlsx"),
        pytest.param(
            ".csv",
            [PRICE_HEADER, *_price_rows(1), (None, None, None, None), *_price_rows(2)],
            id="csv-blank-row-counts",
        ),
    ],
)
async def test_row_limit_rejects_a_file_over_the_limit_without_importing_part(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    suffix: str,
    rows: list[tuple[object, ...]],
) -> None:
    monkeypatch.setattr(tabular_import, "MAX_ROWS", 3)
    pipeline, ctx = _open("daily_prices", _write_table(tmp_path / f"prices{suffix}", rows))
    raw = await pipeline.fetch(ctx)
    with pytest.raises(FetchFailed) as failure:
        pipeline.parse(raw)
    assert failure.value.reason == "too_many_rows"
    assert classify_exception(failure.value)[0] is RunStatus.FAILED


# --- Validate --------------------------------------------------------------------------------

VALID_CELLS: Mapping[ImportKind, Mapping[str, str]] = {
    "daily_prices": {
        "ticker": "SCOM",
        "trade_date": "2026-09-28",
        "close_price": "28.45",
        "volume": "1000",
        "open_price": "",
        "high_price": "",
        "low_price": "",
        "turnover_kes": "",
    },
    "equities": {
        "ticker": "SCOM",
        "company_name": "Safaricom PLC",
        "sector": "Telecommunication",
        "shares_outstanding": "40065428000",
        "isin": "",
        "free_float_pct": "",
        "currency": "",
    },
    "fundamentals": {
        "ticker": "SCOM",
        "period_end_date": "2026-03-31",
        "period_type": "",
        "currency": "",
        "revenue_kes": "388690000000",
        "dps_kes": "",
    },
    "macro_indicators": {
        "observation_date": "2026-09-30",
        "inflation_yoy_pct": "6.8",
        "fx_kes_usd": "",
    },
    "foreign_flows": {
        "ticker": "SCOM",
        "trade_date": "2026-09-28",
        "foreign_net_flow_kes": "-1250000",
    },
}


async def _validate_one_row(tmp_path: Path, kind: ImportKind, cells: Mapping[str, str]) -> _Staged:
    path = _write_csv(tmp_path / f"{kind}.csv", [tuple(cells), tuple(cells.values())])
    return await _stage(kind, path)


@pytest.mark.parametrize("kind", KINDS)
async def test_baseline_rows_are_valid(tmp_path: Path, kind: ImportKind) -> None:
    staged = await _validate_one_row(tmp_path, kind, VALID_CELLS[kind])
    assert staged.outcome.rejected == []
    assert len(staged.outcome.valid) == 1


REJECTION_CASES = [
    pytest.param("daily_prices", {"close_price": "0"}, "non_positive:close_price", id="zero-close"),
    pytest.param(
        "daily_prices", {"close_price": "(1.50)"}, "non_positive:close_price", id="negative-close"
    ),
    pytest.param("daily_prices", {"open_price": "-1"}, "negative:open_price", id="negative-open"),
    pytest.param(
        "daily_prices", {"turnover_kes": "(5.00)"}, "negative:turnover_kes", id="negative-turnover"
    ),
    pytest.param(
        "daily_prices",
        {"high_price": "27.00", "low_price": "28.00"},
        "high_below_low",
        id="high-below-low",
    ),
    pytest.param(
        "daily_prices", {"close_price": "28.45 KES"}, "invalid_decimal:close_price", id="close-text"
    ),
    pytest.param(
        "daily_prices", {"close_price": "12.5%"}, "invalid_decimal:close_price", id="close-percent"
    ),
    # Cells are read without NA inference, so the text "NA" is not an empty cell.
    pytest.param(
        "daily_prices", {"close_price": "NA"}, "invalid_decimal:close_price", id="na-text"
    ),
    pytest.param(
        "daily_prices",
        {"close_price": "100000000000000"},
        "out_of_range:close_price",
        id="close-overflow",
    ),
    pytest.param(
        "daily_prices", {"volume": "1000000000001"}, "out_of_range:volume", id="volume-above-cap"
    ),
    pytest.param("daily_prices", {"volume": "-10"}, "out_of_range:volume", id="negative-volume"),
    pytest.param(
        "daily_prices", {"volume": "12.5"}, "invalid_integer:volume", id="fractional-volume"
    ),
    pytest.param(
        "daily_prices", {"trade_date": "2026/28/09"}, "invalid_date:trade_date", id="bad-date"
    ),
    pytest.param("daily_prices", {"ticker": ""}, "missing_value:ticker", id="empty-ticker"),
    pytest.param(
        "daily_prices",
        {"ticker": "", "volume": "-"},
        "missing_value:ticker;missing_value:volume",
        id="two-empty-required-cells",
    ),
    pytest.param("daily_prices", {"ticker": "S" * 17}, "too_long:ticker", id="long-ticker"),
    pytest.param(
        "daily_prices",
        {"close_price": "10000000000", "volume": "1000000000000"},
        "out_of_range:turnover_kes",
        id="estimated-turnover-overflows",
    ),
    # 265,371,653 x 37,683,000,000,003.7683 = 9,999,999,999,999,999,999,999.9999, below 10^22,
    # which rounds half up to 10^22 and would overflow NUMERIC(24,2).
    pytest.param(
        "daily_prices",
        {"close_price": "37683000000003.7683", "volume": "265371653"},
        "out_of_range:turnover_kes",
        id="estimated-turnover-rounds-up-to-the-limit",
    ),
    pytest.param(
        "daily_prices",
        {"close_price": "0", "high_price": "27", "low_price": "28"},
        "non_positive:close_price;high_below_low",
        id="two-price-rules",
    ),
    pytest.param(
        "equities",
        {"free_float_pct": "100.01"},
        "out_of_range:free_float_pct",
        id="free-float-above-100",
    ),
    pytest.param(
        "equities",
        {"free_float_pct": "-0.5"},
        "out_of_range:free_float_pct",
        id="negative-free-float",
    ),
    pytest.param("equities", {"isin": "KE12345"}, "invalid_isin", id="short-isin"),
    pytest.param("equities", {"isin": "XX1000001402"}, "invalid_isin", id="foreign-isin"),
    pytest.param("equities", {"currency": "KSHS"}, "invalid_currency", id="four-letter-currency"),
    pytest.param("equities", {"currency": "K1S"}, "invalid_currency", id="digit-in-currency"),
    pytest.param(
        "equities",
        {"shares_outstanding": ""},
        "missing_value:shares_outstanding",
        id="empty-shares",
    ),
    pytest.param(
        "equities",
        {"shares_outstanding": "1,000,000,000,001"},
        "out_of_range:shares_outstanding",
        id="shares-above-cap",
    ),
    pytest.param(
        "equities", {"company_name": "C" * 201}, "too_long:company_name", id="long-company-name"
    ),
    pytest.param("equities", {"sector": "S" * 101}, "too_long:sector", id="long-sector"),
    pytest.param("fundamentals", {"period_type": "Q3"}, "invalid_period_type", id="period-q3"),
    pytest.param("fundamentals", {"revenue_kes": ""}, "no_values", id="fundamentals-no-values"),
    pytest.param(
        "fundamentals",
        {"revenue_kes": "n.a."},
        "invalid_decimal:revenue_kes",
        id="unconvertible-value-is-not-no-values",
    ),
    pytest.param(
        "fundamentals",
        {"period_end_date": "31/02/2026"},
        "invalid_date:period_end_date",
        id="impossible-period-end",
    ),
    pytest.param("fundamentals", {"currency": "usdollar"}, "invalid_currency", id="long-currency"),
    pytest.param(
        "fundamentals",
        {"dps_kes": "100000000000000"},
        "out_of_range:dps_kes",
        id="dps-overflow",
    ),
    pytest.param("macro_indicators", {"inflation_yoy_pct": ""}, "no_values", id="macro-no-values"),
    pytest.param(
        "macro_indicators",
        {"inflation_yoy_pct": "high"},
        "invalid_decimal:inflation_yoy_pct",
        id="inflation-text",
    ),
    pytest.param(
        "macro_indicators",
        {"inflation_yoy_pct": "100000"},
        "out_of_range:inflation_yoy_pct",
        id="inflation-overflow",
    ),
    pytest.param(
        "macro_indicators",
        {"fx_kes_usd": "129.20%"},
        "invalid_decimal:fx_kes_usd",
        id="percent-on-fx",
    ),
    pytest.param(
        "macro_indicators",
        {"observation_date": ""},
        "missing_value:observation_date",
        id="empty-observation-date",
    ),
    pytest.param(
        "foreign_flows",
        {"foreign_net_flow_kes": ""},
        "missing_value:foreign_net_flow_kes",
        id="empty-flow",
    ),
    pytest.param(
        "foreign_flows",
        {"foreign_net_flow_kes": "1e22"},
        "out_of_range:foreign_net_flow_kes",
        id="flow-overflow",
    ),
    pytest.param(
        "foreign_flows",
        {"trade_date": "28-09-2026"},
        "invalid_date:trade_date",
        id="flow-date-with-dashes",
    ),
]


@pytest.mark.parametrize(("kind", "cells", "reason"), REJECTION_CASES)
async def test_validate_rejects_a_row_with_its_reason_codes(
    tmp_path: Path, kind: ImportKind, cells: Mapping[str, str], reason: str
) -> None:
    row = {**VALID_CELLS[kind], **cells}
    staged = await _validate_one_row(tmp_path, kind, row)
    assert staged.outcome.valid == []
    assert staged.outcome.rejected == [RejectedRow(index=2, reason=reason, row=row)]


async def test_validate_keeps_an_estimated_turnover_that_rounds_to_just_below_the_limit(
    tmp_path: Path,
) -> None:
    # 859,282,386,827 x 11,637,617,799.8087 = 9,999,999,999,999,999,999,999.9949, which rounds
    # half up to 9,999,999,999,999,999,999,999.99, the largest value NUMERIC(24,2) holds.
    row = {
        **VALID_CELLS["daily_prices"],
        "close_price": "11637617799.8087",
        "volume": "859282386827",
    }
    staged = await _validate_one_row(tmp_path, "daily_prices", row)
    assert staged.outcome.rejected == []
    [record] = staged.outcome.valid
    assert str(record.values["turnover_kes"]) == "9999999999999999999999.99"
    assert record.values["turnover_estimated"] is True


async def test_fundamentals_without_type_or_currency_columns_default_to_fy_and_kes(
    tmp_path: Path,
) -> None:
    path = _write_csv(
        tmp_path / "fundamentals.csv",
        [("Ticker", "Period End Date", "Net Income KES"), ("SCOM", "31/03/2026", "69,800,000,000")],
    )
    staged = await _stage("fundamentals", path)
    assert staged.outcome.rejected == []
    assert _values(staged.outcome.valid) == [
        {
            "ticker": "SCOM",
            "period_end_date": date(2026, 3, 31),
            "net_income_kes": Decimal("69800000000"),
            "period_type": "FY",
            "currency": "KES",
        }
    ]


@pytest.mark.parametrize(
    ("valid_rows", "invalid_rows", "status"),
    [
        pytest.param(10, 0, RunStatus.SUCCEEDED, id="all-valid"),
        pytest.param(9, 1, RunStatus.PARTIAL, id="ten-percent-rejected"),
        pytest.param(18, 2, RunStatus.PARTIAL, id="ten-percent-of-twenty-rejected"),
        pytest.param(8, 2, RunStatus.LAYOUT_CHANGED, id="twenty-percent-rejected"),
    ],
)
async def test_rejected_rows_feed_the_shared_thresholds(
    tmp_path: Path, valid_rows: int, invalid_rows: int, status: RunStatus
) -> None:
    invalid = [("SCOM", date(2026, 8, day), "0", 1_000) for day in range(1, invalid_rows + 1)]
    path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, *_price_rows(valid_rows), *invalid])
    staged = await _stage("daily_prices", path)
    assert len(staged.records) == valid_rows + invalid_rows
    assert len(staged.outcome.rejected) == invalid_rows
    assert {row.reason for row in staged.outcome.rejected} <= {"non_positive:close_price"}
    parsed, rejected = len(staged.records), len(staged.outcome.rejected)
    assert evaluate_thresholds(parsed, rejected, TabularImportPipeline.min_rows) is status


EXTREME_CLOSE = ("SCOM", date(2026, 8, 3), "1e1000000", 1_000)
EXTREME_VOLUME = ("SCOM", date(2026, 8, 4), "28.45", "(1e1000000)")
# 265,371,653 x 37,683,000,000,003.7683 rounds half up to 10^22, beyond NUMERIC(24,2).
TURNOVER_AT_LIMIT = ("SCOM", date(2026, 8, 5), "37683000000003.7683", 265_371_653)


@pytest.mark.parametrize(
    ("bad_rows", "rejected", "status"),
    [
        pytest.param(
            [EXTREME_CLOSE],
            [(22, "out_of_range:close_price")],
            RunStatus.PARTIAL,
            id="extreme-exponent-close",
        ),
        pytest.param(
            [EXTREME_VOLUME],
            [(22, "out_of_range:volume")],
            RunStatus.PARTIAL,
            id="parenthesized-extreme-exponent-volume",
        ),
        pytest.param(
            [EXTREME_CLOSE, TURNOVER_AT_LIMIT],
            [(22, "out_of_range:close_price"), (23, "out_of_range:turnover_kes")],
            RunStatus.PARTIAL,
            id="extreme-close-and-turnover-at-limit-within-ten-percent",
        ),
        pytest.param(
            [EXTREME_CLOSE, EXTREME_VOLUME, TURNOVER_AT_LIMIT],
            [
                (22, "out_of_range:close_price"),
                (23, "out_of_range:volume"),
                (24, "out_of_range:turnover_kes"),
            ],
            RunStatus.LAYOUT_CHANGED,
            id="three-of-twenty-three-above-ten-percent",
        ),
    ],
)
async def test_extreme_cells_and_turnover_overflow_are_rejected_rows_under_the_thresholds(
    tmp_path: Path,
    bad_rows: list[tuple[object, ...]],
    rejected: list[tuple[int, str]],
    status: RunStatus,
) -> None:
    """Twenty valid rows plus cells beyond the decimal context or the turnover column: each bad
    row is rejected with its code instead of failing the run, and counts toward the thresholds."""
    path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, *_price_rows(20), *bad_rows])
    staged = await _stage("daily_prices", path)
    assert len(staged.outcome.valid) == 20
    assert [(row.index, row.reason) for row in staged.outcome.rejected] == rejected
    parsed, count = len(staged.records), len(staged.outcome.rejected)
    assert evaluate_thresholds(parsed, count, TabularImportPipeline.min_rows) is status


# --- Reconcile -------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_of_no_rows_runs_no_query(tmp_path: Path, kind: ImportKind) -> None:
    conn = _ScriptedConnection()
    pipeline, ctx = _open(kind, tmp_path / "empty.csv", conn=conn)
    assert await pipeline.reconcile(ctx, []) == ValidationOutcome(valid=[], rejected=[])
    assert ctx.details == {"duplicates_collapsed": 0}
    assert conn.statements == []


async def test_reconcile_collapses_repeated_macro_dates_without_a_query(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    path = _write_csv(
        tmp_path / "macro.csv",
        [
            ("Observation Date", "Inflation YoY Pct"),
            (date(2026, 9, 30), "6.6"),
            (date(2026, 8, 31), "6.6"),
            (date(2026, 9, 30), "6.8"),
        ],
    )
    conn = _ScriptedConnection()
    staged = await _stage("macro_indicators", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert reconciled.rejected == []
    assert [(record.row_number, dict(record.values)) for record in reconciled.valid] == [
        (4, {"observation_date": date(2026, 9, 30), "inflation_yoy_pct": Decimal("6.8")}),
        (3, {"observation_date": date(2026, 8, 31), "inflation_yoy_pct": Decimal("6.6")}),
    ]
    assert staged.ctx.details["duplicates_collapsed"] == 1
    assert conn.statements == []
    assert "macro_indicators import collapsed 1 rows that repeat a key" in caplog.messages


async def test_daily_prices_reconcile_rejects_unknown_tickers(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path / "prices.csv",
        [PRICE_HEADER, ("SCOM", SESSION, "28.45", 1_000), ("KQ", SESSION, "3.85", 2_000)],
    )
    conn = _ScriptedConnection([("SCOM",)])
    staged = await _stage("daily_prices", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert [record.values["ticker"] for record in reconciled.valid] == ["SCOM"]
    assert reconciled.rejected == [
        RejectedRow(
            index=3,
            reason="unknown_ticker",
            row={
                "ticker": "KQ",
                "trade_date": "2026-09-28",
                "close_price": "3.85",
                "volume": "2000",
            },
        )
    ]
    assert [_query(statement) for statement in conn.statements] == [
        (["terminal_equities"], [["KQ", "SCOM"]])
    ]
    assert conn.unanswered == 0


@pytest.mark.parametrize(
    ("rows", "stored", "valid", "rejected", "queries"),
    [
        pytest.param(
            [
                ("SCOM", date(2026, 3, 31), "FY", "USD", "2,990,000,000"),
                ("SCOM", date(2025, 9, 30), "H1", "usd", "1,450,000,000"),
                ("SCOM", date(2025, 3, 31), None, None, "371,000,000,000"),
                ("KQ", date(2026, 3, 31), "FY", "KES", "190,000,000,000"),
            ],
            [
                [("SCOM",)],
                [
                    ("SCOM", date(2026, 3, 31), "FY", "KES"),
                    ("SCOM", date(2025, 9, 30), "H1", "USD"),
                ],
            ],
            [(3, "H1", "USD"), (4, "FY", "KES")],
            [(2, "currency_conflict"), (5, "unknown_ticker")],
            [
                (["terminal_equities"], [["KQ", "SCOM"]]),
                # Exactly the file's periods of known tickers, never SCOM's other stored periods.
                (
                    ["terminal_fundamentals", "requested"],
                    [
                        *("SCOM", date(2025, 3, 31), "FY"),
                        *("SCOM", date(2025, 9, 30), "H1"),
                        *("SCOM", date(2026, 3, 31), "FY"),
                    ],
                ),
            ],
            id="known_and_unknown_tickers",
        ),
        pytest.param(
            [("KQ", date(2026, 3, 31), "FY", "KES", "190,000,000,000")],
            [[]],
            [],
            [(2, "unknown_ticker")],
            [(["terminal_equities"], [["KQ"]])],
            id="unknown_tickers_only_skip_the_period_query",
        ),
    ],
)
async def test_fundamentals_reconcile_rejects_unknown_tickers_and_currency_conflicts(
    tmp_path: Path,
    rows: list[tuple[object, ...]],
    stored: list[list[tuple[object, ...]]],
    valid: list[tuple[int, str, str]],
    rejected: list[tuple[int, str]],
    queries: list[tuple[list[str], list[object]]],
) -> None:
    path = _write_csv(
        tmp_path / "fundamentals.csv",
        [("Ticker", "Period End Date", "Period Type", "Currency", "Revenue KES"), *rows],
    )
    conn = _ScriptedConnection(*stored)
    staged = await _stage("fundamentals", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert [
        (record.row_number, record.values["period_type"], record.values["currency"])
        for record in reconciled.valid
    ] == valid
    assert [(row.index, row.reason) for row in reconciled.rejected] == rejected
    assert [_query(statement) for statement in conn.statements] == queries
    assert conn.unanswered == 0


async def test_equities_reconcile_keeps_stored_values_and_rejects_isin_conflicts(
    tmp_path: Path,
) -> None:
    path = _write_csv(
        tmp_path / "equities.csv",
        [
            SAMPLES["equities"].header,
            ("SCOM", "Safaricom PLC", "Telecommunication", 40_065_428_000, None, None, None),
            (
                "EQTY",
                "Equity Group Holdings PLC",
                "Banking",
                3_773_674_802,
                "KE1000001402",
                None,
                None,
            ),
            ("KCB", "KCB Group PLC", "Banking", 3_213_462_815, "KE0000000315", None, None),
            ("ABSA", "Absa Bank Kenya PLC", "Banking", 5_431_536_000, "KE0000000315", None, None),
        ],
    )
    conn = _ScriptedConnection(
        [("SCOM", "KE1000001402", Decimal("35.0000"), "KES")],
        [("KE1000001402", "SCOM")],
    )
    staged = await _stage("equities", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert _values(reconciled.valid) == [
        {
            "ticker": "SCOM",
            "company_name": "Safaricom PLC",
            "sector": "Telecommunication",
            "shares_outstanding": 40_065_428_000,
            "isin": "KE1000001402",
            "free_float_pct": Decimal("35.0000"),
            "currency": "KES",
        },
        {
            "ticker": "KCB",
            "company_name": "KCB Group PLC",
            "sector": "Banking",
            "shares_outstanding": 3_213_462_815,
            "isin": "KE0000000315",
            "free_float_pct": None,
            "currency": "KES",
        },
    ]
    assert [(row.index, row.reason) for row in reconciled.rejected] == [
        (3, "isin_conflict"),
        (5, "isin_conflict"),
    ]
    assert [_query(statement) for statement in conn.statements] == [
        (["terminal_equities"], [["ABSA", "EQTY", "KCB", "SCOM"]]),
        (["terminal_equities"], [["KE0000000315", "KE1000001402"]]),
    ]
    assert conn.unanswered == 0


async def test_equities_reconcile_defaults_currency_to_kes_without_an_isin_query(
    tmp_path: Path,
) -> None:
    path = _write_csv(
        tmp_path / "equities.csv",
        [
            ("Ticker", "Company Name", "Sector", "Shares Outstanding"),
            ("KEGN", "KenGen PLC", "Energy & Petroleum", 6_594_522_339),
        ],
    )
    conn = _ScriptedConnection([])
    staged = await _stage("equities", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert reconciled.rejected == []
    assert _values(reconciled.valid) == [
        {
            "ticker": "KEGN",
            "company_name": "KenGen PLC",
            "sector": "Energy & Petroleum",
            "shares_outstanding": 6_594_522_339,
            "isin": None,
            "free_float_pct": None,
            "currency": "KES",
        }
    ]
    assert [_query(statement) for statement in conn.statements] == [
        (["terminal_equities"], [["KEGN"]])
    ]


@pytest.mark.parametrize(
    ("rows", "stored", "valid", "rejected", "binds"),
    [
        pytest.param(
            [
                ("SCOM", SESSION, "(1,250,000.00)"),
                ("SCOM", NEXT_SESSION, "500,000"),
                ("EQTY", date(2026, 9, 25), "3,400,500.565"),
            ],
            [("SCOM", SESSION)],
            [2],
            [(3, "no_price_row"), (4, "no_price_row")],
            [*("EQTY", date(2026, 9, 25)), *("SCOM", SESSION), *("SCOM", NEXT_SESSION)],
            id="rows_without_a_price_row",
        ),
        pytest.param(
            [("SCOM", date(2016, 9, 28), "100,000"), ("SCOM", SESSION, "(200,000)")],
            [("SCOM", date(2016, 9, 28)), ("SCOM", SESSION)],
            [2, 3],
            [],
            # The two sessions only: no date range, so none of the decade between them is read.
            [*("SCOM", date(2016, 9, 28)), *("SCOM", SESSION)],
            id="dates_a_decade_apart",
        ),
    ],
)
async def test_foreign_flows_reconcile_rejects_rows_without_a_price_row(
    tmp_path: Path,
    rows: list[tuple[object, ...]],
    stored: list[tuple[object, ...]],
    valid: list[int],
    rejected: list[tuple[int, str]],
    binds: list[object],
) -> None:
    path = _write_csv(tmp_path / "flows.csv", [SAMPLES["foreign_flows"].header, *rows])
    conn = _ScriptedConnection(stored)
    staged = await _stage("foreign_flows", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert [record.row_number for record in reconciled.valid] == valid
    assert [(row.index, row.reason) for row in reconciled.rejected] == rejected
    assert [_query(statement) for statement in conn.statements] == [
        (["terminal_daily_prices", "requested"], binds)
    ]
    assert conn.unanswered == 0


@pytest.mark.parametrize(
    ("kind", "header", "lookups", "table", "key_suffix", "statement_binds"),
    [
        pytest.param(
            "foreign_flows",
            SAMPLES["foreign_flows"].header,
            [],
            "terminal_daily_prices",
            (),
            [10_000, 2],
            id="foreign_flows",
        ),
        pytest.param(
            "fundamentals",
            ("Ticker", "Period End Date", "Revenue KES"),
            [[("SCOM",)]],
            "terminal_fundamentals",
            ("FY",),
            [1, 15_000, 3],
            id="fundamentals",
        ),
    ],
)
async def test_reconcile_requests_exact_keys_in_batches_of_5000(
    tmp_path: Path,
    kind: ImportKind,
    header: tuple[str, ...],
    lookups: list[list[tuple[object, ...]]],
    table: str,
    key_suffix: tuple[str, ...],
    statement_binds: list[int],
) -> None:
    """5,001 keys take two statements, each binding exactly its slice of the sorted keys and at
    most 15,000 parameters, far below asyncpg's 32,767."""
    days = [date.fromordinal(SESSION.toordinal() - offset) for offset in range(5_001)]
    path = _write_csv(tmp_path / "keys.csv", [header, *(("SCOM", day, "1") for day in days)])
    conn = _ScriptedConnection(*lookups, [], [])
    staged = await _stage(kind, path, conn=conn)
    assert len(staged.outcome.valid) == 5_001
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert len(reconciled.valid) + len(reconciled.rejected) == 5_001
    queries = [_query(statement) for statement in conn.statements]
    assert [len(binds) for _, binds in queries] == statement_binds
    batches = queries[len(lookups) :]
    assert [names for names, _ in batches] == [[table, "requested"]] * 2
    assert [value for _, binds in batches for value in binds] == [
        part for day in sorted(days) for part in ("SCOM", day, *key_suffix)
    ]
    assert conn.unanswered == 0


# --- Upsert ----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _WriterCall:
    """One call the importer made to a ``db.upsert`` writer."""

    writer: str
    conn: object
    rows: list[dict[str, object]]
    options: dict[str, object]


@dataclass(slots=True)
class _Writers:
    calls: list[_WriterCall] = field(default_factory=list)
    missing_flow_keys: tuple[tuple[str, date], ...] = ()


@pytest.fixture
def writers(monkeypatch: pytest.MonkeyPatch) -> _Writers:
    """Replace the five ``db.upsert`` writers with recorders that keep their signatures and report
    every row as written; ``update_flows`` reports ``missing_flow_keys`` as without a price row."""
    recorded = _Writers()

    def keep(
        writer: str, conn: object, rows: Sequence[Mapping[str, object]], **options: object
    ) -> int:
        recorded.calls.append(
            _WriterCall(writer=writer, conn=conn, rows=[dict(row) for row in rows], options=options)
        )
        return len(rows)

    async def upsert_rows(
        executor: object,
        model: object,
        rows: Sequence[Mapping[str, object]],
        key: Sequence[str],
        mode: str,
    ) -> int:
        return keep("upsert_rows", executor, rows, model=model, key=key, mode=mode)

    async def upsert_prices(
        executor: object,
        rows: Sequence[Mapping[str, object]],
        *,
        source: str,
        ingestion_run_id: uuid.UUID | None,
    ) -> int:
        return keep(
            "upsert_prices", executor, rows, source=source, ingestion_run_id=ingestion_run_id
        )

    async def upsert_fundamentals(
        executor: object,
        rows: Sequence[Mapping[str, object]],
        *,
        source: str,
        filing_id: uuid.UUID | None = None,
    ) -> int:
        return keep("upsert_fundamentals", executor, rows, source=source, filing_id=filing_id)

    async def upsert_macro(
        executor: object, rows: Sequence[Mapping[str, object]], *, source: str
    ) -> int:
        return keep("upsert_macro", executor, rows, source=source)

    async def update_flows(
        executor: object, rows: Sequence[Mapping[str, object]], *, flow_source: str
    ) -> FlowUpdateResult:
        written = keep("update_flows", executor, rows, flow_source=flow_source)
        missing = recorded.missing_flow_keys
        return FlowUpdateResult(rows_updated=written - len(missing), missing_keys=missing)

    monkeypatch.setattr(tabular_import, "upsert_rows", upsert_rows)
    monkeypatch.setattr(tabular_import, "upsert_prices", upsert_prices)
    monkeypatch.setattr(tabular_import, "upsert_fundamentals", upsert_fundamentals)
    monkeypatch.setattr(tabular_import, "upsert_macro", upsert_macro)
    monkeypatch.setattr(tabular_import, "update_flows", update_flows)
    return recorded


@pytest.mark.parametrize(
    ("kind", "writer", "options", "stamped"),
    [
        pytest.param(
            "equities",
            "upsert_rows",
            {"model": TerminalEquity, "key": ("ticker",), "mode": "do_update"},
            {"source": "csv_import"},
            id="equities",
        ),
        pytest.param(
            "daily_prices",
            "upsert_prices",
            {"source": "csv_import", "ingestion_run_id": RUN_ID},
            {},
            id="daily_prices",
        ),
        pytest.param(
            "fundamentals",
            "upsert_fundamentals",
            {"source": "csv_import", "filing_id": None},
            {},
            id="fundamentals",
        ),
        pytest.param(
            "macro_indicators", "upsert_macro", {"source": "csv_import"}, {}, id="macro_indicators"
        ),
        pytest.param(
            "foreign_flows", "update_flows", {"flow_source": "csv_import"}, {}, id="foreign_flows"
        ),
    ],
)
async def test_upsert_hands_each_kind_to_its_writer_as_csv_import(
    tmp_path: Path,
    writers: _Writers,
    kind: ImportKind,
    writer: str,
    options: dict[str, object],
    stamped: dict[str, object],
) -> None:
    staged = await _stage(kind, _write_sample(tmp_path, kind))
    write_conn = _ScriptedConnection()
    written = await staged.pipeline.upsert(
        cast(AsyncConnection, write_conn), staged.outcome.valid, RUN_ID
    )
    assert written == 2
    [call] = writers.calls
    assert (call.writer, call.conn, call.options) == (writer, write_conn, options)
    columns = WRITE_COLUMNS[kind]
    assert call.rows == [
        {**{column: expected.get(column) for column in columns}, **stamped}
        for expected in SAMPLES[kind].expected
    ]
    assert [tuple(row) for row in call.rows] == [(*columns, *stamped)] * 2
    assert write_conn.statements == []


async def test_foreign_flows_upsert_records_keys_that_lost_their_price_row(
    tmp_path: Path, writers: _Writers, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    staged = await _stage("foreign_flows", _write_sample(tmp_path, "foreign_flows"))
    writers.missing_flow_keys = (("EQTY", SESSION),)
    written = await staged.pipeline.upsert(
        cast(AsyncConnection, _ScriptedConnection()), staged.outcome.valid, RUN_ID
    )
    assert written == 1
    assert staged.ctx.details["flow_missing_keys"] == {
        "count": 1,
        "sample": [["EQTY", SESSION]],
    }
    assert "foreign_flows import found 1 keys without a price row at write time" in caplog.messages
