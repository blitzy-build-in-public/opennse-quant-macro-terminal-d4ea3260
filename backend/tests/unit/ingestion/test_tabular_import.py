"""Tests for the CSV/Excel importer (``opennse_terminal.ingestion.files.tabular_import``).

Every case before the last section runs without PostgreSQL or a network. Operator-style CSV and
``.xlsx`` files are written into ``tmp_path`` (the workbooks with openpyxl) and the importer runs
stage by stage:

* ``fetch`` with a ``RunContext`` whose HTTP client fails the test if it is touched, then
  ``parse`` and ``validate``;
* ``reconcile`` on a read-only connection double that answers each ``select()`` with scripted
  rows and keeps the statement, so its tables and bound parameters can be checked;
* ``upsert`` with the ``db.upsert`` writers replaced by recorders that keep their signatures,
  and the same connection double answering the stored-currency read after a short write;
* ``import_file`` with the engine factory and ``execute_pipeline`` replaced.

The shared row thresholds are applied to the counts the stages produce.

The last section runs ``import_file`` end to end against a fresh seeded clone per test (the root
conftest's ``seeded_database``, which marks those tests ``integration``): the recorded sample
``fixtures/files/daily_prices_sample.csv`` and files written into ``tmp_path`` are imported, and
the run row, the shared thresholds and the precedence of the ``db.upsert`` writers over seeded and
scraped rows are read back through the ``terminal_`` models.

Expected values are literals from the importer's contract, never recomputed with the code under
test. The one exception is the recorded sample: its cells are read with the stdlib ``csv`` module
and converted with the cell helpers the literal cases above verify, so its 22 rows are not
restated here. Amounts are ``Decimal`` and volumes and share counts ``int``.
"""

import asyncio
import contextlib
import csv
import io
import logging
import os
import re
import tracemalloc
import uuid
import zipfile
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Context, Decimal, localcontext
from pathlib import Path
from typing import Any, Protocol, cast

import pandas
import pytest
from openpyxl import Workbook
from sqlalchemy import ColumnElement, Join, delete, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.upsert import FlowUpdateResult, upsert_fundamentals, upsert_prices
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
    execute_pipeline,
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
from opennse_terminal.models import (
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
    TerminalMacroIndicator,
)

LOGGER = "opennse_terminal.ingestion.files.tabular_import"
UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://nobody:x@127.0.0.1:1/none"
RUN_ID = uuid.UUID("2b7e1516-28ae-4d2a-a6f7-15880cf4f3c1")
SESSION = date(2026, 9, 28)
NEXT_SESSION = date(2026, 9, 29)
FROZEN_NOW = datetime(2026, 10, 29, 22, 30, tzinfo=UTC)
"""The clock the future-date cases inject: 01:30 on 2026-10-30 in Africa/Nairobi (UTC+3) while UTC
is still on 2026-10-29, so "today" is the Nairobi date whenever the tests run."""
NAIROBI_TODAY = date(2026, 10, 30)
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
    now: Callable[[], datetime] | None = None,
) -> tuple[TabularImportPipeline, RunContext]:
    """A fresh pipeline and the run context ``execute_pipeline`` would hand it; ``now`` replaces
    the pipeline's clock."""
    pipeline = TabularImportPipeline(
        _hermetic_settings(), kind=kind, path=path, sheet=sheet, now=now
    )
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
    now: Callable[[], datetime] | None = None,
) -> _Staged:
    pipeline, ctx = _open(kind, path, sheet=sheet, conn=conn, now=now)
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
        # NUL is shown as U+2400 SYMBOL FOR NULL: the name never matches and can be recorded.
        ("Close\x00 Price", "close\u2400_price"),
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
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    name = "prices\r\nFORGED ERROR line.csv"
    # The bytes live under a safe name and are served by the real descriptor read of a pipeline
    # opened on it, so the run under test carries the hostile name in its path, file_name and
    # source_url whether or not the filesystem can hold that name, and every assertion runs.
    safe = _write_csv(
        tmp_path / "prices.csv",
        [(*PRICE_HEADER, "Notes"), ("SCOM", "2026-09-28", "28.45", "1,000", "checked")],
    )
    reader, _ = _open("daily_prices", safe)
    pipeline, ctx = _open("daily_prices", tmp_path / name)
    monkeypatch.setattr(pipeline, "_read_file", reader._read_file)
    assert pipeline.source_url.endswith(name)
    records = pipeline.parse(await pipeline.fetch(ctx))
    staged = _Staged(
        pipeline=pipeline, ctx=ctx, records=records, outcome=pipeline.validate(records)
    )
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


async def test_a_required_header_holding_nul_is_a_layout_change(tmp_path: Path) -> None:
    # Cut at its NUL, "Close" would not match either; read whole, the header is recorded with
    # the NUL shown as U+2400, which details.found (JSONB) can store.
    path = tmp_path / "prices.csv"
    path.write_bytes(b"Ticker,Trade Date,Close\x00 Price,Volume\nSCOM,2026-09-28,28.45,1000\n")
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert (failure.value.expected, failure.value.found, failure.value.message) == (
        ["ticker", "trade_date", "close_price", "volume"],
        ["ticker", "trade_date", "close\u2400_price", "volume"],
        "missing required columns for daily_prices: close_price",
    )


@pytest.mark.parametrize(
    ("file_name", "write"),
    [
        pytest.param(
            "equities.xlsx",
            lambda path: _write_xlsx(path, {"Empty": []}),
            id="empty-worksheet",
        ),
        pytest.param("equities.csv", lambda path: path.write_bytes(b""), id="zero-byte-csv"),
        pytest.param(
            "equities.csv", lambda path: path.write_bytes(b"\xef\xbb\xbf"), id="bom-only-csv"
        ),
        pytest.param(
            "equities.csv", lambda path: path.write_bytes(b"\n\n"), id="line-breaks-only-csv"
        ),
        pytest.param("equities.csv", lambda path: path.write_bytes(b"\r\n"), id="crlf-only-csv"),
    ],
)
async def test_parse_reports_an_empty_file_as_a_layout_change(
    tmp_path: Path, file_name: str, write: Callable[[Path], object]
) -> None:
    # A CSV file without a cell is classified as an empty worksheet is, never as unreadable.
    path = tmp_path / file_name
    write(path)
    pipeline, ctx = _open("equities", path)
    raw = await pipeline.fetch(ctx)
    with pytest.raises(SourceLayoutChanged) as failure:
        pipeline.parse(raw)
    assert failure.value.message == "the file is empty"
    assert failure.value.expected == ["ticker", "company_name", "sector", "shares_outstanding"]
    assert failure.value.found == []
    assert classify_exception(failure.value) == (RunStatus.LAYOUT_CHANGED, "the file is empty")


async def test_a_header_without_data_rows_is_below_the_minimum(tmp_path: Path) -> None:
    staged = await _stage("daily_prices", _write_csv(tmp_path / "prices.csv", [PRICE_HEADER]))
    assert staged.records == []
    assert evaluate_thresholds(0, 0, TabularImportPipeline.min_rows) is RunStatus.LAYOUT_CHANGED


@pytest.mark.parametrize(
    ("file_name", "content", "reason"),
    [
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
            "prices.csv",
            b"ticker,trade_date\nSCOM\n\nEQTY,2026-09-28,extra\n",
            "unreadable_file:ParserError",
            id="ragged-csv-after-short-and-blank-lines",
        ),
        pytest.param(
            "prices.xlsx", b"not a workbook", "unreadable_file:BadZipFile", id="not-a-workbook"
        ),
        # Every private-use character U+E000..U+F8FF is taken, so none can stand in for NUL.
        pytest.param(
            "prices.csv",
            b"ticker\n" + "".join(map(chr, range(0xE000, 0xF900))).encode() + b"\x00\n",
            "unreadable_file:nul_byte",
            id="nul-with-every-private-use-character",
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


async def test_csv_short_and_blank_lines_anywhere_are_widened_to_the_header(
    tmp_path: Path,
) -> None:
    # A short or blank line followed by full rows, which a CSV read in chunks fails on when the
    # short line starts a chunk; read as one table, every line is widened to the header.
    path = tmp_path / "prices.csv"
    path.write_bytes(
        b"Ticker,Trade Date,Close Price,Volume\n"
        b"SCOM,2026-09-28,28.45,1000\n"
        b"\n"
        b"EQTY,2026-09-28\n"
        b"KCB,2026-09-28,40.10,500\n"
        b"ABSA\n"
    )
    staged = await _stage("daily_prices", path)
    assert [record.row_number for record in staged.records] == [2, 4, 5, 6]
    assert [tuple(record.raw) for record in staged.records] == [
        ("ticker", "trade_date", "close_price", "volume")
    ] * 4
    assert [tuple(record.raw.values()) for record in staged.records] == [
        ("SCOM", "2026-09-28", "28.45", "1000"),
        ("EQTY", "2026-09-28", "", ""),
        ("KCB", "2026-09-28", "40.10", "500"),
        ("ABSA", "", "", ""),
    ]
    assert staged.ctx.details["blank_rows_skipped"] == 1


# --- .xlsx archive limits --------------------------------------------------------------------

MANIFEST = "[Content_Types].xml"
WORKBOOK_PART = "xl/workbook.xml"
WORKBOOK_RELATIONSHIPS = "xl/_rels/workbook.xml.rels"
STYLES = "xl/styles.xml"
WORKSHEET = "xl/worksheets/sheet1.xml"
ROUTED = "xl/worksheets/sheet2.xml"
WORKSHEET_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"
SHARED_STRINGS_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"
)
TEMPLATE_MACRO_WORKBOOK_TYPE = "application/vnd.ms-excel.template.macroEnabled.main+xml"
CHARTSHEET_RELATIONSHIP = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/chartsheet"
)
PACKAGE_XML_CAP = 1024 * 1024
SHARED_STRINGS = b'<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"></sst>'
CHARTSHEET = (
    b'<chartsheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"></chartsheet>'
)
LONG_STREAM_BYTES = 32 * 1024 * 1024
PARSE_PEAK_BYTES = 8 * 1024 * 1024
# Offsets of fields in a ZIP central-directory file header (APPNOTE.TXT 4.3.12).
CENTRAL_FLAGS = 8
CENTRAL_CRC = 16
CENTRAL_COMPRESSED_SIZE = 20
CENTRAL_FILE_SIZE = 24
CENTRAL_HEADER_OFFSET = 42


def _members(path: Path) -> dict[str, bytes]:
    """Every member of the archive at ``path``, expanded, in archive order."""
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _save_members(
    path: Path, members: Mapping[str, bytes], compress_types: Mapping[str, int] | None = None
) -> Path:
    """Write ``members`` to ``path``, deflated unless ``compress_types`` names another method."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data, compress_type=(compress_types or {}).get(name))
    return path


def _save_with_long_member(
    path: Path, members: Mapping[str, bytes], name: str, excess: int
) -> Path:
    """Write ``members`` to ``path``, deflated, with ``excess`` spaces after the data of member
    ``name``; they are written 1 MiB at a time, so the long data never exists whole here."""
    piece = b" " * (1024 * 1024)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member, data in members.items():
            if member != name:
                archive.writestr(member, data)
                continue
            with archive.open(member, "w") as part:
                part.write(data)
                for written in range(0, excess, len(piece)):
                    part.write(piece[: excess - written])
    return path


def _member_info(path: Path, name: str) -> zipfile.ZipInfo:
    with zipfile.ZipFile(path) as archive:
        return archive.getinfo(name)


def _member_sizes(path: Path) -> dict[str, int]:
    with zipfile.ZipFile(path) as archive:
        return {member.filename: member.file_size for member in archive.infolist()}


def _patch_central_entry(raw: bytes, name: str, field: int, value: int, width: int = 4) -> bytes:
    """``raw`` with the ``width``-byte little-endian field at offset ``field`` of the
    central-directory file header of member ``name`` set to ``value``."""
    encoded = name.encode()
    entry = raw.find(b"PK\x01\x02")
    while entry >= 0:
        name_length = int.from_bytes(raw[entry + 28 : entry + 30], "little")
        if raw[entry + 46 : entry + 46 + name_length] == encoded:
            start = entry + field
            return raw[:start] + value.to_bytes(width, "little") + raw[start + width :]
        entry = raw.find(b"PK\x01\x02", entry + 4)
    raise AssertionError(f"{name} is not a member of the archive")


def _override(name: str, content_type: str) -> bytes:
    return f'<Override PartName="/{name}" ContentType="{content_type}"/>'.encode()


def _append_child(part: bytes, closing_tag: bytes, *children: bytes) -> bytes:
    """``part`` with ``children`` inserted before its root's closing tag."""
    assert part.count(closing_tag) == 1
    return part.replace(closing_tag, b"".join(children) + closing_tag)


def _chartsheet_relationship(target: str, tag: str = "Relationship") -> bytes:
    return f'<{tag} Type="{CHARTSHEET_RELATIONSHIP}" Target="{target}" Id="rId9"/>'.encode()


def _padded(part: bytes, closing_tag: bytes, size: int) -> bytes:
    """``part`` padded with whitespace before its root's closing tag to exactly ``size`` bytes."""
    return _append_child(part, closing_tag, b" " * (size - len(part)))


def _xlsx_extent(path: Path) -> dict[str, int]:
    """What each ``MAX_XLSX_*`` limit measures in a workbook openpyxl wrote, from its central
    directory: the member count, the members other than its one (streamed) worksheet summed, that
    worksheet, and every member summed, expanded."""
    sizes = _member_sizes(path)
    return {
        "MAX_XLSX_MEMBERS": len(sizes),
        "MAX_XLSX_PARTS_BYTES": sum(sizes.values()) - sizes[WORKSHEET],
        "MAX_XLSX_WORKSHEET_BYTES": sizes[WORKSHEET],
        "MAX_XLSX_EXPANDED_BYTES": sum(sizes.values()),
    }


def _record_excel_opens(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Replace ``pandas.ExcelFile`` by a recorder: a workbook rejected before it opens leaves the
    returned list empty."""
    opened: list[object] = []
    monkeypatch.setattr(pandas, "ExcelFile", lambda *args, **kwargs: opened.append(args))
    return opened


async def _preflight_failure(path: Path) -> FetchFailed:
    return (await _traced_preflight_failure(path))[0]


async def _traced_preflight_failure(path: Path) -> tuple[FetchFailed, int]:
    """Fetch ``path`` and parse it, which must fail the run; the failure, and the peak bytes
    ``tracemalloc`` saw allocated while parsing."""
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    tracemalloc.start()
    try:
        with pytest.raises(FetchFailed) as failure:
            pipeline.parse(raw)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert classify_exception(failure.value)[0] is RunStatus.FAILED
    return failure.value, peak


@pytest.mark.parametrize(
    ("limit", "reason"),
    [
        pytest.param("MAX_XLSX_MEMBERS", "xlsx_too_many_members", id="members"),
        pytest.param("MAX_XLSX_PARTS_BYTES", "xlsx_parts_too_large", id="parts-summed"),
        pytest.param("MAX_XLSX_WORKSHEET_BYTES", "xlsx_worksheet_too_large", id="worksheet"),
        pytest.param("MAX_XLSX_EXPANDED_BYTES", "xlsx_expanded_too_large", id="total"),
    ],
)
async def test_xlsx_one_past_an_archive_limit_fails_before_the_workbook_opens(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, limit: str, reason: str
) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    monkeypatch.setattr(tabular_import, limit, _xlsx_extent(path)[limit] - 1)
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == reason
    assert opened == []


async def test_xlsx_at_every_archive_limit_still_imports(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    for limit, extent in _xlsx_extent(path).items():
        monkeypatch.setattr(tabular_import, limit, extent)
    staged = await _stage("daily_prices", path)
    assert staged.outcome.rejected == []
    assert _values(staged.outcome.valid) == list(SAMPLES["daily_prices"].expected)


async def test_xlsx_worksheet_larger_than_the_parts_budget_still_imports(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The budget is the other parts' sum; the declared worksheet is streamed and outside it.
    rows = [("SCOM", date(2025, 1, 1) + timedelta(days=day), "28.45", 1_000) for day in range(400)]
    path = _write_table(tmp_path / "prices.xlsx", [PRICE_HEADER, *rows])
    sizes = _member_sizes(path)
    budget = sum(sizes.values()) - sizes[WORKSHEET]
    assert sizes[WORKSHEET] > budget
    monkeypatch.setattr(tabular_import, "MAX_XLSX_PARTS_BYTES", budget)
    staged = await _stage("daily_prices", path)
    assert staged.outcome.rejected == []
    assert len(staged.outcome.valid) == 400


def _route_shared_strings(members: dict[str, bytes], size: int) -> None:
    # openpyxl opens the first sharedStrings Override, whatever the name it routes to.
    members[ROUTED] = _padded(SHARED_STRINGS, b"</sst>", size)
    members[MANIFEST] = _append_child(
        members[MANIFEST],
        b"</Types>",
        _override(ROUTED, WORKSHEET_TYPE),
        _override(ROUTED, SHARED_STRINGS_TYPE),
    )


def _route_workbook(members: dict[str, bytes], size: int) -> None:
    # openpyxl opens the workbook its Override names, and that workbook's relationships.
    members[ROUTED] = _padded(members.pop(WORKBOOK_PART), b"</workbook>", size)
    relationships = members.pop(WORKBOOK_RELATIONSHIPS)
    members["xl/worksheets/_rels/sheet2.xml.rels"] = relationships.replace(
        b'Target="styles.xml"', b'Target="../styles.xml"'
    ).replace(b'Target="theme/theme1.xml"', b'Target="../theme/theme1.xml"')
    members[MANIFEST] = _append_child(
        members[MANIFEST].replace(
            f'PartName="/{WORKBOOK_PART}"'.encode(), f'PartName="/{ROUTED}"'.encode()
        ),
        b"</Types>",
        _override(ROUTED, WORKSHEET_TYPE),
    )


def _route_chartsheet(members: dict[str, bytes], size: int) -> None:
    # openpyxl parses a chartsheet relationship's target whole, whatever its name.
    members[ROUTED] = _padded(CHARTSHEET, b"</chartsheet>", size)
    members[MANIFEST] = _append_child(
        members[MANIFEST], b"</Types>", _override(ROUTED, WORKSHEET_TYPE)
    )
    members[WORKBOOK_RELATIONSHIPS] = _append_child(
        members[WORKBOOK_RELATIONSHIPS],
        b"</Relationships>",
        _chartsheet_relationship("worksheets/sheet2.xml"),
    )


@pytest.mark.parametrize(
    "route",
    [
        pytest.param(_route_shared_strings, id="shared-strings-override"),
        pytest.param(_route_workbook, id="workbook-override"),
        pytest.param(_route_chartsheet, id="chartsheet-relationship"),
    ],
)
async def test_xlsx_part_routed_to_a_worksheet_name_counts_toward_the_parts_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    route: Callable[[dict[str, bytes], int], None],
) -> None:
    # The routed member is named and declared like a worksheet, but openpyxl parses it whole;
    # it exceeds the parts budget (every member not named like a worksheet) and is far below
    # the worksheet cap, so a name-based limit would let it through.
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    members = _members(path)
    route(members, 64 * 1024)
    _save_members(path, members)
    sizes = _member_sizes(path)
    budget = sum(size for name, size in sizes.items() if name not in (WORKSHEET, ROUTED))
    assert budget < sizes[ROUTED] < tabular_import.MAX_XLSX_WORKSHEET_BYTES
    monkeypatch.setattr(tabular_import, "MAX_XLSX_PARTS_BYTES", budget)
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == "xlsx_parts_too_large"
    assert opened == []


def _prefix_doctype(name: str, root: str) -> Callable[[dict[str, bytes]], None]:
    def edit(members: dict[str, bytes]) -> None:
        members[name] = f"<!DOCTYPE {root}>".encode() + members[name]

    return edit


def _utf16_manifest_with_doctype(members: dict[str, bytes]) -> None:
    declaration = '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE Types>'
    members[MANIFEST] = (declaration + members[MANIFEST].decode()).encode("utf-16")


def _chartsheet_under_any_tag(members: dict[str, bytes]) -> None:
    # openpyxl reads every child of a relationships part as a relationship, whatever its tag.
    members[WORKBOOK_RELATIONSHIPS] = _append_child(
        members[WORKBOOK_RELATIONSHIPS],
        b"</Relationships>",
        _chartsheet_relationship("chartsheets/sheet1.xml", tag="Chart"),
    )


def _oversized_relationships(members: dict[str, bytes]) -> None:
    members[WORKBOOK_RELATIONSHIPS] = _padded(
        members[WORKBOOK_RELATIONSHIPS], b"</Relationships>", PACKAGE_XML_CAP + 1
    )


def _malformed_relationships(members: dict[str, bytes]) -> None:
    members[WORKBOOK_RELATIONSHIPS] = members[WORKBOOK_RELATIONSHIPS][:-1]


def _absent_relationships(members: dict[str, bytes]) -> None:
    del members[WORKBOOK_RELATIONSHIPS]


def _template_macro_workbook_first(members: dict[str, bytes]) -> None:
    # openpyxl tries the template-macro workbook type before the plain one, so this second
    # workbook, declared after the first, is the one it opens, with its own relationships.
    members["xl/book2.xml"] = members[WORKBOOK_PART]
    members["xl/_rels/book2.xml.rels"] = _append_child(
        members[WORKBOOK_RELATIONSHIPS],
        b"</Relationships>",
        _chartsheet_relationship("chartsheets/sheet1.xml"),
    )
    members[MANIFEST] = _append_child(
        members[MANIFEST], b"</Types>", _override("xl/book2.xml", TEMPLATE_MACRO_WORKBOOK_TYPE)
    )


@pytest.mark.parametrize(
    "edit",
    [
        pytest.param(_prefix_doctype(MANIFEST, "Types"), id="manifest-doctype"),
        pytest.param(_utf16_manifest_with_doctype, id="manifest-doctype-utf-16"),
        pytest.param(_prefix_doctype(WORKBOOK_RELATIONSHIPS, "Relationships"), id="rels-doctype"),
        pytest.param(_chartsheet_under_any_tag, id="rels-chartsheet-any-tag"),
        pytest.param(_oversized_relationships, id="rels-over-1-mib"),
        pytest.param(_malformed_relationships, id="rels-not-well-formed"),
        pytest.param(_absent_relationships, id="rels-absent"),
        pytest.param(_template_macro_workbook_first, id="template-macro-workbook-rels"),
    ],
)
async def test_xlsx_package_parts_that_cannot_be_trusted_withhold_every_worksheet_grant(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, edit: Callable[[dict[str, bytes]], None]
) -> None:
    # The budget is every member but the worksheet: it holds when the worksheet is streamed and
    # is exceeded once the worksheet counts as a part.
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    members = _members(path)
    edit(members)
    _save_members(path, members)
    sizes = _member_sizes(path)
    monkeypatch.setattr(
        tabular_import, "MAX_XLSX_PARTS_BYTES", sum(sizes.values()) - sizes[WORKSHEET]
    )
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == "xlsx_parts_too_large"
    assert opened == []


@pytest.mark.parametrize(
    ("size", "imports"),
    [
        pytest.param(PACKAGE_XML_CAP, True, id="at-1-mib"),
        pytest.param(PACKAGE_XML_CAP + 1, False, id="over-1-mib"),
    ],
)
async def test_xlsx_manifest_over_1_mib_fails_before_it_is_parsed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, size: int, imports: bool
) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    members = _members(path)
    members[MANIFEST] = _padded(members[MANIFEST], b"</Types>", size)
    _save_members(path, members)
    assert _member_sizes(path)[MANIFEST] == size
    if imports:
        staged = await _stage("daily_prices", path)
        assert _values(staged.outcome.valid) == list(SAMPLES["daily_prices"].expected)
        return
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == "xlsx_parts_too_large"
    assert opened == []


@pytest.mark.parametrize(
    ("manifest", "reason"),
    [
        pytest.param(b"<Types>", "unreadable_file:ParseError", id="not-well-formed"),
        pytest.param(None, "unreadable_file:KeyError", id="absent"),
    ],
)
async def test_xlsx_unreadable_manifest_fails_the_run_by_error_type(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, manifest: bytes | None, reason: str
) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    members = _members(path)
    if manifest is None:
        del members[MANIFEST]
    else:
        members[MANIFEST] = manifest
    _save_members(path, members)
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == reason
    assert opened == []


LONGER_DATA = [
    pytest.param(1, id="one-byte-longer"),
    pytest.param(LONG_STREAM_BYTES, id="32-mib-longer"),
]


def _with_long_member(path: Path, member: str, excess: int) -> Path:
    """Rewrite the workbook at ``path`` with ``excess`` bytes more data in ``member`` than its
    central-directory entry declares (the CRC-32 describes the longer data)."""
    members = _members(path)
    _save_with_long_member(path, members, member, excess)
    declared = len(members[member])
    path.write_bytes(_patch_central_entry(path.read_bytes(), member, CENTRAL_FILE_SIZE, declared))
    assert _member_sizes(path)[member] == declared
    return path


@pytest.mark.parametrize("excess", LONGER_DATA)
@pytest.mark.parametrize(
    "member",
    [
        pytest.param(MANIFEST, id="manifest-parsed-by-the-preflight"),
        pytest.param(WORKBOOK_PART, id="workbook-parsed-on-open"),
        pytest.param(STYLES, id="styles-parsed-on-open"),
    ],
)
async def test_xlsx_member_longer_than_its_declared_size_is_unreadable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, member: str, excess: int
) -> None:
    # A whole read (ZipFile.read) inflates all of a member's data in one step and only then cuts
    # it at the declared size, so the preflight inflates every member openpyxl may read whole a
    # bounded chunk at a time first, and stops at the first byte past the declared size.
    path = _with_long_member(_write_sample(tmp_path, "daily_prices", ".xlsx"), member, excess)
    opened = _record_excel_opens(monkeypatch)
    failure, peak = await _traced_preflight_failure(path)
    assert failure.reason == "xlsx_size_mismatch"
    assert opened == []
    assert peak < PARSE_PEAK_BYTES


@pytest.mark.parametrize("excess", LONGER_DATA)
async def test_xlsx_streamed_worksheet_longer_than_its_declared_size_is_unreadable(
    tmp_path: Path, excess: int
) -> None:
    # openpyxl streams a worksheet in reads of a bounded size, which zipfile inflates a bounded
    # chunk at a time and cuts at the declared size; the CRC-32 of the cut data then fails.
    path = _with_long_member(_write_sample(tmp_path, "daily_prices", ".xlsx"), WORKSHEET, excess)
    failure, peak = await _traced_preflight_failure(path)
    assert failure.reason == "unreadable_file:BadZipFile"
    assert peak < PARSE_PEAK_BYTES


def _central_field(
    field: int, value: Callable[[zipfile.ZipInfo, bytes], int]
) -> Callable[[bytes, zipfile.ZipInfo], bytes]:
    def edit(raw: bytes, member: zipfile.ZipInfo) -> bytes:
        return _patch_central_entry(raw, member.filename, field, value(member, raw))

    return edit


def _invalid_deflate_stream(raw: bytes, member: zipfile.ZipInfo) -> bytes:
    # A first byte of 0xFF opens a final block of the reserved type 3, which no inflater accepts.
    lengths = member.header_offset + 26
    start = (
        member.header_offset
        + 30
        + int.from_bytes(raw[lengths : lengths + 2], "little")
        + int.from_bytes(raw[lengths + 2 : lengths + 4], "little")
    )
    return raw[:start] + b"\xff" + raw[start + 1 :]


@pytest.mark.parametrize(
    ("member", "compress_type", "edit"),
    [
        pytest.param(
            STYLES,
            zipfile.ZIP_DEFLATED,
            _central_field(CENTRAL_FILE_SIZE, lambda member, raw: member.file_size + 1),
            id="size-overstated",
        ),
        pytest.param(
            STYLES,
            zipfile.ZIP_STORED,
            _central_field(CENTRAL_FILE_SIZE, lambda member, raw: member.file_size - 1),
            id="stored-sizes-differ",
        ),
        pytest.param(
            WORKBOOK_PART,
            zipfile.ZIP_DEFLATED,
            _central_field(CENTRAL_CRC, lambda member, raw: member.CRC ^ 1),
            id="crc-differs",
        ),
        pytest.param(
            MANIFEST,
            zipfile.ZIP_STORED,
            _central_field(CENTRAL_CRC, lambda member, raw: member.CRC ^ 1),
            id="stored-manifest-crc-differs",
        ),
        pytest.param(
            WORKBOOK_RELATIONSHIPS,
            zipfile.ZIP_DEFLATED,
            _central_field(CENTRAL_CRC, lambda member, raw: member.CRC ^ 1),
            id="rels-crc-differs",
        ),
        pytest.param(
            STYLES,
            zipfile.ZIP_DEFLATED,
            _central_field(CENTRAL_COMPRESSED_SIZE, lambda member, raw: len(raw)),
            id="data-past-the-end",
        ),
        pytest.param(
            STYLES,
            zipfile.ZIP_DEFLATED,
            _central_field(CENTRAL_HEADER_OFFSET, lambda member, raw: member.header_offset + 1),
            id="no-local-header",
        ),
        pytest.param(STYLES, zipfile.ZIP_DEFLATED, _invalid_deflate_stream, id="invalid-deflate"),
    ],
)
async def test_xlsx_member_unlike_its_central_directory_entry_fails_before_the_workbook_opens(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    member: str,
    compress_type: int,
    edit: Callable[[bytes, zipfile.ZipInfo], bytes],
) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    _save_members(path, _members(path), {member: compress_type})
    path.write_bytes(edit(path.read_bytes(), _member_info(path, member)))
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == "xlsx_size_mismatch"
    assert opened == []


@pytest.mark.parametrize(
    ("member", "compress_type", "encrypted"),
    [
        pytest.param(STYLES, zipfile.ZIP_BZIP2, False, id="bzip2"),
        pytest.param(WORKSHEET, zipfile.ZIP_LZMA, False, id="lzma-worksheet"),
        pytest.param(WORKSHEET, zipfile.ZIP_DEFLATED, True, id="encrypted-worksheet"),
        pytest.param(MANIFEST, zipfile.ZIP_STORED, True, id="encrypted-stored-manifest"),
    ],
)
async def test_xlsx_member_outside_the_package_format_fails_before_the_workbook_opens(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    member: str,
    compress_type: int,
    encrypted: bool,
) -> None:
    # Open Packaging Conventions allow only stored and deflated, unencrypted members.
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    _save_members(path, _members(path), {member: compress_type})
    if encrypted:
        flags = _member_info(path, member).flag_bits | 0x1
        path.write_bytes(_patch_central_entry(path.read_bytes(), member, CENTRAL_FLAGS, flags, 2))
    assert _member_info(path, member).compress_type == compress_type
    opened = _record_excel_opens(monkeypatch)
    assert (await _preflight_failure(path)).reason == "xlsx_unsupported_member"
    assert opened == []


async def test_xlsx_with_stored_members_still_imports(tmp_path: Path) -> None:
    path = _write_sample(tmp_path, "daily_prices", ".xlsx")
    members = _members(path)
    _save_members(path, members, dict.fromkeys(members, zipfile.ZIP_STORED))
    with zipfile.ZipFile(path) as archive:
        assert {info.compress_type for info in archive.infolist()} == {zipfile.ZIP_STORED}
    staged = await _stage("daily_prices", path)
    assert staged.outcome.rejected == []
    assert _values(staged.outcome.valid) == list(SAMPLES["daily_prices"].expected)


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


@pytest.mark.parametrize(
    ("kind", "column", "text", "accepted", "reasons"),
    [
        # The past row is the kind's baseline, so its date is always accepted last.
        pytest.param(kind, column, text, [*accepted, past], reasons, id=f"{kind}-{label}")
        for kind, column, past in (
            ("daily_prices", "trade_date", SESSION),
            ("foreign_flows", "trade_date", SESSION),
            ("macro_indicators", "observation_date", date(2026, 9, 30)),
        )
        for label, text, accepted, reasons in (
            ("today", "2026-10-30", [NAIROBI_TODAY], []),
            ("today-day-month-year", "30/10/2026", [NAIROBI_TODAY], []),
            ("tomorrow", "2026-10-31", [], [f"future_date:{column}"]),
            ("future-day-month-year", "31/12/2026", [], [f"future_date:{column}"]),
            # Not a date at all, so reported by its conversion code only.
            ("impossible-future-date", "31/11/2026", [], [f"invalid_date:{column}"]),
        )
    ],
)
async def test_validate_rejects_a_date_after_today_in_nairobi(
    tmp_path: Path,
    kind: ImportKind,
    column: str,
    text: str,
    accepted: list[date],
    reasons: list[str],
) -> None:
    """Today is the Nairobi date of the injected clock, read once for the whole file: a kind whose
    date names an observation (a price or flow ``trade_date``, a macro ``observation_date``)
    accepts it and rejects any later date, which counts toward the thresholds."""
    readings: list[datetime] = []

    def clock() -> datetime:
        readings.append(FROZEN_NOW)
        return FROZEN_NOW

    row = {**VALID_CELLS[kind], column: text}
    # A second row, dated in the past: the clock is read once per file, never once per row.
    path = _write_csv(
        tmp_path / f"{kind}.csv",
        [tuple(row), tuple(row.values()), tuple(VALID_CELLS[kind].values())],
    )
    staged = await _stage(kind, path, now=clock)
    assert len(readings) == 1
    assert [record.values[column] for record in staged.outcome.valid] == accepted
    assert staged.outcome.rejected == [
        RejectedRow(index=2, reason=reason, row=row) for reason in reasons
    ]


async def test_validate_refuses_a_clock_without_a_timezone(tmp_path: Path) -> None:
    cells = VALID_CELLS["daily_prices"]
    path = _write_csv(tmp_path / "prices.csv", [tuple(cells), tuple(cells.values())])
    pipeline, ctx = _open("daily_prices", path, now=lambda: FROZEN_NOW.replace(tzinfo=None))
    records = pipeline.parse(await pipeline.fetch(ctx))
    with pytest.raises(ValueError, match=re.escape("now() must return a timezone-aware datetime")):
        pipeline.validate(records)


PRICES_CSV_HEADER = b"Ticker,Trade Date,Close Price,Volume\n"
KCB_PRICE_ROW = b"KCB,2026-09-28,40.10,500\n"
KCB_PRICE_VALUES = {
    "ticker": "KCB",
    "trade_date": SESSION,
    "close_price": Decimal("40.10"),
    "volume": 500,
    "turnover_kes": Decimal("20050.00"),
    "turnover_estimated": True,
}
NUL_CELL_CASES = [
    pytest.param(
        "daily_prices",
        b"ticker,trade_date,close_price,volume\n"
        b"EQTY,2026-09-17,12\x0034.56,1\n"
        b"KCB,2026-09-17,85.00,7\x00000\n",
        [],
        [
            RejectedRow(
                index=2,
                reason="invalid_character:close_price",
                row={
                    "ticker": "EQTY",
                    "trade_date": "2026-09-17",
                    "close_price": "12\u240034.56",
                    "volume": "1",
                },
            ),
            RejectedRow(
                index=3,
                reason="invalid_character:volume",
                row={
                    "ticker": "KCB",
                    "trade_date": "2026-09-17",
                    "close_price": "85.00",
                    "volume": "7\u2400000",
                },
            ),
        ],
        None,
        id="numbers-are-not-cut-at-the-nul",
    ),
    pytest.param(
        "daily_prices",
        PRICES_CSV_HEADER + b"SCOM\x00,2026-09-28,28.45,1000\n" + KCB_PRICE_ROW,
        [KCB_PRICE_VALUES],
        [
            RejectedRow(
                index=2,
                reason="invalid_character:ticker",
                row={
                    "ticker": "SCOM\u2400",
                    "trade_date": "2026-09-28",
                    "close_price": "28.45",
                    "volume": "1000",
                },
            )
        ],
        None,
        id="ticker-with-a-trailing-nul",
    ),
    pytest.param(
        "daily_prices",
        PRICES_CSV_HEADER + b"EQTY,2026-09-28,10.60,\x00\n" + KCB_PRICE_ROW,
        [KCB_PRICE_VALUES],
        [
            RejectedRow(
                index=2,
                reason="invalid_character:volume",
                row={
                    "ticker": "EQTY",
                    "trade_date": "2026-09-28",
                    "close_price": "10.60",
                    "volume": "\u2400",
                },
            )
        ],
        None,
        id="nul-only-volume-is-not-an-empty-cell",
    ),
    pytest.param(
        "fundamentals",
        b"Ticker,Period End Date,Revenue KES\nSCOM,2026-03-31,\x00\n",
        [],
        [
            RejectedRow(
                index=2,
                reason="invalid_character:revenue_kes",
                row={"ticker": "SCOM", "period_end_date": "2026-03-31", "revenue_kes": "\u2400"},
            )
        ],
        None,
        id="nul-only-value-is-not-no-values",
    ),
    # U+E000, the first stand-in candidate, is the file's own text: it is kept verbatim, and only
    # the cell holding NUL is rejected.
    pytest.param(
        "equities",
        "Ticker,Company Name,Sector,Shares Outstanding\n"
        "SCOM,Safaricom \ue000 PLC,Telecommunication,40065428000\n"
        "KCB,Nul\x00Byte Co,Banking,3213462815\n".encode(),
        [
            {
                "ticker": "SCOM",
                "company_name": "Safaricom \ue000 PLC",
                "sector": "Telecommunication",
                "shares_outstanding": 40_065_428_000,
            }
        ],
        [
            RejectedRow(
                index=3,
                reason="invalid_character:company_name",
                row={
                    "ticker": "KCB",
                    "company_name": "Nul\u2400Byte Co",
                    "sector": "Banking",
                    "shares_outstanding": "3213462815",
                },
            )
        ],
        None,
        id="private-use-text-kept-beside-a-nul-text-cell",
    ),
    # A column the kind does not read is ignored, NUL or not; its name is recorded readable.
    pytest.param(
        "daily_prices",
        b"Ticker,Trade Date,Close Price,Volume,No\x00tes\nKCB,2026-09-28,40.10,500,check\x00ed\n",
        [KCB_PRICE_VALUES],
        [],
        ["no\u2400tes"],
        id="ignored-column",
    ),
]


@pytest.mark.parametrize(("kind", "content", "valid", "rejected", "ignored"), NUL_CELL_CASES)
async def test_a_cell_holding_nul_is_rejected_whole_with_readable_evidence(
    tmp_path: Path,
    kind: ImportKind,
    content: bytes,
    valid: list[dict[str, object]],
    rejected: list[RejectedRow],
    ignored: list[str] | None,
) -> None:
    """pandas' C parser would cut each cell at its NUL ("12\\x0034.56" read as 12). The cell is
    rejected instead, whatever its column, and the row's raw cells show the NUL as U+2400, which
    ``details.rejected`` (JSONB) can store."""
    path = tmp_path / f"{kind}.csv"
    path.write_bytes(content)
    staged = await _stage(kind, path)
    assert _values(staged.outcome.valid) == valid
    assert staged.outcome.rejected == rejected
    assert staged.ctx.details.get("ignored_columns") == ignored


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


# --- Compact rows and memory ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "value", "stored_as_int"),
    [
        pytest.param("close_price", Decimal("28.4500"), True, id="price-at-its-scale"),
        pytest.param("close_price", Decimal("0.0000"), True, id="zero-at-its-scale"),
        pytest.param("foreign_net_flow_kes", Decimal("-1250000.00"), True, id="negative-kes"),
        pytest.param(
            "turnover_kes", Decimal("9999999999999999999999.99"), True, id="largest-turnover"
        ),
        pytest.param("close_price", Decimal("28.45"), False, id="other-exponent-kept"),
        pytest.param("close_price", Decimal("-0.0000"), False, id="negative-zero-kept"),
        pytest.param("turnover_kes", Decimal("1E+3"), False, id="positive-exponent-kept"),
    ],
)
def test_compact_row_reads_back_each_decimal_with_its_value_and_exponent(
    column: str, value: Decimal, stored_as_int: bool
) -> None:
    row = tabular_import._RowLayouts().row({column: value})
    # A decimal at its column's scale is held as its scaled integer; any other stays as it is.
    assert (type(row.cells[0]) is int) is stored_as_int
    # A caller's thread context (3 digits, rounding down) cannot change what is read back.
    with localcontext(Context(prec=3, rounding=ROUND_DOWN)):
        read = [row[column], row.get(column), dict(row)[column]]
    assert [type(item) for item in read] == [Decimal] * 3
    assert [item.as_tuple() for item in read if isinstance(item, Decimal)] == [value.as_tuple()] * 3


def test_compact_row_is_a_read_only_mapping_equal_to_its_dict() -> None:
    items: dict[str, object] = {
        "ticker": "SCOM",
        "trade_date": SESSION,
        "close_price": Decimal("28.4500"),
        "volume": 1_000,
        "open_price": None,
        "turnover_estimated": True,
    }
    row = tabular_import._RowLayouts().row(cast(Any, items))
    assert row == items
    assert items == row
    assert row != {**items, "volume": 1_001}
    assert row != {**items, "high_price": None}
    assert list(row) == list(items)
    assert list(row.items()) == list(items.items())
    assert len(row) == len(items)
    assert ("volume" in row, "high_price" in row) == (True, False)
    assert (row.get("high_price"), row.get("high_price", "-")) == (None, "-")
    assert row["turnover_estimated"] is True
    assert type(row["volume"]) is int
    with pytest.raises(KeyError):
        _ = row["high_price"]
    with pytest.raises(TypeError):
        cast(Any, row)["volume"] = 2
    # An amount is a Decimal or nothing: a float never enters a row.
    with pytest.raises(TypeError, match="close_price holds float, expected Decimal"):
        tabular_import._RowLayouts().row({"close_price": 28.45})


def test_updated_compact_row_shares_unchanged_cells_and_keeps_column_places() -> None:
    layouts = tabular_import._RowLayouts()
    row = layouts.row({"ticker": "SCOM", "close_price": Decimal("28.4500"), "turnover_kes": None})
    changes = {"turnover_kes": Decimal("28450.00"), "turnover_estimated": True}
    updated = layouts.updated(row, changes)
    assert list(updated.items()) == [
        ("ticker", "SCOM"),
        ("close_price", Decimal("28.4500")),
        ("turnover_kes", Decimal("28450.00")),
        ("turnover_estimated", True),
    ]
    assert [updated.cells[index] is row.cells[index] for index in (0, 1)] == [True, True]
    assert row == {"ticker": "SCOM", "close_price": Decimal("28.4500"), "turnover_kes": None}
    assert layouts.updated(row, changes).layout is updated.layout


def test_raw_cells_map_each_column_to_its_text_and_refuse_a_nul() -> None:
    layout = tabular_import._RowLayouts().layout(("ticker", "close_price", "volume"))
    raw = tabular_import._RawCells(layout, "SCOM\x0028.45\x00")
    assert raw == {"ticker": "SCOM", "close_price": "28.45", "volume": ""}
    assert (list(raw), len(raw), "volume" in raw) == (["ticker", "close_price", "volume"], 3, True)
    with pytest.raises(ValueError, match="must not hold NUL"):
        tabular_import._RawCells(layout, "SC\x00OM\x0028.45\x00")


@pytest.mark.parametrize(
    ("limit", "shared"),
    [
        pytest.param(10_000, True, id="shared"),
        pytest.param(1, False, id="table-emptied-when-full"),
    ],
)
async def test_parse_shares_a_value_recurring_in_a_column_through_a_bounded_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, limit: int, shared: bool
) -> None:
    monkeypatch.setattr(tabular_import, "_SHARED_VALUES_LIMIT", limit)
    path = _write_csv(
        tmp_path / "prices.csv",
        [
            PRICE_HEADER,
            ("SCOM", SESSION, "28.45", "12.5"),
            ("SCOM", NEXT_SESSION, "28.45", "12.5"),
            ("SCOM", SESSION, "28.45", "1,000"),
        ],
    )
    staged = await _stage("daily_prices", path)
    first, _, third = staged.records
    # A text that fails is converted, and reported, every time it appears.
    assert [record.errors for record in staged.records] == [("invalid_integer:volume",)] * 2 + [()]
    assert third.values["trade_date"] == first.values["trade_date"] == SESSION
    assert (third.values["trade_date"] is first.values["trade_date"]) is shared


MEMORY_ROWS = 5_000
# Peak bytes per row that tracemalloc sees while parse and validate hold a daily_prices file whose
# prices, volumes and flows are all distinct, so no value is shared and the bound measures the
# rows themselves. Measured on Python 3.12 with pandas 3.0.5: about 970 B/row with compact rows,
# and 1,904 B/row with a dict of Decimals plus a dict of the frame's raw strings per record, which
# took a 1,000,000-row import to about 1.8 GiB. 1,400 leaves over 40% headroom for allocator and
# version differences and still fails rows of per-cell Decimals and dicts.
MEMORY_BOUND_PER_ROW = 1_400


def _distinct_price_rows(count: int) -> list[tuple[object, ...]]:
    """``count`` rows of the daily_prices sample's nine columns, eight tickers per session, an
    empty turnover (``validate`` estimates it) and a flow on every third row, every amount and
    volume distinct."""
    tickers = ("SCOM", "EQTY", "KCB", "EABL", "KEGN", "ABSA", "COOP", "NCBA")
    rows: list[tuple[object, ...]] = []
    for index in range(count):
        close = Decimal(1_000 + index) / 100
        rows.append(
            (
                tickers[index % len(tickers)],
                date(1990, 1, 1) + timedelta(days=index // len(tickers)),
                f"{close - Decimal('0.05'):.2f}",
                f"{close + Decimal('0.50'):.2f}",
                f"{close - Decimal('0.50'):.2f}",
                f"{close:.2f}",
                1_000 + index,
                None,
                f"{index - 25_000}.50" if index % 3 == 0 else None,
            )
        )
    return rows


async def test_parse_and_validate_hold_a_bounded_number_of_bytes_per_row(tmp_path: Path) -> None:
    header = SAMPLES["daily_prices"].header
    # pandas imports parts of itself on first use; a small file first keeps that out of the count.
    await _stage(
        "daily_prices", _write_csv(tmp_path / "warm.csv", [header, *_distinct_price_rows(8)])
    )
    path = _write_csv(tmp_path / "prices.csv", [header, *_distinct_price_rows(MEMORY_ROWS)])
    pipeline, ctx = _open("daily_prices", path)
    raw = await pipeline.fetch(ctx)
    tracemalloc.start()
    try:
        records = pipeline.parse(raw)
        outcome = pipeline.validate(records)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert (len(records), len(outcome.valid), outcome.rejected) == (MEMORY_ROWS, MEMORY_ROWS, [])
    assert peak / MEMORY_ROWS < MEMORY_BOUND_PER_ROW


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


async def test_reconcile_rejects_every_row_of_a_rejected_repeated_key_in_line_order(
    tmp_path: Path,
) -> None:
    """Rows repeating a key collapse into the last one. A rejected key rejects each of its rows,
    in line order among the other rejections; the earlier row of an accepted key is neither
    accepted nor rejected."""
    path = _write_csv(
        tmp_path / "prices.csv",
        [
            PRICE_HEADER,
            ("ZZZZ", SESSION, "1.00", 1),
            ("KQ", SESSION, "3.85", 2),
            ("SCOM", SESSION, "28.40", 3),
            ("ZZZZ", SESSION, "1.10", 4),
            ("SCOM", SESSION, "28.45", 5),
        ],
    )
    conn = _ScriptedConnection([("SCOM",)])
    staged = await _stage("daily_prices", path, conn=conn)
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert [(record.row_number, record.values["close_price"]) for record in reconciled.valid] == [
        (6, Decimal("28.45"))
    ]
    assert [(row.index, row.reason, dict(row.row)) for row in reconciled.rejected] == [
        (
            line,
            "unknown_ticker",
            {"ticker": ticker, "trade_date": "2026-09-28", "close_price": close, "volume": volume},
        )
        for line, ticker, close, volume in (
            (2, "ZZZZ", "1.00", "1"),
            (3, "KQ", "3.85", "2"),
            (5, "ZZZZ", "1.10", "4"),
        )
    ]
    assert staged.ctx.details["duplicates_collapsed"] == 2
    assert [_query(statement) for statement in conn.statements] == [
        (["terminal_equities"], [["KQ", "SCOM", "ZZZZ"]])
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
    every row as written; ``update_flows`` reports the ``missing_flow_keys`` its rows request as
    without a price row."""
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
        requested = {(row["ticker"], row["trade_date"]) for row in rows}
        missing = tuple(key for key in recorded.missing_flow_keys if key in requested)
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
    # Rows reach the writer in conflict-key order, whatever the file order.
    in_key_order = sorted(
        SAMPLES[kind].expected,
        key=lambda expected: tuple(expected[column] for column in CONFLICT_KEYS[kind]),
    )
    assert call.rows == [
        {**{column: expected.get(column) for column in columns}, **stamped}
        for expected in in_key_order
    ]
    assert [tuple(row) for row in call.rows] == [(*columns, *stamped)] * 2
    assert write_conn.statements == []


# Each sample file lists its two rows in descending key order.
KEYS_IN_ORDER: Mapping[ImportKind, list[tuple[object, ...]]] = {
    "equities": [("EQTY",), ("SCOM",)],
    "daily_prices": [("EQTY", SESSION), ("SCOM", SESSION)],
    "fundamentals": [("EQTY", date(2025, 12, 31), "FY"), ("SCOM", date(2026, 3, 31), "FY")],
    "macro_indicators": [(date(2026, 8, 31),), (date(2026, 9, 30),)],
    "foreign_flows": [("EQTY", SESSION), ("SCOM", SESSION)],
}


@pytest.mark.parametrize("kind", KINDS)
async def test_upsert_writes_bounded_batches_in_key_order_on_the_run_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, writers: _Writers, kind: ImportKind
) -> None:
    monkeypatch.setattr(tabular_import, "_WRITE_BATCH_ROWS", 1)
    staged = await _stage(kind, _write_sample(tmp_path, kind))
    write_conn = _ScriptedConnection()
    written = await staged.pipeline.upsert(
        cast(AsyncConnection, write_conn), staged.outcome.valid, RUN_ID
    )
    assert written == 2
    assert [(call.conn, len(call.rows)) for call in writers.calls] == [(write_conn, 1)] * 2
    assert [
        tuple(call.rows[0][column] for column in CONFLICT_KEYS[kind]) for call in writers.calls
    ] == KEYS_IN_ORDER[kind]


FLOW_SAMPLE_NO_PRICE_ROWS: tuple[tuple[int, str, dict[str, str]], ...] = (
    (
        3,
        "no_price_row",
        {"ticker": "EQTY", "trade_date": "2026-09-28", "foreign_net_flow_kes": "3,400,500.565"},
    ),
    (
        2,
        "no_price_row",
        {"ticker": "SCOM", "trade_date": "2026-09-28", "foreign_net_flow_kes": "(1,250,000.00)"},
    ),
)
"""The ``foreign_flows`` sample's rows as write-time ``no_price_row`` rejections (line, reason,
raw cells), in key order: EQTY's line 3 comes first."""


@pytest.mark.parametrize(
    ("sample_limit", "sample"),
    [
        pytest.param(50, [["EQTY", SESSION], ["SCOM", SESSION]], id="every-key"),
        pytest.param(1, [["EQTY", SESSION]], id="first-key-only"),
    ],
)
async def test_foreign_flows_batches_report_their_missing_keys_together(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    writers: _Writers,
    caplog: pytest.LogCaptureFixture,
    sample_limit: int,
    sample: list[list[object]],
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    monkeypatch.setattr(tabular_import, "_WRITE_BATCH_ROWS", 1)
    monkeypatch.setattr(tabular_import, "_MISSING_KEYS_SAMPLE", sample_limit)
    staged = await _stage("foreign_flows", _write_sample(tmp_path, "foreign_flows"))
    writers.missing_flow_keys = (("EQTY", SESSION), ("SCOM", SESSION))
    written = await staged.pipeline.upsert(
        cast(AsyncConnection, _ScriptedConnection()), staged.outcome.valid, RUN_ID
    )
    assert written == 0
    assert len(writers.calls) == 2
    # Every key is rejected, in key order across the batches, whatever the sample keeps.
    assert [
        (row.index, row.reason, dict(row.row)) for row in staged.pipeline.write_rejections()
    ] == list(FLOW_SAMPLE_NO_PRICE_ROWS)
    assert staged.ctx.details["flow_missing_keys"] == {"count": 2, "sample": sample}
    # One line for the whole run: the run id and the count, never the file's name or a value.
    assert caplog.messages == [
        f"foreign_flows import run {RUN_ID} rejected 2 rows at write time: no_price_row"
    ]


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
    assert [
        (row.index, row.reason, dict(row.row)) for row in staged.pipeline.write_rejections()
    ] == [FLOW_SAMPLE_NO_PRICE_ROWS[0]]
    assert staged.ctx.details["flow_missing_keys"] == {
        "count": 1,
        "sample": [["EQTY", SESSION]],
    }
    assert caplog.messages == [
        f"foreign_flows import run {RUN_ID} rejected 1 rows at write time: no_price_row"
    ]


async def test_foreign_flows_upsert_rejects_the_rows_collapsed_into_a_key_without_a_price_row(
    tmp_path: Path, writers: _Writers, caplog: pytest.LogCaptureFixture
) -> None:
    """SCOM's line 4 superseded its line 2 at reconcile; when its price row is gone at write time,
    both lines are rejected, while the flow details still count one missing key."""
    caplog.set_level(logging.WARNING, logger=LOGGER)
    path = _write_csv(
        tmp_path / "flows.csv",
        [
            SAMPLES["foreign_flows"].header,
            ("SCOM", SESSION, "100"),
            ("EQTY", SESSION, "200"),
            ("SCOM", SESSION, "300"),
        ],
    )
    staged = await _stage(
        "foreign_flows", path, conn=_ScriptedConnection([("EQTY", SESSION), ("SCOM", SESSION)])
    )
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert (len(reconciled.valid), reconciled.rejected) == (2, [])
    writers.missing_flow_keys = (("SCOM", SESSION),)
    written = await staged.pipeline.upsert(
        cast(AsyncConnection, _ScriptedConnection()), reconciled.valid, RUN_ID
    )
    assert written == 1
    assert [
        (row.index, row.reason, dict(row.row)) for row in staged.pipeline.write_rejections()
    ] == [
        (
            line,
            "no_price_row",
            {"ticker": "SCOM", "trade_date": "2026-09-28", "foreign_net_flow_kes": flow},
        )
        for line, flow in ((2, "100"), (4, "300"))
    ]
    assert staged.ctx.details["flow_missing_keys"] == {
        "count": 1,
        "sample": [["SCOM", SESSION]],
    }
    assert caplog.messages == [
        f"foreign_flows import run {RUN_ID} rejected 2 rows at write time: no_price_row"
    ]


async def test_foreign_flows_upsert_fails_on_a_missing_key_its_batch_does_not_hold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    staged = await _stage("foreign_flows", _write_sample(tmp_path, "foreign_flows"))

    async def report_a_key_never_requested(
        executor: object, rows: Sequence[Mapping[str, object]], *, flow_source: str
    ) -> FlowUpdateResult:
        return FlowUpdateResult(rows_updated=len(rows), missing_keys=(("KCB", SESSION),))

    monkeypatch.setattr(tabular_import, "update_flows", report_a_key_never_requested)

    with pytest.raises(RuntimeError) as raised:
        await staged.pipeline.upsert(
            cast(AsyncConnection, _ScriptedConnection()), staged.outcome.valid, RUN_ID
        )

    assert str(raised.value) == (
        "update_flows reported 1 keys without a price row that its batch of 2 foreign_flows rows "
        "does not hold"
    )
    # An unexpected exception: the run ends failed and its transaction rolls back.
    assert classify_exception(raised.value)[0] is RunStatus.FAILED
    assert staged.pipeline.write_rejections() == ()
    assert "flow_missing_keys" not in staged.ctx.details
    assert caplog.messages == []


FUNDAMENTALS_HEADER = ("Ticker", "Period End Date", "Period Type", "Currency", "Revenue KES")
SCOM_LINE = ("SCOM", date(2026, 3, 31), "FY", "KES", "1")
EQTY_H1_LINE = ("EQTY", date(2026, 6, 30), "H1", "USD", "2")
"""Two fundamentals rows of one write batch; in key order the EQTY row (line 3) comes first."""


async def _stage_one_skipped_fundamentals_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> _Staged:
    """Stage :data:`SCOM_LINE` and :data:`EQTY_H1_LINE`, with ``upsert_fundamentals`` replaced
    by a writer whose currency guard skips one row of each batch."""

    async def skip_one(
        executor: object,
        rows: Sequence[Mapping[str, object]],
        *,
        source: str,
        filing_id: uuid.UUID | None = None,
    ) -> int:
        return len(rows) - 1

    monkeypatch.setattr(tabular_import, "upsert_fundamentals", skip_one)
    path = _write_csv(tmp_path / "fundamentals.csv", [FUNDAMENTALS_HEADER, SCOM_LINE, EQTY_H1_LINE])
    return await _stage("fundamentals", path)


async def test_fundamentals_upsert_rejects_a_row_skipped_for_a_currency_stored_since_reconcile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    staged = await _stage_one_skipped_fundamentals_row(monkeypatch, tmp_path)
    conn = _ScriptedConnection(
        [("EQTY", date(2026, 6, 30), "H1", "KES"), ("SCOM", date(2026, 3, 31), "FY", "KES")]
    )

    written = await staged.pipeline.upsert(
        cast(AsyncConnection, conn), staged.outcome.valid, RUN_ID
    )

    assert written == 1
    assert [
        (row.index, row.reason, dict(row.row)) for row in staged.pipeline.write_rejections()
    ] == [
        (
            3,
            "currency_conflict",
            {
                "ticker": "EQTY",
                "period_end_date": "2026-06-30",
                "period_type": "H1",
                "currency": "USD",
                "revenue_kes": "2",
            },
        )
    ]
    # One bound select() of exactly the batch's periods, in key order, after the write.
    assert [_query(statement) for statement in conn.statements] == [
        (
            ["terminal_fundamentals", "requested"],
            ["EQTY", date(2026, 6, 30), "H1", "SCOM", date(2026, 3, 31), "FY"],
        )
    ]
    assert conn.unanswered == 0
    # The run id and the count only: never the file's name or a value.
    assert caplog.messages == [
        f"fundamentals import run {RUN_ID} rejected 1 rows at write time: currency_conflict"
    ]


async def test_fundamentals_upsert_rejects_the_rows_collapsed_into_a_period_stored_since(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """EQTY's line 4 superseded its line 3 at reconcile; when a concurrent writer has stored the
    period in KES by the write, both USD lines are rejected for it."""
    caplog.set_level(logging.WARNING, logger=LOGGER)

    async def skip_one(
        executor: object,
        rows: Sequence[Mapping[str, object]],
        *,
        source: str,
        filing_id: uuid.UUID | None = None,
    ) -> int:
        return len(rows) - 1

    monkeypatch.setattr(tabular_import, "upsert_fundamentals", skip_one)
    path = _write_csv(
        tmp_path / "fundamentals.csv",
        [
            FUNDAMENTALS_HEADER,
            SCOM_LINE,
            ("EQTY", date(2026, 6, 30), "H1", "USD", "1"),
            EQTY_H1_LINE,
        ],
    )
    # Both tickers are known and neither period is stored when reconcile reads them.
    staged = await _stage(
        "fundamentals", path, conn=_ScriptedConnection([("EQTY",), ("SCOM",)], [])
    )
    reconciled = await staged.pipeline.reconcile(staged.ctx, staged.outcome.valid)
    assert (len(reconciled.valid), reconciled.rejected) == (2, [])
    conn = _ScriptedConnection(
        [("EQTY", date(2026, 6, 30), "H1", "KES"), ("SCOM", date(2026, 3, 31), "FY", "KES")]
    )

    written = await staged.pipeline.upsert(cast(AsyncConnection, conn), reconciled.valid, RUN_ID)

    assert written == 1
    assert [
        (row.index, row.reason, dict(row.row)) for row in staged.pipeline.write_rejections()
    ] == [
        (
            line,
            "currency_conflict",
            {
                "ticker": "EQTY",
                "period_end_date": "2026-06-30",
                "period_type": "H1",
                "currency": "USD",
                "revenue_kes": revenue,
            },
        )
        for line, revenue in ((3, "1"), (4, "2"))
    ]
    assert caplog.messages == [
        f"fundamentals import run {RUN_ID} rejected 2 rows at write time: currency_conflict"
    ]


@pytest.mark.parametrize(
    ("stored", "explained"),
    [
        pytest.param([], 0, id="nothing-stored"),
        pytest.param(
            [("EQTY", date(2026, 6, 30), "H1", "USD"), ("SCOM", date(2026, 3, 31), "FY", "KES")],
            0,
            id="same-currencies-stored",
        ),
        pytest.param(
            [("EQTY", date(2026, 6, 30), "H1", "KES"), ("SCOM", date(2026, 3, 31), "FY", "USD")],
            2,
            id="more-conflicts-than-skipped-rows",
        ),
    ],
)
async def test_fundamentals_upsert_fails_on_a_short_write_conflicts_do_not_explain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    stored: list[tuple[object, ...]],
    explained: int,
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    staged = await _stage_one_skipped_fundamentals_row(monkeypatch, tmp_path)

    with pytest.raises(RuntimeError) as raised:
        await staged.pipeline.upsert(
            cast(AsyncConnection, _ScriptedConnection(stored)), staged.outcome.valid, RUN_ID
        )

    assert str(raised.value) == (
        "fundamentals upsert left 1 of 2 batch rows unwritten, "
        f"of which currency conflicts explain {explained}"
    )
    # An unexpected exception: the run ends failed and its transaction rolls back.
    assert classify_exception(raised.value)[0] is RunStatus.FAILED
    assert staged.pipeline.write_rejections() == ()
    assert caplog.messages == []


# --- Imports into a seeded database ----------------------------------------------------------
#
# Each test below requests ``seeded_database``, a fresh clone of the migrated and seeded template,
# so it can run alone or in any order. The importer builds its own engine from the clone's
# settings; the tests read and pre-insert rows through the clone's engine with the ``terminal_``
# models and the ``db.upsert`` writers only.


class _Clone(Protocol):
    """The part of the root conftest's ``seeded_database`` handle these tests use."""

    @property
    def settings(self) -> TerminalSettings: ...

    @property
    def engine(self) -> AsyncEngine: ...


SAMPLE_CSV = Path("files") / "daily_prices_sample.csv"
SAMPLE_START = date(2026, 9, 24)
"""The earliest trade date in the sample; snapshots read every price row from here on."""
NEW_SESSION = date(2026, 10, 27)
"""The sample's session after the seeded history, which ends on 2026-09-30."""
INVALID_SAMPLE_ROWS: Mapping[tuple[str, date], str] = {
    ("ABSA", SAMPLE_START): "non_positive:close_price",
    ("SBIC", SAMPLE_START): "out_of_range:volume",
}
"""The sample's two invalid rows (close 0, volume 1000000000001) and their rejection codes."""
CENT = Decimal("0.01")

PRICE_KEY = ("ticker", "trade_date")
PRICE_VALUE_COLUMNS = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "turnover_kes",
    "turnover_estimated",
    "foreign_net_flow_kes",
    "source",
    "flow_source",
)
"""What an import may change on a price row, ``ingestion_run_id`` and ``ingested_at`` aside."""
PRICE_READ_COLUMNS = (*PRICE_VALUE_COLUMNS, "ingestion_run_id", "ingested_at")
FLOW_COLUMNS = ("foreign_net_flow_kes", "flow_source")
FUNDAMENTAL_KEY = ("ticker", "period_end_date", "period_type")
FUNDAMENTAL_READ_COLUMNS = (
    *FUNDAMENTAL_VALUE_COLUMNS,
    "currency",
    "filing_id",
    "source_by_field",
    "updated_at",
)
MACRO_READ_COLUMNS = (*MACRO_COLUMNS, "source_by_field", "updated_at")

StoredRows = dict[tuple[Any, ...], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class _FixtureRow:
    """One data row of the recorded sample: its line, key and cell texts by normalized column."""

    line: int
    ticker: str
    trade_date: date
    texts: tuple[str, ...]
    cells: Mapping[str, str]

    @property
    def key(self) -> tuple[str, date]:
        return (self.ticker, self.trade_date)


def _read_sample(fixtures_dir: Path) -> tuple[list[str], list[_FixtureRow]]:
    """The sample's header as written and its rows, read with the stdlib ``csv`` module."""
    with (fixtures_dir / SAMPLE_CSV).open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        columns = [normalize_column_name(name) for name in header]
        rows: list[_FixtureRow] = []
        for line, texts in enumerate(reader, start=2):
            cells = dict(zip(columns, texts, strict=True))
            trade_date = parse_date_cell(cells["trade_date"])
            assert trade_date is not None
            rows.append(_FixtureRow(line, cells["ticker"], trade_date, tuple(texts), cells))
    return header, rows


def _valid_sample_rows(rows: Sequence[_FixtureRow]) -> list[_FixtureRow]:
    return [row for row in rows if row.key not in INVALID_SAMPLE_ROWS]


def _expected_price_columns(row: _FixtureRow) -> dict[str, object]:
    """The price columns and ``source`` an import stores for a valid sample row.

    An empty turnover is volume x close rounded ``ROUND_HALF_UP`` to the cent, flagged estimated.
    """
    cells = row.cells
    close = parse_decimal_cell(cells["close_price"], "close_price")
    volume = parse_int_cell(cells["volume"])
    assert close is not None
    assert volume is not None
    turnover = parse_decimal_cell(cells["turnover_kes"], "turnover_kes")
    estimated = turnover is None
    if turnover is None:
        turnover = (Decimal(volume) * close).quantize(CENT, rounding=ROUND_HALF_UP)
    return {
        "open_price": parse_decimal_cell(cells["open_price"], "open_price"),
        "high_price": parse_decimal_cell(cells["high_price"], "high_price"),
        "low_price": parse_decimal_cell(cells["low_price"], "low_price"),
        "close_price": close,
        "volume": volume,
        "turnover_kes": turnover,
        "turnover_estimated": estimated,
        "source": "csv_import",
    }


async def _rows_by_key(
    engine: AsyncEngine,
    model: type[Any],
    key: Sequence[str],
    columns: Sequence[str],
    *conditions: ColumnElement[bool],
) -> StoredRows:
    """Stored rows of ``model``'s table keyed by ``key``, each holding ``columns``."""
    statement = select(*(getattr(model, name) for name in (*key, *columns)))
    if conditions:
        statement = statement.where(*conditions)
    async with engine.connect() as conn:
        result = await conn.execute(statement)
        return {
            tuple(mapping[name] for name in key): {name: mapping[name] for name in columns}
            for mapping in result.mappings()
        }


async def _stored_prices(engine: AsyncEngine, *, since: date | None = None) -> StoredRows:
    """Every ``terminal_daily_prices`` row, or those traded on or after ``since``."""
    conditions = () if since is None else (TerminalDailyPrice.trade_date >= since,)
    return await _rows_by_key(
        engine, TerminalDailyPrice, PRICE_KEY, PRICE_READ_COLUMNS, *conditions
    )


def _price_values(rows: StoredRows, keys: Iterable[tuple[Any, ...]]) -> StoredRows:
    """The :data:`PRICE_VALUE_COLUMNS` of ``rows`` at ``keys``."""
    return {key: {name: rows[key][name] for name in PRICE_VALUE_COLUMNS} for key in keys}


async def _run_row(engine: AsyncEngine, run_id: uuid.UUID) -> dict[str, object]:
    """The recorded ``terminal_ingestion_runs`` row of a run."""
    statement = select(
        TerminalIngestionRun.pipeline,
        TerminalIngestionRun.trigger,
        TerminalIngestionRun.status,
        TerminalIngestionRun.rows_fetched,
        TerminalIngestionRun.rows_written,
        TerminalIngestionRun.rows_rejected,
    ).where(TerminalIngestionRun.id == run_id)
    async with engine.connect() as conn:
        return dict((await conn.execute(statement)).mappings().one())


async def _run_count(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        result = await conn.execute(select(func.count()).select_from(TerminalIngestionRun))
        return int(result.scalar_one())


def _rejections(
    result: RunResult, key: Sequence[str]
) -> dict[tuple[object, ...], tuple[object, object]]:
    """``details.rejected`` as ``(line, reason)`` by the rejected row's raw ``key`` cells."""
    entries = result.details["rejected"]
    rejected = {
        tuple(entry["row"][name] for name in key): (entry["index"], entry["reason"])
        for entry in entries
    }
    assert len(rejected) == len(entries)
    return rejected


def _amount_text(value: Decimal) -> str:
    """An amount as an accounting export writes it: separators, parentheses for a negative."""
    return f"({-value:,})" if value < 0 else f"{value:,}"


def test_sample_fixture_exercises_every_cell_form(fixtures_dir: Path) -> None:
    header, rows = _read_sample(fixtures_dir)
    assert "Close Price" in header
    assert len(rows) == 22
    assert {row.key for row in rows} >= set(INVALID_SAMPLE_ROWS)
    valid = _valid_sample_rows(rows)
    assert len(valid) == 20
    assert len({row.key for row in valid}) == 20
    assert sum(1 for row in valid if not row.cells["turnover_kes"]) >= 5
    assert sum(1 for row in valid if not row.cells["foreign_net_flow_kes"]) >= 5
    assert sum(1 for row in valid if row.cells["foreign_net_flow_kes"].startswith("(")) >= 3
    assert sum(1 for row in valid if "/" in row.cells["trade_date"]) >= 4
    assert sum(1 for row in valid if "," in row.cells["volume"]) >= 4
    assert {row.ticker for row in valid if row.trade_date == NEW_SESSION} == {"SCOM", "KCB", "KEGN"}


async def test_bad_arguments_leave_no_run_row_in_the_database(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    before = await _run_count(engine)
    csv_path = _write_csv(tmp_path / "prices.csv", [PRICE_HEADER, ("SCOM", SESSION, "33.15", "1")])
    with pytest.raises(ValueError, match=re.escape("unknown import kind 'bogus'")):
        await import_file("bogus", csv_path, settings=seeded_database.settings)
    with pytest.raises(ValueError, match=re.escape("unsupported file type '.json'")):
        await import_file("daily_prices", tmp_path / "x.json", settings=seeded_database.settings)
    assert await _run_count(engine) == before


async def _insert_scraped_session(engine: AsyncEngine) -> None:
    """SCOM on :data:`NEW_SESSION` as the NSE pipeline writes it: close, volume and an estimated
    turnover, with no OHLC and no flow."""
    close, volume = Decimal("33.25"), 2_754_329
    row = {
        "ticker": "SCOM",
        "trade_date": NEW_SESSION,
        "close_price": close,
        "volume": volume,
        "turnover_kes": (Decimal(volume) * close).quantize(CENT, rounding=ROUND_HALF_UP),
        "turnover_estimated": True,
    }
    async with engine.begin() as conn:
        written = await upsert_prices(
            conn, [row], source="nse_market_statistics", ingestion_run_id=None
        )
    assert written == 1


async def test_daily_prices_sample_overwrites_seeded_and_scraped_prices(
    seeded_database: _Clone, fixtures_dir: Path
) -> None:
    engine = seeded_database.engine
    _header, sample = _read_sample(fixtures_dir)
    valid = _valid_sample_rows(sample)
    csv_keys = {row.key for row in valid}
    await _insert_scraped_session(engine)
    before = await _stored_prices(engine, since=SAMPLE_START)
    scraped = before[("SCOM", NEW_SESSION)]
    assert (scraped["source"], scraped["foreign_net_flow_kes"], scraped["flow_source"]) == (
        "nse_market_statistics",
        None,
        None,
    )
    for row in sample:
        if row.trade_date != NEW_SESSION:
            seeded = before[row.key]
            assert (seeded["source"], seeded["flow_source"]) == ("seed", "seed")
            assert seeded["foreign_net_flow_kes"] is not None

    result = await import_file(
        "daily_prices", fixtures_dir / SAMPLE_CSV, settings=seeded_database.settings
    )

    assert result.pipeline == "csv_import:daily_prices"
    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.PARTIAL,
        22,
        20,
        2,
    )
    assert len(result.details["rejected"]) == 2
    assert _rejections(result, PRICE_KEY) == {
        (row.ticker, row.cells["trade_date"]): (row.line, INVALID_SAMPLE_ROWS[row.key])
        for row in sample
        if row.key in INVALID_SAMPLE_ROWS
    }
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:daily_prices",
        "trigger": "cli",
        "status": "partial",
        "rows_fetched": 22,
        "rows_written": 20,
        "rows_rejected": 2,
    }

    after = await _stored_prices(engine, since=SAMPLE_START)
    for row in valid:
        stored = after[row.key]
        expected = _expected_price_columns(row)
        flow = parse_decimal_cell(row.cells["foreign_net_flow_kes"], "foreign_net_flow_kes")
        if flow is None:
            # An empty flow cell keeps the stored flow: the seeded one, or none on a new session.
            previous = before.get(row.key, dict.fromkeys(FLOW_COLUMNS))
            expected.update({name: previous[name] for name in FLOW_COLUMNS})
        else:
            expected.update(foreign_net_flow_kes=flow, flow_source="csv_import")
        assert {name: stored[name] for name in PRICE_VALUE_COLUMNS} == expected, row.key
        assert type(stored["volume"]) is int
        assert stored["ingestion_run_id"] == result.run_id

    equity = after[("EQTY", SESSION)]
    assert (equity["close_price"], equity["source"]) == (Decimal("95.75"), "csv_import")
    kengen = after[("KEGN", SESSION)]
    # 987241 x 10.605 = 10469690.805, rounded half up.
    assert (kengen["turnover_kes"], kengen["turnover_estimated"]) == (Decimal("10469690.81"), True)
    outflow = after[("SCOM", date(2026, 9, 25))]
    assert (outflow["foreign_net_flow_kes"], outflow["flow_source"]) == (
        Decimal("-1250000.00"),
        "csv_import",
    )
    kept_flow = after[("SCOM", date(2026, 9, 30))]
    assert (kept_flow["turnover_kes"], kept_flow["turnover_estimated"]) == (
        Decimal("89412377.25"),
        False,
    )
    assert (kept_flow["foreign_net_flow_kes"], kept_flow["flow_source"]) == (
        before[("SCOM", date(2026, 9, 30))]["foreign_net_flow_kes"],
        "seed",
    )
    overwritten_scrape = after[("SCOM", NEW_SESSION)]
    assert (
        overwritten_scrape["close_price"],
        overwritten_scrape["volume"],
        overwritten_scrape["source"],
    ) == (Decimal("33.40"), 2_810_455, "csv_import")
    for ticker in ("SCOM", "KCB", "KEGN"):
        new_session = after[(ticker, NEW_SESSION)]
        assert (new_session["foreign_net_flow_kes"], new_session["flow_source"]) == (None, None)

    untouched = set(before) - csv_keys
    assert set(INVALID_SAMPLE_ROWS) <= untouched
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}
    assert all(after[key]["source"] == "seed" for key in INVALID_SAMPLE_ROWS)
    assert set(after) == set(before) | csv_keys


async def test_reimporting_the_daily_prices_sample_changes_no_stored_value(
    seeded_database: _Clone, fixtures_dir: Path
) -> None:
    engine = seeded_database.engine
    _header, sample = _read_sample(fixtures_dir)
    csv_keys = {row.key for row in _valid_sample_rows(sample)}
    path = fixtures_dir / SAMPLE_CSV
    first = await import_file("daily_prices", path, settings=seeded_database.settings)
    after_first = await _stored_prices(engine, since=SAMPLE_START)

    second = await import_file("daily_prices", path, settings=seeded_database.settings)

    assert second.run_id != first.run_id
    for result in (first, second):
        assert (result.status, result.rows_written, result.rows_rejected) == (
            RunStatus.PARTIAL,
            20,
            2,
        )
    after_second = await _stored_prices(engine, since=SAMPLE_START)
    assert set(after_second) == set(after_first)
    assert _price_values(after_second, csv_keys) == _price_values(after_first, csv_keys)
    untouched = set(after_first) - csv_keys
    assert {key: after_second[key] for key in untouched} == {
        key: after_first[key] for key in untouched
    }


LAYOUT_CHANGED_FILES = [
    pytest.param(
        [
            PRICE_HEADER,
            *(
                (ticker, NEXT_SESSION, "50.00", "1,000")
                for ticker in ("SCOM", "EQTY", "KCB", "EABL", "KEGN", "COOP", "ABSA", "SBIC")
            ),
            ("SCOM", date(2026, 9, 30), "0", "1,000"),
            ("EQTY", date(2026, 9, 30), "50.00", "-5"),
        ],
        10,
        2,
        id="two-of-ten-rows-invalid",
    ),
    pytest.param(
        [PRICE_HEADER[:3], ("SCOM", NEXT_SESSION, "50.00"), ("EQTY", NEXT_SESSION, "95.75")],
        0,
        0,
        id="volume-header-missing",
    ),
]


@pytest.mark.parametrize(("rows", "fetched", "rejected"), LAYOUT_CHANGED_FILES)
async def test_daily_prices_layout_change_writes_nothing(
    seeded_database: _Clone,
    tmp_path: Path,
    rows: list[tuple[object, ...]],
    fetched: int,
    rejected: int,
) -> None:
    engine = seeded_database.engine
    before = await _stored_prices(engine)
    path = _write_csv(tmp_path / "prices.csv", rows)

    result = await import_file("daily_prices", path, settings=seeded_database.settings)

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.LAYOUT_CHANGED,
        fetched,
        0,
        rejected,
    )
    assert len(result.details.get("rejected", [])) == rejected
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:daily_prices",
        "trigger": "cli",
        "status": "layout_changed",
        "rows_fetched": fetched,
        "rows_written": 0,
        "rows_rejected": rejected,
    }
    assert await _stored_prices(engine) == before


async def test_a_price_row_dated_after_today_is_rejected_and_writes_nothing(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    """EABL dated 31/12/2026, imported on 2026-10-30 in Nairobi: its only row is rejected, so the
    run is ``layout_changed`` and the latest stored ``trade_date``, every market endpoint's
    default ``as_of``, stays the seeded 2026-09-30."""
    engine = seeded_database.engine
    before = await _stored_prices(engine)
    assert max(day for _ticker, day in before) == date(2026, 9, 30)
    path = _write_csv(
        tmp_path / "prices.csv", [PRICE_HEADER, ("EABL", "31/12/2026", "269.505", "22494")]
    )
    pipeline = TabularImportPipeline(
        seeded_database.settings, kind="daily_prices", path=path, now=lambda: FROZEN_NOW
    )

    result = await execute_pipeline(pipeline, engine, trigger="cli")

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.LAYOUT_CHANGED,
        1,
        0,
        1,
    )
    assert result.details["rejected"] == [
        {
            "index": 2,
            "reason": "future_date:trade_date",
            "row": {
                "ticker": "EABL",
                "trade_date": "31/12/2026",
                "close_price": "269.505",
                "volume": "22494",
            },
        }
    ]
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:daily_prices",
        "trigger": "cli",
        "status": "layout_changed",
        "rows_fetched": 1,
        "rows_written": 0,
        "rows_rejected": 1,
    }
    assert await _stored_prices(engine) == before


async def test_a_macro_row_dated_after_today_is_rejected_and_writes_nothing(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    """Inflation dated 2027-06-30, imported on 2026-10-30 in Nairobi: its only row is rejected, so
    the run is ``layout_changed`` and the latest stored ``observation_date``, the macro overview's
    latest value and the end of its window, stays the seeded 2026-09-30."""
    engine = seeded_database.engine

    async def stored() -> StoredRows:
        return await _rows_by_key(
            engine, TerminalMacroIndicator, ("observation_date",), MACRO_READ_COLUMNS
        )

    before = await stored()
    assert max(day for (day,) in before) == date(2026, 9, 30)
    path = _write_csv(
        tmp_path / "macro.csv", [("observation_date", "inflation_yoy_pct"), ("2027-06-30", "5.55")]
    )
    pipeline = TabularImportPipeline(
        seeded_database.settings, kind="macro_indicators", path=path, now=lambda: FROZEN_NOW
    )

    result = await execute_pipeline(pipeline, engine, trigger="cli")

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.LAYOUT_CHANGED,
        1,
        0,
        1,
    )
    assert result.details["rejected"] == [
        {
            "index": 2,
            "reason": "future_date:observation_date",
            "row": {"observation_date": "2027-06-30", "inflation_yoy_pct": "5.55"},
        }
    ]
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:macro_indicators",
        "trigger": "cli",
        "status": "layout_changed",
        "rows_fetched": 1,
        "rows_written": 0,
        "rows_rejected": 1,
    }
    assert await stored() == before


EQUITY_READ_COLUMNS = (
    "isin",
    "company_name",
    "sector",
    "shares_outstanding",
    "free_float_pct",
    "currency",
    "source",
    "updated_at",
)


async def _stored_import_targets(engine: AsyncEngine) -> StoredRows:
    """Every stored price, fundamentals and equity row, keyed by table name and natural key."""
    stored: StoredRows = {}
    for model, key, columns in (
        (TerminalDailyPrice, PRICE_KEY, PRICE_READ_COLUMNS),
        (TerminalFundamental, FUNDAMENTAL_KEY, FUNDAMENTAL_READ_COLUMNS),
        (TerminalEquity, ("ticker",), EQUITY_READ_COLUMNS),
    ):
        rows = await _rows_by_key(engine, model, key, columns)
        stored.update({(model.__tablename__, *row_key): row for row_key, row in rows.items()})
    return stored


def _raw_cells(header: Sequence[object], row: Sequence[object]) -> dict[str, str]:
    """A written CSV row as a rejection records it: its text by normalized column."""
    return {
        normalize_column_name(name): _csv_cell(value)
        for name, value in zip(header, row, strict=True)
    }


TEN_ROWS = range(1, 11)
REPEATED_KEY_FILES = [
    pytest.param(
        "daily_prices",
        [PRICE_HEADER, *(("ZZZZ", date(2026, 9, 30), "1.00", n) for n in TEN_ROWS)],
        "unknown_ticker",
        RunStatus.LAYOUT_CHANGED,
        {},
        id="daily-prices-unknown-ticker",
    ),
    pytest.param(
        "foreign_flows",
        [
            ("Ticker", "Trade Date", "Foreign Net Flow KES"),
            # Sunday: no seeded price row.
            *(("SCOM", date(2026, 9, 27), f"{n},000.00") for n in TEN_ROWS),
        ],
        "no_price_row",
        RunStatus.LAYOUT_CHANGED,
        {},
        id="foreign-flows-no-price-row",
    ),
    pytest.param(
        "fundamentals",
        [
            ("Ticker", "Period End Date", "Period Type", "Currency", "Revenue KES"),
            # Seeded in KES.
            *(("SCOM", date(2026, 3, 31), "FY", "USD", f"{n},000,000.00") for n in TEN_ROWS),
        ],
        "currency_conflict",
        RunStatus.LAYOUT_CHANGED,
        {},
        id="fundamentals-currency-conflict",
    ),
    pytest.param(
        "equities",
        [
            ("Ticker", "Company Name", "Sector", "Shares Outstanding", "ISIN"),
            # Equity Group's seeded ISIN.
            *(
                ("SCOM", "Safaricom PLC", "Telecommunication", 40_065_428_000 + n, "KE0000000554")
                for n in TEN_ROWS
            ),
        ],
        "isin_conflict",
        RunStatus.LAYOUT_CHANGED,
        {},
        id="equities-isin-conflict",
    ),
    pytest.param(
        "daily_prices",
        [PRICE_HEADER, *(("SCOM", date(2026, 9, 30), f"33.{n:02d}", n * 1_000) for n in TEN_ROWS)],
        None,
        RunStatus.SUCCEEDED,
        {
            ("terminal_daily_prices", "SCOM", date(2026, 9, 30)): {
                "close_price": Decimal("33.10"),
                "volume": 10_000,
                "turnover_kes": Decimal("331000.00"),
                "turnover_estimated": True,
                "source": "csv_import",
            }
        },
        id="daily-prices-valid-last-row",
    ),
]


@pytest.mark.parametrize(("kind", "rows", "reason", "status", "changes"), REPEATED_KEY_FILES)
async def test_every_row_of_a_rejected_repeated_key_counts_toward_the_thresholds(
    seeded_database: _Clone,
    tmp_path: Path,
    kind: ImportKind,
    rows: list[tuple[object, ...]],
    reason: str | None,
    status: RunStatus,
    changes: Mapping[tuple[object, ...], Mapping[str, object]],
) -> None:
    """Ten rows of one key collapse into the last. When it is rejected, all ten rows are, so the
    run is ``layout_changed`` and writes nothing; when it is valid, it alone is written and the
    nine rows it superseded are neither written nor rejected."""
    engine = seeded_database.engine
    before = await _stored_import_targets(engine)
    path = _write_csv(tmp_path / f"{kind}.csv", rows)

    result = await import_file(kind, path, settings=seeded_database.settings)

    header, *data = rows
    rejected = [
        {"index": line, "reason": reason, "row": _raw_cells(header, row)}
        for line, row in enumerate(data, start=2)
        if reason is not None
    ]
    written = 0 if rejected else 1
    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        status,
        10,
        written,
        len(rejected),
    )
    assert result.details["duplicates_collapsed"] == 9
    assert result.details.get("rejected", []) == rejected
    assert await _run_row(engine, result.run_id) == {
        "pipeline": f"csv_import:{kind}",
        "trigger": "cli",
        "status": status.value,
        "rows_fetched": 10,
        "rows_written": written,
        "rows_rejected": len(rejected),
    }
    after = await _stored_import_targets(engine)
    assert set(after) == set(before)
    for key, columns in changes.items():
        assert {name: after[key][name] for name in columns} == columns
    untouched = set(before) - set(changes)
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}


FLOW_UPDATES: Mapping[tuple[str, date], Decimal] = {
    ("SCOM", NEXT_SESSION): Decimal("-1250000.50"),
    ("EQTY", NEXT_SESSION): Decimal("2340500.00"),
    ("KCB", NEXT_SESSION): Decimal("-612250.25"),
    ("EABL", NEXT_SESSION): Decimal("125000.00"),
    ("KEGN", NEXT_SESSION): Decimal("-48500.00"),
    ("COOP", NEXT_SESSION): Decimal("210000.75"),
    ("ABSA", NEXT_SESSION): Decimal("0.00"),
    ("SBIC", NEXT_SESSION): Decimal("35000.00"),
    ("SCOM", date(2026, 9, 30)): Decimal("-415000.00"),
    ("EQTY", date(2026, 9, 30)): Decimal("980000.10"),
}
"""Flows for ten seeded price rows: every ticker on 2026-09-29 and two on 2026-09-30."""


async def test_foreign_flows_set_only_the_flow_of_existing_price_rows(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    flows = [(ticker, day, _amount_text(flow)) for (ticker, day), flow in FLOW_UPDATES.items()]
    # Line 7 has no price row: SCOM did not trade on Sunday 2026-09-27 in the seeded history. The
    # date is in the past, so the row reaches the price-row check whatever day the test runs.
    path = _write_csv(
        tmp_path / "flows.csv",
        [
            ("Ticker", "Trade Date", "Foreign Net Flow KES"),
            *flows[:5],
            ("SCOM", date(2026, 9, 27), "1,000.00"),
            *flows[5:],
        ],
    )
    before = await _stored_prices(engine, since=SAMPLE_START)
    assert all(before[key]["foreign_net_flow_kes"] != flow for key, flow in FLOW_UPDATES.items())

    result = await import_file("foreign_flows", path, settings=seeded_database.settings)

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.PARTIAL,
        11,
        10,
        1,
    )
    assert _rejections(result, PRICE_KEY) == {("SCOM", "2026-09-27"): (7, "no_price_row")}
    assert (await _run_row(engine, result.run_id))["pipeline"] == "csv_import:foreign_flows"
    after = await _stored_prices(engine, since=SAMPLE_START)
    assert set(after) == set(before)
    for key, flow in FLOW_UPDATES.items():
        assert before[key]["source"] == "seed"
        # Prices, turnover, source, ingestion_run_id and ingested_at are as seeded.
        assert after[key] == {
            **before[key],
            "foreign_net_flow_kes": flow,
            "flow_source": "csv_import",
        }
    untouched = set(before) - set(FLOW_UPDATES)
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}


RACED_FLOW_KEYS: tuple[tuple[str, date], ...] = (
    ("SCOM", NEXT_SESSION),
    ("EQTY", NEXT_SESSION),
)
"""Seeded price rows a flows import finds at reconcile and a concurrent writer then deletes on its
own connection before the import writes. They are lines 2 and 3 of a file listing
:data:`FLOW_UPDATES` in order; in key order EQTY comes first."""


@pytest.mark.parametrize(
    ("raced", "status", "written"),
    [
        pytest.param(1, RunStatus.PARTIAL, 9, id="one-of-ten-rows-is-partial"),
        pytest.param(2, RunStatus.LAYOUT_CHANGED, 0, id="two-of-ten-rows-write-nothing"),
    ],
)
async def test_foreign_flows_reject_a_price_row_deleted_after_reconcile(
    seeded_database: _Clone,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    raced: int,
    status: RunStatus,
    written: int,
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    engine = seeded_database.engine
    deleted = RACED_FLOW_KEYS[:raced]
    lines = {key: line for line, key in enumerate(FLOW_UPDATES, start=2)}
    path = _write_csv(
        tmp_path / "flows.csv",
        [
            ("Ticker", "Trade Date", "Foreign Net Flow KES"),
            *((ticker, day, _amount_text(flow)) for (ticker, day), flow in FLOW_UPDATES.items()),
        ],
    )
    before = await _stored_prices(engine, since=SAMPLE_START)
    assert all(before[key]["foreign_net_flow_kes"] != flow for key, flow in FLOW_UPDATES.items())
    reconcile = TabularImportPipeline.reconcile

    async def reconcile_then_delete_price_rows(
        self: TabularImportPipeline, ctx: RunContext, records: list[TabularRecord]
    ) -> ValidationOutcome[TabularRecord]:
        outcome = await reconcile(self, ctx, records)
        # Every price row still existed, so every flow passed the price-row check.
        assert outcome.rejected == []
        async with engine.begin() as conn:
            for ticker, day in deleted:
                await conn.execute(
                    delete(TerminalDailyPrice).where(
                        TerminalDailyPrice.ticker == ticker, TerminalDailyPrice.trade_date == day
                    )
                )
        return outcome

    monkeypatch.setattr(TabularImportPipeline, "reconcile", reconcile_then_delete_price_rows)

    result = await import_file("foreign_flows", path, settings=seeded_database.settings)

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        status,
        10,
        written,
        raced,
    )
    assert _rejections(result, PRICE_KEY) == {
        (ticker, day.isoformat()): (lines[(ticker, day)], "no_price_row") for ticker, day in deleted
    }
    # Reported in key order, with the bounded diagnostics alongside.
    assert [entry["index"] for entry in result.details["rejected"]] == [
        lines[key] for key in sorted(deleted)
    ]
    assert result.details["flow_missing_keys"] == {
        "count": raced,
        "sample": [[ticker, day.isoformat()] for ticker, day in sorted(deleted)],
    }
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:foreign_flows",
        "trigger": "cli",
        "status": status.value,
        "rows_fetched": 10,
        "rows_written": written,
        "rows_rejected": raced,
    }
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == LOGGER and record.levelno == logging.WARNING
    ] == [
        f"foreign_flows import run {result.run_id} rejected {raced} rows at write time: "
        "no_price_row"
    ]
    after = await _stored_prices(engine, since=SAMPLE_START)
    # The concurrent deletions stand; the import never re-creates a price row.
    assert set(after) == set(before) - set(deleted)
    kept = [key for key in FLOW_UPDATES if key not in deleted]
    if status is RunStatus.PARTIAL:
        for key in kept:
            assert after[key] == {
                **before[key],
                "foreign_net_flow_kes": FLOW_UPDATES[key],
                "flow_source": "csv_import",
            }
    else:
        # The breach rolled back the flows the import had already set.
        assert {key: after[key] for key in kept} == {key: before[key] for key in kept}
    untouched = set(after) - set(FLOW_UPDATES)
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}


KCB_REVENUE: Mapping[date, Decimal] = {
    date(year, 12, 31): Decimal(f"{year}000000000.45") for year in range(2016, 2026)
}
"""New revenue for ten seeded KCB fiscal years."""
SCOM_FY2026 = ("SCOM", date(2026, 3, 31), "FY")


async def test_fundamentals_merge_kes_rows_and_reject_a_currency_conflict(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    rows: list[tuple[object, ...]] = [
        ("Ticker", "Period End Date", "Period Type", "Currency", "Revenue KES")
    ]
    for index, (period_end, revenue) in enumerate(KCB_REVENUE.items()):
        if index == 5:
            # Line 7: a USD statement for a period stored in KES.
            rows.append(("SCOM", SCOM_FY2026[1], "FY", "USD", "1,000,000.00"))
        # Every other KCB row leaves period_type and currency empty: they default to FY and KES.
        explicit = index % 2 == 1
        rows.append(
            (
                "KCB",
                period_end,
                "FY" if explicit else None,
                "KES" if explicit else None,
                _amount_text(revenue),
            )
        )
    path = _write_csv(tmp_path / "fundamentals.csv", rows)

    async def stored() -> StoredRows:
        return await _rows_by_key(
            engine,
            TerminalFundamental,
            FUNDAMENTAL_KEY,
            FUNDAMENTAL_READ_COLUMNS,
            TerminalFundamental.ticker.in_(("KCB", "SCOM")),
        )

    before = await stored()

    result = await import_file("fundamentals", path, settings=seeded_database.settings)

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.PARTIAL,
        11,
        10,
        1,
    )
    assert _rejections(result, ("ticker", "period_end_date", "currency")) == {
        ("SCOM", "2026-03-31", "USD"): (7, "currency_conflict")
    }
    after = await stored()
    assert set(after) == set(before)
    assert after[SCOM_FY2026] == before[SCOM_FY2026]
    assert after[SCOM_FY2026]["currency"] == "KES"
    changed = ("revenue_kes", "source_by_field", "updated_at")
    kept = [name for name in FUNDAMENTAL_READ_COLUMNS if name not in changed]
    for period_end, revenue in KCB_REVENUE.items():
        old, new = before[("KCB", period_end, "FY")], after[("KCB", period_end, "FY")]
        assert old["source_by_field"]["revenue_kes"] == "seed"
        assert new["revenue_kes"] == revenue
        assert new["revenue_kes"] != old["revenue_kes"]
        assert new["source_by_field"] == {**old["source_by_field"], "revenue_kes": "csv_import"}
        # COALESCE: the columns the file leaves out keep their values, nulls included.
        assert {name: new[name] for name in kept} == {name: old[name] for name in kept}
        assert new["currency"] == "KES"
    untouched = set(before) - {("KCB", period_end, "FY") for period_end in KCB_REVENUE}
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}


RACED_PERIODS: tuple[tuple[str, date, str], ...] = (
    ("EQTY", date(2026, 6, 30), "H1"),
    ("SCOM", date(2025, 9, 30), "H1"),
)
"""Half-year periods the seed does not hold. A USD import of them passes reconcile; a PDF commit
then stores them in KES on its own connection before the import writes."""
PDF_REVENUE = Decimal("45000000000.00")


@pytest.mark.parametrize(
    ("raced", "status", "written"),
    [
        pytest.param(1, RunStatus.PARTIAL, 9, id="one-of-ten-rows-is-partial"),
        pytest.param(2, RunStatus.LAYOUT_CHANGED, 0, id="two-of-ten-rows-write-nothing"),
    ],
)
async def test_fundamentals_reject_a_currency_conflict_committed_after_reconcile(
    seeded_database: _Clone,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    raced: int,
    status: RunStatus,
    written: int,
) -> None:
    caplog.set_level(logging.WARNING, logger=LOGGER)
    engine = seeded_database.engine
    periods = RACED_PERIODS[:raced]
    kcb = dict(list(KCB_REVENUE.items())[: 10 - raced])
    # Lines 2 to raced + 1 are the USD rows of the raced periods; the KCB rows follow.
    path = _write_csv(
        tmp_path / "fundamentals.csv",
        [
            FUNDAMENTALS_HEADER,
            *((*period, "USD", "1,000,000.00") for period in periods),
            *(
                ("KCB", period_end, "FY", "KES", _amount_text(revenue))
                for period_end, revenue in kcb.items()
            ),
        ],
    )

    async def stored() -> StoredRows:
        return await _rows_by_key(
            engine,
            TerminalFundamental,
            FUNDAMENTAL_KEY,
            FUNDAMENTAL_READ_COLUMNS,
            TerminalFundamental.ticker.in_(("EQTY", "KCB", "SCOM")),
        )

    before = await stored()
    assert not set(periods) & set(before)
    committed_by_pdf: StoredRows = {}
    reconcile = TabularImportPipeline.reconcile

    async def reconcile_then_commit_a_pdf(
        self: TabularImportPipeline, ctx: RunContext, records: list[TabularRecord]
    ) -> ValidationOutcome[TabularRecord]:
        outcome = await reconcile(self, ctx, records)
        # Nothing was stored for the raced periods yet, so every row passed the currency check.
        assert outcome.rejected == []
        async with engine.begin() as conn:
            await upsert_fundamentals(
                conn,
                [
                    {
                        "ticker": ticker,
                        "period_end_date": period_end,
                        "period_type": period_type,
                        "currency": "KES",
                        "revenue_kes": PDF_REVENUE,
                    }
                    for ticker, period_end, period_type in periods
                ],
                source="pdf_filing",
            )
        committed_by_pdf.update(await stored())
        return outcome

    monkeypatch.setattr(TabularImportPipeline, "reconcile", reconcile_then_commit_a_pdf)

    result = await import_file("fundamentals", path, settings=seeded_database.settings)

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        status,
        10,
        written,
        raced,
    )
    assert _rejections(result, ("ticker", "period_end_date", "currency")) == {
        (ticker, period_end.isoformat(), "USD"): (line, "currency_conflict")
        for line, (ticker, period_end, _period_type) in enumerate(periods, start=2)
    }
    assert await _run_row(engine, result.run_id) == {
        "pipeline": "csv_import:fundamentals",
        "trigger": "cli",
        "status": status.value,
        "rows_fetched": 10,
        "rows_written": written,
        "rows_rejected": raced,
    }
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == LOGGER and record.levelno == logging.WARNING
    ] == [
        f"fundamentals import run {result.run_id} rejected {raced} rows at write time: "
        "currency_conflict"
    ]
    after = await stored()
    assert set(after) == set(before) | set(periods)
    for period in periods:
        # The PDF's KES rows keep their values and provenance: nothing of the USD rows merged.
        assert after[period] == committed_by_pdf[period]
        pdf_row = after[period]
        assert (pdf_row["currency"], pdf_row["revenue_kes"], pdf_row["source_by_field"]) == (
            "KES",
            PDF_REVENUE,
            {"revenue_kes": "pdf_filing"},
        )
    kcb_keys = {("KCB", period_end, "FY") for period_end in kcb}
    if status is RunStatus.PARTIAL:
        for period_end, revenue in kcb.items():
            old, new = before[("KCB", period_end, "FY")], after[("KCB", period_end, "FY")]
            assert new["revenue_kes"] == revenue
            assert new["source_by_field"] == {**old["source_by_field"], "revenue_kes": "csv_import"}
    else:
        # The breach rolled back the KCB rows the import had already written.
        assert {key: after[key] for key in kcb_keys} == {key: before[key] for key in kcb_keys}
    untouched = set(before) - kcb_keys
    assert {key: after[key] for key in untouched} == {key: before[key] for key in untouched}


async def test_macro_indicators_merge_one_column_and_insert_a_new_month(
    seeded_database: _Clone, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    september, october = date(2026, 9, 30), date(2026, 10, 31)
    path = _write_csv(
        tmp_path / "macro.csv",
        [("observation_date", "inflation_yoy_pct"), (september, "6.85"), (october, "6.90")],
    )

    async def stored() -> StoredRows:
        return await _rows_by_key(
            engine,
            TerminalMacroIndicator,
            ("observation_date",),
            MACRO_READ_COLUMNS,
            TerminalMacroIndicator.observation_date >= september,
        )

    before = await stored()
    assert set(before) == {(september,)}
    # A fixed clock on 2026-11-02 in Nairobi, so the October row is past: on the real clock it is
    # a future observation (future_date:observation_date) on any day before 2026-10-31.
    pipeline = TabularImportPipeline(
        seeded_database.settings,
        kind="macro_indicators",
        path=path,
        now=lambda: datetime(2026, 11, 2, 9, 0, tzinfo=UTC),
    )

    result = await execute_pipeline(pipeline, engine, trigger="cli")

    assert (result.status, result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RunStatus.SUCCEEDED,
        2,
        2,
        0,
    )
    assert (await _run_row(engine, result.run_id))["pipeline"] == "csv_import:macro_indicators"
    after = await stored()
    seeded, merged = before[(september,)], after[(september,)]
    assert seeded["source_by_field"] == dict.fromkeys(MACRO_COLUMNS, "seed")
    others = [name for name in MACRO_COLUMNS if name != "inflation_yoy_pct"]
    assert merged["inflation_yoy_pct"] == Decimal("6.85")
    assert merged["source_by_field"] == {
        **seeded["source_by_field"],
        "inflation_yoy_pct": "csv_import",
    }
    assert {name: merged[name] for name in others} == {name: seeded[name] for name in others}
    inserted = after[(october,)]
    assert inserted["inflation_yoy_pct"] == Decimal("6.90")
    assert {name: inserted[name] for name in others} == dict.fromkeys(others)
    assert inserted["source_by_field"] == {"inflation_yoy_pct": "csv_import"}


def _spreadsheet_cell(column: str, text: str) -> object:
    """A sample cell as a workbook holds it: a ``date`` for the trade date, an ``int`` volume, a
    number for each price, and the export's own text for turnover and flows (separators and
    parentheses included); an empty cell is ``None``."""
    if not text:
        return None
    if column == "trade_date":
        return parse_date_cell(text)
    if column == "volume":
        return parse_int_cell(text)
    if column.endswith("_price"):
        # The deliberate floats: Excel stores every number as a binary double, so a typed 95.75
        # reaches the importer through openpyxl as a float and must read back exactly.
        return float(text)
    return text


async def test_xlsx_twin_of_the_sample_stores_what_the_csv_stores(
    seeded_database: _Clone, fixtures_dir: Path, tmp_path: Path
) -> None:
    engine = seeded_database.engine
    header, sample = _read_sample(fixtures_dir)
    valid = _valid_sample_rows(sample)
    keys = {row.key for row in valid}
    columns = [normalize_column_name(name) for name in header]
    workbook = _write_xlsx(
        tmp_path / "prices.xlsx",
        {
            "Prices": [
                header,
                *(
                    [
                        _spreadsheet_cell(column, text)
                        for column, text in zip(columns, row.texts, strict=True)
                    ]
                    for row in valid
                ),
            ]
        },
    )
    csv_run = await import_file(
        "daily_prices", fixtures_dir / SAMPLE_CSV, settings=seeded_database.settings
    )
    assert csv_run.status is RunStatus.PARTIAL
    from_csv = _price_values(await _stored_prices(engine, since=SAMPLE_START), keys)

    # No sheet named: the first worksheet is read.
    xlsx_run = await import_file("daily_prices", workbook, settings=seeded_database.settings)

    assert (
        xlsx_run.status,
        xlsx_run.rows_fetched,
        xlsx_run.rows_written,
        xlsx_run.rows_rejected,
    ) == (RunStatus.SUCCEEDED, 20, 20, 0)
    stored = await _stored_prices(engine, since=SAMPLE_START)
    # Every key was rewritten by the workbook's run, and to the values the CSV run stored.
    assert {stored[key]["ingestion_run_id"] for key in keys} == {xlsx_run.run_id}
    assert _price_values(stored, keys) == from_csv
    assert stored[("EQTY", SESSION)]["close_price"] == Decimal("95.75")
    assert stored[("KEGN", SESSION)]["turnover_kes"] == Decimal("10469690.81")
    assert stored[("SCOM", date(2026, 9, 25))]["foreign_net_flow_kes"] == Decimal("-1250000.00")
