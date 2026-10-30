"""Tests for the CBK "Central Bank Rates" CSV pipeline (``ingestion.cbk.rates_csv``, run name
``cbk_rates_csv``).

Source: the Central Bank of Kenya's public "Central Bank Rates" statistical release, a CSV linked
from https://www.centralbank.go.ke/statistics/interest-rates/. The recorded listing page, the
complete file (July 1991 to September 2026, 423 months) and two rejection variants live under
``fixtures/cbk/``: ``central_bank_rates_short.csv`` (33 months, below the 300-row minimum) and
``central_bank_rates_layout_changed.csv`` (its "91-Day Tbill" header renamed).

- **Parse.** ``parse_rates_csv`` runs on the recorded files without a database: the year is
  forward-filled, ``-`` and blank cells are skipped rather than stored as null, only the five
  stored series are kept, the footer is dropped, and a renamed header raises
  ``SourceLayoutChanged``. The row minimum is a run threshold, so the short file still parses.
- **Full runs.** ``execute_pipeline`` runs against a fresh seeded clone (``seeded_database``,
  which marks those tests ``integration``). respx serves ``robots.txt``, the listing page and the
  CSV at the real CBK URLs with ``assert_all_mocked=True``, so nothing reaches the network. The
  run's ``PoliteClient`` spaces its requests on a fake clock that only a fake sleep advances, so
  no test really waits. Tables are read through the ``terminal_`` models with ``select()`` only.
- **Live.** One ``live`` test, deselected by default, checks the real release still parses.
- **Shared CBK helpers.** The ``ingestion.cbk`` helpers this pipeline and the other four CBK
  pipelines are built on are tested directly against their documented contracts on small
  inline HTML and CSV, with no network or database; the section banner states the contract.

Expected values are literals from the release (interbank 8.75, 91-day 8.77 and Central Bank Rate
8.75 for September 2026; 84.67 for the 91-day rate of July 1993) and from the seeded 2026-09-30
row they replace. Every number is compared as ``Decimal``.
"""

import asyncio
import importlib.util
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, cast

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

import opennse_terminal.models.macro as macro_model
from opennse_terminal.ingestion import cbk
from opennse_terminal.ingestion.base import (
    RejectedRow,
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    ValidationOutcome,
    execute_pipeline,
)
from opennse_terminal.ingestion.cbk import (
    CBK_APPROVED_ORIGINS,
    CBK_BASE_URL,
    COLUMN_QUANTUM,
    MISSING_MARKERS,
    MONTHS,
    TENOR_QUANTUM,
    UPSERT_CHUNK_ROWS,
    CbkCsv,
    HtmlTable,
    MacroRecord,
    clean_text,
    decode_cbk_bytes,
    extract_table,
    find_link,
    is_missing_cell,
    month_end,
    parse_month,
    parse_value_cell,
    parse_year,
    quantize_to,
    read_cbk_csv,
    upsert_macro_records,
    validate_macro_records,
)
from opennse_terminal.ingestion.cbk.rates_csv import (
    BOUNDS,
    COLUMN_MAP,
    INTEREST_RATES_PAGE_URL,
    LINK_SUFFIX,
    LINK_TEXT,
    MIN_ROWS,
    OWNED_COLUMNS,
    REQUIRED_HEADERS,
    CbkRatesCsvPipeline,
    parse_rates_csv,
)
from opennse_terminal.ingestion.http import PoliteClient
from opennse_terminal.models import TerminalIngestionRun, TerminalMacroIndicator
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

LISTING_FIXTURE = "cbk/interest_rates_page.html"
CSV_FIXTURE = "cbk/central_bank_rates.csv"
SHORT_FIXTURE = "cbk/central_bank_rates_short.csv"
LAYOUT_CHANGED_FIXTURE = "cbk/central_bank_rates_layout_changed.csv"
FIXTURE_MONTHS = 423
"""Data rows of the recorded file: every month from July 1991 to September 2026."""
SHORT_MONTHS = 33
"""Data rows of the short file: every month from January 2024 to September 2026."""
FIRST_DATA_LINE = 3
"""1-based file line of the first data row, below the title and header lines."""

ROBOTS_URL = f"{CBK_BASE_URL}/robots.txt"
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
"""CBK's own ``robots.txt``: every path allowed except the CMS admin area."""
DISALLOW_STATISTICS = "User-agent: *\nDisallow: /statistics/\n"
"""A ``robots.txt`` that blocks the listing page."""
CENTRAL_BANK_RATES_CSV_URL = (
    "https://www.centralbank.go.ke/uploads/interest_rates/"
    "648402325_Central%20Bank%20Rates_with%20Heads.csv"
)
"""The file linked from the recorded listing page, as httpx requests it."""

FIRST_MONTH = date(1991, 7, 31)
YEARLESS_MONTH = date(1992, 2, 29)
"""A month whose CSV ``YEAR`` cell is blank, so it is dated by the forward-filled 1992."""
PEAK_91D_MONTH = date(1993, 7, 31)
ANCHOR = date(2026, 9, 30)
SOURCE = "cbk_rates_csv"

SEEDED_ANCHOR: Mapping[str, Decimal] = {
    "inflation_yoy_pct": Decimal("6.80"),
    "cbr_rate_pct": Decimal("8.75"),
    "interbank_rate_pct": Decimal("12.85"),
    "tbill_91d_pct": Decimal("15.72"),
    "tbill_182d_pct": Decimal("16.58"),
    "tbill_364d_pct": Decimal("16.84"),
    "fx_kes_usd": Decimal("129.20"),
    "fx_reserves_usd_m": Decimal("7850.00"),
}
"""The seeded 2026-09-30 macro row (the benchmark values plus CBK's 8.75 Central Bank Rate)."""

UNOWNED_COLUMNS = tuple(column for column in MACRO_INDICATOR_COLUMNS if column not in OWNED_COLUMNS)
"""Indicator columns other pipelines own: inflation, KES/USD and FX reserves."""
SNAPSHOT_COLUMNS = (*MACRO_INDICATOR_COLUMNS, "source_by_field", "updated_at")
VALUE_COLUMNS = (*MACRO_INDICATOR_COLUMNS, "source_by_field")
"""What a re-run must leave identical; ``updated_at`` is refreshed by every conflict update."""

RENAMED_HEADERS = [
    "year",
    "month",
    "repo",
    "reverse repo",
    "interbank rate",
    "91 day treasury bill",
    "182-days tbill",
    "364-days tbill",
    "cash reserve requirement",
    "central bank rate",
]
"""The normalized headers of the layout-changed file, where "91-Day Tbill" was renamed."""

MacroSnapshot = dict[date, dict[str, Any]]


class _Clone(Protocol):
    """The part of the root conftest's ``seeded_database`` handle these tests use."""

    @property
    def settings(self) -> Any: ...

    @property
    def engine(self) -> AsyncEngine: ...


class FakeClock:
    """Monotonic seconds, starting at 1000.0, that move only when advanced."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping; it still yields
    to the event loop once, as a real sleep would."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)
        await asyncio.sleep(0)


@dataclass(frozen=True, slots=True)
class RatesRun:
    """One full ``cbk_rates_csv`` run on the mocked CBK site.

    Attributes:
        result: What ``execute_pipeline`` returned.
        requested: Every URL the run requested, in order, as httpx sent it.
        delays: The spacing waits the run's ``PoliteClient`` slept on the fake clock.
    """

    result: RunResult
    requested: list[str]
    delays: list[float]


class RatesRunner(Protocol):
    """``run(handle, csv_bytes, *, robots=ALLOW_ALL)``, the ``run_rates`` fixture's callable."""

    def __call__(
        self, handle: _Clone, csv_bytes: bytes, *, robots: str = ALLOW_ALL
    ) -> Awaitable[RatesRun]: ...


@pytest.fixture(scope="module")
def listing_html(fixture_text: Callable[..., str]) -> str:
    return fixture_text(LISTING_FIXTURE)


@pytest.fixture(scope="module")
def csv_url(listing_html: str) -> str:
    """The CSV link discovered on the recorded listing page exactly as the pipeline discovers it,
    normalized as httpx sends it (spaces as ``%20``)."""
    discovered = find_link(
        listing_html, INTEREST_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=LINK_SUFFIX
    )
    return str(httpx.URL(discovered))


@pytest.fixture(scope="module")
def rates_csv(fixture_bytes: Callable[[str], bytes]) -> bytes:
    return fixture_bytes(CSV_FIXTURE)


@pytest.fixture(scope="module")
def rates_records(rates_csv: bytes) -> list[MacroRecord]:
    return parse_rates_csv(rates_csv)


def mock_cbk(
    router: respx.MockRouter,
    *,
    listing_html: str,
    csv_url: str,
    csv_bytes: bytes,
    robots: str = ALLOW_ALL,
) -> None:
    """Serve CBK's ``robots.txt``, the recorded listing page and ``csv_bytes`` at their real
    URLs."""
    router.get(ROBOTS_URL).respond(200, text=robots)
    router.get(INTEREST_RATES_PAGE_URL).respond(200, html=listing_html)
    router.get(csv_url).respond(200, content=csv_bytes, headers={"Content-Type": "text/csv"})


@pytest.fixture
def run_rates(listing_html: str, csv_url: str) -> RatesRunner:
    """``run(handle, csv_bytes, *, robots=ALLOW_ALL)``: one ``cbk_rates_csv`` run on the clone,
    with a fresh ``PoliteClient`` on a fake clock and respx serving the mocked CBK site; any other
    URL fails the request instead of reaching the network."""

    async def run(handle: _Clone, csv_bytes: bytes, *, robots: str = ALLOW_ALL) -> RatesRun:
        clock = FakeClock()
        sleep = FakeSleep(clock)
        with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
            mock_cbk(
                router,
                listing_html=listing_html,
                csv_url=csv_url,
                csv_bytes=csv_bytes,
                robots=robots,
            )
            async with PoliteClient(handle.settings, sleep=sleep, clock=clock) as client:
                result = await execute_pipeline(
                    CbkRatesCsvPipeline(handle.settings), handle.engine, trigger="cli", http=client
                )
            requested = [str(call.request.url) for call in router.calls]
        return RatesRun(result=result, requested=requested, delays=list(sleep.delays))

    return run


async def macro_snapshot(engine: AsyncEngine) -> MacroSnapshot:
    """Every ``terminal_macro_indicators`` row by ``observation_date``."""
    statement = select(
        TerminalMacroIndicator.observation_date,
        *(getattr(TerminalMacroIndicator, column) for column in SNAPSHOT_COLUMNS),
    ).order_by(TerminalMacroIndicator.observation_date)
    async with engine.connect() as conn:
        result = await conn.execute(statement)
        return {
            row["observation_date"]: {column: row[column] for column in SNAPSHOT_COLUMNS}
            for row in result.mappings()
        }


def snapshot_values(snapshot: MacroSnapshot) -> MacroSnapshot:
    """``snapshot`` without ``updated_at``."""
    return {day: {column: row[column] for column in VALUE_COLUMNS} for day, row in snapshot.items()}


async def run_row(engine: AsyncEngine, run_id: uuid.UUID) -> dict[str, Any]:
    """The recorded ``terminal_ingestion_runs`` row of a run."""
    statement = select(
        TerminalIngestionRun.pipeline,
        TerminalIngestionRun.trigger,
        TerminalIngestionRun.status,
        TerminalIngestionRun.rows_fetched,
        TerminalIngestionRun.rows_written,
        TerminalIngestionRun.rows_rejected,
        TerminalIngestionRun.error_message,
        TerminalIngestionRun.finished_at,
        TerminalIngestionRun.details,
    ).where(TerminalIngestionRun.id == run_id)
    async with engine.connect() as conn:
        return dict((await conn.execute(statement)).mappings().one())


def assert_polite_sequence(run: RatesRun, csv_url: str, min_interval: float) -> None:
    """``robots.txt`` first, then the listing page and the CSV once each, spaced on the clock."""
    assert run.requested == [ROBOTS_URL, INTEREST_RATES_PAGE_URL, csv_url]
    assert len(run.delays) == 2
    assert all(delay >= min_interval for delay in run.delays)


def is_month_end(day: date) -> bool:
    return (day + timedelta(days=1)).day == 1


# --- Module contract and link discovery ------------------------------------------------------


def test_module_constants_state_the_release_contract() -> None:
    assert INTEREST_RATES_PAGE_URL == "https://www.centralbank.go.ke/statistics/interest-rates/"
    assert LINK_TEXT == "Central Bank Rates"
    assert LINK_SUFFIX == ".csv"
    assert dict(COLUMN_MAP) == {
        "interbank rate": "interbank_rate_pct",
        "91-day tbill": "tbill_91d_pct",
        "182-days tbill": "tbill_182d_pct",
        "364-days tbill": "tbill_364d_pct",
        "central bank rate": "cbr_rate_pct",
    }
    assert REQUIRED_HEADERS == ("year", "month", *COLUMN_MAP)
    assert OWNED_COLUMNS == (
        "interbank_rate_pct",
        "tbill_91d_pct",
        "tbill_182d_pct",
        "tbill_364d_pct",
        "cbr_rate_pct",
    )
    assert set(OWNED_COLUMNS) <= set(MACRO_INDICATOR_COLUMNS)
    rate_bounds = (Decimal("0"), Decimal("100"))
    assert dict(BOUNDS) == {
        "interbank_rate_pct": rate_bounds,
        "tbill_91d_pct": rate_bounds,
        "tbill_182d_pct": rate_bounds,
        "tbill_364d_pct": rate_bounds,
        "cbr_rate_pct": (Decimal("0"), Decimal("30")),
    }
    assert MIN_ROWS == 300
    assert CbkRatesCsvPipeline.name == SOURCE
    assert CbkRatesCsvPipeline.min_rows == MIN_ROWS


def test_listing_page_links_the_central_bank_rates_csv_not_the_commercial_one(
    csv_url: str,
) -> None:
    # The page also links the "Commercial Bank Rates" CSV; only the Central Bank Rates file is
    # wanted.
    assert csv_url == CENTRAL_BANK_RATES_CSV_URL
    assert "commercial" not in csv_url.lower()


# --- Pure parse (no database) ----------------------------------------------------------------


def test_parse_reads_every_month_with_exact_anchor_rates(
    rates_records: list[MacroRecord],
) -> None:
    assert len(rates_records) >= MIN_ROWS
    assert len(rates_records) == FIXTURE_MONTHS
    by_date = {record.observation_date: record for record in rates_records}
    assert len(by_date) == len(rates_records)

    anchor = by_date[ANCHOR].values
    assert anchor["interbank_rate_pct"] == Decimal("8.75")
    assert anchor["tbill_91d_pct"] == Decimal("8.77")
    assert anchor["cbr_rate_pct"] == Decimal("8.75")
    assert "tbill_182d_pct" in anchor
    assert "tbill_364d_pct" in anchor
    assert set(anchor) == set(OWNED_COLUMNS)

    # The 1993 T-bill peak lies far above today's rates but inside the [0, 100] sanity bound.
    peak = by_date[PEAK_91D_MONTH].values["tbill_91d_pct"]
    assert peak == Decimal("84.67")
    low, high = BOUNDS["tbill_91d_pct"]
    assert low <= peak <= high


def test_parse_forward_fills_the_year_and_dates_rows_at_consecutive_month_ends(
    rates_records: list[MacroRecord],
) -> None:
    assert rates_records[0].observation_date == FIRST_MONTH
    assert rates_records[0].raw["year"] == "1991"
    by_date = {record.observation_date: record for record in rates_records}
    # February 1992's YEAR cell is blank; it carries 1992 from the January row above it.
    yearless = by_date[YEARLESS_MONTH]
    assert yearless.raw["year"] == ""
    assert yearless.raw["month"] == "Feb"
    months: list[int] = []
    for record in rates_records:
        day = record.observation_date
        assert day is not None
        assert is_month_end(day)
        months.append(day.year * 12 + day.month)
    # One record per consecutive month: no row lost, duplicated or dated in the wrong year.
    assert months == list(range(months[0], months[0] + len(months)))


def test_parse_skips_dash_and_blank_cells_instead_of_storing_nulls(
    rates_records: list[MacroRecord],
) -> None:
    first = rates_records[0]
    # July 1991 predates the Central Bank Rate, the interbank series and the longer bills.
    assert first.raw["central bank rate"] == "-"
    assert first.raw["182-days tbill"] == "-"
    assert first.raw["interbank rate"] == ""
    assert "cbr_rate_pct" not in first.values
    assert "interbank_rate_pct" not in first.values
    assert dict(first.values) == {"tbill_91d_pct": Decimal("17.14")}
    assert all(value is not None for record in rates_records for value in record.values.values())


def test_parse_keeps_only_the_owned_columns_as_quantized_decimals_within_bounds(
    rates_records: list[MacroRecord],
) -> None:
    for record in rates_records:
        assert record.errors == ()
        assert set(record.values) <= set(OWNED_COLUMNS)
        # Repo, Reverse Repo and Cash Reserve Requirement are read but never stored.
        assert {"repo", "reverse repo", "cash reserve requirement"} <= set(record.raw)
        for column, value in record.values.items():
            assert isinstance(value, Decimal)
            # Quantized to the NUMERIC(9,4) column's scale.
            assert value.as_tuple().exponent == -4
            low, high = BOUNDS[column]
            assert low <= value <= high


def test_parse_drops_the_source_footer_and_blank_rows(
    rates_csv: bytes, rates_records: list[MacroRecord]
) -> None:
    footer = [line for line in rates_csv.splitlines() if line.startswith(b"Source:")]
    assert footer == [b"Source: Central Bank of Kenya,,,,,,,,,"]
    assert rates_records[-1].observation_date == ANCHOR
    assert not any(
        cell.startswith("Source") for record in rates_records for cell in record.raw.values()
    )
    # Data rows occupy consecutive file lines right below the header; the blank and footer lines
    # after them produce no record.
    lines = [record.line for record in rates_records]
    assert lines == list(range(FIRST_DATA_LINE, FIRST_DATA_LINE + FIXTURE_MONTHS))
    assert len(rates_csv.splitlines()) > FIRST_DATA_LINE - 1 + FIXTURE_MONTHS


def test_parse_raises_layout_changed_when_the_91_day_header_is_renamed(
    fixture_bytes: Callable[[str], bytes],
) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        parse_rates_csv(fixture_bytes(LAYOUT_CHANGED_FIXTURE))
    error = excinfo.value
    assert error.message == "interest-rate CSV headers changed"
    assert error.expected == list(REQUIRED_HEADERS)
    assert "91-day tbill" in error.expected
    assert error.found == RENAMED_HEADERS


def test_parse_leaves_the_row_minimum_to_the_run_thresholds(
    fixture_bytes: Callable[[str], bytes], rates_records: list[MacroRecord]
) -> None:
    short = parse_rates_csv(fixture_bytes(SHORT_FIXTURE))
    assert len(short) == SHORT_MONTHS < MIN_ROWS
    assert short[0].observation_date == date(2024, 1, 31)
    assert short[-1].observation_date == ANCHOR
    full = {record.observation_date: record.values for record in rates_records}
    # The short file is the tail of the full release, so each month parses to the same values.
    assert all(record.values == full[record.observation_date] for record in short)


# --- Full runs against a seeded clone --------------------------------------------------------


@pytest.mark.parametrize(
    ("fixture_name", "rows_fetched", "error_message", "expected", "found"),
    [
        pytest.param(
            SHORT_FIXTURE,
            SHORT_MONTHS,
            f"{SHORT_MONTHS} rows parsed, below the minimum of {MIN_ROWS}",
            {"min_rows": MIN_ROWS, "max_invalid_fraction": "0.10"},
            {"parsed": SHORT_MONTHS, "rejected": 0},
            id="below_row_minimum",
        ),
        pytest.param(
            LAYOUT_CHANGED_FIXTURE,
            0,
            "interest-rate CSV headers changed",
            list(REQUIRED_HEADERS),
            RENAMED_HEADERS,
            id="renamed_91_day_header",
        ),
    ],
)
async def test_layout_change_ends_layout_changed_and_writes_nothing(
    seeded_database: _Clone,
    run_rates: RatesRunner,
    fixture_bytes: Callable[[str], bytes],
    csv_url: str,
    fixture_name: str,
    rows_fetched: int,
    error_message: str,
    expected: Any,
    found: Any,
) -> None:
    engine = seeded_database.engine
    before = await macro_snapshot(engine)
    assert before[ANCHOR]["interbank_rate_pct"] == SEEDED_ANCHOR["interbank_rate_pct"]

    run = await run_rates(seeded_database, fixture_bytes(fixture_name))

    result = run.result
    assert result.pipeline == SOURCE
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.status == "layout_changed"
    assert result.rows_fetched == rows_fetched
    assert result.rows_written == 0
    assert result.rows_rejected == 0
    assert result.error_message == error_message
    assert result.details["expected"] == expected
    assert result.details["found"] == found
    assert str(httpx.URL(result.details["source_url"])) == csv_url
    assert_polite_sequence(run, csv_url, seeded_database.settings.scraper_min_interval_seconds)

    row = await run_row(engine, result.run_id)
    assert row["pipeline"] == SOURCE
    assert row["trigger"] == "cli"
    assert row["status"] == "layout_changed"
    assert (row["rows_fetched"], row["rows_written"], row["rows_rejected"]) == (rows_fetched, 0, 0)
    assert row["error_message"] == error_message
    assert row["finished_at"] is not None
    assert row["details"]["expected"] == expected
    assert row["details"]["found"] == found

    assert await macro_snapshot(engine) == before


async def test_full_run_writes_cbk_rates_with_provenance_and_a_rerun_changes_nothing(
    seeded_database: _Clone,
    run_rates: RatesRunner,
    rates_csv: bytes,
    rates_records: list[MacroRecord],
    csv_url: str,
) -> None:
    engine = seeded_database.engine
    min_interval = seeded_database.settings.scraper_min_interval_seconds
    before = await macro_snapshot(engine)
    seeded_anchor = before[ANCHOR]
    assert {column: seeded_anchor[column] for column in SEEDED_ANCHOR} == SEEDED_ANCHOR
    assert seeded_anchor["source_by_field"] == dict.fromkeys(MACRO_INDICATOR_COLUMNS, "seed")
    assert FIRST_MONTH not in before
    assert PEAK_91D_MONTH not in before

    first = await run_rates(seeded_database, rates_csv)

    result = first.result
    assert result.pipeline == SOURCE
    assert result.status is RunStatus.SUCCEEDED
    assert result.error_message is None
    assert result.rows_fetched == len(rates_records) == FIXTURE_MONTHS
    assert result.rows_rejected == 0
    assert 0 < result.rows_written <= result.rows_fetched
    assert result.details["listing_url"] == INTEREST_RATES_PAGE_URL
    assert str(httpx.URL(result.details["source_url"])) == csv_url
    assert_polite_sequence(first, csv_url, min_interval)

    row = await run_row(engine, result.run_id)
    assert row["pipeline"] == SOURCE
    assert row["trigger"] == "cli"
    assert row["status"] == "succeeded"
    assert row["error_message"] is None
    assert row["finished_at"] is not None
    assert (row["rows_fetched"], row["rows_written"], row["rows_rejected"]) == (
        result.rows_fetched,
        result.rows_written,
        0,
    )

    after = await macro_snapshot(engine)
    parsed = {record.observation_date: record.values for record in rates_records}

    # CBK's September 2026 figures replace the seeded benchmarks. The Central Bank Rate keeps its
    # value, 8.75, but is now credited to CBK; the other pipelines' columns stay seeded.
    anchor = after[ANCHOR]
    assert anchor["interbank_rate_pct"] == Decimal("8.75")
    assert anchor["tbill_91d_pct"] == Decimal("8.77")
    assert anchor["tbill_182d_pct"] == parsed[ANCHOR]["tbill_182d_pct"]
    assert anchor["tbill_364d_pct"] == parsed[ANCHOR]["tbill_364d_pct"]
    assert anchor["cbr_rate_pct"] == Decimal("8.75")
    assert anchor["inflation_yoy_pct"] == Decimal("6.80")
    assert anchor["fx_kes_usd"] == Decimal("129.20")
    assert anchor["fx_reserves_usd_m"] == Decimal("7850.00")
    assert anchor["source_by_field"] == {
        **dict.fromkeys(UNOWNED_COLUMNS, "seed"),
        **dict.fromkeys(OWNED_COLUMNS, SOURCE),
    }

    # Months before the seeded history are inserted with only this pipeline's columns.
    peak = after[PEAK_91D_MONTH]
    assert peak["tbill_91d_pct"] == Decimal("84.67")
    assert peak["source_by_field"] == {"tbill_91d_pct": SOURCE}
    assert all(
        peak[column] is None for column in MACRO_INDICATOR_COLUMNS if column != "tbill_91d_pct"
    )

    # Every parsed value is stored and credited to cbk_rates_csv. A skipped cell never nulls a
    # stored value (COALESCE), and every other column keeps its value and its provenance.
    for day, values in parsed.items():
        stored, seeded = after[day], before.get(day)
        if seeded is None:
            assert stored["source_by_field"] == dict.fromkeys(values, SOURCE), day
        for column in MACRO_INDICATOR_COLUMNS:
            if column in values:
                expected_value, expected_source = values[column], SOURCE
            elif seeded is None:
                expected_value, expected_source = None, None
            else:
                expected_value = seeded[column]
                expected_source = seeded["source_by_field"].get(column)
            assert stored[column] == expected_value, (day, column)
            assert stored["source_by_field"].get(column) == expected_source, (day, column)
    for day, seeded in before.items():
        if day not in parsed:
            assert after[day] == seeded
    assert set(after) == set(before) | set(parsed)

    second = await run_rates(seeded_database, rates_csv)

    assert second.result.status is RunStatus.SUCCEEDED
    assert second.result.rows_rejected == 0
    assert second.result.run_id != result.run_id
    assert_polite_sequence(second, csv_url, min_interval)
    assert (await run_row(engine, second.result.run_id))["status"] == "succeeded"
    # rows_written may count conflict updates that change nothing; the stored values decide.
    assert snapshot_values(await macro_snapshot(engine)) == snapshot_values(after)


async def test_robots_disallow_blocks_the_run_before_the_listing_page(
    seeded_database: _Clone, run_rates: RatesRunner, rates_csv: bytes
) -> None:
    engine = seeded_database.engine
    before = await macro_snapshot(engine)

    run = await run_rates(seeded_database, rates_csv, robots=DISALLOW_STATISTICS)

    result = run.result
    assert result.pipeline == SOURCE
    assert result.status is RunStatus.BLOCKED_BY_ROBOTS
    assert result.rows_fetched == 0
    assert result.rows_written == 0
    assert result.details["robots"] == {"url": INTEREST_RATES_PAGE_URL, "reason": "disallowed"}
    # Only robots.txt was requested: the disallowed listing page, and so the CSV, never were.
    assert run.requested == [ROBOTS_URL]

    row = await run_row(engine, result.run_id)
    assert row["pipeline"] == SOURCE
    assert row["trigger"] == "cli"
    assert row["status"] == "blocked_by_robots"
    assert row["rows_written"] == 0
    assert row["finished_at"] is not None

    assert await macro_snapshot(engine) == before


# --- Live source (deselected by default) -----------------------------------------------------


@pytest.mark.live
async def test_live_central_bank_rates_csv_still_parses(make_settings: Callable[..., Any]) -> None:
    """Hits the public CBK site (the listing page, then the CSV it links, spaced by the real
    ``PoliteClient``); the ``live`` marker deselects it by default. Checks the layout only."""
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        page = await client.get_text(INTEREST_RATES_PAGE_URL)
        url = find_link(
            page, INTEREST_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=LINK_SUFFIX
        )
        data = await client.get_bytes(url)
    assert len(parse_rates_csv(data)) >= MIN_ROWS


# ---------------------------------------------------------------------------------------------
# Shared CBK helpers (``ingestion.cbk``)
#
# The five CBK pipelines (rates CSV, inflation, FX, bonds, reserves) share the package's
# constants, month and year parsing, month-end dating, missing-cell markers, exact ``Decimal``
# cell parsing and quantization, text cleaning and decoding, link discovery by anchor text,
# HTML-table and CSV reading, range validation and the macro upsert. Each helper is tested
# directly against its documented contract on small inline HTML and CSV built in the test, with
# public Central Bank of Kenya URLs as base URLs. Nothing touches the network or a database:
# ``upsert_macro_records`` is exercised with ``db.upsert.upsert_macro`` replaced by a recorder,
# so only its own de-duplication, date ordering, splitting into same-column runs and chunking
# run.
#
# Expected values are literals taken from the ``ingestion.cbk`` docstrings, never recomputed
# with the code under test. Numbers are ``Decimal`` throughout.
# ---------------------------------------------------------------------------------------------


RATES_PAGE_URL = "https://www.centralbank.go.ke/statistics/interest-rates/"
FX_PAGE_URL = "https://www.centralbank.go.ke/statistics/exchange-rates/"
BULLETIN_PAGE_URL = "https://www.centralbank.go.ke/releases/weekly-bulletin/"
RATES_CSV_URL = (
    "https://www.centralbank.go.ke/uploads/interest_rates/"
    "1715_Central%20Bank%20Rates_with%20Heads.csv"
)
FX_CSV_URL = (
    "https://www.centralbank.go.ke/uploads/exchange_rates/9_Exchange%20Rates%20End%20Period.csv"
)

INFLATION_HEADERS = ("Year", "Month", "Annual Average Inflation", "12-Month Inflation")
NORMALIZED_INFLATION_HEADERS = (
    "year",
    "month",
    "annual average inflation",
    "12-month inflation",
)
RANGE_BOUNDS = {
    "tbill_91d_pct": (Decimal("0"), Decimal("100")),
    "cbr_rate_pct": (Decimal("0"), Decimal("30")),
}
"""Bounds for the ``validate_macro_records`` tests, apart from the imported rates ``BOUNDS``."""
CONNECTION = cast(AsyncConnection, object())
"""Stands in for the run's connection; the recorder below only checks it is passed through."""


def _same_decimal(value: Decimal, expected: str) -> bool:
    """Equal in value, sign, digits and scale, so ``8.750`` never passes for ``8.75``."""
    return isinstance(value, Decimal) and value.as_tuple() == Decimal(expected).as_tuple()


def _anchors(*links: tuple[str, str]) -> str:
    """A listing-page fragment with one ``<a href=...>text</a>`` per ``(text, href)`` pair."""
    items = "".join(f'<li><a href="{href}">{text}</a></li>' for text, href in links)
    return f"<html><body><ul>{items}</ul></body></html>"


def _table(headers: Sequence[str], *rows: Sequence[str]) -> str:
    """A ``<table>`` with a ``<thead>`` header row and one ``<tbody>`` row per ``rows`` item."""
    head = "".join(f"<th>{header}</th>" for header in headers)
    body = "".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


# --- Constants ------------------------------------------------------------------------------


def test_constants_hold_their_documented_values() -> None:
    assert CBK_BASE_URL == "https://www.centralbank.go.ke"
    assert CBK_APPROVED_ORIGINS == frozenset({"https://www.centralbank.go.ke"})
    assert MISSING_MARKERS == frozenset({"", "-", "\u2013", "\u2014"})
    assert _same_decimal(TENOR_QUANTUM, "0.01")
    assert UPSERT_CHUNK_ROWS == 500
    expected_quantum = {
        "inflation_yoy_pct": Decimal("0.0001"),
        "cbr_rate_pct": Decimal("0.0001"),
        "interbank_rate_pct": Decimal("0.0001"),
        "tbill_91d_pct": Decimal("0.0001"),
        "tbill_182d_pct": Decimal("0.0001"),
        "tbill_364d_pct": Decimal("0.0001"),
        "fx_kes_usd": Decimal("0.0001"),
        "fx_reserves_usd_m": Decimal("0.01"),
    }
    # The quantum's scale is what quantization uses, so it is compared as well as its value.
    assert {column: quantum.as_tuple() for column, quantum in COLUMN_QUANTUM.items()} == {
        column: quantum.as_tuple() for column, quantum in expected_quantum.items()
    }
    assert set(COLUMN_QUANTUM) == set(MACRO_INDICATOR_COLUMNS)


@pytest.mark.parametrize(
    ("mapping", "key", "value"),
    [
        pytest.param(MONTHS, "sept", 10, id="months"),
        pytest.param(COLUMN_QUANTUM, "fx_kes_usd", Decimal("1"), id="column-quantum"),
    ],
)
def test_shared_mappings_are_read_only(
    mapping: Mapping[str, object], key: str, value: object
) -> None:
    with pytest.raises(TypeError):
        cast(dict[str, object], mapping)[key] = value


@pytest.mark.parametrize(
    "drifted",
    [
        pytest.param((*MACRO_INDICATOR_COLUMNS, "deposit_rate_pct"), id="column-added-to-model"),
        pytest.param(MACRO_INDICATOR_COLUMNS[1:], id="column-removed-from-model"),
    ],
)
def test_column_quantum_must_match_the_model_at_import(
    monkeypatch: pytest.MonkeyPatch, drifted: tuple[str, ...]
) -> None:
    """The guard runs at import, so the package is executed afresh under a probe name; the
    imported ``opennse_terminal.ingestion.cbk`` is left untouched."""
    monkeypatch.setattr(macro_model, "MACRO_INDICATOR_COLUMNS", drifted)
    spec = importlib.util.spec_from_file_location("cbk_column_quantum_drift_probe", cbk.__file__)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)
    with pytest.raises(RuntimeError, match="do not match models.macro.MACRO_INDICATOR_COLUMNS"):
        spec.loader.exec_module(probe)


# --- Month-end dating, month and year cells -------------------------------------------------


@pytest.mark.parametrize(
    ("year", "month", "expected"),
    [
        pytest.param(2024, 2, date(2024, 2, 29), id="leap-february"),
        pytest.param(2000, 2, date(2000, 2, 29), id="leap-february-400-year-rule"),
        pytest.param(1900, 2, date(1900, 2, 28), id="century-february-not-leap"),
        pytest.param(2026, 2, date(2026, 2, 28), id="common-february"),
        pytest.param(1993, 7, date(1993, 7, 31), id="31-day-month"),
        pytest.param(2026, 9, date(2026, 9, 30), id="30-day-month"),
        pytest.param(2026, 1, date(2026, 1, 31), id="january"),
        pytest.param(2026, 12, date(2026, 12, 31), id="december"),
    ],
)
def test_month_end_is_the_last_day_of_the_month(year: int, month: int, expected: date) -> None:
    assert month_end(year, month) == expected


@pytest.mark.parametrize(
    ("year", "month", "match"),
    [
        pytest.param(2026, 0, "month must be 1-12, got 0", id="month-zero"),
        pytest.param(2026, 13, "month must be 1-12, got 13", id="month-thirteen"),
        pytest.param(2026, -1, "month must be 1-12, got -1", id="negative-month"),
        pytest.param(0, 1, None, id="year-below-date-range"),
        pytest.param(10000, 12, None, id="year-above-date-range"),
    ],
)
def test_month_end_rejects_bad_input(year: int, month: int, match: str | None) -> None:
    with pytest.raises(ValueError, match=match):
        month_end(year, month)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("January", 1, id="full-name"),
        pytest.param("july", 7, id="lowercase-full-name"),
        pytest.param("JUL", 7, id="uppercase-three-letter"),
        pytest.param("  March \n", 3, id="surrounding-whitespace"),
        pytest.param("May", 5, id="may"),
        pytest.param("Sept", 9, id="sept"),
        pytest.param("Sep.", 9, id="abbreviation-with-dot"),
        pytest.param("Dec .", 12, id="abbreviation-with-spaced-dot"),
        pytest.param("December", 12, id="december"),
        pytest.param("1", 1, id="number"),
        pytest.param("01", 1, id="zero-padded-number"),
        pytest.param("9", 9, id="single-digit"),
        pytest.param(" 12 ", 12, id="number-with-whitespace"),
    ],
)
def test_parse_month_reads_names_and_numbers(text: str, expected: int) -> None:
    assert parse_month(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="blank"),
        pytest.param("0", id="zero"),
        pytest.param("00", id="double-zero"),
        pytest.param("13", id="thirteen"),
        pytest.param("012", id="three-digits"),
        pytest.param("1.0", id="decimal"),
        pytest.param("-1", id="negative"),
        pytest.param("Foo", id="unknown-name"),
        pytest.param("Septem", id="truncated-name"),
        pytest.param("Ju ly", id="split-name"),
    ],
)
def test_parse_month_returns_none_for_anything_else(text: str) -> None:
    assert parse_month(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("2026", 2026, id="year"),
        pytest.param(" 1993 ", 1993, id="surrounding-whitespace"),
        pytest.param("1900", 1900, id="lower-limit"),
        pytest.param("2100", 2100, id="upper-limit"),
    ],
)
def test_parse_year_reads_four_digit_years(text: str, expected: int) -> None:
    assert parse_year(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="blank"),
        pytest.param("1899", id="below-1900"),
        pytest.param("2101", id="above-2100"),
        pytest.param("26", id="two-digits"),
        pytest.param("20266", id="five-digits"),
        pytest.param("2026.0", id="decimal"),
        pytest.param("2,026", id="thousands-separator"),
        pytest.param("YEAR", id="header-text"),
    ],
)
def test_parse_year_returns_none_for_anything_else(text: str) -> None:
    assert parse_year(text) is None


# --- Cells: missing markers, exact values and quantization ----------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("", True, id="blank"),
        pytest.param("   ", True, id="whitespace"),
        pytest.param("-", True, id="hyphen"),
        pytest.param(" - ", True, id="padded-hyphen"),
        pytest.param("\u2013", True, id="en-dash"),
        pytest.param("\u2014", True, id="em-dash"),
        pytest.param("\u00a0-\u00a0", True, id="hyphen-between-nbsp"),
        pytest.param("\u200b", True, id="zero-width-space"),
        pytest.param("-1.20", False, id="negative-value"),
        pytest.param("0", False, id="zero"),
        pytest.param("8.75", False, id="value"),
    ],
)
def test_is_missing_cell_recognizes_only_the_missing_markers(text: str, expected: bool) -> None:
    assert is_missing_cell(text) is expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("8.75", "8.75", id="plain"),
        pytest.param(" 6.76 ", "6.76", id="surrounding-whitespace"),
        pytest.param("6.76%", "6.76", id="trailing-percent"),
        pytest.param("6.76 % ", "6.76", id="spaced-trailing-percent"),
        pytest.param("84.67", "84.67", id="1993-91-day-rate"),
        pytest.param("8.7700", "8.7700", id="scale-kept"),
        pytest.param("1,234.50", "1234.50", id="thousands-separator"),
        pytest.param("\u00a012,384\u00a0", "12384", id="nbsp-padded-integer"),
        pytest.param("(1.20)", "-1.20", id="parentheses-negative"),
        pytest.param("-1.20", "-1.20", id="minus-sign"),
        pytest.param("0", "0", id="zero"),
    ],
)
def test_parse_value_cell_is_exact(text: str, expected: str) -> None:
    assert _same_decimal(parse_value_cell(text), expected)


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("", id="blank"),
        pytest.param("  ", id="whitespace"),
        pytest.param("-", id="hyphen"),
        pytest.param("\u2013", id="en-dash"),
        pytest.param("\u2014", id="em-dash"),
        pytest.param("%", id="percent-only"),
        pytest.param("8.75%%", id="double-percent"),
        pytest.param("abc", id="letters"),
        pytest.param("1.2.3", id="two-points"),
        pytest.param("NaN", id="nan"),
        pytest.param("Infinity", id="infinity"),
    ],
)
def test_parse_value_cell_rejects_non_numbers(text: str) -> None:
    with pytest.raises(ValueError, match="not a number"):
        parse_value_cell(text)


@pytest.mark.parametrize(
    ("value", "quantum", "expected"),
    [
        pytest.param("8.77005", "0.0001", "8.7701", id="half-up"),
        pytest.param("-8.77005", "0.0001", "-8.7701", id="half-up-away-from-zero"),
        pytest.param("8.77004", "0.0001", "8.7700", id="round-down"),
        pytest.param("0.125", "0.01", "0.13", id="half-up-not-half-even"),
        pytest.param("8.75", "0.0001", "8.7500", id="scale-extended"),
        pytest.param("12384", "0.01", "12384.00", id="reserves-scale"),
        pytest.param("10.995", "0.01", "11.00", id="tenor-scale-carry"),
    ],
)
def test_quantize_to_rounds_half_up_to_the_quantum_scale(
    value: str, quantum: str, expected: str
) -> None:
    assert _same_decimal(quantize_to(Decimal(value), Decimal(quantum)), expected)


@pytest.mark.parametrize(
    ("value", "quantum"),
    [
        pytest.param("1E+30", "0.0001", id="more-than-28-digits-at-scale"),
        pytest.param("Infinity", "0.01", id="infinity"),
        pytest.param("-Infinity", "0.01", id="negative-infinity"),
    ],
)
def test_quantize_to_turns_invalid_operation_into_value_error(value: str, quantum: str) -> None:
    with pytest.raises(ValueError, match="cannot quantize") as excinfo:
        quantize_to(Decimal(value), Decimal(quantum))
    assert isinstance(excinfo.value.__cause__, InvalidOperation)


# --- Text cleaning and decoding -------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("  Central\u00a0Bank \n Rates\t", "Central Bank Rates", id="whitespace-runs"),
        pytest.param("12-Month\r\nInflation", "12-Month Inflation", id="line-break"),
        pytest.param("\ufeffYear", "Year", id="byte-order-mark"),
        pytest.param("Sep\u200btember", "September", id="zero-width-space"),
        pytest.param(" \u00a0\n ", "", id="only-whitespace"),
        pytest.param("", "", id="empty"),
        pytest.param("8.75", "8.75", id="already-clean"),
    ],
)
def test_clean_text_collapses_whitespace_and_drops_invisible_characters(
    text: str, expected: str
) -> None:
    assert clean_text(text) == expected


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        pytest.param(b"YEAR,MONTH\n", "YEAR,MONTH\n", id="ascii"),
        pytest.param(
            "12-Month Inflation \u2013 2026".encode(),
            "12-Month Inflation \u2013 2026",
            id="utf-8",
        ),
        pytest.param(b"\xef\xbb\xbfYEAR,MONTH", "YEAR,MONTH", id="utf-8-byte-order-mark"),
        pytest.param(
            "Source: Central Bank of Kenya \u2013 2026".encode("cp1252"),
            "Source: Central Bank of Kenya \u2013 2026",
            id="windows-1252-en-dash",
        ),
        pytest.param(b"Caf\xe9", "Caf\u00e9", id="windows-1252-latin-letter"),
        pytest.param(b"", "", id="empty"),
    ],
)
def test_decode_cbk_bytes_reads_utf8_then_windows_1252(data: bytes, expected: str) -> None:
    assert decode_cbk_bytes(data) == expected


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"\x81", id="undefined-in-windows-1252"),
        pytest.param(b"8.75\x8d", id="undefined-byte-after-text"),
    ],
)
def test_decode_cbk_bytes_raises_when_neither_encoding_fits(data: bytes) -> None:
    with pytest.raises(UnicodeDecodeError):
        decode_cbk_bytes(data)


# --- Link discovery -------------------------------------------------------------------------


def test_find_link_matches_anchor_text_and_type_and_returns_an_encoded_absolute_url() -> None:
    html = _anchors(
        ("Home", "/"),
        ("Central Bank Rates", "/uploads/interest_rates/1715_Central Bank Rates.xlsx"),
        (
            "  CENTRAL\u00a0BANK\n RATES (csv) ",
            "/uploads/interest_rates/1715_Central Bank Rates_with Heads.csv",
        ),
    )

    url = find_link(html, RATES_PAGE_URL, text_contains="central  bank rates", href_suffix=".csv")

    assert url == RATES_CSV_URL


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        pytest.param(
            "../../uploads/exchange_rates/9_Exchange Rates End Period.csv",
            FX_CSV_URL,
            id="parent-relative",
        ),
        pytest.param(
            "files/9_Exchange Rates End Period.csv",
            "https://www.centralbank.go.ke/statistics/exchange-rates/files/"
            "9_Exchange%20Rates%20End%20Period.csv",
            id="page-relative",
        ),
        pytest.param(
            "//www.centralbank.go.ke/uploads/exchange_rates/9_Exchange Rates End Period.csv",
            FX_CSV_URL,
            id="scheme-relative",
        ),
        pytest.param(FX_CSV_URL, FX_CSV_URL, id="absolute-with-escapes-kept"),
        pytest.param(
            "/uploads/exchange_rates/9_fx.CSV?ver=2#top",
            "https://www.centralbank.go.ke/uploads/exchange_rates/9_fx.CSV?ver=2#top",
            id="suffix-ignores-case-query-and-fragment",
        ),
    ],
)
def test_find_link_resolves_hrefs_against_the_listing_url(href: str, expected: str) -> None:
    html = _anchors(("Exchange Rates End Period", href))

    url = find_link(
        html, FX_PAGE_URL, text_contains="Exchange Rates End Period", href_suffix=".csv"
    )

    assert url == expected


def test_find_link_considers_only_http_and_https_anchors() -> None:
    html = _anchors(
        ("Central Bank Rates", "mailto:statistics@centralbank.go.ke"),
        ("Central Bank Rates", "javascript:void(0)"),
        ("Central Bank Rates", "ftp://www.centralbank.go.ke/uploads/rates.csv"),
        ("Central Bank Rates", "   "),
        ("Central Bank Rates", "https://www.centralbank.go.ke/uploads/rates.csv"),
    )

    url = find_link(html, RATES_PAGE_URL, text_contains="Central Bank Rates")

    assert url == "https://www.centralbank.go.ke/uploads/rates.csv"


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        pytest.param(
            "/uploads/exchange_rates/9_Exchange_Rates_End_Period.csv",
            "https://www.centralbank.go.ke/uploads/exchange_rates/9_Exchange_Rates_End_Period.csv",
            id="underscores-read-as-spaces",
        ),
        pytest.param(
            "/uploads/exchange_rates/9_Exchange%20Rates%20End%20Period.csv",
            FX_CSV_URL,
            id="url-decoded-file-name",
        ),
    ],
)
def test_find_link_falls_back_to_the_file_name(href: str, expected: str) -> None:
    html = _anchors(
        ("Download", "/uploads/exchange_rates/8_Exchange_Rates_Monthly_Average.csv"),
        ("Download", href),
    )

    url = find_link(
        html, FX_PAGE_URL, text_contains="Exchange Rates End Period", href_suffix=".csv"
    )

    assert url == expected


def test_find_link_prefers_an_anchor_text_match_over_an_earlier_file_name_match() -> None:
    html = _anchors(
        ("Download", "/uploads/interest_rates/1_Central_Bank_Rates_archive.csv"),
        ("Central Bank Rates", "/uploads/interest_rates/2_cbr.csv"),
    )

    url = find_link(html, RATES_PAGE_URL, text_contains="Central Bank Rates", href_suffix=".csv")

    assert url == "https://www.centralbank.go.ke/uploads/interest_rates/2_cbr.csv"


def test_find_link_filters_on_the_path_with_href_contains() -> None:
    html = _anchors(
        (
            "Weekly CBK Bulletin Feb 6 2026",
            "/uploads/press_releases/1_Weekly CBK Bulletin Feb 6 2026.pdf",
        ),
        (
            "Weekly CBK Bulletin Feb 6 2026",
            "/uploads/weekly_bulletin/2_Weekly CBK Bulletin Feb 6 2026.pdf",
        ),
    )

    url = find_link(
        html,
        BULLETIN_PAGE_URL,
        text_contains="Weekly CBK Bulletin",
        href_suffix=".PDF",
        href_contains="/Uploads/Weekly_Bulletin/",
    )

    assert url == (
        "https://www.centralbank.go.ke/uploads/weekly_bulletin/"
        "2_Weekly%20CBK%20Bulletin%20Feb%206%202026.pdf"
    )


@pytest.mark.parametrize(
    ("html", "options", "expected", "found"),
    [
        pytest.param(
            _anchors(("Home", "/"), ("Inflation Rates", "/uploads/inflation/3_inflation.csv")),
            {"text_contains": "Central Bank Rates", "href_suffix": ".csv"},
            ["anchor containing 'Central Bank Rates' ending '.csv'"],
            ["Inflation Rates -> /uploads/inflation/3_inflation.csv"],
            id="candidates-but-no-text-match-lists-candidates",
        ),
        pytest.param(
            _anchors(("Home", " / "), ("Central Bank Rates", "/uploads/interest_rates/1.xlsx")),
            {"text_contains": "Central Bank Rates", "href_suffix": ".csv"},
            ["anchor containing 'Central Bank Rates' ending '.csv'"],
            ["Home -> /", "Central Bank Rates -> /uploads/interest_rates/1.xlsx"],
            id="no-candidates-lists-page-anchors",
        ),
        pytest.param(
            _anchors(("Weekly CBK Bulletin Feb 6 2026", "/uploads/press_releases/1_bulletin.pdf")),
            {
                "text_contains": "Weekly CBK Bulletin",
                "href_suffix": ".pdf",
                "href_contains": "/uploads/weekly_bulletin/",
            },
            [
                "anchor containing 'Weekly CBK Bulletin' ending '.pdf' "
                "with '/uploads/weekly_bulletin/' in its path"
            ],
            ["Weekly CBK Bulletin Feb 6 2026 -> /uploads/press_releases/1_bulletin.pdf"],
            id="href-contains-named-in-expected",
        ),
        pytest.param(
            "<html><body><p>No releases this week.</p></body></html>",
            {"text_contains": "Central Bank Rates", "href_suffix": ".csv"},
            ["anchor containing 'Central Bank Rates' ending '.csv'"],
            [],
            id="page-without-anchors",
        ),
        pytest.param(
            _anchors(*((f"Release {n}", f"/uploads/releases/{n}.csv") for n in range(1, 26))),
            {"text_contains": "Central Bank Rates", "href_suffix": ".csv"},
            ["anchor containing 'Central Bank Rates' ending '.csv'"],
            [f"Release {n} -> /uploads/releases/{n}.csv" for n in range(1, 21)],
            id="at-most-20-links-sampled",
        ),
        pytest.param(
            _anchors(
                ("Home", "https://user:SECRET@www.centralbank.go.ke/?token=SECRET#SECRET"),
                ("Contact", "mailto:SECRET@centralbank.go.ke"),
                ("Central Bank Rates", "https://[::1/uploads/rates.csv"),
            ),
            {"text_contains": "Central Bank Rates", "href_suffix": ".csv"},
            ["anchor containing 'Central Bank Rates' ending '.csv'"],
            [
                "Home -> https://www.centralbank.go.ke/",
                "Contact -> <mailto URL without a host>",
                "Central Bank Rates -> <invalid URL>",
            ],
            id="anchors-named-without-userinfo-query-or-fragment-and-unparseable-skipped",
        ),
    ],
)
def test_find_link_raises_layout_changed_when_nothing_matches(
    html: str, options: dict[str, Any], expected: list[str], found: list[str]
) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        find_link(html, RATES_PAGE_URL, **options)

    assert excinfo.value.expected == expected
    assert excinfo.value.found == found
    assert excinfo.value.message == "file link not found"


@pytest.mark.parametrize(
    ("text", "href", "link", "problem"),
    [
        pytest.param(
            "Central Bank Rates",
            "http://www.centralbank.go.ke/uploads/rates.csv",
            "http://www.centralbank.go.ke/uploads/rates.csv",
            "not HTTPS",
            id="plain-http-on-the-cbk-host",
        ),
        pytest.param(
            "Central Bank Rates",
            "http://169.254.169.254/latest/central_bank_rates.csv",
            "http://169.254.169.254/latest/central_bank_rates.csv",
            "not HTTPS",
            id="link-local-http",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://169.254.169.254/latest/central_bank_rates.csv",
            "https://169.254.169.254/latest/central_bank_rates.csv",
            "host not approved",
            id="link-local-https",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://other.example.net/uploads/rates.csv",
            "https://other.example.net/uploads/rates.csv",
            "host not approved",
            id="other-host",
        ),
        pytest.param(
            "Central Bank Rates",
            "//other.example.net/uploads/rates.csv",
            "https://other.example.net/uploads/rates.csv",
            "host not approved",
            id="scheme-relative-other-host",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://www.centralbank.go.ke.other.example.net/uploads/rates.csv",
            "https://www.centralbank.go.ke.other.example.net/uploads/rates.csv",
            "host not approved",
            id="cbk-host-as-a-prefix",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://user:SECRET@www.centralbank.go.ke/uploads/rates.csv?token=SECRET#SECRET",
            "https://www.centralbank.go.ke/uploads/rates.csv",
            "userinfo",
            id="userinfo-on-the-cbk-host",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://www.centralbank.go.ke:8443/uploads/rates.csv",
            "https://www.centralbank.go.ke:8443/uploads/rates.csv",
            "port 8443",
            id="other-port-on-the-cbk-host",
        ),
        pytest.param(
            "Central Bank Rates",
            "https://www.centralbank.go.ke:https/uploads/rates.csv",
            "<invalid URL>",
            "invalid port",
            id="invalid-port",
        ),
        pytest.param(
            "Download",
            "https://other.example.net/uploads/1_Central_Bank_Rates.csv",
            "https://other.example.net/uploads/1_Central_Bank_Rates.csv",
            "host not approved",
            id="file-name-match-on-another-host",
        ),
    ],
)
def test_find_link_refuses_a_matching_link_off_the_approved_cbk_origin(
    text: str, href: str, link: str, problem: str
) -> None:
    html = _anchors(("Home", "/"), (text, href))

    with pytest.raises(SourceLayoutChanged) as excinfo:
        find_link(html, RATES_PAGE_URL, text_contains="Central Bank Rates", href_suffix=".csv")

    assert excinfo.value.message == "file link not on an approved CBK origin"
    assert excinfo.value.expected == [
        "anchor containing 'Central Bank Rates' ending '.csv' "
        "on https://www.centralbank.go.ke (HTTPS, default port, no userinfo)"
    ]
    assert excinfo.value.found == [f"{text} -> {link} ({problem})"]


def test_find_link_returns_an_approved_link_after_matching_links_off_the_cbk_origin() -> None:
    html = _anchors(
        ("Central Bank Rates", "https://other.example.net/uploads/rates.csv"),
        ("Central Bank Rates", "https://user:SECRET@www.centralbank.go.ke/uploads/rates.csv"),
        ("Download", "/uploads/interest_rates/2_Central_Bank_Rates.csv"),
    )

    url = find_link(html, RATES_PAGE_URL, text_contains="Central Bank Rates", href_suffix=".csv")

    assert url == "https://www.centralbank.go.ke/uploads/interest_rates/2_Central_Bank_Rates.csv"


def test_find_link_logs_its_result_without_userinfo_query_or_fragment(
    caplog: pytest.LogCaptureFixture,
) -> None:
    html = _anchors(("Central Bank Rates", "/uploads/rates.csv?token=SECRET#SECRET"))

    with caplog.at_level(logging.DEBUG, logger="opennse_terminal.ingestion.cbk"):
        url = find_link(
            html,
            f"{RATES_PAGE_URL}?session=SECRET",
            text_contains="Central Bank Rates",
            href_suffix=".csv",
        )

    assert url == "https://www.centralbank.go.ke/uploads/rates.csv?token=SECRET#SECRET"
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == "opennse_terminal.ingestion.cbk"
    ] == [
        "found 'Central Bank Rates' link by anchor text on "
        "https://www.centralbank.go.ke/statistics/interest-rates/: "
        "https://www.centralbank.go.ke/uploads/rates.csv"
    ]


# --- HTML tables ----------------------------------------------------------------------------


def test_extract_table_returns_the_first_table_with_the_expected_headers() -> None:
    html = (
        "<html><body>"
        + _table(("Year", "Month", "12-Month Inflation"), ("2026", "September", "6.76"))
        + "<table><thead><tr>"
        "<th> YEAR </th><th>Month</th><th>Annual\u00a0Average<br/>Inflation</th>"
        "<th>12-Month\n Inflation</th>"
        "</tr></thead><tbody>"
        "<tr><th>2026</th><td>September</td><td>4.20</td><td> 6.76 </td></tr>"
        "<tr><td> </td><td>&nbsp;</td><td></td><td></td></tr>"
        "<tr><td>2026</td><td>Aug<br/>ust</td><td>4.35</td><td>6.60</td></tr>"
        "<tr><td>2025</td></tr>"
        "</tbody></table>"
        + _table(INFLATION_HEADERS, ("1999", "January", "1.00", "1.00"))
        + "</body></html>"
    )

    table = extract_table(html, INFLATION_HEADERS)

    assert table == HtmlTable(
        headers=NORMALIZED_INFLATION_HEADERS,
        rows=[
            (1, ["2026", "September", "4.20", "6.76"]),
            (3, ["2026", "Aug ust", "4.35", "6.60"]),
            (4, ["2025"]),
        ],
    )


@pytest.mark.parametrize(
    "html",
    [
        pytest.param(
            "<table>"
            "<tr><td>Year</td><td>Month</td><td>Annual Average Inflation</td>"
            "<td>12-Month Inflation</td></tr>"
            "<tr><td>2026</td><td>September</td><td>4.20</td><td>6.76</td></tr>"
            "</table>",
            id="first-row-is-header-without-thead",
        ),
        pytest.param(
            "<table><thead>"
            "<tr><th colspan='4'>Inflation Rates (%)</th></tr>"
            "<tr><th>Year</th><th>Month</th><th>Annual Average Inflation</th>"
            "<th>12-Month Inflation</th></tr>"
            "</thead><tbody>"
            "<tr><td>2026</td><td>September</td><td>4.20</td><td>6.76</td></tr>"
            "</tbody></table>",
            id="last-thead-row-is-header",
        ),
        pytest.param(
            "<table><tr><th>Inflation Rates</th></tr><tr><td>"
            + _table(INFLATION_HEADERS, ("2026", "September", "4.20", "6.76"))
            + "</td></tr></table>",
            id="table-nested-in-a-layout-table",
        ),
    ],
)
def test_extract_table_locates_the_header_row(html: str) -> None:
    table = extract_table(html, INFLATION_HEADERS)

    assert table == HtmlTable(
        headers=NORMALIZED_INFLATION_HEADERS, rows=[(1, ["2026", "September", "4.20", "6.76"])]
    )


def test_extract_table_excludes_the_rows_of_nested_tables() -> None:
    html = (
        "<table><thead><tr><th>Year</th><th>Month</th></tr></thead><tbody>"
        "<tr><td>2026</td><td>September<table><tr><td>provisional</td></tr></table></td></tr>"
        "<tr><td>2025</td><td>December</td></tr>"
        "</tbody></table>"
    )

    table = extract_table(html, ("Year", "Month"))

    assert table == HtmlTable(
        headers=("year", "month"),
        rows=[(1, ["2026", "September provisional"]), (2, ["2025", "December"])],
    )


@pytest.mark.parametrize(
    ("html", "found"),
    [
        pytest.param(
            "<html><body><p>Inflation rates</p></body></html>",
            ["no <table> found"],
            id="no-table",
        ),
        pytest.param(
            "<table></table>"
            + _table(
                ("Year", "Month", "Annual Average Inflation", "12 Month Inflation"),
                ("2026", "September", "4.20", "6.76"),
            ),
            [[], ["year", "month", "annual average inflation", "12 month inflation"]],
            id="empty-table-and-renamed-header",
        ),
        pytest.param(
            "".join(_table((f"Column {n}",)) for n in range(1, 7)),
            [[f"column {n}"] for n in range(1, 6)],
            id="at-most-five-tables-sampled",
        ),
    ],
)
def test_extract_table_raises_layout_changed_without_a_matching_table(
    html: str, found: list[object]
) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        extract_table(html, INFLATION_HEADERS)

    assert excinfo.value.expected == list(NORMALIZED_INFLATION_HEADERS)
    assert excinfo.value.found == found
    assert excinfo.value.message == "expected table not found"


# --- CSV files ------------------------------------------------------------------------------


RATES_CSV_LINES = (
    "Central Bank Rates",  # 1: title line, fewer fields than the header
    "YEAR,MONTH,Interbank Rate,91-Day Tbill,Central Bank Rate",  # 2: header
    "2026,September, 8.75 ,8.77,8.75",  # 3
    "",  # 4: blank line
    ",October,-,8.80,",  # 5
    ",,,,",  # 6: blank cells
    "Source: Central Bank of Kenya,,,,",  # 7: footer
    ",NOTE: provisional,,,",  # 8: footer by its first non-blank cell
)
RATES_CSV_PARSED = CbkCsv(
    headers=("year", "month", "interbank rate", "91-day tbill", "central bank rate"),
    rows=[
        (
            3,
            {
                "year": "2026",
                "month": "September",
                "interbank rate": "8.75",
                "91-day tbill": "8.77",
                "central bank rate": "8.75",
            },
        ),
        (
            5,
            {
                "year": "",
                "month": "October",
                "interbank rate": "-",
                "91-day tbill": "8.80",
                "central bank rate": "",
            },
        ),
    ],
)


@pytest.mark.parametrize(
    ("data", "header_row", "expected"),
    [
        pytest.param(
            ("\n".join(RATES_CSV_LINES) + "\n").encode(),
            None,
            RATES_CSV_PARSED,
            id="default-second-line-header-with-blank-and-footer-lines",
        ),
        pytest.param(
            ("\r\n".join(RATES_CSV_LINES) + "\r\n").encode(),
            None,
            RATES_CSV_PARSED,
            id="crlf-line-endings",
        ),
        pytest.param(
            b"\xef\xbb\xbfYear,Month,United States dollar\r\n1993,1,35.9200\r\n",
            0,
            CbkCsv(
                headers=("year", "month", "united states dollar"),
                rows=[(2, {"year": "1993", "month": "1", "united states dollar": "35.9200"})],
            ),
            id="header-on-first-line-after-byte-order-mark",
        ),
        pytest.param(
            "Exchange Rates End Period\nYear,Month,KES \u2013 USD\n2026,9,129.79\n".encode(
                "cp1252"
            ),
            1,
            CbkCsv(
                headers=("year", "month", "kes \u2013 usd"),
                rows=[(3, {"year": "2026", "month": "9", "kes \u2013 usd": "129.79"})],
            ),
            id="windows-1252",
        ),
        pytest.param(
            b"Title\n\nYEAR,MONTH\n\n2026,Jan\n",
            2,
            CbkCsv(headers=("year", "month"), rows=[(5, {"year": "2026", "month": "Jan"})]),
            id="header-row-two-with-blank-lines",
        ),
        pytest.param(
            b"Year,Month,Rate,RATE,,Rate\n1993,1,35.92,36.00,x,37.00\n1993,2\n",
            0,
            CbkCsv(
                headers=("year", "month", "rate", "rate", "unnamed: 4", "rate.1"),
                rows=[
                    (
                        2,
                        {
                            "year": "1993",
                            "month": "1",
                            "rate": "35.92",
                            "unnamed: 4": "x",
                            "rate.1": "37.00",
                        },
                    ),
                    (3, {"year": "1993", "month": "2", "rate": "", "unnamed: 4": "", "rate.1": ""}),
                ],
            ),
            id="blank-repeated-and-colliding-headers-and-short-row",
        ),
        pytest.param(
            b"Central Bank Rates\nYEAR,MONTH\n",
            1,
            CbkCsv(headers=("year", "month"), rows=[]),
            id="header-without-data",
        ),
    ],
)
def test_read_cbk_csv_reads_text_cells_numbered_by_file_line(
    data: bytes, header_row: int | None, expected: CbkCsv
) -> None:
    """``header_row=None`` leaves the documented default, the second line, in force."""
    options = {} if header_row is None else {"header_row": header_row}

    assert read_cbk_csv(data, **options) == expected


@pytest.mark.parametrize(
    ("data", "header_row", "expected"),
    [
        pytest.param(b"", 1, ["CSV header on row 2"], id="empty-file"),
        pytest.param(b"Central Bank Rates\n", 1, ["CSV header on row 2"], id="no-header-line"),
        pytest.param(
            b'YEAR,MONTH\n"2026,September\n', 0, ["CSV header on row 1"], id="unterminated-quote"
        ),
        pytest.param(
            b"YEAR,MONTH\n2026,9\n2026,10,8.75,8.77\n",
            0,
            ["CSV header on row 1"],
            id="row-with-extra-fields",
        ),
        pytest.param(
            b"YEAR,MONTH\n\x81\x8d\n", 0, ["UTF-8 or Windows-1252 text"], id="undecodable"
        ),
    ],
)
def test_read_cbk_csv_raises_layout_changed_when_unreadable(
    data: bytes, header_row: int, expected: list[str]
) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        read_cbk_csv(data, header_row=header_row)

    assert excinfo.value.expected == expected
    assert excinfo.value.message == "csv unreadable"
    found = cast(list[str], excinfo.value.found)
    assert len(found) == 1
    assert 0 < len(found[0]) <= 200
    assert excinfo.value.__cause__ is not None


def test_read_cbk_csv_rejects_a_negative_header_row() -> None:
    with pytest.raises(ValueError, match="header_row must not be negative, got -1"):
        read_cbk_csv(b"YEAR,MONTH\n2026,9\n", header_row=-1)


# --- Validation -----------------------------------------------------------------------------


def test_validate_macro_records_accepts_values_inside_inclusive_bounds() -> None:
    records = [
        MacroRecord(
            date(2026, 9, 30),
            {"tbill_91d_pct": Decimal("8.7700"), "cbr_rate_pct": Decimal("8.7500")},
            line=3,
        ),
        MacroRecord(date(1993, 7, 31), {"tbill_91d_pct": Decimal("84.6700")}, line=4),
        MacroRecord(
            date(2026, 8, 31),
            {"tbill_91d_pct": Decimal("100"), "cbr_rate_pct": Decimal("0")},
            line=5,
        ),
        MacroRecord(date(2026, 7, 31), {"cbr_rate_pct": Decimal("30.0000")}),
        MacroRecord(date(2026, 6, 30), {}),
    ]

    outcome = validate_macro_records(records, RANGE_BOUNDS)

    assert outcome == ValidationOutcome(valid=records, rejected=[])
    assert all(kept is given for kept, given in zip(outcome.valid, records, strict=True))


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        pytest.param(
            MacroRecord(date(2026, 9, 30), {"cbr_rate_pct": Decimal("30.0001")}, line=6),
            RejectedRow(
                index=6,
                reason="out_of_range:cbr_rate_pct:30.0001",
                row={"observation_date": date(2026, 9, 30), "cbr_rate_pct": Decimal("30.0001")},
            ),
            id="above-high",
        ),
        pytest.param(
            MacroRecord(
                date(2026, 9, 30),
                {"tbill_91d_pct": Decimal("-0.0001")},
                raw={"91-day tbill": "-0.0001"},
                line=7,
            ),
            RejectedRow(
                index=7,
                reason="out_of_range:tbill_91d_pct:-0.0001",
                row={"91-day tbill": "-0.0001"},
            ),
            id="below-low-with-raw-cells",
        ),
        pytest.param(
            MacroRecord(date(2026, 9, 30), {"fx_kes_usd": Decimal("129.7900")}, line=8),
            RejectedRow(
                index=8,
                reason="unexpected_column:fx_kes_usd",
                row={"observation_date": date(2026, 9, 30), "fx_kes_usd": Decimal("129.7900")},
            ),
            id="column-without-bounds",
        ),
        pytest.param(
            # NaN never equals itself, so the rejected payload compared here is the raw cells.
            MacroRecord(
                date(2026, 9, 30),
                {"tbill_91d_pct": Decimal("NaN")},
                raw={"91-day tbill": "NaN"},
                line=9,
            ),
            RejectedRow(
                index=9, reason="non_finite:tbill_91d_pct:NaN", row={"91-day tbill": "NaN"}
            ),
            id="nan",
        ),
        pytest.param(
            MacroRecord(date(2026, 9, 30), {"cbr_rate_pct": Decimal("Infinity")}, line=10),
            RejectedRow(
                index=10,
                reason="non_finite:cbr_rate_pct:Infinity",
                row={"observation_date": date(2026, 9, 30), "cbr_rate_pct": Decimal("Infinity")},
            ),
            id="infinity",
        ),
        pytest.param(
            MacroRecord(None, {"cbr_rate_pct": Decimal("8.7500")}, line=11),
            RejectedRow(
                index=11,
                reason="missing_observation_date",
                row={"observation_date": None, "cbr_rate_pct": Decimal("8.7500")},
            ),
            id="missing-observation-date",
        ),
        pytest.param(
            MacroRecord(
                None,
                {},
                errors=("unknown_month:Foo", "no_year"),
                raw={"year": "", "month": "Foo"},
                line=12,
            ),
            RejectedRow(
                index=12, reason="unknown_month:Foo;no_year", row={"year": "", "month": "Foo"}
            ),
            id="undated-with-parse-errors-only",
        ),
        pytest.param(
            MacroRecord(
                date(2026, 9, 30),
                {"cbr_rate_pct": Decimal("45"), "fx_kes_usd": Decimal("129.79")},
                errors=("unparsable:tbill_182d_pct:abc",),
                raw={"182-days tbill": "abc", "central bank rate": "45"},
                line=13,
            ),
            RejectedRow(
                index=13,
                reason=(
                    "unparsable:tbill_182d_pct:abc;out_of_range:cbr_rate_pct:45;"
                    "unexpected_column:fx_kes_usd"
                ),
                row={"182-days tbill": "abc", "central bank rate": "45"},
            ),
            id="several-reasons-joined",
        ),
    ],
)
def test_validate_macro_records_rejects_with_reason_codes(
    record: MacroRecord, expected: RejectedRow
) -> None:
    outcome = validate_macro_records([record], RANGE_BOUNDS)

    assert outcome == ValidationOutcome(valid=[], rejected=[expected])


def test_validate_macro_records_indexes_rejections_by_position_without_a_line() -> None:
    kept = MacroRecord(date(2026, 8, 31), {"cbr_rate_pct": Decimal("9.5000")})
    records = [kept, MacroRecord(date(2026, 9, 30), {"cbr_rate_pct": Decimal("31")})]

    outcome = validate_macro_records(records, RANGE_BOUNDS)

    assert outcome.valid == [kept]
    assert [(row.index, row.reason) for row in outcome.rejected] == [
        (1, "out_of_range:cbr_rate_pct:31")
    ]


@pytest.mark.parametrize(
    "records",
    [
        pytest.param([], id="no-records"),
        pytest.param([MacroRecord(date(2026, 9, 30), {"cbr_rate_pct": Decimal("8.75")})], id="one"),
    ],
)
def test_validate_macro_records_rejects_inverted_bounds(records: list[MacroRecord]) -> None:
    bounds = {**RANGE_BOUNDS, "cbr_rate_pct": (Decimal("30"), Decimal("0"))}

    with pytest.raises(ValueError, match="bounds for 'cbr_rate_pct' are inverted: 30 > 0"):
        validate_macro_records(records, bounds)


def test_validate_macro_records_of_nothing_is_empty() -> None:
    assert validate_macro_records([], RANGE_BOUNDS) == ValidationOutcome(valid=[], rejected=[])


# --- Upsert ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _UpsertCall:
    """One call ``upsert_macro_records`` made to ``db.upsert.upsert_macro``."""

    conn: object
    rows: list[dict[str, object]]
    source: str


@pytest.fixture
def upsert_calls(monkeypatch: pytest.MonkeyPatch) -> list[_UpsertCall]:
    """Replace ``upsert_macro`` with a recorder that reports every row it is given as written."""
    calls: list[_UpsertCall] = []

    async def record_upsert(
        conn: object, rows: Sequence[Mapping[str, object]], *, source: str
    ) -> int:
        calls.append(_UpsertCall(conn=conn, rows=[dict(row) for row in rows], source=source))
        return len(rows)

    monkeypatch.setattr(cbk, "upsert_macro", record_upsert)
    return calls


@pytest.mark.parametrize(
    ("records", "source", "match"),
    [
        pytest.param([], "", "source must be a non-empty string", id="empty-source"),
        pytest.param(
            [
                MacroRecord(date(2026, 9, 30), {"cbr_rate_pct": Decimal("8.7500")}, line=3),
                MacroRecord(None, {"cbr_rate_pct": Decimal("8.7500")}, line=7),
            ],
            "cbk_rates_csv",
            r"1 records without observation_date \(lines \[7\]\)",
            id="undated-record",
        ),
    ],
)
async def test_upsert_macro_records_rejects_bad_arguments_before_writing(
    upsert_calls: list[_UpsertCall], records: list[MacroRecord], source: str, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        await upsert_macro_records(CONNECTION, records, source=source)
    assert upsert_calls == []


@pytest.mark.parametrize(
    "records",
    [
        pytest.param([], id="no-records"),
        pytest.param([MacroRecord(date(2026, 9, 30), {})], id="records-without-values"),
    ],
)
async def test_upsert_macro_records_writes_nothing_without_values(
    upsert_calls: list[_UpsertCall], records: list[MacroRecord]
) -> None:
    assert await upsert_macro_records(CONNECTION, records, source="cbk_rates_csv") == 0
    assert upsert_calls == []


async def test_upsert_macro_records_writes_column_runs_in_date_order_with_the_last_record_winning(
    upsert_calls: list[_UpsertCall],
) -> None:
    records = [
        MacroRecord(date(2026, 7, 31), {"tbill_91d_pct": Decimal("8.0000")}, line=3),
        MacroRecord(
            date(2026, 8, 31),
            {"interbank_rate_pct": Decimal("8.7000"), "cbr_rate_pct": Decimal("8.7500")},
            line=4,
        ),
        MacroRecord(date(2026, 7, 31), {"tbill_91d_pct": Decimal("8.1000")}, line=5),
        MacroRecord(date(2026, 10, 31), {}, line=6),
        MacroRecord(date(2026, 6, 30), {"tbill_91d_pct": Decimal("7.9000")}, line=7),
    ]

    written = await upsert_macro_records(CONNECTION, records, source="cbk_rates_csv")

    assert written == 3
    assert upsert_calls == [
        _UpsertCall(
            conn=CONNECTION,
            rows=[
                {"observation_date": date(2026, 6, 30), "tbill_91d_pct": Decimal("7.9000")},
                {"observation_date": date(2026, 7, 31), "tbill_91d_pct": Decimal("8.1000")},
            ],
            source="cbk_rates_csv",
        ),
        _UpsertCall(
            conn=CONNECTION,
            rows=[
                {
                    "observation_date": date(2026, 8, 31),
                    "interbank_rate_pct": Decimal("8.7000"),
                    "cbr_rate_pct": Decimal("8.7500"),
                }
            ],
            source="cbk_rates_csv",
        ),
    ]


async def test_upsert_macro_records_never_writes_a_column_set_out_of_date_order(
    upsert_calls: list[_UpsertCall],
) -> None:
    # A rates row whose 91-day cell is "-" carries fewer columns. One statement holding both full
    # rows would write either 2026-09-30 before 2026-08-31 or 2026-07-31 after it, against the
    # ascending date order every other pipeline locks rows in.
    records = [
        MacroRecord(
            date(2026, 7, 31),
            {"tbill_91d_pct": Decimal("8.8100"), "cbr_rate_pct": Decimal("8.7500")},
            line=3,
        ),
        MacroRecord(date(2026, 8, 31), {"cbr_rate_pct": Decimal("8.7500")}, line=4),
        MacroRecord(
            date(2026, 9, 30),
            {"tbill_91d_pct": Decimal("8.7700"), "cbr_rate_pct": Decimal("8.7500")},
            line=5,
        ),
    ]

    written = await upsert_macro_records(CONNECTION, records, source="cbk_rates_csv")

    assert written == 3
    assert upsert_calls == [
        _UpsertCall(
            conn=CONNECTION,
            rows=[
                {
                    "observation_date": date(2026, 7, 31),
                    "tbill_91d_pct": Decimal("8.8100"),
                    "cbr_rate_pct": Decimal("8.7500"),
                }
            ],
            source="cbk_rates_csv",
        ),
        _UpsertCall(
            conn=CONNECTION,
            rows=[{"observation_date": date(2026, 8, 31), "cbr_rate_pct": Decimal("8.7500")}],
            source="cbk_rates_csv",
        ),
        _UpsertCall(
            conn=CONNECTION,
            rows=[
                {
                    "observation_date": date(2026, 9, 30),
                    "tbill_91d_pct": Decimal("8.7700"),
                    "cbr_rate_pct": Decimal("8.7500"),
                }
            ],
            source="cbk_rates_csv",
        ),
    ]


@pytest.mark.parametrize(
    "offsets",
    [
        pytest.param(range(1001), id="ascending-input"),
        pytest.param(range(1000, -1, -1), id="descending-input"),
        pytest.param([*range(1, 1001, 2), *range(0, 1001, 2)], id="interleaved-input"),
    ],
)
async def test_upsert_macro_records_writes_in_chunks_of_500_rows_in_date_order(
    upsert_calls: list[_UpsertCall], offsets: Sequence[int]
) -> None:
    first = date(1991, 7, 1)
    records = [
        MacroRecord(first + timedelta(days=offset), {"fx_reserves_usd_m": Decimal("12384.00")})
        for offset in offsets
    ]

    written = await upsert_macro_records(CONNECTION, records, source="cbk_reserves")

    assert written == 1001
    assert [len(call.rows) for call in upsert_calls] == [500, 500, 1]
    assert {call.source for call in upsert_calls} == {"cbk_reserves"}
    dates = [row["observation_date"] for call in upsert_calls for row in call.rows]
    assert dates == [first + timedelta(days=offset) for offset in range(1001)]
    assert (dates[0], dates[499], dates[500], dates[-1]) == (
        date(1991, 7, 1),
        date(1992, 11, 11),
        date(1992, 11, 12),
        date(1994, 3, 27),
    )
