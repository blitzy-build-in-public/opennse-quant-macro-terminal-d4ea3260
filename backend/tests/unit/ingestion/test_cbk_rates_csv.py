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

Expected values are literals from the release (interbank 8.75, 91-day 8.77 and Central Bank Rate
8.75 for September 2026; 84.67 for the 91-day rate of July 1993) and from the seeded 2026-09-30
row they replace. Every number is compared as ``Decimal``.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Protocol

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.ingestion.base import (
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    execute_pipeline,
)
from opennse_terminal.ingestion.cbk import CBK_BASE_URL, MacroRecord, find_link
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
