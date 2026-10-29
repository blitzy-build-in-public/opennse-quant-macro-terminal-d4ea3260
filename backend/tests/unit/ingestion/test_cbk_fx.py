"""Tests for the CBK end-period exchange-rate pipeline (``ingestion.cbk.fx``, run name ``cbk_fx``).

Source: the Central Bank of Kenya's public "Exchange Rates End Period" statistical release, a CSV
linked from https://www.centralbank.go.ke/statistics/exchange-rates/. The recorded listing page and
the complete file (January 1993 to September 2026, 405 months) live under ``fixtures/cbk/``.

- **Parse.** ``parse_fx_csv`` runs on the recorded file without a database: only the "United
  States dollar" column is read, the footer is dropped, and a renamed USD header raises
  ``SourceLayoutChanged``.
- **Full runs.** ``execute_pipeline`` runs against a fresh seeded clone (``seeded_database``, which
  marks those tests ``integration``). respx serves ``robots.txt``, the listing page and the CSV at
  the real CBK URLs with ``assert_all_mocked=True``, so nothing reaches the network. The run's
  ``PoliteClient`` spaces its requests on a fake clock that only a fake sleep advances, so no test
  really waits. Tables are read through the ``terminal_`` models with ``select()`` only.
- **Live.** One ``live`` test, skipped by default, checks the real release still parses.

Expected values are literals from the release: 35.92 KES per USD for January 1993, 129.79 for
September 2026, and the seeded 129.20 it replaces. Every number is compared as ``Decimal``.
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
from opennse_terminal.ingestion.cbk import MacroRecord, find_link
from opennse_terminal.ingestion.cbk.fx import (
    BOUNDS,
    EXCHANGE_RATES_PAGE_URL,
    LINK_TEXT,
    MIN_ROWS,
    OWNED_COLUMNS,
    REQUIRED_HEADERS,
    CbkFxPipeline,
    parse_fx_csv,
)
from opennse_terminal.ingestion.http import PoliteClient
from opennse_terminal.models import TerminalIngestionRun, TerminalMacroIndicator
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

LISTING_FIXTURE = "cbk/exchange_rates_page.html"
CSV_FIXTURE = "cbk/fx_end_period.csv"
FIXTURE_MONTHS = 405
"""Data rows of the recorded file: every month from January 1993 to September 2026."""

ROBOTS_URL = "https://www.centralbank.go.ke/robots.txt"
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
"""CBK's own ``robots.txt``: every path allowed except the CMS admin area."""
END_PERIOD_CSV_URL = (
    "https://www.centralbank.go.ke/uploads/exchange_rates/"
    "1103039762_Monthly%20exchange%20rate%20(end%20period).csv"
)
"""The end-period file linked from the recorded listing page, as httpx requests it."""

FX = "fx_kes_usd"
FIRST_MONTH = date(1993, 1, 31)
ANCHOR = date(2026, 9, 30)
SEEDED_ANCHOR_FX = Decimal("129.20")
OTHER_COLUMNS = tuple(column for column in MACRO_INDICATOR_COLUMNS if column != FX)
SNAPSHOT_COLUMNS = (*MACRO_INDICATOR_COLUMNS, "source_by_field", "updated_at")
VALUE_COLUMNS = (*MACRO_INDICATOR_COLUMNS, "source_by_field")
"""What a re-run must leave identical; ``updated_at`` is refreshed by every conflict update."""

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
class FxRun:
    """One full ``cbk_fx`` run on the mocked CBK site.

    Attributes:
        result: What ``execute_pipeline`` returned.
        requested: Every URL the run requested, in order, as httpx sent it.
        delays: The spacing waits the run's ``PoliteClient`` slept on the fake clock.
    """

    result: RunResult
    requested: list[str]
    delays: list[float]


FxRunner = Callable[[_Clone, bytes], Awaitable[FxRun]]


@pytest.fixture(scope="module")
def listing_html(fixture_text: Callable[..., str]) -> str:
    return fixture_text(LISTING_FIXTURE)


@pytest.fixture(scope="module")
def csv_url(listing_html: str) -> str:
    """The CSV link discovered on the recorded listing page, normalized as httpx sends it."""
    discovered = find_link(
        listing_html, EXCHANGE_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=".csv"
    )
    return str(httpx.URL(discovered))


@pytest.fixture(scope="module")
def fx_csv(fixture_bytes: Callable[[str], bytes]) -> bytes:
    return fixture_bytes(CSV_FIXTURE)


@pytest.fixture(scope="module")
def mutated_csv(fx_csv: bytes) -> bytes:
    """The recorded file with its header's "United States dollar" renamed "US Dollar"."""
    title, header, body = fx_csv.split(b"\n", 2)
    renamed = header.replace(b"United States dollar", b"US Dollar", 1)
    assert renamed != header, "the recorded header no longer names 'United States dollar'"
    return b"\n".join((title, renamed, body))


@pytest.fixture(scope="module")
def unreadable_year_csv(fx_csv: bytes) -> bytes:
    """The recorded file with January 2025's year made unreadable (``bad``) and February 2025's
    left blank, so a year carried past the unreadable one would date February 2025 in 2024."""
    mutated = fx_csv
    for stated, replacement in ((b"\n2025,1,", b"\nbad,1,"), (b"\n2025,2,", b"\n,2,")):
        assert mutated.count(stated) == 1, f"the recorded file has no single {stated!r} row"
        mutated = mutated.replace(stated, replacement, 1)
    return mutated


@pytest.fixture(scope="module")
def fx_records(fx_csv: bytes) -> list[MacroRecord]:
    return parse_fx_csv(fx_csv)


@pytest.fixture
def run_fx(listing_html: str, csv_url: str) -> FxRunner:
    """``run(handle, csv_bytes)``: one ``cbk_fx`` run on the clone, with respx serving CBK's
    ``robots.txt``, the recorded listing page and ``csv_bytes`` at the discovered CSV URL."""

    async def run(handle: _Clone, csv_bytes: bytes) -> FxRun:
        clock = FakeClock()
        sleep = FakeSleep(clock)
        with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
            router.get(ROBOTS_URL).respond(200, text=ALLOW_ALL)
            router.get(EXCHANGE_RATES_PAGE_URL).respond(200, html=listing_html)
            router.get(csv_url).respond(
                200, content=csv_bytes, headers={"Content-Type": "text/csv"}
            )
            async with PoliteClient(handle.settings, sleep=sleep, clock=clock) as client:
                result = await execute_pipeline(
                    CbkFxPipeline(handle.settings), handle.engine, trigger="cli", http=client
                )
            requested = [str(call.request.url) for call in router.calls]
        return FxRun(result=result, requested=requested, delays=list(sleep.delays))

    return run


async def macro_snapshot(engine: AsyncEngine) -> MacroSnapshot:
    """Every ``terminal_macro_indicators`` row by ``observation_date``."""
    statement = select(
        TerminalMacroIndicator.observation_date,
        *(getattr(TerminalMacroIndicator, column) for column in SNAPSHOT_COLUMNS),
    )
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


def other_columns(row: Mapping[str, Any]) -> dict[str, Any]:
    """The indicator values of a snapshot row other than ``fx_kes_usd``."""
    return {column: row[column] for column in OTHER_COLUMNS}


def other_sources(row: Mapping[str, Any]) -> dict[str, str]:
    """The ``source_by_field`` entries of a snapshot row other than ``fx_kes_usd``'s."""
    return {column: source for column, source in row["source_by_field"].items() if column != FX}


def assert_polite_sequence(run: FxRun, csv_url: str, min_interval: float) -> None:
    """``robots.txt`` first, then the listing page and the CSV once each, spaced on the clock."""
    assert run.requested == [ROBOTS_URL, EXCHANGE_RATES_PAGE_URL, csv_url]
    assert len(run.delays) == 2
    assert all(delay >= min_interval for delay in run.delays)


# --- Module contract and link discovery ------------------------------------------------------


def test_module_constants_state_the_release_contract() -> None:
    assert EXCHANGE_RATES_PAGE_URL == "https://www.centralbank.go.ke/statistics/exchange-rates/"
    assert LINK_TEXT == "Exchange Rates End Period"
    assert REQUIRED_HEADERS == ("year", "month", "united states dollar")
    assert OWNED_COLUMNS == ("fx_kes_usd",)
    assert dict(BOUNDS) == {FX: (Decimal("10"), Decimal("1000"))}
    assert MIN_ROWS == 300
    assert CbkFxPipeline.name == "cbk_fx"


def test_listing_page_links_the_end_period_csv_not_the_period_average(csv_url: str) -> None:
    # The page also links "Exchange Rates Average Period"; only the end-period file is wanted.
    assert csv_url == END_PERIOD_CSV_URL
    assert "period%20average" not in csv_url.lower()


# --- Pure parse (no database) ----------------------------------------------------------------


def test_parse_reads_every_month_with_exact_anchor_rates(fx_records: list[MacroRecord]) -> None:
    assert len(fx_records) >= MIN_ROWS
    assert len(fx_records) == FIXTURE_MONTHS
    by_date = {record.observation_date: record for record in fx_records}
    assert len(by_date) == len(fx_records)
    assert fx_records[0].observation_date == FIRST_MONTH
    assert by_date[FIRST_MONTH].values == {FX: Decimal("35.92")}
    assert by_date[ANCHOR].values == {FX: Decimal("129.79")}


def test_parse_keeps_only_the_usd_column_within_bounds_at_month_ends(
    fx_records: list[MacroRecord],
) -> None:
    low, high = BOUNDS[FX]
    months: list[int] = []
    for record in fx_records:
        # Sterling, Euro and every other currency column are never read.
        assert set(record.values) == {FX}
        assert record.errors == ()
        value = record.values[FX]
        assert isinstance(value, Decimal)
        assert low <= value <= high
        # Quantized to the NUMERIC(12,4) column's scale.
        assert value.as_tuple().exponent == -4
        day = record.observation_date
        assert day is not None
        assert (day + timedelta(days=1)).day == 1
        months.append(day.year * 12 + day.month)
    # One record per consecutive month: no row lost or duplicated.
    assert months == list(range(months[0], months[0] + len(months)))


def test_parse_drops_the_source_footer(fx_csv: bytes, fx_records: list[MacroRecord]) -> None:
    assert fx_csv.rstrip().splitlines()[-1] == b"Source: Central Bank of Kenya"
    assert fx_records[-1].observation_date == ANCHOR
    assert not any(
        cell.startswith("Source") for record in fx_records for cell in record.raw.values()
    )


def test_parse_raises_layout_changed_when_the_usd_header_is_renamed(mutated_csv: bytes) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        parse_fx_csv(mutated_csv)
    assert isinstance(excinfo.value.expected, list)
    assert "united states dollar" in excinfo.value.expected
    assert isinstance(excinfo.value.found, list)
    assert "us dollar" in excinfo.value.found
    assert "united states dollar" not in excinfo.value.found


def test_an_unreadable_year_stops_the_carry_until_a_year_is_stated_again(
    make_settings: Callable[..., Any],
) -> None:
    data = (
        b",Kenya Shilling End Period Exchange Rates\n"
        b"Year,Month,United States dollar\n"
        b"2025,1,129.00\n"
        b"bad,1,130.00\n"
        b",2,131.00\n"
        b"2026,3,132.00\n"
    )

    records = parse_fx_csv(data)

    # The blank-year row after "bad" is left undated, never dated in 2025.
    assert [
        (record.observation_date, dict(record.values), record.errors) for record in records
    ] == [
        (date(2025, 1, 31), {FX: Decimal("129.00")}, ()),
        (None, {FX: Decimal("130.00")}, ("invalid_year:bad",)),
        (None, {FX: Decimal("131.00")}, ("no_year",)),
        (date(2026, 3, 31), {FX: Decimal("132.00")}, ()),
    ]

    outcome = CbkFxPipeline(make_settings()).validate(records)

    # Only the stated-year rows reach the writer; both rows of the unreadable year are rejected.
    assert outcome.valid == [records[0], records[3]]
    assert [(row.index, row.reason) for row in outcome.rejected] == [
        (4, "invalid_year:bad"),
        (5, "no_year"),
    ]


# --- Full runs against a seeded clone --------------------------------------------------------


async def test_renamed_usd_header_ends_layout_changed_and_writes_nothing(
    seeded_database: _Clone, run_fx: FxRunner, mutated_csv: bytes, csv_url: str
) -> None:
    engine = seeded_database.engine
    before = await macro_snapshot(engine)
    assert before[ANCHOR][FX] == SEEDED_ANCHOR_FX

    run = await run_fx(seeded_database, mutated_csv)

    result = run.result
    assert result.pipeline == "cbk_fx"
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.rows_written == 0
    assert "united states dollar" in result.details["expected"]
    assert "us dollar" in result.details["found"]
    assert_polite_sequence(run, csv_url, seeded_database.settings.scraper_min_interval_seconds)

    row = await run_row(engine, result.run_id)
    assert row["pipeline"] == "cbk_fx"
    assert row["trigger"] == "cli"
    assert row["status"] == "layout_changed"
    assert row["rows_written"] == 0
    assert row["error_message"] == "exchange-rate CSV headers changed"
    assert row["finished_at"] is not None
    assert "united states dollar" in row["details"]["expected"]

    assert await macro_snapshot(engine) == before


async def test_full_run_writes_cbk_rates_with_provenance_and_a_rerun_changes_nothing(
    seeded_database: _Clone,
    run_fx: FxRunner,
    fx_csv: bytes,
    fx_records: list[MacroRecord],
    csv_url: str,
) -> None:
    engine = seeded_database.engine
    min_interval = seeded_database.settings.scraper_min_interval_seconds
    before = await macro_snapshot(engine)
    seeded_anchor = before[ANCHOR]
    assert seeded_anchor[FX] == SEEDED_ANCHOR_FX
    assert seeded_anchor["inflation_yoy_pct"] == Decimal("6.80")
    assert seeded_anchor["fx_reserves_usd_m"] == Decimal("7850.00")
    assert seeded_anchor["source_by_field"] == dict.fromkeys(MACRO_INDICATOR_COLUMNS, "seed")
    assert FIRST_MONTH not in before

    first = await run_fx(seeded_database, fx_csv)

    result = first.result
    assert result.pipeline == "cbk_fx"
    assert result.status is RunStatus.SUCCEEDED
    assert result.error_message is None
    assert result.rows_rejected == 0
    assert result.rows_fetched == FIXTURE_MONTHS
    assert 0 < result.rows_written <= result.rows_fetched
    assert str(httpx.URL(result.details["source_url"])) == csv_url
    assert_polite_sequence(first, csv_url, min_interval)

    row = await run_row(engine, result.run_id)
    assert row["pipeline"] == "cbk_fx"
    assert row["trigger"] == "cli"
    assert row["status"] == "succeeded"
    assert (row["rows_fetched"], row["rows_written"], row["rows_rejected"]) == (
        result.rows_fetched,
        result.rows_written,
        0,
    )

    after = await macro_snapshot(engine)
    anchor = after[ANCHOR]
    assert anchor[FX] == Decimal("129.79")
    assert anchor["source_by_field"] == {**seeded_anchor["source_by_field"], FX: "cbk_fx"}
    assert anchor["inflation_yoy_pct"] == Decimal("6.80")
    assert anchor["fx_reserves_usd_m"] == Decimal("7850.00")
    assert other_columns(anchor) == other_columns(seeded_anchor)

    first_month = after[FIRST_MONTH]
    assert first_month[FX] == Decimal("35.92")
    assert first_month["source_by_field"] == {FX: "cbk_fx"}
    assert all(first_month[column] is None for column in OTHER_COLUMNS)

    # Every month of the file is stored as parsed, credited to cbk_fx, and the seeded rows keep
    # their other columns and those columns' "seed" provenance.
    expected_fx = {
        record.observation_date: record.values[FX]
        for record in fx_records
        if record.observation_date is not None
    }
    assert len(expected_fx) == FIXTURE_MONTHS
    assert {day: after[day][FX] for day in expected_fx} == expected_fx
    assert all(after[day]["source_by_field"][FX] == "cbk_fx" for day in expected_fx)
    for day, seeded in before.items():
        stored = after[day]
        assert other_columns(stored) == other_columns(seeded)
        assert other_sources(stored) == other_sources(seeded)
        if day not in expected_fx:
            assert stored == seeded
    assert set(after) == set(before) | set(expected_fx)

    second = await run_fx(seeded_database, fx_csv)

    assert second.result.status is RunStatus.SUCCEEDED
    assert second.result.rows_rejected == 0
    assert second.result.run_id != result.run_id
    assert_polite_sequence(second, csv_url, min_interval)
    assert snapshot_values(await macro_snapshot(engine)) == snapshot_values(after)


async def test_rows_after_an_unreadable_year_are_rejected_and_no_stale_year_rate_is_written(
    seeded_database: _Clone, run_fx: FxRunner, unreadable_year_csv: bytes, csv_url: str
) -> None:
    engine = seeded_database.engine
    rejected_months = (date(2025, 1, 31), date(2025, 2, 28))
    before = await macro_snapshot(engine)
    assert all(before[day]["source_by_field"][FX] == "seed" for day in rejected_months)

    run = await run_fx(seeded_database, unreadable_year_csv)

    result = run.result
    assert result.status is RunStatus.PARTIAL
    assert result.rows_fetched == FIXTURE_MONTHS
    assert result.rows_rejected == 2
    assert result.rows_written == FIXTURE_MONTHS - 2
    assert [(entry["index"], entry["reason"]) for entry in result.details["rejected"]] == [
        (387, "invalid_year:bad"),
        (388, "no_year"),
    ]
    assert_polite_sequence(run, csv_url, seeded_database.settings.scraper_min_interval_seconds)

    row = await run_row(engine, result.run_id)
    assert row["status"] == "partial"
    assert (row["rows_fetched"], row["rows_written"], row["rows_rejected"]) == (
        FIXTURE_MONTHS,
        FIXTURE_MONTHS - 2,
        2,
    )

    after = await macro_snapshot(engine)
    # The file's own February 2024 rate, not February 2025's 129.23 dated under a carried 2024.
    february_2024 = after[date(2024, 2, 29)]
    assert february_2024[FX] == Decimal("143.59")
    assert february_2024["source_by_field"][FX] == "cbk_fx"
    # The two rejected months keep their seeded rows, values and provenance alike.
    assert {day: after[day] for day in rejected_months} == {
        day: before[day] for day in rejected_months
    }


# --- Live source (skipped by default) --------------------------------------------------------


@pytest.mark.live
async def test_live_end_period_csv_still_parses(make_settings: Callable[..., Any]) -> None:
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        page = await client.get_text(EXCHANGE_RATES_PAGE_URL)
        url = find_link(page, EXCHANGE_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=".csv")
        data = await client.get_bytes(url)
    assert len(parse_fx_csv(data)) >= MIN_ROWS
