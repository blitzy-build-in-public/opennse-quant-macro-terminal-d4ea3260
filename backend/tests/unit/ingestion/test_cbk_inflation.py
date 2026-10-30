"""Tests for the CBK inflation-rates pipeline (``opennse_terminal.ingestion.cbk.inflation``).

The parser cases run without PostgreSQL or a network, on the recorded page
``fixtures/cbk/inflation_rates.html``: the whole public table, 261 months from September 2026
back to January 2005, newest first, so the production minimum of 100 rows applies unchanged.

The full runs call ``execute_pipeline`` against a fresh seeded clone per test (the root conftest's
``seeded_database``, which marks them ``integration``). respx serves CBK's ``robots.txt`` and the
recorded page at their real URLs with ``assert_all_mocked=True``, so nothing reaches the network,
and the polite client's spacing runs on a fake clock that only a fake sleep advances, so nothing
really waits. Results are read back from ``terminal_macro_indicators`` and
``terminal_ingestion_runs`` through the models.

The one ``live`` test reads the real page and checks its layout only; it is skipped by default.

Expected values are literals from the source page and the seeded 2026-09-30 row, compared as exact
``Decimal`` values.
"""

import asyncio
import calendar
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Any, Protocol

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from opennse_terminal.ingestion.base import (
    MAX_INVALID_FRACTION,
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    execute_pipeline,
)
from opennse_terminal.ingestion.cbk import MacroRecord
from opennse_terminal.ingestion.cbk.inflation import (
    BOUNDS,
    EXPECTED_HEADERS,
    INFLATION_URL,
    MIN_ROWS,
    OWNED_COLUMNS,
    CbkInflationPipeline,
    parse_inflation_table,
)
from opennse_terminal.ingestion.http import PoliteClient
from opennse_terminal.models import TerminalIngestionRun, TerminalMacroIndicator
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

FIXTURE = "cbk/inflation_rates.html"
CBK_ROBOTS_URL = "https://www.centralbank.go.ke/robots.txt"
# The CBK site's own robots.txt: every path allowed except the CMS admin area.
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
NOT_A_TABLE = "<html><body><p>moved</p></body></html>"
RENAMED_HEADERS = ["year", "month", "annual average inflation", "12 month rate"]

INFLATION = "inflation_yoy_pct"
OTHER_COLUMNS = tuple(column for column in MACRO_INDICATOR_COLUMNS if column != INFLATION)
ANCHOR = date(2026, 9, 30)
AUGUST = date(2026, 8, 31)
FIRST_MONTH = date(2005, 1, 31)
FIRST_SEEDED_MONTH = date(2012, 1, 31)
RECORDED_MONTHS = 261
DISTINCT_YEARS = 22  # 2005 to 2026

# The seeded 2026-09-30 row (SEED_MARKET_DATA.md §1, plus CBK's Central Bank Rate for the month).
SEEDED_ANCHOR_ROW: dict[str, Decimal | None] = {
    "inflation_yoy_pct": Decimal("6.80"),
    "cbr_rate_pct": Decimal("8.75"),
    "interbank_rate_pct": Decimal("12.85"),
    "tbill_91d_pct": Decimal("15.72"),
    "tbill_182d_pct": Decimal("16.58"),
    "tbill_364d_pct": Decimal("16.84"),
    "fx_kes_usd": Decimal("129.20"),
    "fx_reserves_usd_m": Decimal("7850.00"),
}
SEEDED_AUGUST_INFLATION = Decimal("6.60")
SPACING_SECONDS = 2.0

_THEAD = re.compile(r"<thead>.*?</thead>", re.DOTALL)
_RENAMED_CELL = re.compile(r">12-Month Inflation<")
_ROW_YEAR = re.compile(r'(<tr id="table_172_row_\d+">\s*<td style="">)(\d{4})(</td>)')
_ROW_THROUGH_MONTH = re.compile(
    r'<tr id="table_172_row_\d+">\s*<td style="">[^<]*</td>\s*<td style="">[^<]*</td>'
)
SURPLUS_CELL = '<td style="">note</td>'
# More than 10% of the recorded rows, the share above which a run ends layout_changed.
SHIFTED_ROWS = 30
# The September 2026 row with a note cell after Month: its cells as details.rejected shows them.
SHIFTED_ANCHOR_CELLS = {
    "year": "2026",
    "month": "September",
    "annual average inflation": "note",
    "12-month inflation": "5.44",
    "extra_1": "6.76",
}


# --- Fakes and helpers ----------------------------------------------------------------------


class FakeClock:
    """Monotonic seconds, starting at 1000.0, that move only when advanced."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping. It still yields
    to the event loop once, as a real sleep would."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)
        await asyncio.sleep(0)


Responder = Callable[[httpx.Request], httpx.Response]


def logged(clock: FakeClock, log: list[tuple[float, str]], respond: Responder) -> Responder:
    """``respond``, first appending ``(fake time, URL)`` to ``log`` as each request reaches the
    mock, so request starts are measured on the fake timeline."""

    def side_effect(request: httpx.Request) -> httpx.Response:
        log.append((clock(), str(request.url)))
        return respond(request)

    return side_effect


class SeededDatabase(Protocol):
    """The parts of the root conftest's ``DatabaseHandle`` these tests use."""

    @property
    def settings(self) -> Any: ...

    @property
    def engine(self) -> AsyncEngine: ...


@dataclass(frozen=True, slots=True)
class Served:
    """One full run and the traffic it sent to the mocked CBK site."""

    result: RunResult
    robots_requests: int
    page_requests: int
    # Every request in the order it reached the mock, as (fake time at its start, URL).
    requests: tuple[tuple[float, str], ...]
    delays: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class MacroRow:
    """One ``terminal_macro_indicators`` row: its eight indicators and ``source_by_field``."""

    values: dict[str, Decimal | None]
    provenance: dict[str, str]


async def run(handle: SeededDatabase, html: str) -> Served:
    """Run ``cbk_inflation`` once, end to end, with respx serving ``html`` at its real URL and
    logging the fake time at which each request starts."""
    clock = FakeClock()
    sleep = FakeSleep(clock)
    log: list[tuple[float, str]] = []
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        robots = router.get(CBK_ROBOTS_URL).mock(
            side_effect=logged(clock, log, lambda _request: httpx.Response(200, text=ALLOW_ALL))
        )
        page = router.get(INFLATION_URL).mock(
            side_effect=logged(clock, log, lambda _request: httpx.Response(200, html=html))
        )
        async with PoliteClient(handle.settings, sleep=sleep, clock=clock) as client:
            result = await execute_pipeline(
                CbkInflationPipeline(handle.settings), handle.engine, trigger="cli", http=client
            )
    return Served(
        result=result,
        robots_requests=robots.call_count,
        page_requests=page.call_count,
        requests=tuple(log),
        delays=tuple(sleep.delays),
    )


async def macro_snapshot(engine: AsyncEngine) -> dict[date, MacroRow]:
    """Every ``terminal_macro_indicators`` row, keyed by ``observation_date``."""
    columns = [getattr(TerminalMacroIndicator, name) for name in MACRO_INDICATOR_COLUMNS]
    statement = select(
        TerminalMacroIndicator.observation_date, TerminalMacroIndicator.source_by_field, *columns
    ).order_by(TerminalMacroIndicator.observation_date)
    async with engine.connect() as conn:
        rows = (await conn.execute(statement)).all()
    return {
        row[0]: MacroRow(
            values=dict(zip(MACRO_INDICATOR_COLUMNS, row[2:], strict=True)),
            provenance=dict(row[1]),
        )
        for row in rows
    }


async def run_row(engine: AsyncEngine, run_id: uuid.UUID) -> TerminalIngestionRun:
    """The ``terminal_ingestion_runs`` row of one run."""
    async with AsyncSession(engine) as session:
        recorded = await session.get(TerminalIngestionRun, run_id)
    assert recorded is not None, f"no run row {run_id}"
    return recorded


def mutated_html(html: str) -> str:
    """The page with its ``12-Month Inflation`` header cell renamed ``12 Month Rate``.

    The recorded table repeats its header row inside ``<thead>`` and is matched on the last one,
    so renaming only the first occurrence would leave a matching header; the cell is renamed in
    both header rows, and the body rows are left as recorded.
    """
    head = _THEAD.search(html)
    assert head is not None, "the recorded page has no <thead>"
    renamed, count = _RENAMED_CELL.subn(">12 Month Rate<", head.group(0))
    assert count == 2, f"expected the two recorded header rows, renamed {count} cells"
    return html[: head.start()] + renamed + html[head.end() :]


def blank_repeated_years(html: str) -> tuple[str, int]:
    """The page with every ``Year`` cell blanked that repeats the row above, as tables that group
    their rows by year show them; returns the page and the number of cells blanked."""
    previous: str | None = None
    blanked = 0

    def blank(match: re.Match[str]) -> str:
        nonlocal previous, blanked
        year = match.group(2)
        if year == previous:
            blanked += 1
            return match.group(1) + match.group(3)
        previous = year
        return match.group(0)

    return _ROW_YEAR.sub(blank, html), blanked


def shift_rows(html: str, rows: int) -> tuple[str, int]:
    """The page with a non-blank cell inserted after ``Month`` in its first ``rows`` body rows,
    which moves each of their values one column right; returns the page and the rows changed."""
    return _ROW_THROUGH_MONTH.subn(lambda match: match.group(0) + SURPLUS_CELL, html, count=rows)


def single_row_page(*cells: str) -> str:
    """A minimal page holding the inflation table with one body row of ``cells``."""
    headers = ("Year", "Month", "Annual Average Inflation", "12-Month Inflation")
    head = "".join(f"<th>{header}</th>" for header in headers)
    body = "".join(f"<td>{cell}</td>" for cell in cells)
    return f"<table><thead><tr>{head}</tr></thead><tbody><tr>{body}</tr></tbody></table>"


def month_ends_newest_first(newest: date, oldest: date) -> list[date]:
    """Every month-end from ``newest`` back to ``oldest``, both included."""
    months: list[date] = []
    year, month = newest.year, newest.month
    while (year, month) >= (oldest.year, oldest.month):
        months.append(date(year, month, calendar.monthrange(year, month)[1]))
        year, month = (year, month - 1) if month > 1 else (year - 1, 12)
    return months


def inflation_by_date(records: list[MacroRecord]) -> dict[date, Decimal]:
    """The parsed 12-month rate of each dated record."""
    return {
        record.observation_date: record.values[INFLATION]
        for record in records
        if record.observation_date is not None
    }


@pytest.fixture(scope="module")
def page(fixture_text: Callable[[str], str]) -> str:
    """The recorded CBK inflation page."""
    return fixture_text(FIXTURE)


@pytest.fixture(scope="module")
def mutated_page(page: str) -> str:
    """The recorded page with its value header renamed."""
    return mutated_html(page)


# --- Contract -------------------------------------------------------------------------------


def test_module_constants_hold_the_documented_contract() -> None:
    assert INFLATION_URL == "https://www.centralbank.go.ke/inflation-rates/"
    assert EXPECTED_HEADERS == (
        "year",
        "month",
        "annual average inflation",
        "12-month inflation",
    )
    assert OWNED_COLUMNS == (INFLATION,)
    assert dict(BOUNDS) == {INFLATION: (Decimal(-20), Decimal(100))}
    assert MIN_ROWS == 100
    assert CbkInflationPipeline.name == "cbk_inflation"
    assert CbkInflationPipeline.min_rows == MIN_ROWS
    assert CbkInflationPipeline.source_url == INFLATION_URL


# --- Parsing the recorded page (no database, no network) ------------------------------------


def test_parse_reads_every_month_of_the_recorded_table(page: str) -> None:
    records = parse_inflation_table(page)

    assert len(records) >= MIN_ROWS
    assert len(records) == RECORDED_MONTHS
    assert all(record.errors == () for record in records)
    dates = [record.observation_date for record in records if record.observation_date is not None]
    assert len(dates) == len(records)
    assert min(dates) == FIRST_MONTH
    assert all((day + timedelta(days=1)).day == 1 for day in dates)
    # Newest first, one record per month and no gap: September 2026 back to January 2005.
    assert dates == month_ends_newest_first(ANCHOR, FIRST_MONTH)


def test_parse_stores_only_the_twelve_month_rate(page: str) -> None:
    records = {record.observation_date: record for record in parse_inflation_table(page)}

    # Annual Average Inflation (5.44 for September 2026) is never stored.
    assert records[ANCHOR].values == {INFLATION: Decimal("6.76")}
    assert records[AUGUST].values == {INFLATION: Decimal("6.59")}
    assert records[FIRST_MONTH].values == {INFLATION: Decimal("14.87")}
    assert all(set(record.values) == {INFLATION} for record in records.values())


def test_parsed_values_are_exact_decimals_inside_the_sanity_bounds(page: str) -> None:
    low, high = Decimal(-20), Decimal(100)
    for record in parse_inflation_table(page):
        value = record.values[INFLATION]
        assert type(value) is Decimal
        assert low <= value <= high
        # Quantized to the NUMERIC(9,4) scale of terminal_macro_indicators.inflation_yoy_pct.
        assert value.as_tuple().exponent == -4


def test_parse_carries_a_blank_year_forward(page: str) -> None:
    grouped, blanked = blank_repeated_years(page)
    # Every row but the first of each year loses its Year cell.
    assert blanked == RECORDED_MONTHS - DISTINCT_YEARS

    records = parse_inflation_table(grouped)

    assert all(record.errors == () for record in records)
    assert [record.observation_date for record in records] == [
        record.observation_date for record in parse_inflation_table(page)
    ]
    # August 2026 follows September 2026, the first row of 2026, and is dated in 2026.
    assert inflation_by_date(records)[AUGUST] == Decimal("6.59")


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        pytest.param(("2026", "September", "5.44", "-"), [], id="dash-is-no-value"),
        pytest.param(("2026", "September", "5.44", ""), [], id="blank-is-no-value"),
        pytest.param(
            ("2026", "Sept", "5.44", "6.76"),
            [(ANCHOR, {INFLATION: Decimal("6.76")}, ())],
            id="abbreviated-month",
        ),
        pytest.param(
            ("", "September", "5.44", "6.76"),
            [(None, {INFLATION: Decimal("6.76")}, ("no_year",))],
            id="no-year-to-carry",
        ),
        pytest.param(
            ("20x6", "September", "5.44", "6.76"),
            [(None, {INFLATION: Decimal("6.76")}, ("invalid_year:20x6",))],
            id="invalid-year",
        ),
        pytest.param(
            ("2026", "Foo", "5.44", "6.76"),
            [(None, {INFLATION: Decimal("6.76")}, ("unknown_month:Foo",))],
            id="unknown-month",
        ),
        pytest.param(
            ("2026", "September", "5.44", "abc"),
            [(ANCHOR, {}, ("unparsable:inflation_yoy_pct:abc",))],
            id="unparsable-value",
        ),
        pytest.param(
            ("2026", "September", "5.44"),
            [(ANCHOR, {}, ("short_row",))],
            id="short-row",
        ),
        pytest.param(
            ("2026", "September", "note", "5.44", "6.76"),
            [(ANCHOR, {}, ("long_row",))],
            id="long-row",
        ),
        pytest.param(
            ("2026", "September", "5.44", "6.76", ""),
            [(ANCHOR, {INFLATION: Decimal("6.76")}, ())],
            id="trailing-blank-cell",
        ),
    ],
)
def test_parse_handles_each_row_shape(
    cells: tuple[str, ...],
    expected: list[tuple[date | None, dict[str, Decimal], tuple[str, ...]]],
) -> None:
    records = parse_inflation_table(single_row_page(*cells))

    assert [
        (record.observation_date, dict(record.values), record.errors) for record in records
    ] == expected


@pytest.mark.parametrize(
    ("value", "accepted"),
    [("-20", True), ("100", True), ("-20.0001", False), ("100.0001", False)],
)
def test_validate_applies_the_inclusive_sanity_bounds(
    make_settings: Callable[..., Any], value: str, accepted: bool
) -> None:
    record = MacroRecord(observation_date=ANCHOR, values={INFLATION: Decimal(value)})

    outcome = CbkInflationPipeline(make_settings()).validate([record])

    assert outcome.valid == ([record] if accepted else [])
    assert [row.reason for row in outcome.rejected] == (
        [] if accepted else [f"out_of_range:{INFLATION}:{value}"]
    )


def test_parse_raises_layout_changed_when_the_value_header_is_renamed(mutated_page: str) -> None:
    with pytest.raises(SourceLayoutChanged) as raised:
        parse_inflation_table(mutated_page)

    expected, found = raised.value.expected, raised.value.found
    assert isinstance(expected, list)
    assert isinstance(found, list)
    assert "12-month inflation" in expected
    assert expected == list(EXPECTED_HEADERS)
    assert RENAMED_HEADERS in found


def test_parse_raises_layout_changed_without_the_table() -> None:
    with pytest.raises(SourceLayoutChanged) as raised:
        parse_inflation_table(NOT_A_TABLE)

    assert raised.value.expected == list(EXPECTED_HEADERS)


# --- Full runs against a seeded clone (PostgreSQL) ------------------------------------------


def assert_one_polite_request(served: Served) -> None:
    """The run read ``robots.txt`` once, then requested the page once, and each request to the
    CBK host started at least ``SPACING_SECONDS`` after the one before it on the fake clock.

    The mocked site answers in no fake time, so the client's single spacing wait, which the fake
    sleep absorbed, is the whole interval: exactly ``SPACING_SECONDS``, putting the page's start
    exactly that far after the start of ``robots.txt``.
    """
    assert (served.robots_requests, served.page_requests) == (1, 1)
    assert [url for _, url in served.requests] == [CBK_ROBOTS_URL, INFLATION_URL]
    starts = [at for at, _ in served.requests]
    assert all(later - earlier >= SPACING_SECONDS for earlier, later in pairwise(starts))
    assert served.delays == (SPACING_SECONDS,)
    assert starts[1] - starts[0] == SPACING_SECONDS


async def test_layout_change_ends_the_run_without_writing(
    seeded_database: SeededDatabase, mutated_page: str
) -> None:
    engine = seeded_database.engine
    before = await macro_snapshot(engine)

    served = await run(seeded_database, mutated_page)

    result = served.result
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert result.error_message
    assert result.details["expected"] == list(EXPECTED_HEADERS)
    assert RENAMED_HEADERS in result.details["found"]
    assert_one_polite_request(served)

    recorded = await run_row(engine, result.run_id)
    assert (recorded.pipeline, recorded.trigger, recorded.status) == (
        "cbk_inflation",
        "cli",
        "layout_changed",
    )
    assert (recorded.rows_fetched, recorded.rows_written, recorded.rows_rejected) == (0, 0, 0)
    assert recorded.source_url == INFLATION_URL
    assert recorded.finished_at is not None
    assert recorded.error_message == result.error_message
    assert recorded.details["expected"] == list(EXPECTED_HEADERS)
    assert recorded.details["source_url"] == INFLATION_URL

    assert await macro_snapshot(engine) == before


async def test_shifted_cells_in_over_a_tenth_of_rows_end_the_run_without_writing(
    seeded_database: SeededDatabase, page: str
) -> None:
    shifted, changed = shift_rows(page, SHIFTED_ROWS)
    assert changed == SHIFTED_ROWS
    assert SHIFTED_ROWS > MAX_INVALID_FRACTION * RECORDED_MONTHS
    engine = seeded_database.engine
    before = await macro_snapshot(engine)

    served = await run(seeded_database, shifted)

    result = served.result
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (
        RECORDED_MONTHS,
        0,
        SHIFTED_ROWS,
    )
    assert result.error_message == (
        f"{SHIFTED_ROWS} of {RECORDED_MONTHS} rows rejected, more than 10% of the rows parsed"
    )
    assert result.details["expected"] == {"min_rows": MIN_ROWS, "max_invalid_fraction": "0.10"}
    assert result.details["found"] == {"parsed": RECORDED_MONTHS, "rejected": SHIFTED_ROWS}
    rejected = result.details["rejected"]
    assert [(row["index"], row["reason"]) for row in rejected] == [
        (line, "long_row") for line in range(1, SHIFTED_ROWS + 1)
    ]
    assert rejected[0]["row"] == SHIFTED_ANCHOR_CELLS
    assert_one_polite_request(served)

    recorded = await run_row(engine, result.run_id)
    assert (recorded.pipeline, recorded.trigger, recorded.status) == (
        "cbk_inflation",
        "cli",
        "layout_changed",
    )
    assert (recorded.rows_fetched, recorded.rows_written, recorded.rows_rejected) == (
        RECORDED_MONTHS,
        0,
        SHIFTED_ROWS,
    )
    assert recorded.error_message == result.error_message
    assert recorded.details["found"] == result.details["found"]
    assert recorded.details["rejected"] == rejected

    # Neither the shifted Annual Average values nor any valid row was written.
    assert await macro_snapshot(engine) == before


async def test_one_shifted_row_is_rejected_and_its_month_keeps_the_seeded_value(
    seeded_database: SeededDatabase, page: str
) -> None:
    shifted, changed = shift_rows(page, 1)
    assert changed == 1
    engine = seeded_database.engine

    served = await run(seeded_database, shifted)

    result = served.result
    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_rejected) == (RECORDED_MONTHS, 1)
    assert 0 < result.rows_written <= RECORDED_MONTHS - 1
    assert result.details["rejected"] == [
        {"index": 1, "reason": "long_row", "row": SHIFTED_ANCHOR_CELLS}
    ]
    assert_one_polite_request(served)
    recorded = await run_row(engine, result.run_id)
    assert (recorded.status, recorded.rows_rejected) == ("partial", 1)

    after = await macro_snapshot(engine)
    # 2026-09-30 keeps the seeded 6.80: neither the shifted 5.44 nor the source's 6.76 is written.
    assert after[ANCHOR].values == SEEDED_ANCHOR_ROW
    assert after[ANCHOR].provenance == dict.fromkeys(MACRO_INDICATOR_COLUMNS, "seed")
    # The unshifted rows are still written.
    assert after[AUGUST].values[INFLATION] == Decimal("6.59")
    assert after[AUGUST].provenance[INFLATION] == "cbk_inflation"


async def test_full_run_merges_the_twelve_month_rate_into_the_seeded_history(
    seeded_database: SeededDatabase, page: str
) -> None:
    engine = seeded_database.engine
    parsed = parse_inflation_table(page)
    expected = inflation_by_date(parsed)
    before = await macro_snapshot(engine)
    # The seeded template: every column of 2026-09-30 from the seed, and no month before 2012.
    assert before[ANCHOR].values == SEEDED_ANCHOR_ROW
    assert before[ANCHOR].provenance == dict.fromkeys(MACRO_INDICATOR_COLUMNS, "seed")
    assert before[AUGUST].values[INFLATION] == SEEDED_AUGUST_INFLATION
    assert min(before) == FIRST_SEEDED_MONTH

    served = await run(seeded_database, page)

    result = served.result
    assert result.status is RunStatus.SUCCEEDED
    assert result.error_message is None
    assert result.rows_rejected == 0
    assert result.rows_fetched == len(parsed)
    assert 0 < result.rows_written <= result.rows_fetched
    assert_one_polite_request(served)

    recorded = await run_row(engine, result.run_id)
    assert (recorded.pipeline, recorded.trigger, recorded.status) == (
        "cbk_inflation",
        "cli",
        "succeeded",
    )
    assert (recorded.rows_fetched, recorded.rows_written, recorded.rows_rejected) == (
        result.rows_fetched,
        result.rows_written,
        0,
    )
    assert recorded.source_url == INFLATION_URL
    assert recorded.finished_at is not None
    assert recorded.error_message is None
    assert "rejected" not in recorded.details

    after = await macro_snapshot(engine)

    # 2026-09-30: CBK's 6.76 replaces the seeded 6.80; every other column keeps its seeded value.
    anchor = after[ANCHOR]
    assert before[ANCHOR].values[INFLATION] == Decimal("6.80")
    assert anchor.values[INFLATION] == Decimal("6.76")
    assert anchor.provenance[INFLATION] == "cbk_inflation"
    for column in OTHER_COLUMNS:
        assert anchor.values[column] == SEEDED_ANCHOR_ROW[column], column
        assert anchor.provenance[column] == "seed", column

    # 2026-08-31: the source's August value replaces the seeded 6.60.
    assert after[AUGUST].values[INFLATION] == expected[AUGUST]
    assert expected[AUGUST] != SEEDED_AUGUST_INFLATION
    assert after[AUGUST].provenance[INFLATION] == "cbk_inflation"

    # 2005-01-31 had no row: it is inserted holding the inflation value alone.
    assert after[FIRST_MONTH].values == {
        INFLATION: expected[FIRST_MONTH],
        **dict.fromkeys(OTHER_COLUMNS),
    }
    assert after[FIRST_MONTH].provenance == {INFLATION: "cbk_inflation"}

    # The whole table: every parsed month carries the source's value with CBK provenance, and
    # every other column and provenance entry is as seeded (absent on the inserted months).
    assert set(after) == set(before) | set(expected)
    no_row = MacroRow(values=dict.fromkeys(MACRO_INDICATOR_COLUMNS), provenance={})
    for day, row in after.items():
        seeded = before.get(day, no_row)
        written = {INFLATION: expected[day]} if day in expected else {}
        assert row.values == {**seeded.values, **written}, day
        assert row.provenance == {
            **seeded.provenance,
            **dict.fromkeys(written, "cbk_inflation"),
        }, day


async def test_rerun_leaves_every_value_and_its_provenance_identical(
    seeded_database: SeededDatabase, page: str
) -> None:
    engine = seeded_database.engine
    first = await run(seeded_database, page)
    assert first.result.status is RunStatus.SUCCEEDED
    assert_one_polite_request(first)
    once = await macro_snapshot(engine)

    second = await run(seeded_database, page)

    assert second.result.status is RunStatus.SUCCEEDED
    assert second.result.rows_rejected == 0
    assert second.result.rows_fetched == first.result.rows_fetched
    # The re-run builds its own client and fake clock, so it reads robots.txt again and is spaced
    # on its own timeline.
    assert_one_polite_request(second)
    assert await macro_snapshot(engine) == once
    async with AsyncSession(engine) as session:
        runs = (
            await session.scalars(
                select(TerminalIngestionRun).where(TerminalIngestionRun.pipeline == "cbk_inflation")
            )
        ).all()
    assert {(recorded.id, recorded.status) for recorded in runs} == {
        (first.result.run_id, "succeeded"),
        (second.result.run_id, "succeeded"),
    }


# --- Live source (skipped by default) -------------------------------------------------------


@pytest.mark.live
async def test_live_page_keeps_the_expected_layout(make_settings: Callable[..., Any]) -> None:
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        html = await client.get_text(INFLATION_URL)

    assert len(parse_inflation_table(html)) >= MIN_ROWS
