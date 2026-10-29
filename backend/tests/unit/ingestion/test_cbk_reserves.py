"""Tests for the ``cbk_reserves`` pipeline (``opennse_terminal.ingestion.cbk.reserves``).

Source: Central Bank of Kenya, Weekly Bulletin. The listing page and the 2026-02-06 bulletin are
recorded fixtures (``fixtures/cbk/weekly_bulletin_page.html`` and
``fixtures/cbk/weekly_bulletin_2026-02-06.pdf``); every other PDF is written in the test by
``helpers.pdf_builder``. Full runs are served by respx at the real CBK URLs with
``assert_all_mocked=True``, so nothing reaches the network, and the run's ``PoliteClient`` spaces
its requests on a fake clock that only the fake sleep advances, so no test really waits.

Reserves are millions of USD and are compared as exact ``Decimal`` values: ``NUMERIC(14,2)``
reads back ``Decimal("12384.00")``, which equals ``Decimal("12384")``.

Pure tests need no database. Tests that request ``seeded_database`` are marked ``integration``
by the root conftest. The one ``live`` test reads the real listing and newest bulletin and is
deselected by default.
"""

from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from typing import Any, Protocol

import httpx
import pytest
import respx
from pdfplumber.page import Page
from sqlalchemy import event, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from helpers.pdf_builder import build_pdf, text_page
from opennse_terminal.ingestion.base import (
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    execute_pipeline,
)
from opennse_terminal.ingestion.cbk import validate_macro_records
from opennse_terminal.ingestion.cbk.reserves import (
    BOUNDS,
    BULLETIN_PATH_MARKER,
    MAX_LIMIT,
    ROW_LABEL,
    TABLE_TITLE,
    WEEKLY_BULLETIN_URL,
    BulletinLink,
    CbkReservesPipeline,
    load_latest_ingested_bulletin,
    parse_bulletin_listing,
    parse_publication_date,
    parse_reserves_bulletin,
    select_bulletins,
)
from opennse_terminal.ingestion.http import PoliteClient
from opennse_terminal.models import TerminalIngestionRun, TerminalMacroIndicator
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

CBK_ORIGIN = "https://www.centralbank.go.ke"
ROBOTS_URL = f"{CBK_ORIGIN}/robots.txt"
# The CBK site's own robots.txt: every path allowed except the CMS admin area.
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
LISTING_FIXTURE = "cbk/weekly_bulletin_page.html"
BULLETIN_FIXTURE = "cbk/weekly_bulletin_2026-02-06.pdf"
SPACING_SECONDS = 2.0
RESERVES = "fx_reserves_usd_m"

NEWEST = date(2026, 2, 6)
SECOND_NEWEST = date(2026, 1, 30)
LISTED_DATES = (
    NEWEST,
    SECOND_NEWEST,
    date(2026, 1, 23),
    date(2026, 1, 16),
    date(2026, 1, 9),
    date(2026, 1, 2),
    date(2025, 12, 24),
    date(2025, 12, 19),
    date(2025, 12, 11),
    date(2025, 12, 5),
)
"""Publication dates of the listing fixture's ten bulletins, newest first."""

EXPECTED_RESERVES: dict[date, Decimal] = {
    date(2026, 1, 8): Decimal("12384"),
    date(2026, 1, 15): Decimal("12341"),
    date(2026, 1, 22): Decimal("12296"),
    date(2026, 1, 29): Decimal("12352"),
    date(2026, 2, 5): Decimal("12387"),
}
"""The 2026-02-06 bulletin's reserves (USD million), keyed by their column header dates."""

SEEDED_DATE = date(2026, 9, 30)
SEEDED_RESERVES = Decimal("7850.00")

TITLE_LINE = "Table 2: Foreign Exchange Reserves (USD Million)"
DATE_ROW = "08-Jan-26 15-Jan-26 22-Jan-26 29-Jan-26 05-Feb-26"
VALUE_LABEL = "1. CBK Foreign Exchange Reserves (USD Million)"
VALUE_ROW = f"{VALUE_LABEL} 12,384 12,341 12,296 12,352 12,387"


def bulletin_pdf(*lines: str) -> bytes:
    """A one-page bulletin whose text layer holds ``lines``, one line each."""
    return build_pdf([text_page(list(lines))])


def bulletin_url(link: BulletinLink) -> str:
    """The URL as httpx requests it, which is the form respx routes and the request log hold."""
    return str(httpx.URL(link.url))


# ---------------------------------------------------------------------------------------------
# Fake time and the fake CBK site
# ---------------------------------------------------------------------------------------------


class FakeClock:
    """Monotonic seconds, starting at 1000.0, that move only when advanced."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)


Responder = Callable[[httpx.Request], httpx.Response]


class FakeCbk:
    """The CBK weekly-bulletin pages as respx serves them to one test.

    ``robots.txt`` allows everything, the listing page is the recorded fixture, and every listed
    bulletin URL serves ``pdf_content[url]``, by default the recorded 2026-02-06 bulletin (the
    same bytes at every URL stand in for the other bulletins). Every request is appended to
    ``log`` as ``(fake time, URL)`` when it reaches the mock, so spacing and request counts are
    measured on the fake timeline.
    """

    def __init__(self, router: respx.MockRouter, listing_html: str, bulletin: bytes) -> None:
        self.clock = FakeClock()
        self.sleep = FakeSleep(self.clock)
        self.bulletin = bulletin
        self.links = parse_bulletin_listing(listing_html)
        self.log: list[tuple[float, str]] = []
        self.pdf_content = {bulletin_url(link): bulletin for link in self.links}
        self._router = router
        self.robots = router.get(ROBOTS_URL).mock(
            side_effect=self._logged(lambda _request: httpx.Response(200, text=ALLOW_ALL))
        )
        self.listing = router.get(WEEKLY_BULLETIN_URL).mock(
            side_effect=self._logged(lambda _request: httpx.Response(200, html=listing_html))
        )
        self.pdfs = {
            url: router.get(url).mock(side_effect=self._logged(self._pdf_responder(url)))
            for url in self.pdf_content
        }

    def _logged(self, respond: Responder) -> Responder:
        def side_effect(request: httpx.Request) -> httpx.Response:
            self.log.append((self.clock(), str(request.url)))
            return respond(request)

        return side_effect

    def _pdf_responder(self, url: str) -> Responder:
        def respond(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=self.pdf_content[url], headers={"content-type": "application/pdf"}
            )

        return respond

    def client(self, settings: Any) -> PoliteClient:
        """A run's polite client on the fake clock and sleep, with the real network mocked."""
        return PoliteClient(settings, sleep=self.sleep, clock=self.clock)

    def requests(self) -> list[tuple[float, str]]:
        """The logged requests other than ``robots.txt``."""
        return [(at, url) for at, url in self.log if url != ROBOTS_URL]

    def pdf_calls(self) -> dict[str, int]:
        """How often each listed bulletin URL was requested."""
        return {url: route.call_count for url, route in self.pdfs.items()}

    def reset(self) -> None:
        """Forget the requests so far, so a test can isolate the next run's."""
        self.log.clear()
        self.sleep.delays.clear()
        self._router.reset()


@pytest.fixture
def cbk(
    fixture_text: Callable[[str], str], fixture_bytes: Callable[[str], bytes]
) -> Iterator[FakeCbk]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield FakeCbk(router, fixture_text(LISTING_FIXTURE), fixture_bytes(BULLETIN_FIXTURE))


class SeededDatabase(Protocol):
    """What these tests use of the root conftest's ``seeded_database`` handle."""

    @property
    def settings(self) -> Any: ...

    @property
    def engine(self) -> AsyncEngine: ...


async def run(handle: SeededDatabase, cbk: FakeCbk, limit: int | None = None) -> RunResult:
    """One CLI-triggered ``cbk_reserves`` run against ``handle`` through the fake CBK site."""
    if limit is None:
        pipeline = CbkReservesPipeline(handle.settings)
    else:
        pipeline = CbkReservesPipeline(handle.settings, limit=limit)
    async with cbk.client(handle.settings) as client:
        return await execute_pipeline(pipeline, handle.engine, trigger="cli", http=client)


async def stored_rows(
    engine: AsyncEngine, days: Sequence[date]
) -> dict[date, TerminalMacroIndicator]:
    """The ``terminal_macro_indicators`` rows stored at ``days``, keyed by observation date."""
    async with AsyncSession(engine) as session:
        rows = await session.scalars(
            select(TerminalMacroIndicator).where(TerminalMacroIndicator.observation_date.in_(days))
        )
        return {row.observation_date: row for row in rows}


async def stored_reserves(engine: AsyncEngine, days: Sequence[date]) -> dict[date, Decimal | None]:
    """``fx_reserves_usd_m`` of the rows stored at ``days``."""
    return {day: row.fx_reserves_usd_m for day, row in (await stored_rows(engine, days)).items()}


async def stored_run(engine: AsyncEngine, result: RunResult) -> TerminalIngestionRun:
    """The ``terminal_ingestion_runs`` row of ``result``."""
    async with AsyncSession(engine) as session:
        row = await session.get(TerminalIngestionRun, result.run_id)
    assert row is not None
    return row


# ---------------------------------------------------------------------------------------------
# Constants and options
# ---------------------------------------------------------------------------------------------


def test_source_constants_and_pipeline_identity() -> None:
    assert WEEKLY_BULLETIN_URL == "https://www.centralbank.go.ke/releases/weekly-bulletin/"
    assert BULLETIN_PATH_MARKER == "/uploads/weekly_bulletin/"
    assert TABLE_TITLE == TITLE_LINE.lower()
    assert ROW_LABEL == VALUE_LABEL.lower()
    assert MAX_LIMIT == 26
    assert dict(BOUNDS) == {RESERVES: (Decimal(100), Decimal(100000))}
    assert CbkReservesPipeline.name == "cbk_reserves"
    assert CbkReservesPipeline.min_rows == 1


@pytest.mark.parametrize("limit", [None, 1, MAX_LIMIT])
def test_pipeline_accepts_limits_from_one_to_max_limit(
    make_settings: Callable[..., Any], limit: int | None
) -> None:
    assert CbkReservesPipeline(make_settings(), limit=limit).limit == limit


@pytest.mark.parametrize("limit", [-1, 0, MAX_LIMIT + 1])
def test_pipeline_rejects_limits_outside_one_to_max_limit(
    make_settings: Callable[..., Any], limit: int
) -> None:
    with pytest.raises(ValueError, match="limit"):
        CbkReservesPipeline(make_settings(), limit=limit)


# ---------------------------------------------------------------------------------------------
# Bulletin PDF parsing
# ---------------------------------------------------------------------------------------------


def test_fixture_bulletin_values_are_dated_by_their_column_headers(
    fixture_bytes: Callable[[str], bytes],
) -> None:
    records = parse_reserves_bulletin(fixture_bytes(BULLETIN_FIXTURE))

    dates = [record.observation_date for record in records]
    assert dates == list(EXPECTED_RESERVES)
    # Dated at each column's header date, never at the bulletin's publication date.
    assert NEWEST not in dates
    assert records[0].values == {RESERVES: Decimal("12384")}
    assert records[-1].values == {RESERVES: Decimal("12387")}
    assert {record.observation_date: record.values[RESERVES] for record in records} == (
        EXPECTED_RESERVES
    )
    low, high = BOUNDS[RESERVES]
    for record in records:
        value = record.values[RESERVES]
        assert isinstance(value, Decimal)
        assert low <= value <= high
        assert record.errors == ()
    assert all(earlier < later for earlier, later in pairwise(d for d in dates if d is not None))


def test_renumbered_table_is_found_past_a_narrative_mention() -> None:
    records = parse_reserves_bulletin(
        bulletin_pdf(
            "Usable foreign exchange reserves remained adequate (see Table 3).",
            "Table 3: Foreign Exchange Reserves (USD Million)",
            "Commentary without dates",
            "Table 3: Foreign Exchange Reserves (USD Million)",
            "01-Oct-26 08-Oct-26",
            "1. CBK Foreign Exchange Reserves (USD Million) 9,051.5 9,102",
        )
    )

    assert [(record.observation_date, record.values) for record in records] == [
        (date(2026, 10, 1), {RESERVES: Decimal("9051.50")}),
        (date(2026, 10, 8), {RESERVES: Decimal("9102.00")}),
    ]


@pytest.mark.parametrize(
    ("content", "message"),
    [
        pytest.param(
            bulletin_pdf(TITLE_LINE, f"{VALUE_LABEL} 12,384 12,390"),
            "reserves table not found",
            id="no-date-row",
        ),
        pytest.param(
            bulletin_pdf(
                TITLE_LINE, "08-Jan-26 15-Jan-26 22-Jan-26", f"{VALUE_LABEL} 12,384 12,390"
            ),
            "reserves row has 2 values for 3 header dates",
            id="value-count-mismatch",
        ),
        pytest.param(
            bulletin_pdf(TITLE_LINE, "08-Jan-26 15-Jan-26", f"{VALUE_LABEL} 12,384 note 12,390"),
            "reserves row has 3 values for 2 header dates",
            id="residual-text-in-value-row",
        ),
        pytest.param(
            bulletin_pdf(DATE_ROW, VALUE_ROW),
            "reserves table not found",
            id="no-title",
        ),
        pytest.param(
            bulletin_pdf(TITLE_LINE, "15-Jan-26 08-Jan-26", f"{VALUE_LABEL} 12,384 12,390"),
            "reserves table header dates not ascending",
            id="dates-not-ascending",
        ),
        pytest.param(
            bulletin_pdf(TITLE_LINE, "08-Jan-26 30-Feb-26", f"{VALUE_LABEL} 12,384 12,390"),
            "reserves table header date unparseable",
            id="date-does-not-exist",
        ),
        pytest.param(
            bulletin_pdf(TITLE_LINE, DATE_ROW, "Table 3: Exchange Rates", VALUE_ROW),
            "reserves value row not found",
            id="value-row-after-next-table",
        ),
        pytest.param(b"<!DOCTYPE html><html></html>", "bulletin is not a PDF", id="not-a-pdf"),
    ],
)
def test_bulletin_layout_changes_are_rejected(content: bytes, message: str) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        parse_reserves_bulletin(content)

    assert excinfo.value.message == message


@pytest.mark.parametrize(
    ("cell", "parsed", "reason"),
    [
        pytest.param(
            "(12,384)", Decimal("-12384"), f"out_of_range:{RESERVES}:-12384.00", id="parenthesized"
        ),
        pytest.param(
            "-12,384", Decimal("-12384"), f"out_of_range:{RESERVES}:-12384.00", id="minus"
        ),
        pytest.param("bad12384", None, f"unparsable:{RESERVES}:bad12384", id="leading-text"),
        pytest.param("12384bad", None, f"unparsable:{RESERVES}:12384bad", id="trailing-text"),
        pytest.param("12,384%", None, f"unparsable:{RESERVES}:12,384%", id="percent-sign"),
        pytest.param("1,2384", None, f"unparsable:{RESERVES}:1,2384", id="misplaced-comma"),
        pytest.param("12,38,4", None, f"unparsable:{RESERVES}:12,38,4", id="short-groups"),
        pytest.param("(12,384", None, f"unparsable:{RESERVES}:(12,384", id="unbalanced"),
        pytest.param("12.384.5", None, f"unparsable:{RESERVES}:12.384.5", id="two-points"),
    ],
)
def test_malformed_or_negative_value_cells_are_rejected_not_repaired(
    cell: str, parsed: Decimal | None, reason: str
) -> None:
    records = parse_reserves_bulletin(
        bulletin_pdf(TITLE_LINE, "08-Jan-26 15-Jan-26", f"{VALUE_LABEL} {cell} 12,390")
    )
    outcome = validate_macro_records(records, BOUNDS)

    assert [record.observation_date for record in records] == [date(2026, 1, 8), date(2026, 1, 15)]
    first, second = records
    assert first.raw["value"] == cell
    assert first.values == ({} if parsed is None else {RESERVES: parsed})
    assert [row.reason for row in outcome.rejected] == [reason]
    # Only the well-formed cell survives: no cell is read as the 12384 its digits spell.
    assert outcome.valid == [second]
    assert second.values == {RESERVES: Decimal("12390")}


def test_minus_sign_in_the_text_layer_reads_as_negative_and_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Embedded fonts with a ToUnicode map give pdfplumber U+2212 for a minus glyph; the test
    # PDF's WinAnsi font cannot draw it, so its extracted text is made to report one.
    extract_text = Page.extract_text
    monkeypatch.setattr(
        Page,
        "extract_text",
        lambda page, **kwargs: extract_text(page, **kwargs).replace("-12,384", "\u221212,384"),
    )

    records = parse_reserves_bulletin(
        bulletin_pdf(TITLE_LINE, "08-Jan-26 15-Jan-26", f"{VALUE_LABEL} -12,384 12,390")
    )
    outcome = validate_macro_records(records, BOUNDS)

    first, second = records
    assert first.raw["value"] == "\u221212,384"
    assert first.values == {RESERVES: Decimal("-12384")}
    assert [row.reason for row in outcome.rejected] == [f"out_of_range:{RESERVES}:-12384.00"]
    assert outcome.valid == [second]
    assert second.values == {RESERVES: Decimal("12390")}


# ---------------------------------------------------------------------------------------------
# Listing, publication dates and selection
# ---------------------------------------------------------------------------------------------


def test_listing_finds_every_dated_bulletin_newest_first(
    fixture_text: Callable[[str], str],
) -> None:
    links = parse_bulletin_listing(fixture_text(LISTING_FIXTURE))

    assert len(links) >= 2
    assert tuple(link.publication_date for link in links) == LISTED_DATES
    assert links[0].publication_date == NEWEST
    assert links[0].title == "06-02-2026"
    assert links[0].url == (
        f"{CBK_ORIGIN}/uploads/weekly_bulletin/"
        "1216473036_Weekly%20CBK%20Bulletin%20Feb%206%202026.pdf"
    )
    for link in links:
        assert BULLETIN_PATH_MARKER in link.url
        assert link.url.startswith(f"{CBK_ORIGIN}/") and link.url.endswith(".pdf")
        # Percent-encoded already, so httpx requests exactly this URL.
        assert bulletin_url(link) == link.url


def test_listing_follows_only_links_on_the_cbk_origin() -> None:
    html = (
        '<a href="https://mirror.example.net/uploads/weekly_bulletin/'
        'Weekly CBK Bulletin Feb 13 2026.pdf">13-02-2026</a>'
        '<a href="http://www.centralbank.go.ke/uploads/weekly_bulletin/'
        'Weekly CBK Bulletin Feb 13 2026.pdf">13-02-2026</a>'
        '<a href="/uploads/weekly_bulletin/1_Weekly CBK Bulletin Feb 6 2026.pdf">06-02-2026</a>'
    )

    assert parse_bulletin_listing(html) == [
        BulletinLink(
            publication_date=NEWEST,
            url=f"{CBK_ORIGIN}/uploads/weekly_bulletin/1_Weekly%20CBK%20Bulletin%20Feb%206%202026.pdf",
            title="06-02-2026",
        )
    ]


@pytest.mark.parametrize(
    ("html", "message"),
    [
        pytest.param(
            '<a href="/statistics/">Statistics</a>',
            "weekly bulletin links not found",
            id="no-bulletin-links",
        ),
        pytest.param(
            '<a href="https://mirror.example.net/uploads/weekly_bulletin/b.pdf">06-02-2026</a>',
            "weekly bulletin links not on an approved CBK origin",
            id="off-origin-only",
        ),
        pytest.param(
            '<a href="/uploads/weekly_bulletin/123_Bulletin.pdf">Download</a>',
            "weekly bulletin links without a publication date",
            id="undated",
        ),
    ],
)
def test_listing_without_a_dated_bulletin_link_is_a_layout_change(html: str, message: str) -> None:
    with pytest.raises(SourceLayoutChanged) as excinfo:
        parse_bulletin_listing(f"<html><body>{html}</body></html>")

    assert excinfo.value.message == message


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Weekly CBK Bulletin Feb 6 2026", date(2026, 2, 6)),
        ("Weekly CBK Bulletin February 6 2026", date(2026, 2, 6)),
        ("Weekly CBK Bulletin, Dec 24, 2025", date(2025, 12, 24)),
        ("Weekly CBK Bulletin, 23 October, 2026", date(2026, 10, 23)),
        ("Weekly CBK Bulletin January 23 2026..3", date(2026, 1, 23)),
        ("06-02-2026", date(2026, 2, 6)),
        ("07-10-16", date(2016, 10, 7)),
        ("Weekly CBK Bulletin Feb 30 2026", None),
        ("Weekly CBK Bulletin Fob 6 2026", None),
        ("Download", None),
    ],
)
def test_publication_date_from_anchor_text_or_file_name(text: str, expected: date | None) -> None:
    assert parse_publication_date(text) == expected


@pytest.mark.parametrize(
    ("ingested", "limit", "expected"),
    [
        pytest.param(set(), None, [NEWEST], id="nothing-ingested-takes-the-newest-only"),
        pytest.param(
            {SECOND_NEWEST, date(2026, 1, 23)}, None, [NEWEST], id="older-ingested-takes-the-newest"
        ),
        # The default rule never reaches back past an ingested newest bulletin: an older
        # bulletin's provisional figures would overwrite the newer one's values for the weeks
        # both carry. The weeks of a skipped bulletin arrive with the next one.
        pytest.param({NEWEST}, None, [], id="newest-ingested-selects-nothing"),
        pytest.param(set(LISTED_DATES), None, [], id="all-ingested-selects-nothing"),
        pytest.param(
            {NEWEST}, 2, [SECOND_NEWEST, NEWEST], id="limit-ignores-ingested-oldest-first"
        ),
        pytest.param(set(), MAX_LIMIT, sorted(LISTED_DATES), id="limit-beyond-the-listing"),
    ],
)
def test_select_bulletins(
    fixture_text: Callable[[str], str],
    ingested: set[date],
    limit: int | None,
    expected: list[date],
) -> None:
    links = parse_bulletin_listing(fixture_text(LISTING_FIXTURE))

    selected = select_bulletins(links, ingested=ingested, limit=limit)

    assert [link.publication_date for link in selected] == expected
    assert all(link in links for link in selected)


def test_select_bulletins_rejects_a_limit_below_one(fixture_text: Callable[[str], str]) -> None:
    links = parse_bulletin_listing(fixture_text(LISTING_FIXTURE))

    with pytest.raises(ValueError, match="limit"):
        select_bulletins(links, ingested=set(), limit=0)


# ---------------------------------------------------------------------------------------------
# Full runs against a seeded database
# ---------------------------------------------------------------------------------------------


async def test_first_run_writes_the_newest_bulletin_at_its_header_dates(
    seeded_database: SeededDatabase, cbk: FakeCbk
) -> None:
    engine = seeded_database.engine
    newest_url = bulletin_url(cbk.links[0])
    # Seeded macro rows are month-ends only: nothing exists yet at the header dates.
    assert await stored_rows(engine, [*EXPECTED_RESERVES, NEWEST]) == {}

    result = await run(seeded_database, cbk)

    assert result.status is RunStatus.SUCCEEDED
    assert result.pipeline == "cbk_reserves"
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (5, 5, 0)
    assert result.error_message is None
    assert result.details["bulletins"] == [NEWEST.isoformat()]
    assert result.details["bulletin_urls"] == [newest_url]

    run_row = await stored_run(engine, result)
    assert (run_row.pipeline, run_row.trigger, run_row.status) == (
        "cbk_reserves",
        "cli",
        "succeeded",
    )
    assert run_row.details["bulletins"] == ["2026-02-06"]
    assert (run_row.rows_fetched, run_row.rows_written, run_row.rows_rejected) == (5, 5, 0)
    assert run_row.source_url == WEEKLY_BULLETIN_URL
    assert run_row.finished_at is not None

    rows = await stored_rows(engine, [*EXPECTED_RESERVES, NEWEST])
    assert set(rows) == set(EXPECTED_RESERVES)
    for day, row in rows.items():
        assert row.fx_reserves_usd_m == EXPECTED_RESERVES[day]
        # New rows: the reserves column is the only one set, and its provenance the only entry.
        assert row.source_by_field == {RESERVES: "cbk_reserves"}
        assert {column: getattr(row, column) for column in MACRO_INDICATOR_COLUMNS} == {
            column: EXPECTED_RESERVES[day] if column == RESERVES else None
            for column in MACRO_INDICATOR_COLUMNS
        }

    seeded = (await stored_rows(engine, [SEEDED_DATE]))[SEEDED_DATE]
    assert seeded.fx_reserves_usd_m == SEEDED_RESERVES
    assert seeded.source_by_field[RESERVES] == "seed"

    assert cbk.listing.call_count == 1
    assert cbk.pdf_calls() == {url: int(url == newest_url) for url in cbk.pdfs}


async def test_repeat_run_without_a_newer_bulletin_is_no_new_data(
    seeded_database: SeededDatabase, cbk: FakeCbk
) -> None:
    engine = seeded_database.engine
    first = await run(seeded_database, cbk)
    assert first.status is RunStatus.SUCCEEDED
    cbk.reset()

    # The listing's newest bulletin is the one the first run ingested; older listed bulletins
    # are never selected by the default rule (see test_select_bulletins).
    repeat = await run(seeded_database, cbk)

    assert repeat.status is RunStatus.NO_NEW_DATA
    assert (repeat.rows_fetched, repeat.rows_written, repeat.rows_rejected) == (0, 0, 0)
    assert repeat.error_message == "no weekly bulletin newer than those already ingested"
    assert repeat.details["newest_listed_bulletin"] == "2026-02-06"
    assert repeat.details["latest_ingested_bulletin"] == "2026-02-06"
    assert "bulletins" not in repeat.details
    assert (await stored_run(engine, repeat)).status == "no_new_data"

    assert cbk.listing.call_count == 1
    assert sum(cbk.pdf_calls().values()) == 0
    assert await stored_reserves(engine, list(EXPECTED_RESERVES)) == EXPECTED_RESERVES


async def test_limit_two_issues_three_spaced_requests(
    seeded_database: SeededDatabase, cbk: FakeCbk
) -> None:
    newest_url, second_url = bulletin_url(cbk.links[0]), bulletin_url(cbk.links[1])

    result = await run(seeded_database, cbk, limit=2)

    assert result.status is RunStatus.SUCCEEDED
    # robots.txt comes first and is not counted: then the listing and the two bulletins, oldest
    # first, one request each.
    assert cbk.log[0][1] == ROBOTS_URL
    assert cbk.robots.call_count == 1
    assert [url for _at, url in cbk.requests()] == [WEEKLY_BULLETIN_URL, second_url, newest_url]
    assert cbk.listing.call_count == 1
    assert cbk.pdf_calls() == {url: int(url in (newest_url, second_url)) for url in cbk.pdfs}

    # Every request to the CBK host, robots.txt included, starts at least 2 s after the previous
    # one on the fake timeline; the waits were fake sleeps, never real ones.
    starts = [at for at, _url in cbk.log]
    assert all(later - earlier >= SPACING_SECONDS for earlier, later in pairwise(starts))
    assert len(cbk.sleep.delays) == 3
    assert all(0 < delay <= SPACING_SECONDS for delay in cbk.sleep.delays)

    assert result.details["bulletins"] == ["2026-01-30", "2026-02-06"]
    assert result.details["bulletin_urls"] == [second_url, newest_url]
    # Both bulletins carry the same five weeks here, so ten records collapse to five rows.
    assert (result.rows_fetched, result.rows_written) == (10, 5)
    assert (await stored_run(seeded_database.engine, result)).details["bulletins"] == [
        "2026-01-30",
        "2026-02-06",
    ]


async def test_newest_bulletin_wins_for_weeks_two_bulletins_repeat(
    seeded_database: SeededDatabase, cbk: FakeCbk
) -> None:
    engine = seeded_database.engine
    # The 2026-01-30 bulletin carries 2026-01-01 and earlier, provisional figures for four weeks
    # the 2026-02-06 bulletin repeats.
    cbk.pdf_content[bulletin_url(cbk.links[1])] = bulletin_pdf(
        "Weekly Bulletin - January 30, 2026",
        TITLE_LINE,
        "01-Jan-26 08-Jan-26 15-Jan-26 22-Jan-26 29-Jan-26",
        f"{VALUE_LABEL} 12,300 12,310 12,320 12,330 12,340",
    )

    backfill = await run(seeded_database, cbk, limit=2)

    assert backfill.status is RunStatus.SUCCEEDED
    assert (backfill.rows_fetched, backfill.rows_written) == (10, 6)
    expected = {date(2026, 1, 1): Decimal("12300"), **EXPECTED_RESERVES}
    assert await stored_reserves(engine, list(expected)) == expected

    # A backfill run counts as ingested, so the default rule finds nothing newer afterwards.
    cbk.reset()
    follow_up = await run(seeded_database, cbk)

    assert follow_up.status is RunStatus.NO_NEW_DATA
    assert follow_up.details["latest_ingested_bulletin"] == "2026-02-06"
    assert sum(cbk.pdf_calls().values()) == 0
    assert await stored_reserves(engine, list(expected)) == expected


async def test_bulletin_without_the_reserves_table_writes_nothing(
    seeded_database: SeededDatabase, cbk: FakeCbk
) -> None:
    engine = seeded_database.engine
    newest_url = bulletin_url(cbk.links[0])
    cbk.pdf_content[newest_url] = bulletin_pdf(
        "Table 1: Interbank Market Rates", "Average weekly interbank rate (%) 9.18 9.16"
    )

    changed = await run(seeded_database, cbk)

    assert changed.status is RunStatus.LAYOUT_CHANGED
    assert (changed.rows_fetched, changed.rows_written) == (0, 0)
    assert changed.error_message == "reserves table not found"
    assert changed.details["bulletins"] == ["2026-02-06"]
    assert changed.details["expected"][0].startswith(TABLE_TITLE)
    assert (await stored_run(engine, changed)).status == "layout_changed"
    assert await stored_rows(engine, list(EXPECTED_RESERVES)) == {}

    # A layout_changed run ingested nothing, so the next run fetches the same bulletin again.
    cbk.pdf_content[newest_url] = cbk.bulletin
    cbk.reset()
    retry = await run(seeded_database, cbk)

    assert retry.status is RunStatus.SUCCEEDED
    assert retry.details["bulletins"] == ["2026-02-06"]
    assert await stored_reserves(engine, list(EXPECTED_RESERVES)) == EXPECTED_RESERVES


@pytest.mark.parametrize(
    ("cell", "reason"),
    [
        pytest.param("(12,384)", f"out_of_range:{RESERVES}:-12384.00", id="parenthesized"),
        pytest.param("bad12384", f"unparsable:{RESERVES}:bad12384", id="leading-text"),
    ],
)
async def test_bulletin_with_a_malformed_value_cell_writes_nothing(
    seeded_database: SeededDatabase, cbk: FakeCbk, cell: str, reason: str
) -> None:
    engine = seeded_database.engine
    cbk.pdf_content[bulletin_url(cbk.links[0])] = bulletin_pdf(
        TITLE_LINE, DATE_ROW, f"{VALUE_LABEL} {cell} 12,341 12,296 12,352 12,387"
    )

    changed = await run(seeded_database, cbk)

    # One rejected cell of the bulletin's five is 20% of the run's rows, above the 10% allowed.
    assert changed.status is RunStatus.LAYOUT_CHANGED
    assert (changed.rows_fetched, changed.rows_written, changed.rows_rejected) == (5, 0, 1)
    assert changed.error_message == "1 of 5 rows rejected, more than 10% of the rows parsed"
    assert [row["reason"] for row in changed.details["rejected"]] == [reason]
    assert (await stored_run(engine, changed)).status == "layout_changed"
    assert await stored_rows(engine, list(EXPECTED_RESERVES)) == {}


# ---------------------------------------------------------------------------------------------
# Ingested-bulletin history
# ---------------------------------------------------------------------------------------------

HISTORY_START = datetime(2026, 2, 6, 15, 15, tzinfo=UTC)
LATER = "2027-01-01"
"""A recorded entry later than every valid ingested bulletin below, so counting it would show."""

HISTORY: tuple[tuple[str, str, Any], ...] = (
    # A backfill holds the newest bulletin, and a later-started default run an older one.
    ("cbk_reserves", "succeeded", {"bulletins": [SECOND_NEWEST.isoformat(), NEWEST.isoformat()]}),
    ("cbk_reserves", "partial", {"bulletins": ["2026-01-23"]}),
    # Runs that ingested nothing, the current run's own running row included.
    *(
        ("cbk_reserves", status, {"bulletins": [LATER]})
        for status in (
            "running",
            "failed",
            "layout_changed",
            "blocked_by_robots",
            "no_new_data",
            "ambiguous_session",
        )
    ),
    ("cbk_fx", "succeeded", {"bulletins": [LATER]}),
    # Ingested runs whose details hold no valid YYYY-MM-DD entry later than NEWEST.
    ("cbk_reserves", "succeeded", {"bulletins": LATER}),
    ("cbk_reserves", "succeeded", {"bulletins": {"date": LATER}}),
    ("cbk_reserves", "succeeded", {"bulletin_urls": [LATER]}),
    ("cbk_reserves", "succeeded", {"bulletins": None}),
    ("cbk_reserves", "succeeded", {"bulletins": [20270101, None, [LATER], {"date": LATER}]}),
    ("cbk_reserves", "partial", {"bulletins": ["not-a-date", "20270101"]}),
    ("cbk_reserves", "partial", {"bulletins": [f" {LATER}", f"{LATER}\n"]}),
    # Well-formed but impossible dates that sort after NEWEST.
    ("cbk_reserves", "succeeded", {"bulletins": ["2099-02-30", "9999-99-99"]}),
    # Details that are not an object: a JSON array and JSON null.
    ("cbk_reserves", "succeeded", [LATER]),
    ("cbk_reserves", "succeeded", None),
)
"""``(pipeline, status, details)`` of the inserted runs, started a day apart in this order."""


async def test_latest_ingested_bulletin_is_aggregated_over_valid_entries_of_ingested_runs(
    seeded_database: SeededDatabase,
) -> None:
    engine = seeded_database.engine
    # The seeded template holds the seeder's own run only.
    async with engine.connect() as conn:
        assert await load_latest_ingested_bulletin(conn) is None

    async with engine.begin() as conn:
        await conn.execute(
            insert(TerminalIngestionRun),
            [
                {
                    "pipeline": pipeline,
                    "trigger": "cli",
                    "started_at": HISTORY_START + timedelta(days=index),
                    "status": status,
                    "details": details,
                }
                for index, (pipeline, status, details) in enumerate(HISTORY)
            ],
        )

    statements: list[str] = []

    def record(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        statements.append(statement)

    event.listen(engine.sync_engine, "after_cursor_execute", record)
    try:
        async with engine.connect() as conn:
            latest = await load_latest_ingested_bulletin(conn)
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", record)

    assert latest == NEWEST
    # Every read of the run history is one ungrouped aggregate, so one row whatever the history
    # size, and no details document is selected.
    reads = [statement for statement in statements if "terminal_ingestion_runs" in statement]
    assert reads
    for statement in reads:
        select_list = statement.split("FROM", 1)[0]
        assert select_list.strip().startswith("SELECT max(")
        assert "details" not in select_list
        assert "GROUP BY" not in statement


# ---------------------------------------------------------------------------------------------
# Live source (deselected by default)
# ---------------------------------------------------------------------------------------------


@pytest.mark.live
async def test_live_newest_bulletin_carries_the_reserves_table(
    make_settings: Callable[..., Any],
) -> None:
    # The real site is fetched with the client's real per-host spacing, as production runs are.
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        links = parse_bulletin_listing(await client.get_text(WEEKLY_BULLETIN_URL))
        assert len(links) >= 1
        pdf = await client.get_bytes(links[0].url)

    records = parse_reserves_bulletin(pdf)

    assert len(records) >= 1
    dates = [record.observation_date for record in records]
    assert all(earlier < later for earlier, later in pairwise(d for d in dates if d is not None))
    # Every published cell is one well-formed number within the bounds.
    assert validate_macro_records(records, BOUNDS).rejected == []
