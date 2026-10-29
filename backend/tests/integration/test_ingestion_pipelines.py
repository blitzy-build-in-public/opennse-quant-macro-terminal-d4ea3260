"""Full ingestion runs against PostgreSQL: NSE statuses, run locks and price/flow provenance.

The only source exercised is the public NSE market-statistics page ("Equity Statistics" tab),
replayed from the recorded fixtures under ``fixtures/nse/`` by :class:`FixtureFetcher` in place of
the Playwright browser session: the rendered page shell with its 13 ``#filtersectors`` options and
the price-list link ``27-OCT-26.pdf``, plus every sector's rendered table. The production checks
(at least 10 sector options, at least 40 rows) therefore run unchanged. Operator CSV files are
loaded with the product's own importer (``import_file``). Nothing reaches the network: respx
serves the single ``robots.txt`` request of each run, and its default ``assert_all_mocked``
rejects any other request.

Every test gets a fresh seeded clone (``seeded_database``). An outer rollback transaction cannot
wrap these runs, because each one holds its own advisory-lock connection and writes its run
record on another. Only ``terminal_`` tables are read, with bound parameters; prices, turnover and
flows are compared as ``Decimal`` and volumes as ``int``.

Covered:

- Statuses: ``succeeded``, ``layout_changed`` (a five-sector page), ``no_new_data`` (stale
  guard), ``ambiguous_session`` and the operator's ``--trade-date`` override,
  ``blocked_by_robots`` and ``failed`` for a year the exchange calendar does not cover.
- The transaction-scoped advisory lock: a second run fails with "lock held" while the first is
  blocked inside its fetch, and the lock is free again after a run whose parser raises and after
  a run cancelled mid-fetch.
- Provenance: a scrape replaces seeded prices and later scraped prices but never CSV-imported
  ones, and never touches ``foreign_net_flow_kes`` or ``flow_source``.
"""

import asyncio
from collections.abc import Callable, Iterator, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Protocol
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.ingestion.base import Pipeline, RunResult, RunStatus, execute_pipeline
from opennse_terminal.ingestion.files.tabular_import import import_file
from opennse_terminal.ingestion.nse.market_statistics import (
    NSE_MARKET_STATISTICS_URL,
    MarketStatisticsPipeline,
    MarketStatisticsSnapshot,
    NseQuote,
    SectorTable,
)
from opennse_terminal.models import TerminalDailyPrice, TerminalIngestionRun

NAIROBI: Final = ZoneInfo("Africa/Nairobi")
NOW: Final = datetime(2026, 10, 28, 7, 30, tzinfo=NAIROBI)
"""The scheduled 07:30 run whose expected session, 2026-10-27, is the fixture's link date."""
SESSION: Final = date(2026, 10, 27)
"""The session the recorded page shows (``27-OCT-26.pdf``)."""
SEEDED_LAST_SESSION: Final = date(2026, 9, 30)
"""The seeder's final synthetic session."""

SLUGS: Final[tuple[str, ...]] = (
    "agric",
    "auto",
    "bank",
    "comm",
    "const",
    "energy",
    "insr",
    "invest",
    "investse",
    "manu",
    "tele",
    "real",
    "exchange",
)
"""The ``#filtersectors`` option values of ``nse/page_shell.html``, in dropdown order; each names
its fragment ``nse/sectors/<slug>.html``."""

SEEDED: Final[dict[str, tuple[str, int, str]]] = {
    "KE1000001402": ("SCOM", 2754329, "33.25"),
    "KE0000000554": ("EQTY", 1888186, "95.75"),
    "KE0000000315": ("KCB", 1186912, "85.00"),
    "KE0000000216": ("EABL", 22494, "269.50"),
    "KE0000000547": ("KEGN", 987240, "10.60"),
    "KE1000001568": ("COOP", 720756, "36.70"),
    "KE0000000067": ("ABSA", 81334, "30.55"),
    "KE0000000091": ("SBIC", 11221, "270.50"),
}
"""ISIN -> (ticker, volume, last traded price) of the eight seeded counters on the fixture page."""
SEEDED_TICKERS: Final = frozenset(ticker for ticker, _volume, _close in SEEDED.values())

ROBOTS_URL: Final = "https://www.nse.co.ke/robots.txt"
ROBOTS_ALLOW: Final = "User-agent: *\nDisallow: /wp-admin/\n"
"""The exchange site's own ``robots.txt``: every path allowed except the CMS admin area."""
ROBOTS_DISALLOW_PAGE: Final = "User-agent: *\nDisallow: /market-statistics/\n"

WAIT_SECONDS: Final = 10.0
"""Bound on every wait for a blocked run, so a lock test never hangs."""

_PRICE_COLUMNS: Final = (
    TerminalDailyPrice.ticker,
    TerminalDailyPrice.open_price,
    TerminalDailyPrice.high_price,
    TerminalDailyPrice.low_price,
    TerminalDailyPrice.close_price,
    TerminalDailyPrice.volume,
    TerminalDailyPrice.turnover_kes,
    TerminalDailyPrice.turnover_estimated,
    TerminalDailyPrice.foreign_net_flow_kes,
    TerminalDailyPrice.source,
    TerminalDailyPrice.flow_source,
    TerminalDailyPrice.ingestion_run_id,
)
"""The price, flow and provenance columns of a :func:`prices_on` snapshot."""
_RUN_COLUMNS: Final = (
    TerminalIngestionRun.id,
    TerminalIngestionRun.pipeline,
    TerminalIngestionRun.trigger,
    TerminalIngestionRun.status,
    TerminalIngestionRun.rows_fetched,
    TerminalIngestionRun.rows_written,
    TerminalIngestionRun.rows_rejected,
    TerminalIngestionRun.error_message,
    TerminalIngestionRun.details,
    TerminalIngestionRun.finished_at,
)
"""The ``terminal_ingestion_runs`` columns :func:`run_row` reads."""
_RUNS_OF_COLUMNS: Final = (
    TerminalIngestionRun.id,
    TerminalIngestionRun.status,
    TerminalIngestionRun.error_message,
    TerminalIngestionRun.finished_at,
)
"""The ``terminal_ingestion_runs`` columns :func:`runs_of` reads."""


class SeededClone(Protocol):
    """The part of the root conftest's ``DatabaseHandle`` these tests use."""

    @property
    def engine(self) -> AsyncEngine:
        """Engine on the clone, owned and disposed by the database factory."""

    @property
    def settings(self) -> Any:
        """The clone's ``TerminalSettings``."""


class FixtureFetcher:
    """Replays the recorded page and sector fragments as one browser session's snapshot.

    Implements the market-statistics ``Fetcher`` protocol. The snapshot is assembled when the
    fetcher is built, so a fixture that breaks its contract (the dropdown's option values must be
    :data:`SLUGS`, in order) fails the test at once rather than surfacing as a ``failed`` run.

    Args:
        load: Reads a fixture by its path under ``fixtures/`` (the ``fixture_text`` fixture).
        slugs: The sector options kept, in dropdown order; the other options are removed from the
            page shell and their tables are not captured.
        transform: Applied to each sector fragment's HTML, for example to change its values.
        entered: Set as soon as a fetch starts.
        release: When given, each fetch waits for it after setting ``entered``; this is where the
            lock and cancellation tests hold a run inside its data transaction.
    """

    def __init__(
        self,
        load: Callable[[str], str],
        *,
        slugs: Sequence[str] = SLUGS,
        transform: Callable[[str], str] | None = None,
        entered: asyncio.Event | None = None,
        release: asyncio.Event | None = None,
    ) -> None:
        unknown = [slug for slug in slugs if slug not in SLUGS]
        if unknown:
            raise ValueError(f"not a recorded sector option: {unknown}")
        shell = load("nse/page_shell.html")
        soup = BeautifulSoup(shell, "lxml")
        dropdown = soup.select_one("#filtersectors")
        assert dropdown is not None, "nse/page_shell.html has no #filtersectors dropdown"
        options = dropdown.find_all("option")
        assert tuple(str(option.get("value")) for option in options) == SLUGS
        labels: dict[str, str] = {}
        for option in options:
            value = str(option.get("value"))
            if value in slugs:
                labels[value] = option.get_text(" ", strip=True)
            else:
                option.decompose()
        page_html = shell if tuple(slugs) == SLUGS else str(soup)
        tables = []
        for slug in SLUGS:
            if slug not in labels:
                continue
            html = load(f"nse/sectors/{slug}.html")
            tables.append(
                SectorTable(
                    option_value=slug,
                    option_label=labels[slug],
                    html=transform(html) if transform is not None else html,
                )
            )
        self._snapshot = MarketStatisticsSnapshot(
            page_html=page_html, sector_tables=tables, fetched_at=NOW
        )
        self._entered = entered
        self._release = release
        self.calls = 0
        self.urls: list[str] = []

    async def fetch(self, url: str, *, user_agent: str) -> MarketStatisticsSnapshot:
        self.calls += 1
        self.urls.append(url)
        if self._entered is not None:
            self._entered.set()
        if self._release is not None:
            await self._release.wait()
        return self._snapshot


class ExplodingParsePipeline(MarketStatisticsPipeline):
    """A market-statistics run whose parser raises inside the locked data transaction."""

    def parse(self, raw: MarketStatisticsSnapshot) -> list[NseQuote]:
        raise RuntimeError("fixture parser failure")


def bump_seeded_volumes(html: str, delta: int = 1000) -> str:
    """``html`` with the Volume cell of every seeded counter's row raised by ``delta`` shares.

    The parser locates ``table.nsetable`` inside the document, so the ``<html><body>`` wrapper
    lxml adds is harmless.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.nsetable")
    assert table is not None, "sector fragment has no table.nsetable"
    for row in table.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) == 5 and cells[1].get_text(strip=True) in SEEDED:
            cells[2].string = str(int(cells[2].get_text(strip=True)) + delta)
    return str(soup)


@pytest.fixture
def robots() -> Iterator[respx.MockRouter]:
    """respx router serving the exchange site's ``robots.txt`` (route name ``"robots"``).

    Tests may answer differently with ``robots.routes["robots"].respond(...)``. Any request
    without a route fails instead of reaching the network.
    """
    with respx.mock(assert_all_called=False) as router:
        router.get(ROBOTS_URL, name="robots").mock(
            return_value=httpx.Response(200, text=ROBOTS_ALLOW)
        )
        yield router


def scrape(
    handle: SeededClone,
    fetcher: FixtureFetcher,
    *,
    trade_date: date | None = None,
    now: datetime = NOW,
) -> MarketStatisticsPipeline:
    """A fresh single-use market-statistics pipeline on the clone's settings."""
    return MarketStatisticsPipeline(
        handle.settings, trade_date=trade_date, fetcher=fetcher, now=lambda: now
    )


async def run(pipeline: Pipeline[Any, Any], handle: SeededClone) -> RunResult:
    """Execute ``pipeline`` on the clone as a CLI run; the run opens its own polite client."""
    return await execute_pipeline(pipeline, handle.engine, trigger="cli")


async def prices_on(handle: SeededClone, day: date) -> dict[str, dict[str, Any]]:
    """Ticker -> the ``terminal_daily_prices`` row of ``day`` (price, flow and provenance
    columns), as plain dicts so snapshots compare by value."""
    statement = (
        select(*_PRICE_COLUMNS)
        .where(TerminalDailyPrice.trade_date == day)
        .order_by(TerminalDailyPrice.ticker)
    )
    async with handle.engine.connect() as conn:
        result = await conn.execute(statement)
        return {row["ticker"]: dict(row) for row in result.mappings()}


async def run_row(handle: SeededClone, run_id: object) -> dict[str, Any]:
    """The recorded ``terminal_ingestion_runs`` row of a run."""
    statement = select(*_RUN_COLUMNS).where(TerminalIngestionRun.id == run_id)
    async with handle.engine.connect() as conn:
        result = await conn.execute(statement)
        return dict(result.mappings().one())


async def runs_of(handle: SeededClone, pipeline: str) -> list[dict[str, Any]]:
    """Every recorded run of ``pipeline``, oldest first."""
    statement = (
        select(*_RUNS_OF_COLUMNS)
        .where(TerminalIngestionRun.pipeline == pipeline)
        .order_by(TerminalIngestionRun.started_at)
    )
    async with handle.engine.connect() as conn:
        result = await conn.execute(statement)
        return [dict(row) for row in result.mappings()]


def write_csv(tmp_path: Path, name: str, header: str, rows: Sequence[str]) -> Path:
    """Write an operator CSV file (header line, then one line per row) and return its path."""
    path = tmp_path / name
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


async def finish_blocked(task: "asyncio.Task[RunResult]", release: asyncio.Event) -> None:
    """Release a blocked run and wait, bounded, until its task is done; a task still running
    after the bound is cancelled. The task's outcome is consumed, so asyncio never reports it."""
    release.set()
    await asyncio.wait({task}, timeout=WAIT_SECONDS)
    if not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=WAIT_SECONDS)
    if task.done() and not task.cancelled():
        task.exception()


def assert_nse_row(row: dict[str, Any], *, volume: int, close: str) -> None:
    """A price row as the market-statistics pipeline writes it: the quoted close and volume,
    OHLC NULL and turnover estimated as volume x close."""
    expected_close = Decimal(close)
    assert row["source"] == "nse_market_statistics"
    assert row["close_price"] == expected_close
    assert row["volume"] == volume
    assert isinstance(row["volume"], int)
    assert (row["open_price"], row["high_price"], row["low_price"]) == (None, None, None)
    assert row["turnover_kes"] == Decimal(volume) * expected_close
    assert row["turnover_estimated"] is True


# --- Full runs and statuses -------------------------------------------------------------------


async def test_full_run_writes_mapped_tickers(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    fetcher = FixtureFetcher(fixture_text)

    result = await run(scrape(seeded_database, fetcher), seeded_database)

    assert result.status is RunStatus.SUCCEEDED, result.error_message
    assert result.pipeline == "nse_market_statistics"
    assert result.rows_written == 8
    assert result.rows_fetched >= 40
    assert result.rows_rejected == 0
    assert result.error_message is None
    assert fetcher.urls == [NSE_MARKET_STATISTICS_URL]
    assert robots.routes["robots"].call_count == 1

    rows = await prices_on(seeded_database, SESSION)
    assert set(rows) == SEEDED_TICKERS
    for ticker, volume, close in SEEDED.values():
        row = rows[ticker]
        assert_nse_row(row, volume=volume, close=close)
        assert row["foreign_net_flow_kes"] is None
        assert row["flow_source"] is None
        assert row["ingestion_run_id"] == result.run_id
    assert rows["SCOM"]["turnover_kes"] == Decimal("91581439.25")
    assert rows["EQTY"]["turnover_kes"] == Decimal("180793809.50")

    details = result.details
    unmapped = {entry["isin"] for entry in details["unmapped_isins"]}
    assert unmapped
    assert unmapped.isdisjoint(SEEDED)
    assert details["link_session"] == "2026-10-27"
    assert details["label_session"] == "2026-10-28"
    assert details["expected_session"] == "2026-10-27"
    assert details["trade_date"] == "2026-10-27"
    assert details["trade_date_override"] is False
    assert details["continuity"]["applies"] is False

    recorded = await run_row(seeded_database, result.run_id)
    assert recorded["pipeline"] == "nse_market_statistics"
    assert recorded["trigger"] == "cli"
    assert recorded["status"] == RunStatus.SUCCEEDED.value
    assert (recorded["rows_fetched"], recorded["rows_written"], recorded["rows_rejected"]) == (
        result.rows_fetched,
        result.rows_written,
        result.rows_rejected,
    )
    assert recorded["finished_at"] is not None
    assert recorded["details"]["trade_date"] == "2026-10-27"
    assert recorded["details"]["unmapped_isins"] == details["unmapped_isins"]


async def test_five_sector_variant_is_layout_changed(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    fetcher = FixtureFetcher(fixture_text, slugs=SLUGS[:5])

    result = await run(scrape(seeded_database, fetcher), seeded_database)

    assert result.status is RunStatus.LAYOUT_CHANGED, result.error_message
    assert result.rows_written == 0
    assert fetcher.calls == 1
    assert "expected" in result.details
    assert result.details["found"] == 5
    assert await prices_on(seeded_database, SESSION) == {}
    recorded = await run_row(seeded_database, result.run_id)
    assert recorded["status"] == RunStatus.LAYOUT_CHANGED.value
    assert recorded["rows_written"] == 0
    assert recorded["details"]["found"] == 5


async def test_unchanged_rerun_is_no_new_data(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    first = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)
    assert first.status is RunStatus.SUCCEEDED, first.error_message
    before = await prices_on(seeded_database, SESSION)

    second = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)

    assert second.status is RunStatus.NO_NEW_DATA, second.error_message
    assert second.rows_written == 0
    assert second.details["stale"] == {"repeated": 8, "mapped": 8}
    after = await prices_on(seeded_database, SESSION)
    assert after == before
    assert {row["ingestion_run_id"] for row in after.values()} == {first.run_id}
    assert (await run_row(seeded_database, second.run_id))["status"] == "no_new_data"


async def test_ambiguous_session_then_trade_date_override(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    day_later = datetime(2026, 10, 29, 7, 30, tzinfo=NAIROBI)

    ambiguous = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text), now=day_later), seeded_database
    )

    assert ambiguous.status is RunStatus.AMBIGUOUS_SESSION, ambiguous.error_message
    assert ambiguous.rows_written == 0
    assert ambiguous.details["expected_session"] == "2026-10-28"
    assert ambiguous.details["link_session"] == "2026-10-27"
    assert ambiguous.details["label_session"] == "2026-10-28"
    assert ambiguous.details["reason"] == "link_date_mismatch"
    assert await prices_on(seeded_database, SESSION) == {}
    assert await prices_on(seeded_database, date(2026, 10, 28)) == {}

    resolved = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text), trade_date=SESSION, now=day_later),
        seeded_database,
    )

    assert resolved.status is RunStatus.SUCCEEDED, resolved.error_message
    assert resolved.rows_written == 8
    assert resolved.details["trade_date_override"] is True
    assert resolved.details["trade_date"] == "2026-10-27"
    rows = await prices_on(seeded_database, SESSION)
    assert set(rows) == SEEDED_TICKERS
    assert all(row["ingestion_run_id"] == resolved.run_id for row in rows.values())


async def test_robots_disallow_blocks_run(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    robots.routes["robots"].respond(200, text=ROBOTS_DISALLOW_PAGE)
    fetcher = FixtureFetcher(fixture_text)

    result = await run(scrape(seeded_database, fetcher), seeded_database)

    assert result.status is RunStatus.BLOCKED_BY_ROBOTS, result.error_message
    assert result.rows_written == 0
    assert result.details["robots"]["reason"] == "disallowed"
    assert fetcher.calls == 0
    assert await prices_on(seeded_database, SESSION) == {}
    assert (await run_row(seeded_database, result.run_id))["status"] == "blocked_by_robots"


async def test_unverified_calendar_year_fails(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    unverified = datetime(2028, 1, 5, 7, 30, tzinfo=NAIROBI)

    result = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text), now=unverified), seeded_database
    )

    assert result.status is RunStatus.FAILED
    assert result.error_message is not None
    assert "not verified for 2028" in result.error_message
    assert result.details["calendar"]["year"] == 2028
    assert 2028 not in result.details["calendar"]["verified_years"]
    assert result.rows_written == 0
    assert await prices_on(seeded_database, SESSION) == {}
    assert await prices_on(seeded_database, date(2028, 1, 4)) == {}


# --- Transaction-scoped advisory lock ---------------------------------------------------------


async def test_second_run_fails_while_lock_held(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    blocked = FixtureFetcher(fixture_text, entered=entered, release=release)
    task = asyncio.create_task(run(scrape(seeded_database, blocked), seeded_database))
    try:
        await asyncio.wait_for(entered.wait(), WAIT_SECONDS)
        contender = FixtureFetcher(fixture_text)

        second = await run(scrape(seeded_database, contender), seeded_database)

        assert second.status is RunStatus.FAILED
        assert second.error_message is not None
        assert "lock held" in second.error_message
        assert second.details["lock_key"] == "terminal:nse_market_statistics"
        assert second.rows_written == 0
        assert contender.calls == 0
        recorded = await run_row(seeded_database, second.run_id)
        assert recorded["status"] == RunStatus.FAILED.value
        assert recorded["error_message"] == "lock held"
        assert not task.done()

        release.set()
        first = await asyncio.wait_for(task, WAIT_SECONDS)
        assert first.status is RunStatus.SUCCEEDED, first.error_message
        assert first.rows_written == 8
        assert blocked.calls == 1
    finally:
        await finish_blocked(task, release)


async def test_lock_released_after_parser_raises(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    exploding = ExplodingParsePipeline(
        seeded_database.settings, fetcher=FixtureFetcher(fixture_text), now=lambda: NOW
    )

    failed = await run(exploding, seeded_database)

    assert failed.status is RunStatus.FAILED
    assert failed.error_message == "RuntimeError: fixture parser failure"
    assert failed.rows_written == 0
    assert await prices_on(seeded_database, SESSION) == {}

    retried = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)

    assert retried.status is RunStatus.SUCCEEDED, retried.error_message
    assert retried.rows_written == 8


async def test_lock_released_after_cancellation(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    blocked = FixtureFetcher(fixture_text, entered=entered, release=release)
    task = asyncio.create_task(run(scrape(seeded_database, blocked), seeded_database))
    try:
        await asyncio.wait_for(entered.wait(), WAIT_SECONDS)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, WAIT_SECONDS)
    finally:
        await finish_blocked(task, release)

    [cancelled] = await runs_of(seeded_database, "nse_market_statistics")
    assert cancelled["status"] == RunStatus.FAILED.value
    assert cancelled["error_message"] == "cancelled"
    assert cancelled["finished_at"] is not None
    assert await prices_on(seeded_database, SESSION) == {}

    retried = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)

    assert retried.status is RunStatus.SUCCEEDED, retried.error_message
    assert retried.rows_written == 8


# --- Price versus flow provenance -------------------------------------------------------------


async def test_seed_then_flow_import_then_scrape(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
    tmp_path: Path,
) -> None:
    flows = write_csv(
        tmp_path,
        "foreign_flows.csv",
        "ticker,trade_date,foreign_net_flow_kes",
        ["SCOM,2026-09-30,-1234567.89"],
    )

    imported = await import_file("foreign_flows", flows, settings=seeded_database.settings)

    assert imported.status is RunStatus.SUCCEEDED, imported.error_message
    assert imported.pipeline == "csv_import:foreign_flows"
    assert imported.rows_written == 1
    seeded = await prices_on(seeded_database, SEEDED_LAST_SESSION)
    assert set(seeded) == SEEDED_TICKERS
    assert seeded["SCOM"]["source"] == "seed"
    assert seeded["SCOM"]["foreign_net_flow_kes"] == Decimal("-1234567.89")
    assert seeded["SCOM"]["flow_source"] == "csv_import"
    for ticker in SEEDED_TICKERS - {"SCOM"}:
        assert seeded[ticker]["source"] == "seed"
        assert seeded[ticker]["flow_source"] == "seed"
        assert seeded[ticker]["foreign_net_flow_kes"] is not None

    scraped = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text), trade_date=SEEDED_LAST_SESSION),
        seeded_database,
    )

    assert scraped.status is RunStatus.SUCCEEDED, scraped.error_message
    assert scraped.rows_written == 8
    rows = await prices_on(seeded_database, SEEDED_LAST_SESSION)
    scom = rows["SCOM"]
    assert_nse_row(scom, volume=2754329, close="33.25")
    assert scom["turnover_kes"] == Decimal("91581439.25")
    assert scom["ingestion_run_id"] == scraped.run_id
    assert scom["foreign_net_flow_kes"] == Decimal("-1234567.89")
    assert scom["flow_source"] == "csv_import"
    for ticker, volume, close in SEEDED.values():
        if ticker == "SCOM":
            continue
        row = rows[ticker]
        assert_nse_row(row, volume=volume, close=close)
        assert row["foreign_net_flow_kes"] == seeded[ticker]["foreign_net_flow_kes"]
        assert row["flow_source"] == "seed"


async def test_scrape_then_flow_import_then_rescrape(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
    tmp_path: Path,
) -> None:
    first = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)
    assert first.status is RunStatus.SUCCEEDED, first.error_message
    flows = write_csv(
        tmp_path,
        "foreign_flows.csv",
        "ticker,trade_date,foreign_net_flow_kes",
        ["SCOM,2026-10-27,5000000.00"],
    )
    imported = await import_file("foreign_flows", flows, settings=seeded_database.settings)
    assert imported.status is RunStatus.SUCCEEDED, imported.error_message
    after_import = (await prices_on(seeded_database, SESSION))["SCOM"]
    assert after_import["source"] == "nse_market_statistics"
    assert after_import["foreign_net_flow_kes"] == Decimal("5000000.00")
    assert after_import["flow_source"] == "csv_import"

    rescraped = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text, transform=bump_seeded_volumes)),
        seeded_database,
    )

    assert rescraped.status is RunStatus.SUCCEEDED, rescraped.error_message
    assert rescraped.rows_written == 8
    rows = await prices_on(seeded_database, SESSION)
    scom = rows["SCOM"]
    assert_nse_row(scom, volume=2755329, close="33.25")
    assert scom["turnover_kes"] == Decimal("91614689.25")
    assert scom["ingestion_run_id"] == rescraped.run_id
    assert scom["foreign_net_flow_kes"] == Decimal("5000000.00")
    assert scom["flow_source"] == "csv_import"
    for ticker, volume, close in SEEDED.values():
        if ticker == "SCOM":
            continue
        assert_nse_row(rows[ticker], volume=volume + 1000, close=close)
        assert rows[ticker]["foreign_net_flow_kes"] is None
        assert rows[ticker]["flow_source"] is None


async def test_csv_imported_prices_survive_scrape(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
    tmp_path: Path,
) -> None:
    prices = write_csv(
        tmp_path,
        "daily_prices.csv",
        "ticker,trade_date,close_price,volume",
        ["SCOM,2026-10-27,34.00,1000"],
    )
    imported = await import_file("daily_prices", prices, settings=seeded_database.settings)
    assert imported.status is RunStatus.SUCCEEDED, imported.error_message
    imported_row = (await prices_on(seeded_database, SESSION))["SCOM"]
    assert imported_row["source"] == "csv_import"

    scraped = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)

    assert scraped.status is RunStatus.SUCCEEDED, scraped.error_message
    assert scraped.rows_written == 7
    rows = await prices_on(seeded_database, SESSION)
    assert set(rows) == SEEDED_TICKERS
    assert rows["SCOM"] == imported_row
    assert rows["SCOM"]["close_price"] == Decimal("34.00")
    assert rows["SCOM"]["volume"] == 1000
    assert_nse_row(rows["EQTY"], volume=1888186, close="95.75")
    assert rows["EQTY"]["ingestion_run_id"] == scraped.run_id
