"""Ingestion pipelines: full runs on PostgreSQL, the pipeline framework and the upsert writers.

The NSE full runs cover statuses, run locks and price/flow provenance. Their only scraped source
is the public NSE market-statistics page ("Equity Statistics" tab), replayed from the
reconstructed 2026-10-27 scenario fixtures under ``fixtures/nse/`` by :class:`FixtureFetcher` in
place of the Playwright browser session: the rendered page shell with its 13 ``#filtersectors``
options and the price-list link ``27-OCT-26.pdf``, plus every sector's rendered table. The
production checks (at least 10 sector options, at least 40 rows) therefore run unchanged.
Operator CSV files are loaded with the product's own importer (``import_file``). Nothing reaches
the network: respx serves the single ``robots.txt`` request of each run, and its default
``assert_all_mocked`` rejects any other request.

Every full-run test gets a fresh seeded clone (``seeded_database``). An outer rollback
transaction cannot wrap these runs, because each one holds its own advisory-lock connection and
writes its run record on another. Only ``terminal_`` tables are read, with bound parameters;
prices, turnover and flows are compared as ``Decimal`` and volumes as ``int``.

Covered:

- Statuses: ``succeeded``, ``layout_changed`` (a five-sector page), ``no_new_data`` (stale
  guard), ``ambiguous_session`` and the operator's ``--trade-date`` override (itself
  ``ambiguous_session`` for a non-session or future date), ``blocked_by_robots``, ``failed`` for
  a year the exchange calendar does not cover, and
  ``partial`` for a row whose turnover, rounded to 0.01, overflows its column.
- The transaction-scoped advisory lock: a second run fails with "lock held" while the first is
  blocked inside its fetch, and the lock is free again after a run whose parser raises and after
  a run cancelled mid-fetch; a run holding it records earlier rows of its pipeline left
  ``running`` as ``failed`` ("abandoned"); and an ``opennse-terminal import`` child process
  stopped by SIGTERM records its run ``failed`` ("cancelled") and exits 143.
- Provenance: a scrape replaces seeded prices and later scraped prices but never CSV-imported
  ones, and never touches ``foreign_net_flow_kes`` or ``flow_source``.
- Consecutive CBK runs through ``registry.run_pipelines`` and the CLI's ``ingest`` loop share one
  per-host request spacing across the run boundary, on recorded CBK fixtures, in their own
  section below.
- The pipeline framework (``ingestion.base``) on engine and connection doubles, the
  ``db.upsert`` writers, and the SQL of the price-precedence guard: each in its own section
  below, whose banner states the contract it holds.
- The ``opennse-terminal`` console script (``cli``), run in-process without a database, in the
  last section below.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import copy
import errno
import gc
import importlib.util
import json
import logging
import os
import re
import runpy
import signal
import sys
import threading
import time as time_module
import uuid
import zlib
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Context, Decimal, localcontext
from enum import Enum
from itertools import pairwise
from pathlib import Path
from types import FrameType, MappingProxyType
from typing import Any, Final, NoReturn, Protocol, cast, get_args
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx
from alembic.script import ScriptDirectory
from alembic.util import CommandError
from bs4 import BeautifulSoup
from sqlalchemy import (
    Boolean,
    Column,
    Executable,
    Insert,
    MetaData,
    Numeric,
    String,
    Table,
    Update,
    Uuid,
    delete,
    func,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.sql import ClauseElement

import opennse_terminal.models.ingestion_runs as ingestion_runs_model
from opennse_terminal import __version__, cli, migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db import upsert as upsert_module
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.db.upsert import (
    FlowUpdateResult,
    UpsertMode,
    update_flows,
    upsert_fundamentals,
    upsert_macro,
    upsert_prices,
    upsert_rows,
)
from opennse_terminal.ingestion import base, registry
from opennse_terminal.ingestion import http as ingestion_http
from opennse_terminal.ingestion.base import (
    MAX_INVALID_FRACTION,
    REJECTED_SAMPLE_LIMIT,
    AmbiguousSession,
    FetchFailed,
    IngestionError,
    LockHeld,
    NoNewData,
    Pipeline,
    RejectedRow,
    RobotsDisallowed,
    RunContext,
    RunRecorder,
    RunResult,
    RunStatus,
    RunTrigger,
    SourceLayoutChanged,
    ValidationOutcome,
    classify_exception,
    dedupe_by_key,
    evaluate_thresholds,
    execute_pipeline,
    normalize_header,
    parse_decimal_text,
    to_jsonable,
)
from opennse_terminal.ingestion.calendar import CalendarNotVerified
from opennse_terminal.ingestion.cbk import CBK_BASE_URL
from opennse_terminal.ingestion.cbk.bonds import TREASURY_BONDS_URL
from opennse_terminal.ingestion.cbk.inflation import INFLATION_URL
from opennse_terminal.ingestion.files import tabular_import
from opennse_terminal.ingestion.files.tabular_import import import_file
from opennse_terminal.ingestion.http import HostSpacing
from opennse_terminal.ingestion.nse.market_statistics import (
    NSE_MARKET_STATISTICS_URL,
    MarketStatisticsPipeline,
    MarketStatisticsSnapshot,
    NseQuote,
    SectorTable,
)
from opennse_terminal.models import (
    TerminalBondYield,
    TerminalCorporateFiling,
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
    TerminalMacroIndicator,
    TerminalWatchlist,
    TerminalWatchlistItem,
)
from opennse_terminal.models.ingestion_runs import RUN_STATUSES, RUN_TRIGGERS

NAIROBI: Final = ZoneInfo("Africa/Nairobi")
NOW: Final = datetime(2026, 10, 28, 7, 30, tzinfo=NAIROBI)
"""The scheduled 07:30 run whose expected session, 2026-10-27, is the fixture's link date."""
SESSION: Final = date(2026, 10, 27)
"""The session the reconstructed scenario fixtures show (``27-OCT-26.pdf``)."""
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
BOUND_VALUE_MARKER: Final = "SECRET-BOUND-VALUE-MARKER"
"""A value bound into a failing statement: only the exception's text could carry it into a run
row."""

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
    """Replays the reconstructed scenario fixtures, the page shell and its sector fragments, as
    one browser session's snapshot.

    Implements the market-statistics ``Fetcher`` protocol. The snapshot is assembled when the
    fetcher is built, so a fixture that breaks its contract (the dropdown's option values must be
    :data:`SLUGS`, in order) fails the test at once rather than surfacing as a ``failed`` run.

    Args:
        load: Reads a fixture by its path under ``fixtures/`` (the ``fixture_text`` fixture).
        slugs: The sector options kept, in dropdown order; the other options are removed from the
            page shell and their tables are left out of the snapshot.
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
            raise ValueError(f"not a sector option of the fixture page: {unknown}")
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


class ConstraintViolatingUpsertPipeline(MarketStatisticsPipeline):
    """A market-statistics run whose upsert writes the session's prices, then executes an INSERT
    that violates ``terminal_equities``' share-count CHECK with :data:`BOUND_VALUE_MARKER` bound,
    on the run's own connection inside the locked data transaction.

    Attributes:
        raised: The ``IntegrityError`` that INSERT raised, kept so a test can confirm its text
            quotes the statement and the marker before checking the run row carries neither.
    """

    raised: IntegrityError | None = None

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[NseQuote], run_id: uuid.UUID
    ) -> int:
        written = await super().upsert(conn, records, run_id)
        try:
            await conn.execute(
                postgresql.insert(TerminalEquity).values(
                    ticker="ZZZZ",
                    company_name=BOUND_VALUE_MARKER,
                    sector="Banking",
                    shares_outstanding=-1,
                    source="csv_import",
                )
            )
        except IntegrityError as error:
            self.raised = error
            raise
        return written


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


def overflow_equity_turnover(html: str) -> str:
    """``html`` with Equity Group's row (``KE0000000554``) quoting 265,371,653 shares at
    37,683,000,000,003.7683 with a change of 0.

    Volume, close and change are each within their own bounds, but the turnover,
    9,999,999,999,999,999,999,999.9999, rounds half up to 10^22, which ``NUMERIC(24,2)`` cannot
    hold. Other fragments keep their values; the ``<html><body>`` wrapper lxml adds is harmless.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.nsetable")
    assert table is not None, "sector fragment has no table.nsetable"
    for row in table.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) == 5 and cells[1].get_text(strip=True) == "KE0000000554":
            cells[2].string = "265371653"
            cells[3].string = "37683000000003.7683"
            cells[4].string = "0"
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


async def finish_blocked(task: asyncio.Task[RunResult], release: asyncio.Event) -> None:
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


def assert_no_statement_text(error_message: str | None, details: Mapping[str, Any]) -> None:
    """Neither a run's ``error_message`` nor its ``details`` (as JSON) carries
    :data:`BOUND_VALUE_MARKER`, a SQL statement or SQLAlchemy's parameter listing."""
    recorded = json.dumps({"error_message": error_message, "details": details}, default=str)
    for leaked in (BOUND_VALUE_MARKER, "INSERT", "[SQL", "[parameters"):
        assert leaked not in recorded


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


@pytest.mark.parametrize(
    ("trade_date", "reason"),
    [
        # A Thursday session, two months after the 2026-10-28 run.
        pytest.param(date(2026, 12, 31), "trade_date_in_future", id="future-session"),
        pytest.param(date(2026, 10, 25), "trade_date_not_a_session", id="sunday"),
        pytest.param(date(2026, 12, 25), "trade_date_not_a_session", id="christmas-day"),
    ],
)
async def test_trade_date_override_the_page_cannot_show_is_ambiguous(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
    trade_date: date,
    reason: str,
) -> None:
    # Written, such a row would move every market endpoint's default as_of to that date.
    result = await run(
        scrape(seeded_database, FixtureFetcher(fixture_text), trade_date=trade_date),
        seeded_database,
    )

    assert result.status is RunStatus.AMBIGUOUS_SESSION, result.error_message
    assert result.error_message == f"ambiguous session: {reason}"
    assert result.rows_written == 0
    details = result.details
    assert details["reason"] == reason
    assert details["trade_date"] == trade_date.isoformat()
    assert details["run_date"] == "2026-10-28"
    assert details["expected_session"] is None
    assert details["trade_date_override"] is True
    assert details["link_session"] == "2026-10-27"
    assert details["label_session"] == "2026-10-28"
    assert await prices_on(seeded_database, trade_date) == {}
    assert await prices_on(seeded_database, SESSION) == {}
    recorded = await run_row(seeded_database, result.run_id)
    assert recorded["status"] == RunStatus.AMBIGUOUS_SESSION.value
    assert recorded["rows_written"] == 0
    assert recorded["details"]["reason"] == reason
    assert recorded["details"]["run_date"] == "2026-10-28"


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


async def test_row_whose_rounded_turnover_overflows_is_rejected_and_the_run_partial(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    # The one row is rejected before the write, so the other seven counters are still written
    # instead of PostgreSQL's numeric overflow rolling back the whole data transaction.
    fetcher = FixtureFetcher(fixture_text, transform=overflow_equity_turnover)

    result = await run(scrape(seeded_database, fetcher), seeded_database)

    assert result.status is RunStatus.PARTIAL, result.error_message
    assert result.error_message is None
    assert result.rows_written == 7
    assert result.rows_rejected == 1
    [rejected] = result.details["rejected"]
    assert rejected["reason"] == "invalid_turnover"
    assert rejected["row"] == {
        "company": "Equity Group Holdings Plc",
        "isin code": "KE0000000554",
        "volume": "265371653",
        "last traded price": "37683000000003.7683",
        "change (%)": "0",
        "sector": "Banking",
    }

    rows = await prices_on(seeded_database, SESSION)
    assert set(rows) == SEEDED_TICKERS - {"EQTY"}
    for ticker, volume, close in SEEDED.values():
        if ticker != "EQTY":
            assert_nse_row(rows[ticker], volume=volume, close=close)
            assert rows[ticker]["ingestion_run_id"] == result.run_id

    recorded = await run_row(seeded_database, result.run_id)
    assert recorded["status"] == RunStatus.PARTIAL.value
    assert (recorded["rows_written"], recorded["rows_rejected"]) == (7, 1)
    assert recorded["details"]["rejected"] == result.details["rejected"]


async def test_database_error_is_recorded_by_its_type_without_its_statement_or_values(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
) -> None:
    """A constraint violation in the upsert fails the run and rolls back the prices already
    written. The run row names the error by its type only: the statement, its bound values and
    PostgreSQL's message, which quotes the failing row, stay out of ``error_message`` and
    ``details``."""
    pipeline = ConstraintViolatingUpsertPipeline(
        seeded_database.settings, fetcher=FixtureFetcher(fixture_text), now=lambda: NOW
    )

    result = await run(pipeline, seeded_database)

    # The error's own text quotes the statement, its parameters and the marker.
    assert pipeline.raised is not None
    raised = str(pipeline.raised)
    assert "INSERT INTO terminal_equities" in raised
    assert "[parameters" in raised
    assert BOUND_VALUE_MARKER in raised.partition("[SQL")[0]
    assert BOUND_VALUE_MARKER in raised.partition("[parameters")[2]

    assert result.status is RunStatus.FAILED
    assert result.error_message == "unexpected_error:IntegrityError"
    assert result.rows_written == 0
    recorded = await run_row(seeded_database, result.run_id)
    assert (recorded["status"], recorded["error_message"]) == (
        RunStatus.FAILED.value,
        "unexpected_error:IntegrityError",
    )
    assert recorded["rows_written"] == 0
    assert recorded["details"]["exception"] == "IntegrityError"
    assert recorded["finished_at"] is not None
    assert_no_statement_text(recorded["error_message"], recorded["details"])
    assert await prices_on(seeded_database, SESSION) == {}


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
    assert failed.error_message == "unexpected_error:RuntimeError"
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


async def test_run_holding_the_lock_finishes_earlier_runs_left_running(
    seeded_database: SeededClone,
    fixture_text: Callable[[str], str],
    robots: respx.MockRouter,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A process killed outright (SIGKILL, out of memory) leaves its run ``running``. The next
    run of that pipeline, once it holds the lock, records such a row ``failed`` ("abandoned")
    with ``finished_at``, naming itself in ``details``; a finished row, a row that started after
    it and another pipeline's running row keep their values."""
    now = datetime.now(UTC)
    rows = {
        "abandoned": ("nse_market_statistics", "running", now - timedelta(hours=1)),
        "finished": ("nse_market_statistics", "failed", now - timedelta(hours=2)),
        "later": ("nse_market_statistics", "running", now + timedelta(hours=1)),
        "other_pipeline": ("cbk_fx", "running", now - timedelta(hours=1)),
    }
    ids = {label: uuid.uuid4() for label in rows}
    async with seeded_database.engine.begin() as conn:
        await conn.execute(
            postgresql.insert(TerminalIngestionRun),
            [
                {
                    "id": ids[label],
                    "pipeline": pipeline,
                    "trigger": "cli",
                    "started_at": started_at,
                    "finished_at": started_at if status != "running" else None,
                    "status": status,
                    "rows_fetched": 3,
                    "rows_written": 0,
                    "rows_rejected": 0,
                    "error_message": "lock held" if status != "running" else None,
                    "details": {},
                }
                for label, (pipeline, status, started_at) in rows.items()
            ],
        )
    before = {label: await run_row(seeded_database, run_id) for label, run_id in ids.items()}
    caplog.set_level(logging.INFO, logger=LOGGER)

    result = await run(scrape(seeded_database, FixtureFetcher(fixture_text)), seeded_database)

    assert result.status is RunStatus.SUCCEEDED, result.error_message
    after = {label: await run_row(seeded_database, run_id) for label, run_id in ids.items()}
    abandoned = after.pop("abandoned")
    assert (abandoned["status"], abandoned["error_message"]) == ("failed", "abandoned")
    assert abandoned["finished_at"] is not None
    assert abandoned["details"] == {"abandoned_by": str(result.run_id)}
    assert abandoned["rows_fetched"] == 3
    assert after == {label: before[label] for label in after}
    assert (await run_row(seeded_database, result.run_id))["status"] == "succeeded"
    assert [record.getMessage() for record in _warnings_and_errors(caplog)] == [
        f"ingestion run {result.run_id} (nse_market_statistics) recorded 1 earlier run(s) left "
        "running as failed (abandoned)"
    ]


# A console-script process whose import blocks inside its locked data transaction: argv is the
# CSV file, a marker file, and the seconds its cancellation takes to unwind. The marker reads
# "locked" once the run holds its lock and "cancelled" once SIGTERM has cancelled it.
_SIGTERM_IMPORT_CHILD = """\
import asyncio
import signal
import sys
from pathlib import Path

from opennse_terminal import cli
from opennse_terminal.ingestion.files import tabular_import

csv_path, marker, unwind_seconds = sys.argv[1], Path(sys.argv[2]), float(sys.argv[3])


async def blocking_fetch(self, ctx):
    marker.write_text("locked")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        marker.write_text("cancelled")
        await asyncio.sleep(unwind_seconds)
        raise


tabular_import.TabularImportPipeline.fetch = blocking_fetch
# A console script starts with SIGTERM at its default, whatever the test runner inherited.
signal.signal(signal.SIGTERM, signal.SIG_DFL)
sys.exit(cli.main(["import", "daily_prices", csv_path]))
"""
_SUBPROCESS_WAIT_SECONDS: Final = 60.0


async def _wait_for_marker(marker: Path, content: str, process: asyncio.subprocess.Process) -> None:
    """Wait, bounded, until the child process has written ``content`` to ``marker``."""
    async with asyncio.timeout(_SUBPROCESS_WAIT_SECONDS):
        while not (marker.exists() and marker.read_text(encoding="utf-8") == content):
            assert process.returncode is None, f"the child exited {process.returncode} early"
            await asyncio.sleep(0.05)


@pytest.mark.parametrize(
    ("signals", "unwind_seconds"),
    [pytest.param(1, "0", id="sigterm"), pytest.param(2, "1.0", id="sigterm-repeated")],
)
async def test_sigterm_to_a_cli_import_records_the_run_cancelled_and_exits_143(
    seeded_database: SeededClone, tmp_path: Path, signals: int, unwind_seconds: str
) -> None:
    """``opennse-terminal import`` stopped by SIGTERM while it holds its lock: the run is rolled
    back and recorded ``failed`` ("cancelled") with ``finished_at``, one WARNING is logged and
    the process exits 143. A second SIGTERM while the run unwinds is logged and ignored. The
    signal goes to a child process only, never to the test runner."""
    csv_path = write_csv(
        tmp_path,
        "prices.csv",
        "ticker,trade_date,close_price,volume",
        ["SCOM,2026-10-27,30.00,100"],
    )
    marker = tmp_path / "marker"
    env = {**os.environ, "TERMINAL_DATABASE_URL": seeded_database.settings.database_url}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _SIGTERM_IMPORT_CHILD,
        str(csv_path),
        str(marker),
        unwind_seconds,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        cwd=tmp_path,
    )
    try:
        await _wait_for_marker(marker, "locked", process)
        process.send_signal(signal.SIGTERM)
        if signals == 2:
            await _wait_for_marker(marker, "cancelled", process)
            process.send_signal(signal.SIGTERM)
        _stdout, stderr_bytes = await asyncio.wait_for(
            process.communicate(), _SUBPROCESS_WAIT_SECONDS
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    stderr = stderr_bytes.decode()
    assert process.returncode == cli.EXIT_TERMINATED, stderr
    warnings = [line for line in stderr.splitlines() if " WARNING " in line]
    assert len(warnings) == 1, stderr
    assert warnings[0].endswith(
        " WARNING opennse_terminal.cli terminated (SIGTERM); uncommitted changes were rolled back"
    )
    assert ("received SIGTERM again" in stderr) == (signals == 2)
    assert "Traceback" not in stderr
    [cancelled] = await runs_of(seeded_database, "csv_import:daily_prices")
    assert (cancelled["status"], cancelled["error_message"]) == ("failed", "cancelled")
    assert cancelled["finished_at"] is not None
    assert await prices_on(seeded_database, SESSION) == {}


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


# ---------------------------------------------------------------------------------------------
# Consecutive runs (``opennse_terminal.ingestion.registry`` and ``opennse_terminal.cli``)
#
# Tests for consecutive pipeline runs through the ingestion registry
# (``opennse_terminal.ingestion.registry.run_pipelines``, the scheduler's batch entry point) and
# the CLI's ``ingest`` loop (``opennse_terminal.cli._ingest``, which ``ingest cbk`` runs).
#
# Each run of a batch opens a ``PoliteClient`` of its own, so its httpx client, cookie jar and
# ``robots.txt`` cache are the run's alone, and every run of the batch shares one ``HostSpacing``,
# so requests to the CBK host start at least ``scraper_min_interval_seconds`` (2.0 s) apart
# across the run boundaries as well as within each run, however quickly the previous run ended.
#
# - **Runs.** ``cbk_inflation`` then ``cbk_bonds``, against a fresh seeded clone per test (the
#   root conftest's ``seeded_database``, which marks the tests ``integration``); the registry and
#   the CLI build their own engines from ``seeded_database.settings``. respx serves CBK's
#   ``robots.txt`` and the recorded full-run fixtures ``fixtures/cbk/inflation_rates.html`` and
#   ``fixtures/cbk/treasury_bonds.html`` at their real URLs with ``assert_all_mocked=True``, so
#   nothing reaches the network. The first run either succeeds or ends at once, blocked by a
#   ``robots.txt`` that disallows every URL and is served to the first ``robots.txt`` request
#   only.
# - **Clock.** ``ingestion.http.HostSpacing`` is replaced by a factory that builds the real class
#   on a fake monotonic clock that only a fake sleep advances, so no test really waits; respx
#   records every request start on that clock.
#
# Expected values are literals: the run statuses and their CLI exit codes (``succeeded`` 0,
# ``blocked_by_robots`` 4) and the 2.0 s spacing of the hermetic test settings.
# ---------------------------------------------------------------------------------------------


NAMES = ("cbk_inflation", "cbk_bonds")
"""Two adjacent CBK pipelines, in run order; both read the one CBK host."""

CBK_ROBOTS_URL = f"{CBK_BASE_URL}/robots.txt"
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
"""CBK's own ``robots.txt``: every path allowed except the CMS admin area."""
DISALLOW_ALL = "User-agent: *\nDisallow: /\n"
"""A ``robots.txt`` that blocks every URL of the site."""

PAGES: Mapping[str, str] = {
    INFLATION_URL: "cbk/inflation_rates.html",
    TREASURY_BONDS_URL: "cbk/treasury_bonds.html",
}
"""The page each pipeline fetches, and the recorded full-run fixture served at it."""

SPACING_SECONDS = 2.0


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
class FakeSpacing:
    """The fake clock and sleep every ``HostSpacing`` of a test runs on, and each one built."""

    clock: FakeClock
    sleep: FakeSleep
    built: list[HostSpacing]


@dataclass(frozen=True, slots=True)
class SentRequest:
    """One request that reached the mocked CBK site, and the fake time it started at."""

    at: float
    url: str


class RecordedSite:
    """The respx side effect of every mocked CBK URL: records each request's start on the fake
    clock, serves ``first_robots`` to the first ``robots.txt`` request and :data:`ALLOW_ALL` to
    every later one, and the recorded fixture bytes at each page URL."""

    def __init__(self, clock: FakeClock, pages: Mapping[str, bytes], first_robots: str) -> None:
        self.sent: list[SentRequest] = []
        self._clock = clock
        self._pages = pages
        self._first_robots = first_robots

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        robots_served = self.urls().count(CBK_ROBOTS_URL)
        self.sent.append(SentRequest(self._clock(), url))
        if url == CBK_ROBOTS_URL:
            return httpx.Response(200, text=ALLOW_ALL if robots_served else self._first_robots)
        return httpx.Response(
            200, content=self._pages[url], headers={"Content-Type": "text/html; charset=utf-8"}
        )

    def urls(self) -> list[str]:
        return [sent.url for sent in self.sent]

    def starts_by_host(self) -> dict[str, list[float]]:
        """Every request start, in order, grouped by the host requested."""
        starts: defaultdict[str, list[float]] = defaultdict(list)
        for sent in self.sent:
            starts[httpx.URL(sent.url).host].append(sent.at)
        return dict(starts)


@pytest.fixture
def fake_spacing(monkeypatch: pytest.MonkeyPatch) -> FakeSpacing:
    """Replace ``ingestion.http.HostSpacing`` with a factory that builds the real class on one
    fake clock and sleep, whatever its caller passes, and records every spacing it builds."""
    clock = FakeClock()
    sleep = FakeSleep(clock)
    built: list[HostSpacing] = []

    def build(**_caller_options: object) -> HostSpacing:
        spacing = HostSpacing(clock=clock, sleep=sleep)
        built.append(spacing)
        return spacing

    monkeypatch.setattr(ingestion_http, "HostSpacing", build)
    return FakeSpacing(clock=clock, sleep=sleep, built=built)


@pytest.mark.parametrize(
    ("first_robots", "first_status", "first_exit_code", "requested"),
    [
        pytest.param(
            ALLOW_ALL,
            RunStatus.SUCCEEDED,
            0,
            [CBK_ROBOTS_URL, INFLATION_URL, CBK_ROBOTS_URL, TREASURY_BONDS_URL],
            id="first-run-succeeds",
        ),
        pytest.param(
            DISALLOW_ALL,
            RunStatus.BLOCKED_BY_ROBOTS,
            4,
            [CBK_ROBOTS_URL, CBK_ROBOTS_URL, TREASURY_BONDS_URL],
            id="first-run-blocked-by-robots",
        ),
    ],
)
@pytest.mark.parametrize("entry_point", ["registry.run_pipelines", "cli._ingest"])
async def test_consecutive_runs_stay_spaced_per_host_across_the_run_boundary(
    entry_point: str,
    first_robots: str,
    first_status: RunStatus,
    first_exit_code: int,
    requested: list[str],
    seeded_database: _Clone,
    fake_spacing: FakeSpacing,
    fixture_bytes: Callable[[str], bytes],
) -> None:
    settings = seeded_database.settings
    site = RecordedSite(
        fake_spacing.clock, {url: fixture_bytes(path) for url, path in PAGES.items()}, first_robots
    )
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        for url in (CBK_ROBOTS_URL, *PAGES):
            router.get(url).mock(side_effect=site)
        if entry_point == "registry.run_pipelines":
            results = await registry.run_pipelines(NAMES, settings, trigger="schedule")
            assert [(result.pipeline, result.status) for result in results] == [
                ("cbk_inflation", first_status),
                ("cbk_bonds", RunStatus.SUCCEEDED),
            ]
        else:
            outcomes = await cli._ingest(NAMES, settings, trade_date=None, limit=None)
            assert outcomes == [("cbk_inflation", first_exit_code), ("cbk_bonds", 0)]

    # Each run read robots.txt itself, so the second run was judged by the file it fetched.
    assert site.urls() == requested
    assert site.urls().count(CBK_ROBOTS_URL) == len(NAMES)
    interval = settings.scraper_min_interval_seconds
    assert SPACING_SECONDS - 1e-9 <= interval <= SPACING_SECONDS + 1e-9
    for starts in site.starts_by_host().values():
        for earlier, later in pairwise(starts):
            assert later - earlier >= interval
    boundary = requested.index(CBK_ROBOTS_URL, 1)
    assert site.sent[boundary].at - site.sent[boundary - 1].at >= interval
    # The fake clock stands still between requests, so every request after the first, the next
    # run's robots.txt included, waited the full interval.
    assert len(fake_spacing.sleep.delays) == len(requested) - 1
    assert all(interval - 1e-9 <= delay <= interval + 1e-9 for delay in fake_spacing.sleep.delays)
    # One spacing for the whole batch, on the fake clock: no run built one of its own.
    assert len(fake_spacing.built) == 1


# ---------------------------------------------------------------------------------------------
# Pipeline framework (``opennse_terminal.ingestion.base``)
#
# The pure contract is tested directly: run statuses and triggers, the exceptions that end a run
# with a specific status and their classification, the record and result types, the shared row
# thresholds and the header, decimal, dedupe and JSON helpers. The run lifecycle of ``RunRecorder``
# and ``execute_pipeline`` runs against an in-memory engine double that records every statement and
# whether each transaction committed or rolled back, so no database, network or filesystem is
# touched. PostgreSQL behaviour itself (the advisory lock, JSONB, CHECK constraints) is left to the
# PostgreSQL-backed tests, such as the full runs above.
#
# Expected values are literals taken from the module's docstrings and the run-status table, never
# recomputed with the code under test. Numbers are ``Decimal`` throughout.
# ---------------------------------------------------------------------------------------------


LOGGER = "opennse_terminal.ingestion.base"
RUN_ID = uuid.UUID("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7")
SOURCE_URL = "https://www.centralbank.go.ke/statistics/interest-rates/"
CSV_URL = "https://www.centralbank.go.ke/uploads/interest_rates/1715_Central Bank Rates.csv"
NSE_URL = "https://www.nse.co.ke/market-statistics/"
CALENDAR_MESSAGE = (
    "exchange calendar not verified for 2028; verified years: 2026, 2027 — add the year's "
    "gazetted closures to ingestion/data/nse_closures.csv"
)
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtext('terminal:' || :pipeline))"
IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '30min'"

RUN_RECORD = "run_record"
DATA = "data"
COMMITTED = "committed"
ROLLED_BACK = "rolled back"


@pytest.fixture
def settings() -> TerminalSettings:
    """Explicit settings: no ``.env`` file, and a database URL nothing ever connects to."""
    return TerminalSettings(
        _env_file=None, database_url="postgresql+asyncpg://localhost:1/never_connected"
    )


# --- Engine, connection and HTTP doubles ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Executed:
    """One statement a connection double executed, with its bound parameters."""

    statement: Any
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ResultDouble:
    """The two result attributes the framework reads: ``rowcount`` and ``scalar_one()``."""

    rowcount: int = 1
    scalar: object = None

    def scalar_one(self) -> object:
        return self.scalar


class _TransactionDouble:
    """Stands in for ``AsyncTransaction``: ``commit()`` settles the outcome explicitly.

    ``engine.commit_started`` is set when a commit begins; ``engine.commit_release``, when given,
    holds the commit until it is set, and ``engine.fail_commit`` makes it raise.
    """

    def __init__(self, conn: _ConnectionDouble) -> None:
        self._conn = conn

    async def commit(self) -> None:
        engine = self._conn._engine
        engine.commit_started.set()
        if engine.commit_release is not None:
            await engine.commit_release.wait()
        if engine.fail_commit:
            raise RuntimeError("commit failed")
        self._conn.outcome = COMMITTED


@asynccontextmanager
async def _transaction(conn: _ConnectionDouble) -> AsyncIterator[_TransactionDouble]:
    """Commit on a clean exit and roll back on any exception, as SQLAlchemy's ``begin()`` does;
    after an explicit ``commit()``, leaving the block changes nothing."""
    try:
        yield _TransactionDouble(conn)
    except BaseException:
        if conn.outcome is None:
            conn.outcome = ROLLED_BACK
        raise
    if conn.outcome is None:
        conn.outcome = COMMITTED


def _is_abandon_update(statement: Any) -> bool:
    """Whether ``statement`` is ``RunRecorder.mark_abandoned``'s update: the one run-row update
    whose ``WHERE`` tests ``status``, where a run's final update matches only its id."""
    return (
        isinstance(statement, Update)
        and statement.whereclause is not None
        and "terminal_ingestion_runs.status = " in str(statement.whereclause)
    )


class _ConnectionDouble:
    """Stands in for ``AsyncConnection``: records statements and its transaction's outcome."""

    def __init__(self, engine: _EngineDouble, kind: str) -> None:
        self._engine = engine
        self.kind = kind
        self.executed: list[_Executed] = []
        self.outcome: str | None = None
        self.closed = False

    async def execute(
        self, statement: Any, parameters: Mapping[str, Any] | None = None
    ) -> _ResultDouble:
        params = dict(statement.compile().params)
        params.update(parameters or {})
        self.executed.append(_Executed(statement, params))
        if isinstance(statement, Insert) and self._engine.insert_release is not None:
            self._engine.insert_started.set()
            await self._engine.insert_release.wait()
        if (
            isinstance(statement, Insert)
            and self.kind == RUN_RECORD
            and self._engine.insert_error is not None
        ):
            raise self._engine.insert_error
        if _is_abandon_update(statement):
            if self._engine.abandon_error is not None:
                raise self._engine.abandon_error
            return _ResultDouble(rowcount=self._engine.abandoned_rowcount)
        if isinstance(statement, Update):
            self._engine.update_started.set()
            if self._engine.update_release is not None:
                await self._engine.update_release.wait()
            if self._engine.fail_run_updates:
                raise RuntimeError("run row update failed")
            return _ResultDouble(rowcount=self._engine.update_rowcount)
        if str(statement) == LOCK_SQL:
            return _ResultDouble(scalar=self._engine.lock_available)
        return _ResultDouble()

    def begin(self) -> AbstractAsyncContextManager[_TransactionDouble]:
        return _transaction(self)


class _EngineDouble:
    """Stands in for ``AsyncEngine``.

    ``begin()`` checks out a run-record connection inside its own transaction; ``connect()``
    checks out the run's dedicated data connection, whose transaction the caller begins.

    ``insert_release``, ``update_release`` and ``commit_release``, when given, hold the run row's
    insert, its final update and the data commit until they are set; the matching ``*_started``
    event is set when each begins. ``insert_error``, when given, is raised by the run row's
    insert once any hold is released.

    The update that finishes earlier runs left ``running`` (``RunRecorder.mark_abandoned``) is
    told apart from a run's final update by its ``status`` condition, and kept out of
    ``updates`` and of the final update's hold and failure settings: it reports
    ``abandoned_rowcount`` rows, or raises ``abandon_error`` when given.
    """

    def __init__(
        self,
        *,
        lock_available: bool = True,
        update_rowcount: int = 1,
        fail_run_updates: bool = False,
        fail_commit: bool = False,
        insert_error: BaseException | None = None,
        hold_insert: bool = False,
        hold_update: bool = False,
        hold_commit: bool = False,
        abandoned_rowcount: int = 0,
        abandon_error: BaseException | None = None,
    ) -> None:
        self.lock_available = lock_available
        self.update_rowcount = update_rowcount
        self.fail_run_updates = fail_run_updates
        self.fail_commit = fail_commit
        self.insert_error = insert_error
        self.abandoned_rowcount = abandoned_rowcount
        self.abandon_error = abandon_error
        self.insert_started = asyncio.Event()
        self.update_started = asyncio.Event()
        self.commit_started = asyncio.Event()
        self.insert_release = asyncio.Event() if hold_insert else None
        self.update_release = asyncio.Event() if hold_update else None
        self.commit_release = asyncio.Event() if hold_commit else None
        self.connections: list[_ConnectionDouble] = []

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[_ConnectionDouble]:
        conn = _ConnectionDouble(self, RUN_RECORD)
        self.connections.append(conn)
        async with _transaction(conn):
            yield conn

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_ConnectionDouble]:
        conn = _ConnectionDouble(self, DATA)
        self.connections.append(conn)
        try:
            yield conn
        finally:
            conn.closed = True

    def _run_record_statements(self, kind: type[Any]) -> list[_Executed]:
        return [
            executed
            for conn in self.connections
            if conn.kind == RUN_RECORD
            for executed in conn.executed
            if isinstance(executed.statement, kind)
        ]

    @property
    def inserts(self) -> list[_Executed]:
        return self._run_record_statements(Insert)

    @property
    def updates(self) -> list[_Executed]:
        """The run rows' final updates, without the abandoned-run update."""
        return [
            executed
            for executed in self._run_record_statements(Update)
            if not _is_abandon_update(executed.statement)
        ]

    @property
    def abandon_updates(self) -> list[_Executed]:
        """The updates of ``RunRecorder.mark_abandoned``, one per run that took the lock."""
        return [
            executed
            for executed in self._run_record_statements(Update)
            if _is_abandon_update(executed.statement)
        ]

    @property
    def data_connections(self) -> list[_ConnectionDouble]:
        return [conn for conn in self.connections if conn.kind == DATA]


class _HttpDouble:
    """Stands in for an injected ``PoliteClient``, which the executor never opens nor closes."""

    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> _HttpDouble:
        self.entered = True
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


# --- Pipeline doubles -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Quote:
    index: int
    cell: str
    value: Decimal | None


class _QuotePipeline(Pipeline[list[str], _Quote]):
    """A minimal concrete pipeline: each fetched cell is one value, and a cell that is not a
    number is rejected as ``invalid_decimal``."""

    name = "test_quotes"

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        cells: Sequence[str] = (),
        source_url: str | None = None,
        discovered_url: str | None = None,
        fetch_error: BaseException | None = None,
        upsert_error: BaseException | None = None,
    ) -> None:
        super().__init__(settings)
        if source_url is not None:
            self.source_url = source_url
        self.cells = list(cells)
        self.discovered_url = discovered_url
        self.fetch_error = fetch_error
        self.upsert_error = upsert_error
        self.contexts: list[RunContext] = []
        self.upserts: list[tuple[AsyncConnection, list[_Quote], uuid.UUID]] = []

    async def fetch(self, ctx: RunContext) -> list[str]:
        self.contexts.append(ctx)
        if self.source_url is not None:
            ctx.details["source_url"] = self.source_url
        if self.discovered_url is not None:
            self.source_url = self.discovered_url
        if self.fetch_error is not None:
            raise self.fetch_error
        return list(self.cells)

    def parse(self, raw: list[str]) -> list[_Quote]:
        return [_Quote(index, cell, parse_decimal_text(cell)) for index, cell in enumerate(raw)]

    def validate(self, records: Sequence[_Quote]) -> ValidationOutcome[_Quote]:
        return ValidationOutcome(
            valid=[record for record in records if record.value is not None],
            rejected=[
                RejectedRow(index=record.index, reason="invalid_decimal", row={"cell": record.cell})
                for record in records
                if record.value is None
            ],
        )

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[_Quote], run_id: uuid.UUID
    ) -> int:
        if self.upsert_error is not None:
            raise self.upsert_error
        self.upserts.append((conn, list(records), run_id))
        return len(records)


class _MinRowsPipeline(_QuotePipeline):
    name = "test_min_rows"
    min_rows = 5


class _ReconcilingPipeline(_QuotePipeline):
    """Rejects the rows at ``unknown`` indexes in :meth:`reconcile`, or raises there."""

    name = "test_reconciling"

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        unknown: Sequence[int] = (),
        reconcile_error: BaseException | None = None,
        **options: Any,
    ) -> None:
        super().__init__(settings, **options)
        self.unknown = tuple(unknown)
        self.reconcile_error = reconcile_error
        self.reconciled: list[_Quote] = []

    async def reconcile(self, ctx: RunContext, records: list[_Quote]) -> ValidationOutcome[_Quote]:
        self.reconciled = list(records)
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return ValidationOutcome(
            valid=[record for record in records if record.index not in self.unknown],
            rejected=[
                RejectedRow(index=record.index, reason="unknown_ticker", row={"cell": record.cell})
                for record in records
                if record.index in self.unknown
            ],
        )


class _WriteRejectingPipeline(_QuotePipeline):
    """Its upsert leaves the rows at ``unwritable`` indexes unwritten and uncounted, and reports
    them through :meth:`write_rejections`, as a write-time conflict would."""

    name = "test_write_rejecting"

    def __init__(
        self, settings: TerminalSettings, *, unwritable: Sequence[int] = (), **options: Any
    ) -> None:
        super().__init__(settings, **options)
        self.unwritable = tuple(unwritable)
        self.unwritten: list[RejectedRow] = []

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[_Quote], run_id: uuid.UUID
    ) -> int:
        self.upserts.append((conn, list(records), run_id))
        self.unwritten = [
            RejectedRow(index=record.index, reason="currency_conflict", row={"cell": record.cell})
            for record in records
            if record.index in self.unwritable
        ]
        return len(records) - len(self.unwritten)

    def write_rejections(self) -> Sequence[RejectedRow]:
        return self.unwritten


class _CsvImportPipeline(_QuotePipeline):
    """Overrides ``run_name`` the way the tabular importer does (``csv_import:<kind>``)."""

    name = "csv_import"

    @property
    def run_name(self) -> str:
        return "csv_import:daily_prices"


class _BlockingPipeline(_QuotePipeline):
    """Its fetch blocks until the run's task is cancelled."""

    name = "test_blocking"

    def __init__(self, settings: TerminalSettings, **options: Any) -> None:
        super().__init__(settings, **options)
        self.fetch_started = asyncio.Event()

    async def fetch(self, ctx: RunContext) -> list[str]:
        self.contexts.append(ctx)
        if self.source_url is not None:
            ctx.details["source_url"] = self.source_url
        self.fetch_started.set()
        never_released = asyncio.Event()
        await never_released.wait()
        return list(self.cells)


class _GatedStagePipeline(_QuotePipeline):
    """Records the thread each synchronous stage runs in; the ``gate`` stage (``parse`` or
    ``validate``) then sets ``entered`` on the event loop that built the pipeline, and blocks its
    thread until the test sets ``release``."""

    name = "test_gated"

    def __init__(self, settings: TerminalSettings, *, gate: str, **options: Any) -> None:
        super().__init__(settings, **options)
        self.gate = gate
        self._loop = asyncio.get_running_loop()
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self.threads: dict[str, int] = {}
        self.finished: list[str] = []

    def _enter(self, stage: str) -> None:
        self.threads[stage] = threading.get_ident()
        if stage == self.gate:
            # asyncio.Event is not thread-safe, so it is set on its loop, where the test awaits it
            # alongside the run's task.
            self._loop.call_soon_threadsafe(self.entered.set)
            # Bounded, so a stage run on the event-loop thread fails the test instead of hanging.
            if not self.release.wait(timeout=10):
                raise TimeoutError(f"{stage} was never released")

    def parse(self, raw: list[str]) -> list[_Quote]:
        self._enter("parse")
        records = super().parse(raw)
        self.finished.append("parse")
        return records

    def validate(self, records: Sequence[_Quote]) -> ValidationOutcome[_Quote]:
        self._enter("validate")
        outcome = super().validate(records)
        self.finished.append("validate")
        return outcome


class _ClosingClient:
    """Stands in for the ``PoliteClient`` a run opens for itself. Closing it raises
    ``close_error``, or, without one, waits until the run's task is cancelled."""

    def __init__(self, settings: TerminalSettings, close_error: BaseException | None) -> None:
        self.settings = settings
        self.close_error = close_error
        self.closing = asyncio.Event()

    async def __aenter__(self) -> _ClosingClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closing.set()
        if self.close_error is not None:
            raise self.close_error
        await asyncio.Event().wait()


def _open_closing_clients(
    monkeypatch: pytest.MonkeyPatch, close_error: BaseException | None
) -> list[_ClosingClient]:
    """Make every run open a :class:`_ClosingClient` in place of a ``PoliteClient``, and return
    the list each opened client is appended to."""
    clients: list[_ClosingClient] = []

    def open_client(
        client_settings: TerminalSettings, *, spacing: ingestion_http.HostSpacing | None = None
    ) -> _ClosingClient:
        client = _ClosingClient(client_settings, close_error)
        clients.append(client)
        return client

    monkeypatch.setattr(ingestion_http, "PoliteClient", open_client)
    return clients


def _other_tasks() -> set[asyncio.Task[Any]]:
    """Tasks of the running loop other than the test's own: a finished run leaves none."""
    return asyncio.all_tasks() - {cast(asyncio.Task[Any], asyncio.current_task())}


def _run_ended_early(run: asyncio.Task[Any], what: str) -> NoReturn:
    """Fail the test because the finished ``run`` ended before ``what``, naming its outcome: the
    exception it raised (also the failure's cause), or that it ended cancelled or returned."""
    if run.cancelled():
        pytest.fail(f"the run ended cancelled before {what}")
    error = run.exception()
    if error is not None:
        raise AssertionError(f"the run raised {error!r} before {what}") from error
    pytest.fail(f"the run returned {run.result()!r} before {what}")


async def _stage_reached(stage: asyncio.Event, run: asyncio.Task[Any], what: str) -> None:
    """Wait until ``stage`` is set, which ``run`` does on reaching ``what``.

    The wait is bounded by ``WAIT_SECONDS`` and watches the run: a run that ends before setting
    ``stage`` fails the test at once with its own outcome (:func:`_run_ended_early`). The
    waiter's task is cancelled and awaited before this returns or raises, so it never outlives
    the call.
    """
    if stage.is_set():
        return
    waiter = asyncio.create_task(stage.wait())
    try:
        await asyncio.wait({waiter, run}, timeout=WAIT_SECONDS, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
        await asyncio.wait({waiter})
    if stage.is_set():
        return
    if run.done():
        _run_ended_early(run, what)
    pytest.fail(f"the run did not reach {what} within {WAIT_SECONDS:g} s")


async def _run_result(run: asyncio.Task[RunResult]) -> RunResult:
    """``run``'s result once it is done, or the exception it ended with raised
    (``CancelledError`` when it ended cancelled), as ``await run`` gives them.

    The wait is bounded by ``WAIT_SECONDS`` and never cancels ``run``: a run absorbs a
    cancellation while one of its steps finishes, so cancelling a stuck run would not end it.
    """
    await asyncio.wait({run}, timeout=WAIT_SECONDS)
    if not run.done():
        pytest.fail(f"the run did not finish within {WAIT_SECONDS:g} s")
    return run.result()


async def _finish_run(
    run: asyncio.Task[Any],
    engine: _EngineDouble,
    pipeline: Pipeline[Any, Any],
    *,
    before: set[asyncio.Task[Any]],
) -> None:
    """End whatever ``run`` still has in progress, however far the test got: the cleanup of a
    test that started it, run from its ``finally``.

    Cancelling a run cannot end a step it waits for through cancellation (the ``running``
    insert, the commit, the final update, a ``parse`` or ``validate`` thread), so every gate
    the test holds is released first: the engine's insert, update and commit holds and a gated
    stage's ``release``. A run still pending is then cancelled at once, since some (a blocked
    fetch, a client close that never returns) end only that way, and waited for, bounded by
    ``WAIT_SECONDS``: a failing test reports promptly, and a run its test already awaited is
    left as it ended. The tasks it left behind, not in ``before`` (shielded steps,
    worker-thread stages), are waited for too, so no stage thread is still running. Each
    finished task's outcome is consumed, so asyncio never reports it, and nothing is raised, so
    a failing test reports its own failure.
    """
    for hold in (engine.insert_release, engine.update_release, engine.commit_release):
        if hold is not None:
            hold.set()
    if isinstance(pipeline, _GatedStagePipeline):
        pipeline.release.set()
    if not run.done():
        run.cancel()
        await asyncio.wait({run}, timeout=WAIT_SECONDS)
    left_behind = _other_tasks() - before
    if left_behind:
        await asyncio.wait(left_behind, timeout=WAIT_SECONDS)
    for task in (run, *left_behind):
        if task.done() and not task.cancelled():
            task.exception()


async def _execute(
    pipeline: Pipeline[Any, Any],
    engine: _EngineDouble,
    http: _HttpDouble,
    *,
    trigger: RunTrigger = "cli",
) -> RunResult:
    return await execute_pipeline(
        pipeline, cast(AsyncEngine, engine), trigger=trigger, http=cast(Any, http)
    )


def _pop_duration(details: dict[str, Any]) -> Decimal:
    """Remove ``duration_seconds`` and check it: a JSON string of seconds to 3 decimal places."""
    raw = details.pop("duration_seconds")
    assert isinstance(raw, str)
    duration = Decimal(raw)
    assert duration.as_tuple().exponent == -3
    assert duration >= 0
    return duration


def _warnings_and_errors(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == LOGGER and record.levelno >= logging.WARNING
    ]


# --- Statuses, triggers, constants and the public surface -----------------------------------


def test_run_status_values_follow_the_run_status_table() -> None:
    assert [status.value for status in RunStatus] == [
        "running",
        "succeeded",
        "partial",
        "failed",
        "layout_changed",
        "blocked_by_robots",
        "no_new_data",
        "ambiguous_session",
    ]
    assert tuple(RunStatus) == RUN_STATUSES
    # A StrEnum compares and formats as its value, as the CHECK constraint and the CLI expect.
    assert RunStatus.PARTIAL == "partial"
    assert f"{RunStatus.LAYOUT_CHANGED}" == "layout_changed"


def test_run_trigger_matches_the_model_triggers() -> None:
    assert get_args(RunTrigger) == ("schedule", "cli", "api", "seed")
    assert get_args(RunTrigger) == RUN_TRIGGERS


def test_thresholds_are_exact_decimal_constants() -> None:
    assert isinstance(MAX_INVALID_FRACTION, Decimal)
    assert str(MAX_INVALID_FRACTION) == "0.10"
    assert REJECTED_SAMPLE_LIMIT == 50


def test_public_surface_is_exported() -> None:
    assert set(base.__all__) == {
        "MAX_INVALID_FRACTION",
        "REJECTED_SAMPLE_LIMIT",
        "AmbiguousSession",
        "FetchFailed",
        "IngestionError",
        "LockHeld",
        "NoNewData",
        "Pipeline",
        "RejectedRow",
        "RobotsDisallowed",
        "RunContext",
        "RunRecorder",
        "RunResult",
        "RunStatus",
        "RunTrigger",
        "SourceLayoutChanged",
        "ValidationOutcome",
        "classify_exception",
        "dedupe_by_key",
        "evaluate_thresholds",
        "execute_pipeline",
        "normalize_header",
        "parse_decimal_text",
        "to_jsonable",
    }
    assert all(hasattr(base, name) for name in base.__all__)


@pytest.mark.parametrize(
    ("attribute", "drifted", "message"),
    [
        pytest.param(
            "RUN_STATUSES",
            (*RUN_STATUSES, "archived"),
            "does not match models.ingestion_runs.RUN_STATUSES",
            id="statuses",
        ),
        pytest.param(
            "RUN_TRIGGERS",
            ("schedule", "cli"),
            "does not match models.ingestion_runs.RUN_TRIGGERS",
            id="triggers",
        ),
    ],
)
def test_import_fails_when_the_model_tuples_drift_from_the_types(
    monkeypatch: pytest.MonkeyPatch, attribute: str, drifted: tuple[str, ...], message: str
) -> None:
    """The guard runs at import, so the module is executed afresh under a probe name; the
    imported ``opennse_terminal.ingestion.base`` is left untouched."""
    monkeypatch.setattr(ingestion_runs_model, attribute, drifted)
    spec = importlib.util.spec_from_file_location("ingestion_base_drift_probe", base.__file__)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)
    with pytest.raises(RuntimeError, match=message):
        spec.loader.exec_module(probe)


# --- Exceptions -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "message", "attributes"),
    [
        pytest.param(
            SourceLayoutChanged(
                expected=("year", "month"), found=("yr",), message="header row changed"
            ),
            "header row changed",
            {"expected": ("year", "month"), "found": ("yr",), "message": "header row changed"},
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL),
            f"robots.txt blocks {NSE_URL} (disallowed)",
            {"url": NSE_URL, "reason": "disallowed"},
            id="robots-disallowed-default-reason",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL, reason="robots_unreachable"),
            f"robots.txt blocks {NSE_URL} (robots_unreachable)",
            {"url": NSE_URL, "reason": "robots_unreachable"},
            id="robots-unreachable",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_404"),
            f"fetch failed for {CSV_URL}: http_404",
            {"url": CSV_URL, "reason": "http_404"},
            id="fetch-failed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "label_mismatch", "label_session": "2026-10-27"}),
            "ambiguous session: label_mismatch",
            {"evidence": {"reason": "label_mismatch", "label_session": "2026-10-27"}},
            id="ambiguous-session-with-reason",
        ),
        pytest.param(
            AmbiguousSession({"link_session": "2026-10-26"}),
            "ambiguous session",
            {"evidence": {"link_session": "2026-10-26"}},
            id="ambiguous-session-without-reason",
        ),
        pytest.param(
            NoNewData("source has not advanced past 2026-10-27"),
            "no new data: source has not advanced past 2026-10-27",
            {"reason": "source has not advanced past 2026-10-27", "evidence": {}},
            id="no-new-data-without-evidence",
        ),
        pytest.param(
            NoNewData("bulletin already loaded", {"bulletin": "2026-02-06"}),
            "no new data: bulletin already loaded",
            {"reason": "bulletin already loaded", "evidence": {"bulletin": "2026-02-06"}},
            id="no-new-data-with-evidence",
        ),
        pytest.param(
            LockHeld("cbk_fx"),
            "lock held: another run of cbk_fx holds its advisory lock",
            {"pipeline": "cbk_fx"},
            id="lock-held",
        ),
    ],
)
def test_ingestion_exceptions_carry_their_attributes_and_message(
    exc: IngestionError, message: str, attributes: dict[str, object]
) -> None:
    assert isinstance(exc, IngestionError)
    assert str(exc) == message
    assert exc.args == (message,)
    assert {name: getattr(exc, name) for name in attributes} == attributes


def test_no_new_data_keeps_its_own_copy_of_the_evidence() -> None:
    evidence: dict[str, Any] = {"latest_session": "2026-10-27"}
    exc = NoNewData("source has not advanced", evidence)
    evidence["latest_session"] = "2026-10-28"
    assert exc.evidence == {"latest_session": "2026-10-27"}


# --- classify_exception ---------------------------------------------------------------------


def _sentinel_integrity_error() -> IntegrityError:
    """The error a constraint-violating INSERT raises: its text quotes the statement, its bound
    parameters and the driver's message, each carrying :data:`BOUND_VALUE_MARKER`."""
    return IntegrityError(
        "INSERT INTO terminal_equities (ticker, company_name) VALUES ($1::VARCHAR, $2::VARCHAR)",
        {"ticker": "ZZZZ", "company_name": BOUND_VALUE_MARKER},
        Exception(
            'new row for relation "terminal_equities" violates check constraint '
            '"ck_terminal_equities_shares_outstanding_range"\n'
            f"DETAIL:  Failing row contains (ZZZZ, {BOUND_VALUE_MARKER}, -1)."
        ),
    )


@pytest.mark.parametrize(
    ("exc", "status", "message"),
    [
        pytest.param(
            SourceLayoutChanged(expected=["year"], found=["yr"], message="expected header 'year'"),
            RunStatus.LAYOUT_CHANGED,
            "expected header 'year'",
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL),
            RunStatus.BLOCKED_BY_ROBOTS,
            f"robots.txt blocks {NSE_URL} (disallowed)",
            id="robots-disallowed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "stale_label"}),
            RunStatus.AMBIGUOUS_SESSION,
            "ambiguous session: stale_label",
            id="ambiguous-session",
        ),
        pytest.param(
            NoNewData("source has not advanced"),
            RunStatus.NO_NEW_DATA,
            "source has not advanced",
            id="no-new-data",
        ),
        pytest.param(
            LockHeld("nse_market_statistics"), RunStatus.FAILED, "lock held", id="lock-held"
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_503"),
            RunStatus.FAILED,
            f"fetch failed for {CSV_URL}: http_503",
            id="fetch-failed",
        ),
        pytest.param(
            CalendarNotVerified(2028, frozenset({2026, 2027})),
            RunStatus.FAILED,
            CALENDAR_MESSAGE,
            id="calendar-not-verified",
        ),
        pytest.param(asyncio.CancelledError(), RunStatus.FAILED, "cancelled", id="cancelled"),
        pytest.param(
            ValueError("could not convert '12,3x'"),
            RunStatus.FAILED,
            "unexpected_error:ValueError",
            id="unexpected-value-error",
        ),
        pytest.param(
            KeyError("ticker"), RunStatus.FAILED, "unexpected_error:KeyError", id="key-error"
        ),
        pytest.param(
            IngestionError("unmapped subclass"),
            RunStatus.FAILED,
            "unexpected_error:IngestionError",
            id="bare-ingestion-error",
        ),
        pytest.param(
            _sentinel_integrity_error(),
            RunStatus.FAILED,
            "unexpected_error:IntegrityError",
            id="sqlalchemy-error-with-sql-and-bound-parameters",
        ),
    ],
)
def test_classify_exception_maps_each_exception_to_its_status_and_message(
    exc: BaseException, status: RunStatus, message: str
) -> None:
    assert classify_exception(exc) == (status, message)


_LONG = "x" * 2_500


@pytest.mark.parametrize(
    ("exc", "untruncated"),
    [
        pytest.param(
            SourceLayoutChanged(expected=None, found=None, message=_LONG),
            _LONG,
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(_LONG), f"robots.txt blocks {_LONG} (disallowed)", id="robots"
        ),
        pytest.param(
            AmbiguousSession({"reason": _LONG}), f"ambiguous session: {_LONG}", id="ambiguous"
        ),
        pytest.param(NoNewData(_LONG), _LONG, id="no-new-data"),
        pytest.param(
            FetchFailed(CSV_URL, _LONG), f"fetch failed for {CSV_URL}: {_LONG}", id="fetch-failed"
        ),
        pytest.param(
            type(_LONG, (RuntimeError,), {})("message"),
            f"unexpected_error:{_LONG}",
            id="unexpected-with-a-long-type-name",
        ),
    ],
)
def test_classify_exception_truncates_messages_to_2000_characters(
    exc: BaseException, untruncated: str
) -> None:
    _, message = classify_exception(exc)
    assert len(message) == 2_000
    assert message == untruncated[:1_999] + "…"


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        pytest.param("r" * 1_999, "r" * 1_999, id="1999-kept"),
        pytest.param("r" * 2_000, "r" * 2_000, id="2000-kept"),
        pytest.param("r" * 2_001, "r" * 1_999 + "…", id="2001-truncated"),
    ],
)
def test_classify_exception_truncation_boundary(reason: str, expected: str) -> None:
    assert classify_exception(NoNewData(reason)) == (RunStatus.NO_NEW_DATA, expected)


# --- Records, outcome, context and result ---------------------------------------------------


def test_rejected_row_is_an_immutable_value() -> None:
    row = RejectedRow(index=7, reason="invalid_isin;volume_out_of_range", row={"isin": "KE0"})
    assert (row.index, row.reason, row.row) == (
        7,
        "invalid_isin;volume_out_of_range",
        {"isin": "KE0"},
    )
    assert row == RejectedRow(
        index=7, reason="invalid_isin;volume_out_of_range", row={"isin": "KE0"}
    )
    assert not hasattr(row, "__dict__")
    with pytest.raises(FrozenInstanceError):
        row.reason = "changed"  # type: ignore[misc]


def test_validation_outcome_holds_the_valid_and_rejected_rows() -> None:
    rejected = RejectedRow(index=1, reason="invalid_decimal", row={"cell": "abc"})
    outcome = ValidationOutcome(valid=[Decimal("6.80")], rejected=[rejected])
    assert outcome.valid == [Decimal("6.80")]
    assert outcome.rejected == [rejected]
    assert outcome == ValidationOutcome(valid=[Decimal("6.80")], rejected=[rejected])


def test_run_result_is_an_immutable_record_of_the_run() -> None:
    result = RunResult(
        run_id=RUN_ID,
        pipeline="cbk_rates_csv",
        status=RunStatus.PARTIAL,
        rows_fetched=10,
        rows_written=9,
        rows_rejected=1,
        error_message=None,
        details={"rejected": []},
    )
    assert (result.run_id, result.pipeline, result.status) == (RUN_ID, "cbk_rates_csv", "partial")
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (10, 9, 1)
    assert result.error_message is None
    assert result.details == {"rejected": []}
    with pytest.raises(FrozenInstanceError):
        result.status = RunStatus.FAILED  # type: ignore[misc]


def test_run_context_gives_each_run_its_own_details(settings: TerminalSettings) -> None:
    conn = cast(AsyncConnection, _ConnectionDouble(_EngineDouble(), DATA))
    http = cast(Any, _HttpDouble())
    first = RunContext(
        run_id=RUN_ID,
        pipeline="cbk_fx",
        trigger="schedule",
        settings=settings,
        conn=conn,
        http=http,
    )
    second = RunContext(
        run_id=RUN_ID,
        pipeline="cbk_fx",
        trigger="schedule",
        settings=settings,
        conn=conn,
        http=http,
    )
    first.details["source_url"] = SOURCE_URL
    assert second.details == {}
    assert (first.run_id, first.pipeline, first.trigger) == (RUN_ID, "cbk_fx", "schedule")
    assert first.settings is settings
    assert first.conn is conn
    assert first.http is http


# --- Pipeline contract ----------------------------------------------------------------------


def test_pipeline_is_abstract(settings: TerminalSettings) -> None:
    with pytest.raises(TypeError, match="abstract"):
        Pipeline(settings)  # type: ignore[abstract]


def test_pipeline_subclass_must_implement_upsert(settings: TerminalSettings) -> None:
    class _WithoutUpsert(Pipeline[str, str]):
        name = "test_without_upsert"

        async def fetch(self, ctx: RunContext) -> str:
            return ""

        def parse(self, raw: str) -> list[str]:
            return [raw]

        def validate(self, records: Sequence[str]) -> ValidationOutcome[str]:
            return ValidationOutcome(valid=list(records), rejected=[])

    with pytest.raises(TypeError, match="upsert"):
        _WithoutUpsert(settings)  # type: ignore[abstract]


def test_minimal_pipeline_defaults(settings: TerminalSettings) -> None:
    pipeline = _QuotePipeline(settings)
    assert pipeline.run_name == "test_quotes"
    assert pipeline.run_name == pipeline.name
    assert pipeline.min_rows == 0
    assert pipeline.source_url is None
    assert pipeline.settings is settings
    assert pipeline.write_rejections() == ()


def test_run_name_can_differ_from_the_registry_name(settings: TerminalSettings) -> None:
    pipeline = _CsvImportPipeline(settings)
    assert pipeline.name == "csv_import"
    assert pipeline.run_name == "csv_import:daily_prices"


async def test_default_reconcile_accepts_every_record_without_touching_the_connection(
    settings: TerminalSettings,
) -> None:
    conn = _ConnectionDouble(_EngineDouble(), DATA)
    ctx = RunContext(
        run_id=RUN_ID,
        pipeline="test_quotes",
        trigger="cli",
        settings=settings,
        conn=cast(AsyncConnection, conn),
        http=cast(Any, _HttpDouble()),
    )
    records = [_Quote(0, "6.80", Decimal("6.80")), _Quote(1, "15.72", Decimal("15.72"))]
    outcome = await _QuotePipeline(settings).reconcile(ctx, records)
    assert outcome.valid == records
    assert outcome.rejected == []
    assert conn.executed == []


# --- evaluate_thresholds --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("parsed", "rejected", "min_rows", "expected"),
    [
        pytest.param(0, 0, 0, RunStatus.SUCCEEDED, id="empty-parse-without-minimum"),
        pytest.param(0, 0, 1, RunStatus.LAYOUT_CHANGED, id="empty-parse-below-minimum"),
        pytest.param(69, 0, 70, RunStatus.LAYOUT_CHANGED, id="one-row-below-minimum"),
        pytest.param(70, 0, 70, RunStatus.SUCCEEDED, id="exactly-the-minimum"),
        pytest.param(69, 1, 70, RunStatus.LAYOUT_CHANGED, id="minimum-checked-before-fraction"),
        pytest.param(10, 0, 0, RunStatus.SUCCEEDED, id="all-valid"),
        pytest.param(10, 1, 0, RunStatus.PARTIAL, id="exactly-10-percent-rejected"),
        pytest.param(10, 2, 0, RunStatus.LAYOUT_CHANGED, id="20-percent-rejected"),
        pytest.param(9, 1, 0, RunStatus.LAYOUT_CHANGED, id="11.1-percent-rejected"),
        pytest.param(100, 10, 0, RunStatus.PARTIAL, id="10-of-100-rejected"),
        pytest.param(100, 11, 0, RunStatus.LAYOUT_CHANGED, id="11-of-100-rejected"),
        pytest.param(1, 1, 0, RunStatus.LAYOUT_CHANGED, id="only-row-rejected"),
        pytest.param(
            1_000_000_000_000,
            100_000_000_000,
            0,
            RunStatus.PARTIAL,
            id="exactly-10-percent-of-10e12-rows",
        ),
        pytest.param(
            1_000_000_000_000,
            100_000_000_001,
            0,
            RunStatus.LAYOUT_CHANGED,
            id="one-over-10-percent-of-10e12-rows",
        ),
    ],
)
def test_evaluate_thresholds(
    parsed: int, rejected: int, min_rows: int, expected: RunStatus
) -> None:
    assert evaluate_thresholds(parsed, rejected, min_rows) is expected


@pytest.mark.parametrize(
    ("parsed", "rejected", "min_rows", "message"),
    [
        pytest.param(-1, 0, 0, "row counts and min_rows must not be negative", id="parsed"),
        pytest.param(0, -1, 0, "row counts and min_rows must not be negative", id="rejected"),
        pytest.param(0, 0, -1, "row counts and min_rows must not be negative", id="min-rows"),
        pytest.param(
            3, 4, 0, r"rejected rows \(4\) exceed parsed rows \(3\)", id="more-rejected-than-parsed"
        ),
    ],
)
def test_evaluate_thresholds_rejects_impossible_counts(
    parsed: int, rejected: int, min_rows: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        evaluate_thresholds(parsed, rejected, min_rows)


# --- normalize_header -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("Last Traded Price", "last traded price", id="lowercased"),
        pytest.param("  Last   Traded\u00a0Price  ", "last traded price", id="trimmed-nbsp-runs"),
        pytest.param("Change\n(%)", "change (%)", id="line-break"),
        pytest.param("Face Value\r\n(Kshs Millions)", "face value (kshs millions)", id="crlf"),
        pytest.param("\tCoupon Rate\t", "coupon rate", id="tabs"),
        pytest.param("\ufeffYEAR", "year", id="byte-order-mark"),
        pytest.param("ISIN\u200b Code", "isin code", id="zero-width-space-before-space"),
        pytest.param("Re\u200bdemption Yield", "redemption yield", id="zero-width-space-removed"),
        pytest.param("91-Day Tbill", "91-day tbill", id="punctuation-kept"),
        pytest.param("", "", id="empty"),
        pytest.param(" \u00a0\n", "", id="whitespace-only"),
    ],
)
def test_normalize_header(text: str, expected: str) -> None:
    assert normalize_header(text) == expected


# --- parse_decimal_text ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "parentheses_negative", "expected"),
    [
        pytest.param("6.80", True, "6.80", id="scale-kept"),
        pytest.param("0.10", True, "0.10", id="trailing-zero-kept"),
        pytest.param("1,234.56", True, "1234.56", id="thousands-separator"),
        pytest.param("12,345,678", True, "12345678", id="several-separators"),
        pytest.param("(1,234.5)", True, "-1234.5", id="parentheses-negative"),
        pytest.param("( 1,234.5 )", True, "-1234.5", id="parentheses-with-spaces"),
        pytest.param("(5)", True, "-5", id="shortest-parentheses"),
        # A zero is never negative: str() of Decimal("-0") is "-0", so these prove the sign.
        pytest.param("-0", True, "0", id="minus-zero"),
        pytest.param("-0.00", True, "0.00", id="minus-zero-with-scale"),
        pytest.param("+0", True, "0", id="plus-zero"),
        pytest.param("-12.5", False, "-12.5", id="minus"),
        pytest.param("+12.5", True, "12.5", id="plus"),
        pytest.param("\u221212.5", True, "-12.5", id="unicode-minus"),
        pytest.param("1\u00a0234.5", True, "1234.5", id="nbsp-group-separator"),
        pytest.param("1\u202f234", True, "1234", id="narrow-nbsp-group-separator"),
        pytest.param("1\u2009234", True, "1234", id="thin-space-group-separator"),
        pytest.param("  15.72\n", True, "15.72", id="surrounding-whitespace"),
        pytest.param(".5", True, "0.5", id="leading-point"),
        pytest.param("5.", True, "5", id="trailing-point"),
        pytest.param("1.5E-2", True, "0.015", id="exponent"),
        pytest.param("1e3", True, "1E+3", id="lowercase-exponent"),
        pytest.param("(1e1000000)", True, "-1E+1000000", id="parentheses-beyond-context-exponent"),
        pytest.param(
            "(1234567890123456789012345678901234.5)",
            True,
            "-1234567890123456789012345678901234.5",
            id="parentheses-beyond-28-digits",
        ),
    ],
)
def test_parse_decimal_text_returns_the_exact_decimal(
    text: str, parentheses_negative: bool, expected: str
) -> None:
    result = parse_decimal_text(text, parentheses_negative=parentheses_negative)
    assert type(result) is Decimal
    assert result == Decimal(expected)
    # The string form proves exactness: the scale and digits came from the text, not a float.
    assert str(result) == expected


@pytest.mark.parametrize(
    ("text", "parentheses_negative"),
    [
        pytest.param(None, True, id="none"),
        pytest.param(None, False, id="none-parentheses-off"),
        pytest.param("", True, id="empty"),
        pytest.param("   ", True, id="blank"),
        pytest.param("-", True, id="hyphen"),
        pytest.param("—", True, id="em-dash"),
        pytest.param("–", True, id="en-dash"),
        pytest.param(" - ", True, id="spaced-hyphen"),
        pytest.param("n/a", True, id="n/a"),
        pytest.param("N/A", True, id="N/A"),
        pytest.param("(1,234.5)", False, id="parentheses-when-off"),
        pytest.param("NaN", True, id="nan"),
        pytest.param("sNaN", True, id="signalling-nan"),
        pytest.param("Infinity", True, id="infinity"),
        pytest.param("-Infinity", True, id="negative-infinity"),
        pytest.param("inf", True, id="inf"),
        pytest.param("abc", True, id="letters"),
        pytest.param("12abc", True, id="trailing-letters"),
        pytest.param("KES 12", True, id="currency-prefix"),
        pytest.param("5%", True, id="percent-sign"),
        pytest.param("1.2.3", True, id="two-points"),
        pytest.param("--5", True, id="double-minus"),
        pytest.param("+-5", True, id="plus-minus"),
        pytest.param("(-5)", True, id="signed-inside-parentheses"),
        pytest.param("(+5)", True, id="plus-inside-parentheses"),
        pytest.param("(-)", True, id="dash-inside-parentheses"),
        pytest.param("()", True, id="empty-parentheses"),
        pytest.param("(5", True, id="unclosed-parenthesis"),
        pytest.param("5)", True, id="unopened-parenthesis"),
        pytest.param("1_000", True, id="underscore-separator"),
        pytest.param("1e9999999999999999999", True, id="exponent-beyond-decimal-range"),
    ],
)
def test_parse_decimal_text_returns_none_for_blanks_and_non_numbers(
    text: str | None, parentheses_negative: bool
) -> None:
    assert parse_decimal_text(text, parentheses_negative=parentheses_negative) is None


def test_parse_decimal_text_reads_parentheses_as_negative_by_default() -> None:
    assert parse_decimal_text("(2,500)") == Decimal("-2500")


@pytest.mark.parametrize("text", ["(0)", "(0.00)"])
def test_parse_decimal_text_keeps_a_parenthesized_zero_positive(text: str) -> None:
    result = parse_decimal_text(text)
    assert result == Decimal(0)
    assert result is not None
    assert not result.is_signed()


@pytest.mark.parametrize(
    ("text", "context", "expected"),
    [
        pytest.param(
            "(1,234,567,890.123456789012345678901234567890)",
            Context(),
            "-1234567890.123456789012345678901234567890",
            id="forty-digits-grouped-with-fraction",
        ),
        pytest.param("(1234567.89)", Context(prec=5), "-1234567.89", id="five-digit-context"),
    ],
)
def test_parse_decimal_text_negates_parentheses_exactly_under_any_context(
    text: str, context: Context, expected: str
) -> None:
    # Context() is the default context (28 digits, Emax 999999, Overflow trapped).
    with localcontext(context):
        result = parse_decimal_text(text)
    assert type(result) is Decimal
    # as_tuple() compares the sign, every digit and the exponent with the leading-minus form,
    # which the Decimal constructor reads exactly whatever the context.
    assert result.as_tuple() == Decimal(expected).as_tuple()
    assert str(result) == expected


# --- dedupe_by_key --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("records", "expected", "dropped"),
    [
        pytest.param(
            [
                ("SCOM", Decimal("28.10")),
                ("EQTY", Decimal("60.25")),
                ("SCOM", Decimal("28.35")),
                ("KCB", Decimal("51.00")),
                ("EQTY", Decimal("60.50")),
            ],
            [("SCOM", Decimal("28.35")), ("EQTY", Decimal("60.50")), ("KCB", Decimal("51.00"))],
            2,
            id="last-value-at-first-position",
        ),
        pytest.param(
            [("SCOM", Decimal("28.10")), ("EQTY", Decimal("60.25"))],
            [("SCOM", Decimal("28.10")), ("EQTY", Decimal("60.25"))],
            0,
            id="no-duplicates",
        ),
        pytest.param(
            [("KCB", Decimal("50.00")), ("KCB", Decimal("50.50")), ("KCB", Decimal("51.00"))],
            [("KCB", Decimal("51.00"))],
            2,
            id="all-one-key",
        ),
        pytest.param([], [], 0, id="empty"),
    ],
)
def test_dedupe_by_key(
    records: list[tuple[str, Decimal]], expected: list[tuple[str, Decimal]], dropped: int
) -> None:
    original = list(records)
    assert dedupe_by_key(records, lambda record: record[0]) == (expected, dropped)
    assert records == original


def test_dedupe_by_key_accepts_composite_keys_and_any_sequence() -> None:
    records = (
        ("SCOM", date(2026, 10, 26), Decimal("28.10")),
        ("SCOM", date(2026, 10, 27), Decimal("28.35")),
        ("SCOM", date(2026, 10, 26), Decimal("28.20")),
    )
    deduped, dropped = dedupe_by_key(records, lambda record: (record[0], record[1]))
    assert deduped == [
        ("SCOM", date(2026, 10, 26), Decimal("28.20")),
        ("SCOM", date(2026, 10, 27), Decimal("28.35")),
    ]
    assert dropped == 1


# --- to_jsonable ----------------------------------------------------------------------------


class _Scale(Enum):
    THOUSANDS = Decimal("1000")


class _Session(Enum):
    PRE_OPEN = 1


class _Labelled:
    def __str__(self) -> str:
        return "labelled object"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(Decimal("7.50"), "7.50", id="decimal"),
        pytest.param(Decimal("-0.00"), "-0.00", id="negative-zero-decimal"),
        pytest.param(Decimal("1E+3"), "1E+3", id="decimal-exponent"),
        pytest.param(Decimal("NaN"), None, id="decimal-nan"),
        pytest.param(Decimal("Infinity"), None, id="decimal-infinity"),
        pytest.param(Decimal("-Infinity"), None, id="decimal-negative-infinity"),
        pytest.param(date(2026, 9, 30), "2026-09-30", id="date"),
        pytest.param(
            datetime(2026, 9, 30, 12, 0, tzinfo=UTC), "2026-09-30T12:00:00+00:00", id="utc"
        ),
        pytest.param(
            datetime(2026, 10, 28, 7, 30, tzinfo=ZoneInfo("Africa/Nairobi")),
            "2026-10-28T07:30:00+03:00",
            id="africa-nairobi",
        ),
        pytest.param(time(9, 30), "09:30:00", id="time"),
        pytest.param(RUN_ID, "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", id="uuid"),
        pytest.param(RunStatus.PARTIAL, "partial", id="str-enum"),
        pytest.param(_Scale.THOUSANDS, "1000", id="enum-with-decimal-value"),
        pytest.param(_Session.PRE_OPEN, 1, id="enum-with-int-value"),
        pytest.param(None, None, id="none"),
        pytest.param(True, True, id="bool"),
        pytest.param(0, 0, id="int"),
        pytest.param("KES", "KES", id="str"),
        pytest.param(b"%PDF-", "b'%PDF-'", id="bytes-not-a-list"),
        pytest.param(bytearray(b"%PDF-"), "bytearray(b'%PDF-')", id="bytearray-not-a-list"),
        pytest.param(_Labelled(), "labelled object", id="other-object"),
        pytest.param(
            RejectedRow,
            "<class 'opennse_terminal.ingestion.base.RejectedRow'>",
            id="dataclass-type",
        ),
    ],
)
def test_to_jsonable_converts_scalars(value: object, expected: object) -> None:
    result = to_jsonable(value)
    assert result == expected
    assert type(result) is type(expected)


def test_to_jsonable_converts_nested_payloads_recursively() -> None:
    payload = {
        "expected": ("year", "month", "12-month inflation"),
        "found": ["year", "month"],
        "rates": {"91d": Decimal("15.72"), "status": RunStatus.LAYOUT_CHANGED},
        "sessions": [date(2026, 10, 26), (date(2026, 10, 27), {"close": Decimal("28.35")})],
        "rejected": RejectedRow(
            index=4, reason="volume_out_of_range", row={"volume": Decimal("1E+13")}
        ),
        "view": MappingProxyType({"weight": Decimal("1.0")}),
        "tickers": frozenset({"SCOM", "EQTY", "KCB"}),
        "levels": {Decimal("10"), Decimal("9"), Decimal("1.5")},
        "empty": (),
    }
    expected = {
        "expected": ["year", "month", "12-month inflation"],
        "found": ["year", "month"],
        "rates": {"91d": "15.72", "status": "layout_changed"},
        "sessions": ["2026-10-26", ["2026-10-27", {"close": "28.35"}]],
        "rejected": {"index": 4, "reason": "volume_out_of_range", "row": {"volume": "1E+13"}},
        "view": {"weight": "1.0"},
        "tickers": ["EQTY", "KCB", "SCOM"],
        # Sets are sorted by their converted strings, so "10" sorts before "9".
        "levels": ["1.5", "10", "9"],
        "empty": [],
    }
    result = to_jsonable(payload)
    assert result == expected
    assert json.loads(json.dumps(result)) == expected


def test_to_jsonable_stringifies_mapping_keys() -> None:
    payload = {
        1: "int",
        False: "bool",
        Decimal("2.50"): "decimal",
        date(2026, 1, 2): "date",
        RunStatus.FAILED: "enum",
        None: "none",
        ("SCOM", 2026): "tuple",
        RUN_ID: "uuid",
    }
    result = to_jsonable(payload)
    assert result == {
        "1": "int",
        "False": "bool",
        "2.50": "decimal",
        "2026-01-02": "date",
        "failed": "enum",
        "None": "none",
        "['SCOM', 2026]": "tuple",
        "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7": "uuid",
    }
    assert all(type(key) is str for key in result)


# --- RunRecorder ----------------------------------------------------------------------------


async def test_run_recorder_start_commits_a_running_row_in_its_own_transaction() -> None:
    engine = _EngineDouble()
    before = datetime.now(UTC)
    run_id = await RunRecorder(cast(AsyncEngine, engine)).start(
        pipeline="cbk_rates_csv", trigger="schedule", source_url=SOURCE_URL
    )
    after = datetime.now(UTC)

    (conn,) = engine.connections
    assert (conn.kind, conn.outcome) == (RUN_RECORD, COMMITTED)
    (insert,) = conn.executed
    assert isinstance(insert.statement, Insert)
    assert insert.statement.table.name == "terminal_ingestion_runs"
    params = insert.params
    assert isinstance(run_id, uuid.UUID)
    assert params["id"] == run_id
    started_at = params.pop("started_at")
    assert started_at.utcoffset() == timedelta(0)
    assert before <= started_at <= after
    assert params == {
        "id": run_id,
        "pipeline": "cbk_rates_csv",
        "trigger": "schedule",
        "source_url": SOURCE_URL,
        "status": "running",
        "rows_fetched": 0,
        "rows_written": 0,
        "rows_rejected": 0,
        "details": {},
    }


async def test_run_recorder_start_gives_each_run_a_new_id() -> None:
    recorder = RunRecorder(cast(AsyncEngine, _EngineDouble()))
    first = await recorder.start(pipeline="cbk_fx", trigger="cli")
    second = await recorder.start(pipeline="cbk_fx", trigger="cli")
    assert first != second


async def test_run_recorder_start_rejects_an_unknown_trigger_before_connecting() -> None:
    engine = _EngineDouble()
    with pytest.raises(ValueError, match="unknown run trigger 'manual'"):
        await RunRecorder(cast(AsyncEngine, engine)).start(
            pipeline="cbk_fx",
            trigger="manual",  # type: ignore[arg-type]
        )
    assert engine.connections == []


async def test_run_recorder_finish_records_final_status_counts_and_jsonable_details() -> None:
    engine = _EngineDouble()
    before = datetime.now(UTC)
    await RunRecorder(cast(AsyncEngine, engine)).finish(
        RUN_ID,
        status=RunStatus.PARTIAL,
        rows_fetched=10,
        rows_written=9,
        rows_rejected=1,
        error_message=None,
        details={"latest": Decimal("6.80"), "session": date(2026, 10, 27)},
        source_url=CSV_URL,
    )
    after = datetime.now(UTC)

    (conn,) = engine.connections
    assert (conn.kind, conn.outcome) == (RUN_RECORD, COMMITTED)
    (update,) = conn.executed
    assert isinstance(update.statement, Update)
    assert update.statement.table.name == "terminal_ingestion_runs"
    params = update.params
    finished_at = params.pop("finished_at")
    assert finished_at.utcoffset() == timedelta(0)
    assert before <= finished_at <= after
    assert params == {
        "status": "partial",
        "rows_fetched": 10,
        "rows_written": 9,
        "rows_rejected": 1,
        "error_message": None,
        "details": {"latest": "6.80", "session": "2026-10-27"},
        "source_url": CSV_URL,
        "id_1": RUN_ID,
    }


async def test_run_recorder_finish_keeps_the_stored_url_and_defaults_details() -> None:
    engine = _EngineDouble()
    await RunRecorder(cast(AsyncEngine, engine)).finish(
        RUN_ID, status=RunStatus.FAILED, error_message="lock held"
    )
    (update,) = engine.updates
    assert "source_url" not in update.params
    assert update.params["details"] == {}
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (0, 0)
    assert update.params["rows_rejected"] == 0
    assert update.params["error_message"] == "lock held"


@pytest.mark.parametrize(
    ("status", "message"),
    [
        pytest.param(
            RunStatus.RUNNING, "a finished run needs a final status, not 'running'", id="running"
        ),
        pytest.param("archived", "'archived' is not a valid RunStatus", id="not-a-status"),
    ],
)
async def test_run_recorder_finish_rejects_a_non_final_status_before_connecting(
    status: RunStatus, message: str
) -> None:
    engine = _EngineDouble()
    with pytest.raises(ValueError, match=message):
        await RunRecorder(cast(AsyncEngine, engine)).finish(RUN_ID, status=status)
    assert engine.connections == []


async def test_run_recorder_finish_raises_when_no_run_row_matches() -> None:
    engine = _EngineDouble(update_rowcount=0)
    with pytest.raises(LookupError, match=f"no ingestion run with id {RUN_ID}"):
        await RunRecorder(cast(AsyncEngine, engine)).finish(RUN_ID, status=RunStatus.SUCCEEDED)


async def test_run_recorder_record_writes_a_complete_row_in_two_transactions() -> None:
    engine = _EngineDouble()
    run_id = await RunRecorder(cast(AsyncEngine, engine)).record(
        pipeline="pdf_filing",
        trigger="api",
        status=RunStatus.FAILED,
        source_url="upload:annual-report.pdf",
        error_message="pdf_encrypted",
        details={"sha256_prefix": "9f86d081"},
        rows_fetched=1,
        rows_written=0,
        rows_rejected=1,
    )
    assert [(conn.kind, conn.outcome) for conn in engine.connections] == [
        (RUN_RECORD, COMMITTED),
        (RUN_RECORD, COMMITTED),
    ]
    (insert,) = engine.inserts
    (update,) = engine.updates
    assert insert.params["id"] == run_id
    assert insert.params["source_url"] == "upload:annual-report.pdf"
    assert (insert.params["pipeline"], insert.params["trigger"]) == ("pdf_filing", "api")
    assert update.params["id_1"] == run_id
    assert update.params["status"] == "failed"
    assert update.params["error_message"] == "pdf_encrypted"
    assert update.params["details"] == {"sha256_prefix": "9f86d081"}
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (1, 0)
    assert update.params["rows_rejected"] == 1
    assert "source_url" not in update.params


# --- execute_pipeline -----------------------------------------------------------------------


async def test_succeeded_run_writes_every_row_in_one_locked_committed_transaction(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    http = _HttpDouble()
    pipeline = _QuotePipeline(
        settings, cells=["6.80", "15.72", "(1,234.50)"], source_url=SOURCE_URL
    )

    result = await _execute(pipeline, engine, http, trigger="schedule")

    # The running row commits first and the data transaction runs on its own connection; once
    # the lock is held, the update finishing earlier runs left running, and then the final
    # update, each use a separate run-record connection.
    assert [conn.kind for conn in engine.connections] == [RUN_RECORD, DATA, RUN_RECORD, RUN_RECORD]
    (insert,) = engine.inserts
    run_id = insert.params["id"]
    assert (insert.params["pipeline"], insert.params["trigger"]) == ("test_quotes", "schedule")
    assert insert.params["source_url"] == SOURCE_URL
    abandon_conn = engine.connections[2]
    assert abandon_conn.outcome == COMMITTED
    (abandon,) = engine.abandon_updates
    assert [executed.statement for executed in abandon_conn.executed] == [abandon.statement]
    abandon_params = dict(abandon.params)
    assert abandon_params.pop("finished_at").utcoffset() == timedelta(0)
    # Rows of this pipeline still running, other than this run, that started before it.
    assert abandon_params == {
        "status": "failed",
        "error_message": "abandoned",
        "details": {"abandoned_by": str(run_id)},
        "pipeline_1": "test_quotes",
        "status_1": "running",
        "id_1": run_id,
        "id_2": run_id,
    }

    (data,) = engine.data_connections
    assert (data.outcome, data.closed) == (COMMITTED, True)
    assert [str(executed.statement) for executed in data.executed] == [IDLE_TIMEOUT_SQL, LOCK_SQL]
    assert data.executed[1].params == {"pipeline": "test_quotes"}

    (ctx,) = pipeline.contexts
    assert (ctx.run_id, ctx.pipeline, ctx.trigger) == (run_id, "test_quotes", "schedule")
    assert ctx.settings is settings
    assert ctx.conn is data
    assert ctx.http is http
    assert (http.entered, http.exited) == (False, False)

    (upsert,) = pipeline.upserts
    upsert_conn, upserted, upsert_run_id = upsert
    assert upsert_conn is data
    assert upsert_run_id == run_id
    assert [quote.value for quote in upserted] == [
        Decimal("6.80"),
        Decimal("15.72"),
        Decimal("-1234.50"),
    ]

    details = dict(result.details)
    duration = _pop_duration(details)
    assert details == {"source_url": SOURCE_URL}
    assert result == RunResult(
        run_id=run_id,
        pipeline="test_quotes",
        status=RunStatus.SUCCEEDED,
        rows_fetched=3,
        rows_written=3,
        rows_rejected=0,
        error_message=None,
        details=result.details,
    )

    (update,) = engine.updates
    assert update.params["id_1"] == run_id
    assert update.params["status"] == "succeeded"
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (3, 3)
    assert update.params["rows_rejected"] == 0
    assert update.params["error_message"] is None
    assert update.params["details"] == result.details
    assert update.params["source_url"] == SOURCE_URL

    messages = [record.getMessage() for record in caplog.records if record.name == LOGGER]
    assert messages == [
        f"ingestion run {run_id} started: pipeline=test_quotes trigger=schedule",
        f"ingestion run {run_id} finished: pipeline=test_quotes status=succeeded fetched=3 "
        f"written=3 rejected=0 duration={duration}s",
    ]
    assert _warnings_and_errors(caplog) == []


@pytest.mark.parametrize(
    ("readings", "expected"),
    [
        pytest.param((1_000_000_000, 3_500_500_000), "2.501", id="half-millisecond-rounds-up"),
        pytest.param((1_000_000_000, 1_000_499_999), "0.000", id="below-half-rounds-down"),
        pytest.param((0, 86_400_000_000_001), "86400.000", id="one-day"),
    ],
)
async def test_duration_is_measured_in_integer_nanoseconds(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    readings: tuple[int, int],
    expected: str,
) -> None:
    """``duration_seconds`` is the difference of two ``time.monotonic_ns`` readings divided
    exactly by 10^9 and rounded ``ROUND_HALF_UP`` to milliseconds, with no float clock."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    clock = iter(readings)
    monkeypatch.setattr(base.time, "monotonic_ns", lambda: next(clock))

    result = await _execute(
        _QuotePipeline(settings, cells=["1.00"]), _EngineDouble(), _HttpDouble()
    )

    # Read exactly twice: when the run starts and when its details are assembled.
    assert next(clock, None) is None
    assert result.status is RunStatus.SUCCEEDED
    assert result.details["duration_seconds"] == expected
    finished = [record.getMessage() for record in caplog.records if record.name == LOGGER][-1]
    assert finished.endswith(f" duration={expected}s")


async def test_partial_run_writes_valid_rows_and_keeps_the_first_50_rejections(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    """600 rows with every tenth unparseable: 60 rejected is exactly 10%, so ``partial``."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    cells = ["abc" if index % 10 == 0 else "12.50" for index in range(600)]
    pipeline = _QuotePipeline(settings, cells=cells)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (600, 540, 60)
    assert result.error_message is None
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED
    (upsert,) = pipeline.upserts
    assert len(upsert[1]) == 540
    details = dict(result.details)
    _pop_duration(details)
    assert details == {
        "rejected": [
            {"index": index, "reason": "invalid_decimal", "row": {"cell": "abc"}}
            for index in range(0, 500, 10)
        ]
    }
    assert len(details["rejected"]) == REJECTED_SAMPLE_LIMIT
    (update,) = engine.updates
    assert update.params["status"] == "partial"
    assert (update.params["rows_written"], update.params["rows_rejected"]) == (540, 60)
    assert "source_url" not in update.params
    assert _warnings_and_errors(caplog) == []


@pytest.mark.parametrize(
    ("pipeline_class", "cells", "message", "expected", "found", "rejected"),
    [
        pytest.param(
            _QuotePipeline,
            ["abc", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "xyz"],
            "2 of 10 rows rejected, more than 10% of the rows parsed",
            {"min_rows": 0, "max_invalid_fraction": "0.10"},
            {"parsed": 10, "rejected": 2},
            [
                {"index": 0, "reason": "invalid_decimal", "row": {"cell": "abc"}},
                {"index": 9, "reason": "invalid_decimal", "row": {"cell": "xyz"}},
            ],
            id="more-than-10-percent-rejected",
        ),
        pytest.param(
            _MinRowsPipeline,
            ["1.00", "2.00", "3.00", "4.00"],
            "4 rows parsed, below the minimum of 5",
            {"min_rows": 5, "max_invalid_fraction": "0.10"},
            {"parsed": 4, "rejected": 0},
            None,
            id="below-min-rows",
        ),
    ],
)
async def test_threshold_breach_ends_layout_changed_and_writes_nothing(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    pipeline_class: type[_QuotePipeline],
    cells: list[str],
    message: str,
    expected: dict[str, object],
    found: dict[str, int],
    rejected: list[dict[str, object]] | None,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    pipeline = pipeline_class(settings, cells=cells)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.error_message == message
    assert (result.rows_fetched, result.rows_written) == (found["parsed"], 0)
    assert result.rows_rejected == found["rejected"]
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    details = dict(result.details)
    _pop_duration(details)
    assert details.pop("rejected", None) == rejected
    assert details == {"expected": expected, "found": found}
    (record,) = _warnings_and_errors(caplog)
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"ingestion run {result.run_id} ({pipeline.name}) layout_changed: "
        f"expected={expected!r} found={found!r}"
    )
    assert message not in caplog.text


async def test_reconcile_rejections_count_toward_the_thresholds(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(settings, cells=["1.00"] * 10, unknown=[3])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (10, 9, 1)
    assert [quote.index for quote in pipeline.upserts[0][1]] == [0, 1, 2, 4, 5, 6, 7, 8, 9]
    assert result.details["rejected"] == [
        {"index": 3, "reason": "unknown_ticker", "row": {"cell": "1.00"}}
    ]


async def test_reconcile_sees_only_validated_rows_and_both_rejection_kinds_add_up(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(settings, cells=["abc", *(["1.00"] * 9)], unknown=[3])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert [quote.index for quote in pipeline.reconciled] == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.error_message == "2 of 10 rows rejected, more than 10% of the rows parsed"
    assert result.details["found"] == {"parsed": 10, "rejected": 2}
    assert result.details["rejected"] == [
        {"index": 0, "reason": "invalid_decimal", "row": {"cell": "abc"}},
        {"index": 3, "reason": "unknown_ticker", "row": {"cell": "1.00"}},
    ]
    assert pipeline.upserts == []


BREACH_EVIDENCE: Mapping[str, object] = {
    "expected": {"min_rows": 0, "max_invalid_fraction": "0.10"},
    "found": {"parsed": 10, "rejected": 2},
}


@pytest.mark.parametrize(
    ("cells", "unwritable", "status", "message", "written", "outcome", "rejected", "evidence"),
    [
        pytest.param(
            ["1.00"] * 10,
            [4],
            RunStatus.PARTIAL,
            None,
            9,
            COMMITTED,
            [(4, "currency_conflict")],
            {},
            id="one-of-ten-unwritten-is-partial",
        ),
        pytest.param(
            ["1.00"] * 10,
            [4, 7],
            RunStatus.LAYOUT_CHANGED,
            "2 of 10 rows rejected, more than 10% of the rows parsed",
            0,
            ROLLED_BACK,
            [(4, "currency_conflict"), (7, "currency_conflict")],
            BREACH_EVIDENCE,
            id="two-of-ten-unwritten-roll-the-upsert-back",
        ),
        pytest.param(
            ["abc", *(["1.00"] * 9)],
            [4],
            RunStatus.LAYOUT_CHANGED,
            "2 of 10 rows rejected, more than 10% of the rows parsed",
            0,
            ROLLED_BACK,
            [(0, "invalid_decimal"), (4, "currency_conflict")],
            BREACH_EVIDENCE,
            id="validation-and-write-rejections-add-up",
        ),
    ],
)
async def test_rows_the_upsert_could_not_write_count_toward_the_thresholds(
    settings: TerminalSettings,
    cells: list[str],
    unwritable: list[int],
    status: RunStatus,
    message: str | None,
    written: int,
    outcome: str,
    rejected: list[tuple[int, str]],
    evidence: Mapping[str, object],
) -> None:
    """The thresholds are applied again once the upsert has run: its write rejections join the
    earlier ones, and a breach rolls back the rows it had already written."""
    engine = _EngineDouble()
    pipeline = _WriteRejectingPipeline(settings, cells=cells, unwritable=unwritable)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is status
    assert result.error_message == message
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (
        10,
        written,
        len(rejected),
    )
    # The upsert ran either way; the second threshold check decides whether its rows commit.
    assert len(pipeline.upserts) == 1
    (data,) = engine.data_connections
    assert data.outcome == outcome
    details = dict(result.details)
    _pop_duration(details)
    assert details == {
        **evidence,
        "rejected": [
            {"index": index, "reason": reason, "row": {"cell": cells[index]}}
            for index, reason in rejected
        ],
    }
    (update,) = engine.updates
    assert (update.params["status"], update.params["rows_written"]) == (status.value, written)


async def test_empty_parse_succeeds_without_calling_upsert(settings: TerminalSettings) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=[])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.SUCCEEDED
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED


@pytest.mark.parametrize(
    ("error", "status", "message", "evidence", "logged"),
    [
        pytest.param(
            SourceLayoutChanged(
                expected=("year", "month"), found=("yr",), message="header row changed"
            ),
            RunStatus.LAYOUT_CHANGED,
            "header row changed",
            {"expected": ["year", "month"], "found": ["yr"]},
            (
                logging.ERROR,
                "layout_changed: expected=['year', 'month'] found=['yr']",
            ),
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(CSV_URL),
            RunStatus.BLOCKED_BY_ROBOTS,
            f"robots.txt blocks {CSV_URL} (disallowed)",
            {"robots": {"url": CSV_URL, "reason": "disallowed"}},
            (logging.WARNING, "blocked_by_robots: disallowed"),
            id="robots-disallowed",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_503"),
            RunStatus.FAILED,
            f"fetch failed for {CSV_URL}: http_503",
            {"fetch_failure": {"url": CSV_URL, "reason": "http_503"}},
            (logging.ERROR, "failed: fetch_failed:http_503"),
            id="fetch-failed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "label_mismatch", "label_session": date(2026, 10, 27)}),
            RunStatus.AMBIGUOUS_SESSION,
            "ambiguous session: label_mismatch",
            {"reason": "label_mismatch", "label_session": "2026-10-27"},
            (logging.WARNING, "ambiguous_session: AmbiguousSession"),
            id="ambiguous-session",
        ),
        pytest.param(
            NoNewData("source has not advanced", {"latest_session": date(2026, 10, 27)}),
            RunStatus.NO_NEW_DATA,
            "source has not advanced",
            {"latest_session": "2026-10-27"},
            None,
            id="no-new-data",
        ),
        pytest.param(
            CalendarNotVerified(2028, frozenset({2026, 2027})),
            RunStatus.FAILED,
            CALENDAR_MESSAGE,
            {"calendar": {"year": 2028, "verified_years": [2026, 2027]}},
            (logging.ERROR, "failed: calendar_not_verified:2028"),
            id="calendar-not-verified",
        ),
        pytest.param(
            RuntimeError("parser exploded"),
            RunStatus.FAILED,
            "unexpected_error:RuntimeError",
            {"exception": "RuntimeError"},
            (logging.ERROR, "failed: RuntimeError"),
            id="unexpected",
        ),
        pytest.param(
            _sentinel_integrity_error(),
            RunStatus.FAILED,
            "unexpected_error:IntegrityError",
            {"exception": "IntegrityError"},
            (logging.ERROR, "failed: IntegrityError"),
            id="sqlalchemy-error-with-sql-and-bound-parameters",
        ),
    ],
)
async def test_fetch_exception_becomes_the_run_status_with_its_evidence(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    error: BaseException,
    status: RunStatus,
    message: str,
    evidence: dict[str, object],
    logged: tuple[int, str] | None,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"], source_url=SOURCE_URL, fetch_error=error)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is status
    assert result.error_message == message
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    details = dict(result.details)
    _pop_duration(details)
    assert details == {"source_url": SOURCE_URL, **evidence}
    (update,) = engine.updates
    assert update.params["status"] == status.value
    assert update.params["error_message"] == message
    assert update.params["details"] == result.details
    assert_no_statement_text(update.params["error_message"], update.params["details"])

    records = _warnings_and_errors(caplog)
    if logged is None:
        assert records == []
    else:
        (record,) = records
        level, line = logged
        assert (record.levelno, record.getMessage()) == (
            level,
            f"ingestion run {result.run_id} (test_quotes) {line}",
        )
        # No traceback, and no recorded message: they are exception text (here carrying the
        # source URL), recorded in the run row only.
        assert record.exc_info is None
        assert message not in caplog.text
        assert CSV_URL not in caplog.text


async def test_exception_after_parsing_keeps_the_fetched_count_and_writes_nothing(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(
        settings, cells=["1.00", "2.00", "3.00"], upsert_error=RuntimeError("unique violation")
    )

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.FAILED
    assert result.error_message == "unexpected_error:RuntimeError"
    # Written rows are counted only once the data transaction has committed.
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (3, 0, 0)
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK


async def test_no_new_data_from_reconcile_rolls_back_and_records_its_evidence(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(
        settings,
        cells=["28.35", "60.50"],
        reconcile_error=NoNewData("source has not advanced", {"latest": Decimal("28.35")}),
    )

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.NO_NEW_DATA
    assert result.error_message == "source has not advanced"
    assert (result.rows_fetched, result.rows_written) == (2, 0)
    assert result.details["latest"] == "28.35"
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK


async def test_held_lock_ends_failed_without_fetching(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble(lock_available=False)
    pipeline = _QuotePipeline(settings, cells=["1.00"])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.FAILED
    assert result.error_message == "lock held"
    assert pipeline.contexts == []
    # Without the lock, another run of the pipeline may be live: no earlier run is finished.
    assert engine.abandon_updates == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    assert [str(executed.statement) for executed in data.executed] == [IDLE_TIMEOUT_SQL, LOCK_SQL]
    details = dict(result.details)
    _pop_duration(details)
    assert details == {"lock_key": "terminal:test_quotes"}
    warning, error = _warnings_and_errors(caplog)
    assert (warning.levelno, warning.getMessage()) == (
        logging.WARNING,
        "advisory lock held for test_quotes",
    )
    assert (error.levelno, error.getMessage()) == (
        logging.ERROR,
        f"ingestion run {result.run_id} (test_quotes) failed: lock held",
    )
    assert error.exc_info is None


@pytest.mark.parametrize(
    ("engine_options", "warning"),
    [
        pytest.param(
            {"abandoned_rowcount": 2},
            "ingestion run {run_id} (test_quotes) recorded 2 earlier run(s) left running as "
            "failed (abandoned)",
            id="rows-marked",
        ),
        pytest.param(
            {"abandon_error": RuntimeError("update failed: password=s3cr3t")},
            "ingestion run {run_id} (test_quotes) could not finish earlier runs left running: "
            "RuntimeError",
            id="update-failed",
        ),
    ],
)
async def test_earlier_runs_left_running_are_finished_best_effort(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    engine_options: dict[str, Any],
    warning: str,
) -> None:
    """With the lock held, the abandoned-run update runs once: rows it marked are one WARNING
    with their count, and its failure is one WARNING by type only, after which the run goes on
    and succeeds."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble(**engine_options)
    pipeline = _QuotePipeline(settings, cells=["1.00", "2.00"])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.SUCCEEDED
    assert result.rows_written == 2
    assert len(engine.abandon_updates) == 1
    (update,) = engine.updates
    assert update.params["status"] == "succeeded"
    (record,) = _warnings_and_errors(caplog)
    assert (record.levelno, record.getMessage()) == (
        logging.WARNING,
        warning.format(run_id=result.run_id),
    )
    assert record.exc_info is None
    assert "s3cr3t" not in caplog.text


async def test_run_name_keys_the_run_row_the_lock_and_the_context(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _CsvImportPipeline(settings, cells=["1.00"])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.pipeline == "csv_import:daily_prices"
    assert engine.inserts[0].params["pipeline"] == "csv_import:daily_prices"
    assert engine.data_connections[0].executed[1].params == {"pipeline": "csv_import:daily_prices"}
    assert pipeline.contexts[0].pipeline == "csv_import:daily_prices"


async def test_source_url_discovered_during_the_run_is_recorded(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(
        settings, cells=["8.75"], source_url=SOURCE_URL, discovered_url=CSV_URL
    )

    await _execute(pipeline, engine, _HttpDouble())

    assert engine.inserts[0].params["source_url"] == SOURCE_URL
    assert engine.updates[0].params["source_url"] == CSV_URL


async def test_unknown_trigger_fails_before_any_data_connection(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    with pytest.raises(ValueError, match="unknown run trigger 'nightly'"):
        await _execute(pipeline, engine, _HttpDouble(), trigger="nightly")  # type: ignore[arg-type]
    assert engine.connections == []
    assert pipeline.contexts == []


@pytest.mark.parametrize(
    ("engine_options", "error", "message"),
    [
        pytest.param({"fail_run_updates": True}, RuntimeError, "run row update failed", id="error"),
        pytest.param({"update_rowcount": 0}, LookupError, "no ingestion run with id", id="lost"),
    ],
)
async def test_errors_writing_the_run_row_propagate(
    settings: TerminalSettings,
    engine_options: dict[str, Any],
    error: type[Exception],
    message: str,
) -> None:
    engine = _EngineDouble(**engine_options)
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    with pytest.raises(error, match=message):
        await _execute(pipeline, engine, _HttpDouble())
    # The data transaction had already committed; only the run row's final update failed.
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED


@pytest.mark.parametrize(
    "interrupt",
    [pytest.param(KeyboardInterrupt(), id="keyboard"), pytest.param(SystemExit(3), id="exit")],
)
async def test_interrupts_propagate_untouched(
    settings: TerminalSettings, interrupt: BaseException
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"], fetch_error=interrupt)
    with pytest.raises(type(interrupt)):
        await _execute(pipeline, engine, _HttpDouble())
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    # The run row is left as ``running``: no final update is attempted.
    assert engine.updates == []


async def test_cancelled_run_rolls_back_records_failed_and_reraises(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble()
    pipeline = _BlockingPipeline(settings, source_url=NSE_URL)
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble(), trigger="schedule"))
    try:
        await _stage_reached(pipeline.fetch_started, task, "its fetch")

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        (data,) = engine.data_connections
        assert (data.outcome, data.closed) == (ROLLED_BACK, True)
        (insert,) = engine.inserts
        run_id = insert.params["id"]
        (update,) = engine.updates
        assert update.params["id_1"] == run_id
        assert update.params["status"] == "failed"
        assert update.params["error_message"] == "cancelled"
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (0, 0)
        assert update.params["rows_rejected"] == 0
        assert update.params["source_url"] == NSE_URL
        details = dict(update.params["details"])
        _pop_duration(details)
        assert details == {"source_url": NSE_URL}
        (record,) = _warnings_and_errors(caplog)
        assert (record.levelno, record.getMessage()) == (
            logging.ERROR,
            f"ingestion run {run_id} (test_blocking) failed: cancelled",
        )
        assert record.exc_info is None
    finally:
        await _finish_run(task, engine, pipeline, before=before)


async def test_cancelled_run_still_reraises_when_its_run_row_cannot_be_written(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble(fail_run_updates=True)
    pipeline = _BlockingPipeline(settings)
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(pipeline.fetch_started, task, "its fetch")

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        run_id = engine.inserts[0].params["id"]
        not_recorded, failed = _warnings_and_errors(caplog)
        # The audit error is named by its type only: no message text and no traceback.
        assert (not_recorded.levelno, not_recorded.getMessage()) == (
            logging.ERROR,
            f"could not record ingestion run {run_id} (test_blocking) as failed: RuntimeError",
        )
        assert not_recorded.exc_info is None
        assert "run row update failed" not in caplog.text
        assert failed.getMessage() == f"ingestion run {run_id} (test_blocking) failed: cancelled"
        assert failed.exc_info is None
    finally:
        await _finish_run(task, engine, pipeline, before=before)


# --- Worker-thread stages, cancel-safe recording and the committed outcome --------------------


@pytest.mark.parametrize("gate", ["parse", "validate"])
async def test_parse_and_validate_run_in_a_worker_thread_while_the_loop_stays_free(
    settings: TerminalSettings, gate: str
) -> None:
    before = _other_tasks()
    engine = _EngineDouble()
    pipeline = _GatedStagePipeline(settings, gate=gate, cells=["1.00", "2.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(pipeline.entered, task, f"its {gate} stage")

        # The gated stage holds its thread, yet timers and other coroutines keep running.
        await asyncio.sleep(0.02)
        assert not task.done()
        pipeline.release.set()
        result = await _run_result(task)

        assert result.status is RunStatus.SUCCEEDED
        assert (result.rows_fetched, result.rows_written) == (2, 2)
        assert pipeline.finished == ["parse", "validate"]
        assert set(pipeline.threads) == {"parse", "validate"}
        assert threading.get_ident() not in pipeline.threads.values()
        # Still one data transaction, committed after both stages.
        (data,) = engine.data_connections
        assert data.outcome == COMMITTED
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    ("gate", "stages", "fetched"),
    [
        pytest.param("parse", ["parse"], 0, id="parse"),
        pytest.param("validate", ["parse", "validate"], 2, id="validate"),
    ],
)
async def test_cancellation_waits_for_the_running_stage_then_rolls_back(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    gate: str,
    stages: list[str],
    fetched: int,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble()
    pipeline = _GatedStagePipeline(settings, gate=gate, cells=["1.00", "2.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(pipeline.entered, task, f"its {gate} stage")

        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.01)
        # The stage's thread cannot be interrupted: the run waits for it, transaction still open.
        assert not task.done()
        (data,) = engine.data_connections
        assert data.outcome is None
        assert engine.updates == []

        pipeline.release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        # The stage completed and its result was discarded; no later stage ran.
        assert pipeline.finished == stages
        assert set(pipeline.threads) == set(stages)
        assert pipeline.upserts == []
        assert (data.outcome, data.closed) == (ROLLED_BACK, True)
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("failed", "cancelled")
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (fetched, 0)
        (record,) = _warnings_and_errors(caplog)
        assert record.getMessage().endswith("(test_gated) failed: cancelled")
        assert _other_tasks() == before
    finally:
        await _finish_run(task, engine, pipeline, before=before)


async def test_cancellation_during_the_commit_lets_it_finish_and_keeps_the_committed_run(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble(hold_commit=True)
    pipeline = _QuotePipeline(settings, cells=["1.00", "2.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(engine.commit_started, task, "its commit")

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert engine.commit_release is not None
        engine.commit_release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        (data,) = engine.data_connections
        assert data.outcome == COMMITTED
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("succeeded", None)
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (2, 2)
        assert update.params["details"]["after_commit_error"] == "CancelledError"
        (record,) = _warnings_and_errors(caplog)
        run_id = engine.inserts[0].params["id"]
        assert (record.levelno, record.getMessage()) == (
            logging.WARNING,
            f"ingestion run {run_id} (test_quotes) was interrupted after its data committed: "
            "cancelled",
        )
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    ("cancel", "message"),
    [
        pytest.param(False, "unexpected_error:RuntimeError", id="error"),
        pytest.param(True, "cancelled", id="error-while-cancelled"),
    ],
)
async def test_failed_commit_records_failed_with_nothing_written(
    settings: TerminalSettings, cancel: bool, message: str
) -> None:
    before = _other_tasks()
    engine = _EngineDouble(fail_commit=True, hold_commit=cancel)
    pipeline = _QuotePipeline(settings, cells=["1.00", "2.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        if cancel:
            await _stage_reached(engine.commit_started, task, "its commit")
            task.cancel()
            await asyncio.sleep(0)
            assert engine.commit_release is not None
            engine.commit_release.set()
            with pytest.raises(asyncio.CancelledError):
                await _run_result(task)
        else:
            assert (await _run_result(task)).status is RunStatus.FAILED

        (data,) = engine.data_connections
        assert data.outcome == ROLLED_BACK
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("failed", message)
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (2, 0)
    finally:
        await _finish_run(task, engine, pipeline, before=before)


async def test_cancellation_during_the_final_update_still_records_the_true_status(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble(hold_update=True)
    pipeline = _QuotePipeline(settings, cells=["1.00", "abc", *(["2.00"] * 9)])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(engine.update_started, task, "its final update")

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert engine.update_release is not None
        engine.update_release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("partial", None)
        assert (update.params["rows_written"], update.params["rows_rejected"]) == (10, 1)
        # The update's own transaction committed: the row is not left running.
        assert engine.connections[-1].kind == RUN_RECORD
        assert engine.connections[-1].outcome == COMMITTED
        assert _warnings_and_errors(caplog) == []
        duration = update.params["details"]["duration_seconds"]
        assert caplog.messages[-1].endswith(
            f" status=partial fetched=11 written=10 rejected=1 duration={duration}s"
        )
        assert _other_tasks() == before
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize("fail_update", [False, True], ids=["recorded", "update-fails"])
async def test_repeated_cancellation_while_recording_a_cancelled_run_waits_for_the_update(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture, fail_update: bool
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    try:
        before = _other_tasks()
        engine = _EngineDouble(hold_update=True, fail_run_updates=fail_update)
        pipeline = _BlockingPipeline(settings)
        task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
        try:
            await _stage_reached(pipeline.fetch_started, task, "its fetch")
            task.cancel()
            await _stage_reached(engine.update_started, task, "its final update")

            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            assert engine.update_release is not None
            engine.update_release.set()
            with pytest.raises(asyncio.CancelledError):
                await _run_result(task)
            # Checked before the cleanup, which would wait for a task the run left behind.
            assert _other_tasks() == before
        finally:
            await _finish_run(task, engine, pipeline, before=before)
        # The cleanup is done and keeps no reference, so collecting the run frees its steps.
        del task
        gc.collect()
    finally:
        loop.set_exception_handler(previous_handler)

    # The update was waited for, not abandoned, and its error observed rather than reported
    # by the event loop as never retrieved.
    assert unhandled == []
    assert _other_tasks() == before
    (update,) = engine.updates
    assert (update.params["status"], update.params["error_message"]) == ("failed", "cancelled")
    assert engine.connections[-1].outcome == (ROLLED_BACK if fail_update else COMMITTED)
    run_id = engine.inserts[0].params["id"]
    messages = [record.getMessage() for record in _warnings_and_errors(caplog)]
    expected_failed = f"ingestion run {run_id} (test_blocking) failed: cancelled"
    if fail_update:
        assert messages == [
            f"could not record ingestion run {run_id} (test_blocking) as failed: RuntimeError",
            expected_failed,
        ]
    else:
        assert messages == [expected_failed]


async def test_cancellation_while_the_running_row_is_inserted_still_finishes_the_row(
    settings: TerminalSettings,
) -> None:
    before = _other_tasks()
    engine = _EngineDouble(hold_insert=True)
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        await _stage_reached(engine.insert_started, task, "its running insert")

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert engine.insert_release is not None
        engine.insert_release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        # No data transaction was started, and the committed running row was finished.
        assert engine.data_connections == []
        assert pipeline.contexts == []
        (insert,) = engine.inserts
        (update,) = engine.updates
        assert update.params["id_1"] == insert.params["id"]
        assert (update.params["status"], update.params["error_message"]) == ("failed", "cancelled")
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    "close_error",
    [
        pytest.param(RuntimeError("close failed: SECRET-CLOSE-MARKER"), id="cleanup-raises"),
        pytest.param(None, id="cleanup-cancelled"),
    ],
)
async def test_cleanup_after_the_commit_keeps_the_committed_status_and_counts(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    close_error: BaseException | None,
) -> None:
    """The run's own HTTP client is closed after the data commit: whether closing raises or
    is cancelled, the run keeps ``partial`` and its written rows."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    clients: list[_ClosingClient] = []

    def open_client(
        client_settings: TerminalSettings, *, spacing: ingestion_http.HostSpacing | None = None
    ) -> _ClosingClient:
        client = _ClosingClient(client_settings, close_error)
        clients.append(client)
        return client

    monkeypatch.setattr(ingestion_http, "PoliteClient", open_client)
    before = _other_tasks()
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["abc", *(["1.00"] * 10)])
    task = asyncio.create_task(execute_pipeline(pipeline, cast(AsyncEngine, engine), trigger="cli"))
    try:
        if close_error is None:
            async with asyncio.timeout(5):
                while not (clients and clients[0].closing.is_set()):
                    if task.done():
                        _run_ended_early(task, "closing its HTTP client")
                    await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await _run_result(task)
            after_commit = "CancelledError"
            logged = "cancelled"
        else:
            result = await _run_result(task)
            assert (result.status, result.rows_written, result.error_message) == (
                RunStatus.PARTIAL,
                10,
                None,
            )
            after_commit = logged = "RuntimeError"

        (client,) = clients
        assert client.settings is settings
        assert client.closing.is_set()
        (data,) = engine.data_connections
        assert data.outcome == COMMITTED
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("partial", None)
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (11, 10)
        assert update.params["rows_rejected"] == 1
        assert update.params["details"]["after_commit_error"] == after_commit
        run_id = engine.inserts[0].params["id"]
        (record,) = _warnings_and_errors(caplog)
        assert (record.levelno, record.getMessage()) == (
            logging.WARNING,
            f"ingestion run {run_id} (test_quotes) was interrupted after its data committed: "
            f"{logged}",
        )
        assert record.exc_info is None
        assert "SECRET-CLOSE-MARKER" not in caplog.text
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    ("interrupt", "raised"),
    [
        pytest.param("cancel", asyncio.CancelledError, id="cancelled-twice"),
        pytest.param("timeout", TimeoutError, id="asyncio-timeout"),
    ],
)
async def test_cancellation_during_the_commit_is_reraised_when_cleanup_then_raises(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    interrupt: str,
    raised: type[BaseException],
) -> None:
    """The cancellation the commit absorbed survives closing the run's own HTTP client raising
    afterwards: the committed run is recorded as such and the call still ends cancelled, which an
    ``asyncio.timeout()`` around it turns into ``TimeoutError``."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    clients = _open_closing_clients(monkeypatch, RuntimeError("close failed: SECRET-CLOSE-MARKER"))
    engine = _EngineDouble(hold_commit=True)
    pipeline = _QuotePipeline(settings, cells=["1.00", "2.00"])
    deadlines: list[asyncio.Timeout] = []

    async def run() -> RunResult:
        if interrupt == "cancel":
            return await execute_pipeline(pipeline, cast(AsyncEngine, engine), trigger="cli")
        async with asyncio.timeout(None) as deadline:
            deadlines.append(deadline)
            return await execute_pipeline(pipeline, cast(AsyncEngine, engine), trigger="cli")

    task = asyncio.create_task(run())
    try:
        await _stage_reached(engine.commit_started, task, "its commit")
        if interrupt == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
        else:
            (deadline,) = deadlines
            deadline.reschedule(asyncio.get_running_loop().time())
            async with asyncio.timeout(5):
                while not deadline.expired():
                    if task.done():
                        _run_ended_early(task, "its deadline expired")
                    await asyncio.sleep(0)
        await asyncio.sleep(0)
        # The commit is still held, so the run is waiting for it rather than abandoning it.
        assert not task.done()
        assert engine.commit_release is not None
        engine.commit_release.set()
        with pytest.raises(raised):
            await _run_result(task)

        assert task.cancelled() is (interrupt == "cancel")
        (client,) = clients
        assert client.closing.is_set()
        (data,) = engine.data_connections
        assert (data.outcome, data.closed) == (COMMITTED, True)
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("succeeded", None)
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (2, 2)
        details = dict(update.params["details"])
        _pop_duration(details)
        assert details == {"after_commit_error": "RuntimeError"}
        run_id = engine.inserts[0].params["id"]
        (record,) = _warnings_and_errors(caplog)
        assert (record.levelno, record.getMessage()) == (
            logging.WARNING,
            f"ingestion run {run_id} (test_quotes) was interrupted after its data committed: "
            "RuntimeError",
        )
        assert all(entry.exc_info is None for entry in caplog.records)
        assert "SECRET-CLOSE-MARKER" not in caplog.text
        assert _other_tasks() == before
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    ("stage", "fetched"),
    [
        pytest.param("fetch", 0, id="fetch"),
        pytest.param("parse", 0, id="parse"),
        pytest.param("validate", 2, id="validate"),
    ],
)
async def test_cancellation_before_the_commit_is_reraised_when_cleanup_then_raises(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    fetched: int,
) -> None:
    """Closing the run's own HTTP client raises while a cancelled run unwinds, replacing its
    ``CancelledError``: the run is still recorded as cancelled with nothing written, the
    replacing error is named by its type only, and the call ends cancelled."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    clients = _open_closing_clients(monkeypatch, RuntimeError("close failed: SECRET-CLOSE-MARKER"))
    engine = _EngineDouble()
    pipeline: _QuotePipeline
    reached: asyncio.Event
    if stage == "fetch":
        blocking = _BlockingPipeline(settings, cells=["1.00", "2.00"])
        pipeline, reached = blocking, blocking.fetch_started
    else:
        gated = _GatedStagePipeline(settings, gate=stage, cells=["1.00", "2.00"])
        pipeline, reached = gated, gated.entered
    task = asyncio.create_task(execute_pipeline(pipeline, cast(AsyncEngine, engine), trigger="cli"))
    try:
        await _stage_reached(reached, task, f"its {stage} stage")
        task.cancel()
        if isinstance(pipeline, _GatedStagePipeline):
            await asyncio.sleep(0.01)
            pipeline.release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        assert task.cancelled()
        (client,) = clients
        assert client.closing.is_set()
        assert pipeline.upserts == []
        (data,) = engine.data_connections
        assert (data.outcome, data.closed) == (ROLLED_BACK, True)
        (update,) = engine.updates
        assert (update.params["status"], update.params["error_message"]) == ("failed", "cancelled")
        assert (update.params["rows_fetched"], update.params["rows_written"]) == (fetched, 0)
        details = dict(update.params["details"])
        _pop_duration(details)
        assert details == {"error_during_cancellation": "RuntimeError"}
        run_id = engine.inserts[0].params["id"]
        name = pipeline.run_name
        assert [
            (record.levelno, record.getMessage()) for record in _warnings_and_errors(caplog)
        ] == [
            (
                logging.WARNING,
                f"ingestion run {run_id} ({name}) was cancelled; unwinding it raised RuntimeError",
            ),
            (logging.ERROR, f"ingestion run {run_id} ({name}) failed: cancelled"),
        ]
        assert all(entry.exc_info is None for entry in caplog.records)
        assert "SECRET-CLOSE-MARKER" not in caplog.text
        assert _other_tasks() == before
    finally:
        await _finish_run(task, engine, pipeline, before=before)


@pytest.mark.parametrize(
    ("insert_error", "cancel", "logged"),
    [
        pytest.param(
            RuntimeError("run row insert failed: SECRET-INSERT-MARKER"),
            True,
            ["could not record the start of an ingestion run of test_quotes: RuntimeError"],
            id="insert-fails-while-cancelled",
        ),
        pytest.param(asyncio.CancelledError(), False, [], id="insert-task-ends-cancelled"),
    ],
)
async def test_a_running_row_that_was_never_inserted_ends_the_call_cancelled(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    insert_error: BaseException,
    cancel: bool,
    logged: list[str],
) -> None:
    """The running insert fails while a cancellation is pending, or its own task ends cancelled:
    there is no run row to finish, nothing runs, and the call ends cancelled."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    before = _other_tasks()
    engine = _EngineDouble(hold_insert=cancel, insert_error=insert_error)
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    try:
        if cancel:
            await _stage_reached(engine.insert_started, task, "its running insert")
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert engine.insert_release is not None
            engine.insert_release.set()
        with pytest.raises(asyncio.CancelledError):
            await _run_result(task)

        # The insert's own transaction rolled back; no data connection was opened.
        (run_record,) = engine.connections
        assert (run_record.kind, run_record.outcome) == (RUN_RECORD, ROLLED_BACK)
        assert engine.updates == []
        assert pipeline.contexts == []
        assert [record.getMessage() for record in _warnings_and_errors(caplog)] == logged
        assert all(entry.exc_info is None for entry in caplog.records)
        assert "SECRET-INSERT-MARKER" not in caplog.text
        assert _other_tasks() == before
    finally:
        await _finish_run(task, engine, pipeline, before=before)


def _raised_from(error: FetchFailed, cause: BaseException) -> FetchFailed:
    """``error`` as ``raise error from cause`` leaves it, with ``cause`` as its ``__cause__``."""
    try:
        raise error from cause
    except FetchFailed as raised:
        return raised


@pytest.mark.parametrize(
    ("error", "level", "logged", "marker"),
    [
        pytest.param(
            SourceLayoutChanged(
                expected=["year"],
                found=["yr"],
                message="input /srv/private/SECRET-LAYOUT-MARKER.csv token=SECRET-LAYOUT-MARKER",
            ),
            logging.ERROR,
            "layout_changed: expected=['year'] found=['yr']",
            "SECRET-LAYOUT-MARKER",
            id="layout-changed-with-a-path-and-token-in-its-message",
        ),
        pytest.param(
            FetchFailed("/srv/imports/SECRET-PATH-MARKER/prices.csv", "file_not_found"),
            logging.ERROR,
            "failed: fetch_failed:file_not_found",
            "SECRET-PATH-MARKER",
            id="fetch-failed-with-a-file-path",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "see /srv/SECRET-REASON-MARKER"),
            logging.ERROR,
            "failed: fetch_failed",
            "SECRET-REASON-MARKER",
            id="fetch-failed-with-a-path-in-its-reason",
        ),
        pytest.param(
            FetchFailed("/srv/private/PRIVATE_FILE.csv", "PRIVATE_FILE.csv"),
            logging.ERROR,
            "failed: fetch_failed",
            "PRIVATE_FILE",
            id="fetch-failed-with-a-file-name-as-its-reason",
        ),
        pytest.param(
            FetchFailed("https://example.org/SECRET-STATUS-MARKER", "http_503"),
            logging.ERROR,
            "failed: fetch_failed:http_503",
            "SECRET-STATUS-MARKER",
            id="fetch-failed-with-an-http-status",
        ),
        pytest.param(
            FetchFailed("https://example.org/SECRET-ROBOTS-MARKER/robots.txt", "robots_http_503"),
            logging.ERROR,
            "failed: fetch_failed:robots_http_503",
            "SECRET-ROBOTS-MARKER",
            id="fetch-failed-with-a-robots-http-status",
        ),
        pytest.param(
            FetchFailed("https://user:SECRET-USERINFO@example.org/x.csv", "url_has_userinfo"),
            logging.ERROR,
            "failed: fetch_failed:url_has_userinfo",
            "SECRET-USERINFO",
            id="fetch-failed-for-a-url-with-userinfo",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_404 SECRET-HTTP-MARKER"),
            logging.ERROR,
            "failed: fetch_failed",
            "SECRET-HTTP-MARKER",
            id="fetch-failed-with-text-after-an-http-status",
        ),
        pytest.param(
            _raised_from(
                FetchFailed("https://example.org/SECRET-INVALID-MARKER", "invalid_url"),
                httpx.InvalidURL("Invalid URL https://example.org/SECRET-INVALID-MARKER"),
            ),
            logging.ERROR,
            "failed: fetch_failed:invalid_url:InvalidURL",
            "SECRET-INVALID-MARKER",
            id="fetch-failed-with-a-known-code-raised-from-its-cause",
        ),
        pytest.param(
            _raised_from(
                FetchFailed(
                    "/srv/imports/SECRET-UNREADABLE-MARKER/prices.xlsx",
                    "unreadable_file:PermissionError",
                ),
                PermissionError(
                    13, "Permission denied", "/srv/imports/SECRET-UNREADABLE-MARKER/prices.xlsx"
                ),
            ),
            logging.ERROR,
            "failed: fetch_failed:unreadable_file:PermissionError",
            "SECRET-UNREADABLE-MARKER",
            id="fetch-failed-unreadable-file-raised-from-its-cause",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "unreadable_file:SECRET-SUFFIX-MARKER"),
            logging.ERROR,
            "failed: fetch_failed:unreadable_file",
            "SECRET-SUFFIX-MARKER",
            id="fetch-failed-with-text-after-a-known-code",
        ),
        pytest.param(
            _raised_from(
                FetchFailed(
                    "https://example.org/SECRET-RETRY-MARKER", "ConnectTimeout after 4 attempts"
                ),
                httpx.ConnectTimeout("timed out connecting to example.org/SECRET-RETRY-MARKER"),
            ),
            logging.ERROR,
            "failed: fetch_failed:ConnectTimeout",
            "SECRET-RETRY-MARKER",
            id="fetch-failed-after-retries-raised-from-its-cause",
        ),
        *(
            pytest.param(
                FetchFailed(f"/srv/imports/SECRET-XLSX-MARKER/{code}.xlsx", code),
                logging.ERROR,
                f"failed: fetch_failed:{code}",
                "SECRET-XLSX-MARKER",
                id=f"fetch-failed-with-the-xlsx-limit-code-{code}",
            )
            for code in (
                "xlsx_too_many_members",
                "xlsx_expanded_too_large",
                "xlsx_unsupported_member",
                "xlsx_worksheet_too_large",
                "xlsx_parts_too_large",
                "xlsx_size_mismatch",
            )
        ),
        pytest.param(
            _raised_from(
                FetchFailed("/srv/imports/SECRET-INFLATE-MARKER/prices.xlsx", "xlsx_size_mismatch"),
                zlib.error("Error -3 while decompressing data: invalid block type"),
            ),
            logging.ERROR,
            "failed: fetch_failed:xlsx_size_mismatch:error",
            "SECRET-INFLATE-MARKER",
            id="fetch-failed-xlsx-size-mismatch-raised-from-an-inflate-error",
        ),
        pytest.param(
            RuntimeError("token=SECRET-TOKEN-MARKER"),
            logging.ERROR,
            "failed: RuntimeError",
            "SECRET-TOKEN-MARKER",
            id="unexpected-with-a-secret",
        ),
        pytest.param(
            IntegrityError(
                "INSERT INTO terminal_daily_prices (ticker) VALUES ($1::VARCHAR)",
                {"ticker": "SECRET-PARAM-MARKER"},
                Exception("duplicate key"),
            ),
            logging.ERROR,
            "failed: IntegrityError",
            "SECRET-PARAM-MARKER",
            id="sqlalchemy-error-with-bound-parameters",
        ),
        pytest.param(
            RobotsDisallowed("https://example.org/SECRET-URL-MARKER", "robots_unreachable"),
            logging.WARNING,
            "blocked_by_robots: robots_unreachable",
            "SECRET-URL-MARKER",
            id="robots-with-a-url",
        ),
        pytest.param(
            RobotsDisallowed(CSV_URL, "SECRET-ROBOTS-MARKER"),
            logging.WARNING,
            "blocked_by_robots: RobotsDisallowed",
            "SECRET-ROBOTS-MARKER",
            id="robots-with-an-undocumented-reason",
        ),
        pytest.param(
            AmbiguousSession({"reason": "https://example.org/SECRET-EVIDENCE-MARKER"}),
            logging.WARNING,
            "ambiguous_session: AmbiguousSession",
            "SECRET-EVIDENCE-MARKER",
            id="ambiguous-session-with-a-url-reason",
        ),
        pytest.param(
            AmbiguousSession({"reason": "label_mismatch SECRET-AMBIGUOUS-MARKER"}),
            logging.WARNING,
            "ambiguous_session: AmbiguousSession",
            "SECRET-AMBIGUOUS-MARKER",
            id="ambiguous-session-with-a-marker-in-its-reason",
        ),
    ],
)
async def test_run_logs_name_failures_by_code_without_their_text_or_traceback(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    error: BaseException,
    level: int,
    logged: str,
    marker: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    pipeline = _QuotePipeline(settings, cells=["1.00"], fetch_error=error)

    result = await _execute(pipeline, _EngineDouble(), _HttpDouble())

    (record,) = _warnings_and_errors(caplog)
    assert (record.levelno, record.getMessage()) == (
        level,
        f"ingestion run {result.run_id} (test_quotes) {logged}",
    )
    assert all(entry.exc_info is None for entry in caplog.records)
    assert marker not in caplog.text
    assert "Traceback" not in caplog.text


def _literal_fetch_failed_reasons() -> dict[str, str]:
    """Each string literal a module of the ingestion package passes to ``FetchFailed`` as its
    reason, mapped to the first module (its path within the package) that raises it. A reason
    built at run time, such as ``f"http_{status}"`` or ``f"unreadable_file:{type}"``, is not a
    literal; the cases above cover those."""
    package = Path(base.__file__).parent
    reasons: dict[str, str] = {}
    for module in sorted(package.rglob("*.py")):
        for node in ast.walk(ast.parse(module.read_text(encoding="utf-8"), filename=str(module))):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name | ast.Attribute)
                and (node.func.id if isinstance(node.func, ast.Name) else node.func.attr)
                == "FetchFailed"
            ):
                continue
            reason = next(
                (keyword.value for keyword in node.keywords if keyword.arg == "reason"),
                node.args[1] if len(node.args) > 1 else None,
            )
            if isinstance(reason, ast.Constant) and isinstance(reason.value, str):
                reasons.setdefault(reason.value, module.relative_to(package).as_posix())
    return reasons


async def test_every_literal_fetch_failed_reason_of_the_ingestion_package_is_logged_by_its_code(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed run names each reason code an ingestion module raises, so a code added to a
    module but not to the codes a log line may carry fails here rather than logging a bare
    ``fetch_failed``."""
    reasons = _literal_fetch_failed_reasons()
    assert {"http.py", "files/tabular_import.py"} <= set(reasons.values())
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    for reason, module in sorted(reasons.items()):
        caplog.clear()
        error = FetchFailed("/srv/imports/SECRET-GUARD-MARKER/prices.xlsx", reason)
        pipeline = _QuotePipeline(settings, cells=["1.00"], fetch_error=error)

        result = await _execute(pipeline, _EngineDouble(), _HttpDouble())

        # The documented code is the reason up to any ":".
        code = reason.partition(":")[0]
        (record,) = _warnings_and_errors(caplog)
        assert (record.levelno, record.getMessage()) == (
            logging.ERROR,
            f"ingestion run {result.run_id} (test_quotes) failed: fetch_failed:{code}",
        ), f"{reason!r}, raised in {module}"
        assert "SECRET-GUARD-MARKER" not in caplog.text


# ---------------------------------------------------------------------------------------------
# Upsert writers (``opennse_terminal.db.upsert``, other than the price guard's SQL)
#
# The contract held here:
#
# - **Validation before SQL.** Every writer checks its arguments and rows before it builds a
#   statement. An empty conflict key or one naming a non-column, an unknown mode or price source, an
#   empty source, a row key that is not a column or that the writer does not accept, a missing
#   required column, a ``None`` conflict-key value and a ``None`` flow raise ``ValueError``; a
#   ``float`` value, an ``update_flows`` key of the wrong type and a model without a ``Table`` raise
#   ``TypeError``; a column constant absent from its model raises ``RuntimeError``. Nothing is
#   executed.
# - **Nothing to write.** Empty input, and COALESCE-merge rows whose written values are all
#   ``None``, return 0 (``update_flows``: an empty ``FlowUpdateResult``) without touching the
#   executor.
# - **Batching.** Rows are de-duplicated and sorted by key, and each statement carries at most 1,000
#   rows and 32,000 bind parameters, counting the parameter SQLAlchemy renders per row for an absent
#   column with a Python default.
# - **Macro and fundamentals merges.** Each written column becomes
#   ``COALESCE(EXCLUDED.c, stored c)`` and ``source_by_field`` is merged with ``||``, so only the
#   columns a writer supplies change value and provenance (seeded columns stay ``"seed"``). A
#   re-run leaves the data values and ``source_by_field`` provenance unchanged, apart from
#   ``updated_at``, which each later transaction refreshes. A fundamentals row in another currency
#   is skipped; ``period_type`` defaults to ``FY`` and ``currency`` to KES; ``filing_id`` is
#   replaced only when given.
# - **Generic upserts.** ``do_nothing`` never touches a stored row, so a re-run inserts 0;
#   ``do_update`` sets the columns each same-column run supplies, never ``id`` or ``created_at``; a
#   run of key columns only falls back to ``DO NOTHING``.
# - **Prices and flows.** ``csv_import`` replaces any row and owns the flow columns it supplies;
#   ``nse_market_statistics`` replaces seeded and scraped rows only and never writes flows;
#   ``update_flows`` sets flows on existing rows only and reports the missing keys sorted.
# - **Session executors.** Every writer resolves a session's connection for its model's mapper and
#   table, so a host session with no default bind, configured only through ``binds`` (by
#   ``TerminalBase`` or per model) or through a routing ``get_bind`` that reads ``mapper.class_``,
#   writes inside the session's transaction: visible to it, undone by its rollback and kept by its
#   commit.
#
# Pure cases run on stand-in connections. PostgreSQL cases run on the root conftest's
# ``seeded_database`` clone, which marks them ``integration``, because ``Numeric``, ``JSONB`` and
# ``ON CONFLICT`` behaviour must be real. Values a writer does not touch are compared with the row
# read before the write; every written value is an exact ``Decimal`` or literal.
# ---------------------------------------------------------------------------------------------


ANCHOR = date(2026, 9, 30)
"""The last seeded session and macro observation date."""
AFTER_ANCHOR = date(2026, 10, 1)
"""A date after every seeded row: no price, macro or bond row exists on it."""
SCOM_FY = date(2026, 3, 31)
"""Safaricom's seeded FY2026 period end."""
BOND_KEY = ("observation_date", "bond_code")

WriterCall = Callable[[AsyncConnection], Awaitable[object]]


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...


class _RefusingConnection:
    """Stands in for an ``AsyncConnection`` that a writer must never reach."""

    async def execute(self, stmt: Executable) -> object:
        pytest.fail(f"a statement was executed: {type(stmt).__name__}")


REFUSING = cast(AsyncConnection, _RefusingConnection())


class _Result:
    """What ``_count`` reads: ``all()``, one entry per ``RETURNING`` row."""

    def __init__(self, n_rows: int) -> None:
        self._n_rows = n_rows

    def all(self) -> list[tuple[()]]:
        return [()] * self._n_rows


class _ChunkRecorder:
    """Compiles each statement with the PostgreSQL dialect and reports every row as written."""

    ROW_KEY = re.compile(r"code_m\d+")

    def __init__(self) -> None:
        self.params: list[Mapping[str, Any]] = []

    async def execute(self, stmt: Executable) -> _Result:
        params = stmt.compile(dialect=postgresql.dialect()).params
        self.params.append(params)
        return _Result(sum(1 for name in params if self.ROW_KEY.fullmatch(name)))


# A host's custom routing ``Session``, for the tests of how the terminal resolves its bind.
#
# SQLAlchemy's documented custom-partitioning pattern subclasses ``Session`` and overrides
# ``get_bind(mapper=None, clause=None, **kw)``, routing on ``mapper.class_``. Such a session has
# neither a default bind nor ``binds``, so the terminal reaches its database through it only when
# every connection it asks for names the target model's ``Mapper`` (or its ``terminal_`` table as
# the clause), as SQLAlchemy's own ORM statements do. Build one with
# ``AsyncSession(sync_session_class=RoutingSession, terminal_bind=<sync Engine or Connection>)``.
class RoutingSession(Session):
    """Routes the terminal's models to ``terminal_bind`` by mapper, or by table as the clause.

    Anything else falls through to ``Session.get_bind``, which, with no default bind and no
    ``binds``, raises ``UnboundExecutionError``. A mapped class passed where SQLAlchemy passes a
    ``Mapper`` has no ``class_`` and raises ``AttributeError``, as in a host's routing session.
    """

    def __init__(self, *, terminal_bind: Engine | Connection, **kw: Any) -> None:
        super().__init__(**kw)
        self.terminal_bind = terminal_bind

    def get_bind(  # type: ignore[override]
        self,
        mapper: Mapper[Any] | None = None,
        clause: ClauseElement | None = None,
        **kw: Any,
    ) -> Engine | Connection:
        if mapper is not None and issubclass(mapper.class_, TerminalBase):
            return self.terminal_bind
        if isinstance(clause, Table) and clause.name.startswith("terminal_"):
            return self.terminal_bind
        return super().get_bind(mapper, clause=clause, **kw)


def _probe_model(n_values: int) -> type[TerminalBase]:
    """A ``terminal_``-prefixed table that is only compiled, never created: a UUID ``id``, the
    conflict key ``code``, ``n_values`` numeric columns and a ``flag`` whose Python default makes
    SQLAlchemy render one more parameter per row when rows omit it."""
    table = Table(
        "terminal_upsert_probe",
        MetaData(),
        Column("id", Uuid, primary_key=True, default=uuid.uuid4),
        Column("code", String(16), nullable=False, unique=True),
        *(Column(f"v{index:02d}", Numeric(9, 4)) for index in range(n_values)),
        Column("flag", Boolean, nullable=False, default=False),
    )
    return cast(type[TerminalBase], type("TerminalUpsertProbe", (), {"__table__": table}))


async def _one(conn: AsyncConnection, model: type[TerminalBase], **key: object) -> dict[str, Any]:
    """The single row of ``model`` whose columns equal ``key``, as a plain dict."""
    table = model.__table__
    stmt = select(table).where(*(table.c[name] == value for name, value in key.items()))
    return dict((await conn.execute(stmt)).mappings().one())


async def _bond_rows(conn: AsyncConnection, code_pattern: str) -> list[dict[str, Any]]:
    """Every ``terminal_bond_yields`` row whose ``bond_code`` matches ``code_pattern``, in key
    order, as plain dicts."""
    bond = TerminalBondYield.__table__
    stmt = (
        select(bond)
        .where(bond.c["bond_code"].like(code_pattern))
        .order_by(bond.c["observation_date"], bond.c["bond_code"])
    )
    return [dict(row) for row in (await conn.execute(stmt)).mappings()]


def _price(
    ticker: str, trade_date: date, close: str, volume: int, **extra: object
) -> dict[str, object]:
    """A price row; ``turnover_kes`` is ``volume x close`` at the column's 2 dp, as callers
    quantize."""
    close_price = Decimal(close)
    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "close_price": close_price,
        "volume": volume,
        "turnover_kes": (close_price * volume).quantize(Decimal("0.01")),
        **extra,
    }


PRICE_ROW = _price("SCOM", ANCHOR, "33.2500", 2_754_329)
BOND_ROW = {
    "observation_date": AFTER_ANCHOR,
    "bond_code": "FXD1/2026/010",
    "original_tenor_years": Decimal("10.00"),
    "remaining_tenor_years": Decimal("10.00"),
    "maturity_date": date(2036, 9, 22),
    "yield_pct": Decimal("13.7500"),
    "source": "cbk_bonds",
}
FLOW_ROW = {"ticker": "SCOM", "trade_date": ANCHOR, "foreign_net_flow_kes": Decimal("-1250000.00")}
MACRO_ROW = {"observation_date": ANCHOR, "tbill_91d_pct": Decimal("8.7700")}
FUNDAMENTALS_ROW = {
    "ticker": "SCOM",
    "period_end_date": SCOM_FY,
    "revenue_kes": Decimal("388688500000.00"),
}

SCOM_FY2027 = date(2027, 3, 31)
"""A Safaricom period end the seeder does not write."""
NEW_PRICE_ROW = _price("SCOM", AFTER_ANCHOR, "33.9000", 750_000)
NEW_FLOW_ROW = {**FLOW_ROW, "trade_date": AFTER_ANCHOR}
NEW_MACRO_ROW = {**MACRO_ROW, "observation_date": AFTER_ANCHOR}
NEW_FUNDAMENTALS_ROW = {**FUNDAMENTALS_ROW, "period_end_date": SCOM_FY2027}
BINDS_ONLY_MODELS: tuple[type[TerminalBase], ...] = (
    TerminalBondYield,
    TerminalDailyPrice,
    TerminalMacroIndicator,
    TerminalFundamental,
)
"""The models the binds-only session test writes, the only ones its per-model ``binds`` map."""


@pytest.mark.parametrize(
    ("call", "error", "match"),
    [
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [BOND_ROW], (), "do_update"),
            ValueError,
            r"key must name at least one column",
            id="upsert_rows-empty-key",
        ),
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [BOND_ROW], ("bond_kode",), "do_update"),
            ValueError,
            r"key columns \['bond_kode'\] are not columns",
            id="upsert_rows-key-not-a-column",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [BOND_ROW], BOND_KEY, cast(UpsertMode, "merge")
            ),
            ValueError,
            r"unknown mode 'merge'",
            id="upsert_rows-unknown-mode",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [BOND_ROW, {**BOND_ROW, "tenor": 10}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"terminal_bond_yields row 1: 'tenor' is not a column",
            id="upsert_rows-unknown-column-names-the-row",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{"observation_date": AFTER_ANCHOR}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"row 0: missing required columns \['bond_code'\]",
            id="upsert_rows-missing-key-column",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{**BOND_ROW, "bond_code": None}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"conflict-key column 'bond_code' is None",
            id="upsert_rows-none-key-value",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{**BOND_ROW, "yield_pct": 13.75}], BOND_KEY, "do_update"
            ),
            TypeError,
            r"column 'yield_pct' holds a float; pass a Decimal",
            id="upsert_rows-float-value",
        ),
        pytest.param(
            lambda c: upsert_prices(c, [PRICE_ROW], source="cbk_fx", ingestion_run_id=None),
            ValueError,
            r"source 'cbk_fx' is not one of \['seed', 'nse_market_statistics', 'csv_import'\]",
            id="upsert_prices-unknown-source",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c,
                [{**PRICE_ROW, "foreign_net_flow_kes": Decimal("1.00")}],
                source="nse_market_statistics",
                ingestion_run_id=None,
            ),
            ValueError,
            r"column 'foreign_net_flow_kes' is not accepted here",
            id="upsert_prices-flow-only-from-csv_import",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c, [{**PRICE_ROW, "source": "seed"}], source="seed", ingestion_run_id=None
            ),
            ValueError,
            r"column 'source' is not accepted here",
            id="upsert_prices-source-column-in-row",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c,
                [{k: v for k, v in PRICE_ROW.items() if k != "turnover_kes"}],
                source="csv_import",
                ingestion_run_id=None,
            ),
            ValueError,
            r"missing required columns \['turnover_kes'\]",
            id="upsert_prices-missing-turnover",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c, [{**PRICE_ROW, "close_price": 33.25}], source="seed", ingestion_run_id=None
            ),
            TypeError,
            r"column 'close_price' holds a float",
            id="upsert_prices-float-close",
        ),
        pytest.param(
            lambda c: update_flows(c, [FLOW_ROW], flow_source=""),
            ValueError,
            r"update_flows: source must be a non-empty string",
            id="update_flows-empty-source",
        ),
        pytest.param(
            lambda c: update_flows(c, [{**FLOW_ROW, "ticker": 7}], flow_source="csv_import"),
            TypeError,
            r"update_flows row 0: ticker must be str and trade_date a date",
            id="update_flows-ticker-not-str",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [FLOW_ROW, {**FLOW_ROW, "trade_date": "2026-09-30"}], flow_source="csv_import"
            ),
            TypeError,
            r"update_flows row 1: ticker must be str and trade_date a date",
            id="update_flows-trade_date-not-date",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "foreign_net_flow_kes": None}], flow_source="csv_import"
            ),
            ValueError,
            r"update_flows row 0: foreign_net_flow_kes must not be None",
            id="update_flows-none-flow",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "close_price": Decimal("33.25")}], flow_source="csv_import"
            ),
            ValueError,
            r"column 'close_price' is not accepted here",
            id="update_flows-price-column",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "foreign_net_flow_kes": -1250000.0}], flow_source="csv_import"
            ),
            TypeError,
            r"column 'foreign_net_flow_kes' holds a float",
            id="update_flows-float-flow",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [MACRO_ROW], source=""),
            ValueError,
            r"upsert_macro: source must be a non-empty string",
            id="upsert_macro-empty-source",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c, [{**MACRO_ROW, "source_by_field": {}}], source="cbk_rates_csv"
            ),
            ValueError,
            r"column 'source_by_field' is not accepted here",
            id="upsert_macro-provenance-column",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [{"tbill_91d_pct": Decimal("8.77")}], source="cbk_rates_csv"),
            ValueError,
            r"missing required columns \['observation_date'\]",
            id="upsert_macro-missing-date",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c, [{**MACRO_ROW, "observation_date": None}], source="cbk_rates_csv"
            ),
            ValueError,
            r"conflict-key column 'observation_date' is None",
            id="upsert_macro-none-date",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(c, [FUNDAMENTALS_ROW], source=""),
            ValueError,
            r"upsert_fundamentals: source must be a non-empty string",
            id="upsert_fundamentals-empty-source",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "filing_id": uuid.uuid4()}], source="pdf_filing"
            ),
            ValueError,
            r"column 'filing_id' is not accepted here",
            id="upsert_fundamentals-filing_id-in-row",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{"ticker": "SCOM", "revenue_kes": Decimal("1.00")}], source="csv_import"
            ),
            ValueError,
            r"missing required columns \['period_end_date'\]",
            id="upsert_fundamentals-missing-period_end_date",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "ticker": None}], source="csv_import"
            ),
            ValueError,
            r"conflict-key column 'ticker' is None",
            id="upsert_fundamentals-none-ticker",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "dps_kes": 1.2}], source="csv_import"
            ),
            TypeError,
            r"column 'dps_kes' holds a float",
            id="upsert_fundamentals-float-dps",
        ),
    ],
)
async def test_writer_rejects_bad_input_before_any_sql(
    call: WriterCall, error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        await call(REFUSING)


async def test_model_whose_table_is_not_a_table_is_rejected() -> None:
    probe_table = _probe_model(1).__table__
    not_a_table = type("TerminalSubqueryProbe", (), {"__table__": select(probe_table).subquery()})

    with pytest.raises(TypeError, match=r"TerminalSubqueryProbe\.__table__ is not a Table"):
        await upsert_rows(
            REFUSING, cast(type[TerminalBase], not_a_table), [], ("code",), "do_nothing"
        )


def test_column_constant_drift_fails_loudly() -> None:
    with pytest.raises(
        RuntimeError,
        match=r"db\.upsert names columns absent from terminal_macro_indicators: \['cpi_pct'\]",
    ):
        upsert_module._require_columns(TerminalMacroIndicator, ("inflation_yoy_pct", "cpi_pct"))


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [], BOND_KEY, "do_update"),
            0,
            id="upsert_rows-no-rows",
        ),
        pytest.param(
            lambda c: upsert_prices(c, [], source="csv_import", ingestion_run_id=None),
            0,
            id="upsert_prices-no-rows",
        ),
        pytest.param(
            lambda c: update_flows(c, [], flow_source="csv_import"),
            FlowUpdateResult(rows_updated=0, missing_keys=()),
            id="update_flows-no-rows",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [{"observation_date": ANCHOR}], source="cbk_fx"),
            0,
            id="upsert_macro-no-indicator-column",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c,
                [{"observation_date": ANCHOR, "fx_kes_usd": None, "fx_reserves_usd_m": None}],
                source="cbk_fx",
            ),
            0,
            id="upsert_macro-all-values-none",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "revenue_kes": None}], source="csv_import"
            ),
            0,
            id="upsert_fundamentals-all-values-none",
        ),
    ],
)
async def test_nothing_to_write_never_reaches_the_executor(
    call: WriterCall, expected: object
) -> None:
    assert await call(REFUSING) == expected


@pytest.mark.parametrize(
    ("n_values", "expected_chunks"),
    [
        # id, code, v00 and the defaulted flag: 4 parameters a row, so the 1,000-row cap binds.
        pytest.param(1, [1_000, 1_000], id="row-cap"),
        # id, code, v00..v37 and the defaulted flag: 41 a row, so 32,000 // 41 = 780 rows bind.
        pytest.param(38, [780, 780, 440], id="bind-parameter-cap"),
    ],
)
async def test_statements_stay_within_row_and_bind_parameter_bounds(
    n_values: int, expected_chunks: list[int]
) -> None:
    model = _probe_model(n_values)
    values = {f"v{index:02d}": Decimal("1.2500") for index in range(n_values)}
    # Offered in descending key order: the writer sorts them.
    rows = [{"code": f"W{n:04d}", **values} for n in reversed(range(2_000))]
    snapshot = copy.deepcopy(rows)
    recorder = _ChunkRecorder()

    written = await upsert_rows(
        cast(AsyncConnection, recorder), model, rows, ("code",), "do_update"
    )

    assert written == 2_000
    params_per_row = n_values + 3
    assert [len(params) for params in recorder.params] == [
        size * params_per_row for size in expected_chunks
    ]
    assert max(len(params) for params in recorder.params) <= 32_000
    start = 0
    for params, size in zip(recorder.params, expected_chunks, strict=True):
        # Each statement covers the key range right after the previous statement's.
        first, last = params["code_m0"], params[f"code_m{size - 1}"]
        assert (first, last) == (f"W{start:04d}", f"W{start + size - 1:04d}")
        start += size
    assert rows == snapshot


async def test_macro_merge_coalesces_and_relabels_only_written_columns(
    seeded_database: SeededDatabase,
) -> None:
    october = date(2026, 10, 31)
    # CBK's published September 2026 figures replace the seeded ones; CBR is not in this release.
    rows = [
        {
            "observation_date": ANCHOR,
            "tbill_91d_pct": Decimal("8.7700"),
            "interbank_rate_pct": Decimal("8.7500"),
            "cbr_rate_pct": None,
        },
        {"observation_date": october, "tbill_91d_pct": Decimal("8.6900")},
        # A repeated date: the last row for it wins.
        {"observation_date": october, "tbill_91d_pct": Decimal("8.6100")},
    ]
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)

        written = await upsert_macro(conn, rows, source="cbk_rates_csv")

        merged = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)
        inserted = await _one(conn, TerminalMacroIndicator, observation_date=october)
        rerun = await upsert_macro(conn, rows, source="cbk_rates_csv")
        after_rerun = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)

    assert written == 2
    assert before["source_by_field"]["cbr_rate_pct"] == "seed"
    assert merged["tbill_91d_pct"] == Decimal("8.7700")
    assert merged["interbank_rate_pct"] == Decimal("8.7500")
    unwritten = ("inflation_yoy_pct", "cbr_rate_pct", "tbill_182d_pct", "tbill_364d_pct")
    for name in (*unwritten, "fx_kes_usd", "fx_reserves_usd_m", "id", "created_at"):
        assert merged[name] == before[name], name
    assert merged["source_by_field"] == {
        **before["source_by_field"],
        "tbill_91d_pct": "cbk_rates_csv",
        "interbank_rate_pct": "cbk_rates_csv",
    }
    assert merged["updated_at"] > before["updated_at"]
    assert inserted["tbill_91d_pct"] == Decimal("8.6100")
    assert inserted["source_by_field"] == {"tbill_91d_pct": "cbk_rates_csv"}
    for name in upsert_module.MACRO_INDICATOR_COLUMNS:
        if name != "tbill_91d_pct":
            assert inserted[name] is None, name
    assert rerun == 2
    assert after_rerun == merged


async def test_fundamentals_merge_keeps_unwritten_columns_and_links_the_filing(
    seeded_database: SeededDatabase,
) -> None:
    sha256 = "a" * 64
    filing = {
        "ticker": "SCOM",
        "document_type": "annual_report",
        "period_end_date": SCOM_FY,
        "period_type": "FY",
        "file_name": "safaricom-fy2026.pdf",
        "file_sha256": sha256,
        "file_size_bytes": 48_211,
        "page_count": 3,
        "extraction": [],
        "status": "partial",
    }
    key = {"ticker": "SCOM", "period_end_date": SCOM_FY, "period_type": "FY"}
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalFundamental, **key)
        filed = await upsert_rows(
            conn, TerminalCorporateFiling, [filing], ("file_sha256",), "do_update"
        )
        filing_id = (await _one(conn, TerminalCorporateFiling, file_sha256=sha256))["id"]

        # period_type is absent: the row merges into the seeded FY row.
        from_pdf = await upsert_fundamentals(
            conn,
            [
                {
                    "ticker": "SCOM",
                    "period_end_date": SCOM_FY,
                    "revenue_kes": Decimal("388688500000.00"),
                    "net_income_kes": Decimal("84500000000.00"),
                    "ebitda_kes": None,
                }
            ],
            source="pdf_filing",
            filing_id=filing_id,
        )
        after_pdf = await _one(conn, TerminalFundamental, **key)
        from_csv = await upsert_fundamentals(
            conn, [{**key, "dps_kes": Decimal("1.2000")}], source="csv_import"
        )
        after_csv = await _one(conn, TerminalFundamental, **key)

    assert filed == 1
    assert from_pdf == 1
    assert before["source_by_field"]["ebitda_kes"] == "seed"
    assert after_pdf["revenue_kes"] == Decimal("388688500000.00")
    assert after_pdf["net_income_kes"] == Decimal("84500000000.00")
    for name in (*upsert_module.FUNDAMENTAL_VALUE_COLUMNS, "id", "created_at", "currency"):
        if name not in ("revenue_kes", "net_income_kes"):
            assert after_pdf[name] == before[name], name
    assert after_pdf["source_by_field"] == {
        **before["source_by_field"],
        "revenue_kes": "pdf_filing",
        "net_income_kes": "pdf_filing",
    }
    assert after_pdf["filing_id"] == filing_id
    assert after_pdf["updated_at"] > before["updated_at"]
    assert from_csv == 1
    assert after_csv["dps_kes"] == Decimal("1.2000")
    assert after_csv["filing_id"] == filing_id
    assert after_csv["source_by_field"] == {**after_pdf["source_by_field"], "dps_kes": "csv_import"}


async def test_fundamentals_currency_guard_and_key_defaults(
    seeded_database: SeededDatabase,
) -> None:
    seeded_key = {"ticker": "SCOM", "period_end_date": SCOM_FY, "period_type": "FY"}
    eabl_fy = date(2027, 6, 30)
    kcb_h1 = date(2026, 6, 30)
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalFundamental, **seeded_key)

        usd = await upsert_fundamentals(
            conn,
            [{**seeded_key, "currency": "USD", "revenue_kes": Decimal("3000000000.00")}],
            source="csv_import",
        )
        unchanged = await _one(conn, TerminalFundamental, **seeded_key)
        inserted = await upsert_fundamentals(
            conn,
            [
                # An absent and an explicit FY are one key; the later row wins.
                {"ticker": "EABL", "period_end_date": eabl_fy, "revenue_kes": Decimal("1.00")},
                {
                    "ticker": "EABL",
                    "period_end_date": eabl_fy,
                    "period_type": "FY",
                    "currency": None,
                    "revenue_kes": Decimal("131200000000.00"),
                },
                {
                    "ticker": "KCB",
                    "period_end_date": kcb_h1,
                    "period_type": "H1",
                    "net_income_kes": Decimal("33900000000.00"),
                },
            ],
            source="csv_import",
        )
        eabl = await _one(conn, TerminalFundamental, ticker="EABL", period_end_date=eabl_fy)
        kcb = await _one(conn, TerminalFundamental, ticker="KCB", period_end_date=kcb_h1)

    assert usd == 0
    assert unchanged == before
    assert inserted == 2
    assert (eabl["period_type"], eabl["currency"]) == ("FY", "KES")
    assert eabl["revenue_kes"] == Decimal("131200000000.00")
    assert eabl["net_income_kes"] is None
    assert eabl["source_by_field"] == {"revenue_kes": "csv_import"}
    assert eabl["filing_id"] is None
    assert (kcb["period_type"], kcb["currency"]) == ("H1", "KES")
    assert kcb["net_income_kes"] == Decimal("33900000000.00")
    assert kcb["revenue_kes"] is None
    assert kcb["source_by_field"] == {"net_income_kes": "csv_import"}


async def test_do_nothing_reruns_insert_zero_across_chunks(seeded_database: SeededDatabase) -> None:
    bond = TerminalBondYield.__table__
    rows = [
        {**BOND_ROW, "bond_code": f"FXD-TEST-{n:04d}", "isin": None, "coupon_rate_pct": None}
        for n in range(2_500)
    ]
    repriced = [{**row, "yield_pct": Decimal("14.1250")} for row in rows]
    test_codes = bond.c["bond_code"].like("FXD-TEST-%")
    async with seeded_database.engine.begin() as conn:
        seeded = await _bond_rows(conn, "FXD-SEED-%")

        reseeded = await upsert_rows(conn, TerminalBondYield, seeded, BOND_KEY, "do_nothing")
        first = await upsert_rows(conn, TerminalBondYield, rows, BOND_KEY, "do_nothing")
        ids = set((await conn.execute(select(bond.c["id"]).where(test_codes))).scalars())
        rerun = await upsert_rows(conn, TerminalBondYield, repriced, BOND_KEY, "do_nothing")
        kept = set((await conn.execute(select(bond.c["yield_pct"]).where(test_codes))).scalars())
        updated = await upsert_rows(conn, TerminalBondYield, repriced, BOND_KEY, "do_update")
        after = (
            await conn.execute(select(bond.c["id"], bond.c["yield_pct"]).where(test_codes))
        ).all()
        seeded_after = await _bond_rows(conn, "FXD-SEED-%")

    assert len(seeded) > 0
    assert reseeded == 0
    assert seeded_after == seeded
    assert first == 2_500
    assert len(ids) == 2_500
    assert rerun == 0
    assert kept == {Decimal("13.7500")}
    assert updated == 2_500
    assert {row_id for row_id, _ in after} == ids
    assert {yield_pct for _, yield_pct in after} == {Decimal("14.1250")}


async def test_do_update_writes_each_runs_supplied_columns_only(
    seeded_database: SeededDatabase,
) -> None:
    async with seeded_database.engine.begin() as conn:
        eabl_before = await _one(conn, TerminalEquity, ticker="EABL")
        scom_before = await _one(conn, TerminalEquity, ticker="SCOM")
        required = ("ticker", "company_name", "sector", "shares_outstanding")
        rows: list[dict[str, object]] = [
            # Sorted by ticker, then written as same-column runs: [EABL, NMG], then [SCOM].
            {
                **{name: scom_before[name] for name in required},
                "isin": scom_before["isin"],
                "free_float_pct": Decimal("35.0000"),
                "source": "csv_import",
            },
            {**{name: eabl_before[name] for name in required}, "source": "csv_import"},
            {
                "ticker": "NMG",
                "company_name": "Nation Media Group PLC",
                "sector": "Commercial & Services",
                "shares_outstanding": 207_396_515,
                "source": "csv_import",
            },
        ]
        snapshot = copy.deepcopy(rows)

        written = await upsert_rows(conn, TerminalEquity, rows, ("ticker",), "do_update")

        eabl = await _one(conn, TerminalEquity, ticker="EABL")
        scom = await _one(conn, TerminalEquity, ticker="SCOM")
        nmg = await _one(conn, TerminalEquity, ticker="NMG")

    assert written == 3
    assert rows == snapshot
    assert eabl["source"] == "csv_import"
    for name in ("id", "created_at", "isin", "free_float_pct", "currency"):
        assert eabl[name] == eabl_before[name], name
    assert eabl["updated_at"] > eabl_before["updated_at"]
    assert scom["free_float_pct"] == Decimal("35.0000")
    assert (scom["id"], scom["created_at"]) == (scom_before["id"], scom_before["created_at"])
    assert nmg["currency"] == "KES"
    assert (nmg["isin"], nmg["free_float_pct"]) == (None, None)
    assert nmg["shares_outstanding"] == 207_396_515


async def test_composite_key_without_id_and_key_only_update(
    seeded_database: SeededDatabase,
) -> None:
    items = TerminalWatchlistItem.__table__
    key = ("watchlist_id", "ticker")
    async with seeded_database.engine.begin() as conn:
        watchlist = await _one(conn, TerminalWatchlist, owner_key="local", name="NSE Core 8")
        on_list = items.c["watchlist_id"] == watchlist["id"]
        await conn.execute(delete(items).where(on_list, items.c["ticker"] == "KCB"))
        kcb = {"watchlist_id": watchlist["id"], "ticker": "KCB"}
        scom = {"watchlist_id": watchlist["id"], "ticker": "SCOM"}

        added = await upsert_rows(conn, TerminalWatchlistItem, [scom, kcb], key, "do_nothing")
        # Only key columns: nothing to update, so DO NOTHING applies even in do_update mode.
        key_only = await upsert_rows(conn, TerminalWatchlistItem, [kcb, scom], key, "do_update")
        count = (
            await conn.execute(select(func.count()).select_from(items).where(on_list))
        ).scalar_one()

    assert added == 1
    assert key_only == 0
    assert count == 8


async def test_price_precedence_and_flow_ownership(seeded_database: SeededDatabase) -> None:
    scom = {"ticker": "SCOM", "trade_date": ANCHOR}
    kcb = {"ticker": "KCB", "trade_date": ANCHOR}
    run_id = uuid.uuid4()
    run = {"id": run_id, "pipeline": "nse_market_statistics", "trigger": "cli", "status": "running"}
    async with seeded_database.engine.begin() as conn:
        seeded = await _one(conn, TerminalDailyPrice, **scom)
        kcb_seeded = await _one(conn, TerminalDailyPrice, **kcb)
        run_written = await upsert_rows(conn, TerminalIngestionRun, [run], ("id",), "do_nothing")

        scraped = await upsert_prices(
            conn,
            [_price("SCOM", ANCHOR, "33.5000", 1_000_000, turnover_estimated=True)],
            source="nse_market_statistics",
            ingestion_run_id=run_id,
        )
        after_scrape = await _one(conn, TerminalDailyPrice, **scom)
        imported = await upsert_prices(
            conn,
            [
                _price(
                    "SCOM",
                    ANCHOR,
                    "33.7500",
                    900_000,
                    open_price=Decimal("33.0000"),
                    high_price=Decimal("34.0000"),
                    low_price=Decimal("32.7500"),
                    foreign_net_flow_kes=Decimal("-1250000.00"),
                )
            ],
            source="csv_import",
            ingestion_run_id=None,
        )
        after_import = await _one(conn, TerminalDailyPrice, **scom)
        rescraped = await upsert_prices(
            conn,
            [_price("SCOM", ANCHOR, "34.0000", 500_000, turnover_estimated=True)],
            source="nse_market_statistics",
            ingestion_run_id=None,
        )
        after_rescrape = await _one(conn, TerminalDailyPrice, **scom)
        flowless = await upsert_prices(
            conn,
            [
                _price("SCOM", AFTER_ANCHOR, "33.9000", 750_000),
                _price("KCB", ANCHOR, "85.2500", 400_000, foreign_net_flow_kes=None),
            ],
            source="csv_import",
            ingestion_run_id=None,
        )
        new_row = await _one(conn, TerminalDailyPrice, ticker="SCOM", trade_date=AFTER_ANCHOR)
        kcb_after = await _one(conn, TerminalDailyPrice, **kcb)

    assert run_written == 1
    assert seeded["source"] == "seed"
    assert seeded["flow_source"] == "seed"
    assert scraped == 1
    assert after_scrape["source"] == "nse_market_statistics"
    assert after_scrape["ingestion_run_id"] == run_id
    assert after_scrape["close_price"] == Decimal("33.5000")
    assert after_scrape["volume"] == 1_000_000
    assert after_scrape["turnover_kes"] == Decimal("33500000.00")
    assert after_scrape["turnover_estimated"] is True
    assert (after_scrape["open_price"], after_scrape["high_price"]) == (None, None)
    assert after_scrape["low_price"] is None
    for name in ("id", "foreign_net_flow_kes", "flow_source"):
        assert after_scrape[name] == seeded[name], name
    assert after_scrape["ingested_at"] > seeded["ingested_at"]

    assert imported == 1
    assert after_import["source"] == "csv_import"
    assert after_import["ingestion_run_id"] is None
    assert after_import["open_price"] == Decimal("33.0000")
    assert after_import["high_price"] == Decimal("34.0000")
    assert after_import["low_price"] == Decimal("32.7500")
    assert after_import["close_price"] == Decimal("33.7500")
    assert after_import["turnover_kes"] == Decimal("30375000.00")
    assert after_import["turnover_estimated"] is False
    assert after_import["foreign_net_flow_kes"] == Decimal("-1250000.00")
    assert after_import["flow_source"] == "csv_import"

    assert rescraped == 0
    assert after_rescrape == after_import

    assert flowless == 2
    assert (new_row["source"], new_row["close_price"]) == ("csv_import", Decimal("33.9000"))
    assert (new_row["foreign_net_flow_kes"], new_row["flow_source"]) == (None, None)
    assert kcb_after["source"] == "csv_import"
    assert kcb_after["close_price"] == Decimal("85.2500")
    assert kcb_after["foreign_net_flow_kes"] == kcb_seeded["foreign_net_flow_kes"]
    assert kcb_after["flow_source"] == "seed"


async def test_update_flows_sets_existing_rows_and_reports_missing_keys(
    seeded_database: SeededDatabase,
) -> None:
    key = {"ticker": "SCOM", "trade_date": ANCHOR}
    rows = [
        {**key, "foreign_net_flow_kes": Decimal("5000000.00")},
        {"ticker": "SCOM", "trade_date": AFTER_ANCHOR, "foreign_net_flow_kes": Decimal("1.00")},
        {"ticker": "EQTY", "trade_date": AFTER_ANCHOR, "foreign_net_flow_kes": Decimal("2.00")},
        # The last row for a key wins.
        {**key, "foreign_net_flow_kes": Decimal("-750000.00")},
    ]
    # A session executor: the writer joins the session's transaction.
    async with AsyncSession(seeded_database.engine) as session, session.begin():
        conn = await session.connection()
        before = await _one(conn, TerminalDailyPrice, **key)

        outcome = await update_flows(session, rows, flow_source="csv_import")

        after = await _one(conn, TerminalDailyPrice, **key)
        stray = (
            await conn.execute(
                select(func.count())
                .select_from(TerminalDailyPrice)
                .where(TerminalDailyPrice.trade_date == AFTER_ANCHOR)
            )
        ).scalar_one()

    assert outcome == FlowUpdateResult(
        rows_updated=1, missing_keys=(("EQTY", AFTER_ANCHOR), ("SCOM", AFTER_ANCHOR))
    )
    assert after["foreign_net_flow_kes"] == Decimal("-750000.00")
    assert after["flow_source"] == "csv_import"
    unchanged = {name: value for name, value in before.items() if "flow" not in name}
    assert {name: after[name] for name in unchanged} == unchanged
    assert stray == 0


async def _new_row_values(executor: AsyncSession | AsyncConnection) -> tuple[object, ...]:
    """The values the binds-only writes set, ``None`` where a row is absent.

    Read with ORM column selects, so a session without a default bind resolves each statement's
    bind from its model, as its own queries do.
    """
    statements = (
        select(TerminalBondYield.yield_pct).where(
            TerminalBondYield.observation_date == AFTER_ANCHOR,
            TerminalBondYield.bond_code == BOND_ROW["bond_code"],
        ),
        select(TerminalDailyPrice.close_price).where(
            TerminalDailyPrice.ticker == "SCOM", TerminalDailyPrice.trade_date == AFTER_ANCHOR
        ),
        select(TerminalDailyPrice.foreign_net_flow_kes).where(
            TerminalDailyPrice.ticker == "SCOM", TerminalDailyPrice.trade_date == AFTER_ANCHOR
        ),
        select(TerminalMacroIndicator.tbill_91d_pct).where(
            TerminalMacroIndicator.observation_date == AFTER_ANCHOR
        ),
        select(TerminalFundamental.revenue_kes).where(
            TerminalFundamental.ticker == "SCOM",
            TerminalFundamental.period_end_date == SCOM_FY2027,
            TerminalFundamental.period_type == "FY",
        ),
    )
    return tuple([(await executor.execute(stmt)).scalar_one_or_none() for stmt in statements])


async def _write_with_every_writer(session: AsyncSession) -> tuple[object, ...]:
    """Run each writer once through ``session`` and return what each reports.

    ``update_flows`` targets the price row ``upsert_prices`` inserts just before it, which it finds
    only inside the same transaction.
    """
    return (
        await upsert_rows(session, TerminalBondYield, [BOND_ROW], BOND_KEY, "do_update"),
        await upsert_prices(session, [NEW_PRICE_ROW], source="csv_import", ingestion_run_id=None),
        await update_flows(session, [NEW_FLOW_ROW], flow_source="csv_import"),
        await upsert_macro(session, [NEW_MACRO_ROW], source="cbk_rates_csv"),
        await upsert_fundamentals(session, [NEW_FUNDAMENTALS_ROW], source="csv_import"),
    )


@pytest.mark.parametrize(
    "session_options",
    [
        pytest.param(lambda engine: {"binds": {TerminalBase: engine}}, id="base-class-binds"),
        pytest.param(
            lambda engine: {"binds": {model: engine for model in BINDS_ONLY_MODELS}},
            id="per-model-binds",
        ),
        pytest.param(
            lambda engine: {
                "sync_session_class": RoutingSession,
                "terminal_bind": engine.sync_engine,
            },
            id="routing-get-bind",
        ),
    ],
)
async def test_every_writer_joins_a_binds_only_session(
    seeded_database: SeededDatabase,
    session_options: Callable[[AsyncEngine], dict[str, Any]],
) -> None:
    engine = seeded_database.engine
    written = (1, 1, FlowUpdateResult(rows_updated=1, missing_keys=()), 1, 1)
    values = (
        BOND_ROW["yield_pct"],
        NEW_PRICE_ROW["close_price"],
        NEW_FLOW_ROW["foreign_net_flow_kes"],
        NEW_MACRO_ROW["tbill_91d_pct"],
        NEW_FUNDAMENTALS_ROW["revenue_kes"],
    )
    absent = (None,) * len(values)
    # No default bind: only the binds mapping, or the routing get_bind, locates the database.
    session = AsyncSession(**session_options(engine))
    assert session.sync_session.bind is None
    async with engine.connect() as observer:
        assert await _new_row_values(observer) == absent

        async with session:
            rolled_back = await _write_with_every_writer(session)
            in_session = await _new_row_values(session)
            # The writes are uncommitted session state, not autocommitted on another connection.
            outside_before_rollback = await _new_row_values(observer)
            await session.rollback()
            after_rollback = await _new_row_values(observer)

            committed = await _write_with_every_writer(session)
            await session.commit()
        after_commit = await _new_row_values(observer)

    assert rolled_back == written
    assert in_session == values
    assert outside_before_rollback == absent
    assert after_rollback == absent
    assert committed == written
    assert after_commit == values


# ---------------------------------------------------------------------------------------------
# Price precedence guard (``opennse_terminal.db.upsert.upsert_prices``)
#
# ``csv_import``, the highest-ranked writer, must replace a stored row whatever its ``source``, so
# its ``ON CONFLICT DO UPDATE`` carries no ``WHERE``. A lower writer updates only rows whose stored
# source is listed at or below its rank, as a bound ``IN`` list, never inline literals.
#
# Every case runs without PostgreSQL: a recording connection captures each statement, which is
# compiled with the PostgreSQL dialect.
# ---------------------------------------------------------------------------------------------


ROW = {
    "ticker": "SCOM",
    "trade_date": date(2026, 9, 30),
    "close_price": Decimal("25.0000"),
    "volume": 1_000,
    "turnover_kes": Decimal("25000.00"),
}
CONFLICT_UPDATE = "ON CONFLICT (ticker, trade_date) DO UPDATE SET "
# The IN list must hold bound placeholders only, so an inline literal fails the match.
GUARD_RE = re.compile(
    r"DO UPDATE SET .* WHERE terminal_daily_prices\.source IN \(((?:%\(\w+\)s(?:, )?)+)\)"
)
PLACEHOLDER_RE = re.compile(r"%\((\w+)\)s")


class _PriceResult:
    """What ``_count`` reads: ``all()``, one entry per ``RETURNING`` row."""

    def __init__(self, returned: list[tuple[object, ...]]) -> None:
        self._returned = returned

    def all(self) -> list[tuple[object, ...]]:
        return self._returned


class _RecordingConnection:
    """Stands in for an ``AsyncConnection``: records each statement and reports one written row."""

    def __init__(self) -> None:
        self.statements: list[Executable] = []

    async def execute(self, stmt: Executable) -> _PriceResult:
        self.statements.append(stmt)
        return _PriceResult([(ROW["ticker"], ROW["trade_date"])])


@pytest.mark.parametrize(
    ("source", "overwritable"),
    [
        pytest.param("csv_import", None, id="csv_import-overwrites-any-source"),
        pytest.param(
            "nse_market_statistics",
            ("seed", "nse_market_statistics"),
            id="nse_market_statistics-seed-and-nse-only",
        ),
        pytest.param("seed", ("seed",), id="seed-only-seed"),
    ],
)
async def test_conflict_guard_by_writer_rank(
    source: str, overwritable: tuple[str, ...] | None
) -> None:
    conn = _RecordingConnection()

    written = await upsert_prices(
        cast(AsyncConnection, conn), [ROW], source=source, ingestion_run_id=None
    )

    assert written == 1
    assert len(conn.statements) == 1
    compiled = conn.statements[0].compile(
        dialect=postgresql.dialect(), compile_kwargs={"render_postcompile": True}
    )
    sql = compiled.string
    assert CONFLICT_UPDATE in sql
    for name in ("seed", "nse_market_statistics", "csv_import"):
        assert f"'{name}'" not in sql
    if overwritable is None:
        assert "WHERE" not in sql
        return
    guard = GUARD_RE.search(sql)
    assert guard is not None
    placeholders = PLACEHOLDER_RE.findall(guard.group(1))
    assert tuple(compiled.params[name] for name in placeholders) == overwritable


# ---------------------------------------------------------------------------------------------
# Console script (``opennse_terminal.cli``)
#
# Unit tests of the ``opennse-terminal`` console script, ``opennse_terminal.cli``.
#
# They run ``cli.main`` in-process, with no database: handlers and the work they call are
# replaced where a test needs them to fail or to record their arguments. The integration suite
# covers the commands against PostgreSQL (``test_migrations_isolation.py`` and
# ``test_seeder.py``).
# ---------------------------------------------------------------------------------------------


_CLI_LOGGER = "opennse_terminal.cli"
# Never connected to: the migration functions are replaced in every test that loads settings.
_UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://u:p@127.0.0.1:1/x"


# --- migrate log lines ----------------------------------------------------------------------

# A revision as an attacker would type it: free text, then a forged log record. Each payload is
# paired with the escape that must stand for it in the log line.
_FORGED_RECORD = "2026-10-29 00:00:00,000 ERROR opennse_terminal.cli forged line"
_REVISION_PAYLOADS = [
    pytest.param("\r\n", "\\r\\n", id="crlf"),
    pytest.param("\n", "\\n", id="lf"),
    pytest.param("\x85", "\\x85", id="nel"),
    pytest.param("\u2028", "\\u2028", id="line-separator"),
    pytest.param("\x1b[31m", "\\x1b[31m", id="ansi-escape"),
    pytest.param("\\", "\\\\", id="backslash"),
]


def _alembic_error(revision: str) -> Exception:
    """Alembic's unknown-revision error, which quotes the revision as typed."""
    return CommandError(f"Can't locate revision identified by '{revision}'")


def _os_error(revision: str) -> Exception:
    """An ``OSError`` whose text carries the same characters as the revision."""
    return ConnectionError(f"terminal migration to {revision} could not connect: refused")


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
@pytest.mark.parametrize(
    ("make_error", "exit_code", "level", "quotes"),
    [
        pytest.param(None, cli.EXIT_OK, logging.INFO, 1, id="migrated"),
        pytest.param(_alembic_error, cli.EXIT_FAILED, logging.ERROR, 2, id="alembic-error"),
        pytest.param(_os_error, cli.EXIT_FAILED, logging.ERROR, 2, id="os-error"),
    ],
)
@pytest.mark.parametrize(("payload", "escaped"), _REVISION_PAYLOADS)
def test_migrate_log_line_escapes_revision_and_reason(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    payload: str,
    escaped: str,
    make_error: Callable[[str], Exception] | None,
    exit_code: int,
    level: int,
    quotes: int,
    direction: str,
) -> None:
    """The migrate outcome is one log line, the revision and reason escaped; Alembic gets it raw.

    ``quotes`` counts the escaped revision in the line: once in the success template, and twice
    on failure, where the reason quotes it again.
    """
    revision = f"nope{payload}{_FORGED_RECORD}"
    calls: list[tuple[str, str]] = []

    def fake_migrate(database_url: str, target_revision: str) -> None:
        calls.append((database_url, target_revision))
        if make_error is not None:
            raise make_error(target_revision)

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_migrate imports both functions when it runs, so it picks up the fakes.
    monkeypatch.setattr(migrations, "run_upgrade", fake_migrate)
    monkeypatch.setattr(migrations, "run_downgrade", fake_migrate)
    caplog.set_level(logging.INFO, logger=_CLI_LOGGER)

    assert cli.main(["migrate", direction, revision]) == exit_code

    assert calls == [(_UNREACHABLE_DATABASE_URL, revision)]
    records = [record for record in caplog.records if record.name == _CLI_LOGGER]
    assert [record.levelno for record in records] == [level]
    message = records[0].getMessage()
    assert len(message.splitlines()) == 1
    assert message.isprintable(), f"raw control or separator character in {message!r}"
    shown = f"nope{escaped}{_FORGED_RECORD}"
    assert message.count(shown) == quotes
    assert f'{direction} to {shown} on database "x" on 127.0.0.1:1' in message


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("head", "head", id="head"),
        pytest.param("terminal_0001", "terminal_0001", id="revision-id"),
        pytest.param(
            "Can't locate revision identified by 'x'",
            "Can't locate revision identified by 'x'",
            id="alembic-message",
        ),
        pytest.param("Ksh — Nairobi é 日本", "Ksh — Nairobi é 日本", id="non-ascii-letters"),
        pytest.param("", "", id="empty"),
        pytest.param("a\rb\nc\td", "a\\rb\\nc\\td", id="cr-lf-tab"),
        pytest.param(
            "\x00\x07\x0b\x0c\x1b\x1c\x1d\x1e\x1f\x7f",
            "\\x00\\x07\\x0b\\x0c\\x1b\\x1c\\x1d\\x1e\\x1f\\x7f",
            id="other-c0-and-del",
        ),
        pytest.param("\x85\x9b\xa0", "\\x85\\x9b\\xa0", id="c1-and-no-break-space"),
        pytest.param("\u2028\u2029", "\\u2028\\u2029", id="line-and-paragraph-separators"),
        pytest.param("\u200b\ufeff\udcff", "\\u200b\\ufeff\\udcff", id="format-and-surrogate"),
        pytest.param("\U000e0001", "\\U000e0001", id="astral-format"),
        pytest.param("C:\\new\\x85", "C:\\\\new\\\\x85", id="backslash-doubled"),
    ],
)
def test_migrate_log_text_escapes_only_unprintable(text: str, expected: str) -> None:
    """Printable text is unchanged; every other character, and the backslash, is escaped."""
    assert cli._log_text(text) == expected


# --- migrate REVISION argument --------------------------------------------------------------

# Text that no revision can match. Alembic would reject each only after connecting, most of them
# with an AssertionError that carries no message.
_BLANK_REVISION = "must not be empty or whitespace only, got {}"
_NOTHING_AFTER_AT = (
    "no revision follows '@' in {}; expected LABEL@REVISION, such as terminal_0001@+1"
)
_UNMATCHABLE_REVISIONS = [
    pytest.param("", _BLANK_REVISION.format("''"), id="empty"),
    pytest.param(" ", _BLANK_REVISION.format("' '"), id="space"),
    pytest.param("\t\n", _BLANK_REVISION.format("'\\t\\n'"), id="tab-newline"),
    pytest.param("\u2028", _BLANK_REVISION.format("'\\u2028'"), id="line-separator"),
    pytest.param("@", _NOTHING_AFTER_AT.format("'@'"), id="at-sign"),
    pytest.param("@@", _NOTHING_AFTER_AT.format("'@@'"), id="two-at-signs"),
    pytest.param("head@", _NOTHING_AFTER_AT.format("'head@'"), id="head-at"),
    pytest.param("terminal_0001@", _NOTHING_AFTER_AT.format("'terminal_0001@'"), id="id-at"),
    pytest.param("x@y@", _NOTHING_AFTER_AT.format("'x@y@'"), id="label-revision-at"),
]

# Revision syntax Alembic parses, beside the rejected shapes: an empty label before "@" and
# surrounding whitespace included, which Alembic resolves or reports itself.
_ALEMBIC_REVISION_SYNTAX = {
    "head": "head",
    "heads": "heads",
    "base": "base",
    "step-up": "+1",
    "step-down": "-1",
    "revision-id": "terminal_0001",
    "label-step": "terminal_0001@+1",
    "heads-label-step": "heads@-1",
    "empty-label": "@head",
    "leading-space": " head",
    "space-after-at": "head@ ",
}


def _recording_migrate(
    monkeypatch: pytest.MonkeyPatch, error: Callable[[], Exception] | None = None
) -> list[tuple[str, str]]:
    """Replace both migration functions with one recording its calls and raising ``error()``."""
    calls: list[tuple[str, str]] = []

    def fake_migrate(database_url: str, target_revision: str) -> None:
        calls.append((database_url, target_revision))
        if error is not None:
            raise error()

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_migrate imports both functions when it runs, so it picks up the fakes.
    monkeypatch.setattr(migrations, "run_upgrade", fake_migrate)
    monkeypatch.setattr(migrations, "run_downgrade", fake_migrate)
    return calls


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
@pytest.mark.parametrize(("revision", "message"), _UNMATCHABLE_REVISIONS)
def test_migrate_rejects_a_revision_nothing_can_match(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    revision: str,
    message: str,
    direction: str,
) -> None:
    """Empty or whitespace-only text, and text ending in ``@``, is a usage error (exit 2) that
    names the problem on one line after the usage line, and Alembic is never called."""
    calls = _recording_migrate(monkeypatch)

    assert cli.main(["migrate", direction, revision]) == 2

    assert calls == []
    usage, error = capsys.readouterr().err.splitlines()
    assert usage.startswith(f"usage: {cli.PROG} migrate {direction} [-h] ")
    assert error == f"{cli.PROG} migrate {direction}: error: argument REVISION: {message}"


@pytest.mark.parametrize(
    ("argv", "revision"),
    [
        pytest.param(["upgrade"], "head", id="upgrade-default"),
        *(
            pytest.param([direction, text], text, id=f"{direction}-{name}")
            for direction in ("upgrade", "downgrade")
            for name, text in _ALEMBIC_REVISION_SYNTAX.items()
        ),
    ],
)
def test_migrate_passes_alembic_revision_syntax_unchanged(
    monkeypatch: pytest.MonkeyPatch, argv: list[str], revision: str
) -> None:
    """Every other revision, and upgrade's default ``head``, reaches Alembic exactly as typed."""
    calls = _recording_migrate(monkeypatch)

    assert cli.main(["migrate", *argv]) == cli.EXIT_OK

    assert calls == [(_UNREACHABLE_DATABASE_URL, revision)]


def _revision_map_assertion() -> AssertionError:
    """The ``AssertionError`` without a message that Alembic's revision map raises for
    ``head+1``, a step past the head, its traceback ending inside the map. No database is read."""
    script = ScriptDirectory.from_config(migrations.build_alembic_config())
    with pytest.raises(AssertionError) as raised:
        list(script.iterate_revisions("head+1", "base"))
    assert str(raised.value) == ""
    return raised.value


_UNRESOLVED = "Alembic could not resolve it from the database's current revision"


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
@pytest.mark.parametrize(
    ("revision", "error", "reason"),
    [
        pytest.param(
            "head+1",
            _revision_map_assertion,
            f"invalid revision 'head+1': {_UNRESOLVED}",
            id="revision-map",
        ),
        pytest.param(
            f"nope\r\n{_FORGED_RECORD}",
            _revision_map_assertion,
            f"invalid revision 'nope\\r\\n{_FORGED_RECORD}': {_UNRESOLVED}",
            id="revision-map-escaped",
        ),
        pytest.param(
            "head+1",
            lambda: AssertionError("internal detail"),
            "AssertionError",
            id="other-assertion",
        ),
    ],
)
def test_migrate_names_the_revision_an_alembic_assertion_rejects(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    revision: str,
    error: Callable[[], Exception],
    reason: str,
    direction: str,
) -> None:
    """An ``AssertionError`` from Alembic's revision map is logged as the revision being
    invalid, escaped onto the one ERROR line; one raised anywhere else is named by its type
    only, and its text is never logged."""
    calls = _recording_migrate(monkeypatch, error)
    caplog.set_level(logging.INFO, logger=_CLI_LOGGER)

    assert cli.main(["migrate", direction, revision]) == cli.EXIT_FAILED

    assert calls == [(_UNREACHABLE_DATABASE_URL, revision)]
    records = [record for record in caplog.records if record.name == _CLI_LOGGER]
    assert [record.levelno for record in records] == [logging.ERROR]
    message = records[0].getMessage()
    assert len(message.splitlines()) == 1
    assert message.isprintable(), f"raw control or separator character in {message!r}"
    assert message.startswith(f"migrate {direction} to ")
    assert message.endswith(f' on database "x" on 127.0.0.1:1 failed: {reason}')
    assert "internal detail" not in message


# --- Subcommand help ------------------------------------------------------------------------

# Phrases every subcommand help but seed's carries: argparse shows the top-level epilog, which
# names the configuration source, only in the top-level help.
_SETTINGS_SOURCE = (
    "TERMINAL_DATABASE_URL",
    ".env in the working directory",
    'README.md, section "Configuration"',
)
# Settings the pipelines read, which the help of ingest and scheduler name.
_PIPELINE_SETTINGS = (
    "TERMINAL_SCRAPER_CONTACT",
    "TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS",
    "TERMINAL_SCRAPER_TIMEOUT_SECONDS",
    "TERMINAL_SCRAPER_MAX_RETRIES",
    "TERMINAL_TIMEZONE",
)
_CBK_ORDER = "cbk runs cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, cbk_reserves in that order"


def _help_text(capsys: pytest.CaptureFixture[str]) -> str:
    """Captured stdout with each run of whitespace made one space, so wrapped phrases match."""
    return " ".join(capsys.readouterr().out.split())


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        pytest.param(["migrate", "--help"], (), id="migrate"),
        pytest.param(["migrate", "upgrade", "--help"], (), id="migrate-upgrade"),
        pytest.param(["migrate", "downgrade", "--help"], (), id="migrate-downgrade"),
        pytest.param(["ingest", "--help"], (*_PIPELINE_SETTINGS, _CBK_ORDER), id="ingest"),
        pytest.param(["import", "--help"], (), id="import"),
        pytest.param(["scheduler", "--help"], _PIPELINE_SETTINGS, id="scheduler"),
    ],
)
def test_help_names_the_configuration_source(
    capsys: pytest.CaptureFixture[str], argv: list[str], expected: tuple[str, ...]
) -> None:
    """Each subcommand's own help names where its settings come from, and the settings the
    pipelines read where it runs them; ingest keeps its note on the cbk order."""
    assert cli.main(argv) == cli.EXIT_OK

    text = _help_text(capsys)
    for phrase in (*_SETTINGS_SOURCE, *expected):
        assert phrase in text, f"{phrase!r} missing from {' '.join(argv)}"


def test_help_of_seed_is_the_seeders_own(capsys: pytest.CaptureFixture[str]) -> None:
    """seed --help is the seeder's help under the console script's name, without the CLI note."""
    assert cli.main(["seed", "--help"]) == cli.EXIT_OK

    out = capsys.readouterr().out
    assert out.startswith("usage: opennse-terminal seed [-h] [--database-url URL]\n")
    assert "TERMINAL_DATABASE_URL (or .env in the working directory)" in " ".join(out.split())
    assert 'README.md, section "Configuration"' not in " ".join(out.split())


# --- ingest, import and scheduler dispatch --------------------------------------------------

# The exit code of a recorded run by its final status, as the console script documents it.
# Written out rather than read from cli.EXIT_CODES, so a changed mapping fails here; any other
# status, running included, exits 1.
_EXIT_CODE_BY_STATUS = MappingProxyType(
    {
        "succeeded": 0,
        "partial": 0,
        "no_new_data": 0,
        "failed": 1,
        "layout_changed": 3,
        "blocked_by_robots": 4,
        "ambiguous_session": 5,
        "running": 1,
    }
)
# The run order of ingest cbk.
_CBK_RUN_ORDER = ("cbk_rates_csv", "cbk_inflation", "cbk_fx", "cbk_bonds", "cbk_reserves")
_CLI_RUN_ID = uuid.UUID("6d1f0c4e-2b7a-4e59-9c3d-8a5b7e1f2c40")
_SCHEDULER_MODULE = "opennse_terminal.ingestion.scheduler"
# A database URL with the leak marker (_LEAK_MARKER, defined with the boundary tests below) as
# its password, filled in when a test runs.
_MARKED_URL_TEMPLATE = "postgresql+asyncpg://u:{marker}@127.0.0.1:1/x"


@dataclass(frozen=True)
class _PipelineCall:
    """One ``registry.run_pipeline`` call, as the ``ingest`` handler made it."""

    name: str
    settings: TerminalSettings
    trigger: RunTrigger
    trade_date: date | None
    limit: int | None
    spacing: HostSpacing | None


def _cli_run_result(pipeline: str, status: str) -> RunResult:
    """A recorded run of ``pipeline`` ending ``status``: 7 rows fetched, 5 written, 2 rejected.

    Its ``error_message`` carries the leak marker, which the outcome line must not repeat.
    """
    return RunResult(
        run_id=_CLI_RUN_ID,
        pipeline=pipeline,
        status=RunStatus(status),
        rows_fetched=7,
        rows_written=5,
        rows_rejected=2,
        error_message=f"lost {_MARKED_DATABASE_URL}",
        details={},
    )


def _outcome_line(label: str, pipeline: str, status: str, exit_code: int) -> str:
    """The one INFO line the console script logs for a run returned by ``_cli_run_result``."""
    return (
        f"{label}: pipeline={pipeline} status={status} exit_code={exit_code} fetched=7 "
        "written=5 rejected=2 run_id=6d1f0c4e-2b7a-4e59-9c3d-8a5b7e1f2c40"
    )


def _record_pipeline_runs(
    monkeypatch: pytest.MonkeyPatch, outcomes: Mapping[str, str | type[Exception]]
) -> list[_PipelineCall]:
    """Replace ``registry.run_pipeline`` with a recorder, and return the calls it records.

    A call returns ``_cli_run_result`` with the status ``outcomes`` gives its name, or raises the
    exception type given instead, its text carrying the leak marker. No pipeline, engine or run
    row is created. ``TERMINAL_DATABASE_URL`` is set to ``_UNREACHABLE_DATABASE_URL``.
    """
    calls: list[_PipelineCall] = []

    async def run_pipeline(
        name: str,
        settings: TerminalSettings,
        *,
        trigger: RunTrigger,
        trade_date: date | None = None,
        limit: int | None = None,
        spacing: HostSpacing | None = None,
    ) -> RunResult:
        calls.append(_PipelineCall(name, settings, trigger, trade_date, limit, spacing))
        outcome = outcomes[name]
        if isinstance(outcome, str):
            return _cli_run_result(name, outcome)
        raise outcome(f"the run row could not be written to {_MARKED_DATABASE_URL}")

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _ingest looks run_pipeline up on the registry module when it runs, so it calls the fake.
    monkeypatch.setattr(registry, "run_pipeline", run_pipeline)
    return calls


def _assert_no_leak(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]) -> None:
    """Neither a log line nor stdout or stderr carries the leak marker."""
    captured = capsys.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in output


def test_cli_dispatch_scheduler_runs_run_forever_with_the_environment_settings(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """``scheduler`` calls ``ingestion.scheduler.run_forever`` once, with ``TerminalSettings``
    read from the environment, and exits 0 when it returns after a stop signal."""
    calls: list[TerminalSettings | None] = []

    def run_forever(settings: TerminalSettings | None = None) -> None:
        calls.append(settings)

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_scheduler imports run_forever when it runs, so it calls the fake.
    monkeypatch.setattr(f"{_SCHEDULER_MODULE}.run_forever", run_forever)
    caplog.set_level(logging.INFO)

    assert cli.main(["scheduler"]) == 0

    (settings,) = calls
    assert isinstance(settings, TerminalSettings)
    assert settings.database_url == _UNREACHABLE_DATABASE_URL
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.mark.parametrize(
    ("argv", "database_url", "error_type", "error_text", "exit_code", "runs", "error", "usage"),
    [
        pytest.param(
            ["scheduler"],
            _MARKED_URL_TEMPLATE,
            RuntimeError,
            "job store lost on " + _MARKED_URL_TEMPLATE,
            1,
            1,
            re.escape("scheduler stopped on an error: RuntimeError"),
            None,
            id="error-named-by-type",
        ),
        pytest.param(
            ["scheduler"],
            _MARKED_URL_TEMPLATE,
            OSError,
            f"cannot reach 127.0.0.1:1\r\n{_FORGED_RECORD}",
            1,
            1,
            re.escape(
                f"scheduler stopped on an error: cannot reach 127.0.0.1:1\\r\\n{_FORGED_RECORD}"
            ),
            None,
            id="os-error-text-escaped",
        ),
        pytest.param(
            ["scheduler"],
            "postgresql://u:{marker}@127.0.0.1:1/x",
            None,
            "",
            1,
            0,
            r"invalid terminal settings: (?s:.*)database_url must use the asyncpg driver(?s:.*)",
            None,
            id="invalid-settings",
        ),
        pytest.param(
            ["scheduler", "extra"],
            _MARKED_URL_TEMPLATE,
            None,
            "",
            2,
            0,
            None,
            f"{cli.PROG}: error: unrecognized arguments: extra\n",
            id="extra-argument",
        ),
    ],
)
def test_cli_dispatch_scheduler_failure_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    database_url: str,
    error_type: type[Exception] | None,
    error_text: str,
    exit_code: int,
    runs: int,
    error: str | None,
    usage: str | None,
) -> None:
    """A scheduler that raises exits 1 after one ERROR line: an ``OSError`` with its text
    escaped, anything else by its type only. Invalid settings exit 1 with one ERROR line and an
    extra argument exits 2 through argparse, both before the scheduler is started. The database
    password reaches no log line or stream.

    ``database_url`` and ``error_text`` are templates whose ``{marker}`` is the leak marker;
    ``error`` is the pattern the one ERROR line matches in full, ``usage`` the end of stderr.
    """
    calls: list[TerminalSettings | None] = []

    def run_forever(settings: TerminalSettings | None = None) -> None:
        calls.append(settings)
        if error_type is not None:
            raise error_type(error_text.format(marker=_LEAK_MARKER))

    monkeypatch.setenv("TERMINAL_DATABASE_URL", database_url.format(marker=_LEAK_MARKER))
    monkeypatch.setattr(f"{_SCHEDULER_MODULE}.run_forever", run_forever)
    caplog.set_level(logging.INFO)

    assert cli.main(argv) == exit_code

    assert len(calls) == runs
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert [record.name for record in errors] == ([] if error is None else [_CLI_LOGGER])
    if error is not None:
        assert re.fullmatch(error, errors[0].getMessage()), errors[0].getMessage()
        assert errors[0].exc_info is None
    captured = capsys.readouterr()
    if usage is None:
        assert "error:" not in captured.err
    else:
        assert captured.err.endswith(usage)
    for output in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in output
        assert "Traceback" not in output


@pytest.mark.parametrize(
    ("argv", "label", "pipeline"),
    [
        pytest.param(["ingest", "cbk_fx"], "ingest cbk_fx", "cbk_fx", id="ingest"),
        pytest.param(
            ["import", "daily_prices", "{csv}"],
            "import daily_prices",
            "csv_import:daily_prices",
            id="import",
        ),
    ],
)
@pytest.mark.parametrize("status", list(RunStatus), ids=str)
def test_cli_dispatch_run_status_becomes_the_documented_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    status: RunStatus,
    argv: list[str],
    label: str,
    pipeline: str,
) -> None:
    """A recorded run's status is the exit code of ``ingest NAME`` and ``import KIND PATH``
    alike: succeeded, partial and no_new_data 0, failed 1, layout_changed 3, blocked_by_robots
    4, ambiguous_session 5, and running 1. The run is logged as one INFO line with its counts and
    run id, never its error message."""
    exit_code = _EXIT_CODE_BY_STATUS[status]
    csv_path = write_csv(tmp_path, "prices.csv", "ticker,trade_date,close_price,volume", [])
    imports: list[tuple[str, str | Path, str | None, str | None, RunTrigger]] = []

    async def import_file(
        kind: str,
        path: str | Path,
        sheet: str | None = None,
        *,
        settings: TerminalSettings | None = None,
        trigger: RunTrigger = "cli",
    ) -> RunResult:
        url = None if settings is None else settings.database_url
        imports.append((kind, path, sheet, url, trigger))
        return _cli_run_result(f"csv_import:{kind}", status)

    runs = _record_pipeline_runs(monkeypatch, {"cbk_fx": status})
    # _cmd_import imports import_file when it runs, so it calls the fake.
    monkeypatch.setattr(tabular_import, "import_file", import_file)
    caplog.set_level(logging.INFO, logger=_CLI_LOGGER)

    assert cli.main([token.format(csv=csv_path) for token in argv]) == exit_code

    if argv[0] == "ingest":
        assert [(run.name, run.trigger) for run in runs] == [("cbk_fx", "cli")]
        assert imports == []
    else:
        assert runs == []
        assert imports == [("daily_prices", csv_path, None, _UNREACHABLE_DATABASE_URL, "cli")]
    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == _CLI_LOGGER
    ] == [(logging.INFO, _outcome_line(label, pipeline, status, exit_code))]
    _assert_no_leak(caplog, capsys)


@pytest.mark.parametrize(
    ("runs", "exit_code", "summary"),
    [
        pytest.param(
            [
                ("succeeded", 0),
                ("partial", 0),
                ("no_new_data", 0),
                ("succeeded", 0),
                ("partial", 0),
            ],
            0,
            "ingest cbk: exit_code=0 (cbk_rates_csv=0, cbk_inflation=0, cbk_fx=0, cbk_bonds=0, "
            "cbk_reserves=0)",
            id="every-run-exits-0",
        ),
        pytest.param(
            [
                ("succeeded", 0),
                ("layout_changed", 3),
                ("failed", 1),
                ("blocked_by_robots", 4),
                ("succeeded", 0),
            ],
            3,
            "ingest cbk: exit_code=3 (cbk_rates_csv=0, cbk_inflation=3, cbk_fx=1, cbk_bonds=4, "
            "cbk_reserves=0)",
            id="first-non-zero-neither-last-nor-largest",
        ),
        pytest.param(
            [
                ("succeeded", 0),
                ("succeeded", 0),
                ("no_new_data", 0),
                ("succeeded", 0),
                ("blocked_by_robots", 4),
            ],
            4,
            "ingest cbk: exit_code=4 (cbk_rates_csv=0, cbk_inflation=0, cbk_fx=0, cbk_bonds=0, "
            "cbk_reserves=4)",
            id="only-the-last-run-fails",
        ),
        pytest.param(
            [
                ("succeeded", 0),
                (RuntimeError, 1),
                ("layout_changed", 3),
                ("succeeded", 0),
                ("succeeded", 0),
            ],
            1,
            "ingest cbk: exit_code=1 (cbk_rates_csv=0, cbk_inflation=1, cbk_fx=3, cbk_bonds=0, "
            "cbk_reserves=0)",
            id="a-raising-run-counts-1-and-the-rest-still-run",
        ),
    ],
)
def test_cli_dispatch_ingest_cbk_runs_five_pipelines_and_exits_with_the_first_non_zero_code(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    runs: list[tuple[str | type[Exception], int]],
    exit_code: int,
    summary: str,
) -> None:
    """``ingest cbk`` makes one ``run_pipeline`` call per CBK pipeline, in the order
    cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, cbk_reserves, each triggered ``cli`` with no
    trade date or limit, the environment's settings and one ``HostSpacing`` shared by all five.

    ``runs`` gives each run's status, or the exception type its call raises, with the exit code
    that run counts. Each run logs its outcome line, a raising run one ERROR line by type, and the
    sequence always runs to the end; the command then logs the summary and exits with the code of
    the first run that did not exit 0, or 0.
    """
    outcomes = dict(zip(_CBK_RUN_ORDER, (outcome for outcome, _code in runs), strict=True))
    calls = _record_pipeline_runs(monkeypatch, outcomes)
    caplog.set_level(logging.INFO, logger=_CLI_LOGGER)

    assert cli.main(["ingest", "cbk"]) == exit_code

    assert [call.name for call in calls] == list(_CBK_RUN_ORDER)
    options = [(call.trigger, call.trade_date, call.limit) for call in calls]
    assert options == [("cli", None, None)] * 5
    assert [call.settings.database_url for call in calls] == [_UNREACHABLE_DATABASE_URL] * 5
    spacing = calls[0].spacing
    assert isinstance(spacing, HostSpacing)
    assert all(call.spacing is spacing for call in calls)
    expected: list[tuple[int, str]] = []
    for name, (outcome, code) in zip(_CBK_RUN_ORDER, runs, strict=True):
        if isinstance(outcome, str):
            expected.append((logging.INFO, _outcome_line(f"ingest {name}", name, outcome, code)))
        else:
            line = f"ingest {name}: the run could not be recorded: {outcome.__name__}"
            expected.append((logging.ERROR, line))
    expected.append((logging.INFO, summary))
    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == _CLI_LOGGER
    ] == expected
    _assert_no_leak(caplog, capsys)


@pytest.mark.parametrize(
    ("argv", "trade_date", "limit"),
    [
        pytest.param(
            ["ingest", "nse_market_statistics", "--trade-date", "2026-10-27"],
            date(2026, 10, 27),
            None,
            id="trade-date",
        ),
        pytest.param(["ingest", "cbk_reserves", "--limit", "5"], None, 5, id="limit"),
        pytest.param(["ingest", "cbk_fx"], None, None, id="no-options"),
    ],
)
def test_cli_dispatch_ingest_forwards_its_options_to_one_run(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    trade_date: date | None,
    limit: int | None,
) -> None:
    """``ingest NAME`` makes one ``run_pipeline`` call for NAME, triggered ``cli``, with
    ``--trade-date`` as a ``date`` and ``--limit`` as an ``int``, each ``None`` when not given,
    the environment's settings and a ``HostSpacing``; its succeeded run exits 0."""
    name = argv[1]
    calls = _record_pipeline_runs(monkeypatch, {name: "succeeded"})

    assert cli.main(argv) == 0

    (call,) = calls
    assert (call.name, call.trigger) == (name, "cli")
    assert (call.trade_date, call.limit) == (trade_date, limit)
    assert call.settings.database_url == _UNREACHABLE_DATABASE_URL
    assert isinstance(call.spacing, HostSpacing)


# --- import path check ----------------------------------------------------------------------

# A regular file the import check accepts; nothing reads it, since the handler stops first.
_IMPORT_CHECK_CSV = "ticker,trade_date,close_price,volume\nSCOM,2026-10-27,28.45,1000\n"
_NEEDS_FIFOS = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="the platform has no FIFOs")
# Flags that would let the check write, create or truncate the operator's file.
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND


class _OpenSpy:
    """``os`` as ``cli`` sees it, recording each ``open`` of one path and each ``close`` of its
    descriptors.

    Patched in as ``cli.os`` only, so no other caller of ``os`` meets the hooks. ``before`` runs
    once, just before the path is opened; ``error``, when given, is raised instead of opening it.
    """

    def __init__(
        self,
        path: Path,
        *,
        before: Callable[[], None] | None = None,
        error: OSError | None = None,
    ) -> None:
        self.path = os.fspath(path)
        self.flags: list[int] = []
        self.opened: list[int] = []
        self.closed: list[int] = []
        self._before = before
        self._error = error

    def __getattr__(self, name: str) -> Any:
        return getattr(os, name)

    def open(self, path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if os.fspath(path) != self.path:
            return os.open(path, flags, mode)
        self.flags.append(flags)
        if self._before is not None:
            before, self._before = self._before, None
            before()
        if self._error is not None:
            raise self._error
        fd = os.open(path, flags, mode)
        self.opened.append(fd)
        return fd

    def close(self, fd: int) -> None:
        if fd in self.opened and fd not in self.closed:
            self.closed.append(fd)
        os.close(fd)


def _release_fifo_readers(path: Path) -> None:
    """Open the FIFO at ``path`` for writing and close it, so a reader blocked opening it returns.

    A regression that opened the path blocking would otherwise leave its worker thread waiting
    for a writer forever, and the test process could not exit after the timeout failed the test.
    """
    if not path.is_fifo():
        return
    with contextlib.suppress(OSError):  # ENXIO: no reader is waiting
        os.close(os.open(path, os.O_WRONLY | os.O_NONBLOCK))


@pytest.fixture
def import_check_handler(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each time the ``import`` handler is reached, and stop it there with exit 1.

    The handler loads settings only after ``_check_import`` accepted the arguments, so an empty
    list means the check rejected them. ``import_file`` fails the test if it is ever called.
    """
    reached: list[str] = []

    def load_settings() -> None:
        reached.append("settings")

    def import_file(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail(f"import_file was called with {args} {kwargs}")

    # Both are looked up when the handler runs: the settings as a module global, the importer
    # by its lazy import.
    monkeypatch.setattr(cli, "_load_settings", load_settings)
    monkeypatch.setattr(tabular_import, "import_file", import_file)
    return reached


def _import_error(message: str) -> str:
    """The line argparse writes to stderr for an invalid ``import`` argument."""
    return f"{cli.PROG} import: error: {message}\n"


@pytest.mark.parametrize(
    ("denial", "reason"),
    [
        pytest.param(
            PermissionError(errno.EACCES, "Permission denied"), "Permission denied", id="eacces"
        ),
        pytest.param(PermissionError(), "PermissionError", id="no-strerror"),
        pytest.param(None, "Permission denied", id="mode-000"),
    ],
)
def test_import_check_rejects_an_unreadable_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    denial: OSError | None,
    reason: str,
) -> None:
    """A regular file that cannot be opened is an argument error naming the OS reason, or the
    error's type where it has none, and no settings, engine or run row is reached."""
    target = tmp_path / "prices.csv"
    target.write_text(_IMPORT_CHECK_CSV)
    if denial is None:
        target.chmod(0)
        # Root with CAP_DAC_OVERRIDE, or a platform without POSIX modes, still reads the file.
        with contextlib.suppress(PermissionError):
            os.close(os.open(target, os.O_RDONLY))
            pytest.skip("this process reads a mode-000 file")
    else:
        monkeypatch.setattr(cli, "os", _OpenSpy(target, error=denial))

    assert cli.main(["import", "daily_prices", str(target)]) == 2

    err = capsys.readouterr().err
    assert err.startswith(f"usage: {cli.PROG} import ")
    assert err.endswith(_import_error(f"cannot read {target}: {reason}"))
    assert import_check_handler == []


@pytest.mark.parametrize(
    ("name", "extra", "message"),
    [
        pytest.param("missing.csv", [], "{path} does not exist or is not a file", id="missing"),
        pytest.param("prices.txt", [], "{path} must be a .csv or .xlsx file", id="suffix"),
        pytest.param(
            "prices.csv",
            ["--sheet", "Prices"],
            "--sheet applies only to .xlsx files",
            id="sheet-with-csv",
        ),
    ],
)
def test_import_check_reports_argument_errors_before_opening_the_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    name: str,
    extra: list[str],
    message: str,
) -> None:
    """A missing path, a wrong suffix and ``--sheet`` with a CSV are reported as such, even for
    a file that could not be read: the file is opened only after every other check passed."""
    target = tmp_path / name
    if name != "missing.csv":
        target.write_text(_IMPORT_CHECK_CSV)
    spy = _OpenSpy(target, error=PermissionError(errno.EACCES, "Permission denied"))
    monkeypatch.setattr(cli, "os", spy)

    assert cli.main(["import", "daily_prices", str(target), *extra]) == 2

    assert capsys.readouterr().err.endswith(_import_error(message.format(path=target)))
    assert spy.flags == []
    assert import_check_handler == []


@pytest.mark.parametrize(
    "swapped",
    [
        pytest.param(False, id="at-the-stat-check"),
        pytest.param(True, id="swapped-in-before-the-open"),
    ],
)
@pytest.mark.parametrize("kind", [pytest.param("fifo", marks=_NEEDS_FIFOS), "directory"])
async def test_import_check_rejects_a_path_that_is_not_a_regular_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    kind: str,
    swapped: bool,
) -> None:
    """A FIFO or directory is rejected without blocking, whether the stat check sees it or it
    replaces the file between that check and the open, which ``fstat`` then rejects; the
    descriptor of that open is closed."""
    target = tmp_path / "prices.csv"

    def make_non_regular() -> None:
        if kind == "fifo":
            os.mkfifo(target)
        else:
            target.mkdir()

    def swap() -> None:
        target.unlink()
        make_non_regular()

    if swapped:
        target.write_text(_IMPORT_CHECK_CSV)
        spy = _OpenSpy(target, before=swap)
    else:
        make_non_regular()
        spy = _OpenSpy(target)
    monkeypatch.setattr(cli, "os", spy)
    try:
        # Bounded: a FIFO opened blocking would wait for a writer forever.
        code = await asyncio.wait_for(
            asyncio.to_thread(cli.main, ["import", "daily_prices", str(target)]), timeout=5
        )
    finally:
        _release_fifo_readers(target)

    assert code == 2
    assert capsys.readouterr().err.endswith(
        _import_error(f"{target} does not exist or is not a file")
    )
    assert import_check_handler == []
    assert len(spy.opened) == int(swapped)
    assert spy.closed == spy.opened


@pytest.mark.parametrize(
    ("name", "extra"),
    [
        pytest.param("prices.csv", [], id="csv"),
        pytest.param("prices.XLSX", ["--sheet", "Prices"], id="xlsx-with-sheet"),
    ],
)
def test_import_check_accepts_a_readable_file_after_a_read_only_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    name: str,
    extra: list[str],
) -> None:
    """A readable file passes after one read-only, non-blocking open whose descriptor is closed,
    and the handler is reached (exit 1 from the stubbed settings)."""
    target = tmp_path / name
    target.write_text(_IMPORT_CHECK_CSV)
    spy = _OpenSpy(target)
    monkeypatch.setattr(cli, "os", spy)

    assert cli.main(["import", "daily_prices", str(target), *extra]) == cli.EXIT_FAILED

    assert "error:" not in capsys.readouterr().err
    assert import_check_handler == ["settings"]
    assert len(spy.flags) == 1
    assert not spy.flags[0] & _WRITE_FLAGS
    assert spy.flags[0] & os.O_NONBLOCK
    assert len(spy.opened) == 1
    assert spy.closed == spy.opened


# --- Entry-point boundary and logging ownership ---------------------------------------------

# Stands for a database password in exception text: it must never reach a log line or a stream.
_LEAK_MARKER = "s3cr3t-marker-7f1c"
# Never connected to, like _UNREACHABLE_DATABASE_URL, but its password is the marker.
_MARKED_DATABASE_URL = f"postgresql+asyncpg://u:{_LEAK_MARKER}@127.0.0.1:1/x"
_SEED_MODULE = "opennse_terminal.seed.seed_terminal_db"
_INTERRUPTED_LINE = "interrupted; uncommitted changes were rolled back"
_TERMINATED_LINE = "terminated (SIGTERM); uncommitted changes were rolled back"


def _fail_building_parsers(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make parser construction raise with the marker in its text; no command is parsed yet."""

    def build_parsers() -> NoReturn:
        raise RuntimeError(f"cannot build the parsers for {_MARKED_DATABASE_URL}")

    monkeypatch.setattr(cli, "_build_parsers", build_parsers)
    return cli.PROG


def _fail_alembic_import(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make the migrate handler's lazy ``alembic.util`` import fail, as with Alembic missing."""
    monkeypatch.setitem(sys.modules, "alembic.util", None)
    return "migrate"


def _fail_seed_handler(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make the seed handler raise a ``ValueError`` whose text quotes the marker's URL."""

    def cmd_seed(seed_argv: list[str]) -> NoReturn:
        raise ValueError(f"invalid database URL {_MARKED_DATABASE_URL} in {seed_argv}")

    monkeypatch.setattr(cli, "_cmd_seed", cmd_seed)
    return "seed"


@pytest.mark.parametrize(
    ("break_cli", "argv", "error_type"),
    [
        pytest.param(
            _fail_building_parsers,
            ["migrate", "upgrade", "head"],
            "RuntimeError",
            id="parser-construction",
        ),
        pytest.param(
            _fail_alembic_import,
            ["migrate", "upgrade", "head"],
            "ModuleNotFoundError",
            id="lazy-alembic-import",
        ),
        pytest.param(_fail_seed_handler, ["seed"], "ValueError", id="handler-error"),
    ],
)
def test_boundary_unexpected_error_is_one_error_line_by_type(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    break_cli: Callable[[pytest.MonkeyPatch], str],
    argv: list[str],
    error_type: str,
) -> None:
    """An exception escaping parsing or a handler returns 1 with one ERROR line that names the
    command and the exception's type only: no text, no traceback, so no password."""
    monkeypatch.setenv("TERMINAL_DATABASE_URL", _MARKED_DATABASE_URL)
    command = break_cli(monkeypatch)
    caplog.set_level(logging.INFO)

    assert cli.main(argv) == cli.EXIT_FAILED

    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert [(record.name, record.getMessage()) for record in errors] == [
        (_CLI_LOGGER, f"{command} failed on an unexpected error: {error_type}")
    ]
    assert errors[0].exc_info is None
    assert errors[0].stack_info is None
    captured = capsys.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in output
        assert "Traceback" not in output


def _interrupt_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deliver Ctrl-C while the parsers are being built."""

    def build_parsers() -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_build_parsers", build_parsers)


def _interrupt_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deliver Ctrl-C from inside the migrate handler's migration call."""

    def run_upgrade(database_url: str, revision: str) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_migrate imports run_upgrade when it runs, so it picks up the fake.
    monkeypatch.setattr(migrations, "run_upgrade", run_upgrade)


@pytest.mark.parametrize(
    "interrupt",
    [
        pytest.param(_interrupt_parsing, id="while-parsing"),
        pytest.param(_interrupt_migration, id="in-migrate-handler"),
    ],
)
def test_boundary_ctrl_c_returns_130_with_one_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    interrupt: Callable[[pytest.MonkeyPatch], None],
) -> None:
    """Ctrl-C, before or after a command is parsed, is one WARNING line and exit 130."""
    interrupt(monkeypatch)
    caplog.set_level(logging.INFO)

    assert cli.main(["migrate", "upgrade", "head"]) == cli.EXIT_INTERRUPTED

    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(_CLI_LOGGER, logging.WARNING, _INTERRUPTED_LINE)]


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["ingest", "cbk_fx"], id="ingest"),
        pytest.param(["import", "daily_prices", "{csv}"], id="import"),
    ],
)
def test_boundary_sigterm_in_ingest_or_import_returns_143_with_one_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    argv: list[str],
) -> None:
    """``ingest`` and ``import`` run their coroutine through ``run_terminable``; the
    ``TerminatedError`` it raises after SIGTERM is one WARNING line and exit 143."""
    csv_path = write_csv(tmp_path, "prices.csv", "ticker,trade_date,close_price,volume", [])
    ran: list[str] = []

    def terminated(main: Any) -> NoReturn:
        ran.append(main.__qualname__)
        main.close()
        raise cli.TerminatedError("terminated by SIGTERM")

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    monkeypatch.setattr(cli, "run_terminable", terminated)
    caplog.set_level(logging.INFO)

    code = cli.main([token.format(csv=csv_path) for token in argv])

    assert code == cli.EXIT_TERMINATED == 143
    assert ran == ["_ingest" if argv[0] == "ingest" else "import_file"]
    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(_CLI_LOGGER, logging.WARNING, _TERMINATED_LINE)]


def _host_sigterm_handler(signum: int, frame: FrameType | None) -> None:
    """A host application's own SIGTERM handler. No signal is sent, so it never runs."""
    raise AssertionError("no SIGTERM is sent in this test")


def _sigterm_disposition_inside_run_terminable() -> tuple[object, str]:
    """SIGTERM's disposition while ``run_terminable`` runs its coroutine, and the result."""

    async def read_disposition() -> tuple[object, str]:
        return signal.getsignal(signal.SIGTERM), "done"

    return cli.run_terminable(read_disposition())


@pytest.mark.parametrize(
    ("disposition", "in_worker_thread", "installed"),
    [
        pytest.param(signal.SIG_DFL, False, True, id="main-thread-default"),
        pytest.param(signal.SIG_IGN, False, False, id="ignored"),
        pytest.param(_host_sigterm_handler, False, False, id="host-handler"),
        pytest.param(signal.SIG_DFL, True, False, id="worker-thread"),
    ],
)
def test_run_terminable_routes_sigterm_only_from_the_main_thread_at_its_default(
    disposition: Any, in_worker_thread: bool, installed: bool
) -> None:
    """While the coroutine runs, SIGTERM is routed to the event loop only in the main thread and
    only when its disposition was ``SIG_DFL``, the rule ``asyncio.run`` applies to SIGINT: a
    host's handler, ``SIG_IGN`` and a worker thread's call leave it untouched. Afterwards the
    disposition is the one found. The disposition is only read; no signal is sent."""
    previous = signal.signal(signal.SIGTERM, disposition)
    try:
        if in_worker_thread:
            outcomes: list[tuple[object, str]] = []
            worker = threading.Thread(
                target=lambda: outcomes.append(_sigterm_disposition_inside_run_terminable())
            )
            worker.start()
            worker.join(WAIT_SECONDS)
            (outcome,) = outcomes
        else:
            outcome = _sigterm_disposition_inside_run_terminable()
        after = signal.getsignal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, previous)

    seen, result = outcome
    assert result == "done"
    assert (seen is not disposition) == installed
    assert after is disposition


def test_run_terminable_lets_a_cancellation_without_sigterm_propagate() -> None:
    """A coroutine that ends cancelled with no SIGTERM received raises ``CancelledError``, not
    ``TerminatedError``."""

    async def cancelled() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        cli.run_terminable(cancelled())


@pytest.mark.parametrize(
    ("argv", "exit_code", "stream", "expected"),
    [
        pytest.param(["--help"], 0, "out", f"usage: {cli.PROG} ", id="help"),
        pytest.param(
            ["migrate", "upgrade", "--help"],
            0,
            "out",
            f"usage: {cli.PROG} migrate upgrade ",
            id="subcommand-help",
        ),
        pytest.param(["--version"], 0, "out", f"{cli.PROG} {__version__}", id="version"),
        pytest.param(["bogus"], 2, "err", "invalid choice: 'bogus'", id="unknown-command"),
        pytest.param([], 2, "err", "the following arguments are required: command", id="none"),
        pytest.param(
            ["ingest", cli.CBK_GROUP, "--limit", "5"],
            2,
            "err",
            f"--limit is valid only with cbk_reserves, not {cli.CBK_GROUP}",
            id="post-parse-check",
        ),
    ],
)
def test_boundary_argparse_exits_become_the_returned_code(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    exit_code: int,
    stream: str,
    expected: str,
) -> None:
    """argparse's own exits keep their codes, 0 for help and version and 2 for invalid
    arguments, and the boundary logs nothing for them."""
    caplog.set_level(logging.INFO)

    assert cli.main(argv) == exit_code

    assert expected in getattr(capsys.readouterr(), stream)
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.mark.parametrize(
    ("outcome", "exit_code", "errors"),
    [
        pytest.param(0, 0, [], id="seeded"),
        pytest.param(1, 1, [], id="seed-failed"),
        pytest.param(2, 2, [], id="schema-not-at-head"),
        pytest.param(130, 130, [], id="seed-interrupted"),
        pytest.param(SystemExit(3), 3, [], id="system-exit-int"),
        pytest.param(SystemExit(None), 0, [], id="system-exit-none"),
        pytest.param(
            SystemExit(f"stopped on {_MARKED_DATABASE_URL}"),
            1,
            ["seed exited with a non-integer status: str"],
            id="system-exit-message",
        ),
    ],
)
def test_boundary_seed_outcome_becomes_the_returned_code(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    outcome: int | SystemExit,
    exit_code: int,
    errors: list[str],
) -> None:
    """The seeder's own codes pass through unchanged, and a ``SystemExit`` from the handler is
    returned as its code rather than raised; a message code is one ERROR line by type."""
    calls: list[tuple[list[str], str]] = []

    def seed_main(argv: list[str], *, prog: str) -> int:
        calls.append((argv, prog))
        if isinstance(outcome, SystemExit):
            raise outcome
        return outcome

    # _cmd_seed imports the seeder's main when it runs, so it picks up the fake.
    monkeypatch.setattr(f"{_SEED_MODULE}.main", seed_main)
    caplog.set_level(logging.INFO)

    assert cli.main(["seed", "--database-url", _UNREACHABLE_DATABASE_URL]) == exit_code

    assert calls == [(["--database-url", _UNREACHABLE_DATABASE_URL], f"{cli.PROG} seed")]
    assert [
        record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
    ] == errors
    captured = capsys.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in output


@contextlib.contextmanager
def _bare_root_logger() -> Iterator[logging.Logger]:
    """Give the root logger no handlers inside the block, as in a fresh CLI process.

    Its handlers and level are restored on exit, inside the test, because pytest adds its capture
    handlers during the call phase and removes them from whichever list the root then holds.
    """
    root = logging.getLogger()
    handlers, level = root.handlers, root.level
    root.handlers = []
    try:
        yield root
    finally:
        root.handlers = handlers
        # setLevel, unlike assigning the attribute, also clears the loggers' cached levels.
        root.setLevel(level)


def test_logging_configure_logging_sets_info_format_and_quiets_alembic_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``configure_logging`` leaves a root logger that already has handlers (pytest's) alone,
    and gives one without INFO and a single stderr handler in the CLI format with UTC times.
    Either way it raises Alembic's plugin logger to WARNING."""
    plugins = logging.getLogger("alembic.runtime.plugins")
    root = logging.getLogger()
    harness_handlers, harness_level = list(root.handlers), root.level
    assert harness_handlers
    monkeypatch.setattr(plugins, "level", logging.NOTSET)

    cli.configure_logging()

    assert root.handlers == harness_handlers
    assert root.level == harness_level
    assert plugins.level == logging.WARNING

    monkeypatch.setattr(plugins, "level", logging.NOTSET)
    with _bare_root_logger() as bare_root:
        cli.configure_logging()
        handlers, level = list(bare_root.handlers), bare_root.level

    (handler,) = handlers
    assert level == logging.INFO
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr
    assert isinstance(handler.formatter, cli._UtcFormatter)
    assert handler.formatter._fmt == cli._LOG_FORMAT
    assert plugins.level == logging.WARNING
    assert root.handlers == harness_handlers


def test_logging_times_are_utc_iso_8601_on_a_nairobi_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On a host in Africa/Nairobi time (UTC+3), the handler ``configure_logging`` installs
    renders a record's time in UTC with milliseconds and ``Z``, the clock of the run rows'
    ``started_at`` and ``finished_at``; the stdlib default reads three hours ahead, unmarked."""
    with _bare_root_logger() as bare_root:
        cli.configure_logging()
        (handler,) = bare_root.handlers
    record = logging.makeLogRecord(
        {
            "name": "opennse_terminal.ingestion.base",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "ingestion run started",
            "created": datetime(2026, 10, 29, 23, 19, 45, tzinfo=UTC).timestamp(),
            "msecs": 871.0,
        }
    )
    try:
        with monkeypatch.context() as patch:
            # Africa/Nairobi's rule as a POSIX TZ string, so no system tz database is needed.
            patch.setenv("TZ", "EAT-3")
            time_module.tzset()
            configured = handler.format(record)
            stdlib_default = logging.Formatter(cli._LOG_FORMAT).format(record)
    finally:
        time_module.tzset()

    rest = "INFO opennse_terminal.ingestion.base ingestion run started"
    assert stdlib_default == f"2026-10-30 02:19:45,871 {rest}"
    assert configured == f"2026-10-29T23:19:45.871Z {rest}"


def test_logging_seed_module_entry_uses_the_cli_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``python -m opennse_terminal.seed.seed_terminal_db`` configures logging only through
    ``cli.configure_logging``: the seeder itself never calls ``logging.basicConfig``."""
    configured: list[str] = []

    def reject_basic_config(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail(f"logging.basicConfig called by the seeder entry with {args} {kwargs}")

    monkeypatch.setattr(cli, "configure_logging", lambda: configured.append("configure_logging"))
    monkeypatch.setattr(logging, "basicConfig", reject_basic_config)
    monkeypatch.setattr(sys, "argv", [_SEED_MODULE, "--help"])
    # The root conftest has imported the seeder; runpy warns when the module it runs as
    # __main__ is already in sys.modules. monkeypatch restores the entry afterwards.
    monkeypatch.delitem(sys.modules, _SEED_MODULE, raising=False)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module(_SEED_MODULE, run_name="__main__")

    assert exit_info.value.code == 0
    assert configured == ["configure_logging"]
    assert "usage: python -m opennse_terminal.seed.seed_terminal_db" in capsys.readouterr().out
