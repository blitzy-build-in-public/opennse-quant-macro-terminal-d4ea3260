"""NSE daily equity prices, volumes and turnover from the public market-statistics page.

Source: the "Equity Statistics" tab of https://www.nse.co.ke/market-statistics/. The page's own
script injects the equity table one sector at a time, through the ``#filtersectors`` dropdown, so
:class:`MarketStatisticsFetcher` renders the page in headless Chromium (Playwright), selects each
sector in turn and captures the rendered ``table.nsetable``; BeautifulSoup4 then parses each
captured fragment (:func:`parse_sector_table`). Only the rendered public page is read: no request
the page's script makes is called or intercepted, and the image-only "Daily Equity Price List"
PDF is never downloaded, only the date in its link's file name is read. Per the NSE terms, data
from this page is for personal or internal business use only unless the operator is licensed to
distribute it.

Units and columns written to ``terminal_daily_prices`` (conflict key ``(ticker, trade_date)``):
``close_price`` is the last traded price in KES per share, ``volume`` whole shares and
``turnover_kes`` whole KES, estimated as volume x close (``turnover_estimated = true``) because
the source publishes no turnover. The source publishes no open, high or low, so they stay NULL.
Price precedence is ``csv_import`` > ``nse_market_statistics`` > ``seed``: this pipeline replaces
seeded and earlier scraped prices but never imported ones, and never writes
``foreign_net_flow_kes`` or ``flow_source``. Rows are mapped to tickers by ISIN; ISINs that no
``terminal_equities`` row carries are reported in ``details.unmapped_isins`` and skipped.

Session dating: the page's "Statistics as of" label has been seen a day ahead of its values, so
it is recorded but never trusted. A snapshot is written for ``expected_session``, the latest
exchange session strictly before the run's Africa/Nairobi date (runs are scheduled at 07:30,
before the open), only when the price-list link's file date equals it and, where the stored
history allows, the reported changes agree with the previous session's stored closes. Anything
else ends the run ``ambiguous_session`` with nothing written; the operator then names the session
with ``opennse-terminal ingest nse_market_statistics --trade-date YYYY-MM-DD``.
"""

import asyncio
import dataclasses
import json
import logging
import re
import uuid
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar, Final, Protocol
from urllib.parse import unquote, urlsplit

from bs4 import BeautifulSoup, Comment, Tag
from bs4.element import NavigableString
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.config import MAX_SHARE_COUNT, TerminalSettings
from opennse_terminal.db.upsert import upsert_prices
from opennse_terminal.ingestion.base import (
    AmbiguousSession,
    FetchFailed,
    NoNewData,
    Pipeline,
    RejectedRow,
    RunContext,
    RunStatus,
    SourceLayoutChanged,
    ValidationOutcome,
    dedupe_by_key,
    evaluate_thresholds,
    normalize_header,
    parse_decimal_text,
)
from opennse_terminal.ingestion.calendar import ExchangeCalendar
from opennse_terminal.models import TerminalDailyPrice, TerminalEquity

if TYPE_CHECKING:
    # Type-only: Playwright is imported inside MarketStatisticsFetcher's methods, so the
    # registry, the CLI and the parser tests import this module without loading a browser driver.
    from playwright.async_api import Browser, Page

__all__ = [
    "CHANGE_PCT_BOUND",
    "CONTINUITY_MIN_SHARE",
    "CONTINUITY_TOLERANCE_PP",
    "EXPECTED_HEADERS",
    "ISIN_RE",
    "MIN_ROWS",
    "MIN_SECTOR_OPTIONS",
    "NAVIGATION_RETRIES",
    "NAVIGATION_TIMEOUT_MS",
    "NSE_MARKET_STATISTICS_URL",
    "RUN_CAP_SECONDS",
    "STALE_SHARE",
    "ContinuityResult",
    "Fetcher",
    "MarketStatisticsFetcher",
    "MarketStatisticsPipeline",
    "MarketStatisticsSnapshot",
    "NseQuote",
    "ParsedPage",
    "SectorTable",
    "StoredRow",
    "build_price_rows",
    "check_continuity",
    "is_stale",
    "parse_page",
    "parse_price_list_date",
    "parse_sector_table",
    "parse_statistics_as_of",
    "resolve_session",
]

logger = logging.getLogger("opennse_terminal.ingestion.nse.market_statistics")

NSE_MARKET_STATISTICS_URL: Final = "https://www.nse.co.ke/market-statistics/"
"""The public page rendered by the pipeline ("Equity Statistics" tab)."""

EXPECTED_HEADERS: Final[tuple[str, ...]] = (
    "company",
    "isin code",
    "volume",
    "last traded price",
    "change (%)",
)
"""The normalized header row every sector table must carry, in order."""

MIN_SECTOR_OPTIONS: Final = 10
"""Fewest real ``#filtersectors`` options (placeholders excluded) a valid page offers."""

MIN_ROWS: Final = 40
"""Fewest equity rows a valid snapshot holds across all sectors."""

ISIN_RE: Final = re.compile(r"^KE[0-9A-Z]{9}[0-9]$")
"""A Kenyan ISIN: ``KE``, nine alphanumerics and a check digit."""

CHANGE_PCT_BOUND: Final = Decimal(25)
"""Sanity bound on the reported daily change, in percent either way."""

CONTINUITY_TOLERANCE_PP: Final = Decimal("0.05")
"""Largest gap, in percentage points, between a reported change and the change implied by the
previous session's stored close."""

CONTINUITY_MIN_SHARE: Final = Decimal("0.90")
"""Share of eligible tickers whose reported change must agree with the stored history."""

STALE_SHARE: Final = Decimal("0.90")
"""Share of mapped tickers repeating their latest stored close and volume that marks the source
as not advanced."""

RUN_CAP_SECONDS: Final = 300
"""Cap on the whole browser session, launch to the last capture."""

NAVIGATION_TIMEOUT_MS: Final = 30_000
"""Timeout of the page navigation and of each sector render, in milliseconds."""

NAVIGATION_RETRIES: Final = 2
"""Navigation attempts made after the first one fails."""

SELECTOR_SECTORS: Final = "#filtersectors"
SELECTOR_TABLE: Final = "table.nsetable"
PRICE_LIST_LINK_TEXT: Final = "daily equity price list"
PRICE_LIST_HREF_RE: Final = re.compile(r"(\d{2})-([A-Za-z]{3})-(\d{2})\.pdf", re.IGNORECASE)
STATISTICS_AS_OF_RE: Final = re.compile(r"statistics\s+as\s+of\s*:?\s*(?P<date>.*)", re.IGNORECASE)
TURNOVER_QUANTUM: Final = Decimal("0.01")
PRICE_QUANTUM: Final = Decimal("0.0001")

_MONTH_NAMES: Final = tuple(
    "january february march april may june july august september october november december".split()
)
_MONTHS: Final[Mapping[str, int]] = MappingProxyType(
    {
        **{name: number for number, name in enumerate(_MONTH_NAMES, start=1)},
        **{name[:3]: number for number, name in enumerate(_MONTH_NAMES, start=1)},
        "sept": 9,
    }
)
"""Lowercase English month names, full and three-letter (plus ``sept``), mapped to 1-12."""

_SEED_SOURCE: Final = "seed"
# NUMERIC(18,4) holds fewer than 10^14 whole shillings; NUMERIC(24,2) fewer than 10^22. A row
# beyond either is rejected rather than failing the run's whole write on the column bound.
_PRICE_INTEGER_DIGITS: Final = 14
_TURNOVER_LIMIT: Final = Decimal(10) ** 22
_DASHES: Final = frozenset({"-", "—", "–"})
_DIGITS_RE: Final = re.compile(r"[0-9]+")
_GROUP_SEPARATORS: Final = str.maketrans("", "", ", \u00a0\u202f\u2009")
_WEEKDAY_PREFIX_RE: Final = re.compile(
    r"^(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday"
    r"|mon|tues|tue|wed|thurs|thur|thu|fri|sat|sun)\.?,?\s+"
)
# Text nodes after a "Statistics as of" label searched for its date, and elements whose text is
# never visible.
_LABEL_LOOKAHEAD: Final = 4
_HIDDEN_TEXT: Final = frozenset({"script", "style"})
_ORDINAL_SUFFIX_RE: Final = re.compile(r"(?<=\d)(?:st|nd|rd|th)(?![a-z])")
_ISO_DATE_RE: Final = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})(?!\w)")
_NUMERIC_DATE_RE: Final = re.compile(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})(?!\w)")
_DAY_MONTH_YEAR_RE: Final = re.compile(r"(\d{1,2})[\s./,-]+([a-z]+)\.?[\s./,-]+(\d{4})(?!\w)")
_MONTH_DAY_YEAR_RE: Final = re.compile(r"([a-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})(?!\w)")
# A lone colspan body cell reading one of these (lowercased, no final period): an empty sector.
_NO_DATA_PLACEHOLDERS: Final = frozenset(
    {
        "no data",
        "no data available",
        "no data available in table",
        "no data found",
        "no matching records found",
        "no records",
        "no records found",
        "no record found",
        "no results found",
        "no entries",
    }
)

# Browser-side helpers shared by the fetcher's page scripts. The page holds bond and derivative
# tables besides the equity one, each after its own <select> filter, and until the default sector
# has rendered the derivatives table is the page's first table.nsetable. The sector table is
# therefore the first table.nsetable after #filtersectors in document order, before any other
# <select>.
_JS_HELPERS: Final = r"""
const sectorSelect = () => document.querySelector(%SECTORS%);
const sectorTable = () => {
  const select = sectorSelect();
  if (!select) return null;
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
  walker.currentNode = select;
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    if (node.tagName === "SELECT") return null;
    if (node.matches(%TABLE%)) return node;
  }
  return null;
};
const dataRows = (table) =>
  Array.from(table.rows).filter((row) => row.querySelector(":scope > td")).length;
const isDisabled = (el) => !!el && (el.classList.contains("disabled")
  || el.hasAttribute("disabled") || el.getAttribute("aria-disabled") === "true");
const isPaginated = (table) => {
  const scope = table.closest(".dataTables_wrapper") || table.parentElement;
  const next = scope && scope.querySelector(".dataTables_paginate .next, .pagination .next");
  return !!next && !isDisabled(next) && !isDisabled(next.parentElement);
};
const sectorState = () => {
  const select = sectorSelect();
  const table = sectorTable();
  return {
    value: select ? select.value : null,
    rows: table ? dataRows(table) : 0,
    html: table ? table.outerHTML : "",
    paginated: table ? isPaginated(table) : false,
  };
};
""".replace("%SECTORS%", json.dumps(SELECTOR_SECTORS)).replace(
    "%TABLE%", json.dumps(SELECTOR_TABLE)
)
_JS_STATE: Final = "() => {" + _JS_HELPERS + "return sectorState(); }"
# Truthy (the state itself) once the sector table differs from what was there before the
# selection, has data rows, and sits beside a select showing the requested sector; the page
# re-renders the select with each sector, so a late response for another sector never passes.
_JS_RENDERED: Final = (
    "(expected) => {"
    + _JS_HELPERS
    + "const state = sectorState();"
    + "return state.html !== expected.before && state.rows > 0"
    + " && state.value === expected.value ? state : false; }"
)
_JS_OPTIONS: Final = (
    r"(options) => options.map((option) => ({value: option.value,"
    r" label: (option.textContent || '').replace(/\s+/g, ' ').trim(),"
    r" disabled: option.disabled}))"
)


@dataclass(frozen=True, slots=True)
class NseQuote:
    """One row of a sector table, as parsed.

    Attributes:
        company: The company cell, trimmed (logged for unmapped ISINs, never stored).
        isin: The ISIN cell, trimmed and upper-cased; the join key to ``terminal_equities``.
        volume: Shares traded; ``0`` for a dash; ``None`` when the cell does not convert.
        close_price: Last traded price in KES per share; ``None`` when it does not convert.
        change_pct: Reported change in percent (``-0.06`` is -0.06%); ``Decimal(0)`` for a dash;
            ``None`` when it does not convert.
        sector: The dropdown label of the sector the row was captured under.
        raw: Normalized header to trimmed cell text, for rejection evidence.
        ticker: Set by the pipeline once the ISIN is mapped.
        trade_date: Set by the pipeline once the session is dated.
    """

    company: str
    isin: str
    volume: int | None
    close_price: Decimal | None
    change_pct: Decimal | None
    sector: str
    raw: Mapping[str, str]
    ticker: str | None = None
    trade_date: date | None = None


@dataclass(frozen=True, slots=True)
class SectorTable:
    """The rendered ``table.nsetable`` of one ``#filtersectors`` option, as ``outerHTML``."""

    option_value: str
    option_label: str
    html: str


@dataclass(frozen=True, slots=True)
class MarketStatisticsSnapshot:
    """One browser session's capture: the final page HTML (sector options, price-list link and
    "Statistics as of" label) and one table per sector option, in dropdown order."""

    page_html: str
    sector_tables: Sequence[SectorTable]
    fetched_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "sector_tables", tuple(self.sector_tables))


@dataclass(frozen=True, slots=True)
class ParsedPage:
    """What :func:`parse_page` reads from a snapshot.

    Attributes:
        option_count: Real ``#filtersectors`` options (placeholders excluded).
        option_labels: Their labels, in dropdown order.
        link_href: The "Download Daily Equity Price List" link, or ``None``.
        link_session: The date in that link's file name (``DD-MON-YY.pdf``), or ``None``.
        label_session: The "Statistics as of" date, or ``None``; evidence only.
        quotes: Every sector table's rows, in dropdown order.
    """

    option_count: int
    option_labels: list[str]
    link_href: str | None
    link_session: date | None
    label_session: date | None
    quotes: list[NseQuote]


@dataclass(frozen=True, slots=True)
class StoredRow:
    """A stored ``terminal_daily_prices`` row, as the stale and continuity checks read it."""

    trade_date: date
    close_price: Decimal
    volume: int
    source: str


@dataclass(frozen=True, slots=True)
class ContinuityResult:
    """Outcome of :func:`check_continuity`.

    Attributes:
        applies: Whether any ticker was eligible for the comparison.
        matched: Eligible tickers whose reported change agrees with the stored close.
        eligible: Tickers compared.
        passed: ``True`` when the check does not apply or enough eligible tickers matched.
    """

    applies: bool
    matched: int
    eligible: int
    passed: bool


@dataclass(frozen=True, slots=True)
class _SectorOption:
    value: str
    label: str


@dataclass(frozen=True, slots=True)
class _SectorState:
    """The sector select's value and the sector table as the page shows them now."""

    value: str | None
    rows: int
    html: str
    paginated: bool

    @classmethod
    def from_js(cls, payload: object) -> "_SectorState":
        data: Mapping[str, object] = payload if isinstance(payload, dict) else {}
        value, rows, html = data.get("value"), data.get("rows"), data.get("html")
        return cls(
            value=value if isinstance(value, str) else None,
            rows=rows if isinstance(rows, int) else 0,
            html=html if isinstance(html, str) else "",
            paginated=data.get("paginated") is True,
        )

    def shows(self, option: _SectorOption) -> bool:
        return self.value == option.value and self.rows > 0


class Fetcher(Protocol):
    """Produces a :class:`MarketStatisticsSnapshot`; tests substitute recorded fixtures."""

    async def fetch(self, url: str, *, user_agent: str) -> MarketStatisticsSnapshot: ...


class MarketStatisticsFetcher:
    """Renders the public market-statistics page in headless Chromium and captures the equity
    table of every sector.

    Each run gets a fresh browser context with the product's user agent and no stored state, so
    no cookie outlives it. The page is loaded once (up to ``1 + NAVIGATION_RETRIES`` attempts),
    then each ``#filtersectors`` option is selected in dropdown order and the table it renders is
    captured; consecutive selections and navigation attempts are at least
    ``scraper_min_interval_seconds`` apart. The whole session is capped at
    :data:`RUN_CAP_SECONDS`.

    Failures surface as :class:`FetchFailed` (``failed``) with the reasons
    ``browser_not_installed``, ``browser_launch_failed``, ``navigation_failed``,
    ``sector_render_timeout:<label>``, ``run_timeout`` or ``browser_error``, and as
    :class:`SourceLayoutChanged` when a sector table turns out to be paginated. No Playwright
    exception escapes.

    Args:
        settings: Supplies the per-host spacing.
        sleep: Awaited for every spacing wait.
        headless: Launch Chromium without a window.
    """

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        headless: bool = True,
    ) -> None:
        self._settings = settings
        self._sleep = sleep
        self._headless = headless

    async def fetch(self, url: str, *, user_agent: str) -> MarketStatisticsSnapshot:
        """Render ``url`` and return the page HTML plus one captured table per sector option."""
        from playwright.async_api import Error as PlaywrightError
        from playwright.async_api import async_playwright

        deadline = asyncio.timeout(RUN_CAP_SECONDS)
        try:
            async with deadline, async_playwright() as playwright:
                try:
                    browser = await playwright.chromium.launch(headless=self._headless)
                except PlaywrightError as exc:
                    message = str(exc).lower()
                    if "executable doesn't exist" in message or "playwright install" in message:
                        raise FetchFailed(
                            url,
                            "browser_not_installed: run python -m playwright install "
                            "--with-deps chromium",
                        ) from exc
                    raise FetchFailed(url, f"browser_launch_failed: {type(exc).__name__}") from exc
                try:
                    return await self._capture(browser, url, user_agent=user_agent)
                finally:
                    await browser.close()
        except (TimeoutError, PlaywrightError) as exc:
            # The cap cancels whatever is awaited when it expires, which surfaces either as the
            # cap's TimeoutError or as the Playwright error of the interrupted call.
            if deadline.expired():
                raise FetchFailed(url, "run_timeout") from exc
            if isinstance(exc, PlaywrightError):
                raise FetchFailed(url, f"browser_error: {type(exc).__name__}") from exc
            raise

    async def _capture(
        self, browser: "Browser", url: str, *, user_agent: str
    ) -> MarketStatisticsSnapshot:
        context = await browser.new_context(user_agent=user_agent)
        try:
            page = await context.new_page()
            await self._navigate(page, url)
            tables: list[SectorTable] = []
            for index, option in enumerate(await self._read_options(page)):
                if index:
                    await self._sleep(self._settings.scraper_min_interval_seconds)
                tables.append(await self._capture_sector(page, url, option))
            page_html = await page.content()
        finally:
            await context.close()
        return MarketStatisticsSnapshot(
            page_html=page_html, sector_tables=tuple(tables), fetched_at=datetime.now(UTC)
        )

    async def _navigate(self, page: "Page", url: str) -> None:
        from playwright.async_api import Error as PlaywrightError

        attempts = 1 + NAVIGATION_RETRIES
        for attempt in range(1, attempts + 1):
            # The robots.txt check has just requested this host, and a retry follows a failed
            # request to it, so every attempt first waits out the per-host spacing.
            await self._sleep(self._settings.scraper_min_interval_seconds)
            try:
                response = await page.goto(
                    url, timeout=NAVIGATION_TIMEOUT_MS, wait_until="domcontentloaded"
                )
                if response is None or response.status < 400:
                    await page.wait_for_selector(SELECTOR_SECTORS, timeout=NAVIGATION_TIMEOUT_MS)
                    return
                failure = f"http_{response.status}"
            except PlaywrightError as exc:
                failure = type(exc).__name__
            logger.warning(
                "navigation attempt %d of %d to the market-statistics page failed (%s)%s",
                attempt,
                attempts,
                failure,
                "; retrying" if attempt < attempts else "",
            )
        raise FetchFailed(url, "navigation_failed")

    async def _read_options(self, page: "Page") -> list[_SectorOption]:
        payload = await page.eval_on_selector_all(f"{SELECTOR_SECTORS} option", _JS_OPTIONS)
        options: list[_SectorOption] = []
        for item in payload if isinstance(payload, list) else []:
            if not isinstance(item, dict):
                continue
            value, label = item.get("value"), item.get("label")
            # Placeholders ("Select Sector") have an empty value; parse_page skips the same ones.
            if not isinstance(value, str) or not value.strip() or item.get("disabled") is True:
                continue
            options.append(
                _SectorOption(value=value, label=label if isinstance(label, str) else "")
            )
        return options

    async def _capture_sector(self, page: "Page", url: str, option: _SectorOption) -> SectorTable:
        state = await self._state(page)
        # The page renders its default sector itself; selecting the option it already shows
        # would only request the same table again.
        if not state.shows(option):
            state = await self._select(page, url, option, before=state.html)
        if state.paginated:
            # A paginated table would yield only its first page; a partial sector is a layout
            # change to fail on, not data to write.
            raise SourceLayoutChanged(
                expected="one unpaginated table.nsetable per sector",
                found={"sector": option.label, "paginated": True, "rows_on_page": state.rows},
                message=f"the {option.label} sector table is paginated",
            )
        logger.info("captured NSE sector %s: %d rows", option.label, state.rows)
        return SectorTable(option_value=option.value, option_label=option.label, html=state.html)

    async def _select(
        self, page: "Page", url: str, option: _SectorOption, *, before: str
    ) -> _SectorState:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        previous = before
        for attempt in (1, 2):
            if attempt == 2:
                current = await self._state(page)
                if current.shows(option) and current.html != before:
                    # It rendered just after the wait gave up.
                    return current
                logger.warning(
                    "NSE sector %s did not render within %d ms; selecting it again",
                    option.label,
                    NAVIGATION_TIMEOUT_MS,
                )
                await self._sleep(self._settings.scraper_min_interval_seconds)
                previous = current.html
            try:
                # Playwright dispatches input and change events even when the value is already
                # selected, so a repeat selection makes the page render the sector again.
                await page.select_option(
                    SELECTOR_SECTORS, value=option.value, timeout=NAVIGATION_TIMEOUT_MS
                )
                handle = await page.wait_for_function(
                    _JS_RENDERED,
                    arg={"before": previous, "value": option.value},
                    timeout=NAVIGATION_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError:
                continue
            try:
                return _SectorState.from_js(await handle.json_value())
            finally:
                await handle.dispose()
        raise FetchFailed(url, f"sector_render_timeout:{option.label}")

    async def _state(self, page: "Page") -> _SectorState:
        return _SectorState.from_js(await page.evaluate(_JS_STATE))


# ---------------------------------------------------------------------------------------------
# Pure parsers
# ---------------------------------------------------------------------------------------------


def _text(element: Tag) -> str:
    """An element's text with every run of whitespace collapsed to one space."""
    return " ".join(element.get_text(" ", strip=True).split())


def _child_tags(element: Tag, names: Sequence[str]) -> list[Tag]:
    return list(element.find_all(names, recursive=False))


def _parse_volume(text: str) -> int | None:
    compact = text.translate(_GROUP_SEPARATORS)
    if compact in _DASHES:
        # A listed counter shown with a dash reported no trades: a zero-volume session, unlike a
        # counter missing from the table.
        return 0
    if _DIGITS_RE.fullmatch(compact) is None:
        return None
    try:
        return int(compact)
    except ValueError:
        # Longer than Python's integer-string conversion limit: far beyond any share count.
        return None


def _parse_change(text: str) -> Decimal | None:
    compact = "".join(text.replace("%", "").split())
    if compact in _DASHES:
        return Decimal(0)
    return parse_decimal_text(compact)


def _quote_from_cells(cells: Sequence[str], sector: str) -> NseQuote:
    raw = {
        (EXPECTED_HEADERS[index] if index < len(EXPECTED_HEADERS) else f"cell_{index + 1}"): text
        for index, text in enumerate(cells)
    }
    company = cells[0] if cells else ""
    isin = cells[1].strip().upper() if len(cells) > 1 else ""
    if len(cells) != len(EXPECTED_HEADERS):
        # Kept so validation rejects and counts it rather than the row vanishing.
        return NseQuote(company, isin, None, None, None, sector, raw)
    return NseQuote(
        company=company,
        isin=isin,
        volume=_parse_volume(cells[2]),
        close_price=parse_decimal_text(cells[3]),
        change_pct=_parse_change(cells[4]),
        sector=sector,
        raw=raw,
    )


def parse_sector_table(html: str, *, sector: str = "") -> list[NseQuote]:
    """Parse one captured sector table into quotes, without I/O.

    The header row (the first ``thead`` row, else the first row holding ``th`` cells) must read
    exactly :data:`EXPECTED_HEADERS` once normalized. Every other row becomes a quote, except
    two kinds that hold no data and are skipped: a row whose cells are all empty, and a single
    ``colspan`` cell whose text, normalized like a header and without a final period, is a
    recognized empty-table placeholder (:data:`_NO_DATA_PLACEHOLDERS`, for example DataTables'
    "No data available in table" or "No matching records found"). Any other row is kept: one
    with the wrong number of cells, such as a single cell reading "Service unavailable", gets
    null values (and an empty ISIN without a second cell), so validation rejects and counts it
    rather than the sector vanishing uncounted. Volume is an integer (thousands separators
    allowed, a dash meaning no trades), close and change are exact ``Decimal`` values, and a
    cell that does not convert is ``None``.

    Raises:
        SourceLayoutChanged: The fragment has no ``table.nsetable`` or its headers differ.
    """
    table = BeautifulSoup(html, "lxml").select_one(SELECTOR_TABLE)
    if table is None:
        raise SourceLayoutChanged(
            expected=EXPECTED_HEADERS, found=(), message="table.nsetable not found"
        )
    head_rows: list[Tag] = []
    rows: list[Tag] = []
    for part in _child_tags(table, ("thead", "tbody", "tfoot", "tr")):
        if part.name == "tr":
            rows.append(part)
        else:
            (head_rows if part.name == "thead" else rows).extend(_child_tags(part, ("tr",)))
    header = (
        head_rows[0]
        if head_rows
        else next((row for row in rows if row.find("th", recursive=False) is not None), None)
    )
    headers = (
        tuple(normalize_header(_text(cell)) for cell in _child_tags(header, ("th", "td")))
        if header is not None
        else ()
    )
    if headers != EXPECTED_HEADERS:
        raise SourceLayoutChanged(
            expected=EXPECTED_HEADERS,
            found=headers,
            message="NSE equity table headers differ from the expected layout",
        )
    quotes: list[NseQuote] = []
    for row in rows:
        if row is header:
            continue
        cells = _child_tags(row, ("td",))
        texts = [_text(cell) for cell in cells]
        if not any(texts):
            continue
        if (
            len(cells) == 1
            and cells[0].has_attr("colspan")
            and normalize_header(texts[0]).rstrip(". ") in _NO_DATA_PLACEHOLDERS
        ):
            # An empty sector's placeholder. Any other lone cell, an error message included, is
            # kept so validation rejects and counts it.
            continue
        quotes.append(_quote_from_cells(texts, sector))
    return quotes


def _price_list_file(href: str) -> re.Match[str] | None:
    """Match :data:`PRICE_LIST_HREF_RE` against the file name of ``href``'s path."""
    try:
        path = urlsplit(href).path
    except ValueError:
        return None
    return PRICE_LIST_HREF_RE.fullmatch(unquote(path).rsplit("/", 1)[-1])


def parse_price_list_date(href: str) -> date | None:
    """The session date in a price-list link's file name, ``DD-MON-YY.pdf`` (``27-OCT-26.pdf`` is
    2026-10-27); ``None`` when the name does not have that form or names no real date.

    The link may be absolute, relative or percent-encoded; only its path's last segment is read,
    and the PDF itself is never fetched.
    """
    match = _price_list_file(href)
    if match is None:
        return None
    day, month_name, year = match.groups()
    month = _MONTHS.get(month_name.lower())
    if month is None:
        return None
    try:
        return date(2000 + int(year), month, int(day))
    except ValueError:
        return None


def _calendar_date(year: str, month: str, day: str) -> date | None:
    """``date(year, month, day)`` from digit strings, or from a month name; ``None`` when the
    month name is unknown or the date impossible."""
    month_number = int(month) if month.isdigit() else _MONTHS.get(month)
    if month_number is None:
        return None
    try:
        return date(int(year), month_number, int(day))
    except ValueError:
        return None


def parse_statistics_as_of(text: str) -> date | None:
    """Parse the date of the page's "Statistics as of" label, without ``strptime``.

    Accepts, after an optional "Statistics as of" prefix and weekday name, and with ordinal
    suffixes (``st``, ``nd``, ``rd``, ``th``) ignored: ``27 Oct 2026``, ``27 October 2026`` and
    ``27-Oct-2026`` (day, month name, year); ``27/10/2026`` and ``27-10-2026`` (day first);
    ``October 27, 2026`` and ``Oct 27 2026``; and ``2026-10-27``. Returns ``None`` for anything
    else, including impossible dates.
    """
    normalized = " ".join(text.split())
    prefixed = STATISTICS_AS_OF_RE.search(normalized)
    if prefixed is not None:
        normalized = prefixed.group("date")
    normalized = _ORDINAL_SUFFIX_RE.sub("", _WEEKDAY_PREFIX_RE.sub("", normalized.lower()))
    if (match := _ISO_DATE_RE.match(normalized)) is not None:
        year, month, day = match.groups()
    elif (match := _NUMERIC_DATE_RE.match(normalized)) is not None:
        day, month, year = match.groups()
    elif (match := _DAY_MONTH_YEAR_RE.match(normalized)) is not None:
        day, month, year = match.groups()
    elif (match := _MONTH_DAY_YEAR_RE.match(normalized)) is not None:
        month, day, year = match.groups()
    else:
        return None
    return _calendar_date(year, month, day)


def _sector_option_labels(soup: BeautifulSoup) -> list[str]:
    """Labels of the real ``#filtersectors`` options, skipping placeholders as the fetcher does:
    an option whose value (its text when it has no ``value`` attribute) is blank, or one marked
    ``disabled``."""
    select = soup.select_one(SELECTOR_SECTORS)
    if select is None:
        return []
    labels: list[str] = []
    for option in select.find_all("option"):
        if option.has_attr("disabled"):
            continue
        value = option.get("value")
        if not (value if isinstance(value, str) else _text(option)).strip():
            continue
        labels.append(_text(option))
    return labels


def _price_list_href(soup: BeautifulSoup) -> str | None:
    """The first anchor reading "Daily Equity Price List" whose file name is a price-list date;
    failing that, the first anchor with such a file name."""
    fallback: str | None = None
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href")
        if not isinstance(href, str) or _price_list_file(href) is None:
            continue
        if PRICE_LIST_LINK_TEXT in normalize_header(_text(anchor)):
            return href
        if fallback is None:
            fallback = href
    return fallback


def _is_visible_text(node: NavigableString) -> bool:
    parent = node.parent
    return not isinstance(node, Comment) and (parent is None or parent.name not in _HIDDEN_TEXT)


def _statistics_as_of(soup: BeautifulSoup) -> date | None:
    """The date immediately following the first "Statistics as of" label in the page's visible
    text, in the label's own text node or the next ones (``<b>Statistics as of</b>
    <span>28-Oct-2026</span>``). Commented-out markup (the page keeps an old label in an HTML
    comment), scripts and styles are skipped."""
    for node in soup.find_all(string=STATISTICS_AS_OF_RE):
        if not _is_visible_text(node):
            continue
        following = [
            str(text)
            for text in node.find_all_next(string=True, limit=_LABEL_LOOKAHEAD)
            if _is_visible_text(text)
        ]
        match = STATISTICS_AS_OF_RE.search(" ".join(" ".join([str(node), *following]).split()))
        found = parse_statistics_as_of(match.group("date")) if match is not None else None
        if found is not None:
            return found
    return None


def parse_page(snapshot: MarketStatisticsSnapshot) -> ParsedPage:
    """Read a snapshot without I/O: the sector options, the price-list link and its date, the
    "Statistics as of" date and every sector table's quotes (see :func:`parse_sector_table`).

    Raises:
        SourceLayoutChanged: A sector table is missing or its headers differ.
    """
    soup = BeautifulSoup(snapshot.page_html, "lxml")
    labels = _sector_option_labels(soup)
    link_href = _price_list_href(soup)
    return ParsedPage(
        option_count=len(labels),
        option_labels=labels,
        link_href=link_href,
        link_session=parse_price_list_date(link_href) if link_href is not None else None,
        label_session=_statistics_as_of(soup),
        quotes=[
            quote
            for table in snapshot.sector_tables
            for quote in parse_sector_table(table.html, sector=table.option_label)
        ],
    )


# ---------------------------------------------------------------------------------------------
# Session dating, stale guard and continuity (pure)
# ---------------------------------------------------------------------------------------------


def _iso(day: date | None) -> str | None:
    return day.isoformat() if day is not None else None


def resolve_session(
    *,
    expected_session: date | None,
    link_session: date | None,
    label_session: date | None,
    trade_date_override: date | None,
) -> date:
    """The session a snapshot is written for.

    An operator's ``--trade-date`` override wins. Otherwise the price-list link's file date must
    equal ``expected_session`` (the latest exchange session before the run's local date). The
    "Statistics as of" label is recorded as evidence but never decides.

    Raises:
        AmbiguousSession: No override, and the link date is missing (``link_date_missing``) or
            differs from ``expected_session`` (``link_date_mismatch``); ``evidence`` holds the
            three dates as ISO strings and the reason.
    """
    if trade_date_override is not None:
        return trade_date_override
    if link_session is not None and link_session == expected_session:
        return link_session
    raise AmbiguousSession(
        evidence={
            "expected_session": _iso(expected_session),
            "link_session": _iso(link_session),
            "label_session": _iso(label_session),
            "reason": "link_date_missing" if link_session is None else "link_date_mismatch",
        }
    )


def _repeats(quotes_by_ticker: Mapping[str, NseQuote], latest_rows: Mapping[str, StoredRow]) -> int:
    """Mapped tickers whose quote repeats their latest stored ``(close_price, volume)``; the
    ``Decimal`` comparison ignores scale, so ``33.2500`` repeats ``33.25``."""
    repeated = 0
    for ticker, quote in quotes_by_ticker.items():
        stored = latest_rows.get(ticker)
        if (
            stored is not None
            and quote.close_price is not None
            and stored.close_price == quote.close_price
            and stored.volume == quote.volume
        ):
            repeated += 1
    return repeated


def is_stale(
    quotes_by_ticker: Mapping[str, NseQuote], latest_rows: Mapping[str, StoredRow]
) -> bool:
    """Whether the source has not advanced: at least one ticker is mapped and at least
    :data:`STALE_SHARE` of the mapped tickers repeat their latest stored close and volume."""
    mapped = len(quotes_by_ticker)
    if mapped == 0:
        return False
    return Decimal(_repeats(quotes_by_ticker, latest_rows)) / Decimal(mapped) >= STALE_SHARE


def check_continuity(
    quotes_by_ticker: Mapping[str, NseQuote],
    latest_rows: Mapping[str, StoredRow],
    *,
    previous_session: date,
) -> ContinuityResult:
    """Check that the snapshot is the session following the stored history.

    ``latest_rows`` holds each ticker's latest stored row strictly before the trade date. A
    ticker is eligible when that row is real (``source != 'seed'``) and dated
    ``previous_session``, its quote has a non-zero volume and both a close and a change, and the
    stored close is positive. It matches when the reported change is within
    :data:`CONTINUITY_TOLERANCE_PP` of ``(close - stored_close) / stored_close x 100``, computed
    in ``Decimal`` without rounding. The check applies only when some ticker is eligible, so
    seeded-only history never applies, and passes when it does not apply or at least
    :data:`CONTINUITY_MIN_SHARE` of the eligible tickers match.
    """
    eligible = matched = 0
    for ticker, quote in quotes_by_ticker.items():
        stored = latest_rows.get(ticker)
        if (
            stored is None
            or stored.source == _SEED_SOURCE
            or stored.trade_date != previous_session
            or not quote.volume
            or quote.change_pct is None
            or quote.close_price is None
            or stored.close_price <= 0
        ):
            continue
        eligible += 1
        implied = (quote.close_price - stored.close_price) / stored.close_price * 100
        if abs(quote.change_pct - implied) <= CONTINUITY_TOLERANCE_PP:
            matched += 1
    applies = eligible > 0
    passed = not applies or Decimal(matched) / Decimal(eligible) >= CONTINUITY_MIN_SHARE
    return ContinuityResult(applies=applies, matched=matched, eligible=eligible, passed=passed)


def _quantized_close(value: Decimal) -> Decimal | None:
    """``value`` at the column's 4 decimal places (``ROUND_HALF_UP``), or ``None`` when it is not
    positive at that scale or does not fit ``NUMERIC(18,4)``."""
    if not value.is_finite() or value <= 0 or value.adjusted() >= _PRICE_INTEGER_DIGITS:
        return None
    quantized = value.quantize(PRICE_QUANTUM, rounding=ROUND_HALF_UP)
    if quantized <= 0 or quantized.adjusted() >= _PRICE_INTEGER_DIGITS:
        return None
    return quantized


def build_price_rows(quotes: Sequence[NseQuote]) -> list[dict[str, object]]:
    """``terminal_daily_prices`` rows for ``upsert_prices``, one per mapped and dated quote.

    ``close_price`` is quantized to 4 decimal places and ``turnover_kes`` is
    ``volume x close`` quantized to 0.01 (``ROUND_HALF_UP``) with ``turnover_estimated = True``.
    Open, high and low are ``None``; no flow column is ever included.

    Raises:
        ValueError: A quote lacks its ticker, trade date, volume or a storable close; the
            pipeline only passes quotes that validation and reconciliation accepted.
    """
    rows: list[dict[str, object]] = []
    for quote in quotes:
        if quote.ticker is None or quote.trade_date is None or quote.volume is None:
            raise ValueError(f"quote for ISIN {quote.isin!r} is not mapped, dated and validated")
        close = _quantized_close(quote.close_price) if quote.close_price is not None else None
        if close is None:
            raise ValueError(f"quote for ISIN {quote.isin!r} has no storable close price")
        rows.append(
            {
                "ticker": quote.ticker,
                "trade_date": quote.trade_date,
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "close_price": close,
                "volume": quote.volume,
                "turnover_kes": (Decimal(quote.volume) * close).quantize(
                    TURNOVER_QUANTUM, rounding=ROUND_HALF_UP
                ),
                "turnover_estimated": True,
            }
        )
    return rows


def _rejection_reasons(quote: NseQuote) -> list[str]:
    reasons: list[str] = []
    if ISIN_RE.fullmatch(quote.isin) is None:
        reasons.append("invalid_isin")
    close = _quantized_close(quote.close_price) if quote.close_price is not None else None
    if close is None:
        reasons.append("invalid_close")
    volume = quote.volume
    if volume is None or volume < 0 or volume > MAX_SHARE_COUNT:
        reasons.append("invalid_volume")
    elif close is not None and Decimal(volume) * close >= _TURNOVER_LIMIT:
        # Beyond NUMERIC(24,2): one such row would otherwise fail the whole write.
        reasons.append("invalid_turnover")
    change = quote.change_pct
    # copy_abs and the comparison are exact: abs() would round to the context's precision (a
    # change just above 25 reading as 25) or overflow its exponent range (1e1000000 raising).
    if change is None or not change.is_finite() or change.copy_abs() > CHANGE_PCT_BOUND:
        reasons.append("invalid_change")
    return reasons


async def _load_latest_rows(
    conn: AsyncConnection, tickers: Sequence[str], *, before: date | None = None
) -> dict[str, StoredRow]:
    """Each ticker's latest stored row, or its latest strictly before ``before`` (PostgreSQL
    ``DISTINCT ON``, bound parameters only)."""
    statement = (
        select(
            TerminalDailyPrice.ticker,
            TerminalDailyPrice.trade_date,
            TerminalDailyPrice.close_price,
            TerminalDailyPrice.volume,
            TerminalDailyPrice.source,
        )
        .where(TerminalDailyPrice.ticker.in_(tickers))
        .distinct(TerminalDailyPrice.ticker)
        .order_by(TerminalDailyPrice.ticker, TerminalDailyPrice.trade_date.desc())
    )
    if before is not None:
        statement = statement.where(TerminalDailyPrice.trade_date < before)
    result = await conn.execute(statement)
    return {
        ticker: StoredRow(trade_date=trade_date, close_price=close, volume=volume, source=source)
        for ticker, trade_date, close, volume, source in result.all()
    }


# ---------------------------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------------------------


class MarketStatisticsPipeline(Pipeline[MarketStatisticsSnapshot, NseQuote]):
    """``nse_market_statistics``: the public NSE equity table into ``terminal_daily_prices``.

    Stages, run by :func:`~opennse_terminal.ingestion.base.execute_pipeline` inside one data
    transaction:

    1. ``fetch``: the ``robots.txt`` check through the run's polite client, then the browser
       session (:class:`MarketStatisticsFetcher`, or an injected :class:`Fetcher`).
    2. ``parse``: the page and every sector table (:func:`parse_page`); fewer than
       :data:`MIN_SECTOR_OPTIONS` options, or a captured table count differing from the option
       count, is ``layout_changed``.
    3. ``validate``: ISIN, close, volume and change checks; the shared thresholds then give
       ``partial`` or ``layout_changed``, and fewer than :data:`MIN_ROWS` rows is
       ``layout_changed``.
    4. ``reconcile``, in order: ISIN to ticker mapping, the stale guard, session dating and the
       continuity check (see their functions). An unchanged snapshot ends ``no_new_data`` and
       writes nothing, so re-running over one is idempotent in effect.
    5. ``upsert``: :func:`~opennse_terminal.db.upsert.upsert_prices` with
       ``source = 'nse_market_statistics'``.

    An instance serves one run: the page evidence read by ``parse`` is kept for ``reconcile``.

    Args:
        settings: The run's settings (spacing, timezone).
        trade_date: The operator's ``--trade-date``; replaces the expected session and the link
            date check, while validation, the stale guard and the continuity check still run.
        fetcher: Replaces the Playwright fetcher, for example with recorded fixtures.
        calendar: Replaces the shipped exchange calendar.
        now: Returns the current timezone-aware time; defaults to now in ``settings.tzinfo``.
    """

    name: ClassVar[str] = "nse_market_statistics"
    min_rows: ClassVar[int] = MIN_ROWS
    source_url: str | None = NSE_MARKET_STATISTICS_URL

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        trade_date: date | None = None,
        fetcher: Fetcher | None = None,
        calendar: ExchangeCalendar | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(settings)
        if isinstance(trade_date, datetime):
            raise TypeError("trade_date takes a date, not a datetime; pass value.date()")
        self._trade_date = trade_date
        self._fetcher: Fetcher = (
            fetcher if fetcher is not None else MarketStatisticsFetcher(settings)
        )
        self._calendar = calendar if calendar is not None else ExchangeCalendar.load_default()
        self._now: Callable[[], datetime] = (
            now if now is not None else (lambda: datetime.now(settings.tzinfo))
        )
        self._parsed: ParsedPage | None = None
        # (rows parsed, rows rejected) by validate, for reconcile's threshold pre-check.
        self._validated: tuple[int, int] | None = None

    async def fetch(self, ctx: RunContext) -> MarketStatisticsSnapshot:
        """Check ``robots.txt`` for the page, then render it and capture every sector.

        Raises:
            RobotsDisallowed: ``robots.txt`` refuses the page; no browser is launched.
            FetchFailed: The browser session failed (see :class:`MarketStatisticsFetcher`).
        """
        ctx.details["source_url"] = NSE_MARKET_STATISTICS_URL
        await ctx.http.ensure_allowed(NSE_MARKET_STATISTICS_URL)
        return await self._fetcher.fetch(NSE_MARKET_STATISTICS_URL, user_agent=ctx.http.user_agent)

    def parse(self, raw: MarketStatisticsSnapshot) -> list[NseQuote]:
        """Parse the snapshot and keep its dating evidence for :meth:`reconcile`.

        Raises:
            SourceLayoutChanged: A table is missing or its headers differ, the dropdown offers
                fewer than :data:`MIN_SECTOR_OPTIONS` sectors, or a sector was not captured.
        """
        parsed = parse_page(raw)
        if parsed.option_count < MIN_SECTOR_OPTIONS:
            raise SourceLayoutChanged(
                expected=f">= {MIN_SECTOR_OPTIONS} {SELECTOR_SECTORS} options",
                found=parsed.option_count,
                message=(
                    f"{parsed.option_count} sector options found, fewer than {MIN_SECTOR_OPTIONS}"
                ),
            )
        captured = [table.option_label for table in raw.sector_tables]
        if len(captured) != parsed.option_count:
            raise SourceLayoutChanged(
                expected={"sector_tables": parsed.option_count, "options": parsed.option_labels},
                found={"sector_tables": len(captured), "captured": captured},
                message=(
                    f"{len(captured)} sector tables captured for {parsed.option_count} sector "
                    "options"
                ),
            )
        self._parsed = parsed
        per_sector = Counter(quote.sector for quote in parsed.quotes)
        logger.info(
            "parsed %d NSE rows across %d sectors: %s",
            len(parsed.quotes),
            len(captured),
            ", ".join(f"{label} {per_sector[label]}" for label in captured),
        )
        return list(parsed.quotes)

    def validate(self, records: Sequence[NseQuote]) -> ValidationOutcome[NseQuote]:
        """Reject rows with an invalid ISIN (``invalid_isin``), a missing, non-positive or
        unstorable close (``invalid_close``), a missing or out-of-range volume
        (``invalid_volume``), a turnover beyond its column (``invalid_turnover``) or a missing
        change or one beyond :data:`CHANGE_PCT_BOUND` (``invalid_change``)."""
        valid: list[NseQuote] = []
        rejected: list[RejectedRow] = []
        for index, quote in enumerate(records):
            reasons = _rejection_reasons(quote)
            if reasons:
                row = {**quote.raw, "sector": quote.sector}
                rejected.append(RejectedRow(index=index, reason=";".join(reasons), row=row))
            else:
                valid.append(quote)
        self._validated = (len(records), len(rejected))
        return ValidationOutcome(valid=valid, rejected=rejected)

    async def reconcile(
        self, ctx: RunContext, records: list[NseQuote]
    ) -> ValidationOutcome[NseQuote]:
        """Map, guard, date and check the validated quotes inside the run's transaction.

        1. **Map** each ISIN to its ``terminal_equities`` ticker. Unmapped ISINs are listed in
           ``details.unmapped_isins`` and skipped; an ISIN listed under two sectors is kept once
           (the last), and ``details.duplicate_isins`` lists those whose values differ.
        2. **Stale guard** (:func:`is_stale`) against each ticker's latest stored row.
        3. **Session dating** (:func:`resolve_session`) from the exchange calendar's
           ``expected_session``, or the ``--trade-date`` override.
        4. **Continuity** (:func:`check_continuity`) against the rows stored before the session.

        The dating evidence is recorded in ``details`` before any outcome is raised. When the
        shared thresholds already mean ``layout_changed`` (too few rows, or more than 10%
        rejected), the quotes are returned unchecked, so the framework reports the layout change
        rather than a stale or dating outcome masking it, and nothing is written.

        Raises:
            NoNewData: No tracked equity holds an ISIN, or the source has not advanced.
            SourceLayoutChanged: No row maps to a tracked equity.
            AmbiguousSession: The session cannot be established, or continuity fails.
            CalendarNotVerified: The calendar does not cover a year the dating needs.
        """
        parsed = self._parsed
        if parsed is None:
            raise RuntimeError("parse() must run before reconcile()")
        ctx.details.update(
            {
                "option_count": parsed.option_count,
                "sector_rows": dict(Counter(quote.sector for quote in parsed.quotes)),
                "link_href": parsed.link_href,
                "link_session": _iso(parsed.link_session),
                "label_session": _iso(parsed.label_session),
            }
        )
        if (
            self._validated is not None
            and evaluate_thresholds(*self._validated, self.min_rows) is RunStatus.LAYOUT_CHANGED
        ):
            return ValidationOutcome(valid=records, rejected=[])

        quotes_by_ticker = await self._map_tickers(ctx, records)
        tickers = list(quotes_by_ticker)
        latest = await _load_latest_rows(ctx.conn, tickers)
        if is_stale(quotes_by_ticker, latest):
            stale = {"repeated": _repeats(quotes_by_ticker, latest), "mapped": len(tickers)}
            ctx.details["stale"] = stale
            raise NoNewData("source has not advanced", {"stale": stale})

        trade_date = self._date_session(ctx, parsed)
        previous = self._calendar.previous_session(trade_date)
        stored_before = await _load_latest_rows(ctx.conn, tickers, before=trade_date)
        continuity = check_continuity(quotes_by_ticker, stored_before, previous_session=previous)
        ctx.details["continuity"] = {
            "previous_session": previous.isoformat(),
            "applies": continuity.applies,
            "matched": continuity.matched,
            "eligible": continuity.eligible,
            "passed": continuity.passed,
        }
        if continuity.applies and not continuity.passed:
            logger.info(
                "NSE session %s failed continuity: %d of %d eligible changes agree with %s",
                trade_date,
                continuity.matched,
                continuity.eligible,
                previous,
            )
            raise AmbiguousSession(
                evidence={
                    "expected_session": ctx.details.get("expected_session"),
                    "link_session": _iso(parsed.link_session),
                    "label_session": _iso(parsed.label_session),
                    "trade_date": trade_date.isoformat(),
                    "previous_session": previous.isoformat(),
                    "reason": "continuity_failed",
                    "matched": continuity.matched,
                    "eligible": continuity.eligible,
                }
            )
        return ValidationOutcome(
            valid=[
                dataclasses.replace(quote, trade_date=trade_date)
                for quote in quotes_by_ticker.values()
            ],
            rejected=[],
        )

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[NseQuote], run_id: uuid.UUID
    ) -> int:
        """Write the dated quotes; the price precedence guard keeps CSV-imported prices, and no
        flow column is written, so imported flows survive."""
        return await upsert_prices(
            conn, build_price_rows(records), source=self.name, ingestion_run_id=run_id
        )

    async def _map_tickers(self, ctx: RunContext, records: list[NseQuote]) -> dict[str, NseQuote]:
        result = await ctx.conn.execute(
            select(TerminalEquity.ticker, TerminalEquity.isin).where(
                TerminalEquity.isin.is_not(None)
            )
        )
        isin_to_ticker = {isin: ticker for ticker, isin in result.all() if isin is not None}
        if not isin_to_ticker:
            # Nothing is tracked by ISIN, so there is nothing to write; not a layout change.
            raise NoNewData("no tracked equities with ISINs")

        mapped: list[tuple[str, NseQuote]] = []
        unmapped: dict[str, dict[str, str]] = {}
        for quote in records:
            ticker = isin_to_ticker.get(quote.isin)
            if ticker is None:
                unmapped.setdefault(
                    quote.isin,
                    {"isin": quote.isin, "company": quote.company, "sector": quote.sector},
                )
            else:
                mapped.append((ticker, dataclasses.replace(quote, ticker=ticker)))
        ctx.details["unmapped_isins"] = list(unmapped.values())
        if unmapped:
            logger.warning(
                "%d NSE ISINs are not tracked in terminal_equities and were skipped: %s",
                len(unmapped),
                ", ".join(list(unmapped)[:10]),
            )
        if not mapped:
            raise SourceLayoutChanged(
                expected="ISINs present in terminal_equities",
                found=[quote.isin for quote in records[:10]],
                message="no NSE row maps to a tracked equity",
            )

        by_ticker: defaultdict[str, list[NseQuote]] = defaultdict(list)
        for ticker, quote in mapped:
            by_ticker[ticker].append(quote)
        conflicting = [
            {"isin": quotes[0].isin, "ticker": ticker, "sectors": [q.sector for q in quotes]}
            for ticker, quotes in by_ticker.items()
            if len({(q.close_price, q.volume, q.change_pct) for q in quotes}) > 1
        ]
        if conflicting:
            ctx.details["duplicate_isins"] = conflicting
        unique, _ = dedupe_by_key(mapped, key=lambda pair: pair[0])
        return dict(unique)

    def _date_session(self, ctx: RunContext, parsed: ParsedPage) -> date:
        override = self._trade_date
        expected: date | None = None
        if override is None:
            current = self._now()
            if current.tzinfo is None:
                raise ValueError("now() must return a timezone-aware datetime")
            expected = self._calendar.previous_session(
                current.astimezone(self.settings.tzinfo).date()
            )
        ctx.details.update(
            {
                "expected_session": _iso(expected),
                "trade_date_override": override is not None,
                "trade_date": None,
            }
        )
        try:
            trade_date = resolve_session(
                expected_session=expected,
                link_session=parsed.link_session,
                label_session=parsed.label_session,
                trade_date_override=override,
            )
        except AmbiguousSession as exc:
            logger.info(
                "NSE session dating ambiguous (%s): expected %s, link %s, label %s",
                exc.evidence.get("reason"),
                _iso(expected),
                _iso(parsed.link_session),
                _iso(parsed.label_session),
            )
            raise
        ctx.details["trade_date"] = trade_date.isoformat()
        logger.info(
            "NSE session dated %s (%s): expected %s, link %s, label %s",
            trade_date,
            "--trade-date override" if override is not None else "link date matches",
            _iso(expected),
            _iso(parsed.link_session),
            _iso(parsed.label_session),
        )
        return trade_date
