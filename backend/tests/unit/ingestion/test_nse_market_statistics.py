"""Tests for the NSE market-statistics pipeline's pure stages and its browser adapter
(``opennse_terminal.ingestion.nse.market_statistics``).

Everything here runs without PostgreSQL, a browser or a network. The fixtures are reconstructed
test fixtures for a 2026-10-27 session scenario, not live captures. Sector tables are parsed from
``fixtures/nse/sectors/<option value>.html``, each reproducing one sector's rendered table: real
counter names and ISINs, with values reconstructed for the session as each file's header states.
The page comes from ``fixtures/nse/page_shell.html``, a reconstruction of the page as the 07:30
Africa/Nairobi job sees it, with 13 sector options, the price-list link ``27-OCT-26.pdf`` and a
"Statistics as of" label one day ahead of the values. Session dating, the stale guard and the
continuity check are pure functions, tested on literal quotes and stored rows. Full runs that map
ISINs, date the session and write rows are in ``integration/test_ingestion_pipelines.py``.

The browser adapter, ``MarketStatisticsFetcher``, runs against Playwright doubles installed in
place of ``async_playwright``: they emulate the page scripts it runs and serve these fixtures, so
its navigation, retries, sector selection, render waits, spacing, failure mapping and cleanup are
checked without launching a browser.

The one ``live`` test renders the real public page, after its ``robots.txt`` check, and checks
its layout only; it is skipped by default.

Expected values are literals from the fixtures, compared as exact ``Decimal`` and ``int`` values.
"""

import asyncio
import dataclasses
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

import pytest
from bs4 import BeautifulSoup
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from opennse_terminal.config import MAX_SHARE_COUNT, TerminalSettings
from opennse_terminal.ingestion.base import (
    AmbiguousSession,
    FetchFailed,
    RunStatus,
    SourceLayoutChanged,
    evaluate_thresholds,
)
from opennse_terminal.ingestion.calendar import CalendarNotVerified, ExchangeCalendar
from opennse_terminal.ingestion.http import PoliteClient, build_user_agent
from opennse_terminal.ingestion.nse.market_statistics import (
    _JS_OPTIONS,
    _JS_RENDERED,
    _JS_STATE,
    CHANGE_PCT_BOUND,
    EXPECTED_HEADERS,
    ISIN_RE,
    MIN_ROWS,
    MIN_SECTOR_OPTIONS,
    NAVIGATION_RETRIES,
    NAVIGATION_TIMEOUT_MS,
    NSE_MARKET_STATISTICS_URL,
    RUN_CAP_SECONDS,
    ContinuityResult,
    MarketStatisticsFetcher,
    MarketStatisticsPipeline,
    MarketStatisticsSnapshot,
    NseQuote,
    SectorTable,
    StoredRow,
    build_price_rows,
    check_continuity,
    check_trade_date_override,
    is_stale,
    parse_page,
    parse_price_list_date,
    parse_sector_table,
    parse_statistics_as_of,
    resolve_session,
)

NAIROBI = ZoneInfo("Africa/Nairobi")
FETCHED_AT = datetime(2026, 10, 28, 7, 30, tzinfo=NAIROBI)
"""The fixed run time of the reconstructed scenario: the scheduled 07:30 Africa/Nairobi run on
2026-10-28, before the 09:30 open."""

SESSION = date(2026, 10, 27)
"""The session the reconstructed values are dated to, and the date in the price-list link."""

LABEL_DATE = date(2026, 10, 28)
"""The reconstructed page's "Statistics as of" label: one day ahead of the values."""

PREVIOUS_SESSION = date(2026, 10, 26)
REAL_SOURCE = "nse_market_statistics"

SLUGS = (
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
"""The ``#filtersectors`` option values of the reconstructed page shell, in dropdown order."""

LABELS = (
    "Agricultural",
    "Automobiles & Accessories",
    "Banking",
    "Commercial & Services",
    "Construction & Allied",
    "Energy & Petroleum",
    "Insurance",
    "Investment",
    "Investment Services",
    "Manufacturing & Allied",
    "Telecommunication",
    "Real Estate Investment Trust",
    "Exchange Traded Funds",
)
"""The matching option labels."""

LABEL_BY_SLUG = dict(zip(SLUGS, LABELS, strict=True))

ANCHOR_ROWS = (
    # (ISIN, ticker, sector file, volume, close)
    ("KE1000001402", "SCOM", "tele", 2_754_329, "33.25"),
    ("KE0000000554", "EQTY", "bank", 1_888_186, "95.75"),
    ("KE0000000315", "KCB", "bank", 1_186_912, "85.00"),
    ("KE0000000216", "EABL", "manu", 22_494, "269.50"),
    ("KE0000000547", "KEGN", "energy", 987_240, "10.60"),
    ("KE1000001568", "COOP", "bank", 720_756, "36.70"),
    ("KE0000000067", "ABSA", "bank", 81_334, "30.55"),
    ("KE0000000091", "SBIC", "bank", 11_221, "270.50"),
)
"""The eight seeded counters, with the values the reconstructed session carries for them."""


# --- Helpers ----------------------------------------------------------------------------------


def snapshot(
    page_html: str, sector_html: Mapping[str, str], slugs: Sequence[str] = SLUGS
) -> MarketStatisticsSnapshot:
    """A snapshot of ``page_html`` with the reconstructed table of each sector in ``slugs``."""
    return MarketStatisticsSnapshot(
        page_html=page_html,
        sector_tables=[SectorTable(slug, LABEL_BY_SLUG[slug], sector_html[slug]) for slug in slugs],
        fetched_at=FETCHED_AT,
    )


def quote(isin: str, close: str, volume: int, change: str, ticker: str | None = None) -> NseQuote:
    """A parsed row with exact values built from ``Decimal`` strings."""
    return NseQuote(
        company=f"Company {isin}",
        isin=isin,
        volume=volume,
        close_price=Decimal(close),
        change_pct=Decimal(change),
        sector="Banking",
        raw={
            "company": f"Company {isin}",
            "isin code": isin,
            "volume": str(volume),
            "last traded price": close,
            "change (%)": change,
        },
        ticker=ticker,
    )


def stored(trade_date: date, close: str, volume: int, source: str = REAL_SOURCE) -> StoredRow:
    """A ticker's latest stored ``terminal_daily_prices`` row (keyed by ticker by the caller)."""
    return StoredRow(
        trade_date=trade_date, close_price=Decimal(close), volume=volume, source=source
    )


def ticker_and_isin(index: int) -> tuple[str, str]:
    """A distinct ticker and a well-formed Kenyan ISIN for a synthetic counter."""
    return f"T{index:02d}", f"KE{index:09d}0"


def continuity_inputs(
    changes: Sequence[str],
    *,
    volumes: Sequence[int] | None = None,
    source: str = REAL_SOURCE,
    stored_on: date = PREVIOUS_SESSION,
) -> tuple[dict[str, NseQuote], dict[str, StoredRow]]:
    """One ticker per entry of ``changes``: stored at 100.00 on ``stored_on`` and now trading at
    101.00, an exact 1.00% move, while the source reports ``changes[i]``."""
    quotes: dict[str, NseQuote] = {}
    rows: dict[str, StoredRow] = {}
    for index, change in enumerate(changes):
        ticker, isin = ticker_and_isin(index)
        volume = volumes[index] if volumes is not None else 1_000 + index
        quotes[ticker] = quote(isin, "101.00", volume, change, ticker=ticker)
        rows[ticker] = stored(stored_on, "100.00", 900 + index, source)
    return quotes, rows


def inline_table(headers: Sequence[str], cells: Sequence[str]) -> str:
    """A ``table.nsetable`` as the fetcher captures it, with one header row and one data row."""
    head = "".join(f"<th>{header}</th>" for header in headers)
    row = "".join(f"<td>{cell}</td>" for cell in cells)
    return (
        f'<table class="nsetable"><thead><tr>{head}</tr></thead>'
        f"<tbody><tr>{row}</tr></tbody></table>"
    )


SOURCE_HEADERS = ("Company", "ISIN Code", "Volume", "Last Traded Price", "Change (%)")
"""The header cells as the page prints them."""

EQUITY_CELLS = ("Equity Group Holdings Plc", "KE0000000554", "1888186", "95.75", "0.79")


@pytest.fixture(scope="module")
def sector_html(fixture_text: Callable[[str], str]) -> dict[str, str]:
    """The reconstructed table of every sector, by option value."""
    return {slug: fixture_text(f"nse/sectors/{slug}.html") for slug in SLUGS}


@pytest.fixture(scope="module")
def page_shell(fixture_text: Callable[[str], str]) -> str:
    """The reconstructed page shell, its own equity table left empty."""
    return fixture_text("nse/page_shell.html")


@pytest.fixture(scope="module")
def quotes_by_sector(sector_html: Mapping[str, str]) -> dict[str, list[NseQuote]]:
    """Every reconstructed sector table, parsed, by option value."""
    return {
        slug: parse_sector_table(html, sector=LABEL_BY_SLUG[slug])
        for slug, html in sector_html.items()
    }


# --- Sector table parsing ---------------------------------------------------------------------


def test_bank_table_parses_exact_values(sector_html: Mapping[str, str]) -> None:
    quotes = {item.isin: item for item in parse_sector_table(sector_html["bank"])}

    equity = quotes["KE0000000554"]
    assert type(equity.volume) is int
    assert equity.volume == 1_888_186
    assert isinstance(equity.close_price, Decimal)
    assert equity.close_price == Decimal("95.75")
    assert equity.change_pct == Decimal("0.79")
    assert equity.company == "Equity Group Holdings Plc"
    assert equity.sector == ""
    assert (equity.ticker, equity.trade_date) == (None, None)

    # The printed scale survives: the text "85.00" is not normalized to 85.
    kcb = quotes["KE0000000315"]
    assert kcb.close_price == Decimal("85.00")
    assert str(kcb.close_price) == "85.00"


def test_telecommunication_table_parses_safaricom(sector_html: Mapping[str, str]) -> None:
    (safaricom,) = parse_sector_table(sector_html["tele"], sector="Telecommunication")

    assert safaricom.isin == "KE1000001402"
    assert type(safaricom.volume) is int
    assert safaricom.volume == 2_754_329
    assert safaricom.close_price == Decimal("33.25")
    assert safaricom.sector == "Telecommunication"


@pytest.mark.parametrize(
    ("isin", "ticker", "slug", "volume", "close"),
    ANCHOR_ROWS,
    ids=[row[1] for row in ANCHOR_ROWS],
)
def test_each_seeded_counter_appears_once_with_its_fixture_values(
    quotes_by_sector: Mapping[str, list[NseQuote]],
    isin: str,
    ticker: str,
    slug: str,
    volume: int,
    close: str,
) -> None:
    found = [
        (sector_slug, item)
        for sector_slug, quotes in quotes_by_sector.items()
        for item in quotes
        if item.isin == isin
    ]

    assert len(found) == 1, f"{ticker} ({isin}) found {len(found)} times"
    (found_slug, item) = found[0]
    assert found_slug == slug
    assert type(item.volume) is int
    assert item.volume == volume
    assert item.close_price == Decimal(close)


def test_every_fixture_row_meets_the_production_checks(
    quotes_by_sector: Mapping[str, list[NseQuote]],
) -> None:
    quotes = [item for slug in SLUGS for item in quotes_by_sector[slug]]
    isins = [item.isin for item in quotes]

    assert len(quotes) >= MIN_ROWS
    assert len(set(isins)) == len(isins)
    for item in quotes:
        assert ISIN_RE.fullmatch(item.isin), item.isin
        assert type(item.volume) is int and item.volume >= 0, item.isin
        assert isinstance(item.close_price, Decimal) and item.close_price > 0, item.isin
        assert isinstance(item.change_pct, Decimal), item.isin
        assert abs(item.change_pct) <= CHANGE_PCT_BOUND, item.isin


def test_renamed_price_header_raises_layout_changed(fixture_text: Callable[[str], str]) -> None:
    with pytest.raises(SourceLayoutChanged) as exc_info:
        parse_sector_table(fixture_text("nse/market_statistics_layout_changed.html"))

    assert exc_info.value.expected == EXPECTED_HEADERS
    assert exc_info.value.found == ("company", "isin code", "volume", "closing price", "change (%)")


def test_inline_table_with_the_source_headers_parses() -> None:
    # The control for the mutations below: the inline builder itself yields a valid table.
    (item,) = parse_sector_table(inline_table(SOURCE_HEADERS, EQUITY_CELLS))

    assert (item.isin, item.volume, item.close_price) == (
        "KE0000000554",
        1_888_186,
        Decimal("95.75"),
    )


@pytest.mark.parametrize(
    ("html", "found"),
    [
        pytest.param(
            inline_table(SOURCE_HEADERS[:4], EQUITY_CELLS[:4]),
            ("company", "isin code", "volume", "last traded price"),
            id="change-column-missing",
        ),
        pytest.param(
            inline_table(
                ("Company", "ISIN Code", "Last Traded Price", "Volume", "Change (%)"),
                ("Equity Group Holdings Plc", "KE0000000554", "95.75", "1888186", "0.79"),
            ),
            ("company", "isin code", "last traded price", "volume", "change (%)"),
            id="columns-reordered",
        ),
        pytest.param(
            inline_table(SOURCE_HEADERS, EQUITY_CELLS).replace("nsetable", "bondtable"),
            (),
            id="no-nsetable",
        ),
    ],
)
def test_mutated_layouts_raise_layout_changed(html: str, found: tuple[str, ...]) -> None:
    with pytest.raises(SourceLayoutChanged) as exc_info:
        parse_sector_table(html)

    assert exc_info.value.expected == EXPECTED_HEADERS
    assert exc_info.value.found == found


@pytest.mark.parametrize(
    ("volume_text", "close_text", "change_text", "volume", "close", "change"),
    [
        pytest.param(
            "1,888,186", "95.75", "0.79", 1_888_186, Decimal("95.75"), Decimal("0.79"), id="grouped"
        ),
        pytest.param("-", "95.75", "-", 0, Decimal("95.75"), Decimal(0), id="dash-no-trades"),
        pytest.param(
            "1888186", "95.75", "+0.79%", 1_888_186, Decimal("95.75"), Decimal("0.79"), id="sign"
        ),
        pytest.param("1888186", "n/a", "0.79", 1_888_186, None, Decimal("0.79"), id="no-close"),
        pytest.param("12abc", "95.75", "0.79", None, Decimal("95.75"), Decimal("0.79"), id="junk"),
    ],
)
def test_cell_shapes_convert_exactly_or_to_none(
    volume_text: str,
    close_text: str,
    change_text: str,
    volume: int | None,
    close: Decimal | None,
    change: Decimal | None,
) -> None:
    cells = ("Equity Group Holdings Plc", "KE0000000554", volume_text, close_text, change_text)
    (item,) = parse_sector_table(inline_table(SOURCE_HEADERS, cells))

    assert (item.volume, item.close_price, item.change_pct) == (volume, close, change)


def test_short_row_is_kept_with_nulls_for_validation_to_reject() -> None:
    (item,) = parse_sector_table(inline_table(SOURCE_HEADERS, EQUITY_CELLS[:4]))

    assert item.isin == "KE0000000554"
    assert (item.volume, item.close_price, item.change_pct) == (None, None, None)


def placeholder_row(text: str, attributes: str = 'colspan="5"') -> str:
    """A body row of one cell spanning the table, as a table library renders a message."""
    return f"<tr><td {attributes}>{text}</td></tr>"


def placeholder_table(text: str, attributes: str = 'colspan="5"') -> str:
    """A ``table.nsetable`` as the fetcher captures it, with the source headers and one
    placeholder row."""
    head = "".join(f"<th>{header}</th>" for header in SOURCE_HEADERS)
    return (
        f'<table class="nsetable"><thead><tr>{head}</tr></thead>'
        f"<tbody>{placeholder_row(text, attributes)}</tbody></table>"
    )


@pytest.mark.parametrize(
    ("text", "attributes"),
    [
        pytest.param(
            "No data available in table",
            'valign="top" colspan="5" class="dataTables_empty"',
            id="datatables-empty-table",
        ),
        pytest.param(
            "No matching records found",
            'valign="top" colspan="5" class="dataTables_empty"',
            id="datatables-zero-records",
        ),
        pytest.param("NO DATA", 'colspan="5"', id="upper-case"),
        pytest.param("No records found.", 'colspan="5"', id="final-period"),
        pytest.param("  No\u00a0data \n available ", 'colspan="5"', id="whitespace-runs"),
        pytest.param("No record found", 'colspan="3"', id="partial-span"),
        pytest.param("", 'colspan="5"', id="empty-cell"),
    ],
)
def test_no_data_placeholder_row_is_skipped(text: str, attributes: str) -> None:
    assert parse_sector_table(placeholder_table(text, attributes)) == []


@pytest.mark.parametrize(
    "text",
    ["Service unavailable", "Error loading data", "Please try again later", "Loading..."],
)
def test_other_single_cell_row_is_kept_for_validation_to_reject(
    pipeline: MarketStatisticsPipeline, text: str
) -> None:
    sector = "Real Estate Investment Trust"
    (item,) = parse_sector_table(placeholder_table(text), sector=sector)

    assert (item.company, item.isin) == (text, "")
    assert (item.volume, item.close_price, item.change_pct) == (None, None, None)

    outcome = pipeline.validate([item])

    assert outcome.valid == []
    (rejected,) = outcome.rejected
    assert set(rejected.reason.split(";")) == {
        "invalid_isin",
        "invalid_close",
        "invalid_volume",
        "invalid_change",
    }
    assert rejected.row == {"company": text, "sector": sector}


def test_error_placeholder_in_one_sector_is_counted_and_makes_the_run_partial(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    fixture_rows = len(parse_page(snapshot(page_shell, sector_html)).quotes)
    error_row = placeholder_row("Service unavailable")
    real = sector_html["real"].replace("</tbody>", f"{error_row}</tbody>")
    assert real != sector_html["real"]

    records = pipeline.parse(snapshot(page_shell, {**sector_html, "real": real}))
    outcome = pipeline.validate(records)

    assert len(records) == fixture_rows + 1
    (rejected,) = outcome.rejected
    assert records[rejected.index].company == "Service unavailable"
    assert rejected.row["sector"] == LABEL_BY_SLUG["real"]
    assert "invalid_isin" in rejected.reason.split(";")
    assert len(outcome.valid) == fixture_rows
    assert evaluate_thresholds(len(records), len(outcome.rejected), MIN_ROWS) is RunStatus.PARTIAL


# --- Page parsing and dates -------------------------------------------------------------------


def test_parse_page_reads_options_link_label_and_every_sector(
    page_shell: str, sector_html: Mapping[str, str]
) -> None:
    parsed = parse_page(snapshot(page_shell, sector_html))

    assert parsed.option_count == 13
    assert parsed.option_labels == list(LABELS)
    assert parsed.link_href is not None
    assert parsed.link_href.endswith("27-OCT-26.pdf")
    assert parsed.link_session == SESSION
    assert parsed.label_session == LABEL_DATE
    assert parsed.quotes == [
        item
        for slug in SLUGS
        for item in parse_sector_table(sector_html[slug], sector=LABEL_BY_SLUG[slug])
    ]


def test_parse_page_never_reads_the_shells_own_table(page_shell: str) -> None:
    parsed = parse_page(
        MarketStatisticsSnapshot(page_html=page_shell, sector_tables=[], fetched_at=FETCHED_AT)
    )

    assert parsed.option_count == 13
    assert parsed.quotes == []


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("/wp-content/uploads/27-OCT-26.pdf", date(2026, 10, 27)),
        ("https://www.nse.co.ke/wp-content/uploads/02-Jan-27.pdf", date(2027, 1, 2)),
        ("/wp-content/uploads/05-SEP-99.pdf", date(2099, 9, 5)),
        ("/wp-content/uploads/31-FEB-26.pdf", None),
        ("/wp-content/uploads/27-XYZ-26.pdf", None),
        ("/wp-content/uploads/daily-price-list.pdf", None),
    ],
)
def test_parse_price_list_date_reads_the_file_name(href: str, expected: date | None) -> None:
    assert parse_price_list_date(href) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("28 Oct 2026", LABEL_DATE),
        ("28/10/2026", LABEL_DATE),
        ("October 28, 2026", LABEL_DATE),
        ("2026-10-28", LABEL_DATE),
        ("Statistics as of 28 Oct 2026", LABEL_DATE),
        ("Statistics as of: Wednesday, 28th October 2026", LABEL_DATE),
        ("30/02/2026", None),
        ("not a date", None),
        ("", None),
    ],
)
def test_parse_statistics_as_of_reads_the_label_formats(text: str, expected: date | None) -> None:
    assert parse_statistics_as_of(text) == expected


# --- Session resolution -----------------------------------------------------------------------


def test_matching_link_date_dates_the_snapshot_and_the_label_is_ignored() -> None:
    session = resolve_session(
        expected_session=SESSION,
        link_session=SESSION,
        label_session=LABEL_DATE,
        trade_date_override=None,
    )

    assert session == SESSION


@pytest.mark.parametrize(
    ("link_session", "label_session", "evidence"),
    [
        # Even a label that agrees with the expected session never stands in for the link.
        pytest.param(
            None,
            SESSION,
            {
                "expected_session": "2026-10-27",
                "link_session": None,
                "label_session": "2026-10-27",
                "reason": "link_date_missing",
            },
            id="link-missing",
        ),
        pytest.param(
            date(2026, 10, 26),
            LABEL_DATE,
            {
                "expected_session": "2026-10-27",
                "link_session": "2026-10-26",
                "label_session": "2026-10-28",
                "reason": "link_date_mismatch",
            },
            id="link-differs",
        ),
    ],
)
def test_missing_or_different_link_date_is_ambiguous(
    link_session: date | None, label_session: date, evidence: Mapping[str, str | None]
) -> None:
    with pytest.raises(AmbiguousSession) as exc_info:
        resolve_session(
            expected_session=SESSION,
            link_session=link_session,
            label_session=label_session,
            trade_date_override=None,
        )

    recorded = exc_info.value.evidence
    assert {key: recorded.get(key) for key in evidence} == evidence


@pytest.mark.parametrize(
    "link_session",
    [None, date(2026, 10, 26), SESSION],
    ids=["link-missing", "link-differs", "link-matches"],
)
def test_trade_date_override_dates_the_snapshot_whatever_the_page_shows(
    link_session: date | None,
) -> None:
    override = date(2026, 10, 23)

    session = resolve_session(
        expected_session=SESSION,
        link_session=link_session,
        label_session=LABEL_DATE,
        trade_date_override=override,
    )

    assert session == override


@pytest.mark.parametrize(
    ("trade_date", "link_session", "reason"),
    [
        pytest.param(date(2026, 10, 25), SESSION, "trade_date_not_a_session", id="sunday"),
        pytest.param(date(2026, 12, 25), None, "trade_date_not_a_session", id="christmas-day"),
        # A Thursday session, but two months after the run's local date.
        pytest.param(date(2026, 12, 31), SESSION, "trade_date_in_future", id="future-session"),
    ],
)
def test_trade_date_override_the_page_cannot_show_is_ambiguous(
    trade_date: date, link_session: date | None, reason: str
) -> None:
    with pytest.raises(AmbiguousSession) as exc_info:
        check_trade_date_override(
            trade_date,
            run_date=FETCHED_AT.date(),
            calendar=ExchangeCalendar.load_default(),
            link_session=link_session,
            label_session=LABEL_DATE,
        )

    assert exc_info.value.evidence == {
        "expected_session": None,
        "link_session": link_session.isoformat() if link_session is not None else None,
        "label_session": "2026-10-28",
        "trade_date": trade_date.isoformat(),
        "run_date": "2026-10-28",
        "reason": reason,
    }


@pytest.mark.parametrize(
    "trade_date",
    [FETCHED_AT.date(), SESSION],
    ids=["the-run-date-itself", "an-earlier-session"],
)
def test_trade_date_override_on_a_session_up_to_the_run_date_is_accepted(
    trade_date: date,
) -> None:
    # Returns without raising: the run date is accepted for a run after that day's close.
    check_trade_date_override(
        trade_date,
        run_date=FETCHED_AT.date(),
        calendar=ExchangeCalendar.load_default(),
        link_session=SESSION,
        label_session=LABEL_DATE,
    )


@pytest.mark.parametrize(
    "trade_date",
    # 2028-01-05 is also after the run date: the session check comes first and never guesses.
    [date(2025, 6, 2), date(2028, 1, 5)],
    ids=["2025", "2028"],
)
def test_trade_date_override_in_an_unverified_year_raises_calendar_not_verified(
    trade_date: date,
) -> None:
    with pytest.raises(CalendarNotVerified) as exc_info:
        check_trade_date_override(
            trade_date,
            run_date=FETCHED_AT.date(),
            calendar=ExchangeCalendar.load_default(),
            link_session=SESSION,
            label_session=LABEL_DATE,
        )

    assert exc_info.value.year == trade_date.year


# --- Continuity check and stale guard ---------------------------------------------------------


@pytest.mark.parametrize(
    ("matching", "passed"),
    [(10, True), (9, True), (8, False)],
    ids=["all-agree", "ninety-percent", "eighty-percent"],
)
def test_continuity_needs_ninety_percent_of_eligible_changes_to_agree(
    matching: int, passed: bool
) -> None:
    # A failed check that applies is what the pipeline turns into ambiguous_session; the full
    # runs proving that live in integration/test_ingestion_pipelines.py.
    quotes, rows = continuity_inputs(["1.00"] * matching + ["2.00"] * (10 - matching))

    result = check_continuity(quotes, rows, previous_session=PREVIOUS_SESSION)

    assert result == ContinuityResult(applies=True, matched=matching, eligible=10, passed=passed)


@pytest.mark.parametrize(
    ("stored_close", "close", "change", "matched"),
    [
        pytest.param("100.00", "101.00", "1.05", 1, id="0.05-above"),
        pytest.param("100.00", "101.00", "0.95", 1, id="0.05-below"),
        pytest.param("100.00", "101.00", "1.06", 0, id="0.06-above"),
        pytest.param("100.00", "101.00", "0.94", 0, id="0.06-below"),
        # The implied change is 0.789473...%: compared unrounded, a reported 0.84 is 0.0505 off.
        pytest.param("95.00", "95.75", "0.79", 1, id="unrounded-match"),
        pytest.param("95.00", "95.75", "0.84", 0, id="unrounded-miss"),
    ],
)
def test_continuity_tolerance_is_inclusive_at_five_hundredths(
    stored_close: str, close: str, change: str, matched: int
) -> None:
    quotes = {"EQTY": quote("KE0000000554", close, 1_888_186, change, ticker="EQTY")}
    rows = {"EQTY": stored(PREVIOUS_SESSION, stored_close, 1_500_000)}

    result = check_continuity(quotes, rows, previous_session=PREVIOUS_SESSION)

    assert (result.applies, result.eligible, result.matched) == (True, 1, matched)


def test_zero_volume_tickers_are_not_eligible() -> None:
    # Untraded counters repeat a stale change, so the two that disagree here must not count.
    quotes, rows = continuity_inputs(["1.00"] * 8 + ["3.00"] * 2, volumes=[1_000] * 8 + [0] * 2)

    result = check_continuity(quotes, rows, previous_session=PREVIOUS_SESSION)

    assert result == ContinuityResult(applies=True, matched=8, eligible=8, passed=True)


@pytest.mark.parametrize(
    ("source", "stored_on"),
    [
        pytest.param("seed", PREVIOUS_SESSION, id="seeded-history"),
        pytest.param(REAL_SOURCE, date(2026, 10, 23), id="other-session"),
    ],
)
def test_continuity_does_not_apply_without_real_rows_from_the_previous_session(
    source: str, stored_on: date
) -> None:
    quotes, rows = continuity_inputs(["5.00"] * 10, source=source, stored_on=stored_on)

    result = check_continuity(quotes, rows, previous_session=PREVIOUS_SESSION)

    assert result == ContinuityResult(applies=False, matched=0, eligible=0, passed=True)


@pytest.mark.parametrize(
    ("repeating", "stale"),
    [(10, True), (9, True), (8, False)],
    ids=["all-repeat", "ninety-percent", "eighty-percent"],
)
def test_stale_guard_needs_ninety_percent_repeating_close_and_volume(
    repeating: int, stale: bool
) -> None:
    quotes: dict[str, NseQuote] = {}
    rows: dict[str, StoredRow] = {}
    for index in range(10):
        ticker, isin = ticker_and_isin(index)
        quotes[ticker] = quote(isin, "33.25", 1_000 + index, "0.00", ticker=ticker)
        if index < repeating:
            # Stored at the column's four decimal places: equal by value to the printed 33.25.
            rows[ticker] = stored(PREVIOUS_SESSION, "33.2500", 1_000 + index)
        elif index % 2:
            rows[ticker] = stored(PREVIOUS_SESSION, "33.00", 1_000 + index)
        else:
            rows[ticker] = stored(PREVIOUS_SESSION, "33.2500", 2_000 + index)

    assert is_stale(quotes, rows) is stale


def test_stale_guard_needs_a_mapped_ticker() -> None:
    assert is_stale({}, {}) is False


# --- Pipeline parse and validate (no I/O) -----------------------------------------------------


@pytest.fixture
def pipeline(test_settings: TerminalSettings) -> MarketStatisticsPipeline:
    # Construction launches nothing: the default fetcher starts a browser only in fetch(), and
    # the default calendar reads the closures file shipped with the package.
    return MarketStatisticsPipeline(test_settings)


def shell_with_options(page_shell: str, keep: int) -> str:
    """The reconstructed page shell with only its first ``keep`` sector options."""
    soup = BeautifulSoup(page_shell, "lxml")
    select = soup.select_one("#filtersectors")
    assert select is not None
    for option in select.find_all("option")[keep:]:
        option.decompose()
    return str(soup)


def test_pipeline_parse_returns_every_fixture_row_and_all_pass_validation(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    raw = snapshot(page_shell, sector_html)

    records = pipeline.parse(raw)
    outcome = pipeline.validate(records)

    assert records == parse_page(raw).quotes
    assert len(records) >= MIN_ROWS
    assert outcome.valid == records
    assert outcome.rejected == []


def test_pipeline_parse_accepts_exactly_ten_sector_options(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    raw = snapshot(shell_with_options(page_shell, 10), sector_html, SLUGS[:10])

    records = pipeline.parse(raw)

    assert {item.sector for item in records} <= set(LABELS[:10])


def test_pipeline_parse_rejects_fewer_than_ten_sector_options(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    raw = snapshot(shell_with_options(page_shell, 9), sector_html, SLUGS[:9])
    assert parse_page(raw).option_count == 9

    with pytest.raises(SourceLayoutChanged) as exc_info:
        pipeline.parse(raw)

    assert exc_info.value.found == 9


def test_pipeline_parse_rejects_a_sector_left_uncaptured(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    raw = snapshot(page_shell, sector_html, SLUGS[:12])

    with pytest.raises(SourceLayoutChanged) as exc_info:
        pipeline.parse(raw)

    assert exc_info.value.found == {"sector_tables": 12, "captured": list(LABELS[:12])}


def test_validate_rejects_each_invalid_field(pipeline: MarketStatisticsPipeline) -> None:
    valid = quote("KE0000000554", "95.75", 1_888_186, "0.79")
    invalid = [
        quote("US0378331005", "95.75", 1_000, "0.10"),
        quote("KE0000000315", "0", 1_000, "0.10"),
        quote("KE0000000067", "30.55", MAX_SHARE_COUNT + 1, "0.10"),
        quote("KE0000000091", "270.50", 1_000, "25.01"),
    ]

    outcome = pipeline.validate([valid, *invalid])

    assert outcome.valid == [valid]
    assert len(outcome.rejected) == 4
    assert [(row.index, row.reason) for row in outcome.rejected] == [
        (1, "invalid_isin"),
        (2, "invalid_close"),
        (3, "invalid_volume"),
        (4, "invalid_change"),
    ]
    assert outcome.rejected[0].row["isin code"] == "US0378331005"


@pytest.mark.parametrize(
    ("volume", "change"),
    [(MAX_SHARE_COUNT, "0.79"), (1_000, "25"), (1_000, "-25"), (0, "0.00")],
    ids=["max-share-count", "plus-25", "minus-25", "zero-volume"],
)
def test_validate_accepts_the_inclusive_bounds(
    pipeline: MarketStatisticsPipeline, volume: int, change: str
) -> None:
    record = quote("KE0000000554", "95.75", volume, change)

    outcome = pipeline.validate([record])

    assert outcome.valid == [record]
    assert outcome.rejected == []


OUT_OF_BOUND_CHANGES = (
    "1e1000000",
    "-1E+1000000",
    "25.0000000000000000000000000001",
    "-25.0000000000000000000000000001",
)
"""Change cells the parser keeps exact but the default ``decimal`` context cannot hold: an
exponent beyond its ``Emax`` and, just outside the bound, 30 significant digits against its 28."""


@pytest.mark.parametrize(
    "change_text",
    OUT_OF_BOUND_CHANGES,
    ids=["above-emax", "negative-above-emax", "above-plus-25", "below-minus-25"],
)
def test_validate_rejects_an_exact_change_beyond_the_bound_without_rounding(
    pipeline: MarketStatisticsPipeline, change_text: str
) -> None:
    # abs() would raise decimal.Overflow for the exponents and round the others to 25.
    (item,) = parse_sector_table(inline_table(SOURCE_HEADERS, (*EQUITY_CELLS[:4], change_text)))
    assert item.change_pct == Decimal(change_text)

    outcome = pipeline.validate([item])

    assert outcome.valid == []
    assert [(row.index, row.reason) for row in outcome.rejected] == [(0, "invalid_change")]
    assert outcome.rejected[0].row["change (%)"] == change_text


def test_out_of_bound_changes_are_counted_and_make_the_run_partial(
    pipeline: MarketStatisticsPipeline, page_shell: str, sector_html: Mapping[str, str]
) -> None:
    # Rows appended to the reconstructed Banking table reach the shared thresholds as rejected
    # rows, so the run ends partial with every reconstructed row kept, not failed.
    injected = "".join(
        f"<tr><td>Injected {index}</td><td>{ticker_and_isin(index)[1]}</td><td>1000</td>"
        f"<td>10.00</td><td>{change}</td></tr>"
        for index, change in enumerate(OUT_OF_BOUND_CHANGES)
    )
    tables = {**sector_html, "bank": sector_html["bank"].replace("</tbody>", f"{injected}</tbody>")}

    records = pipeline.parse(snapshot(page_shell, tables))
    outcome = pipeline.validate(records)

    assert [(row.row["change (%)"], row.row["sector"], row.reason) for row in outcome.rejected] == [
        (change, "Banking", "invalid_change") for change in OUT_OF_BOUND_CHANGES
    ]
    assert outcome.valid == parse_page(snapshot(page_shell, sector_html)).quotes
    assert evaluate_thresholds(len(records), len(outcome.rejected), MIN_ROWS) is RunStatus.PARTIAL


def test_turnover_that_rounds_to_just_below_the_limit_is_kept_and_written_rounded(
    pipeline: MarketStatisticsPipeline,
) -> None:
    # 859,282,386,827 x 11,637,617,799.8087 = 9,999,999,999,999,999,999,999.9949, which rounds
    # half up to 9,999,999,999,999,999,999,999.99, the largest value NUMERIC(24,2) holds.
    record = quote("KE0000000554", "11637617799.8087", 859_282_386_827, "0")

    outcome = pipeline.validate([record])
    [row] = build_price_rows([dataclasses.replace(record, ticker="EQTY", trade_date=SESSION)])

    assert outcome.valid == [record]
    assert outcome.rejected == []
    assert str(row["turnover_kes"]) == "9999999999999999999999.99"
    assert row["turnover_estimated"] is True


@pytest.mark.parametrize(
    ("close", "volume"),
    [
        # 151,982,882 x 65,796,883,625,354.5975 = 10^22 - 0.005 exactly: half up gives 10^22.
        ("65796883625354.5975", 151_982_882),
        # 265,371,653 x 37,683,000,000,003.7683 = 9,999,999,999,999,999,999,999.9999.
        ("37683000000003.7683", 265_371_653),
        ("10000000000", MAX_SHARE_COUNT),
        # The largest volume and close each bound allows: the product, about 10^26, exceeds 10^22.
        ("99999999999999.9999", MAX_SHARE_COUNT),
    ],
    ids=["half-cent-below-the-limit", "rounds-up-to-the-limit", "at-the-limit", "largest-inputs"],
)
def test_turnover_that_does_not_fit_its_column_once_rounded_is_rejected(
    pipeline: MarketStatisticsPipeline, close: str, volume: int
) -> None:
    # Close and volume are each within their own bounds; only the stored turnover overflows.
    record = quote("KE0000000554", close, volume, "0")

    outcome = pipeline.validate([record])

    assert outcome.valid == []
    assert [(row.index, row.reason) for row in outcome.rejected] == [(0, "invalid_turnover")]
    with pytest.raises(ValueError, match="no storable turnover"):
        build_price_rows([dataclasses.replace(record, ticker="EQTY", trade_date=SESSION)])


# --- Browser adapter (Playwright doubles, no browser) -----------------------------------------

Call = tuple[str, dict[str, object]]
"""One recorded call: its name and its arguments, by parameter name."""

Pick = Literal["renders", "renders_late", "stale", "other_sector", "select_times_out"]
"""How the double page answers one selection of a sector.

``renders``: the sector table renders at once. ``renders_late``: the select changes at once and
the table renders only once the adapter's wait for it has timed out. ``stale``: the select
changes but the table does not. ``other_sector``: a late response for another sector re-renders
the select and the table for that sector instead. ``select_times_out``: ``select_option`` itself
times out and nothing changes.
"""

Navigation = tuple[int | Exception | None, Exception | None]
"""One navigation attempt: what ``goto`` answers (a status, ``None`` for no response object, or
the error it raises), then the error ``wait_for_selector`` raises, if any."""

ADAPTER_LOGGER = "opennse_terminal.ingestion.nse.market_statistics"

SHELL_TABLE = '<table class="nsetable"></table>'
"""The page shell's own sector table, empty until the page renders a sector into it."""

DATATABLES_EMPTY_ROW = (
    '<tr class="odd"><td valign="top" colspan="5" class="dataTables_empty">'
    "No data available in table</td></tr>"
)
"""The body row DataTables renders in a table without data."""

PAGE_SCRIPTS = {_JS_OPTIONS: "_JS_OPTIONS", _JS_RENDERED: "_JS_RENDERED", _JS_STATE: "_JS_STATE"}
"""The adapter's page scripts by module name: the doubles emulate these and no other."""


def data_rows(html: str) -> int:
    """Rows of the ``table.nsetable`` in ``html`` holding a ``td`` cell, as the page script's
    ``dataRows`` counts them."""
    table = BeautifulSoup(html, "lxml").select_one("table.nsetable")
    if table is None:
        return 0
    return sum(1 for row in table.find_all("tr") if row.find("td", recursive=False) is not None)


def rendered_table(html: str) -> str:
    """A reconstructed sector table as the page renders it.

    The page script counts a sector as rendered only once its table holds a data row, and
    DataTables gives a table without data its "No data available in table" row, which
    :func:`parse_sector_table` skips. A fixture with an empty body is served with that row.
    """
    if data_rows(html) > 0:
        return html
    return html.replace("</tbody>", f"{DATATABLES_EMPTY_ROW}</tbody>")


def script_name(expression: str) -> str:
    """The module name of the page script ``expression``; any other script fails the test."""
    name = PAGE_SCRIPTS.get(expression)
    if name is None:
        pytest.fail(f"the adapter ran a page script the doubles do not emulate: {expression!r}")
    return name


@dataclasses.dataclass(frozen=True)
class Shown:
    """What the double page shows: the sector select's value beside the sector table."""

    value: str | None
    html: str
    paginated: bool = False

    @property
    def rows(self) -> int:
        """The table's data rows, counted as the page script counts them."""
        return data_rows(self.html)

    def state(self) -> dict[str, object]:
        """The ``_JS_STATE`` payload of this view."""
        return {
            "value": self.value,
            "rows": self.rows,
            "html": self.html,
            "paginated": self.paginated,
        }


LATE_RESPONSE = Shown("late-sector", inline_table(SOURCE_HEADERS, EQUITY_CELLS))
"""A late response for an earlier selection: the page re-renders the select with that sector's
value beside that sector's table."""


@dataclasses.dataclass
class Scenario:
    """What the double browser serves to every ``fetch()`` run made against it.

    Attributes:
        options: The ``_JS_OPTIONS`` payload of ``#filtersectors``.
        rendered: By option value, what the page shows once that sector has rendered.
        initial: What a freshly loaded page shows, before any selection.
        page_html: What ``content()`` returns.
        picks: By option value, how each successive selection of it is answered; a selection
            beyond its list renders.
        navigation: The navigation attempts, in order; attempts beyond the list answer 200.
        raises: By call name, the error that call raises.
        hangs: By call name, a call that blocks until cancelled and then raises the given error
            or, for ``None``, lets the cancellation through.
    """

    options: object
    rendered: dict[str, Shown]
    initial: Shown
    page_html: str = "<html><body></body></html>"
    picks: dict[str, list[Pick]] = dataclasses.field(default_factory=dict)
    navigation: list[Navigation] = dataclasses.field(default_factory=list)
    raises: dict[str, Exception] = dataclasses.field(default_factory=dict)
    hangs: dict[str, Exception | None] = dataclasses.field(default_factory=dict)


class BrowserDouble:
    """Stands in for ``async_playwright`` and everything it opens.

    Every call to the doubles, and every spacing sleep, is appended to one ordered :attr:`log`.
    """

    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.log: list[Call] = []
        self.browsers: list[FakeBrowser] = []
        self.handles: list[FakeHandle] = []
        self._navigation = list(scenario.navigation)
        self._picks = {value: list(picks) for value, picks in scenario.picks.items()}

    def async_playwright(self) -> "FakePlaywright":
        """The replacement for ``playwright.async_api.async_playwright``."""
        return FakePlaywright(self)

    async def sleep(self, seconds: float) -> None:
        """The fetcher's spacing wait: recorded, never slept."""
        self.log.append(("sleep", {"seconds": seconds}))

    async def call(self, name: str, **args: object) -> None:
        """Record a call, then raise or block as the scenario scripts it."""
        self.log.append((name, args))
        error = self.scenario.raises.get(name)
        if error is not None:
            raise error
        if name in self.scenario.hangs:
            surfaced = self.scenario.hangs[name]
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if surfaced is None:
                    raise
                raise surfaced from None

    def next_navigation(self) -> Navigation:
        """The outcome of the next navigation attempt."""
        return self._navigation.pop(0) if self._navigation else (200, None)

    def next_pick(self, value: str) -> Pick:
        """How the next selection of ``value`` is answered."""
        picks = self._picks.get(value)
        return picks.pop(0) if picks else "renders"

    def names(self) -> list[str]:
        """The recorded call names, in order."""
        return [name for name, _ in self.log]

    def calls(self, name: str) -> list[dict[str, object]]:
        """The arguments of every recorded call named ``name``, in order."""
        return [args for called, args in self.log if called == name]


class FakePlaywright:
    """The ``async_playwright()`` context manager and the Playwright object it yields."""

    def __init__(self, double: BrowserDouble) -> None:
        self.chromium = FakeChromium(double)

    async def __aenter__(self) -> "FakePlaywright":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeChromium:
    """``playwright.chromium``: launches a :class:`FakeBrowser`."""

    def __init__(self, double: BrowserDouble) -> None:
        self._double = double

    async def launch(self, *, headless: bool) -> "FakeBrowser":
        await self._double.call("launch", headless=headless)
        browser = FakeBrowser(self._double)
        self._double.browsers.append(browser)
        return browser


class FakeBrowser:
    """A launched browser: opens contexts and records its closing."""

    def __init__(self, double: BrowserDouble) -> None:
        self._double = double
        self.contexts: list[FakeContext] = []
        self.closed = False

    async def new_context(self, **options: object) -> "FakeContext":
        await self._double.call("new_context", **options)
        context = FakeContext(self._double)
        self.contexts.append(context)
        return context

    async def close(self) -> None:
        await self._double.call("browser.close")
        self.closed = True


class FakeContext:
    """A browser context: opens pages and records its closing."""

    def __init__(self, double: BrowserDouble) -> None:
        self._double = double
        self.pages: list[FakePage] = []
        self.closed = False

    async def new_page(self) -> "FakePage":
        await self._double.call("new_page")
        page = FakePage(self._double)
        self.pages.append(page)
        return page

    async def close(self) -> None:
        await self._double.call("context.close")
        self.closed = True


@dataclasses.dataclass(frozen=True)
class FakeResponse:
    """The navigation response; the adapter reads only its status."""

    status: int


class FakeHandle:
    """The ``wait_for_function`` result: the rendered state, until disposed."""

    def __init__(self, double: BrowserDouble, payload: dict[str, object]) -> None:
        self._double = double
        self._payload = payload
        self.disposed = False

    async def json_value(self) -> dict[str, object]:
        await self._double.call("json_value")
        return self._payload

    async def dispose(self) -> None:
        await self._double.call("dispose")
        self.disposed = True


class FakePage:
    """The market-statistics page, emulating the adapter's page scripts on :class:`Shown`."""

    def __init__(self, double: BrowserDouble) -> None:
        self._double = double
        self._scenario = double.scenario
        self.shown = double.scenario.initial
        # Rendered once the adapter's current wait for a render times out (Pick "renders_late").
        self._pending: Shown | None = None
        self._selector_error: Exception | None = None

    async def goto(self, url: str, *, timeout: float, wait_until: str) -> FakeResponse | None:
        await self._double.call("goto", url=url, timeout=timeout, wait_until=wait_until)
        response, self._selector_error = self._double.next_navigation()
        if isinstance(response, Exception):
            raise response
        return None if response is None else FakeResponse(response)

    async def wait_for_selector(self, selector: str, *, timeout: float) -> None:
        await self._double.call("wait_for_selector", selector=selector, timeout=timeout)
        if self._selector_error is not None:
            raise self._selector_error

    async def eval_on_selector_all(self, selector: str, expression: str) -> object:
        await self._double.call(
            "eval_on_selector_all", selector=selector, script=script_name(expression)
        )
        return self._scenario.options

    async def evaluate(self, expression: str) -> dict[str, object]:
        await self._double.call("evaluate", script=script_name(expression))
        return self.shown.state()

    async def select_option(self, selector: str, value: str, *, timeout: float) -> list[str]:
        await self._double.call("select_option", selector=selector, value=value, timeout=timeout)
        pick = self._double.next_pick(value)
        if pick == "select_times_out":
            raise PlaywrightTimeoutError(f"Timeout {timeout}ms exceeded.")
        if pick == "renders":
            self.shown = self._scenario.rendered[value]
        elif pick == "other_sector":
            self.shown = LATE_RESPONSE
        else:
            # The select shows the requested sector at once; its table has not changed yet.
            self.shown = dataclasses.replace(self.shown, value=value)
            if pick == "renders_late":
                self._pending = self._scenario.rendered[value]
        return [value]

    async def wait_for_function(
        self, expression: str, *, arg: Mapping[str, object], timeout: float
    ) -> FakeHandle:
        await self._double.call(
            "wait_for_function", script=script_name(expression), arg=dict(arg), timeout=timeout
        )
        shown = self.shown
        # _JS_RENDERED: a changed table, with data rows, beside the requested sector's value.
        if shown.html != arg["before"] and shown.rows > 0 and shown.value == arg["value"]:
            handle = FakeHandle(self._double, shown.state())
            self._double.handles.append(handle)
            return handle
        if self._pending is not None:
            self.shown, self._pending = self._pending, None
        raise PlaywrightTimeoutError(f"Timeout {timeout}ms exceeded.")

    async def content(self) -> str:
        await self._double.call("content")
        return self._scenario.page_html


def option_payload(slugs: Sequence[str]) -> list[object]:
    """The ``_JS_OPTIONS`` payload of the page shell's options in ``slugs``."""
    return [{"value": slug, "label": LABEL_BY_SLUG[slug], "disabled": False} for slug in slugs]


def fixture_scenario(
    page_shell: str, sector_html: Mapping[str, str], slugs: Sequence[str] = SLUGS
) -> Scenario:
    """The reconstructed page: its shell, the listed sectors' tables, and the first listed sector
    selected beside the shell's empty table, as the page loads."""
    return Scenario(
        options=option_payload(slugs),
        rendered={slug: Shown(slug, rendered_table(sector_html[slug])) for slug in slugs},
        initial=Shown(slugs[0], SHELL_TABLE),
        page_html=page_shell,
    )


@pytest.fixture
def install_browser(monkeypatch: pytest.MonkeyPatch) -> Callable[[Scenario], BrowserDouble]:
    """Installs a :class:`BrowserDouble` as ``async_playwright``, which the fetcher imports when
    it runs, so no browser is launched and nothing is requested."""

    def install(scenario: Scenario) -> BrowserDouble:
        double = BrowserDouble(scenario)
        monkeypatch.setattr("playwright.async_api.async_playwright", double.async_playwright)
        return double

    return install


async def fetch_with(double: BrowserDouble, settings: TerminalSettings) -> MarketStatisticsSnapshot:
    """One ``fetch()`` of the market-statistics page through ``double``, with the product's user
    agent and the double's recorded sleep."""
    fetcher = MarketStatisticsFetcher(settings, sleep=double.sleep)
    return await fetcher.fetch(NSE_MARKET_STATISTICS_URL, user_agent=build_user_agent(settings))


def adapter_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The adapter's WARNING messages, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == ADAPTER_LOGGER and record.levelno == logging.WARNING
    ]


def assert_closed(double: BrowserDouble) -> None:
    """Every launched browser was closed, after every context it opened."""
    assert double.browsers, "no browser was launched"
    for browser in double.browsers:
        assert browser.closed
        assert all(context.closed for context in browser.contexts)
    names = double.names()
    assert names[-1] == "browser.close"
    if any(browser.contexts for browser in double.browsers):
        assert names[-2] == "context.close"


def test_browser_session_limits_match_the_politeness_policy(
    test_settings: TerminalSettings,
) -> None:
    # 30 s per navigation and per sector render, two navigation retries, a five-minute session
    # cap, and steps at least 2.0 s apart unless the operator configures a longer interval.
    assert NAVIGATION_TIMEOUT_MS == 30_000
    assert NAVIGATION_RETRIES == 2
    assert RUN_CAP_SECONDS == 300
    assert TerminalSettings.model_fields["scraper_min_interval_seconds"].default >= 2.0
    assert test_settings.scraper_min_interval_seconds >= 2.0


async def test_fetch_captures_every_sector_in_dropdown_order_and_feeds_parse_page(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
) -> None:
    scenario = fixture_scenario(page_shell, sector_html)
    double = install_browser(scenario)
    started = datetime.now(UTC)

    captured = await fetch_with(double, test_settings)

    assert captured.page_html == page_shell
    assert captured.sector_tables == tuple(
        SectorTable(slug, LABEL_BY_SLUG[slug], scenario.rendered[slug].html) for slug in SLUGS
    )
    assert captured.fetched_at.tzinfo is UTC
    assert started <= captured.fetched_at <= datetime.now(UTC)
    assert len(double.handles) == len(SLUGS)
    assert all(handle.disposed for handle in double.handles)
    # The adapter's output is exactly what the parser and the pipeline consume.
    parsed = parse_page(captured)
    assert parsed.option_count == len(SLUGS)
    assert parsed.option_labels == list(LABELS)
    assert parsed.link_session == SESSION
    assert parsed.quotes == [
        item
        for slug in SLUGS
        for item in parse_sector_table(sector_html[slug], sector=LABEL_BY_SLUG[slug])
    ]
    assert_closed(double)


async def test_fetch_makes_exactly_the_expected_browser_calls_in_order(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
) -> None:
    scenario = fixture_scenario(page_shell, sector_html)
    double = install_browser(scenario)
    interval = test_settings.scraper_min_interval_seconds

    await fetch_with(double, test_settings)

    # The page loads with Agricultural selected and the table empty, so every sector, the first
    # included, is selected, and each wait expects the table shown just before its selection.
    selections: list[Call] = []
    before = SHELL_TABLE
    for index, slug in enumerate(SLUGS):
        if index:
            selections.append(("sleep", {"seconds": interval}))
        selections += [
            ("evaluate", {"script": "_JS_STATE"}),
            ("select_option", {"selector": "#filtersectors", "value": slug, "timeout": 30_000}),
            (
                "wait_for_function",
                {
                    "script": "_JS_RENDERED",
                    "arg": {"before": before, "value": slug},
                    "timeout": 30_000,
                },
            ),
            ("json_value", {}),
            ("dispose", {}),
        ]
        before = scenario.rendered[slug].html
    assert double.log == [
        ("launch", {"headless": True}),
        ("new_context", {"user_agent": build_user_agent(test_settings)}),
        ("new_page", {}),
        ("sleep", {"seconds": interval}),
        (
            "goto",
            {
                "url": NSE_MARKET_STATISTICS_URL,
                "timeout": 30_000,
                "wait_until": "domcontentloaded",
            },
        ),
        ("wait_for_selector", {"selector": "#filtersectors", "timeout": 30_000}),
        ("eval_on_selector_all", {"selector": "#filtersectors option", "script": "_JS_OPTIONS"}),
        *selections,
        ("content", {}),
        ("context.close", {}),
        ("browser.close", {}),
    ]


@pytest.mark.parametrize(
    ("options", "headless"),
    [
        pytest.param({}, True, id="default"),
        pytest.param({"headless": True}, True, id="headless"),
        pytest.param({"headless": False}, False, id="headed"),
    ],
)
async def test_fetch_launches_chromium_headless_unless_told_otherwise(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    options: dict[str, bool],
    headless: bool,
) -> None:
    double = install_browser(fixture_scenario(page_shell, sector_html, SLUGS[:2]))
    fetcher = MarketStatisticsFetcher(test_settings, sleep=double.sleep, **options)

    await fetcher.fetch(NSE_MARKET_STATISTICS_URL, user_agent=build_user_agent(test_settings))

    assert double.calls("launch") == [{"headless": headless}]


async def test_each_fetch_gets_a_fresh_browser_and_context_with_only_the_user_agent(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
) -> None:
    double = install_browser(fixture_scenario(page_shell, sector_html))
    fetcher = MarketStatisticsFetcher(test_settings, sleep=double.sleep)
    user_agent = build_user_agent(test_settings)

    first = await fetcher.fetch(NSE_MARKET_STATISTICS_URL, user_agent=user_agent)
    second = await fetcher.fetch(NSE_MARKET_STATISTICS_URL, user_agent=user_agent)

    assert first.sector_tables == second.sector_tables
    assert user_agent.startswith("OpenNSE-Terminal/")
    # No storage state, cookies or other options: only the product's user agent.
    assert double.calls("new_context") == [{"user_agent": user_agent}] * 2
    first_browser, second_browser = double.browsers
    assert first_browser is not second_browser
    for browser in double.browsers:
        (context,) = browser.contexts
        assert len(context.pages) == 1
        assert context.closed
        assert browser.closed
    assert first_browser.contexts[0] is not second_browser.contexts[0]
    assert double.names().count("launch") == 2


@pytest.mark.parametrize(
    ("navigation", "calls", "failures"),
    [
        pytest.param(
            [(None, None)],
            ["sleep", "goto", "wait_for_selector"],
            [],
            id="no-response-object-is-accepted",
        ),
        pytest.param(
            [(399, None)],
            ["sleep", "goto", "wait_for_selector"],
            [],
            id="status-399-is-accepted",
        ),
        pytest.param(
            [(400, None)],
            ["sleep", "goto", "sleep", "goto", "wait_for_selector"],
            ["http_400"],
            id="status-400-is-retried",
        ),
        pytest.param(
            [(503, None)],
            ["sleep", "goto", "sleep", "goto", "wait_for_selector"],
            ["http_503"],
            id="status-503-is-retried",
        ),
        pytest.param(
            [(PlaywrightError("net::ERR_CONNECTION_RESET"), None)],
            ["sleep", "goto", "sleep", "goto", "wait_for_selector"],
            ["Error"],
            id="goto-error-is-retried",
        ),
        pytest.param(
            [(200, PlaywrightTimeoutError("Timeout 30000ms exceeded."))],
            ["sleep", "goto", "wait_for_selector", "sleep", "goto", "wait_for_selector"],
            ["TimeoutError"],
            id="dropdown-wait-timeout-is-retried",
        ),
        pytest.param(
            [(200, PlaywrightError("Target crashed"))],
            ["sleep", "goto", "wait_for_selector", "sleep", "goto", "wait_for_selector"],
            ["Error"],
            id="dropdown-wait-error-is-retried",
        ),
        pytest.param(
            [(404, None), (PlaywrightError("net::ERR_TIMED_OUT"), None), (200, None)],
            ["sleep", "goto", "sleep", "goto", "sleep", "goto", "wait_for_selector"],
            ["http_404", "Error"],
            id="third-attempt-succeeds",
        ),
    ],
)
async def test_navigation_retries_until_the_dropdown_is_shown(
    install_browser: Callable[[Scenario], BrowserDouble],
    make_settings: Callable[..., TerminalSettings],
    page_shell: str,
    sector_html: Mapping[str, str],
    caplog: pytest.LogCaptureFixture,
    navigation: list[Navigation],
    calls: list[str],
    failures: list[str],
) -> None:
    caplog.set_level(logging.WARNING, logger=ADAPTER_LOGGER)
    settings = make_settings(scraper_min_interval_seconds=2.5)
    scenario = fixture_scenario(page_shell, sector_html, SLUGS[:2])
    double = install_browser(dataclasses.replace(scenario, navigation=navigation))

    captured = await fetch_with(double, settings)

    assert [item.option_value for item in captured.sector_tables] == list(SLUGS[:2])
    names = double.names()
    loaded = names.index("eval_on_selector_all")
    assert names[names.index("new_page") + 1 : loaded] == calls
    attempts = len(failures) + 1
    # Every attempt, the first included, waits out the configured spacing first.
    sleeps = [args for name, args in double.log[:loaded] if name == "sleep"]
    assert sleeps == [{"seconds": 2.5}] * attempts
    navigated = {
        "url": NSE_MARKET_STATISTICS_URL,
        "timeout": 30_000,
        "wait_until": "domcontentloaded",
    }
    assert double.calls("goto") == [navigated] * attempts
    assert all(
        args == {"selector": "#filtersectors", "timeout": 30_000}
        for args in double.calls("wait_for_selector")
    )
    assert adapter_warnings(caplog) == [
        f"navigation attempt {attempt} of 3 to the market-statistics page failed ({failure})"
        "; retrying"
        for attempt, failure in enumerate(failures, start=1)
    ]


@pytest.mark.parametrize(
    ("navigation", "failures"),
    [
        pytest.param([(500, None)] * 3, ["http_500"] * 3, id="http-500"),
        pytest.param(
            [(PlaywrightError("net::ERR_NAME_NOT_RESOLVED"), None)] * 3,
            ["Error"] * 3,
            id="goto-error",
        ),
        pytest.param(
            [(200, PlaywrightTimeoutError("Timeout 30000ms exceeded."))] * 3,
            ["TimeoutError"] * 3,
            id="dropdown-wait-timeout",
        ),
        pytest.param(
            [
                (429, None),
                (PlaywrightError("net::ERR_CONNECTION_REFUSED"), None),
                (None, PlaywrightError("Target crashed")),
            ],
            ["http_429", "Error", "Error"],
            id="mixed",
        ),
    ],
)
async def test_three_failed_navigation_attempts_fail_the_fetch_and_close_the_browser(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    caplog: pytest.LogCaptureFixture,
    navigation: list[Navigation],
    failures: list[str],
) -> None:
    caplog.set_level(logging.WARNING, logger=ADAPTER_LOGGER)
    scenario = fixture_scenario(page_shell, sector_html)
    double = install_browser(dataclasses.replace(scenario, navigation=navigation))

    with pytest.raises(FetchFailed) as exc_info:
        await fetch_with(double, test_settings)

    assert (exc_info.value.url, exc_info.value.reason) == (
        NSE_MARKET_STATISTICS_URL,
        "navigation_failed",
    )
    # 1 + NAVIGATION_RETRIES attempts, each after the spacing wait, and no option is read.
    assert double.names().count("goto") == 3
    assert double.calls("sleep") == [{"seconds": test_settings.scraper_min_interval_seconds}] * 3
    assert "eval_on_selector_all" not in double.names()
    assert adapter_warnings(caplog) == [
        f"navigation attempt 1 of 3 to the market-statistics page failed ({failures[0]}); retrying",
        f"navigation attempt 2 of 3 to the market-statistics page failed ({failures[1]}); retrying",
        f"navigation attempt 3 of 3 to the market-statistics page failed ({failures[2]})",
    ]
    assert_closed(double)


async def test_placeholder_disabled_and_malformed_options_are_skipped(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    sector_html: Mapping[str, str],
) -> None:
    options: list[object] = [
        {"value": "", "label": "Select Sector", "disabled": False},
        {"value": "  ", "label": "All Sectors", "disabled": False},
        {"value": "agric", "label": "Agricultural", "disabled": True},
        "bank",
        None,
        {"value": 7, "label": "Seven", "disabled": False},
        {"label": "No value", "disabled": False},
        {"value": "bank", "label": "Banking", "disabled": False},
        {"value": "tele", "label": None, "disabled": False},
        {"value": "manu", "label": "Manufacturing & Allied"},
    ]
    kept = ("bank", "tele", "manu")
    double = install_browser(
        Scenario(
            options=options,
            rendered={slug: Shown(slug, sector_html[slug]) for slug in ("agric", *kept)},
            initial=Shown(None, SHELL_TABLE),
        )
    )

    captured = await fetch_with(double, test_settings)

    assert captured.sector_tables == (
        SectorTable("bank", "Banking", sector_html["bank"]),
        SectorTable("tele", "", sector_html["tele"]),
        SectorTable("manu", "Manufacturing & Allied", sector_html["manu"]),
    )
    assert [args["value"] for args in double.calls("select_option")] == list(kept)
    assert double.calls("eval_on_selector_all") == [
        {"selector": "#filtersectors option", "script": "_JS_OPTIONS"}
    ]


@pytest.mark.parametrize("payload", [None, {}, "agric"], ids=["null", "object", "string"])
async def test_an_options_payload_that_is_not_a_list_yields_no_sectors(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    pipeline: MarketStatisticsPipeline,
    page_shell: str,
    payload: object,
) -> None:
    double = install_browser(
        Scenario(
            options=payload, rendered={}, initial=Shown(None, SHELL_TABLE), page_html=page_shell
        )
    )

    captured = await fetch_with(double, test_settings)

    assert (captured.page_html, captured.sector_tables) == (page_shell, ())
    assert {"evaluate", "select_option"}.isdisjoint(double.names())
    assert_closed(double)
    # The pipeline then finds the shell's 13 options without their tables: a layout change.
    with pytest.raises(SourceLayoutChanged) as exc_info:
        pipeline.parse(captured)
    assert exc_info.value.found == {"sector_tables": 0, "captured": []}


@pytest.mark.parametrize(
    ("initial_value", "initial_table", "selected"),
    [
        pytest.param("agric", "agric", ["auto", "bank"], id="first-sector-already-rendered"),
        pytest.param("agric", None, ["agric", "auto", "bank"], id="first-sector-table-empty"),
        pytest.param("bank", "bank", ["agric", "auto", "bank"], id="another-sector-rendered"),
    ],
)
async def test_the_sector_the_page_already_shows_is_captured_without_selecting_it(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    initial_value: str,
    initial_table: str | None,
    selected: list[str],
) -> None:
    slugs = ("agric", "auto", "bank")
    initial = Shown(
        initial_value, SHELL_TABLE if initial_table is None else sector_html[initial_table]
    )
    scenario = dataclasses.replace(
        fixture_scenario(page_shell, sector_html, slugs), initial=initial
    )
    double = install_browser(scenario)

    captured = await fetch_with(double, test_settings)

    assert captured.sector_tables == tuple(
        SectorTable(slug, LABEL_BY_SLUG[slug], sector_html[slug]) for slug in slugs
    )
    assert [args["value"] for args in double.calls("select_option")] == selected
    assert double.calls("wait_for_function")[0]["arg"] == {
        "before": initial.html,
        "value": selected[0],
    }
    assert len(double.handles) == len(selected)
    assert all(handle.disposed for handle in double.handles)


async def test_spacing_waits_the_configured_interval_before_navigation_and_between_sectors(
    install_browser: Callable[[Scenario], BrowserDouble],
    make_settings: Callable[..., TerminalSettings],
    page_shell: str,
    sector_html: Mapping[str, str],
) -> None:
    settings = make_settings(scraper_min_interval_seconds=2.5)
    double = install_browser(fixture_scenario(page_shell, sector_html))

    await fetch_with(double, settings)

    # One wait before the page load and one before each sector after the first.
    assert double.calls("sleep") == [{"seconds": 2.5}] * len(SLUGS)
    paced = [name for name in double.names() if name in {"sleep", "goto", "select_option"}]
    assert paced == [
        *("sleep", "goto", "select_option"),
        *("sleep", "select_option") * (len(SLUGS) - 1),
    ]


@pytest.mark.parametrize(
    ("picks", "bank_calls", "wait_befores", "warned"),
    [
        pytest.param(
            ["renders_late"],
            ["sleep", "evaluate", "select_option", "wait_for_function", "evaluate"],
            ["agric"],
            False,
            id="renders-just-after-the-wait-gives-up",
        ),
        pytest.param(
            ["stale", "renders"],
            [
                *("sleep", "evaluate", "select_option", "wait_for_function", "evaluate"),
                *("sleep", "select_option", "wait_for_function", "json_value", "dispose"),
            ],
            ["agric", "agric"],
            True,
            id="stale-table-is-selected-again",
        ),
        pytest.param(
            ["other_sector", "renders"],
            [
                *("sleep", "evaluate", "select_option", "wait_for_function", "evaluate"),
                *("sleep", "select_option", "wait_for_function", "json_value", "dispose"),
            ],
            ["agric", "late"],
            True,
            id="late-response-for-another-sector-is-selected-again",
        ),
        pytest.param(
            ["select_times_out", "renders"],
            [
                *("sleep", "evaluate", "select_option", "evaluate"),
                *("sleep", "select_option", "wait_for_function", "json_value", "dispose"),
            ],
            ["agric"],
            True,
            id="select-timeout-is-selected-again",
        ),
    ],
)
async def test_only_a_changed_render_of_the_requested_sector_is_captured(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    caplog: pytest.LogCaptureFixture,
    picks: list[Pick],
    bank_calls: list[str],
    wait_befores: list[str],
    warned: bool,
) -> None:
    caplog.set_level(logging.WARNING, logger=ADAPTER_LOGGER)
    interval = test_settings.scraper_min_interval_seconds
    # Agricultural is already rendered, so a stale table seen once Banking is selected has data
    # rows beside the requested value: only its unchanged HTML tells it apart.
    scenario = dataclasses.replace(
        fixture_scenario(page_shell, sector_html, ("agric", "bank")),
        initial=Shown("agric", sector_html["agric"]),
        picks={"bank": picks},
    )
    double = install_browser(scenario)
    befores = {"agric": sector_html["agric"], "late": LATE_RESPONSE.html}

    captured = await fetch_with(double, test_settings)

    assert captured.sector_tables[1] == SectorTable("bank", "Banking", sector_html["bank"])
    assert double.names() == [
        *("launch", "new_context", "new_page", "sleep", "goto", "wait_for_selector"),
        *("eval_on_selector_all", "evaluate"),
        *bank_calls,
        *("content", "context.close", "browser.close"),
    ]
    assert all(
        args == {"selector": "#filtersectors", "value": "bank", "timeout": 30_000}
        for args in double.calls("select_option")
    )
    assert [args["arg"] for args in double.calls("wait_for_function")] == [
        {"before": befores[name], "value": "bank"} for name in wait_befores
    ]
    assert double.calls("sleep") == [{"seconds": interval}] * (3 if warned else 2)
    assert adapter_warnings(caplog) == (
        ["NSE sector Banking did not render within 30000 ms; selecting it again"] if warned else []
    )
    assert all(handle.disposed for handle in double.handles)


@pytest.mark.parametrize(
    "picks",
    [
        pytest.param(["stale", "stale"], id="stale-twice"),
        pytest.param(["other_sector", "other_sector"], id="another-sector-twice"),
        pytest.param(["select_times_out", "select_times_out"], id="select-times-out-twice"),
        pytest.param(["other_sector", "select_times_out"], id="mixed"),
    ],
)
async def test_a_sector_that_never_renders_fails_the_fetch_and_closes_the_browser(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    caplog: pytest.LogCaptureFixture,
    picks: list[Pick],
) -> None:
    caplog.set_level(logging.WARNING, logger=ADAPTER_LOGGER)
    scenario = dataclasses.replace(
        fixture_scenario(page_shell, sector_html, ("agric", "bank", "tele")),
        initial=Shown("agric", sector_html["agric"]),
        picks={"bank": picks},
    )
    double = install_browser(scenario)

    with pytest.raises(FetchFailed) as exc_info:
        await fetch_with(double, test_settings)

    assert (exc_info.value.url, exc_info.value.reason) == (
        NSE_MARKET_STATISTICS_URL,
        "sector_render_timeout:Banking",
    )
    assert [args["value"] for args in double.calls("select_option")] == ["bank", "bank"]
    assert adapter_warnings(caplog) == [
        "NSE sector Banking did not render within 30000 ms; selecting it again"
    ]
    assert "content" not in double.names()
    assert_closed(double)


@pytest.mark.parametrize(
    ("paginated_slug", "shown_on_load", "label", "rows", "selected"),
    [
        pytest.param("agric", True, "Agricultural", 6, [], id="already-shown"),
        pytest.param("bank", False, "Banking", 10, ["agric", "bank"], id="after-selection"),
    ],
)
async def test_a_paginated_sector_table_is_a_layout_change(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    paginated_slug: str,
    shown_on_load: bool,
    label: str,
    rows: int,
    selected: list[str],
) -> None:
    scenario = fixture_scenario(page_shell, sector_html, ("agric", "bank", "tele"))
    paginated = Shown(paginated_slug, sector_html[paginated_slug], paginated=True)
    scenario.rendered[paginated_slug] = paginated
    if shown_on_load:
        scenario.initial = paginated
    double = install_browser(scenario)

    with pytest.raises(SourceLayoutChanged) as exc_info:
        await fetch_with(double, test_settings)

    assert exc_info.value.expected == "one unpaginated table.nsetable per sector"
    assert exc_info.value.found == {"sector": label, "paginated": True, "rows_on_page": rows}
    assert exc_info.value.message == f"the {label} sector table is paginated"
    # Later sectors are never selected, and nothing is captured.
    assert [args["value"] for args in double.calls("select_option")] == selected
    assert "content" not in double.names()
    assert all(handle.disposed for handle in double.handles)
    assert_closed(double)


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        pytest.param(
            PlaywrightError(
                "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium/chrome"
            ),
            "browser_not_installed: run python -m playwright install --with-deps chromium",
            id="executable-missing",
        ),
        pytest.param(
            PlaywrightError(
                "Looks like Playwright was just installed or updated. Please run the following"
                " command to download new browsers: PLAYWRIGHT INSTALL"
            ),
            "browser_not_installed: run python -m playwright install --with-deps chromium",
            id="install-prompt",
        ),
        pytest.param(
            PlaywrightError("BrowserType.launch: Target page, context or browser has been closed"),
            "browser_launch_failed: Error",
            id="launch-error",
        ),
        pytest.param(
            PlaywrightTimeoutError("BrowserType.launch: Timeout 180000ms exceeded."),
            "browser_launch_failed: TimeoutError",
            id="launch-timeout",
        ),
    ],
)
async def test_a_browser_that_cannot_launch_fails_the_fetch(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    error: PlaywrightError,
    reason: str,
) -> None:
    scenario = fixture_scenario(page_shell, sector_html)
    double = install_browser(dataclasses.replace(scenario, raises={"launch": error}))

    with pytest.raises(FetchFailed) as exc_info:
        await fetch_with(double, test_settings)

    assert (exc_info.value.url, exc_info.value.reason) == (NSE_MARKET_STATISTICS_URL, reason)
    assert exc_info.value.__cause__ is error
    # Nothing else is opened, so there is nothing to close.
    assert double.names() == ["launch"]
    assert double.browsers == []


@pytest.mark.parametrize(
    ("call", "error", "reason"),
    [
        pytest.param("new_context", PlaywrightError("closed"), "browser_error: Error", id="ctx"),
        pytest.param("new_page", PlaywrightError("crashed"), "browser_error: Error", id="page"),
        pytest.param(
            "eval_on_selector_all",
            PlaywrightError("Execution context was destroyed"),
            "browser_error: Error",
            id="options",
        ),
        pytest.param(
            "evaluate", PlaywrightError("Target crashed"), "browser_error: Error", id="state"
        ),
        pytest.param(
            "select_option",
            PlaywrightError("Element is not a <select> element"),
            "browser_error: Error",
            id="select",
        ),
        pytest.param(
            "wait_for_function",
            PlaywrightError("Execution context was destroyed"),
            "browser_error: Error",
            id="wait",
        ),
        pytest.param(
            "json_value", PlaywrightError("Target closed"), "browser_error: Error", id="handle"
        ),
        pytest.param(
            "content", PlaywrightError("Target closed"), "browser_error: Error", id="html"
        ),
        pytest.param(
            "content",
            PlaywrightTimeoutError("Timeout 30000ms exceeded."),
            "browser_error: TimeoutError",
            id="html-timeout",
        ),
    ],
)
async def test_a_browser_error_mid_session_fails_the_fetch_and_closes_the_browser(
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    call: str,
    error: PlaywrightError,
    reason: str,
) -> None:
    scenario = fixture_scenario(page_shell, sector_html, ("agric", "bank"))
    double = install_browser(dataclasses.replace(scenario, raises={call: error}))

    with pytest.raises(FetchFailed) as exc_info:
        await fetch_with(double, test_settings)

    assert (exc_info.value.url, exc_info.value.reason) == (NSE_MARKET_STATISTICS_URL, reason)
    assert exc_info.value.__cause__ is error
    assert double.names().count(call) == 1
    assert_closed(double)


@pytest.mark.parametrize(
    ("call", "surfaces"),
    [
        pytest.param("goto", None, id="navigation"),
        pytest.param("evaluate", None, id="state"),
        pytest.param("wait_for_function", None, id="render-wait"),
        pytest.param(
            "wait_for_function",
            PlaywrightError("Target page, context or browser has been closed"),
            id="render-wait-surfaces-a-playwright-error",
        ),
        pytest.param(
            "eval_on_selector_all",
            PlaywrightError("Execution context was destroyed"),
            id="options-surface-a-playwright-error",
        ),
        pytest.param(
            "content",
            PlaywrightError("Target page, context or browser has been closed"),
            id="content-surfaces-a-playwright-error",
        ),
    ],
)
async def test_the_run_cap_ends_a_hung_session_as_run_timeout(
    monkeypatch: pytest.MonkeyPatch,
    install_browser: Callable[[Scenario], BrowserDouble],
    test_settings: TerminalSettings,
    page_shell: str,
    sector_html: Mapping[str, str],
    call: str,
    surfaces: PlaywrightError | None,
) -> None:
    # A zero cap expires at the first await that yields: the hung call.
    monkeypatch.setattr("opennse_terminal.ingestion.nse.market_statistics.RUN_CAP_SECONDS", 0)
    scenario = fixture_scenario(page_shell, sector_html, ("agric", "bank"))
    double = install_browser(dataclasses.replace(scenario, hangs={call: surfaces}))

    with pytest.raises(FetchFailed) as exc_info:
        # A guard so that a regression fails this test instead of hanging the suite.
        async with asyncio.timeout(5):
            await fetch_with(double, test_settings)

    assert (exc_info.value.url, exc_info.value.reason) == (NSE_MARKET_STATISTICS_URL, "run_timeout")
    if surfaces is None:
        assert isinstance(exc_info.value.__cause__, TimeoutError)
    else:
        assert exc_info.value.__cause__ is surfaces
    names = double.names()
    assert names.count(call) == 1
    assert names[names.index(call) + 1 :] == ["context.close", "browser.close"]
    assert_closed(double)


# --- Live source (skipped by default) ---------------------------------------------------------


@pytest.mark.live
async def test_live_page_keeps_the_expected_layout(
    make_settings: Callable[..., TerminalSettings],
) -> None:
    """Render the real public page and check its layout only.

    Requires network access and Playwright's Chromium build
    (``python -m playwright install --with-deps chromium``); deselected by default through the
    ``-m 'not live'`` addopts. Under the NSE terms the page's data is for personal or internal
    business use only, so nothing is stored and no value is asserted.
    """
    pytest.importorskip("playwright.async_api")
    settings = make_settings(scraper_contact="tests@example.invalid")

    async with PoliteClient(settings) as client:
        await client.ensure_allowed(NSE_MARKET_STATISTICS_URL)
        try:
            captured = await MarketStatisticsFetcher(settings).fetch(
                NSE_MARKET_STATISTICS_URL, user_agent=build_user_agent(settings)
            )
        except FetchFailed as exc:
            if exc.reason.startswith("browser_not_installed"):
                pytest.skip(
                    "Playwright's Chromium is not installed: "
                    "run python -m playwright install --with-deps chromium"
                )
            raise

    # parse_page raises SourceLayoutChanged unless every sector table has EXPECTED_HEADERS.
    parsed = parse_page(captured)
    assert parsed.option_count >= MIN_SECTOR_OPTIONS
    assert len(captured.sector_tables) == parsed.option_count
    assert len(parsed.quotes) >= MIN_ROWS
