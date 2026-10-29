"""Tests for the NSE market-statistics pipeline's pure stages
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

The one ``live`` test renders the real public page, after its ``robots.txt`` check, and checks
its layout only; it is skipped by default.

Expected values are literals from the fixtures, compared as exact ``Decimal`` and ``int`` values.
"""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from bs4 import BeautifulSoup

from opennse_terminal.config import MAX_SHARE_COUNT, TerminalSettings
from opennse_terminal.ingestion.base import (
    AmbiguousSession,
    FetchFailed,
    RunStatus,
    SourceLayoutChanged,
    evaluate_thresholds,
)
from opennse_terminal.ingestion.http import PoliteClient, build_user_agent
from opennse_terminal.ingestion.nse.market_statistics import (
    CHANGE_PCT_BOUND,
    EXPECTED_HEADERS,
    ISIN_RE,
    MIN_ROWS,
    MIN_SECTOR_OPTIONS,
    NSE_MARKET_STATISTICS_URL,
    ContinuityResult,
    MarketStatisticsFetcher,
    MarketStatisticsPipeline,
    MarketStatisticsSnapshot,
    NseQuote,
    SectorTable,
    StoredRow,
    check_continuity,
    is_stale,
    parse_page,
    parse_price_list_date,
    parse_sector_table,
    parse_statistics_as_of,
    resolve_session,
)

NAIROBI = ZoneInfo("Africa/Nairobi")
FETCHED_AT = datetime(2026, 10, 28, 7, 30, tzinfo=NAIROBI)
"""When the recorded snapshot was taken: the scheduled 07:30 run, before the 09:30 open."""

SESSION = date(2026, 10, 27)
"""The session the recorded values belong to, and the date in the price-list link."""

LABEL_DATE = date(2026, 10, 28)
"""The recorded "Statistics as of" label: one day ahead of the values."""

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
"""The ``#filtersectors`` option values of the recorded page, in dropdown order."""

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
"""The eight seeded counters, with the values the recorded session carries for them."""


# --- Helpers ----------------------------------------------------------------------------------


def snapshot(
    page_html: str, sector_html: Mapping[str, str], slugs: Sequence[str] = SLUGS
) -> MarketStatisticsSnapshot:
    """A snapshot of ``page_html`` with the recorded table of each sector in ``slugs``."""
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
    """A captured ``table.nsetable`` with one header row and one data row."""
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
    """The recorded table of every sector, by option value."""
    return {slug: fixture_text(f"nse/sectors/{slug}.html") for slug in SLUGS}


@pytest.fixture(scope="module")
def page_shell(fixture_text: Callable[[str], str]) -> str:
    """The recorded page, its own equity table left empty."""
    return fixture_text("nse/page_shell.html")


@pytest.fixture(scope="module")
def quotes_by_sector(sector_html: Mapping[str, str]) -> dict[str, list[NseQuote]]:
    """Every recorded sector table, parsed, by option value."""
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
def test_each_seeded_counter_appears_once_with_its_recorded_values(
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


def test_every_recorded_row_meets_the_production_checks(
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
    """A captured ``table.nsetable`` with the source headers and one placeholder row."""
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
    recorded = len(parse_page(snapshot(page_shell, sector_html)).quotes)
    error_row = placeholder_row("Service unavailable")
    real = sector_html["real"].replace("</tbody>", f"{error_row}</tbody>")
    assert real != sector_html["real"]

    records = pipeline.parse(snapshot(page_shell, {**sector_html, "real": real}))
    outcome = pipeline.validate(records)

    assert len(records) == recorded + 1
    (rejected,) = outcome.rejected
    assert records[rejected.index].company == "Service unavailable"
    assert rejected.row["sector"] == LABEL_BY_SLUG["real"]
    assert "invalid_isin" in rejected.reason.split(";")
    assert len(outcome.valid) == recorded
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
    """The recorded page with only its first ``keep`` sector options."""
    soup = BeautifulSoup(page_shell, "lxml")
    select = soup.select_one("#filtersectors")
    assert select is not None
    for option in select.find_all("option")[keep:]:
        option.decompose()
    return str(soup)


def test_pipeline_parse_returns_every_recorded_row_and_all_pass_validation(
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
    # Rows appended to the recorded Banking table reach the shared thresholds as rejected rows,
    # so the run ends partial with every recorded row kept, not failed.
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
