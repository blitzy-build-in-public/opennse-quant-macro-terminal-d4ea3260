"""``cbk_reserves``: CBK foreign-exchange reserves from the Weekly Bulletin PDF.

Source: Central Bank of Kenya, Weekly Bulletin (public PDF releases). The listing page and every
bulletin are fetched through the run's ``PoliteClient`` (``robots.txt``, per-host spacing,
retries, 25 MiB cap), the listing is parsed with BeautifulSoup4 and the PDF text layer read with
pdfplumber. No browser, OCR or third-party API is involved.

Bulletin reading. A downloaded bulletin is untrusted, so its text layer is read by
``ingestion.files.pdf_line_items.iter_page_text`` in the separate parser process the PDF upload
uses, never in the pipeline's own process. That process has 40 seconds of wall time for the first
50 pages and 1 GiB of address space; it rejects cross-reference sections that list more objects
than the file holds, bounds each page's decoded content to 8 MiB before the page is interpreted,
lists pages one at a time so a longer page tree is never walked past page 50, and sends at most 1
Mi characters of text. It sends each page as soon as it has read it, and the pages are searched in
order: reading stops at the page holding the reserves table, where the process is killed, so a
later page can neither delay nor decide the outcome. Before the table, a bulletin that does not
open, or a page the reader cannot get past (unparseable, over 8 MiB of content, past the text
cap, beyond the memory limit), ends the run ``layout_changed`` ("bulletin unreadable: <reason>")
with nothing written; reaching the time limit, or a fault of the parser process, ends it
``failed``.

Listing and publication dates. ``GET https://www.centralbank.go.ke/releases/weekly-bulletin/``
lists each bulletin as a link whose path contains ``/uploads/weekly_bulletin/`` and ends ``.pdf``;
only links on the CBK origin (HTTPS, default port, no userinfo) are followed. A link's
publication date is read from its anchor text, else from its URL-decoded file name without the
numeric upload-id prefix: "Weekly CBK Bulletin" followed by ``Month D, YYYY`` or ``D Month,
YYYY`` (full or abbreviated month, ``Sept`` included), or a bare day-first ``DD-MM-YYYY`` or
``DD-MM-YY`` date, the form the listing's anchor texts take. Undated links are skipped, and one
link per publication date is kept (the first on the page).

Selection. By default the newest listed bulletin is fetched only when it is newer than every
bulletin an earlier ``succeeded`` or ``partial`` run recorded in ``details.bulletins``; otherwise
the run ends ``no_new_data``. ``limit=N`` (1-26; ``opennse-terminal ingest cbk_reserves --limit
N``) fetches the newest N whatever was ingested before, for backfills. A run makes 1 + N requests
after ``robots.txt``.

Table location. The reserves table starts at the line "Table <n>: Foreign Exchange Reserves (USD
Million)" whose next non-empty line holds the table's observation dates (``DD-Mon-YY``, for
example ``08-Jan-26 ... 05-Feb-26``, strictly ascending); the table number varies between
bulletins. Its value row is the first later line on that page, before the next table, starting
"1. CBK Foreign Exchange Reserves (USD Million)", with exactly one whitespace-separated cell per
date, latest last; any other cell count, residual text included, is a layout change. Each cell
must be one number as a whole: plain (``12384``), comma-grouped in threes (``12,384``), with an
optional fraction (``9,051.5``), and optionally signed (``-``, ``+``, U+2212 minus) or enclosed in
parentheses. Any other cell (``bad12384``, ``12,384%``, ``1,2384``) is rejected as unparsable,
never read for the digits in it.

Dating, units and bounds. Each value is written to ``terminal_macro_indicators.fx_reserves_usd_m``
in millions of USD (NUMERIC(14,2), quantized ``ROUND_HALF_UP``) at **its column's header date**,
never at the bulletin's publication date. Values outside [100, 100,000] are rejected: sanity
bounds that catch parse, scale and column-shift errors. A negative value (``-12,384`` or
``(12,384)``) is parsed as negative and so rejected, never written as its magnitude. Rejected
cells count against the shared run thresholds: up to 10% of a run's rows rejected gives
``partial``, more gives ``layout_changed`` with nothing written, so one bad cell of a single
five-date bulletin ends the run ``layout_changed``.

Upsert. Conflict key ``observation_date``: ``fx_reserves_usd_m = COALESCE(EXCLUDED, stored)`` and
``source_by_field["fx_reserves_usd_m"] = "cbk_reserves"``. Consecutive bulletins repeat earlier
weeks, so bulletins are parsed oldest first and the newest bulletin's value for a header date
wins, within a run and, through the default selection rule, across runs.

``ingestion.files.pdf_line_items``, which loads pdfplumber, is imported only when a bulletin is
parsed, so importing this module loads neither.
"""

import logging
import re
import uuid
from collections.abc import Collection, Generator, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from itertools import count
from types import MappingProxyType
from typing import Final
from urllib.parse import SplitResult, quote, unquote, urljoin, urlsplit

from bs4 import BeautifulSoup, Tag
from sqlalchemy import Select, case, func, literal, select, true
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.base import (
    NoNewData,
    Pipeline,
    RunContext,
    SourceLayoutChanged,
    ValidationOutcome,
)
from opennse_terminal.ingestion.cbk import (
    CBK_APPROVED_ORIGINS,
    COLUMN_QUANTUM,
    MONTHS,
    MacroRecord,
    clean_text,
    parse_value_cell,
    quantize_to,
    upsert_macro_records,
    validate_macro_records,
)
from opennse_terminal.ingestion.http import describe_url
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "BOUNDS",
    "BULLETIN_PATH_MARKER",
    "HEADER_DATE_RE",
    "INGESTED_STATUSES",
    "MAX_LIMIT",
    "MAX_SCANNED_PAGES",
    "MIN_ROWS",
    "NUMERIC_DATE_RE",
    "OWNED_COLUMNS",
    "PIPELINE_NAME",
    "PUBLICATION_RE",
    "RESERVES_COLUMN",
    "ROW_LABEL",
    "TABLE_TITLE",
    "TABLE_TITLE_RE",
    "WEEKLY_BULLETIN_URL",
    "BulletinLink",
    "BulletinPdf",
    "CbkReservesPipeline",
    "load_latest_ingested_bulletin",
    "parse_bulletin_listing",
    "parse_publication_date",
    "parse_reserves_bulletin",
    "select_bulletins",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk.reserves")

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------

PIPELINE_NAME: Final = "cbk_reserves"
"""Registry name, run name and the ``source_by_field`` value of the reserves column."""

WEEKLY_BULLETIN_URL: Final = "https://www.centralbank.go.ke/releases/weekly-bulletin/"
"""The public listing page of every CBK Weekly Bulletin PDF."""

BULLETIN_PATH_MARKER: Final = "/uploads/weekly_bulletin/"
"""Path segment (compared lowercased, URL-decoded) of every bulletin PDF link."""

TABLE_TITLE: Final = "table 2: foreign exchange reserves (usd million)"
"""The reserves table title, lowercased, as the February 2026 bulletins print it."""

# The bulletin renumbers its tables (Table 2 in February 2026, Table 3 in October 2026), so the
# title is matched with any table number rather than as the literal TABLE_TITLE; a literal match
# would end every run on a renumbered bulletin layout_changed.
TABLE_TITLE_RE: Final = re.compile(
    r"\btable\s+\d{1,2}\s*:\s*foreign\s+exchange\s+reserves\s+\(usd\s+million\)"
)
"""Matches the reserves table title (on lowercased text) whatever its table number."""

ROW_LABEL: Final = "1. cbk foreign exchange reserves (usd million)"
"""Label, lowercased and whitespace-collapsed, that starts the table's USD-million value row."""

# The listing's file names put the day before or after the month, with or without commas
# ("Weekly CBK Bulletin Feb 6 2026", "Weekly CBK Bulletin, 23 October, 2026"), so both orders are
# accepted, not only "<Month D YYYY>".
PUBLICATION_RE: Final = re.compile(
    r"weekly\s+cbk\s+bulletin[\s,.:\-\u2013\u2014]*"
    r"(?:(?P<month>[a-z]+)\.?\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s+(?P<year>\d{4})"
    r"|(?P<day_first>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month_after>[a-z]+)\.?,?\s+"
    r"(?P<year_after>\d{4}))(?!\d)",
    re.IGNORECASE,
)
"""A "Weekly CBK Bulletin <date>" title in an anchor text or file name, month first or day first."""

# The listing's anchor texts are bare day-first dates ("06-02-2026", "07-10-16").
NUMERIC_DATE_RE: Final = re.compile(r"(\d{1,2})([-/])(\d{1,2})\2(\d{4}|\d{2})")
"""A whole anchor text or file stem that is a day-first ``DD-MM-YYYY`` or ``DD-MM-YY`` date."""

HEADER_DATE_RE: Final = re.compile(r"\b(\d{1,2})-([A-Za-z]{3,4})-(\d{2})\b")
"""One observation date in the table's date row, ``08-Jan-26`` (two-digit year = 2000 + YY)."""

# A value cell is matched whole, never searched for a number in it, which would read "bad12384"
# as 12384. The match also runs before the shared cell reader, which drops every comma and a
# trailing "%" and so would read "1,2384" or "12,384%" as 12384.
VALUE_TOKEN_RE: Final = re.compile(
    r"(?:(?P<open>\()|[-+\u2212]?)(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?(open)\))", re.ASCII
)
"""The whole of one value-row cell, used with ``fullmatch``: ASCII digits, plain (``12384``) or
in exact groups of three (``12,384``), with an optional fraction (``9,051.5``), either enclosed in
balanced parentheses (``(12,384)``) or after an optional ``-``, ``+`` or minus sign (U+2212)."""

RESERVES_COLUMN: Final = "fx_reserves_usd_m"
"""The ``terminal_macro_indicators`` column this pipeline writes, in millions of USD."""

OWNED_COLUMNS: Final[tuple[str, ...]] = (RESERVES_COLUMN,)
"""Every column this pipeline writes."""

BOUNDS: Final[Mapping[str, tuple[Decimal, Decimal]]] = MappingProxyType(
    {RESERVES_COLUMN: (Decimal(100), Decimal(100000))}
)
"""Inclusive sanity bounds per owned column (USD millions)."""

MAX_LIMIT: Final = 26
"""Largest ``limit``: half a year of weekly bulletins per backfill run."""

MIN_ROWS: Final = 1
"""Fewest parsed observations a run accepts; every bulletin carries five."""

INGESTED_STATUSES: Final[tuple[str, ...]] = ("succeeded", "partial")
"""Run statuses whose ``details.bulletins`` count as ingested."""

MAX_SCANNED_PAGES: Final = 50
"""Pages of one bulletin searched for the reserves table; a bulletin has about six."""

if not set(OWNED_COLUMNS) <= set(MACRO_INDICATOR_COLUMNS) or set(BOUNDS) != set(OWNED_COLUMNS):
    raise RuntimeError(
        f"cbk_reserves columns {OWNED_COLUMNS!r} and bounds {sorted(BOUNDS)!r} must be "
        f"terminal_macro_indicators columns {MACRO_INDICATOR_COLUMNS!r}, bounded one to one"
    )

# The safe set cbk.find_link percent-encodes discovered file URLs with: spaces become %20, while
# existing escapes, the query and the fragment are kept.
_URL_SAFE: Final = ":/?&=%#~@!$'()*+,;"
_UPLOAD_ID_PREFIX: Final = re.compile(r"^\d+_")
_PDF_SUFFIX: Final = ".pdf"
_PDF_MAGIC: Final = b"%PDF-"
_RESERVES_MENTION: Final = "foreign exchange reserves"
_NEXT_TABLE_PREFIX: Final = "table "
_LINK_SAMPLE_LIMIT: Final = 20
_LINE_SAMPLE_LIMIT: Final = 10
_LISTING_EXPECTED: Final = (
    "Weekly CBK Bulletin <Month D YYYY> PDF links under /uploads/weekly_bulletin/"
)


# ---------------------------------------------------------------------------------------------
# Raw content types
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BulletinLink:
    """One bulletin listed on the weekly-bulletin page.

    Attributes:
        publication_date: The bulletin's date, from its anchor text or file name.
        url: The absolute, percent-encoded PDF URL on the CBK origin.
        title: The anchor text, or the file name when the anchor has no text.
    """

    publication_date: date
    url: str
    title: str


@dataclass(frozen=True, slots=True)
class BulletinPdf:
    """A downloaded bulletin: the raw content :meth:`CbkReservesPipeline.parse` reads."""

    publication_date: date
    url: str
    content: bytes = field(repr=False)


# ---------------------------------------------------------------------------------------------
# Listing and selection (pure)
# ---------------------------------------------------------------------------------------------


def _safe_date(year: int, month: int, day: int) -> date | None:
    """``date(year, month, day)``, or ``None`` when that day does not exist."""
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_publication_date(text: str) -> date | None:
    """Read a bulletin's publication date from an anchor text or a URL-decoded file name.

    Underscores count as spaces. Accepted, case-insensitively: "Weekly CBK Bulletin" followed by
    ``Month D[st|nd|rd|th][,] YYYY`` or ``D Month[,] YYYY`` anywhere in the text (full or
    abbreviated month names of ``cbk.MONTHS``, ``Sept`` and a trailing ``.`` included), or the
    whole text being a day-first ``DD-MM-YYYY`` or ``DD-MM-YY`` date (``-`` or ``/``; a two-digit
    year is 2000 + YY). Returns ``None`` for anything else, an unknown month name included, and for
    a day that does not exist (``Feb 30 2026``).
    """
    cleaned = clean_text(text.replace("_", " "))
    match = PUBLICATION_RE.search(cleaned)
    if match is not None:
        if match.group("month") is not None:
            month_name, day, year = match.group("month", "day", "year")
        else:
            month_name, day, year = match.group("month_after", "day_first", "year_after")
        month = MONTHS.get(month_name.lower())
        return None if month is None else _safe_date(int(year), month, int(day))
    numeric = NUMERIC_DATE_RE.fullmatch(cleaned)
    if numeric is None:
        return None
    day, _separator, month_number, year = numeric.groups()
    full_year = int(year) if len(year) == 4 else 2000 + int(year)
    return _safe_date(full_year, int(month_number), int(day))


def _origin_problem(parts: SplitResult) -> str | None:
    """Why the resolved link ``parts`` is not on an approved CBK origin, or ``None`` when it is.

    The same policy as ``cbk.find_link``: HTTPS on a host of ``cbk.CBK_APPROVED_ORIGINS``, on the
    default port and without userinfo, so a listing page can never direct a fetch elsewhere.
    """
    if parts.scheme != "https":
        return "not HTTPS"
    if "@" in parts.netloc:
        return "userinfo"
    try:
        port = parts.port
    except ValueError:
        return "invalid port"
    if port not in (None, 443):
        return f"port {port}"
    if f"https://{parts.hostname or ''}" not in CBK_APPROVED_ORIGINS:
        return "host not approved"
    return None


def _file_stem(decoded_path: str) -> str:
    """The file name of a URL-decoded path without its ``.pdf`` suffix and upload-id prefix:
    ``/uploads/weekly_bulletin/1216473036_Weekly CBK Bulletin Feb 6 2026.pdf`` gives
    ``Weekly CBK Bulletin Feb 6 2026``."""
    name = decoded_path.rsplit("/", 1)[-1]
    if name.lower().endswith(_PDF_SUFFIX):
        name = name[: -len(_PDF_SUFFIX)]
    return _UPLOAD_ID_PREFIX.sub("", name)


def parse_bulletin_listing(html: str, base_url: str = WEEKLY_BULLETIN_URL) -> list[BulletinLink]:
    """Find the bulletin PDF links on the weekly-bulletin listing page, newest first.

    A link qualifies when its ``href``, resolved against ``base_url``, has a URL-decoded path that
    contains :data:`BULLETIN_PATH_MARKER` and ends ``.pdf`` (case-insensitive), and is on an
    approved CBK origin (HTTPS, default port, no userinfo). Its publication date comes from the
    anchor text, else from the file name (:func:`parse_publication_date`); undated links are
    skipped. One link per publication date is kept, the first on the page. URLs are returned
    percent-encoded as ``cbk.find_link`` returns them (spaces become ``%20``).

    Raises:
        SourceLayoutChanged: No qualifying link carries a publication date. The message says
            whether bulletin links were missing altogether, only off the CBK origin, or present
            but undated; ``found`` lists up to 20 of the links concerned (named by
            ``http.describe_url``), or of the page's links when no bulletin link exists.
    """
    soup = BeautifulSoup(html, "lxml")
    page_links: list[str] = []
    bulletin_links: list[str] = []
    off_origin: list[str] = []
    by_date: dict[date, BulletinLink] = {}
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = anchor.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        page_links.append(describe_url(href.strip()))
        try:
            absolute = urljoin(base_url, href.strip())
            parts = urlsplit(absolute)
        except ValueError:
            # urlsplit refuses unbalanced IPv6 brackets and hosts that normalize into delimiters.
            continue
        path = unquote(parts.path)
        lowered = path.lower()
        if BULLETIN_PATH_MARKER not in lowered or not lowered.endswith(_PDF_SUFFIX):
            continue
        bulletin_links.append(describe_url(absolute))
        problem = _origin_problem(parts)
        if problem is not None:
            off_origin.append(f"{describe_url(absolute)} ({problem})")
            continue
        anchor_text = clean_text(anchor.get_text(" "))
        stem = _file_stem(path)
        published = parse_publication_date(anchor_text) or parse_publication_date(stem)
        if published is None:
            continue
        by_date.setdefault(
            published,
            BulletinLink(
                publication_date=published,
                url=quote(absolute, safe=_URL_SAFE),
                title=anchor_text or stem,
            ),
        )
    if not by_date:
        if not bulletin_links:
            message, found = "weekly bulletin links not found", page_links
        elif len(off_origin) == len(bulletin_links):
            message, found = "weekly bulletin links not on an approved CBK origin", off_origin
        else:
            message, found = "weekly bulletin links without a publication date", bulletin_links
        raise SourceLayoutChanged(
            expected=[_LISTING_EXPECTED], found=found[:_LINK_SAMPLE_LIMIT], message=message
        )
    links = sorted(by_date.values(), key=lambda link: link.publication_date, reverse=True)
    logger.debug(
        "weekly bulletin listing: %d dated bulletins, newest %s",
        len(links),
        links[0].publication_date.isoformat(),
    )
    return links


def select_bulletins(
    links: Sequence[BulletinLink], *, ingested: Collection[date], limit: int | None
) -> list[BulletinLink]:
    """Choose the bulletins a run fetches, returned oldest first.

    With ``limit=N`` the newest N links are chosen whatever ``ingested`` holds (a backfill).
    Without it, only the newest link, and only when its publication date is later than every
    date in ``ingested`` (the bulletins earlier successful runs processed); otherwise nothing.

    The default rule reads "the newest bulletin not listed in an earlier successful run" as "the
    newest bulletin, if newer than everything already ingested". Picking an older, never-ingested
    bulletin instead would let its provisional figures overwrite a newer bulletin's values for the
    header dates both carry, because the incoming value wins the COALESCE merge. A bulletin
    repeats only a rolling window of recent weekly header dates, so the newest bulletin supplies
    a missed bulletin's dates only while they fall inside its window. After a longer gap the
    earlier dates are not written until a backfill with ``limit=N`` (``opennse-terminal ingest
    cbk_reserves --limit N``) fetches the older bulletins that carry them.

    Raises:
        ValueError: ``limit`` is below 1.
    """
    newest_first = sorted(links, key=lambda link: link.publication_date, reverse=True)
    if limit is not None:
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}")
        chosen = newest_first[:limit]
    elif newest_first and (not ingested or newest_first[0].publication_date > max(ingested)):
        chosen = newest_first[:1]
    else:
        chosen = []
    return sorted(chosen, key=lambda link: link.publication_date)


# ---------------------------------------------------------------------------------------------
# Bulletin PDF parsing (pure)
# ---------------------------------------------------------------------------------------------


def _unreadable(reason: str) -> SourceLayoutChanged:
    """The layout change for a bulletin the PDF reader could not read, ``reason`` being the
    reader's own fixed text (never text taken from the document)."""
    return SourceLayoutChanged(
        expected=["PDF with a text layer"], found=[reason], message=f"bulletin unreadable: {reason}"
    )


def _iter_page_lines(pdf_bytes: bytes) -> Generator[tuple[int, list[str]], None, None]:
    """Yield ``(page number, non-empty cleaned text lines)`` for the first
    :data:`MAX_SCANNED_PAGES` pages, page 1 first, each as soon as the reader has read it.

    The pages stream from ``pdf_line_items.iter_page_text`` and its time- and memory-limited
    parser process (see the module docstring); a longer bulletin is read to page
    :data:`MAX_SCANNED_PAGES` only. Closing this generator, as :func:`parse_reserves_bulletin`
    does at the reserves table, closes the reader, which kills that process at once, so a caller
    that stops at the table reads no further page and no later page decides the outcome. A page
    the reader cannot get past raises when it is reached, after the pages before it.

    Raises:
        SourceLayoutChanged: ``"bulletin unreadable: <reason>"``, with the reader's fixed reason
            as ``found``, when the PDF does not open (unparseable, encrypted with a password,
            cross-reference sections listing more objects than it holds, no pages), or the
            reading reaches a page it cannot get past (unparseable, over 8 MiB of decoded
            content, past the text cap, beyond the memory limit or stopped by a signal).
        PdfParseTimeout: The reading was still going at its wall-clock limit.
        RuntimeError: The parser process could not run or failed: a server fault.
    """
    # Deferred: only this pipeline reads PDFs, and the reader's module loads pdfplumber.
    from opennse_terminal.ingestion.files import pdf_line_items

    reader = pdf_line_items.iter_page_text(pdf_bytes, max_pages=MAX_SCANNED_PAGES)
    with closing(reader) as pages:
        for number in count(start=1):
            try:
                text = next(pages, None)
            except (pdf_line_items.PdfUnreadable, pdf_line_items.PdfEncrypted) as exc:
                raise _unreadable(str(exc)) from exc
            if text is None:
                return
            lines = [line for line in (clean_text(raw) for raw in text.splitlines()) if line]
            yield number, lines


def _header_dates(matches: Sequence[re.Match[str]]) -> list[tuple[date, str]]:
    """The date row's ``(observation date, source text)`` pairs.

    Raises:
        SourceLayoutChanged: A date does not exist or has an unknown month, or the dates are not
            strictly ascending.
    """
    parsed: list[tuple[date, str]] = []
    for match in matches:
        day, month_name, year = match.groups()
        month = MONTHS.get(month_name.lower())
        observed = None if month is None else _safe_date(2000 + int(year), month, int(day))
        if observed is None:
            raise SourceLayoutChanged(
                expected=["DD-Mon-YY header dates"],
                found=[match.group(0)],
                message="reserves table header date unparseable",
            )
        if parsed and observed <= parsed[-1][0]:
            raise SourceLayoutChanged(
                expected=["strictly ascending header dates"],
                found=[text for _, text in parsed] + [match.group(0)],
                message="reserves table header dates not ascending",
            )
        parsed.append((observed, match.group(0)))
    return parsed


def _value_tokens(following: Sequence[str], count: int) -> list[str]:
    """The value cells of the value row: the first line of ``following``, before the next line
    that starts a table, whose lowercased text starts with :data:`ROW_LABEL`. Every
    whitespace-separated cell after the label counts, whatever it holds, so residual text in the
    row is a count mismatch rather than a skipped word; :func:`_reserves_records` checks each
    cell's grammar.

    Raises:
        SourceLayoutChanged: No such line, or its cell count differs from ``count``.
    """
    scanned: list[str] = []
    for line in following:
        lowered = line.lower()
        if lowered.startswith(_NEXT_TABLE_PREFIX):
            break
        if lowered.startswith(ROW_LABEL):
            # Sliced on the lowercased text: lowercasing keeps ASCII lengths but not every
            # character's, so the original line is never cut at the label's length.
            tokens = lowered[len(ROW_LABEL) :].split()
            if len(tokens) != count:
                raise SourceLayoutChanged(
                    expected=[f"{count} values"],
                    found=[str(len(tokens))],
                    message=f"reserves row has {len(tokens)} values for {count} header dates",
                )
            return tokens
        scanned.append(lowered)
    raise SourceLayoutChanged(
        expected=[ROW_LABEL],
        found=scanned[:_LINE_SAMPLE_LIMIT],
        message="reserves value row not found",
    )


def _reserves_records(
    dates: Sequence[tuple[date, str]], tokens: Sequence[str], page_number: int
) -> list[MacroRecord]:
    """One record per date-value pair, dated at the column's header date.

    A cell that :data:`VALUE_TOKEN_RE` matches whole is read by :func:`parse_value_cell` and
    quantized, so a signed or parenthesized negative stays negative for the bounds to reject. Any
    other cell, or one that does not convert, gives a record without values whose error is
    ``unparsable:fx_reserves_usd_m:<cell>``.
    """
    quantum = COLUMN_QUANTUM[RESERVES_COLUMN]
    records: list[MacroRecord] = []
    for (observed, date_text), token in zip(dates, tokens, strict=True):
        raw = {"date": date_text, "value": token, "page": str(page_number)}
        try:
            if VALUE_TOKEN_RE.fullmatch(token) is None:
                raise ValueError(f"not a reserves value cell: {token!r}")
            value = quantize_to(parse_value_cell(token), quantum)
        except ValueError:
            record = MacroRecord(
                observation_date=observed,
                values={},
                errors=(f"unparsable:{RESERVES_COLUMN}:{token}",),
                raw=raw,
            )
        else:
            record = MacroRecord(
                observation_date=observed, values={RESERVES_COLUMN: value}, raw=raw
            )
        records.append(record)
    return records


def _parse_reserves_page(
    lines: Sequence[str], page_number: int, mentions: list[str]
) -> list[MacroRecord] | None:
    """The page's reserves records, or ``None`` when the page has no reserves table.

    A title line qualifies only when the next line holds header dates, so a contents-page or
    narrative mention of the title is passed over. Lines mentioning foreign exchange reserves are
    appended to ``mentions`` (lowercased, at most ten in all) as layout evidence.
    """
    for index, line in enumerate(lines):
        lowered = line.lower()
        if _RESERVES_MENTION in lowered and len(mentions) < _LINE_SAMPLE_LIMIT:
            mentions.append(lowered)
        if TABLE_TITLE_RE.search(lowered) is None or index + 1 >= len(lines):
            continue
        date_matches = list(HEADER_DATE_RE.finditer(lines[index + 1]))
        if not date_matches:
            continue
        dates = _header_dates(date_matches)
        tokens = _value_tokens(lines[index + 2 :], len(dates))
        return _reserves_records(dates, tokens, page_number)
    return None


def parse_reserves_bulletin(pdf_bytes: bytes) -> list[MacroRecord]:
    """Parse one Weekly Bulletin PDF into ``fx_reserves_usd_m`` records, one per header date.

    The text layer of the first :data:`MAX_SCANNED_PAGES` pages is read with pdfplumber in the
    limited parser process of ``pdf_line_items.iter_page_text``, and its pages are searched in
    order, each as soon as it arrives, until the reserves table is found. The reading stops at
    that page and the process is killed, so a later page can neither delay nor decide the result
    (see the module docstring). The value row holds one whitespace-separated cell per header
    date. Each cell is parsed exactly (``"12,384"`` gives ``Decimal("12384.00")``), quantized to
    0.01 ``ROUND_HALF_UP`` and dated at its column's header date; ``raw`` keeps the date and value
    texts and the page number (1 for the first page). A cell that is not a plain, comma-grouped,
    signed or parenthesized number (:data:`VALUE_TOKEN_RE`), or does not convert, becomes a record
    with an ``unparsable`` error for validation to reject; a negative value (``-12,384``,
    ``(12,384)``) is parsed as negative and rejected by the bounds, never read as its magnitude.

    Raises:
        SourceLayoutChanged: The bytes do not start with ``%PDF-``; the reader cannot open the
            document, or reaches a page it cannot get past before the table is found ("bulletin
            unreadable: <reason>"); no title line is followed by a date row ("reserves table not
            found", ``found`` holding up to ten lines that mention foreign exchange reserves); a
            header date is invalid or the dates do not ascend; the value row is missing; or the
            row's cell count differs from the date count.
        PdfParseTimeout: The reserves table was not reached within the reading's wall-clock
            limit, 40 seconds for :data:`MAX_SCANNED_PAGES` pages; the run ends ``failed``.
        RuntimeError: The parser process could not run or failed: a server fault, so the run
            ends ``failed``.
    """
    if not pdf_bytes.startswith(_PDF_MAGIC):
        raise SourceLayoutChanged(
            expected=["%PDF- header"], found=[repr(pdf_bytes[:8])], message="bulletin is not a PDF"
        )
    mentions: list[str] = []
    with closing(_iter_page_lines(pdf_bytes)) as pages:
        for page_number, lines in pages:
            records = _parse_reserves_page(lines, page_number, mentions)
            if records is not None:
                return records
    raise SourceLayoutChanged(
        expected=[f"{TABLE_TITLE} (any table number)", "date row of DD-Mon-YY dates"],
        found=mentions,
        message="reserves table not found",
    )


# ---------------------------------------------------------------------------------------------
# Run history and the pipeline
# ---------------------------------------------------------------------------------------------


# Runs record each bulletin as date.isoformat(). Only entries of that fixed-width form are
# compared, as text, and none is cast to a date in SQL, where one impossible date would raise and
# abort the run's data transaction.
_RECORDED_DATE_PATTERN: Final = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"


def _iso_date(value: object) -> date | None:
    """``value`` as a date when it is an ISO date string, else ``None``."""
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _latest_recorded_bulletin(below: str | None) -> Select[tuple[str]]:
    """``SELECT max(entry)`` over the ``details.bulletins`` entries of ingested runs: one row.

    The entries are the text elements of each ``succeeded`` or ``partial`` ``cbk_reserves`` run's
    ``details.bulletins`` array that match ``YYYY-MM-DD`` and, when ``below`` is given, sort
    before it. A ``bulletins`` value that is missing or not an array, or a ``details`` that is not
    an object, contributes no element: the ``CASE`` hands ``jsonb_array_elements_text`` an empty
    array instead, because that function raises on a non-array and a ``WHERE`` guard is not
    guaranteed to run first. A non-string element's text never matches the pattern. Entries are
    compared bytewise (collation ``"C"``), so the fixed-width text orders chronologically whatever
    the database collation. Every value is a bound parameter.
    """
    recorded = TerminalIngestionRun.details["bulletins"]
    entries = (
        func.jsonb_array_elements_text(
            case((func.jsonb_typeof(recorded) == "array", recorded), else_=literal([], JSONB))
        )
        .table_valued("value")
        .render_derived(name="bulletin")
        .lateral()
    )
    entry = entries.c.value.collate("C")
    statement = (
        select(func.max(entry))
        .select_from(TerminalIngestionRun)
        .join(entries, true())
        .where(
            TerminalIngestionRun.pipeline == PIPELINE_NAME,
            TerminalIngestionRun.status.in_(INGESTED_STATUSES),
            entries.c.value.regexp_match(_RECORDED_DATE_PATTERN),
        )
    )
    return statement if below is None else statement.where(entry < below)


async def load_latest_ingested_bulletin(conn: AsyncConnection) -> date | None:
    """The latest publication date an earlier ``cbk_reserves`` run ingested, or ``None``.

    Reads the ``details.bulletins`` entries of every run whose status is one of
    :data:`INGESTED_STATUSES`, in the ``YYYY-MM-DD`` form runs record them in
    (``date.isoformat()``). Any other entry, a non-string element and a ``bulletins`` value that
    is not an array are ignored. The reduction runs in PostgreSQL: each bound ``SELECT max(...)``
    returns one row whatever the size of the run history, and no ``details`` document is
    transferred. The greatest entry is checked here with ``date.fromisoformat``; a well-formed but
    impossible one (``2026-02-30``, ``9999-99-99``) is skipped by repeating the query below it,
    until a valid date or nothing remains, so it can neither hide the true latest date nor raise
    inside PostgreSQL.

    Run rows are committed on the recorder's own connection, so ``conn`` (the run's data
    transaction) sees them, and the current run's own row is still ``running`` and therefore not
    read.
    """
    below: str | None = None
    while True:
        candidate = await conn.scalar(_latest_recorded_bulletin(below))
        if candidate is None:
            return None
        latest = _iso_date(candidate)
        if latest is not None:
            return latest
        # Strictly decreasing over a finite set of entries, so the loop ends.
        below = candidate


class CbkReservesPipeline(Pipeline[list[BulletinPdf], MacroRecord]):
    """CBK Weekly Bulletin FX reserves into ``terminal_macro_indicators.fx_reserves_usd_m``.

    Constructed as ``CbkReservesPipeline(settings)`` for the newest bulletin not yet ingested, or
    ``CbkReservesPipeline(settings, limit=N)`` to backfill the newest N (1-26). See the module
    docstring for the listing, selection, table and dating rules. Source: Central Bank of Kenya.
    """

    name = PIPELINE_NAME
    min_rows = MIN_ROWS
    source_url = WEEKLY_BULLETIN_URL

    def __init__(self, settings: TerminalSettings, *, limit: int | None = None) -> None:
        """Raises:
        ValueError: ``limit`` is given and outside 1-:data:`MAX_LIMIT`.
        """
        if limit is not None and not 1 <= limit <= MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
        super().__init__(settings)
        self.limit: int | None = limit

    async def fetch(self, ctx: RunContext) -> list[BulletinPdf]:
        """Fetch the listing, select the bulletins and download each PDF, oldest first.

        Records ``source_url``, ``bulletins`` (ISO publication dates) and ``bulletin_urls`` in
        ``ctx.details`` once the selection is known.

        Raises:
            NoNewData: Without ``limit``, the newest listed bulletin is not newer than the latest
                ingested one (:func:`load_latest_ingested_bulletin`); ``details`` gets the newest
                listed and latest ingested dates.
            SourceLayoutChanged: The listing has no dated bulletin link.
            FetchFailed, RobotsDisallowed: From ``ctx.http``.
        """
        ctx.details["source_url"] = WEEKLY_BULLETIN_URL
        html = await ctx.http.get_text(WEEKLY_BULLETIN_URL)
        links = parse_bulletin_listing(html)
        latest = None if self.limit is not None else await load_latest_ingested_bulletin(ctx.conn)
        selected = select_bulletins(
            links, ingested=() if latest is None else (latest,), limit=self.limit
        )
        if not selected:
            raise NoNewData(
                "no weekly bulletin newer than those already ingested",
                {
                    "newest_listed_bulletin": links[0].publication_date,
                    "latest_ingested_bulletin": latest,
                },
            )
        ctx.details["bulletins"] = [link.publication_date.isoformat() for link in selected]
        ctx.details["bulletin_urls"] = [link.url for link in selected]
        bulletins: list[BulletinPdf] = []
        for link in selected:
            content = await ctx.http.get_bytes(link.url)
            bulletins.append(
                BulletinPdf(publication_date=link.publication_date, url=link.url, content=content)
            )
        return bulletins

    def parse(self, raw: list[BulletinPdf]) -> list[MacroRecord]:
        """Parse every bulletin oldest first, so the newest bulletin's value for a header date
        comes last and wins the upsert's de-duplication. Each record's ``raw`` also names its
        bulletin's publication date."""
        records: list[MacroRecord] = []
        for bulletin in sorted(raw, key=lambda item: item.publication_date):
            published = bulletin.publication_date.isoformat()
            parsed = parse_reserves_bulletin(bulletin.content)
            records.extend(
                replace(record, raw={**record.raw, "bulletin": published}) for record in parsed
            )
            logger.info(
                "parsed CBK weekly bulletin %s: %d reserves observations", published, len(parsed)
            )
        return records

    def validate(self, records: Sequence[MacroRecord]) -> ValidationOutcome[MacroRecord]:
        """Reject undated, unparsable and out-of-bounds records (:data:`BOUNDS`)."""
        return validate_macro_records(records, BOUNDS)

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[MacroRecord], run_id: uuid.UUID
    ) -> int:
        """COALESCE-merge the records into ``terminal_macro_indicators`` on ``observation_date``,
        recording ``cbk_reserves`` as the column's source; the last record per date wins."""
        return await upsert_macro_records(conn, records, source=self.name)
