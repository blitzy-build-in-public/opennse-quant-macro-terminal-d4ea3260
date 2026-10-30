"""CBK macro-series pipelines and the helpers they share.

Five pipelines load Central Bank of Kenya statistical releases, each in its own submodule:

- ``cbk_rates_csv`` (``rates_csv``): the "Central Bank Rates" CSV (interbank rate, 91/182/364-day
  T-bills, Central Bank Rate);
- ``cbk_inflation`` (``inflation``): the 12-month inflation HTML table;
- ``cbk_fx`` (``fx``): the "Exchange Rates End Period" CSV (KES per USD);
- ``cbk_bonds`` (``bonds``): the Treasury bond auction results HTML table;
- ``cbk_reserves`` (``reserves``): FX reserves from the Weekly Bulletin PDF.

Every page and file is a public release fetched by the pipelines through ``PoliteClient``
(``robots.txt``, per-host spacing, retries). Source: Central Bank of Kenya.

This module holds what four or more of them share: the raw and record types, link discovery by
anchor text, HTML-table and CSV reading, cell parsing to ``Decimal``, range validation and the
macro upsert. Units of the macro columns: ``*_pct`` percentage points (``8.75`` means 8.75%),
``fx_kes_usd`` KES per USD and ``fx_reserves_usd_m`` millions of USD. Macro rows are written to
``terminal_macro_indicators`` with conflict key ``observation_date``; ``cbk_bonds`` writes
``terminal_bond_yields`` itself.

Cells go from ``str`` to ``Decimal`` through ``base.parse_decimal_text``, never through
``float``, and are quantized to the target column's scale with ``ROUND_HALF_UP``. Importing this
package has no side effect beyond a consistency check of its constants: pandas is imported inside
:func:`read_cbk_csv` only, and pdfplumber only by ``reserves``.
"""

import calendar as stdlib_calendar
import io
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from itertools import groupby
from types import MappingProxyType
from typing import Final
from urllib.parse import SplitResult, quote, unquote, urljoin, urlsplit

from bs4 import BeautifulSoup, Tag
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.db.upsert import upsert_macro
from opennse_terminal.ingestion.base import (
    RejectedRow,
    SourceLayoutChanged,
    ValidationOutcome,
    dedupe_by_key,
    normalize_header,
    parse_decimal_text,
)
from opennse_terminal.ingestion.http import describe_url
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "CBK_APPROVED_ORIGINS",
    "CBK_BASE_URL",
    "COLUMN_QUANTUM",
    "MISSING_MARKERS",
    "MONTHS",
    "TENOR_QUANTUM",
    "UPSERT_CHUNK_ROWS",
    "CbkCsv",
    "FetchedFile",
    "FetchedPage",
    "HtmlTable",
    "MacroRecord",
    "clean_text",
    "decode_cbk_bytes",
    "extract_table",
    "find_link",
    "is_missing_cell",
    "month_end",
    "parse_month",
    "parse_value_cell",
    "parse_year",
    "quantize_to",
    "read_cbk_csv",
    "upsert_macro_records",
    "validate_macro_records",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk")

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------

CBK_BASE_URL: Final = "https://www.centralbank.go.ke"
"""Origin of every CBK release page and file."""

CBK_APPROVED_ORIGINS: Final = frozenset({CBK_BASE_URL})
"""Origins, as ``https://<host>``, that :func:`find_link` returns links on. A link qualifies only
when it is HTTPS on one of these hosts, on the default port and without userinfo."""

# Spelled out rather than taken from calendar.month_name, which follows the process locale.
MONTHS: Final[Mapping[str, int]] = MappingProxyType(
    {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "sept": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
)
"""Lowercase month names (full, three-letter, and ``sept``) to month numbers."""

COLUMN_QUANTUM: Final[Mapping[str, Decimal]] = MappingProxyType(
    {
        "inflation_yoy_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "cbr_rate_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "interbank_rate_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "tbill_91d_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "tbill_182d_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "tbill_364d_pct": Decimal("0.0001"),  # NUMERIC(9,4)
        "fx_kes_usd": Decimal("0.0001"),  # NUMERIC(12,4)
        "fx_reserves_usd_m": Decimal("0.01"),  # NUMERIC(14,2)
    }
)
"""Scale of each ``terminal_macro_indicators`` indicator column, for :func:`quantize_to`."""

# A column added to the model without a scale here would be written unquantized; fail at import.
if set(COLUMN_QUANTUM) != set(MACRO_INDICATOR_COLUMNS):
    raise RuntimeError(
        f"COLUMN_QUANTUM keys {sorted(COLUMN_QUANTUM)!r} do not match "
        f"models.macro.MACRO_INDICATOR_COLUMNS {sorted(MACRO_INDICATOR_COLUMNS)!r}"
    )

TENOR_QUANTUM: Final = Decimal("0.01")
"""Scale of ``terminal_bond_yields`` tenor columns (NUMERIC(6,2)), in years."""

MISSING_MARKERS: Final = frozenset({"", "-", "\u2013", "\u2014"})
"""Cell texts CBK uses for "no value": blank, hyphen, en dash and em dash."""

UPSERT_CHUNK_ROWS: Final = 500
"""Rows per upsert call; keeps every statement far below asyncpg's 32,767 bind parameters."""

_YEAR_RE: Final = re.compile(r"[0-9]{4}")
_MONTH_NUMBER_RE: Final = re.compile(r"[0-9]{1,2}")
# Zero-width space and byte-order mark are not whitespace to str.split() but appear in scraped
# cells and Excel exports.
_INVISIBLE: Final = str.maketrans("", "", "\u200b\ufeff")
_YEAR_MIN: Final = 1900
_YEAR_MAX: Final = 2100
_FOOTER_PREFIXES: Final = ("source", "note")
_LINK_SAMPLE_LIMIT: Final = 20
_TABLE_SAMPLE_LIMIT: Final = 5
_QUOTE_SAFE: Final = ":/?&=%#~@!$'()*+,;"


# ---------------------------------------------------------------------------------------------
# Small pure helpers
# ---------------------------------------------------------------------------------------------


def month_end(year: int, month: int) -> date:
    """Return the last day of ``month`` in ``year``, the date monthly series are stored at.

    Raises:
        ValueError: ``month`` is not 1-12, or ``year`` is outside ``date``'s range.
    """
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1-12, got {month}")
    return date(year, month, stdlib_calendar.monthrange(year, month)[1])


def parse_month(text: str) -> int | None:
    """Read a month cell: a name (``July``, ``Jul``, ``Sept``, ``Sep.``; any case) or a number
    ``1``-``12`` (``01`` included). Returns ``None`` for anything else."""
    token = text.strip().lower()
    if token.endswith("."):
        token = token[:-1].rstrip()
    month = MONTHS.get(token)
    if month is not None:
        return month
    if _MONTH_NUMBER_RE.fullmatch(token):
        number = int(token)
        if 1 <= number <= 12:
            return number
    return None


def parse_year(text: str) -> int | None:
    """Read a year cell: exactly four digits between 1900 and 2100, else ``None``."""
    token = text.strip()
    if _YEAR_RE.fullmatch(token) is None:
        return None
    year = int(token)
    return year if _YEAR_MIN <= year <= _YEAR_MAX else None


def is_missing_cell(text: str) -> bool:
    """Whether a cell holds no value: blank, ``-``, en dash or em dash. ``-1.20`` is a value."""
    return clean_text(text) in MISSING_MARKERS


def parse_value_cell(text: str) -> Decimal:
    """Parse a numeric cell exactly: whitespace and a trailing ``%`` are removed, then
    ``base.parse_decimal_text`` reads it (thousands separators, parentheses as negative).

    Raises:
        ValueError: The cell is blank, a missing marker, or not a finite number.
    """
    token = text.strip()
    if token.endswith("%"):
        token = token[:-1].strip()
    value = parse_decimal_text(token)
    if value is None:
        raise ValueError(f"not a number: {text!r}")
    return value


def quantize_to(value: Decimal, quantum: Decimal) -> Decimal:
    """Round ``value`` to the scale of ``quantum`` with ``ROUND_HALF_UP``.

    Raises:
        ValueError: The value does not fit the decimal context's 28 significant digits at that
            scale, which only a garbled cell produces.
    """
    try:
        return value.quantize(quantum, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError(f"cannot quantize {value} to {quantum}") from exc


def decode_cbk_bytes(data: bytes) -> str:
    """Decode a downloaded CBK text file: UTF-8 (a byte-order mark is dropped), falling back to
    Windows-1252, the encoding Excel writes CSV files in.

    Raises:
        UnicodeDecodeError: The bytes are valid in neither encoding.
    """
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("cp1252")


def clean_text(text: str) -> str:
    """Collapse every run of whitespace (non-breaking spaces and line breaks included) to one
    space, drop zero-width characters, and trim."""
    return " ".join(text.translate(_INVISIBLE).split())


# ---------------------------------------------------------------------------------------------
# Raw content and record types
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FetchedPage:
    """Raw content of the HTML-table pipelines (``cbk_inflation``, ``cbk_bonds``)."""

    url: str
    html: str


@dataclass(frozen=True, slots=True)
class FetchedFile:
    """Raw content of the CSV pipelines (``cbk_rates_csv``, ``cbk_fx``): the file discovered on
    ``listing_url`` and downloaded from ``url``."""

    url: str
    listing_url: str
    content: bytes


@dataclass(frozen=True, slots=True)
class MacroRecord:
    """One parsed observation date of ``terminal_macro_indicators`` columns.

    Attributes:
        observation_date: The month-end of a monthly series, or the bulletin column's header date
            for FX reserves. ``None`` only when the row's date could not be resolved, in which
            case ``errors`` says why.
        values: The pipeline's own columns that were present and parsed, already quantized. A
            missing cell (``-`` or blank) is absent: never null and never a rejection.
        errors: Machine-readable problems found while parsing, for example
            ``"unparsable:tbill_91d_pct:abc"``, ``"unknown_month:Foo"`` or ``"no_year"``.
        raw: The source cells, recorded in ``details.rejected`` when the record is rejected.
        line: The 1-based source row, for diagnostics: an HTML table's body row, or a CSV record
            number as in ``CbkCsv.rows``.

    Parsers drop a row whose owned cells are all missing and that has no errors.
    """

    observation_date: date | None
    values: Mapping[str, Decimal]
    errors: tuple[str, ...] = ()
    raw: Mapping[str, str] = field(default_factory=dict)
    line: int | None = None


@dataclass(frozen=True, slots=True)
class HtmlTable:
    """A table found by :func:`extract_table`.

    Attributes:
        headers: The header cells, normalized with ``base.normalize_header``.
        rows: ``(1-based body row index, cleaned cell texts)`` for each body row with at least one
            non-blank cell. A row may have fewer cells than ``headers``.
    """

    headers: tuple[str, ...]
    rows: list[tuple[int, list[str]]]


@dataclass(frozen=True, slots=True)
class CbkCsv:
    """A CSV file read by :func:`read_cbk_csv`.

    Attributes:
        headers: The header cells, normalized with ``base.normalize_header``; pandas names blank
            and repeated headers ``unnamed: <n>`` and ``<name>.1``.
        rows: ``(CSV record number, {normalized header: trimmed cell text})`` for each data row,
            numbered from 1 with the records above the header, the header and blank records
            counted (the file line unless an earlier quoted cell spans line breaks); blank and
            footer rows are not included.
    """

    headers: tuple[str, ...]
    rows: list[tuple[int, dict[str, str]]]


# ---------------------------------------------------------------------------------------------
# Discovery and reading
# ---------------------------------------------------------------------------------------------


def find_link(
    html: str,
    base_url: str,
    *,
    text_contains: str,
    href_suffix: str | None = None,
    href_contains: str | None = None,
) -> str:
    """Find a file link on a CBK listing page by its anchor text, and return its absolute URL.

    CBK file names carry a numeric id prefix that changes with every upload (for example
    ``/uploads/interest_rates/<id>_Central Bank Rates_with Heads.csv``), so links are discovered
    by text, never hard-coded.

    Candidates are links on an approved CBK origin (:data:`CBK_APPROVED_ORIGINS`: HTTPS, the
    default port, no userinfo) whose resolved path (URL-decoded, lowercased, without query or
    fragment) ends with ``href_suffix`` and contains ``href_contains`` when given. An ``http(s)``
    link anywhere else is never returned, so a listing page cannot direct a fetch to another
    host, port or scheme, or make it carry credentials. The first candidate whose anchor text
    contains ``text_contains`` (case-insensitive, whitespace collapsed) wins; when none does, the
    first whose URL-decoded file name, with underscores read as spaces, contains it. The URL is
    returned with unsafe characters percent-encoded (spaces become ``%20``; existing escapes,
    query and fragment are kept).

    Links in ``found`` and in the DEBUG log line are named by ``http.describe_url``: scheme, host,
    port and path, never userinfo, query or fragment.

    Raises:
        SourceLayoutChanged: ``"file link not on an approved CBK origin"`` when no candidate
            matches but an ``http(s)`` link elsewhere would have; ``found`` lists up to 20 of
            those as ``text -> link (problem)``. Otherwise ``"file link not found"`` when nothing
            matches; ``found`` lists up to 20 ``text -> href`` pairs: the ``http(s)`` links of the
            right type, whatever their origin, when there are any, else the page's anchors.
    """
    needle = clean_text(text_contains).lower()
    suffix = href_suffix.lower() if href_suffix is not None else None
    contains = href_contains.lower() if href_contains is not None else None
    soup = BeautifulSoup(html, "lxml")
    seen: list[str] = []
    candidate_seen: list[str] = []
    candidates: list[tuple[str, str, str]] = []  # (anchor text, file name, absolute URL)
    off_origin: list[tuple[str, str, str]] = []  # (anchor text, file name, found entry)
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = anchor.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        anchor_text = clean_text(anchor.get_text(" "))
        seen.append(f"{anchor_text} -> {describe_url(href.strip())}")
        try:
            absolute = urljoin(base_url, href.strip())
            parts = urlsplit(absolute)
        except ValueError:
            # urlsplit refuses unbalanced IPv6 brackets and a host that NFKC normalization turns
            # into URL delimiters; such an href is no link to follow.
            continue
        if parts.scheme not in ("http", "https"):
            continue
        path = unquote(parts.path)
        lowered = path.lower()
        if suffix is not None and not lowered.endswith(suffix):
            continue
        if contains is not None and contains not in lowered:
            continue
        file_name = clean_text(path.rsplit("/", 1)[-1].replace("_", " "))
        candidate_seen.append(seen[-1])
        problem = _origin_problem(parts)
        if problem is None:
            candidates.append((anchor_text, file_name, absolute))
        else:
            entry = f"{anchor_text} -> {describe_url(absolute)} ({problem})"
            off_origin.append((anchor_text, file_name, entry))

    match = next((url for text, _, url in candidates if needle in text.lower()), None)
    matched_by = "anchor text"
    if match is None:
        match = next((url for _, name, url in candidates if needle in name.lower()), None)
        matched_by = "file name"
    if match is None:
        expected = f"anchor containing {text_contains!r} ending {href_suffix!r}"
        if href_contains is not None:
            expected += f" with {href_contains!r} in its path"
        refused = [
            entry
            for text, name, entry in off_origin
            if needle in text.lower() or needle in name.lower()
        ]
        if refused:
            origins = ", ".join(sorted(CBK_APPROVED_ORIGINS))
            raise SourceLayoutChanged(
                expected=[f"{expected} on {origins} (HTTPS, default port, no userinfo)"],
                found=refused[:_LINK_SAMPLE_LIMIT],
                message="file link not on an approved CBK origin",
            )
        # Links of the right file type say most about what changed; else show the page's links.
        raise SourceLayoutChanged(
            expected=[expected],
            found=(candidate_seen or seen)[:_LINK_SAMPLE_LIMIT],
            message="file link not found",
        )
    url = quote(match, safe=_QUOTE_SAFE)
    logger.debug(
        "found %r link by %s on %s: %s",
        text_contains,
        matched_by,
        describe_url(base_url),
        describe_url(url),
    )
    return url


def _origin_problem(parts: SplitResult) -> str | None:
    """Why the resolved ``http(s)`` link ``parts`` is not on an approved CBK origin (see
    :data:`CBK_APPROVED_ORIGINS`), or ``None`` when it is."""
    if parts.scheme != "https":
        return "not HTTPS"
    # Any "@" in the authority, even with empty userinfo, is refused: no CBK link has one.
    if "@" in parts.netloc:
        return "userinfo"
    try:
        port = parts.port
    except ValueError:
        return "invalid port"
    if port not in (None, 443):
        return f"port {port}"
    # urlsplit lowercases the host name, as host names compare case-insensitively.
    if f"https://{parts.hostname or ''}" not in CBK_APPROVED_ORIGINS:
        return "host not approved"
    return None


def _own_rows(table: Tag) -> list[Tag]:
    """The ``<tr>`` elements of ``table`` itself, excluding those of nested tables."""
    return [
        row
        for row in table.find_all("tr")
        if isinstance(row, Tag) and row.find_parent("table") is table
    ]


def _row_cells(row: Tag) -> list[Tag]:
    return [cell for cell in row.find_all(["th", "td"], recursive=False) if isinstance(cell, Tag)]


def _split_header(table: Tag) -> tuple[list[Tag], list[Tag]]:
    """Return a table's header cells and its body rows.

    The header is the last row of the table's own ``<thead>`` when it has one with rows, else its
    first row; the body is every other row of the table.
    """
    rows = _own_rows(table)
    if not rows:
        return [], []
    head_ids = {
        id(row)
        for row in rows
        if (section := row.find_parent("thead")) is not None
        and section.find_parent("table") is table
    }
    if head_ids:
        head_rows = [row for row in rows if id(row) in head_ids]
        body = [row for row in rows if id(row) not in head_ids]
        return _row_cells(head_rows[-1]), body
    return _row_cells(rows[0]), rows[1:]


def extract_table(html: str, expected_headers: Sequence[str]) -> HtmlTable:
    """Return the first ``<table>`` whose normalized header cells equal ``expected_headers``.

    Headers are compared after ``base.normalize_header`` on both sides, exactly and in order.
    Body rows keep their 1-based position among the table's body rows; rows whose cells are all
    blank are left out. Cell text is joined with spaces and cleaned with :func:`clean_text`.

    Raises:
        SourceLayoutChanged: No table has those headers; ``found`` lists the normalized headers
            of up to five tables on the page.
    """
    expected = tuple(normalize_header(header) for header in expected_headers)
    soup = BeautifulSoup(html, "lxml")
    found: list[list[str]] = []
    for table in soup.find_all("table"):
        if not isinstance(table, Tag):
            continue
        header_cells, body = _split_header(table)
        headers = tuple(normalize_header(cell.get_text(" ")) for cell in header_cells)
        if headers != expected:
            if len(found) < _TABLE_SAMPLE_LIMIT:
                found.append(list(headers))
            continue
        rows: list[tuple[int, list[str]]] = []
        for index, row in enumerate(body, start=1):
            cells = [clean_text(cell.get_text(" ")) for cell in _row_cells(row)]
            if any(cells):
                rows.append((index, cells))
        return HtmlTable(headers=headers, rows=rows)
    raise SourceLayoutChanged(
        expected=list(expected),
        found=found or ["no <table> found"],
        message="expected table not found",
    )


def read_cbk_csv(data: bytes, *, header_row: int = 1) -> CbkCsv:
    """Read a CBK CSV download with every cell kept as text.

    ``header_row`` is the 0-based index of the header record; CBK files carry a one-line title
    record first, so it defaults to 1. Records above it are skipped before parsing, which also
    avoids pandas' tokenizer error on a title with fewer fields. pandas reads with ``dtype=str``
    and ``keep_default_na=False``, so no value ever passes through ``float``, and with
    ``index_col=False``, so a trailing comma cannot shift the columns. Blank lines are kept while
    parsing, each as a record, so each data row is numbered by its 1-based CSV record:
    ``header_row + 2`` plus its 0-based position below the header. That is its physical line
    only when no earlier quoted cell spans a line break.

    Footer rows are dropped and are not rejections: rows whose cells are all blank, and rows
    whose first non-blank cell starts with ``Source`` or ``Note`` (any case), such as "Source:
    Central Bank of Kenya".

    Raises:
        SourceLayoutChanged: The bytes are not UTF-8 or Windows-1252 text, there is no header
            record, or pandas cannot tokenize the file.
        ValueError: ``header_row`` is negative.
    """
    if header_row < 0:
        raise ValueError(f"header_row must not be negative, got {header_row}")
    import pandas as pd  # Deferred: only the CSV pipelines need pandas.

    expected = [f"CSV header on row {header_row + 1}"]
    try:
        text = decode_cbk_bytes(data)
    except UnicodeDecodeError as exc:
        raise SourceLayoutChanged(
            expected=["UTF-8 or Windows-1252 text"],
            found=[str(exc)[:200]],
            message="csv unreadable",
        ) from exc
    try:
        frame = pd.read_csv(
            io.StringIO(text),
            skiprows=header_row,
            header=0,
            dtype=str,
            keep_default_na=False,
            index_col=False,
            skip_blank_lines=False,
        )
    except (pd.errors.ParserError, pd.errors.EmptyDataError) as exc:
        raise SourceLayoutChanged(
            expected=expected, found=[str(exc)[:200]], message="csv unreadable"
        ) from exc
    # Fields missing from short rows are NaN even with keep_default_na=False.
    frame = frame.fillna("")

    headers = tuple(normalize_header(str(name)) for name in frame.columns)
    rows: list[tuple[int, dict[str, str]]] = []
    for position, values in enumerate(frame.itertuples(index=False, name=None)):
        cells = [str(value).strip() for value in values]
        first = next((cell for cell in cells if cell), None)
        if first is None or first.lower().startswith(_FOOTER_PREFIXES):
            continue
        row: dict[str, str] = {}
        for header, cell in zip(headers, cells, strict=True):
            # A header that normalizes like an earlier one never shadows it.
            row.setdefault(header, cell)
        rows.append((header_row + 2 + position, row))
    logger.debug(
        "read CBK CSV: %d columns, %d data rows of %d", len(headers), len(rows), len(frame)
    )
    return CbkCsv(headers=headers, rows=rows)


# ---------------------------------------------------------------------------------------------
# Validation and upsert
# ---------------------------------------------------------------------------------------------


def _rejected_payload(record: MacroRecord) -> Mapping[str, object]:
    """The row recorded in ``details.rejected``: the source cells, else the parsed values."""
    if record.raw:
        return dict(record.raw)
    return {"observation_date": record.observation_date, **record.values}


def validate_macro_records(
    records: Sequence[MacroRecord], bounds: Mapping[str, tuple[Decimal, Decimal]]
) -> ValidationOutcome[MacroRecord]:
    """Split parsed macro records into valid and rejected ones.

    A record is rejected when its ``observation_date`` is ``None``, it carries parse ``errors``,
    it has a value for a column without bounds (``unexpected_column:<column>``), or a value lies
    outside its inclusive ``[low, high]`` bound (``out_of_range:<column>:<value>``). Several
    reasons are joined with ``;``; ``RejectedRow.index`` is the record's 1-based source row
    (``MacroRecord.line``), or its 0-based position when that is unknown.

    Bounds are sanity limits, wide enough for the source's full published history, that catch
    parse, scale and column-shift errors rather than unusual genuine values. The shared run
    thresholds are applied by ``execute_pipeline``, not here: up to 10% of parsed rows rejected
    gives ``partial``; more than 10% rejected, or fewer parsed rows than the pipeline's
    ``min_rows``, gives ``layout_changed``.

    Raises:
        ValueError: A bound's low end exceeds its high end.
    """
    for column, (low, high) in bounds.items():
        if low > high:
            raise ValueError(f"bounds for {column!r} are inverted: {low} > {high}")
    valid: list[MacroRecord] = []
    rejected: list[RejectedRow] = []
    for position, record in enumerate(records):
        reasons = list(record.errors)
        if record.observation_date is None and not reasons:
            reasons.append("missing_observation_date")
        for column, value in record.values.items():
            limits = bounds.get(column)
            if limits is None:
                reasons.append(f"unexpected_column:{column}")
            elif not value.is_finite():
                reasons.append(f"non_finite:{column}:{value}")
            elif not limits[0] <= value <= limits[1]:
                reasons.append(f"out_of_range:{column}:{value}")
        if reasons:
            rejected.append(
                RejectedRow(
                    index=record.line if record.line is not None else position,
                    reason=";".join(reasons),
                    row=_rejected_payload(record),
                )
            )
        else:
            valid.append(record)
    return ValidationOutcome(valid=valid, rejected=rejected)


async def upsert_macro_records(
    conn: AsyncConnection, records: Sequence[MacroRecord], *, source: str
) -> int:
    """COALESCE-merge validated macro records into ``terminal_macro_indicators`` on
    ``observation_date`` and return the number of rows inserted or updated.

    Records sharing a date are collapsed with the **last** one winning, because one
    ``INSERT ... ON CONFLICT DO UPDATE`` cannot touch a row twice; ``cbk_reserves`` passes its
    bulletins oldest first, so the newest bulletin's value for a header date is kept. A record
    without values writes nothing. Each row carries only the columns the record supplied (never
    padded with nulls). The rows are sorted by ``observation_date`` and written through
    ``db.upsert.upsert_macro`` as contiguous runs of dates that share a column set, each run in
    date order in chunks of :data:`UPSERT_CHUNK_ROWS`. Every statement therefore covers dates
    above the previous one's, so concurrent pipelines, each under its own advisory lock, lock
    ``terminal_macro_indicators`` rows in one ascending date order, which rules out a deadlock
    between them on overlapping dates. ``upsert_macro`` sets
    ``col = COALESCE(EXCLUDED.col, stored)`` and records ``source`` in ``source_by_field`` for
    each written column. Nothing is committed here: the run's transaction, which holds its
    advisory lock, does that.

    Raises:
        ValueError: ``source`` is empty, or a record has no ``observation_date`` (validation
            must have rejected it).
    """
    if not source:
        raise ValueError("upsert_macro_records: source must be a non-empty string")
    undated = [record.line for record in records if record.observation_date is None]
    if undated:
        raise ValueError(
            f"upsert_macro_records: {len(undated)} records without observation_date "
            f"(lines {undated[:10]})"
        )
    unique, duplicates = dedupe_by_key(records, lambda record: record.observation_date)
    # Undated records were refused above; the ``is not None`` test only narrows the type.
    dated = sorted(
        (
            (record.observation_date, record)
            for record in unique
            if record.observation_date is not None and record.values
        ),
        key=lambda pair: pair[0],
    )
    written = 0
    # Contiguous runs in date order, never a partition by column set: statements must follow the
    # global date order, or two pipelines writing overlapping dates can lock rows in opposite
    # orders and deadlock.
    for _columns, run in groupby(dated, key=lambda pair: frozenset(pair[1].values)):
        rows: list[dict[str, object]] = [
            {"observation_date": observation_date, **record.values}
            for observation_date, record in run
        ]
        for start in range(0, len(rows), UPSERT_CHUNK_ROWS):
            written += await upsert_macro(
                conn, rows[start : start + UPSERT_CHUNK_ROWS], source=source
            )
    logger.debug(
        "upsert_macro_records (%s): %d records, %d duplicate dates dropped, %d rows written",
        source,
        len(records),
        duplicates,
        written,
    )
    return written
