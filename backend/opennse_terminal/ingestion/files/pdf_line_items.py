"""Extract key line items from corporate filing and IPO prospectus PDFs with pdfplumber.

:func:`extract_line_items` reads Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and
Shares Outstanding from the text layer of an annual report, interim report or prospectus. There
is no OCR: a PDF whose pages carry no text yields ``no_items`` with the warning ``no_text_layer``.

The PDF is untrusted input, so it is parsed in a separate Python process that the calling thread
supervises and waits for: wall time sized to the page limit (20 seconds plus 0.4 per page, 180
seconds at the default 400 pages), 1 GiB of address space and 8 MiB of decoded content per page,
with cross-reference sections that list more objects than the file holds rejected before any page
is enumerated. Exceeding the memory or content limit makes the PDF unreadable. Reaching the wall
time is reported apart, as :class:`PdfParseTimeout`, because it is the server's time budget rather
than a fault of the file. The caller can also stop a parse early by setting its ``cancel`` event,
which kills the process (:class:`ParseCancelledError`). The caller is never hung or exhausted.
``services/filings_service.py`` therefore still runs it in a worker thread for ``POST
/ingest/pdf``. It reads no settings and keeps no module-level mutable state; the bytes go to that
process through a pipe and are never written to disk.

How a value is chosen:

* Pages are classified by their title region (first 8 lines) as income statement, statement of
  financial position or statement of cash flows: the first line there that opens with a
  statement title, after qualifier words such as "Consolidated" or "Group" only, decides; a
  title mentioned mid-line ("(c) Statement of cash flows policy") does not. Each item is
  searched on its preferred statement pages, or on every page when the document has none of
  that type.
* A page's scale is the first scale marker in its header region (the lines through its first
  column-header row and a units line directly below it, else the first 8 lines). On a page that
  is not a statement only a unit caption counts: a caption line ("Amounts in KShs million") or a
  parenthesized money unit that closes its line ("Prospectus summary (USD '000)"), never prose
  such as "Revenue grew to KShs 310 billion" or "customers (in thousands)". A page without a
  marker inherits the most frequent page scale; with none anywhere the scale is x1 and the
  warning ``scale_not_detected`` is added.
* Labels are matched as anchored patterns in priority order (never substrings, so "cost of
  sales" is not "sales"); the highest-priority pattern with any match decides the item.
* The number on a matched line is selected by column evidence, never by token position: the
  nearest column-header row above it (years, "current"/"prior", Group/Company entities and a Note
  column) gives each column an x-range, and the token under the period column of
  ``period_end_date`` (else the latest year), Group before Company, is read. Note-column tokens
  are discarded. Insufficient evidence makes the item ``ambiguous``, which is never committed.
* ``(1,234)`` is -1234, a lone dash is 0, and monetary values are ``raw x page scale`` in
  ``Decimal`` ("KShs '000" is x1,000, "KShs million" x1,000,000, "billion"/"bn"
  x1,000,000,000). Share counts are never scaled by the page scale, only by a unit hint of their
  own line or column. Values are exact, never rounded: scaling moves the printed digits' decimal
  point and derived totals are summed without precision limit, whatever the active ``decimal``
  context. A zero is never negative.

Units: monetary items are whole KES after scaling, or whole USD when a statement header is in
USD (``currency="USD"``, warning ``non_kes_currency``; nothing is converted). A monetary value,
derived totals included, must fit the NUMERIC(24,2) columns of ``terminal_fundamentals``: its
magnitude, rounded half-up to 2 decimal places, below 10^22. A value beyond that has no value
(``None``) and makes its item ``ambiguous``, never committed. Shares are integral ``Decimal``
counts of at most ``MAX_SHARE_COUNT``.
"""

import contextlib
import io
import json
import logging
import math
import os
import re
import subprocess
import sys
import threading
import time
import types
import zlib
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import date
from decimal import (
    MAX_EMAX,
    MAX_PREC,
    MIN_EMIN,
    Context,
    Decimal,
    Inexact,
    InvalidOperation,
    Rounded,
)
from typing import BinaryIO, Final, Literal, Protocol, TypeGuard, TypeVar, get_args

import pdfplumber

# pdfplumber is the parser (Rule 2); these are the pdfminer.six objects underneath it, the exact
# version pdfplumber pins and re-exports. Bounding page enumeration and decoding needs pdfminer's
# page generator, cross-reference and stream objects, which pdfplumber does not expose lazily.
from pdfminer.pdfdocument import PDFDocument, PDFXRef, PDFXRefStream
from pdfminer.pdfpage import PDFPage
from pdfminer.pdftypes import LITERALS_FLATE_DECODE, PDFStream, resolve1
from pdfminer.psparser import LIT
from pdfplumber.page import Page as _PlumberPage

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.ingestion.base import parse_decimal_text

__all__ = [
    "ITEM_COLUMNS",
    "LINE_ITEMS",
    "Candidate",
    "DerivationPart",
    "ExtractedLineItem",
    "ExtractionResult",
    "FilingStatus",
    "ItemStatus",
    "ItemUnit",
    "LineItemName",
    "ParseCancelledError",
    "PdfEncrypted",
    "PdfParseTimeout",
    "PdfUnreadable",
    "TooManyPages",
    "extract_line_items",
]

logger = logging.getLogger("opennse_terminal.ingestion.files.pdf_line_items")

# ---------------------------------------------------------------------------------------------
# Public declarations
# ---------------------------------------------------------------------------------------------

LineItemName = Literal[
    "revenue",
    "ebitda",
    "net_income",
    "total_debt",
    "operating_cash_flow",
    "shares_outstanding",
]
"""The six extractable line items; the same values as ``schemas.ingest.LineItemName``."""

LINE_ITEMS: Final[tuple[LineItemName, ...]] = (
    "revenue",
    "ebitda",
    "net_income",
    "total_debt",
    "operating_cash_flow",
    "shares_outstanding",
)
"""Every line item, in declaration order: the default of :func:`extract_line_items`."""

ITEM_COLUMNS: Final[Mapping[str, str]] = types.MappingProxyType(
    {
        "revenue": "revenue_kes",
        "ebitda": "ebitda_kes",
        "net_income": "net_income_kes",
        "total_debt": "total_debt_kes",
        "operating_cash_flow": "operating_cash_flow_kes",
        "shares_outstanding": "shares_outstanding",
    }
)
"""``terminal_fundamentals`` column each committed (``found`` or ``derived``) item is written to."""

ItemStatus = Literal["found", "derived", "ambiguous", "not_found"]
ItemUnit = Literal["KES", "USD", "shares"]
FilingStatus = Literal["extracted", "partial", "no_items"]


class PdfEncrypted(ValueError):  # noqa: N818 - names the condition, as in the error code
    """The PDF is encrypted. It is rejected even when an empty user password would open it."""

    def __init__(self, message: str = "the PDF is encrypted; upload an unencrypted copy") -> None:
        super().__init__(message)


class PdfUnreadable(ValueError):  # noqa: N818 - names the condition, as in the error code
    """The bytes could not be parsed as a PDF, or a page's content could not be read."""

    def __init__(self, message: str = "the PDF could not be parsed") -> None:
        super().__init__(message)


class TooManyPages(ValueError):  # noqa: N818 - names the condition, as in the error code
    """The PDF has more pages than the configured maximum; nothing was extracted."""

    def __init__(self, page_count: int, max_pages: int) -> None:
        super().__init__(f"the PDF has {page_count} pages; at most {max_pages} can be parsed")
        self.page_count = page_count
        self.max_pages = max_pages


class PdfParseTimeout(ValueError):  # noqa: N818 - names the condition, as in the error code
    """The parse did not finish within its wall-clock limit of ``time_limit`` seconds: its parser
    process was killed and nothing was extracted. The limit is the server's time budget, so this
    is never a :class:`PdfUnreadable`."""

    def __init__(self, time_limit: float) -> None:
        super().__init__(f"the PDF could not be parsed within {time_limit:g} seconds")
        self.time_limit = time_limit


class ParseCancelledError(Exception):
    """The caller set the parse's ``cancel`` event: the parser process was killed, or never
    started, and nothing was extracted. It reports the caller's own request, never a property of
    the PDF."""

    def __init__(self, message: str = "the PDF parse was cancelled") -> None:
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class DerivationPart:
    """One statement line a ``derived`` value was summed from, value after scaling."""

    label: str
    page_number: int
    value: Decimal


@dataclass(frozen=True, slots=True)
class Candidate:
    """One conflicting match of an ``ambiguous`` item: the token as printed and its scaled value
    (``None`` when the token is not a number or not a valid value for the item: a monetary value
    beyond the NUMERIC(24,2) range, or a share count that is fractional, negative or above
    ``MAX_SHARE_COUNT``)."""

    label: str
    page_number: int
    raw_value: str
    value: Decimal | None


@dataclass(frozen=True, slots=True)
class ExtractedLineItem:
    """Extraction outcome for one requested item (field names mirror ``schemas.ingest``).

    ``value`` is scaled (whole units of ``unit``), exact and never a negative zero; a monetary
    ``value`` is below 10^22 in magnitude once rounded to 2 places, so it fits NUMERIC(24,2).
    ``raw_value`` is the selected token exactly as printed and ``scale_applied`` its multiplier.
    ``derivation`` is non-empty only for ``derived`` items and ``candidates`` (at most five) only
    for ``ambiguous`` ones. A ``not_found`` item has every optional field ``None`` and both lists
    empty.
    """

    item: str
    status: ItemStatus
    value: Decimal | None
    unit: ItemUnit
    raw_value: str | None = None
    scale_applied: int | None = None
    page_number: int | None = None
    matched_label: str | None = None
    derivation: list[DerivationPart] = field(default_factory=list)
    candidates: list[Candidate] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """Outcome for a whole PDF: one item per requested name, in the caller's order.

    ``detected_scale`` is the most frequent page scale marker, 1 when no page carries one (with
    the warning ``scale_not_detected``) and ``None`` only when the PDF has no text layer.
    ``status`` is ``extracted`` when no requested item is ``not_found``, ``no_items`` when every
    one is, and ``partial`` otherwise; ``missing_items`` lists the ``not_found`` ones.
    """

    page_count: int
    detected_scale: int | None
    currency: str
    status: FilingStatus
    items: list[ExtractedLineItem]
    missing_items: list[str]
    warnings: list[str]


# ---------------------------------------------------------------------------------------------
# Parse limits
# ---------------------------------------------------------------------------------------------

_MIB: Final = 1 << 20
_MAX_PAGE_CONTENT_BYTES: Final = 8 * _MIB
"""Largest decoded size of one page's content streams plus every Form XObject they can draw,
each stream counted once. Statement pages hold tens of KiB; a page over this is rejected before
pdfminer decodes or interprets it, which bounds decompression bombs and operator floods."""
_PARSE_TIME_BASE_SECONDS: Final = 20.0
"""Fixed part of one parse's wall-clock limit, the PDF parser process's start included; the limit
is this plus :data:`_PARSE_SECONDS_PER_PAGE` for each page the parse admits (``max_pages``)."""
_PARSE_SECONDS_PER_PAGE: Final = 0.4
"""Per-page part of the wall-clock limit, so the limit grows with the largest document admitted:
180 seconds at the default 400 pages. A dense 400-page document (about 1,190 words per page, 3.8
to 4.0 MB) parsed in 45 to 52 seconds, alone and as one of two concurrent parses; 180 seconds is
about 3.5 times that, margin for a loaded host. The parser process is killed at the limit and
:class:`PdfParseTimeout` is raised."""
_SUPERVISION_SLICE_SECONDS: Final = 0.25
"""Longest the supervising thread waits on the parser process between checks of the deadline and
of the ``cancel`` event, so a cancelled parse is stopped within this time."""
_PARSE_MEMORY_LIMIT_BYTES: Final = 1 << 30
"""Address-space limit (``RLIMIT_AS``) of the PDF parser process, about 100 MiB of it taken by
the interpreter and imports. A parse that needs more fails there, never in the caller."""
_CPU_LIMIT_MARGIN_SECONDS: Final = 5
# The parser process's own CPU limit exceeds the wall-clock limit by this margin: it only takes
# effect if the supervising process is gone and cannot kill it.
_MEMORY_RESERVE_BYTES: Final = 4 * _MIB
# Address space the parser process holds back and releases when a parse fails, so a failure at
# the memory limit can still be reported.
_MAX_REQUEST_HEADER_BYTES: Final = 4096
_MAX_ERROR_MESSAGE_CHARS: Final = 500
_LITERAL_FORM: Final = LIT("Form")


# ---------------------------------------------------------------------------------------------
# Layout constants and patterns
# ---------------------------------------------------------------------------------------------

_TITLE_REGION_LINES: Final = 8
_LINE_TOP_TOLERANCE: Final = 3.0
_COLUMN_TOLERANCE: Final = 2.0
_MAX_CANDIDATES: Final = 5
_MAX_EXCEPTION_CHAIN: Final = 16
_PDF_WORD_TOLERANCE: Final = 3
# Ranks of the non-numeric period labels, so "current" sorts above any year and "prior" below.
_CURRENT_RANK: Final = 10_000
_PRIOR_RANK: Final = -1

_StatementKind = Literal["income", "position", "cash_flows"]
_DebtComponent = Literal["current", "non_current", "short_term", "long_term", "bank_loans"]

_NORMALIZE_TABLE: Final = str.maketrans(
    {
        "\u2019": "'",
        "\u2018": "'",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u2010": "-",
        "\u2011": "-",
        "\u200b": None,
        "\ufeff": None,
    }
)
_NUMERIC_TOKEN_RE: Final = re.compile(r"\(?-?[0-9][0-9,]*(?:\.[0-9]+)?\)?")
_YEAR_RE: Final = re.compile(r"(?:19|20)\d{2}")
_PERIOD_LABELS: Final = frozenset({"current", "prior"})
_ENTITY_WORDS: Final[Mapping[str, str]] = types.MappingProxyType(
    {"group": "group", "consolidated": "group", "company": "company"}
)
_NOTE_WORDS: Final = frozenset({"note", "notes"})
# Words that may stand beside the period tokens of a column-header row: unit and currency
# markers, restatement and audit labels, and the words of "In millions of Kenya shillings".
_HEADER_FILLER_RE: Final = re.compile(
    r"\(?(?:kshs|kes|shs|ksh|usd|us\$)?\.?'?(?:000|m|mn|millions?|thousands?|bn|billions?)?\)?"
    r"|\(?(?:restated|audited|unaudited)\)?"
    r"|years?|period|months|ended|the|and|in|of|kenya|kenyan|shillings|us|dollars"
)
# Words that may precede a statement title on its title line, as in "Consolidated and separate
# statement of financial position" or "Group statement of cash flows". A title after any other
# word, a list marker such as "(c)" or "3." included, is a mention, not the page's title.
_TITLE_QUALIFIERS: Final = (
    "consolidated",
    "company",
    "group",
    "and",
    "separate",
    "standalone",
    "bank",
    "interim",
    "condensed",
    "abridged",
    "summary",
    "summarised",
    "summarized",
    "audited",
    "unaudited",
)
# Used with ``re.match`` on a normalized line: the line must start with the title, optionally
# after qualifier words; any text may follow it.
_TITLE_RE: Final = re.compile(
    rf"(?:(?:{'|'.join(_TITLE_QUALIFIERS)}) )*"
    r"(?:"
    r"(?P<income>statements? of profit or loss|income statements?"
    r"|statements? of comprehensive income)"
    r"|(?P<position>statements? of financial position|balance sheets?)"
    r"|(?P<cash_flows>statements? of cash flows?|cash flow statements?)"
    r")\b"
)
_STATEMENT_KINDS: Final[Mapping[str, _StatementKind]] = types.MappingProxyType(
    {"income": "income", "position": "position", "cash_flows": "cash_flows"}
)
# The usd/us$ prefixes extend the KShs markers to "USD '000" and "USD million" statements, whose
# figures are then returned in USD at the right scale.
_SCALE_MARKER_RE: Final = re.compile(
    r"(?P<thousand>\b(?:kshs|kes|shs|ksh|usd|us\$)\s*'?\s*000|\bin thousands\b)"
    r"|(?P<million>\b(?:kshs|kes|shs|ksh|usd|us\$)\s*'?\s*(?:m|mn|millions?)\b|\bin millions\b)"
    r"|(?P<billion>\bbillions?\b|\bbn\b)"
)
_SCALE_VALUES: Final[Mapping[str, int]] = types.MappingProxyType(
    {"thousand": 1_000, "million": 1_000_000, "billion": 1_000_000_000}
)
# Lead-in words of a unit caption on a page that is not a statement, as in "Amounts in KShs
# million" or "All figures are in KShs '000 unless otherwise stated". They are kept apart from
# _HEADER_FILLER_RE, which also decides column-header rows, so they never make a header row.
_CAPTION_LEAD_INS: Final = frozenset(
    {
        "all",
        "amount",
        "amounts",
        "figures",
        "values",
        "currency",
        "units",
        "are",
        "is",
        "expressed",
        "presented",
        "reported",
        "stated",
        "shown",
        "unless",
        "otherwise",
        "indicated",
        "noted",
    }
)
# Punctuation that may wrap a caption word: "(All amounts in KShs '000, unless otherwise stated)".
_CAPTION_PUNCTUATION: Final = "(),;:."
# A parenthesized group of a normalized line: "(usd '000)" in "Prospectus summary (USD '000)".
_PARENTHESIZED_RE: Final = re.compile(r"\(([^()]*)\)")
_USD_RE: Final = re.compile(r"\busd\b|\bus\$")
_CURRENCY_RE: Final = re.compile(r"\b(?:kshs|kes|shs|ksh|usd|us\$)")
# Currency names that, with _CURRENCY_RE, make a parenthesized unit group a money caption, as in
# "Five-year review (in millions of Kenya shillings)".
_CURRENCY_NAME_RE: Final = re.compile(r"\b(?:shillings|dollars)\b")
_SHARE_UNIT_RE: Final = re.compile(
    r"(?P<thousand>'000|\bthousands?\b)"
    r"|(?P<million>\bmillions?\b|\bmn\b)"
    r"|(?P<billion>\bbillions?\b|\bbn\b)"
)
# Labels may end in parentheticals such as "(restated)" or a unit, and in a note reference word.
_LABEL_SUFFIX: Final = r"(?: \([^()]*\))*(?: notes?)?(?: \d{1,3}(?:\([a-z]\)|[a-z]))?"


def _labels(*bodies: str) -> tuple[re.Pattern[str], ...]:
    """Compile label bodies, in priority order, into patterns used with ``re.fullmatch`` on a
    normalized label."""
    return tuple(re.compile(f"(?:{body}){_LABEL_SUFFIX}") for body in bodies)


@dataclass(frozen=True, slots=True)
class _ItemSpec:
    """Preferred statement kinds (empty: every page) and label patterns in priority order."""

    kinds: tuple[_StatementKind, ...]
    patterns: tuple[re.Pattern[str], ...]


_REVENUE: Final = _ItemSpec(
    ("income",),
    _labels(
        r"revenue(?: from contracts with customers)?",
        r"total revenue",
        r"turnover",
        r"sales",
        r"total operating income",
        r"total income",
    ),
)
_EBITDA: Final = _ItemSpec(
    ("income", "cash_flows"),
    _labels(
        r"ebitda",
        r"earnings before interest,? tax(?:es|ation)?,? depreciation and amorti[sz]ation",
    ),
)
_OPERATING_PROFIT: Final = _ItemSpec(
    ("income", "cash_flows"),
    _labels(
        r"operating profit(?:/\(loss\))?",
        r"profit from operations",
        r"earnings before interest and tax(?:es|ation)?",
    ),
)
_DA_KINDS: Final[tuple[_StatementKind, ...]] = ("cash_flows", "income")
_DA_COMBINED: Final = _ItemSpec(
    _DA_KINDS, _labels(r"depreciation and amorti[sz]ation(?: (?:charge|expenses?))?")
)
_DEPRECIATION: Final = _ItemSpec(_DA_KINDS, _labels(r"depreciation(?: .*)?"))
_AMORTISATION: Final = _ItemSpec(_DA_KINDS, _labels(r"amorti[sz]ation(?: .*)?"))
_NET_INCOME: Final = _ItemSpec(
    ("income",),
    _labels(
        r"(?:profit|profit/\(loss\)|\(loss\)/profit) for the (?:year|period)",
        r"profit after tax(?:ation)?",
        r"net profit",
        r"profit attributable to (?:the )?(?:equity holders|owners|shareholders)(?: of .*)?",
        r"net income",
    ),
)
_OPERATING_CASH_FLOW: Final = _ItemSpec(
    ("cash_flows",),
    _labels(
        r"net cash (?:flows? )?(?:generated from|generated by|from|used in"
        r"|\(used in\)/generated from|generated from/\(used in\)|\(used in\)/from"
        r"|from/\(used in\)) operating activities",
        r"cash generated from operations",
    ),
)
_SHARES_OUTSTANDING: Final = _ItemSpec(
    (),
    _labels(
        r"number of ordinary shares in issue",
        r"ordinary shares in issue",
        r"number of shares(?: in issue)?",
        r"weighted average number of ordinary shares(?: in issue)?",
    ),
)
_DEBT_KINDS: Final[tuple[_StatementKind, ...]] = ("position",)
_DEBT_TOTALS: Final = _labels(r"total borrowings", r"loans and borrowings", r"total debt")
_DEBT_GENERIC: Final = _labels(r"borrowings")[0]
_DEBT_COMPONENTS: Final[tuple[tuple[re.Pattern[str], _DebtComponent], ...]] = (
    (_labels(r"borrowings ?- ?current|current borrowings")[0], "current"),
    (_labels(r"borrowings ?- ?non[- ]current|non[- ]current borrowings")[0], "non_current"),
    (_labels(r"short[- ]term borrowings")[0], "short_term"),
    (_labels(r"long[- ]term borrowings|long[- ]term debt")[0], "long_term"),
    (_labels(r"bank loans")[0], "bank_loans"),
)
_DEBT_PAIRS: Final = (
    frozenset({"current", "non_current"}),
    frozenset({"short_term", "long_term"}),
)
_SIMPLE_ITEMS: Final[Mapping[str, _ItemSpec]] = types.MappingProxyType(
    {
        "revenue": _REVENUE,
        "net_income": _NET_INCOME,
        "operating_cash_flow": _OPERATING_CASH_FLOW,
    }
)


# ---------------------------------------------------------------------------------------------
# Internal structures
# ---------------------------------------------------------------------------------------------


class _PdfPage(Protocol):
    """The part of ``pdfplumber.page.Page`` this module uses."""

    def extract_words(
        self, *, x_tolerance: float, y_tolerance: float, keep_blank_chars: bool
    ) -> object: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _Word:
    """One word of a page's text layer.

    Geometry is pdfplumber's float layout data in points (``top`` measured from the page top).
    It only places words in lines and columns; no amount is ever computed from it.
    """

    text: str
    norm: str
    x0: float
    x1: float
    top: float


@dataclass(frozen=True, slots=True)
class _Line:
    """Words sharing a baseline, left to right, with the label/value split and header roles.

    The label runs up to and including the last word that is neither a numeric token nor a lone
    dash, so "Borrowings – current" stays whole; the words after it are the value tokens.
    """

    page_number: int
    index: int
    words: tuple[_Word, ...]
    text: str
    label: str
    norm_label: str
    value_words: tuple[_Word, ...]
    is_header: bool
    is_context: bool
    is_entity_conflict: bool


@dataclass(frozen=True, slots=True)
class _Column:
    """One column of a header row: a period column (year, "current" or "prior") or a Note
    column, with its x-range, its entity and the unit text printed over it."""

    x0: float
    x1: float
    is_note: bool
    is_year: bool
    rank: int
    entity: str | None
    hints: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Header:
    """Column evidence for a matched line. ``entity_only`` marks Group and Company headers that
    have no period sub-headers, which makes every value under them ambiguous."""

    columns: tuple[_Column, ...]
    entity_only: bool = False


_ENTITY_ONLY_HEADER: Final = _Header(columns=(), entity_only=True)


@dataclass(frozen=True, slots=True)
class _Page:
    """A page's lines and its statement kind, own scale marker and USD marker; ``scale`` is the
    marker, or the document's inherited scale. ``headers[i]`` is the column evidence for line
    ``i`` (see :func:`_page_headers`), computed once per page so a match looks it up in O(1)."""

    number: int
    lines: tuple[_Line, ...]
    kind: _StatementKind | None
    marker_scale: int | None
    has_usd: bool
    headers: tuple[_Header | None, ...]
    scale: int = 1


@dataclass(frozen=True, slots=True)
class _Document:
    """The analysed pages plus the period year that selects columns."""

    pages: tuple[_Page, ...]
    period_year: int | None


@dataclass(frozen=True, slots=True)
class _Reading:
    """The number read from one matched line.

    ``token`` and ``amount`` (sign applied, unscaled) are set when column evidence selected a
    single readable token; otherwise the line is ambiguous and ``tokens`` holds the value tokens
    offered as candidates. ``hints`` is the unit text printed over the selected column.
    """

    page_number: int
    scale: int
    label: str
    norm_label: str
    token: _Word | None
    amount: Decimal | None
    hints: tuple[str, ...]
    tokens: tuple[_Word, ...]


@dataclass(frozen=True, slots=True)
class _Valuation:
    """How an item turns a token amount into its value: ``scale_of`` gives the multiplier and
    ``value_of`` the value, ``None`` when the amount is not a valid value for the item."""

    scale_of: Callable[[_Reading], int]
    value_of: Callable[[_Reading, Decimal], Decimal | None]


@dataclass(frozen=True, slots=True)
class _Outcome:
    """A resolved item before conversion to :class:`ExtractedLineItem`. ``evidence`` holds the
    candidates reported when the outcome is, or a combination of outcomes becomes, ambiguous."""

    status: Literal["found", "derived", "ambiguous"]
    value: Decimal | None
    raw_value: str | None
    scale: int | None
    page_number: int | None
    label: str | None
    parts: tuple[DerivationPart, ...] = ()
    evidence: tuple[Candidate, ...] = ()


# ---------------------------------------------------------------------------------------------
# Opening the PDF and reading words
# ---------------------------------------------------------------------------------------------


def _normalize(text: str) -> str:
    """Normalize text for matching: lowercase, typographic quotes to ``'``, en/em dashes and the
    minus sign to ``-``, whitespace collapsed, trailing ``:``, ``.`` and ``*`` stripped."""
    return " ".join(text.translate(_NORMALIZE_TABLE).lower().split()).rstrip(":.* ")


def _is_value_token(norm: str) -> bool:
    """A numeric token such as ``1,234``, ``(1,234)`` or ``1,234.5``, or a lone dash."""
    return norm == "-" or _NUMERIC_TOKEN_RE.fullmatch(norm) is not None


def _token_amount(norm: str) -> Decimal | None:
    """Exact value of a value token: parentheses negate and a lone dash is 0; ``None`` for a
    token that is not a well-formed number (for example an unbalanced parenthesis)."""
    if norm == "-":
        return Decimal(0)
    return parse_decimal_text(norm)


def _exception_chain(exc: BaseException) -> list[BaseException]:
    """``exc`` and the exceptions it wraps (cause, context and exception arguments), breadth
    first, at most ``_MAX_EXCEPTION_CHAIN`` of them."""
    chain: list[BaseException] = []
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending and len(chain) < _MAX_EXCEPTION_CHAIN:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        chain.append(current)
        for linked in (current.__cause__, current.__context__, *current.args):
            if isinstance(linked, BaseException):
                pending.append(linked)
    return chain


def _is_encryption_error(exc: BaseException) -> bool:
    """Whether ``exc`` or an exception it wraps is a pdfminer password or encryption error.

    pdfplumber wraps pdfminer errors in ``PdfminerException`` (the original is its argument and
    context), so the whole chain is checked, by pdfminer class name and by message.
    """
    for current in _exception_chain(exc):
        if type(current).__name__ in {"PDFPasswordIncorrect", "PDFEncryptionError"}:
            return True
        message = str(current).lower()
        if "password" in message or "encrypt" in message:
            return True
    return False


def _is_memory_exhaustion(exc: BaseException) -> bool:
    """Whether ``exc`` or an exception it wraps is a :class:`MemoryError`."""
    return any(isinstance(current, MemoryError) for current in _exception_chain(exc))


def _open_failure(exc: Exception) -> ValueError:
    """The error to raise for a failure while opening the PDF or listing its pages."""
    if _is_encryption_error(exc):
        return PdfEncrypted()
    # Name the pdfminer error pdfplumber wrapped, which says more than the wrapper's class.
    wrapped = exc.args[0] if exc.args and isinstance(exc.args[0], BaseException) else exc
    return PdfUnreadable(f"the PDF could not be parsed ({type(wrapped).__name__})")


def _coordinate(entry: Mapping[object, object], key: str, page_number: int) -> float:
    """A numeric geometry field of a pdfplumber word dictionary."""
    value = entry.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise PdfUnreadable(f"page {page_number} returned a word without a numeric {key}")
    return value


def _page_words(page: _PdfPage, page_number: int) -> list[_Word]:
    """Every word of one page's text layer; a parse failure raises :class:`PdfUnreadable`."""
    try:
        raw: object = page.extract_words(
            x_tolerance=_PDF_WORD_TOLERANCE,
            y_tolerance=_PDF_WORD_TOLERANCE,
            keep_blank_chars=False,
        )
    except Exception as exc:
        raise PdfUnreadable(f"page {page_number} of the PDF could not be parsed") from exc
    finally:
        # Drops the page's cached layout objects, so memory stays bounded by one page rather
        # than growing with every page of a large filing.
        page.close()
    if not isinstance(raw, list):
        raise PdfUnreadable(f"page {page_number} returned no word list")
    words: list[_Word] = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise PdfUnreadable(f"page {page_number} returned a malformed word")
        text = entry.get("text")
        if not isinstance(text, str):
            raise PdfUnreadable(f"page {page_number} returned a word without text")
        if not text.strip():
            continue
        words.append(
            _Word(
                text=text,
                norm=_normalize(text),
                x0=_coordinate(entry, "x0", page_number),
                x1=_coordinate(entry, "x1", page_number),
                top=_coordinate(entry, "top", page_number),
            )
        )
    return words


def _is_count(value: object) -> TypeGuard[int]:
    """A non-negative integer (a PDF boolean is not one)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_cross_references(document: PDFDocument, file_size: int) -> None:
    """Reject cross-reference sections that list more objects than the file can hold.

    pdfminer enumerates every listed object id when the page tree cannot be found, about 20
    microseconds each, so an 800-byte cross-reference stream claiming 10^9 objects would take
    hours. Each stream's ranges must be non-negative and backed by an entry in its data, and the
    objects of all sections together may not outnumber the bytes of the file.
    """
    claimed = 0
    for xref in document.xrefs:
        if isinstance(xref, PDFXRefStream):
            ranges_valid = all(
                _is_count(start) and _is_count(count) for start, count in xref.ranges
            )
            listed = sum(count for _, count in xref.ranges) if ranges_valid else 0
            entry_length = xref.entlen
            if not ranges_valid or (
                listed > 0
                and (
                    not _is_count(entry_length)
                    or entry_length == 0
                    or listed * entry_length > len(xref.data or b"")
                )
            ):
                raise PdfUnreadable(
                    "the PDF's cross-reference stream lists more objects than it holds entries for"
                )
            claimed += listed
        elif isinstance(xref, PDFXRef):
            claimed += len(xref.offsets)
    if claimed > file_size:
        raise PdfUnreadable("the PDF's cross-reference sections list more objects than it holds")


def _declared_page_count(document: PDFDocument, counted: int) -> int:
    """The page count reported by :class:`TooManyPages` once ``counted`` pages were enumerated:
    the root page tree's ``/Count`` when it is an integer no smaller, else ``counted``."""
    try:
        tree = resolve1(document.catalog.get("Pages"))
        declared = resolve1(tree.get("Count")) if isinstance(tree, dict) else None
    except Exception:
        return counted
    if _is_count(declared) and declared >= counted:
        return declared
    return counted


def _flate_size(data: bytes, stages: int, limit: int) -> int | None:
    """The size of ``data`` after ``stages`` zlib decodings, or ``limit + 1`` as soon as a stage
    exceeds ``limit``: no stage ever decodes more than ``limit + 1`` bytes. ``None`` when the
    data is not valid zlib, which pdfminer recovers from its own way."""
    try:
        for _ in range(stages):
            data = zlib.decompressobj().decompress(data, limit + 1)
            if len(data) > limit:
                return limit + 1
    except zlib.error:
        return None
    return len(data)


def _decoded_size(stream: PDFStream, limit: int) -> int:
    """The decoded size of ``stream``, or more than ``limit`` once it is known to exceed it.

    A FlateDecode-only stream is measured without decoding past ``limit``, so a decompression
    bomb is never expanded. Any other filter chain is decoded as pdfminer would decode it for the
    page anyway; the parser process's memory limit bounds that.
    """
    if stream.data is not None:
        return len(stream.data)
    filters = stream.get_filters()
    if (
        stream.rawdata is not None
        and stream.decipher is None
        and filters
        and all(name in LITERALS_FLATE_DECODE for name, _ in filters)
    ):
        size = _flate_size(stream.rawdata, len(filters), limit)
        if size is not None:
            return size
    return len(stream.get_data())


def _page_content_size(page: PDFPage, limit: int) -> int:
    """Decoded bytes of a page's content streams and of every Form XObject reachable from its
    resources, through each form's own resources, each stream counted once; more than ``limit``
    as soon as the total exceeds it. Images are not counted: text extraction never decodes them.
    """
    total = 0
    streams: list[object] = list(page.contents)
    resources: list[object] = [page.resources]
    seen_streams: dict[int, PDFStream] = {}
    seen_resources: dict[int, dict[object, object]] = {}
    while streams or resources:
        if resources:
            entry = resolve1(resources.pop())
            if not isinstance(entry, dict) or id(entry) in seen_resources:
                continue
            seen_resources[id(entry)] = entry
            xobjects = resolve1(entry.get("XObject"))
            if not isinstance(xobjects, dict):
                continue
            for value in xobjects.values():
                xobject = resolve1(value)
                if (
                    isinstance(xobject, PDFStream)
                    and id(xobject) not in seen_streams
                    and resolve1(xobject.get("Subtype")) is _LITERAL_FORM
                ):
                    streams.append(xobject)
                    resources.append(xobject.get("Resources"))
            continue
        stream = resolve1(streams.pop())
        if not isinstance(stream, PDFStream) or id(stream) in seen_streams:
            continue
        seen_streams[id(stream)] = stream
        total += _decoded_size(stream, limit - total)
        if total > limit:
            return total
    return total


def _check_page_content(page: PDFPage, number: int) -> None:
    """Reject page ``number`` when its decoded content exceeds ``_MAX_PAGE_CONTENT_BYTES``,
    before pdfminer decodes and interprets it; a stream that cannot be decoded makes the page
    unreadable."""
    try:
        size = _page_content_size(page, _MAX_PAGE_CONTENT_BYTES)
    except Exception as exc:
        raise PdfUnreadable(f"page {number} of the PDF could not be parsed") from exc
    if size > _MAX_PAGE_CONTENT_BYTES:
        raise PdfUnreadable(
            f"page {number} has more than {_MAX_PAGE_CONTENT_BYTES // _MIB} MiB of decoded content"
        )


def _read_pages(pdf_bytes: bytes, max_pages: int) -> list[list[_Word]]:
    """Open the PDF, apply the encryption, cross-reference, page-count and page-content guards,
    and read each page's words.

    Encrypted files are rejected even when they open with an empty password. Pages are
    enumerated lazily, so :class:`TooManyPages` is raised once ``max_pages + 1`` pages have been
    found and before any text is extracted; a document without pages is unreadable. Each page's
    decoded content is bounded before its words are read.

    Neither ``pdf.pages`` nor ``pdf.close()`` is used: pdfplumber builds the page list for the
    whole document, and ``close()`` flushes its caches and lists every page again through
    pdfminer, the unbounded work these guards exist to avoid. Each page is closed after its words
    are read, and the in-memory stream is owned here, so nothing is left open.
    """
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    except Exception as exc:
        raise _open_failure(exc) from exc
    document: object = getattr(pdf, "doc", None)
    if not isinstance(document, PDFDocument):
        raise PdfUnreadable()
    if document.encryption is not None:
        raise PdfEncrypted()
    _check_cross_references(document, len(pdf_bytes))
    pages: list[tuple[PDFPage, _PdfPage]] = []
    doctop: float = 0.0
    try:
        for number, page_obj in enumerate(PDFPage.create_pages(document), start=1):
            if number > max_pages:
                raise TooManyPages(_declared_page_count(document, number), max_pages)
            page = _PlumberPage(pdf, page_obj, page_number=number, initial_doctop=doctop)
            doctop += page.height
            pages.append((page_obj, page))
    except TooManyPages:
        raise
    except Exception as exc:
        raise _open_failure(exc) from exc
    if not pages:
        raise PdfUnreadable("the PDF has no pages")
    page_words: list[list[_Word]] = []
    for number, (page_obj, text_page) in enumerate(pages, start=1):
        _check_page_content(page_obj, number)
        page_words.append(_page_words(text_page, number))
    return page_words


# ---------------------------------------------------------------------------------------------
# Lines and page analysis
# ---------------------------------------------------------------------------------------------


def _is_period_word(norm: str) -> bool:
    """A period header token: a four-digit year, or a "current"/"prior" period label."""
    return norm in _PERIOD_LABELS or _YEAR_RE.fullmatch(norm) is not None


def _is_header_word(norm: str) -> bool:
    """An entity, note, unit or filler word that may accompany the period tokens of a header."""
    return (
        norm in _ENTITY_WORDS
        or norm in _NOTE_WORDS
        or not norm
        or _HEADER_FILLER_RE.fullmatch(norm) is not None
    )


def _make_line(page_number: int, index: int, words: Sequence[_Word]) -> _Line:
    """Build a line from its words (already in x order), deriving label, values and roles.

    A column-header row holds at least one period token and otherwise only header words, so a
    title such as "... for the year ended 31 December 2025" never becomes a header. A context
    line (entities, notes and units only) may sit directly above or below a header row.
    """
    last_label = -1
    for position, word in enumerate(words):
        if not _is_value_token(word.norm):
            last_label = position
    label_words = words[: last_label + 1]
    label = " ".join(word.text for word in label_words)
    has_period = any(_is_period_word(word.norm) for word in words)
    all_header_words = all(
        _is_period_word(word.norm) or _is_header_word(word.norm) for word in words
    )
    entities = {_ENTITY_WORDS[word.norm] for word in words if word.norm in _ENTITY_WORDS}
    is_context = all_header_words and not has_period
    return _Line(
        page_number=page_number,
        index=index,
        words=tuple(words),
        text=_normalize(" ".join(word.text for word in words)),
        label=label,
        norm_label=_normalize(label),
        value_words=tuple(words[last_label + 1 :]),
        is_header=all_header_words and has_period,
        is_context=is_context,
        is_entity_conflict=is_context and {"group", "company"} <= entities,
    )


def _lines(page_number: int, words: Sequence[_Word]) -> tuple[_Line, ...]:
    """Group words into lines: sorted by (top, x0), a word starts a new line when its top is
    more than 3 points from the current line's top; each line is then sorted by x0."""
    groups: list[list[_Word]] = []
    line_top = 0.0
    for word in sorted(words, key=lambda item: (item.top, item.x0)):
        if groups and abs(word.top - line_top) <= _LINE_TOP_TOLERANCE:
            groups[-1].append(word)
        else:
            groups.append([word])
            line_top = word.top
    return tuple(
        _make_line(page_number, index, sorted(group, key=lambda item: item.x0))
        for index, group in enumerate(groups)
    )


def _classify_page(lines: Sequence[_Line]) -> _StatementKind | None:
    """Statement kind from the first title line, in reading order, of the title region (first 8
    lines); ``None`` when no line there is a title line.

    A title line starts with a statement title, optionally after qualifier words only
    (:data:`_TITLE_QUALIFIERS`: "Consolidated and separate statement of financial position",
    "Group statement of cash flows"); any text may follow the title ("... for the year ended
    31 December 2025", "... and other comprehensive income", "(continued)"). A title after any
    other word is a mention, not a title: a list marker ("(c) Statement of cash flows policy",
    "3. Balance sheet") or a sentence ("... as shown in the statement of financial position").
    """
    for line in lines[:_TITLE_REGION_LINES]:
        match = _TITLE_RE.match(line.text)
        if match is not None and match.lastgroup is not None:
            return _STATEMENT_KINDS[match.lastgroup]
    return None


def _header_region(lines: Sequence[_Line]) -> Sequence[_Line]:
    """Lines from the top through the first column-header row (plus a units/notes context line
    directly below it), or the first 8 lines when the page has no header row."""
    first = next((line.index for line in lines if line.is_header), None)
    if first is None:
        return lines[:_TITLE_REGION_LINES]
    end = first + 1
    if end < len(lines) and lines[end].is_context:
        end += 1
    return lines[:end]


def _caption_word(text: str) -> str:
    """A normalized word without the parentheses, commas, semicolons, colons and full stops
    that may wrap it in a caption ("(kshs" and "'000," become "kshs" and "'000")."""
    return text.strip(_CAPTION_PUNCTUATION)


def _is_caption_line(line: _Line) -> bool:
    """A unit caption line: every word, bar its wrapping punctuation, is a period word, a header
    word (entity, note, unit or filler) or a caption lead-in word (:data:`_CAPTION_LEAD_INS`),
    as in "Amounts in KShs million", "In thousands of Kenya shillings" or "KShs '000". A line
    holding any other word, a sentence or an amount such as "310", is not a caption."""
    for word in line.words:
        core = _caption_word(word.norm)
        if not (_is_period_word(core) or _is_header_word(core) or core in _CAPTION_LEAD_INS):
            return False
    return True


def _unit_parenthetical_marker(line: _Line) -> re.Match[str] | None:
    """The first scale marker of ``line.text`` inside a parenthesized money-unit caption that
    closes the line ("Prospectus summary (USD '000)", "Summary financial information (KShs
    million)", "Five-year review (in millions of Kenya shillings)"); ``None`` when there is none.

    A group qualifies only when it closes the line, nothing but caption punctuation following
    its ")": a heading followed by its unit caption qualifies, a parenthetical inside a sentence
    ("We measure customers (in thousands), not revenue") does not. The group must also be made
    only of header words and name a currency (KShs, KES, Shs, Ksh, USD, US$, shillings or
    dollars), so a count of customers or shares ("Financial highlights (in millions)", "(bn)")
    is not a money unit. The line must hold no value token other than period and unit words, so
    "Number of shares (bn) 40" is a data line, not a caption. The line's end is found once and
    each group is searched once, so the cost is linear in the line's length."""
    for word in line.words:
        if _is_value_token(word.norm) and not (
            _is_period_word(word.norm) or _is_header_word(word.norm)
        ):
            return None
    # Where the line's trailing run of caption punctuation and spaces begins: a group whose ")"
    # reaches it closes the line.
    line_end = len(line.text.rstrip(_CAPTION_PUNCTUATION + " "))
    for group in _PARENTHESIZED_RE.finditer(line.text):
        if group.end() < line_end:
            continue
        start, end = group.span(1)
        if not all(_is_header_word(_caption_word(text)) for text in group.group(1).split()):
            continue
        if (
            _CURRENCY_RE.search(line.text, start, end) is None
            and _CURRENCY_NAME_RE.search(line.text, start, end) is None
        ):
            continue
        marker = _SCALE_MARKER_RE.search(line.text, start, end)
        if marker is not None:
            return marker
    return None


def _scale_marker(region: Sequence[_Line], *, captions_only: bool) -> int | None:
    """The scale of the first qualifying marker, in reading order, within the header region;
    ``None`` when no marker there qualifies.

    On a statement page every marker in the header region qualifies. With ``captions_only``, for
    a page that is not a statement, only a marker on a unit caption does: a caption line
    (:func:`_is_caption_line`, "Amounts in KShs million") or a parenthesized money-unit group
    that names a currency and closes a line without amounts (:func:`_unit_parenthetical_marker`,
    "Prospectus summary (USD '000)"). A marker in prose ("Revenue grew to KShs 310 billion",
    "customers in thousands of villages", "We measure customers (in thousands), not revenue"),
    in a parenthesized group naming no currency ("Financial highlights (in millions)") or on a
    data line ("Number of shares (bn) 40") never sets a page scale, so such a page inherits the
    document's scale and takes no part in choosing it.
    """
    for line in region:
        if not captions_only or _is_caption_line(line):
            marker = _SCALE_MARKER_RE.search(line.text)
        else:
            marker = _unit_parenthetical_marker(line)
        if marker is not None and marker.lastgroup is not None:
            return _SCALE_VALUES[marker.lastgroup]
    return None


def _analyse_page(page_number: int, words: Sequence[_Word]) -> _Page:
    """Lines, statement kind, scale marker, USD marker and per-line column evidence of one
    page; the column evidence takes one forward pass over the lines. The scale marker is read
    from the header region: any marker there on a statement page, only a unit caption's on any
    other page (see :func:`_scale_marker`)."""
    lines = _lines(page_number, words)
    region = _header_region(lines)
    kind = _classify_page(lines)
    return _Page(
        number=page_number,
        lines=lines,
        kind=kind,
        marker_scale=_scale_marker(region, captions_only=kind is None),
        has_usd=any(_USD_RE.search(line.text) is not None for line in region),
        headers=_page_headers(lines),
    )


# ---------------------------------------------------------------------------------------------
# Column evidence and reading a matched line
# ---------------------------------------------------------------------------------------------


def _overlaps(a_x0: float, a_x1: float, b_x0: float, b_x1: float) -> bool:
    """Whether two x-ranges overlap, allowing the 2-point column tolerance."""
    return a_x0 <= b_x1 + _COLUMN_TOLERANCE and a_x1 >= b_x0 - _COLUMN_TOLERANCE


def _centre(word: _Word) -> float:
    """The x-centre of ``word`` in points: layout geometry only, never part of an amount."""
    return (word.x0 + word.x1) / 2


def _context_line(lines: Sequence[_Line], index: int) -> _Line | None:
    """The page line at ``index`` when it exists and holds only entity, note and unit words;
    ``None`` otherwise, including an ``index`` outside the page. Constant time."""
    if 0 <= index < len(lines) and lines[index].is_context:
        return lines[index]
    return None


def _column_entity(column: _Word, same_line: Sequence[_Word], above: Sequence[_Word]) -> str | None:
    """Entity of a period column: the overlapping entity token (closest centre first), else the
    nearest entity token to its left on the header row, else the entity token on the line above
    with the closest x-centre, else none."""
    overlapping = [
        word for word in (*same_line, *above) if _overlaps(word.x0, word.x1, column.x0, column.x1)
    ]
    if overlapping:
        nearest = min(overlapping, key=lambda word: abs(_centre(word) - _centre(column)))
        return _ENTITY_WORDS[nearest.norm]
    left = [word for word in same_line if word.x0 < column.x0]
    if left:
        return _ENTITY_WORDS[max(left, key=lambda word: word.x0).norm]
    if above:
        nearest = min(above, key=lambda word: abs(_centre(word) - _centre(column)))
        return _ENTITY_WORDS[nearest.norm]
    return None


def _column_hints(column: _Word, lines: Sequence[_Line]) -> tuple[str, ...]:
    """Unit text printed over a period column: per header line, the non-period words whose
    x-range overlaps the column, joined."""
    hints: list[str] = []
    for line in lines:
        text = " ".join(
            word.norm
            for word in line.words
            if not _is_period_word(word.norm) and _overlaps(word.x0, word.x1, column.x0, column.x1)
        )
        if text:
            hints.append(text)
    return tuple(hints)


def _build_header(lines: Sequence[_Line], index: int) -> _Header:
    """Columns of the page's header row ``lines[index]``: one per period token, plus a Note
    column per note token on the row or its context lines. Entity tokens come from the row
    itself and the context line directly above it. Only the row and its two neighbouring lines
    are read, so the cost does not grow with the page's line count."""
    line = lines[index]
    above = _context_line(lines, index - 1)
    below = _context_line(lines, index + 1)
    context = tuple(item for item in (above, line, below) if item is not None)
    same_entities = [word for word in line.words if word.norm in _ENTITY_WORDS]
    above_entities = (
        [word for word in above.words if word.norm in _ENTITY_WORDS] if above is not None else []
    )
    columns: list[_Column] = []
    for word in line.words:
        if not _is_period_word(word.norm):
            continue
        is_year = word.norm not in _PERIOD_LABELS
        if is_year:
            rank = int(word.norm)
        else:
            rank = _CURRENT_RANK if word.norm == "current" else _PRIOR_RANK
        columns.append(
            _Column(
                x0=word.x0,
                x1=word.x1,
                is_note=False,
                is_year=is_year,
                rank=rank,
                entity=_column_entity(word, same_entities, above_entities),
                hints=_column_hints(word, context),
            )
        )
    for context_line in context:
        for word in context_line.words:
            if word.norm in _NOTE_WORDS:
                columns.append(
                    _Column(
                        x0=word.x0,
                        x1=word.x1,
                        is_note=True,
                        is_year=False,
                        rank=0,
                        entity=None,
                        hints=(),
                    )
                )
    return _Header(columns=tuple(columns))


def _page_headers(lines: Sequence[_Line]) -> tuple[_Header | None, ...]:
    """Column evidence for every line of a page, in one forward pass.

    Entry ``i`` comes from the nearest line above line ``i`` that is a column-header row or a
    Group-and-Company line without periods: the header built from that row, or the entity-only
    header for such a line; ``None`` when neither lies above. Each header row is built once and
    the same :class:`_Header` is shared by every line up to the next such line, so the work is
    linear in the page's lines however many of them match an item.
    """
    headers: list[_Header | None] = []
    current: _Header | None = None
    for index, line in enumerate(lines):
        headers.append(current)
        if line.is_header:
            current = _build_header(lines, index)
        elif line.is_entity_conflict:
            current = _ENTITY_ONLY_HEADER
    return tuple(headers)


def _header_row(page: _Page, index: int) -> _Header | None:
    """Column evidence for the line at ``index``: the nearest column-header row above it on the
    same page. A Group-and-Company line without periods met first yields the entity-only
    header; ``None`` means the page has no header row above the line. A constant-time lookup
    in the page's precomputed :attr:`_Page.headers`."""
    return page.headers[index]


def _choose_column(columns: Sequence[_Column], period_year: int | None) -> _Column | None:
    """The period column to read: Group columns when Group and Company both exist; the year of
    ``period_end_date`` when supplied (else the latest year, "current" counting as latest),
    resolved by year and never by position. ``None`` unless exactly one column qualifies."""
    if not columns:
        return None
    entities = {column.entity for column in columns}
    if {"group", "company"} <= entities:
        pool = [column for column in columns if column.entity == "group"]
    else:
        pool = list(columns)
    years = [column for column in pool if column.is_year]
    if period_year is not None and years:
        matching = [column for column in years if column.rank == period_year]
    else:
        best = max(column.rank for column in pool)
        matching = [column for column in pool if column.rank == best]
    return matching[0] if len(matching) == 1 else None


def _select_value(document: _Document, page: _Page, index: int) -> _Reading:
    """Read the number on a matched line by column evidence, never by token position.

    Without a header row a single value token is read and several are ambiguous. With one, every
    token must overlap exactly one column; Note-column tokens are discarded, and the chosen
    period column must hold exactly one readable token. Anything else is ambiguous.
    """
    line = page.lines[index]
    tokens = line.value_words

    def reading(token: _Word | None, offered: Sequence[_Word], hints: tuple[str, ...]) -> _Reading:
        # A selected token that is not a well-formed number (an unbalanced parenthesis, say)
        # leaves the line unread, offered as a candidate.
        amount = _token_amount(token.norm) if token is not None else None
        return _Reading(
            page_number=page.number,
            scale=page.scale,
            label=line.label,
            norm_label=line.norm_label,
            token=token if amount is not None else None,
            amount=amount,
            hints=hints if amount is not None else (),
            tokens=tuple(offered),
        )

    header = _header_row(page, index)
    if header is None:
        if len(tokens) == 1:
            return reading(tokens[0], tokens, ())
        return reading(None, tokens, ())
    if header.entity_only:
        return reading(None, tokens, ())
    chosen = _choose_column(
        [column for column in header.columns if not column.is_note], document.period_year
    )
    offered: list[_Word] = []
    in_chosen: list[_Word] = []
    for token in tokens:
        hits = [
            column
            for column in header.columns
            if _overlaps(column.x0, column.x1, token.x0, token.x1)
        ]
        if len(hits) != 1:
            return reading(None, tokens, ())
        if hits[0].is_note:
            continue
        offered.append(token)
        if hits[0] is chosen:
            in_chosen.append(token)
    if chosen is None or len(in_chosen) != 1 or _token_amount(in_chosen[0].norm) is None:
        return reading(None, offered, ())
    return reading(in_chosen[0], in_chosen, chosen.hints)


# ---------------------------------------------------------------------------------------------
# Values, resolution and candidates
# ---------------------------------------------------------------------------------------------


# terminal_fundamentals money columns are NUMERIC(24,2): 22 integer digits and 2 decimal places.
# PostgreSQL rounds a stored value half-up to those places and rejects it unless it then stays
# below 10^22, so 10^22 - 0.005, the smallest magnitude that rounds to 10^22, is the bound.
_MONEY_LIMIT: Final = Decimal("9999999999999999999999.995")
"""Exclusive bound on the magnitude of a monetary value, extracted or derived."""


def _scaled(amount: Decimal, scale: int) -> Decimal:
    """``amount x scale`` exactly, for a power-of-ten ``scale`` (1, 1,000, 1,000,000 or
    1,000,000,000).

    The printed digits are kept and the decimal point moved right by the scale's power of ten,
    so the product is never rounded, whatever the active ``decimal`` context. A whole product
    has a zero exponent ("1,234" x1,000 is ``Decimal("1234000")``, never ``1.234E+6``) and a
    fractional one keeps exactly the printed digits ("1,234.5678" x1,000 is
    ``Decimal("1234567.8")``). A zero is returned as a positive zero.

    Raises ``ValueError`` for a scale that is not a positive power of ten, or a NaN or infinite
    amount; neither occurs, since every scale is 1 or comes from a scale marker or a unit hint,
    and value tokens are finite numbers.
    """
    power = len(str(scale)) - 1
    if scale != 10**power:
        raise ValueError(f"a scale must be a positive power of ten, not {scale}")
    sign, digits, exponent = amount.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError(f"{amount!r} is not a finite amount")
    shifted = exponent + power
    # A whole product takes trailing zeros rather than a positive exponent: at most ``power`` of
    # them, so an amount that already has a positive exponent keeps it.
    padding = min(max(shifted, 0), power)
    value = Decimal((sign, (*digits, *(0,) * padding), shifted - padding))
    return value.copy_abs() if value.is_zero() else value


def _exact_sum(values: Sequence[Decimal]) -> Decimal:
    """The sum of ``values`` (``0`` for none), exact at any length, never a negative zero.

    The additions run in a local context of unbounded precision and exponent range, never the
    active one, so adding finite numbers is exact; ``Inexact`` and ``Rounded`` are trapped, so a
    sum that lost a digit would raise rather than be returned rounded.
    """
    if not values:
        return Decimal(0)
    context = Context(
        prec=MAX_PREC, Emax=MAX_EMAX, Emin=MIN_EMIN, traps=[InvalidOperation, Inexact, Rounded]
    )
    total = values[0]
    for value in values[1:]:
        total = context.add(total, value)
    return total.copy_abs() if total.is_zero() else total


def _fits_money_column(value: Decimal) -> bool:
    """Whether a monetary ``value`` can be committed to a NUMERIC(24,2) column of
    ``terminal_fundamentals``: its magnitude is below ``_MONEY_LIMIT``. Exact under any
    ``decimal`` context, since ``copy_abs`` and comparisons never round."""
    return value.copy_abs() < _MONEY_LIMIT


def _money_scale(reading: _Reading) -> int:
    """Multiplier of a monetary reading: its page's scale (the page's own marker, else the
    document's inherited scale)."""
    return reading.scale


def _money_value(reading: _Reading, amount: Decimal) -> Decimal | None:
    """A monetary value: the printed amount times the page scale, exact (:func:`_scaled`) in
    whole units of the statement currency. ``None`` when the value cannot be committed to a
    NUMERIC(24,2) money column (:func:`_fits_money_column`), which makes its item
    ``ambiguous``."""
    value = _scaled(amount, reading.scale)
    return value if _fits_money_column(value) else None


def _unit_multiplier(text: str) -> int | None:
    """A share-count unit hint ('000, thousands, millions, mn, billions, bn) in ``text``.
    Text naming a currency describes money, not shares, and gives no hint."""
    if _CURRENCY_RE.search(text) is not None:
        return None
    match = _SHARE_UNIT_RE.search(text)
    if match is None or match.lastgroup is None:
        return None
    return _SCALE_VALUES[match.lastgroup]


def _share_scale(reading: _Reading) -> int:
    """Multiplier of a share count: its own label's unit hint, else the selected column's, else
    1. The page's monetary scale never applies to shares."""
    for text in (reading.norm_label, *reading.hints):
        multiplier = _unit_multiplier(text)
        if multiplier is not None:
            return multiplier
    return 1


def _share_value(reading: _Reading, amount: Decimal) -> Decimal | None:
    """An integral share count in 0..``MAX_SHARE_COUNT``, or ``None`` when the amount, scaled
    exactly by its unit hint (:func:`_scaled`), is fractional, negative or too large."""
    value = _scaled(amount, _share_scale(reading))
    if value != value.to_integral_value() or not 0 <= value <= MAX_SHARE_COUNT:
        return None
    return Decimal(int(value))


_MONEY: Final = _Valuation(scale_of=_money_scale, value_of=_money_value)
_SHARES: Final = _Valuation(scale_of=_share_scale, value_of=_share_value)


def _reading_value(reading: _Reading, valuation: _Valuation) -> Decimal | None:
    """The item's value of ``reading`` under ``valuation``, after scaling; ``None`` when the line
    was not read or its amount is not a valid value for the item."""
    if reading.amount is None:
        return None
    return valuation.value_of(reading, reading.amount)


def _evidence(readings: Sequence[_Reading], valuation: _Valuation) -> tuple[Candidate, ...]:
    """Up to five candidates: the selected token of each resolved reading and every offered
    token of an ambiguous one, each with its value after scaling (``None`` if unreadable)."""
    candidates: list[Candidate] = []
    for reading in readings:
        for token in reading.tokens:
            amount = _token_amount(token.norm)
            value = valuation.value_of(reading, amount) if amount is not None else None
            candidates.append(Candidate(reading.label, reading.page_number, token.text, value))
            if len(candidates) == _MAX_CANDIDATES:
                return tuple(candidates)
    return tuple(candidates)


def _resolve(
    readings: Sequence[_Reading], primary_page: int | None, valuation: _Valuation
) -> _Outcome:
    """Settle equal-priority matches: ``found`` when every match was read and all agree on the
    value, otherwise ``ambiguous``. The reported match is the first on the primary statement
    page, or the first overall."""
    values = [_reading_value(reading, valuation) for reading in readings]
    chosen = next(
        (position for position, item in enumerate(readings) if item.page_number == primary_page),
        0,
    )
    first = readings[chosen]
    agreed = all(value is not None for value in values) and len(set(values)) == 1
    status: Literal["found", "ambiguous"] = "found" if agreed else "ambiguous"
    return _Outcome(
        status=status,
        value=values[chosen],
        raw_value=first.token.text if first.token is not None else None,
        scale=valuation.scale_of(first),
        page_number=first.page_number,
        label=first.label,
        evidence=_evidence(readings, valuation),
    )


# ---------------------------------------------------------------------------------------------
# Item matching
# ---------------------------------------------------------------------------------------------


def _search_pages(
    document: _Document, kinds: tuple[_StatementKind, ...]
) -> tuple[tuple[_Page, ...], int | None]:
    """Pages to search for an item and its primary statement page.

    The preferred statement pages, in the order of ``kinds`` and then page order, with the first
    of them as the primary page; every page, with no primary page, when the item has no
    preference or the document has no page of the preferred kinds.
    """
    preferred = tuple(page for kind in kinds for page in document.pages if page.kind == kind)
    if preferred:
        return preferred, preferred[0].number
    return document.pages, None


def _is_candidate_line(line: _Line, *, monetary: bool) -> bool:
    """A line that can match a label: it has a label and value tokens (a label without values
    is a heading), and for monetary items its label does not mention "per share"."""
    if not line.value_words or not line.norm_label:
        return False
    return not (monetary and "per share" in line.norm_label)


def _match_readings(
    document: _Document, spec: _ItemSpec, *, monetary: bool = True
) -> tuple[list[_Reading], int | None]:
    """Search an item's pages pattern by pattern: the readings of every candidate line matched
    by the highest-priority pattern with any match, in search-page then line order, and the
    item's primary statement page. The list is empty when no pattern matches."""
    pages, primary = _search_pages(document, spec.kinds)
    for pattern in spec.patterns:
        readings = [
            _select_value(document, page, line.index)
            for page in pages
            for line in page.lines
            if _is_candidate_line(line, monetary=monetary)
            and pattern.fullmatch(line.norm_label) is not None
        ]
        if readings:
            return readings, primary
    return [], primary


def _match_item(
    document: _Document, spec: _ItemSpec, valuation: _Valuation, *, monetary: bool = True
) -> _Outcome | None:
    """Search an item's pages pattern by pattern; the highest-priority pattern with any match
    decides it through :func:`_resolve`. ``None`` when no pattern matches."""
    readings, primary = _match_readings(document, spec, monetary=monetary)
    if not readings:
        return None
    return _resolve(readings, primary, valuation)


def _combine(outcomes: Sequence[_Outcome], primary_page: int | None) -> _Outcome:
    """Merge per-page outcomes: identical values on every page keep the primary page's outcome;
    differing values or any ambiguous page make the item ambiguous."""
    chosen = next((item for item in outcomes if item.page_number == primary_page), outcomes[0])
    if len(outcomes) == 1:
        return chosen
    agreed = all(item.status != "ambiguous" for item in outcomes) and (
        len({item.value for item in outcomes}) == 1
    )
    if agreed:
        return chosen
    evidence = tuple(candidate for item in outcomes for candidate in item.evidence)
    return replace(chosen, status="ambiguous", parts=(), evidence=evidence[:_MAX_CANDIDATES])


def _debt_on_page(document: _Document, page: _Page) -> _Outcome | None:
    """The debt rule on one statement page.

    A total line ("total borrowings", "loans and borrowings", "total debt", in that priority)
    is used alone. Otherwise exactly one current and one non-current component, or one
    short-term and one long-term component, are summed exactly into a ``derived`` value. A
    generic "borrowings" line, a lone component, more than two components, repeated or unpaired
    component types, an unread component line, or a component or sum beyond the NUMERIC(24,2)
    range (:func:`_fits_money_column`) is ambiguous. No section-heading inference.
    """
    totals: list[list[_Reading]] = [[] for _ in _DEBT_TOTALS]
    matched: list[_Reading] = []
    components: list[tuple[_DebtComponent, _Reading]] = []
    has_generic = False
    for line in page.lines:
        if not _is_candidate_line(line, monetary=True):
            continue
        label = line.norm_label
        level = next(
            (rank for rank, pattern in enumerate(_DEBT_TOTALS) if pattern.fullmatch(label)),
            None,
        )
        if level is not None:
            totals[level].append(_select_value(document, page, line.index))
            continue
        component = next(
            (kind for pattern, kind in _DEBT_COMPONENTS if pattern.fullmatch(label)), None
        )
        is_generic = _DEBT_GENERIC.fullmatch(label) is not None
        if component is None and not is_generic:
            continue
        reading = _select_value(document, page, line.index)
        matched.append(reading)
        if component is None:
            has_generic = True
        else:
            components.append((component, reading))
    for readings in totals:
        if readings:
            return _resolve(readings, page.number, _MONEY)
    if not matched:
        return None
    evidence = _evidence(matched, _MONEY)
    kinds = frozenset(kind for kind, _ in components)
    # Each component is valued by _money_value: an unread line, or an amount beyond the money
    # column's range, has no value and gives no part.
    values = [_reading_value(reading, _MONEY) for _, reading in components]
    parts = tuple(
        DerivationPart(reading.label, reading.page_number, value)
        for (_, reading), value in zip(components, values, strict=True)
        if value is not None
    )
    total = _exact_sum([part.value for part in parts])
    summable = (
        not has_generic
        and len(components) == 2
        and kinds in _DEBT_PAIRS
        and len(parts) == len(components)
        and _fits_money_column(total)
    )
    if not summable:
        first = matched[0]
        return _Outcome(
            status="ambiguous",
            value=_reading_value(first, _MONEY),
            raw_value=first.token.text if first.token is not None else None,
            scale=first.scale,
            page_number=first.page_number,
            label=first.label,
            evidence=evidence,
        )
    return _Outcome(
        status="derived",
        value=total,
        raw_value=None,
        scale=components[0][1].scale,
        page_number=page.number,
        label=None,
        parts=parts,
        evidence=evidence,
    )


def _debt_rule(document: _Document) -> _Outcome | None:
    """Total debt: the debt rule per statement-of-financial-position page (every page when the
    document has none), with the per-page outcomes merged by :func:`_combine`."""
    pages, primary = _search_pages(document, _DEBT_KINDS)
    outcomes = [
        outcome
        for outcome in (_debt_on_page(document, page) for page in pages)
        if outcome is not None
    ]
    if not outcomes:
        return None
    return _combine(outcomes, primary)


def _da_component(document: _Document, spec: _ItemSpec) -> _Outcome | None:
    """One D&A component, ``_DEPRECIATION`` or ``_AMORTISATION``, for the fallback of
    :func:`_depreciation_and_amortisation`: its matches settled by :func:`_resolve`, except that
    distinct component lines are never merged.

    More than one distinct label, or more than one matched line on one page, is ``ambiguous``
    even when the amounts agree, keeping :func:`_resolve`'s reported line and candidates, since
    only an explicit combined line establishes their total. One label printed once on each of
    several pages with equal values (the same aggregate on the income statement and the
    statement of cash flows) stays ``found``. ``None`` when no line matches.
    """
    readings, primary = _match_readings(document, spec)
    if not readings:
        return None
    outcome = _resolve(readings, primary, _MONEY)
    labels = {reading.norm_label for reading in readings}
    pages = [reading.page_number for reading in readings]
    if len(labels) > 1 or len(set(pages)) < len(pages):
        return replace(outcome, status="ambiguous")
    return outcome


def _depreciation_and_amortisation(document: _Document) -> list[_Outcome] | None:
    """D&A from the cash-flow or income-statement pages: a combined "depreciation and
    amortisation" line (the explicit total, settled by :func:`_resolve`), otherwise exactly one
    "depreciation ..." line plus at most one "amortisation ..." line, each settled by
    :func:`_da_component`, so distinct component lines are ambiguous, never merged or summed.
    ``None`` when no depreciation is found."""
    combined = _match_item(document, _DA_COMBINED, _MONEY)
    if combined is not None:
        return [combined]
    depreciation = _da_component(document, _DEPRECIATION)
    if depreciation is None:
        return None
    amortisation = _da_component(document, _AMORTISATION)
    return [depreciation] if amortisation is None else [depreciation, amortisation]


def _derive_ebitda(document: _Document) -> _Outcome | None:
    """EBITDA derived as operating profit plus the absolute value of D&A (expense lines may be
    printed in parentheses), summed exactly. A missing part gives ``None`` (not found); an
    ambiguous part makes EBITDA ambiguous, reported with that part's line and candidates but no
    value. A sum beyond the NUMERIC(24,2) range (:func:`_fits_money_column`) is ambiguous too,
    reported with the operating profit line and every part's candidates but no value."""
    operating = _match_item(document, _OPERATING_PROFIT, _MONEY)
    if operating is None:
        return None
    amortisation_parts = _depreciation_and_amortisation(document)
    if amortisation_parts is None:
        return None
    outcomes = [operating, *amortisation_parts]
    parts: list[DerivationPart] = []
    for position, outcome in enumerate(outcomes):
        if (
            outcome.status == "ambiguous"
            or outcome.value is None
            or outcome.label is None
            or outcome.page_number is None
        ):
            return replace(outcome, status="ambiguous", value=None, raw_value=None, parts=())
        # copy_abs is exact; abs() would round to the active context's precision.
        value = outcome.value if position == 0 else outcome.value.copy_abs()
        parts.append(DerivationPart(outcome.label, outcome.page_number, value))
    evidence = tuple(candidate for outcome in outcomes for candidate in outcome.evidence)
    total = _exact_sum([part.value for part in parts])
    if not _fits_money_column(total):
        return replace(
            operating,
            status="ambiguous",
            value=None,
            raw_value=None,
            parts=(),
            evidence=evidence[:_MAX_CANDIDATES],
        )
    return _Outcome(
        status="derived",
        value=total,
        raw_value=None,
        scale=operating.scale,
        page_number=operating.page_number,
        label=None,
        parts=tuple(parts),
        evidence=evidence[:_MAX_CANDIDATES],
    )


def _not_found(name: str, unit: ItemUnit) -> ExtractedLineItem:
    """The ``not_found`` item ``name`` in ``unit``: value, raw value, scale, page and label
    ``None``, and ``derivation`` and ``candidates`` empty."""
    return ExtractedLineItem(item=name, status="not_found", value=None, unit=unit)


def _extract_item(document: _Document, name: str, money_unit: ItemUnit) -> ExtractedLineItem:
    """Resolve one requested item and convert it to :class:`ExtractedLineItem`."""
    unit: ItemUnit = "shares" if name == "shares_outstanding" else money_unit
    outcome: _Outcome | None
    if name == "total_debt":
        outcome = _debt_rule(document)
    elif name == "ebitda":
        outcome = _match_item(document, _EBITDA, _MONEY)
        if outcome is None:
            outcome = _derive_ebitda(document)
    elif name == "shares_outstanding":
        outcome = _match_item(document, _SHARES_OUTSTANDING, _SHARES, monetary=False)
    else:
        outcome = _match_item(document, _SIMPLE_ITEMS[name], _MONEY)
    if outcome is None:
        return _not_found(name, unit)
    return ExtractedLineItem(
        item=name,
        status=outcome.status,
        value=outcome.value,
        unit=unit,
        raw_value=outcome.raw_value,
        scale_applied=outcome.scale,
        page_number=outcome.page_number,
        matched_label=outcome.label,
        derivation=list(outcome.parts) if outcome.status == "derived" else [],
        candidates=list(outcome.evidence) if outcome.status == "ambiguous" else [],
    )


# ---------------------------------------------------------------------------------------------
# Requested items and the document result
# ---------------------------------------------------------------------------------------------


def _requested_items(items: Sequence[str]) -> list[str]:
    """Validated, de-duplicated item names in the caller's order."""
    if isinstance(items, str):
        raise ValueError("items must be a sequence of line-item names, not a single string")
    requested: list[str] = []
    for name in items:
        if name not in LINE_ITEMS:
            raise ValueError(
                f"unknown line item {name!r}; expected one of: {', '.join(LINE_ITEMS)}"
            )
        if name not in requested:
            requested.append(name)
    if not requested:
        raise ValueError("at least one line item must be requested")
    return requested


def _document_scale(pages: Sequence[_Page]) -> tuple[int, bool]:
    """The scale inherited by pages without a marker, and whether any page had a marker.

    The most frequent marked scale wins; a tie goes to the scale first seen on the earliest
    page. With no marker anywhere the scale is 1.
    """
    counts = Counter(page.marker_scale for page in pages if page.marker_scale is not None)
    if not counts:
        return 1, False
    first_page: dict[int, int] = {}
    for page in pages:
        if page.marker_scale is not None:
            first_page.setdefault(page.marker_scale, page.number)
    return min(counts, key=lambda scale: (-counts[scale], first_page[scale])), True


def _filing_status(items: Sequence[ExtractedLineItem]) -> FilingStatus:
    """``extracted`` with no item ``not_found``, ``no_items`` when all are, else ``partial``.

    Status follows not-found items only, as the response schema documents, so an ambiguous item
    is neither found nor missing.
    """
    missing = sum(1 for item in items if item.status == "not_found")
    if missing == 0:
        return "extracted"
    if missing == len(items):
        return "no_items"
    return "partial"


def _log_result(result: ExtractionResult) -> None:
    """Log the INFO extraction summary: page count, found-or-derived, ambiguous and missing item
    counts, detected scale, currency and status. Never the PDF bytes; no file hash is known here."""
    found = sum(1 for item in result.items if item.status in ("found", "derived"))
    ambiguous = sum(1 for item in result.items if item.status == "ambiguous")
    logger.info(
        "PDF line items: pages=%d found_or_derived=%d ambiguous=%d missing=%d "
        "detected_scale=%s currency=%s status=%s",
        result.page_count,
        found,
        ambiguous,
        len(result.missing_items),
        result.detected_scale,
        result.currency,
        result.status,
    )


def _extract(
    pdf_bytes: bytes,
    requested: Sequence[str],
    period_end_date: date | None,
    max_pages: int,
) -> ExtractionResult:
    """Read the PDF and extract the ``requested`` items (validated, de-duplicated names): the
    work :func:`extract_line_items` runs in the parser process. Nothing is logged here."""
    page_words = _read_pages(pdf_bytes, max_pages)
    page_count = len(page_words)

    # The pages exist (a document without pages is unreadable) but none has a word.
    if not any(page_words):
        return ExtractionResult(
            page_count=page_count,
            detected_scale=None,
            currency=DEFAULT_CURRENCY,
            status="no_items",
            items=[
                _not_found(name, "shares" if name == "shares_outstanding" else "KES")
                for name in requested
            ],
            missing_items=list(requested),
            warnings=["no_text_layer"],
        )

    analysed = [_analyse_page(number, words) for number, words in enumerate(page_words, start=1)]
    document_scale, marked = _document_scale(analysed)
    pages = tuple(
        replace(page, scale=page.marker_scale if page.marker_scale is not None else document_scale)
        for page in analysed
    )
    # The presentation currency is read from statement headers: a USD mention in the opening
    # lines of a notes page (a dollar bond, say) is not a header and must not relabel a KES
    # filing. Only a document without statement pages is judged from every page.
    statement_pages = [page for page in pages if page.kind is not None]
    is_usd = any(page.has_usd for page in (statement_pages or pages))

    warnings: list[str] = []
    if not marked:
        warnings.append("scale_not_detected")
    if is_usd:
        warnings.append("non_kes_currency")
    document = _Document(
        pages=pages,
        period_year=period_end_date.year if period_end_date is not None else None,
    )
    money_unit: ItemUnit = "USD" if is_usd else "KES"
    extracted = [_extract_item(document, name, money_unit) for name in requested]
    return ExtractionResult(
        page_count=page_count,
        # With no marker the scale applied is 1, reported as 1 beside scale_not_detected; None
        # is reserved for a PDF without a text layer, as the response schema documents.
        detected_scale=document_scale,
        currency="USD" if is_usd else DEFAULT_CURRENCY,
        status=_filing_status(extracted),
        items=extracted,
        missing_items=[item.item for item in extracted if item.status == "not_found"],
        warnings=warnings,
    )


# ---------------------------------------------------------------------------------------------
# The PDF parser process
# ---------------------------------------------------------------------------------------------

_Choice = TypeVar("_Choice", bound=str)
_ITEM_STATUSES: Final[tuple[ItemStatus, ...]] = get_args(ItemStatus)
_ITEM_UNITS: Final[tuple[ItemUnit, ...]] = get_args(ItemUnit)
_FILING_STATUSES: Final[tuple[FilingStatus, ...]] = get_args(FilingStatus)


@dataclass(frozen=True, slots=True)
class _ParseRequest:
    """What the parser process is asked to do: :func:`_extract`'s arguments and its limits."""

    pdf_bytes: bytes
    items: list[str]
    period_end_date: date | None
    max_pages: int
    time_limit: float
    memory_limit: int


def _json_default(value: object) -> str:
    """Encode a ``Decimal`` as its exact string form, sign, digits and exponent kept."""
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not part of an extraction result")


def _record(value: object, name: str) -> Mapping[str, object]:
    """A JSON object of the request or reply; ``ValueError`` names what is malformed."""
    if not isinstance(value, dict):
        raise ValueError(f"{name} is not a JSON object")
    return value


def _get(data: Mapping[str, object], key: str) -> object:
    if key not in data:
        raise ValueError(f"the field {key!r} is missing")
    return data[key]


def _integer(data: Mapping[str, object], key: str) -> int:
    value = _get(data, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"the field {key!r} is not an integer")
    return value


def _optional_integer(data: Mapping[str, object], key: str) -> int | None:
    return None if _get(data, key) is None else _integer(data, key)


def _number(data: Mapping[str, object], key: str) -> float:
    """A limit in seconds, integer or not; it is never a monetary value."""
    value = _get(data, key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"the field {key!r} is not a number")
    return value


def _text(data: Mapping[str, object], key: str) -> str:
    value = _get(data, key)
    if not isinstance(value, str):
        raise ValueError(f"the field {key!r} is not a string")
    return value


def _optional_text(data: Mapping[str, object], key: str) -> str | None:
    return None if _get(data, key) is None else _text(data, key)


def _texts(data: Mapping[str, object], key: str) -> list[str]:
    values = _array(data, key)
    if not all(isinstance(value, str) for value in values):
        raise ValueError(f"the field {key!r} is not a list of strings")
    return [str(value) for value in values]


def _decimal(data: Mapping[str, object], key: str) -> Decimal:
    """An exact decimal sent as its string form; constructing it uses no context precision."""
    text = _text(data, key)
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"the field {key!r} is not a decimal") from exc
    if not value.is_finite():
        raise ValueError(f"the field {key!r} is not a finite decimal")
    return value


def _optional_decimal(data: Mapping[str, object], key: str) -> Decimal | None:
    return None if _get(data, key) is None else _decimal(data, key)


def _array(data: Mapping[str, object], key: str) -> list[object]:
    value = _get(data, key)
    if not isinstance(value, list):
        raise ValueError(f"the field {key!r} is not a list")
    return value


def _choice(data: Mapping[str, object], key: str, options: tuple[_Choice, ...]) -> _Choice:
    value = _get(data, key)
    for option in options:
        if value == option:
            return option
    raise ValueError(f"the field {key!r} is not one of {', '.join(options)}")


def _decode_item(data: Mapping[str, object]) -> ExtractedLineItem:
    """Rebuild one :class:`ExtractedLineItem` from its ``dataclasses.asdict`` form."""
    parts = [_record(entry, "a derivation part") for entry in _array(data, "derivation")]
    candidates = [_record(entry, "a candidate") for entry in _array(data, "candidates")]
    return ExtractedLineItem(
        item=_text(data, "item"),
        status=_choice(data, "status", _ITEM_STATUSES),
        value=_optional_decimal(data, "value"),
        unit=_choice(data, "unit", _ITEM_UNITS),
        raw_value=_optional_text(data, "raw_value"),
        scale_applied=_optional_integer(data, "scale_applied"),
        page_number=_optional_integer(data, "page_number"),
        matched_label=_optional_text(data, "matched_label"),
        derivation=[
            DerivationPart(
                label=_text(part, "label"),
                page_number=_integer(part, "page_number"),
                value=_decimal(part, "value"),
            )
            for part in parts
        ],
        candidates=[
            Candidate(
                label=_text(candidate, "label"),
                page_number=_integer(candidate, "page_number"),
                raw_value=_text(candidate, "raw_value"),
                value=_optional_decimal(candidate, "value"),
            )
            for candidate in candidates
        ],
    )


def _decode_result(data: Mapping[str, object]) -> ExtractionResult:
    """Rebuild an :class:`ExtractionResult` from its ``dataclasses.asdict`` form, validating
    every field, so the reply is exactly the result the parser process computed."""
    return ExtractionResult(
        page_count=_integer(data, "page_count"),
        detected_scale=_optional_integer(data, "detected_scale"),
        currency=_text(data, "currency"),
        status=_choice(data, "status", _FILING_STATUSES),
        items=[_decode_item(_record(entry, "an item")) for entry in _array(data, "items")],
        missing_items=_texts(data, "missing_items"),
        warnings=_texts(data, "warnings"),
    )


def _failure_reply(exc: BaseException, memory_limit: int) -> dict[str, object]:
    """The parser process's reply for a parse that raised ``exc``.

    ``kind`` is the rejection to raise in the caller (``PdfEncrypted``, ``PdfUnreadable`` or
    ``TooManyPages``), or ``internal`` for a fault of the parser itself; ``type`` names the
    exception. Running out of memory anywhere in the exception chain makes the PDF unreadable.
    """
    page_count: int | None = None
    max_pages: int | None = None
    message = str(exc)
    if _is_memory_exhaustion(exc):
        kind, name = "PdfUnreadable", "MemoryError"
        message = f"the PDF needs more than {memory_limit // _MIB} MiB of memory to parse"
    elif isinstance(exc, TooManyPages):
        kind, name = "TooManyPages", type(exc).__name__
        page_count, max_pages = exc.page_count, exc.max_pages
    elif isinstance(exc, PdfEncrypted):
        kind, name = "PdfEncrypted", type(exc).__name__
    elif isinstance(exc, PdfUnreadable):
        kind, name = "PdfUnreadable", type(exc).__name__
    else:
        kind, name = "internal", type(exc).__name__
        message = message[:_MAX_ERROR_MESSAGE_CHARS]
    return {
        "error": {
            "kind": kind,
            "type": name,
            "message": message,
            "page_count": page_count,
            "max_pages": max_pages,
        }
    }


def _decode_failure(error: Mapping[str, object], memory_limit: int) -> Exception:
    """The exception to raise in the caller for the parser process's error reply."""
    kind = _text(error, "kind")
    message = _text(error, "message")
    if kind == "PdfEncrypted":
        return PdfEncrypted(message)
    if kind == "PdfUnreadable":
        if _text(error, "type") == "MemoryError":
            logger.warning("PDF parse stopped at the memory limit of %d MiB", memory_limit // _MIB)
        return PdfUnreadable(message)
    if kind == "TooManyPages":
        return TooManyPages(_integer(error, "page_count"), _integer(error, "max_pages"))
    if kind == "internal":
        return RuntimeError(f"the PDF parser failed with {_text(error, 'type')}: {message}")
    raise ValueError(f"the error kind {kind!r} is unknown")


def _read_request(stream: BinaryIO) -> _ParseRequest:
    """Read the request :func:`_run_isolated` writes: one JSON header line, then exactly
    ``length`` PDF bytes."""
    line = stream.readline(_MAX_REQUEST_HEADER_BYTES)
    if not line.endswith(b"\n"):
        raise ValueError("the parse request has no header line")
    header = _record(json.loads(line), "the parse request header")
    length = _integer(header, "length")
    pdf_bytes = stream.read(length)
    if len(pdf_bytes) != length:
        raise ValueError(f"the parse request holds {len(pdf_bytes)} of {length} PDF bytes")
    period = _optional_text(header, "period_end_date")
    return _ParseRequest(
        pdf_bytes=pdf_bytes,
        items=_requested_items(_texts(header, "items")),
        period_end_date=None if period is None else date.fromisoformat(period),
        max_pages=_integer(header, "max_pages"),
        time_limit=_number(header, "time_limit"),
        memory_limit=_integer(header, "memory_limit"),
    )


def _apply_limits(time_limit: float, memory_limit: int) -> None:
    """Limit the parser process: no core file, ``memory_limit`` bytes of address space, and CPU
    time ``time_limit`` plus a margin, a backstop for when the supervising process is gone (it
    kills this one at ``time_limit`` of wall time). A limit the platform rejects is skipped, and
    Windows has none of them: there the wall-clock limit alone applies."""
    if sys.platform == "win32":
        return
    import resource

    limits = (
        (resource.RLIMIT_CORE, 0),
        (resource.RLIMIT_CPU, math.ceil(time_limit) + _CPU_LIMIT_MARGIN_SECONDS),
        (resource.RLIMIT_AS, memory_limit),
    )
    for kind, wanted in limits:
        with contextlib.suppress(ValueError, OSError):
            _, hard = resource.getrlimit(kind)
            value = wanted if hard == resource.RLIM_INFINITY else min(wanted, hard)
            resource.setrlimit(kind, (value, value))


def _child_main() -> None:
    """Entry point of the PDF parser process that :func:`_run_isolated` starts.

    Reads one request from standard input, limits the process, runs :func:`_extract` and writes
    one JSON reply to standard output: ``{"result": ...}`` with the result's fields, decimals as
    exact strings, or ``{"error": ...}`` (see :func:`_failure_reply`). Logging is disabled in
    this process; the caller logs the outcome.
    """
    logging.disable(logging.CRITICAL)
    reply_stream = sys.stdout.buffer
    # Anything else printed goes to the discarded error stream, never into the reply.
    sys.stdout = sys.stderr
    reserve = bytearray(_MEMORY_RESERVE_BYTES)
    memory_limit = _PARSE_MEMORY_LIMIT_BYTES
    reply: dict[str, object]
    try:
        request = _read_request(sys.stdin.buffer)
        memory_limit = request.memory_limit
        _apply_limits(request.time_limit, request.memory_limit)
        result = _extract(
            request.pdf_bytes, request.items, request.period_end_date, request.max_pages
        )
        reply = {"result": asdict(result)}
    except BaseException as exc:
        # Gives back address space, so a failure at the memory limit can still be reported.
        reserve.clear()
        reply = _failure_reply(exc, memory_limit)
    reply_stream.write(
        json.dumps(reply, default=_json_default, separators=(",", ":")).encode("ascii")
    )
    reply_stream.flush()


def _child_environment() -> dict[str, str]:
    """The parser process's environment: the caller's, with ``PYTHONPATH`` set to the caller's
    import path, so the process imports this same package."""
    path = os.pathsep.join(entry for entry in sys.path if isinstance(entry, str) and entry)
    return {**os.environ, "PYTHONPATH": path}


def _parse_time_limit(max_pages: int) -> float:
    """Wall-clock limit in seconds of a parse that admits up to ``max_pages`` pages:
    :data:`_PARSE_TIME_BASE_SECONDS` plus :data:`_PARSE_SECONDS_PER_PAGE` per page, 20 + 0.4 x
    ``max_pages`` (180 seconds at 400 pages)."""
    return _PARSE_TIME_BASE_SECONDS + _PARSE_SECONDS_PER_PAGE * max_pages


def _exchange(
    process: subprocess.Popen[bytes],
    request: bytes,
    time_limit: float,
    cancel: threading.Event | None,
) -> bytes:
    """Send ``request`` to the parser process and return its standard output, stopping the
    process at ``time_limit`` seconds or once ``cancel`` is set.

    One ``communicate`` call runs on a helper thread, because CPython does not resume a timed-out
    ``communicate`` with the input it had not yet written, so the exchange cannot be retried in
    slices. The calling thread waits for the helper in slices of at most
    :data:`_SUPERVISION_SLICE_SECONDS`, checking the deadline and ``cancel`` between them. To stop
    the process it kills it and waits for the helper, whose ``communicate`` then reads to the end
    of the dead process's output and reaps it, so no process outlives this call. A finished
    exchange is returned even when ``cancel`` was set meanwhile.

    Raises:
        PdfParseTimeout: The deadline passed first.
        ParseCancelledError: ``cancel`` was set first.
        RuntimeError: The request could not be sent or the reply read: a fault of the server.
    """
    replies: list[bytes] = []
    failures: list[Exception] = []

    def communicate() -> None:
        try:
            replies.append(process.communicate(request)[0])
        except Exception as exc:
            # Handed to the supervising thread, which raises it as a server fault.
            failures.append(exc)

    helper = threading.Thread(target=communicate, name="pdf-parser-exchange", daemon=True)
    deadline = time.monotonic() + time_limit
    helper.start()
    stopped: PdfParseTimeout | ParseCancelledError | None = None
    try:
        while helper.is_alive():
            if cancel is not None and cancel.is_set():
                stopped = ParseCancelledError()
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stopped = PdfParseTimeout(time_limit)
                break
            helper.join(min(remaining, _SUPERVISION_SLICE_SECONDS))
    except BaseException:
        process.kill()
        helper.join()
        raise
    if stopped is not None:
        process.kill()
        helper.join()
        if isinstance(stopped, PdfParseTimeout):
            logger.warning("PDF parse stopped at the time limit of %g seconds", time_limit)
        raise stopped
    if failures:
        raise RuntimeError("the PDF parser process could not be sent its request") from failures[0]
    return replies[0]


def _run_isolated(
    pdf_bytes: bytes,
    requested: Sequence[str],
    period_end_date: date | None,
    max_pages: int,
    *,
    time_limit: float | None = None,
    memory_limit: int = _PARSE_MEMORY_LIMIT_BYTES,
    cancel: threading.Event | None = None,
) -> ExtractionResult:
    """Run :func:`_extract` in a separate Python process and return its result.

    The calling thread supervises the process and blocks until it ends. The process gets
    ``time_limit`` seconds of wall time (``None``: :func:`_parse_time_limit` of ``max_pages``)
    and ``memory_limit`` bytes of address space, and is killed at the deadline or, within
    :data:`_SUPERVISION_SLICE_SECONDS`, once another thread sets ``cancel``. The request (a JSON
    header line, then the PDF bytes) is written to its standard input and the reply read from its
    standard output (see :func:`_exchange`), so the PDF never touches the disk; decimals cross as
    exact strings and every field of the reply is validated.

    Raises:
        PdfEncrypted, PdfUnreadable, TooManyPages: As :func:`_extract` raised them, rebuilt.
        PdfUnreadable: Also when the parse needed more memory than its limit allows, or the
            process was stopped by a signal (its CPU-time backstop included).
        PdfParseTimeout: The parse reached ``time_limit``.
        ParseCancelledError: ``cancel`` was set before the parse answered; when it is set
            already, no process is started.
        RuntimeError: The process could not be started, failed, or sent a malformed reply: a
            fault of the server, not of the PDF.
    """
    if time_limit is None:
        time_limit = _parse_time_limit(max_pages)
    if not sys.executable:
        raise RuntimeError("no Python interpreter is available to start the PDF parser process")
    if cancel is not None and cancel.is_set():
        raise ParseCancelledError
    # Only the calendar date crosses: a datetime, which is also a date, selects by its year too.
    period = (
        None
        if period_end_date is None
        else date(period_end_date.year, period_end_date.month, period_end_date.day)
    )
    header = {
        "items": list(requested),
        "period_end_date": None if period is None else period.isoformat(),
        "max_pages": max_pages,
        "length": len(pdf_bytes),
        "time_limit": time_limit,
        "memory_limit": memory_limit,
    }
    request = json.dumps(header, separators=(",", ":")).encode("ascii") + b"\n" + pdf_bytes
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-P",
                "-c",
                "from opennse_terminal.ingestion.files.pdf_line_items import _child_main; "
                "_child_main()",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=_child_environment(),
        )
    except OSError as exc:
        raise RuntimeError("the PDF parser process could not be started") from exc
    with process:
        output = _exchange(process, request, time_limit, cancel)
    if process.returncode < 0:
        logger.warning(
            "PDF parse stopped by signal %d at the limits of %g seconds and %d MiB",
            -process.returncode,
            time_limit,
            memory_limit // _MIB,
        )
        raise PdfUnreadable(
            f"the PDF could not be parsed within {time_limit:g} seconds and "
            f"{memory_limit // _MIB} MiB of memory"
        )
    if process.returncode != 0:
        raise RuntimeError(f"the PDF parser process exited with status {process.returncode}")
    outcome: ExtractionResult | Exception
    try:
        reply = _record(json.loads(output), "the parse reply")
        if "error" in reply:
            outcome = _decode_failure(_record(reply["error"], "the parse error"), memory_limit)
        else:
            outcome = _decode_result(_record(_get(reply, "result"), "the parse result"))
    except (ValueError, RecursionError) as exc:
        raise RuntimeError("the PDF parser process sent a malformed reply") from exc
    if isinstance(outcome, ExtractionResult):
        return outcome
    raise outcome


# ---------------------------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------------------------


def extract_line_items(
    pdf_bytes: bytes,
    items: Sequence[str] = LINE_ITEMS,
    *,
    period_end_date: date | None = None,
    max_pages: int = 400,
    cancel: threading.Event | None = None,
) -> ExtractionResult:
    """Extract the requested line items from a filing or prospectus PDF.

    The arguments are validated here; the PDF is then parsed in a separate Python process with
    bounded wall time (``_PARSE_TIME_BASE_SECONDS`` plus ``_PARSE_SECONDS_PER_PAGE`` per page of
    ``max_pages``: 20 s + 0.4 s x ``max_pages``, 180 s at 400 pages), address space
    (``_PARSE_MEMORY_LIMIT_BYTES``, 1 GiB) and decoded content per page
    (``_MAX_PAGE_CONTENT_BYTES``, 8 MiB), so a hostile file can neither hang nor exhaust the
    caller. The calling thread waits for that process, so services still call this in a worker
    thread (``run_in_threadpool``), and another thread stops the wait early by setting
    ``cancel``. Concurrent calls from several threads are independent: nothing is shared between
    them. The INFO summary is logged once, here.

    Args:
        pdf_bytes: The whole PDF. Size, the ``%PDF-`` magic bytes and concurrency are the
            caller's checks; nothing but these bytes is read, and they are never written to disk.
        items: Names from :data:`LINE_ITEMS`, results in this order (duplicates dropped).
        period_end_date: Selects the period column by its year; without it the latest year is
            read.
        max_pages: Largest page count parsed; it also sizes the wall-clock limit.
        cancel: An event another thread sets to stop the parse, for example when the request it
            serves is abandoned: the parser process is killed within 0.25 s
            (``_SUPERVISION_SLICE_SECONDS``) and :class:`ParseCancelledError` is raised. When it
            is set already, the arguments are still validated but no parse starts.

    Returns:
        One :class:`ExtractedLineItem` per requested item. Monetary values are whole KES after
        the page scale, or whole USD when a statement header is in USD (``currency="USD"`` and
        the warning ``non_kes_currency``; never converted). ``shares_outstanding`` is an
        integral count, scaled only by its own unit hint. Values are exact ``Decimal`` values.
        Item statuses are ``found``, ``derived`` (summed from component lines listed in
        ``derivation``), ``ambiguous`` (insufficient evidence, with up to five ``candidates``;
        never to be committed) and ``not_found``. Warnings: ``no_text_layer`` (pages without a
        word; no OCR is attempted), ``scale_not_detected`` and ``non_kes_currency``.

    Raises:
        ValueError: An unknown item name, no item, or ``max_pages`` below 1.
        PdfEncrypted: The PDF is encrypted, even with an empty user password.
        PdfUnreadable: The bytes cannot be parsed; a cross-reference section lists more objects
            than it holds; the PDF has no pages; a page's content cannot be read or decodes to
            more than 8 MiB; or the parse exceeds its memory limit or is stopped by a signal.
        PdfParseTimeout: The parse did not finish within its wall-clock limit; its
            ``time_limit`` names the seconds allowed.
        TooManyPages: More than ``max_pages`` pages, found after enumerating at most
            ``max_pages + 1`` of them and before any text is extracted.
        ParseCancelledError: ``cancel`` was set before the parse finished.
        RuntimeError: The parser process could not run or failed: a server fault.
    """
    requested = _requested_items(items)
    if max_pages < 1:
        raise ValueError(f"max_pages must be at least 1, got {max_pages}")
    result = _run_isolated(
        pdf_bytes,
        requested,
        period_end_date,
        max_pages,
        time_limit=_parse_time_limit(max_pages),
        cancel=cancel,
    )
    _log_result(result)
    return result
