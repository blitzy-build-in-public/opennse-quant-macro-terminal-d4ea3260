"""Extract key line items from corporate filing and IPO prospectus PDFs with pdfplumber.

:func:`extract_line_items` reads Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and
Shares Outstanding from the text layer of an annual report, interim report or prospectus. It is
pure and synchronous: no I/O beyond the given bytes, no settings, no module-level mutable state,
so ``services/filings_service.py`` runs it in a worker thread for ``POST /ingest/pdf``. There is
no OCR: a PDF without a text layer yields ``no_items`` with the warning ``no_text_layer``.

How a value is chosen:

* Pages are classified by their title region (first 8 lines) as income statement, statement of
  financial position or statement of cash flows; each item is searched on its preferred
  statement pages, or on every page when the document has none of that type.
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
  own line or column. Values are not rounded here.

Units: monetary items are whole KES after scaling, or whole USD when a statement header is in
USD (``currency="USD"``, warning ``non_kes_currency``; nothing is converted). Shares are integral
``Decimal`` counts of at most ``MAX_SHARE_COUNT``.
"""

import contextlib
import io
import logging
import re
import types
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from typing import Final, Literal, Protocol

import pdfplumber

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
    "PdfEncrypted",
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


@dataclass(frozen=True, slots=True)
class DerivationPart:
    """One statement line a ``derived`` value was summed from, value after scaling."""

    label: str
    page_number: int
    value: Decimal


@dataclass(frozen=True, slots=True)
class Candidate:
    """One conflicting match of an ``ambiguous`` item: the token as printed and its scaled value
    (``None`` when the token is not a number)."""

    label: str
    page_number: int
    raw_value: str
    value: Decimal | None


@dataclass(frozen=True, slots=True)
class ExtractedLineItem:
    """Extraction outcome for one requested item (field names mirror ``schemas.ingest``).

    ``value`` is scaled (whole units of ``unit``); ``raw_value`` is the selected token exactly as
    printed and ``scale_applied`` its multiplier. ``derivation`` is non-empty only for
    ``derived`` items and ``candidates`` (at most five) only for ``ambiguous`` ones. A
    ``not_found`` item has every optional field ``None`` and both lists empty.
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
_TITLE_RE: Final = re.compile(
    r"\b(?:"
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
_USD_RE: Final = re.compile(r"\busd\b|\bus\$")
_CURRENCY_RE: Final = re.compile(r"\b(?:kshs|kes|shs|ksh|usd|us\$)")
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
    marker, or the document's inherited scale."""

    number: int
    lines: tuple[_Line, ...]
    kind: _StatementKind | None
    marker_scale: int | None
    has_usd: bool
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


def _is_encryption_error(exc: BaseException) -> bool:
    """Whether ``exc`` or an exception it wraps is a pdfminer password or encryption error.

    pdfplumber wraps pdfminer errors in ``PdfminerException`` (the original is its argument and
    context), so the chain is walked by class name and message instead of importing pdfminer.
    """
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending and len(seen) < _MAX_EXCEPTION_CHAIN:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        if type(current).__name__ in {"PDFPasswordIncorrect", "PDFEncryptionError"}:
            return True
        message = str(current).lower()
        if "password" in message or "encrypt" in message:
            return True
        for linked in (current.__cause__, current.__context__, *current.args):
            if isinstance(linked, BaseException):
                pending.append(linked)
    return False


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


def _read_pages(pdf_bytes: bytes, max_pages: int) -> list[list[_Word]]:
    """Open the PDF, apply the encryption and page-count guards, and read each page's words.

    The document is closed on every path. Encrypted files are rejected even when they open with
    an empty password, and the page count is checked before any text is extracted.

    Closing never replaces the outcome. pdfplumber's ``close()`` lists the pages again, so a
    document whose pages could not be listed fails to close with that same error; a close failure
    while an error is already being raised is suppressed, and that error propagates. A close
    failure after every page was read is mapped like a failure to open, normally to
    :class:`PdfUnreadable`.
    """
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    except Exception as exc:
        raise _open_failure(exc) from exc
    try:
        try:
            encryption: object = getattr(getattr(pdf, "doc", None), "encryption", None)
            pages: list[_PdfPage] = list(pdf.pages)
        except Exception as exc:
            raise _open_failure(exc) from exc
        if encryption is not None:
            raise PdfEncrypted()
        if len(pages) > max_pages:
            raise TooManyPages(len(pages), max_pages)
        page_words = [_page_words(page, number) for number, page in enumerate(pages, start=1)]
    except BaseException:
        # Closing re-lists the pages and can fail with the very error raised above; that
        # failure must not take the place of the error already propagating.
        with contextlib.suppress(Exception):
            pdf.close()
        raise
    try:
        pdf.close()
    except Exception as exc:
        raise _open_failure(exc) from exc
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
    """Statement kind from the first statement title in the title region (first 8 lines)."""
    for line in lines[:_TITLE_REGION_LINES]:
        match = _TITLE_RE.search(line.text)
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


def _scale_marker(region: Sequence[_Line]) -> int | None:
    """The first scale marker in reading order within the header region, if any."""
    for line in region:
        match = _SCALE_MARKER_RE.search(line.text)
        if match is not None and match.lastgroup is not None:
            return _SCALE_VALUES[match.lastgroup]
    return None


def _analyse_page(page_number: int, words: Sequence[_Word]) -> _Page:
    """Lines, statement kind, scale marker and USD marker of one page."""
    lines = _lines(page_number, words)
    region = _header_region(lines)
    return _Page(
        number=page_number,
        lines=lines,
        kind=_classify_page(lines),
        marker_scale=_scale_marker(region),
        has_usd=any(_USD_RE.search(line.text) is not None for line in region),
    )


# ---------------------------------------------------------------------------------------------
# Column evidence and reading a matched line
# ---------------------------------------------------------------------------------------------


def _overlaps(a_x0: float, a_x1: float, b_x0: float, b_x1: float) -> bool:
    """Whether two x-ranges overlap, allowing the 2-point column tolerance."""
    return a_x0 <= b_x1 + _COLUMN_TOLERANCE and a_x1 >= b_x0 - _COLUMN_TOLERANCE


def _centre(word: _Word) -> float:
    return (word.x0 + word.x1) / 2


def _context_line(page: _Page, index: int) -> _Line | None:
    """The line at ``index`` when it exists and holds only entity, note and unit words."""
    if 0 <= index < len(page.lines) and page.lines[index].is_context:
        return page.lines[index]
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


def _build_header(page: _Page, index: int) -> _Header:
    """Columns of the header row at ``index``: one per period token, plus a Note column per
    note token on the row or its context lines. Entity tokens come from the row itself and the
    context line directly above it."""
    line = page.lines[index]
    above = _context_line(page, index - 1)
    below = _context_line(page, index + 1)
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


def _header_row(page: _Page, index: int) -> _Header | None:
    """Column evidence for the line at ``index``: the nearest column-header row above it on the
    same page. A Group-and-Company line without periods met first yields the entity-only
    header; ``None`` means the page has no header row above the line."""
    for above in range(index - 1, -1, -1):
        candidate = page.lines[above]
        if candidate.is_header:
            return _build_header(page, above)
        if candidate.is_entity_conflict:
            return _ENTITY_ONLY_HEADER
    return None


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


def _money_scale(reading: _Reading) -> int:
    return reading.scale


def _money_value(reading: _Reading, amount: Decimal) -> Decimal | None:
    """A monetary value: the printed amount times the page scale, in ``Decimal``."""
    return amount * Decimal(reading.scale)


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
    """An integral share count in 0..``MAX_SHARE_COUNT``, or ``None`` when the scaled amount is
    fractional, negative or too large."""
    value = amount * Decimal(_share_scale(reading))
    if value != value.to_integral_value() or not 0 <= value <= MAX_SHARE_COUNT:
        return None
    return Decimal(int(value))


_MONEY: Final = _Valuation(scale_of=_money_scale, value_of=_money_value)
_SHARES: Final = _Valuation(scale_of=_share_scale, value_of=_share_value)


def _reading_value(reading: _Reading, valuation: _Valuation) -> Decimal | None:
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


def _match_item(
    document: _Document, spec: _ItemSpec, valuation: _Valuation, *, monetary: bool = True
) -> _Outcome | None:
    """Search an item's pages pattern by pattern; the highest-priority pattern with any match
    decides it through :func:`_resolve`. ``None`` when no pattern matches."""
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
            return _resolve(readings, primary, valuation)
    return None


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
    short-term and one long-term component, are summed into a ``derived`` value. A generic
    "borrowings" line, a lone component, more than two components, repeated or unpaired
    component types, or an unread component line is ambiguous. No section-heading inference.
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
    summable = (
        not has_generic
        and len(components) == 2
        and kinds in _DEBT_PAIRS
        and all(reading.amount is not None for _, reading in components)
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
    parts = tuple(
        DerivationPart(reading.label, reading.page_number, reading.amount * Decimal(reading.scale))
        for _, reading in components
        if reading.amount is not None
    )
    return _Outcome(
        status="derived",
        value=sum((part.value for part in parts), Decimal(0)),
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


def _depreciation_and_amortisation(document: _Document) -> list[_Outcome] | None:
    """D&A from the cash-flow or income-statement pages: a combined "depreciation and
    amortisation" line, otherwise one "depreciation ..." line plus at most one "amortisation
    ..." line. ``None`` when no depreciation is found."""
    combined = _match_item(document, _DA_COMBINED, _MONEY)
    if combined is not None:
        return [combined]
    depreciation = _match_item(document, _DEPRECIATION, _MONEY)
    if depreciation is None:
        return None
    amortisation = _match_item(document, _AMORTISATION, _MONEY)
    return [depreciation] if amortisation is None else [depreciation, amortisation]


def _derive_ebitda(document: _Document) -> _Outcome | None:
    """EBITDA derived as operating profit plus the absolute value of D&A (expense lines may be
    printed in parentheses). A missing part gives ``None`` (not found); an ambiguous part makes
    EBITDA ambiguous, reported with that part's line and candidates but no value."""
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
        value = outcome.value if position == 0 else abs(outcome.value)
        parts.append(DerivationPart(outcome.label, outcome.page_number, value))
    evidence = tuple(candidate for outcome in outcomes for candidate in outcome.evidence)
    return _Outcome(
        status="derived",
        value=sum((part.value for part in parts), Decimal(0)),
        raw_value=None,
        scale=operating.scale,
        page_number=operating.page_number,
        label=None,
        parts=tuple(parts),
        evidence=evidence[:_MAX_CANDIDATES],
    )


def _not_found(name: str, unit: ItemUnit) -> ExtractedLineItem:
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
# Public entry point
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


def extract_line_items(
    pdf_bytes: bytes,
    items: Sequence[str] = LINE_ITEMS,
    *,
    period_end_date: date | None = None,
    max_pages: int = 400,
) -> ExtractionResult:
    """Extract the requested line items from a filing or prospectus PDF.

    Args:
        pdf_bytes: The whole PDF. Size, the ``%PDF-`` magic bytes and concurrency are the
            caller's checks; nothing but these bytes is read.
        items: Names from :data:`LINE_ITEMS`, results in this order (duplicates dropped).
        period_end_date: Selects the period column by its year; without it the latest year is
            read.
        max_pages: Largest page count parsed.

    Returns:
        One :class:`ExtractedLineItem` per requested item. Monetary values are whole KES after
        the page scale, or whole USD when a statement header is in USD (``currency="USD"`` and
        the warning ``non_kes_currency``; never converted). ``shares_outstanding`` is an
        integral count, scaled only by its own unit hint. Item statuses are ``found``,
        ``derived`` (summed from component lines listed in ``derivation``), ``ambiguous``
        (insufficient evidence, with up to five ``candidates``; never to be committed) and
        ``not_found``. Warnings: ``no_text_layer`` (no OCR is attempted), ``scale_not_detected``
        and ``non_kes_currency``.

    Raises:
        ValueError: An unknown item name, no item, or ``max_pages`` below 1.
        PdfEncrypted: The PDF is encrypted, even with an empty user password.
        PdfUnreadable: The bytes cannot be parsed, or a page's text cannot be read.
        TooManyPages: More than ``max_pages`` pages; raised before any text is extracted.
    """
    requested = _requested_items(items)
    if max_pages < 1:
        raise ValueError(f"max_pages must be at least 1, got {max_pages}")
    page_words = _read_pages(pdf_bytes, max_pages)
    page_count = len(page_words)

    if not any(page_words):
        result = ExtractionResult(
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
        _log_result(result)
        return result

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
    result = ExtractionResult(
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
    _log_result(result)
    return result
