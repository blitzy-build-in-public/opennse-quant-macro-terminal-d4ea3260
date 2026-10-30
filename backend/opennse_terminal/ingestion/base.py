"""Shared ingestion framework: the pipeline contract, the run recorder and the run executor.

The source pipelines (the public NSE market-statistics web table, the five CBK statistical
releases and CSV/Excel imports) implement :class:`Pipeline`, and :func:`execute_pipeline` runs
each the same way: fetch -> parse -> validate -> reconcile -> upsert in one data transaction
holding the run's advisory lock, with the run recorded in ``terminal_ingestion_runs`` on a
separate connection. :func:`execute_pipeline` documents the run contract (lifecycle, locking,
committed outcome and cancellation), :class:`RunStatus` the final statuses, and
:func:`classify_exception` the status and ``error_message`` each exception records.

Corporate PDF uploads (``POST /ingest/pdf``) are not pipelines. The filings service
(``opennse_terminal.services.filings_service``) calls
:func:`~opennse_terminal.ingestion.files.pdf_line_items.extract_line_items` and records the
``pdf_filing`` audit row through :class:`RunRecorder` itself, in a data transaction of its own
and without an advisory lock. Of this module, that path uses only :class:`RunRecorder`,
:class:`RunStatus` and :func:`parse_decimal_text`.

Units convention for every record a pipeline writes: ``*_pct`` columns hold percentage points
(``6.80`` means 6.80%), ``*_kes`` columns whole KES (never thousands), volumes and share counts
integers, ``fx_kes_usd`` KES per USD and ``fx_reserves_usd_m`` millions of USD. Values are
``Decimal`` end to end; :func:`parse_decimal_text` never goes through ``float`` and
:func:`to_jsonable` writes decimals into ``details`` as strings.

``opennse_terminal.ingestion.http`` imports this module's exceptions at module level, so this
module imports ``PoliteClient`` and ``HostSpacing`` only for type checking and, at run time,
``PoliteClient`` inside :func:`execute_pipeline`.
"""

import asyncio
import dataclasses
import logging
import math
import re
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from datetime import time as dt_time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, Literal, TypeVar, get_args

from sqlalchemy import String, bindparam, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.orm import aliased

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.calendar import CalendarNotVerified
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.models.ingestion_runs import RUN_STATUSES, RUN_TRIGGERS

if TYPE_CHECKING:
    # Type-only: ingestion.http imports this module at load time (see the module docstring).
    from opennse_terminal.ingestion.http import HostSpacing, PoliteClient

__all__ = [
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
]

logger = logging.getLogger("opennse_terminal.ingestion.base")

RawT = TypeVar("RawT")
RecordT = TypeVar("RecordT")

MAX_INVALID_FRACTION: Final = Decimal("0.10")
"""Largest share of parsed rows that may be rejected before a run is ``layout_changed``."""

REJECTED_SAMPLE_LIMIT: Final = 50
"""Number of rejected rows kept in ``details.rejected``."""

_MAX_ERROR_MESSAGE_CHARS: Final = 2_000
# error_message of a run whose process ended without recording it (RunRecorder.mark_abandoned).
_ABANDONED: Final = "abandoned"


# ---------------------------------------------------------------------------------------------
# Statuses and triggers
# ---------------------------------------------------------------------------------------------


class RunStatus(StrEnum):
    """Status of a ``terminal_ingestion_runs`` row: ``running``, then exactly one final status.

    The final statuses of a run of :func:`execute_pipeline` (:func:`classify_exception` maps each
    exception that ends a run to its status):

    =====================  ========================================================  ============
    Status                 Condition                                                 Data written
    =====================  ========================================================  ============
    ``succeeded``          every parsed row valid                                    yes
    ``partial``            up to 10% of parsed rows rejected; ``details.rejected``   valid rows
                           holds the first 50
    ``no_new_data``        the source has not advanced, or there is nothing new to   no
                           write
    ``layout_changed``     headers, table or file link missing or different, fewer   no
                           rows than ``min_rows``, or more than 10% rejected;
                           ``details.expected`` and ``details.found`` record the
                           comparison
    ``blocked_by_robots``  ``robots.txt`` disallows the URL                          no
    ``ambiguous_session``  the trading session of a snapshot cannot be               no
                           established; the dating evidence goes into ``details``
    ``failed``             a failed fetch (for example a network failure after       no
                           retries), lock held, exchange calendar not verified,
                           cancellation or an unexpected error; or, with
                           ``error_message`` ``"abandoned"``, a run whose process
                           died while ``running``, once a later run of its name
                           holds the lock
    =====================  ========================================================  ============

    A PDF upload's ``pdf_filing`` audit row ends ``succeeded`` or ``failed`` as
    :func:`~opennse_terminal.services.filings_service.ingest_pdf` describes.
    """

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    LAYOUT_CHANGED = "layout_changed"
    BLOCKED_BY_ROBOTS = "blocked_by_robots"
    NO_NEW_DATA = "no_new_data"
    AMBIGUOUS_SESSION = "ambiguous_session"


RunTrigger = Literal["schedule", "cli", "api", "seed"]
"""What started a run: the scheduler process, the CLI, an API request or the seeder."""

# The table's CHECK constraints are rendered from the model's tuples; a drift between them and
# these types would only surface as a constraint violation at run time, so fail at import instead.
if tuple(RunStatus) != RUN_STATUSES:
    raise RuntimeError(
        f"RunStatus {tuple(RunStatus)!r} does not match models.ingestion_runs.RUN_STATUSES "
        f"{RUN_STATUSES!r}"
    )
if get_args(RunTrigger) != RUN_TRIGGERS:
    raise RuntimeError(
        f"RunTrigger {get_args(RunTrigger)!r} does not match models.ingestion_runs.RUN_TRIGGERS "
        f"{RUN_TRIGGERS!r}"
    )


# ---------------------------------------------------------------------------------------------
# Exceptions a pipeline raises to end its run with a specific status
# ---------------------------------------------------------------------------------------------


class IngestionError(Exception):
    """Base class of the exceptions that end a run with a specific status (see
    :func:`classify_exception`)."""


class SourceLayoutChanged(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """The source no longer has the expected layout; the run ends ``layout_changed``.

    Raised before any write when headers, a required table or file link, or a row-count minimum
    do not match. The framework also raises it when more than 10% of the parsed rows are
    rejected, including after the upsert and before the commit when rows the upsert could not
    write tip the count; the transaction then rolls back and nothing is written.
    ``expected`` and ``found`` are recorded as ``details.expected`` and ``details.found``
    (through :func:`to_jsonable`).

    Attributes:
        expected: What the parser expected, for example the normalized header tuple.
        found: What the source actually had.
        message: A one-line description, used as the run's ``error_message``.
    """

    def __init__(self, expected: object, found: object, message: str) -> None:
        self.expected = expected
        self.found = found
        self.message = message
        super().__init__(message)


class RobotsDisallowed(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """``robots.txt`` disallows the URL for the product's user agent; the run ends
    ``blocked_by_robots`` without requesting it.

    Attributes:
        url: The URL that may not be fetched.
        reason: ``"disallowed"``, or ``"robots_unreachable"`` when ``robots.txt`` answered 5xx or
            429 after retries.
    """

    def __init__(self, url: str, reason: str = "disallowed") -> None:
        self.url = url
        self.reason = reason
        super().__init__(f"robots.txt blocks {url} ({reason})")


class FetchFailed(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """Fetching a source failed; the run ends ``failed``.

    Covers network failures after retries, HTTP error statuses, oversize responses, a missing
    browser build, the run time cap and file limits.

    Attributes:
        url: The URL or file path that could not be fetched.
        reason: A short machine-readable reason, for example ``"http_404"``.
    """

    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(f"fetch failed for {url}: {reason}")


class AmbiguousSession(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """The trading session a snapshot belongs to cannot be established; the run ends
    ``ambiguous_session`` with nothing written.

    Attributes:
        evidence: The dating evidence (expected, link and label sessions, continuity result and
            a ``reason`` code), merged into the run's ``details``.
    """

    def __init__(self, evidence: dict[str, Any]) -> None:
        self.evidence = evidence
        reason = evidence.get("reason")
        super().__init__(f"ambiguous session: {reason}" if reason else "ambiguous session")


class NoNewData(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """The source has not advanced, or there is nothing new to write; the run ends
    ``no_new_data`` with nothing written.

    Attributes:
        reason: Why there is nothing to write, used as the run's ``error_message``.
        evidence: Supporting values merged into the run's ``details``.
    """

    def __init__(self, reason: str, evidence: dict[str, Any] | None = None) -> None:
        self.reason = reason
        self.evidence: dict[str, Any] = dict(evidence or {})
        super().__init__(f"no new data: {reason}")


class LockHeld(IngestionError):  # noqa: N818 - names the condition, as in the run status
    """Another run of the same pipeline holds its advisory lock; the run ends ``failed`` with
    ``error_message = "lock held"``.

    Attributes:
        pipeline: The run name whose lock is held.
    """

    def __init__(self, pipeline: str) -> None:
        self.pipeline = pipeline
        super().__init__(f"lock held: another run of {pipeline} holds its advisory lock")


def _truncate(message: str) -> str:
    if len(message) <= _MAX_ERROR_MESSAGE_CHARS:
        return message
    return message[: _MAX_ERROR_MESSAGE_CHARS - 1] + "…"


def classify_exception(exc: BaseException) -> tuple[RunStatus, str]:
    """Map an exception that ended a run to its final status and ``error_message``.

    ==============================  =====================  ======================================
    Exception                       Status                 ``error_message``
    ==============================  =====================  ======================================
    :class:`SourceLayoutChanged`    ``layout_changed``     its ``message``
    :class:`RobotsDisallowed`       ``blocked_by_robots``  ``str(exc)``
    :class:`AmbiguousSession`       ``ambiguous_session``  ``str(exc)``
    :class:`NoNewData`              ``no_new_data``        its ``reason``
    :class:`LockHeld`               ``failed``             ``"lock held"``
    :class:`FetchFailed`            ``failed``             ``str(exc)``
    ``CalendarNotVerified``         ``failed``             its message
    ``asyncio.CancelledError``      ``failed``             ``"cancelled"``
    any other exception             ``failed``             ``"unexpected_error:<ExceptionType>"``
    ==============================  =====================  ======================================

    Any other exception is recorded by its type name only, because its text can carry SQL with
    its bound parameters, driver details or connection details. Messages are truncated to 2,000
    characters.
    """
    if isinstance(exc, SourceLayoutChanged):
        return RunStatus.LAYOUT_CHANGED, _truncate(exc.message)
    if isinstance(exc, RobotsDisallowed):
        return RunStatus.BLOCKED_BY_ROBOTS, _truncate(str(exc))
    if isinstance(exc, AmbiguousSession):
        return RunStatus.AMBIGUOUS_SESSION, _truncate(str(exc))
    if isinstance(exc, NoNewData):
        return RunStatus.NO_NEW_DATA, _truncate(exc.reason)
    if isinstance(exc, LockHeld):
        return RunStatus.FAILED, "lock held"
    if isinstance(exc, (FetchFailed, CalendarNotVerified)):
        return RunStatus.FAILED, _truncate(str(exc))
    if isinstance(exc, asyncio.CancelledError):
        return RunStatus.FAILED, "cancelled"
    return RunStatus.FAILED, _truncate(f"unexpected_error:{type(exc).__name__}")


# ---------------------------------------------------------------------------------------------
# Records, validation outcome, run context and run result
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RejectedRow:
    """A parsed row that failed validation or reconciliation, or that the upsert could not write.

    Attributes:
        index: The row's position in the source, as the pipeline reports it: a 0-based index, or
            a 1-based source row number (the CSV record or worksheet row of tabular imports and
            CBK CSV files, the body row of CBK HTML tables).
        reason: A machine-readable reason code, for example ``invalid_isin``; several codes may
            be joined with ``;``.
        row: The row's raw cells or values; recorded in ``details.rejected`` through
            :func:`to_jsonable`.
    """

    index: int
    reason: str
    row: Mapping[str, object]


@dataclass(slots=True)
class ValidationOutcome(Generic[RecordT]):
    """The result of :meth:`Pipeline.validate` or :meth:`Pipeline.reconcile`: the rows that pass
    and the rows rejected. Every rejection counts toward the run's thresholds."""

    valid: list[RecordT]
    rejected: list[RejectedRow]


@dataclass(slots=True)
class RunContext:
    """What a pipeline sees during its run.

    Attributes:
        run_id: The ``terminal_ingestion_runs.id`` of this run; already committed, so
            ``ingestion_run_id`` foreign keys written in the data transaction are satisfiable.
        pipeline: The run name (``Pipeline.run_name``), which is also the advisory-lock key.
        trigger: What started the run.
        settings: The run's settings.
        conn: The data-transaction connection that holds the advisory lock. Pipelines may read
            through it (for example the ISIN-to-ticker mapping, or earlier runs' ``details``);
            they never begin, commit or roll back on it.
        http: The run's polite HTTP client (spacing, retries, ``robots.txt``). One per run.
        details: Mutable evidence merged into the run's ``details`` whatever the outcome, for
            example ``source_url``, ``unmapped_isins``, ``link_session``, ``bulletins`` or
            ``duplicates``. Values pass through :func:`to_jsonable`.
    """

    run_id: uuid.UUID
    pipeline: str
    trigger: RunTrigger
    settings: TerminalSettings
    conn: AsyncConnection
    http: "PoliteClient"
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RunResult:
    """The outcome of one run, as recorded in ``terminal_ingestion_runs``.

    The CLI maps ``status`` to exit codes: ``succeeded``, ``partial`` and ``no_new_data`` 0,
    ``failed`` 1, ``layout_changed`` 3, ``blocked_by_robots`` 4, ``ambiguous_session`` 5.
    ``details`` is the JSON-ready payload stored on the run row.
    """

    run_id: uuid.UUID
    pipeline: str
    status: RunStatus
    rows_fetched: int
    rows_written: int
    rows_rejected: int
    error_message: str | None
    details: dict[str, Any]


# ---------------------------------------------------------------------------------------------
# The pipeline contract
# ---------------------------------------------------------------------------------------------


class Pipeline(ABC, Generic[RawT, RecordT]):
    """One public source loaded into ``terminal_`` tables: fetch -> parse -> validate ->
    reconcile -> upsert, run by :func:`execute_pipeline` inside one transaction.

    ``RawT`` is the fetched content (HTML, CSV or PDF bytes, a rendered page snapshot) and
    ``RecordT`` the typed row record holding ``Decimal``, ``int`` and ``date`` values.

    A pipeline instance is single-use: one instance per run. It may keep per-run evidence found
    while parsing (for example the date in a page's price-list link) for :meth:`reconcile`; the
    module-level ``parse_*`` functions of each source module stay pure. Subclasses take their
    options as keyword arguments, ``__init__(self, settings, **options)``, and call
    ``super().__init__(settings)``.

    Attributes:
        name: The registry name, for example ``"cbk_rates_csv"``.
        min_rows: A parsed row count below this ends the run ``layout_changed``.
        source_url: The primary URL or file path, recorded on the run row; a pipeline may update
            it during the run (for example to a discovered file link).
        settings: The run's settings.
    """

    name: ClassVar[str]
    min_rows: ClassVar[int] = 0
    source_url: str | None = None

    def __init__(self, settings: TerminalSettings) -> None:
        self.settings = settings

    @property
    def run_name(self) -> str:
        """The run row's ``pipeline`` value and the advisory-lock key (``terminal:<run_name>``).

        Defaults to :attr:`name`; the tabular importer returns ``csv_import:<kind>``.
        """
        return self.name

    @abstractmethod
    async def fetch(self, ctx: RunContext) -> RawT:
        """Fetch the raw content, through ``ctx.http`` for network sources.

        Raises :class:`FetchFailed`, :class:`RobotsDisallowed` or :class:`SourceLayoutChanged`
        (for example when a required file link is missing).
        """

    @abstractmethod
    def parse(self, raw: RawT) -> list[RecordT]:
        """Turn raw content into typed records, without I/O.

        Runs in a worker thread (``asyncio.to_thread``) once :meth:`fetch` has returned, so heavy
        decoding never blocks the event loop; no other stage of the run executes meanwhile. It
        must not use the event loop, ``ctx.conn`` or ``ctx.http``; it may keep per-run evidence
        on the instance, including in the ``ctx.details`` mapping it kept from :meth:`fetch`. A
        cancellation that arrives while it runs takes effect once it returns, and its result is
        then discarded.

        Raises :class:`SourceLayoutChanged` when expected headers or tables are missing or
        different. Rows whose cells do not convert are returned for :meth:`validate` to reject.
        """

    @abstractmethod
    def validate(self, records: Sequence[RecordT]) -> ValidationOutcome[RecordT]:
        """Check each record against the source's range and format rules, without I/O.

        Runs in a worker thread once :meth:`parse` has returned, under the same rules as
        :meth:`parse`. The framework applies the shared thresholds: up to 10% rejected gives
        ``partial``, more gives ``layout_changed``.
        """

    async def reconcile(
        self, ctx: RunContext, records: list[RecordT]
    ) -> ValidationOutcome[RecordT]:
        """Database-aware checks inside the run's transaction, on ``ctx.conn``.

        The place for checks that need stored data: mapping source identifiers to tickers,
        stale-source guards, session dating, unknown tickers and currency conflicts. It receives
        the records :meth:`validate` accepted; its rejections count toward the same thresholds.
        It may raise :class:`SourceLayoutChanged`, :class:`AmbiguousSession` or
        :class:`NoNewData`. The default accepts every record.
        """
        return ValidationOutcome(valid=records, rejected=[])

    @abstractmethod
    async def upsert(
        self, conn: AsyncConnection, records: Sequence[RecordT], run_id: uuid.UUID
    ) -> int:
        """Write the accepted records through ``opennse_terminal.db.upsert`` on ``conn`` and
        return the number of rows written. Never commits; the framework does.

        A record found unwritable only while writing (for example a conflict a concurrent writer
        created after :meth:`reconcile`) is left out of that number and reported through
        :meth:`write_rejections`, never dropped silently. Not called when no record was
        accepted.
        """

    def write_rejections(self) -> Sequence[RejectedRow]:
        """The rows :meth:`upsert` found it could not write, for example a row whose key a
        concurrent writer stored in another currency after :meth:`reconcile` checked it.

        Read once the upsert stage is over, before the commit. These rows count toward the run's
        thresholds like :meth:`validate`'s and :meth:`reconcile`'s rejections: up to 10% of the
        parsed rows rejected in all gives ``partial``, more gives ``layout_changed`` and rolls
        the upsert back. The default reports none.
        """
        return ()


# ---------------------------------------------------------------------------------------------
# Shared helpers (pure)
# ---------------------------------------------------------------------------------------------


def evaluate_thresholds(parsed_count: int, rejected_count: int, min_rows: int) -> RunStatus:
    """Apply the shared run thresholds to a run's row counts.

    Returns ``LAYOUT_CHANGED`` when fewer than ``min_rows`` rows were parsed or more than
    :data:`MAX_INVALID_FRACTION` (10%) of them were rejected, ``PARTIAL`` when some (up to 10%)
    were rejected, and ``SUCCEEDED`` otherwise, including an empty parse with ``min_rows == 0``.

    Raises:
        ValueError: A count is negative, or more rows were rejected than parsed.
    """
    if parsed_count < 0 or rejected_count < 0 or min_rows < 0:
        raise ValueError("row counts and min_rows must not be negative")
    if rejected_count > parsed_count:
        raise ValueError(f"rejected rows ({rejected_count}) exceed parsed rows ({parsed_count})")
    if parsed_count < min_rows:
        return RunStatus.LAYOUT_CHANGED
    # Exact: Decimal("0.10") * int involves no binary rounding.
    if rejected_count > MAX_INVALID_FRACTION * parsed_count:
        return RunStatus.LAYOUT_CHANGED
    if rejected_count > 0:
        return RunStatus.PARTIAL
    return RunStatus.SUCCEEDED


# Zero-width space and byte-order mark: not whitespace to str.split(), but common in scraped
# headers and exported CSV files.
_INVISIBLE = str.maketrans("", "", "\u200b\ufeff")


def normalize_header(text: str) -> str:
    """Normalize a header cell for comparison: trimmed, lowercased, with every run of whitespace
    (NBSP and line breaks included) collapsed to one space."""
    return " ".join(text.translate(_INVISIBLE).split()).lower()


_NULL_DECIMAL_TOKENS: Final = frozenset({"", "-", "—", "–", "n/a"})
_DECIMAL_TEXT_RE: Final = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
# NBSP, narrow NBSP and thin space appear as digit-group separators in scraped and PDF text.
_DECIMAL_SPACES = str.maketrans("", "", "\u00a0\u202f\u2009,")


def parse_decimal_text(text: str | None, *, parentheses_negative: bool = True) -> Decimal | None:
    """Parse a scraped, CSV or PDF number into an exact ``Decimal``, never through ``float``.

    Whitespace (NBSP included) and ``,`` thousands separators are removed; the Unicode minus
    sign counts as ``-``; a leading ``+`` or ``-`` is accepted; ``(1,234.5)`` reads as
    ``Decimal("-1234.5")`` when ``parentheses_negative`` is true.

    The result is exact at any length and exponent, whatever the active ``decimal`` context: a
    parenthesized non-zero number equals its leading-minus form digit for digit. A zero is
    always a positive zero with the printed scale, so ``(0)``, ``-0`` and ``+0`` read as
    ``Decimal("0")`` and ``-0.00`` as ``Decimal("0.00")``. Range checks are the caller's.

    Returns ``None`` for ``None``, an empty cell, ``-``, ``—``, ``–``, ``n/a`` (any case) and
    anything that is not a finite decimal number (``NaN``, ``Infinity``, stray letters). The
    caller decides what ``None`` means: skip, reject, or (in PDF statements) a dash as zero.
    """
    if text is None:
        return None
    cleaned = "".join(text.translate(_DECIMAL_SPACES).split()).replace("\u2212", "-")
    if cleaned.lower() in _NULL_DECIMAL_TOKENS:
        return None
    negative = False
    if parentheses_negative and len(cleaned) > 2 and cleaned[0] == "(" and cleaned[-1] == ")":
        cleaned = cleaned[1:-1]
        if cleaned[0] in "+-":
            return None
        negative = True
    if _DECIMAL_TEXT_RE.fullmatch(cleaned) is None:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    # copy_abs and copy_negate are exact: unlike abs() and unary minus they never round to the
    # context's precision or overflow its exponent range. A zero, "-0" included, is made positive.
    if value.is_zero():
        return value.copy_abs()
    return value.copy_negate() if negative else value


def dedupe_by_key(
    records: Sequence[RecordT], key: Callable[[RecordT], Hashable]
) -> tuple[list[RecordT], int]:
    """Collapse records that share a key, keeping the **last** occurrence of each key in the
    position where the key was first seen; return the records and the number dropped.

    One ``INSERT ... ON CONFLICT DO UPDATE`` statement cannot touch the same row twice, so every
    upsert batch is deduplicated on its conflict key first (bond reopenings, overlapping weekly
    bulletins, repeated file rows).
    """
    latest: dict[Hashable, RecordT] = {}
    for record in records:
        # Re-assigning an existing key keeps its original insertion position.
        latest[key(record)] = record
    return list(latest.values()), len(records) - len(latest)


def to_jsonable(value: object) -> Any:
    """Convert a ``details`` payload into JSON-safe values for a JSONB column.

    ``Decimal`` becomes its string (never a float), ``date``, ``datetime`` and ``time`` ISO
    strings, ``uuid.UUID`` its string, an ``Enum`` its value, a dataclass instance a dict of its
    fields, mappings dicts with string keys, and tuples, lists and other sequences lists (sets
    sorted for a stable order). A ``float`` becomes its shortest round-trip string, or ``None``
    when not finite. ``None``, ``bool``, ``int`` and ``str`` pass through; anything else becomes
    ``str(value)``.
    """
    if isinstance(value, Enum):
        return to_jsonable(value.value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else None
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else None
    if isinstance(value, (date, dt_time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_jsonable(getattr(value, item.name)) for item in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {_json_key(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted((to_jsonable(item) for item in value), key=str)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [to_jsonable(item) for item in value]
    return str(value)


def _json_key(key: object) -> str:
    converted = to_jsonable(key)
    return converted if isinstance(converted, str) else str(converted)


# ---------------------------------------------------------------------------------------------
# Run recording
# ---------------------------------------------------------------------------------------------


class RunRecorder:
    """Writes ``terminal_ingestion_runs`` rows, each in its own short transaction.

    Every write checks out a pooled connection of its own, separate from any data transaction,
    and commits immediately. A run row therefore stays visible when the run's data transaction
    rolls back, the ``terminal_daily_prices.ingestion_run_id`` foreign key is satisfiable inside
    that transaction, and run-record writes never release or extend the run's advisory lock.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def start(
        self,
        *,
        pipeline: str,
        trigger: RunTrigger,
        source_url: str | None = None,
        run_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Insert a ``running`` row (zero counts, empty ``details``, ``started_at`` now in UTC)
        and return its id.

        Args:
            run_id: The row's id, returned as given; a new ``uuid4`` when omitted. A caller that
                generates it knows the id even when this call raises after its insert has
                committed (on connection release, for example), so it can still finish that row.

        Raises:
            ValueError: ``trigger`` is not one of the run triggers.
        """
        if trigger not in RUN_TRIGGERS:
            raise ValueError(f"unknown run trigger {trigger!r}; expected one of {RUN_TRIGGERS}")
        if run_id is None:
            run_id = uuid.uuid4()
        # The mapped class, not TerminalIngestionRun.__table__ (typed FromClause, which insert()
        # and update() reject under mypy --strict), targets the same table as a Core statement.
        statement = insert(TerminalIngestionRun).values(
            id=run_id,
            pipeline=pipeline,
            trigger=trigger,
            source_url=source_url,
            started_at=datetime.now(UTC),
            status=RunStatus.RUNNING.value,
            rows_fetched=0,
            rows_written=0,
            rows_rejected=0,
            details={},
        )
        async with self._engine.begin() as conn:
            await conn.execute(statement)
        return run_id

    async def finish(
        self,
        run_id: uuid.UUID,
        *,
        status: RunStatus,
        rows_fetched: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        error_message: str | None = None,
        details: Mapping[str, Any] | None = None,
        source_url: str | None = None,
    ) -> None:
        """Record a run's final status, ``finished_at`` (now, UTC), counts, ``error_message`` and
        ``details`` (through :func:`to_jsonable`); ``source_url`` replaces the stored URL when
        given.

        Raises:
            ValueError: ``status`` is ``running`` or not a run status.
            LookupError: No run row has ``run_id``.
        """
        final_status = RunStatus(status)
        if final_status is RunStatus.RUNNING:
            raise ValueError("a finished run needs a final status, not 'running'")
        values: dict[str, Any] = {
            "status": final_status.value,
            "finished_at": datetime.now(UTC),
            "rows_fetched": rows_fetched,
            "rows_written": rows_written,
            "rows_rejected": rows_rejected,
            "error_message": error_message,
            "details": to_jsonable(dict(details or {})),
        }
        if source_url is not None:
            values["source_url"] = source_url
        statement = (
            update(TerminalIngestionRun).where(TerminalIngestionRun.id == run_id).values(**values)
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(statement)
        if result.rowcount == 0:
            raise LookupError(f"no ingestion run with id {run_id}")

    async def mark_abandoned(self, pipeline: str, *, current_run_id: uuid.UUID) -> int:
        """Finish the earlier runs of ``pipeline`` that their process left ``running``; return
        how many rows were changed.

        One conditional ``UPDATE`` in its own short transaction: every row of ``pipeline`` whose
        status is still ``running``, other than ``current_run_id``, and whose ``started_at`` is
        before that run's becomes ``failed`` with ``finished_at`` now (UTC), ``error_message``
        ``"abandoned"`` and ``details`` ``{"abandoned_by": "<current_run_id>"}``. Its counts are
        kept. A process stopped by SIGKILL, an out-of-memory kill or a host crash records nothing
        itself, so this is how such a row reaches a final status.

        Call it only while holding the pipeline's advisory lock: no other run of ``pipeline`` is
        then inside its locked phase. A run that started earlier but has not yet tried the lock
        may be marked too; its own final update, which is unconditional, then replaces the mark,
        so a live run's recorded outcome always stands.
        """
        # An alias, so the subquery reads the current run's row and never correlates with the
        # row being updated.
        current = aliased(TerminalIngestionRun, name="current_run")
        current_started_at = (
            select(current.started_at).where(current.id == current_run_id).scalar_subquery()
        )
        statement = (
            update(TerminalIngestionRun)
            .where(
                TerminalIngestionRun.pipeline == pipeline,
                TerminalIngestionRun.status == RunStatus.RUNNING.value,
                TerminalIngestionRun.id != current_run_id,
                TerminalIngestionRun.started_at < current_started_at,
            )
            .values(
                status=RunStatus.FAILED.value,
                finished_at=datetime.now(UTC),
                error_message=_ABANDONED,
                details={"abandoned_by": str(current_run_id)},
            )
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(statement)
        abandoned: int = result.rowcount
        return abandoned

    async def record(
        self,
        *,
        pipeline: str,
        trigger: RunTrigger,
        status: RunStatus,
        source_url: str | None = None,
        error_message: str | None = None,
        details: Mapping[str, Any] | None = None,
        rows_fetched: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
    ) -> uuid.UUID:
        """Write a complete run row in one call (:meth:`start` then :meth:`finish`) and return
        its id, for a run whose outcome is known before it is recorded."""
        run_id = await self.start(pipeline=pipeline, trigger=trigger, source_url=source_url)
        await self.finish(
            run_id,
            status=status,
            rows_fetched=rows_fetched,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            error_message=error_message,
            details=details,
        )
        return run_id


# ---------------------------------------------------------------------------------------------
# Run execution
# ---------------------------------------------------------------------------------------------

_SET_IDLE_TIMEOUT: Final = text("SET LOCAL idle_in_transaction_session_timeout = '30min'")
# Typed bind parameter: the asyncpg dialect then renders $1::VARCHAR, so the concatenation never
# depends on PostgreSQL's resolution of an untyped parameter.
_TRY_XACT_LOCK: Final = text(
    "SELECT pg_try_advisory_xact_lock(hashtext('terminal:' || :pipeline))"
).bindparams(bindparam("pipeline", type_=String()))

_NANOSECONDS_PER_SECOND: Final = Decimal(1_000_000_000)
_DURATION_QUANTUM: Final = Decimal("0.001")


@dataclass(slots=True)
class _RunProgress:
    """Counts kept while a run proceeds, so a failure still reports what was fetched, and the
    confirmed outcome of its data transaction.

    ``written`` and ``committed`` (the ``succeeded`` or ``partial`` status committed) are set
    only once the commit has returned; from then on the data is in the database, whatever a later
    cleanup step or cancellation does.
    """

    fetched: int = 0
    written: int = 0
    rejected: list[RejectedRow] = field(default_factory=list)
    committed: RunStatus | None = None


_ArgT = TypeVar("_ArgT")
_ResultT = TypeVar("_ResultT")


async def _wait_through_cancellation(task: asyncio.Future[Any]) -> bool:
    """Wait until ``task`` is done, however often the calling task is cancelled meanwhile, and
    return whether it was asked to cancel while waiting.

    ``asyncio.wait`` neither cancels ``task`` nor raises its exception, so ``task`` always runs
    to completion and its outcome stays on it for the caller to read; the caller's reference
    keeps it alive. A cancellation received while waiting is absorbed and reported through the
    return value: the caller deals with the finished task, then raises
    ``asyncio.CancelledError`` itself, so the calling task still ends cancelled.
    """
    cancel_requested = False
    while not task.done():
        try:
            await asyncio.wait((task,))
        except asyncio.CancelledError:
            cancel_requested = True
    return cancel_requested


def _task_error(task: asyncio.Future[Any]) -> BaseException | None:
    """The exception a finished task ended with (``CancelledError`` when it was itself
    cancelled), or ``None``; reading it marks it retrieved, so asyncio never reports it."""
    if task.cancelled():
        return asyncio.CancelledError()
    return task.exception()


async def _run_stage(stage: Callable[[_ArgT], _ResultT], argument: _ArgT) -> _ResultT:
    """Run a synchronous pipeline stage (``parse`` or ``validate``) in a worker thread and
    return its result or raise its exception, leaving the event loop free meanwhile.

    A thread cannot be interrupted, so a cancellation received while the stage runs waits for it
    to return; the stage therefore never outlives the run, nor touches the run's ``details``
    while they are being recorded. Its result or exception is then discarded and
    ``asyncio.CancelledError`` raised, which rolls the data transaction back.
    """
    task = asyncio.create_task(asyncio.to_thread(stage, argument))
    if await _wait_through_cancellation(task):
        _task_error(task)
        raise asyncio.CancelledError
    return task.result()


def _exception_evidence(exc: BaseException) -> dict[str, Any]:
    """The ``details`` entries an exception contributes to its run."""
    if isinstance(exc, SourceLayoutChanged):
        return {"expected": exc.expected, "found": exc.found}
    if isinstance(exc, (AmbiguousSession, NoNewData)):
        return dict(exc.evidence)
    if isinstance(exc, RobotsDisallowed):
        return {"robots": {"url": exc.url, "reason": exc.reason}}
    if isinstance(exc, FetchFailed):
        return {"fetch_failure": {"url": exc.url, "reason": exc.reason}}
    if isinstance(exc, LockHeld):
        return {"lock_key": f"terminal:{exc.pipeline}"}
    if isinstance(exc, CalendarNotVerified):
        return {"calendar": {"year": exc.year, "verified_years": sorted(exc.verified_years)}}
    return {"exception": type(exc).__name__}


def _run_details(
    context_details: Mapping[str, Any],
    evidence: Mapping[str, Any],
    progress: _RunProgress,
    started_ns: int,
) -> dict[str, Any]:
    """Assemble the JSON-ready ``details`` of a finished run.

    ``started_ns`` is the run's :func:`time.monotonic_ns` reading. ``duration_seconds`` is the
    elapsed time in seconds, computed from integer nanoseconds without ``float`` and rounded
    ``ROUND_HALF_UP`` to 3 decimal places (milliseconds).
    """
    elapsed_ns = time.monotonic_ns() - started_ns
    elapsed = (Decimal(elapsed_ns) / _NANOSECONDS_PER_SECOND).quantize(
        _DURATION_QUANTUM, rounding=ROUND_HALF_UP
    )
    payload: dict[str, Any] = {**context_details, **evidence, "duration_seconds": elapsed}
    if progress.rejected:
        payload["rejected"] = [
            {"index": row.index, "reason": row.reason, "row": row.row}
            for row in progress.rejected[:REJECTED_SAMPLE_LIMIT]
        ]
    details: dict[str, Any] = to_jsonable(payload)
    return details


def _threshold_breach(parsed: int, rejected: int, min_rows: int) -> SourceLayoutChanged:
    """The :class:`SourceLayoutChanged` of a run whose counts :func:`evaluate_thresholds` finds
    ``layout_changed``: ``expected`` holds the thresholds and ``found`` the counts."""
    if parsed < min_rows:
        message = f"{parsed} rows parsed, below the minimum of {min_rows}"
    else:
        message = (
            f"{rejected} of {parsed} rows rejected, more than "
            f"{MAX_INVALID_FRACTION:.0%} of the rows parsed"
        )
    return SourceLayoutChanged(
        expected={"min_rows": min_rows, "max_invalid_fraction": str(MAX_INVALID_FRACTION)},
        found={"parsed": parsed, "rejected": rejected},
        message=message,
    )


async def _mark_abandoned_runs(engine: AsyncEngine, run_name: str, run_id: uuid.UUID) -> None:
    """Finish the earlier runs of ``run_name`` left ``running`` by a process that ended without
    recording them (:meth:`RunRecorder.mark_abandoned`).

    Called once the run holds its advisory lock; the update uses a run-record connection of its
    own, never the data connection. Best effort: an error is logged at WARNING by its type only
    and the run goes on; a cancellation propagates like any other. Marked rows are counted in a
    WARNING.
    """
    try:
        abandoned = await RunRecorder(engine).mark_abandoned(run_name, current_run_id=run_id)
    except Exception as exc:
        logger.warning(
            "ingestion run %s (%s) could not finish earlier runs left running: %s",
            run_id,
            run_name,
            type(exc).__name__,
        )
        return
    if abandoned:
        logger.warning(
            "ingestion run %s (%s) recorded %d earlier run(s) left running as failed (abandoned)",
            run_id,
            run_name,
            abandoned,
        )


async def _run_in_transaction(
    pipeline: Pipeline[Any, Any],
    engine: AsyncEngine,
    *,
    run_id: uuid.UUID,
    trigger: RunTrigger,
    http: "PoliteClient",
    details: dict[str, Any],
    progress: _RunProgress,
) -> RunStatus:
    """Run fetch -> parse -> validate -> reconcile -> upsert in one data transaction holding the
    run's transaction-scoped advisory lock, and return ``succeeded`` or ``partial``.

    Once the lock is held, earlier runs of the same run name still ``running`` are recorded
    ``failed`` ("abandoned") by :func:`_mark_abandoned_runs`, before the fetch.

    The thresholds of :func:`evaluate_thresholds` are applied to the validation and
    reconciliation rejections before the upsert, then, when :meth:`Pipeline.write_rejections`
    reports rows the upsert could not write, again with those added, before the commit; a breach
    either time raises :class:`SourceLayoutChanged`, which rolls the transaction back.

    ``parse`` and ``validate`` run in a worker thread (:func:`_run_stage`). Any exception before
    the commit rolls the transaction back. The commit is explicit and runs to completion even if
    the run is cancelled meanwhile, so its outcome is never in doubt: once it has returned,
    ``progress`` records the committed status and the rows written, and a cancellation received
    during it is re-raised only then. PostgreSQL releases the lock with the transaction.
    """
    run_name = pipeline.run_name
    cancel_requested = False
    async with engine.connect() as conn, conn.begin() as transaction:
        await conn.execute(_SET_IDLE_TIMEOUT)
        acquired = (await conn.execute(_TRY_XACT_LOCK, {"pipeline": run_name})).scalar_one()
        if not acquired:
            logger.warning("advisory lock held for %s", run_name)
            raise LockHeld(run_name)
        await _mark_abandoned_runs(engine, run_name, run_id)

        ctx = RunContext(
            run_id=run_id,
            pipeline=run_name,
            trigger=trigger,
            settings=pipeline.settings,
            conn=conn,
            http=http,
            details=details,
        )
        # Each stage's input is released once the next stage holds what it needs, so a large run
        # keeps one generation of its data alive rather than the raw content and every record
        # list until the write ends.
        raw = await pipeline.fetch(ctx)
        records = await _run_stage(pipeline.parse, raw)
        del raw
        parsed = len(records)
        progress.fetched = parsed
        validated = await _run_stage(pipeline.validate, records)
        del records
        progress.rejected.extend(validated.rejected)
        reconciled = await pipeline.reconcile(ctx, validated.valid)
        del validated
        progress.rejected.extend(reconciled.rejected)

        status = evaluate_thresholds(parsed, len(progress.rejected), pipeline.min_rows)
        if status is RunStatus.LAYOUT_CHANGED:
            raise _threshold_breach(parsed, len(progress.rejected), pipeline.min_rows)
        written = await pipeline.upsert(conn, reconciled.valid, run_id) if reconciled.valid else 0
        # Rows the upsert could not write count like the earlier rejections; a breach they cause
        # rolls back the rows already written.
        write_rejected = pipeline.write_rejections()
        if write_rejected:
            progress.rejected.extend(write_rejected)
            status = evaluate_thresholds(parsed, len(progress.rejected), pipeline.min_rows)
            if status is RunStatus.LAYOUT_CHANGED:
                raise _threshold_breach(parsed, len(progress.rejected), pipeline.min_rows)
        # Committed explicitly, in a task of its own that a cancellation cannot abandon midway, so
        # the outcome is known before anything else happens; leaving the block then changes
        # nothing.
        commit = asyncio.create_task(transaction.commit())
        cancel_requested = await _wait_through_cancellation(commit)
        commit_error = _task_error(commit)
        if commit_error is not None:
            if cancel_requested:
                raise asyncio.CancelledError
            raise commit_error
        # Counted only once the commit has succeeded.
        progress.written = written
        progress.committed = status
    if cancel_requested:
        raise asyncio.CancelledError
    return status


# The FetchFailed reason codes the ingestion modules raise, besides "http_<status>" and
# "robots_http_<status>". A reason reaches a log line only as one of these codes, so no URL,
# file path or file name can.
_FETCH_FAILED_CODES: Final = frozenset(
    {
        # ingestion/http.py
        "invalid_url",
        "unsupported_url",
        "url_has_userinfo",
        "unsupported_content_encoding",
        "invalid_content_encoding",
        "too_many_redirects",
        "invalid_redirect",
        "response_too_large",
        "origin_not_approved",
        # ingestion/files/tabular_import.py
        "too_many_rows",
        "file_too_large",
        "file_not_found",
        "not_a_file",
        "unreadable_file",
        "xlsx_too_many_members",
        "xlsx_expanded_too_large",
        "xlsx_unsupported_member",
        "xlsx_worksheet_too_large",
        "xlsx_parts_too_large",
        "xlsx_size_mismatch",
        # ingestion/nse/market_statistics.py
        "browser_not_installed",
        "browser_launch_failed",
        "navigation_failed",
        "off_origin_redirect",
        "sector_render_timeout",
        "run_timeout",
        "browser_error",
    }
)
_HTTP_STATUS_REASON: Final = re.compile(r"(?:robots_)?http_[0-9]{3}")
# The documented values of RobotsDisallowed.reason.
_ROBOTS_REASONS: Final = frozenset({"disallowed", "robots_unreachable"})


def _fetch_failed_code(exc: FetchFailed) -> str:
    """``fetch_failed``, then ``:<code>`` when the reason's text before its first ``:`` is a
    known code, then ``:<type>`` of the exception it was raised from, if any."""
    parts = ["fetch_failed"]
    code = exc.reason.partition(":")[0] if isinstance(exc.reason, str) else ""
    if code in _FETCH_FAILED_CODES or _HTTP_STATUS_REASON.fullmatch(code):
        parts.append(code)
    if exc.__cause__ is not None:
        parts.append(type(exc.__cause__).__name__)
    return ":".join(parts)


def _log_code(exc: BaseException) -> str:
    """Name an exception in the run's log lines without its text.

    Exception messages, reasons and tracebacks can carry input file paths, file names, URLs and
    SQL statements with their bound parameters, so a log line gets only fixed codes and type
    names. A :class:`FetchFailed` reason is logged only as a code in ``_FETCH_FAILED_CODES`` or
    one matching ``_HTTP_STATUS_REASON``; any other text in it is never logged, and an exception
    type is taken from the exception's ``__cause__``, never from the reason.

    ==============================  ===================================================
    Exception                       Logged as
    ==============================  ===================================================
    ``asyncio.CancelledError``      ``cancelled``
    :class:`LockHeld`               ``lock held``
    :class:`FetchFailed`            ``fetch_failed``, then ``:<code>`` when its
                                    ``reason``, up to any ``:``, is such a code, then
                                    ``:<type>`` of its ``__cause__``
                                    (:func:`_fetch_failed_code`), for example
                                    ``fetch_failed:http_404`` or
                                    ``fetch_failed:ConnectTimeout``
    ``CalendarNotVerified``         ``calendar_not_verified:<year>``
    :class:`RobotsDisallowed`       its ``reason`` when in ``_ROBOTS_REASONS``, else
                                    its type name
    :class:`AmbiguousSession`       its type name; the dating evidence is recorded in
                                    the run's ``details``, never logged
    :class:`SourceLayoutChanged`    ``layout_changed``
    :class:`NoNewData`              ``no_new_data``
    any other exception             its type name, for example ``IntegrityError``
    ==============================  ===================================================
    """
    if isinstance(exc, asyncio.CancelledError):
        return "cancelled"
    if isinstance(exc, LockHeld):
        return "lock held"
    if isinstance(exc, FetchFailed):
        return _fetch_failed_code(exc)
    if isinstance(exc, CalendarNotVerified):
        return f"calendar_not_verified:{int(exc.year)}"
    if isinstance(exc, RobotsDisallowed):
        return exc.reason if exc.reason in _ROBOTS_REASONS else type(exc).__name__
    if isinstance(exc, AmbiguousSession):
        return type(exc).__name__
    if isinstance(exc, SourceLayoutChanged):
        return "layout_changed"
    if isinstance(exc, NoNewData):
        return "no_new_data"
    return type(exc).__name__


def _log_run_end(result: RunResult, exc: BaseException | None) -> None:
    """Log a finished run at the level its status calls for, then its summary at INFO.

    Every line carries the run id and run name. A failure, robots block or ambiguous session is
    named by :func:`_log_code`, never by the exception's text or traceback. A layout change is
    logged with ``details.expected`` and ``details.found`` (the expected and found headers, row
    counts, sheet names or links) and without its message, which can name the input file; the
    message is the run row's ``error_message``.
    """
    status = result.status
    reason = "unknown" if exc is None else _log_code(exc)
    if status is RunStatus.LAYOUT_CHANGED:
        logger.error(
            "ingestion run %s (%s) layout_changed: expected=%r found=%r",
            result.run_id,
            result.pipeline,
            result.details.get("expected"),
            result.details.get("found"),
        )
    elif status is RunStatus.FAILED:
        logger.error("ingestion run %s (%s) failed: %s", result.run_id, result.pipeline, reason)
    elif status in (RunStatus.BLOCKED_BY_ROBOTS, RunStatus.AMBIGUOUS_SESSION):
        logger.warning(
            "ingestion run %s (%s) %s: %s",
            result.run_id,
            result.pipeline,
            status.value,
            reason,
        )
    logger.info(
        "ingestion run %s finished: pipeline=%s status=%s fetched=%d written=%d rejected=%d "
        "duration=%ss",
        result.run_id,
        result.pipeline,
        status.value,
        result.rows_fetched,
        result.rows_written,
        result.rows_rejected,
        result.details.get("duration_seconds"),
    )


async def execute_pipeline(
    pipeline: Pipeline[Any, Any],
    engine: AsyncEngine,
    *,
    trigger: RunTrigger,
    http: "PoliteClient | None" = None,
    spacing: "HostSpacing | None" = None,
) -> RunResult:
    """Run one pipeline end to end and record it in ``terminal_ingestion_runs``.

    1. A ``running`` row is committed through :class:`RunRecorder` in its own short transaction,
       so the run stays visible whatever happens to the data.
    2. A :class:`~opennse_terminal.ingestion.http.PoliteClient` is opened for the run, unless
       ``http`` is injected (tests); an injected client is neither opened nor closed here, and
       keeps its own spacing. The opened client spaces its requests on ``spacing`` when given
       (a :class:`~opennse_terminal.ingestion.http.HostSpacing` the caller passes to every run
       of a sequence, so requests to one host stay ``scraper_min_interval_seconds`` apart
       across the runs), else on a spacing of its own; its cookies and ``robots.txt`` cache are
       the run's own either way.
    3. One dedicated connection opens the data transaction before fetching, sets the 30-minute
       ``idle_in_transaction_session_timeout`` and takes the transaction-scoped advisory lock
       ``pg_try_advisory_xact_lock(hashtext('terminal:' || run_name))``, never at session level
       nor twice on one connection; a held lock raises :class:`LockHeld`. With the lock held,
       earlier rows of ``run_name`` still ``running``, whose process died without recording
       them (for example on SIGKILL), are recorded ``failed`` ("abandoned") on a separate
       connection (:meth:`RunRecorder.mark_abandoned`; best effort, an error is logged at
       WARNING by type and the run goes on). Fetch, parse, validate, reconcile, the thresholds
       of :func:`evaluate_thresholds` and the upsert run inside it, in that order, then the
       thresholds again when :meth:`Pipeline.write_rejections` reports rows the upsert could not
       write; ``parse`` and ``validate`` run in a worker thread, so decoding a large file never
       blocks the event loop. It commits on success and rolls back on any exception. PostgreSQL
       releases the lock when the transaction ends: on commit, on rollback after an exception,
       and on cancellation or process death when the connection closes.
    4. The run row is updated on a separate connection, so run-record writes never release or
       extend the lock, with the final status (:class:`RunStatus`; :func:`classify_exception`
       maps the exception that ended the run), counts, ``error_message`` and ``details``:
       ``ctx.details``, the exception's evidence, ``duration_seconds`` (seconds to 3 decimal
       places, measured with :func:`time.monotonic_ns`) and the first 50 rejected rows.

    Only ``succeeded`` and ``partial`` runs write data (``partial`` writes the valid rows only).
    Pipeline exceptions become statuses and are not raised.

    The recorded status is the data transaction's confirmed outcome. Once the commit has
    returned, an exception or cancellation while the run's HTTP client is closed or its
    connection released keeps the committed status and ``rows_written``, adds
    ``details.after_commit_error`` (the exception's type name) and is logged at WARNING; the
    exception itself is not raised, a pending cancellation is (below).

    ``asyncio.CancelledError`` before the commit rolls the data back and records ``failed`` with
    ``"cancelled"``; a ``parse`` or ``validate`` stage already running in its worker thread is
    first allowed to return. The ``running`` insert, the commit and the final update each run in
    a task of their own to completion, however often the run is cancelled meanwhile, so a
    started run is never left ``running`` and nothing outlives the call. A cancellation received
    at any point is re-raised once the run row is final.

    A cancellation is pending while the calling task's :meth:`asyncio.Task.cancelling` count
    stays above its value when the call began; nothing here calls ``uncancel()``. It is therefore
    re-raised even when another exception replaced its ``CancelledError`` while the run unwound,
    for example from releasing the run's connection or closing an HTTP client that raises: before
    the commit, the run is still recorded ``failed`` with ``"cancelled"``,
    ``details.error_during_cancellation`` names the replacing exception's type, and a WARNING
    names it by its type or reason code, never by its text. The ``CancelledError`` raised leaves
    the count as it is, so an ``asyncio.timeout()`` around the call raises ``TimeoutError``.

    An error writing the run row propagates, unless a cancellation is pending: the error is then
    logged at ERROR (its type only) and the cancellation re-raised. ``KeyboardInterrupt`` and
    ``SystemExit`` propagate untouched, leaving the row ``running``.
    """
    recorder = RunRecorder(engine)
    run_name = pipeline.run_name
    # A cancellation request stays counted on the task until uncancel(), which nothing here
    # calls: a rise in the count is a cancellation of this run, whether it was absorbed while a
    # step ran to completion or its CancelledError was replaced by a cleanup error.
    task = asyncio.current_task()
    cancels_at_entry = 0 if task is None else task.cancelling()

    def cancel_pending() -> bool:
        return task is not None and task.cancelling() > cancels_at_entry

    # The running row is inserted in a task of its own that a cancellation cannot abandon
    # midway: once the insert has committed, the row is finished below whatever happens.
    start = asyncio.create_task(
        recorder.start(pipeline=run_name, trigger=trigger, source_url=pipeline.source_url)
    )
    cancelled_while_starting = await _wait_through_cancellation(start) or cancel_pending()
    start_error = _task_error(start)
    if start_error is not None:
        if cancelled_while_starting:
            if not isinstance(start_error, asyncio.CancelledError):
                logger.error(
                    "could not record the start of an ingestion run of %s: %s",
                    run_name,
                    _log_code(start_error),
                )
            raise asyncio.CancelledError
        raise start_error
    run_id = start.result()
    started_ns = time.monotonic_ns()
    logger.info("ingestion run %s started: pipeline=%s trigger=%s", run_id, run_name, trigger)

    context_details: dict[str, Any] = {}
    progress = _RunProgress()
    evidence: dict[str, Any] = {}
    error_message: str | None = None
    failure: BaseException | None = None
    cancellation: asyncio.CancelledError | None = None
    try:
        if cancelled_while_starting:
            raise asyncio.CancelledError
        async with AsyncExitStack() as stack:
            client = http
            if client is None:
                from opennse_terminal.ingestion.http import PoliteClient

                client = PoliteClient(pipeline.settings, spacing=spacing)
                await stack.enter_async_context(client)
            status = await _run_in_transaction(
                pipeline,
                engine,
                run_id=run_id,
                trigger=trigger,
                http=client,
                details=context_details,
                progress=progress,
            )
    except asyncio.CancelledError as exc:
        # Recorded below, then re-raised once the run row is finished.
        failure = cancellation = exc
        status, error_message = RunStatus.FAILED, "cancelled"
    except Exception as exc:
        failure = exc
        status, error_message = classify_exception(exc)
        evidence = _exception_evidence(exc)
    if cancellation is None and cancel_pending():
        # The run was cancelled, but the CancelledError was absorbed while the commit finished
        # or replaced by an error raised while the run unwound; it is re-raised below.
        cancellation = asyncio.CancelledError()
        if progress.committed is None and failure is not None:
            # Nothing committed: the cancellation, not the replacing error, ended the run.
            logger.warning(
                "ingestion run %s (%s) was cancelled; unwinding it raised %s",
                run_id,
                run_name,
                _log_code(failure),
            )
            status, error_message = RunStatus.FAILED, "cancelled"
            evidence = {"error_during_cancellation": type(failure).__name__}
            failure = cancellation
    if progress.committed is not None and failure is not None:
        # The data had committed when closing the run's HTTP client or connection raised, or
        # the run was cancelled: the committed outcome stands, and the interruption is noted.
        status, error_message = progress.committed, None
        evidence = {"after_commit_error": type(failure).__name__}
        logger.warning(
            "ingestion run %s (%s) was interrupted after its data committed: %s",
            run_id,
            run_name,
            _log_code(failure),
        )

    result = RunResult(
        run_id=run_id,
        pipeline=run_name,
        status=status,
        rows_fetched=progress.fetched,
        rows_written=progress.written,
        rows_rejected=len(progress.rejected),
        error_message=error_message,
        details=_run_details(context_details, evidence, progress, started_ns),
    )
    # Like the insert, the final update runs to completion however often the task is cancelled
    # meanwhile, and its error is observed here rather than left to the event loop.
    audit = asyncio.create_task(_finish(recorder, result, pipeline.source_url))
    if await _wait_through_cancellation(audit) and cancellation is None:
        cancellation = asyncio.CancelledError()
    audit_error = _task_error(audit)
    if audit_error is not None:
        if cancellation is None:
            raise audit_error
        logger.error(
            "could not record ingestion run %s (%s) as %s: %s",
            run_id,
            run_name,
            status.value,
            _log_code(audit_error),
        )
    _log_run_end(result, failure)
    if cancellation is not None:
        raise cancellation
    return result


async def _finish(recorder: RunRecorder, result: RunResult, source_url: str | None) -> None:
    await recorder.finish(
        result.run_id,
        status=result.status,
        rows_fetched=result.rows_fetched,
        rows_written=result.rows_written,
        rows_rejected=result.rows_rejected,
        error_message=result.error_message,
        details=result.details,
        source_url=source_url,
    )
