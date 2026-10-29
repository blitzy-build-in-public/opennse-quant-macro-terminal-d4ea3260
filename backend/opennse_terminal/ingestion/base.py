"""Shared ingestion framework: the pipeline contract, the run recorder and the run executor.

Every source module (the public NSE market-statistics web table, the CBK statistical releases,
PDF filings and CSV/Excel files) implements :class:`Pipeline`, and :func:`execute_pipeline` runs
it the same way: fetch -> parse -> validate -> reconcile -> upsert -> record.

Run lifecycle (one call of :func:`execute_pipeline`):

1. :class:`RunRecorder` inserts a ``terminal_ingestion_runs`` row with status ``running`` in its
   own short transaction, so the run stays visible whatever happens to the data.
2. One dedicated connection opens the data transaction *before* fetching, sets
   ``idle_in_transaction_session_timeout = '30min'`` and takes the transaction-scoped advisory
   lock ``pg_try_advisory_xact_lock(hashtext('terminal:' || <run name>))``. A held lock ends the
   run ``failed`` with "lock held". PostgreSQL releases the lock when the transaction ends: on
   commit, on rollback after an exception, and on cancellation or process death when the
   connection closes. The lock is never taken at session level nor twice on one connection.
3. Fetching, parsing, validation, reconciliation and the upsert all happen inside that
   transaction; any exception rolls everything back.
4. The run row is updated with its final status, counts, ``error_message`` and ``details`` on a
   separate connection, so run-record writes never release or extend the lock.

Final statuses:

=====================  ==========================================================  ============
Status                 Condition                                                   Data written
=====================  ==========================================================  ============
``succeeded``          all parsed rows valid                                       yes
``partial``            up to 10% of parsed rows rejected; ``details.rejected``     valid rows
                       holds the first 50
``no_new_data``        the source has not advanced (:class:`NoNewData`)            no
``layout_changed``     headers, table or file link missing or different, fewer     no
                       rows than ``min_rows``, or more than 10% rejected
                       (:class:`SourceLayoutChanged`; ``details.expected`` and
                       ``details.found`` record the comparison)
``blocked_by_robots``  ``robots.txt`` disallows the URL (:class:`RobotsDisallowed`)  no
``ambiguous_session``  the trading session of a snapshot cannot be established     no
                       (:class:`AmbiguousSession`; the evidence goes into
                       ``details``)
``failed``             network failure after retries (:class:`FetchFailed`), lock  no
                       held (:class:`LockHeld`), exchange calendar not verified,
                       a rejected PDF upload, cancellation or an unexpected error
=====================  ==========================================================  ============

Units convention for every record a pipeline writes: ``*_pct`` columns hold percentage points
(``6.80`` means 6.80%), ``*_kes`` columns whole KES (never thousands), volumes and share counts
integers, ``fx_kes_usd`` KES per USD and ``fx_reserves_usd_m`` millions of USD. Values are
``Decimal`` end to end; :func:`parse_decimal_text` never goes through ``float`` and
:func:`to_jsonable` writes decimals into ``details`` as strings.

``opennse_terminal.ingestion.http`` imports this module's exceptions at module level, so this
module imports ``PoliteClient`` only for type checking and, at run time, inside
:func:`execute_pipeline`.
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

from sqlalchemy import String, bindparam, insert, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.calendar import CalendarNotVerified
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.models.ingestion_runs import RUN_STATUSES, RUN_TRIGGERS

if TYPE_CHECKING:
    # The ignore covers a tree in which ingestion/http.py is not present. With the module
    # present the ignore is unused (unused-ignore keeps strict mode quiet) and mypy checks the
    # import in full.
    from opennse_terminal.ingestion.http import (  # type: ignore[import-not-found, unused-ignore]
        PoliteClient,
    )

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


# ---------------------------------------------------------------------------------------------
# Statuses and triggers
# ---------------------------------------------------------------------------------------------


class RunStatus(StrEnum):
    """Status of a ``terminal_ingestion_runs`` row: ``running``, then exactly one final status."""

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
    do not match. ``expected`` and ``found`` are recorded as ``details.expected`` and
    ``details.found`` (through :func:`to_jsonable`).

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
        reason: ``"disallowed"``, or ``"robots_unreachable"`` when ``robots.txt`` answered 5xx.
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

    ==============================  =====================  ===================================
    Exception                       Status                 ``error_message``
    ==============================  =====================  ===================================
    :class:`SourceLayoutChanged`    ``layout_changed``     its ``message``
    :class:`RobotsDisallowed`       ``blocked_by_robots``  ``str(exc)``
    :class:`AmbiguousSession`       ``ambiguous_session``  ``str(exc)``
    :class:`NoNewData`              ``no_new_data``        its ``reason``
    :class:`LockHeld`               ``failed``             ``"lock held"``
    :class:`FetchFailed`            ``failed``             ``str(exc)``
    ``CalendarNotVerified``         ``failed``             its message
    ``asyncio.CancelledError``      ``failed``             ``"cancelled"``
    any other exception             ``failed``             ``"<ExceptionType>: <exc>"``
    ==============================  =====================  ===================================

    Messages are truncated to 2,000 characters.
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
    return RunStatus.FAILED, _truncate(f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------------------------
# Records, validation outcome, run context and run result
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RejectedRow:
    """A parsed row that failed validation or reconciliation.

    Attributes:
        index: The row's position in the source (0-based index, or the file's line number for
            tabular imports), as the pipeline reports it.
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

        Raises :class:`SourceLayoutChanged` when expected headers or tables are missing or
        different. Rows whose cells do not convert are returned for :meth:`validate` to reject.
        """

    @abstractmethod
    def validate(self, records: Sequence[RecordT]) -> ValidationOutcome[RecordT]:
        """Check each record against the source's range and format rules, without I/O.

        The framework applies the shared thresholds: up to 10% rejected gives ``partial``,
        more gives ``layout_changed``.
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

        Not called when no record was accepted.
        """


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
    # copy_negate is exact: unlike unary minus it never rounds to the context's precision or
    # overflows its exponent range. A zero is left as is, so "(0)" stays a positive zero.
    return value.copy_negate() if negative and not value.is_zero() else value


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
        self, *, pipeline: str, trigger: RunTrigger, source_url: str | None = None
    ) -> uuid.UUID:
        """Insert a ``running`` row (zero counts, empty ``details``, ``started_at`` now in UTC)
        and return its id.

        Raises:
            ValueError: ``trigger`` is not one of the run triggers.
        """
        if trigger not in RUN_TRIGGERS:
            raise ValueError(f"unknown run trigger {trigger!r}; expected one of {RUN_TRIGGERS}")
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
        its id; for audit rows such as a PDF upload's ``pdf_filing`` run."""
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
    """Counts kept while a run proceeds, so a failure still reports what was fetched."""

    fetched: int = 0
    written: int = 0
    rejected: list[RejectedRow] = field(default_factory=list)


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

    The transaction commits when the block exits cleanly and rolls back on any exception;
    either way PostgreSQL releases the lock with it.
    """
    run_name = pipeline.run_name
    async with engine.connect() as conn, conn.begin():
        await conn.execute(_SET_IDLE_TIMEOUT)
        acquired = (await conn.execute(_TRY_XACT_LOCK, {"pipeline": run_name})).scalar_one()
        if not acquired:
            logger.warning("advisory lock held for %s", run_name)
            raise LockHeld(run_name)

        ctx = RunContext(
            run_id=run_id,
            pipeline=run_name,
            trigger=trigger,
            settings=pipeline.settings,
            conn=conn,
            http=http,
            details=details,
        )
        raw = await pipeline.fetch(ctx)
        records = pipeline.parse(raw)
        progress.fetched = len(records)
        validated = pipeline.validate(records)
        progress.rejected.extend(validated.rejected)
        reconciled = await pipeline.reconcile(ctx, validated.valid)
        progress.rejected.extend(reconciled.rejected)

        status = evaluate_thresholds(len(records), len(progress.rejected), pipeline.min_rows)
        if status is RunStatus.LAYOUT_CHANGED:
            if len(records) < pipeline.min_rows:
                message = f"{len(records)} rows parsed, below the minimum of {pipeline.min_rows}"
            else:
                message = (
                    f"{len(progress.rejected)} of {len(records)} rows rejected, more than "
                    f"{MAX_INVALID_FRACTION:.0%} of the rows parsed"
                )
            raise SourceLayoutChanged(
                expected={
                    "min_rows": pipeline.min_rows,
                    "max_invalid_fraction": str(MAX_INVALID_FRACTION),
                },
                found={"parsed": len(records), "rejected": len(progress.rejected)},
                message=message,
            )
        written = await pipeline.upsert(conn, reconciled.valid, run_id) if reconciled.valid else 0
    # Counted only once the commit has succeeded.
    progress.written = written
    return status


def _log_run_end(result: RunResult, exc: BaseException | None) -> None:
    """Log a finished run at the level its status calls for, then its summary at INFO."""
    status = result.status
    if status is RunStatus.LAYOUT_CHANGED:
        logger.error(
            "ingestion run %s (%s) layout_changed: %s; expected=%r found=%r",
            result.run_id,
            result.pipeline,
            result.error_message,
            result.details.get("expected"),
            result.details.get("found"),
        )
    elif status is RunStatus.FAILED:
        unexpected = exc is not None and not isinstance(
            exc, (IngestionError, CalendarNotVerified, asyncio.CancelledError)
        )
        logger.error(
            "ingestion run %s (%s) failed: %s",
            result.run_id,
            result.pipeline,
            result.error_message,
            exc_info=exc if unexpected else None,
        )
    elif status in (RunStatus.BLOCKED_BY_ROBOTS, RunStatus.AMBIGUOUS_SESSION):
        logger.warning(
            "ingestion run %s (%s) %s: %s",
            result.run_id,
            result.pipeline,
            status.value,
            result.error_message,
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
) -> RunResult:
    """Run one pipeline end to end and record it in ``terminal_ingestion_runs``.

    1. A ``running`` row is committed through :class:`RunRecorder`.
    2. A :class:`~opennse_terminal.ingestion.http.PoliteClient` is opened for the run, unless
       ``http`` is injected (tests); an injected client is neither opened nor closed here.
    3. One dedicated connection opens the data transaction before fetching, sets the 30-minute
       ``idle_in_transaction_session_timeout`` and takes
       ``pg_try_advisory_xact_lock(hashtext('terminal:' || run_name))``; a held lock raises
       :class:`LockHeld`. Fetch, parse, validate, reconcile, the thresholds of
       :func:`evaluate_thresholds` and the upsert run inside it. It commits on success and rolls
       back on any exception, and PostgreSQL releases the lock either way.
    4. The run row is updated with the final status (see :func:`classify_exception`), counts,
       ``error_message`` and ``details``: ``ctx.details``, the exception's evidence,
       ``duration_seconds`` (seconds to 3 decimal places, measured with
       :func:`time.monotonic_ns`) and the first 50 rejected rows.

    Only ``succeeded`` and ``partial`` runs write data (``partial`` writes the valid rows only).
    Pipeline exceptions become statuses and are not raised. ``asyncio.CancelledError`` rolls the
    data back, records ``failed`` with ``"cancelled"`` and is re-raised; ``KeyboardInterrupt``
    and ``SystemExit`` propagate untouched. Errors writing the run row itself propagate.
    """
    recorder = RunRecorder(engine)
    run_name = pipeline.run_name
    run_id = await recorder.start(
        pipeline=run_name, trigger=trigger, source_url=pipeline.source_url
    )
    started_ns = time.monotonic_ns()
    logger.info("ingestion run %s started: pipeline=%s trigger=%s", run_id, run_name, trigger)

    context_details: dict[str, Any] = {}
    progress = _RunProgress()
    evidence: dict[str, Any] = {}
    error_message: str | None = None
    failure: BaseException | None = None
    try:
        async with AsyncExitStack() as stack:
            client = http
            if client is None:
                from opennse_terminal.ingestion.http import PoliteClient

                client = PoliteClient(pipeline.settings)
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
        # The data transaction has rolled back on the way out; record the run even if this task
        # is cancelled again while waiting, then let the cancellation continue.
        cancelled = RunResult(
            run_id=run_id,
            pipeline=run_name,
            status=RunStatus.FAILED,
            rows_fetched=progress.fetched,
            rows_written=0,
            rows_rejected=len(progress.rejected),
            error_message="cancelled",
            details=_run_details(context_details, {}, progress, started_ns),
        )
        try:
            await asyncio.shield(_finish(recorder, cancelled, pipeline.source_url))
        except Exception:
            logger.exception("could not record cancelled ingestion run %s", run_id)
        _log_run_end(cancelled, exc)
        raise
    except Exception as exc:
        failure = exc
        status, error_message = classify_exception(exc)
        evidence = _exception_evidence(exc)

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
    await _finish(recorder, result, pipeline.source_url)
    _log_run_end(result, failure)
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
