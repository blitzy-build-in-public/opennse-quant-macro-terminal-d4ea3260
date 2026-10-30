"""PDF filing extraction, persistence and commit: the service behind ``POST /ingest/pdf``.

The route (``api.routes.ingest``) has already bounded the request and the file size, checked the
``%PDF-`` magic bytes, taken a parse slot and applied ``require_ingest_permission``.
:func:`ingest_pdf` owns everything after that:

1. A ``terminal_ingestion_runs`` row (``pipeline='pdf_filing'``, ``trigger='api'``) is started on
   its own connection, so it survives the rollback of a rejected request. A request whose row
   cannot be started is logged once and answered with 500 ``internal_error``, or the cancellation
   re-raised when the request or the insert was cancelled; nothing else runs and no filing or
   fundamentals row is written. The row's id is generated before its insert, so a row that a
   failed insert committed anyway is still finished ``failed``, best effort.
2. Pre-parse checks, whose read transaction is committed before the pdfplumber parse runs in a
   worker thread, so the request holds no transaction or lock while parsing; then the commit
   checks.
3. One transaction on the request session upserts ``terminal_corporate_filings`` on
   ``file_sha256`` and, with ``commit``, merges the ``found`` and ``derived`` items into
   ``terminal_fundamentals``. Any error rolls both back, so a rejected request writes neither.
4. The run row is finished ``succeeded``, or ``failed`` with the error code as ``error_message``.

Errors, each a coded body ``{"detail": {"code", "message", "field"}}`` with no extraction:

======  ======================================  ==============================================
Status  Code                                    Condition
======  ======================================  ==============================================
422     ``commit_requires_ticker_and_period``   ``commit`` without ``ticker`` or
                                                ``period_end_date`` (checked before parsing)
404     ``unknown_ticker``                      ``ticker`` is not in ``terminal_equities``,
                                                with or without ``commit``
422     ``pdf_encrypted``                       the PDF is encrypted
422     ``pdf_unreadable``                      the PDF cannot be parsed
422     ``pdf_parse_timeout``                   the parse did not finish within its wall-clock
                                                limit, 20 s plus 0.4 s per page of
                                                ``TERMINAL_PDF_MAX_PAGES`` (180 s at 400)
422     ``too_many_pages``                      more than ``TERMINAL_PDF_MAX_PAGES`` pages
422     ``commit_requires_kes``                 ``commit`` on a statement not in KES
409     ``currency_conflict``                   ``commit`` into a fundamentals row whose
                                                currency is not KES
500     ``internal_error``                      a server fault, such as a database error; its
                                                text is never returned or logged
======  ======================================  ==============================================

Units: monetary items are whole KES after the statement scale, or whole USD when the statement is
presented in USD (never converted; such a filing can be previewed but never committed); share
counts are integers. Values are ``Decimal`` throughout and are rounded once, to 2 places
``ROUND_HALF_UP``, for the response and the stored columns.

Idempotency: a re-upload of the same bytes updates the same filing row (same ``filing_id``) with
the new extraction, and a repeated commit re-merges the same values. The raw PDF bytes are never
persisted or logged; only their SHA-256, size and page count are kept.

This module never imports ``opennse_terminal.api``, and it is the only service that imports the
pdfplumber-based extractor.
"""

import asyncio
import contextlib
import hashlib
import logging
import threading
import uuid
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, TypeGuard, TypeVar, overload

import anyio
from fastapi import HTTPException, status
from sqlalchemy import ColumnElement, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
from starlette.concurrency import run_in_threadpool

from opennse_terminal.config import DEFAULT_CURRENCY, TerminalSettings
from opennse_terminal.db.upsert import upsert_fundamentals, upsert_rows
from opennse_terminal.ingestion.base import RunRecorder, RunStatus
from opennse_terminal.ingestion.files.pdf_line_items import (
    ITEM_COLUMNS,
    LINE_ITEMS,
    ExtractedLineItem,
    ExtractionResult,
    PdfEncrypted,
    PdfParseTimeout,
    PdfUnreadable,
    TooManyPages,
    extract_line_items,
)
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
)
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.schemas.common import ErrorDetail
from opennse_terminal.schemas.ingest import (
    Candidate,
    DerivationEntry,
    DocumentType,
    ExtractedItem,
    IngestPdfResponse,
    LineItemName,
    PeriodType,
)

__all__ = ["PDF_FILING_PIPELINE", "PdfIngestForm", "ingest_pdf"]

logger = logging.getLogger("opennse_terminal.services.filings_service")

PDF_FILING_PIPELINE: Final = "pdf_filing"
"""``pipeline`` of the audit run row written for every request this service handles; it is also
the ``source_by_field`` label of every fundamentals column a commit writes."""

_FILE_NAME_MAX_LENGTH: Final = 255
_COMMITTABLE_STATUSES: Final = frozenset({"found", "derived"})
_SHARES_UNIT: Final = "shares"
_SHA_LOG_PREFIX: Final = 12

_T = TypeVar("_T")


@dataclass(frozen=True)
class PdfIngestForm:
    """The multipart form fields of ``POST /ingest/pdf``, built by the route.

    ``ticker`` is normalized on construction: surrounding whitespace is stripped and it is
    upper-cased, and an empty value becomes ``None``. Unlisted IPO issuers send ``company_name``
    and no ticker. ``items`` defaults to all six line items.
    """

    document_type: DocumentType = "annual_report"
    ticker: str | None = None
    company_name: str | None = None
    period_end_date: date | None = None
    period_type: PeriodType = "FY"
    items: tuple[LineItemName, ...] = LINE_ITEMS
    commit: bool = False

    def __post_init__(self) -> None:
        normalized = self.ticker.strip().upper() if self.ticker is not None else ""
        # A frozen dataclass can only set its own field through object.__setattr__.
        object.__setattr__(self, "ticker", normalized or None)


def _coded_error(
    status_code: int, code: str, message: str, field: str | None = None
) -> HTTPException:
    """Build the coded error ``{"detail": {"code", "message", "field"}}`` for ``status_code``.

    The body never carries the extraction: results are returned only with HTTP 200.
    """
    return HTTPException(
        status_code, detail=ErrorDetail(code=code, message=message, field=field).model_dump()
    )


def _internal_error() -> HTTPException:
    """The coded 500 ``internal_error`` that answers a server fault.

    Its message is fixed and names nothing of the fault: an exception's text can carry SQL with
    its bound parameters (the file name, the form fields, the extraction).
    """
    return _coded_error(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        "internal_error",
        "The PDF could not be ingested because of an internal error.",
        None,
    )


def _error_code(exc: HTTPException) -> str:
    """The ``code`` of a coded ``HTTPException``, or ``http_<status>`` for any other one."""
    detail = exc.detail
    if isinstance(detail, Mapping):
        code = detail.get("code")
        if isinstance(code, str):
            return code
    return f"http_{exc.status_code}"


def _audit_engine(session: AsyncSession) -> AsyncEngine:
    """The engine behind ``session``, on which run rows are written outside its transaction.

    A session bound to an ``AsyncEngine`` (the standalone provider) yields that engine; one bound
    to an ``AsyncConnection`` (a test or host session joining an outer transaction) yields the
    connection's engine. A session configured through ``binds`` or a custom ``get_bind`` is
    resolved through its sync session for ``TerminalIngestionRun``'s ``Mapper`` and table, as an
    ORM statement on it would be, and the engine found is wrapped in an ``AsyncEngine``.
    """
    bind = getattr(session, "bind", None)
    if isinstance(bind, AsyncEngine):
        return bind
    if isinstance(bind, AsyncConnection):
        return bind.engine
    # Engine.engine is the engine itself and Connection.engine its engine.
    return AsyncEngine(
        session.sync_session.get_bind(
            TerminalIngestionRun.__mapper__, clause=TerminalIngestionRun.__table__
        ).engine
    )


async def _run_to_completion(
    awaitable: Coroutine[Any, Any, _T],
) -> tuple[asyncio.Task[_T], bool]:
    """Run ``awaitable`` as its own task until it is done, however often the caller is cancelled
    meanwhile, and return the finished task with whether a cancellation was requested.

    Used for the transaction commits and the run-row writes, so a cancelled request never leaves a
    commit's outcome unknown or its run row ``running``; :func:`_extract` waits for its parse
    thread in the same way once it has cancelled the parse. The task's result or exception stays
    on it for the caller to read.
    """
    task = asyncio.create_task(awaitable)
    cancel_requested = False
    while not task.done():
        try:
            await asyncio.wait((task,))
        except asyncio.CancelledError:
            cancel_requested = True
    return task, cancel_requested


async def _start_run(
    session: AsyncSession, details: Mapping[str, Any]
) -> tuple[RunRecorder, uuid.UUID, bool]:
    """Insert the request's ``running`` run row; return its recorder, its id and whether the
    caller was cancelled while the row was written.

    The id is generated here and the insert runs to completion in a task of its own, so a row
    the insert has committed is never abandoned: after a cancellation the caller finishes it
    before honouring the cancellation, and an insert that fails, which may have committed its
    row first (on connection release, for example), has that row finished here.

    A failed start is logged once, never by the fault's text: an insert that itself ended
    cancelled at WARNING as ``cancelled``, and a fault (the audit engine cannot be resolved, or
    the insert fails) at ERROR as ``unexpected_error:<Type>``. The row is then finished
    ``failed`` with that code and ``details``, best effort: a missing row (an insert that never
    committed) is expected and not logged, any other failure to finish it is logged at ERROR by
    its type, and neither is raised. Without an audit engine there is no recorder, so nothing is
    finished. The cancellation is then re-raised when the insert ended cancelled or the caller
    was cancelled during the insert or the finish; otherwise the coded 500 ``internal_error`` is
    raised.
    """
    try:
        recorder = RunRecorder(_audit_engine(session))
    except Exception as exc:
        logger.error(
            "PDF ingestion run could not be started: %s", f"unexpected_error:{type(exc).__name__}"
        )
        raise _internal_error() from None
    run_id = uuid.uuid4()
    task, cancel_requested = await _run_to_completion(
        recorder.start(pipeline=PDF_FILING_PIPELINE, trigger="api", source_url=None, run_id=run_id)
    )
    error = task.exception() if not task.cancelled() else asyncio.CancelledError()
    if error is None:
        return recorder, task.result(), cancel_requested
    insert_cancelled = isinstance(error, asyncio.CancelledError)
    if insert_cancelled:
        # A cancellation, not a fault: logged by its code, and it stands.
        error_message = "cancelled"
        logger.warning("PDF ingestion run could not be started: %s", error_message)
    else:
        error_message = f"unexpected_error:{type(error).__name__}"
        logger.error("PDF ingestion run could not be started: %s", error_message)
    cancel_requested |= await _finish_run(
        recorder,
        run_id,
        run_status=RunStatus.FAILED,
        rows_fetched=0,
        rows_written=0,
        error_message=error_message,
        details=details,
        row_may_be_missing=True,
    )
    if insert_cancelled or cancel_requested:
        raise asyncio.CancelledError
    raise _internal_error() from None


async def _finish_run(
    recorder: RunRecorder,
    run_id: uuid.UUID,
    *,
    run_status: RunStatus,
    rows_fetched: int,
    rows_written: int,
    error_message: str | None,
    details: Mapping[str, Any],
    row_may_be_missing: bool = False,
) -> bool:
    """Finish the run row, logging (never raising) a failure to do so; return whether the
    caller was cancelled meanwhile.

    With ``row_may_be_missing``, for a row whose insert failed and so may never have committed,
    finding no row with ``run_id`` (``LookupError``) is an expected outcome and is not logged.
    """
    task, cancel_requested = await _run_to_completion(
        recorder.finish(
            run_id,
            status=run_status,
            rows_fetched=rows_fetched,
            rows_written=rows_written,
            error_message=error_message,
            details=details,
        )
    )
    error = task.exception() if not task.cancelled() else asyncio.CancelledError()
    if error is not None and not (row_may_be_missing and isinstance(error, LookupError)):
        logger.error(
            "PDF ingestion run %s: recording status %s failed: %s",
            run_id,
            run_status.value,
            type(error).__name__,
        )
    return cancel_requested


async def _record_failure(
    session: AsyncSession,
    recorder: RunRecorder,
    run_id: uuid.UUID,
    *,
    error_message: str,
    rows_fetched: int,
    details: Mapping[str, Any],
) -> None:
    """Roll the request transaction back and finish the run ``failed``.

    Neither step raises: the caller then raises the request's own outcome (the rejection, the
    cancellation or the coded 500), so a failure here is only logged.
    """
    task, _ = await _run_to_completion(session.rollback())
    error = task.exception() if not task.cancelled() else asyncio.CancelledError()
    if error is not None:
        logger.warning("PDF ingestion run %s: rollback failed: %s", run_id, type(error).__name__)
    await _finish_run(
        recorder,
        run_id,
        run_status=RunStatus.FAILED,
        rows_fetched=rows_fetched,
        rows_written=0,
        error_message=error_message,
        details=details,
    )


# ---------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------


async def _check_preconditions(session: AsyncSession, form: PdfIngestForm) -> None:
    """The checks made before parsing: a commit needs a ticker and a period, and a ticker must
    name a known equity even for a preview, because the filing row references it."""
    if form.commit and (form.ticker is None or form.period_end_date is None):
        missing = "ticker" if form.ticker is None else "period_end_date"
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "commit_requires_ticker_and_period",
            "commit=true requires both ticker and period_end_date; "
            f"{missing} is missing. Send commit=false to preview the extraction.",
            missing,
        )
    if form.ticker is None:
        return
    known = await session.scalar(
        select(TerminalEquity.ticker).where(TerminalEquity.ticker == form.ticker)
    )
    if known is None:
        raise _coded_error(
            status.HTTP_404_NOT_FOUND,
            "unknown_ticker",
            f"Ticker {form.ticker!r} is not a known equity.",
            "ticker",
        )


async def _extract(
    upload_bytes: bytes, form: PdfIngestForm, settings: TerminalSettings
) -> ExtractionResult:
    """Parse the PDF in a worker thread, mapping the parser's rejections to coded 422s.

    The parse gets a ``threading.Event`` as its ``cancel``. The first cancellation that reaches
    the wait sets it, which kills the parser process: a task cancellation (``Task.cancel()``,
    which the route sends when its client disconnects, as does a server shutdown) or a cancelled
    anyio cancel scope around the request (a host's task group or timeout). The wait then goes on,
    shielded and absorbing further cancellations, until the worker thread has ended, within
    moments of the kill, so a cancelled request never outlives its parse thread and the route's
    parse slot is freed promptly. The thread's outcome is retrieved and the cancellation
    re-raised; the parse's outcome is never mapped then.

    Otherwise the result is returned, or the parser's rejection raised as 422 ``pdf_encrypted``,
    ``pdf_unreadable``, ``pdf_parse_timeout`` (the parse reached its wall-clock limit, 20 s plus
    0.4 s per page of ``pdf_max_pages``) or ``too_many_pages``, each on the field ``file``.
    """
    cancel = threading.Event()
    task = asyncio.create_task(
        run_in_threadpool(
            extract_line_items,
            upload_bytes,
            list(form.items),
            period_end_date=form.period_end_date,
            max_pages=settings.pdf_max_pages,
            cancel=cancel,
        )
    )
    try:
        # Unshielded, so the first cancellation of either kind lands here.
        await asyncio.wait((task,))
    except asyncio.CancelledError:
        cancel.set()
        # The shield keeps a cancelled anyio scope from re-delivering its cancellation on every
        # pass of the wait, which would spin the event loop until the thread has ended.
        with anyio.CancelScope(shield=True):
            while not task.done():
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait((task,))
        # The cancellation supersedes the parse's outcome, which is still retrieved so that
        # asyncio never reports it as an unhandled exception.
        if not task.cancelled():
            task.exception()
        raise
    try:
        return task.result()
    except PdfEncrypted as exc:
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "pdf_encrypted",
            "The PDF is encrypted; upload an unencrypted copy.",
            "file",
        ) from exc
    except PdfUnreadable as exc:
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "pdf_unreadable",
            "The PDF could not be parsed.",
            "file",
        ) from exc
    except PdfParseTimeout as exc:
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "pdf_parse_timeout",
            f"The PDF could not be parsed within {exc.time_limit:g} seconds.",
            "file",
        ) from exc
    except TooManyPages as exc:
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "too_many_pages",
            f"The PDF has {exc.page_count} pages; at most {exc.max_pages} can be parsed.",
            "file",
        ) from exc


def _fundamentals_key(form: PdfIngestForm) -> tuple[ColumnElement[bool], ...]:
    """``WHERE`` terms selecting the fundamentals row of the form's ticker and period."""
    return (
        TerminalFundamental.ticker == form.ticker,
        TerminalFundamental.period_end_date == form.period_end_date,
        TerminalFundamental.period_type == form.period_type,
    )


async def _check_commit_currency(
    session: AsyncSession, form: PdfIngestForm, result: ExtractionResult
) -> None:
    """The checks a commit makes after parsing: the statement is in KES, and so is any existing
    fundamentals row for the key, so values are never merged across currencies."""
    if result.currency != DEFAULT_CURRENCY:
        raise _coded_error(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "commit_requires_kes",
            f"The statement is presented in {result.currency}; only {DEFAULT_CURRENCY} "
            "statements can be committed. Send commit=false to preview it.",
            "commit",
        )
    stored = await session.scalar(
        select(TerminalFundamental.currency).where(*_fundamentals_key(form))
    )
    if stored is not None and stored != DEFAULT_CURRENCY:
        raise _currency_conflict(form, stored)


def _currency_conflict(form: PdfIngestForm, stored: str | None) -> HTTPException:
    held = f"are held in {stored}" if stored is not None else f"are not held in {DEFAULT_CURRENCY}"
    return _coded_error(
        status.HTTP_409_CONFLICT,
        "currency_conflict",
        f"The {form.period_type} {form.period_end_date} fundamentals of {form.ticker} {held}; "
        f"{DEFAULT_CURRENCY} values cannot be merged into them.",
        "ticker",
    )


# ---------------------------------------------------------------------------------------------
# Mapping the extraction to the response
# ---------------------------------------------------------------------------------------------


def _is_line_item(name: str) -> TypeGuard[LineItemName]:
    return name in LINE_ITEMS


def _line_item(name: str) -> LineItemName:
    """Narrow an extractor item name to :data:`LineItemName`; the extractor only returns names
    it was asked for, so anything else is a broken contract."""
    if not _is_line_item(name):
        raise ValueError(f"the extractor returned an unknown line item {name!r}")
    return name


@overload
def _output_value(value: Decimal, unit: str) -> Decimal: ...


@overload
def _output_value(value: None, unit: str) -> None: ...


def _output_value(value: Decimal | None, unit: str) -> Decimal | None:
    """Round an extracted value once: 2 places for money, an exact integer for shares."""
    if value is None:
        return None
    if unit == _SHARES_UNIT:
        # The extractor returns share counts as integral Decimals, so int() is exact.
        return Decimal(int(value))
    return quantize_out(value, "money")


def _response_item(item: ExtractedLineItem) -> ExtractedItem:
    unit = item.unit
    return ExtractedItem(
        item=_line_item(item.item),
        status=item.status,
        value=_output_value(item.value, unit),
        unit=unit,
        raw_value=item.raw_value,
        scale_applied=item.scale_applied,
        page_number=item.page_number,
        matched_label=item.matched_label,
        derivation=[
            DerivationEntry(
                label=part.label,
                page_number=part.page_number,
                value=_output_value(part.value, unit),
            )
            for part in item.derivation
        ],
        candidates=[
            Candidate(
                label=candidate.label,
                page_number=candidate.page_number,
                raw_value=candidate.raw_value,
                value=_output_value(candidate.value, unit),
            )
            for candidate in item.candidates
        ],
    )


def _fundamentals_values(items: list[ExtractedItem]) -> dict[str, object]:
    """The ``terminal_fundamentals`` columns a commit writes: only ``found`` and ``derived``
    items, never ``ambiguous`` or ``not_found``. Money is the response's 2-place value, which is
    the column scale, and shares an ``int``."""
    values: dict[str, object] = {}
    for item in items:
        if item.status not in _COMMITTABLE_STATUSES or item.value is None:
            continue
        column = ITEM_COLUMNS[item.item]
        values[column] = int(item.value) if item.unit == _SHARES_UNIT else item.value
    return values


# ---------------------------------------------------------------------------------------------
# Writes, inside the request transaction
# ---------------------------------------------------------------------------------------------


async def _upsert_filing(
    session: AsyncSession,
    *,
    form: PdfIngestForm,
    file_name: str,
    file_sha256: str,
    file_size_bytes: int,
    result: ExtractionResult,
    items: list[ExtractedItem],
    commits_fundamentals: bool,
) -> uuid.UUID:
    """Upsert the filing on ``file_sha256`` and return its id.

    ``committed_to_fundamentals`` is written only as true, only by an upload that commits
    fundamentals, and in the same ``INSERT ... ON CONFLICT`` statement as the rest of the row.
    Other uploads omit the column: a new row takes its default (false) and a conflicting row keeps
    its stored value, so the flag becomes stored OR new atomically, under the conflict update's
    row lock. No upload can clear it, including a concurrent first upload of the same document,
    for which no row existed to lock beforehand. The filing and fundamentals writes share the
    request transaction, so a rolled-back commit sets nothing.
    """
    row: dict[str, object] = {
        "ticker": form.ticker,
        "company_name": form.company_name,
        "document_type": form.document_type,
        "period_end_date": form.period_end_date,
        "period_type": form.period_type,
        "file_name": file_name[:_FILE_NAME_MAX_LENGTH],
        "file_sha256": file_sha256,
        "file_size_bytes": file_size_bytes,
        "page_count": result.page_count,
        "detected_scale": result.detected_scale,
        "extraction": [item.model_dump(mode="json") for item in items],
        "status": result.status,
        "currency": result.currency,
    }
    if commits_fundamentals:
        # Supplied only as true: the conflict update sets exactly the supplied columns, so a
        # false here would overwrite a flag another upload has committed.
        row["committed_to_fundamentals"] = True
    await upsert_rows(
        session, TerminalCorporateFiling, [row], key=("file_sha256",), mode="do_update"
    )
    filing_id = await session.scalar(
        select(TerminalCorporateFiling.id).where(TerminalCorporateFiling.file_sha256 == file_sha256)
    )
    if filing_id is None:
        raise RuntimeError("the upserted corporate filing row could not be read back")
    return filing_id


async def _commit_fundamentals(
    session: AsyncSession,
    form: PdfIngestForm,
    values: dict[str, object],
    filing_id: uuid.UUID,
) -> uuid.UUID:
    """COALESCE-merge ``values`` into the fundamentals row of the form's key and return its id.

    Only the written columns' ``source_by_field`` entries become ``pdf_filing``; every other
    column keeps its earlier provenance. A row the merge skipped had its currency changed by a
    concurrent writer after :func:`_check_commit_currency`, and is reported as that conflict.
    """
    row: dict[str, object] = {
        "ticker": form.ticker,
        "period_end_date": form.period_end_date,
        "period_type": form.period_type,
        "currency": DEFAULT_CURRENCY,
        **values,
    }
    written = await upsert_fundamentals(
        session, [row], source=PDF_FILING_PIPELINE, filing_id=filing_id
    )
    if written == 0:
        stored = await session.scalar(
            select(TerminalFundamental.currency).where(*_fundamentals_key(form))
        )
        raise _currency_conflict(form, stored)
    fundamentals_id = await session.scalar(
        select(TerminalFundamental.id).where(*_fundamentals_key(form))
    )
    if fundamentals_id is None:
        raise RuntimeError("the merged fundamentals row could not be read back")
    return fundamentals_id


# ---------------------------------------------------------------------------------------------
# The endpoint's service
# ---------------------------------------------------------------------------------------------


async def ingest_pdf(
    session: AsyncSession,
    settings: TerminalSettings,
    upload_bytes: bytes,
    file_name: str,
    form: PdfIngestForm,
) -> IngestPdfResponse:
    """Extract the requested line items from an uploaded PDF, store the extraction and, with
    ``form.commit``, merge the committable items into ``terminal_fundamentals``.

    Contract (see the module docstring for the error table):

    * Every call that reaches this function writes one ``terminal_ingestion_runs`` row
      (``pipeline='pdf_filing'``, ``trigger='api'``) on its own connection: ``succeeded`` for an
      HTTP 200 or a request cancelled once its data commit has started (below), otherwise
      ``failed`` with the error code (``unexpected_error:<Type>`` for a server fault,
      ``cancelled`` for a request cancelled before that commit) as ``error_message``. Its
      ``details`` hold ``file_sha256``, ``ticker``, ``period_end_date``, ``document_type`` and
      ``commit``, plus ``filing_id``, ``status``, ``committed`` and ``fundamentals_id`` on
      success. When the row cannot be started (the audit engine cannot be resolved, or its
      insert fails), unless the request or the insert was cancelled (below), the fault is
      logged once at ERROR as ``unexpected_error:<Type>`` and the request answered with 500
      ``internal_error``. The row's id is generated before its insert, so a row that a failed
      insert committed anyway is first finished ``failed`` with that code, best effort: finding
      no row is expected and not logged, and any other failure to finish it is logged at ERROR
      by its type, never raised.
    * An HTTP 200 upserts ``terminal_corporate_filings`` on ``file_sha256``, with or without
      ``commit``. A commit writes only ``found`` and ``derived`` items, never ``ambiguous``
      ones, COALESCE-merged so existing values are never nulled, with ``filing_id`` set and
      ``source_by_field`` set to ``pdf_filing`` for exactly the written columns. When no item is
      committable nothing is merged and ``committed`` is false.
    * Any rejection rolls the request transaction back, so neither table is written or changed,
      and is re-raised unchanged after the run row is finished. A server fault is rolled back
      and recorded alike, logged once at ERROR by its type, and then answered with the coded 500
      ``internal_error`` raised ``from None``: its text, which can carry SQL with bound
      parameters (the file name, the form fields, the extraction), never reaches the response,
      a log or a traceback.
    * The pre-parse checks' read is committed before parsing, so the request session holds no
      transaction, and no lock on ``terminal_equities``, while the PDF is parsed; the filing and
      fundamentals writes then share one new transaction. That commit runs to completion even if
      the request is cancelled meanwhile; such a cancellation is then honoured before parsing
      and the run row finished ``failed`` with ``cancelled``. A host session joined to an outer
      transaction only releases its savepoint (``create_savepoint``) or leaves that transaction
      open (``rollback_only``); the outer transaction keeps the check's lock until the host ends
      it.
    * The transaction is committed here (services own write transactions). The run-row insert,
      the commit and the run-row writes run to completion even if the request is cancelled
      meanwhile. A request cancelled while parsing (by ``Task.cancel()``, as the route does when
      its client disconnects, or by an anyio cancel scope) stops its parse instead: the parser
      process is killed, the cancellation is raised once the parse thread has ended, moments
      later, and the route's parse slot is then freed; no filing or fundamentals row is written
      and the run row is finished ``failed`` with ``cancelled``. A cancellation received while
      the row is inserted finishes it ``failed`` with ``cancelled`` and then re-raises; a
      cancelled request whose row could not be started re-raises the cancellation, logged
      once: at WARNING as ``cancelled`` when the cancellation ended the insert itself,
      otherwise at ERROR as the insert's fault, after any row that insert committed is finished
      ``failed`` with that code. A cancellation received while such a row is finished is
      honoured once it is: the cancellation is re-raised instead of the 500.

    Args:
        session: The request session; the filing and fundamentals writes share its transaction.
        settings: Supplies ``pdf_max_pages``.
        upload_bytes: The whole PDF, already size- and type-checked by the route; never stored.
        file_name: The uploaded file name, truncated to 255 characters when stored.
        form: The form fields, with ``ticker`` already normalized.
    """
    file_sha256 = hashlib.sha256(upload_bytes).hexdigest()
    base_details: dict[str, Any] = {
        "file_sha256": file_sha256,
        "ticker": form.ticker,
        "period_end_date": (
            form.period_end_date.isoformat() if form.period_end_date is not None else None
        ),
        "document_type": form.document_type,
        "commit": form.commit,
    }
    recorder, run_id, cancelled_while_starting = await _start_run(session, base_details)
    rows_fetched = 0
    try:
        if cancelled_while_starting:
            # The row is committed: the cancellation branch below finishes it, then re-raises.
            raise asyncio.CancelledError
        await _check_preconditions(session, form)
        # The check's read transaction ends before the parse, so none (and no lock, such as the
        # check's on terminal_equities) is held while it runs. A commit, never a rollback, which
        # would roll back the outer transaction a "rollback_only" host session joins.
        read_commit, cancelled_while_committing = await _run_to_completion(session.commit())
        if cancelled_while_committing:
            # The cancellation supersedes the commit's outcome, which is still retrieved so that
            # asyncio never reports it as an unhandled exception.
            if not read_commit.cancelled():
                read_commit.exception()
            raise asyncio.CancelledError
        read_commit.result()
        result = await _extract(upload_bytes, form, settings)
        rows_fetched = len(result.items)
        if form.commit:
            await _check_commit_currency(session, form, result)
        items = [_response_item(item) for item in result.items]
        values = _fundamentals_values(items) if form.commit else {}
        filing_id = await _upsert_filing(
            session,
            form=form,
            file_name=file_name,
            file_sha256=file_sha256,
            file_size_bytes=len(upload_bytes),
            result=result,
            items=items,
            commits_fundamentals=bool(values),
        )
        fundamentals_id = (
            await _commit_fundamentals(session, form, values, filing_id) if values else None
        )
        # Built before the commit from plain values (the ids are UUIDs read back by query), so
        # nothing touches an ORM attribute afterwards, which a host session may expire on commit.
        response = IngestPdfResponse(
            filing_id=filing_id,
            file_sha256=file_sha256,
            page_count=result.page_count,
            detected_scale=result.detected_scale,
            currency=result.currency,
            status=result.status,
            items=items,
            missing_items=[_line_item(name) for name in result.missing_items],
            committed=fundamentals_id is not None,
            fundamentals_id=fundamentals_id,
            warnings=list(result.warnings),
        )
        commit_task, cancel_requested = await _run_to_completion(session.commit())
        commit_task.result()
    except HTTPException as exc:
        code = _error_code(exc)
        logger.info("PDF ingestion run %s rejected: %s", run_id, code)
        await _record_failure(
            session,
            recorder,
            run_id,
            error_message=code,
            rows_fetched=rows_fetched,
            details=base_details,
        )
        raise
    except asyncio.CancelledError:
        logger.warning("PDF ingestion run %s cancelled", run_id)
        await _record_failure(
            session,
            recorder,
            run_id,
            error_message="cancelled",
            rows_fetched=rows_fetched,
            details=base_details,
        )
        raise
    except Exception as exc:
        # The type name only: an exception's text can carry SQL with its bound parameters.
        error_message = f"unexpected_error:{type(exc).__name__}"
        logger.error("PDF ingestion run %s failed: %s", run_id, error_message)
        await _record_failure(
            session,
            recorder,
            run_id,
            error_message=error_message,
            rows_fetched=rows_fetched,
            details=base_details,
        )
        # Answered, not re-raised: a propagated fault would reach the server's traceback log
        # with its text, and "from None" keeps it out of any traceback of the coded error.
        raise _internal_error() from None

    cancel_requested |= await _finish_run(
        recorder,
        run_id,
        run_status=RunStatus.SUCCEEDED,
        rows_fetched=rows_fetched,
        rows_written=2 if response.committed else 1,
        error_message=None,
        details={
            **base_details,
            "filing_id": response.filing_id,
            "status": response.status,
            "committed": response.committed,
            "fundamentals_id": response.fundamentals_id,
        },
    )
    logger.info(
        "PDF filing ingested: run=%s sha256=%s pages=%d found=%d missing=%d committed=%s",
        run_id,
        file_sha256[:_SHA_LOG_PREFIX],
        response.page_count,
        sum(1 for item in response.items if item.status in _COMMITTABLE_STATUSES),
        len(response.missing_items),
        response.committed,
    )
    if cancel_requested:
        # The data is committed and recorded; the caller asked to stop, so honour it now.
        raise asyncio.CancelledError
    return response
