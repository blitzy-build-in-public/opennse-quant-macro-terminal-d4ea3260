"""``POST /ingest/pdf``: line-item extraction from uploaded filing and prospectus PDFs.

Corporate annual and interim reports and IPO prospectuses arrive as multipart form data. This
route applies the upload limits and hands the bytes to ``services.filings_service.ingest_pdf``,
which parses them with pdfplumber in a worker thread, enforces the commit preconditions, stores
the extraction and records the ``pdf_filing`` run. The route itself never parses, stores or logs
the file's bytes, and never touches an extracted value.

Limits, in the order they apply:

1. The whole request is bounded before multipart parsing by :class:`TerminalUploadRoute`, the
   route class of this sub-router: ``TERMINAL_PDF_MAX_BYTES`` plus 1 MiB of multipart overhead,
   answered with 413 ``request_too_large``. FastAPI parses and spools a multipart body before the
   endpoint runs, so a check inside the endpoint could not bound it.
2. The file itself: more than ``TERMINAL_PDF_MAX_BYTES`` is 413 ``file_too_large``, and bytes that
   do not start with ``%PDF-`` are 415 ``not_a_pdf``.
3. At most ``TERMINAL_PDF_MAX_CONCURRENCY`` (2) parses run at once per mounted router, which is
   per process for the standalone app and for a host that mounts ``terminal_router`` once. A
   request beyond it is answered at once with 429 ``ingest_busy`` and ``Retry-After: 5``; it
   never queues. Each mount counts its own active parses across settings changes: a lower limit
   applies to new requests at once while parses admitted under the old one finish, so a request
   is admitted only while fewer parses than the limit in force are running.

A request rejected by these limits writes nothing and records no run row. Every coded error is the
body ``{"detail": {"code", "message", "field"}}`` and carries no extraction. Single-field form
failures (an unknown ``document_type``, an over-long ``ticker``, an invalid date, a missing
``file``) stay FastAPI's standard 422 list.

The sub-router carries no prefix, tags, middleware or exception handlers: ``api/router.py`` mounts
it under ``/api/v1/terminal``, where the router-level principal dependency applies, and the route
adds ``require_ingest_permission``, which a host overrides to restrict ingestion. ``include_router``
preserves the route class, so the request-size guard travels with ``terminal_router`` into a host.
The module holds no mutable state: the parse limiter and the settings-fallback warning belong to
each mounted copy of the route (see :class:`TerminalUploadRoute`).

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` form and dependency metadata from the route signature.
"""

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated, Any, Final

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.requests import ClientDisconnect
from starlette.types import Message, Receive

from opennse_terminal.api.deps import (
    SessionDep,
    SettingsDep,
    get_terminal_settings,
    require_ingest_permission,
)
from opennse_terminal.config import TerminalSettings
from opennse_terminal.schemas import (
    ALL_LINE_ITEMS,
    DocumentType,
    ErrorDetail,
    ErrorResponse,
    IngestPdfResponse,
    LineItemName,
    PeriodType,
    ValidationErrorResponse,
)
from opennse_terminal.services import filings_service

__all__ = [
    "MULTIPART_OVERHEAD_BYTES",
    "PdfParseLimiter",
    "TerminalUploadRoute",
    "get_pdf_parse_limiter",
    "ingest_pdf",
    "router",
]

MULTIPART_OVERHEAD_BYTES: Final = 1_048_576
"""Bytes a request may carry beyond ``TERMINAL_PDF_MAX_BYTES`` for multipart boundaries, part
headers and the other form fields (1 MiB)."""

PDF_MAGIC: Final = b"%PDF-"
RETRY_AFTER_SECONDS: Final = "5"
# Lengths of terminal_equities.ticker (VARCHAR(16)), terminal_corporate_filings.company_name
# (VARCHAR(200)) and terminal_corporate_filings.file_name (VARCHAR(255)).
TICKER_MAX_LENGTH: Final = 16
COMPANY_NAME_MAX_LENGTH: Final = 200
FILE_NAME_MAX_LENGTH: Final = 255
DEFAULT_FILE_NAME: Final = "upload.pdf"
# PostgreSQL text cannot hold a NUL character, so a form field carrying one is refused as a
# single-field 422 here rather than failing as a 500 when the service queries or stores it.
_NO_NUL_PATTERN: Final = r"^[^\x00]*$"
# FastAPI's own answer to a body it cannot read, reused when the client disconnects mid-body.
_BODY_ERROR_MESSAGE: Final = "There was an error parsing the body"
# ASGI scope key under which the route hands its mount's parse limiter to the endpoint.
_PARSE_LIMITER_SCOPE_KEY: Final = "opennse_terminal.pdf_parse_limiter"
# Seconds an over-limit body is drained at most: long enough for what a proxy that forwarded just
# past the limit still has in flight, short enough that a stalled sender gets its 413 promptly.
_DRAIN_TIMEOUT_SECONDS: Final = 1.0

logger = logging.getLogger("opennse_terminal.api.routes.ingest")


# ---------------------------------------------------------------------------------------------
# Whole-request size guard
# ---------------------------------------------------------------------------------------------


class _RequestTooLargeError(Exception):
    """Raised by the counting ``receive`` once a body without ``Content-Length`` passes the
    request limit."""


async def _resolve_settings(
    request: Request, on_fallback: Callable[[object], None]
) -> TerminalSettings:
    """The settings that govern this request, honouring a host's ``get_terminal_settings``
    override.

    The guard runs before FastAPI resolves any dependency, so it calls the provider itself: the
    override registered on the application in ``app.dependency_overrides``, else
    ``get_terminal_settings``. An override must be a zero-argument callable, as in
    ``lambda: TerminalSettings(...)``; it may be async. One that returns anything other than a
    ``TerminalSettings`` is passed to ``on_fallback`` and the cached default settings apply
    instead.
    """
    overrides: Mapping[Any, Any] = getattr(request.scope.get("app"), "dependency_overrides", {})
    provider = overrides.get(get_terminal_settings, get_terminal_settings)
    value = provider()
    if inspect.isawaitable(value):
        value = await value
    if isinstance(value, TerminalSettings):
        return value
    on_fallback(value)
    return get_terminal_settings()


def _too_large_response(limit: int) -> JSONResponse:
    """The 413 ``request_too_large`` answer, in the same ``{"detail": {...}}`` shape that an
    ``HTTPException`` with an ``ErrorDetail`` produces."""
    logger.info("PDF upload rejected: request_too_large (limit %d bytes)", limit)
    error = ErrorDetail(
        code="request_too_large",
        message=f"Request body exceeds {limit} bytes.",
        field=None,
    )
    return JSONResponse(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE, content={"detail": error.model_dump()}
    )


async def _discard_body(receive: Receive, allowance: int) -> None:
    """Read and drop the rest of a request body whose end has not been received yet.

    Messages are read until one ends the body (``more_body`` false), one is not
    ``http.request`` (a disconnect), more than ``allowance`` bytes have been read, or
    :data:`_DRAIN_TIMEOUT_SECONDS` have passed since the drain began; a negative ``allowance``
    reads nothing. No byte is kept. The deadline bounds the whole drain, so a sender that stalls
    or trickles cannot hold the request. Never call it once the body has ended: an ASGI server's
    ``receive`` then waits for the client to disconnect. An external cancellation
    (``asyncio.CancelledError``) propagates.
    """
    discarded = 0
    deadline = asyncio.timeout(_DRAIN_TIMEOUT_SECONDS)
    try:
        async with deadline:
            while discarded <= allowance:
                message = await receive()
                if message["type"] != "http.request":
                    return
                discarded += len(message.get("body", b""))
                if not message.get("more_body", False):
                    return
    except TimeoutError:
        # Only the deadline's own expiry ends the drain; a TimeoutError raised by ``receive``
        # while the deadline still stood propagates as is.
        if not deadline.expired():
            raise


class TerminalUploadRoute(APIRoute):
    """Route class of the ingest sub-router: bounds the whole request before the body is parsed.

    The limit is ``pdf_max_bytes`` of the request's settings plus
    :data:`MULTIPART_OVERHEAD_BYTES`. It is checked before any dependency (session, principal,
    ingest permission) is resolved and before multipart parsing, and is answered with 413
    ``request_too_large``:

    * A request with ``Content-Length`` above the limit, or with a value that is not a
      non-negative integer, is refused without reading its body. Within the limit the body is
      parsed as usual; the server's framing stops it at the declared length.
    * A request without ``Content-Length`` (chunked) is read through a counting ``receive`` that
      stops the stream once the limit is passed. The bounded body is cached on the request, and
      FastAPI's form parsing reads it from there. Once the limit is passed, the rest of the body
      is read and discarded before the 413 is sent, so the connection closes cleanly and a proxy
      that forwarded slightly past the limit receives the 413 rather than a reset. The drain
      stops at the body's end, once 1 MiB past the limit has been read, or after one second
      (``_DRAIN_TIMEOUT_SECONDS``), so a sender that stalls past the limit still gets its 413
      promptly. The request is still never parsed or spooled.

    A router cannot carry middleware, and ``include_router`` preserves the route class, so the
    guard reaches every host that mounts ``terminal_router`` without any ingress setting. A host
    proxy may impose a stricter limit.

    Each handler built by :meth:`get_route_handler` also owns the state of one mount. FastAPI
    builds one handler for every application that includes ``terminal_router``, and replaces it
    (and with it that state) only if routes are added to the terminal routers after mounting,
    which neither the product nor the ``include_router`` host contract does. The handler owns:

    * a :class:`PdfParseLimiter`, which counts that mount's active parses for its whole life,
      whatever ``pdf_max_concurrency`` each request's settings carry. The handler places it in
      the request scope, where :func:`get_pdf_parse_limiter` hands it to the endpoint;
    * the once-only warning logged when the mount's ``get_terminal_settings`` override returns
      something other than ``TerminalSettings``.

    Two hosts in one process therefore never share upload accounting. Nothing is keyed on the
    settings object, which an override such as ``lambda: TerminalSettings(...)`` builds anew for
    every request.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap FastAPI's handler with the request-size check and this mount's parse limiter."""
        # Built here, synchronously: FastAPI resolves an included route's context during this
        # call, so deferring it to request time would lose that context.
        original = super().get_route_handler()
        limiter = PdfParseLimiter()
        fallback_warned = False

        def warn_settings_fallback(value: object) -> None:
            nonlocal fallback_warned
            if fallback_warned:
                return
            fallback_warned = True
            logger.warning(
                "The get_terminal_settings override returned %s, not TerminalSettings; the upload "
                "size guard uses the default settings",
                type(value).__name__,
            )

        async def guarded(request: Request) -> Response:
            request.scope[_PARSE_LIMITER_SCOPE_KEY] = limiter
            settings = await _resolve_settings(request, warn_settings_fallback)
            limit = settings.pdf_max_bytes + MULTIPART_OVERHEAD_BYTES
            header = request.headers.get("content-length")
            if header is not None:
                try:
                    declared = int(header)
                except ValueError:
                    return _too_large_response(limit)
                if declared < 0 or declared > limit:
                    return _too_large_response(limit)
                return await original(request)

            received = 0
            body_ended = False

            async def counting_receive() -> Message:
                nonlocal received, body_ended
                message = await request.receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    body_ended = not message.get("more_body", False)
                    if received > limit:
                        raise _RequestTooLargeError
                return message

            counted = Request(request.scope, receive=counting_receive)
            # Read here, inside this try, because FastAPI turns any exception raised while it
            # parses the form into a 400 rather than letting this guard answer 413.
            try:
                await counted.body()
            except _RequestTooLargeError:
                # Answering with body bytes still unread makes the server reset the connection,
                # so a proxy still forwarding them fails its write and never relays the 413. The
                # body is drained first, until it ends, 1 MiB past the bound has been read or the
                # drain deadline passes.
                if not body_ended:
                    await _discard_body(
                        request.receive, limit + MULTIPART_OVERHEAD_BYTES - received
                    )
                return _too_large_response(limit)
            except ClientDisconnect as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=_BODY_ERROR_MESSAGE
                ) from exc
            return await original(counted)

        return guarded


router = APIRouter(route_class=TerminalUploadRoute)


# ---------------------------------------------------------------------------------------------
# Parse concurrency
# ---------------------------------------------------------------------------------------------


class PdfParseLimiter:
    """Non-blocking admission of PDF parses, resized to each request's limit in constant time.

    An ``asyncio.Semaphore`` holds the free parse slots: ``limit - active`` of them, or none
    while ``active`` is at or above the limit in force. :meth:`slot` first makes the request's
    ``pdf_max_concurrency`` the limit in force. A changed limit replaces the semaphore with one
    holding the free slots under the new limit, a single step whatever the size of the change,
    and keeps every slot in use:

    * a higher limit frees its extra slots at once;
    * a lower limit applies to new requests at once, while parses admitted under the old limit
      run to completion: a finishing parse frees its slot only if fewer parses than the limit in
      force are then active.

    A request is therefore admitted only while fewer parses than the limit in force are active.
    Admission never waits: a free slot is taken at once, or the request is refused. A semaphore
    is acquired only while it has a free slot, so no coroutine ever waits on one, replacing it
    strands no waiter, and it never binds to an event loop. Like every asyncio primitive, a
    limiter serves the coroutines of one event loop at a time.
    """

    def __init__(self) -> None:
        self._semaphore = asyncio.Semaphore(0)
        self._limit = 0
        self._active = 0

    @property
    def limit(self) -> int:
        """The limit in force: the latest request's ``pdf_max_concurrency``, 0 before the first."""
        return self._limit

    @property
    def active(self) -> int:
        """Parses holding a slot. It exceeds :attr:`limit` only while parses admitted under a
        higher limit are still running."""
        return self._active

    @asynccontextmanager
    async def slot(self, limit: int) -> AsyncIterator[bool]:
        """Resize to ``limit`` slots and take a free one, without waiting.

        Yields ``True`` when a slot was taken; it is given back when the block exits, also on an
        exception or a cancellation. Yields ``False`` when every slot is in use, and the caller
        refuses the request.

        Raises:
            ValueError: ``limit`` is below 1, which ``TerminalSettings`` never allows.
        """
        if limit < 1:
            raise ValueError(f"The PDF parse limit must be at least 1, got {limit}")
        # Nothing here suspends before the slot is taken: resizing is synchronous, and acquiring
        # a free slot of an unlocked semaphore completes without yielding to the event loop, so
        # checking for a free slot and taking it cannot interleave with another request.
        self._resize(limit)
        admitted = not self._semaphore.locked()
        if admitted:
            await self._semaphore.acquire()
            self._active += 1
        try:
            yield admitted
        finally:
            if admitted:
                self._release()

    def _resize(self, limit: int) -> None:
        """Make ``limit`` the limit in force, keeping every slot in use, in constant time."""
        if limit != self._limit:
            self._semaphore = asyncio.Semaphore(max(0, limit - self._active))
            self._limit = limit

    def _release(self) -> None:
        """Give back a slot, unless the parses still active fill the limit in force."""
        self._active -= 1
        if self._active < self._limit:
            self._semaphore.release()


async def get_pdf_parse_limiter(request: Request) -> PdfParseLimiter:
    """The parse limiter of the mount serving ``request``.

    :class:`TerminalUploadRoute` puts its mount's limiter in the request scope before any
    dependency runs. A host can override this dependency, for example to share one
    :class:`PdfParseLimiter` between several mounts.

    Raises:
        RuntimeError: ``request`` was not served by ``TerminalUploadRoute``.
    """
    limiter = request.scope.get(_PARSE_LIMITER_SCOPE_KEY)
    if not isinstance(limiter, PdfParseLimiter):
        raise RuntimeError(
            "POST /ingest/pdf must be served by TerminalUploadRoute, which supplies the parse "
            "limiter"
        )
    return limiter


ParseLimiterDep = Annotated[PdfParseLimiter, Depends(get_pdf_parse_limiter)]


def _rejection(
    status_code: int,
    code: str,
    message: str,
    field: str | None,
    headers: dict[str, str] | None = None,
) -> HTTPException:
    """A coded upload rejection, logged at INFO because it records no run row."""
    logger.info("PDF upload rejected: %s", code)
    return HTTPException(
        status_code=status_code,
        detail=ErrorDetail(code=code, message=message, field=field).model_dump(),
        headers=headers,
    )


def _blank_to_none(value: str | None) -> str | None:
    """``value`` without surrounding whitespace, or ``None`` when nothing is left."""
    if value is None:
        return None
    return value.strip() or None


# ---------------------------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------------------------


@router.post(
    "/ingest/pdf",
    response_model=IngestPdfResponse,
    summary="Extract financial line items from a filing or prospectus PDF",
    dependencies=[Depends(require_ingest_permission)],
    responses={
        404: {"model": ErrorResponse, "description": "unknown_ticker"},
        409: {
            "model": ErrorResponse,
            "description": (
                "currency_conflict: the fundamentals row for the ticker and period is held in "
                "another currency"
            ),
        },
        413: {
            "model": ErrorResponse,
            "description": (
                "request_too_large (whole request above TERMINAL_PDF_MAX_BYTES plus 1 MiB) or "
                "file_too_large (file above TERMINAL_PDF_MAX_BYTES)"
            ),
        },
        415: {"model": ErrorResponse, "description": "not_a_pdf: no %PDF- header"},
        422: {
            "model": ErrorResponse | ValidationErrorResponse,
            "description": (
                "Coded: pdf_encrypted, pdf_unreadable, too_many_pages, "
                "commit_requires_ticker_and_period or commit_requires_kes; or FastAPI's standard "
                "validation list for a single form field"
            ),
        },
        429: {
            "model": ErrorResponse,
            "description": "ingest_busy: every parse slot is in use; retry after 5 seconds",
        },
        500: {
            "model": ErrorResponse,
            "description": "internal_error: a server fault, whose text is never returned",
        },
    },
)
async def ingest_pdf(
    session: SessionDep,
    settings: SettingsDep,
    limiter: ParseLimiterDep,
    file: Annotated[
        UploadFile,
        File(description="PDF with a text layer; at most TERMINAL_PDF_MAX_BYTES bytes."),
    ],
    document_type: Annotated[
        DocumentType,
        Form(description="Kind of document: annual_report, interim_report, prospectus or other."),
    ] = "annual_report",
    ticker: Annotated[
        str | None,
        Form(
            max_length=TICKER_MAX_LENGTH,
            pattern=_NO_NUL_PATTERN,
            description=(
                "NSE ticker of a known equity, for example SCOM; required with commit. Omit it "
                "for an unlisted IPO issuer."
            ),
        ),
    ] = None,
    company_name: Annotated[
        str | None,
        Form(
            max_length=COMPANY_NAME_MAX_LENGTH,
            pattern=_NO_NUL_PATTERN,
            description="Issuer name, for a prospectus of a company without a ticker.",
        ),
    ] = None,
    period_end_date: Annotated[
        date | None,
        Form(
            description=(
                "Financial year or half-year end (YYYY-MM-DD); selects the period column by its "
                "year and is required with commit."
            ),
        ),
    ] = None,
    period_type: Annotated[
        PeriodType, Form(description="FY (full year) or H1 (first half).")
    ] = "FY",
    items: Annotated[
        list[LineItemName] | None,
        Form(description="Repeatable; defaults to all six items."),
    ] = None,
    commit: Annotated[
        bool,
        Form(
            description=(
                "Merge the found and derived items into terminal_fundamentals; requires ticker, "
                "period_end_date and a KES statement. False previews the extraction."
            ),
        ),
    ] = False,
) -> IngestPdfResponse:
    """Extract Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and Shares
    Outstanding from an uploaded PDF, and with ``commit`` merge them into the fundamentals.

    Returns 200 with the extraction for every accepted upload, with or without ``commit``:
    monetary values in whole units of the filing's currency (KES, or USD returned unconverted)
    as decimal strings, share counts as integers, and a ``not_found`` entry for each missing
    item. Every 200 upserts the ``terminal_corporate_filings`` row on the file's SHA-256; the raw
    bytes are never stored.

    Errors carry no extraction and write no filing or fundamentals row: 413
    ``request_too_large`` or ``file_too_large``, 415 ``not_a_pdf``, 429 ``ingest_busy`` (with
    ``Retry-After: 5``), and from the service 422 ``pdf_encrypted``, ``pdf_unreadable``,
    ``too_many_pages``, ``commit_requires_ticker_and_period`` or ``commit_requires_kes``, 409
    ``currency_conflict``, 404 ``unknown_ticker`` and 500 ``internal_error`` for a server fault.
    Send ``commit=false`` to preview what a PDF yields before committing it.
    """
    data = await file.read(settings.pdf_max_bytes + 1)
    if len(data) > settings.pdf_max_bytes:
        raise _rejection(
            status.HTTP_413_CONTENT_TOO_LARGE,
            "file_too_large",
            f"The uploaded file exceeds {settings.pdf_max_bytes} bytes.",
            "file",
        )
    if not data.startswith(PDF_MAGIC):
        raise _rejection(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "not_a_pdf",
            "The uploaded file is not a PDF: it does not start with %PDF-.",
            "file",
        )
    # Only the stored copy is cleaned: NUL characters are dropped because PostgreSQL text cannot
    # hold them, and the name is cut to the column length.
    file_name = (file.filename or "").replace("\x00", "")[:FILE_NAME_MAX_LENGTH] or (
        DEFAULT_FILE_NAME
    )
    form = filings_service.PdfIngestForm(
        document_type=document_type,
        ticker=_blank_to_none(ticker),
        company_name=_blank_to_none(company_name),
        period_end_date=period_end_date,
        period_type=period_type,
        items=tuple(items or ALL_LINE_ITEMS),
        commit=commit,
    )
    # A free slot is checked for and taken without waiting, and given back when the block exits,
    # also when the service raises or the request is cancelled. A cancelled service call returns
    # only once its parse thread has ended, so the slot is held for as long as that parse runs.
    async with limiter.slot(settings.pdf_max_concurrency) as admitted:
        if not admitted:
            raise _rejection(
                status.HTTP_429_TOO_MANY_REQUESTS,
                "ingest_busy",
                f"All {settings.pdf_max_concurrency} PDF parse slots are in use; retry shortly.",
                None,
                headers={"Retry-After": RETRY_AFTER_SECONDS},
            )
        return await filings_service.ingest_pdf(session, settings, data, file_name, form)
