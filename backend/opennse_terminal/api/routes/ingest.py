"""``POST /ingest/pdf``: line-item extraction from uploaded filing and prospectus PDFs.

Corporate annual and interim reports and IPO prospectuses arrive as multipart form data. This
route applies the upload limits and hands the bytes to ``services.filings_service.ingest_pdf``,
which parses them with pdfplumber in a worker thread, enforces the commit preconditions, stores
the extraction and records the ``pdf_filing`` run. The route itself never parses, stores or logs
the file's bytes, and never touches an extracted value.

Limits, in the order they apply:

1. The whole request and its form parts are bounded by :class:`TerminalUploadRoute`, the route
   class of this sub-router, before any dependency runs. The request may carry
   ``TERMINAL_PDF_MAX_BYTES`` plus 1 MiB of multipart overhead, and beyond it is answered with 413
   ``request_too_large``: a declared ``Content-Length`` is checked before the body is read, and a
   chunked body is counted while the multipart parser streams it, never cached whole. The form
   may carry one file part and at most 32 other fields of at most 4 KiB each; beyond these, or
   for any other multipart body the parser refuses (for example one without a boundary), the
   answer is the framework's 400 with a string ``detail``. FastAPI parses and spools a multipart
   body before the endpoint runs, so a check inside the endpoint could not bound it.
2. The file itself, judged without loading it into memory: a spooled size above
   ``TERMINAL_PDF_MAX_BYTES`` is 413 ``file_too_large``, and a file whose first five bytes are not
   ``%PDF-`` is 415 ``not_a_pdf``.
3. At most ``TERMINAL_PDF_MAX_CONCURRENCY`` (2) parses run at once per mounted router, which is
   per process for the standalone app and for a host that mounts ``terminal_router`` once. A
   request beyond it is answered at once with 429 ``ingest_busy`` and ``Retry-After: 5``; it
   never queues, and its file is not read beyond the five magic bytes the type check reads.
   Each mount counts its own active parses across settings changes: a lower limit applies to new
   requests at once while parses admitted under the old one finish, so a request is admitted
   only while fewer parses than the limit in force are running. Before admission a request holds
   in memory at most its form fields and the first 1 MiB of its file, which the multipart parser
   spools to a temporary file once it grows past that. Only an admitted request reads its whole
   file into memory, inside its parse slot, so whole-file memory grows with admitted parses
   while each other upload holds a small, bounded amount.

A request rejected by these limits writes nothing and records no run row. A client that
disconnects while its admitted request is served cancels it. Before the service's data commit
starts (while its run row is started, its pre-parse checks run, it parses or it makes its
uncommitted writes), the parse is stopped or never started, no filing or fundamentals row is
written and the run row ends ``failed`` with ``cancelled``. Once that commit has started, it runs
to completion: the filing, and any merged fundamentals, are stored and the run row ends
``succeeded``, although no response is delivered. Either way the route logs the disconnect once
at INFO and returns a bare 499 that nobody receives (see :func:`ingest_pdf`). Every coded error
is the body ``{"detail": {"code", "message", "field"}}`` and carries no extraction.
Single-field form failures (an unknown ``document_type``, an over-long ``ticker``, an invalid
date, a missing ``file``) stay FastAPI's standard 422 list.

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
import os
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager
from datetime import date
from typing import Annotated, Any, Final

import anyio
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException
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
    NoQueryParams,
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
# FastAPI validates the query string into a fresh NoQueryParams on every request, so this default
# is used only when ingest_pdf is awaited directly without it. The model has no fields, so the
# shared instance holds no state.
_NO_QUERY_PARAMS: Final = NoQueryParams()
# FastAPI's own answer to a body it cannot read, reused when the route's form parsing cannot
# read the body, as when the client disconnects mid-body.
_BODY_ERROR_MESSAGE: Final = "There was an error parsing the body"
# File parts the upload form may carry: it declares one, ``file``.
_MAX_FORM_FILES: Final = 1
# Non-file parts the upload form may carry. It declares seven fields, ``items`` repeatable over
# six names, so 12 parts carry every accepted request; 32 leaves room for repeated ``items``.
_MAX_FORM_FIELDS: Final = 32
# Bytes per non-file part. The longest accepted value, a 200-character ``company_name``, is at
# most 800 bytes of UTF-8.
_MAX_FORM_FIELD_BYTES: Final = 4096
# ASGI scope key under which the route hands its mount's parse limiter to the endpoint.
_PARSE_LIMITER_SCOPE_KEY: Final = "opennse_terminal.pdf_parse_limiter"
# Seconds an over-limit body is drained at most: long enough for what a proxy that forwarded just
# past the limit still has in flight, short enough that a stalled sender gets its 413 promptly.
_DRAIN_TIMEOUT_SECONDS: Final = 1.0
# Status of the bare answer to a request whose client has disconnected (nginx's "client closed
# request"). No client receives it: the server discards a response to a closed connection.
_CLIENT_CLOSED_REQUEST: Final = 499

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


async def _parse_form(request: Request) -> None:
    """Parse the form of ``request`` within the upload form's part limits and cache it on the
    request, where FastAPI's own form parsing finds it instead of parsing the body again.

    The body is read through ``request.stream()`` as the parser consumes it, so a counting
    ``receive`` behind ``request`` sees every byte. A multipart body may carry one file part
    (:data:`_MAX_FORM_FILES`) and at most :data:`_MAX_FORM_FIELDS` other fields of at most
    :data:`_MAX_FORM_FIELD_BYTES` each, and a URL-encoded body the same fields. The parser keeps
    each field in memory and spools the file: its first 1 MiB in memory, all of it in a
    temporary file once it grows past that. A body of any other type is not read, and the form
    is empty. Failures are answered as FastAPI answers them when it parses a form:

    * a body the parser refuses, such as a multipart body without a boundary, a second file
      part, a field too many or a field over the size limit, raises Starlette's
      ``HTTPException`` 400 with the parser's message as a string ``detail`` (for example
      ``Too many files. Maximum number of files is 1.``), unchanged;
    * :class:`_RequestTooLargeError`, raised by a counting ``receive``, propagates;
    * any other failure to read the body, a client disconnect included, raises
      ``HTTPException`` 400 ``There was an error parsing the body``.

    The parser closes every file it spooled before a failure leaves it, and a cancellation
    propagates.
    """
    try:
        await request.form(
            max_files=_MAX_FORM_FILES,
            max_fields=_MAX_FORM_FIELDS,
            max_part_size=_MAX_FORM_FIELD_BYTES,
        )
    except (StarletteHTTPException, _RequestTooLargeError):
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=_BODY_ERROR_MESSAGE
        ) from exc


class TerminalUploadRoute(APIRoute):
    """Route class of the ingest sub-router: bounds the whole request and its form parts before
    any dependency runs.

    The request limit is ``pdf_max_bytes`` of the request's settings plus
    :data:`MULTIPART_OVERHEAD_BYTES`. It is checked before any dependency (session, principal,
    ingest permission) is resolved, and is answered with 413 ``request_too_large``:

    * A request with ``Content-Length`` above the limit, or with a value that is not a
      non-negative integer, is refused without reading its body. Within the limit the body is
      parsed; the server's framing stops it at the declared length.
    * A request without ``Content-Length`` (chunked) is parsed through a counting ``receive``
      that stops the stream once the limit is passed, so the body is never cached whole. Once
      the limit is passed, the parser closes the file it spooled, dropping the bytes received so
      far, and the rest of the body is read and discarded before the 413 is sent, so the
      connection closes cleanly and a proxy that forwarded slightly past the limit receives the
      413 rather than a reset. The drain stops at the body's end, once 1 MiB past the limit has
      been read, or after one second (``_DRAIN_TIMEOUT_SECONDS``), so a sender that stalls past
      the limit still gets its 413 promptly. Nothing of the request reaches a dependency or the
      endpoint.

    The form is parsed here within its part limits (see :func:`_parse_form`) and cached on the
    request, where FastAPI's form parsing finds it: one file part, and at most
    :data:`_MAX_FORM_FIELDS` (32) other fields of at most :data:`_MAX_FORM_FIELD_BYTES` (4 KiB)
    each. The parser holds the fields and the first 1 MiB of the file in memory, and spools a
    larger file to a temporary file. A body beyond these limits, or any other multipart body the
    parser refuses (for example one without a boundary), is the framework's 400 with a string
    ``detail`` such as ``Too many files. Maximum number of files is 1.``, answered once the
    parser meets it without reading the rest of the body; a body that cannot be read is 400
    ``There was an error parsing the body``, as FastAPI answers it.

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
        """Wrap FastAPI's handler with the request-size check, the bounded form parse and this
        mount's parse limiter."""
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
                # Parsed here within the form's part limits; FastAPI would otherwise parse it
                # with Starlette's defaults of 1000 files and 1000 fields of 1 MiB each.
                await _parse_form(request)
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
            # Parsed here, inside this try, because FastAPI turns any exception raised while it
            # parses the form into a 400 rather than letting this guard answer 413. The parser
            # streams the body through the counting receive, so it is never cached whole.
            try:
                await _parse_form(counted)
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


async def _upload_size(file: UploadFile) -> int:
    """The uploaded file's size in bytes, found without loading the file: the size the multipart
    parser counted while spooling it, else the end offset of the spooled file."""
    if file.size is not None:
        return file.size
    spooled = file.file
    return await run_in_threadpool(spooled.seek, 0, os.SEEK_END)


# ---------------------------------------------------------------------------------------------
# Client disconnects
# ---------------------------------------------------------------------------------------------


async def _client_disconnect(receive: Receive) -> None:
    """Return once the client has disconnected, at the first ``http.disconnect`` message.

    Called only after the request body has been read to its end, so every message the server
    still delivers reports the connection, as in Starlette's ``listen_for_disconnect``: the next
    one arrives when the client goes away.
    """
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


async def _settle(*tasks: asyncio.Task[Any]) -> asyncio.CancelledError | None:
    """Wait until every task has ended, however often the caller is cancelled meanwhile; return
    the first such cancellation for the caller to raise, or ``None``.

    The shield keeps a cancelled anyio scope around the request from re-delivering its
    cancellation on every pass of the wait, which would spin the event loop.
    """
    received: asyncio.CancelledError | None = None
    with anyio.CancelScope(shield=True):
        while not all(task.done() for task in tasks):
            try:
                await asyncio.wait(tasks)
            except asyncio.CancelledError as exc:
                if received is None:
                    received = exc
    return received


def _retrieve(task: asyncio.Task[Any]) -> None:
    """Mark the ended ``task``'s exception as retrieved, so asyncio never reports it as
    unhandled when the caller answers otherwise."""
    if not task.cancelled():
        task.exception()


async def _serve_while_connected(
    request: Request, service: Coroutine[Any, Any, IngestPdfResponse]
) -> IngestPdfResponse | Response:
    """Run the ``service`` call to its end while watching for the client's disconnect.

    * The service ends first: its response is returned, or its exception raised, unchanged.
    * The client disconnects first: the service is cancelled, and this waits until it has ended,
      so the caller's parse slot is held until then and never longer. A service cancelled before
      its data commit starts kills its parser process, if one runs, and writes no filing or
      fundamentals row; one cancelled once that commit has started completes it and records its
      run ``succeeded`` (see ``filings_service.ingest_pdf``). The abandoned request is logged
      once at INFO and answered with a bare 499 (client closed request) that nobody receives:
      the server discards a response to a closed connection, so no error code or exception is
      raised for it.
    * The request itself is cancelled (a server shutdown, or a host's task group or timeout):
      the cancellation is passed to the service, and raised once the service has ended.

    The watcher reads the request's ``receive`` only after FastAPI has read the body to its end.
    A watcher that fails leaves the service to run to its end unwatched.
    """
    serving = asyncio.create_task(service)
    watching = asyncio.create_task(_client_disconnect(request.receive))
    waiting: set[asyncio.Task[Any]] = {serving, watching}
    disconnected = False
    try:
        while not serving.done() and not disconnected:
            await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            if serving.done() or not watching.done():
                continue
            # Only a watcher that returned saw a disconnect; one that failed or was cancelled saw
            # nothing, and the service then runs to its end unwatched.
            failure = asyncio.CancelledError() if watching.cancelled() else watching.exception()
            if failure is None:
                disconnected = True
            else:
                logger.warning(
                    "PDF upload: the client's connection cannot be watched: %s",
                    type(failure).__name__,
                )
                waiting = {serving}
    except asyncio.CancelledError:
        serving.cancel()
        watching.cancel()
        await _settle(serving, watching)
        _retrieve(serving)
        raise
    if not disconnected:
        watching.cancel()
        cancellation = await _settle(watching)
        if cancellation is not None:
            _retrieve(serving)
            raise cancellation
        return serving.result()
    serving.cancel()
    cancellation = await _settle(serving)
    _retrieve(serving)
    logger.info("PDF upload abandoned: the client disconnected before the answer")
    if cancellation is not None:
        raise cancellation
    return Response(status_code=_CLIENT_CLOSED_REQUEST)


# ---------------------------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------------------------


@router.post(
    "/ingest/pdf",
    response_model=IngestPdfResponse,
    summary="Extract financial line items from a filing or prospectus PDF",
    dependencies=[Depends(require_ingest_permission)],
    responses={
        400: {
            "description": (
                "The framework's 400 with a string detail: a multipart body the parser refuses, "
                "such as one without a boundary or with more than one file part, more than 32 "
                "fields or a field over 4 KiB, or a body that could not be read"
            ),
        },
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
                "Coded: pdf_encrypted, pdf_unreadable, pdf_parse_timeout (the parse reached its "
                "wall-clock limit, 20 s plus 0.4 s per page of TERMINAL_PDF_MAX_PAGES), "
                "too_many_pages, commit_requires_ticker_and_period or commit_requires_kes; or "
                "FastAPI's standard validation list for a single form field"
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
    request: Request,
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
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> IngestPdfResponse | Response:
    """Extract Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and Shares
    Outstanding from an uploaded PDF, and with ``commit`` merge them into the fundamentals.

    Returns 200 with the extraction for every accepted upload, with or without ``commit``:
    monetary values as 2-place decimal strings in whole units of the filing's currency (KES, or
    USD returned unconverted), share counts as integral-valued decimal strings in ``value`` with
    no fractional digits (``"40065428000"``), and a ``not_found`` entry for each missing item.
    Derivation and candidate values take the same form, while metadata such as ``page_count``,
    ``detected_scale``, ``scale_applied`` and ``page_number`` are JSON integers. Every 200
    upserts the ``terminal_corporate_filings`` row on the file's SHA-256; the raw bytes are never
    stored.

    Errors carry no extraction and write no filing or fundamentals row: 413
    ``request_too_large`` or ``file_too_large``, 415 ``not_a_pdf``, 429 ``ingest_busy`` (with
    ``Retry-After: 5``), and from the service 422 ``pdf_encrypted``, ``pdf_unreadable``,
    ``pdf_parse_timeout`` (the parse reached its wall-clock limit, 20 s plus 0.4 s per page of
    ``TERMINAL_PDF_MAX_PAGES``: 180 s at 400), ``too_many_pages``,
    ``commit_requires_ticker_and_period`` or ``commit_requires_kes``, 409 ``currency_conflict``,
    404 ``unknown_ticker`` and 500 ``internal_error`` for a server fault. The 413
    ``file_too_large``, 415 and 429 are decided before the file is loaded into memory. Send
    ``commit=false`` to preview what a PDF yields before committing it. A multipart body the
    parser refuses (for example one without a boundary, or with more than one file part, more
    than 32 fields or a field over 4 KiB) or a body that cannot be read is the framework's 400
    with a string ``detail``, decided before any dependency runs.

    A client that disconnects before its answer cancels the request, and its parse slot is freed
    once the service has ended. A disconnect before the service's data commit starts (during the
    run-row start, the pre-parse checks, the parse or the uncommitted writes) stops the parse, or
    keeps it from starting, writes no filing or fundamentals row and finishes the run row
    ``failed`` with ``cancelled``. A disconnect once that commit has started lets it complete:
    the filing, and any merged fundamentals, are stored and the run row is finished
    ``succeeded``, although no response is delivered. Either way the route logs it once at INFO
    and returns a bare 499, which the server discards because nobody is left to receive it.

    Every input is a form field, and the endpoint takes no query parameter: ``params`` carries
    none and is not read, and any query parameter, for example ``?foo=1``, is FastAPI's standard
    422 list with one ``extra_forbidden`` entry per unknown name, and the upload is neither parsed
    nor stored and records no run row.
    """
    if await _upload_size(file) > settings.pdf_max_bytes:
        raise _rejection(
            status.HTTP_413_CONTENT_TOO_LARGE,
            "file_too_large",
            f"The uploaded file exceeds {settings.pdf_max_bytes} bytes.",
            "file",
        )
    await file.seek(0)
    if await file.read(len(PDF_MAGIC)) != PDF_MAGIC:
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
    # also when the service raises, the request is cancelled or the client disconnects. A
    # refused request never loads its whole file, which the multipart parser holds spooled, so
    # whole-file memory grows with admitted parses only. A service call cancelled while parsing
    # kills its parser process and returns once its parse thread has ended, moments later; one
    # cancelled once its data commit has started returns when that commit and its run row are
    # written. The slot is held until then and no longer.
    async with limiter.slot(settings.pdf_max_concurrency) as admitted:
        if not admitted:
            raise _rejection(
                status.HTTP_429_TOO_MANY_REQUESTS,
                "ingest_busy",
                f"All {settings.pdf_max_concurrency} PDF parse slots are in use; retry shortly.",
                None,
                headers={"Retry-After": RETRY_AFTER_SECONDS},
            )
        await file.seek(0)
        data = await file.read(settings.pdf_max_bytes + 1)
        # The size checked above is the multipart parser's count; the bytes read are the proof.
        if len(data) > settings.pdf_max_bytes:
            raise _rejection(
                status.HTTP_413_CONTENT_TOO_LARGE,
                "file_too_large",
                f"The uploaded file exceeds {settings.pdf_max_bytes} bytes.",
                "file",
            )
        return await _serve_while_connected(
            request, filings_service.ingest_pdf(session, settings, data, file_name, form)
        )
