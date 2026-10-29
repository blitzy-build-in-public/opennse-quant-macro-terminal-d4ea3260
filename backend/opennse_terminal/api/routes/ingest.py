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
3. At most ``TERMINAL_PDF_MAX_CONCURRENCY`` (2) parses run per process. A request beyond it is
   answered at once with 429 ``ingest_busy`` and ``Retry-After: 5``; it never queues.

A request rejected by these limits writes nothing and records no run row. Every coded error is the
body ``{"detail": {"code", "message", "field"}}`` and carries no extraction. Single-field form
failures (an unknown ``document_type``, an over-long ``ticker``, an invalid date, a missing
``file``) stay FastAPI's standard 422 list.

The sub-router carries no prefix, tags, middleware or exception handlers: ``api/router.py`` mounts
it under ``/api/v1/terminal``, where the router-level principal dependency applies, and the route
adds ``require_ingest_permission``, which a host overrides to restrict ingestion. ``include_router``
preserves the route class, so the request-size guard travels with ``terminal_router`` into a host.

Annotations are evaluated at runtime (no ``from __future__ import annotations``), because FastAPI
reads the ``Annotated`` form and dependency metadata from the route signature.
"""

import asyncio
import inspect
import logging
from collections.abc import Callable, Coroutine, Mapping
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
from starlette.types import Message

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
)
from opennse_terminal.services import filings_service

__all__ = ["MULTIPART_OVERHEAD_BYTES", "TerminalUploadRoute", "ingest_pdf", "router"]

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

logger = logging.getLogger("opennse_terminal.api.routes.ingest")

_settings_fallback_warned = False
_parse_semaphore: tuple[int, asyncio.Semaphore] | None = None


# ---------------------------------------------------------------------------------------------
# Whole-request size guard
# ---------------------------------------------------------------------------------------------


class _RequestTooLargeError(Exception):
    """Raised by the counting ``receive`` once a body without ``Content-Length`` passes the
    request limit."""


async def _resolve_settings(request: Request) -> TerminalSettings:
    """The settings that govern this request, honouring a host's ``get_terminal_settings``
    override.

    The guard runs before FastAPI resolves any dependency, so it calls the provider itself: the
    override registered on the application in ``app.dependency_overrides``, else
    ``get_terminal_settings``. An override must be a zero-argument callable, as in
    ``lambda: TerminalSettings(...)``; it may be async. One that returns anything other than a
    ``TerminalSettings`` is logged once and the cached default settings apply instead.
    """
    global _settings_fallback_warned
    overrides: Mapping[Any, Any] = getattr(request.scope.get("app"), "dependency_overrides", {})
    provider = overrides.get(get_terminal_settings, get_terminal_settings)
    value = provider()
    if inspect.isawaitable(value):
        value = await value
    if isinstance(value, TerminalSettings):
        return value
    if not _settings_fallback_warned:
        _settings_fallback_warned = True
        logger.warning(
            "The get_terminal_settings override returned %s, not TerminalSettings; the upload "
            "size guard uses the default settings",
            type(value).__name__,
        )
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
      FastAPI's form parsing reads it from there.

    A router cannot carry middleware, and ``include_router`` preserves the route class, so the
    guard reaches every host that mounts ``terminal_router`` without any ingress setting. A host
    proxy may impose a stricter limit.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap FastAPI's handler with the request-size check."""
        # Built here, synchronously: FastAPI resolves an included route's context during this
        # call, so deferring it to request time would lose that context.
        original = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            settings = await _resolve_settings(request)
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

            async def counting_receive() -> Message:
                nonlocal received
                message = await request.receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    if received > limit:
                        raise _RequestTooLargeError
                return message

            counted = Request(request.scope, receive=counting_receive)
            # Read here, inside this try, because FastAPI turns any exception raised while it
            # parses the form into a 400 rather than letting this guard answer 413.
            try:
                await counted.body()
            except _RequestTooLargeError:
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


def _get_parse_semaphore(limit: int) -> asyncio.Semaphore:
    """The process-wide parse semaphore for ``limit`` slots, created on first use.

    It is recreated only when ``TERMINAL_PDF_MAX_CONCURRENCY`` changes; a request already holding
    a slot of the previous semaphore releases that one. The route never waits on it (it checks
    ``locked()`` and acquires only a free slot), so the semaphore never binds to an event loop.
    """
    global _parse_semaphore
    if _parse_semaphore is None or _parse_semaphore[0] != limit:
        _parse_semaphore = (limit, asyncio.Semaphore(limit))
    return _parse_semaphore[1]


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
            "model": ErrorResponse,
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
    semaphore = _get_parse_semaphore(settings.pdf_max_concurrency)
    # No await may come between this check and the acquisition below, so that checking for a
    # free slot and taking it cannot interleave with another request on the event loop.
    if semaphore.locked():
        raise _rejection(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "ingest_busy",
            f"All {settings.pdf_max_concurrency} PDF parse slots are in use; retry shortly.",
            None,
            headers={"Retry-After": RETRY_AFTER_SECONDS},
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
    async with semaphore:
        return await filings_service.ingest_pdf(session, settings, data, file_name, form)
