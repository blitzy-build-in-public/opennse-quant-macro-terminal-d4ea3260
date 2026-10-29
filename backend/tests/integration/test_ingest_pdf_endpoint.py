"""``POST /api/v1/terminal/ingest/pdf``: preview, commit preconditions and upload limits.

Every filing is written by ``helpers.pdf_builder`` through the ``statement_pdf`` fixture and parsed
by the product's own pdfplumber extractor, so no third-party report is committed. Committed values
are compared as exact ``Decimal`` values, response decimals must arrive as JSON strings, and a USD
statement can be previewed but is never committed, because nothing is converted between
currencies.

Visibility: the filing and fundamentals writes of a request happen inside the per-test outer
transaction, so they are read through ``db_session`` and rolled back after the test. The audit row
of ``terminal_ingestion_runs`` (``pipeline='pdf_filing'``, ``trigger='api'``) is written by the
service on a separate connection and persists in the module clone, so it is read through
``committed_query`` and always filtered by the upload's SHA-256. Each test prints a distinct tag
into its filing, which keeps every SHA-256, and with it every run-row filter, unique.

Rejections come from two layers. The route (size guard, file size, ``%PDF-`` magic bytes, parse
slots) records no run row; the service (commit preconditions, parse failures, currency rules)
records one ``failed`` run row carrying the error code. Either way the error body is the coded
``{"detail": {"code", "message", "field"}}`` with no extraction, and no filing or fundamentals row
is written. Only ``terminal_`` tables are read or written here.
"""

import asyncio
import hashlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, NoReturn, Protocol

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.api.routes.ingest import MULTIPART_OVERHEAD_BYTES
from opennse_terminal.ingestion.files.pdf_line_items import ITEM_COLUMNS
from opennse_terminal.models import TerminalCorporateFiling, TerminalFundamental
from opennse_terminal.services import filings_service

URL: Final = "/api/v1/terminal/ingest/pdf"

CommittedQuery = Callable[..., Awaitable[list[RowMapping]]]
"""The ``committed_query`` fixture: ``(sql, /, **params) -> rows`` on a separate connection."""
StatementPdf = Callable[..., bytes]
"""The ``statement_pdf`` fixture: ``(currency="KES", *, tag, pages=None) -> bytes``."""
SettingsConfigurer = Callable[..., object]
"""The ``configure_settings`` fixture: ``(**overrides) -> TerminalSettings``."""


class StatementExpectation(Protocol):
    """The ``statement_expected`` fixture: the six column values a KES commit writes."""

    @property
    def page_count(self) -> int: ...

    @property
    def detected_scale(self) -> int: ...

    def __getitem__(self, column: str, /) -> Decimal | int: ...


SCOM_PERIOD: Final = date(2026, 3, 31)
"""Seeded SCOM FY row (financial year ending 31 March); every column's provenance is ``seed``."""
SCOM_FORM: Final[Mapping[str, str]] = {
    "ticker": "SCOM",
    "period_end_date": SCOM_PERIOD.isoformat(),
    "period_type": "FY",
}
KCB_PERIOD: Final = date(2026, 12, 31)
"""A KCB period the seeder does not write (December counters stop at FY2025)."""

COMMITTED_COLUMNS: Final[tuple[str, ...]] = tuple(ITEM_COLUMNS.values())
"""The six ``terminal_fundamentals`` columns a commit of the test filing writes."""
MONEY_ITEMS: Final[frozenset[str]] = frozenset(ITEM_COLUMNS) - {"shares_outstanding"}
"""Line items whose unit is the filing's currency."""

PDF_MAX_BYTES_SMALL: Final = 1024
"""``pdf_max_bytes`` of the size-limit tests; the request bound is this plus 1 MiB."""
REQUEST_LIMIT_SMALL: Final = PDF_MAX_BYTES_SMALL + MULTIPART_OVERHEAD_BYTES
OVERSIZED_REQUEST_PAYLOAD: Final = b"%PDF-1.4\n" + b"0" * 1_100_000
"""A file part that alone exceeds the whole-request bound under ``pdf_max_bytes=1024``."""
OVERSIZED_FILE: Final = b"%PDF-" + b"0" * (PDF_MAX_BYTES_SMALL - 4)
"""1025 bytes: one byte over ``pdf_max_bytes=1024``, far inside the whole-request bound."""
NOT_A_PDF: Final = b"PK\x03\x04 not a pdf"

UNREADABLE: Final = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<< /Type /Catalog"
"""``%PDF-`` magic bytes followed by an unterminated catalog: no cross-reference table, no
trailer and no page tree, which pdfminer cannot recover a document from."""

CHUNK_SIZE: Final = 64 * 1024
BOUNDARY: Final = "opennse-terminal-test-boundary"

_TRAILER_ROOT: Final = b"/Root 1 0 R >>"
# A standard security handler (RC4, revision 2) whose /O and /U entries match no password, so the
# empty user password fails pdfminer's /U check. Inserted into the trailer, which follows the
# cross-reference table, it leaves every recorded object offset valid.
_ENCRYPTED_TRAILER_ROOT: Final = (
    "/Root 1 0 R /Encrypt << /Filter /Standard /V 1 /R 2 "
    f"/O <{'4f' * 32}> /U <{'55' * 32}> /P -4 >> "
    f"/ID [<{'0a' * 16}> <{'0b' * 16}>] >>"
).encode("ascii")

_RUN_ROWS_SQL: Final = (
    "SELECT status, trigger, error_message, finished_at IS NOT NULL AS finished, "
    "details->>'file_sha256' AS file_sha256, details->>'ticker' AS ticker, "
    "details->>'period_end_date' AS period_end_date, details->>'filing_id' AS filing_id "
    "FROM terminal_ingestion_runs "
    "WHERE pipeline = 'pdf_filing' AND details->>'file_sha256' = :sha "
    "ORDER BY started_at"
)


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


async def upload(client: httpx.AsyncClient, pdf: bytes, **form: str) -> httpx.Response:
    """POST ``pdf`` as the multipart ``file`` part, with ``form`` as the other form fields."""
    return await client.post(URL, files={"file": ("filing.pdf", pdf, "application/pdf")}, data=form)


def sha(data: bytes) -> str:
    """Lowercase hex SHA-256 of ``data``, the key of the filing row and the run-row filter."""
    return hashlib.sha256(data).hexdigest()


async def run_rows(committed_query: CommittedQuery, data: bytes) -> list[RowMapping]:
    """The committed ``pdf_filing`` run rows of the upload ``data``, oldest first."""
    return await committed_query(_RUN_ROWS_SQL, sha=sha(data))


async def filing_row(db_session: AsyncSession, data: bytes) -> RowMapping | None:
    """The ``terminal_corporate_filings`` row of the upload ``data`` as seen by the request.

    Selected as Core columns rather than ORM entities, so a value the service rewrote through an
    ``INSERT ... ON CONFLICT`` upsert is never served from the session's identity map.
    """
    result = await db_session.execute(
        select(TerminalCorporateFiling.__table__).where(
            TerminalCorporateFiling.file_sha256 == sha(data)
        )
    )
    return result.mappings().one_or_none()


async def fundamentals_row(
    db_session: AsyncSession, ticker: str, period_end: date, period_type: str = "FY"
) -> RowMapping | None:
    """The ``terminal_fundamentals`` row of ``(ticker, period_end, period_type)``, as Core rows."""
    result = await db_session.execute(
        select(TerminalFundamental.__table__).where(
            TerminalFundamental.ticker == ticker,
            TerminalFundamental.period_end_date == period_end,
            TerminalFundamental.period_type == period_type,
        )
    )
    return result.mappings().one_or_none()


def encrypted(pdf: bytes) -> bytes:
    """``pdf`` with a standard-security ``/Encrypt`` dictionary and ``/ID`` in its final trailer."""
    trailer_at = pdf.rindex(b"trailer")
    head, trailer = pdf[:trailer_at], pdf[trailer_at:]
    assert trailer.count(_TRAILER_ROOT) == 1, trailer
    return head + trailer.replace(_TRAILER_ROOT, _ENCRYPTED_TRAILER_ROOT)


def coded_error(
    response: httpx.Response, status_code: int, code: str, field: str | None
) -> dict[str, Any]:
    """Assert ``response`` is the coded error ``code`` on ``field`` with no extraction."""
    assert response.status_code == status_code, response.text
    body = response.json()
    assert set(body) == {"detail"}, body
    detail = body["detail"]
    assert isinstance(detail, dict), body
    assert detail["code"] == code, detail
    assert detail["field"] == field, detail
    assert isinstance(detail["message"], str) and detail["message"], detail
    return detail


async def chunked_multipart(payload: bytes) -> AsyncIterator[bytes]:
    """A one-part multipart body carrying ``payload`` as ``file``, in 64 KiB chunks.

    An async iterator has no known length, so httpx sends it with chunked transfer encoding and
    no ``Content-Length``.
    """
    body = (
        f"--{BOUNDARY}\r\n"
        'Content-Disposition: form-data; name="file"; filename="filing.pdf"\r\n'
        "Content-Type: application/pdf\r\n\r\n"
    ).encode("ascii")
    body += payload + f"\r\n--{BOUNDARY}--\r\n".encode("ascii")
    for start in range(0, len(body), CHUNK_SIZE):
        yield body[start : start + CHUNK_SIZE]


# ---------------------------------------------------------------------------------------------
# Preview and currency rules
# ---------------------------------------------------------------------------------------------


async def test_usd_statement_preview_returns_extraction(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    statement_pdf: StatementPdf,
    statement_expected: StatementExpectation,
) -> None:
    """A USD statement previews with HTTP 200: values in USD, unconverted, and nothing committed."""
    pdf = statement_pdf("USD", tag="dollar preview")
    before = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert before is not None

    response = await upload(client, pdf, **SCOM_FORM, commit="false")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["file_sha256"] == sha(pdf)
    assert body["currency"] == "USD"
    assert "non_kes_currency" in body["warnings"]
    assert body["committed"] is False
    assert body["fundamentals_id"] is None
    assert body["status"] == "extracted"
    assert body["missing_items"] == []
    items = {item["item"]: item for item in body["items"]}
    assert set(items) == set(ITEM_COLUMNS)
    for name, item in items.items():
        assert item["status"] == "found", item
        assert item["unit"] == ("USD" if name in MONEY_ITEMS else "shares"), item
        # Decimals travel as JSON strings; USD figures keep their printed digits times the scale.
        assert isinstance(item["value"], str), item
        assert Decimal(item["value"]) == statement_expected[ITEM_COLUMNS[name]], item

    # Every HTTP 200 upserts the filing row, preview or not.
    filing = await filing_row(db_session, pdf)
    assert filing is not None
    assert str(filing["id"]) == body["filing_id"]
    assert filing["currency"] == "USD"
    assert filing["committed_to_fundamentals"] is False
    after = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert after is not None
    assert dict(after) == dict(before)

    runs = await run_rows(committed_query, pdf)
    assert [(run["status"], run["trigger"], run["finished"]) for run in runs] == [
        ("succeeded", "api", True)
    ]
    assert runs[0]["filing_id"] == body["filing_id"]


async def test_usd_statement_commit_is_rejected_without_writes(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    statement_pdf: StatementPdf,
) -> None:
    """``commit=true`` on a USD statement is 422 ``commit_requires_kes`` and writes no data row.

    The failed request is visible only as its committed audit run row.
    """
    pdf = statement_pdf("USD", tag="dollar commit")
    before = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert before is not None

    response = await upload(client, pdf, **SCOM_FORM, commit="true")

    coded_error(response, 422, "commit_requires_kes", "commit")
    assert await filing_row(db_session, pdf) is None
    after = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert after is not None
    assert dict(after) == dict(before)

    runs = await run_rows(committed_query, pdf)
    assert len(runs) == 1, runs
    run = runs[0]
    assert run["status"] == "failed"
    assert run["trigger"] == "api"
    assert run["finished"] is True
    assert run["error_message"] == "commit_requires_kes"
    assert run["file_sha256"] == sha(pdf)
    assert run["ticker"] == "SCOM"
    assert run["period_end_date"] == SCOM_PERIOD.isoformat()


async def test_kes_commit_against_usd_row_conflicts(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    statement_pdf: StatementPdf,
) -> None:
    """A KES commit into a fundamentals row held in USD is 409 ``currency_conflict``.

    The USD row is committed by the session first, which releases its SAVEPOINT, so the
    service's rollback of the rejected request cannot be what leaves it unchanged.
    """
    db_session.add(
        TerminalFundamental(
            ticker="KCB",
            period_end_date=KCB_PERIOD,
            period_type="FY",
            currency="USD",
            revenue_kes=Decimal("1000.00"),
            source_by_field={"revenue_kes": "csv_import"},
        )
    )
    await db_session.commit()
    before = await fundamentals_row(db_session, "KCB", KCB_PERIOD)
    assert before is not None
    assert before["currency"] == "USD"
    pdf = statement_pdf("KES", tag="currency conflict")

    response = await upload(
        client,
        pdf,
        ticker="KCB",
        period_end_date=KCB_PERIOD.isoformat(),
        period_type="FY",
        commit="true",
    )

    coded_error(response, 409, "currency_conflict", "ticker")
    after = await fundamentals_row(db_session, "KCB", KCB_PERIOD)
    assert after is not None
    assert dict(after) == dict(before)
    assert after["revenue_kes"] == Decimal("1000.00")
    assert after["source_by_field"] == {"revenue_kes": "csv_import"}
    assert await filing_row(db_session, pdf) is None
    runs = await run_rows(committed_query, pdf)
    assert [(run["status"], run["error_message"]) for run in runs] == [
        ("failed", "currency_conflict")
    ]


async def test_commit_into_seeded_scom_row_updates_six_columns(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    statement_pdf: StatementPdf,
    statement_expected: StatementExpectation,
) -> None:
    """A KES commit writes exactly the six extracted columns and their provenance.

    Every other column of the seeded row keeps its value and its ``seed`` provenance, and a
    re-upload of the same bytes updates the same filing row.
    """
    before = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert before is not None
    seeded_provenance = dict(before["source_by_field"])
    assert set(seeded_provenance.values()) == {"seed"}
    assert set(COMMITTED_COLUMNS) <= set(seeded_provenance)
    pdf = statement_pdf("KES", tag="scom commit")

    response = await upload(client, pdf, **SCOM_FORM, commit="true")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["committed"] is True
    assert body["fundamentals_id"] == str(before["id"])
    assert body["currency"] == "KES"
    assert body["warnings"] == []
    assert body["page_count"] == statement_expected.page_count
    assert body["detected_scale"] == statement_expected.detected_scale
    assert [item["status"] for item in body["items"]] == ["found"] * len(ITEM_COLUMNS)

    after = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert after is not None
    provenance = after["source_by_field"]
    assert set(provenance) == set(seeded_provenance)
    for column in COMMITTED_COLUMNS:
        assert after[column] == statement_expected[column], column
        assert provenance[column] == "pdf_filing", column
    untouched = set(seeded_provenance) - set(COMMITTED_COLUMNS)
    assert untouched, seeded_provenance
    for column in untouched:
        assert provenance[column] == "seed", column
        assert after[column] == before[column], column
    assert after["currency"] == "KES"
    assert after["filing_id"] == uuid.UUID(body["filing_id"])

    filing = await filing_row(db_session, pdf)
    assert filing is not None
    assert filing["id"] == uuid.UUID(body["filing_id"])
    assert filing["committed_to_fundamentals"] is True
    assert filing["detected_scale"] == statement_expected.detected_scale
    assert filing["currency"] == "KES"
    assert filing["page_count"] == statement_expected.page_count
    assert filing["status"] == "extracted"
    assert filing["ticker"] == "SCOM"
    assert filing["period_end_date"] == SCOM_PERIOD

    # The same bytes again: the filing upsert on file_sha256 keeps its id.
    again = await upload(client, pdf, **SCOM_FORM, commit="true")

    assert again.status_code == 200, again.text
    assert again.json()["filing_id"] == body["filing_id"]
    assert again.json()["fundamentals_id"] == body["fundamentals_id"]
    final = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert final is not None
    for column in COMMITTED_COLUMNS:
        assert final[column] == statement_expected[column], column
    runs = await run_rows(committed_query, pdf)
    assert [(run["status"], run["filing_id"]) for run in runs] == [
        ("succeeded", body["filing_id"]),
        ("succeeded", body["filing_id"]),
    ]


# ---------------------------------------------------------------------------------------------
# Request validation and limits
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Rejection:
    """An upload and the coded error it must produce.

    ``pdf`` builds the uploaded bytes from the ``statement_pdf`` fixture; ``settings`` are
    ``configure_settings`` overrides; ``recorded`` is whether the request reaches the service,
    which records a ``failed`` run row carrying ``code`` (route-level rejections record none).
    """

    pdf: Callable[[StatementPdf], bytes]
    form: Mapping[str, str]
    status_code: int
    code: str
    field: str | None
    recorded: bool
    settings: Mapping[str, int] | None = None


SMALL_FILE_LIMIT: Final[Mapping[str, int]] = {"pdf_max_bytes": PDF_MAX_BYTES_SMALL}

REJECTIONS: Final = [
    pytest.param(
        Rejection(
            pdf=lambda _build: NOT_A_PDF,
            form={},
            status_code=415,
            code="not_a_pdf",
            field="file",
            recorded=False,
        ),
        id="not_a_pdf",
    ),
    pytest.param(
        Rejection(
            pdf=lambda _build: OVERSIZED_FILE,
            form={},
            status_code=413,
            code="file_too_large",
            field="file",
            recorded=False,
            settings=SMALL_FILE_LIMIT,
        ),
        id="file_too_large",
    ),
    # The commit preconditions and the ticker lookup are checked before parsing: an encrypted
    # file, which the parser would reject, still gets the precondition's error.
    pytest.param(
        Rejection(
            pdf=lambda build: encrypted(build(tag="missing ticker")),
            form={"period_end_date": SCOM_PERIOD.isoformat(), "commit": "true"},
            status_code=422,
            code="commit_requires_ticker_and_period",
            field="ticker",
            recorded=True,
        ),
        id="commit_requires_ticker",
    ),
    pytest.param(
        Rejection(
            pdf=lambda build: encrypted(build(tag="missing period")),
            form={"ticker": "SCOM", "commit": "true"},
            status_code=422,
            code="commit_requires_ticker_and_period",
            field="period_end_date",
            recorded=True,
        ),
        id="commit_requires_period",
    ),
    pytest.param(
        Rejection(
            pdf=lambda build: encrypted(build(tag="unknown ticker")),
            form={"ticker": "NOPE", "period_end_date": SCOM_PERIOD.isoformat(), "commit": "true"},
            status_code=404,
            code="unknown_ticker",
            field="ticker",
            recorded=True,
        ),
        id="unknown_ticker",
    ),
    pytest.param(
        Rejection(
            pdf=lambda build: encrypted(build(tag="encrypted")),
            form={},
            status_code=422,
            code="pdf_encrypted",
            field="file",
            recorded=True,
        ),
        id="pdf_encrypted",
    ),
    pytest.param(
        Rejection(
            pdf=lambda _build: UNREADABLE,
            form={},
            status_code=422,
            code="pdf_unreadable",
            field="file",
            recorded=True,
        ),
        id="pdf_unreadable",
    ),
    pytest.param(
        Rejection(
            pdf=lambda build: build(tag="page limit"),
            form={},
            status_code=422,
            code="too_many_pages",
            field="file",
            recorded=True,
            settings={"pdf_max_pages": 3},
        ),
        id="too_many_pages",
    ),
]


@pytest.mark.parametrize("rejection", REJECTIONS)
async def test_rejected_upload_writes_no_data(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    configure_settings: SettingsConfigurer,
    statement_pdf: StatementPdf,
    rejection: Rejection,
) -> None:
    """Each rejection answers its coded error, writes no filing row and is audited as stated."""
    if rejection.settings is not None:
        configure_settings(**rejection.settings)
    pdf = rejection.pdf(statement_pdf)

    response = await upload(client, pdf, **rejection.form)

    coded_error(response, rejection.status_code, rejection.code, rejection.field)
    assert await filing_row(db_session, pdf) is None
    runs = await run_rows(committed_query, pdf)
    expected = [("failed", "api", True, rejection.code)] if rejection.recorded else []
    assert [
        (run["status"], run["trigger"], run["finished"], run["error_message"]) for run in runs
    ] == expected


@pytest.mark.parametrize("chunked", [False, True], ids=["by_content_length", "without_length"])
async def test_request_too_large(
    client: httpx.AsyncClient,
    committed_query: CommittedQuery,
    configure_settings: SettingsConfigurer,
    chunked: bool,
) -> None:
    """A request beyond ``pdf_max_bytes`` plus 1 MiB is 413 ``request_too_large``.

    With ``Content-Length`` it is refused unread; a chunked body without one is cut off by the
    counting ``receive`` once it passes the bound. Neither reaches the service.
    """
    configure_settings(pdf_max_bytes=PDF_MAX_BYTES_SMALL)

    if chunked:
        response = await client.post(
            URL,
            content=chunked_multipart(OVERSIZED_REQUEST_PAYLOAD),
            headers={"Content-Type": f"multipart/form-data; boundary={BOUNDARY}"},
        )
        assert "content-length" not in response.request.headers
        assert response.request.headers["transfer-encoding"] == "chunked"
    else:
        response = await upload(client, OVERSIZED_REQUEST_PAYLOAD)
        assert int(response.request.headers["content-length"]) > REQUEST_LIMIT_SMALL

    coded_error(response, 413, "request_too_large", None)
    assert await run_rows(committed_query, OVERSIZED_REQUEST_PAYLOAD) == []


async def test_concurrency_limit_returns_429(
    client: httpx.AsyncClient,
    configure_settings: SettingsConfigurer,
    monkeypatch: pytest.MonkeyPatch,
    statement_pdf: StatementPdf,
) -> None:
    """With both parse slots held, a third upload is answered at once with 429 ``ingest_busy``.

    The service is replaced by one that holds its slot until released, so no parse or database
    write happens. The held requests are always released and awaited, which frees the
    process-wide semaphore for later tests and leaves no pending task.
    """
    configure_settings(pdf_max_concurrency=2)
    calls = 0
    both_inside = asyncio.Event()
    release = asyncio.Event()

    async def blocked(*_args: object, **_kwargs: object) -> NoReturn:
        nonlocal calls
        calls += 1
        if calls == 2:
            both_inside.set()
        await release.wait()
        raise HTTPException(409, detail={"code": "released", "message": "released", "field": None})

    monkeypatch.setattr(filings_service, "ingest_pdf", blocked)
    pdf = statement_pdf(tag="parse slots")
    held = [asyncio.create_task(upload(client, pdf)) for _ in range(2)]
    try:
        await asyncio.wait_for(both_inside.wait(), 10)

        response = await upload(client, pdf)

        coded_error(response, 429, "ingest_busy", None)
        assert response.headers["retry-after"] == "5"
    finally:
        release.set()
        outcomes = await asyncio.gather(*held, return_exceptions=True)

    assert calls == 2
    for outcome in outcomes:
        assert isinstance(outcome, httpx.Response), outcome
        coded_error(outcome, 409, "released", None)
