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
into its filing, which keeps every SHA-256, and with it every run-row filter, unique. The
binds-only test serves its requests from a host session with no default bind, configured only
through ``binds`` or a routing ``get_bind`` on that same connection, instead of ``db_session``.

Rejections come from two layers. The route (size guard, file size, ``%PDF-`` magic bytes, parse
slots) records no run row; the service (commit preconditions, parse failures, currency rules)
records one ``failed`` run row carrying the error code. Either way the error body is the coded
``{"detail": {"code", "message", "field"}}`` with no extraction, and no filing or fundamentals row
is written; a server fault is the coded 500 ``internal_error``, never the fault itself. Only
``terminal_`` tables are read or written here.

The last section calls ``filings_service.ingest_pdf`` directly, below the route, and holds the
service's own contract (run audit, preview and commit, statuses, currency, coded errors and
cancellation) on PDFs it writes with ``helpers.pdf_builder``. Its database tests each run on a
fresh seeded clone rather than the module clone; the section's banner states why.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import traceback
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Final, NoReturn, Protocol

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from sqlalchemy import Executable, Table, func, literal_column, select, text, update
from sqlalchemy.engine import Connection, Engine, RowMapping
from sqlalchemy.exc import IntegrityError, UnboundExecutionError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.sql import ClauseElement

from helpers.pdf_builder import Page, StatementRow, build_pdf, statement_page, text_page
from opennse_terminal.api import get_terminal_session
from opennse_terminal.api.routes.ingest import MULTIPART_OVERHEAD_BYTES
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.ingestion.base import RunRecorder, RunStatus
from opennse_terminal.ingestion.files.pdf_line_items import (
    ITEM_COLUMNS,
    LINE_ITEMS,
    ExtractionResult,
)
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalFundamental,
    TerminalIngestionRun,
)
from opennse_terminal.models.fundamentals import FUNDAMENTAL_LINE_ITEM_COLUMNS
from opennse_terminal.services import filings_service
from opennse_terminal.services import filings_service as fs
from opennse_terminal.services.filings_service import PdfIngestForm, ingest_pdf

URL: Final = "/api/v1/terminal/ingest/pdf"

CommittedQuery = Callable[[Executable], Awaitable[list[RowMapping]]]
"""The ``committed_query`` fixture: ``(statement, /) -> rows`` on a separate connection."""
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

_RUN_DETAIL_KEYS: Final[tuple[str, ...]] = ("file_sha256", "ticker", "period_end_date", "filing_id")
"""``details`` keys :func:`run_rows` projects as text columns of the same names."""


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
    run = TerminalIngestionRun
    return await committed_query(
        select(
            run.status,
            run.trigger,
            run.error_message,
            run.finished_at.is_not(None).label("finished"),
            *(run.details[key].astext.label(key) for key in _RUN_DETAIL_KEYS),
        )
        .where(run.pipeline == "pdf_filing", run.details["file_sha256"].astext == sha(data))
        .order_by(run.started_at)
    )


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
# A host's custom routing ``Session``, for the tests of how the terminal resolves its bind
# ---------------------------------------------------------------------------------------------
# SQLAlchemy's documented custom-partitioning pattern subclasses ``Session`` and overrides
# ``get_bind(mapper=None, clause=None, **kw)``, routing on ``mapper.class_``. Such a session has
# neither a default bind nor ``binds``, so the terminal reaches its database through it only when
# every connection it asks for names the target model's ``Mapper`` (or its ``terminal_`` table as
# the clause), as SQLAlchemy's own ORM statements do. Build one with
# ``AsyncSession(sync_session_class=RoutingSession, terminal_bind=<sync Engine or Connection>)``.


class RoutingSession(Session):
    """Routes the terminal's models to ``terminal_bind`` by mapper, or by table as the clause.

    Anything else falls through to ``Session.get_bind``, which, with no default bind and no
    ``binds``, raises ``UnboundExecutionError``. A mapped class passed where SQLAlchemy passes a
    ``Mapper`` has no ``class_`` and raises ``AttributeError``, as in a host's routing session.
    """

    def __init__(self, *, terminal_bind: Engine | Connection, **kw: Any) -> None:
        super().__init__(**kw)
        self.terminal_bind = terminal_bind

    def get_bind(  # type: ignore[override]
        self,
        mapper: Mapper[Any] | None = None,
        clause: ClauseElement | None = None,
        **kw: Any,
    ) -> Engine | Connection:
        if mapper is not None and issubclass(mapper.class_, TerminalBase):
            return self.terminal_bind
        if isinstance(clause, Table) and clause.name.startswith("terminal_"):
            return self.terminal_bind
        return super().get_bind(mapper, clause=clause, **kw)


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


HostSessionOptions = Callable[[AsyncConnection], dict[str, Any]]
"""Builds a host session's bind configuration onto the per-test connection: a ``binds`` mapping,
or a routing ``sync_session_class`` with its ``terminal_bind``."""

_XMIN: Final = literal_column("xmin::text")
"""The id of the (sub)transaction that last wrote a row, read from PostgreSQL's system column."""
_SCOM_FUNDAMENTALS_KEY: Final = (
    TerminalFundamental.ticker == "SCOM",
    TerminalFundamental.period_end_date == SCOM_PERIOD,
    TerminalFundamental.period_type == "FY",
)
"""``WHERE`` terms of the seeded SCOM FY fundamentals row the binds-only commit merges into."""


@pytest.mark.parametrize(
    ("tag", "session_options"),
    [
        pytest.param(
            "binds by base class",
            lambda conn: {"binds": {TerminalBase: conn}},
            id="base_class_binds",
        ),
        pytest.param(
            "binds per model",
            lambda conn: {
                "binds": {mapper.class_: conn for mapper in TerminalBase.registry.mappers}
            },
            id="per_model_binds",
        ),
        pytest.param(
            "routing get_bind",
            lambda conn: {
                "sync_session_class": RoutingSession,
                "terminal_bind": conn.sync_connection,
            },
            id="routing_get_bind",
        ),
    ],
)
async def test_binds_only_host_session_previews_and_commits(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    statement_pdf: StatementPdf,
    statement_expected: StatementExpectation,
    tag: str,
    session_options: HostSessionOptions,
) -> None:
    """A host session with no default bind, configured only through ``binds`` or a routing
    ``get_bind``, previews and commits a KES filing.

    The override maps the terminal's base class or every mapped class to the per-test
    connection, or routes the terminal's mappers to it as a host's custom ``get_bind`` reading
    ``mapper.class_`` does, and joins its outer transaction through SAVEPOINTs, so the outer
    rollback still undoes the writes. The preview upserts the filing row; the commit writes the
    filing and the fundamentals in one transaction, which another connection never sees.
    """
    host_session = AsyncSession(
        **session_options(db_connection),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    )
    assert host_session.sync_session.bind is None

    async def _host_session_override() -> AsyncIterator[AsyncSession]:
        yield host_session

    app.dependency_overrides[get_terminal_session] = _host_session_override
    before = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert before is not None
    pdf = statement_pdf("KES", tag=tag)

    try:
        preview = await upload(client, pdf, **SCOM_FORM, commit="false")
        previewed = await filing_row(db_session, pdf)
        commit = await upload(client, pdf, **SCOM_FORM, commit="true")
    finally:
        await host_session.close()

    assert preview.status_code == 200, preview.text
    assert preview.json()["committed"] is False
    assert previewed is not None
    assert str(previewed["id"]) == preview.json()["filing_id"]
    assert previewed["committed_to_fundamentals"] is False

    assert commit.status_code == 200, commit.text
    body = commit.json()
    assert body["committed"] is True
    assert body["filing_id"] == preview.json()["filing_id"]
    assert body["fundamentals_id"] == str(before["id"])
    after = await fundamentals_row(db_session, "SCOM", SCOM_PERIOD)
    assert after is not None
    for column in COMMITTED_COLUMNS:
        assert after[column] == statement_expected[column], column
        assert after["source_by_field"][column] == "pdf_filing", column
    assert after["filing_id"] == uuid.UUID(body["filing_id"])
    filing = await filing_row(db_session, pdf)
    assert filing is not None
    assert filing["committed_to_fundamentals"] is True

    # Rows written in one (sub)transaction carry its transaction id as xmin.
    filing_xmin = await db_session.scalar(
        select(_XMIN)
        .select_from(TerminalCorporateFiling)
        .where(TerminalCorporateFiling.file_sha256 == sha(pdf))
    )
    fundamentals_xmin = await db_session.scalar(
        select(_XMIN).select_from(TerminalFundamental).where(*_SCOM_FUNDAMENTALS_KEY)
    )
    assert filing_xmin is not None
    assert filing_xmin == fundamentals_xmin
    # Both writes stayed inside the outer transaction: another connection sees neither.
    assert (
        await committed_query(
            select(TerminalCorporateFiling.id).where(
                TerminalCorporateFiling.file_sha256 == sha(pdf)
            )
        )
        == []
    )
    outside = await committed_query(
        select(TerminalFundamental.source_by_field).where(*_SCOM_FUNDAMENTALS_KEY)
    )
    assert [row["source_by_field"] for row in outside] == [before["source_by_field"]]
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
    write happens. The held requests are always released and awaited, which frees the app's
    parse slots for later tests and leaves no pending task.
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


# ---------------------------------------------------------------------------------------------
# Server faults
# ---------------------------------------------------------------------------------------------


async def test_server_fault_is_answered_with_coded_internal_error_and_no_upload_text(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    committed_query: CommittedQuery,
    monkeypatch: pytest.MonkeyPatch,
    statement_pdf: StatementPdf,
) -> None:
    """A database fault mid-request reaches the client as the coded 500 ``internal_error`` JSON
    body, never as the fault itself, and is audited by its type.

    The fault is the filing insert naming a ticker no equity holds, once the pre-parse check is
    bypassed (as when the equity is deleted after it); its text carries the uploaded file name
    and company name, which the response must not.
    """

    async def no_preconditions(*_args: object) -> None:
        return None

    monkeypatch.setattr(filings_service, "_check_preconditions", no_preconditions)
    sentinel = "leak-sentinel-3b8f04"
    pdf = statement_pdf(tag="server fault")

    response = await client.post(
        URL,
        files={"file": (f"{sentinel}.pdf", pdf, "application/pdf")},
        data={
            "ticker": "ZZZZ",
            "company_name": f"{sentinel} Ltd",
            "period_end_date": SCOM_PERIOD.isoformat(),
        },
    )

    assert response.status_code == 500, response.text
    assert response.json() == {
        "detail": {
            "code": "internal_error",
            "message": "The PDF could not be ingested because of an internal error.",
            "field": None,
        }
    }
    assert sentinel not in response.text
    assert await filing_row(db_session, pdf) is None
    runs = await run_rows(committed_query, pdf)
    assert [
        (run["status"], run["trigger"], run["finished"], run["error_message"]) for run in runs
    ] == [("failed", "api", True, "unexpected_error:IntegrityError")]


# ---------------------------------------------------------------------------------------------
# Service contract: ``filings_service.ingest_pdf`` called directly, each test on its own clone
# ---------------------------------------------------------------------------------------------
# ``services/filings_service.py`` receives an upload the route has already bounded (request and
# file size, the ``%PDF-`` magic bytes, a parse slot) and owns everything after that:
#
# - Run audit: every call writes one ``terminal_ingestion_runs`` row on its own connection,
#   ``pipeline='pdf_filing'`` and ``trigger='api'``. An HTTP 200 ends it ``succeeded`` with
#   ``rows_written`` 1 (the filing) or 2 (filing and fundamentals); anything else ends it
#   ``failed`` with the error code as ``error_message`` (``unexpected_error:<Type>`` for a
#   server fault, ``cancelled`` for a cancelled request). ``details`` hold ``file_sha256``,
#   ``ticker``, ``period_end_date``, ``document_type`` and ``commit``, plus the outcome on
#   success. A row that cannot be started is logged once at ERROR by the fault's type, and no
#   row is written.
# - Preview and commit: every success upserts ``terminal_corporate_filings`` on
#   ``file_sha256``; a re-upload keeps the ``filing_id`` and never unsets
#   ``committed_to_fundamentals``. Money is rounded once to 2 dp (ROUND_HALF_UP) and share
#   counts are exact integers. A commit COALESCE-merges only ``found`` and ``derived`` items
#   into the fundamentals row of ``(ticker, period_end_date, period_type)``, sets ``filing_id``
#   and relabels exactly the written columns ``pdf_filing`` in ``source_by_field``, so every
#   other seeded column stays ``seed``. With nothing committable nothing is merged and
#   ``committed`` is false.
# - Statuses: ``extracted``; ``partial`` when some items are ``not_found`` (listed in
#   ``missing_items``); ``no_items`` when all are, as for a PDF without a text layer (warning
#   ``no_text_layer``).
# - Currency: a USD statement is previewed in USD, unconverted, with ``non_kes_currency``, and
#   is never committed.
# - Coded errors ``HTTPException(status, detail={"code", "message", "field"})``, each leaving
#   both tables untouched: 422 ``commit_requires_ticker_and_period``, ``pdf_encrypted``,
#   ``pdf_unreadable``, ``too_many_pages`` (``TERMINAL_PDF_MAX_PAGES``) and
#   ``commit_requires_kes``; 404 ``unknown_ticker``, with or without commit; 409
#   ``currency_conflict``; 500 ``internal_error`` for a server fault, raised ``from None`` so
#   neither the response, a log nor a traceback carries the fault's text.
# - Cancellation: a request cancelled while its run row is inserted, or before its commit,
#   rolls back and records ``cancelled``, and one cancelled while parsing re-raises only once
#   its parse thread has ended; one cancelled during its commit still commits and records
#   ``succeeded``, then re-raises; one whose row could not be started re-raises without a row.
#
# The request-size, magic-byte and concurrency checks (413, 415, 429) belong to the route, so
# the endpoint tests above exercise them and this section does not.
#
# Every PDF here is written in the test: by ``helpers.pdf_builder`` (the encrypted one with an
# ``/Encrypt`` trailer entry added by ``with_encrypt_entry``), except the unreadable one, a bare
# header. The real pdfplumber extractor parses them in its parser process. The pure helpers need
# no database. Every other test gets a fresh seeded clone, the root conftest's
# ``seeded_database``, instead of the module clone: these calls commit on their own session and
# the service writes its run row on its own connection, which no outer transaction can wrap, so
# each test changes only its own clone. The seeded SCOM FY 2026-03-31 fundamentals row carries
# all ten value columns, each labelled ``seed``. Every expected value is an exact ``Decimal`` or
# literal.

COLUMNS = [("Note", 300.0), ("2026", 400.0), ("2025", 480.0)]
# Value positions without any header token: a line with one number there is found, a line with
# two is ambiguous.
NO_HEADER_COLUMNS = [("", 400.0), ("", 480.0)]
YEAR_COLUMNS = [("2026", 400.0), ("2025", 480.0)]
THOUSANDS = "KShs '000"
WRITTEN_COLUMNS = (
    "revenue_kes",
    "ebitda_kes",
    "net_income_kes",
    "total_debt_kes",
    "operating_cash_flow_kes",
    "shares_outstanding",
)
SEED_ONLY_COLUMNS = ("total_assets_kes", "total_equity_kes", "cash_and_equivalents_kes", "dps_kes")
INTERNAL_ERROR_MESSAGE = "The PDF could not be ingested because of an internal error."
# Upload text that a server fault's own text may carry and nothing the service returns or logs.
LEAK_SENTINEL = "leak-sentinel-7d2e91"


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...

    @property
    def settings(self) -> TerminalSettings: ...


# ---------------------------------------------------------------------------------------------
# Service: documents, written in the test
# ---------------------------------------------------------------------------------------------


def statement(
    title: str,
    rows: Sequence[tuple[str, Sequence[str]]],
    *,
    scale_note: str | None = THOUSANDS,
    columns: Sequence[tuple[str, float]] = COLUMNS,
) -> Page:
    return statement_page(
        title=title,
        scale_note=scale_note,
        columns=columns,
        rows=[StatementRow(label, values) for label, values in rows],
    )


def with_encrypt_entry(pdf_bytes: bytes) -> bytes:
    """Add a standard security handler ``/Encrypt`` dictionary (RC4, revision 2) whose password
    check fails, and a file ``/ID``, to the trailer; the trailer follows the xref table, so
    every object offset stays valid."""
    owner = bytes(range(32)).hex()
    file_id = bytes(range(16)).hex()
    entry = (
        f"/Encrypt << /Filter /Standard /V 1 /R 2 /O <{owner}> /U <{'00' * 32}> /P -4 >> "
        f"/ID [<{file_id}> <{file_id}>]"
    )
    return pdf_bytes.replace(b"/Root 1 0 R >>", b"/Root 1 0 R " + entry.encode("ascii") + b" >>")


# All six items in KShs '000 on the 2026 column: EBITDA derived from operating profit plus D&A,
# and the share count on a notes page, left unscaled.
FULL_KES_PDF = build_pdf(
    [
        statement(
            "Statement of profit or loss",
            [
                ("Revenue", ["5", "1,234,567", "1,100,000"]),
                ("Operating profit", ["6", "400,000", "350,000"]),
                ("Depreciation and amortisation", ["7", "(50,000)", "(45,000)"]),
                ("Profit for the year", ["", "250,000", "200,000"]),
            ],
        ),
        statement(
            "Statement of financial position", [("Total borrowings", ["12", "5,500,000", "1"])]
        ),
        statement(
            "Statement of cash flows",
            [("Net cash generated from operating activities", ["", "600,000", "1"])],
        ),
        text_page(
            [
                "Notes to the financial statements",
                "Number of ordinary shares in issue 40,065,428,000",
            ]
        ),
    ]
)
# Without a header row, revenue's single number is found and net income's two numbers are
# ambiguous; the other four items are absent.
PARTIAL_KES_PDF = build_pdf(
    [
        statement(
            "Statement of profit or loss",
            [("Revenue", ["1,234,567", ""]), ("Profit for the year", ["250,000", "200,000"])],
            columns=NO_HEADER_COLUMNS,
        )
    ]
)
# A USD statement without a scale marker: amounts are dollars exactly as printed (scale x1).
USD_PDF = build_pdf(
    [
        statement(
            "Statement of profit or loss",
            [("Revenue", ["1,234.565", "1,100"]), ("Profit for the year", ["(12,345)", "20,000"])],
            scale_note="USD",
            columns=YEAR_COLUMNS,
        )
    ]
)
NO_TEXT_PDF = build_pdf([Page(cells=())])
THREE_PAGE_PDF = build_pdf([Page(cells=()), Page(cells=()), Page(cells=())])
ENCRYPTED_PDF = with_encrypt_entry(build_pdf([text_page(["Revenue 1,234"])]))
UNREADABLE_PDF = b"%PDF-1.4\nthis is not a PDF body\n"


def sha256(pdf_bytes: bytes) -> str:
    return hashlib.sha256(pdf_bytes).hexdigest()


def found(
    item: str,
    value: str,
    raw_value: str,
    page_number: int,
    label: str,
    *,
    unit: str = "KES",
    scale: int = 1000,
) -> dict[str, Any]:
    """The JSON form of a ``found`` item, decimals as strings."""
    return {
        "item": item,
        "status": "found",
        "value": value,
        "unit": unit,
        "raw_value": raw_value,
        "scale_applied": scale,
        "page_number": page_number,
        "matched_label": label,
        "derivation": [],
        "candidates": [],
    }


def not_found(item: str) -> dict[str, Any]:
    """The JSON form of a ``not_found`` item: every optional field null, both lists empty."""
    return {
        "item": item,
        "status": "not_found",
        "value": None,
        "unit": "shares" if item == "shares_outstanding" else "KES",
        "raw_value": None,
        "scale_applied": None,
        "page_number": None,
        "matched_label": None,
        "derivation": [],
        "candidates": [],
    }


FULL_KES_ITEMS = [
    found("revenue", "1234567000.00", "1,234,567", 1, "Revenue"),
    {
        "item": "ebitda",
        "status": "derived",
        "value": "450000000.00",
        "unit": "KES",
        "raw_value": None,
        "scale_applied": 1000,
        "page_number": 1,
        "matched_label": None,
        "derivation": [
            {"label": "Operating profit", "page_number": 1, "value": "400000000.00"},
            {"label": "Depreciation and amortisation", "page_number": 1, "value": "50000000.00"},
        ],
        "candidates": [],
    },
    found("net_income", "250000000.00", "250,000", 1, "Profit for the year"),
    found("total_debt", "5500000000.00", "5,500,000", 2, "Total borrowings"),
    found(
        "operating_cash_flow",
        "600000000.00",
        "600,000",
        3,
        "Net cash generated from operating activities",
    ),
    found(
        "shares_outstanding",
        "40065428000",
        "40,065,428,000",
        4,
        "Number of ordinary shares in issue",
        unit="shares",
        scale=1,
    ),
]
FULL_KES_VALUES: dict[str, Decimal | int] = {
    "revenue_kes": Decimal("1234567000.00"),
    "ebitda_kes": Decimal("450000000.00"),
    "net_income_kes": Decimal("250000000.00"),
    "total_debt_kes": Decimal("5500000000.00"),
    "operating_cash_flow_kes": Decimal("600000000.00"),
    "shares_outstanding": 40_065_428_000,
}


# ---------------------------------------------------------------------------------------------
# Service: fixtures and readers
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def engine(seeded_database: SeededDatabase) -> AsyncEngine:
    """The engine of this test's own seeded clone."""
    return seeded_database.engine


@pytest.fixture
def settings(seeded_database: SeededDatabase) -> TerminalSettings:
    """The clone's settings: at most 400 pages per PDF."""
    return seeded_database.settings


@pytest_asyncio.fixture
async def session(seeded_database: SeededDatabase) -> AsyncIterator[AsyncSession]:
    """The request session, on the clone's engine like the standalone session provider."""
    factory = async_sessionmaker(seeded_database.engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session


def reader(engine: AsyncEngine) -> AsyncSession:
    """A fresh session that sees only committed rows."""
    return AsyncSession(engine, expire_on_commit=False)


async def pdf_runs(engine: AsyncEngine) -> list[TerminalIngestionRun]:
    """The ``pdf_filing`` run rows, oldest first."""
    async with reader(engine) as db:
        rows = await db.scalars(
            select(TerminalIngestionRun)
            .where(TerminalIngestionRun.pipeline == "pdf_filing")
            .order_by(TerminalIngestionRun.started_at, TerminalIngestionRun.id)
        )
        return list(rows)


async def filing_by_sha(engine: AsyncEngine, file_sha256: str) -> TerminalCorporateFiling | None:
    async with reader(engine) as db:
        return await db.scalar(
            select(TerminalCorporateFiling).where(
                TerminalCorporateFiling.file_sha256 == file_sha256
            )
        )


async def filing_count(engine: AsyncEngine) -> int:
    async with reader(engine) as db:
        count = await db.scalar(select(func.count()).select_from(TerminalCorporateFiling))
    assert count is not None
    return count


async def scom_fundamentals(engine: AsyncEngine) -> dict[str, Any]:
    """The SCOM FY 2026-03-31 fundamentals row: id, values, provenance, link and timestamps."""
    async with reader(engine) as db:
        row = await db.scalar(
            select(TerminalFundamental).where(
                TerminalFundamental.ticker == "SCOM",
                TerminalFundamental.period_end_date == SCOM_PERIOD,
                TerminalFundamental.period_type == "FY",
            )
        )
    assert row is not None
    names = (
        "id",
        *FUNDAMENTAL_LINE_ITEM_COLUMNS,
        "filing_id",
        "source_by_field",
        "currency",
        "updated_at",
    )
    return {name: getattr(row, name) for name in names}


def run_details(
    file_sha256: str,
    *,
    ticker: str | None = "SCOM",
    period_end_date: str | None = "2026-03-31",
    document_type: str = "annual_report",
    commit: bool = False,
    **outcome: object,
) -> dict[str, object]:
    """The ``details`` JSON of a run row: the request fields, plus the outcome on success."""
    return {
        "file_sha256": file_sha256,
        "ticker": ticker,
        "period_end_date": period_end_date,
        "document_type": document_type,
        "commit": commit,
        **outcome,
    }


def assert_run(
    run: TerminalIngestionRun,
    *,
    status: str,
    error_message: str | None,
    rows_fetched: int,
    rows_written: int,
    details: dict[str, object],
) -> None:
    """``run`` is a finished ``pdf_filing`` API run with these outcomes."""
    assert (run.pipeline, run.trigger, run.source_url) == ("pdf_filing", "api", None)
    assert run.finished_at is not None
    assert (run.status, run.error_message) == (status, error_message)
    assert (run.rows_fetched, run.rows_written, run.rows_rejected) == (
        rows_fetched,
        rows_written,
        0,
    )
    assert run.details == details


def assert_coded(
    exc: HTTPException, status_code: int, code: str, field: str | None, message: str
) -> None:
    """``exc`` is the coded error ``{code, message, field}`` at ``status_code``."""
    assert exc.status_code == status_code
    assert exc.detail == {"code": code, "message": message, "field": field}


# ---------------------------------------------------------------------------------------------
# Service: pure helpers
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ticker", "normalized"),
    [
        pytest.param(" scom ", "SCOM", id="stripped_and_upper_cased"),
        pytest.param("Kcb", "KCB", id="upper_cased"),
        pytest.param("   ", None, id="blank"),
        pytest.param("", None, id="empty"),
        pytest.param(None, None, id="absent"),
    ],
)
def test_form_normalizes_the_ticker_and_defaults_to_a_six_item_preview(
    ticker: str | None, normalized: str | None
) -> None:
    """The ticker is stripped and upper-cased, and a blank one is no ticker (an unlisted IPO
    issuer). The other fields default to an ``annual_report`` ``FY`` preview of all six items."""
    form = PdfIngestForm(ticker=ticker)
    assert form.ticker == normalized
    assert (form.document_type, form.period_type, form.items, form.commit) == (
        "annual_report",
        "FY",
        LINE_ITEMS,
        False,
    )


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        pytest.param(
            HTTPException(409, detail={"code": "currency_conflict", "message": "m"}),
            "currency_conflict",
            id="coded",
        ),
        pytest.param(HTTPException(500, detail="Internal error"), "http_500", id="text_detail"),
        pytest.param(HTTPException(404, detail={"code": 7}), "http_404", id="non_text_code"),
    ],
)
def test_run_error_message_is_the_code_or_the_status(exc: HTTPException, code: str) -> None:
    """A coded error is recorded by its ``code``; any other ``HTTPException`` by its status."""
    assert fs._error_code(exc) == code


def test_an_extractor_item_outside_the_six_breaks_the_contract() -> None:
    """The extractor returns only the names it was asked for; any other name is a bug."""
    with pytest.raises(ValueError, match="^the extractor returned an unknown line item 'dps'$"):
        fs._line_item("dps")


@pytest.mark.parametrize(
    ("stored", "held"),
    [
        pytest.param("USD", "are held in USD", id="stored_currency"),
        pytest.param(None, "are not held in KES", id="row_gone"),
    ],
)
def test_currency_conflict_names_the_key_and_the_stored_currency(
    stored: str | None, held: str
) -> None:
    """The 409 names the period, the ticker and the stored currency, or says the row is no
    longer held in KES when it disappeared before it could be read."""
    exc = fs._currency_conflict(
        PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True), stored
    )
    assert_coded(
        exc,
        409,
        "currency_conflict",
        "ticker",
        f"The FY 2026-03-31 fundamentals of SCOM {held}; KES values cannot be merged into them.",
    )


async def test_run_rows_use_the_engine_behind_an_engine_or_binds_session(
    test_settings: TerminalSettings,
) -> None:
    """A session bound to an engine yields that engine; one configured through ``binds``, or
    through a routing ``get_bind`` that reads ``mapper.class_``, yields an ``AsyncEngine`` over
    the same sync engine. No resolution connects."""
    engine = create_async_engine(test_settings.database_url)
    try:
        assert fs._audit_engine(AsyncSession(bind=engine)) is engine
        resolved = fs._audit_engine(AsyncSession(binds={TerminalIngestionRun: engine}))
        assert resolved.sync_engine is engine.sync_engine
        routing = AsyncSession(sync_session_class=RoutingSession, terminal_bind=engine.sync_engine)
        assert routing.sync_session.bind is None
        assert fs._audit_engine(routing).sync_engine is engine.sync_engine
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------------------------
# Service: successful requests
# ---------------------------------------------------------------------------------------------


async def test_preview_stores_the_extraction_and_leaves_fundamentals_unchanged(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """``commit=false`` returns all six items, rounded once, and upserts the filing with the
    file name cut to 255 characters; the fundamentals row is not touched."""
    seeded = await scom_fundamentals(engine)
    sha = sha256(FULL_KES_PDF)

    response = await ingest_pdf(
        session,
        settings,
        FULL_KES_PDF,
        "x" * 300 + ".pdf",
        PdfIngestForm(ticker=" scom ", period_end_date=SCOM_PERIOD),
    )

    stored = await filing_by_sha(engine, sha)
    assert stored is not None
    assert response.model_dump(mode="json") == {
        "filing_id": str(stored.id),
        "file_sha256": sha,
        "page_count": 4,
        "detected_scale": 1000,
        "currency": "KES",
        "status": "extracted",
        "items": FULL_KES_ITEMS,
        "missing_items": [],
        "committed": False,
        "fundamentals_id": None,
        "warnings": [],
    }
    assert (stored.ticker, stored.company_name, stored.document_type) == (
        "SCOM",
        None,
        "annual_report",
    )
    assert (stored.period_end_date, stored.period_type) == (SCOM_PERIOD, "FY")
    assert stored.file_name == "x" * 255
    assert (stored.file_size_bytes, stored.page_count, stored.detected_scale) == (
        len(FULL_KES_PDF),
        4,
        1000,
    )
    assert stored.extraction == FULL_KES_ITEMS
    assert (stored.status, stored.currency, stored.committed_to_fundamentals) == (
        "extracted",
        "KES",
        False,
    )
    assert await scom_fundamentals(engine) == seeded
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=1,
        details=run_details(
            sha, filing_id=str(stored.id), status="extracted", committed=False, fundamentals_id=None
        ),
    )


async def test_commit_merges_every_item_and_a_later_preview_keeps_the_commit_flag(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """A commit into the seeded SCOM FY 2026-03-31 row writes the six items, links the filing and
    relabels exactly those six columns ``pdf_filing``; the four columns a filing does not carry
    keep their seeded values and ``seed``. Re-uploading the same bytes as a preview updates the
    same filing row, keeps ``committed_to_fundamentals`` and leaves the merged row alone."""
    seeded = await scom_fundamentals(engine)
    sha = sha256(FULL_KES_PDF)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)

    committed = await ingest_pdf(session, settings, FULL_KES_PDF, "scom-fy2026.pdf", form)

    assert (committed.committed, committed.fundamentals_id) == (True, seeded["id"])
    assert [item.model_dump(mode="json") for item in committed.items] == FULL_KES_ITEMS
    merged = await scom_fundamentals(engine)
    assert {column: merged[column] for column in WRITTEN_COLUMNS} == FULL_KES_VALUES
    assert {column: merged[column] for column in SEED_ONLY_COLUMNS} == {
        column: seeded[column] for column in SEED_ONLY_COLUMNS
    }
    assert merged["source_by_field"] == {
        **{column: "pdf_filing" for column in WRITTEN_COLUMNS},
        **{column: "seed" for column in SEED_ONLY_COLUMNS},
    }
    assert (merged["filing_id"], merged["currency"]) == (committed.filing_id, "KES")
    stored = await filing_by_sha(engine, sha)
    assert stored is not None
    assert (stored.id, stored.committed_to_fundamentals) == (committed.filing_id, True)

    preview = await ingest_pdf(
        session,
        settings,
        FULL_KES_PDF,
        "scom-fy2026-copy.pdf",
        PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD),
    )

    assert (preview.filing_id, preview.committed, preview.fundamentals_id) == (
        committed.filing_id,
        False,
        None,
    )
    restored = await filing_by_sha(engine, sha)
    assert restored is not None
    assert (restored.file_name, restored.committed_to_fundamentals) == (
        "scom-fy2026-copy.pdf",
        True,
    )
    assert await filing_count(engine) == 1
    assert await scom_fundamentals(engine) == merged
    first, second = await pdf_runs(engine)
    filing_id = str(committed.filing_id)
    assert_run(
        first,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=2,
        details=run_details(
            sha,
            commit=True,
            filing_id=filing_id,
            status="extracted",
            committed=True,
            fundamentals_id=str(seeded["id"]),
        ),
    )
    assert_run(
        second,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=1,
        details=run_details(
            sha, filing_id=filing_id, status="extracted", committed=False, fundamentals_id=None
        ),
    )


async def test_concurrent_first_preview_never_clears_the_flag_of_a_committing_upload(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """Two first uploads of the same bytes overlap. The commit has inserted the filing and merged
    the fundamentals but not committed, so the preview finds no filing row and its insert waits
    on the commit's unique-index entry. Once the commit lands, the preview's conflict update
    replaces the extraction on the same row but keeps ``committed_to_fundamentals`` true, as the
    fundamentals row's ``filing_id`` requires."""
    sha = sha256(FULL_KES_PDF)
    commit_form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)
    preview_form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD)
    result = await fs._extract(FULL_KES_PDF, commit_form, settings)
    items = [fs._response_item(item) for item in result.items]

    async def upload(db: AsyncSession, form: PdfIngestForm, file_name: str) -> uuid.UUID:
        return await fs._upsert_filing(
            db,
            form=form,
            file_name=file_name,
            file_sha256=sha,
            file_size_bytes=len(FULL_KES_PDF),
            result=result,
            items=items,
            commits_fundamentals=form.commit,
        )

    filing_id = await upload(session, commit_form, "commit.pdf")
    await fs._commit_fundamentals(session, commit_form, fs._fundamentals_values(items), filing_id)
    committer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
    async with AsyncSession(engine, expire_on_commit=False) as preview_session:
        preview_pid = await preview_session.scalar(text("SELECT pg_backend_pid()"))
        preview = asyncio.create_task(upload(preview_session, preview_form, "preview.pdf"))
        try:
            async with engine.connect() as monitor, asyncio.timeout(30):
                await monitor.execution_options(isolation_level="AUTOCOMMIT")
                while not await monitor.scalar(
                    text("SELECT :committer = ANY(pg_blocking_pids(:preview))"),
                    {"committer": committer_pid, "preview": preview_pid},
                ):
                    assert not preview.done(), "the preview never waited on the commit"
                    await asyncio.sleep(0.01)
        finally:
            # Committing on every path releases the preview, so it never outlives the test.
            await session.commit()
            preview_id = await asyncio.wait_for(preview, 30)
        await preview_session.commit()

    stored = await filing_by_sha(engine, sha)
    assert stored is not None
    assert (preview_id, stored.id) == (filing_id, filing_id)
    assert (stored.file_name, stored.committed_to_fundamentals) == ("preview.pdf", True)
    assert await filing_count(engine) == 1
    assert (await scom_fundamentals(engine))["filing_id"] == filing_id


async def test_partial_commit_writes_only_the_found_item_never_the_ambiguous_one(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """Revenue is ``found``; net income has two numbers and no header row, so it is
    ``ambiguous`` with both candidates; the other four are ``not_found``. Only ``revenue_kes`` is
    written, and the seeded net income keeps its value and ``seed`` label."""
    seeded = await scom_fundamentals(engine)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)

    response = await ingest_pdf(session, settings, PARTIAL_KES_PDF, "partial.pdf", form)

    assert response.status == "partial"
    assert response.missing_items == [
        "ebitda",
        "total_debt",
        "operating_cash_flow",
        "shares_outstanding",
    ]
    assert [item.model_dump(mode="json") for item in response.items] == [
        found("revenue", "1234567000.00", "1,234,567", 1, "Revenue"),
        not_found("ebitda"),
        {
            "item": "net_income",
            "status": "ambiguous",
            "value": None,
            "unit": "KES",
            "raw_value": None,
            "scale_applied": 1000,
            "page_number": 1,
            "matched_label": "Profit for the year",
            "derivation": [],
            "candidates": [
                {
                    "label": "Profit for the year",
                    "page_number": 1,
                    "raw_value": "250,000",
                    "value": "250000000.00",
                },
                {
                    "label": "Profit for the year",
                    "page_number": 1,
                    "raw_value": "200,000",
                    "value": "200000000.00",
                },
            ],
        },
        not_found("total_debt"),
        not_found("operating_cash_flow"),
        not_found("shares_outstanding"),
    ]
    assert (response.committed, response.fundamentals_id) == (True, seeded["id"])
    merged = await scom_fundamentals(engine)
    assert merged["revenue_kes"] == Decimal("1234567000.00")
    assert merged["source_by_field"] == {
        **{column: "seed" for column in (*WRITTEN_COLUMNS, *SEED_ONLY_COLUMNS)},
        "revenue_kes": "pdf_filing",
    }
    unchanged = [column for column in FUNDAMENTAL_LINE_ITEM_COLUMNS if column != "revenue_kes"]
    assert {column: merged[column] for column in unchanged} == {
        column: seeded[column] for column in unchanged
    }
    assert merged["filing_id"] == response.filing_id
    stored = await filing_by_sha(engine, sha256(PARTIAL_KES_PDF))
    assert stored is not None
    assert (stored.status, stored.committed_to_fundamentals) == ("partial", True)


async def test_commit_of_a_pdf_without_a_text_layer_merges_nothing(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """A scanned (image-only) PDF yields ``no_items`` with ``no_text_layer`` and no scale. With
    nothing committable the fundamentals row is untouched and ``committed`` is false, while the
    filing is still stored."""
    seeded = await scom_fundamentals(engine)
    sha = sha256(NO_TEXT_PDF)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)

    response = await ingest_pdf(session, settings, NO_TEXT_PDF, "scan.pdf", form)

    assert response.model_dump(mode="json") == {
        "filing_id": str(response.filing_id),
        "file_sha256": sha,
        "page_count": 1,
        "detected_scale": None,
        "currency": "KES",
        "status": "no_items",
        "items": [not_found(item) for item in LINE_ITEMS],
        "missing_items": list(LINE_ITEMS),
        "committed": False,
        "fundamentals_id": None,
        "warnings": ["no_text_layer"],
    }
    assert await scom_fundamentals(engine) == seeded
    stored = await filing_by_sha(engine, sha)
    assert stored is not None
    assert (stored.id, stored.status, stored.detected_scale, stored.committed_to_fundamentals) == (
        response.filing_id,
        "no_items",
        None,
        False,
    )
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=1,
        details=run_details(
            sha,
            commit=True,
            filing_id=str(response.filing_id),
            status="no_items",
            committed=False,
            fundamentals_id=None,
        ),
    )


async def test_usd_prospectus_preview_is_returned_unconverted_without_a_ticker(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """An unlisted issuer's USD prospectus is previewed by company name: items in USD exactly
    as printed (1,234.565 rounds half-up to 1234.57, the loss is negative), with
    ``scale_not_detected`` and ``non_kes_currency``; the filing is stored in USD."""
    sha = sha256(USD_PDF)
    form = PdfIngestForm(
        document_type="prospectus",
        company_name="Quickmart Supermarket",
        items=("revenue", "net_income"),
    )

    response = await ingest_pdf(session, settings, USD_PDF, "quickmart.pdf", form)

    stored = await filing_by_sha(engine, sha)
    assert stored is not None
    assert response.model_dump(mode="json") == {
        "filing_id": str(stored.id),
        "file_sha256": sha,
        "page_count": 1,
        "detected_scale": 1,
        "currency": "USD",
        "status": "extracted",
        "items": [
            found("revenue", "1234.57", "1,234.565", 1, "Revenue", unit="USD", scale=1),
            found(
                "net_income", "-12345.00", "(12,345)", 1, "Profit for the year", unit="USD", scale=1
            ),
        ],
        "missing_items": [],
        "committed": False,
        "fundamentals_id": None,
        "warnings": ["scale_not_detected", "non_kes_currency"],
    }
    assert (stored.ticker, stored.company_name, stored.document_type) == (
        None,
        "Quickmart Supermarket",
        "prospectus",
    )
    assert (stored.period_end_date, stored.currency, stored.committed_to_fundamentals) == (
        None,
        "USD",
        False,
    )
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="succeeded",
        error_message=None,
        rows_fetched=2,
        rows_written=1,
        details=run_details(
            sha,
            ticker=None,
            period_end_date=None,
            document_type="prospectus",
            filing_id=str(stored.id),
            status="extracted",
            committed=False,
            fundamentals_id=None,
        ),
    )


async def test_host_session_on_an_outer_transaction_keeps_the_run_row_after_rollback(
    engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """A host or test session joined to an outer transaction through a savepoint: the commit
    releases only the savepoint, so rolling the outer transaction back removes the filing and the
    merge, while the run row, written on the connection's engine, stays."""
    seeded = await scom_fundamentals(engine)
    sha = sha256(FULL_KES_PDF)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)

    async with engine.connect() as connection:
        outer = await connection.begin()
        host_session = AsyncSession(
            bind=connection, join_transaction_mode="create_savepoint", expire_on_commit=False
        )
        response = await ingest_pdf(host_session, settings, FULL_KES_PDF, "scom.pdf", form)
        inside = await connection.scalar(
            select(TerminalCorporateFiling.id).where(TerminalCorporateFiling.file_sha256 == sha)
        )
        await host_session.close()
        await outer.rollback()

    assert inside == response.filing_id
    assert response.committed is True
    assert await filing_by_sha(engine, sha) is None
    assert await scom_fundamentals(engine) == seeded
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=2,
        details=run_details(
            sha,
            commit=True,
            filing_id=str(response.filing_id),
            status="extracted",
            committed=True,
            fundamentals_id=str(seeded["id"]),
        ),
    )


# ---------------------------------------------------------------------------------------------
# Service: rejected requests
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("pdf_bytes", "form", "max_pages", "status_code", "code", "field", "message", "fetched"),
    [
        pytest.param(
            FULL_KES_PDF,
            PdfIngestForm(period_end_date=SCOM_PERIOD, commit=True),
            400,
            422,
            "commit_requires_ticker_and_period",
            "ticker",
            "commit=true requires both ticker and period_end_date; ticker is missing. "
            "Send commit=false to preview the extraction.",
            0,
            id="commit_without_ticker",
        ),
        pytest.param(
            FULL_KES_PDF,
            PdfIngestForm(ticker="SCOM", commit=True),
            400,
            422,
            "commit_requires_ticker_and_period",
            "period_end_date",
            "commit=true requires both ticker and period_end_date; period_end_date is missing. "
            "Send commit=false to preview the extraction.",
            0,
            id="commit_without_period",
        ),
        pytest.param(
            FULL_KES_PDF,
            PdfIngestForm(ticker="zzzz", period_end_date=SCOM_PERIOD),
            400,
            404,
            "unknown_ticker",
            "ticker",
            "Ticker 'ZZZZ' is not a known equity.",
            0,
            id="unknown_ticker_preview",
        ),
        pytest.param(
            ENCRYPTED_PDF,
            PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD),
            400,
            422,
            "pdf_encrypted",
            "file",
            "The PDF is encrypted; upload an unencrypted copy.",
            0,
            id="encrypted",
        ),
        pytest.param(
            UNREADABLE_PDF,
            PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD),
            400,
            422,
            "pdf_unreadable",
            "file",
            "The PDF could not be parsed.",
            0,
            id="unreadable",
        ),
        pytest.param(
            THREE_PAGE_PDF,
            PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD),
            2,
            422,
            "too_many_pages",
            "file",
            "The PDF has 3 pages; at most 2 can be parsed.",
            0,
            id="too_many_pages",
        ),
        pytest.param(
            USD_PDF,
            PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True),
            400,
            422,
            "commit_requires_kes",
            "commit",
            "The statement is presented in USD; only KES statements can be committed. "
            "Send commit=false to preview it.",
            6,
            id="usd_commit",
        ),
    ],
)
async def test_rejected_request_writes_neither_table_and_records_its_code(
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
    pdf_bytes: bytes,
    form: PdfIngestForm,
    max_pages: int,
    status_code: int,
    code: str,
    field: str,
    message: str,
    fetched: int,
) -> None:
    """Each rejection is a coded error with no extraction. No filing or fundamentals row is
    written or changed, and the run row ends ``failed`` with the code, counting the items parsed
    before a post-parse rejection."""
    seeded = await scom_fundamentals(engine)
    filings_before = await filing_count(engine)
    limited = settings.model_copy(update={"pdf_max_pages": max_pages})

    with pytest.raises(HTTPException) as excinfo:
        await ingest_pdf(session, limited, pdf_bytes, "rejected.pdf", form)

    assert_coded(excinfo.value, status_code, code, field, message)
    assert await filing_count(engine) == filings_before
    assert await scom_fundamentals(engine) == seeded
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="failed",
        error_message=code,
        rows_fetched=fetched,
        rows_written=0,
        details=run_details(
            sha256(pdf_bytes),
            ticker=form.ticker,
            period_end_date=(
                form.period_end_date.isoformat() if form.period_end_date is not None else None
            ),
            commit=form.commit,
        ),
    )


async def test_commit_into_a_non_kes_fundamentals_row_is_a_currency_conflict(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """A KES commit into a row held in USD is refused after parsing, before any write, and the
    merge itself refuses a row whose currency changed after that check (a concurrent writer), so
    values are never merged across currencies."""
    async with engine.begin() as connection:
        await connection.execute(
            update(TerminalFundamental)
            .where(
                TerminalFundamental.ticker == "SCOM",
                TerminalFundamental.period_end_date == SCOM_PERIOD,
                TerminalFundamental.period_type == "FY",
            )
            .values(currency="USD")
        )
    usd_row = await scom_fundamentals(engine)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)
    message = (
        "The FY 2026-03-31 fundamentals of SCOM are held in USD; "
        "KES values cannot be merged into them."
    )

    with pytest.raises(HTTPException) as excinfo:
        await ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form)

    assert_coded(excinfo.value, 409, "currency_conflict", "ticker", message)
    assert await filing_count(engine) == 0
    assert await scom_fundamentals(engine) == usd_row
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="failed",
        error_message="currency_conflict",
        rows_fetched=6,
        rows_written=0,
        details=run_details(sha256(FULL_KES_PDF), commit=True),
    )

    with pytest.raises(HTTPException) as merge_excinfo:
        await fs._commit_fundamentals(
            session, form, {"revenue_kes": Decimal("1234567000.00")}, uuid.uuid4()
        )
    await session.rollback()

    assert_coded(merge_excinfo.value, 409, "currency_conflict", "ticker", message)
    assert await scom_fundamentals(engine) == usd_row


async def test_unexpected_error_is_recorded_by_type_and_answered_as_internal_error(
    session: AsyncSession, engine: AsyncEngine, settings: TerminalSettings
) -> None:
    """A fault outside the coded errors (here the extractor refusing an empty item list) is
    recorded as ``unexpected_error:<Type>``, never with its text, and answered with the coded
    500 ``internal_error`` raised ``from None``, so no traceback of it chains the fault."""
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, items=())

    with pytest.raises(HTTPException) as excinfo:
        await ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form)

    assert_coded(excinfo.value, 500, "internal_error", None, INTERNAL_ERROR_MESSAGE)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__ is True
    assert await filing_count(engine) == 0
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="failed",
        error_message="unexpected_error:ValueError",
        rows_fetched=0,
        rows_written=0,
        details=run_details(sha256(FULL_KES_PDF)),
    )


async def test_failed_statement_is_answered_without_its_bound_parameters(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A statement that fails mid-request carries its bound parameters in its text: here the
    filing insert, whose ticker names no equity once the pre-parse check is bypassed (as when
    the equity is deleted after it), with the upload's file name and company name. The request
    is answered with the coded 500 alone, the run row records the fault by type, and neither a
    log record nor a traceback of the raised error holds those values."""

    async def no_preconditions(_session: AsyncSession, _form: PdfIngestForm) -> None:
        return None

    monkeypatch.setattr(fs, "_check_preconditions", no_preconditions)
    form = PdfIngestForm(
        ticker="ZZZZ", company_name=f"{LEAK_SENTINEL} Ltd", period_end_date=SCOM_PERIOD
    )

    with caplog.at_level(logging.DEBUG), pytest.raises(HTTPException) as excinfo:
        await ingest_pdf(session, settings, FULL_KES_PDF, f"{LEAK_SENTINEL}.pdf", form)

    fault = excinfo.value.__context__
    assert isinstance(fault, IntegrityError)
    assert f"{LEAK_SENTINEL}.pdf" in str(fault)
    assert f"{LEAK_SENTINEL} Ltd" in str(fault)
    assert_coded(excinfo.value, 500, "internal_error", None, INTERNAL_ERROR_MESSAGE)
    assert LEAK_SENTINEL not in "".join(traceback.format_exception(excinfo.value))
    assert await filing_count(engine) == 0
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="failed",
        error_message="unexpected_error:IntegrityError",
        rows_fetched=6,
        rows_written=0,
        details=run_details(sha256(FULL_KES_PDF), ticker="ZZZZ"),
    )
    assert LEAK_SENTINEL not in caplog.text
    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.ERROR
    ] == [
        (
            fs.logger.name,
            logging.ERROR,
            f"PDF ingestion run {run.id} failed: unexpected_error:IntegrityError",
        )
    ]


def unresolvable_audit_engine(_session: AsyncSession) -> AsyncEngine:
    """A host session whose binds name no engine for the run rows."""
    raise UnboundExecutionError(f"no bind is configured for {LEAK_SENTINEL}")


async def refused_run_start(_recorder: RunRecorder, **_kwargs: object) -> uuid.UUID:
    """A run-row insert whose pooled connection is refused."""
    raise ConnectionRefusedError(f"connection to {LEAK_SENTINEL} refused")


@pytest.mark.parametrize(
    ("owner", "attribute", "replacement", "fault"),
    [
        pytest.param(
            fs, "_audit_engine", unresolvable_audit_engine, "UnboundExecutionError", id="engine"
        ),
        pytest.param(
            RunRecorder, "start", refused_run_start, "ConnectionRefusedError", id="insert"
        ),
    ],
)
async def test_run_that_cannot_be_started_is_logged_once_and_answered_as_internal_error(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    owner: object,
    attribute: str,
    replacement: object,
    fault: str,
) -> None:
    """When the run row cannot be started, because the audit engine cannot be resolved or its
    insert fails, there is no row to finish: the fault is logged once at ERROR by its type,
    never by its text, the request is answered with the coded 500, and nothing is written."""
    seeded = await scom_fundamentals(engine)
    monkeypatch.setattr(owner, attribute, replacement)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)

    with caplog.at_level(logging.DEBUG), pytest.raises(HTTPException) as excinfo:
        await ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form)

    assert_coded(excinfo.value, 500, "internal_error", None, INTERNAL_ERROR_MESSAGE)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__ is True
    assert LEAK_SENTINEL not in "".join(traceback.format_exception(excinfo.value))
    assert LEAK_SENTINEL not in caplog.text
    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [
        (
            fs.logger.name,
            logging.ERROR,
            f"PDF ingestion run could not be started: unexpected_error:{fault}",
        )
    ]
    assert await pdf_runs(engine) == []
    assert await filing_count(engine) == 0
    assert await scom_fundamentals(engine) == seeded


async def test_run_row_that_cannot_be_finished_is_logged_not_raised(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    """Recording a final status never replaces the request's own outcome: a failure to write it
    is logged at ERROR by type and the call returns normally."""
    run_id = uuid.uuid4()

    with caplog.at_level(logging.ERROR, logger=fs.logger.name):
        cancel_requested = await fs._finish_run(
            RunRecorder(engine),
            run_id,
            run_status=RunStatus.FAILED,
            rows_fetched=0,
            rows_written=0,
            error_message="pdf_unreadable",
            details={},
        )

    assert cancel_requested is False
    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == fs.logger.name
    ] == [
        (logging.ERROR, f"PDF ingestion run {run_id}: recording status failed failed: LookupError")
    ]


# ---------------------------------------------------------------------------------------------
# Service: cancellation
# ---------------------------------------------------------------------------------------------


async def test_request_cancelled_while_its_run_row_is_inserted_finishes_the_row_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
) -> None:
    """A cancellation that arrives while the run row is inserted (here after the insert has
    committed, before its id is returned) never leaves the row ``running``: the insert
    completes, the row is finished ``failed`` with ``cancelled`` and the request's details,
    nothing else is written, and the cancellation is re-raised."""
    seeded = await scom_fundamentals(engine)
    inserted = asyncio.Event()
    release_start = asyncio.Event()
    real_start = RunRecorder.start

    async def gated_start(recorder: RunRecorder, **kwargs: Any) -> uuid.UUID:
        run_id = await real_start(recorder, **kwargs)
        inserted.set()
        await release_start.wait()
        return run_id

    monkeypatch.setattr(RunRecorder, "start", gated_start)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)
    request = asyncio.create_task(ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form))

    await asyncio.wait_for(inserted.wait(), 30)
    [running] = await pdf_runs(engine)
    assert (running.status, running.finished_at) == ("running", None)
    request.cancel()
    release_start.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert await filing_count(engine) == 0
    assert await scom_fundamentals(engine) == seeded
    [run] = await pdf_runs(engine)
    assert run.id == running.id
    assert_run(
        run,
        status="failed",
        error_message="cancelled",
        rows_fetched=0,
        rows_written=0,
        details=run_details(sha256(FULL_KES_PDF), commit=True),
    )


@pytest.mark.parametrize(
    ("raised", "cancel_request", "level", "message"),
    [
        pytest.param(
            ConnectionRefusedError,
            True,
            logging.ERROR,
            "PDF ingestion run could not be started: unexpected_error:ConnectionRefusedError",
            id="insert_fails_after_the_request_is_cancelled",
        ),
        pytest.param(
            asyncio.CancelledError,
            False,
            logging.WARNING,
            "PDF ingestion run could not be started: cancelled",
            id="insert_itself_cancelled",
        ),
    ],
)
async def test_cancellation_meeting_a_run_row_never_started_is_re_raised_without_a_row(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    raised: type[BaseException],
    cancel_request: bool,
    level: int,
    message: str,
) -> None:
    """A run-row insert that fails while the request is being cancelled, or that itself ends
    cancelled, leaves no row to finish: the cancellation is re-raised rather than answered,
    nothing is written, and one record is logged, never with the fault's text. A failed insert
    is an ERROR naming its type; an insert that ended cancelled is a WARNING ``cancelled``, not
    a fault."""
    starting = asyncio.Event()
    release_start = asyncio.Event()

    async def failing_start(_recorder: RunRecorder, **_kwargs: object) -> uuid.UUID:
        starting.set()
        await release_start.wait()
        raise raised(LEAK_SENTINEL)

    monkeypatch.setattr(RunRecorder, "start", failing_start)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD)

    with caplog.at_level(logging.DEBUG):
        request = asyncio.create_task(ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form))
        await asyncio.wait_for(starting.wait(), 30)
        if cancel_request:
            request.cancel()
        release_start.set()
        with pytest.raises(asyncio.CancelledError):
            await request

    assert LEAK_SENTINEL not in caplog.text
    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(fs.logger.name, level, message)]
    assert await pdf_runs(engine) == []
    assert await filing_count(engine) == 0


async def test_request_cancelled_while_parsing_rolls_back_and_records_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
) -> None:
    """A request cancelled while its PDF is being parsed re-raises the cancellation only once its
    parse thread has ended, writes no filing or fundamentals row and finishes its run row
    ``failed`` with ``cancelled``."""
    seeded = await scom_fundamentals(engine)
    parse_started = threading.Event()
    release_parse = threading.Event()
    parse_done = threading.Event()
    real_extract = fs.extract_line_items

    def gated_extract(*args: Any, **kwargs: Any) -> ExtractionResult:
        parse_started.set()
        try:
            release_parse.wait(30)
            return real_extract(*args, **kwargs)
        finally:
            parse_done.set()

    monkeypatch.setattr(fs, "extract_line_items", gated_extract)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)
    request = asyncio.create_task(ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form))

    assert await asyncio.to_thread(parse_started.wait, 30)
    request.cancel()
    done, _ = await asyncio.wait({request}, timeout=0.5)
    assert not done, "the cancelled request ended while its parse thread was still running"
    assert not parse_done.is_set()
    release_parse.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert parse_done.is_set()

    assert await filing_count(engine) == 0
    assert await scom_fundamentals(engine) == seeded
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="failed",
        error_message="cancelled",
        rows_fetched=0,
        rows_written=0,
        details=run_details(sha256(FULL_KES_PDF), commit=True),
    )


async def test_request_cancelled_during_its_commit_still_commits_and_records_success(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    engine: AsyncEngine,
    settings: TerminalSettings,
) -> None:
    """A cancellation that arrives while the transaction commits never leaves its outcome
    unknown: the commit completes, the run row ends ``succeeded``, and only then is the
    cancellation re-raised."""
    seeded = await scom_fundamentals(engine)
    committing = asyncio.Event()
    release_commit = asyncio.Event()
    real_commit = session.commit

    async def gated_commit() -> None:
        committing.set()
        await release_commit.wait()
        await real_commit()

    monkeypatch.setattr(session, "commit", gated_commit)
    form = PdfIngestForm(ticker="SCOM", period_end_date=SCOM_PERIOD, commit=True)
    request = asyncio.create_task(ingest_pdf(session, settings, FULL_KES_PDF, "scom.pdf", form))

    await asyncio.wait_for(committing.wait(), 60)
    request.cancel()
    release_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await request

    stored = await filing_by_sha(engine, sha256(FULL_KES_PDF))
    assert stored is not None
    assert stored.committed_to_fundamentals is True
    merged = await scom_fundamentals(engine)
    assert {column: merged[column] for column in WRITTEN_COLUMNS} == FULL_KES_VALUES
    assert merged["filing_id"] == stored.id
    [run] = await pdf_runs(engine)
    assert_run(
        run,
        status="succeeded",
        error_message=None,
        rows_fetched=6,
        rows_written=2,
        details=run_details(
            sha256(FULL_KES_PDF),
            commit=True,
            filing_id=str(stored.id),
            status="extracted",
            committed=True,
            fundamentals_id=str(seeded["id"]),
        ),
    )
