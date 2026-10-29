"""Integration fixtures: module clones, rollback-wrapped sessions, the ASGI app and request data.

These build on the root conftest's lazy PostgreSQL primitives (``database_factory``,
``seeded_template``, ``seeded_database``, ``make_settings``), which they use as defined there and
never redefine. Databases are created and dropped only through ``database_factory``.

Database scopes:

- ``module_database``: one seeded clone, ``opennse_test_<module>_<uuid8>``, per API-level module
  (``test_api_seeded``, ``test_host_mount``, ``test_watchlists``, ``test_ingest_pdf_endpoint``),
  dropped after the module.
- ``db_connection`` and ``db_session``: a per-test outer transaction on that clone, rolled back
  after the test. The session joins it with ``join_transaction_mode="create_savepoint"``, so a
  service's own ``commit()`` or ``rollback()`` acts on a SAVEPOINT only and every mutating request
  is invisible to later tests.
- ``committed_query``: reads on a separate connection, for the audit rows of
  ``terminal_ingestion_runs`` that the PDF endpoint writes outside the outer transaction.
- ``seeded_database`` (root, function scope): ``test_ingestion_pipelines`` and ``test_seeder``
  use it directly. The runs they exercise open their own connections, the advisory-lock
  connection and the separate run-record connection, which an outer transaction cannot wrap, so
  each of those tests gets a fresh clone instead.
- ``empty_database``: an empty ``opennse_iso_<uuid8>`` copied from ``template0`` per test, for
  ``test_migrations_isolation`` (the only module that creates simulated host objects) and the
  "not at head" cases of ``test_seeder``.

Application: ``app`` is ``create_app()`` with ``get_terminal_session`` served by ``db_session`` and
``get_terminal_settings`` pointing at the module clone; ``client`` drives it over ASGI;
``configure_settings`` and ``set_principal`` replace the settings and principal overrides
mid-test. Nothing registers middleware or exception handlers on the app.

Request data: ``quickmart_assumptions`` (the seeded IPO preset as a JSON body), ``statement_pdf``
(a deterministic four-page filing written with ``helpers.pdf_builder``) and
``statement_expected`` (the values a KES commit of that filing writes).

Every test collected from this directory is marked ``integration``. This file creates no table.
"""

import re
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal, Protocol

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from helpers.pdf_builder import Page, StatementRow, TextCell, build_pdf, statement_page, text_page
from opennse_terminal.api import (
    TerminalPrincipal,
    dispose_terminal_engine,
    get_terminal_principal,
    get_terminal_session,
    get_terminal_settings,
)
from opennse_terminal.config import TerminalSettings
from opennse_terminal.main import create_app

_HERE: Final[Path] = Path(__file__).resolve().parent
"""This directory; the marker hook marks only the items collected beneath it."""

STATEMENT_PAGE_COUNT: Final[int] = 4
"""Pages of the full ``statement_pdf`` document."""

STATEMENT_SCALE: Final[int] = 1_000
"""Scale the parser detects on every statement page ("KShs '000" or "USD in thousands")."""

_STATEMENT_COLUMNS: Final[tuple[tuple[str, float], ...]] = (
    ("Note", 300.0),
    ("2026", 400.0),
    ("2025", 480.0),
)
_SCALE_NOTES: Final[Mapping[str, str]] = MappingProxyType(
    {"KES": "KShs '000", "USD": "USD in thousands"}
)
_TAG_PATTERN: Final = re.compile(r"[A-Za-z_ ]+")
# Words the parser reads as a scale or currency marker anywhere on a page's opening lines, where
# the tag line also stands. A tag holding one would silently change the document's scale or
# currency, so it is refused.
_TAG_RESERVED_WORDS: Final[frozenset[str]] = frozenset(
    {
        "kshs",
        "kes",
        "shs",
        "ksh",
        "usd",
        "thousand",
        "thousands",
        "million",
        "millions",
        "mn",
        "billion",
        "billions",
        "bn",
    }
)
# Baseline of the tag footer on statement pages, far below their last row.
_FOOTER_X: Final[float] = 50.0
_FOOTER_Y: Final[float] = 60.0


class DatabaseHandleLike(Protocol):
    """The part of the root conftest's ``DatabaseHandle`` these fixtures use."""

    @property
    def url(self) -> str:
        """Connection URL of the database, password included."""

    @property
    def engine(self) -> AsyncEngine:
        """Engine on the database, owned and disposed by the factory."""


class DatabaseFactoryLike(Protocol):
    """The part of the root conftest's ``DatabaseFactory`` these fixtures use."""

    async def create_from_template(
        self, label: str, template: str | None = None
    ) -> DatabaseHandleLike:
        """Create ``opennse_test_<label>_<uuid8>`` as a copy of ``template``."""

    async def create_empty(self, label: str) -> DatabaseHandleLike:
        """Create an empty ``opennse_iso_[<label>_]<uuid8>`` from ``template0``."""

    async def drop(self, handle: DatabaseHandleLike) -> None:
        """Dispose the handle's engine and drop its database."""


class CommittedQuery(Protocol):
    """Signature of the ``committed_query`` fixture's reader."""

    async def __call__(self, sql: str, /, **params: object) -> list[RowMapping]: ...


class SettingsConfigurer(Protocol):
    """Signature of the ``configure_settings`` fixture's installer."""

    def __call__(self, **overrides: object) -> TerminalSettings: ...


class PrincipalSetter(Protocol):
    """Signature of the ``set_principal`` fixture's switch."""

    def __call__(self, subject: str | None, *, authenticated: bool = True) -> None: ...


class StatementPdfBuilder(Protocol):
    """Signature of the ``statement_pdf`` fixture's builder."""

    def __call__(
        self,
        currency: Literal["KES", "USD"] = "KES",
        *,
        tag: str,
        pages: int | None = None,
    ) -> bytes: ...


@dataclass(frozen=True, eq=False)
class StatementExpectation(Mapping[str, Decimal | int]):
    """What a KES commit of ``statement_pdf`` writes, read as a mapping of the six columns.

    Iterating, indexing and ``==`` cover only the ``terminal_fundamentals`` columns
    (``revenue_kes``, ``ebitda_kes``, ``net_income_kes``, ``total_debt_kes``,
    ``operating_cash_flow_kes`` as ``Decimal``; ``shares_outstanding`` as ``int``).
    ``page_count`` and ``detected_scale`` are the filing metadata the response and the
    ``terminal_corporate_filings`` row report.
    """

    columns: Mapping[str, Decimal | int]
    page_count: int
    detected_scale: int

    def __getitem__(self, key: str) -> Decimal | int:
        return self.columns[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.columns)

    def __len__(self) -> int:
        return len(self.columns)


def _with_footer(page: Page, footer: str) -> Page:
    """``page`` plus a single footer line at the bottom, below every statement row."""
    return Page(
        cells=(*page.cells, TextCell(footer, _FOOTER_X, _FOOTER_Y)),
        width=page.width,
        height=page.height,
    )


def build_statement_pdf(
    currency: Literal["KES", "USD"] = "KES",
    *,
    tag: str,
    pages: int | None = None,
) -> bytes:
    """Write the deterministic test filing, in which all six line items are ``found``.

    Pages: (1) statement of profit or loss with Revenue, EBITDA and Profit for the year;
    (2) statement of financial position with a Total borrowings line only, so the debt rule takes
    the total; (3) statement of cash flows with Net cash generated from operating activities;
    (4) notes with "Number of ordinary shares in issue 40,065,428,000", which carries no unit hint
    and so stays unscaled. Each statement page has the columns ``Note | 2026 | 2025`` and the
    marker "KShs '000" (KES) or "USD in thousands" (USD), both a x1,000 scale; the USD marker also
    makes the filing's currency USD. No other line matches a label the parser looks for.

    ``tag`` (letters, underscores and spaces; no scale or currency word) is printed on every page
    as "Prepared for <tag>", so each test's bytes, and with them its ``file_sha256``, are unique
    even when ``pages`` truncates the document. Digits are refused because the parser could read
    them as amounts. ``pages`` (1-4) keeps only the first N pages. Identical arguments give
    byte-identical output.

    Raises:
        ValueError: For an unknown currency, an invalid tag or ``pages`` outside 1-4.
    """
    marker = _SCALE_NOTES.get(currency)
    if marker is None:
        raise ValueError(f"currency must be 'KES' or 'USD', got {currency!r}")
    if _TAG_PATTERN.fullmatch(tag) is None or not tag.strip():
        raise ValueError(
            f"tag must hold only letters, underscores and spaces, and not be blank: {tag!r}"
        )
    reserved = sorted(_TAG_RESERVED_WORDS.intersection(tag.lower().split()))
    if reserved:
        raise ValueError(
            f"tag {tag!r} contains scale or currency words the parser would read: "
            f"{', '.join(reserved)}"
        )
    if pages is not None and not 1 <= pages <= STATEMENT_PAGE_COUNT:
        raise ValueError(f"pages must be 1-{STATEMENT_PAGE_COUNT} or None, got {pages!r}")

    footer = f"Prepared for {tag}"
    statements: tuple[tuple[str, tuple[StatementRow, ...]], ...] = (
        (
            "Statement of profit or loss",
            (
                StatementRow("Revenue", ["5", "371,234,567", "349,000,000"]),
                StatementRow("EBITDA", ["", "190,123,456", "175,000,000"]),
                StatementRow("Profit for the year", ["6", "64,321,987", "60,000,000"]),
            ),
        ),
        (
            "Statement of financial position",
            (StatementRow("Total borrowings", ["12", "98,765,432", "90,000,000"]),),
        ),
        (
            "Statement of cash flows",
            (
                StatementRow(
                    "Net cash generated from operating activities",
                    ["", "150,000,001", "140,000,000"],
                ),
            ),
        ),
    )
    document = [
        _with_footer(
            statement_page(title=title, scale_note=marker, columns=_STATEMENT_COLUMNS, rows=rows),
            footer,
        )
        for title, rows in statements
    ]
    document.append(
        text_page(
            [
                "Notes to the financial statements",
                "Number of ordinary shares in issue 40,065,428,000",
                footer,
            ]
        )
    )
    keep = STATEMENT_PAGE_COUNT if pages is None else pages
    return build_pdf(document[:keep])


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark ``integration`` every test collected from this directory.

    A sub-directory conftest's hook receives the whole session's items, so only those whose file
    lies under this directory are marked. ``tryfirst`` runs it before pytest's ``-m``
    deselection, so ``-m "not integration"`` sees the marks.
    """
    for item in items:
        if Path(item.path).resolve().is_relative_to(_HERE):
            item.add_marker(pytest.mark.integration)


# ---------------------------------------------------------------------------------------------
# Database scopes
# ---------------------------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module")
async def module_database(
    request: pytest.FixtureRequest,
    database_factory: DatabaseFactoryLike,
    seeded_template: str,
) -> AsyncIterator[DatabaseHandleLike]:
    """A seeded clone ``opennse_test_<module>_<uuid8>`` shared by one module, dropped after it.

    The label is the module name without ``test_`` (``api_seeded`` for ``test_api_seeded``).
    Tests mutate it only through ``db_connection``/``db_session``, whose writes roll back.
    """
    label = request.module.__name__.rpartition(".")[2].removeprefix("test_")
    handle = await database_factory.create_from_template(label, seeded_template)
    try:
        yield handle
    finally:
        await database_factory.drop(handle)


@pytest_asyncio.fixture
async def empty_database(
    database_factory: DatabaseFactoryLike,
) -> AsyncIterator[DatabaseHandleLike]:
    """A fresh empty database ``opennse_iso_<uuid8>`` from ``template0``, dropped afterwards.

    Never the seeded template or a clone: the isolation tests create host objects in it and run
    a destructive downgrade.
    """
    # No label: the factory then names the database opennse_iso_<uuid8>.
    handle = await database_factory.create_empty("")
    try:
        yield handle
    finally:
        await database_factory.drop(handle)


@pytest_asyncio.fixture
async def db_connection(module_database: DatabaseHandleLike) -> AsyncIterator[AsyncConnection]:
    """A connection to the module clone holding an outer transaction, rolled back after the test.

    Every write a request makes through ``db_session`` stays inside this transaction, so no later
    test sees it.
    """
    async with module_database.engine.connect() as connection:
        transaction = await connection.begin()
        try:
            yield connection
        finally:
            if transaction.is_active:
                await transaction.rollback()


@pytest_asyncio.fixture
async def db_session(db_connection: AsyncConnection) -> AsyncIterator[AsyncSession]:
    """An ``AsyncSession`` joined to ``db_connection``'s outer transaction through SAVEPOINTs.

    With ``join_transaction_mode="create_savepoint"``, ``commit()`` releases and ``rollback()``
    rolls back a SAVEPOINT only, so services keep their own transaction handling and the outer
    rollback still undoes everything. Rows written in a request are visible here.
    """
    session = AsyncSession(
        bind=db_connection,
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    )
    try:
        yield session
    finally:
        await session.close()


@pytest.fixture
def committed_query(module_database: DatabaseHandleLike) -> CommittedQuery:
    """Reader ``(sql, /, **params) -> list[RowMapping]`` on a separate connection to the clone.

    It sees only committed rows, such as the ``terminal_ingestion_runs`` audit rows the PDF
    endpoint and ``RunRecorder`` write on their own connection, outside the outer transaction.
    Those rows persist for the rest of the module, so filter them by a unique key (the upload's
    ``file_sha256`` or the run id). Rows written inside the request (filings, fundamentals,
    watchlists) are visible only through ``db_session``/``db_connection``, never here. ``sql``
    goes through ``text()`` with ``params`` as bound parameters.
    """
    engine = module_database.engine

    async def query(sql: str, /, **params: object) -> list[RowMapping]:
        async with engine.connect() as connection:
            result = await connection.execute(text(sql), params)
            return list(result.mappings().all())

    return query


# ---------------------------------------------------------------------------------------------
# Application and client
# ---------------------------------------------------------------------------------------------


@pytest_asyncio.fixture
async def app(
    module_database: DatabaseHandleLike,
    db_session: AsyncSession,
    make_settings: Callable[..., TerminalSettings],
) -> AsyncIterator[FastAPI]:
    """``create_app()`` serving every request from ``db_session`` on the module clone.

    ``get_terminal_settings`` is overridden with settings addressing the clone; the upload size
    guard reads the same override, so ``configure_settings`` limits apply to it as well.
    """
    application = create_app()
    settings = make_settings(database_url=module_database.url)

    async def _session_override() -> AsyncIterator[AsyncSession]:
        yield db_session

    application.dependency_overrides[get_terminal_session] = _session_override
    application.dependency_overrides[get_terminal_settings] = lambda: settings
    try:
        yield application
    finally:
        application.dependency_overrides.clear()
        get_terminal_settings.cache_clear()
        await dispose_terminal_engine()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An ``httpx.AsyncClient`` driving ``app`` over ASGI at ``http://test`` (no lifespan run)."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


@pytest.fixture
def configure_settings(
    app: FastAPI,
    module_database: DatabaseHandleLike,
    make_settings: Callable[..., TerminalSettings],
) -> SettingsConfigurer:
    """Installer ``(**overrides) -> TerminalSettings`` of the app's settings override.

    It builds ``make_settings(database_url=<module clone>, **overrides)``, installs it as the
    ``get_terminal_settings`` override and returns it, for example
    ``configure_settings(pdf_max_bytes=1024)``. An unknown field raises ``TypeError``.
    """

    def configure(**overrides: object) -> TerminalSettings:
        settings = make_settings(**{"database_url": module_database.url, **overrides})
        app.dependency_overrides[get_terminal_settings] = lambda: settings
        return settings

    return configure


@pytest.fixture
def set_principal(app: FastAPI) -> PrincipalSetter:
    """Switch ``(subject, *, authenticated=True)`` of the caller the app resolves.

    A subject installs a ``get_terminal_principal`` override returning
    ``TerminalPrincipal(subject, authenticated)``; ``None`` removes it, restoring the default
    unauthenticated principal ``local``. It can be called again mid-test to change identity. An
    invalid subject raises ``ValueError`` or ``TypeError`` from ``TerminalPrincipal``.
    """

    def set_subject(subject: str | None, *, authenticated: bool = True) -> None:
        if subject is None:
            app.dependency_overrides.pop(get_terminal_principal, None)
            return
        principal = TerminalPrincipal(subject=subject, is_authenticated=authenticated)

        async def _principal_override() -> TerminalPrincipal:
            return principal

        app.dependency_overrides[get_terminal_principal] = _principal_override

    return set_subject


# ---------------------------------------------------------------------------------------------
# Shared request data
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def quickmart_assumptions() -> dict[str, object]:
    """A fresh ``POST /ipo/valuation`` body holding the seeded Quickmart Supermarket preset.

    Exactly the 18 ``IpoValuationRequest`` fields: decimals as strings, counts as ``int``.
    """
    return {
        "offer_price_kes": "7.50",
        "revenue_growth_pct": "10.00",
        "ebitda_margin_pct": "4.50",
        "margin_expansion_pct": "0.50",
        "wacc_pct": "18.00",
        "terminal_growth_pct": "5.00",
        "net_debt_kes": "4000000000.00",
        "nwc_pct_revenue": "-3.00",
        "peer_ev_ebitda_multiple": "6.00",
        "dcf_weight_pct": "50.00",
        "capex_pct_revenue": "2.00",
        "da_pct_revenue": "1.50",
        "tax_rate_pct": "30.00",
        "forecast_years": 5,
        "base_revenue_kes": "45000000000.00",
        "shares_outstanding": 1_000_000_000,
        "shares_offered": 500_000_000,
        "primary_proceeds_kes": "0.00",
    }


@pytest.fixture
def statement_pdf() -> StatementPdfBuilder:
    """Builder ``(currency="KES", *, tag, pages=None) -> bytes`` of the test filing.

    See :func:`build_statement_pdf`; use a distinct ``tag`` per test.
    """
    return build_statement_pdf


@pytest.fixture
def statement_expected() -> StatementExpectation:
    """The values a KES commit of ``statement_pdf`` writes (period year 2026, scale x1,000).

    A mapping of the six ``terminal_fundamentals`` columns, with ``page_count`` (4) and
    ``detected_scale`` (1000) as attributes.
    """
    return StatementExpectation(
        columns=MappingProxyType(
            {
                "revenue_kes": Decimal("371234567000"),
                "ebitda_kes": Decimal("190123456000"),
                "net_income_kes": Decimal("64321987000"),
                "total_debt_kes": Decimal("98765432000"),
                "operating_cash_flow_kes": Decimal("150000001000"),
                "shares_outstanding": 40_065_428_000,
            }
        ),
        page_count=STATEMENT_PAGE_COUNT,
        detected_scale=STATEMENT_SCALE,
    )
