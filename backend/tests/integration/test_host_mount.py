"""A foreign FastAPI host mounts ``terminal_router`` with one line, and nothing collides.

The host declares its own ``GET /health``, ``GET /api/v1/users/me`` and ``GET /api/v1/items``,
then calls ``include_router(terminal_router)``, the only line a host adds. It serves terminal
requests from its own session provider, supplies its own settings and maps its user to the
principal subject ``host-user-42``. The session provider is bound to ``db_connection``'s outer
transaction through SAVEPOINTs, so every write rolls back after its test. The tests prove that:

- every ``(method, path)`` and every OpenAPI ``operationId`` is unique, the 16 terminal operations
  sit under ``/api/v1/terminal/`` with ``terminal_``-prefixed IDs, the upload route keeps its
  ``TerminalUploadRoute`` class, and the host's routes, middleware, exception handlers and
  startup and shutdown hooks are left as the host declared them;
- the host's session and principal overrides serve the screener and own the watchlist it creates;
- a host session with no default bind, configured only through ``binds``, answers ``/health``;
- a principal override raising 401 blocks every terminal operation while the host's own routes
  still answer 200;
- the whole-request upload bound answers 413 inside the host with no host configuration at all.

Authentication stays pluggable through dependency injection: ownership is the opaque principal
subject stored as ``owner_key``, and no users or auth table is read or created.
"""

import hashlib
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Protocol

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException, status
from fastapi.routing import APIRoute, iter_route_contexts
from sqlalchemy import Select, Table, func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from opennse_terminal import __version__
from opennse_terminal.api import (
    TerminalPrincipal,
    dispose_terminal_engine,
    get_terminal_principal,
    get_terminal_session,
    get_terminal_settings,
    terminal_router,
)
from opennse_terminal.api.routes.ingest import TerminalUploadRoute
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalIngestionRun,
    TerminalWatchlist,
)

TERMINAL_PREFIX: Final = "/api/v1/terminal"
"""The mount prefix a host relies on, stated here as the contract rather than read from the
router, so a router that loses its prefix fails these tests."""

HOST_SUBJECT: Final = "host-user-42"

HOST_PDF_MAX_BYTES: Final = 4096
"""Per-file PDF limit in the host's settings. The whole-request bound is this plus 1 MiB of
multipart overhead, so the size-guard test needs a body of only about 1.1 MB."""

MULTIPART_OVERHEAD_BYTES: Final = 1_048_576

EXPECTED_OPERATIONS: Final[tuple[tuple[str, str, str], ...]] = (
    ("GET", "/health", "get_health"),
    ("GET", "/macro/overview", "get_macro_overview"),
    ("GET", "/market/screener", "get_screener"),
    ("GET", "/market/sector-heatmap", "get_sector_heatmap"),
    ("GET", "/market/valuation-bands", "get_valuation_bands"),
    ("GET", "/market/anomalies", "get_anomalies"),
    ("GET", "/market/flows", "get_flows"),
    ("GET", "/cycles/backtest", "get_cycle_backtest"),
    ("GET", "/ipo/scenarios", "list_ipo_scenarios"),
    ("POST", "/ipo/valuation", "value_ipo"),
    ("POST", "/ingest/pdf", "ingest_pdf"),
    ("GET", "/watchlists", "list_watchlists"),
    ("POST", "/watchlists", "create_watchlist"),
    ("DELETE", "/watchlists/{watchlist_id}", "delete_watchlist"),
    ("PUT", "/watchlists/{watchlist_id}/items/{ticker}", "add_watchlist_item"),
    ("DELETE", "/watchlists/{watchlist_id}/items/{ticker}", "remove_watchlist_item"),
)
"""``(method, path relative to the prefix, endpoint function name)`` of every terminal operation;
each ``operationId`` is ``terminal_<function name>``."""

HOST_RESPONSES: Final[Mapping[tuple[str, str], object]] = {
    ("GET", "/health"): {"status": "host-ok"},
    ("GET", "/api/v1/users/me"): {"id": HOST_SUBJECT},
    ("GET", "/api/v1/items"): [],
}
"""The host's own routes and the bodies they answer."""

_HTTP_METHODS: Final = frozenset({"get", "put", "post", "delete", "patch", "options", "head"})

BindsFor = Callable[[AsyncConnection], dict[type[TerminalBase] | Table, AsyncConnection]]
"""Builds a binds-only host session's ``binds`` mapping onto the per-test connection."""


class _DatabaseHandle(Protocol):
    """The part of the ``module_database`` handle this module uses."""

    @property
    def url(self) -> str:
        """Connection URL of the module clone, password included."""
        ...


@dataclass
class HostApp:
    """The host application, with what it held before ``include_router(terminal_router)``.

    ``sessions_opened`` counts the sessions the host's own session provider has yielded.
    """

    app: FastAPI
    host_routes: dict[tuple[str, str], APIRoute]
    middleware_count: int
    exception_handlers: dict[Any, Any]
    startup_count: int
    shutdown_count: int
    sessions_opened: int = 0


def _api_operations(app: FastAPI) -> list[tuple[str, str, APIRoute]]:
    """Every ``(method, mounted path, route)`` the app serves through an ``APIRoute``.

    FastAPI 0.162 keeps an included router as a single entry of ``app.routes``.
    ``iter_route_contexts``, the walk FastAPI's own OpenAPI generator uses, yields each effective
    route with its mounted path; ``original_route`` is the route object the sub-router declared,
    so it carries the sub-router's route class.
    """
    operations: list[tuple[str, str, APIRoute]] = []
    for context in iter_route_contexts(app.routes):
        route = context.original_route
        path = context.path
        if not isinstance(route, APIRoute) or path is None:
            continue
        operations.extend((method, path, route) for method in sorted(context.methods or ()))
    return operations


async def _count(connection: AsyncConnection, statement: Select[tuple[int]]) -> int:
    """Run a ``SELECT count(*)`` statement, its values bound inside it, and return the count."""
    return int((await connection.execute(statement)).scalar_one())


@pytest_asyncio.fixture
async def host_app(
    module_database: _DatabaseHandle,
    db_connection: AsyncConnection,
    make_settings: Callable[..., object],
) -> AsyncIterator[HostApp]:
    """A host platform with its own routes that mounts ``terminal_router`` and overrides the
    terminal's session, settings and principal providers with its own."""
    host = FastAPI(title="Host platform")

    @host.get("/health")
    async def host_health() -> dict[str, str]:
        return {"status": "host-ok"}

    @host.get("/api/v1/users/me")
    async def read_current_user() -> dict[str, str]:
        return {"id": HOST_SUBJECT}

    @host.get("/api/v1/items")
    async def list_items() -> list[dict[str, str]]:
        return []

    mounted = HostApp(
        app=host,
        host_routes={(method, path): route for method, path, route in _api_operations(host)},
        middleware_count=len(host.user_middleware),
        exception_handlers=dict(host.exception_handlers),
        startup_count=len(host.router.on_startup),
        shutdown_count=len(host.router.on_shutdown),
    )
    host.include_router(terminal_router)

    # A typical host session factory, keeping SQLAlchemy's default expire_on_commit=True. Bound
    # to the test's outer transaction, a service's commit only releases a SAVEPOINT.
    host_sessionmaker = async_sessionmaker(
        bind=db_connection, join_transaction_mode="create_savepoint"
    )

    async def host_get_async_session() -> AsyncIterator[AsyncSession]:
        mounted.sessions_opened += 1
        async with host_sessionmaker() as session:
            yield session

    async def host_get_terminal_principal() -> TerminalPrincipal:
        return TerminalPrincipal(subject=HOST_SUBJECT, is_authenticated=True)

    settings = make_settings(database_url=module_database.url, pdf_max_bytes=HOST_PDF_MAX_BYTES)
    host.dependency_overrides[get_terminal_session] = host_get_async_session
    host.dependency_overrides[get_terminal_settings] = lambda: settings
    host.dependency_overrides[get_terminal_principal] = host_get_terminal_principal
    try:
        yield mounted
    finally:
        host.dependency_overrides.clear()
        get_terminal_settings.cache_clear()
        # A no-op here: with the host's session provider in place no terminal engine is created.
        await dispose_terminal_engine()


@pytest_asyncio.fixture
async def host_client(host_app: HostApp) -> AsyncIterator[httpx.AsyncClient]:
    """An ``httpx.AsyncClient`` driving the host application over ASGI at ``http://host``."""
    transport = httpx.ASGITransport(app=host_app.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://host") as client:
        yield client


# ---------------------------------------------------------------------------------------------
# Route table and OpenAPI
# ---------------------------------------------------------------------------------------------


async def test_routes_and_operation_ids_do_not_collide(host_app: HostApp) -> None:
    """Mounting adds exactly the 16 prefixed terminal operations, with unique paths and
    ``terminal_`` operation IDs, and changes nothing the host declared itself."""
    host = host_app.app
    operations = _api_operations(host)
    keys = [(method, path) for method, path, _route in operations]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    assert not duplicates, f"(method, path) served twice: {duplicates}"

    terminal = {
        (method, path, route.name)
        for method, path, route in operations
        if path.startswith(f"{TERMINAL_PREFIX}/")
    }
    expected = {
        (method, f"{TERMINAL_PREFIX}{path}", name) for method, path, name in EXPECTED_OPERATIONS
    }
    assert len(expected) == 16
    assert terminal == expected

    served = {(method, path): route for method, path, route in operations}
    undeclared = sorted(
        key
        for key in served
        if key not in host_app.host_routes and not key[1].startswith(f"{TERMINAL_PREFIX}/")
    )
    assert not undeclared, (
        f"routes outside {TERMINAL_PREFIX}/ the host never declared: {undeclared}"
    )
    assert set(host_app.host_routes) == set(HOST_RESPONSES)
    for key, route in host_app.host_routes.items():
        assert served.get(key) is route, f"host route {key} was replaced"

    # include_router keeps the sub-router's route class, so the whole-request size guard is in
    # force inside the host.
    assert isinstance(served[("POST", f"{TERMINAL_PREFIX}/ingest/pdf")], TerminalUploadRoute)

    paths = host.openapi()["paths"]
    operation_ids = {
        (method.upper(), path): operation["operationId"]
        for path, path_item in paths.items()
        for method, operation in path_item.items()
        if method in _HTTP_METHODS
    }
    all_ids = list(operation_ids.values())
    assert len(all_ids) == len(set(all_ids)), "duplicate operationId in the host's schema"
    assert {
        key: operation_id
        for key, operation_id in operation_ids.items()
        if key[1].startswith(f"{TERMINAL_PREFIX}/")
    } == {
        (method, f"{TERMINAL_PREFIX}{path}"): f"terminal_{name}"
        for method, path, name in EXPECTED_OPERATIONS
    }
    assert {
        key: operation_id
        for key, operation_id in operation_ids.items()
        if not key[1].startswith(f"{TERMINAL_PREFIX}/")
    } == {key: route.unique_id for key, route in host_app.host_routes.items()}

    # A router cannot carry middleware or exception handlers into a host, and the terminal
    # router brings no startup or shutdown hooks either.
    assert len(host.user_middleware) == host_app.middleware_count
    assert host.exception_handlers == host_app.exception_handlers
    assert len(host.router.on_startup) == host_app.startup_count
    assert len(host.router.on_shutdown) == host_app.shutdown_count


# ---------------------------------------------------------------------------------------------
# Host overrides drive session and principal
# ---------------------------------------------------------------------------------------------


async def test_host_session_and_principal_overrides(
    host_app: HostApp, host_client: httpx.AsyncClient, db_connection: AsyncConnection
) -> None:
    """Terminal requests run on the host's sessions, and a watchlist created through the host
    is owned by the host's subject and visible to it alone."""
    screener = await host_client.get(f"{TERMINAL_PREFIX}/market/screener")
    assert screener.status_code == 200, screener.text
    body = screener.json()
    assert body["count"] == 8
    assert len(body["rows"]) == 8
    closes = {row["ticker"]: row["close_price"] for row in body["rows"]}
    assert closes["SCOM"] == "33.25"
    assert host_app.sessions_opened == 1

    created = await host_client.post(
        f"{TERMINAL_PREFIX}/watchlists", json={"name": "Host list", "tickers": ["SCOM"]}
    )
    assert created.status_code == 201, created.text
    watchlist = created.json()
    assert (watchlist["name"], watchlist["tickers"]) == ("Host list", ["SCOM"])
    owner = await db_connection.execute(
        select(TerminalWatchlist.owner_key).where(
            TerminalWatchlist.id == uuid.UUID(watchlist["id"])
        )
    )
    assert owner.scalar_one() == HOST_SUBJECT

    # The seeded "NSE Core 8" belongs to the standalone subject "local", not to the host user.
    listed = await host_client.get(f"{TERMINAL_PREFIX}/watchlists")
    assert listed.status_code == 200, listed.text
    assert [(item["id"], item["name"], item["tickers"]) for item in listed.json()] == [
        (watchlist["id"], "Host list", ["SCOM"])
    ]

    health = await host_client.get(f"{TERMINAL_PREFIX}/health")
    assert health.status_code == 200, health.text
    assert (health.json()["status"], health.json()["database"]) == ("ok", "ok")
    assert host_app.sessions_opened == 4


@pytest.mark.parametrize(
    "binds_for",
    [
        pytest.param(lambda conn: {TerminalBase: conn}, id="base_class_binds"),
        pytest.param(
            lambda conn: {mapper.class_: conn for mapper in TerminalBase.registry.mappers},
            id="per_model_binds",
        ),
        pytest.param(
            lambda conn: {table: conn for table in TerminalBase.metadata.tables.values()},
            id="per_table_binds",
        ),
    ],
)
async def test_binds_only_host_session_answers_health(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    binds_for: BindsFor,
) -> None:
    """A host session with no default bind, configured only through ``binds`` (keyed by the
    terminal's base class, by every mapped class or by every table), answers ``GET /health``
    200: the probe reaches the bind the terminal's own queries use instead of failing to find
    one and answering a false 503."""
    host_sessionmaker = async_sessionmaker(
        binds=binds_for(db_connection), join_transaction_mode="create_savepoint"
    )

    async def host_binds_only_session() -> AsyncIterator[AsyncSession]:
        async with host_sessionmaker() as session:
            assert session.sync_session.bind is None
            yield session

    host_app.app.dependency_overrides[get_terminal_session] = host_binds_only_session

    health = await host_client.get(f"{TERMINAL_PREFIX}/health")

    assert health.status_code == 200, health.text
    assert health.json() == {"status": "ok", "database": "ok", "version": __version__}


# ---------------------------------------------------------------------------------------------
# 401 propagation
# ---------------------------------------------------------------------------------------------


async def test_host_auth_failure_blocks_every_terminal_operation(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    quickmart_assumptions: dict[str, object],
    statement_pdf: Callable[..., bytes],
) -> None:
    """A principal override that raises 401 answers every terminal operation with the host's
    401, while the host's own routes keep answering 200."""

    async def host_rejects_caller() -> TerminalPrincipal:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="host says no")

    host_app.app.dependency_overrides[get_terminal_principal] = host_rejects_caller

    # Each request is valid apart from the caller, so 401 is the only answer it can get.
    missing_id = uuid.uuid4()
    pdf = statement_pdf("KES", tag="host unauthenticated", pages=1)
    requests: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {
        ("GET", "/health"): ("/health", {}),
        ("GET", "/macro/overview"): ("/macro/overview", {}),
        ("GET", "/market/screener"): ("/market/screener", {}),
        ("GET", "/market/sector-heatmap"): ("/market/sector-heatmap", {}),
        ("GET", "/market/valuation-bands"): (
            "/market/valuation-bands",
            {"params": {"ticker": "SCOM"}},
        ),
        ("GET", "/market/anomalies"): ("/market/anomalies", {}),
        ("GET", "/market/flows"): ("/market/flows", {}),
        ("GET", "/cycles/backtest"): ("/cycles/backtest", {}),
        ("GET", "/ipo/scenarios"): ("/ipo/scenarios", {}),
        ("POST", "/ipo/valuation"): ("/ipo/valuation", {"json": quickmart_assumptions}),
        ("POST", "/ingest/pdf"): (
            "/ingest/pdf",
            {"files": {"file": ("statement.pdf", pdf, "application/pdf")}},
        ),
        ("GET", "/watchlists"): ("/watchlists", {}),
        ("POST", "/watchlists"): ("/watchlists", {"json": {"name": "x"}}),
        ("DELETE", "/watchlists/{watchlist_id}"): (f"/watchlists/{missing_id}", {}),
        ("PUT", "/watchlists/{watchlist_id}/items/{ticker}"): (
            f"/watchlists/{missing_id}/items/SCOM",
            {},
        ),
        ("DELETE", "/watchlists/{watchlist_id}/items/{ticker}"): (
            f"/watchlists/{missing_id}/items/SCOM",
            {},
        ),
    }
    assert set(requests) == {(method, path) for method, path, _name in EXPECTED_OPERATIONS}

    failures: list[str] = []
    for (method, template), (path, options) in requests.items():
        response = await host_client.request(method, f"{TERMINAL_PREFIX}{path}", **options)
        if response.status_code != 401 or response.json() != {"detail": "host says no"}:
            failures.append(f"{method} {template}: {response.status_code} {response.text[:200]}")
    assert not failures, "terminal operations not blocked by the host:\n" + "\n".join(failures)

    for (method, path), expected_body in HOST_RESPONSES.items():
        response = await host_client.request(method, path)
        assert response.status_code == 200, f"{method} {path}: {response.status_code}"
        assert response.json() == expected_body


# ---------------------------------------------------------------------------------------------
# Upload guard inside the host
# ---------------------------------------------------------------------------------------------


async def test_request_size_guard_travels_with_router(
    host_client: httpx.AsyncClient, db_connection: AsyncConnection
) -> None:
    """An authenticated upload whose request exceeds the host settings' PDF limit plus 1 MiB is
    answered 413 ``request_too_large`` and writes neither a filing nor a run row."""
    oversized = b"%PDF-1.4\n" + b"0" * 1_200_000
    file_sha256 = hashlib.sha256(oversized).hexdigest()
    filings_count = select(func.count()).select_from(TerminalCorporateFiling)
    runs_count = (
        select(func.count())
        .select_from(TerminalIngestionRun)
        .where(TerminalIngestionRun.pipeline == "pdf_filing")
    )
    filings_before = await _count(db_connection, filings_count)
    runs_before = await _count(db_connection, runs_count)

    response = await host_client.post(
        f"{TERMINAL_PREFIX}/ingest/pdf",
        files={"file": ("oversized.pdf", oversized, "application/pdf")},
    )

    declared = int(response.request.headers["content-length"])
    assert declared > HOST_PDF_MAX_BYTES + MULTIPART_OVERHEAD_BYTES
    assert response.status_code == 413, response.text
    assert response.json()["detail"]["code"] == "request_too_large"
    assert (
        await _count(
            db_connection,
            filings_count.where(TerminalCorporateFiling.file_sha256 == file_sha256),
        )
        == 0
    )
    assert await _count(db_connection, filings_count) == filings_before
    # Run rows are committed on their own connection; READ COMMITTED shows them here as well.
    assert await _count(db_connection, runs_count) == runs_before
