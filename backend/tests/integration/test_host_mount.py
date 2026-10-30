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
- a host session with no default bind, configured only through ``binds`` or a custom routing
  ``get_bind``, serves all 16 terminal operations, ``/health`` and backtests that read the
  lead-ins of several windows in one statement included, and answers those backtests exactly as
  a default-bound session does;
- a principal override raising 401 blocks every terminal operation while the host's own routes
  still answer 200, also for a malformed, over-nested or oversized JSON body: the host's 401
  precedes every body verdict, and for an admitted caller the verdict comes after the principal
  and before any session is opened;
- the whole-request upload bound answers 413 inside the host with no host configuration at all;
- a ``require_ingest_permission`` override answering 403 refuses only ingestion, before any parse
  or write, an override that admits the caller is handed the host's principal, and the principal
  is resolved first, so a caller the host rejects gets its 401, never the 403.

The contracts a host inherits through ``include_router`` and through the settings and sessions it
supplies are proved in sections of their own below, each without PostgreSQL: hostile JSON request
bodies within 64 KiB get one of the two 422 shapes, an undecodable one FastAPI's ``json_invalid``
entry, and larger ones the coded 413 ``request_too_large`` without being decoded, while a host's
``RequestValidationError`` handler receives every rejected body in a form it can encode;
``GET /health`` answers a coded 503 once its database round trip outlives its bound; the health
service's ``SELECT 1`` probe answers ok, or a coded 503 that names only the failure's type;
``TerminalSettings`` bounds its durations and PDF upload limits and keeps ``database_url`` out of
its repr; and the session module names a database only by its credential-free label. Two more
such sections sit beside the checks they extend: after the route table, a bare host app's OpenAPI
documents both 422 shapes on every operation that declares its own 422, and after the upload
guard and ingest permission checks, ``PdfParseLimiter`` counts PDF parse slots against the limit
in force, separately for every app that mounts the router. A last such section ends the file: the
standalone app's lifespan disposes its default engine when it ends, normally or with an
exception, and a mounted ``terminal_router`` adds nothing to a host's lifespan.

Authentication stays pluggable through dependency injection: ownership is the opaque principal
subject stored as ``owner_key``, and no users or auth table is read or created.
"""

import asyncio
import dataclasses
import hashlib
import itertools
import json
import logging
import os
import secrets
import socket
import sys
import threading
import time
import traceback
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Final, NoReturn, Protocol, cast

import anyio
import httpx
import pytest
import pytest_asyncio
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute, iter_route_contexts
from pydantic import AfterValidator, ValidationError
from pydantic_core import PydanticCustomError
from sqlalchemy import Select, Table, TextClause, func, select
from sqlalchemy.engine import URL, Connection, Engine, make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.sql import ClauseElement

import opennse_terminal
from helpers.pdf_builder import build_pdf, text_page
from opennse_terminal import __version__
from opennse_terminal.api import (
    TerminalPrincipal,
    dispose_terminal_engine,
    get_terminal_principal,
    get_terminal_session,
    get_terminal_settings,
    require_ingest_permission,
    terminal_router,
)
from opennse_terminal.api.deps import TerminalJsonRoute
from opennse_terminal.api.routes import health
from opennse_terminal.api.routes.ingest import (
    PdfParseLimiter,
    TerminalUploadRoute,
    get_pdf_parse_limiter,
)
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db import session as db_session
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.db.session import build_engine, database_log_label, session_factory
from opennse_terminal.ingestion.files.pdf_line_items import ParseCancelledError, PdfUnreadable
from opennse_terminal.main import create_app
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalIngestionRun,
    TerminalWatchlist,
)
from opennse_terminal.schemas import ErrorDetail, ErrorResponse, ValidationErrorResponse
from opennse_terminal.schemas.common import HealthResponse, RequestDecimal, TerminalRequestModel
from opennse_terminal.schemas.ipo import IpoValuationRequest, IpoValuationResponse
from opennse_terminal.schemas.watchlists import WatchlistCreate
from opennse_terminal.services import filings_service, health_service, ipo_service

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
# OpenAPI documents both 422 shapes on every terminal operation that declares its own 422
#
# A 422 is either a coded body, ``detail`` an object (``ErrorResponse``), or FastAPI's standard
# validation list, ``detail`` an array (``ValidationErrorResponse``). Declaring a 422 response on
# a route replaces FastAPI's automatic ``HTTPValidationError`` schema, so the seven operations
# that declare one must name the union of both models; every other operation keeps FastAPI's
# automatic schema. The app is a bare ``FastAPI()`` that includes ``terminal_router``, as a host
# does. Only ``app.openapi()`` is read: no request is sent and no database is touched.
# ---------------------------------------------------------------------------------------------

REF_PREFIX = "#/components/schemas/"
CODED_REF = REF_PREFIX + ErrorResponse.__name__
VALIDATION_REF = REF_PREFIX + ValidationErrorResponse.__name__
FASTAPI_VALIDATION_REF = REF_PREFIX + "HTTPValidationError"

UNION_422_OPERATIONS = [
    pytest.param("get", "/api/v1/terminal/market/screener", id="screener"),
    pytest.param("get", "/api/v1/terminal/market/valuation-bands", id="valuation-bands"),
    pytest.param("get", "/api/v1/terminal/market/flows", id="flows"),
    pytest.param("get", "/api/v1/terminal/cycles/backtest", id="cycles-backtest"),
    pytest.param("post", "/api/v1/terminal/ipo/valuation", id="ipo-valuation"),
    pytest.param("post", "/api/v1/terminal/ingest/pdf", id="ingest-pdf"),
    pytest.param("post", "/api/v1/terminal/watchlists", id="watchlist-create"),
]


@pytest.fixture(scope="module")
def openapi() -> dict[str, Any]:
    """The OpenAPI document of a bare host app that includes ``terminal_router``."""
    app = FastAPI()
    app.include_router(terminal_router)
    return app.openapi()


def _component(openapi: dict[str, Any], ref: str) -> dict[str, Any]:
    """The ``components/schemas`` entry ``ref`` points at, failing when it does not resolve."""
    assert ref.startswith(REF_PREFIX), ref
    schemas: dict[str, Any] = openapi["components"]["schemas"]
    name = ref.removeprefix(REF_PREFIX)
    assert name in schemas, f"{ref} does not resolve in components/schemas"
    component: dict[str, Any] = schemas[name]
    return component


def _refs(node: object) -> set[str]:
    """Every ``$ref`` value at any depth of ``node``."""
    found: set[str] = set()
    pending = [node]
    while pending:
        current = pending.pop()
        if isinstance(current, dict):
            ref = current.get("$ref")
            if isinstance(ref, str):
                found.add(ref)
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    return found


def _schema_422(openapi: dict[str, Any], method: str, path: str) -> dict[str, Any] | None:
    """The ``application/json`` schema of the operation's 422 response, or None without one."""
    response = openapi["paths"][path][method]["responses"].get("422")
    if response is None:
        return None
    schema: dict[str, Any] = response["content"]["application/json"]["schema"]
    return schema


@pytest.mark.parametrize(("method", "path"), UNION_422_OPERATIONS)
def test_declared_422_documents_coded_and_validation_shapes(
    openapi: dict[str, Any], method: str, path: str
) -> None:
    """The 422 schema is an anyOf/oneOf of exactly the coded and the validation-list models, every
    reference it reaches resolves, and the two alternatives differ by the JSON type of detail."""
    schema = _schema_422(openapi, method, path)
    assert schema is not None, f"{method.upper()} {path} declares no 422 response"
    alternatives = schema.get("anyOf", schema.get("oneOf"))
    assert isinstance(alternatives, list), schema
    assert {alternative.get("$ref") for alternative in alternatives} == {
        CODED_REF,
        VALIDATION_REF,
    }
    assert openapi["paths"][path][method]["responses"]["422"]["description"]

    # Every reference reachable from the 422 schema resolves, transitively.
    seen: set[str] = set()
    pending = sorted(_refs(schema))
    while pending:
        ref = pending.pop()
        if ref not in seen:
            seen.add(ref)
            pending.extend(_refs(_component(openapi, ref)) - seen)
    assert {CODED_REF, VALIDATION_REF, REF_PREFIX + ErrorDetail.__name__} <= seen

    coded_detail = _component(openapi, CODED_REF)["properties"]["detail"]
    error_detail = _component(openapi, coded_detail["$ref"])
    assert error_detail["type"] == "object"
    assert {"code", "message"} <= set(error_detail["required"])
    assert set(error_detail["properties"]) == {"code", "message", "field"}

    validation_detail = _component(openapi, VALIDATION_REF)["properties"]["detail"]
    assert validation_detail["type"] == "array"
    item = _component(openapi, validation_detail["items"]["$ref"])
    assert item["type"] == "object"
    assert set(item["required"]) == {"loc", "msg", "type"}
    assert {"loc", "msg", "type", "input", "ctx"} == set(item["properties"])
    assert item["properties"]["loc"]["type"] == "array"


def test_every_documented_422_admits_the_validation_list(openapi: dict[str, Any]) -> None:
    """No operation documents its 422 as a coded object alone: the seven declaring operations name
    the union, and every other operation with a 422 keeps FastAPI's automatic schema."""
    declared = {(param.values[0], param.values[1]) for param in UNION_422_OPERATIONS}
    union_operations: set[tuple[str, str]] = set()
    for path, item in openapi["paths"].items():
        for method in item:
            schema = _schema_422(openapi, method, path)
            if schema is None:
                continue
            alternatives = {
                alternative.get("$ref") for alternative in schema.get("anyOf", [schema])
            }
            if VALIDATION_REF in alternatives:
                union_operations.add((method, path))
            else:
                assert alternatives == {FASTAPI_VALIDATION_REF}, (method, path, schema)
    assert union_operations == declared


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


# A host's custom routing ``Session``, as in SQLAlchemy's documented custom-partitioning pattern:
# it subclasses ``Session`` and overrides ``get_bind(mapper=None, clause=None, **kw)``, routing on
# ``mapper.class_``. Such a session has neither a default bind nor ``binds``, so the terminal
# reaches its database through it only when every statement it executes names the target model's
# ``Mapper`` (or its ``terminal_`` table as the clause), as SQLAlchemy's own ORM statements do.


class RoutingSession(Session):
    """Routes the terminal's models to ``terminal_bind`` by mapper, or by table as the clause.

    Anything else falls through to ``Session.get_bind``, which, with no default bind and no
    ``binds``, raises ``UnboundExecutionError``.
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


HostSessionOptions = Callable[[AsyncConnection], dict[str, Any]]
"""Builds the configuration of a host session with no default bind onto the per-test connection:
a ``binds`` mapping, or a routing ``sync_session_class`` with its ``terminal_bind``."""

HOST_SESSION_BACKTESTS: Final[tuple[tuple[dict[str, Any], list[str]], ...]] = (
    ({}, ["complete", "complete", "complete", "awaiting_data"]),
    ({"event_dates": ["2017-08-08", "2022-08-09"]}, ["complete", "complete"]),
    ({"pre_months": 12, "post_months": 12}, ["complete", "complete", "complete", "partial"]),
    (
        {"tickers": "SCOM", "sector": "Banking"},
        ["no_constituents", "no_constituents", "no_constituents", "awaiting_data"],
    ),
)
"""``GET /cycles/backtest`` queries and the statuses of their cycles. In the first three several
windows hold data, so the lead-ins of all of them are read in one ``UNION``; in the last no
candidate remains, so the windows are judged on the market's latest session and sessions."""


@pytest.mark.parametrize(
    ("tag", "session_options"),
    [
        pytest.param(
            "base class binds",
            lambda conn: {"binds": {TerminalBase: conn}},
            id="base_class_binds",
        ),
        pytest.param(
            "per model binds",
            lambda conn: {
                "binds": {mapper.class_: conn for mapper in TerminalBase.registry.mappers}
            },
            id="per_model_binds",
        ),
        pytest.param(
            "per table binds",
            lambda conn: {
                "binds": {table: conn for table in TerminalBase.metadata.tables.values()}
            },
            id="per_table_binds",
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
async def test_host_session_without_a_default_bind_serves_every_operation(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    quickmart_assumptions: dict[str, object],
    statement_pdf: Callable[..., bytes],
    tag: str,
    session_options: HostSessionOptions,
) -> None:
    """A host session with no default bind, configured only through ``binds`` (keyed by the
    terminal's base class, by every mapped class or by every table) or through a custom
    ``get_bind`` routing on the mapper or table, serves all 16 terminal operations with their
    success status: the reads, a PDF preview, and the creation, item changes and deletion of a
    watchlist of its own. Backtests whose windows hold data in several places, or that leave no
    candidate, answer exactly as through the host's default-bound session."""
    default_session_provider = host_app.app.dependency_overrides[get_terminal_session]
    host_sessionmaker = async_sessionmaker(
        **session_options(db_connection), join_transaction_mode="create_savepoint"
    )

    async def host_session_without_default_bind() -> AsyncIterator[AsyncSession]:
        async with host_sessionmaker() as session:
            assert session.sync_session.bind is None
            yield session

    host_app.app.dependency_overrides[get_terminal_session] = host_session_without_default_bind

    responses: dict[tuple[str, str], httpx.Response] = {}

    async def call(method: str, template: str, path: str, **options: Any) -> httpx.Response:
        response = await host_client.request(method, f"{TERMINAL_PREFIX}{path}", **options)
        responses[(method, template)] = response
        return response

    pdf = statement_pdf("KES", tag=f"host session {tag}")
    reads: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {
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
            {
                "files": {"file": ("statement.pdf", pdf, "application/pdf")},
                "data": {"commit": "false"},
            },
        ),
        ("GET", "/watchlists"): ("/watchlists", {}),
    }
    for (method, template), (path, options) in reads.items():
        await call(method, template, path, **options)
    created = await call(
        "POST", "/watchlists", "/watchlists", json={"name": f"Host {tag}", "tickers": ["SCOM"]}
    )
    assert created.status_code == 201, created.text
    watchlist = f"/watchlists/{created.json()['id']}"
    added = await call("PUT", "/watchlists/{watchlist_id}/items/{ticker}", f"{watchlist}/items/KCB")
    removed = await call(
        "DELETE", "/watchlists/{watchlist_id}/items/{ticker}", f"{watchlist}/items/KCB"
    )
    await call("DELETE", "/watchlists/{watchlist_id}", watchlist)
    assert set(responses) == {(method, path) for method, path, _name in EXPECTED_OPERATIONS}

    success_status = {("POST", "/watchlists"): 201, ("DELETE", "/watchlists/{watchlist_id}"): 204}
    failures = [
        f"{method} {template}: {response.status_code} {response.text[:200]}"
        for (method, template), response in responses.items()
        if response.status_code != success_status.get((method, template), 200)
    ]
    assert not failures, "terminal operations failed on the host's session:\n" + "\n".join(failures)
    assert responses[("GET", "/market/screener")].json()["count"] == 8
    assert responses[("POST", "/ingest/pdf")].json()["committed"] is False
    assert (added.json()["tickers"], removed.json()["tickers"]) == (["KCB", "SCOM"], ["SCOM"])

    without_default_bind: list[Any] = []
    for params, statuses in HOST_SESSION_BACKTESTS:
        response = await host_client.get(f"{TERMINAL_PREFIX}/cycles/backtest", params=params)
        assert response.status_code == 200, f"{params}: {response.text[:200]}"
        assert [cycle["status"] for cycle in response.json()["cycles"]] == statuses, params
        without_default_bind.append(response.json())

    host_app.app.dependency_overrides[get_terminal_session] = default_session_provider
    for (params, _statuses), body in zip(HOST_SESSION_BACKTESTS, without_default_bind, strict=True):
        response = await host_client.get(f"{TERMINAL_PREFIX}/cycles/backtest", params=params)
        assert response.status_code == 200, f"{params}: {response.text[:200]}"
        assert response.json() == body, params


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


JSON_BODY_LIMIT_BYTES: Final = 65_536
"""Largest JSON body ``POST /ipo/valuation`` and ``POST /watchlists`` accept, stated here as the
contract rather than read from ``api.deps``."""

JSON_BODY_DRAIN_BYTES: Final = 1_048_576
"""Bytes past the JSON body limit that an over-limit chunked body is read and dropped at most."""

LONG_ZERO_PADDING: Final = "0" * 4_300
"""Leading zeroes that, with any digit after them, make a ``Content-Length`` longer than the 4,300
digits ``int()`` converts by default, while leaving its value unchanged."""

STREAM_CHUNK_BYTES: Final = 16_384

DEPS_LOGGER: Final = "opennse_terminal.api.deps"
TOO_LARGE_LOG: Final = "JSON request body rejected: request_too_large (limit 65536 bytes)"


@dataclass
class TrackedBody:
    """A JSON request body streamed to the app in chunks, counting the bytes the app pulled.

    ``httpx.ASGITransport`` pulls the next chunk only when the app calls ``receive``, so
    ``pulled`` is how much of the body the app has read. ``declared`` is sent as the
    ``Content-Length``; without it the body is framed as chunked.
    """

    data: bytes
    declared: str | None = None
    pulled: int = 0

    async def _chunks(self) -> AsyncIterator[bytes]:
        for start in range(0, len(self.data), STREAM_CHUNK_BYTES):
            chunk = self.data[start : start + STREAM_CHUNK_BYTES]
            self.pulled += len(chunk)
            yield chunk

    async def post(self, client: httpx.AsyncClient, url: str) -> httpx.Response:
        """POST the body to ``url`` as ``application/json``."""
        headers = {"content-type": "application/json"}
        if self.declared is not None:
            headers["content-length"] = self.declared
        return await client.post(url, content=self._chunks(), headers=headers)


def framed(data: bytes, *, chunked: bool) -> TrackedBody:
    """``data`` as a body declaring its own length, or framed as chunked."""
    return TrackedBody(data, declared=None if chunked else str(len(data)))


def padded_json(document: object, size: int) -> bytes:
    """``document`` as JSON text followed by insignificant whitespace, ``size`` bytes in all."""
    text = json.dumps(document).encode()
    assert len(text) <= size
    return text + b" " * (size - len(text))


def too_large_records(caplog: pytest.LogCaptureFixture) -> list[str]:
    """The messages of the 413 ``request_too_large`` records logged by ``api.deps``."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == DEPS_LOGGER and "request_too_large" in record.getMessage()
    ]


OVERSIZED_WATCHLIST: Final = padded_json(
    {"name": "big", "tickers": ["SCOM"]}, 4 * JSON_BODY_LIMIT_BYTES
)
OVERSIZED_OBJECT: Final = padded_json({}, 4 * JSON_BODY_LIMIT_BYTES)


@pytest.mark.parametrize(
    ("path", "body"),
    [
        pytest.param("/watchlists", framed(b'{"name":', chunked=False), id="watchlist_malformed"),
        pytest.param("/ipo/valuation", framed(b"{bad", chunked=False), id="ipo_malformed"),
        pytest.param(
            "/watchlists",
            framed(b"[" * 5_000 + b"]" * 5_000, chunked=False),
            id="watchlist_over_nested",
        ),
        pytest.param(
            "/ipo/valuation",
            framed(b"[" * 5_000 + b"]" * 5_000, chunked=False),
            id="ipo_over_nested",
        ),
        pytest.param(
            "/watchlists",
            framed(OVERSIZED_WATCHLIST, chunked=False),
            id="watchlist_oversized_content_length",
        ),
        pytest.param(
            "/ipo/valuation",
            framed(OVERSIZED_OBJECT, chunked=False),
            id="ipo_oversized_content_length",
        ),
        pytest.param(
            "/watchlists",
            framed(OVERSIZED_WATCHLIST, chunked=True),
            id="watchlist_oversized_chunked",
        ),
        pytest.param(
            "/ipo/valuation", framed(OVERSIZED_OBJECT, chunked=True), id="ipo_oversized_chunked"
        ),
        pytest.param(
            "/watchlists",
            TrackedBody(b'{"name": "x"}', declared=LONG_ZERO_PADDING + "13"),
            id="watchlist_zero_padded_content_length",
        ),
        pytest.param(
            "/ipo/valuation",
            TrackedBody(b"{}", declared=LONG_ZERO_PADDING + "2"),
            id="ipo_zero_padded_content_length",
        ),
        pytest.param(
            "/watchlists",
            TrackedBody(b'{"name": "x"}', declared="9" * 5_000),
            id="watchlist_5000_digit_content_length",
        ),
        pytest.param(
            "/ipo/valuation",
            TrackedBody(b"{}", declared="9" * 5_000),
            id="ipo_5000_digit_content_length",
        ),
        pytest.param(
            "/watchlists",
            TrackedBody(b'{"name": "x"}', declared=LONG_ZERO_PADDING + "65537"),
            id="watchlist_zero_padded_oversized_content_length",
        ),
        pytest.param(
            "/ipo/valuation",
            TrackedBody(b"{}", declared=LONG_ZERO_PADDING + "65537"),
            id="ipo_zero_padded_oversized_content_length",
        ),
    ],
)
async def test_host_auth_failure_precedes_every_body_verdict(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    caplog: pytest.LogCaptureFixture,
    path: str,
    body: TrackedBody,
) -> None:
    """A principal override that raises 401 answers a malformed, over-nested or oversized JSON
    body, or one whose ``Content-Length`` has more digits than ``int()`` converts, with the host's
    401, not the body's 422 or 413: the body verdict waits for the host's dependencies, so no
    session is opened and no watchlist row is written."""

    async def host_rejects_caller() -> TerminalPrincipal:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="host says no")

    host_app.app.dependency_overrides[get_terminal_principal] = host_rejects_caller
    watchlists_count = select(func.count()).select_from(TerminalWatchlist)
    watchlists_before = await _count(db_connection, watchlists_count)
    caplog.set_level(logging.INFO, logger=DEPS_LOGGER)

    response = await body.post(host_client, f"{TERMINAL_PREFIX}{path}")

    assert response.status_code == 401, response.text
    assert response.json() == {"detail": "host says no"}
    assert host_app.sessions_opened == 0
    assert await _count(db_connection, watchlists_count) == watchlists_before
    assert too_large_records(caplog) == []


@pytest.mark.parametrize(
    ("body", "status_code", "sessions_opened"),
    [
        pytest.param(framed(b'{"name":', chunked=False), 422, 0, id="malformed"),
        pytest.param(framed(OVERSIZED_WATCHLIST, chunked=False), 413, 0, id="oversized"),
        pytest.param(framed(OVERSIZED_WATCHLIST, chunked=True), 413, 0, id="oversized_chunked"),
        pytest.param(framed(b'{"name": "Ordered"}', chunked=False), 201, 1, id="valid"),
    ],
)
async def test_body_verdict_follows_the_principal_and_precedes_the_session(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    body: TrackedBody,
    status_code: int,
    sessions_opened: int,
) -> None:
    """``POST /watchlists`` resolves the host's principal first, then judges the body, and opens
    the host's session only for a body that passes: a refused body is answered after the
    principal ran once, with no session opened and no row written."""
    principal_calls: list[str] = []

    async def host_records_caller() -> TerminalPrincipal:
        principal_calls.append(HOST_SUBJECT)
        return TerminalPrincipal(subject=HOST_SUBJECT, is_authenticated=True)

    host_app.app.dependency_overrides[get_terminal_principal] = host_records_caller
    watchlists_count = select(func.count()).select_from(TerminalWatchlist)
    watchlists_before = await _count(db_connection, watchlists_count)

    response = await body.post(host_client, f"{TERMINAL_PREFIX}/watchlists")

    assert response.status_code == status_code, response.text
    assert principal_calls == [HOST_SUBJECT]
    assert host_app.sessions_opened == sessions_opened
    assert await _count(db_connection, watchlists_count) == watchlists_before + sessions_opened


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


# ---------------------------------------------------------------------------------------------
# Ingest permission inside the host
#
# ``require_ingest_permission`` guards ``POST /ingest/pdf`` on top of the router-level principal
# dependency, so a host restricts ingestion, for example to its administrators, without
# restricting any other terminal operation. The host overrides the object exported by
# ``opennse_terminal.api``, the one the route depends on. ``ingestion_calls`` records every call
# that reaches the real ingestion service and its PDF parser, both still run unchanged, so a
# refused upload is shown to stop before either starts.
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def ingestion_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Calls that reached ``filings_service.ingest_pdf`` (``"service"``) and its parser
    ``filings_service.extract_line_items`` (``"parse"``), in order; each call is passed on to the
    real function."""
    calls: list[str] = []
    service = filings_service.ingest_pdf
    parser = filings_service.extract_line_items

    async def recorded_service(*args: Any, **kwargs: Any) -> Any:
        calls.append("service")
        return await service(*args, **kwargs)

    def recorded_parser(*args: Any, **kwargs: Any) -> Any:
        calls.append("parse")
        return parser(*args, **kwargs)

    monkeypatch.setattr(filings_service, "ingest_pdf", recorded_service)
    monkeypatch.setattr(filings_service, "extract_line_items", recorded_parser)
    return calls


async def test_ingest_permission_override_refuses_upload_before_any_ingestion_work(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    db_connection: AsyncConnection,
    statement_pdf: Callable[..., bytes],
    ingestion_calls: list[str],
) -> None:
    """An authenticated caller whom the host's ``require_ingest_permission`` override refuses
    gets the host's 403 for an otherwise valid upload before the ingestion service starts: no
    parse, no filing row and no run row. Only ingestion is restricted: the same caller still
    reads the screener and creates a watchlist."""
    assert require_ingest_permission is opennse_terminal.api.deps.require_ingest_permission
    refusals: list[str] = []

    async def host_requires_administrator() -> None:
        refusals.append("refused")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="administrators only")

    host_app.app.dependency_overrides[require_ingest_permission] = host_requires_administrator

    # Valid in every respect, so without the guard it would be parsed and answered 200.
    pdf = statement_pdf("KES", tag="host ingest refused")
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
        files={"file": ("statement.pdf", pdf, "application/pdf")},
    )

    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "administrators only"}
    assert refusals == ["refused"]
    assert ingestion_calls == []
    file_sha256 = hashlib.sha256(pdf).hexdigest()
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

    screener = await host_client.get(f"{TERMINAL_PREFIX}/market/screener")
    assert screener.status_code == 200, screener.text
    assert screener.json()["count"] == 8
    created = await host_client.post(
        f"{TERMINAL_PREFIX}/watchlists", json={"name": "Ingest refused", "tickers": ["KCB"]}
    )
    assert created.status_code == 201, created.text
    assert (created.json()["name"], created.json()["tickers"]) == ("Ingest refused", ["KCB"])


async def test_ingest_permission_override_is_handed_the_host_principal(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    statement_pdf: Callable[..., bytes],
    ingestion_calls: list[str],
) -> None:
    """An override that declares the principal dependency receives the caller the host's
    principal override resolved, and by admitting that caller lets the upload through to the
    service: the preview answers 200."""
    administrators = frozenset({HOST_SUBJECT})
    callers: list[TerminalPrincipal] = []

    async def host_admits_administrators(
        principal: Annotated[TerminalPrincipal, Depends(get_terminal_principal)],
    ) -> None:
        callers.append(principal)
        if principal.subject not in administrators:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="administrators only")

    host_app.app.dependency_overrides[require_ingest_permission] = host_admits_administrators
    pdf = statement_pdf("KES", tag="host ingest permitted")

    response = await host_client.post(
        f"{TERMINAL_PREFIX}/ingest/pdf",
        files={"file": ("statement.pdf", pdf, "application/pdf")},
        data={"commit": "false"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["file_sha256"] == hashlib.sha256(pdf).hexdigest()
    assert (body["committed"], body["missing_items"]) == (False, [])
    assert callers == [TerminalPrincipal(subject=HOST_SUBJECT, is_authenticated=True)]
    assert ingestion_calls == ["service", "parse"]


async def test_host_principal_is_resolved_before_the_ingest_permission(
    host_app: HostApp,
    host_client: httpx.AsyncClient,
    statement_pdf: Callable[..., bytes],
    ingestion_calls: list[str],
) -> None:
    """A caller the host's principal override rejects gets its 401, not the 403 of an ingest
    permission override that does not itself depend on the principal: the router-level
    principal dependency runs first, so the permission check never runs for that caller."""
    permission_checks: list[str] = []

    async def host_rejects_caller() -> TerminalPrincipal:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="host says no")

    async def host_refuses_ingestion() -> None:
        permission_checks.append("checked")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="administrators only")

    host_app.app.dependency_overrides[get_terminal_principal] = host_rejects_caller
    host_app.app.dependency_overrides[require_ingest_permission] = host_refuses_ingestion
    pdf = statement_pdf("KES", tag="host ingest unauthenticated")

    response = await host_client.post(
        f"{TERMINAL_PREFIX}/ingest/pdf",
        files={"file": ("statement.pdf", pdf, "application/pdf")},
    )

    assert response.status_code == 401, response.text
    assert response.json() == {"detail": "host says no"}
    assert permission_checks == []
    assert ingestion_calls == []


# ---------------------------------------------------------------------------------------------
# PDF parse slots: ``PdfParseLimiter`` accounting and its isolation between mounted hosts
#
# ``PdfParseLimiter`` admits a parse only while fewer parses than the limit in force are active,
# never waits for a slot, and keeps counting the parses it admitted when a request arrives with a
# different ``pdf_max_concurrency``: a lower limit refuses new requests at once while the running
# parses finish, and a higher one admits up to the new limit. Slots come back when the block
# exits, also on an exception or a cancellation. A cancelled upload sets its parse's ``cancel``
# event, which the real extractor answers by killing its parser process, and keeps its slot until
# its parse thread has ended: moments later for a parse that honours the event, and for as long as
# one that ignores it runs.
#
# ``TerminalUploadRoute`` gives every application that mounts ``terminal_router`` its own limiter
# and its own once-only settings-fallback warning, so two hosts in one process never share upload
# accounting. The endpoint tests mount ``terminal_router`` in separate ``FastAPI()`` apps whose
# ``get_terminal_settings`` override builds a new ``TerminalSettings`` on every call, as a host's
# ``lambda: TerminalSettings(...)`` does. ``filings_service.ingest_pdf`` is replaced by
# :class:`HeldParses`, so no parse runs and no database is touched; the cancellation tests replace
# it with :func:`_extract_only`, which runs the real ``filings_service._extract`` with
# :class:`GatedParses` as the extractor, still without a database.
# ---------------------------------------------------------------------------------------------

INGEST_PDF_URL: Final = "/api/v1/terminal/ingest/pdf"
PDF: Final = build_pdf([text_page(["Parse limiter test filing"])])
"""A valid one-page text PDF; it passes the ``%PDF-`` check and is never parsed here."""
INGEST_LOGGER: Final = "opennse_terminal.api.routes.ingest"
SETTINGS_FALLBACK_WARNING: Final = "not TerminalSettings"
PARSE_GATE_SECONDS: Final = 30.0
"""How long a :class:`GatedParses` parse waits to be released before it gives up, so a failing
test never leaves a worker thread behind."""
CANCEL_POLL_SECONDS: Final = 0.01
"""How often a cancel-honouring :class:`GatedParses` parse checks its ``cancel`` event."""
TASK_CANCEL: Final = "task"
SCOPE_CANCEL: Final = "anyio_scope"


class HeldSlots:
    """Slots taken through ``PdfParseLimiter.slot`` and held open until released, oldest first."""

    def __init__(self, limiter: PdfParseLimiter) -> None:
        self._limiter = limiter
        self._held: list[AbstractAsyncContextManager[bool]] = []

    @property
    def count(self) -> int:
        """Slots currently held."""
        return len(self._held)

    async def take(self, limit: int) -> bool:
        """Request a slot at ``limit``; keep it when admitted, and return whether it was."""
        context = self._limiter.slot(limit)
        admitted = await context.__aenter__()
        if admitted:
            self._held.append(context)
        else:
            await context.__aexit__(None, None, None)
        return admitted

    async def release(self) -> None:
        """Leave the oldest held slot's block normally."""
        await self._held.pop(0).__aexit__(None, None, None)


@dataclass(frozen=True)
class Take:
    """A request at ``limit``, expected to be admitted or refused."""

    limit: int
    admitted: bool


@dataclass(frozen=True)
class Release:
    """The oldest held slot's parse finishes."""


Step = Take | Release


class HeldParses:
    """Stand-in for ``filings_service.ingest_pdf``: each call holds its parse slot until released.

    A released call raises a coded 409 ``released``, so the route gives its slot back through the
    exception path. After :meth:`release_all`, later calls are released on arrival.
    """

    def __init__(self) -> None:
        self._gates: list[asyncio.Event] = []
        self._arrived = asyncio.Event()
        self._open = False
        self.tasks: list[asyncio.Task[httpx.Response]] = []

    @property
    def calls(self) -> int:
        """Calls that reached the service, that is, requests admitted to a parse slot."""
        return len(self._gates)

    async def __call__(self, *_args: object, **_kwargs: object) -> NoReturn:
        gate = asyncio.Event()
        if self._open:
            gate.set()
        self._gates.append(gate)
        self._arrived.set()
        await gate.wait()
        raise HTTPException(409, detail={"code": "released", "message": "released", "field": None})

    def hold(self, host: "Host") -> asyncio.Task[httpx.Response]:
        """Start an upload to ``host`` in the background; it holds a slot once admitted."""
        task = asyncio.create_task(host.upload())
        self.tasks.append(task)
        return task

    async def wait_for_calls(self, count: int) -> None:
        """Wait until ``count`` calls have reached the service, for at most 10 seconds."""
        async with asyncio.timeout(10):
            while self.calls < count:
                self._arrived.clear()
                await self._arrived.wait()

    def release(self, index: int) -> None:
        """Let call ``index`` (0-based, in arrival order) finish."""
        self._gates[index].set()

    def release_all(self) -> None:
        """Let every call finish, including calls still to come."""
        self._open = True
        for gate in self._gates:
            gate.set()


class Host:
    """A host ``FastAPI()`` app that mounts ``terminal_router`` and nothing else.

    Its ``get_terminal_settings`` override builds a new ``TerminalSettings`` on every call, with
    ``pdf_max_concurrency`` set to :attr:`limit`, which a test may change between requests.
    """

    def __init__(
        self, make_settings: Callable[..., TerminalSettings], limit: int, stack: AsyncExitStack
    ) -> None:
        self.limit = limit
        self.app = FastAPI()
        self.app.include_router(terminal_router)
        self.app.dependency_overrides[get_terminal_session] = _no_session
        self.app.dependency_overrides[get_terminal_settings] = lambda: make_settings(
            pdf_max_concurrency=self.limit
        )
        self._stack = stack

    async def start(self) -> "Host":
        """Open this host's HTTP client, closed when the test ends."""
        transport = httpx.ASGITransport(app=self.app)
        self.client = await self._stack.enter_async_context(
            httpx.AsyncClient(transport=transport, base_url="http://host.test")
        )
        return self

    async def upload(self) -> httpx.Response:
        """POST the test PDF as a preview."""
        return await self.client.post(
            INGEST_PDF_URL, files={"file": ("filing.pdf", PDF, "application/pdf")}
        )


async def _no_session() -> AsyncIterator[None]:
    """Session override: the stand-in service never touches a database."""
    yield None


@dataclass
class GatedParse:
    """One parse of :class:`GatedParses`: the gate its worker thread waits on, and its progress."""

    gate: threading.Event = dataclasses.field(default_factory=threading.Event)
    started: threading.Event = dataclasses.field(default_factory=threading.Event)
    finished: threading.Event = dataclasses.field(default_factory=threading.Event)


class GatedParses:
    """Stand-in for ``filings_service.extract_line_items``, which the real
    ``filings_service._extract`` runs in a worker thread.

    Each parse blocks its thread until released and then raises ``PdfUnreadable``, so a released
    upload ends as the coded 422 ``pdf_unreadable``. Parses are numbered from 0 in arrival order;
    after :meth:`release_all`, later parses are released on arrival. By default a parse ignores
    its ``cancel`` event; with ``honours_cancel`` it also ends once that event is set, raising
    ``ParseCancelledError`` as the real extractor does after killing its parser process.
    """

    def __init__(self, *, honours_cancel: bool = False) -> None:
        self._lock = threading.Lock()
        self._parses: list[GatedParse] = []
        self._arrived = 0
        self._running = 0
        self._max_running = 0
        self._open = False
        self._honours_cancel = honours_cancel
        self._cancelled: list[int] = []
        # Background uploads, released and awaited when the test ends.
        self.tasks: list[asyncio.Task[Any]] = []

    @property
    def arrived(self) -> int:
        """Parses that have started, that is, uploads admitted to a parse slot."""
        with self._lock:
            return self._arrived

    @property
    def max_running(self) -> int:
        """The most parse threads that were running at once."""
        with self._lock:
            return self._max_running

    @property
    def cancelled(self) -> list[int]:
        """Parses ended by their ``cancel`` event, in the order they ended."""
        with self._lock:
            return list(self._cancelled)

    def parse(self, index: int) -> GatedParse:
        """Parse ``index``, created ahead of its arrival when a test refers to it first."""
        with self._lock:
            while len(self._parses) <= index:
                created = GatedParse()
                if self._open:
                    created.gate.set()
                self._parses.append(created)
            return self._parses[index]

    def __call__(
        self, *_args: object, cancel: threading.Event | None = None, **_kwargs: object
    ) -> NoReturn:
        with self._lock:
            index = self._arrived
            self._arrived += 1
            self._running += 1
            self._max_running = max(self._max_running, self._running)
        parse = self.parse(index)
        parse.started.set()
        try:
            if self._honours_cancel and cancel is not None:
                self._wait_unless_cancelled(index, parse, cancel)
            elif not parse.gate.wait(PARSE_GATE_SECONDS):
                raise TimeoutError(f"gated parse {index} was never released")
            raise PdfUnreadable
        finally:
            with self._lock:
                self._running -= 1
            parse.finished.set()

    def _wait_unless_cancelled(
        self, index: int, parse: GatedParse, cancel: threading.Event
    ) -> None:
        """Return once parse ``index`` is released; raise ``ParseCancelledError`` once ``cancel``
        is set first. The two events are polled, as one thread cannot wait on both."""
        deadline = time.monotonic() + PARSE_GATE_SECONDS
        while not parse.gate.wait(CANCEL_POLL_SECONDS):
            if cancel.is_set():
                with self._lock:
                    self._cancelled.append(index)
                raise ParseCancelledError
            if time.monotonic() >= deadline:
                raise TimeoutError(f"gated parse {index} was never released or cancelled")

    def hold(self, host: Host) -> None:
        """Start an upload to ``host`` in the background; once admitted, its parse is held."""
        self.tasks.append(asyncio.create_task(host.upload()))

    async def wait_started(self, index: int) -> bool:
        """Whether parse ``index`` starts within 10 seconds."""
        return await asyncio.to_thread(self.parse(index).started.wait, 10)

    def release(self, index: int) -> None:
        """Let parse ``index`` end."""
        self.parse(index).gate.set()

    def release_all(self) -> None:
        """Let every parse end, including parses still to come."""
        with self._lock:
            self._open = True
            parses = list(self._parses)
        for parse in parses:
            parse.gate.set()

    async def wait_all_finished(self) -> bool:
        """Whether the thread of every parse that started has ended."""
        with self._lock:
            parses = self._parses[: self._arrived]
        return all([await asyncio.to_thread(p.finished.wait, PARSE_GATE_SECONDS) for p in parses])


async def _extract_only(
    _session: object,
    settings: TerminalSettings,
    upload_bytes: bytes,
    _file_name: str,
    form: filings_service.PdfIngestForm,
) -> NoReturn:
    """Database-free stand-in for ``filings_service.ingest_pdf``: only the real ``_extract``
    runs, with :class:`GatedParses` as the extractor in its worker thread."""
    result = await filings_service._extract(upload_bytes, form, settings)
    raise AssertionError(f"the gated extractor returned {result!r}")


def assert_coded(response: httpx.Response, status_code: int, code: str) -> dict[str, Any]:
    """Assert ``response`` is the coded error ``code`` with no field and no other body member."""
    assert response.status_code == status_code, response.text
    body = response.json()
    assert set(body) == {"detail"}, body
    assert body["detail"]["code"] == code, body
    assert body["detail"]["field"] is None, body
    return dict(body["detail"])


def assert_busy(response: httpx.Response) -> None:
    """Assert ``response`` is the immediate 429 ``ingest_busy`` with ``Retry-After: 5``."""
    assert_coded(response, 429, "ingest_busy")
    assert response.headers["retry-after"] == "5"


@pytest.fixture
async def parses(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[HeldParses]:
    """:class:`HeldParses` installed as the service; every held upload is released and awaited."""
    held = HeldParses()
    monkeypatch.setattr(filings_service, "ingest_pdf", held)
    try:
        yield held
    finally:
        held.release_all()
        await asyncio.gather(*held.tasks, return_exceptions=True)


@asynccontextmanager
async def _installed_parses(
    monkeypatch: pytest.MonkeyPatch, gated: GatedParses
) -> AsyncIterator[GatedParses]:
    """``gated`` installed as the extractor behind :func:`_extract_only`; on exit every upload is
    released and awaited, and every parse thread has ended."""
    monkeypatch.setattr(filings_service, "extract_line_items", gated)
    monkeypatch.setattr(filings_service, "ingest_pdf", _extract_only)
    try:
        yield gated
    finally:
        gated.release_all()
        await asyncio.gather(*gated.tasks, return_exceptions=True)
        assert await gated.wait_all_finished(), "a gated parse thread did not end"


@pytest.fixture
async def gated_parses(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[GatedParses]:
    """:class:`GatedParses` whose parses ignore their ``cancel`` event, installed by
    :func:`_installed_parses`."""
    async with _installed_parses(monkeypatch, GatedParses()) as gated:
        yield gated


@pytest.fixture
async def cancellable_parses(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[GatedParses]:
    """:class:`GatedParses` whose parses end once their ``cancel`` event is set, installed by
    :func:`_installed_parses`."""
    async with _installed_parses(monkeypatch, GatedParses(honours_cancel=True)) as gated:
        yield gated


@pytest.fixture
async def make_host(
    make_settings: Callable[..., TerminalSettings],
) -> AsyncIterator[Callable[[int], Awaitable[Host]]]:
    """Factory ``async (limit) -> Host`` of independently mounted host apps."""
    async with AsyncExitStack() as stack:

        async def build(limit: int) -> Host:
            return await Host(make_settings, limit, stack).start()

        yield build


# ---------------------------------------------------------------------------------------------
# PdfParseLimiter
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "steps",
    [
        pytest.param(
            [Take(2, True)],
            id="first_request_sets_the_limit",
        ),
        pytest.param(
            [
                *[Take(3, True)] * 3,
                Take(1, False),
                Release(),
                Take(1, False),
                Release(),
                Take(1, False),
                Release(),
                Take(1, True),
                Take(1, False),
            ],
            id="shrink_while_held_admits_nothing_until_active_is_below_the_new_limit",
        ),
        pytest.param(
            [
                Take(1, True),
                Take(1, False),
                Take(3, True),
                Take(3, True),
                Take(3, False),
                Release(),
                Take(3, True),
                Take(3, False),
            ],
            id="grow_while_held_admits_up_to_the_new_limit",
        ),
        pytest.param(
            [
                Take(2, True),
                Take(2, True),
                Take(1, False),
                Take(2, False),
                Release(),
                Take(2, True),
                Take(2, False),
            ],
            id="shrink_then_grow_back_keeps_the_held_parses",
        ),
        pytest.param(
            [
                *[Take(4, True)] * 4,
                Take(1, False),
                Take(2, False),
                Release(),
                Release(),
                Take(2, False),
                Release(),
                Take(2, True),
                Take(2, False),
            ],
            id="grow_cancels_part_of_a_pending_retirement",
        ),
    ],
)
async def test_slots_follow_the_limit_in_force(steps: Sequence[Step]) -> None:
    """Each request sees the limit it carries, and the slots held under earlier limits."""
    limiter = PdfParseLimiter()
    held = HeldSlots(limiter)
    for number, step in enumerate(steps):
        if isinstance(step, Release):
            await held.release()
        else:
            assert await held.take(step.limit) is step.admitted, f"step {number}: {step}"
            assert limiter.limit == step.limit
        assert limiter.active == held.count, f"step {number}: {step}"


async def test_admission_never_exceeds_the_limit_in_force_across_every_resize_sequence() -> None:
    """Every sequence of six requests at limits 1-3 and releases admits exactly while fewer
    parses than the request's limit are active, and leaves no slot lost or leaked."""
    operations = (1, 2, 3, None)  # a request at that limit, or None: the oldest parse finishes
    for sequence in itertools.product(operations, repeat=6):
        limiter = PdfParseLimiter()
        held = HeldSlots(limiter)
        for limit in sequence:
            if limit is None:
                if held.count:
                    await held.release()
                continue
            expected = held.count < limit
            assert await held.take(limit) is expected, sequence
            assert limiter.active == held.count, sequence
            if expected:
                assert limiter.active <= limit, sequence
        while held.count:
            await held.release()
        assert limiter.active == 0, sequence
        assert [await held.take(3) for _ in range(4)] == [True, True, True, False], sequence


HUGE_PARSE_LIMIT: Final = 10**7
"""Far above the highest ``pdf_max_concurrency`` that ``TerminalSettings`` allows: a limiter that
resizes one slot at a time takes ten million steps to reach it, failing the time bound below
without hanging the run, while constant-time accounting meets it."""
RESIZE_SECONDS_BOUND: Final = 0.1


async def test_a_huge_limit_is_applied_without_per_slot_work() -> None:
    """Growing to a huge limit and shrinking back to 1 each take constant time, and both keep the
    slot in use: the shrink refuses while it is held, and its release admits the next request."""
    limiter = PdfParseLimiter()
    held = HeldSlots(limiter)

    started = time.perf_counter()
    grown = await held.take(HUGE_PARSE_LIMIT)
    grow_seconds = time.perf_counter() - started
    assert grown
    assert (limiter.limit, limiter.active) == (HUGE_PARSE_LIMIT, 1)

    started = time.perf_counter()
    shrunk = await held.take(1)
    shrink_seconds = time.perf_counter() - started
    assert not shrunk
    assert (limiter.limit, limiter.active) == (1, 1)

    assert grow_seconds < RESIZE_SECONDS_BOUND
    assert shrink_seconds < RESIZE_SECONDS_BOUND
    await held.release()
    assert await held.take(1)
    assert (limiter.limit, limiter.active) == (1, 1)


class ParseFailedError(Exception):
    """Raised inside a held slot to stand for a failing parse."""


SLOT_WAIT_SECONDS: Final = 10.0
"""How long the abnormal-ending test waits for its parse to hold the slot, and then to end,
before it fails, so a parse that goes wrong first reports its own error instead of hanging."""


async def wait_for_signal(event: asyncio.Event, producer: asyncio.Task[None]) -> None:
    """Wait until ``producer`` sets ``event``, for at most :data:`SLOT_WAIT_SECONDS`.

    A producer that ends without setting it fails the test at once: with the producer's own
    exception when it raised one, otherwise with a message saying it was cancelled or returned.
    """
    waiter = asyncio.create_task(event.wait())
    try:
        await asyncio.wait(
            {waiter, producer}, timeout=SLOT_WAIT_SECONDS, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
    if event.is_set():
        return
    if not producer.done():
        pytest.fail(f"the parse did not take its slot within {SLOT_WAIT_SECONDS} s")
    if producer.cancelled():
        pytest.fail("the parse was cancelled before it took its slot")
    await producer  # re-raises the exception the parse ended with
    pytest.fail("the parse returned before it took its slot")


@pytest.mark.parametrize("ending", ["exception", "cancellation"])
async def test_slot_is_given_back_when_the_block_ends_abnormally(ending: str) -> None:
    """A parse that raises or whose request is cancelled frees its slot for the next request."""
    limiter = PdfParseLimiter()
    inside = asyncio.Event()

    async def parse() -> None:
        async with limiter.slot(1) as admitted:
            assert admitted
            assert limiter.active == 1
            inside.set()
            if ending == "exception":
                raise ParseFailedError
            await asyncio.Event().wait()

    task = asyncio.create_task(parse())
    try:
        await wait_for_signal(inside, task)
        if ending == "cancellation":
            async with limiter.slot(1) as admitted:
                assert not admitted
            task.cancel()
        expected: type[BaseException] = (
            ParseFailedError if ending == "exception" else asyncio.CancelledError
        )
        with pytest.raises(expected):
            async with asyncio.timeout(SLOT_WAIT_SECONDS):
                await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert limiter.active == 0
    async with limiter.slot(1) as admitted:
        assert admitted


@pytest.mark.parametrize("limit", [0, -1])
async def test_limit_below_one_is_refused(limit: int) -> None:
    """``TerminalSettings`` requires ``pdf_max_concurrency >= 1``; anything lower raises."""
    limiter = PdfParseLimiter()
    with pytest.raises(ValueError, match=rf"^The PDF parse limit must be at least 1, got {limit}$"):
        async with limiter.slot(limit):
            pass
    assert (limiter.limit, limiter.active) == (0, 0)


async def test_limiter_dependency_requires_the_upload_route() -> None:
    """Outside ``TerminalUploadRoute`` no limiter is in the request scope, which is an error."""
    request = Request({"type": "http", "method": "POST", "path": INGEST_PDF_URL, "headers": []})
    with pytest.raises(RuntimeError, match="TerminalUploadRoute"):
        await get_pdf_parse_limiter(request)


# ---------------------------------------------------------------------------------------------
# Separately mounted hosts
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("shared_limiter", "second_host_admits"),
    [
        pytest.param(False, True, id="each_mount_owns_its_limiter"),
        pytest.param(True, False, id="host_override_shares_one_limiter"),
    ],
)
async def test_hosts_in_one_process_keep_separate_parse_slots(
    make_host: Callable[[int], Awaitable[Host]],
    parses: HeldParses,
    shared_limiter: bool,
    second_host_admits: bool,
) -> None:
    """Host A at limit 2 holds two parses; host B at limit 1 then uploads, and A's third upload
    is refused.

    Each mount counts only its own parses, so B is admitted and A stays full. A host that
    overrides ``get_pdf_parse_limiter`` with one shared limiter gets one count for both mounts.
    """
    host_a = await make_host(2)
    host_b = await make_host(1)
    if shared_limiter:
        shared = PdfParseLimiter()

        async def shared_parse_limiter() -> PdfParseLimiter:
            return shared

        for host in (host_a, host_b):
            host.app.dependency_overrides[get_pdf_parse_limiter] = shared_parse_limiter

    parses.hold(host_a)
    parses.hold(host_a)
    await parses.wait_for_calls(2)

    if second_host_admits:
        parses.hold(host_b)
        await parses.wait_for_calls(3)
        assert_busy(await host_b.upload())
    else:
        assert_busy(await host_b.upload())
    assert_busy(await host_a.upload())

    assert parses.calls == (3 if second_host_admits else 2)
    parses.release_all()
    for outcome in await asyncio.gather(*parses.tasks):
        assert_coded(outcome, 409, "released")


async def test_settings_change_keeps_counting_the_parses_already_admitted(
    make_host: Callable[[int], Awaitable[Host]], parses: HeldParses
) -> None:
    """With two parses held at limit 2, a request at limit 1 and then one back at limit 2 are
    both refused; only once a held parse ends is the next request admitted."""
    host = await make_host(2)
    # Calls are released in arrival order, so the second upload starts only once the first has
    # arrived: then release(0) ends ``first``.
    first = parses.hold(host)
    await parses.wait_for_calls(1)
    parses.hold(host)
    await parses.wait_for_calls(2)

    host.limit = 1
    async with asyncio.timeout(10):
        assert_busy(await host.upload())
    host.limit = 2
    async with asyncio.timeout(10):
        assert_busy(await host.upload())
    assert parses.calls == 2

    parses.release(0)
    async with asyncio.timeout(10):
        assert_coded(await first, 409, "released")
    parses.hold(host)
    await parses.wait_for_calls(3)
    async with asyncio.timeout(10):
        assert_busy(await host.upload())
    assert parses.calls == 3


async def test_settings_fallback_warning_is_logged_once_per_mount(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A settings override returning a non-``TerminalSettings`` is warned about once per mount.

    The size guard then applies the default settings, which here come from no ``.env`` and no
    ``TERMINAL_*`` variable, and refuses the declared 1 TB body with 413 before any dependency.
    """
    monkeypatch.chdir(tmp_path)
    for name in [name for name in os.environ if name.startswith("TERMINAL_")]:
        monkeypatch.delenv(name)
    get_terminal_settings.cache_clear()
    caplog.set_level(logging.WARNING, logger=INGEST_LOGGER)
    try:
        for uploads in (2, 1):
            app = FastAPI()
            app.include_router(terminal_router)
            app.dependency_overrides[get_terminal_settings] = lambda: {"pdf_max_bytes": 1}
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://host.test") as http:
                for _ in range(uploads):
                    response = await http.post(
                        INGEST_PDF_URL, content=b"", headers={"content-length": str(10**12)}
                    )
                    assert_coded(response, 413, "request_too_large")
    finally:
        get_terminal_settings.cache_clear()

    warnings = [
        record.getMessage()
        for record in caplog.records
        if record.name == INGEST_LOGGER and SETTINGS_FALLBACK_WARNING in record.getMessage()
    ]
    assert len(warnings) == 2, warnings
    assert all("returned dict" in message for message in warnings), warnings


# ---------------------------------------------------------------------------------------------
# Cancelled uploads and their parse threads
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("limit", [1, 2])
@pytest.mark.parametrize(
    "cancellation",
    [
        pytest.param(TASK_CANCEL, id="task_cancel"),
        pytest.param(SCOPE_CANCEL, id="anyio_cancel_scope"),
    ],
)
async def test_cancelled_upload_holds_its_slot_until_its_parse_thread_ends(
    make_host: Callable[[int], Awaitable[Host]],
    gated_parses: GatedParses,
    limit: int,
    cancellation: str,
) -> None:
    """``limit`` uploads hold every slot with their parses running in worker threads, and the
    first one is then cancelled: by ``Task.cancel()`` on its task, as a server shutdown or an
    asyncio task group does, or through an anyio cancel scope around it, as a host's anyio task
    group or timeout does.

    The cancellation sets the parse's ``cancel`` event, which these parses ignore (the real
    extractor kills its parser process; see the next test). A worker thread cannot be stopped,
    so the cancelled request stays in flight and keeps its slot while its parse runs, without
    spinning the event loop, and a new upload is refused with 429. Once that parse ends the
    request ends: a task cancellation is re-raised, and a cancel scope's cancellation stays
    inside that scope. The next upload is then admitted. No more than ``limit`` parses ever run
    at once, and every other parse still ends as its coded 422.
    """
    host = await make_host(limit)
    scope = anyio.CancelScope()

    async def first_upload() -> httpx.Response | None:
        with scope:
            response = await host.upload()
            await asyncio.sleep(0)  # the caller's next await
            return response
        return None  # the scope caught its own cancellation

    first = asyncio.create_task(first_upload())
    gated_parses.tasks.append(first)
    assert await gated_parses.wait_started(0)
    for index in range(1, limit):
        gated_parses.hold(host)
        assert await gated_parses.wait_started(index)

    if cancellation == TASK_CANCEL:
        first.cancel()
    else:
        scope.cancel()
    cpu_before = time.process_time()
    done, _ = await asyncio.wait({first}, timeout=0.5)
    cpu_used = time.process_time() - cpu_before
    assert not done, "the cancelled request ended while its parse thread was still running"
    assert cpu_used < 0.1, f"the waiting request spun the event loop for {cpu_used:.3f} s of CPU"
    async with asyncio.timeout(10):
        refused = await host.upload()
    assert_busy(refused)
    assert gated_parses.arrived == limit
    assert not gated_parses.parse(0).finished.is_set()

    gated_parses.release(0)
    if cancellation == TASK_CANCEL:
        with pytest.raises(asyncio.CancelledError):
            async with asyncio.timeout(10):
                await first
    else:
        async with asyncio.timeout(10):
            assert await first is None
        assert scope.cancelled_caught
    assert gated_parses.parse(0).finished.is_set()

    gated_parses.hold(host)
    assert await gated_parses.wait_started(limit)
    gated_parses.release_all()
    for outcome in await asyncio.gather(*gated_parses.tasks[1:]):
        assert outcome.status_code == 422, outcome.text
        assert outcome.json()["detail"]["code"] == "pdf_unreadable", outcome.text
    assert gated_parses.max_running == limit


@pytest.mark.parametrize("limit", [1, 2])
@pytest.mark.parametrize(
    "cancellation",
    [
        pytest.param(TASK_CANCEL, id="task_cancel"),
        pytest.param(SCOPE_CANCEL, id="anyio_cancel_scope"),
    ],
)
async def test_cancelled_upload_stops_its_parse_and_frees_its_slot_promptly(
    make_host: Callable[[int], Awaitable[Host]],
    cancellable_parses: GatedParses,
    limit: int,
    cancellation: str,
) -> None:
    """``limit`` uploads hold every slot, and the first one is then cancelled by
    ``Task.cancel()`` or through an anyio cancel scope around it, as in the previous test.

    Here the parse honours its ``cancel`` event, as the real extractor does by killing its
    parser process, so the cancelled request ends within moments, without its parse being
    released: a task cancellation is re-raised, and a cancel scope's cancellation stays inside
    that scope. Its slot is then free for the next upload at once, no more than ``limit`` parses
    ever run at once, and every other parse still ends as its coded 422.
    """
    host = await make_host(limit)
    scope = anyio.CancelScope()

    async def first_upload() -> httpx.Response | None:
        with scope:
            response = await host.upload()
            await asyncio.sleep(0)  # the caller's next await
            return response
        return None  # the scope caught its own cancellation

    first = asyncio.create_task(first_upload())
    cancellable_parses.tasks.append(first)
    assert await cancellable_parses.wait_started(0)
    for index in range(1, limit):
        cancellable_parses.hold(host)
        assert await cancellable_parses.wait_started(index)

    if cancellation == TASK_CANCEL:
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            async with asyncio.timeout(2):
                await first
    else:
        scope.cancel()
        async with asyncio.timeout(2):
            assert await first is None
        assert scope.cancelled_caught
    assert cancellable_parses.parse(0).finished.is_set()
    assert cancellable_parses.cancelled == [0]

    cancellable_parses.hold(host)
    assert await cancellable_parses.wait_started(limit)
    cancellable_parses.release_all()
    for outcome in await asyncio.gather(*cancellable_parses.tasks[1:]):
        assert outcome.status_code == 422, outcome.text
        assert outcome.json()["detail"]["code"] == "pdf_unreadable", outcome.text
    assert cancellable_parses.max_running == limit


# ---------------------------------------------------------------------------------------------
# Request body errors: hostile JSON bodies get one of the two 422 shapes or the coded 413, never a
# 500 or a 400
#
# Two layers make every rejected JSON body within the bound answer with FastAPI's standard 422
# list:
#
# * ``TerminalRequestModel``'s wrap hook re-spells, as a string FastAPI's default handler can
#   serialize, every rejected input it cannot render as sent: a NaN or infinite float as its
#   JSON literal (``"NaN"``, ``"Infinity"``, ``"-Infinity"``), a ``Decimal`` as its exact
#   decimal string, and ``bytes``, the raw body of a request sent without a JSON media type, as
#   UTF-8 text with U+FFFD for each invalid sequence. It applies on any route class, so both route
#   classes are tested.
# * ``TerminalJsonRoute`` decodes a JSON number with a fraction or an exponent as an exact
#   ``Decimal``, so it is judged exactly as the same value written as a decimal string; the
#   plain route, which decodes it as a binary float, is the control. It also turns every
#   decoding failure of the standard-library JSON decoder (deep nesting, over-long integer
#   literals, number exponents beyond the decimal range, invalid UTF-8, unpaired surrogates) into
#   FastAPI's own ``json_invalid`` entry instead of a 400 with a string ``detail``. A host's
#   ``RequestValidationError`` handler receives the rejected body with each value FastAPI's
#   encoder cannot render spelled as the 422 list spells it.
#
# ``TerminalJsonRoute`` also bounds the body to 64 KiB: a larger one, declared by its
# ``Content-Length`` or counted while it is read, is the coded 413 ``request_too_large``. Every
# verdict is raised only after the router's principal dependency; the 401 propagation section
# above proves that order inside a host.
#
# The app mirrors the planned body routes, ``POST /ipo/valuation`` and ``POST /watchlists``,
# each on a plain ``APIRouter()`` and on ``APIRouter(route_class=TerminalJsonRoute)``, included
# under a prefixed router carrying the terminal principal dependency as ``terminal_router``
# does. Bodies are sent as raw bytes, because the hostile literals are not producible by
# ``json.dumps``. No database is touched.
#
# FastAPI reads the endpoint signatures at runtime, so this module does not use
# ``from __future__ import annotations``.
# ---------------------------------------------------------------------------------------------

PLAIN_PREFIX = "/plain/api/v1/terminal"
JSON_ROUTE_PREFIX = "/json-route/api/v1/terminal"
BOTH_PREFIXES = [
    pytest.param(PLAIN_PREFIX, id="APIRoute"),
    pytest.param(JSON_ROUTE_PREFIX, id="TerminalJsonRoute"),
]
IPO = "/ipo/valuation"
WATCHLISTS = "/watchlists"
JSON_HEADERS = {"content-type": "application/json"}

# The Quickmart preset body of tests/unit/quant/test_dcf_and_comparables.py::request_payload().
QUICKMART: dict[str, object] = {
    "offer_price_kes": "7.50",
    "revenue_growth_pct": "10.00",
    "ebitda_margin_pct": "4.50",
    "margin_expansion_pct": "0.50",
    "wacc_pct": "18.00",
    "terminal_growth_pct": "5.00",
    "net_debt_kes": "4000000000",
    "nwc_pct_revenue": "-3.00",
    "peer_ev_ebitda_multiple": "6.00",
    "dcf_weight_pct": "50",
    "capex_pct_revenue": "2.00",
    "da_pct_revenue": "1.50",
    "tax_rate_pct": "30.00",
    "forecast_years": 5,
    "base_revenue_kes": "45000000000",
    "shares_outstanding": 1_000_000_000,
    "shares_offered": 500_000_000,
    "primary_proceeds_kes": "0.00",
}

FINITE_NUMBER_MSG = "Input should be a finite number"
INT_TYPE_MSG = "Input should be a valid integer"
DECIMAL_PARSING_MSG = "Input should be a valid decimal"
STRING_TYPE_MSG = "Input should be a valid string"
JSON_NESTING_ERROR = "JSON nesting exceeds the 200-level limit"
LONE_SURROGATE_ERROR = "JSON text contains an unpaired UTF-16 surrogate"
INTEGER_LIMIT_ERROR = "Integer literal exceeds the 4300-digit limit"
DECIMAL_RANGE_ERROR = "Number literal exponent exceeds the decimal range"
MAX_DIGITS_MSG = "Decimal input should have no more than 28 digits in total"
MODEL_ATTRIBUTES_MSG = "Input should be a valid dictionary or object to extract fields from"


def quickmart_raw(**raw_values: str) -> bytes:
    """The Quickmart body as JSON text, with each named member's value replaced by raw JSON text.

    A name that is not a Quickmart field is appended as an extra member.
    """
    members = {name: json.dumps(value) for name, value in QUICKMART.items()}
    members.update(raw_values)
    text = ", ".join(f"{json.dumps(name)}: {value}" for name, value in members.items())
    return ("{" + text + "}").encode()


def nested_array(depth: int) -> str:
    """An empty JSON array nested ``depth`` levels deep."""
    return "[" * depth + "]" * depth


def nested_object(depth: int) -> str:
    """A JSON object nested ``depth`` levels deep around the number 1."""
    return '{"a":' * depth + "1" + "}" * depth


def build_app() -> FastAPI:
    """Both body routes on a plain and a ``TerminalJsonRoute`` sub-router, mounted like a host."""
    app = FastAPI()
    for prefix, sub_router in (
        (PLAIN_PREFIX, APIRouter()),
        (JSON_ROUTE_PREFIX, APIRouter(route_class=TerminalJsonRoute)),
    ):

        @sub_router.post(IPO)
        async def value_ipo(body: IpoValuationRequest) -> IpoValuationResponse:
            return await ipo_service.value_ipo(body)

        @sub_router.post(WATCHLISTS, status_code=201)
        async def create_watchlist(body: WatchlistCreate) -> WatchlistCreate:
            return body

        terminal = APIRouter(prefix=prefix, dependencies=[Depends(get_terminal_principal)])
        terminal.include_router(sub_router)
        app.include_router(terminal)
    return app


# Named apart from the integration conftest's ``client``, which drives the seeded standalone app.
@pytest.fixture
async def body_client() -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client on the test app; an exception escaping the app fails the test."""
    transport = httpx.ASGITransport(app=build_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        yield http


async def post_raw(client: httpx.AsyncClient, path: str, body: bytes) -> httpx.Response:
    """POST ``body`` unchanged as ``application/json``."""
    return await client.post(path, content=body, headers=JSON_HEADERS)


# ---------------------------------------------------------------------------------------------
# Non-finite JSON number literals: the standard 422 list on every route class
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("path", "body", "expected"),
    [
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=literal),
            {
                "type": "finite_number",
                "loc": ["body", "offer_price_kes"],
                "msg": FINITE_NUMBER_MSG,
                "input": spelled,
            },
            id=f"offer_price_kes_{literal}",
        )
        for literal, spelled in (
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
        )
    ]
    + [
        pytest.param(
            IPO,
            quickmart_raw(x="NaN"),
            {
                "type": "extra_forbidden",
                "loc": ["body", "x"],
                "msg": "Extra inputs are not permitted",
                "input": "NaN",
            },
            id="extra_member_NaN",
        ),
        # Share counts are strict integers, so a NaN is refused as a non-integer, not as NaN.
        pytest.param(
            IPO,
            quickmart_raw(shares_outstanding="NaN"),
            {
                "type": "int_type",
                "loc": ["body", "shares_outstanding"],
                "msg": INT_TYPE_MSG,
                "input": "NaN",
            },
            id="shares_outstanding_NaN",
        ),
        pytest.param(
            IPO,
            quickmart_raw(sensitivity_wacc_pct="[NaN]"),
            {
                "type": "finite_number",
                "loc": ["body", "sensitivity_wacc_pct", 0],
                "msg": FINITE_NUMBER_MSG,
                "input": "NaN",
            },
            id="sensitivity_axis_element_NaN",
        ),
        pytest.param(
            IPO,
            b"NaN",
            {
                "type": "model_attributes_type",
                "loc": ["body"],
                "msg": "Input should be a valid dictionary or object to extract fields from",
                "input": "NaN",
            },
            id="whole_body_NaN",
        ),
        pytest.param(
            IPO,
            b"[NaN]",
            {
                "type": "model_attributes_type",
                "loc": ["body"],
                "msg": "Input should be a valid dictionary or object to extract fields from",
                "input": ["NaN"],
            },
            id="whole_body_list_NaN",
        ),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes='{"v": [NaN, 1, {"w": -Infinity}]}'),
            {
                "type": "decimal_type",
                "loc": ["body", "offer_price_kes"],
                "msg": "Decimal input should be an integer, float, string or Decimal object",
                "input": {"v": ["NaN", 1, {"w": "-Infinity"}]},
            },
            id="offer_price_kes_nested_non_finite",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": NaN}',
            {
                "type": "string_type",
                "loc": ["body", "name"],
                "msg": STRING_TYPE_MSG,
                "input": "NaN",
            },
            id="watchlist_name_NaN",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "ok", "tickers": [Infinity]}',
            {
                "type": "string_type",
                "loc": ["body", "tickers", 0],
                "msg": STRING_TYPE_MSG,
                "input": "Infinity",
            },
            id="watchlist_ticker_Infinity",
        ),
    ],
)
async def test_non_finite_literal_returns_standard_422(
    body_client: httpx.AsyncClient, prefix: str, path: str, body: bytes, expected: dict[str, Any]
) -> None:
    response = await post_raw(body_client, prefix + path, body)

    assert response.status_code == 422
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {"detail": [expected]}


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
async def test_non_finite_literal_leaves_other_errors_unchanged(
    body_client: httpx.AsyncClient, prefix: str
) -> None:
    """Every other error of the body keeps its type, location, message and context."""
    with_nan = await post_raw(
        body_client, prefix + IPO, quickmart_raw(offer_price_kes="NaN", wacc_pct='"100"')
    )
    without_nan = await post_raw(body_client, prefix + IPO, quickmart_raw(wacc_pct='"100"'))

    assert with_nan.status_code == without_nan.status_code == 422
    finite_error, *other_errors = with_nan.json()["detail"]
    assert finite_error["type"] == "finite_number"
    assert finite_error["input"] == "NaN"
    assert other_errors == without_nan.json()["detail"]
    assert other_errors[0]["type"] == "less_than_equal"
    assert "ctx" in other_errors[0]


def test_custom_error_type_survives_non_finite_rendering() -> None:
    """An error type outside pydantic-core's own keeps its code, message and context."""

    def reject(value: str) -> str:
        raise PydanticCustomError("custom_code", "Custom rejection of {value}", {"value": value})

    class CustomRequest(TerminalRequestModel):
        label: Annotated[str, AfterValidator(reject)]
        amount: RequestDecimal

    with pytest.raises(ValidationError) as caught:
        CustomRequest.model_validate(json.loads('{"label": "abc", "amount": NaN}'))

    errors = caught.value.errors(include_url=False)
    rendered = json.loads(json.dumps(errors, allow_nan=False))
    assert rendered == [
        {
            "type": "custom_code",
            "loc": ["label"],
            "msg": "Custom rejection of abc",
            "input": "abc",
            "ctx": {"value": "abc"},
        },
        {"type": "finite_number", "loc": ["amount"], "msg": FINITE_NUMBER_MSG, "input": "NaN"},
    ]


# ---------------------------------------------------------------------------------------------
# Canonical numbers: share counts are JSON integers, decimal strings are ASCII decimal text
# ---------------------------------------------------------------------------------------------

# Messages of the standard 422 entries that carry no context.
CONTEXT_FREE_MESSAGES: Final[dict[str, str]] = {
    "int_type": INT_TYPE_MSG,
    "decimal_parsing": DECIMAL_PARSING_MSG,
    "finite_number": FINITE_NUMBER_MSG,
}


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("member", "raw", "loc", "error_type", "rejected"),
    [
        pytest.param("shares_offered", "true", [], "int_type", True, id="shares_offered_true"),
        pytest.param("shares_offered", "false", [], "int_type", False, id="shares_offered_false"),
        pytest.param(
            "shares_offered",
            '"5_0000_0000"',
            [],
            "int_type",
            "5_0000_0000",
            id="shares_offered_underscored_string",
        ),
        pytest.param(
            "shares_outstanding",
            '"1000000000"',
            [],
            "int_type",
            "1000000000",
            id="shares_outstanding_digit_string",
        ),
        pytest.param("forecast_years", '"5"', [], "int_type", "5", id="forecast_years_string"),
        pytest.param(
            "offer_price_kes",
            '"\\u0667.\\u0665\\u0660"',
            [],
            "decimal_parsing",
            "\u0667.\u0665\u0660",
            id="offer_price_kes_arabic_indic_digits",
        ),
        pytest.param(
            "offer_price_kes",
            '"\\uff17.\\uff15\\uff10"',
            [],
            "decimal_parsing",
            "\uff17.\uff15\uff10",
            id="offer_price_kes_full_width_digits",
        ),
        pytest.param(
            "offer_price_kes",
            '" 7.50\\n"',
            [],
            "decimal_parsing",
            " 7.50\n",
            id="offer_price_kes_whitespace_padded",
        ),
        pytest.param(
            "offer_price_kes",
            '"7_50"',
            [],
            "decimal_parsing",
            "7_50",
            id="offer_price_kes_underscore",
        ),
        pytest.param(
            "sensitivity_wacc_pct",
            '["16", "\\u0661\\u0668"]',
            [1],
            "decimal_parsing",
            "\u0661\u0668",
            id="wacc_axis_element_arabic_indic_digits",
        ),
        # ASCII spellings of NaN and Infinity keep the decimal validator's own finite_number.
        pytest.param(
            "offer_price_kes",
            '"-Infinity"',
            [],
            "finite_number",
            "-Infinity",
            id="offer_price_kes_infinity_string",
        ),
        pytest.param(
            "offer_price_kes",
            '"sNaN"',
            [],
            "finite_number",
            "sNaN",
            id="offer_price_kes_snan_string",
        ),
    ],
)
async def test_non_canonical_number_returns_standard_422(
    body_client: httpx.AsyncClient,
    prefix: str,
    member: str,
    raw: str,
    loc: list[int],
    error_type: str,
    rejected: object,
) -> None:
    """A JSON boolean or string is not a share count or forecast horizon, and a decimal string
    with non-ASCII digits, whitespace or ``_`` separators is not a decimal."""
    response = await post_raw(body_client, prefix + IPO, quickmart_raw(**{member: raw}))

    assert response.status_code == 422
    assert response.json() == {
        "detail": [
            {
                "type": error_type,
                "loc": ["body", member, *loc],
                "msg": CONTEXT_FREE_MESSAGES[error_type],
                "input": rejected,
            }
        ]
    }


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("member", "raw"),
    [
        pytest.param("offer_price_kes", '"7.5"', id="fewer_fraction_digits"),
        pytest.param("offer_price_kes", '"+7.50"', id="explicit_plus"),
        pytest.param("offer_price_kes", '"7.5e0"', id="exponent"),
        pytest.param("offer_price_kes", '"75E-1"', id="upper_case_exponent"),
        pytest.param("offer_price_kes", "7.5", id="json_number"),
        pytest.param("dcf_weight_pct", '"50."', id="trailing_point"),
        pytest.param("margin_expansion_pct", '".5"', id="leading_point"),
        pytest.param("net_debt_kes", "4000000000", id="json_integer"),
        pytest.param(
            "sensitivity_wacc_pct", '["16", 17, 18.0, "1.9e1", "+20."]', id="wacc_axis_mixed_forms"
        ),
    ],
)
async def test_canonical_spellings_value_like_the_preset(
    body_client: httpx.AsyncClient, prefix: str, member: str, raw: str
) -> None:
    """Every canonical spelling of a preset value, as a JSON number or ASCII decimal text,
    including the ``"12."`` and ``".5"`` forms the sandbox may send, values exactly like the
    preset itself."""
    preset = await post_raw(body_client, prefix + IPO, quickmart_raw())
    response = await post_raw(body_client, prefix + IPO, quickmart_raw(**{member: raw}))

    assert preset.status_code == response.status_code == 200
    assert response.json()["blended_fair_value_per_share_kes"] == "6.82"
    assert response.json() == preset.json()


# ---------------------------------------------------------------------------------------------
# JSON numbers on TerminalJsonRoute: exact decimals, judged exactly as their decimal strings
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("members", "status_code", "code", "field"),
    [
        pytest.param(
            {"primary_proceeds_kes": "3750000000.00000001"},
            422,
            "primary_exceeds_offer_size",
            "primary_proceeds_kes",
            id="primary_just_over_offer_size",
        ),
        pytest.param(
            {"primary_proceeds_kes": "3750000000.00"}, 200, None, None, id="primary_at_offer_size"
        ),
        pytest.param(
            {"wacc_pct": "8.000000000000000001", "terminal_growth_pct": "8"},
            200,
            None,
            None,
            id="wacc_just_above_terminal_growth",
        ),
        pytest.param(
            {"wacc_pct": "8", "terminal_growth_pct": "8"},
            422,
            "wacc_not_above_terminal_growth",
            "wacc_pct",
            id="wacc_equal_to_terminal_growth",
        ),
        # The standard list entry itself is asserted in the decoded-form test below.
        pytest.param({"wacc_pct": "30.000000000000000001"}, 422, None, None, id="wacc_over_bound"),
    ],
)
async def test_json_number_is_judged_exactly_as_its_decimal_string(
    body_client: httpx.AsyncClient,
    members: dict[str, str],
    status_code: int,
    code: str | None,
    field: str | None,
) -> None:
    """A JSON number keeps every digit through the bounds and the cross-field checks, so it gets
    exactly the answer of the same value sent as a decimal string, even where a binary float
    would round it onto or off a limit."""
    as_numbers = quickmart_raw(**members)
    as_strings = quickmart_raw(**{name: json.dumps(raw) for name, raw in members.items()})

    numeric = await post_raw(body_client, JSON_ROUTE_PREFIX + IPO, as_numbers)
    textual = await post_raw(body_client, JSON_ROUTE_PREFIX + IPO, as_strings)

    assert numeric.status_code == status_code, numeric.text
    assert (numeric.status_code, numeric.json()) == (textual.status_code, textual.json())
    if code is not None:
        detail = numeric.json()["detail"]
        assert (detail["code"], detail["field"]) == (code, field)


@pytest.mark.parametrize(
    ("prefix", "path", "body", "expected"),
    [
        # The plain route decodes a fraction or an exponent as a binary float: the control.
        pytest.param(
            PLAIN_PREFIX,
            IPO,
            quickmart_raw(offer_price_kes=literal),
            {
                "type": "finite_number",
                "loc": ["body", "offer_price_kes"],
                "msg": FINITE_NUMBER_MSG,
                "input": spelled,
            },
            id=f"APIRoute-offer_price_kes_{literal}",
        )
        for literal, spelled in (("1e999999", "Infinity"), ("-1e999999", "-Infinity"))
    ]
    + [
        pytest.param(
            PLAIN_PREFIX,
            IPO,
            quickmart_raw(forecast_years="5.0"),
            {
                "type": "int_type",
                "loc": ["body", "forecast_years"],
                "msg": INT_TYPE_MSG,
                "input": 5.0,
            },
            id="APIRoute-forecast_years_5.0",
        ),
    ]
    + [
        pytest.param(
            JSON_ROUTE_PREFIX,
            IPO,
            quickmart_raw(offer_price_kes=literal),
            {
                "type": "decimal_max_digits",
                "loc": ["body", "offer_price_kes"],
                "msg": MAX_DIGITS_MSG,
                "input": spelled,
                "ctx": {"max_digits": 28},
            },
            id=f"TerminalJsonRoute-offer_price_kes_{name}",
        )
        for name, literal, spelled in (
            ("1e999999", "1e999999", "1E+999999"),
            ("-1e999999", "-1e999999", "-1E+999999"),
            ("1e999999999999999999", "1e999999999999999999", "1E+999999999999999999"),
            ("400_digit_fraction", "9" * 400 + ".5", "9" * 400 + ".5"),
        )
    ]
    + [
        pytest.param(
            JSON_ROUTE_PREFIX,
            IPO,
            quickmart_raw(forecast_years="5.0"),
            {
                "type": "int_type",
                "loc": ["body", "forecast_years"],
                "msg": INT_TYPE_MSG,
                "input": "5.0",
            },
            id="TerminalJsonRoute-forecast_years_5.0",
        ),
        pytest.param(
            JSON_ROUTE_PREFIX,
            IPO,
            quickmart_raw(wacc_pct="30.000000000000000001"),
            {
                "type": "less_than_equal",
                "loc": ["body", "wacc_pct"],
                "msg": "Input should be less than or equal to 30.00",
                "input": "30.000000000000000001",
                "ctx": {"le": 30.0},
            },
            id="TerminalJsonRoute-wacc_pct_just_over_30",
        ),
        pytest.param(
            JSON_ROUTE_PREFIX,
            WATCHLISTS,
            b'{"name": 1e999999}',
            {
                "type": "string_type",
                "loc": ["body", "name"],
                "msg": STRING_TYPE_MSG,
                "input": "1E+999999",
            },
            id="TerminalJsonRoute-watchlist_name_1e999999",
        ),
        pytest.param(
            JSON_ROUTE_PREFIX,
            WATCHLISTS,
            b'{"name": "ok", "tickers": [1.5]}',
            {
                "type": "string_type",
                "loc": ["body", "tickers", 0],
                "msg": STRING_TYPE_MSG,
                "input": "1.5",
            },
            id="TerminalJsonRoute-watchlist_ticker_1.5",
        ),
    ],
)
async def test_rejected_number_is_echoed_as_it_was_decoded(
    body_client: httpx.AsyncClient, prefix: str, path: str, body: bytes, expected: dict[str, Any]
) -> None:
    """A rejected fraction or exponent is echoed as the float a plain route decodes, and on
    ``TerminalJsonRoute`` as the exact decimal string, at once: never as the million-digit
    integer, infinite float or rounded float that FastAPI's encoder would make of a ``Decimal``.
    """
    started = time.monotonic()
    response = await post_raw(body_client, prefix + path, body)
    elapsed = time.monotonic() - started

    assert response.status_code == 422, response.text
    assert response.json() == {"detail": [expected]}
    assert elapsed < 2.0


# ---------------------------------------------------------------------------------------------
# Bodies without a JSON media type: FastAPI validates the raw bytes, echoed as UTF-8 text
# ---------------------------------------------------------------------------------------------

NON_JSON_MEDIA = [
    pytest.param({"content-type": "application/octet-stream"}, id="octet_stream"),
    pytest.param({"content-type": "text/plain"}, id="text_plain"),
    pytest.param({}, id="no_content_type"),
]


@pytest.mark.parametrize(
    "host_refuses", [pytest.param(False, id="admitted"), pytest.param(True, id="host_401")]
)
@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize("path", [IPO, WATCHLISTS])
@pytest.mark.parametrize("headers", NON_JSON_MEDIA)
@pytest.mark.parametrize(
    ("body", "echoed"),
    [
        pytest.param(b"\xff", "\ufffd", id="invalid_utf8"),
        pytest.param(b'{"name": "Banks"}', '{"name": "Banks"}', id="utf8_text"),
    ],
)
async def test_body_without_json_media_type_returns_standard_422(
    headers: dict[str, str],
    path: str,
    prefix: str,
    host_refuses: bool,
    body: bytes,
    echoed: str,
) -> None:
    """FastAPI does not decode a body that declares a non-JSON media type, or none, and
    validates its raw bytes instead: the standard 422 entry echoes them as UTF-8 text, with
    U+FFFD for each invalid sequence, never a 500. A principal override that raises 401 still
    answers first."""

    async def host_rejects_caller() -> TerminalPrincipal:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="host says no")

    app = build_app()
    if host_refuses:
        app.dependency_overrides[get_terminal_principal] = host_rejects_caller

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        response = await http.post(prefix + path, content=body, headers=headers)

    # The client adds no media type of its own to a body sent without one.
    assert response.request.headers.get("content-type") == headers.get("content-type")
    if host_refuses:
        assert response.status_code == 401, response.text
        assert response.json() == {"detail": "host says no"}
    else:
        assert response.status_code == 422, response.text
        assert response.json() == {
            "detail": [
                {
                    "type": "model_attributes_type",
                    "loc": ["body"],
                    "msg": MODEL_ATTRIBUTES_MSG,
                    "input": echoed,
                }
            ]
        }


# ---------------------------------------------------------------------------------------------
# Undecodable bodies on TerminalJsonRoute: FastAPI's json_invalid entry, never a 400
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "body", "position", "error"),
    [
        pytest.param(IPO, nested_array(10_000).encode(), 0, JSON_NESTING_ERROR, id="array_10k"),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=nested_array(10_000)),
            0,
            JSON_NESTING_ERROR,
            id="offer_price_kes_array_10k",
        ),
        pytest.param(IPO, nested_object(10_000).encode(), 0, JSON_NESTING_ERROR, id="object_10k"),
        # The deepest array that fits the 64 KiB body bound.
        pytest.param(IPO, nested_array(32_000).encode(), 0, JSON_NESTING_ERROR, id="array_32k"),
        # Decodable by the standard library, but too deep for FastAPI's error renderer.
        pytest.param(IPO, nested_array(5_000).encode(), 0, JSON_NESTING_ERROR, id="array_5k"),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=nested_array(200)),
            0,
            JSON_NESTING_ERROR,
            id="nesting_201_levels",
        ),
        pytest.param(
            IPO,
            quickmart_raw(base_revenue_kes="9" * 5_000),
            0,
            INTEGER_LIMIT_ERROR,
            id="base_revenue_kes_5000_digits",
        ),
        pytest.param(
            IPO,
            quickmart_raw(shares_outstanding="9" * 5_000),
            0,
            INTEGER_LIMIT_ERROR,
            id="shares_outstanding_5000_digits",
        ),
        pytest.param(
            IPO,
            b'{"offer_price_kes": "\xff"}',
            21,
            "Invalid utf-8 text: invalid start byte",
            id="invalid_utf8",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "a\xc3\x28"}',
            11,
            "Invalid utf-8 text: invalid continuation byte",
            id="watchlist_invalid_utf8",
        ),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes="1e99999999999999999999"),
            0,
            DECIMAL_RANGE_ERROR,
            id="offer_price_kes_exponent_above_decimal_range",
        ),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes="1e-99999999999999999999"),
            0,
            DECIMAL_RANGE_ERROR,
            id="offer_price_kes_exponent_below_decimal_range",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "\\ud800"}',
            0,
            LONE_SURROGATE_ERROR,
            id="escaped_lone_surrogate",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "ok", "\xed\xa0\x80": 1}',
            0,
            LONE_SURROGATE_ERROR,
            id="raw_lone_surrogate_key",
        ),
    ],
)
async def test_undecodable_body_returns_json_invalid(
    body_client: httpx.AsyncClient, path: str, body: bytes, position: int, error: str
) -> None:
    # Within the body bound, so the decoder rather than the bound judges every case.
    assert len(body) <= JSON_BODY_LIMIT_BYTES
    response = await post_raw(body_client, JSON_ROUTE_PREFIX + path, body)

    assert response.status_code == 422
    assert response.json() == {
        "detail": [
            {
                "type": "json_invalid",
                "loc": ["body", position],
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": error},
            }
        ]
    }


async def test_nesting_at_the_limit_is_validated(body_client: httpx.AsyncClient) -> None:
    """200 levels decode, so the body reaches validation and gets the field's own error."""
    response = await post_raw(
        body_client, JSON_ROUTE_PREFIX + IPO, quickmart_raw(offer_price_kes=nested_array(199))
    )

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["type"] == "decimal_type"
    assert error["loc"] == ["body", "offer_price_kes"]


async def test_host_validation_handler_receives_undecodable_body() -> None:
    """A host's RequestValidationError handler answers an undecodable body, not a 400, and
    receives the decoded body text as FastAPI's own ``json_invalid`` error carries it."""
    app = build_app()

    async def host_handler(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, RequestValidationError)
        return JSONResponse(
            {"host_errors": [error["type"] for error in exc.errors()], "body": exc.body}, 418
        )

    app.add_exception_handler(RequestValidationError, host_handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        response = await post_raw(http, JSON_ROUTE_PREFIX + IPO, nested_array(10_000).encode())

    assert response.status_code == 418
    assert response.json() == {"host_errors": ["json_invalid"], "body": nested_array(10_000)}


@pytest.mark.parametrize(
    ("path", "body", "headers", "echoed"),
    [
        pytest.param(
            WATCHLISTS,
            b'{"name": 1e999999}',
            JSON_HEADERS,
            {"name": "1E+999999"},
            id="decimal_exponent",
        ),
        pytest.param(
            IPO,
            quickmart_raw(wacc_pct="30.000000000000000001"),
            JSON_HEADERS,
            {**QUICKMART, "wacc_pct": "30.000000000000000001"},
            id="decimal_fraction",
        ),
        pytest.param(WATCHLISTS, b'{"name": NaN}', JSON_HEADERS, {"name": "NaN"}, id="nan"),
        pytest.param(
            WATCHLISTS,
            b"\xff",
            {"content-type": "application/octet-stream"},
            "\ufffd",
            id="invalid_utf8_octet_stream",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "\xed\xa0\x80',
            JSON_HEADERS,
            '{"name": "\ufffd',
            id="undecodable_raw_surrogate",
        ),
        pytest.param(
            WATCHLISTS, b'{"tickers": []}', JSON_HEADERS, {"tickers": []}, id="strings_only"
        ),
    ],
)
async def test_host_validation_handler_can_encode_the_rejected_body(
    path: str, body: bytes, headers: dict[str, str], echoed: object
) -> None:
    """A host handler written as FastAPI documents it, echoing ``exc.body`` through
    ``jsonable_encoder``, answers at once with the body as the 422 list spells its values: never
    the seconds of CPU and 500 that a raw ``Decimal``, NaN, ``bytes`` or surrogate would cost."""
    app = build_app()

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        started = time.monotonic()
        response = await http.post(JSON_ROUTE_PREFIX + path, content=body, headers=headers)
        elapsed = time.monotonic() - started

    assert response.status_code == 422, response.text
    assert response.json()["body"] == echoed
    assert response.json()["detail"]
    assert elapsed < 2.0


# ---------------------------------------------------------------------------------------------
# Body bound on TerminalJsonRoute: a body over 64 KiB is the coded 413, read at most to the bound
# ---------------------------------------------------------------------------------------------

TOO_LARGE_DETAIL: Final = {
    "code": "request_too_large",
    "message": "Request body exceeds 65536 bytes.",
    "field": None,
}
VALID_BODIES = [
    pytest.param(IPO, QUICKMART, 200, id="ipo"),
    pytest.param(WATCHLISTS, {"name": "Banks", "tickers": ["KCB"]}, 201, id="watchlist"),
]
FRAMINGS = [
    pytest.param(False, id="content_length"),
    pytest.param(True, id="chunked"),
]


@pytest.mark.parametrize("chunked", FRAMINGS)
@pytest.mark.parametrize(("path", "document", "status_code"), VALID_BODIES)
async def test_body_over_the_bound_returns_request_too_large(
    body_client: httpx.AsyncClient,
    caplog: pytest.LogCaptureFixture,
    path: str,
    document: dict[str, object],
    status_code: int,
    chunked: bool,
) -> None:
    """A body one byte over 64 KiB, valid apart from its size, is the coded 413 with one INFO
    record naming only the limit. Declared by its ``Content-Length``, the body is never read;
    counted while it is read, it is read to its end and dropped, never decoded."""
    body = framed(padded_json(document, JSON_BODY_LIMIT_BYTES + 1), chunked=chunked)
    caplog.set_level(logging.INFO, logger=DEPS_LOGGER)

    response = await body.post(body_client, JSON_ROUTE_PREFIX + path)

    assert response.status_code == 413, response.text
    assert response.json() == {"detail": TOO_LARGE_DETAIL}
    assert body.pulled == (JSON_BODY_LIMIT_BYTES + 1 if chunked else 0)
    assert too_large_records(caplog) == [TOO_LARGE_LOG]


@pytest.mark.parametrize("chunked", FRAMINGS)
@pytest.mark.parametrize(("path", "document", "status_code"), VALID_BODIES)
async def test_body_at_the_bound_is_judged_normally(
    body_client: httpx.AsyncClient,
    path: str,
    document: dict[str, object],
    status_code: int,
    chunked: bool,
) -> None:
    """A valid body of exactly 64 KiB, however it is framed, is read whole and answered exactly
    as the same document without its padding."""
    body = framed(padded_json(document, JSON_BODY_LIMIT_BYTES), chunked=chunked)

    response = await body.post(body_client, JSON_ROUTE_PREFIX + path)
    unpadded = await body_client.post(JSON_ROUTE_PREFIX + path, json=document)

    assert response.status_code == status_code, response.text
    assert body.pulled == JSON_BODY_LIMIT_BYTES
    assert (response.status_code, response.json()) == (unpadded.status_code, unpadded.json())


@pytest.mark.parametrize(
    "host_refuses", [pytest.param(False, id="admitted"), pytest.param(True, id="host_401")]
)
@pytest.mark.parametrize("path", [IPO, WATCHLISTS])
@pytest.mark.parametrize(
    "declared",
    [
        "200000000",
        "-1",
        "+12",
        "1_000",
        "1e3",
        "abc",
        pytest.param("9" * 5_000, id="5000_nines"),
        pytest.param(LONG_ZERO_PADDING + "65537", id="zero_padded_65537"),
        pytest.param(LONG_ZERO_PADDING + "100000", id="zero_padded_100000"),
    ],
)
async def test_invalid_or_oversized_content_length_is_refused_unread(
    declared: str, path: str, host_refuses: bool
) -> None:
    """A ``Content-Length`` over the bound, however many digits or leading zeroes it has, or one
    that is not a decimal integer (even one that ``int()`` would accept), is the coded 413 and
    not one byte of the body is read; a principal override that raises 401 still answers first,
    with the body equally unread."""

    async def host_rejects_caller() -> TerminalPrincipal:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="host says no")

    app = build_app()
    if host_refuses:
        app.dependency_overrides[get_terminal_principal] = host_rejects_caller
    body = TrackedBody(b'{"name": "x"}', declared=declared)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        response = await body.post(http, JSON_ROUTE_PREFIX + path)

    if host_refuses:
        assert response.status_code == 401, response.text
        assert response.json() == {"detail": "host says no"}
    else:
        assert response.status_code == 413, response.text
        assert response.json() == {"detail": TOO_LARGE_DETAIL}
    assert body.pulled == 0


@pytest.mark.parametrize(
    ("path", "data", "status_code"),
    [
        pytest.param(IPO, quickmart_raw(), 200, id="ipo"),
        pytest.param(IPO, b"{}", 422, id="ipo_schema_422"),
        pytest.param(WATCHLISTS, b'{"name": "Banks", "tickers": ["KCB"]}', 201, id="watchlist"),
        pytest.param(WATCHLISTS, b"{}", 422, id="watchlist_schema_422"),
        pytest.param(WATCHLISTS, b"", 422, id="watchlist_empty_body"),
    ],
)
async def test_zero_padded_content_length_is_judged_by_its_value(
    body_client: httpx.AsyncClient, path: str, data: bytes, status_code: int
) -> None:
    """A ``Content-Length`` of 4,300 leading zeroes and then the body's length, longer than
    ``int()`` converts, declares that same length: the body is read whole and answered exactly
    as with the canonical ``Content-Length``. An empty body's all-zero length is 0."""
    padded = TrackedBody(data, declared=LONG_ZERO_PADDING + str(len(data)))
    canonical = TrackedBody(data, declared=str(len(data)))

    response = await padded.post(body_client, JSON_ROUTE_PREFIX + path)
    expected = await canonical.post(body_client, JSON_ROUTE_PREFIX + path)

    assert response.status_code == status_code, response.text
    assert padded.pulled == canonical.pulled == len(data)
    assert (response.status_code, response.json()) == (expected.status_code, expected.json())


async def test_oversized_chunked_body_is_read_at_most_a_mebibyte_past_the_bound(
    body_client: httpx.AsyncClient,
) -> None:
    """A 4 MiB chunked body is the coded 413 once 1 MiB past the bound has been drained, give
    or take a chunk; the rest is never read."""
    body = framed(padded_json({}, 4 * JSON_BODY_DRAIN_BYTES), chunked=True)

    response = await body.post(body_client, JSON_ROUTE_PREFIX + IPO)

    assert response.status_code == 413, response.text
    assert response.json() == {"detail": TOO_LARGE_DETAIL}
    assert JSON_BODY_LIMIT_BYTES + JSON_BODY_DRAIN_BYTES < body.pulled
    assert body.pulled <= JSON_BODY_LIMIT_BYTES + JSON_BODY_DRAIN_BYTES + 2 * STREAM_CHUNK_BYTES


async def test_stalled_sender_past_the_bound_gets_413_after_the_drain_deadline(
    body_client: httpx.AsyncClient,
) -> None:
    """A chunked body that stops arriving once past the bound is answered 413 when the
    one-second drain deadline expires, not when the sender resumes."""
    sender_stalled = asyncio.Event()

    async def stalling_body() -> AsyncIterator[bytes]:
        yield b" " * (JSON_BODY_LIMIT_BYTES + 1)
        sender_stalled.set()
        await asyncio.Event().wait()

    started = time.monotonic()
    async with asyncio.timeout(10):
        response = await body_client.post(
            JSON_ROUTE_PREFIX + IPO, content=stalling_body(), headers=JSON_HEADERS
        )
    elapsed = time.monotonic() - started

    assert response.status_code == 413, response.text
    assert response.json() == {"detail": TOO_LARGE_DETAIL}
    assert sender_stalled.is_set()
    assert 0.9 <= elapsed < 5.0


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
async def test_client_disconnect_mid_body_gets_fastapi_400(prefix: str) -> None:
    """A client that disconnects while its body is read gets FastAPI's own 400 on either route
    class. The app is driven over raw ASGI, since an HTTP client cannot disconnect mid-body."""
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": b'{"name": ', "more_body": True},
        {"type": "http.disconnect"},
    ]
    sent: list[Mapping[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message: Mapping[str, Any]) -> None:
        sent.append(message)

    path = prefix + WATCHLISTS
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"terminal.test"), (b"content-type", b"application/json")],
        "server": ("terminal.test", 80),
        "client": ("127.0.0.1", 50000),
    }

    await build_app()(scope, receive, send)

    [start] = [message for message in sent if message["type"] == "http.response.start"]
    body = b"".join(
        message.get("body", b"") for message in sent if message["type"] == "http.response.body"
    )
    assert start["status"] == 400
    assert json.loads(body) == {"detail": "There was an error parsing the body"}


def test_body_verdict_dependency_is_appended_once() -> None:
    """``TerminalJsonRoute`` adds its body-verdict dependency after the route's own, and a route
    rebuilt from an existing route's dependencies keeps exactly one."""

    async def create(body: WatchlistCreate) -> WatchlistCreate:
        return body

    route = TerminalJsonRoute(
        WATCHLISTS, create, methods=["POST"], dependencies=[Depends(get_terminal_principal)]
    )
    rebuilt = TerminalJsonRoute(
        WATCHLISTS, create, methods=["POST"], dependencies=route.dependencies
    )

    assert len(route.dependencies) == 2
    assert route.dependencies[0].dependency is get_terminal_principal
    assert rebuilt.dependencies == route.dependencies
    assert len(rebuilt.dependant.dependencies) == len(route.dependant.dependencies)


@pytest.mark.parametrize("path", [IPO, WATCHLISTS])
def test_json_route_documents_the_operation_of_a_plain_route(path: str) -> None:
    """The bound and its verdict dependency add no parameter, body or response to OpenAPI."""
    paths = build_app().openapi()["paths"]
    plain = dict(paths[PLAIN_PREFIX + path]["post"])
    bounded = dict(paths[JSON_ROUTE_PREFIX + path]["post"])
    plain.pop("operationId")
    bounded.pop("operationId")

    assert bounded == plain


# ---------------------------------------------------------------------------------------------
# Controls: decodable bodies behave exactly as on a plain route
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(quickmart_raw(), id="quickmart"),
        pytest.param(b"\xef\xbb\xbf" + quickmart_raw(), id="quickmart_utf8_bom"),
    ],
)
async def test_valid_quickmart_body_is_valued(
    body_client: httpx.AsyncClient, prefix: str, body: bytes
) -> None:
    response = await post_raw(body_client, prefix + IPO, body)

    assert response.status_code == 200
    assert response.json()["blended_fair_value_per_share_kes"] == "6.82"


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
async def test_truncated_json_keeps_fastapi_json_invalid(
    body_client: httpx.AsyncClient, prefix: str
) -> None:
    response = await post_raw(body_client, prefix + IPO, b'{"offer_price_kes": ')

    assert response.status_code == 422
    assert response.json() == {
        "detail": [
            {
                "type": "json_invalid",
                "loc": ["body", 20],
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": "Expecting value"},
            }
        ]
    }


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("body", "name"),
    [
        pytest.param(b'{"name": " Banks ", "tickers": ["KCB"]}', "Banks", id="ascii"),
        pytest.param(
            '{"name": "Benki \u2713", "tickers": ["KCB"]}'.encode(), "Benki \u2713", id="utf8"
        ),
        pytest.param(
            b'{"name": "Top \\ud83d\\ude00", "tickers": ["KCB"]}',
            "Top \U0001f600",
            id="escaped_surrogate_pair",
        ),
    ],
)
async def test_valid_watchlist_body_is_accepted(
    body_client: httpx.AsyncClient, prefix: str, body: bytes, name: str
) -> None:
    response = await post_raw(body_client, prefix + WATCHLISTS, body)

    assert response.status_code == 201
    assert response.json() == {"name": name, "tickers": ["KCB"]}


# ---------------------------------------------------------------------------------------------
# Health check timeout: the time bound on the database round trip of ``GET /health``
#
# A hung database must answer the coded 503 ``database_unavailable`` once
# ``HEALTH_CHECK_TIMEOUT_SECONDS`` expires, rather than after asyncpg's 60 s connect timeout,
# and a cancellation from outside the route must still propagate instead of becoming that 503.
#
# Every case runs without PostgreSQL: one hangs a stub session supplied through a
# ``get_terminal_session`` override, another points the real session factory at a TCP listener
# that accepts connections and never replies, and a third overrides the session with one that
# has no default bind, configured only through ``binds`` on an engine aimed at that listener, so
# the check must reach the hung engine rather than fail at once for want of a bind. The bound is
# shortened for the tests through the ``health_service`` module constant, which the service
# reads at call time. The credentials are synthetic markers, so a leak shows as a marker in the
# response body or a log record.
# ---------------------------------------------------------------------------------------------

HEALTH_URL = "http://testserver/api/v1/terminal/health"
TEST_BOUND_SECONDS = 0.2
# Far below the 5 s production bound and asyncpg's 60 s default, with room for a loaded host.
PROMPT_SECONDS = 2.0

UNAVAILABLE_BODY = {
    "detail": {
        "code": "database_unavailable",
        "message": "The terminal database is unavailable.",
        "field": None,
    }
}
TIMEOUT_WARNING = "terminal health check failed: TimeoutError"

# HEALTH_-prefixed: the session-label section below has a USER_MARKER of its own.
HEALTH_USER_MARKER = "healthuserMARK"
PW_MARKER = "healthpwMARK"

# Health-prefixed: the host-mount section above has a ``BindsFor`` of its own, over connections.
HealthBindsFor = Callable[[AsyncEngine], dict[type[TerminalBase] | Table, AsyncEngine]]
"""Builds a binds-only session's ``binds`` mapping onto one engine."""


class HangingSession:
    """Stands in for an ``AsyncSession`` whose ``execute`` never returns until cancelled."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False
        self._never_set = asyncio.Event()

    async def execute(self, statement: object, bind_arguments: object = None) -> None:
        self.started.set()
        try:
            await self._never_set.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


@pytest.fixture
def short_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the health check's bound for one test."""
    monkeypatch.setattr(health_service, "HEALTH_CHECK_TIMEOUT_SECONDS", TEST_BOUND_SECONDS)


@pytest.fixture
def silent_listener_port() -> Iterator[int]:
    """Yield the port of a local TCP listener that never accepts, reads or replies.

    The kernel completes each handshake into the listen backlog, so asyncpg connects and then
    waits for a server reply that never comes, as it would against a hung database.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        port: int = listener.getsockname()[1]
        yield port


# Shared with the session-label and lifespan sections below.
@pytest.fixture
async def no_cached_engines() -> AsyncIterator[None]:
    """Start and end with an empty engine cache, so no engine outlives its test."""
    await dispose_terminal_engine()
    yield
    await dispose_terminal_engine()


def _health_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health.router, prefix="/api/v1/terminal")
    return app


async def _timed_health_request(app: FastAPI) -> tuple[httpx.Response, float]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
        started = time.monotonic()
        # An unbounded route fails the test here instead of hanging it.
        async with asyncio.timeout(PROMPT_SECONDS):
            response = await client.get(HEALTH_URL)
        return response, time.monotonic() - started


def _health_records(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == health_service.logger.name
    ]


@pytest.mark.usefixtures("short_bound")
async def test_hung_session_answers_coded_503_once_the_bound_expires(
    caplog: pytest.LogCaptureFixture,
) -> None:
    hanging = HangingSession()

    async def hanging_session_provider() -> AsyncIterator[HangingSession]:
        yield hanging

    app = _health_app()
    app.dependency_overrides[get_terminal_session] = hanging_session_provider
    caplog.set_level(logging.WARNING, logger=health_service.logger.name)

    response, elapsed = await _timed_health_request(app)

    assert response.status_code == 503
    assert response.json() == UNAVAILABLE_BODY
    assert TEST_BOUND_SECONDS <= elapsed < PROMPT_SECONDS
    # The hung round trip is cancelled, not left running behind the response.
    assert hanging.cancelled
    assert _health_records(caplog) == [(logging.WARNING, TIMEOUT_WARNING)]


@pytest.mark.usefixtures("short_bound", "no_cached_engines")
async def test_hung_database_answers_coded_503_once_the_bound_expires(
    silent_listener_port: int, caplog: pytest.LogCaptureFixture
) -> None:
    settings = TerminalSettings(
        database_url=(
            f"postgresql+asyncpg://{HEALTH_USER_MARKER}:{PW_MARKER}"
            f"@127.0.0.1:{silent_listener_port}/hung"
        ),
        _env_file=None,
    )
    app = _health_app()
    app.dependency_overrides[get_terminal_settings] = lambda: settings
    # Every logger, so a credential leaked by the terminal, SQLAlchemy or asyncpg is caught.
    caplog.set_level(logging.DEBUG)

    response, elapsed = await _timed_health_request(app)

    assert response.status_code == 503
    assert response.json() == UNAVAILABLE_BODY
    assert TEST_BOUND_SECONDS <= elapsed < PROMPT_SECONDS
    assert _health_records(caplog) == [(logging.WARNING, TIMEOUT_WARNING)]
    for marker in (HEALTH_USER_MARKER, PW_MARKER):
        assert marker not in response.text
        assert marker not in caplog.text


@pytest.mark.parametrize(
    "binds_for",
    [
        pytest.param(lambda engine: {TerminalBase: engine}, id="base_class_binds"),
        pytest.param(
            lambda engine: {mapper.class_: engine for mapper in TerminalBase.registry.mappers},
            id="per_model_binds",
        ),
        pytest.param(
            lambda engine: {table: engine for table in TerminalBase.metadata.tables.values()},
            id="per_table_binds",
        ),
    ],
)
@pytest.mark.usefixtures("short_bound")
async def test_binds_only_session_probes_its_terminal_bind(
    silent_listener_port: int, caplog: pytest.LogCaptureFixture, binds_for: HealthBindsFor
) -> None:
    """A host session with no default bind, configured only through ``binds``, is probed on the
    engine those binds give the terminal: against a hung database the check times out, rather
    than failing at once because no bind was found."""
    engine = create_async_engine(
        f"postgresql+asyncpg://{HEALTH_USER_MARKER}:{PW_MARKER}"
        f"@127.0.0.1:{silent_listener_port}/hung"
    )
    session = AsyncSession(binds=binds_for(engine))
    assert session.sync_session.bind is None

    async def binds_only_session_provider() -> AsyncIterator[AsyncSession]:
        yield session

    app = _health_app()
    app.dependency_overrides[get_terminal_session] = binds_only_session_provider
    caplog.set_level(logging.WARNING, logger=health_service.logger.name)

    try:
        response, elapsed = await _timed_health_request(app)
    finally:
        await session.close()
        await engine.dispose()

    assert response.status_code == 503
    assert response.json() == UNAVAILABLE_BODY
    assert TEST_BOUND_SECONDS <= elapsed < PROMPT_SECONDS
    assert _health_records(caplog) == [(logging.WARNING, TIMEOUT_WARNING)]


async def test_external_cancellation_propagates_instead_of_answering_503(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The production bound stays in force, so the cancellation arrives well before it expires.
    hanging = HangingSession()
    caplog.set_level(logging.WARNING, logger=health_service.logger.name)
    task = asyncio.create_task(health.get_health(hanging))  # type: ignore[arg-type]
    async with asyncio.timeout(PROMPT_SECONDS):
        await hanging.started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert hanging.cancelled
    assert _health_records(caplog) == []


# ---------------------------------------------------------------------------------------------
# Health service: ``health_service.get_health``, the ``SELECT 1`` probe and its failure contract
#
# A round trip that completes answers ``HealthResponse(status="ok", database="ok", version=...)``
# after executing exactly the constant ``SELECT 1``, resolved for the terminal's bind through
# ``TerminalIngestionRun``'s mapper and table so a host session configured only through ``binds``
# or a custom ``get_bind`` is probed too. Any failure the session raises answers the coded 503
# ``database_unavailable``, chained to the original exception, with one WARNING record on the
# service logger that names only the exception type: a refused or failed connect arrives
# as a raw ``OSError``, a driver error wrapped by SQLAlchemy as ``OperationalError``, asyncpg's
# own connect timeout as ``TimeoutError``, and an ``asyncpg.PostgresError`` (bad credentials, a
# missing database, too many connections) as a type the package cannot name, stood in for here
# by a plain ``Exception`` subclass. The expired bound and external cancellation are covered by
# the health check timeout section above.
#
# Every case runs on a stub session, without PostgreSQL. Each failure message embeds a synthetic
# marker standing for a credential, so a leak shows as that marker in the 503 detail or a log
# record.
# ---------------------------------------------------------------------------------------------

LEAK_MARKER = "healthleakMARK"

UNAVAILABLE_DETAIL = {
    "code": "database_unavailable",
    "message": "The terminal database is unavailable.",
    "field": None,
}


class InvalidPasswordError(Exception):
    """Stands in for ``asyncpg.exceptions.InvalidPasswordError``, an ``asyncpg.PostgresError``
    that SQLAlchemy does not wrap at connect time and the package never imports."""


class RecordingSession:
    """Stands in for an ``AsyncSession`` whose ``execute`` succeeds; records each statement.

    It has no ``commit`` or ``close``, so a probe that tried either would fail its test.
    """

    def __init__(self) -> None:
        self.statements: list[object] = []
        self.bind_arguments: list[object] = []

    async def execute(self, statement: object, bind_arguments: object = None) -> None:
        self.statements.append(statement)
        self.bind_arguments.append(bind_arguments)


class FailingSession:
    """Stands in for an ``AsyncSession`` whose ``execute`` raises ``error`` on every call."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def execute(self, statement: object, bind_arguments: object = None) -> None:
        self.calls += 1
        raise self.error


def _service_records(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == health_service.logger.name
    ]


async def test_completed_round_trip_reports_ok_after_executing_select_1(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = RecordingSession()
    caplog.set_level(logging.DEBUG)

    result = await health_service.get_health(cast(AsyncSession, session))

    assert result == HealthResponse(
        status="ok", database="ok", version=opennse_terminal.__version__
    )
    assert len(session.statements) == 1
    statement = session.statements[0]
    assert isinstance(statement, TextClause)
    assert statement.text == "SELECT 1"
    assert session.bind_arguments == [
        {"mapper": TerminalIngestionRun.__mapper__, "clause": TerminalIngestionRun.__table__}
    ]
    assert _service_records(caplog) == []


@pytest.mark.parametrize(
    ("error", "type_name"),
    [
        pytest.param(
            ConnectionRefusedError(
                111, f"Connect call failed ('127.0.0.1', 5432) for {LEAK_MARKER}"
            ),
            "ConnectionRefusedError",
            id="connection-refused",
        ),
        pytest.param(
            OSError(f"Multiple exceptions: connect to {LEAK_MARKER} failed"),
            "OSError",
            id="os-error",
        ),
        pytest.param(
            OperationalError(
                "SELECT 1", None, Exception(f"server closed the connection for {LEAK_MARKER}")
            ),
            "OperationalError",
            id="sqlalchemy-operational-error",
        ),
        pytest.param(
            TimeoutError(f"driver connect timeout to {LEAK_MARKER}"),
            "TimeoutError",
            id="driver-timeout",
        ),
        pytest.param(
            InvalidPasswordError(f'password authentication failed for user "{LEAK_MARKER}"'),
            "InvalidPasswordError",
            id="unwrapped-postgres-error",
        ),
    ],
)
async def test_failed_round_trip_answers_coded_503_and_logs_only_the_type_name(
    error: Exception, type_name: str, caplog: pytest.LogCaptureFixture
) -> None:
    session = FailingSession(error)
    # Every logger, so a message leaked by the service or by anything it calls is caught.
    caplog.set_level(logging.DEBUG)

    with pytest.raises(HTTPException) as excinfo:
        await health_service.get_health(cast(AsyncSession, session))

    # Starlette types ``detail`` as str; FastAPI passes the coded dict through unchanged.
    detail: object = excinfo.value.detail
    assert excinfo.value.status_code == 503
    assert detail == UNAVAILABLE_DETAIL
    assert excinfo.value.__cause__ is error
    # One attempt: the probe never retries.
    assert session.calls == 1
    assert _service_records(caplog) == [
        (logging.WARNING, f"terminal health check failed: {type_name}")
    ]
    assert LEAK_MARKER not in caplog.text
    assert LEAK_MARKER not in str(detail)


# ---------------------------------------------------------------------------------------------
# Settings: ``TerminalSettings`` (``opennse_terminal.config``) duration and PDF limit bounds, repr
#
# ``scraper_min_interval_seconds`` feeds ``asyncio.sleep`` and ``scraper_timeout_seconds`` feeds
# ``httpx.Timeout`` in the polite client, so both must load as finite numbers: the spacing is at
# least 2.0 s, the scrapers' politeness minimum between requests to one host, the timeout must be
# positive, and neither has an upper bound. ``pdf_max_bytes`` is at most 1 GiB, the parser
# process's memory limit, which also keeps the upload's file read inside the index range.
# ``pdf_max_concurrency`` caps the parses one mounted router admits at once, each holding a worker
# thread, and is at most 32, below the 40 tokens of anyio's default thread pool. That bounds one
# router's admitted parses; it does not guarantee free threads across several mounts, each
# counting its own admissions, or under a host's own thread limit. A value above either PDF bound
# is rejected at load, naming the field.
# ``repr()`` and ``str()`` must leave out ``database_url``, which carries the database password.
# Every case runs through keyword arguments and through the ``TERMINAL_*`` environment variables,
# with ``_env_file=None`` so the checkout's ``.env`` is never read. Offline and synchronous: no
# network, no database.
# ---------------------------------------------------------------------------------------------

INTERVAL = "scraper_min_interval_seconds"
TIMEOUT = "scraper_timeout_seconds"
DURATION_FIELDS = (INTERVAL, TIMEOUT)
SOURCES = ("keyword", "environment")
LARGEST_FINITE = repr(sys.float_info.max)


def _env_var(field: str) -> str:
    """The environment variable a settings field is read from."""
    return f"TERMINAL_{field.upper()}"


# Requested by each settings test rather than autouse, so the module's other tests keep the
# duration variables they inherit.
@pytest.fixture
def _no_duration_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any inherited duration variables, so each test sees only what it sets."""
    for field in DURATION_FIELDS:
        monkeypatch.delenv(_env_var(field), raising=False)


def _load(monkeypatch: pytest.MonkeyPatch, source: str, field: str, raw: str) -> TerminalSettings:
    """Build settings with ``field`` set to ``raw`` through the given source."""
    if source == "environment":
        monkeypatch.setenv(_env_var(field), raw)
        return TerminalSettings(_env_file=None)
    return TerminalSettings(_env_file=None, **{field: raw})


@pytest.mark.usefixtures("_no_duration_env")
def test_duration_defaults() -> None:
    settings = TerminalSettings(_env_file=None)

    assert settings.scraper_min_interval_seconds == 2.0
    assert settings.scraper_timeout_seconds == 30.0


@pytest.mark.usefixtures("_no_duration_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("raw", ["inf", "+inf", "-inf", "nan", "1e309"])
@pytest.mark.parametrize("field", DURATION_FIELDS)
def test_non_finite_duration_is_rejected(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, source: str
) -> None:
    # "1e309" overflows to inf when parsed, so it is rejected the same way.
    with pytest.raises(ValidationError) as exc_info:
        _load(monkeypatch, source, field, raw)

    errors = exc_info.value.errors()
    assert [(error["loc"], error["type"]) for error in errors] == [((field,), "finite_number")]
    assert field in str(exc_info.value)


@pytest.mark.usefixtures("_no_duration_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw", "expected"),
    [
        (INTERVAL, "2", 2.0),
        (INTERVAL, "2.5", 2.5),
        (INTERVAL, LARGEST_FINITE, sys.float_info.max),
        (TIMEOUT, "0.001", 0.001),
        (TIMEOUT, LARGEST_FINITE, sys.float_info.max),
    ],
)
def test_finite_duration_in_range_is_accepted(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, expected: float, source: str
) -> None:
    settings = _load(monkeypatch, source, field, raw)

    assert getattr(settings, field) == expected


@pytest.mark.usefixtures("_no_duration_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw", "error_type"),
    [
        (INTERVAL, "1.999", "greater_than_equal"),
        (INTERVAL, "0.5", "greater_than_equal"),
        (INTERVAL, "0", "greater_than_equal"),
        (INTERVAL, "-0.001", "greater_than_equal"),
        (INTERVAL, "-1", "greater_than_equal"),
        (TIMEOUT, "0", "greater_than"),
        (TIMEOUT, "-0.001", "greater_than"),
        (TIMEOUT, "-30", "greater_than"),
    ],
)
def test_duration_below_lower_bound_is_rejected(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, error_type: str, source: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _load(monkeypatch, source, field, raw)

    errors = exc_info.value.errors()
    assert [(error["loc"], error["type"]) for error in errors] == [((field,), error_type)]
    assert field in str(exc_info.value)


@pytest.mark.usefixtures("_no_duration_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("render", [repr, str], ids=["repr", "str"])
def test_rendered_settings_omit_database_url(
    monkeypatch: pytest.MonkeyPatch, render: Callable[[object], str], source: str
) -> None:
    # The password is generated per run, so no credential sits in this file.
    password = secrets.token_hex(16)
    url = URL.create(
        "postgresql+asyncpg",
        username="opennse",
        password=password,
        host="localhost",
        port=5432,
        database="opennse",
    ).render_as_string(hide_password=False)
    monkeypatch.delenv(_env_var("database_url"), raising=False)

    settings = _load(monkeypatch, source, "database_url", url)
    rendered = render(settings)

    assert password not in rendered
    assert "database_url" not in rendered
    assert "pdf_max_bytes=" in rendered
    assert settings.database_url == url


PDF_MAX_BYTES = "pdf_max_bytes"
PDF_MAX_CONCURRENCY = "pdf_max_concurrency"
PDF_LIMIT_FIELDS = (PDF_MAX_BYTES, PDF_MAX_CONCURRENCY)
INT64_MAX = str(2**63 - 1)


# Requested by each PDF limit test rather than autouse, so the module's other tests keep the PDF
# limit variables they inherit.
@pytest.fixture
def _no_pdf_limit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any inherited PDF limit variables, so each test sees only what it sets."""
    for field in PDF_LIMIT_FIELDS:
        monkeypatch.delenv(_env_var(field), raising=False)


@pytest.mark.usefixtures("_no_pdf_limit_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw", "expected"),
    [(PDF_MAX_BYTES, "1073741824", 1 << 30), (PDF_MAX_CONCURRENCY, "32", 32)],
)
def test_pdf_limit_at_its_upper_bound_is_accepted(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, expected: int, source: str
) -> None:
    settings = _load(monkeypatch, source, field, raw)

    assert getattr(settings, field) == expected


@pytest.mark.usefixtures("_no_pdf_limit_env")
@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw"),
    [
        (PDF_MAX_BYTES, "1073741825"),
        (PDF_MAX_BYTES, INT64_MAX),
        (PDF_MAX_BYTES, "99999999999999999999"),
        (PDF_MAX_CONCURRENCY, "33"),
        (PDF_MAX_CONCURRENCY, INT64_MAX),
        (PDF_MAX_CONCURRENCY, "99999999999999999999"),
    ],
)
def test_pdf_limit_above_its_upper_bound_is_rejected(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, source: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _load(monkeypatch, source, field, raw)

    errors = exc_info.value.errors()
    assert [(error["loc"], error["type"]) for error in errors] == [((field,), "less_than_equal")]
    assert field in str(exc_info.value)


# ---------------------------------------------------------------------------------------------
# Session log label: the credential-free database label of ``opennse_terminal.db.session``
#
# ``database_log_label`` is the only form in which the session module logs or quotes a database
# URL. It is a prose label, never a URL or a libpq ``key=value`` connection string, and must drop
# the driver, the username, the password before ``@`` and every query value, because asyncpg also
# reads the password from ``?password=...``, which ``render_as_string(hide_password=True)``
# prints.
#
# A failing engine dispose is reported by that label and the exception's type name alone: the
# driver's error text and its cause chain can quote the URL, so neither is logged or chained.
#
# Every case runs without PostgreSQL: engines are created but never connect. The secrets are
# synthetic markers, so a leak shows as the marker in a label, an error message, a traceback or a
# log record. The ``no_cached_engines`` fixture is the health check section's.
# ---------------------------------------------------------------------------------------------

USER_MARKER = "labeluserMARK"
USERINFO_PW_MARKER = "pwSECRETmark"
QUERY_PW_MARKER = "QSECRET"
QUERY_VALUE_MARKER = "qvalueMARK"
MARKERS = (USER_MARKER, USERINFO_PW_MARKER, QUERY_PW_MARKER, QUERY_VALUE_MARKER)

MARKED_URL = (
    f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example:5432/opennse"
    f"?password={QUERY_PW_MARKER}&application_name={QUERY_VALUE_MARKER}"
)
MARKED_URL_LABEL = 'database "opennse" on db.example:5432'
SECOND_MARKED_URL = (
    f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@db2.example:5433/opennse_two"
    f"?password={QUERY_PW_MARKER}&application_name={QUERY_VALUE_MARKER}"
)
SECOND_MARKED_URL_LABEL = 'database "opennse_two" on db2.example:5433'
# The password of the DSN that a failing dispose's error and its cause quote.
DISPOSE_SENTINEL = "disposeSENTINELmark"
PLACEHOLDER = "<unparseable database URL>"
INVALID_URL_MESSAGE = (
    "database URL is not valid; expected postgresql+asyncpg://user:password@host:port/database"
)


def _assert_no_secret(text: str) -> None:
    for secret in MARKERS:
        assert secret not in text


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(MARKED_URL, MARKED_URL_LABEL, id="userinfo-and-query-password"),
        pytest.param(make_url(MARKED_URL), MARKED_URL_LABEL, id="url-object"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@localhost/opennse",
            'database "opennse" on localhost',
            id="username-only-no-port",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@/opennse_c1?host=/run/pg",
            'database "opennse_c1"',
            id="unix-socket-host-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@/db?host=host-a:5432&host=host-b"
            f"&password={QUERY_PW_MARKER}",
            'database "db"',
            id="multihost-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@[::1]:5433/opennse",
            'database "opennse" on [::1]:5433',
            id="ipv6-host",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@localhost:5432",
            "default database on localhost:5432",
            id="no-database",
        ),
        pytest.param("postgresql+asyncpg://", "default database", id="no-host-no-database"),
        pytest.param(
            f'postgresql+asyncpg://{USER_MARKER}@db\nexample/op"en\nnse',
            'database "op\\"en\\nnse" on db\\nexample',
            id="quote-and-newline-escaped",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@db\u2028forged\x85x\u2029y:5432/opennse",
            'database "opennse" on db\\u2028forged\\u0085x\\u2029y:5432',
            id="line-separators-and-nel-escaped",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@db.example:5432/op\x9ben\x7fnse",
            'database "op\\u009ben\\u007fnse" on db.example:5432',
            id="c1-control-and-del-escaped",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@db.example:5432/donn\u00e9es",
            'database "donn\u00e9es" on db.example:5432',
            id="printable-non-ascii-kept",
        ),
        pytest.param(
            f"mysql+pymysql://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example:3306/app"
            f"?password={QUERY_PW_MARKER}",
            'database "app" on db.example:3306',
            id="other-driver",
        ),
    ],
)
def test_label_names_only_the_database_host_and_port(url: str | URL, expected: str) -> None:
    label = database_log_label(url)

    assert label == expected
    # A prose label: no driver, and neither a URL nor a libpq key=value connection string.
    assert "://" not in label
    assert "=" not in label
    assert "postgresql" not in label
    assert "mysql" not in label
    assert "\n" not in label
    assert len(label.splitlines()) == 1
    _assert_no_secret(label)


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("", id="empty"),
        pytest.param(f"not a url {USERINFO_PW_MARKER}", id="not-a-url"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@h:{QUERY_VALUE_MARKER}/db",
            id="non-numeric-port",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:pw@{USERINFO_PW_MARKER}@db.example/opennse",
            id="unescaped-at-in-password",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}/x:{USERINFO_PW_MARKER}@db.example/opennse",
            id="unescaped-slash-in-username",
        ),
    ],
)
def test_label_of_an_unreliable_url_is_the_fixed_placeholder(url: str) -> None:
    assert database_log_label(url) == PLACEHOLDER


@pytest.mark.parametrize(
    ("database_url", "expected"),
    [
        pytest.param(
            MARKED_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://"),
            "database URL must use the postgresql+asyncpg:// scheme; "
            'got driver "postgresql+psycopg" for database "opennse" on db.example:5432',
            id="other-driver",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@h:{QUERY_VALUE_MARKER}/db",
            INVALID_URL_MESSAGE,
            id="non-numeric-port",
        ),
        pytest.param(f"not a url {USERINFO_PW_MARKER}", INVALID_URL_MESSAGE, id="not-a-url"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example/opennse"
            f"?prepared_statement_cache_size={QUERY_VALUE_MARKER}",
            'database URL has an invalid query parameter value for database "opennse" on '
            "db.example",
            id="query-value-of-the-wrong-type",
        ),
    ],
)
def test_build_engine_errors_are_source_neutral_and_quote_only_the_label(
    database_url: str, expected: str
) -> None:
    # model_construct skips the settings validator, which rejects the other driver and the
    # non-URL text, as a host that builds the settings object itself could.
    settings = TerminalSettings.model_construct(database_url=database_url)

    with pytest.raises(ValueError) as raised:
        build_engine(settings)

    message = str(raised.value)
    assert message == expected
    # The caller adds where the URL came from; the session layer cannot know it.
    assert "TERMINAL_DATABASE_URL" not in message
    # No parser error, which can quote the URL, is chained into a traceback.
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ or raised.value.__context__ is None
    _assert_no_secret(message)


async def test_build_engine_debug_log_and_connect_url(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal.db.session")

    engine = build_engine(TerminalSettings(database_url=MARKED_URL, _env_file=None))
    try:
        # The URL the engine connects with keeps its query password.
        assert engine.url.query["password"] == QUERY_PW_MARKER
        assert engine.url.password == USERINFO_PW_MARKER
    finally:
        await engine.dispose()

    messages = [record.getMessage() for record in caplog.records]
    assert f"created engine for {MARKED_URL_LABEL}" in messages
    _assert_no_secret(caplog.text)


@pytest.mark.usefixtures("no_cached_engines")
async def test_session_factory_info_log(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="opennse_terminal.db.session")

    session_factory(TerminalSettings(database_url=MARKED_URL, _env_file=None))

    messages = [record.getMessage() for record in caplog.records]
    assert f"terminal database engine initialised for {MARKED_URL_LABEL}" in messages
    _assert_no_secret(caplog.text)


@pytest.mark.parametrize(
    ("database_urls", "failing_urls", "expected_message"),
    [
        pytest.param(
            [MARKED_URL],
            [MARKED_URL],
            f"failed to dispose 1 terminal database engine: {MARKED_URL_LABEL} (OSError)",
            id="one-engine",
        ),
        pytest.param(
            [MARKED_URL, SECOND_MARKED_URL],
            [MARKED_URL],
            f"failed to dispose 1 terminal database engine: {MARKED_URL_LABEL} (OSError)",
            id="first-of-two-fails",
        ),
        pytest.param(
            [MARKED_URL, SECOND_MARKED_URL],
            [MARKED_URL, SECOND_MARKED_URL],
            "failed to dispose 2 terminal database engines: "
            f"{MARKED_URL_LABEL} (OSError); {SECOND_MARKED_URL_LABEL} (OSError)",
            id="both-of-two-fail",
        ),
    ],
)
@pytest.mark.usefixtures("no_cached_engines")
async def test_failing_dispose_log(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    database_urls: list[str],
    failing_urls: list[str],
    expected_message: str,
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal.db.session")
    for url in database_urls:
        session_factory(TerminalSettings(database_url=url, _env_file=None))
    failing_engines = [db_session._CACHE[url][0] for url in failing_urls]
    real_dispose = AsyncEngine.dispose
    attempted: list[str] = []

    async def failing_dispose(self: AsyncEngine, close: bool = True) -> None:
        attempted.append(database_log_label(self.url))
        if not any(self is engine for engine in failing_engines):
            await real_dispose(self, close)
            return
        # A driver error quoting the DSN, chained to a cause that quotes it too.
        dsn = f"postgresql+asyncpg://u:{DISPOSE_SENTINEL}@db.example/opennse"
        try:
            raise ConnectionResetError(f"reset while closing {dsn}?password={DISPOSE_SENTINEL}")
        except ConnectionResetError as cause:
            raise OSError(f"connection to {dsn}?password={DISPOSE_SENTINEL} lost") from cause

    # AsyncEngine has __slots__, so the failure is patched on the class for this test only.
    monkeypatch.setattr(AsyncEngine, "dispose", failing_dispose)

    with pytest.raises(RuntimeError) as raised:
        await dispose_terminal_engine()

    error = raised.value
    assert str(error) == expected_message
    # The original exception is neither the cause nor the context, so no traceback shows it.
    assert error.__cause__ is None
    assert error.__context__ is None
    assert error.__suppress_context__
    # Every cached engine was tried, in cache order, and the cache is empty.
    assert attempted == [database_log_label(url) for url in database_urls]
    assert db_session._CACHE == {}
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [
        f"failed to dispose terminal database engine for {database_log_label(url)}: OSError"
        for url in failing_urls
    ]
    assert all(record.exc_info is None for record in caplog.records)
    for rendered in (str(error), "".join(traceback.format_exception(error)), caplog.text):
        assert DISPOSE_SENTINEL not in rendered
        _assert_no_secret(rendered)

    monkeypatch.undo()
    for engine in failing_engines:
        await engine.dispose()


# ---------------------------------------------------------------------------------------------
# Lifespan: the standalone app disposes its default engine; the router adds nothing to a host's
#
# ``create_app()`` declares the only lifespan: it logs the start with the package version and
# calls ``dispose_terminal_engine()`` when the block ends, normally or with an exception.
# ``terminal_router`` registers no lifespan, startup or shutdown hook, so a host that mounts it
# keeps its own lifespan as the only one and owns the lifecycle of any default engine.
# ``httpx.ASGITransport`` runs no lifespan, so the ``client`` and ``host_client`` fixtures never
# exercise one; each case enters ``router.lifespan_context`` itself. ``include_router`` wraps the
# including app's ``lifespan_context`` in a merged one, so the cases assert what a lifespan does,
# never which object it is.
#
# Every case runs without PostgreSQL: the default engine is cached through ``session_factory`` but
# never connects. ``AsyncEngine.dispose()`` replaces the engine's pool, so an unchanged pool shows
# the engine was not disposed. The ``no_cached_engines`` fixture is the health check section's.
# ---------------------------------------------------------------------------------------------

TERMINAL_LOGGER: Final = "opennse_terminal"

LIFESPAN_START_MESSAGE: Final = f"OpenNSE terminal API starting (version {__version__})"


class LifespanEndedError(Exception):
    """Raised inside a lifespan block to end it with an exception."""


def _terminal_records(caplog: pytest.LogCaptureFixture) -> list[tuple[str, int, str]]:
    """``(logger, level, message)`` of every captured record of an ``opennse_terminal`` logger."""
    return [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == TERMINAL_LOGGER or record.name.startswith(f"{TERMINAL_LOGGER}.")
    ]


@pytest.mark.parametrize("ending", ["normal", "exception"])
@pytest.mark.usefixtures("no_cached_engines")
async def test_standalone_lifespan_disposes_the_default_engine_on_every_exit(
    ending: str,
    caplog: pytest.LogCaptureFixture,
    make_settings: Callable[..., TerminalSettings],
) -> None:
    """Startup logs the version and leaves the engine cache as it found it; shutdown disposes the
    cached engine and empties the cache, also when the block raises, and that exception reaches
    the caller unchanged."""
    caplog.set_level(logging.INFO, logger="opennse_terminal.main")
    session_factory(make_settings())
    cached = dict(db_session._CACHE)
    ((engine, _factory),) = cached.values()
    pool = engine.sync_engine.pool
    app = create_app()
    error = LifespanEndedError("the served block failed")

    async def serve() -> None:
        async with app.router.lifespan_context(app):
            assert [
                (record.levelno, record.getMessage())
                for record in caplog.records
                if record.name == "opennse_terminal.main"
            ] == [(logging.INFO, LIFESPAN_START_MESSAGE)]
            # Startup neither creates nor disposes an engine.
            assert db_session._CACHE == cached
            assert engine.sync_engine.pool is pool
            if ending == "exception":
                raise error

    if ending == "exception":
        with pytest.raises(LifespanEndedError) as raised:
            await serve()
        assert raised.value is error
    else:
        await serve()

    assert db_session._CACHE == {}
    assert engine.sync_engine.pool is not pool


@pytest.mark.usefixtures("no_cached_engines")
async def test_mounted_router_adds_nothing_to_the_host_lifespan(
    caplog: pytest.LogCaptureFixture, make_settings: Callable[..., TerminalSettings]
) -> None:
    """A host's own lifespan runs once, start and stop, with the router mounted, which logs
    nothing and leaves the cached default engine for the host to dispose."""
    assert terminal_router.on_startup == []
    assert terminal_router.on_shutdown == []
    events: list[str] = []

    @asynccontextmanager
    async def host_lifespan(_app: FastAPI) -> AsyncIterator[None]:
        events.append("start")
        try:
            yield
        finally:
            events.append("stop")

    host = FastAPI(title="Host platform", lifespan=host_lifespan)
    host.include_router(terminal_router)
    session_factory(make_settings())
    cached = dict(db_session._CACHE)
    ((engine, _factory),) = cached.values()
    pool = engine.sync_engine.pool
    caplog.set_level(logging.DEBUG, logger=TERMINAL_LOGGER)
    caplog.clear()

    async with host.router.lifespan_context(host):
        assert events == ["start"]
        assert db_session._CACHE == cached

    assert events == ["start", "stop"]
    # Neither creating nor disposing an engine, nor logging anything, is the router's part.
    assert db_session._CACHE == cached
    assert engine.sync_engine.pool is pool
    assert _terminal_records(caplog) == []

    # The host releases the engine itself, through the re-exported shutdown hook.
    await dispose_terminal_engine()
    assert db_session._CACHE == {}
    assert engine.sync_engine.pool is not pool
