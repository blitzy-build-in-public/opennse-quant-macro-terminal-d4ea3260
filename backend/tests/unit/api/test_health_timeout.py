"""Tests for the time bound on the database round trip of ``GET /health``.

A hung database must answer the coded 503 ``database_unavailable`` once
``HEALTH_CHECK_TIMEOUT_SECONDS`` expires, rather than after asyncpg's 60 s connect timeout, and a
cancellation from outside the route must still propagate instead of becoming that 503.

Every case runs without PostgreSQL: one hangs a stub session supplied through a
``get_terminal_session`` override, another points the real session factory at a TCP listener
that accepts connections and never replies, and a third overrides the session with one that has
no default bind, configured only through ``binds`` on an engine aimed at that listener, so the
check must reach the hung engine rather than fail at once for want of a bind. The bound is
shortened for the tests through the ``health_service`` module constant, which the service reads
at call time. The credentials are synthetic markers, so a leak shows as a marker in the response
body or a log record.
"""

import asyncio
import logging
import socket
import time
from collections.abc import AsyncIterator, Callable, Iterator

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from opennse_terminal.api.deps import get_terminal_session, get_terminal_settings
from opennse_terminal.api.routes import health
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.db.session import dispose_terminal_engine
from opennse_terminal.services import health_service

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

USER_MARKER = "healthuserMARK"
PW_MARKER = "healthpwMARK"

BindsFor = Callable[[AsyncEngine], dict[type[TerminalBase] | Table, AsyncEngine]]
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
            f"postgresql+asyncpg://{USER_MARKER}:{PW_MARKER}@127.0.0.1:{silent_listener_port}/hung"
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
    for marker in (USER_MARKER, PW_MARKER):
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
    silent_listener_port: int, caplog: pytest.LogCaptureFixture, binds_for: BindsFor
) -> None:
    """A host session with no default bind, configured only through ``binds``, is probed on the
    engine those binds give the terminal: against a hung database the check times out, rather
    than failing at once because no bind was found."""
    engine = create_async_engine(
        f"postgresql+asyncpg://{USER_MARKER}:{PW_MARKER}@127.0.0.1:{silent_listener_port}/hung"
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
