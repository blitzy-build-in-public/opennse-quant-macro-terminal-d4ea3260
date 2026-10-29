"""Tests for ``health_service.get_health``: the ``SELECT 1`` probe and its failure contract.

A round trip that completes answers ``HealthResponse(status="ok", database="ok", version=...)``
after executing exactly the constant ``SELECT 1``, resolved for the terminal's bind through
``TerminalIngestionRun``'s mapper and table so a host session configured only through ``binds``
or a custom ``get_bind`` is probed too. Any failure the session raises answers the
coded 503 ``database_unavailable``, chained to the original exception, with one WARNING record on
the service logger that names only the exception type: a refused or failed connect arrives as a
raw ``OSError``, a driver error wrapped by SQLAlchemy as ``OperationalError``, asyncpg's own
connect timeout as ``TimeoutError``, and an ``asyncpg.PostgresError`` (bad credentials, a missing
database, too many connections) as a type the package cannot name, stood in for here by a plain
``Exception`` subclass. The expired bound and external cancellation are covered by
``tests/unit/api/test_health_timeout.py``.

Every case runs on a stub session, without PostgreSQL. Each failure message embeds a synthetic
marker standing for a credential, so a leak shows as that marker in the 503 detail or a log
record.
"""

import logging
from typing import cast

import pytest
from fastapi import HTTPException
from sqlalchemy import TextClause
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

import opennse_terminal
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.schemas.common import HealthResponse
from opennse_terminal.services import health_service

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
