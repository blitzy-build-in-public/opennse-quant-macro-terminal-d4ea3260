"""Failure paths of the seeder (``opennse_terminal.seed.seed_terminal_db``) that need no database.

* ``run_seed`` prints nothing: each of its diagnostics is one ERROR record on the
  ``opennse_terminal.seed`` logger, with no traceback, so a host's logging routes it.
* An engine that cannot be created gives exit code 1 and one credential-safe record naming where
  the URL came from.
* A database that cannot be reached gets one readable ``could not connect to <label>: <reason>``
  record: the OS reason for an ``OSError``, the type name for anything else.
* The not-at-head message is logged once, and the ``seeding <label>`` line is never a URL.
* ``main`` returns its exit code and never raises ``SystemExit``: 1 for a usage error, 0 for
  ``--help`` (which documents the precondition, examples and exit codes), 130 for Ctrl-C.
* A failure once connected is logged by exception type only. SQLAlchemy errors embed the
  failed statement's parameters, such as ``default_owner_key``, so neither their text nor a
  traceback may reach stderr, a log record or the failed run's ``error_message``.
* Each failure is reported exactly once. ``seed`` reports what it raises: a failed run by its
  run id, the only record of it, and a failure before the run starts (the schema check, invalid
  default settings) by type. ``run_seed`` adds a record only for a failure outside ``seed``,
  such as closing the session.
* ``seed`` generates its dataset inside the recorded operation, after the schema check and before
  the run row and any data write, so a generator or invariant failure leaves a failed run row.
* Sessions come from the shared ``db/session.py`` configuration: ``build_session_factory`` binds
  ``async_sessionmaker(expire_on_commit=False)`` to the caller's engine, and both
  ``session_factory`` and ``run_seed`` build their sessionmaker through it.

Secrets are synthetic markers, so a leak shows as the marker in stderr or a log record. The
session is a recording double; writing rows and the run record for real needs PostgreSQL.
"""

import asyncio
import logging
import socket
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal import migrations
from opennse_terminal.config import TerminalSettings
from opennse_terminal.db import session as db_session
from opennse_terminal.db.session import (
    build_engine,
    build_session_factory,
    dispose_terminal_engine,
    session_factory,
)
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.seed import seed_terminal_db
from opennse_terminal.seed.seed_terminal_db import SCHEMA_HINT, SchemaNotAtHeadError, run_seed

URL_PW_MARKER = "urlpwMARK"
OWNER_MARKER = "OWNERMARK123"
USER_MARKER = "userMARK77"
# Templates of the two engine-creation lines; {source} is where the URL came from.
INVALID_URL_LINE = (
    "cannot seed: database URL is not valid; "
    "expected postgresql+asyncpg://user:password@host:port/database (from {source})"
)
ENGINE_FAILURE_LINE = (
    "cannot seed: could not create a database engine from {source} (ArgumentError)"
)
# Passes the settings prefix check; SQLAlchemy's asyncpg dialect raises ArgumentError (not a
# ValueError) because the second host has no port.
MULTIHOST_MISSING_PORT_URL = (
    f"postgresql+asyncpg://user:{URL_PW_MARKER}@/db?host=host-a:5432&host=host-b"
)
# build_engine's own ValueError.
NON_NUMERIC_PORT_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:notaport/db"
VALID_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:5432/opennse"
VALID_URL_LABEL = 'database "opennse" on db.example:5432'
# Nothing listens on port 1, so connecting is refused at once, with no database or network.
REFUSED_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@127.0.0.1:1/opennse"
URL_PREFIX = "postgresql+asyncpg://"
DEFAULT_PROG = "python -m opennse_terminal.seed.seed_terminal_db"
SEED_LOGGER = "opennse_terminal.seed"


class InvalidPasswordError(Exception):
    """Stands in for asyncpg's server error of the same name, whose text quotes the user."""


def _watchlist_insert_error() -> sa.exc.IntegrityError:
    """The error a failed watchlist INSERT raises: its text lists the bound parameters."""
    return sa.exc.IntegrityError(
        "INSERT INTO terminal_watchlists (id, owner_key, name) VALUES ($1, $2, $3)",
        {"id": "00000000-0000-0000-0000-000000000001", "owner_key": OWNER_MARKER},
        Exception(f"duplicate key value (owner_key)=({OWNER_MARKER})"),
    )


class _SessionDouble:
    """Records the calls the seeder makes on its ``AsyncSession``.

    ``commit_error`` is raised by every ``commit``, and ``close_error`` when ``async with`` closes
    the session. ``execute`` and ``scalar`` are the calls a data write or a query would make; they
    record and then fail, so an unexpected one is seen.
    """

    def __init__(
        self, commit_error: Exception | None = None, close_error: Exception | None = None
    ) -> None:
        self.calls: list[str] = []
        self.added: list[object] = []
        self.commit_error = commit_error
        self.close_error = close_error

    async def __aenter__(self) -> "_SessionDouble":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.calls.append("close")
        if self.close_error is not None:
            raise self.close_error

    async def rollback(self) -> None:
        self.calls.append("rollback")

    def add(self, instance: object) -> None:
        self.calls.append("add")
        self.added.append(instance)

    async def flush(self) -> None:
        self.calls.append("flush")

    async def commit(self) -> None:
        self.calls.append("commit")
        if self.commit_error is not None:
            raise self.commit_error

    async def execute(self, *args: object, **kwargs: object) -> None:
        self.calls.append("execute")
        raise AssertionError("unexpected execute")

    async def scalar(self, *args: object, **kwargs: object) -> None:
        self.calls.append("scalar")
        raise AssertionError("unexpected scalar")


def _assert_type_only(caplog: pytest.LogCaptureFixture, *texts: str) -> None:
    """No record carries a traceback, and no text holds the owner or URL marker."""
    assert all(record.exc_info is None for record in caplog.records)
    for text in (caplog.text, *texts):
        assert "Traceback" not in text
        assert OWNER_MARKER not in text
        assert URL_PW_MARKER not in text


def _seed_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Messages of the ERROR records, each of which must come from the seeder's logger."""
    records = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert [record.name for record in records] == [SEED_LOGGER] * len(records)
    return [record.getMessage() for record in records]


async def _connected(engine: AsyncEngine) -> None:
    """Replaces ``_check_connection`` where the database is not reached: it always succeeds."""


def _record_engines(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[AsyncEngine], list[AsyncEngine]]:
    """Record every engine ``run_seed`` builds and every engine disposed, in that order."""
    built: list[AsyncEngine] = []
    disposed: list[AsyncEngine] = []
    original_dispose = AsyncEngine.dispose

    def recording_build_engine(settings: TerminalSettings) -> AsyncEngine:
        engine = build_engine(settings)
        built.append(engine)
        return engine

    async def recording_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)
        await original_dispose(self, close)

    monkeypatch.setattr(seed_terminal_db, "build_engine", recording_build_engine)
    # AsyncEngine has __slots__, so disposal is recorded on the class for this test only.
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)
    return built, disposed


def _use_session_double(
    monkeypatch: pytest.MonkeyPatch, session: _SessionDouble
) -> list[AsyncEngine]:
    """Make ``run_seed`` open ``session`` through ``build_session_factory``; return the engines
    the factory was built on."""
    bound: list[AsyncEngine] = []

    def session_double_factory(engine: AsyncEngine) -> Callable[[], _SessionDouble]:
        bound.append(engine)
        return lambda: session

    monkeypatch.setattr(seed_terminal_db, "build_session_factory", session_double_factory)
    return bound


ENGINE_CREATION_FAILURES = [
    pytest.param(MULTIHOST_MISSING_PORT_URL, ENGINE_FAILURE_LINE, id="multihost-missing-port"),
    pytest.param(NON_NUMERIC_PORT_URL, INVALID_URL_LINE, id="non-numeric-port"),
]


@pytest.mark.parametrize(("database_url", "expected_line"), ENGINE_CREATION_FAILURES)
async def test_run_seed_engine_creation_failure_returns_1(
    database_url: str,
    expected_line: str,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=database_url, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    # Logged once at ERROR and never printed, so the host's logging routes it.
    assert stderr == ""
    assert _seed_errors(caplog) == [expected_line.format(source="settings.database_url")]
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(("database_url", "expected_line"), ENGINE_CREATION_FAILURES)
@pytest.mark.parametrize("source", ["--database-url", "TERMINAL_DATABASE_URL"])
def test_main_names_the_url_source_in_engine_creation_failures(
    database_url: str,
    expected_line: str,
    source: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # No .env is read from tmp_path, and the environment's own URL is never used.
    monkeypatch.chdir(tmp_path)
    if source == "--database-url":
        monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)
        argv = ["--database-url", database_url]
    else:
        monkeypatch.setenv("TERMINAL_DATABASE_URL", database_url)
        argv = []
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = seed_terminal_db.main(argv)

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [expected_line.format(source=source)]
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(
    ("argv", "error"),
    [
        pytest.param(["--bogus"], "unrecognized arguments: --bogus", id="unknown-option"),
        pytest.param(["extra"], "unrecognized arguments: extra", id="extra-positional"),
        pytest.param(
            ["--database-url"],
            "argument --database-url: expected one argument",
            id="missing-value",
        ),
    ],
)
def test_main_returns_1_for_a_usage_error(
    argv: list[str],
    error: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)

    # Returned, never raised as SystemExit; 2 would read as "schema not at head".
    exit_code = seed_terminal_db.main(argv)

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith(f"usage: {DEFAULT_PROG} ")
    assert captured.err.endswith(f"\n{DEFAULT_PROG}: error: {error}\n")


@pytest.mark.parametrize(
    ("prog", "shown"),
    [
        pytest.param(None, DEFAULT_PROG, id="default-prog"),
        pytest.param("opennse-terminal seed", "opennse-terminal seed", id="cli-prog"),
    ],
)
def test_main_help_documents_the_precondition_examples_and_exit_codes(
    prog: str | None,
    shown: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)

    exit_code = seed_terminal_db.main(["--help"], prog=prog)

    assert exit_code == 0
    help_text = capsys.readouterr().out
    assert help_text.startswith(f"usage: {shown} [-h]")
    assert "TERMINAL_DATABASE_URL (or .env" in help_text
    assert "run `opennse-terminal migrate upgrade head` against the same" in help_text
    assert f"\n  opennse-terminal migrate upgrade head && {shown}\n" in help_text
    assert f"\n  {shown} --database-url 'postgresql+asyncpg://USER:PASSWORD@" in help_text
    for exit_line in (
        "\n  0    seeded, including a re-run that inserts nothing\n",
        "\n  1    failed and rolled back, or invalid arguments or settings\n",
        "\n  2    schema not at migration head: run `opennse-terminal migrate upgrade head` "
        "first\n",
        "\n  130  interrupted (Ctrl-C); any uncommitted seed transaction was rolled back\n",
    ):
        assert exit_line in help_text
    assert "opennse:opennse@" not in help_text


def test_main_reports_an_interrupt_in_one_line_and_returns_130(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sources: list[str] = []

    # A plain function, so no coroutine or event loop is created: the interrupt arrives where
    # asyncio.run would raise it.
    def interrupted_run_seed(settings: TerminalSettings, *, url_source: str) -> int:
        sources.append(url_source)
        raise KeyboardInterrupt

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)
    monkeypatch.setattr(seed_terminal_db, "run_seed", interrupted_run_seed)

    exit_code = seed_terminal_db.main(["--database-url", VALID_URL])

    assert exit_code == 130
    assert sources == ["--database-url"]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "seeding interrupted; any uncommitted seed transaction was rolled back, "
        "and re-running the seeder is safe\n"
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(_watchlist_insert_error(), id="integrity-error-with-parameters"),
        pytest.param(
            RuntimeError(f"seeded watchlist 'NSE Core 8' for owner {OWNER_MARKER!r} is missing"),
            id="runtime-error-naming-owner",
        ),
    ],
)
async def test_run_seed_reports_a_failed_run_once_by_its_run_id(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    dataset = object()
    write_run_ids: list[uuid.UUID] = []
    built, disposed = _record_engines(monkeypatch)
    bound = _use_session_double(monkeypatch, session)

    async def schema_at_head(executor: object) -> str:
        assert executor is session
        return "terminal_0001"

    def generate_all() -> object:
        return dataset

    async def failing_write(
        write_session: object, written: object, *, owner_key: str, run_id: uuid.UUID
    ) -> None:
        assert write_session is session
        assert written is dataset
        assert owner_key == OWNER_MARKER
        session.calls.append("write")
        write_run_ids.append(run_id)
        raise error

    # db.example is never reached: the connection check passes, and the real seed() runs on the
    # session double up to the failing data write.
    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", schema_at_head)
    monkeypatch.setattr(seed_terminal_db.synthetic, "generate_all", generate_all)
    monkeypatch.setattr(seed_terminal_db, "_write_dataset", failing_write)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")
    settings = TerminalSettings(
        database_url=f"{VALID_URL}?password={URL_PW_MARKER}",
        default_owner_key=OWNER_MARKER,
        _env_file=None,
    )

    exit_code = await run_seed(settings)

    assert exit_code == 1
    assert len(built) == 1
    assert bound == built
    assert disposed == built
    # The running row and the failed write, then the failure recorded in a fresh transaction;
    # returning the exit code still closes the session.
    assert session.calls == ["add", "flush", "write", "rollback", "add", "commit", "close"]
    running, failed = session.added
    assert isinstance(running, TerminalIngestionRun)
    assert isinstance(failed, TerminalIngestionRun)
    assert running.id == failed.id == write_run_ids[0]
    assert failed.status == "failed"
    assert failed.error_message == type(error).__name__
    messages = [record.getMessage() for record in caplog.records]
    assert f"seeding {VALID_URL_LABEL}" in messages
    # seed()'s record, naming the failed run row, is the only one: run_seed adds none.
    assert _seed_errors(caplog) == [
        f"seed run {failed.id} failed and was rolled back: {type(error).__name__}"
    ]
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_failed_schema_check_once_by_type(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    query_error = sa.exc.OperationalError(
        "SELECT version_num FROM terminal_alembic_version WHERE owner_key = $1",
        {"owner_key": OWNER_MARKER},
        Exception(f"server closed the connection while serving {OWNER_MARKER}"),
    )
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def failing_schema_check(executor: object) -> str:
        assert executor is session
        raise query_error

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", failing_schema_check)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    assert disposed == built
    assert len(built) == 1
    # No run had started, so nothing was added; the one record names the failure by type.
    assert session.calls == ["close"]
    assert _seed_errors(caplog) == ["seeding failed before the run started: OperationalError"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_failure_outside_seed_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    close_error = sa.exc.OperationalError(
        "ROLLBACK",
        {"owner_key": OWNER_MARKER},
        Exception(f"connection lost while closing the session of {OWNER_MARKER}"),
    )
    session = _SessionDouble(close_error=close_error)
    seeded: list[object] = []
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def succeeding_seed(seed_session: object, *, settings: TerminalSettings) -> None:
        seeded.append(seed_session)

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "seed", succeeding_seed)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    assert seeded == [session]
    assert session.calls == ["close"]
    assert disposed == built
    assert len(built) == 1
    assert _seed_errors(caplog) == ["seeding failed: OperationalError"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    _assert_type_only(caplog, stderr)


async def test_run_seed_logs_the_not_at_head_message_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = _SessionDouble()
    built, disposed = _record_engines(monkeypatch)
    _use_session_double(monkeypatch, session)

    async def no_stored_revision(executor: object) -> None:
        assert executor is session

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    # The real ensure_schema_at_head finds no stored revision, as on an unmigrated database.
    monkeypatch.setattr(seed_terminal_db, "current_schema_revision", no_stored_revision)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    not_at_head = str(SchemaNotAtHeadError(None, migrations.head_revision()))
    assert exit_code == 2
    assert disposed == built
    assert len(built) == 1
    # Nothing is written, not even a run row, and the session is closed.
    assert session.calls == ["close"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [not_at_head]
    assert not_at_head.endswith(SCHEMA_HINT)
    assert f"seeding {VALID_URL_LABEL}" in [record.getMessage() for record in caplog.records]
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_reports_a_refused_connection_readably(
    capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=REFUSED_URL, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [
        'cannot seed: could not connect to database "opennse" on 127.0.0.1:1: '
        "[Errno 111] Connect call failed ('127.0.0.1', 1)"
    ]
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        pytest.param(
            socket.gaierror(-2, "Name or service not known"),
            "[Errno -2] Name or service not known",
            id="unresolvable-host",
        ),
        pytest.param(
            OSError(
                "Multiple exceptions: [Errno 111] Connect call failed ('127.0.0.1', 5999), "
                "[Errno 99] Cannot assign requested address"
            ),
            "Multiple exceptions: [Errno 111] Connect call failed ('127.0.0.1', 5999), "
            "[Errno 99] Cannot assign requested address",
            id="several-addresses",
        ),
        pytest.param(TimeoutError(), "timed out", id="connect-timeout"),
        pytest.param(OSError(), "OSError", id="os-error-without-text"),
        pytest.param(
            InvalidPasswordError(f'password authentication failed for user "{USER_MARKER}"'),
            "InvalidPasswordError",
            id="server-error-type-only",
        ),
    ],
)
async def test_run_seed_connection_failure_reason(
    error: Exception,
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def failing_connection(engine: AsyncEngine) -> None:
        raise error

    async def unreachable_seed(session: object, *, settings: TerminalSettings) -> None:
        raise AssertionError("seed must not run after a failed connection")

    monkeypatch.setattr(seed_terminal_db, "_check_connection", failing_connection)
    monkeypatch.setattr(seed_terminal_db, "seed", unreachable_seed)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert _seed_errors(caplog) == [
        f"cannot seed: could not connect to {VALID_URL_LABEL}: {reason}"
    ]
    assert USER_MARKER not in stderr
    assert USER_MARKER not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_lets_a_cancelled_connection_attempt_propagate(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    built: list[AsyncEngine] = []
    disposed: list[AsyncEngine] = []
    original_dispose = AsyncEngine.dispose

    def recording_build_engine(settings: TerminalSettings) -> AsyncEngine:
        engine = build_engine(settings)
        built.append(engine)
        return engine

    async def recording_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)
        await original_dispose(self, close)

    async def cancelled_connection(engine: AsyncEngine) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(seed_terminal_db, "build_engine", recording_build_engine)
    monkeypatch.setattr(seed_terminal_db, "_check_connection", cancelled_connection)
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)

    # An interrupt is not a connection failure: it propagates, after the engine is disposed.
    with pytest.raises(asyncio.CancelledError):
        await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert len(built) == 1
    assert disposed == built
    assert capsys.readouterr().err == ""


async def test_seed_records_a_generation_failure_as_a_failed_run(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    session = _SessionDouble()
    invariant_error = RuntimeError(
        "planted spike SCOM 2026-09-15 has volume Z-score 3.10, below the required 4.00"
    )

    async def schema_at_head(executor: object) -> str:
        assert executor is session
        session.calls.append("schema_check")
        return "terminal_0001"

    def failing_generate_all() -> None:
        session.calls.append("generate_all")
        raise invariant_error

    monkeypatch.setattr(seed_terminal_db, "ensure_schema_at_head", schema_at_head)
    monkeypatch.setattr(seed_terminal_db.synthetic, "generate_all", failing_generate_all)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")
    settings = TerminalSettings(default_owner_key=OWNER_MARKER, _env_file=None)
    before = datetime.now(UTC)

    with pytest.raises(RuntimeError) as raised:
        await seed_terminal_db.seed(session, settings=settings)  # type: ignore[arg-type]

    assert raised.value is invariant_error
    # The schema check comes first; generation fails before the run row or any data write, and
    # the failure is then recorded in a fresh transaction.
    assert session.calls == ["schema_check", "generate_all", "rollback", "add", "commit"]
    (run,) = session.added
    assert isinstance(run, TerminalIngestionRun)
    assert run.pipeline == "seed"
    assert run.trigger == "seed"
    assert run.status == "failed"
    assert run.error_message == "RuntimeError"
    assert (run.rows_fetched, run.rows_written, run.rows_rejected) == (0, 0, 0)
    assert run.details == {}
    assert isinstance(run.id, uuid.UUID)
    assert run.finished_at is not None
    assert before <= run.started_at <= run.finished_at
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"seed run {run.id} failed and was rolled back: RuntimeError"]
    assert "Z-score" not in caplog.text
    _assert_type_only(caplog)


async def test_seed_reports_invalid_default_settings_once_by_type(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    session = _SessionDouble()
    # No .env is read from tmp_path; the environment's URL lacks the asyncpg driver.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(
        "TERMINAL_DATABASE_URL", f"postgresql://user:{URL_PW_MARKER}@db.example:5432/opennse"
    )
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    with pytest.raises(ValidationError):
        await seed_terminal_db.seed(session)  # type: ignore[arg-type]

    # The settings are read before the schema check, so the session is never used.
    assert session.calls == []
    assert _seed_errors(caplog) == ["seeding failed before the run started: ValidationError"]
    _assert_type_only(caplog)


async def test_record_failure_stores_the_type_and_logs_its_own_failure_by_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    run_id = uuid.uuid4()
    commit_error = sa.exc.OperationalError(
        "INSERT INTO terminal_ingestion_runs (error_message) VALUES ($1)",
        {"error_message": OWNER_MARKER},
        Exception(f"connection lost while writing {OWNER_MARKER}"),
    )
    session = _SessionDouble(commit_error=commit_error)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    await seed_terminal_db._record_failure(
        session,  # type: ignore[arg-type]
        run_id=run_id,
        started_at=datetime(2026, 9, 30, 12, 0, tzinfo=UTC),
        exc=_watchlist_insert_error(),
    )

    assert session.calls == ["rollback", "add", "commit"]
    (run,) = session.added
    assert isinstance(run, TerminalIngestionRun)
    assert run.error_message == "IntegrityError"
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"could not record seed run {run_id} as failed: OperationalError"]
    _assert_type_only(caplog)


async def test_build_session_factory_is_the_shared_session_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = TerminalSettings(database_url=VALID_URL, _env_file=None)
    engine = build_engine(settings)
    built: list[async_sessionmaker[AsyncSession]] = []
    original_build_session_factory = db_session.build_session_factory

    def recording_build_session_factory(bound: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        factory = original_build_session_factory(bound)
        built.append(factory)
        return factory

    await dispose_terminal_engine()
    try:
        factory = build_session_factory(engine)
        assert isinstance(factory, async_sessionmaker)
        assert factory.kw["bind"] is engine
        assert factory.kw["expire_on_commit"] is False
        # Opening and closing a session connects to nothing, so db.example is never reached.
        async with factory() as session:
            assert session.bind is engine

        # The API's cached sessionmaker is built by the same function, on its own engine.
        monkeypatch.setattr(db_session, "build_session_factory", recording_build_session_factory)
        cached = session_factory(settings)
        assert built == [cached]
        assert cached.kw["bind"] is db_session._CACHE[VALID_URL][0]
        assert cached.kw["expire_on_commit"] is False
        assert session_factory(settings) is cached
        assert len(built) == 1
    finally:
        await dispose_terminal_engine()
        await engine.dispose()
