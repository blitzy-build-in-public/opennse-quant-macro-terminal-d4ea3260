"""Failure paths of the seeder (``opennse_terminal.seed.seed_terminal_db``) that need no database.

* ``run_seed`` returns exit code 1 for an engine that cannot be created, with one credential-safe
  line on stderr naming where the URL came from, no duplicate ERROR record and no traceback.
* A database that cannot be reached gets one readable ``could not connect to <label>: <reason>``
  line: the OS reason for an ``OSError``, the type name for anything else.
* The not-at-head message is printed once, and the ``seeding <label>`` line is never a URL.
* ``main`` returns its exit code and never raises ``SystemExit``: 1 for a usage error, 0 for
  ``--help`` (which documents the precondition, examples and exit codes), 130 for Ctrl-C.
* A failure once connected is logged by exception type only. SQLAlchemy errors embed the
  failed statement's parameters, such as ``default_owner_key``, so neither their text nor a
  traceback may reach stderr, a log record or the failed run's ``error_message``.
* ``seed`` generates its dataset inside the recorded operation, after the schema check and before
  the run row and any data write, so a generator or invariant failure leaves a failed run row.

Secrets are synthetic markers, so a leak shows as the marker in stderr or a log record. The
session is a recording double; writing rows and the run record for real needs PostgreSQL.
"""

import asyncio
import logging
import socket
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine
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

    ``commit_error`` is raised by every ``commit``. ``execute`` and ``scalar`` are the calls a
    data write or a query would make; they record and then fail, so an unexpected one is seen.
    """

    def __init__(self, commit_error: Exception | None = None) -> None:
        self.calls: list[str] = []
        self.added: list[object] = []
        self.commit_error = commit_error

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


async def _connected(engine: AsyncEngine) -> None:
    """Replaces ``_check_connection`` where the database is not reached: it always succeeds."""


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
    # Printed once; the line is not repeated as an ERROR record.
    assert stderr == expected_line.format(source="settings.database_url") + "\n"
    assert [record for record in caplog.records if record.levelno >= logging.ERROR] == []
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
) -> None:
    # No .env is read from tmp_path, and the environment's own URL is never used.
    monkeypatch.chdir(tmp_path)
    if source == "--database-url":
        monkeypatch.setenv("TERMINAL_DATABASE_URL", REFUSED_URL)
        argv = ["--database-url", database_url]
    else:
        monkeypatch.setenv("TERMINAL_DATABASE_URL", database_url)
        argv = []

    exit_code = seed_terminal_db.main(argv)

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == expected_line.format(source=source) + "\n"
    assert URL_PW_MARKER not in stderr


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
async def test_run_seed_failure_logs_the_exception_type_only(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
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

    async def failing_seed(session: object, *, settings: TerminalSettings) -> None:
        assert settings.default_owner_key == OWNER_MARKER
        raise error

    monkeypatch.setattr(seed_terminal_db, "build_engine", recording_build_engine)
    # db.example is never reached: the connection check passes, so the failure is a seed's.
    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "seed", failing_seed)
    # AsyncEngine has __slots__, so disposal is recorded on the class for this test only.
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")
    settings = TerminalSettings(
        database_url=f"{VALID_URL}?password={URL_PW_MARKER}",
        default_owner_key=OWNER_MARKER,
        _env_file=None,
    )

    exit_code = await run_seed(settings)

    assert exit_code == 1
    assert len(built) == 1
    assert disposed == built
    messages = [record.getMessage() for record in caplog.records]
    assert f"seeding {VALID_URL_LABEL}" in messages
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [f"seeding failed: {type(error).__name__}"]
    stderr = capsys.readouterr().err
    assert stderr == ""
    assert URL_PREFIX not in caplog.text
    _assert_type_only(caplog, stderr)


async def test_run_seed_prints_the_not_at_head_message_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    not_at_head = SchemaNotAtHeadError(None, "terminal_0001")

    async def unmigrated_seed(session: object, *, settings: TerminalSettings) -> None:
        raise not_at_head

    monkeypatch.setattr(seed_terminal_db, "_check_connection", _connected)
    monkeypatch.setattr(seed_terminal_db, "seed", unmigrated_seed)
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=VALID_URL, _env_file=None))

    assert exit_code == 2
    stderr = capsys.readouterr().err
    assert stderr == f"{not_at_head}\n"
    assert stderr.rstrip("\n").endswith(SCHEMA_HINT)
    assert [record for record in caplog.records if record.levelno >= logging.ERROR] == []
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
    assert stderr == (
        'cannot seed: could not connect to database "opennse" on 127.0.0.1:1: '
        "[Errno 111] Connect call failed ('127.0.0.1', 1)\n"
    )
    assert [record for record in caplog.records if record.levelno >= logging.ERROR] == []
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
    assert stderr == f"cannot seed: could not connect to {VALID_URL_LABEL}: {reason}\n"
    assert [record for record in caplog.records if record.levelno >= logging.ERROR] == []
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
