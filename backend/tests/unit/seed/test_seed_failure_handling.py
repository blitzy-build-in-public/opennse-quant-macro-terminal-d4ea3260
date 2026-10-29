"""Failure paths of the seeder (``opennse_terminal.seed.seed_terminal_db``) that need no database.

* ``run_seed`` returns exit code 1 for an engine that cannot be created, with a credential-safe
  line on stderr and no traceback.
* A failure once the engine exists is logged by exception type only. SQLAlchemy errors embed the
  failed statement's parameters, such as ``default_owner_key``, so neither their text nor a
  traceback may reach stderr, a log record or the failed run's ``error_message``.
* ``seed`` generates its dataset inside the recorded operation, after the schema check and before
  the run row and any data write, so a generator or invariant failure leaves a failed run row.

Secrets are synthetic markers, so a leak shows as the marker in stderr or a log record. The
session is a recording double; writing rows and the run record for real needs PostgreSQL.
"""

import logging
import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine
from opennse_terminal.models import TerminalIngestionRun
from opennse_terminal.seed import seed_terminal_db
from opennse_terminal.seed.seed_terminal_db import run_seed

URL_PW_MARKER = "urlpwMARK"
OWNER_MARKER = "OWNERMARK123"
INVALID_URL_LINE = (
    "TERMINAL_DATABASE_URL is not a valid database URL; "
    "expected postgresql+asyncpg://user:password@host:port/database"
)
ENGINE_FAILURE_LINE = (
    "cannot seed: could not create a database engine from TERMINAL_DATABASE_URL (ArgumentError)"
)
VALID_URL = f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:5432/opennse"
VALID_URL_LABEL = "postgresql+asyncpg://db.example:5432/opennse"


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


@pytest.mark.parametrize(
    ("database_url", "expected_stderr", "expected_log"),
    [
        pytest.param(
            # Passes the settings prefix check; SQLAlchemy's asyncpg dialect raises ArgumentError
            # (not a ValueError) because the second host has no port.
            f"postgresql+asyncpg://user:{URL_PW_MARKER}@/db?host=host-a:5432&host=host-b",
            ENGINE_FAILURE_LINE,
            ENGINE_FAILURE_LINE,
            id="multihost-missing-port",
        ),
        pytest.param(
            # build_engine's own ValueError keeps its message.
            f"postgresql+asyncpg://user:{URL_PW_MARKER}@db.example:notaport/db",
            INVALID_URL_LINE,
            f"cannot seed: {INVALID_URL_LINE}",
            id="non-numeric-port",
        ),
    ],
)
async def test_run_seed_engine_creation_failure_returns_1(
    database_url: str,
    expected_stderr: str,
    expected_log: str,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal")

    exit_code = await run_seed(TerminalSettings(database_url=database_url, _env_file=None))

    assert exit_code == 1
    stderr = capsys.readouterr().err
    assert stderr == f"{expected_stderr}\n"
    errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == [expected_log]
    _assert_type_only(caplog, stderr)


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
    _assert_type_only(caplog, capsys.readouterr().err)


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
