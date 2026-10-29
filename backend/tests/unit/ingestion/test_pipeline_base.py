"""Tests for the shared ingestion framework (``opennse_terminal.ingestion.base``).

The pure contract is tested directly: run statuses and triggers, the exceptions that end a run
with a specific status and their classification, the record and result types, the shared row
thresholds and the header, decimal, dedupe and JSON helpers. The run lifecycle of
``RunRecorder`` and ``execute_pipeline`` runs against an in-memory engine double that records
every statement and whether each transaction committed or rolled back, so no database, network or
filesystem is touched. PostgreSQL behaviour itself (the advisory lock, JSONB, CHECK constraints)
belongs to the integration suite.

Expected values are literals taken from the module's docstrings and the run-status table, never
recomputed with the code under test. Numbers are ``Decimal`` throughout.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, cast, get_args
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import Insert, Update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

import opennse_terminal.models.ingestion_runs as ingestion_runs_model
from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion import base
from opennse_terminal.ingestion.base import (
    MAX_INVALID_FRACTION,
    REJECTED_SAMPLE_LIMIT,
    AmbiguousSession,
    FetchFailed,
    IngestionError,
    LockHeld,
    NoNewData,
    Pipeline,
    RejectedRow,
    RobotsDisallowed,
    RunContext,
    RunRecorder,
    RunResult,
    RunStatus,
    RunTrigger,
    SourceLayoutChanged,
    ValidationOutcome,
    classify_exception,
    dedupe_by_key,
    evaluate_thresholds,
    execute_pipeline,
    normalize_header,
    parse_decimal_text,
    to_jsonable,
)
from opennse_terminal.ingestion.calendar import CalendarNotVerified
from opennse_terminal.models.ingestion_runs import RUN_STATUSES, RUN_TRIGGERS

LOGGER = "opennse_terminal.ingestion.base"
RUN_ID = uuid.UUID("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7")
SOURCE_URL = "https://www.centralbank.go.ke/statistics/interest-rates/"
CSV_URL = "https://www.centralbank.go.ke/uploads/interest_rates/1715_Central Bank Rates.csv"
NSE_URL = "https://www.nse.co.ke/market-statistics/"
CALENDAR_MESSAGE = (
    "exchange calendar not verified for 2028; verified years: 2026, 2027 — add the year's "
    "gazetted closures to ingestion/data/nse_closures.csv"
)
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtext('terminal:' || :pipeline))"
IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '30min'"

RUN_RECORD = "run_record"
DATA = "data"
COMMITTED = "committed"
ROLLED_BACK = "rolled back"


@pytest.fixture
def settings() -> TerminalSettings:
    """Explicit settings: no ``.env`` file, and a database URL nothing ever connects to."""
    return TerminalSettings(
        _env_file=None, database_url="postgresql+asyncpg://localhost:1/never_connected"
    )


# --- Engine, connection and HTTP doubles ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Executed:
    """One statement a connection double executed, with its bound parameters."""

    statement: Any
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ResultDouble:
    """The two result attributes the framework reads: ``rowcount`` and ``scalar_one()``."""

    rowcount: int = 1
    scalar: object = None

    def scalar_one(self) -> object:
        return self.scalar


@asynccontextmanager
async def _transaction(conn: _ConnectionDouble) -> AsyncIterator[_ConnectionDouble]:
    """Commit on a clean exit and roll back on any exception, as SQLAlchemy's ``begin()`` does."""
    try:
        yield conn
    except BaseException:
        conn.outcome = ROLLED_BACK
        raise
    conn.outcome = COMMITTED


class _ConnectionDouble:
    """Stands in for ``AsyncConnection``: records statements and its transaction's outcome."""

    def __init__(self, engine: _EngineDouble, kind: str) -> None:
        self._engine = engine
        self.kind = kind
        self.executed: list[_Executed] = []
        self.outcome: str | None = None
        self.closed = False

    async def execute(
        self, statement: Any, parameters: Mapping[str, Any] | None = None
    ) -> _ResultDouble:
        params = dict(statement.compile().params)
        params.update(parameters or {})
        self.executed.append(_Executed(statement, params))
        if isinstance(statement, Update):
            if self._engine.fail_run_updates:
                raise RuntimeError("run row update failed")
            return _ResultDouble(rowcount=self._engine.update_rowcount)
        if str(statement) == LOCK_SQL:
            return _ResultDouble(scalar=self._engine.lock_available)
        return _ResultDouble()

    def begin(self) -> AbstractAsyncContextManager[_ConnectionDouble]:
        return _transaction(self)


class _EngineDouble:
    """Stands in for ``AsyncEngine``.

    ``begin()`` checks out a run-record connection inside its own transaction; ``connect()``
    checks out the run's dedicated data connection, whose transaction the caller begins.
    """

    def __init__(
        self,
        *,
        lock_available: bool = True,
        update_rowcount: int = 1,
        fail_run_updates: bool = False,
    ) -> None:
        self.lock_available = lock_available
        self.update_rowcount = update_rowcount
        self.fail_run_updates = fail_run_updates
        self.connections: list[_ConnectionDouble] = []

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[_ConnectionDouble]:
        conn = _ConnectionDouble(self, RUN_RECORD)
        self.connections.append(conn)
        async with _transaction(conn):
            yield conn

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[_ConnectionDouble]:
        conn = _ConnectionDouble(self, DATA)
        self.connections.append(conn)
        try:
            yield conn
        finally:
            conn.closed = True

    def _run_record_statements(self, kind: type[Any]) -> list[_Executed]:
        return [
            executed
            for conn in self.connections
            if conn.kind == RUN_RECORD
            for executed in conn.executed
            if isinstance(executed.statement, kind)
        ]

    @property
    def inserts(self) -> list[_Executed]:
        return self._run_record_statements(Insert)

    @property
    def updates(self) -> list[_Executed]:
        return self._run_record_statements(Update)

    @property
    def data_connections(self) -> list[_ConnectionDouble]:
        return [conn for conn in self.connections if conn.kind == DATA]


class _HttpDouble:
    """Stands in for an injected ``PoliteClient``, which the executor never opens nor closes."""

    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> _HttpDouble:
        self.entered = True
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


# --- Pipeline doubles -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Quote:
    index: int
    cell: str
    value: Decimal | None


class _QuotePipeline(Pipeline[list[str], _Quote]):
    """A minimal concrete pipeline: each fetched cell is one value, and a cell that is not a
    number is rejected as ``invalid_decimal``."""

    name = "test_quotes"

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        cells: Sequence[str] = (),
        source_url: str | None = None,
        discovered_url: str | None = None,
        fetch_error: BaseException | None = None,
        upsert_error: BaseException | None = None,
    ) -> None:
        super().__init__(settings)
        if source_url is not None:
            self.source_url = source_url
        self.cells = list(cells)
        self.discovered_url = discovered_url
        self.fetch_error = fetch_error
        self.upsert_error = upsert_error
        self.contexts: list[RunContext] = []
        self.upserts: list[tuple[AsyncConnection, list[_Quote], uuid.UUID]] = []

    async def fetch(self, ctx: RunContext) -> list[str]:
        self.contexts.append(ctx)
        if self.source_url is not None:
            ctx.details["source_url"] = self.source_url
        if self.discovered_url is not None:
            self.source_url = self.discovered_url
        if self.fetch_error is not None:
            raise self.fetch_error
        return list(self.cells)

    def parse(self, raw: list[str]) -> list[_Quote]:
        return [_Quote(index, cell, parse_decimal_text(cell)) for index, cell in enumerate(raw)]

    def validate(self, records: Sequence[_Quote]) -> ValidationOutcome[_Quote]:
        return ValidationOutcome(
            valid=[record for record in records if record.value is not None],
            rejected=[
                RejectedRow(index=record.index, reason="invalid_decimal", row={"cell": record.cell})
                for record in records
                if record.value is None
            ],
        )

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[_Quote], run_id: uuid.UUID
    ) -> int:
        if self.upsert_error is not None:
            raise self.upsert_error
        self.upserts.append((conn, list(records), run_id))
        return len(records)


class _MinRowsPipeline(_QuotePipeline):
    name = "test_min_rows"
    min_rows = 5


class _ReconcilingPipeline(_QuotePipeline):
    """Rejects the rows at ``unknown`` indexes in :meth:`reconcile`, or raises there."""

    name = "test_reconciling"

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        unknown: Sequence[int] = (),
        reconcile_error: BaseException | None = None,
        **options: Any,
    ) -> None:
        super().__init__(settings, **options)
        self.unknown = tuple(unknown)
        self.reconcile_error = reconcile_error
        self.reconciled: list[_Quote] = []

    async def reconcile(self, ctx: RunContext, records: list[_Quote]) -> ValidationOutcome[_Quote]:
        self.reconciled = list(records)
        if self.reconcile_error is not None:
            raise self.reconcile_error
        return ValidationOutcome(
            valid=[record for record in records if record.index not in self.unknown],
            rejected=[
                RejectedRow(index=record.index, reason="unknown_ticker", row={"cell": record.cell})
                for record in records
                if record.index in self.unknown
            ],
        )


class _CsvImportPipeline(_QuotePipeline):
    """Overrides ``run_name`` the way the tabular importer does (``csv_import:<kind>``)."""

    name = "csv_import"

    @property
    def run_name(self) -> str:
        return "csv_import:daily_prices"


class _BlockingPipeline(_QuotePipeline):
    """Its fetch blocks until the run's task is cancelled."""

    name = "test_blocking"

    def __init__(self, settings: TerminalSettings, **options: Any) -> None:
        super().__init__(settings, **options)
        self.fetch_started = asyncio.Event()

    async def fetch(self, ctx: RunContext) -> list[str]:
        self.contexts.append(ctx)
        if self.source_url is not None:
            ctx.details["source_url"] = self.source_url
        self.fetch_started.set()
        never_released = asyncio.Event()
        await never_released.wait()
        return list(self.cells)


async def _execute(
    pipeline: Pipeline[Any, Any],
    engine: _EngineDouble,
    http: _HttpDouble,
    *,
    trigger: RunTrigger = "cli",
) -> RunResult:
    return await execute_pipeline(
        pipeline, cast(AsyncEngine, engine), trigger=trigger, http=cast(Any, http)
    )


def _pop_duration(details: dict[str, Any]) -> Decimal:
    """Remove ``duration_seconds`` and check it: a JSON string of seconds to 3 decimal places."""
    raw = details.pop("duration_seconds")
    assert isinstance(raw, str)
    duration = Decimal(raw)
    assert duration.as_tuple().exponent == -3
    assert duration >= 0
    return duration


def _warnings_and_errors(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == LOGGER and record.levelno >= logging.WARNING
    ]


# --- Statuses, triggers, constants and the public surface -----------------------------------


def test_run_status_values_follow_the_run_status_table() -> None:
    assert [status.value for status in RunStatus] == [
        "running",
        "succeeded",
        "partial",
        "failed",
        "layout_changed",
        "blocked_by_robots",
        "no_new_data",
        "ambiguous_session",
    ]
    assert tuple(RunStatus) == RUN_STATUSES
    # A StrEnum compares and formats as its value, as the CHECK constraint and the CLI expect.
    assert RunStatus.PARTIAL == "partial"
    assert f"{RunStatus.LAYOUT_CHANGED}" == "layout_changed"


def test_run_trigger_matches_the_model_triggers() -> None:
    assert get_args(RunTrigger) == ("schedule", "cli", "api", "seed")
    assert get_args(RunTrigger) == RUN_TRIGGERS


def test_thresholds_are_exact_decimal_constants() -> None:
    assert isinstance(MAX_INVALID_FRACTION, Decimal)
    assert str(MAX_INVALID_FRACTION) == "0.10"
    assert REJECTED_SAMPLE_LIMIT == 50


def test_public_surface_is_exported() -> None:
    assert set(base.__all__) == {
        "MAX_INVALID_FRACTION",
        "REJECTED_SAMPLE_LIMIT",
        "AmbiguousSession",
        "FetchFailed",
        "IngestionError",
        "LockHeld",
        "NoNewData",
        "Pipeline",
        "RejectedRow",
        "RobotsDisallowed",
        "RunContext",
        "RunRecorder",
        "RunResult",
        "RunStatus",
        "RunTrigger",
        "SourceLayoutChanged",
        "ValidationOutcome",
        "classify_exception",
        "dedupe_by_key",
        "evaluate_thresholds",
        "execute_pipeline",
        "normalize_header",
        "parse_decimal_text",
        "to_jsonable",
    }
    assert all(hasattr(base, name) for name in base.__all__)


@pytest.mark.parametrize(
    ("attribute", "drifted", "message"),
    [
        pytest.param(
            "RUN_STATUSES",
            (*RUN_STATUSES, "archived"),
            "does not match models.ingestion_runs.RUN_STATUSES",
            id="statuses",
        ),
        pytest.param(
            "RUN_TRIGGERS",
            ("schedule", "cli"),
            "does not match models.ingestion_runs.RUN_TRIGGERS",
            id="triggers",
        ),
    ],
)
def test_import_fails_when_the_model_tuples_drift_from_the_types(
    monkeypatch: pytest.MonkeyPatch, attribute: str, drifted: tuple[str, ...], message: str
) -> None:
    """The guard runs at import, so the module is executed afresh under a probe name; the
    imported ``opennse_terminal.ingestion.base`` is left untouched."""
    monkeypatch.setattr(ingestion_runs_model, attribute, drifted)
    spec = importlib.util.spec_from_file_location("ingestion_base_drift_probe", base.__file__)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)
    with pytest.raises(RuntimeError, match=message):
        spec.loader.exec_module(probe)


# --- Exceptions -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "message", "attributes"),
    [
        pytest.param(
            SourceLayoutChanged(
                expected=("year", "month"), found=("yr",), message="header row changed"
            ),
            "header row changed",
            {"expected": ("year", "month"), "found": ("yr",), "message": "header row changed"},
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL),
            f"robots.txt blocks {NSE_URL} (disallowed)",
            {"url": NSE_URL, "reason": "disallowed"},
            id="robots-disallowed-default-reason",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL, reason="robots_unreachable"),
            f"robots.txt blocks {NSE_URL} (robots_unreachable)",
            {"url": NSE_URL, "reason": "robots_unreachable"},
            id="robots-unreachable",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_404"),
            f"fetch failed for {CSV_URL}: http_404",
            {"url": CSV_URL, "reason": "http_404"},
            id="fetch-failed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "label_mismatch", "label_session": "2026-10-27"}),
            "ambiguous session: label_mismatch",
            {"evidence": {"reason": "label_mismatch", "label_session": "2026-10-27"}},
            id="ambiguous-session-with-reason",
        ),
        pytest.param(
            AmbiguousSession({"link_session": "2026-10-26"}),
            "ambiguous session",
            {"evidence": {"link_session": "2026-10-26"}},
            id="ambiguous-session-without-reason",
        ),
        pytest.param(
            NoNewData("source has not advanced past 2026-10-27"),
            "no new data: source has not advanced past 2026-10-27",
            {"reason": "source has not advanced past 2026-10-27", "evidence": {}},
            id="no-new-data-without-evidence",
        ),
        pytest.param(
            NoNewData("bulletin already loaded", {"bulletin": "2026-02-06"}),
            "no new data: bulletin already loaded",
            {"reason": "bulletin already loaded", "evidence": {"bulletin": "2026-02-06"}},
            id="no-new-data-with-evidence",
        ),
        pytest.param(
            LockHeld("cbk_fx"),
            "lock held: another run of cbk_fx holds its advisory lock",
            {"pipeline": "cbk_fx"},
            id="lock-held",
        ),
    ],
)
def test_ingestion_exceptions_carry_their_attributes_and_message(
    exc: IngestionError, message: str, attributes: dict[str, object]
) -> None:
    assert isinstance(exc, IngestionError)
    assert str(exc) == message
    assert exc.args == (message,)
    assert {name: getattr(exc, name) for name in attributes} == attributes


def test_no_new_data_keeps_its_own_copy_of_the_evidence() -> None:
    evidence: dict[str, Any] = {"latest_session": "2026-10-27"}
    exc = NoNewData("source has not advanced", evidence)
    evidence["latest_session"] = "2026-10-28"
    assert exc.evidence == {"latest_session": "2026-10-27"}


# --- classify_exception ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "status", "message"),
    [
        pytest.param(
            SourceLayoutChanged(expected=["year"], found=["yr"], message="expected header 'year'"),
            RunStatus.LAYOUT_CHANGED,
            "expected header 'year'",
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(NSE_URL),
            RunStatus.BLOCKED_BY_ROBOTS,
            f"robots.txt blocks {NSE_URL} (disallowed)",
            id="robots-disallowed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "stale_label"}),
            RunStatus.AMBIGUOUS_SESSION,
            "ambiguous session: stale_label",
            id="ambiguous-session",
        ),
        pytest.param(
            NoNewData("source has not advanced"),
            RunStatus.NO_NEW_DATA,
            "source has not advanced",
            id="no-new-data",
        ),
        pytest.param(
            LockHeld("nse_market_statistics"), RunStatus.FAILED, "lock held", id="lock-held"
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_503"),
            RunStatus.FAILED,
            f"fetch failed for {CSV_URL}: http_503",
            id="fetch-failed",
        ),
        pytest.param(
            CalendarNotVerified(2028, frozenset({2026, 2027})),
            RunStatus.FAILED,
            CALENDAR_MESSAGE,
            id="calendar-not-verified",
        ),
        pytest.param(asyncio.CancelledError(), RunStatus.FAILED, "cancelled", id="cancelled"),
        pytest.param(
            ValueError("could not convert '12,3x'"),
            RunStatus.FAILED,
            "ValueError: could not convert '12,3x'",
            id="unexpected-value-error",
        ),
        pytest.param(KeyError("ticker"), RunStatus.FAILED, "KeyError: 'ticker'", id="key-error"),
        pytest.param(
            IngestionError("unmapped subclass"),
            RunStatus.FAILED,
            "IngestionError: unmapped subclass",
            id="bare-ingestion-error",
        ),
    ],
)
def test_classify_exception_maps_each_exception_to_its_status_and_message(
    exc: BaseException, status: RunStatus, message: str
) -> None:
    assert classify_exception(exc) == (status, message)


_LONG = "x" * 2_500


@pytest.mark.parametrize(
    ("exc", "untruncated"),
    [
        pytest.param(
            SourceLayoutChanged(expected=None, found=None, message=_LONG),
            _LONG,
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(_LONG), f"robots.txt blocks {_LONG} (disallowed)", id="robots"
        ),
        pytest.param(
            AmbiguousSession({"reason": _LONG}), f"ambiguous session: {_LONG}", id="ambiguous"
        ),
        pytest.param(NoNewData(_LONG), _LONG, id="no-new-data"),
        pytest.param(
            FetchFailed(CSV_URL, _LONG), f"fetch failed for {CSV_URL}: {_LONG}", id="fetch-failed"
        ),
        pytest.param(RuntimeError(_LONG), f"RuntimeError: {_LONG}", id="unexpected"),
    ],
)
def test_classify_exception_truncates_messages_to_2000_characters(
    exc: BaseException, untruncated: str
) -> None:
    _, message = classify_exception(exc)
    assert len(message) == 2_000
    assert message == untruncated[:1_999] + "…"


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        pytest.param("r" * 1_999, "r" * 1_999, id="1999-kept"),
        pytest.param("r" * 2_000, "r" * 2_000, id="2000-kept"),
        pytest.param("r" * 2_001, "r" * 1_999 + "…", id="2001-truncated"),
    ],
)
def test_classify_exception_truncation_boundary(reason: str, expected: str) -> None:
    assert classify_exception(NoNewData(reason)) == (RunStatus.NO_NEW_DATA, expected)


# --- Records, outcome, context and result ---------------------------------------------------


def test_rejected_row_is_an_immutable_value() -> None:
    row = RejectedRow(index=7, reason="invalid_isin;volume_out_of_range", row={"isin": "KE0"})
    assert (row.index, row.reason, row.row) == (
        7,
        "invalid_isin;volume_out_of_range",
        {"isin": "KE0"},
    )
    assert row == RejectedRow(
        index=7, reason="invalid_isin;volume_out_of_range", row={"isin": "KE0"}
    )
    assert not hasattr(row, "__dict__")
    with pytest.raises(FrozenInstanceError):
        row.reason = "changed"  # type: ignore[misc]


def test_validation_outcome_holds_the_valid_and_rejected_rows() -> None:
    rejected = RejectedRow(index=1, reason="invalid_decimal", row={"cell": "abc"})
    outcome = ValidationOutcome(valid=[Decimal("6.80")], rejected=[rejected])
    assert outcome.valid == [Decimal("6.80")]
    assert outcome.rejected == [rejected]
    assert outcome == ValidationOutcome(valid=[Decimal("6.80")], rejected=[rejected])


def test_run_result_is_an_immutable_record_of_the_run() -> None:
    result = RunResult(
        run_id=RUN_ID,
        pipeline="cbk_rates_csv",
        status=RunStatus.PARTIAL,
        rows_fetched=10,
        rows_written=9,
        rows_rejected=1,
        error_message=None,
        details={"rejected": []},
    )
    assert (result.run_id, result.pipeline, result.status) == (RUN_ID, "cbk_rates_csv", "partial")
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (10, 9, 1)
    assert result.error_message is None
    assert result.details == {"rejected": []}
    with pytest.raises(FrozenInstanceError):
        result.status = RunStatus.FAILED  # type: ignore[misc]


def test_run_context_gives_each_run_its_own_details(settings: TerminalSettings) -> None:
    conn = cast(AsyncConnection, _ConnectionDouble(_EngineDouble(), DATA))
    http = cast(Any, _HttpDouble())
    first = RunContext(
        run_id=RUN_ID,
        pipeline="cbk_fx",
        trigger="schedule",
        settings=settings,
        conn=conn,
        http=http,
    )
    second = RunContext(
        run_id=RUN_ID,
        pipeline="cbk_fx",
        trigger="schedule",
        settings=settings,
        conn=conn,
        http=http,
    )
    first.details["source_url"] = SOURCE_URL
    assert second.details == {}
    assert (first.run_id, first.pipeline, first.trigger) == (RUN_ID, "cbk_fx", "schedule")
    assert first.settings is settings
    assert first.conn is conn
    assert first.http is http


# --- Pipeline contract ----------------------------------------------------------------------


def test_pipeline_is_abstract(settings: TerminalSettings) -> None:
    with pytest.raises(TypeError, match="abstract"):
        Pipeline(settings)  # type: ignore[abstract]


def test_pipeline_subclass_must_implement_upsert(settings: TerminalSettings) -> None:
    class _WithoutUpsert(Pipeline[str, str]):
        name = "test_without_upsert"

        async def fetch(self, ctx: RunContext) -> str:
            return ""

        def parse(self, raw: str) -> list[str]:
            return [raw]

        def validate(self, records: Sequence[str]) -> ValidationOutcome[str]:
            return ValidationOutcome(valid=list(records), rejected=[])

    with pytest.raises(TypeError, match="upsert"):
        _WithoutUpsert(settings)  # type: ignore[abstract]


def test_minimal_pipeline_defaults(settings: TerminalSettings) -> None:
    pipeline = _QuotePipeline(settings)
    assert pipeline.run_name == "test_quotes"
    assert pipeline.run_name == pipeline.name
    assert pipeline.min_rows == 0
    assert pipeline.source_url is None
    assert pipeline.settings is settings


def test_run_name_can_differ_from_the_registry_name(settings: TerminalSettings) -> None:
    pipeline = _CsvImportPipeline(settings)
    assert pipeline.name == "csv_import"
    assert pipeline.run_name == "csv_import:daily_prices"


async def test_default_reconcile_accepts_every_record_without_touching_the_connection(
    settings: TerminalSettings,
) -> None:
    conn = _ConnectionDouble(_EngineDouble(), DATA)
    ctx = RunContext(
        run_id=RUN_ID,
        pipeline="test_quotes",
        trigger="cli",
        settings=settings,
        conn=cast(AsyncConnection, conn),
        http=cast(Any, _HttpDouble()),
    )
    records = [_Quote(0, "6.80", Decimal("6.80")), _Quote(1, "15.72", Decimal("15.72"))]
    outcome = await _QuotePipeline(settings).reconcile(ctx, records)
    assert outcome.valid == records
    assert outcome.rejected == []
    assert conn.executed == []


# --- evaluate_thresholds --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("parsed", "rejected", "min_rows", "expected"),
    [
        pytest.param(0, 0, 0, RunStatus.SUCCEEDED, id="empty-parse-without-minimum"),
        pytest.param(0, 0, 1, RunStatus.LAYOUT_CHANGED, id="empty-parse-below-minimum"),
        pytest.param(69, 0, 70, RunStatus.LAYOUT_CHANGED, id="one-row-below-minimum"),
        pytest.param(70, 0, 70, RunStatus.SUCCEEDED, id="exactly-the-minimum"),
        pytest.param(69, 1, 70, RunStatus.LAYOUT_CHANGED, id="minimum-checked-before-fraction"),
        pytest.param(10, 0, 0, RunStatus.SUCCEEDED, id="all-valid"),
        pytest.param(10, 1, 0, RunStatus.PARTIAL, id="exactly-10-percent-rejected"),
        pytest.param(10, 2, 0, RunStatus.LAYOUT_CHANGED, id="20-percent-rejected"),
        pytest.param(9, 1, 0, RunStatus.LAYOUT_CHANGED, id="11.1-percent-rejected"),
        pytest.param(100, 10, 0, RunStatus.PARTIAL, id="10-of-100-rejected"),
        pytest.param(100, 11, 0, RunStatus.LAYOUT_CHANGED, id="11-of-100-rejected"),
        pytest.param(1, 1, 0, RunStatus.LAYOUT_CHANGED, id="only-row-rejected"),
        pytest.param(
            1_000_000_000_000,
            100_000_000_000,
            0,
            RunStatus.PARTIAL,
            id="exactly-10-percent-of-10e12-rows",
        ),
        pytest.param(
            1_000_000_000_000,
            100_000_000_001,
            0,
            RunStatus.LAYOUT_CHANGED,
            id="one-over-10-percent-of-10e12-rows",
        ),
    ],
)
def test_evaluate_thresholds(
    parsed: int, rejected: int, min_rows: int, expected: RunStatus
) -> None:
    assert evaluate_thresholds(parsed, rejected, min_rows) is expected


@pytest.mark.parametrize(
    ("parsed", "rejected", "min_rows", "message"),
    [
        pytest.param(-1, 0, 0, "row counts and min_rows must not be negative", id="parsed"),
        pytest.param(0, -1, 0, "row counts and min_rows must not be negative", id="rejected"),
        pytest.param(0, 0, -1, "row counts and min_rows must not be negative", id="min-rows"),
        pytest.param(
            3, 4, 0, r"rejected rows \(4\) exceed parsed rows \(3\)", id="more-rejected-than-parsed"
        ),
    ],
)
def test_evaluate_thresholds_rejects_impossible_counts(
    parsed: int, rejected: int, min_rows: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        evaluate_thresholds(parsed, rejected, min_rows)


# --- normalize_header -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("Last Traded Price", "last traded price", id="lowercased"),
        pytest.param("  Last   Traded\u00a0Price  ", "last traded price", id="trimmed-nbsp-runs"),
        pytest.param("Change\n(%)", "change (%)", id="line-break"),
        pytest.param("Face Value\r\n(Kshs Millions)", "face value (kshs millions)", id="crlf"),
        pytest.param("\tCoupon Rate\t", "coupon rate", id="tabs"),
        pytest.param("\ufeffYEAR", "year", id="byte-order-mark"),
        pytest.param("ISIN\u200b Code", "isin code", id="zero-width-space-before-space"),
        pytest.param("Re\u200bdemption Yield", "redemption yield", id="zero-width-space-removed"),
        pytest.param("91-Day Tbill", "91-day tbill", id="punctuation-kept"),
        pytest.param("", "", id="empty"),
        pytest.param(" \u00a0\n", "", id="whitespace-only"),
    ],
)
def test_normalize_header(text: str, expected: str) -> None:
    assert normalize_header(text) == expected


# --- parse_decimal_text ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "parentheses_negative", "expected"),
    [
        pytest.param("6.80", True, "6.80", id="scale-kept"),
        pytest.param("0.10", True, "0.10", id="trailing-zero-kept"),
        pytest.param("1,234.56", True, "1234.56", id="thousands-separator"),
        pytest.param("12,345,678", True, "12345678", id="several-separators"),
        pytest.param("(1,234.5)", True, "-1234.5", id="parentheses-negative"),
        pytest.param("( 1,234.5 )", True, "-1234.5", id="parentheses-with-spaces"),
        pytest.param("(5)", True, "-5", id="shortest-parentheses"),
        pytest.param("-12.5", False, "-12.5", id="minus"),
        pytest.param("+12.5", True, "12.5", id="plus"),
        pytest.param("\u221212.5", True, "-12.5", id="unicode-minus"),
        pytest.param("1\u00a0234.5", True, "1234.5", id="nbsp-group-separator"),
        pytest.param("1\u202f234", True, "1234", id="narrow-nbsp-group-separator"),
        pytest.param("1\u2009234", True, "1234", id="thin-space-group-separator"),
        pytest.param("  15.72\n", True, "15.72", id="surrounding-whitespace"),
        pytest.param(".5", True, "0.5", id="leading-point"),
        pytest.param("5.", True, "5", id="trailing-point"),
        pytest.param("1.5E-2", True, "0.015", id="exponent"),
        pytest.param("1e3", True, "1E+3", id="lowercase-exponent"),
    ],
)
def test_parse_decimal_text_returns_the_exact_decimal(
    text: str, parentheses_negative: bool, expected: str
) -> None:
    result = parse_decimal_text(text, parentheses_negative=parentheses_negative)
    assert type(result) is Decimal
    assert result == Decimal(expected)
    # The string form proves exactness: the scale and digits came from the text, not a float.
    assert str(result) == expected


@pytest.mark.parametrize(
    ("text", "parentheses_negative"),
    [
        pytest.param(None, True, id="none"),
        pytest.param(None, False, id="none-parentheses-off"),
        pytest.param("", True, id="empty"),
        pytest.param("   ", True, id="blank"),
        pytest.param("-", True, id="hyphen"),
        pytest.param("—", True, id="em-dash"),
        pytest.param("–", True, id="en-dash"),
        pytest.param(" - ", True, id="spaced-hyphen"),
        pytest.param("n/a", True, id="n/a"),
        pytest.param("N/A", True, id="N/A"),
        pytest.param("(1,234.5)", False, id="parentheses-when-off"),
        pytest.param("NaN", True, id="nan"),
        pytest.param("sNaN", True, id="signalling-nan"),
        pytest.param("Infinity", True, id="infinity"),
        pytest.param("-Infinity", True, id="negative-infinity"),
        pytest.param("inf", True, id="inf"),
        pytest.param("abc", True, id="letters"),
        pytest.param("12abc", True, id="trailing-letters"),
        pytest.param("KES 12", True, id="currency-prefix"),
        pytest.param("5%", True, id="percent-sign"),
        pytest.param("1.2.3", True, id="two-points"),
        pytest.param("--5", True, id="double-minus"),
        pytest.param("+-5", True, id="plus-minus"),
        pytest.param("(-5)", True, id="signed-inside-parentheses"),
        pytest.param("(+5)", True, id="plus-inside-parentheses"),
        pytest.param("(-)", True, id="dash-inside-parentheses"),
        pytest.param("()", True, id="empty-parentheses"),
        pytest.param("(5", True, id="unclosed-parenthesis"),
        pytest.param("5)", True, id="unopened-parenthesis"),
        pytest.param("1_000", True, id="underscore-separator"),
        pytest.param("1e9999999999999999999", True, id="exponent-beyond-decimal-range"),
    ],
)
def test_parse_decimal_text_returns_none_for_blanks_and_non_numbers(
    text: str | None, parentheses_negative: bool
) -> None:
    assert parse_decimal_text(text, parentheses_negative=parentheses_negative) is None


def test_parse_decimal_text_reads_parentheses_as_negative_by_default() -> None:
    assert parse_decimal_text("(2,500)") == Decimal("-2500")


# --- dedupe_by_key --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("records", "expected", "dropped"),
    [
        pytest.param(
            [
                ("SCOM", Decimal("28.10")),
                ("EQTY", Decimal("60.25")),
                ("SCOM", Decimal("28.35")),
                ("KCB", Decimal("51.00")),
                ("EQTY", Decimal("60.50")),
            ],
            [("SCOM", Decimal("28.35")), ("EQTY", Decimal("60.50")), ("KCB", Decimal("51.00"))],
            2,
            id="last-value-at-first-position",
        ),
        pytest.param(
            [("SCOM", Decimal("28.10")), ("EQTY", Decimal("60.25"))],
            [("SCOM", Decimal("28.10")), ("EQTY", Decimal("60.25"))],
            0,
            id="no-duplicates",
        ),
        pytest.param(
            [("KCB", Decimal("50.00")), ("KCB", Decimal("50.50")), ("KCB", Decimal("51.00"))],
            [("KCB", Decimal("51.00"))],
            2,
            id="all-one-key",
        ),
        pytest.param([], [], 0, id="empty"),
    ],
)
def test_dedupe_by_key(
    records: list[tuple[str, Decimal]], expected: list[tuple[str, Decimal]], dropped: int
) -> None:
    original = list(records)
    assert dedupe_by_key(records, lambda record: record[0]) == (expected, dropped)
    assert records == original


def test_dedupe_by_key_accepts_composite_keys_and_any_sequence() -> None:
    records = (
        ("SCOM", date(2026, 10, 26), Decimal("28.10")),
        ("SCOM", date(2026, 10, 27), Decimal("28.35")),
        ("SCOM", date(2026, 10, 26), Decimal("28.20")),
    )
    deduped, dropped = dedupe_by_key(records, lambda record: (record[0], record[1]))
    assert deduped == [
        ("SCOM", date(2026, 10, 26), Decimal("28.20")),
        ("SCOM", date(2026, 10, 27), Decimal("28.35")),
    ]
    assert dropped == 1


# --- to_jsonable ----------------------------------------------------------------------------


class _Scale(Enum):
    THOUSANDS = Decimal("1000")


class _Session(Enum):
    PRE_OPEN = 1


class _Labelled:
    def __str__(self) -> str:
        return "labelled object"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(Decimal("7.50"), "7.50", id="decimal"),
        pytest.param(Decimal("-0.00"), "-0.00", id="negative-zero-decimal"),
        pytest.param(Decimal("1E+3"), "1E+3", id="decimal-exponent"),
        pytest.param(Decimal("NaN"), None, id="decimal-nan"),
        pytest.param(Decimal("Infinity"), None, id="decimal-infinity"),
        pytest.param(Decimal("-Infinity"), None, id="decimal-negative-infinity"),
        pytest.param(date(2026, 9, 30), "2026-09-30", id="date"),
        pytest.param(
            datetime(2026, 9, 30, 12, 0, tzinfo=UTC), "2026-09-30T12:00:00+00:00", id="utc"
        ),
        pytest.param(
            datetime(2026, 10, 28, 7, 30, tzinfo=ZoneInfo("Africa/Nairobi")),
            "2026-10-28T07:30:00+03:00",
            id="africa-nairobi",
        ),
        pytest.param(time(9, 30), "09:30:00", id="time"),
        pytest.param(RUN_ID, "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", id="uuid"),
        pytest.param(RunStatus.PARTIAL, "partial", id="str-enum"),
        pytest.param(_Scale.THOUSANDS, "1000", id="enum-with-decimal-value"),
        pytest.param(_Session.PRE_OPEN, 1, id="enum-with-int-value"),
        pytest.param(None, None, id="none"),
        pytest.param(True, True, id="bool"),
        pytest.param(0, 0, id="int"),
        pytest.param("KES", "KES", id="str"),
        pytest.param(b"%PDF-", "b'%PDF-'", id="bytes-not-a-list"),
        pytest.param(bytearray(b"%PDF-"), "bytearray(b'%PDF-')", id="bytearray-not-a-list"),
        pytest.param(_Labelled(), "labelled object", id="other-object"),
        pytest.param(
            RejectedRow,
            "<class 'opennse_terminal.ingestion.base.RejectedRow'>",
            id="dataclass-type",
        ),
    ],
)
def test_to_jsonable_converts_scalars(value: object, expected: object) -> None:
    result = to_jsonable(value)
    assert result == expected
    assert type(result) is type(expected)


def test_to_jsonable_converts_nested_payloads_recursively() -> None:
    payload = {
        "expected": ("year", "month", "12-month inflation"),
        "found": ["year", "month"],
        "rates": {"91d": Decimal("15.72"), "status": RunStatus.LAYOUT_CHANGED},
        "sessions": [date(2026, 10, 26), (date(2026, 10, 27), {"close": Decimal("28.35")})],
        "rejected": RejectedRow(
            index=4, reason="volume_out_of_range", row={"volume": Decimal("1E+13")}
        ),
        "view": MappingProxyType({"weight": Decimal("1.0")}),
        "tickers": frozenset({"SCOM", "EQTY", "KCB"}),
        "levels": {Decimal("10"), Decimal("9"), Decimal("1.5")},
        "empty": (),
    }
    expected = {
        "expected": ["year", "month", "12-month inflation"],
        "found": ["year", "month"],
        "rates": {"91d": "15.72", "status": "layout_changed"},
        "sessions": ["2026-10-26", ["2026-10-27", {"close": "28.35"}]],
        "rejected": {"index": 4, "reason": "volume_out_of_range", "row": {"volume": "1E+13"}},
        "view": {"weight": "1.0"},
        "tickers": ["EQTY", "KCB", "SCOM"],
        # Sets are sorted by their converted strings, so "10" sorts before "9".
        "levels": ["1.5", "10", "9"],
        "empty": [],
    }
    result = to_jsonable(payload)
    assert result == expected
    assert json.loads(json.dumps(result)) == expected


def test_to_jsonable_stringifies_mapping_keys() -> None:
    payload = {
        1: "int",
        False: "bool",
        Decimal("2.50"): "decimal",
        date(2026, 1, 2): "date",
        RunStatus.FAILED: "enum",
        None: "none",
        ("SCOM", 2026): "tuple",
        RUN_ID: "uuid",
    }
    result = to_jsonable(payload)
    assert result == {
        "1": "int",
        "False": "bool",
        "2.50": "decimal",
        "2026-01-02": "date",
        "failed": "enum",
        "None": "none",
        "['SCOM', 2026]": "tuple",
        "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7": "uuid",
    }
    assert all(type(key) is str for key in result)


# --- RunRecorder ----------------------------------------------------------------------------


async def test_run_recorder_start_commits_a_running_row_in_its_own_transaction() -> None:
    engine = _EngineDouble()
    before = datetime.now(UTC)
    run_id = await RunRecorder(cast(AsyncEngine, engine)).start(
        pipeline="cbk_rates_csv", trigger="schedule", source_url=SOURCE_URL
    )
    after = datetime.now(UTC)

    (conn,) = engine.connections
    assert (conn.kind, conn.outcome) == (RUN_RECORD, COMMITTED)
    (insert,) = conn.executed
    assert isinstance(insert.statement, Insert)
    assert insert.statement.table.name == "terminal_ingestion_runs"
    params = insert.params
    assert isinstance(run_id, uuid.UUID)
    assert params["id"] == run_id
    started_at = params.pop("started_at")
    assert started_at.utcoffset() == timedelta(0)
    assert before <= started_at <= after
    assert params == {
        "id": run_id,
        "pipeline": "cbk_rates_csv",
        "trigger": "schedule",
        "source_url": SOURCE_URL,
        "status": "running",
        "rows_fetched": 0,
        "rows_written": 0,
        "rows_rejected": 0,
        "details": {},
    }


async def test_run_recorder_start_gives_each_run_a_new_id() -> None:
    recorder = RunRecorder(cast(AsyncEngine, _EngineDouble()))
    first = await recorder.start(pipeline="cbk_fx", trigger="cli")
    second = await recorder.start(pipeline="cbk_fx", trigger="cli")
    assert first != second


async def test_run_recorder_start_rejects_an_unknown_trigger_before_connecting() -> None:
    engine = _EngineDouble()
    with pytest.raises(ValueError, match="unknown run trigger 'manual'"):
        await RunRecorder(cast(AsyncEngine, engine)).start(
            pipeline="cbk_fx",
            trigger="manual",  # type: ignore[arg-type]
        )
    assert engine.connections == []


async def test_run_recorder_finish_records_final_status_counts_and_jsonable_details() -> None:
    engine = _EngineDouble()
    before = datetime.now(UTC)
    await RunRecorder(cast(AsyncEngine, engine)).finish(
        RUN_ID,
        status=RunStatus.PARTIAL,
        rows_fetched=10,
        rows_written=9,
        rows_rejected=1,
        error_message=None,
        details={"latest": Decimal("6.80"), "session": date(2026, 10, 27)},
        source_url=CSV_URL,
    )
    after = datetime.now(UTC)

    (conn,) = engine.connections
    assert (conn.kind, conn.outcome) == (RUN_RECORD, COMMITTED)
    (update,) = conn.executed
    assert isinstance(update.statement, Update)
    assert update.statement.table.name == "terminal_ingestion_runs"
    params = update.params
    finished_at = params.pop("finished_at")
    assert finished_at.utcoffset() == timedelta(0)
    assert before <= finished_at <= after
    assert params == {
        "status": "partial",
        "rows_fetched": 10,
        "rows_written": 9,
        "rows_rejected": 1,
        "error_message": None,
        "details": {"latest": "6.80", "session": "2026-10-27"},
        "source_url": CSV_URL,
        "id_1": RUN_ID,
    }


async def test_run_recorder_finish_keeps_the_stored_url_and_defaults_details() -> None:
    engine = _EngineDouble()
    await RunRecorder(cast(AsyncEngine, engine)).finish(
        RUN_ID, status=RunStatus.FAILED, error_message="lock held"
    )
    (update,) = engine.updates
    assert "source_url" not in update.params
    assert update.params["details"] == {}
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (0, 0)
    assert update.params["rows_rejected"] == 0
    assert update.params["error_message"] == "lock held"


@pytest.mark.parametrize(
    ("status", "message"),
    [
        pytest.param(
            RunStatus.RUNNING, "a finished run needs a final status, not 'running'", id="running"
        ),
        pytest.param("archived", "'archived' is not a valid RunStatus", id="not-a-status"),
    ],
)
async def test_run_recorder_finish_rejects_a_non_final_status_before_connecting(
    status: RunStatus, message: str
) -> None:
    engine = _EngineDouble()
    with pytest.raises(ValueError, match=message):
        await RunRecorder(cast(AsyncEngine, engine)).finish(RUN_ID, status=status)
    assert engine.connections == []


async def test_run_recorder_finish_raises_when_no_run_row_matches() -> None:
    engine = _EngineDouble(update_rowcount=0)
    with pytest.raises(LookupError, match=f"no ingestion run with id {RUN_ID}"):
        await RunRecorder(cast(AsyncEngine, engine)).finish(RUN_ID, status=RunStatus.SUCCEEDED)


async def test_run_recorder_record_writes_a_complete_row_in_two_transactions() -> None:
    engine = _EngineDouble()
    run_id = await RunRecorder(cast(AsyncEngine, engine)).record(
        pipeline="pdf_filing",
        trigger="api",
        status=RunStatus.FAILED,
        source_url="upload:annual-report.pdf",
        error_message="pdf_encrypted",
        details={"sha256_prefix": "9f86d081"},
        rows_fetched=1,
        rows_written=0,
        rows_rejected=1,
    )
    assert [(conn.kind, conn.outcome) for conn in engine.connections] == [
        (RUN_RECORD, COMMITTED),
        (RUN_RECORD, COMMITTED),
    ]
    (insert,) = engine.inserts
    (update,) = engine.updates
    assert insert.params["id"] == run_id
    assert insert.params["source_url"] == "upload:annual-report.pdf"
    assert (insert.params["pipeline"], insert.params["trigger"]) == ("pdf_filing", "api")
    assert update.params["id_1"] == run_id
    assert update.params["status"] == "failed"
    assert update.params["error_message"] == "pdf_encrypted"
    assert update.params["details"] == {"sha256_prefix": "9f86d081"}
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (1, 0)
    assert update.params["rows_rejected"] == 1
    assert "source_url" not in update.params


# --- execute_pipeline -----------------------------------------------------------------------


async def test_succeeded_run_writes_every_row_in_one_locked_committed_transaction(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    http = _HttpDouble()
    pipeline = _QuotePipeline(
        settings, cells=["6.80", "15.72", "(1,234.50)"], source_url=SOURCE_URL
    )

    result = await _execute(pipeline, engine, http, trigger="schedule")

    # The running row commits first, the data transaction runs on its own connection, and the
    # final update uses a third, separate connection.
    assert [conn.kind for conn in engine.connections] == [RUN_RECORD, DATA, RUN_RECORD]
    (insert,) = engine.inserts
    run_id = insert.params["id"]
    assert (insert.params["pipeline"], insert.params["trigger"]) == ("test_quotes", "schedule")
    assert insert.params["source_url"] == SOURCE_URL

    (data,) = engine.data_connections
    assert (data.outcome, data.closed) == (COMMITTED, True)
    assert [str(executed.statement) for executed in data.executed] == [IDLE_TIMEOUT_SQL, LOCK_SQL]
    assert data.executed[1].params == {"pipeline": "test_quotes"}

    (ctx,) = pipeline.contexts
    assert (ctx.run_id, ctx.pipeline, ctx.trigger) == (run_id, "test_quotes", "schedule")
    assert ctx.settings is settings
    assert ctx.conn is data
    assert ctx.http is http
    assert (http.entered, http.exited) == (False, False)

    (upsert,) = pipeline.upserts
    upsert_conn, upserted, upsert_run_id = upsert
    assert upsert_conn is data
    assert upsert_run_id == run_id
    assert [quote.value for quote in upserted] == [
        Decimal("6.80"),
        Decimal("15.72"),
        Decimal("-1234.50"),
    ]

    details = dict(result.details)
    duration = _pop_duration(details)
    assert details == {"source_url": SOURCE_URL}
    assert result == RunResult(
        run_id=run_id,
        pipeline="test_quotes",
        status=RunStatus.SUCCEEDED,
        rows_fetched=3,
        rows_written=3,
        rows_rejected=0,
        error_message=None,
        details=result.details,
    )

    (update,) = engine.updates
    assert update.params["id_1"] == run_id
    assert update.params["status"] == "succeeded"
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (3, 3)
    assert update.params["rows_rejected"] == 0
    assert update.params["error_message"] is None
    assert update.params["details"] == result.details
    assert update.params["source_url"] == SOURCE_URL

    messages = [record.getMessage() for record in caplog.records if record.name == LOGGER]
    assert messages == [
        f"ingestion run {run_id} started: pipeline=test_quotes trigger=schedule",
        f"ingestion run {run_id} finished: pipeline=test_quotes status=succeeded fetched=3 "
        f"written=3 rejected=0 duration={duration}s",
    ]
    assert _warnings_and_errors(caplog) == []


@pytest.mark.parametrize(
    ("readings", "expected"),
    [
        pytest.param((1_000_000_000, 3_500_500_000), "2.501", id="half-millisecond-rounds-up"),
        pytest.param((1_000_000_000, 1_000_499_999), "0.000", id="below-half-rounds-down"),
        pytest.param((0, 86_400_000_000_001), "86400.000", id="one-day"),
    ],
)
async def test_duration_is_measured_in_integer_nanoseconds(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    readings: tuple[int, int],
    expected: str,
) -> None:
    """``duration_seconds`` is the difference of two ``time.monotonic_ns`` readings divided
    exactly by 10^9 and rounded ``ROUND_HALF_UP`` to milliseconds, with no float clock."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    clock = iter(readings)
    monkeypatch.setattr(base.time, "monotonic_ns", lambda: next(clock))

    result = await _execute(
        _QuotePipeline(settings, cells=["1.00"]), _EngineDouble(), _HttpDouble()
    )

    # Read exactly twice: when the run starts and when its details are assembled.
    assert next(clock, None) is None
    assert result.status is RunStatus.SUCCEEDED
    assert result.details["duration_seconds"] == expected
    finished = [record.getMessage() for record in caplog.records if record.name == LOGGER][-1]
    assert finished.endswith(f" duration={expected}s")


async def test_partial_run_writes_valid_rows_and_keeps_the_first_50_rejections(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    """600 rows with every tenth unparseable: 60 rejected is exactly 10%, so ``partial``."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    cells = ["abc" if index % 10 == 0 else "12.50" for index in range(600)]
    pipeline = _QuotePipeline(settings, cells=cells)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (600, 540, 60)
    assert result.error_message is None
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED
    (upsert,) = pipeline.upserts
    assert len(upsert[1]) == 540
    details = dict(result.details)
    _pop_duration(details)
    assert details == {
        "rejected": [
            {"index": index, "reason": "invalid_decimal", "row": {"cell": "abc"}}
            for index in range(0, 500, 10)
        ]
    }
    assert len(details["rejected"]) == REJECTED_SAMPLE_LIMIT
    (update,) = engine.updates
    assert update.params["status"] == "partial"
    assert (update.params["rows_written"], update.params["rows_rejected"]) == (540, 60)
    assert "source_url" not in update.params
    assert _warnings_and_errors(caplog) == []


@pytest.mark.parametrize(
    ("pipeline_class", "cells", "message", "expected", "found", "rejected"),
    [
        pytest.param(
            _QuotePipeline,
            ["abc", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "1.00", "xyz"],
            "2 of 10 rows rejected, more than 10% of the rows parsed",
            {"min_rows": 0, "max_invalid_fraction": "0.10"},
            {"parsed": 10, "rejected": 2},
            [
                {"index": 0, "reason": "invalid_decimal", "row": {"cell": "abc"}},
                {"index": 9, "reason": "invalid_decimal", "row": {"cell": "xyz"}},
            ],
            id="more-than-10-percent-rejected",
        ),
        pytest.param(
            _MinRowsPipeline,
            ["1.00", "2.00", "3.00", "4.00"],
            "4 rows parsed, below the minimum of 5",
            {"min_rows": 5, "max_invalid_fraction": "0.10"},
            {"parsed": 4, "rejected": 0},
            None,
            id="below-min-rows",
        ),
    ],
)
async def test_threshold_breach_ends_layout_changed_and_writes_nothing(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    pipeline_class: type[_QuotePipeline],
    cells: list[str],
    message: str,
    expected: dict[str, object],
    found: dict[str, int],
    rejected: list[dict[str, object]] | None,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    pipeline = pipeline_class(settings, cells=cells)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.error_message == message
    assert (result.rows_fetched, result.rows_written) == (found["parsed"], 0)
    assert result.rows_rejected == found["rejected"]
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    details = dict(result.details)
    _pop_duration(details)
    assert details.pop("rejected", None) == rejected
    assert details == {"expected": expected, "found": found}
    (record,) = _warnings_and_errors(caplog)
    assert record.levelno == logging.ERROR
    assert record.getMessage() == (
        f"ingestion run {result.run_id} ({pipeline.name}) layout_changed: {message}; "
        f"expected={expected!r} found={found!r}"
    )


async def test_reconcile_rejections_count_toward_the_thresholds(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(settings, cells=["1.00"] * 10, unknown=[3])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (10, 9, 1)
    assert [quote.index for quote in pipeline.upserts[0][1]] == [0, 1, 2, 4, 5, 6, 7, 8, 9]
    assert result.details["rejected"] == [
        {"index": 3, "reason": "unknown_ticker", "row": {"cell": "1.00"}}
    ]


async def test_reconcile_sees_only_validated_rows_and_both_rejection_kinds_add_up(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(settings, cells=["abc", *(["1.00"] * 9)], unknown=[3])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert [quote.index for quote in pipeline.reconciled] == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.error_message == "2 of 10 rows rejected, more than 10% of the rows parsed"
    assert result.details["found"] == {"parsed": 10, "rejected": 2}
    assert result.details["rejected"] == [
        {"index": 0, "reason": "invalid_decimal", "row": {"cell": "abc"}},
        {"index": 3, "reason": "unknown_ticker", "row": {"cell": "1.00"}},
    ]
    assert pipeline.upserts == []


async def test_empty_parse_succeeds_without_calling_upsert(settings: TerminalSettings) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=[])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.SUCCEEDED
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED


@pytest.mark.parametrize(
    ("error", "status", "message", "evidence", "level"),
    [
        pytest.param(
            SourceLayoutChanged(
                expected=("year", "month"), found=("yr",), message="header row changed"
            ),
            RunStatus.LAYOUT_CHANGED,
            "header row changed",
            {"expected": ["year", "month"], "found": ["yr"]},
            logging.ERROR,
            id="source-layout-changed",
        ),
        pytest.param(
            RobotsDisallowed(CSV_URL),
            RunStatus.BLOCKED_BY_ROBOTS,
            f"robots.txt blocks {CSV_URL} (disallowed)",
            {"robots": {"url": CSV_URL, "reason": "disallowed"}},
            logging.WARNING,
            id="robots-disallowed",
        ),
        pytest.param(
            FetchFailed(CSV_URL, "http_503"),
            RunStatus.FAILED,
            f"fetch failed for {CSV_URL}: http_503",
            {"fetch_failure": {"url": CSV_URL, "reason": "http_503"}},
            logging.ERROR,
            id="fetch-failed",
        ),
        pytest.param(
            AmbiguousSession({"reason": "label_mismatch", "label_session": date(2026, 10, 27)}),
            RunStatus.AMBIGUOUS_SESSION,
            "ambiguous session: label_mismatch",
            {"reason": "label_mismatch", "label_session": "2026-10-27"},
            logging.WARNING,
            id="ambiguous-session",
        ),
        pytest.param(
            NoNewData("source has not advanced", {"latest_session": date(2026, 10, 27)}),
            RunStatus.NO_NEW_DATA,
            "source has not advanced",
            {"latest_session": "2026-10-27"},
            None,
            id="no-new-data",
        ),
        pytest.param(
            CalendarNotVerified(2028, frozenset({2026, 2027})),
            RunStatus.FAILED,
            CALENDAR_MESSAGE,
            {"calendar": {"year": 2028, "verified_years": [2026, 2027]}},
            logging.ERROR,
            id="calendar-not-verified",
        ),
        pytest.param(
            RuntimeError("parser exploded"),
            RunStatus.FAILED,
            "RuntimeError: parser exploded",
            {"exception": "RuntimeError"},
            logging.ERROR,
            id="unexpected",
        ),
    ],
)
async def test_fetch_exception_becomes_the_run_status_with_its_evidence(
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    error: BaseException,
    status: RunStatus,
    message: str,
    evidence: dict[str, object],
    level: int | None,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"], source_url=SOURCE_URL, fetch_error=error)

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is status
    assert result.error_message == message
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    details = dict(result.details)
    _pop_duration(details)
    assert details == {"source_url": SOURCE_URL, **evidence}
    (update,) = engine.updates
    assert update.params["status"] == status.value
    assert update.params["error_message"] == message
    assert update.params["details"] == result.details

    records = _warnings_and_errors(caplog)
    if level is None:
        assert records == []
    else:
        (record,) = records
        assert record.levelno == level
        assert record.getMessage().startswith(f"ingestion run {result.run_id} (test_quotes) ")
        # Only an unexpected exception carries its traceback into the log.
        assert (record.exc_info is not None) is isinstance(error, RuntimeError)


async def test_exception_after_parsing_keeps_the_fetched_count_and_writes_nothing(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(
        settings, cells=["1.00", "2.00", "3.00"], upsert_error=RuntimeError("unique violation")
    )

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.FAILED
    assert result.error_message == "RuntimeError: unique violation"
    # Written rows are counted only once the data transaction has committed.
    assert (result.rows_fetched, result.rows_written, result.rows_rejected) == (3, 0, 0)
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK


async def test_no_new_data_from_reconcile_rolls_back_and_records_its_evidence(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _ReconcilingPipeline(
        settings,
        cells=["28.35", "60.50"],
        reconcile_error=NoNewData("source has not advanced", {"latest": Decimal("28.35")}),
    )

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.NO_NEW_DATA
    assert result.error_message == "source has not advanced"
    assert (result.rows_fetched, result.rows_written) == (2, 0)
    assert result.details["latest"] == "28.35"
    assert pipeline.upserts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK


async def test_held_lock_ends_failed_without_fetching(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble(lock_available=False)
    pipeline = _QuotePipeline(settings, cells=["1.00"])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.status is RunStatus.FAILED
    assert result.error_message == "lock held"
    assert pipeline.contexts == []
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    assert [str(executed.statement) for executed in data.executed] == [IDLE_TIMEOUT_SQL, LOCK_SQL]
    details = dict(result.details)
    _pop_duration(details)
    assert details == {"lock_key": "terminal:test_quotes"}
    warning, error = _warnings_and_errors(caplog)
    assert (warning.levelno, warning.getMessage()) == (
        logging.WARNING,
        "advisory lock held for test_quotes",
    )
    assert (error.levelno, error.getMessage()) == (
        logging.ERROR,
        f"ingestion run {result.run_id} (test_quotes) failed: lock held",
    )
    assert error.exc_info is None


async def test_run_name_keys_the_run_row_the_lock_and_the_context(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _CsvImportPipeline(settings, cells=["1.00"])

    result = await _execute(pipeline, engine, _HttpDouble())

    assert result.pipeline == "csv_import:daily_prices"
    assert engine.inserts[0].params["pipeline"] == "csv_import:daily_prices"
    assert engine.data_connections[0].executed[1].params == {"pipeline": "csv_import:daily_prices"}
    assert pipeline.contexts[0].pipeline == "csv_import:daily_prices"


async def test_source_url_discovered_during_the_run_is_recorded(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(
        settings, cells=["8.75"], source_url=SOURCE_URL, discovered_url=CSV_URL
    )

    await _execute(pipeline, engine, _HttpDouble())

    assert engine.inserts[0].params["source_url"] == SOURCE_URL
    assert engine.updates[0].params["source_url"] == CSV_URL


async def test_unknown_trigger_fails_before_any_data_connection(
    settings: TerminalSettings,
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    with pytest.raises(ValueError, match="unknown run trigger 'nightly'"):
        await _execute(pipeline, engine, _HttpDouble(), trigger="nightly")  # type: ignore[arg-type]
    assert engine.connections == []
    assert pipeline.contexts == []


@pytest.mark.parametrize(
    ("engine_options", "error", "message"),
    [
        pytest.param({"fail_run_updates": True}, RuntimeError, "run row update failed", id="error"),
        pytest.param({"update_rowcount": 0}, LookupError, "no ingestion run with id", id="lost"),
    ],
)
async def test_errors_writing_the_run_row_propagate(
    settings: TerminalSettings,
    engine_options: dict[str, Any],
    error: type[Exception],
    message: str,
) -> None:
    engine = _EngineDouble(**engine_options)
    pipeline = _QuotePipeline(settings, cells=["1.00"])
    with pytest.raises(error, match=message):
        await _execute(pipeline, engine, _HttpDouble())
    # The data transaction had already committed; only the run row's final update failed.
    (data,) = engine.data_connections
    assert data.outcome == COMMITTED


@pytest.mark.parametrize(
    "interrupt",
    [pytest.param(KeyboardInterrupt(), id="keyboard"), pytest.param(SystemExit(3), id="exit")],
)
async def test_interrupts_propagate_untouched(
    settings: TerminalSettings, interrupt: BaseException
) -> None:
    engine = _EngineDouble()
    pipeline = _QuotePipeline(settings, cells=["1.00"], fetch_error=interrupt)
    with pytest.raises(type(interrupt)):
        await _execute(pipeline, engine, _HttpDouble())
    (data,) = engine.data_connections
    assert data.outcome == ROLLED_BACK
    # The run row is left as ``running``: no final update is attempted.
    assert engine.updates == []


async def test_cancelled_run_rolls_back_records_failed_and_reraises(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble()
    pipeline = _BlockingPipeline(settings, source_url=NSE_URL)
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble(), trigger="schedule"))
    await pipeline.fetch_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    (data,) = engine.data_connections
    assert (data.outcome, data.closed) == (ROLLED_BACK, True)
    (insert,) = engine.inserts
    run_id = insert.params["id"]
    (update,) = engine.updates
    assert update.params["id_1"] == run_id
    assert update.params["status"] == "failed"
    assert update.params["error_message"] == "cancelled"
    assert (update.params["rows_fetched"], update.params["rows_written"]) == (0, 0)
    assert update.params["rows_rejected"] == 0
    assert update.params["source_url"] == NSE_URL
    details = dict(update.params["details"])
    _pop_duration(details)
    assert details == {"source_url": NSE_URL}
    (record,) = _warnings_and_errors(caplog)
    assert (record.levelno, record.getMessage()) == (
        logging.ERROR,
        f"ingestion run {run_id} (test_blocking) failed: cancelled",
    )
    assert record.exc_info is None


async def test_cancelled_run_still_reraises_when_its_run_row_cannot_be_written(
    settings: TerminalSettings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    engine = _EngineDouble(fail_run_updates=True)
    pipeline = _BlockingPipeline(settings)
    task = asyncio.create_task(_execute(pipeline, engine, _HttpDouble()))
    await pipeline.fetch_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    run_id = engine.inserts[0].params["id"]
    not_recorded, failed = _warnings_and_errors(caplog)
    assert not_recorded.getMessage() == f"could not record cancelled ingestion run {run_id}"
    assert not_recorded.exc_info is not None
    assert failed.getMessage() == f"ingestion run {run_id} (test_blocking) failed: cancelled"
