"""Tests for the scheduler process (``opennse_terminal.ingestion.scheduler``).

Covered: the two cron jobs and their options (:data:`JOB_DEFINITIONS`, :data:`JOB_DEFAULTS`);
:func:`build_scheduler`, both as built (unstarted, Africa/Nairobi, the real cron triggers' fire
times) and through a capturing ``AsyncIOScheduler`` double (the exact constructor, ``add_job``
and ``add_listener`` calls); the job-event log lines; the scheduled job callable and
:func:`run_job` (trigger ``"schedule"``, per-result log lines, errors named by type only, the
cancellation contract); the SIGINT/SIGTERM stop handlers, with and without loop signal support;
the wait for in-flight jobs; the ``_serve`` and :func:`run_forever` lifecycle; and the
import-time check that every scheduled pipeline is registered.

Every test is offline and runs no scheduled job: no network, no database, no real sleep. The
registry's ``run_pipelines`` and the APScheduler scheduler are replaced by capturing doubles
wherever a job would run, and the real schedulers built here are never started. Code that
inspects every task of its loop runs in a synchronous test on an event loop of its own, and code
that calls ``asyncio.run`` in a thread of its own, never on the session loop the async tests share.
Tests of signal handling run under the standard SIGINT and SIGTERM dispositions of a Python
process, whatever the launch of pytest left in place, and compare the handlers with those; the
dispositions found before each such test are reinstated after it.
Expected schedules, options and messages are literals from the plan (cron Tuesday-Saturday 07:30
and Monday-Friday 18:15 in Africa/Nairobi; ``coalesce=True``, ``max_instances=1``,
``misfire_grace_time=3600``) and the module's documented log lines.
"""

import asyncio
import importlib.util
import logging
import signal
import sys
import threading
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import FrameType, MappingProxyType
from typing import Any, TypeVar, cast
from zoneinfo import ZoneInfo

import pytest
from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobEvent,
    JobExecutionEvent,
)
from apscheduler.triggers.cron import CronTrigger

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion import registry, scheduler
from opennse_terminal.ingestion.base import RunResult, RunStatus
from opennse_terminal.ingestion.scheduler import JobDefinition

LOGGER = "opennse_terminal.ingestion.scheduler"
NAIROBI = ZoneInfo("Africa/Nairobi")
NSE_NAMES = ("nse_market_statistics",)
CBK_NAMES = ("cbk_rates_csv", "cbk_inflation", "cbk_fx", "cbk_bonds", "cbk_reserves")
JOB_OPTIONS = {"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600}
STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)

# Monday 26 October 2026, midnight in Nairobi: the week the fire-time tests walk through.
MONDAY_MIDNIGHT = datetime(2026, 10, 26, 0, 0, tzinfo=NAIROBI)
NSE_RUN_TIME = datetime(2026, 10, 27, 7, 30, tzinfo=NAIROBI)
CBK_RUN_TIME = datetime(2026, 10, 26, 18, 15, tzinfo=NAIROBI)

# Stands for connection detail a real database error could carry; no log line may repeat an
# exception's message or traceback.
SENTINEL = "sentinel-connection-detail-9b2e"

# Bounds a wait that returns at once when the code under test works; reached only when it hangs.
SAFETY_TIMEOUT_SECONDS = 30
_YIELD_LIMIT = 100

SettingsFactory = Callable[..., TerminalSettings]
SignalHandler = Callable[[int, FrameType | None], Any] | int | None

_T = TypeVar("_T")

RATES_RESULT = RunResult(
    run_id=uuid.UUID("5d2c7a10-8f3e-4c1b-9a6d-2e4f6a8b0c11"),
    pipeline="cbk_rates_csv",
    status=RunStatus.SUCCEEDED,
    rows_fetched=424,
    rows_written=423,
    rows_rejected=1,
    error_message=None,
    details={},
)
INFLATION_RESULT = RunResult(
    run_id=uuid.UUID("5d2c7a10-8f3e-4c1b-9a6d-2e4f6a8b0c12"),
    pipeline="cbk_inflation",
    status=RunStatus.FAILED,
    rows_fetched=0,
    rows_written=0,
    rows_rejected=0,
    error_message="lock held",
    details={},
)
NSE_RESULT = RunResult(
    run_id=uuid.UUID("5d2c7a10-8f3e-4c1b-9a6d-2e4f6a8b0c13"),
    pipeline="nse_market_statistics",
    status=RunStatus.AMBIGUOUS_SESSION,
    rows_fetched=70,
    rows_written=0,
    rows_rejected=0,
    error_message=None,
    details={},
)


# --- Helpers and doubles ---------------------------------------------------------------------


def scheduler_records(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    """The scheduler's captured log records as ``(level, message)`` pairs, in order."""
    return [
        (record.levelno, record.getMessage()) for record in caplog.records if record.name == LOGGER
    ]


def current_stop_handlers() -> dict[signal.Signals, SignalHandler]:
    """The handlers now installed for SIGINT and SIGTERM."""
    return {sig: signal.getsignal(sig) for sig in STOP_SIGNALS}


def raise_handled_signal(signum: signal.Signals, pre_test_handler: SignalHandler) -> None:
    """Send ``signum`` to this process, refusing while its pre-test handler is still installed:
    under pytest that handler is the default action, which would end the test run rather than
    exercise the code under test."""
    if signal.getsignal(signum) == pre_test_handler:
        raise AssertionError(f"no stop handler is installed for {signum.name}; not raising it")
    signal.raise_signal(signum)


def run_on_fresh_loop(main: Callable[[], Coroutine[Any, Any, _T]]) -> _T:
    """Run ``main()`` to completion on a new event loop, never the session loop, then close it."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


def run_in_new_thread(target: Callable[[], object]) -> int:
    """Run ``target()`` to completion in a new thread, raise again here what it raised, and
    return that thread's identifier. A thread still running after the safety timeout fails the
    test; being a daemon, it cannot keep the test process alive."""
    raised: list[BaseException] = []

    def run() -> None:
        try:
            target()
        except BaseException as exc:  # handed to the calling thread, which raises it again
            raised.append(exc)

    worker = threading.Thread(target=run, name="test-scheduler-run-forever", daemon=True)
    worker.start()
    worker.join(SAFETY_TIMEOUT_SECONDS)
    if worker.is_alive():
        raise AssertionError(f"the thread did not finish within {SAFETY_TIMEOUT_SECONDS} seconds")
    if raised:
        raise raised[0]
    assert worker.ident is not None
    return worker.ident


async def within_safety_timeout(awaitable: Awaitable[_T]) -> _T:
    """Await ``awaitable``, failing with ``TimeoutError`` only if it hangs."""
    async with asyncio.timeout(SAFETY_TIMEOUT_SECONDS):
        return await awaitable


async def yield_until(condition: Callable[[], bool], what: str) -> None:
    """Give the running loop up to 100 iterations, without sleeping, until ``condition()``."""
    for _ in range(_YIELD_LIMIT):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError(f"{what} did not happen within {_YIELD_LIMIT} loop iterations")


def fire_times(trigger: Any, start: datetime, count: int) -> list[datetime]:
    """The first ``count`` fire times of ``trigger`` after ``start``, each computed from the
    previous one as APScheduler does after a run."""
    times: list[datetime] = []
    previous: datetime | None = None
    now = start
    for _ in range(count):
        fire = trigger.get_next_fire_time(previous, now)
        assert isinstance(fire, datetime)
        times.append(fire)
        previous = now = fire
    return times


@pytest.fixture
def stop_handlers_before() -> Iterator[dict[signal.Signals, SignalHandler]]:
    """The standard SIGINT and SIGTERM dispositions, installed for the test, which are yielded;
    the dispositions really in place before the test are reinstated after it, ``SIG_IGN``
    included.

    The standard dispositions, ``default_int_handler`` for SIGINT and ``SIG_DFL`` for SIGTERM,
    are those of a Python process started normally, which the ``opennse-terminal scheduler``
    process is. They are installed because, on Unix, the loop's ``remove_signal_handler`` resets
    each signal to its standard disposition, not to the one it replaced; the dispositions pytest
    inherits depend on how it was launched (a background job of a non-interactive shell starts
    with SIGINT ignored, and Python keeps that), so only the standard ones make the comparison
    after removal independent of the launch.
    """
    really_before = current_stop_handlers()
    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        yield current_stop_handlers()
    finally:
        for sig, handler in really_before.items():
            # getsignal() gives None for a handler not installed from Python, which signal()
            # cannot reinstate; that signal keeps its standard disposition.
            if handler is not None:
                signal.signal(sig, handler)


class RecordingAsyncIOScheduler:
    """Stands in for ``AsyncIOScheduler`` in ``build_scheduler``: records the constructor's
    keyword arguments and every ``add_job`` and ``add_listener`` call, and schedules nothing."""

    def __init__(self, **options: object) -> None:
        self.options = options
        self.added_jobs: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.listeners: list[tuple[object, ...]] = []

    def add_job(self, *args: object, **kwargs: object) -> None:
        self.added_jobs.append((args, kwargs))

    def add_listener(self, *args: object) -> None:
        self.listeners.append(args)


class RunPipelinesDouble:
    """Stands in for ``registry.run_pipelines``: records each call together with the scheduler
    log lines already emitted when it was made, then raises ``error``, blocks until cancelled
    (``block``), or returns ``results``."""

    def __init__(
        self,
        caplog: pytest.LogCaptureFixture,
        *,
        results: list[RunResult] | None = None,
        error: BaseException | None = None,
        block: bool = False,
    ) -> None:
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.logged_before_call: list[list[tuple[int, str]]] = []
        self.started = asyncio.Event()
        self._caplog = caplog
        self._results = results if results is not None else []
        self._error = error
        self._block = block
        self._never_set = asyncio.Event()

    async def __call__(self, *args: object, **kwargs: object) -> list[RunResult]:
        self.calls.append((args, kwargs))
        self.logged_before_call.append(scheduler_records(self._caplog))
        self.started.set()
        if self._block:
            await self._never_set.wait()
        if self._error is not None:
            raise self._error
        return self._results


class LoopWithoutSignalHandlers:
    """An event loop as ``_install_stop_handlers`` sees one on Windows: ``add_signal_handler``
    raises ``NotImplementedError``, and ``call_soon_threadsafe`` records the arguments of each
    callback and runs it at once."""

    def __init__(self) -> None:
        self.threadsafe_calls: list[tuple[object, ...]] = []

    def add_signal_handler(self, sig: int, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError

    def call_soon_threadsafe(self, callback: Callable[..., object], *args: object) -> None:
        self.threadsafe_calls.append(args)
        callback(*args)


class StoppingScheduler:
    """Stands in for an ``AsyncIOScheduler`` whose ``shutdown(wait=False)`` takes effect a few
    loop iterations later: ``running`` reads True ``polls_while_running`` times, then False."""

    def __init__(self, polls_while_running: int) -> None:
        self.polls = 0
        self._polls_while_running = polls_while_running

    @property
    def running(self) -> bool:
        self.polls += 1
        return self.polls <= self._polls_while_running


@dataclass(frozen=True, slots=True)
class FakeJob:
    """What ``_serve`` reads of a scheduled job."""

    id: str
    next_run_time: datetime


class TimelineHandler(logging.Handler):
    """Appends each log line to the timeline the fake scheduler records its calls on, so the
    order of calls and log lines can be asserted together."""

    def __init__(self, timeline: list[str]) -> None:
        super().__init__(level=logging.INFO)
        self.timeline = timeline

    def emit(self, record: logging.LogRecord) -> None:
        self.timeline.append(f"{record.levelname} {record.getMessage()}")


class ServedScheduler:
    """Stands in for the scheduler ``build_scheduler`` returns, in ``_serve``: records
    ``start``, ``get_jobs`` and ``shutdown`` on the timeline. ``start()`` raises
    ``start_error``; otherwise it starts the task of a scheduled job, a run in progress when the
    process is stopped, and asks the running loop to deliver SIGTERM, as an operator stopping
    the process would, once it has checked that a stop handler is installed.

    Like ``AsyncIOScheduler``, it applies ``shutdown()`` on a later loop iteration: ``running``
    still reads True for ``POLLS_WHILE_STOPPING`` polls after the call. At the next poll the
    scheduler stops and cancels the job's task, as its executor does, and the job then takes
    ``RECORDING_YIELDS`` loop iterations, without sleeping, to finish recording the run it
    interrupted, as ``execute_pipeline`` does, before it records that on the timeline."""

    POLLS_WHILE_STOPPING = 3
    RECORDING_YIELDS = 5

    def __init__(
        self,
        timeline: list[str],
        pre_test_handlers: dict[signal.Signals, SignalHandler],
        start_error: Exception | None = None,
    ) -> None:
        self.timeline = timeline
        self.handlers_at_start: dict[signal.Signals, SignalHandler] | None = None
        self.job: asyncio.Task[None] | None = None
        self.polls_after_shutdown = 0
        self._pre_test_handlers = pre_test_handlers
        self._start_error = start_error
        self._running = False
        self._shutdown_requested = False

    @property
    def running(self) -> bool:
        if self._running and self._shutdown_requested:
            self.polls_after_shutdown += 1
            if self.polls_after_shutdown > self.POLLS_WHILE_STOPPING:
                self._running = False
                self.timeline.append("shutdown applied: job nse_equities_eod cancelled")
                assert self.job is not None
                self.job.cancel()
        return self._running

    def start(self) -> None:
        self.timeline.append("start")
        self.handlers_at_start = current_stop_handlers()
        if self._start_error is not None:
            raise self._start_error
        if signal.getsignal(signal.SIGTERM) == self._pre_test_handlers[signal.SIGTERM]:
            raise AssertionError("no SIGTERM stop handler is installed; not delivering SIGTERM")
        self._running = True
        loop = asyncio.get_running_loop()
        self.job = loop.create_task(self._run_job_until_cancelled())
        loop.call_soon(signal.raise_signal, signal.SIGTERM)

    def get_jobs(self) -> list[FakeJob]:
        self.timeline.append("get_jobs")
        return [
            FakeJob("nse_equities_eod", NSE_RUN_TIME),
            FakeJob("cbk_macro", CBK_RUN_TIME),
        ]

    def shutdown(self, wait: bool = True) -> None:
        self.timeline.append(f"shutdown(wait={wait})")
        self._shutdown_requested = True

    async def _run_job_until_cancelled(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            for _ in range(self.RECORDING_YIELDS):
                await asyncio.sleep(0)
            self.timeline.append("job nse_equities_eod recorded its run")
            raise


@pytest.fixture
def timeline(caplog: pytest.LogCaptureFixture) -> Iterator[list[str]]:
    """An ordered list of the fake scheduler's calls and the scheduler's INFO and higher lines."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    entries: list[str] = []
    handler = TimelineHandler(entries)
    logger = logging.getLogger(LOGGER)
    logger.addHandler(handler)
    try:
        yield entries
    finally:
        logger.removeHandler(handler)


@pytest.fixture
async def session_loop() -> asyncio.AbstractEventLoop:
    """The session loop the async tests share, held by the thread every test runs in."""
    return asyncio.get_running_loop()


# --- Job definitions and options -------------------------------------------------------------


def test_job_definitions_are_the_two_planned_jobs_in_order() -> None:
    assert scheduler.JOB_DEFINITIONS == (
        JobDefinition(
            job_id="nse_equities_eod",
            pipelines=("nse_market_statistics",),
            day_of_week="tue-sat",
            hour=7,
            minute=30,
        ),
        JobDefinition(
            job_id="cbk_macro",
            pipelines=(
                "cbk_rates_csv",
                "cbk_inflation",
                "cbk_fx",
                "cbk_bonds",
                "cbk_reserves",
            ),
            day_of_week="mon-fri",
            hour=18,
            minute=15,
        ),
    )


def test_cbk_macro_runs_the_registry_cbk_pipelines() -> None:
    """The scheduler and ``ingest cbk`` run the same five pipelines, in the same order."""
    cbk_macro = scheduler.JOB_DEFINITIONS[1]
    assert cbk_macro.pipelines == registry.CBK_PIPELINES == CBK_NAMES


def test_every_scheduled_pipeline_is_registered() -> None:
    scheduled = {name for job in scheduler.JOB_DEFINITIONS for name in job.pipelines}
    assert scheduled == {"nse_market_statistics", *CBK_NAMES}
    assert scheduled <= set(registry.PIPELINES)


def test_job_defaults_coalesce_never_overlap_and_allow_an_hour_late() -> None:
    writable_view: Any = scheduler.JOB_DEFAULTS
    assert scheduler.JOB_DEFAULTS == {
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 3600,
    }
    assert isinstance(scheduler.JOB_DEFAULTS, MappingProxyType)
    with pytest.raises(TypeError):
        writable_view["coalesce"] = False
    assert scheduler.JOB_DEFAULTS["coalesce"] is True


# --- build_scheduler -------------------------------------------------------------------------


def test_build_scheduler_needs_no_running_loop_and_leaves_the_scheduler_unstarted(
    test_settings: TerminalSettings,
) -> None:
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    built = scheduler.build_scheduler(test_settings)

    assert built.running is False
    assert built.timezone == NAIROBI
    assert [job.id for job in built.get_jobs()] == ["nse_equities_eod", "cbk_macro"]


@pytest.mark.parametrize(
    ("job_id", "pipelines"),
    [
        pytest.param("nse_equities_eod", NSE_NAMES, id="nse_equities_eod"),
        pytest.param("cbk_macro", CBK_NAMES, id="cbk_macro"),
    ],
)
def test_each_built_job_runs_its_pipelines_with_the_planned_options(
    test_settings: TerminalSettings, job_id: str, pipelines: tuple[str, ...]
) -> None:
    [job] = [job for job in scheduler.build_scheduler(test_settings).get_jobs() if job.id == job_id]

    assert job.name == job_id
    assert job.func is scheduler._run_scheduled_job
    assert job.args == (job_id, pipelines, test_settings)
    assert job.args[2] is test_settings
    assert job.kwargs == {}
    assert job.coalesce is True
    assert job.max_instances == 1
    assert job.misfire_grace_time == 3600
    assert isinstance(job.trigger, CronTrigger)
    assert job.trigger.timezone == NAIROBI


@pytest.mark.parametrize(
    ("job_id", "expected_fire_times", "expected_weekdays"),
    [
        pytest.param(
            "nse_equities_eod",
            [
                "2026-10-27T07:30:00+03:00",
                "2026-10-28T07:30:00+03:00",
                "2026-10-29T07:30:00+03:00",
                "2026-10-30T07:30:00+03:00",
                "2026-10-31T07:30:00+03:00",
                "2026-11-03T07:30:00+03:00",
            ],
            # Monday is 0: Tuesday to Saturday, then the next Tuesday.
            [1, 2, 3, 4, 5, 1],
            id="nse_equities_eod-tuesday-to-saturday-0730",
        ),
        pytest.param(
            "cbk_macro",
            [
                "2026-10-26T18:15:00+03:00",
                "2026-10-27T18:15:00+03:00",
                "2026-10-28T18:15:00+03:00",
                "2026-10-29T18:15:00+03:00",
                "2026-10-30T18:15:00+03:00",
                "2026-11-02T18:15:00+03:00",
            ],
            # Monday to Friday, then the next Monday.
            [0, 1, 2, 3, 4, 0],
            id="cbk_macro-monday-to-friday-1815",
        ),
    ],
)
def test_built_triggers_fire_on_the_planned_days_and_times_in_nairobi(
    test_settings: TerminalSettings,
    job_id: str,
    expected_fire_times: list[str],
    expected_weekdays: list[int],
) -> None:
    [job] = [job for job in scheduler.build_scheduler(test_settings).get_jobs() if job.id == job_id]

    times = fire_times(job.trigger, MONDAY_MIDNIGHT, len(expected_fire_times))

    assert [fire.isoformat() for fire in times] == expected_fire_times
    assert [fire.weekday() for fire in times] == expected_weekdays
    assert all(fire.utcoffset() == timedelta(hours=3) for fire in times)
    assert all(fire.tzinfo == NAIROBI for fire in times)


@pytest.mark.parametrize(
    ("zone", "first_nse_fire"),
    [
        pytest.param("Africa/Nairobi", "2026-10-27T07:30:00+03:00", id="africa-nairobi-default"),
        pytest.param("UTC", "2026-10-27T07:30:00+00:00", id="utc"),
    ],
)
def test_the_scheduler_and_its_triggers_take_the_zone_from_settings(
    make_settings: SettingsFactory, zone: str, first_nse_fire: str
) -> None:
    built = scheduler.build_scheduler(make_settings(timezone=zone))
    jobs = built.get_jobs()

    assert built.timezone == ZoneInfo(zone)
    assert [job.trigger.timezone for job in jobs] == [ZoneInfo(zone), ZoneInfo(zone)]
    [first] = fire_times(jobs[0].trigger, datetime(2026, 10, 26, tzinfo=ZoneInfo(zone)), 1)
    assert first.isoformat() == first_nse_fire


def test_build_scheduler_constructs_the_scheduler_with_the_job_defaults_and_listener(
    monkeypatch: pytest.MonkeyPatch, test_settings: TerminalSettings
) -> None:
    monkeypatch.setattr(scheduler, "AsyncIOScheduler", RecordingAsyncIOScheduler)

    built = scheduler.build_scheduler(test_settings)

    assert isinstance(built, RecordingAsyncIOScheduler)
    assert built.options == {"timezone": NAIROBI, "job_defaults": JOB_OPTIONS}
    assert type(built.options["job_defaults"]) is dict
    assert built.listeners == [
        (scheduler._log_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED),
    ]


@pytest.mark.parametrize(
    ("index", "job_id", "pipelines", "cron"),
    [
        pytest.param(
            0,
            "nse_equities_eod",
            NSE_NAMES,
            "cron[day_of_week='tue-sat', hour='7', minute='30']",
            id="nse_equities_eod",
        ),
        pytest.param(
            1,
            "cbk_macro",
            CBK_NAMES,
            "cron[day_of_week='mon-fri', hour='18', minute='15']",
            id="cbk_macro",
        ),
    ],
)
def test_build_scheduler_adds_each_job_with_explicit_options(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    index: int,
    job_id: str,
    pipelines: tuple[str, ...],
    cron: str,
) -> None:
    """Every job passes ``coalesce``, ``max_instances`` and ``misfire_grace_time`` itself, so
    none depends on APScheduler's own defaults, and replaces an existing job of its id."""
    monkeypatch.setattr(scheduler, "AsyncIOScheduler", RecordingAsyncIOScheduler)

    built = scheduler.build_scheduler(test_settings)

    assert isinstance(built, RecordingAsyncIOScheduler)
    assert len(built.added_jobs) == 2
    args, kwargs = built.added_jobs[index]
    func, trigger = args
    assert func is scheduler._run_scheduled_job
    assert isinstance(trigger, CronTrigger)
    assert str(trigger) == cron
    assert trigger.timezone == NAIROBI
    assert kwargs == {
        "id": job_id,
        "name": job_id,
        "args": (job_id, pipelines, test_settings),
        "replace_existing": True,
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 3600,
    }


# --- Job event log lines ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        pytest.param(
            JobExecutionEvent(
                EVENT_JOB_EXECUTED, "nse_equities_eod", "default", NSE_RUN_TIME, retval=[NSE_RESULT]
            ),
            [
                (
                    logging.INFO,
                    "job nse_equities_eod finished (scheduled for 2026-10-27 07:30:00+03:00)",
                )
            ],
            id="finished",
        ),
        pytest.param(
            JobExecutionEvent(EVENT_JOB_EXECUTED, "cbk_macro", "default", CBK_RUN_TIME, retval=[]),
            [(logging.INFO, "job cbk_macro finished (scheduled for 2026-10-26 18:15:00+03:00)")],
            id="finished-with-no-results",
        ),
        pytest.param(
            JobExecutionEvent(
                EVENT_JOB_EXECUTED, "cbk_macro", "default", CBK_RUN_TIME, retval=None
            ),
            [
                (
                    logging.WARNING,
                    "job cbk_macro was cancelled while running "
                    "(scheduled for 2026-10-26 18:15:00+03:00)",
                )
            ],
            id="cancelled-while-running",
        ),
        pytest.param(
            JobExecutionEvent(EVENT_JOB_MISSED, "nse_equities_eod", "default", NSE_RUN_TIME),
            [
                (
                    logging.WARNING,
                    "job nse_equities_eod missed its run scheduled for 2026-10-27 07:30:00+03:00 "
                    "by more than 3600 seconds",
                )
            ],
            id="missed",
        ),
        pytest.param(
            JobExecutionEvent(
                EVENT_JOB_ERROR,
                "cbk_macro",
                "default",
                CBK_RUN_TIME,
                exception=RuntimeError(f"could not connect: {SENTINEL}"),
                traceback=f"Traceback (most recent call last):\nRuntimeError: {SENTINEL}\n",
            ),
            [
                (
                    logging.ERROR,
                    "job cbk_macro raised RuntimeError (scheduled for 2026-10-26 18:15:00+03:00)",
                )
            ],
            id="error-named-by-type-only",
        ),
        pytest.param(
            JobEvent(EVENT_JOB_ADDED, "cbk_macro", "default"),
            [],
            id="other-event-not-logged",
        ),
    ],
)
def test_job_events_are_logged_once_at_their_level(
    caplog: pytest.LogCaptureFixture, event: Any, expected: list[tuple[int, str]]
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    scheduler._log_job_event(event)

    assert scheduler_records(caplog) == expected
    assert SENTINEL not in caplog.text


# --- The scheduled job callable and run_job --------------------------------------------------


@pytest.mark.parametrize(
    ("job_id", "pipelines", "fired", "results"),
    [
        pytest.param(
            "nse_equities_eod",
            NSE_NAMES,
            "job nse_equities_eod fired: running nse_market_statistics",
            [NSE_RESULT],
            id="nse_equities_eod",
        ),
        pytest.param(
            "cbk_macro",
            CBK_NAMES,
            "job cbk_macro fired: running cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, "
            "cbk_reserves",
            [RATES_RESULT, INFLATION_RESULT],
            id="cbk_macro",
        ),
    ],
)
async def test_scheduled_job_logs_that_it_fired_before_running_its_pipelines(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    test_settings: TerminalSettings,
    job_id: str,
    pipelines: tuple[str, ...],
    fired: str,
    results: list[RunResult],
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    double = RunPipelinesDouble(caplog, results=results)
    # The scheduler runs its jobs through the registry module itself, the one patched here.
    assert vars(scheduler)["registry"] is registry
    monkeypatch.setattr(registry, "run_pipelines", double)

    returned = await scheduler._run_scheduled_job(job_id, pipelines, test_settings)

    assert returned == results
    assert double.logged_before_call == [[(logging.INFO, fired)]]
    assert scheduler_records(caplog)[0] == (logging.INFO, fired)


async def test_scheduled_job_cancelled_while_running_returns_none_and_withdraws_it(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    test_settings: TerminalSettings,
) -> None:
    """The cancellation is absorbed: the task ends normally with ``None`` and no pending
    cancellation, which ``_log_job_event`` reports as cancelled while running."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    double = RunPipelinesDouble(caplog, block=True)
    monkeypatch.setattr(registry, "run_pipelines", double)
    task = asyncio.create_task(scheduler._run_scheduled_job("cbk_macro", CBK_NAMES, test_settings))
    await within_safety_timeout(double.started.wait())

    task.cancel()
    returned = await task

    assert returned is None
    assert task.cancelled() is False
    assert task.cancelling() == 0
    assert len(double.calls) == 1
    assert scheduler_records(caplog) == [
        (
            logging.INFO,
            "job cbk_macro fired: running cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, "
            "cbk_reserves",
        ),
    ]


async def test_run_job_runs_the_pipelines_on_the_schedule_trigger_and_logs_each_result(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    test_settings: TerminalSettings,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    results = [RATES_RESULT, INFLATION_RESULT]
    double = RunPipelinesDouble(caplog, results=results)
    monkeypatch.setattr(registry, "run_pipelines", double)
    names = ["cbk_rates_csv", "cbk_inflation"]

    returned = await scheduler.run_job("cbk_macro", names, test_settings)

    assert returned == results
    assert double.calls == [((names, test_settings), {"trigger": "schedule"})]
    [(call_args, _call_kwargs)] = double.calls
    assert call_args[0] is names
    assert call_args[1] is test_settings
    assert scheduler_records(caplog) == [
        (
            logging.INFO,
            "job cbk_macro: pipeline=cbk_rates_csv status=succeeded fetched=424 written=423 "
            "rejected=1 run_id=5d2c7a10-8f3e-4c1b-9a6d-2e4f6a8b0c11",
        ),
        (
            logging.INFO,
            "job cbk_macro: pipeline=cbk_inflation status=failed fetched=0 written=0 "
            "rejected=0 run_id=5d2c7a10-8f3e-4c1b-9a6d-2e4f6a8b0c12",
        ),
    ]


@pytest.mark.parametrize(
    ("error", "logged"),
    [
        pytest.param(
            ExceptionGroup(
                "2 of 5 ingestion runs raised instead of returning a result",
                [RuntimeError(f"run row: {SENTINEL}"), TypeError(f"constructor: {SENTINEL}")],
            ),
            "job cbk_macro: 2 ingestion run(s) raised instead of returning a result "
            "(RuntimeError, TypeError); the scheduler keeps running",
            id="exception-group",
        ),
        pytest.param(
            ValueError(f"database URL is not usable: {SENTINEL}"),
            "job cbk_macro could not run its pipelines: ValueError; the scheduler keeps running",
            id="ValueError",
        ),
        pytest.param(
            KeyError(f"unknown pipeline {SENTINEL}"),
            "job cbk_macro could not run its pipelines: KeyError; the scheduler keeps running",
            id="KeyError",
        ),
        pytest.param(
            TypeError(f"names: {SENTINEL}"),
            "job cbk_macro could not run its pipelines: TypeError; the scheduler keeps running",
            id="TypeError",
        ),
        pytest.param(
            RuntimeError(f"engine: {SENTINEL}"),
            "job cbk_macro could not run its pipelines: RuntimeError; the scheduler keeps running",
            id="RuntimeError",
        ),
    ],
)
async def test_run_job_logs_an_error_by_type_only_and_returns_no_results(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    test_settings: TerminalSettings,
    error: Exception,
    logged: str,
) -> None:
    """No run can stop the scheduler, and no exception text or traceback reaches a log line."""
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    double = RunPipelinesDouble(caplog, error=error)
    monkeypatch.setattr(registry, "run_pipelines", double)

    returned = await scheduler.run_job("cbk_macro", CBK_NAMES, test_settings)

    assert returned == []
    assert len(double.calls) == 1
    assert scheduler_records(caplog) == [(logging.ERROR, logged)]
    assert all(record.exc_info is None for record in caplog.records)
    assert SENTINEL not in caplog.text


async def test_run_job_lets_a_cancellation_propagate(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    test_settings: TerminalSettings,
) -> None:
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    cancellation = asyncio.CancelledError()
    monkeypatch.setattr(registry, "run_pipelines", RunPipelinesDouble(caplog, error=cancellation))

    with pytest.raises(asyncio.CancelledError) as excinfo:
        await scheduler.run_job("cbk_macro", CBK_NAMES, test_settings)

    assert excinfo.value is cancellation
    assert scheduler_records(caplog) == []


# --- Stop handlers ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "signum",
    [pytest.param(signal.SIGINT, id="SIGINT"), pytest.param(signal.SIGTERM, id="SIGTERM")],
)
def test_a_stop_signal_sets_the_stop_event_and_a_second_one_is_ignored(
    caplog: pytest.LogCaptureFixture,
    stop_handlers_before: dict[signal.Signals, SignalHandler],
    signum: signal.Signals,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    loop = asyncio.new_event_loop()
    try:
        stop = asyncio.Event()
        remove_handlers = scheduler._install_stop_handlers(loop, stop)
        try:
            assert all(signal.getsignal(sig) != stop_handlers_before[sig] for sig in STOP_SIGNALS)
            raise_handled_signal(signum, stop_handlers_before[signum])
            loop.run_until_complete(yield_until(stop.is_set, f"{signum.name} setting stop"))
            raise_handled_signal(signum, stop_handlers_before[signum])
            loop.run_until_complete(
                yield_until(
                    lambda: len(scheduler_records(caplog)) == 2, f"a second {signum.name} handled"
                )
            )
            assert stop.is_set()
        finally:
            remove_handlers()
        assert current_stop_handlers() == stop_handlers_before
    finally:
        loop.close()

    assert scheduler_records(caplog) == [
        (logging.INFO, f"received {signum.name}; stopping the scheduler"),
        (logging.INFO, f"received {signum.name}; already stopping, waiting for in-flight runs"),
    ]


@pytest.mark.parametrize(
    "signum",
    [pytest.param(signal.SIGINT, id="SIGINT"), pytest.param(signal.SIGTERM, id="SIGTERM")],
)
def test_stop_handlers_fall_back_to_signal_handlers_without_loop_support(
    caplog: pytest.LogCaptureFixture,
    stop_handlers_before: dict[signal.Signals, SignalHandler],
    signum: signal.Signals,
) -> None:
    """Where the loop cannot add signal handlers, ``signal.signal`` handlers set the event
    through ``call_soon_threadsafe``, and the remover reinstates the previous handlers. These
    are set to ``SIG_IGN`` first, which is neither a scheduler handler nor a standard
    disposition, so only an exact restoration, not a reset, leaves them in place."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    for sig in STOP_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)
    previous = current_stop_handlers()
    assert all(previous[sig] != stop_handlers_before[sig] for sig in STOP_SIGNALS)
    loop = LoopWithoutSignalHandlers()
    stop = asyncio.Event()

    remove_handlers = scheduler._install_stop_handlers(cast(asyncio.AbstractEventLoop, loop), stop)
    try:
        assert all(signal.getsignal(sig) != previous[sig] for sig in STOP_SIGNALS)
        raise_handled_signal(signum, previous[signum])
        assert stop.is_set()
        assert loop.threadsafe_calls == [(signum.name,)]
    finally:
        remove_handlers()

    assert current_stop_handlers() == previous
    assert scheduler_records(caplog) == [
        (logging.INFO, f"received {signum.name}; stopping the scheduler"),
    ]


# --- Waiting for in-flight jobs --------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_yields", "expected_logged", "expected_finished"),
    [
        pytest.param(
            5,
            [(logging.INFO, "waiting for 1 in-flight task(s) to finish recording")],
            ["recorded"],
            id="one-job-still-recording",
        ),
        pytest.param(None, [], [], id="no-job-in-flight"),
    ],
)
def test_wait_for_in_flight_jobs_returns_once_the_scheduler_and_its_jobs_are_done(
    caplog: pytest.LogCaptureFixture,
    job_yields: int | None,
    expected_logged: list[tuple[int, str]],
    expected_finished: list[str],
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    stopping = StoppingScheduler(polls_while_running=3)
    finished: list[str] = []

    async def job_recording_its_run(yields: int) -> None:
        for _ in range(yields):
            await asyncio.sleep(0)
        finished.append("recorded")

    async def main() -> bool:
        job = None if job_yields is None else asyncio.create_task(job_recording_its_run(job_yields))
        await scheduler._wait_for_in_flight_jobs(stopping)
        return job is None or job.done()

    all_done = run_on_fresh_loop(main)

    assert stopping.polls == 4
    assert all_done is True
    assert finished == expected_finished
    assert scheduler_records(caplog) == expected_logged


# --- _serve and run_forever ------------------------------------------------------------------


def test_serve_starts_logs_each_next_run_and_stops_on_sigterm(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    timeline: list[str],
    stop_handlers_before: dict[signal.Signals, SignalHandler],
) -> None:
    """A job running when SIGTERM arrives has finished recording before "scheduler stopped",
    and the stop handlers are gone once ``_serve`` returns. Both are read while the loop is
    still open: closing a Unix event loop removes its signal handlers itself."""
    served = ServedScheduler(timeline, stop_handlers_before)
    built_for: list[TerminalSettings] = []

    def build(settings: TerminalSettings) -> ServedScheduler:
        built_for.append(settings)
        return served

    async def main() -> tuple[dict[signal.Signals, SignalHandler], bool]:
        await within_safety_timeout(scheduler._serve(test_settings))
        return current_stop_handlers(), served.job is not None and served.job.done()

    monkeypatch.setattr(scheduler, "build_scheduler", build)

    handlers_once_served, job_done_once_served = run_on_fresh_loop(main)

    assert built_for == [test_settings]
    assert built_for[0] is test_settings
    assert timeline == [
        "start",
        "INFO scheduler started in timezone Africa/Nairobi; it must be the only scheduler process",
        "get_jobs",
        "INFO job nse_equities_eod: next run at 2026-10-27 07:30:00+03:00",
        "INFO job cbk_macro: next run at 2026-10-26 18:15:00+03:00",
        "INFO received SIGTERM; stopping the scheduler",
        "shutdown(wait=False)",
        "shutdown applied: job nse_equities_eod cancelled",
        "INFO waiting for 1 in-flight task(s) to finish recording",
        "job nse_equities_eod recorded its run",
        "INFO scheduler stopped",
    ]
    assert served.polls_after_shutdown == ServedScheduler.POLLS_WHILE_STOPPING + 1
    assert job_done_once_served is True
    assert handlers_once_served == stop_handlers_before
    assert current_stop_handlers() == stop_handlers_before


def test_serve_removes_the_stop_handlers_when_the_scheduler_cannot_start(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    timeline: list[str],
    stop_handlers_before: dict[signal.Signals, SignalHandler],
) -> None:
    """The handlers are read as the error leaves ``_serve``, while the loop is still open:
    closing a Unix event loop removes its signal handlers itself."""
    error = RuntimeError("scheduler could not start")
    served = ServedScheduler(timeline, stop_handlers_before, start_error=error)
    handlers_once_raised: list[dict[signal.Signals, SignalHandler]] = []

    async def main() -> None:
        try:
            await within_safety_timeout(scheduler._serve(test_settings))
        finally:
            handlers_once_raised.append(current_stop_handlers())

    monkeypatch.setattr(scheduler, "build_scheduler", lambda settings: served)

    with pytest.raises(RuntimeError) as excinfo:
        run_on_fresh_loop(main)

    assert excinfo.value is error
    assert served.handlers_at_start is not None
    assert all(served.handlers_at_start[sig] != stop_handlers_before[sig] for sig in STOP_SIGNALS)
    assert served.job is None
    assert timeline == ["start"]
    assert handlers_once_raised == [stop_handlers_before]
    assert current_stop_handlers() == stop_handlers_before


@pytest.mark.parametrize(
    "pass_settings",
    [pytest.param(False, id="default-settings"), pytest.param(True, id="given-settings")],
)
def test_run_forever_serves_the_settings_on_an_event_loop_of_its_own(
    monkeypatch: pytest.MonkeyPatch,
    make_settings: SettingsFactory,
    session_loop: asyncio.AbstractEventLoop,
    stop_handlers_before: dict[signal.Signals, SignalHandler],
    pass_settings: bool,
) -> None:
    """``run_forever`` runs in a thread of its own, so the current loop ``asyncio.run`` clears
    when it returns is that thread's, never the session loop of the thread running the tests.
    ``_serve`` is replaced, so no signal handler is installed."""
    default_settings = make_settings()
    given = make_settings(timezone="UTC") if pass_settings else None
    factory_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    served: list[tuple[TerminalSettings, asyncio.AbstractEventLoop, int]] = []

    def settings_factory(*args: object, **kwargs: object) -> TerminalSettings:
        factory_calls.append((args, kwargs))
        return default_settings

    async def serve(settings: TerminalSettings) -> None:
        served.append((settings, asyncio.get_running_loop(), threading.get_ident()))

    monkeypatch.setattr(scheduler, "TerminalSettings", settings_factory)
    monkeypatch.setattr(scheduler, "_serve", serve)

    worker = run_in_new_thread(lambda: scheduler.run_forever(given))

    [(served_settings, loop, served_on)] = served
    assert served_settings is (given if given is not None else default_settings)
    assert factory_calls == ([] if pass_settings else [((), {})])
    assert served_on == worker
    assert served_on != threading.get_ident()
    assert loop.is_closed()
    assert loop is not session_loop
    assert session_loop.is_closed() is False
    assert current_stop_handlers() == stop_handlers_before


# --- Import-time drift check -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("unregistered", "named"),
    [
        pytest.param(("cbk_bonds",), "cbk_bonds", id="cbk_bonds"),
        pytest.param(("nse_market_statistics",), "nse_market_statistics", id="nse"),
        pytest.param(("cbk_reserves", "cbk_fx"), "cbk_fx, cbk_reserves", id="two-sorted"),
    ],
)
def test_the_scheduler_module_refuses_to_import_with_an_unregistered_pipeline(
    monkeypatch: pytest.MonkeyPatch, unregistered: tuple[str, ...], named: str
) -> None:
    """The module source runs under a throwaway name, so the imported scheduler is untouched."""
    monkeypatch.setattr(
        registry,
        "PIPELINES",
        MappingProxyType(
            {name: spec for name, spec in registry.PIPELINES.items() if name not in unregistered}
        ),
    )
    spec = importlib.util.spec_from_file_location("scheduler_drift_probe", scheduler.__file__)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)

    with pytest.raises(RuntimeError) as excinfo:
        spec.loader.exec_module(probe)

    assert excinfo.value.args == (
        f"JOB_DEFINITIONS names pipelines missing from ingestion.registry.PIPELINES: {named}",
    )
    assert "scheduler_drift_probe" not in sys.modules
    assert sys.modules["opennse_terminal.ingestion.scheduler"] is scheduler
