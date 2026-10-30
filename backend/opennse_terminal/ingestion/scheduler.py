"""The scheduler process: runs the public-source ingestion pipelines on cron schedules.

Two jobs fire in the configured timezone (``TerminalSettings.timezone``, by default
Africa/Nairobi, UTC+3 with no daylight saving):

* ``nse_equities_eod``, Tuesday to Saturday at 07:30, runs ``nse_market_statistics``. It fires
  before the 09:30 open and collects the session the public NSE market-statistics page shows;
  the pipeline itself decides whether that session can be dated and written.
* ``cbk_macro``, Monday to Friday at 18:15, runs the five CBK pipelines of
  :data:`~opennse_terminal.ingestion.registry.CBK_PIPELINES` in sequence (rates CSV, inflation,
  FX, Treasury bonds, FX reserves), each with its own run record.

Every job uses ``coalesce=True``, ``max_instances=1`` and ``misfire_grace_time=3600``. Jobs live
in APScheduler's in-memory job store: nothing is persisted, and a fire time that passes while
the process is down is not caught up on restart.

``opennse-terminal scheduler`` calls :func:`run_forever`, which must run alone in a dedicated
process, never inside the API: started in each web worker, the scheduler would run every job
once per worker. Each run takes the transaction-scoped advisory lock of
:func:`~opennse_terminal.ingestion.base.execute_pipeline`, so a scheduled run and an overlapping
``opennse-terminal ingest`` run of the same pipeline never both proceed: the later one ends
``failed`` with "lock held".

This module adds no logging handlers; the CLI configures logging before calling
:func:`run_forever`.
"""

import asyncio
import logging
import signal
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import FrameType, MappingProxyType
from typing import Any, Final

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobEvent,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion import registry
from opennse_terminal.ingestion.base import RunResult

__all__ = [
    "JOB_DEFAULTS",
    "JOB_DEFINITIONS",
    "JobDefinition",
    "build_scheduler",
    "run_forever",
    "run_job",
]

logger = logging.getLogger("opennse_terminal.ingestion.scheduler")


@dataclass(frozen=True, slots=True)
class JobDefinition:
    """One cron job: the pipelines it runs, in order, and when it fires.

    Attributes:
        job_id: The APScheduler job id and name, used in every log line about the job.
        pipelines: Names registered in :data:`~opennse_terminal.ingestion.registry.PIPELINES`,
            run one after another.
        day_of_week: The cron day-of-week field, for example ``"tue-sat"``.
        hour: Hour of the day (0-23) in the scheduler's timezone.
        minute: Minute of the hour (0-59).
    """

    job_id: str
    pipelines: tuple[str, ...]
    day_of_week: str
    hour: int
    minute: int


JOB_DEFINITIONS: Final[tuple[JobDefinition, ...]] = (
    JobDefinition(
        job_id="nse_equities_eod",
        pipelines=("nse_market_statistics",),
        day_of_week="tue-sat",
        hour=7,
        minute=30,
    ),
    JobDefinition(
        job_id="cbk_macro",
        pipelines=registry.CBK_PIPELINES,
        day_of_week="mon-fri",
        hour=18,
        minute=15,
    ),
)
"""The scheduler's jobs, in the order they are added."""

JOB_DEFAULTS: Final[Mapping[str, Any]] = MappingProxyType(
    {"coalesce": True, "max_instances": 1, "misfire_grace_time": 3600}
)
"""Options of every job: overdue fire times collapse into one run, a job never overlaps itself,
and a run up to an hour late still starts. Set as the scheduler's ``job_defaults`` and passed to
each ``add_job`` as well, so no job depends on APScheduler's own defaults. Read-only."""

_SCHEDULED_PIPELINES: Final = frozenset(name for job in JOB_DEFINITIONS for name in job.pipelines)

# A job naming a pipeline the registry lacks would fail only when it fires (for
# nse_equities_eod, at 07:30 on the next trading morning), so the drift fails the import instead.
if not _SCHEDULED_PIPELINES <= registry.PIPELINES.keys():
    raise RuntimeError(
        "JOB_DEFINITIONS names pipelines missing from ingestion.registry.PIPELINES: "
        + ", ".join(sorted(_SCHEDULED_PIPELINES.difference(registry.PIPELINES)))
    )

_LOGGED_EVENTS: Final = EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED

_STOP_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM)


def build_scheduler(settings: TerminalSettings) -> AsyncIOScheduler:
    """Build an ``AsyncIOScheduler`` holding every job of :data:`JOB_DEFINITIONS`, unstarted.

    The scheduler and every cron trigger use ``settings.tzinfo``. Each job calls
    :func:`_run_scheduled_job` with its id, its pipeline names and ``settings``; it logs that the
    job fired and runs :func:`run_job`. :func:`_log_job_event` is attached as the listener for job
    completion, misses and errors. Building needs no running event loop: the scheduler binds to
    the running loop only when it is started.
    """
    timezone = settings.tzinfo
    scheduler = AsyncIOScheduler(timezone=timezone, job_defaults=dict(JOB_DEFAULTS))
    for job in JOB_DEFINITIONS:
        scheduler.add_job(
            _run_scheduled_job,
            CronTrigger(
                day_of_week=job.day_of_week,
                hour=job.hour,
                minute=job.minute,
                timezone=timezone,
            ),
            id=job.job_id,
            name=job.job_id,
            args=(job.job_id, job.pipelines, settings),
            replace_existing=True,
            **JOB_DEFAULTS,
        )
    scheduler.add_listener(_log_job_event, _LOGGED_EVENTS)
    return scheduler


def _log_job_event(event: JobEvent) -> None:
    """Log an APScheduler job event: finished at INFO, missed at WARNING, an error at ERROR.

    The "fired" line is logged by :func:`_run_scheduled_job` itself, because APScheduler declares
    a miss only after submitting the job, so a missed run is logged as missed only. A job
    cancelled while the scheduler stops returns ``None`` from :func:`_run_scheduled_job`; its
    completion is logged at WARNING, because it was stopped rather than having failed; the run it
    interrupted is recorded as ``execute_pipeline`` describes. Exceptions are named by type only,
    as everywhere in ingestion, so no exception text reaches a log line."""
    code = event.code
    if code == EVENT_JOB_EXECUTED:
        if event.retval is None:
            logger.warning(
                "job %s was cancelled while running (scheduled for %s)",
                event.job_id,
                event.scheduled_run_time,
            )
        else:
            logger.info(
                "job %s finished (scheduled for %s)", event.job_id, event.scheduled_run_time
            )
    elif code == EVENT_JOB_MISSED:
        logger.warning(
            "job %s missed its run scheduled for %s by more than %s seconds",
            event.job_id,
            event.scheduled_run_time,
            JOB_DEFAULTS["misfire_grace_time"],
        )
    elif code == EVENT_JOB_ERROR:
        logger.error(
            "job %s raised %s (scheduled for %s)",
            event.job_id,
            type(event.exception).__name__,
            event.scheduled_run_time,
        )


async def _run_scheduled_job(
    job_id: str, pipeline_names: Sequence[str], settings: TerminalSettings
) -> list[RunResult] | None:
    """The callable of every scheduled job: log that the job fired, then :func:`run_job`.

    APScheduler awaits this only once the run time is within ``misfire_grace_time``, so the INFO
    "fired" line is logged for runs that actually start and never for a missed one. Returns the
    results of :func:`run_job`, or ``None`` when the job is cancelled: only APScheduler's
    executor shutdown, or ``asyncio.run`` tearing down the loop, cancels a scheduled job's task,
    and by the time the cancellation reaches this function ``execute_pipeline`` has finished
    recording the run it interrupted (``failed`` with "cancelled" when the cancellation came
    before the commit; otherwise the committed status stands). Never raises, since
    :func:`run_job` raises only ``asyncio.CancelledError``, so APScheduler reports every run
    through ``EVENT_JOB_EXECUTED``.
    """
    logger.info("job %s fired: running %s", job_id, ", ".join(pipeline_names))
    try:
        return await run_job(job_id, pipeline_names, settings)
    except asyncio.CancelledError:
        # The cancellation is absorbed here: APScheduler's coroutine wrapper would swallow it
        # anyway, but only after logging it at ERROR with a full traceback. The task's pending
        # cancellation is withdrawn because it is suppressed, and None marks the job cancelled
        # for _log_job_event.
        task = asyncio.current_task()
        if task is not None:
            task.uncancel()
        return None


async def run_job(
    job_id: str, pipeline_names: Sequence[str], settings: TerminalSettings
) -> list[RunResult]:
    """Run a job's pipelines in sequence with trigger ``"schedule"`` and return their results.

    The runs go through :func:`~opennse_terminal.ingestion.registry.run_pipelines`: each pipeline
    gets its own run row, data transaction and advisory lock, a pipeline that fails ends with its
    status on its result, and the remaining pipelines still run. The job's runs share one
    per-host spacing, so their requests to one host stay ``scraper_min_interval_seconds`` apart
    across the runs. Each result is logged at INFO with the job id, pipeline, status and row
    counts.

    Only ``asyncio.CancelledError`` is raised, so no run can stop the scheduler. Any other
    exception from ``run_pipelines`` is logged at ERROR and an empty list returned: a run that
    raised instead of returning a result (an ``ExceptionGroup`` of them, each already logged by
    the registry), or an unusable ``settings.database_url``. Runs that did return are recorded in
    ``terminal_ingestion_runs`` and logged by ``execute_pipeline`` either way.
    """
    # Exceptions are named by type only, never with their text or traceback (no
    # logger.exception): as everywhere in ingestion, their messages can carry connection
    # details of the database URL.
    try:
        results = await registry.run_pipelines(pipeline_names, settings, trigger="schedule")
    except ExceptionGroup as group:
        logger.error(
            "job %s: %d ingestion run(s) raised instead of returning a result (%s); "
            "the scheduler keeps running",
            job_id,
            len(group.exceptions),
            ", ".join(type(exc).__name__ for exc in group.exceptions),
        )
        return []
    except Exception as exc:
        logger.error(
            "job %s could not run its pipelines: %s; the scheduler keeps running",
            job_id,
            type(exc).__name__,
        )
        return []
    for result in results:
        logger.info(
            "job %s: pipeline=%s status=%s fetched=%d written=%d rejected=%d run_id=%s",
            job_id,
            result.pipeline,
            result.status.value,
            result.rows_fetched,
            result.rows_written,
            result.rows_rejected,
            result.run_id,
        )
    return results


def _install_stop_handlers(
    loop: asyncio.AbstractEventLoop, stop: asyncio.Event
) -> Callable[[], None]:
    """Make SIGINT and SIGTERM set ``stop``, and return a function that removes the handlers.

    ``loop.add_signal_handler`` is used where the loop supports it (Unix). Where it raises
    ``NotImplementedError`` (the Windows event loops), ``signal.signal`` installs a handler that
    sets ``stop`` through ``loop.call_soon_threadsafe``, the one loop call that is safe from a
    signal handler, and the returned function restores the previous handlers. A signal received
    while already stopping is logged and otherwise ignored, so in-flight runs finish recording.
    """

    def request_stop(signame: str) -> None:
        if stop.is_set():
            logger.info("received %s; already stopping, waiting for in-flight runs", signame)
            return
        logger.info("received %s; stopping the scheduler", signame)
        stop.set()

    try:
        for sig in _STOP_SIGNALS:
            loop.add_signal_handler(sig, request_stop, sig.name)
    except NotImplementedError:
        previous = {sig: signal.getsignal(sig) for sig in _STOP_SIGNALS}

        def handle_signal(signum: int, _frame: FrameType | None) -> None:
            loop.call_soon_threadsafe(request_stop, signal.Signals(signum).name)

        for sig in _STOP_SIGNALS:
            signal.signal(sig, handle_signal)

        def restore_previous_handlers() -> None:
            for sig, handler in previous.items():
                # getsignal() gives None for a handler not installed from Python; signal()
                # cannot reinstate one, so that signal keeps the scheduler's handler.
                if handler is not None:
                    signal.signal(sig, handler)

        return restore_previous_handlers

    def remove_handlers() -> None:
        for sig in _STOP_SIGNALS:
            loop.remove_signal_handler(sig)

    return remove_handlers


async def _wait_for_in_flight_jobs(scheduler: AsyncIOScheduler) -> None:
    """After ``scheduler.shutdown()``, wait until every job that was running has finished.

    ``AsyncIOScheduler`` applies ``shutdown()`` on a later loop iteration; its executor then
    cancels each running job's task, and ``execute_pipeline`` finishes recording the run it
    interrupted, so no run row is left ``running``. A cancellation that came before the data
    commit rolls the run's data back and records it ``failed`` ("cancelled"); otherwise the
    committed status and ``rows_written`` stand. Either way the run's advisory lock is released.
    Each cancelled job then returns ``None`` from :func:`_run_scheduled_job` and is logged once,
    at WARNING, as cancelled while running.
    """
    # The tasks are awaited here instead of being left to asyncio.run's teardown, which cancels
    # every remaining task: that would include the tasks execute_pipeline runs its commit and
    # run-row writes in, which must run to completion or a run could stay "running".
    while scheduler.running:
        await asyncio.sleep(0)
    current = asyncio.current_task()
    in_flight = [task for task in asyncio.all_tasks() if task is not current]
    if in_flight:
        logger.info("waiting for %d in-flight task(s) to finish recording", len(in_flight))
        await asyncio.wait(in_flight)


async def _serve(settings: TerminalSettings) -> None:
    """Start the scheduler on the running loop, serve until SIGINT or SIGTERM, then stop it."""
    scheduler = build_scheduler(settings)
    stop = asyncio.Event()
    remove_stop_handlers = _install_stop_handlers(asyncio.get_running_loop(), stop)
    try:
        scheduler.start()
        try:
            logger.info(
                "scheduler started in timezone %s; it must be the only scheduler process",
                settings.timezone,
            )
            for job in scheduler.get_jobs():
                logger.info("job %s: next run at %s", job.id, job.next_run_time)
            await stop.wait()
        finally:
            scheduler.shutdown(wait=False)
            await _wait_for_in_flight_jobs(scheduler)
    finally:
        remove_stop_handlers()
    logger.info("scheduler stopped")


def run_forever(settings: TerminalSettings | None = None) -> None:
    """Run the scheduler in this process until SIGINT or SIGTERM, then return.

    ``settings`` defaults to a new ``TerminalSettings()``. The scheduler runs on an event loop of
    its own (``asyncio.run``), so this must be called from the main thread, where the stop
    signals are handled, and never from inside a running event loop. On a stop signal no further
    job fires, and jobs already running are cancelled. The run each one interrupted is finished,
    never left ``running``: ``failed`` ("cancelled") when the cancellation came before its data
    commit; otherwise the committed status stands. Each such job is logged once at WARNING, with
    no traceback, before this returns. Further signals while stopping are ignored.
    """
    asyncio.run(_serve(settings if settings is not None else TerminalSettings()))
