"""Offload of the services' pure computation: ``compute(abandoned, func, /, *args, **kwargs)``.

``compute`` awaits ``abandoned`` (when given) on the event loop first. When it reports that the
client went away, ``RequestAbandonedError`` is raised and ``func`` never runs; otherwise ``func``
runs in a Starlette threadpool worker, so the event loop keeps running other tasks meanwhile,
and its result is returned unchanged. No database, network or filesystem is touched.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

import pytest

from opennse_terminal.services.offload import (
    CLIENT_CLOSED_REQUEST,
    RequestAbandonedError,
    compute,
)

LOGGER = "opennse_terminal.services.offload"


class AbandonProbe:
    """An abandon check that answers ``answer`` every time and counts how often it was awaited."""

    def __init__(self, answer: bool) -> None:
        self.answer = answer
        self.calls = 0

    async def __call__(self) -> bool:
        self.calls += 1
        return self.answer


def test_client_closed_request_is_status_499() -> None:
    """The status a route answers an abandoned request with: "client closed request"."""
    assert CLIENT_CLOSED_REQUEST == 499


async def test_func_runs_in_a_worker_thread_while_the_loop_keeps_running() -> None:
    """While ``func`` sleeps 0.3 s in another thread, a 10 ms ticker on the loop keeps ticking.

    Run inline, the sleep would hold the event loop and the ticker could not tick until it ended.
    """
    loop_thread = threading.get_ident()
    ticks = 0
    seen: list[tuple[int, int, int]] = []

    def blocking(value: int, *, scale: int) -> int:
        before = ticks
        time.sleep(0.3)
        seen.append((threading.get_ident(), before, ticks))
        return value * scale

    async def ticker(stop: asyncio.Event) -> None:
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    stop = asyncio.Event()
    ticking = asyncio.create_task(ticker(stop))
    try:
        result = await compute(None, blocking, 7, scale=6)
    finally:
        stop.set()
        await ticking

    assert result == 42
    ((worker_thread, before, after),) = seen
    assert worker_thread != loop_thread
    assert after > before


@pytest.mark.parametrize("abandoned", [None, False], ids=["no-check", "client-still-there"])
async def test_func_runs_with_its_arguments_unless_abandoned(abandoned: bool | None) -> None:
    """Without a check, or with one answering ``False``, ``func`` gets every argument as given.

    The check, when given, is awaited exactly once, before ``func`` runs.
    """
    probe = None if abandoned is None else AbandonProbe(abandoned)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def record(*args: object, **kwargs: object) -> str:
        calls.append((args, kwargs))
        return "done"

    # ``abandoned`` is positional-only, so func may take a keyword of that name itself.
    result = await compute(probe, record, 1, "two", abandoned="kept", window=20)

    assert result == "done"
    assert calls == [((1, "two"), {"abandoned": "kept", "window": 20})]
    if probe is not None:
        assert probe.calls == 1


async def test_abandoned_request_raises_before_func_runs(caplog: pytest.LogCaptureFixture) -> None:
    """A check answering ``True`` raises ``RequestAbandonedError``; ``func`` is never called.

    One DEBUG record names the function that was not started.
    """
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    calls: list[None] = []

    def never() -> None:
        calls.append(None)

    probe = AbandonProbe(True)

    with pytest.raises(RequestAbandonedError):
        await compute(probe, never)

    assert probe.calls == 1
    assert calls == []
    (record,) = [record for record in caplog.records if record.name == LOGGER]
    assert record.levelno == logging.DEBUG
    assert "never" in record.getMessage()


async def test_func_exception_propagates_unchanged() -> None:
    """An exception raised by ``func`` in the worker thread reaches the caller as raised."""

    def fail() -> None:
        raise ValueError("window must be between 5 and 250, got 4")

    with pytest.raises(ValueError, match="window must be between 5 and 250, got 4"):
        await compute(AbandonProbe(False), fail)
