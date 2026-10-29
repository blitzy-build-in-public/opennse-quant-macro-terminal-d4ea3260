"""PDF parse slots: ``PdfParseLimiter`` accounting and its isolation between mounted hosts.

``PdfParseLimiter`` admits a parse only while fewer parses than the limit in force are active,
never waits for a slot, and keeps counting the parses it admitted when a request arrives with a
different ``pdf_max_concurrency``: a lower limit refuses new requests at once while the running
parses finish, and a higher one admits up to the new limit. Slots come back when the block exits,
also on an exception or a cancellation. A cancelled upload keeps its slot until its parse thread
has ended, because a worker thread cannot be stopped.

``TerminalUploadRoute`` gives every application that mounts ``terminal_router`` its own limiter
and its own once-only settings-fallback warning, so two hosts in one process never share upload
accounting. The endpoint tests mount ``terminal_router`` in separate ``FastAPI()`` apps whose
``get_terminal_settings`` override builds a new ``TerminalSettings`` on every call, as a host's
``lambda: TerminalSettings(...)`` does. ``filings_service.ingest_pdf`` is replaced by
:class:`HeldParses`, so no parse runs and no database is touched; the cancellation test replaces
it with :func:`_extract_only`, which runs the real ``filings_service._extract`` with
:class:`GatedParses` as the extractor, still without a database.

FastAPI reads the endpoint signatures at runtime, so this module does not use
``from __future__ import annotations``.
"""

import asyncio
import itertools
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, NoReturn

import anyio
import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request

from helpers.pdf_builder import build_pdf, text_page
from opennse_terminal.api import get_terminal_session, get_terminal_settings, terminal_router
from opennse_terminal.api.routes.ingest import PdfParseLimiter, get_pdf_parse_limiter
from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.files.pdf_line_items import PdfUnreadable
from opennse_terminal.services import filings_service

URL: Final = "/api/v1/terminal/ingest/pdf"
PDF: Final = build_pdf([text_page(["Parse limiter test filing"])])
"""A valid one-page text PDF; it passes the ``%PDF-`` check and is never parsed here."""
INGEST_LOGGER: Final = "opennse_terminal.api.routes.ingest"
SETTINGS_FALLBACK_WARNING: Final = "not TerminalSettings"
PARSE_GATE_SECONDS: Final = 30.0
"""How long a :class:`GatedParses` parse waits to be released before it gives up, so a failing
test never leaves a worker thread behind."""
TASK_CANCEL: Final = "task"
SCOPE_CANCEL: Final = "anyio_scope"


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


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
        return await self.client.post(URL, files={"file": ("filing.pdf", PDF, "application/pdf")})


async def _no_session() -> AsyncIterator[None]:
    """Session override: the stand-in service never touches a database."""
    yield None


@dataclass
class GatedParse:
    """One parse of :class:`GatedParses`: the gate its worker thread waits on, and its progress."""

    gate: threading.Event = field(default_factory=threading.Event)
    started: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)


class GatedParses:
    """Stand-in for ``filings_service.extract_line_items``, which the real
    ``filings_service._extract`` runs in a worker thread.

    Each parse blocks its thread until released and then raises ``PdfUnreadable``, so a released
    upload ends as the coded 422 ``pdf_unreadable``. Parses are numbered from 0 in arrival order;
    after :meth:`release_all`, later parses are released on arrival.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._parses: list[GatedParse] = []
        self._arrived = 0
        self._running = 0
        self._max_running = 0
        self._open = False
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

    def parse(self, index: int) -> GatedParse:
        """Parse ``index``, created ahead of its arrival when a test refers to it first."""
        with self._lock:
            while len(self._parses) <= index:
                created = GatedParse()
                if self._open:
                    created.gate.set()
                self._parses.append(created)
            return self._parses[index]

    def __call__(self, *_args: object, **_kwargs: object) -> NoReturn:
        with self._lock:
            index = self._arrived
            self._arrived += 1
            self._running += 1
            self._max_running = max(self._max_running, self._running)
        parse = self.parse(index)
        parse.started.set()
        try:
            if not parse.gate.wait(PARSE_GATE_SECONDS):
                raise TimeoutError(f"gated parse {index} was never released")
            raise PdfUnreadable
        finally:
            with self._lock:
                self._running -= 1
            parse.finished.set()

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


@pytest.fixture
async def gated_parses(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[GatedParses]:
    """:class:`GatedParses` installed as the extractor behind :func:`_extract_only`; every upload
    is released and awaited, and every parse thread has ended before the test is done."""
    gated = GatedParses()
    monkeypatch.setattr(filings_service, "extract_line_items", gated)
    monkeypatch.setattr(filings_service, "ingest_pdf", _extract_only)
    try:
        yield gated
    finally:
        gated.release_all()
        await asyncio.gather(*gated.tasks, return_exceptions=True)
        assert await gated.wait_all_finished(), "a gated parse thread did not end"


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


class ParseFailedError(Exception):
    """Raised inside a held slot to stand for a failing parse."""


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
    await inside.wait()
    if ending == "cancellation":
        async with limiter.slot(1) as admitted:
            assert not admitted
        task.cancel()
    expected: type[BaseException] = (
        ParseFailedError if ending == "exception" else asyncio.CancelledError
    )
    with pytest.raises(expected):
        await task

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
    request = Request({"type": "http", "method": "POST", "path": URL, "headers": []})
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
    first = parses.hold(host)
    parses.hold(host)
    await parses.wait_for_calls(2)

    host.limit = 1
    assert_busy(await host.upload())
    host.limit = 2
    assert_busy(await host.upload())
    assert parses.calls == 2

    parses.release(0)
    assert_coded(await first, 409, "released")
    parses.hold(host)
    await parses.wait_for_calls(3)
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
                        URL, content=b"", headers={"content-length": str(10**12)}
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

    A worker thread cannot be stopped, so the cancelled request stays in flight and keeps its
    slot while its parse runs, without spinning the event loop, and a new upload is refused with
    429. Once that parse ends the request ends: a task cancellation is re-raised, and a cancel
    scope's cancellation takes effect at the caller's next await and stays inside that scope.
    The next upload is then admitted. No more than ``limit`` parses ever run at once, and every
    other parse still ends as its coded 422.
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
