"""Tests for consecutive pipeline runs through the ingestion registry
(``opennse_terminal.ingestion.registry.run_pipelines``, the scheduler's batch entry point) and
the CLI's ``ingest`` loop (``opennse_terminal.cli._ingest``, which ``ingest cbk`` runs).

Each run of a batch opens a ``PoliteClient`` of its own, so its httpx client, cookie jar and
``robots.txt`` cache are the run's alone, and every run of the batch shares one ``HostSpacing``,
so requests to the CBK host start at least ``scraper_min_interval_seconds`` (2.0 s) apart
across the run boundaries as well as within each run, however quickly the previous run ended.

- **Runs.** ``cbk_inflation`` then ``cbk_bonds``, against a fresh seeded clone per test (the
  root conftest's ``seeded_database``, which marks the tests ``integration``); the registry and
  the CLI build their own engines from ``seeded_database.settings``. respx serves CBK's
  ``robots.txt`` and the recorded full-run fixtures ``fixtures/cbk/inflation_rates.html`` and
  ``fixtures/cbk/treasury_bonds.html`` at their real URLs with ``assert_all_mocked=True``, so
  nothing reaches the network. The first run either succeeds or ends at once, blocked by a
  ``robots.txt`` that disallows every URL and is served to the first ``robots.txt`` request
  only.
- **Clock.** ``ingestion.http.HostSpacing`` is replaced by a factory that builds the real class
  on a fake monotonic clock that only a fake sleep advances, so no test really waits; respx
  records every request start on that clock.

Expected values are literals: the run statuses and their CLI exit codes (``succeeded`` 0,
``blocked_by_robots`` 4) and the 2.0 s spacing of the hermetic test settings.
"""

import asyncio
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from itertools import pairwise
from typing import Any, Protocol

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal import cli
from opennse_terminal.ingestion import http as polite_http
from opennse_terminal.ingestion import registry
from opennse_terminal.ingestion.base import RunStatus
from opennse_terminal.ingestion.cbk import CBK_BASE_URL
from opennse_terminal.ingestion.cbk.bonds import TREASURY_BONDS_URL
from opennse_terminal.ingestion.cbk.inflation import INFLATION_URL
from opennse_terminal.ingestion.http import HostSpacing

NAMES = ("cbk_inflation", "cbk_bonds")
"""Two adjacent CBK pipelines, in run order; both read the one CBK host."""

ROBOTS_URL = f"{CBK_BASE_URL}/robots.txt"
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
"""CBK's own ``robots.txt``: every path allowed except the CMS admin area."""
DISALLOW_ALL = "User-agent: *\nDisallow: /\n"
"""A ``robots.txt`` that blocks every URL of the site."""

PAGES: Mapping[str, str] = {
    INFLATION_URL: "cbk/inflation_rates.html",
    TREASURY_BONDS_URL: "cbk/treasury_bonds.html",
}
"""The page each pipeline fetches, and the recorded full-run fixture served at it."""

SPACING_SECONDS = 2.0


class _Clone(Protocol):
    """The part of the root conftest's ``seeded_database`` handle these tests use."""

    @property
    def settings(self) -> Any: ...

    @property
    def engine(self) -> AsyncEngine: ...


class FakeClock:
    """Monotonic seconds, starting at 1000.0, that move only when advanced."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping; it still yields
    to the event loop once, as a real sleep would."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)
        await asyncio.sleep(0)


@dataclass(frozen=True, slots=True)
class FakeSpacing:
    """The fake clock and sleep every ``HostSpacing`` of a test runs on, and each one built."""

    clock: FakeClock
    sleep: FakeSleep
    built: list[HostSpacing]


@dataclass(frozen=True, slots=True)
class SentRequest:
    """One request that reached the mocked CBK site, and the fake time it started at."""

    at: float
    url: str


class RecordedSite:
    """The respx side effect of every mocked CBK URL: records each request's start on the fake
    clock, serves ``first_robots`` to the first ``robots.txt`` request and :data:`ALLOW_ALL` to
    every later one, and the recorded fixture bytes at each page URL."""

    def __init__(self, clock: FakeClock, pages: Mapping[str, bytes], first_robots: str) -> None:
        self.sent: list[SentRequest] = []
        self._clock = clock
        self._pages = pages
        self._first_robots = first_robots

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        robots_served = self.urls().count(ROBOTS_URL)
        self.sent.append(SentRequest(self._clock(), url))
        if url == ROBOTS_URL:
            return httpx.Response(200, text=ALLOW_ALL if robots_served else self._first_robots)
        return httpx.Response(
            200, content=self._pages[url], headers={"Content-Type": "text/html; charset=utf-8"}
        )

    def urls(self) -> list[str]:
        return [sent.url for sent in self.sent]

    def starts_by_host(self) -> dict[str, list[float]]:
        """Every request start, in order, grouped by the host requested."""
        starts: defaultdict[str, list[float]] = defaultdict(list)
        for sent in self.sent:
            starts[httpx.URL(sent.url).host].append(sent.at)
        return dict(starts)


@pytest.fixture
def fake_spacing(monkeypatch: pytest.MonkeyPatch) -> FakeSpacing:
    """Replace ``ingestion.http.HostSpacing`` with a factory that builds the real class on one
    fake clock and sleep, whatever its caller passes, and records every spacing it builds."""
    clock = FakeClock()
    sleep = FakeSleep(clock)
    built: list[HostSpacing] = []

    def build(**_caller_options: object) -> HostSpacing:
        spacing = HostSpacing(clock=clock, sleep=sleep)
        built.append(spacing)
        return spacing

    monkeypatch.setattr(polite_http, "HostSpacing", build)
    return FakeSpacing(clock=clock, sleep=sleep, built=built)


@pytest.mark.parametrize(
    ("first_robots", "first_status", "first_exit_code", "requested"),
    [
        pytest.param(
            ALLOW_ALL,
            RunStatus.SUCCEEDED,
            0,
            [ROBOTS_URL, INFLATION_URL, ROBOTS_URL, TREASURY_BONDS_URL],
            id="first-run-succeeds",
        ),
        pytest.param(
            DISALLOW_ALL,
            RunStatus.BLOCKED_BY_ROBOTS,
            4,
            [ROBOTS_URL, ROBOTS_URL, TREASURY_BONDS_URL],
            id="first-run-blocked-by-robots",
        ),
    ],
)
@pytest.mark.parametrize("entry_point", ["registry.run_pipelines", "cli._ingest"])
async def test_consecutive_runs_stay_spaced_per_host_across_the_run_boundary(
    entry_point: str,
    first_robots: str,
    first_status: RunStatus,
    first_exit_code: int,
    requested: list[str],
    seeded_database: _Clone,
    fake_spacing: FakeSpacing,
    fixture_bytes: Callable[[str], bytes],
) -> None:
    settings = seeded_database.settings
    site = RecordedSite(
        fake_spacing.clock, {url: fixture_bytes(path) for url, path in PAGES.items()}, first_robots
    )
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        for url in (ROBOTS_URL, *PAGES):
            router.get(url).mock(side_effect=site)
        if entry_point == "registry.run_pipelines":
            results = await registry.run_pipelines(NAMES, settings, trigger="schedule")
            assert [(result.pipeline, result.status) for result in results] == [
                ("cbk_inflation", first_status),
                ("cbk_bonds", RunStatus.SUCCEEDED),
            ]
        else:
            outcomes = await cli._ingest(NAMES, settings, trade_date=None, limit=None)
            assert outcomes == [("cbk_inflation", first_exit_code), ("cbk_bonds", 0)]

    # Each run read robots.txt itself, so the second run was judged by the file it fetched.
    assert site.urls() == requested
    assert site.urls().count(ROBOTS_URL) == len(NAMES)
    interval = settings.scraper_min_interval_seconds
    assert SPACING_SECONDS - 1e-9 <= interval <= SPACING_SECONDS + 1e-9
    for starts in site.starts_by_host().values():
        for earlier, later in pairwise(starts):
            assert later - earlier >= interval
    boundary = requested.index(ROBOTS_URL, 1)
    assert site.sent[boundary].at - site.sent[boundary - 1].at >= interval
    # The fake clock stands still between requests, so every request after the first, the next
    # run's robots.txt included, waited the full interval.
    assert len(fake_spacing.sleep.delays) == len(requested) - 1
    assert all(interval - 1e-9 <= delay <= interval + 1e-9 for delay in fake_spacing.sleep.delays)
    # One spacing for the whole batch, on the fake clock: no run built one of its own.
    assert len(fake_spacing.built) == 1
