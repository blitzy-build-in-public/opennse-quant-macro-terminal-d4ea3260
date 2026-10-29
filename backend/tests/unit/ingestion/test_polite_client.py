"""Tests for the polite HTTP client (``opennse_terminal.ingestion.http``).

Every request goes through ``httpx.MockTransport`` backed by a respx router created with
``assert_all_mocked=True``, so an unmocked URL fails the test and nothing reaches the network.
Spacing and backoff run on a fake monotonic clock that only the fake sleep advances, so no test
really waits. Expected values are literals from the politeness contract (the user agent, 2.0 s
per-host spacing, 3 retries, 2^n s plus up to 1 s of jitter capped at 60 s, 5 redirects and the
response size cap), compared with explicit bounds rather than float equality.
"""

import asyncio
import logging
import tracemalloc
import zlib
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from itertools import pairwise
from typing import Any

import httpx
import pytest
import respx

import opennse_terminal
from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion import http as polite_http
from opennse_terminal.ingestion.base import FetchFailed, RobotsDisallowed
from opennse_terminal.ingestion.http import (
    ATTEMPT_DEADLINE_READ_TIMEOUTS,
    BACKOFF_CAP_SECONDS,
    MAX_REDIRECTS,
    MAX_RESPONSE_BYTES,
    RETRY_STATUSES,
    ROBOTS_AGENT,
    FetchedResponse,
    HostSpacing,
    PoliteClient,
    build_user_agent,
    describe_url,
)

LOGGER = "opennse_terminal.ingestion.http"
USER_AGENT = "OpenNSE-Terminal/0.1.0 (+mailto:tests@example.invalid)"

CBK = "https://www.centralbank.go.ke"
NSE = "https://www.nse.co.ke"
OTHER = "https://other.example.net"
CBK_ROBOTS = f"{CBK}/robots.txt"
OTHER_ROBOTS = f"{OTHER}/robots.txt"
INFLATION_URL = f"{CBK}/inflation-rates/"
BONDS_URL = f"{CBK}/bills-bonds/treasury-bonds/"
RATES_PAGE_URL = f"{CBK}/statistics/interest-rates/"
CSV_URL = f"{CBK}/uploads/interest_rates/central_bank_rates.csv"
PDF_URL = f"{CBK}/uploads/weekly_bulletin/weekly_bulletin.pdf"

# The public NSE and CBK sites' own robots.txt: every path allowed except the CMS admin area.
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"
MIB = 1024 * 1024
MIB_CHUNK = b"\0" * MIB
ATTEMPTS_WITH_THREE_RETRIES = 4

SettingsFactory = Callable[..., TerminalSettings]


class FakeClock:
    """Monotonic seconds, starting at 1000.0, that move only when advanced."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping. It still yields
    to the event loop once, as a real sleep would, so concurrent coroutines interleave."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)
        await asyncio.sleep(0)

    def reset(self) -> None:
        """Forget the waits recorded so far, so a test can isolate the ones that follow."""
        self.delays.clear()


@dataclass(frozen=True, slots=True)
class SentRequest:
    """One request that reached the mock transport, and the fake time it started at."""

    at: float
    request: httpx.Request

    @property
    def url(self) -> str:
        return str(self.request.url)


class FakeNetwork:
    """A respx router behind ``httpx.MockTransport`` that logs every request it receives."""

    def __init__(self, clock: FakeClock) -> None:
        self.router = respx.MockRouter(assert_all_mocked=True, assert_all_called=False)
        self.sent: list[SentRequest] = []
        self._clock = clock

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        self.sent.append(SentRequest(self._clock(), request))
        return await self.router.async_handler(request)

    def robots(self, origin: str, body: str = ALLOW_ALL) -> respx.Route:
        return self.router.get(f"{origin}/robots.txt").respond(200, text=body)

    def urls(self) -> list[str]:
        return [sent.url for sent in self.sent]

    def starts(self, url: str | None = None) -> list[float]:
        return [sent.at for sent in self.sent if url is None or sent.url == url]


class ChunkedBody:
    """A streamed body of ``chunks`` 1 MiB chunks that counts how many the client pulled."""

    def __init__(self, chunks: int) -> None:
        self.chunks = chunks
        self.produced = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(self.chunks):
            self.produced += 1
            yield MIB_CHUNK


ClientFactory = Callable[..., PoliteClient]


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleep(clock: FakeClock) -> FakeSleep:
    return FakeSleep(clock)


@pytest.fixture
def network(clock: FakeClock) -> FakeNetwork:
    return FakeNetwork(clock)


@pytest.fixture
def make_client(
    network: FakeNetwork, clock: FakeClock, sleep: FakeSleep, make_settings: SettingsFactory
) -> ClientFactory:
    """Builds a ``PoliteClient`` on the fake network, clock and sleep, with the hermetic test
    settings; keyword arguments override settings fields."""

    def build(**overrides: Any) -> PoliteClient:
        return PoliteClient(
            make_settings(**overrides), transport=network.transport(), sleep=sleep, clock=clock
        )

    return build


def assert_spaced(starts: list[float], interval: float = 2.0) -> None:
    for earlier, later in pairwise(starts):
        assert later - earlier >= interval


def assert_backoff(delays: list[float]) -> None:
    """Waits before retries 1, 2 and 3: 2^n s plus up to 1 s of jitter."""
    bounds = [(2.0, 3.0), (4.0, 5.0), (8.0, 9.0)]
    assert len(delays) <= len(bounds)
    for delay, (low, high) in zip(delays, bounds, strict=False):
        assert low <= delay <= high


def warnings_logged(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == LOGGER and record.levelno == logging.WARNING
    ]


# --- Contract constants and identification ---------------------------------------------------


async def test_contract_constants() -> None:
    assert MAX_REDIRECTS == 5
    assert frozenset({429, 500, 502, 503, 504}) == RETRY_STATUSES
    assert 60.0 <= BACKOFF_CAP_SECONDS <= 60.0
    assert MAX_RESPONSE_BYTES == 25 * 1024 * 1024
    assert ATTEMPT_DEADLINE_READ_TIMEOUTS == 4
    assert ROBOTS_AGENT == "OpenNSE-Terminal"


async def test_user_agent_is_sent_on_robots_and_target_requests(
    network: FakeNetwork, make_client: ClientFactory, make_settings: SettingsFactory
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="<table></table>")
    client = make_client()
    expected = f"OpenNSE-Terminal/{opennse_terminal.__version__} (+mailto:tests@example.invalid)"
    assert client.user_agent == build_user_agent(make_settings()) == expected == USER_AGENT

    async with client as http:
        await http.get_text(INFLATION_URL)

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert [sent.request.headers["user-agent"] for sent in network.sent] == [USER_AGENT] * 2


async def test_user_agent_carries_the_configured_contact(make_client: ClientFactory) -> None:
    client = make_client(scraper_contact="data-desk@example.invalid")
    assert client.user_agent == "OpenNSE-Terminal/0.1.0 (+mailto:data-desk@example.invalid)"


async def test_requests_use_a_10_s_connect_and_the_configured_read_timeout(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="ok")
    async with make_client() as http:
        await http.fetch(INFLATION_URL)

    assert len(network.sent) == 2
    for sent in network.sent:
        timeout = sent.request.extensions["timeout"]
        assert 9.99 < timeout["connect"] < 10.01
        assert 29.99 < timeout["read"] < 30.01


async def test_cookies_set_by_a_source_are_never_sent_back(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.router.get(CBK_ROBOTS).respond(
        200, text=ALLOW_ALL, headers={"Set-Cookie": "robots_visit=1; Path=/"}
    )
    network.router.get(INFLATION_URL).respond(
        200, text="a", headers={"Set-Cookie": "session=abc; Path=/"}
    )
    network.router.get(BONDS_URL).respond(200, text="b")
    async with make_client() as http:
        await http.fetch(INFLATION_URL)
        await http.fetch(BONDS_URL)

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL, BONDS_URL]
    assert all("cookie" not in sent.request.headers for sent in network.sent)


async def test_cookie_set_while_a_body_streams_is_neither_stored_nor_sent_concurrently(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    mid_body = asyncio.Event()
    release = asyncio.Event()

    async def slow_body() -> AsyncIterator[bytes]:
        yield b"first half, "
        # Reached when the client asks for more: the headers, Set-Cookie included, are in.
        mid_body.set()
        await release.wait()
        yield b"second half"

    network.router.get(CSV_URL).mock(
        side_effect=lambda request: httpx.Response(
            200, headers={"Set-Cookie": "session=abc; Path=/"}, content=slow_body()
        )
    )
    network.router.get(INFLATION_URL).respond(200, text="fast")
    async with make_client() as http:
        slow = asyncio.create_task(http.get_bytes(CSV_URL))
        await asyncio.wait_for(mid_body.wait(), timeout=5.0)
        cookies_while_streaming = len(http._require_client().cookies.jar)
        await http.fetch(INFLATION_URL)
        release.set()
        body = await slow

    assert body == b"first half, second half"
    assert cookies_while_streaming == 0
    assert network.urls() == [CBK_ROBOTS, CSV_URL, INFLATION_URL]
    assert all("cookie" not in sent.request.headers for sent in network.sent)


# --- Per-host spacing ---------------------------------------------------------------------------


@pytest.mark.parametrize("interval", [2.0, 3.5])
async def test_requests_to_one_host_start_at_least_the_min_interval_apart(
    interval: float, network: FakeNetwork, make_client: ClientFactory, sleep: FakeSleep
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="a")
    network.router.get(BONDS_URL).respond(200, text="b")
    async with make_client(scraper_min_interval_seconds=interval) as http:
        await http.fetch(INFLATION_URL)
        await http.fetch(BONDS_URL)

    starts = network.starts()
    assert len(starts) == 3
    assert_spaced(starts, interval)
    assert len(sleep.delays) == 2
    assert all(0 < delay <= interval for delay in sleep.delays)


async def test_spacing_waits_only_the_rest_of_the_interval(
    network: FakeNetwork, make_client: ClientFactory, clock: FakeClock, sleep: FakeSleep
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="a")
    async with make_client(scraper_min_interval_seconds=2.0) as http:
        await http.ensure_allowed(INFLATION_URL)
        clock.advance(0.5)
        await http.get_text(INFLATION_URL)

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert len(sleep.delays) == 1
    assert 1.5 - 1e-9 <= sleep.delays[0] <= 1.5 + 1e-9
    robots_at, page_at = network.starts()
    assert page_at - robots_at >= 2.0


async def test_another_host_is_not_delayed_by_spacing(
    network: FakeNetwork, make_client: ClientFactory, sleep: FakeSleep
) -> None:
    network.robots(CBK)
    network.robots(OTHER)
    network.router.get(INFLATION_URL).respond(200, text="a")
    async with make_client() as http:
        await http.fetch(INFLATION_URL)
        waits_before = len(sleep.delays)
        await http.ensure_allowed(f"{OTHER}/files/report.csv")
        assert len(sleep.delays) == waits_before

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL, OTHER_ROBOTS]
    last_cbk, first_other = network.sent[1], network.sent[2]
    assert first_other.at - last_cbk.at < 2.0


async def test_no_wait_once_the_min_interval_has_already_passed(
    network: FakeNetwork, make_client: ClientFactory, clock: FakeClock, sleep: FakeSleep
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="a")
    async with make_client() as http:
        await http.ensure_allowed(INFLATION_URL)
        clock.advance(5.0)
        await http.fetch(INFLATION_URL)

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert sleep.delays == []


async def test_concurrent_fetches_share_one_robots_request_and_stay_spaced(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    async def robots_after_yielding(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)
        return httpx.Response(200, text=ALLOW_ALL)

    robots = network.router.get(CBK_ROBOTS).mock(side_effect=robots_after_yielding)
    network.router.get(INFLATION_URL).respond(200, text="inflation")
    network.router.get(BONDS_URL).respond(200, text="bonds")
    async with make_client() as http:
        texts = await asyncio.gather(http.get_text(INFLATION_URL), http.get_text(BONDS_URL))

    assert list(texts) == ["inflation", "bonds"]
    assert robots.call_count == 1
    starts = sorted(network.starts())
    assert len(starts) == 3
    assert_spaced(starts)


async def test_reopened_client_rereads_robots_but_keeps_spacing(
    network: FakeNetwork, make_client: ClientFactory, sleep: FakeSleep
) -> None:
    robots = network.robots(CBK)
    client = make_client()
    async with client as http:
        await http.ensure_allowed(INFLATION_URL)
    async with client as http:
        await http.ensure_allowed(INFLATION_URL)

    assert robots.call_count == 2
    assert_spaced(network.starts())
    assert len(sleep.delays) == 1


@pytest.mark.parametrize(
    ("first_robots", "first_status", "first_error", "first_urls"),
    [
        pytest.param(ALLOW_ALL, 200, None, [CBK_ROBOTS, INFLATION_URL], id="after-a-fetch"),
        pytest.param(
            "User-agent: *\nDisallow: /\n", 200, RobotsDisallowed, [CBK_ROBOTS], id="robots-blocked"
        ),
        pytest.param(
            ALLOW_ALL, 404, FetchFailed, [CBK_ROBOTS, INFLATION_URL], id="non-retryable-status"
        ),
    ],
)
async def test_runs_sharing_a_host_spacing_stay_spaced_across_the_run_boundary(
    first_robots: str,
    first_status: int,
    first_error: type[Exception] | None,
    first_urls: list[str],
    network: FakeNetwork,
    clock: FakeClock,
    sleep: FakeSleep,
    make_settings: SettingsFactory,
) -> None:
    robots = network.router.get(CBK_ROBOTS).mock(
        side_effect=[httpx.Response(200, text=first_robots), httpx.Response(200, text=ALLOW_ALL)]
    )
    network.router.get(INFLATION_URL).respond(first_status, text="inflation")
    network.router.get(BONDS_URL).respond(200, text="bonds")
    settings = make_settings()
    spacing = HostSpacing(clock=clock, sleep=sleep)

    def run_client() -> PoliteClient:
        """A new client, as each run opens: its own httpx client, cookies and robots cache."""
        return PoliteClient(settings, transport=network.transport(), sleep=sleep, spacing=spacing)

    async with run_client() as http:
        if first_error is None:
            await http.fetch(INFLATION_URL)
        else:
            with pytest.raises(first_error):
                await http.fetch(INFLATION_URL)
    # The first run ends, and its row is recorded, half a second before the next run starts.
    clock.advance(0.5)
    sleep.reset()
    async with run_client() as http:
        assert await http.get_text(BONDS_URL) == "bonds"

    assert network.urls() == [*first_urls, CBK_ROBOTS, BONDS_URL]
    assert robots.call_count == 2
    starts = network.starts()
    assert_spaced(starts)
    boundary = len(first_urls)
    assert starts[boundary] - starts[boundary - 1] >= 2.0
    # Only the rest of the interval is waited at the boundary, then a full one before the page.
    boundary_wait, page_wait = sleep.delays
    assert 1.5 - 1e-9 <= boundary_wait <= 1.5 + 1e-9
    assert 2.0 - 1e-9 <= page_wait <= 2.0 + 1e-9


def test_host_spacing_reused_on_a_later_event_loop_keeps_its_times() -> None:
    clock = FakeClock()
    sleep = FakeSleep(clock)
    spacing = HostSpacing(clock=clock, sleep=sleep)
    starts: list[float] = []

    async def turn() -> None:
        await spacing.wait_turn("www.centralbank.go.ke", 2.0)
        starts.append(clock())

    async def three_concurrent_turns() -> None:
        # The second turn sleeps holding the host's lock, so the third waits on it and binds the
        # lock to the running loop.
        await asyncio.gather(turn(), turn(), turn())

    for _ in range(2):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(three_concurrent_turns())
        finally:
            loop.close()

    assert len(starts) == 6
    assert_spaced(starts)
    # Every turn after the very first waited a full interval, the first on the second loop too.
    assert len(sleep.delays) == 5
    assert all(2.0 - 1e-9 <= delay <= 2.0 + 1e-9 for delay in sleep.delays)


# --- Retries and backoff ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        pytest.param(httpx.Response(429), "http_429", id="429"),
        pytest.param(httpx.Response(500), "http_500", id="500"),
        pytest.param(httpx.Response(502), "http_502", id="502"),
        pytest.param(httpx.Response(503), "http_503", id="503"),
        pytest.param(httpx.Response(504), "http_504", id="504"),
        pytest.param(httpx.ConnectTimeout, "ConnectTimeout after 4 attempts", id="connect-timeout"),
        pytest.param(httpx.ReadTimeout, "ReadTimeout after 4 attempts", id="read-timeout"),
        pytest.param(httpx.ConnectError, "ConnectError after 4 attempts", id="connect-error"),
        pytest.param(
            httpx.RemoteProtocolError, "RemoteProtocolError after 4 attempts", id="remote-protocol"
        ),
    ],
)
async def test_transient_failure_is_retried_three_times_then_fails(
    failure: httpx.Response | type[httpx.TransportError],
    reason: str,
    network: FakeNetwork,
    make_client: ClientFactory,
    clock: FakeClock,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.robots(CBK)
    page = network.router.get(INFLATION_URL).mock(
        side_effect=[failure] * ATTEMPTS_WITH_THREE_RETRIES
    )
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        # Default 2.0 s spacing: with robots.txt read and its spacing long past, every wait that
        # follows is a backoff, each of which already outlasts the spacing.
        async with make_client() as http:
            await http.ensure_allowed(INFLATION_URL)
            clock.advance(10.0)
            sleep.reset()
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(INFLATION_URL)

    assert caught.value.reason == reason
    assert caught.value.url == INFLATION_URL
    assert page.call_count == ATTEMPTS_WITH_THREE_RETRIES
    assert len(sleep.delays) == 3
    assert_backoff(sleep.delays)
    retries = warnings_logged(caplog)
    assert len(retries) == 3
    assert all(f"attempt {n} of 4" in message for n, message in enumerate(retries, start=1))


async def test_success_on_the_third_attempt_returns_the_body(
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.robots(CBK)
    page = network.router.get(CSV_URL).mock(
        side_effect=[
            httpx.ConnectTimeout,
            httpx.Response(503),
            httpx.Response(200, content=b"YEAR,MONTH\n"),
        ]
    )
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            body = await http.get_bytes(CSV_URL)

    assert body == b"YEAR,MONTH\n"
    assert page.call_count == 3
    assert len(sleep.delays) == 2
    assert_backoff(sleep.delays)
    first, second = warnings_logged(caplog)
    assert "attempt 1 of 4" in first and "ConnectTimeout" in first
    assert "attempt 2 of 4" in second and "HTTP 503" in second


@pytest.mark.parametrize("status", [403, 404, 410, 501])
async def test_non_retryable_status_fails_at_once_without_reading_the_body(
    status: int, network: FakeNetwork, make_client: ClientFactory, sleep: FakeSleep
) -> None:
    network.robots(CBK)
    body = ChunkedBody(chunks=3)
    page = network.router.get(INFLATION_URL).mock(
        side_effect=lambda request: httpx.Response(status, content=body)
    )
    async with make_client(scraper_min_interval_seconds=0.0) as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(INFLATION_URL)

    assert caught.value.reason == f"http_{status}"
    assert page.call_count == 1
    assert sleep.delays == []
    assert body.produced == 0


@pytest.mark.parametrize(("retries", "calls"), [(0, 1), (1, 2)])
async def test_configured_retry_count_bounds_the_attempts(
    retries: int, calls: int, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    page = network.router.get(INFLATION_URL).respond(503)
    async with make_client(scraper_max_retries=retries, scraper_min_interval_seconds=0.0) as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(INFLATION_URL)

    assert caught.value.reason == "http_503"
    assert page.call_count == calls


@pytest.mark.parametrize(
    "error", [httpx.UnsupportedProtocol, httpx.ProxyError, httpx.LocalProtocolError]
)
async def test_non_transient_transport_error_fails_without_retry(
    error: type[httpx.HTTPError],
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
) -> None:
    network.robots(CBK)
    page = network.router.get(INFLATION_URL).mock(side_effect=error)
    async with make_client(scraper_min_interval_seconds=0.0) as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(INFLATION_URL)

    assert caught.value.reason == error.__name__
    assert page.call_count == 1
    assert sleep.delays == []


@pytest.mark.parametrize(
    ("retry_after", "low", "high"),
    [
        pytest.param("7", 7.0, 8.0, id="seconds"),
        pytest.param(" 7 ", 7.0, 8.0, id="seconds-with-whitespace"),
        pytest.param("120", 60.0, 60.0, id="seconds-capped-at-60"),
        pytest.param("9" * 400, 60.0, 60.0, id="400-digit-seconds-capped-at-60"),
        pytest.param("9" * 5000, 60.0, 60.0, id="5000-digit-seconds-capped-at-60"),
        pytest.param("0007", 7.0, 8.0, id="seconds-with-leading-zeros"),
        pytest.param("0", 0.0, 0.0, id="zero"),
        pytest.param("Wed, 21 Oct 2015 07:28:00 GMT", 0.0, 0.0, id="http-date-in-the-past"),
        pytest.param("Wed, 21 Oct 2015 07:28:00 -0000", 0.0, 0.0, id="http-date-naive-zone"),
        pytest.param(
            f"Wed, 21 Oct {'9' * 30} 07:28:00 GMT", 2.0, 3.0, id="http-date-overflow-uses-backoff"
        ),
        pytest.param("soon", 2.0, 3.0, id="malformed-uses-backoff"),
        pytest.param("-5", 2.0, 3.0, id="negative-uses-backoff"),
        pytest.param("", 2.0, 3.0, id="empty-uses-backoff"),
    ],
)
async def test_retry_after_sets_the_wait_before_the_retry(
    retry_after: str,
    low: float,
    high: float,
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
) -> None:
    network.robots(CBK)
    page = network.router.get(INFLATION_URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": retry_after}),
            httpx.Response(200, text="ok"),
        ]
    )
    async with make_client(scraper_min_interval_seconds=0.0) as http:
        assert await http.get_text(INFLATION_URL) == "ok"

    assert page.call_count == 2
    assert len(sleep.delays) == 1
    assert low <= sleep.delays[0] <= high


@pytest.mark.parametrize(
    ("ahead", "low", "high"),
    [
        pytest.param(timedelta(seconds=30), 25.0, 30.0, id="in-30-seconds"),
        pytest.param(timedelta(hours=1), 60.0, 60.0, id="in-an-hour-capped-at-60"),
    ],
)
async def test_retry_after_http_date_waits_until_that_time(
    ahead: timedelta,
    low: float,
    high: float,
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
) -> None:
    network.robots(CBK)
    retry_at = format_datetime(datetime.now(UTC) + ahead, usegmt=True)
    network.router.get(INFLATION_URL).mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": retry_at}),
            httpx.Response(200, text="ok"),
        ]
    )
    async with make_client(scraper_min_interval_seconds=0.0) as http:
        assert await http.get_text(INFLATION_URL) == "ok"

    assert len(sleep.delays) == 1
    assert low <= sleep.delays[0] <= high


async def test_retry_is_spaced_even_when_retry_after_is_shorter(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": "0"}),
            httpx.Response(200, text="ok"),
        ]
    )
    async with make_client() as http:
        await http.fetch(INFLATION_URL)

    attempts = network.starts(INFLATION_URL)
    assert len(attempts) == 2
    assert_spaced(network.starts())


# The attempt deadline runs on the event loop's real clock, so these tests shrink it to 50 ms and
# stall the mock source on real sleeps far longer than that.
TEST_DEADLINE_SECONDS = 0.05
STALL_SECONDS = 5.0


@pytest.fixture
def deadlines_computed(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Each attempt deadline the client computes, while every attempt really gets 50 ms."""
    computed: list[float] = []
    compute = polite_http._attempt_deadline_seconds

    def shrunk(read_timeout: float) -> float:
        computed.append(compute(read_timeout))
        return TEST_DEADLINE_SECONDS

    monkeypatch.setattr(polite_http, "_attempt_deadline_seconds", shrunk)
    return computed


@pytest.mark.parametrize("stall", ["before-headers", "mid-body"])
@pytest.mark.parametrize(
    ("stalled_url", "sent"),
    [
        pytest.param(
            INFLATION_URL, [CBK_ROBOTS] + [INFLATION_URL] * ATTEMPTS_WITH_THREE_RETRIES, id="page"
        ),
        pytest.param(CBK_ROBOTS, [CBK_ROBOTS] * ATTEMPTS_WITH_THREE_RETRIES, id="robots-txt"),
    ],
)
async def test_attempt_running_past_its_deadline_is_retried_then_fails(
    stall: str,
    stalled_url: str,
    sent: list[str],
    deadlines_computed: list[float],
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def trickle() -> AsyncIterator[bytes]:
        yield b"a byte inside the read timeout"
        await asyncio.sleep(STALL_SECONDS)
        yield b"never reached"

    async def respond(request: httpx.Request) -> httpx.Response:
        if stall == "before-headers":
            await asyncio.sleep(STALL_SECONDS)
        return httpx.Response(200, content=trickle())

    network.router.get(stalled_url).mock(side_effect=respond)
    if stalled_url != CBK_ROBOTS:
        network.robots(CBK)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(INFLATION_URL)

    assert caught.value.reason == "DeadlineExceeded after 4 attempts"
    assert caught.value.url == stalled_url
    assert network.urls() == sent
    # 10 s to connect plus four 30 s read timeouts, at the default settings.
    assert deadlines_computed
    assert all(129.99 < deadline < 130.01 for deadline in deadlines_computed)
    assert len(sleep.delays) == 3
    assert_backoff(sleep.delays)
    retries = warnings_logged(caplog)
    assert len(retries) == 3
    for n, message in enumerate(retries, start=1):
        assert f"attempt {n} of 4 failed (DeadlineExceeded)" in message


async def test_cancelling_a_fetch_mid_attempt_propagates_without_retry(
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.robots(CBK)
    started = asyncio.Event()

    async def stall(request: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.sleep(STALL_SECONDS)
        return httpx.Response(200, text="never served")

    network.router.get(INFLATION_URL).mock(side_effect=stall)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            fetching = asyncio.create_task(http.fetch(INFLATION_URL))
            await asyncio.wait_for(started.wait(), timeout=STALL_SECONDS)
            fetching.cancel()
            with pytest.raises(asyncio.CancelledError):
                await fetching

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert sleep.delays == []
    assert warnings_logged(caplog) == []


# --- robots.txt -------------------------------------------------------------------------------


async def test_robots_txt_is_fetched_once_per_origin_per_client(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    robots = network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="a")
    network.router.get(BONDS_URL).respond(200, text="b")
    async with make_client() as http:
        await http.fetch(INFLATION_URL)
        await http.fetch(BONDS_URL)
        await http.ensure_allowed(RATES_PAGE_URL)
    assert robots.call_count == 1

    async with make_client() as http:
        await http.fetch(INFLATION_URL)
    assert robots.call_count == 2


async def test_ensure_allowed_requests_only_robots_txt(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(NSE, "User-agent: *\nDisallow: /private/\n")
    async with make_client() as http:
        await http.ensure_allowed(f"{NSE}/market-statistics/")
        with pytest.raises(RobotsDisallowed) as caught:
            await http.ensure_allowed(f"{NSE}/private/report/")

    assert caught.value.reason == "disallowed"
    assert network.urls() == [f"{NSE}/robots.txt"]


@pytest.mark.parametrize(
    ("robots_body", "url"),
    [
        pytest.param("User-agent: *\nDisallow: /statistics/\n", RATES_PAGE_URL, id="every-agent"),
        pytest.param(
            "User-agent: OpenNSE-Terminal\nDisallow: /\n", RATES_PAGE_URL, id="product-token"
        ),
        pytest.param(
            "User-agent: OpenNSE-Terminal\nDisallow: /statistics/\n\nUser-agent: *\nAllow: /\n",
            RATES_PAGE_URL,
            id="product-group-over-wildcard",
        ),
        # RFC 9309 matching: the longest rule wins wherever it stands, "*" and "$" are patterns.
        pytest.param(
            "User-agent: *\nAllow: /\nDisallow: /blocked\n",
            f"{CBK}/blocked",
            id="longer-disallow-after-allow-all",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /*.pdf$\n", f"{CBK}/ok.pdf", id="wildcard-and-end-anchor"
        ),
        pytest.param("User-agent: *\nDisallow: /bl*ed\n", f"{CBK}/blocked", id="inner-wildcard"),
        pytest.param(
            "User-agent: *\nDisallow: /*a*b*c$\n", f"{CBK}/xaybzc", id="several-wildcards"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /search?q=\n", f"{CBK}/search?q=kes", id="query-string-rule"
        ),
        # A rule's path is matched as its line writes it, not as robotparser's RuleLine rewrites
        # it ("/*?" into "/*", "/search?" into "/search"); an escaped "*" or "$" is literal.
        pytest.param(
            "User-agent: *\nDisallow: /*?\n", f"{CBK}/page?x=1", id="wildcard-then-question-mark"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /search?\n", f"{CBK}/search?q=1", id="trailing-question-mark"
        ),
        pytest.param("User-agent: *\nDisallow: /a;b\n", f"{CBK}/a;b", id="semicolon-in-path"),
        pytest.param(
            "User-agent: *\nDisallow: /a%2Ab\n", f"{CBK}/a*b", id="escaped-asterisk-literal"
        ),
        pytest.param("User-agent: *\nDisallow: /a%24\n", f"{CBK}/a$", id="escaped-dollar-literal"),
        # Group selection: the product token is a value's leading letters, "_" and "-", in any
        # case, and every group naming it (or, failing that, every "*" group) applies.
        pytest.param(
            "User-agent: OpenNSE-Terminal/1.0\nDisallow: /\n", RATES_PAGE_URL, id="token-version"
        ),
        pytest.param(
            "User-agent: opennse-terminal\nDisallow: /\n", RATES_PAGE_URL, id="token-lowercase"
        ),
        pytest.param(
            "User-agent: OpenNSE-Terminal\nDisallow: /a/\n\n"
            "User-agent: opennse-terminal\nDisallow: /statistics/\n",
            RATES_PAGE_URL,
            id="product-groups-combined",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a/\n\nUser-agent: *\nDisallow: /statistics/\n",
            RATES_PAGE_URL,
            id="wildcard-groups-combined",
        ),
        pytest.param(
            "User-agent: *\nAllow: /\n\n"
            "User-agent: OpenNSE-Terminal\nUser-agent: *\nDisallow: /statistics/\n",
            RATES_PAGE_URL,
            id="later-group-naming-product-and-wildcard",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /\n\nUser-agent: SomeOtherBot\n",
            RATES_PAGE_URL,
            id="rule-less-other-agent-group-at-end",
        ),
        pytest.param(
            "User-agent: *\n\nDisallow: /private\n", f"{CBK}/private", id="empty-line-in-group"
        ),
        pytest.param(
            "\ufeffUser-agent: *\nDisallow: /statistics/\n", RATES_PAGE_URL, id="byte-order-mark"
        ),
        # Lines end at CR, LF or CRLF alone (RFC 9309 §2.2's EOL), so no other character that
        # str.splitlines breaks at can cut a longer Allow down to one that wins the tie.
        pytest.param(
            "User-agent: *\r\nDisallow: /private\r\n", f"{CBK}/private", id="crlf-line-endings"
        ),
        pytest.param(
            "User-agent: *\rDisallow: /private\r", f"{CBK}/private", id="lone-cr-line-endings"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\x85/public\n",
            f"{CBK}/private",
            id="next-line-character-inside-a-line",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\u2028/public\n",
            f"{CBK}/private",
            id="line-separator-inside-a-line",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\x0c/public\n",
            f"{CBK}/private",
            id="form-feed-inside-a-line",
        ),
        # A value is trimmed of space and tab alone (RFC 9309 §2.2's WS): other whitespace ending
        # it stays in the path, so it cannot cut a longer Allow down to one that wins the tie.
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\x85\n",
            f"{CBK}/private",
            id="next-line-character-ending-a-value",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\u2028\n",
            f"{CBK}/private",
            id="line-separator-ending-a-value",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\x0c\n",
            f"{CBK}/private",
            id="form-feed-ending-a-value",
        ),
        pytest.param(
            "User-agent: *\nDisallow:\t/private \t\n", f"{CBK}/private", id="space-and-tab-trimmed"
        ),
        # Rules and URLs compare in one canonical form (RFC 9309 §2.2.2), whichever side is
        # escaped: non-ASCII characters as their UTF-8 escapes, hex digits uppercase, and the
        # escapes of unreserved characters decoded.
        pytest.param(
            "User-agent: *\nDisallow: /caf%C3%A9\n", f"{CBK}/café", id="escaped-rule-raw-url"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /café\n", f"{CBK}/caf%C3%A9", id="raw-rule-escaped-url"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /caf%c3%a9\n", f"{CBK}/caf%C3%A9", id="lowercase-hex-rule"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /caf%C3%A9\n", f"{CBK}/caf%c3%a9", id="lowercase-hex-url"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /baz\n", f"{CBK}/%62%61%7a", id="escaped-unreserved-url"
        ),
        # Every other escape stays encoded, matching itself and nothing else: an escaped "%" stays
        # literal, an escaped "#" is part of the path rather than the start of a fragment, and an
        # escaped delimiter is not the delimiter. Characters outside the unreserved and reserved
        # sets, a "%" that starts no escape and a "$" that is not final compare escaped.
        pytest.param(
            "User-agent: *\nDisallow: /a%2541\n", f"{CBK}/a%2541", id="escaped-percent-literal"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a%23b\n", f"{CBK}/a%23b", id="escaped-hash-in-path"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a%3Fb\n", f"{CBK}/a%3Fb", id="escaped-question-mark"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a%zz\n", f"{CBK}/a%25zz", id="percent-starting-no-escape"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a|b\n", f"{CBK}/a%7Cb", id="unsafe-character-escaped"
        ),
        pytest.param("User-agent: *\nDisallow: /a$b\n", f"{CBK}/a$b", id="inner-dollar-literal"),
        # A longer Allow written with an escaped delimiter, or with an escaped octet that is not
        # UTF-8, does not reach a URL with the delimiter or with another octet.
        pytest.param(
            "User-agent: *\nDisallow: /private/\nAllow: /private%2F\n",
            f"{CBK}/private/secret",
            id="escaped-slash-allow-not-the-slash",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /blocked\nAllow: /blocked%2Fok\n",
            f"{CBK}/blocked/ok",
            id="longer-escaped-slash-allow-not-the-slash",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private%3Fpublic\n",
            f"{CBK}/private?public=1",
            id="escaped-question-mark-allow-not-the-query",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private%FF\n",
            f"{CBK}/private%FE",
            id="non-utf8-escapes-distinct",
        ),
        # RFC 9309 §2.2.2, Figure 4: each row's path, as a rule, blocks its "path to match".
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar?baz=quz\n",
            f"{CBK}/foo/bar?baz=quz",
            id="rfc9309-figure-4-row-1",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar?baz=https%3A%2F%2Ffoo.bar\n",
            f"{CBK}/foo/bar?baz=https%3A%2F%2Ffoo.bar",
            id="rfc9309-figure-4-row-2",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar/ツ\n",
            f"{CBK}/foo/bar/%E3%83%84",
            id="rfc9309-figure-4-row-3",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar/%E3%83%84\n",
            f"{CBK}/foo/bar/%E3%83%84",
            id="rfc9309-figure-4-row-4",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar/%62%61%7A\n",
            f"{CBK}/foo/bar/baz",
            id="rfc9309-figure-4-row-5",
        ),
        # Delimiters a rule writes literally match the same delimiters in the URL.
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar?baz=https://foo.bar\n",
            f"{CBK}/foo/bar?baz=https://foo.bar",
            id="literal-delimiters-in-query",
        ),
    ],
)
async def test_url_robots_txt_disallows_raises_and_is_never_requested(
    robots_body: str,
    url: str,
    network: FakeNetwork,
    make_client: ClientFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.robots(CBK, robots_body)
    page = network.router.get(url).respond(200, text="never served")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client() as http:
            with pytest.raises(RobotsDisallowed) as caught:
                await http.fetch(url)

    assert caught.value.reason == "disallowed"
    assert caught.value.url == describe_url(url)
    assert page.call_count == 0
    assert network.urls() == [CBK_ROBOTS]
    assert any("disallows" in message for message in warnings_logged(caplog))


@pytest.mark.parametrize(
    ("robots_body", "url"),
    [
        pytest.param(
            "User-agent: SomeOtherBot\nDisallow: /\n", RATES_PAGE_URL, id="other-agent-only"
        ),
        pytest.param("User-agent: *\nDisallow: /private/\n", RATES_PAGE_URL, id="other-path-only"),
        pytest.param("", RATES_PAGE_URL, id="empty-file"),
        pytest.param(
            "User-agent: *\nDisallow: /\nAllow: /ok\n", f"{CBK}/ok", id="longer-allow-after-all"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /page\nAllow: /page\n", f"{CBK}/page", id="tie-goes-to-allow"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /*.pdf$\n", f"{CBK}/a.pdf?x=1", id="anchor-unmet-by-query"
        ),
        pytest.param("User-agent: *\nDisallow: /*.pdf$\n", f"{CBK}/a.pdfx", id="anchor-unmet"),
        pytest.param(
            "User-agent: *\nDisallow: /search?q=\n", f"{CBK}/search?page=2", id="other-query"
        ),
        # Rule paths as written: neither a trailing "?" nor a lone "?", ";" or "//" widens a rule
        # (robotparser's RuleLine would make each match every URL), and an escaped "*" or "$" is
        # neither a wildcard nor an anchor.
        pytest.param(
            "User-agent: *\nDisallow: /*?\n", f"{CBK}/page", id="wildcard-then-question-mark-unmet"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /search?\n",
            f"{CBK}/searchengine",
            id="trailing-question-mark-unmet",
        ),
        pytest.param("User-agent: *\nDisallow: ?\n", f"{CBK}/x", id="question-mark-only"),
        pytest.param("User-agent: *\nDisallow: ;\n", f"{CBK}/x", id="semicolon-only"),
        pytest.param("User-agent: *\nDisallow: //\n", f"{CBK}/x", id="double-slash-only"),
        pytest.param(
            "User-agent: *\nDisallow: /a%2Ab\n", f"{CBK}/axb", id="escaped-asterisk-not-wildcard"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a%24\n", f"{CBK}/a", id="escaped-dollar-not-anchor"
        ),
        # Only the escapes of unreserved characters are decoded (RFC 9309 §2.2.2): a rule with an
        # escaped delimiter does not reach the URL with the delimiter, nor the reverse, and the
        # escape of an octet that is not UTF-8 reaches that octet alone. A path weighs the same
        # however it is escaped, so the Allow wins the tie.
        pytest.param(
            "User-agent: *\nDisallow: /a%3Fb\n",
            f"{CBK}/a?b",
            id="escaped-question-mark-not-the-delimiter",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /a%2Fb\n", f"{CBK}/a/b", id="escaped-slash-not-the-slash"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private%FF\n",
            f"{CBK}/private%FF",
            id="non-utf8-escape-allow-applies",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar?baz=https%3A%2F%2Ffoo.bar\n",
            f"{CBK}/foo/bar?baz=https://foo.bar",
            id="escaped-query-not-literal-query",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /foo/bar?baz=https://foo.bar\n",
            f"{CBK}/foo/bar?baz=https%3A%2F%2Ffoo.bar",
            id="literal-query-not-escaped-query",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /caf%c3%a9\nAllow: /café\n",
            f"{CBK}/caf%C3%A9",
            id="tie-across-spellings-goes-to-allow",
        ),
        # A character str.splitlines breaks at, but RFC 9309 §2.2's EOL does not, starts no line:
        # it neither begins another rule nor leaves the path it stands in.
        pytest.param(
            "User-agent: *\nDisallow: /private\u2029Disallow: /\n",
            RATES_PAGE_URL,
            id="paragraph-separator-starts-no-rule",
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow: /private\x85/public\n",
            f"{CBK}/private%C2%85/public",
            id="next-line-character-stays-in-the-path",
        ),
        # A value of whitespace other than space and tab is a path, not an empty value: it
        # matches only URLs starting with it.
        pytest.param(
            "User-agent: *\nDisallow: \u2028\n", f"{CBK}/x", id="line-separator-only-value"
        ),
        pytest.param(
            "User-agent: *\nDisallow: /private\nAllow:\t/private\u2028 \t\n",
            f"{CBK}/private%E2%80%A8",
            id="line-separator-ending-an-allow-applies",
        ),
        pytest.param("User-agent: OpenNSE\nDisallow: /\n", RATES_PAGE_URL, id="token-prefix-only"),
        pytest.param(
            "User-agent: OpenNSE-Terminal-Beta\nDisallow: /\n", RATES_PAGE_URL, id="longer-token"
        ),
        pytest.param(
            "User-agent: OpenNSE-Terminal\nAllow: /\n\nUser-agent: *\nDisallow: /\n",
            RATES_PAGE_URL,
            id="product-group-overrides-wildcard",
        ),
        # A group that ends the file without a rule still applies, and allows every URL.
        pytest.param(
            "User-agent: *\nDisallow: /\n\nUser-agent: OpenNSE-Terminal\n",
            RATES_PAGE_URL,
            id="rule-less-product-group-at-end",
        ),
        pytest.param(
            "User-agent: SomeOtherBot\nDisallow: /\n\nUser-agent: *\n",
            RATES_PAGE_URL,
            id="rule-less-wildcard-group-at-end",
        ),
    ],
)
async def test_url_robots_txt_permits_is_fetched(
    robots_body: str, url: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK, robots_body)
    network.router.get(url).respond(200, text="page")
    async with make_client() as http:
        assert await http.get_text(url) == "page"

    assert network.urls() == [CBK_ROBOTS, url]


async def test_robots_txt_judges_the_url_as_it_would_be_requested(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(NSE, "User-agent: *\nDisallow: /private/\n")
    async with make_client() as http:
        with pytest.raises(RobotsDisallowed) as caught:
            await http.ensure_allowed(f"{NSE}/public/../private/report/")

    assert caught.value.url == f"{NSE}/private/report/"
    assert network.urls() == [f"{NSE}/robots.txt"]


@pytest.mark.parametrize(
    ("content_type", "body", "blocked"),
    [
        # Decoded by its declared codec, the escape would be a lone surrogate, which robotparser
        # cannot quote; as UTF-8 it is six ASCII characters.
        pytest.param(
            "text/plain; charset=unicode_escape",
            b"User-agent: *\nDisallow: /a\\ud800b\nDisallow: /private\n",
            f"{CBK}/private",
            id="unicode-escape-declared",
        ),
        pytest.param(
            "text/plain; charset=windows-1252",
            "User-agent: *\nDisallow: /café\n".encode(),
            f"{CBK}/café",
            id="windows-1252-declared-utf8-sent",
        ),
        pytest.param(
            "text/plain; charset=windows-1252",
            b"\xef\xbb\xbfUser-agent: *\nDisallow: /private\n",
            f"{CBK}/private",
            id="utf8-byte-order-mark-under-another-charset",
        ),
        # Undecodable bytes become U+FFFD in a User-agent value and a rule's path alike.
        pytest.param(
            "text/plain; charset=utf-8",
            b"User-agent: \xffBot\nDisallow: /\n\n"
            b"User-agent: *\nDisallow: /a\xffb\nDisallow: /private\n",
            f"{CBK}/private",
            id="undecodable-bytes-in-agent-and-rule",
        ),
        # Values robotparser converts with an error on them: rule paths urlparse rejects as
        # malformed authorities, and a crawl rate str.isdigit accepts but int does not. The line
        # stays a rule line and the file's other rules stay in force.
        pytest.param(
            "text/plain; charset=utf-8",
            b"User-agent: *\nDisallow: //x]\nDisallow: /private\n",
            f"{CBK}/private",
            id="unopened-bracket-rule",
        ),
        pytest.param(
            "text/plain; charset=utf-8",
            b"User-agent: *\nAllow: //[x\nDisallow: /private\n",
            f"{CBK}/private",
            id="unclosed-bracket-rule",
        ),
        pytest.param(
            "text/plain; charset=utf-8",
            "User-agent: *\nDisallow: //a\uff03b\nDisallow: /private\n".encode(),
            f"{CBK}/private",
            id="fullwidth-delimiter-in-authority-rule",
        ),
        pytest.param(
            "text/plain; charset=utf-8",
            "User-agent: *\nCrawl-delay: \u00b2\nDisallow: /private\n".encode(),
            f"{CBK}/private",
            id="superscript-crawl-delay",
        ),
        pytest.param(
            "text/plain; charset=utf-8",
            "User-agent: *\nRequest-rate: \u00b2/1\nDisallow: /private\n".encode(),
            f"{CBK}/private",
            id="superscript-request-rate",
        ),
    ],
)
async def test_robots_txt_is_read_as_utf8_and_no_line_makes_robotparser_raise(
    content_type: str,
    body: bytes,
    blocked: str,
    network: FakeNetwork,
    make_client: ClientFactory,
) -> None:
    network.router.get(CBK_ROBOTS).respond(
        200, headers={"Content-Type": content_type}, content=body
    )
    page = network.router.get(blocked).respond(200, text="never served")
    public = f"{CBK}/public"
    network.router.get(public).respond(200, text="public")
    async with make_client() as http:
        with pytest.raises(RobotsDisallowed) as caught:
            await http.fetch(blocked)
        assert await http.get_text(public) == "public"

    assert caught.value.reason == "disallowed"
    assert page.call_count == 0
    assert network.urls() == [CBK_ROBOTS, public]


class BytesPathRuleLine:
    """A ``urllib.robotparser.RuleLine`` stand-in whose ``path`` is not the str CPython's is."""

    def __init__(self, path: str, allowance: bool) -> None:
        self.path = path.encode()
        self.allowance = allowance


class KindFlippedRuleLine:
    """A ``urllib.robotparser.RuleLine`` stand-in whose ``allowance`` is the opposite of the one
    its line sets, as a parser classifying rule lines differently would record."""

    def __init__(self, path: str, allowance: bool) -> None:
        self.path = path
        self.allowance = not (allowance or path == "")


_RECORD_ENTRY = polite_http._GroupRecordingParser._add_entry


def record_entry_without_its_last_rule(parser: Any, entry: Any) -> None:
    """The client's ``_add_entry`` hook, losing the entry's last rule line first, as a parser
    that made no rule of one of the body's rule lines would."""
    del entry.rulelines[-1:]
    _RECORD_ENTRY(parser, entry)


def record_entry_with_its_last_rule_repeated(parser: Any, entry: Any) -> None:
    """The client's ``_add_entry`` hook, repeating the entry's last rule line first, as a parser
    that made a rule of a line the body has no rule line for would."""
    entry.rulelines.extend(entry.rulelines[-1:])
    _RECORD_ENTRY(parser, entry)


@pytest.mark.parametrize(
    ("target", "replacement"),
    [
        # parse() files its groups through a hook other than the one the client overrides.
        pytest.param(
            "opennse_terminal.ingestion.http._GroupRecordingParser._add_entry",
            None,
            id="hook-bypassed",
        ),
        pytest.param("urllib.robotparser.RobotFileParser._add_entry", None, id="hook-removed"),
        pytest.param("urllib.robotparser.RuleLine", BytesPathRuleLine, id="rule-shape-changed"),
        # parse()'s rules no longer pair, in order and in kind, with the body's rule lines, whose
        # paths the rules are built from.
        pytest.param(
            "opennse_terminal.ingestion.http._GroupRecordingParser._add_entry",
            record_entry_without_its_last_rule,
            id="rule-line-dropped",
        ),
        pytest.param(
            "opennse_terminal.ingestion.http._GroupRecordingParser._add_entry",
            record_entry_with_its_last_rule_repeated,
            id="rule-line-added",
        ),
        pytest.param("urllib.robotparser.RuleLine", KindFlippedRuleLine, id="rule-kind-changed"),
    ],
)
async def test_robots_txt_is_not_judged_when_robotparser_internals_change(
    target: str,
    replacement: Callable[..., object] | None,
    network: FakeNetwork,
    make_client: ClientFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if replacement is None:
        monkeypatch.delattr(target)
    else:
        monkeypatch.setattr(target, replacement)
    robots = network.robots(CBK, "User-agent: *\nDisallow: /private/\n")
    page = network.router.get(RATES_PAGE_URL).respond(200, text="never served")
    async with make_client() as http:
        with pytest.raises(RuntimeError, match="robots.txt cannot be evaluated"):
            await http.fetch(RATES_PAGE_URL)
        # No verdict was kept, so the next check reads the file again and fails again.
        with pytest.raises(RuntimeError, match="robots.txt cannot be evaluated"):
            await http.ensure_allowed(RATES_PAGE_URL)

    assert robots.call_count == 2
    assert page.call_count == 0


@pytest.mark.parametrize(
    ("status", "robots_calls"),
    [
        pytest.param(429, ATTEMPTS_WITH_THREE_RETRIES, id="429-after-retries"),
        pytest.param(500, ATTEMPTS_WITH_THREE_RETRIES, id="500-after-retries"),
        pytest.param(503, ATTEMPTS_WITH_THREE_RETRIES, id="503-after-retries"),
        pytest.param(501, 1, id="501-not-retried"),
    ],
)
async def test_unreachable_robots_txt_disallows_the_whole_origin(
    status: int,
    robots_calls: int,
    network: FakeNetwork,
    make_client: ClientFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    robots = network.router.get(CBK_ROBOTS).respond(status)
    page = network.router.get(INFLATION_URL).respond(200, text="never served")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(RobotsDisallowed) as caught:
                await http.fetch(INFLATION_URL)
            # The verdict is cached for the run: a second URL is refused without a new request.
            with pytest.raises(RobotsDisallowed):
                await http.ensure_allowed(BONDS_URL)

    assert caught.value.reason == "robots_unreachable"
    assert caught.value.url == INFLATION_URL
    assert robots.call_count == robots_calls
    assert page.call_count == 0
    assert any("unreachable" in message for message in warnings_logged(caplog))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 451])
async def test_unavailable_robots_txt_means_no_rules_apply(
    status: int, network: FakeNetwork, make_client: ClientFactory
) -> None:
    robots = network.router.get(CBK_ROBOTS).respond(status)
    network.router.get(INFLATION_URL).respond(200, text="inflation")
    async with make_client() as http:
        assert await http.get_text(INFLATION_URL) == "inflation"
        await http.ensure_allowed(BONDS_URL)

    assert robots.call_count == 1


async def test_robots_txt_redirect_is_followed_and_its_rules_apply(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.router.get(CBK_ROBOTS).respond(301, headers={"Location": OTHER_ROBOTS})
    network.robots(OTHER, "User-agent: *\nDisallow: /statistics/\n")
    page = network.router.get(RATES_PAGE_URL).respond(200, text="never served")
    async with make_client() as http:
        with pytest.raises(RobotsDisallowed):
            await http.fetch(RATES_PAGE_URL)

    assert page.call_count == 0
    # The redirect target is a robots.txt itself, so it is not checked against another one.
    assert network.urls() == [CBK_ROBOTS, OTHER_ROBOTS]


async def test_robots_txt_network_failure_fails_the_fetch(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    robots = network.router.get(CBK_ROBOTS).mock(side_effect=httpx.ConnectTimeout)
    page = network.router.get(INFLATION_URL).respond(200, text="never served")
    async with make_client(scraper_min_interval_seconds=0.0) as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(INFLATION_URL)

    assert caught.value.reason == "ConnectTimeout after 4 attempts"
    assert caught.value.url == CBK_ROBOTS
    assert robots.call_count == ATTEMPTS_WITH_THREE_RETRIES
    assert page.call_count == 0


@pytest.mark.parametrize(
    ("status", "headers"),
    [
        pytest.param(301, {}, id="301-without-location"),
        pytest.param(302, {}, id="302-without-location"),
        pytest.param(307, {}, id="307-without-location"),
        pytest.param(300, {"Location": OTHER_ROBOTS}, id="300-not-followed"),
        pytest.param(304, {}, id="304-not-modified"),
    ],
)
async def test_robots_txt_status_granting_nothing_fails_the_fetch(
    status: int,
    headers: dict[str, str],
    network: FakeNetwork,
    make_client: ClientFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.router.get(CBK_ROBOTS).respond(status, headers=headers)
    page = network.router.get(INFLATION_URL).respond(200, text="never served")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(INFLATION_URL)
            # No allow-all verdict was kept for the origin, so a later check fails as well.
            with pytest.raises(FetchFailed) as again:
                await http.ensure_allowed(BONDS_URL)

    assert caught.value.reason == again.value.reason == f"robots_http_{status}"
    assert caught.value.url == again.value.url == CBK_ROBOTS
    assert page.call_count == 0
    assert set(network.urls()) == {CBK_ROBOTS}
    assert any(f"answered HTTP {status}" in message for message in warnings_logged(caplog))


# --- Response size ----------------------------------------------------------------------------


async def test_streamed_body_over_the_cap_is_abandoned_early(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    body = ChunkedBody(chunks=40)
    network.router.get(PDF_URL).mock(side_effect=lambda request: httpx.Response(200, content=body))
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.get_bytes(PDF_URL)

    assert caught.value.reason == "response_too_large"
    assert body.produced < body.chunks
    assert MAX_RESPONSE_BYTES < body.produced * MIB <= MAX_RESPONSE_BYTES + MIB


async def test_declared_content_length_over_the_cap_is_rejected_before_reading(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    body = ChunkedBody(chunks=40)
    network.router.get(PDF_URL).mock(
        side_effect=lambda request: httpx.Response(
            200, headers={"Content-Length": str(MAX_RESPONSE_BYTES + 1)}, content=body
        )
    )
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(PDF_URL)

    assert caught.value.reason == "response_too_large"
    assert body.produced == 0


@pytest.mark.parametrize(
    "content_length",
    [
        pytest.param("9" * 30, id="30-digits"),
        pytest.param("9" * 5000, id="5000-digits"),
    ],
)
async def test_declared_content_length_of_any_digit_count_over_the_cap_is_rejected_unread(
    content_length: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    body = ChunkedBody(chunks=40)
    network.router.get(PDF_URL).mock(
        side_effect=lambda request: httpx.Response(
            200, headers={"Content-Length": content_length}, content=body
        )
    )
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(PDF_URL)

    assert caught.value.reason == "response_too_large"
    assert body.produced == 0


async def test_declared_content_length_with_leading_zeros_under_the_cap_is_accepted(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    content = b"x" * 123
    network.router.get(PDF_URL).respond(
        200, headers={"Content-Length": "0" * 5000 + "123"}, content=content
    )
    async with make_client() as http:
        body = await http.get_bytes(PDF_URL)

    assert body == content


async def test_body_of_exactly_the_cap_is_accepted(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    network.router.get(PDF_URL).respond(200, content=b"\0" * MAX_RESPONSE_BYTES)
    async with make_client() as http:
        body = await http.get_bytes(PDF_URL)

    assert len(body) == MAX_RESPONSE_BYTES


# --- Content codings ----------------------------------------------------------------------------

PAYLOAD = b"YEAR,MONTH,Repo,Interbank Rate\n2026,September,8.75,8.75\n" * 200
_WINDOW_BITS = {"gzip": 16 + zlib.MAX_WBITS, "deflate": zlib.MAX_WBITS, "raw": -zlib.MAX_WBITS}


def encode(data: bytes, coding: str) -> bytes:
    """``data`` as one gzip member (``"gzip"``), a zlib stream (``"deflate"``) or raw DEFLATE
    data (``"raw"``, which some servers send as ``deflate``)."""
    compressor = zlib.compressobj(9, zlib.DEFLATED, _WINDOW_BITS[coding])
    return compressor.compress(data) + compressor.flush()


class StreamedBody:
    """``data`` streamed in ``chunk_size`` slices, as a network transport delivers a body, counting
    the slices the client pulled. (A response built from bytes reaches the client already read.)"""

    def __init__(self, data: bytes, chunk_size: int = 64 * 1024) -> None:
        self.data = data
        self.chunk_size = chunk_size
        self.chunks = -(-len(data) // chunk_size)
        self.produced = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for start in range(0, len(self.data), self.chunk_size):
            self.produced += 1
            yield self.data[start : start + self.chunk_size]


def mock_streamed(
    network: FakeNetwork, url: str, content_encoding: str, body: StreamedBody
) -> respx.Route:
    return network.router.get(url).mock(
        side_effect=lambda request: httpx.Response(
            200, headers={"Content-Encoding": content_encoding}, content=body
        )
    )


async def test_requests_ask_for_an_unencoded_body(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(200, text="ok")
    async with make_client() as http:
        await http.fetch(INFLATION_URL)

    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert [sent.request.headers["accept-encoding"] for sent in network.sent] == ["identity"] * 2


@pytest.mark.parametrize("coding", ["gzip", "deflate"])
async def test_compressed_body_expanding_past_the_cap_is_rejected_in_bounded_memory(
    coding: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    # About 100 KiB on the wire that would expand to four times the cap.
    bomb = StreamedBody(encode(b"\0" * (4 * MAX_RESPONSE_BYTES), coding))
    mock_streamed(network, CSV_URL, coding, bomb)
    tracemalloc.start()
    try:
        async with make_client() as http:
            with pytest.raises(FetchFailed) as caught:
                await http.get_bytes(CSV_URL)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert caught.value.reason == "response_too_large"
    assert bomb.produced < bomb.chunks
    # The decoded bytes kept never pass the cap; decoding adds at most a few small buffers.
    assert peak < MAX_RESPONSE_BYTES + 4 * MIB


@pytest.mark.parametrize("chunk_size", [1, 64 * 1024])
@pytest.mark.parametrize(
    ("content_encoding", "encoded", "decoded"),
    [
        pytest.param("gzip", encode(PAYLOAD, "gzip"), PAYLOAD, id="gzip"),
        pytest.param("X-GZIP", encode(PAYLOAD, "gzip"), PAYLOAD, id="x-gzip"),
        pytest.param("deflate", encode(PAYLOAD, "deflate"), PAYLOAD, id="deflate-zlib"),
        pytest.param("deflate", encode(PAYLOAD, "raw"), PAYLOAD, id="deflate-raw"),
        pytest.param(
            "gzip",
            encode(PAYLOAD[:1000], "gzip") + encode(PAYLOAD[1000:], "gzip"),
            PAYLOAD,
            id="gzip-two-members",
        ),
        pytest.param("identity, gzip", encode(PAYLOAD, "gzip"), PAYLOAD, id="identity-and-gzip"),
        pytest.param("identity", PAYLOAD, PAYLOAD, id="identity"),
        pytest.param("gzip", b"", b"", id="empty-gzip"),
    ],
)
async def test_encoded_body_is_decoded_as_it_streams(
    content_encoding: str,
    encoded: bytes,
    decoded: bytes,
    chunk_size: int,
    network: FakeNetwork,
    make_client: ClientFactory,
) -> None:
    network.robots(CBK)
    mock_streamed(network, CSV_URL, content_encoding, StreamedBody(encoded, chunk_size))
    async with make_client() as http:
        body = await http.get_bytes(CSV_URL)

    assert body == decoded


@pytest.mark.parametrize("coding", ["gzip", "deflate"])
async def test_decoded_body_of_exactly_the_cap_is_accepted_and_one_byte_more_is_not(
    coding: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    mock_streamed(
        network, CSV_URL, coding, StreamedBody(encode(b"\0" * MAX_RESPONSE_BYTES, coding))
    )
    mock_streamed(
        network, PDF_URL, coding, StreamedBody(encode(b"\0" * (MAX_RESPONSE_BYTES + 1), coding))
    )
    async with make_client() as http:
        body = await http.get_bytes(CSV_URL)
        with pytest.raises(FetchFailed) as caught:
            await http.get_bytes(PDF_URL)

    assert len(body) == MAX_RESPONSE_BYTES
    assert caught.value.reason == "response_too_large"


@pytest.mark.parametrize("content_encoding", ["br", "zstd", "compress", "gzip, gzip", "gzip, br"])
async def test_unsupported_content_coding_fails_without_reading_the_body(
    content_encoding: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    body = StreamedBody(encode(PAYLOAD, "gzip"))
    mock_streamed(network, CSV_URL, content_encoding, body)
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.get_bytes(CSV_URL)

    assert caught.value.reason == "unsupported_content_encoding"
    assert body.produced == 0


@pytest.mark.parametrize(
    ("content_encoding", "encoded"),
    [
        pytest.param("gzip", PAYLOAD, id="gzip-not-compressed"),
        pytest.param("gzip", encode(PAYLOAD, "gzip")[:-12], id="gzip-truncated"),
        pytest.param("gzip", encode(PAYLOAD, "gzip")[:-8] + bytes(8), id="gzip-bad-checksum"),
        pytest.param("gzip", encode(PAYLOAD, "gzip") + b"trailing", id="gzip-trailing-garbage"),
        pytest.param("deflate", encode(PAYLOAD, "deflate")[:-4], id="deflate-truncated"),
        pytest.param("deflate", encode(PAYLOAD, "deflate") + b"x", id="deflate-data-after-end"),
        pytest.param("deflate", b"x", id="deflate-one-byte"),
    ],
)
async def test_undecodable_body_fails_the_fetch(
    content_encoding: str, encoded: bytes, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    mock_streamed(network, CSV_URL, content_encoding, StreamedBody(encoded, chunk_size=7))
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.get_bytes(CSV_URL)

    assert caught.value.reason == "invalid_content_encoding"
    assert network.urls() == [CBK_ROBOTS, CSV_URL]


async def test_body_a_transport_has_already_read_is_taken_as_decoded(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    # httpx.MockTransport reads, and decodes, a response built from bytes before the client
    # sees it; the size cap and the supported codings still apply.
    network.robots(CBK)
    gzip_header = {"Content-Encoding": "gzip"}
    network.router.get(CSV_URL).respond(200, content=encode(PAYLOAD, "gzip"), headers=gzip_header)
    network.router.get(PDF_URL).respond(
        200, content=encode(b"\0" * (MAX_RESPONSE_BYTES + 1), "gzip"), headers=gzip_header
    )
    network.router.get(BONDS_URL).respond(200, content=b"x", headers={"Content-Encoding": "br"})
    async with make_client() as http:
        body = await http.get_bytes(CSV_URL)
        with pytest.raises(FetchFailed) as too_large:
            await http.get_bytes(PDF_URL)
        with pytest.raises(FetchFailed) as unsupported:
            await http.get_bytes(BONDS_URL)

    assert body == PAYLOAD
    assert too_large.value.reason == "response_too_large"
    assert unsupported.value.reason == "unsupported_content_encoding"


# --- Redirects ---------------------------------------------------------------------------------


def hop(n: int) -> str:
    return f"{CBK}/hop/{n}"


def mock_redirect_chain(network: FakeNetwork, redirects: int) -> respx.Route:
    """``hop(0)`` redirects to ``hop(1)`` and so on ``redirects`` times; returns the route of the
    last URL, which answers 200."""
    for n in range(redirects):
        network.router.get(hop(n)).respond(302, headers={"Location": f"/hop/{n + 1}"})
    return network.router.get(hop(redirects)).respond(200, text="landed")


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
async def test_redirect_is_followed_by_hand_and_reports_the_final_url(
    status: int, network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    old_url = f"{CBK}/old-statistics/"
    network.router.get(old_url).respond(status, headers={"Location": "/statistics/interest-rates/"})
    network.router.get(RATES_PAGE_URL).respond(200, text="rates")
    async with make_client() as http:
        response = await http.fetch(old_url)

    assert response.url == RATES_PAGE_URL
    assert response.status_code == 200
    assert response.text() == "rates"
    assert network.urls() == [CBK_ROBOTS, old_url, RATES_PAGE_URL]
    assert_spaced(network.starts())


async def test_five_redirects_are_followed(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    robots = network.robots(CBK)
    final = mock_redirect_chain(network, redirects=5)
    async with make_client() as http:
        response = await http.fetch(hop(0))

    assert response.url == hop(5)
    assert response.text() == "landed"
    assert final.call_count == 1
    assert robots.call_count == 1
    assert network.urls() == [CBK_ROBOTS, *(hop(n) for n in range(6))]
    assert {sent.request.headers["user-agent"] for sent in network.sent} == {USER_AGENT}
    assert_spaced(network.starts())


async def test_a_sixth_redirect_fails(network: FakeNetwork, make_client: ClientFactory) -> None:
    network.robots(CBK)
    beyond = mock_redirect_chain(network, redirects=6)
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(hop(0))

    assert caught.value.reason == "too_many_redirects"
    assert caught.value.url == hop(0)
    assert beyond.call_count == 0


async def test_cross_host_redirect_checks_the_other_hosts_robots_txt(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    other_robots = network.robots(OTHER, "User-agent: *\nDisallow: /private/\n")
    mirror_url = f"{OTHER}/mirror/weekly_bulletin.pdf"
    network.router.get(PDF_URL).respond(302, headers={"Location": mirror_url})
    network.router.get(mirror_url).respond(200, content=b"%PDF-1.7 bulletin")
    async with make_client() as http:
        assert await http.get_bytes(PDF_URL) == b"%PDF-1.7 bulletin"

    assert other_robots.call_count == 1
    assert network.urls() == [CBK_ROBOTS, PDF_URL, OTHER_ROBOTS, mirror_url]
    assert_spaced(network.starts(CBK_ROBOTS) + network.starts(PDF_URL))
    assert_spaced(network.starts(OTHER_ROBOTS) + network.starts(mirror_url))


async def test_cross_host_redirect_to_a_disallowed_url_is_not_requested(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    network.robots(OTHER, "User-agent: *\nDisallow: /private/\n")
    blocked_url = f"{OTHER}/private/weekly_bulletin.pdf"
    network.router.get(PDF_URL).respond(302, headers={"Location": blocked_url})
    blocked = network.router.get(blocked_url).respond(200, content=b"%PDF-1.7")
    async with make_client() as http:
        with pytest.raises(RobotsDisallowed) as caught:
            await http.fetch(PDF_URL)

    assert caught.value.url == blocked_url
    assert caught.value.reason == "disallowed"
    assert blocked.call_count == 0
    assert network.urls() == [CBK_ROBOTS, PDF_URL, OTHER_ROBOTS]


@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        pytest.param({}, "http_302", id="no-location"),
        pytest.param(
            {"Location": "ftp://other.example.net/files/rates.csv"},
            "unsupported_url",
            id="non-http-location",
        ),
        pytest.param(
            {"Location": "https://user:SECRET@other.example.net/files/rates.csv"},
            "url_has_userinfo",
            id="userinfo-location",
        ),
        pytest.param({"Location": "data:text/plain,x"}, "unsupported_url", id="data-location"),
        pytest.param(
            {"Location": "javascript:alert(1)"}, "unsupported_url", id="javascript-location"
        ),
        pytest.param({"Location": "http://[::1"}, "invalid_redirect", id="unparseable-location"),
        pytest.param(
            {"Location": "http://xn--/files/rates.csv"}, "invalid_url", id="malformed-idna-host"
        ),
        pytest.param(
            {"Location": "https://other.example.net:65536/files/rates.csv"},
            "invalid_url",
            id="port-out-of-range",
        ),
    ],
)
async def test_unusable_redirect_fails_the_fetch(
    headers: dict[str, str],
    reason: str,
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    network.robots(CBK)
    network.router.get(INFLATION_URL).respond(302, headers=headers)
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(INFLATION_URL)

    assert caught.value.reason == reason
    assert "SECRET" not in str(caught.value)
    # Refused at once: one request, no retry, and the target never requested.
    assert network.urls() == [CBK_ROBOTS, INFLATION_URL]
    assert sleep.delays == []
    assert warnings_logged(caplog) == []


@pytest.mark.parametrize(
    ("location", "reason"),
    [
        pytest.param("data:text/plain,x", "unsupported_url", id="data"),
        pytest.param("javascript:alert(1)", "unsupported_url", id="javascript"),
        pytest.param("http://[::1", "invalid_redirect", id="unparseable"),
    ],
)
async def test_robots_txt_redirect_to_an_unusable_location_fails_without_a_verdict(
    location: str,
    reason: str,
    network: FakeNetwork,
    make_client: ClientFactory,
    sleep: FakeSleep,
    caplog: pytest.LogCaptureFixture,
) -> None:
    robots = network.router.get(CBK_ROBOTS).respond(302, headers={"Location": location})
    page = network.router.get(INFLATION_URL).respond(200, text="never served")
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(INFLATION_URL)
            # No verdict was kept for the origin, so a later check asks again and fails again.
            with pytest.raises(FetchFailed) as again:
                await http.ensure_allowed(BONDS_URL)

    assert caught.value.reason == again.value.reason == reason
    assert robots.call_count == 2
    assert page.call_count == 0
    assert sleep.delays == []
    assert warnings_logged(caplog) == []


# --- URL validation, fetch variants and decoding ---------------------------------------------


@pytest.mark.parametrize(
    ("url", "reason", "described"),
    [
        pytest.param(
            "ftp://www.centralbank.go.ke/rates.csv",
            "unsupported_url",
            "ftp://www.centralbank.go.ke/rates.csv",
            id="ftp",
        ),
        pytest.param(
            "/statistics/interest-rates/",
            "unsupported_url",
            "/statistics/interest-rates/",
            id="relative",
        ),
        pytest.param(
            "https:///statistics/", "unsupported_url", "<https URL without a host>", id="no-host"
        ),
        pytest.param(f"{CBK}/\x00", "invalid_url", "<invalid URL>", id="control-character"),
        pytest.param(
            "https://xn--/rates.csv", "invalid_url", "https://xn--/rates.csv", id="malformed-idna"
        ),
        pytest.param(
            f"{CBK}:65536/rates.csv",
            "invalid_url",
            f"{CBK}:65536/rates.csv",
            id="port-out-of-range",
        ),
        pytest.param(
            "https://user:SECRET@www.centralbank.go.ke/rates.csv?token=SECRET#SECRET",
            "url_has_userinfo",
            f"{CBK}/rates.csv",
            id="user-and-password",
        ),
        pytest.param(
            "https://SECRET@www.centralbank.go.ke/rates.csv",
            "url_has_userinfo",
            f"{CBK}/rates.csv",
            id="user-only",
        ),
        pytest.param(
            "http://:SECRET@www.centralbank.go.ke:8080/rates.csv",
            "url_has_userinfo",
            "http://www.centralbank.go.ke:8080/rates.csv",
            id="password-only",
        ),
        pytest.param(
            "ftp://user:SECRET@www.centralbank.go.ke/rates.csv",
            "unsupported_url",
            "ftp://www.centralbank.go.ke/rates.csv",
            id="ftp-with-userinfo",
        ),
    ],
)
async def test_urls_that_are_not_absolute_http_or_carry_userinfo_are_rejected_unsent(
    url: str, reason: str, described: str, network: FakeNetwork, make_client: ClientFactory
) -> None:
    async with make_client() as http:
        with pytest.raises(FetchFailed) as caught:
            await http.fetch(url)
        with pytest.raises(FetchFailed) as checked:
            await http.ensure_allowed(url)

    assert caught.value.reason == checked.value.reason == reason
    assert caught.value.url == checked.value.url == described
    assert "SECRET" not in str(caught.value)
    assert network.sent == []


async def test_retries_and_failures_name_urls_without_userinfo_query_or_error_text(
    network: FakeNetwork, make_client: ClientFactory, caplog: pytest.LogCaptureFixture
) -> None:
    network.robots(CBK)
    network.router.get(f"{CBK}/retry/file.csv?token=SECRET").mock(
        side_effect=[
            httpx.ConnectTimeout("timed out connecting to SECRET"),
            httpx.Response(503),
            httpx.Response(302, headers={"Location": "/moved/file.csv?token=SECRET#SECRET"}),
        ]
    )
    network.router.get(f"{CBK}/moved/file.csv?token=SECRET").respond(404)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as caught:
                await http.fetch(f"{CBK}/retry/file.csv?token=SECRET#SECRET")

    assert caught.value.reason == "http_404"
    assert caught.value.url == f"{CBK}/moved/file.csv"
    assert "SECRET" not in str(caught.value)
    messages = [record.getMessage() for record in caplog.records if record.name == LOGGER]
    assert messages
    assert not [message for message in messages if "SECRET" in message or "token" in message]
    first, second = warnings_logged(caplog)
    assert f"GET {CBK}/retry/file.csv attempt 1 of 4 failed (ConnectTimeout);" in first
    assert f"GET {CBK}/retry/file.csv attempt 2 of 4 failed (HTTP 503);" in second
    assert f"GET {CBK}/moved/file.csv -> 404" in messages


LIBRARY_LOGGERS = frozenset(
    {
        "httpx",
        "httpcore.connection",
        "httpcore.http11",
        "httpcore.http2",
        "httpcore.proxy",
        "httpcore.socks",
    }
)
# What httpcore traces at DEBUG once a response's headers are in, on a real connection; the mock
# transport has no connection, so the mock source writes it.
HEADERS_TRACE = (
    "receive_response_headers.complete return_value=(b'HTTP/1.1', {status}, [{headers}])"
)


def library_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name in LIBRARY_LOGGERS]


async def test_library_records_of_own_requests_are_redacted_and_a_hosts_are_not(
    network: FakeNetwork, make_client: ClientFactory, caplog: pytest.LogCaptureFixture
) -> None:
    trace = logging.getLogger("httpcore.http11")
    userinfo_location = "https://bob:SECRET@other.example.net/files/rates.csv"

    def traced_redirect(request: httpx.Request) -> httpx.Response:
        trace.debug(
            HEADERS_TRACE.format(status=302, headers=f"(b'Location', b'{userinfo_location}')")
        )
        return httpx.Response(302, headers={"Location": userinfo_location})

    def traced_host_response(request: httpx.Request) -> httpx.Response:
        trace.debug(HEADERS_TRACE.format(status=200, headers="(b'X-Host', b'hostheader')"))
        return httpx.Response(200)

    network.robots(CBK)
    network.router.get(f"{CBK}/files/rates.csv?token=SECRET").respond(404)
    network.router.get(f"{CBK}/moved?api_key=SECRET").mock(side_effect=traced_redirect)
    with caplog.at_level(logging.DEBUG):
        async with make_client(scraper_min_interval_seconds=0.0) as http:
            with pytest.raises(FetchFailed) as missing:
                await http.fetch(f"{CBK}/files/rates.csv?token=SECRET#SECRET")
            with pytest.raises(FetchFailed) as refused:
                await http.fetch(f"{CBK}/moved?api_key=SECRET")
        own = library_messages(caplog)
        caplog.clear()
        # A host application's own client keeps the httpx and httpcore records it always had.
        transport = httpx.MockTransport(traced_host_response)
        async with httpx.AsyncClient(transport=transport) as host_client:
            await host_client.get(f"{CBK}/host?hostquery=1#top")
        host = library_messages(caplog)

    assert missing.value.reason == "http_404"
    assert refused.value.reason == "url_has_userinfo"
    assert own == [
        f'HTTP Request: GET {CBK_ROBOTS} "HTTP/1.1 200 OK"',
        f'HTTP Request: GET {CBK}/files/rates.csv "HTTP/1.1 404 Not Found"',
        "receive_response_headers.complete",
        f'HTTP Request: GET {CBK}/moved "HTTP/1.1 302 Found"',
    ]
    assert host == [
        HEADERS_TRACE.format(status=200, headers="(b'X-Host', b'hostheader')"),
        f'HTTP Request: GET {CBK}/host?hostquery=1#top "HTTP/1.1 200 OK"',
    ]


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(
            "https://user:SECRET@www.centralbank.go.ke/uploads/a b.csv?token=SECRET#SECRET",
            f"{CBK}/uploads/a%20b.csv",
            id="userinfo-query-and-fragment-dropped",
        ),
        pytest.param(f"{CBK}:443/x", f"{CBK}/x", id="default-port-dropped"),
        pytest.param("http://127.0.0.1:8080/x?y=1", "http://127.0.0.1:8080/x", id="port-kept"),
        pytest.param("http://[::1]:8443/x", "http://[::1]:8443/x", id="ipv6-host"),
        pytest.param("https://bücher.example/x", "https://xn--bcher-kva.example/x", id="idna"),
        pytest.param(CBK, f"{CBK}/", id="empty-path"),
        pytest.param(f"{CBK}/a%0Ab.csv", f"{CBK}/a%0Ab.csv", id="escapes-kept"),
        pytest.param("/uploads/a b.csv?ver=2#top", "/uploads/a%20b.csv", id="relative-path"),
        pytest.param("#top", "", id="fragment-only"),
        pytest.param(
            "//user:SECRET@other.example.net/x?q=1", "//other.example.net/x", id="network-path"
        ),
        pytest.param(
            "https:user:SECRET@other.example.net/x",
            "<https URL without a host>",
            id="scheme-without-host",
        ),
        pytest.param("mailto:SECRET@example.net", "<mailto URL without a host>", id="mailto"),
        pytest.param(f"{CBK}/\x00?token=SECRET", "<invalid URL>", id="unparseable"),
        pytest.param("http://xn--/x?token=SECRET", "http://xn--/x", id="malformed-idna-host"),
        pytest.param(
            httpx.URL("https://user:SECRET@www.centralbank.go.ke/x?token=SECRET"),
            f"{CBK}/x",
            id="httpx-url",
        ),
    ],
)
async def test_describe_url_keeps_only_scheme_host_port_and_path(
    url: str | httpx.URL, expected: str
) -> None:
    assert describe_url(url) == expected


async def test_fetch_returns_the_response_and_helpers_return_its_body(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    csv = b"YEAR,MONTH,Repo\r\n1991,July,-\r\n"
    network.router.get(CSV_URL).respond(200, content=csv, headers={"Content-Type": "text/csv"})
    network.router.get(INFLATION_URL).respond(
        200,
        content="Année 2026: 6.76".encode("iso-8859-1"),
        headers={"Content-Type": "text/html; charset=ISO-8859-1"},
    )
    async with make_client() as http:
        response = await http.fetch(CSV_URL)
        raw = await http.get_bytes(CSV_URL)
        text = await http.get_text(INFLATION_URL)

    assert isinstance(response, FetchedResponse)
    assert response.url == CSV_URL
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv"
    assert response.headers["Content-Type"] == "text/csv"
    assert response.content == csv
    assert raw == csv
    assert text == "Année 2026: 6.76"


@pytest.mark.parametrize(
    ("content_type", "content", "expected"),
    [
        pytest.param(None, "Année".encode(), "Année", id="no-content-type"),
        pytest.param("text/html", "Année".encode(), "Année", id="no-charset"),
        pytest.param(
            "text/html; charset=ISO-8859-1", "Année".encode("iso-8859-1"), "Année", id="latin-1"
        ),
        pytest.param(
            'text/html; Charset="windows-1252"', b"\x93cbk\x94", "\u201ccbk\u201d", id="quoted"
        ),
        pytest.param("text/html; charset=", "Année".encode(), "Année", id="empty-charset"),
        pytest.param(
            "text/html; charset=x-no-such-codec", "Année".encode(), "Année", id="unknown-codec"
        ),
        pytest.param(
            "text/plain; charset=base64", "Année".encode(), "Année", id="not-a-text-codec"
        ),
        pytest.param(
            "text/html; charset=utf-8", b"bad \xff byte", "bad \ufffd byte", id="undecodable"
        ),
    ],
)
async def test_text_uses_the_declared_charset_or_utf8(
    content_type: str | None, content: bytes, expected: str
) -> None:
    headers = httpx.Headers({} if content_type is None else {"Content-Type": content_type})
    response = FetchedResponse(INFLATION_URL, 200, headers, content)
    assert response.text() == expected


# --- Context manager use ------------------------------------------------------------------------


async def test_requests_need_an_open_client(
    network: FakeNetwork, make_client: ClientFactory
) -> None:
    network.robots(CBK)
    client = make_client()
    with pytest.raises(RuntimeError, match="not open"):
        await client.fetch(INFLATION_URL)

    async with client:
        with pytest.raises(RuntimeError, match="already open"):
            async with client:
                pass

    with pytest.raises(RuntimeError, match="not open"):
        await client.get_text(INFLATION_URL)
    # Closing a client that is not open is a no-op.
    await client.__aexit__(None, None, None)
    assert network.sent == []
