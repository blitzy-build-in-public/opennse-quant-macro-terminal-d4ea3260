"""Polite HTTP client for the public web pages and files the ingestion pipelines read.

Every network fetch in ``opennse_terminal.ingestion`` goes through one :class:`PoliteClient` per
run (``RunContext.http``): the CBK statistics pages, their CSV files and the weekly-bulletin PDFs,
and the ``robots.txt`` check the NSE market-statistics pipeline makes before it renders that
public page in a browser. No other ingestion module creates an httpx client.

Politeness contract:

* **Identification.** Every request, ``robots.txt`` included, carries the user agent
  ``OpenNSE-Terminal/<version> (+mailto:<TERMINAL_SCRAPER_CONTACT>)``.
* **No stored state.** One ``httpx.AsyncClient`` per run, whose cookie jar is cleared after every
  response, so no cookie is ever sent back to a source, within a run or across runs.
* **Timeouts.** 10 s to connect and ``TERMINAL_SCRAPER_TIMEOUT_SECONDS`` (30 s) to read.
* **Spacing.** At least ``TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS`` (2.0 s) between the starts of two
  requests to the same host, counting retries, redirect hops and ``robots.txt`` fetches.
* **Retries.** Up to ``TERMINAL_SCRAPER_MAX_RETRIES`` (3) on connect or read timeouts, other
  transient network failures and HTTP 429, 500, 502, 503 and 504. The wait before retry *n* is
  the server's ``Retry-After`` when it sends one, otherwise 2^n s plus up to 1 s of jitter, and
  never more than 60 s.
* **Redirects and size.** At most 5 redirects, followed by hand so that every hop is spaced and
  checked against its own origin's ``robots.txt``. Bodies are streamed and abandoned once they
  exceed 25 MiB.
* **robots.txt.** Fetched once per origin per run through this client, interpreted as RFC 9309
  §2.3.1 sets out, and checked for the ``OpenNSE-Terminal`` product token before every URL.

Failures surface as the ingestion exceptions the run executor maps to run statuses:
:class:`~opennse_terminal.ingestion.base.RobotsDisallowed` (``blocked_by_robots``) and
:class:`~opennse_terminal.ingestion.base.FetchFailed` (``failed``).
"""

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Final, Self
from urllib.robotparser import RobotFileParser

import httpx

from opennse_terminal import __version__
from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.base import FetchFailed, RobotsDisallowed

__all__ = [
    "BACKOFF_CAP_SECONDS",
    "CONNECT_TIMEOUT_SECONDS",
    "MAX_REDIRECTS",
    "MAX_RESPONSE_BYTES",
    "RETRY_STATUSES",
    "ROBOTS_AGENT",
    "FetchedResponse",
    "PoliteClient",
    "build_user_agent",
]

logger = logging.getLogger("opennse_terminal.ingestion.http")

ROBOTS_AGENT: Final = "OpenNSE-Terminal"
"""Product token of the user agent, matched against the ``User-agent`` lines of ``robots.txt``."""

MAX_RESPONSE_BYTES: Final = 25 * 1024 * 1024
"""Largest response body accepted, in bytes after any ``Content-Encoding`` is removed."""

MAX_REDIRECTS: Final = 5
"""Most redirects followed for one fetch."""

RETRY_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
"""HTTP statuses retried with backoff; any other non-2xx status fails at once."""

CONNECT_TIMEOUT_SECONDS: Final = 10.0
"""Connect timeout of every request; the read timeout is ``scraper_timeout_seconds``."""

BACKOFF_CAP_SECONDS: Final = 60.0
"""Longest wait before a retry, whatever ``Retry-After`` asks for."""

# Connect and read timeouts, plus the other transport failures a later attempt can cure: refused
# or reset connections and a server dropping a kept-alive connection mid-response. Errors the
# client itself causes (an unsupported scheme, a local protocol error) and proxy errors are not
# retried.
_TRANSIENT_ERRORS: Final = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)

_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})
_ALLOWED_SCHEMES: Final = frozenset({"http", "https"})


def build_user_agent(settings: TerminalSettings) -> str:
    """The scrapers' user agent, ``OpenNSE-Terminal/<version> (+mailto:<scraper_contact>)``."""
    return f"{ROBOTS_AGENT}/{__version__} (+mailto:{settings.scraper_contact})"


@dataclass(frozen=True, slots=True)
class FetchedResponse:
    """A completed GET request.

    Attributes:
        url: The final URL, after redirects. Resolve relative links found in the body against it.
        status_code: The HTTP status; always 2xx for what :meth:`PoliteClient.fetch` returns.
        headers: The response headers, looked up case-insensitively.
        content: The body with any ``Content-Encoding`` removed, at most
            :data:`MAX_RESPONSE_BYTES` bytes.
    """

    url: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes

    def text(self) -> str:
        """The body decoded with the ``Content-Type`` charset, or UTF-8 when none is declared or
        the declared one is unknown. Undecodable bytes become U+FFFD instead of raising."""
        charset = _declared_charset(self.headers.get("content-type")) or "utf-8"
        try:
            return self.content.decode(charset, errors="replace")
        except LookupError:
            # The header named an unknown codec, or one that is not a text encoding ("base64").
            return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True, slots=True)
class _RobotsRules:
    """What one origin's ``robots.txt`` permits.

    ``parser`` is None when the file was unavailable (RFC 9309 §2.3.1.3: no rule applies), unless
    ``unreachable`` is set (§2.3.1.4: everything is disallowed).
    """

    parser: RobotFileParser | None
    unreachable: bool = False


def _declared_charset(content_type: str | None) -> str | None:
    """The ``charset`` parameter of a ``Content-Type`` header value, if it has one."""
    if content_type is None:
        return None
    for parameter in content_type.split(";")[1:]:
        name, _, value = parameter.partition("=")
        if name.strip().lower() == "charset":
            return value.strip().strip("\"'") or None
    return None


def _content_length(headers: Mapping[str, str]) -> int | None:
    """The declared ``Content-Length``, or None when it is absent or not a plain integer."""
    value = headers.get("content-length")
    if value is None:
        return None
    value = value.strip()
    return int(value) if value.isascii() and value.isdigit() else None


def _retry_after_seconds(value: str | None) -> float | None:
    """Seconds a ``Retry-After`` header asks the client to wait, capped at 60 s.

    Accepts delta-seconds (``7``) or an HTTP-date, which is compared with the current UTC time; a
    date already past means no wait. Returns None when the header is absent or malformed, so the
    exponential backoff applies instead.
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped.isascii() and stripped.isdigit():
        seconds = float(int(stripped))
    else:
        try:
            when = parsedate_to_datetime(stripped)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            # HTTP-dates are GMT; a "-0000" zone parses as a naive datetime.
            when = when.replace(tzinfo=UTC)
        seconds = max(0.0, (when - datetime.now(UTC)).total_seconds())
    return min(BACKOFF_CAP_SECONDS, seconds)


def _backoff_seconds(attempt: int, retry_after: float | None) -> float:
    """Wait before retrying after failed attempt number ``attempt`` (1-based): ``retry_after``
    when the server sent one, else 2^attempt s plus up to 1 s of jitter; at most 60 s."""
    if retry_after is not None:
        return min(BACKOFF_CAP_SECONDS, retry_after)
    jitter = random.uniform(0.0, 1.0)  # noqa: S311 - jitter only spreads retries; not a secret
    return min(BACKOFF_CAP_SECONDS, 2.0**attempt + jitter)


def _parse_url(url: str) -> httpx.URL:
    """``url`` as an absolute http(s) URL with a host; anything else raises FetchFailed."""
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL as exc:
        raise FetchFailed(url, "invalid_url") from exc
    if parsed.scheme not in _ALLOWED_SCHEMES or not parsed.host:
        raise FetchFailed(url, "unsupported_url")
    return parsed


def _origin(url: httpx.URL) -> str:
    """``<scheme>://<host>[:<port>]``, the scope of one ``robots.txt`` (RFC 9309 §2.3)."""
    return f"{url.scheme}://{url.netloc.decode('ascii')}"


class PoliteClient:
    """The run's HTTP client for public pages and files: identified, spaced, retried and
    ``robots.txt``-checked (see the module docstring for the full contract).

    Use it as an async context manager; each opening is one run, with its own ``httpx`` client
    and its own ``robots.txt`` cache::

        async with PoliteClient(settings) as http:
            html = await http.get_text("https://www.centralbank.go.ke/inflation-rates/")

    It is safe for concurrent coroutines on one event loop (per-host and per-origin locks
    serialize spacing and ``robots.txt`` loading) but not for use from several threads.

    Args:
        settings: Supplies the contact address of the user agent, the read timeout, the per-host
            spacing and the retry count.
        transport: An httpx transport to send requests through instead of the network, for
            example ``httpx.MockTransport`` in tests.
        sleep: Awaited for every spacing and backoff wait.
        clock: Monotonic seconds, used to measure the spacing between requests.
    """

    def __init__(
        self,
        settings: TerminalSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._transport = transport
        self._sleep = sleep
        self._clock = clock
        self._user_agent = build_user_agent(settings)
        self._client: httpx.AsyncClient | None = None
        self._host_locks: dict[str, asyncio.Lock] = {}
        # Kept across openings of one instance, so a reopened client still spaces its requests.
        self._last_request_at: dict[str, float] = {}
        self._robots: dict[str, _RobotsRules] = {}
        self._robots_locks: dict[str, asyncio.Lock] = {}

    @property
    def user_agent(self) -> str:
        """The ``User-Agent`` header sent with every request (see :func:`build_user_agent`)."""
        return self._user_agent

    async def __aenter__(self) -> Self:
        """Open the run's ``httpx.AsyncClient``: the user agent, a 10 s connect and
        ``scraper_timeout_seconds`` read timeout, and no automatic redirects. The ``robots.txt``
        cache starts empty, so every run reads each origin's file afresh."""
        if self._client is not None:
            raise RuntimeError("PoliteClient is already open")
        self._host_locks.clear()
        self._robots.clear()
        self._robots_locks.clear()
        self._client = httpx.AsyncClient(
            headers={"User-Agent": self._user_agent},
            timeout=httpx.Timeout(
                self._settings.scraper_timeout_seconds, connect=CONNECT_TIMEOUT_SECONDS
            ),
            follow_redirects=False,
            transport=self._transport,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the ``httpx`` client and its connections."""
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def ensure_allowed(self, url: str) -> None:
        """Return only if ``robots.txt`` of the URL's origin lets ``OpenNSE-Terminal`` fetch it.

        The origin's ``robots.txt`` is requested at most once while the client is open, through
        this client (same user agent, spacing, retries, redirect and size limits), and is never
        read by ``RobotFileParser.read``, which would bypass all of them. Its final status is
        interpreted as RFC 9309 §2.3.1 sets out:

        * 2xx: the rules are parsed and applied;
        * 5xx after retries: unreachable, so every URL of the origin is disallowed. A 429 after
          retries is treated the same way, because being rate-limited is not permission to crawl;
        * any other status, 404 included: unavailable, so no rule applies.

        Raises:
            RobotsDisallowed: With reason ``"disallowed"`` when the rules block the URL, or
                ``"robots_unreachable"`` when ``robots.txt`` was unreachable. Logged at WARNING.
            FetchFailed: When ``url`` is not an absolute http(s) URL, or ``robots.txt`` could not
                be fetched at all (network failure after retries, too many redirects, oversize).
        """
        parsed = _parse_url(url)
        origin = _origin(parsed)
        rules = await self._robots_rules(origin)
        if rules.unreachable:
            logger.warning(
                "robots.txt of %s is unreachable; treating %s as disallowed", origin, url
            )
            raise RobotsDisallowed(url, "robots_unreachable")
        if rules.parser is not None and not rules.parser.can_fetch(ROBOTS_AGENT, url):
            logger.warning("robots.txt of %s disallows %s for %s", origin, url, ROBOTS_AGENT)
            raise RobotsDisallowed(url)

    async def fetch(self, url: str) -> FetchedResponse:
        """GET ``url`` politely and return the final 2xx response.

        Checks ``robots.txt`` first and again for every redirect hop; spaces every request to a
        host; retries transient failures and retryable statuses with backoff; follows at most
        :data:`MAX_REDIRECTS` redirects; reads at most :data:`MAX_RESPONSE_BYTES`.

        Raises:
            RobotsDisallowed: ``robots.txt`` blocks the URL or a redirect target, which is then
                not requested.
            FetchFailed: With reason ``"http_<status>"`` for a final non-2xx status (a retryable
                one after the last retry, any other at once), ``"<ExceptionName> after <n>
                attempts"`` when transient network errors persist, ``"too_many_redirects"``,
                ``"response_too_large"``, ``"invalid_url"`` or ``"unsupported_url"``.
        """
        response = await self._get(url, check_robots=True)
        if not 200 <= response.status_code < 300:
            raise FetchFailed(response.url, f"http_{response.status_code}")
        return response

    async def get_text(self, url: str) -> str:
        """:meth:`fetch` ``url`` and return its body decoded (see :meth:`FetchedResponse.text`)."""
        return (await self.fetch(url)).text()

    async def get_bytes(self, url: str) -> bytes:
        """:meth:`fetch` ``url`` and return its raw body, for CSV and PDF files."""
        return (await self.fetch(url)).content

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("PoliteClient is not open; use it as 'async with PoliteClient(...)'")
        return self._client

    async def _robots_rules(self, origin: str) -> _RobotsRules:
        """The origin's cached rules, loading them on first use; concurrent first uses of one
        origin share a single ``robots.txt`` request."""
        lock = self._robots_locks.setdefault(origin, asyncio.Lock())
        async with lock:
            rules = self._robots.get(origin)
            if rules is None:
                rules = await self._load_robots(origin)
                self._robots[origin] = rules
            return rules

    async def _load_robots(self, origin: str) -> _RobotsRules:
        robots_url = f"{origin}/robots.txt"
        response = await self._get(robots_url, check_robots=False)
        status = response.status_code
        if 200 <= status < 300:
            parser = RobotFileParser(robots_url)
            parser.parse(response.text().splitlines())
            logger.debug("robots.txt of %s loaded (%d bytes)", origin, len(response.content))
            return _RobotsRules(parser=parser)
        # RFC 9309 lets a 4xx mean "no rules", but a 429 is a rate limit, not permission to
        # crawl, so it is treated like a 5xx: the whole origin is disallowed for this run.
        if status >= 500 or status == 429:
            return _RobotsRules(parser=None, unreachable=True)
        logger.debug("robots.txt of %s unavailable (HTTP %d); no rules apply", origin, status)
        return _RobotsRules(parser=None)

    async def _get(self, url: str, *, check_robots: bool) -> FetchedResponse:
        """GET ``url``, following redirects by hand, and return the final response whatever its
        status. Each hop is a separately spaced and retried request; with ``check_robots`` each
        hop's URL is checked against its origin's ``robots.txt`` before it is requested."""
        current = _parse_url(url)
        redirects = 0
        while True:
            if check_robots:
                await self.ensure_allowed(str(current))
            response = await self._send(current)
            location = response.headers.get("location")
            if response.status_code not in _REDIRECT_STATUSES or location is None:
                return response
            if redirects >= MAX_REDIRECTS:
                raise FetchFailed(url, "too_many_redirects")
            redirects += 1
            try:
                target = current.join(location)
            except httpx.InvalidURL as exc:
                raise FetchFailed(url, "invalid_redirect") from exc
            current = _parse_url(str(target))

    async def _send(self, url: httpx.URL) -> FetchedResponse:
        """One request with up to ``scraper_max_retries`` retries. Returns the last response,
        even when it still has a retryable status, so callers can interpret it."""
        client = self._require_client()
        attempts = 1 + self._settings.scraper_max_retries
        attempt = 1
        while True:
            await self._wait_turn(url.host)
            try:
                response = await self._attempt(client, url)
            except _TRANSIENT_ERRORS as exc:
                if attempt >= attempts:
                    raise FetchFailed(
                        str(url), f"{type(exc).__name__} after {attempts} attempts"
                    ) from exc
                cause = f"{type(exc).__name__}: {exc}"
                delay = _backoff_seconds(attempt, None)
            except httpx.HTTPError as exc:
                raise FetchFailed(str(url), type(exc).__name__) from exc
            else:
                if response.status_code not in RETRY_STATUSES or attempt >= attempts:
                    return response
                cause = f"HTTP {response.status_code}"
                retry_after = _retry_after_seconds(response.headers.get("retry-after"))
                delay = _backoff_seconds(attempt, retry_after)
            finally:
                # No cookie set by a source is ever sent back to it.
                client.cookies.clear()
            logger.warning(
                "GET %s attempt %d of %d failed (%s); retrying in %.1f s",
                url,
                attempt,
                attempts,
                cause,
                delay,
            )
            await self._sleep(delay)
            attempt += 1

    async def _attempt(self, client: httpx.AsyncClient, url: httpx.URL) -> FetchedResponse:
        """Stream one GET. Only a 2xx body is read, and the stream is abandoned once it passes
        :data:`MAX_RESPONSE_BYTES` (or the declared ``Content-Length`` already does)."""
        async with client.stream("GET", url) as response:
            final_url = str(response.url)
            status = response.status_code
            headers = httpx.Headers(response.headers)
            if not response.is_success:
                # Redirects, errors and retryable statuses need no body; leaving the block closes
                # the stream unread.
                logger.debug("GET %s -> %d", final_url, status)
                return FetchedResponse(final_url, status, headers, b"")
            declared = _content_length(headers)
            if declared is not None and declared > MAX_RESPONSE_BYTES:
                raise FetchFailed(final_url, "response_too_large")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise FetchFailed(final_url, "response_too_large")
        logger.debug("GET %s -> %d (%d bytes)", final_url, status, len(body))
        return FetchedResponse(final_url, status, headers, bytes(body))

    async def _wait_turn(self, host: str) -> None:
        """Wait until ``scraper_min_interval_seconds`` have passed since the start of the previous
        request to ``host``, then claim the current time as this request's start."""
        interval = self._settings.scraper_min_interval_seconds
        lock = self._host_locks.setdefault(host, asyncio.Lock())
        async with lock:
            previous = self._last_request_at.get(host)
            if previous is not None:
                remaining = interval - (self._clock() - previous)
                if remaining > 0:
                    await self._sleep(remaining)
            self._last_request_at[host] = self._clock()
