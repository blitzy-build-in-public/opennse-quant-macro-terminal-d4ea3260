"""Polite HTTP client for the public web pages and files the ingestion pipelines read.

Every network fetch in ``opennse_terminal.ingestion`` goes through one :class:`PoliteClient` per
run (``RunContext.http``): the CBK statistics pages, their CSV files and the weekly-bulletin PDFs,
and the ``robots.txt`` check the NSE market-statistics pipeline makes before it renders that
public page in a browser. No other ingestion module creates an httpx client.

Politeness contract:

* **Identification.** Every request, ``robots.txt`` included, carries the user agent
  ``OpenNSE-Terminal/<version> (+mailto:<TERMINAL_SCRAPER_CONTACT>)``.
* **No stored state.** One ``httpx.AsyncClient`` per run, whose cookie jar refuses every cookie,
  so no cookie a source sets is ever stored or sent back to it, within a run (concurrent fetches
  included) or across runs.
* **Timeouts.** 10 s to connect and ``TERMINAL_SCRAPER_TIMEOUT_SECONDS`` (30 s) to read, the
  read timeout bounding each wait for data. Each attempt as a whole, from connecting to the last
  body byte, is also abandoned once it has run for 10 s plus four read timeouts (130 s at the
  default), so a source sending a byte just inside the read timeout cannot hold a fetch open; an
  abandoned attempt is retried as a read timeout is.
* **Spacing.** At least ``TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS`` (2.0 s) between the starts of two
  requests to the same host, counting retries, redirect hops and ``robots.txt`` fetches.
* **Retries.** Up to ``TERMINAL_SCRAPER_MAX_RETRIES`` (3) on connect or read timeouts, other
  transient network failures and HTTP 429, 500, 502, 503 and 504. The wait before retry *n* is
  the server's ``Retry-After`` when it sends one, otherwise 2^n s plus up to 1 s of jitter, and
  never more than 60 s.
* **Redirects and size.** At most 5 redirects, followed by hand so that every hop is spaced and
  checked against its own origin's ``robots.txt``. Every request asks for an unencoded body
  (``Accept-Encoding: identity``); a ``gzip`` or ``deflate`` body sent anyway is decoded as it
  streams, and any other content coding fails the fetch. Bodies are abandoned once the bytes
  received or the bytes decoded would exceed 25 MiB, before the excess is held in memory.
* **robots.txt.** Fetched once per origin per run through this client, its status interpreted as
  RFC 9309 §2.3.1 sets out, and checked for the ``OpenNSE-Terminal`` product token before every
  URL. ``urllib.robotparser`` parses the file; its rules are matched as RFC 9309 §2.2 requires:
  every group naming the product token applies (case-insensitively), else every ``*`` group; the
  longest matching ``Allow`` or ``Disallow`` path wins, ``Allow`` on a tie; ``*`` matches any
  characters and a final ``$`` anchors the end.
* **No credentials.** No source needs any, so a URL carrying userinfo (``user:password@``),
  whether passed in or reached by a redirect, is refused before it is requested. Logs and
  exceptions name a URL only by :func:`describe_url`: scheme, host, port and path, never its
  userinfo, query or fragment, and never the text of a transport error. The records the httpx
  and httpcore loggers write for this client's own requests are redacted the same way: httpx's
  request line names its URL by :func:`describe_url`, and each httpcore trace keeps only its
  event name. Their levels and handlers stay as the host set them, and their records of any
  other client pass unchanged.

Failures surface as the ingestion exceptions the run executor maps to run statuses:
:class:`~opennse_terminal.ingestion.base.RobotsDisallowed` (``blocked_by_robots``) and
:class:`~opennse_terminal.ingestion.base.FetchFailed` (``failed``).
"""

import asyncio
import logging
import random
import re
import time
import zlib
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar, DefaultCookiePolicy
from types import TracebackType
from typing import Final, Self
from urllib.robotparser import RobotFileParser

import httpx

from opennse_terminal import __version__
from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion.base import FetchFailed, RobotsDisallowed

__all__ = [
    "ATTEMPT_DEADLINE_READ_TIMEOUTS",
    "BACKOFF_CAP_SECONDS",
    "CONNECT_TIMEOUT_SECONDS",
    "MAX_REDIRECTS",
    "MAX_RESPONSE_BYTES",
    "RETRY_STATUSES",
    "ROBOTS_AGENT",
    "FetchedResponse",
    "PoliteClient",
    "build_user_agent",
    "describe_url",
]

logger = logging.getLogger("opennse_terminal.ingestion.http")

ROBOTS_AGENT: Final = "OpenNSE-Terminal"
"""Product token of the user agent, matched against the ``User-agent`` lines of ``robots.txt``."""

MAX_RESPONSE_BYTES: Final = 25 * 1024 * 1024
"""Largest response body accepted, in bytes, both as received and after any ``Content-Encoding``
is removed."""

MAX_REDIRECTS: Final = 5
"""Most redirects followed for one fetch."""

RETRY_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
"""HTTP statuses retried with backoff; any other non-2xx status fails at once."""

CONNECT_TIMEOUT_SECONDS: Final = 10.0
"""Connect timeout of every request; the read timeout is ``scraper_timeout_seconds``."""

ATTEMPT_DEADLINE_READ_TIMEOUTS: Final = 4
"""Read timeouts one attempt may last beyond the connect timeout. An attempt (connecting, the
request, the response headers and the whole body) is abandoned after ``CONNECT_TIMEOUT_SECONDS +
ATTEMPT_DEADLINE_READ_TIMEOUTS * scraper_timeout_seconds`` seconds (130 s at the 30 s default),
because the read timeout bounds each socket read, not their sum."""

BACKOFF_CAP_SECONDS: Final = 60.0
"""Longest wait before a retry, whatever ``Retry-After`` asks for."""

# Connect and read timeouts, plus the other transport failures a later attempt can cure: refused
# or reset connections and a server dropping a kept-alive connection mid-response. Errors the
# client itself causes (an unsupported scheme, a local protocol error) and proxy errors are not
# retried.
_TRANSIENT_ERRORS: Final = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)

_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})
_ALLOWED_SCHEMES: Final = frozenset({"http", "https"})

_GZIP_CODINGS: Final = frozenset({"gzip", "x-gzip"})
_DECODED_CODINGS: Final = _GZIP_CODINGS | {"deflate"}
_GZIP_WINDOW_BITS: Final = 16 + zlib.MAX_WBITS
# Most bytes one decompression call may produce, so decoding holds little beyond the body itself.
_DECODE_STEP_BYTES: Final = 64 * 1024

# True while one of this client's own requests runs in the current task (and in any task it
# starts, which copies the context), so only the library records those requests cause are
# redacted; see _LibraryLogRedactor.
_OWN_REQUEST: Final[ContextVar[bool]] = ContextVar("opennse_terminal_own_request", default=False)
_HTTPX_LOGGER: Final = "httpx"
# Every logger httpcore writes its connection and protocol traces to.
_HTTPCORE_LOGGERS: Final = (
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
)


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
        content: The body with its ``gzip`` or ``deflate`` ``Content-Encoding``, if any,
            removed; at most :data:`MAX_RESPONSE_BYTES` bytes.
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


# robots.txt is parsed by urllib.robotparser, but its verdict, RobotFileParser.can_fetch, does not
# honour the file as RFC 9309 defines it: the first matching rule wins rather than the longest,
# "*" and "$" are literal characters, a group applies when its User-agent value is a substring of
# the agent name, only the first "*" group counts, and an empty line inside a group discards the
# group. So robotparser decides the groups and which lines are rules, and those groups are read
# back from it and matched here, by RFC 9309. Each rule's path, though, is taken as written on its
# own line, not from the RuleLine robotparser builds: RuleLine round-trips the path through
# urlparse and urlunparse, which rewrites it ("/*?" becomes "/*", which matches every URL;
# "/search?" becomes "/search"; "?", ";" and "//" become an empty path, which matches every URL),
# where RFC 9309 matches the path as written. Rule paths and URLs are then compared octet by octet
# in the one canonical form _robots_canonical gives both, which decodes only the escapes of
# unreserved characters: an escaped delimiter ("%2F", "%3F") stays distinct from the delimiter,
# and an escaped octet that is not UTF-8 ("%FE") from every other octet.

_ROBOTS_EOL: Final = re.compile(r"\r\n|\r|\n")
"""A ``robots.txt`` line ending: CR, LF or CRLF, RFC 9309 §2.2's ``EOL = %x0D / %x0A / %x0D.0A``.
No other character ends a line, though ``str.splitlines`` also breaks at vertical tab, form
feed, U+001C to U+001E, U+0085, U+2028 and U+2029."""

_ROBOTS_PRODUCT_TOKEN: Final = re.compile(r"[A-Za-z_-]+")
"""The product token a ``User-agent`` value starts with (RFC 9309 §2.2.1)."""

_ROBOTS_UNRESERVED: Final = frozenset(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
"""The octets of RFC 3986's unreserved characters: a percent-escape of one is decoded to it."""

_ROBOTS_LITERAL: Final = _ROBOTS_UNRESERVED | frozenset(b":/?#[]@!&'()+,;=")
"""The octets a canonical path keeps literal: the unreserved characters and RFC 3986's reserved
delimiters, except ``*`` and ``$``, which a rule writes as its wildcard and end anchor."""

_ROBOTS_OCTET: Final = re.compile(rb"%([0-9A-Fa-f]{2})|.", re.DOTALL)
"""A percent-escape, its two hex digits captured, or else any one octet."""


def _robots_canonical(octets: bytes) -> str:
    """``octets`` (a piece of a ``robots.txt`` path, or a URL's path and query) in the form rule
    paths and URLs are compared in (RFC 9309 §2.2.2, §2.2.3), so that the spellings of one URL
    compare equal and distinct URLs never do:

    * the escape of an unreserved character (``A``-``Z``, ``a``-``z``, ``0``-``9``, ``-``,
      ``.``, ``_``, ``~``) is decoded to it: ``%62%61%7A`` is ``baz``;
    * every other escape stays encoded, its hex digits uppercased: ``%2f`` is ``%2F``, which is
      not ``/``, and ``%FE`` is not ``%FF``;
    * an unreserved character, or a reserved delimiter other than ``*`` and ``$``, stays literal;
    * every other octet is percent-encoded in uppercase hex: ``*`` and ``$`` (so a rule's
      ``%2A`` and ``%24`` match them), a ``%`` that starts no escape (``%25``), space, controls,
      DEL, the other ASCII punctuation (``"<>\\^`{|}``) and the UTF-8 octets of non-ASCII
      characters: ``é`` is ``%C3%A9``.
    """
    canonical: list[str] = []
    for match in _ROBOTS_OCTET.finditer(octets):
        escape = match[1]
        if escape is not None:
            value = int(escape, 16)
            literal = value in _ROBOTS_UNRESERVED
        else:
            value = match[0][0]
            literal = value in _ROBOTS_LITERAL
        canonical.append(chr(value) if literal else f"%{value:02X}")
    return "".join(canonical)


@dataclass(frozen=True, slots=True)
class _RobotsRule:
    """One ``Allow`` or ``Disallow`` rule of ``robots.txt``, in the form it is matched in.

    Attributes:
        pieces: The rule's path, less a final ``$``, split at each ``*`` wildcard, every piece
            in the form :func:`_robots_canonical` gives it, which :func:`_robots_target` gives
            the URL too; a single piece when the path has no ``*``.
        anchored: The path ended in ``$``, so a match must reach the end of the URL.
        length: How specific the rule is (RFC 9309 §2.2.2: the match with the most octets): the
            length of its canonical pattern, the pieces joined by ``*``, plus 1 for a final
            ``$``; so a path weighs the same however it is escaped.
        allowance: True for ``Allow``, and for an empty ``Disallow``, which allows everything;
            False for ``Disallow``.
    """

    pieces: tuple[str, ...]
    anchored: bool
    length: int
    allowance: bool

    @classmethod
    def from_path(cls, path: str, allowance: bool) -> Self:
        """The rule an ``Allow`` or ``Disallow`` line sets, given its ``path`` exactly as the line
        writes it (its value, without comment or surrounding whitespace) and its ``allowance``.

        The path is split at each ``*`` and at a final ``$`` first, so ``%2A`` and ``%24`` stand
        for a literal ``*`` and ``$`` rather than a wildcard and an end anchor (RFC 9309 §2.2.3).
        Each piece, as UTF-8 octets, is then put in the form :func:`_robots_canonical` gives it,
        which encodes any other ``$`` as ``%24`` and keeps a ``%25`` escape encoded. An empty
        path gives one empty piece of length 0, which matches every URL.
        """
        anchored = path.endswith("$")
        pieces = tuple(
            _robots_canonical(piece.encode("utf-8")) for piece in path.removesuffix("$").split("*")
        )
        return cls(
            pieces=pieces,
            anchored=anchored,
            length=len("*".join(pieces)) + int(anchored),
            allowance=allowance,
        )

    def matches(self, target: str) -> bool:
        """Whether the rule matches ``target``, a URL as :func:`_robots_target` gives it, from its
        start (RFC 9309 §2.2.2, §2.2.3): ``*`` matches any run of characters and the final ``$``
        the end of the URL."""
        first, *rest = self.pieces
        if not target.startswith(first):
            return False
        if not rest:
            return not self.anchored or len(target) == len(first)
        position = len(first)
        *middle, last = rest
        # Taking each piece at its earliest occurrence leaves the most room for the ones after it.
        for piece in middle:
            found = target.find(piece, position)
            if found < 0:
                return False
            position = found + len(piece)
        if self.anchored:
            return target.endswith(last) and len(target) - len(last) >= position
        return target.find(last, position) >= 0


@dataclass(frozen=True, slots=True)
class _RobotsGroup:
    """One group of ``robots.txt``: its ``User-agent`` values and its rules, in file order."""

    agents: tuple[str, ...]
    rules: tuple[_RobotsRule, ...]

    def names_product(self) -> bool:
        """Whether a ``User-agent`` value names ``OpenNSE-Terminal``: its leading run of letters,
        ``_`` and ``-`` equals the product token, compared case-insensitively (RFC 9309 §2.2.1),
        so ``opennse-terminal/1.0`` does and ``OpenNSE`` or ``OpenNSE-Terminal-Beta`` does not."""
        product = ROBOTS_AGENT.lower()
        for agent in self.agents:
            token = _ROBOTS_PRODUCT_TOKEN.match(agent)
            if token is not None and token.group().lower() == product:
                return True
        return False


def _robotparser_changed(detail: str) -> RuntimeError:
    """The error raised when ``urllib.robotparser`` no longer builds what this module reads."""
    return RuntimeError(
        f"urllib.robotparser internals are not the ones this client reads ({detail}); "
        "robots.txt cannot be evaluated"
    )


def _written_rule_lines(lines: Iterable[str]) -> list[tuple[str, bool]]:
    """The path and kind (True for ``Allow``) of every ``Allow`` and ``Disallow`` line in
    ``lines`` that ``RobotFileParser.parse`` makes a rule of, in order, each path as written.

    Each line's field is recognised as ``parse`` recognises it: the line cut at the first ``#``,
    stripped and split at the first ``:``, the field name stripped and lowercased. The path is
    the text after that ``:`` in the cut but unstripped line, trimmed of RFC 9309 §2.2's ``WS``
    (space and tab) alone, and not percent-decoded (see :meth:`_RobotsRule.from_path`): any other
    whitespace (U+0085, U+2028, a form feed) stays part of the path, as it does inside one, so a
    trailing one cannot shorten a longer ``Allow`` to one that ties a ``Disallow``. ``parse``
    ignores a rule line it reads before any ``User-agent`` line; ``lines`` holds no empty line,
    so that is the only rule line it ignores.
    """
    written: list[tuple[str, bool]] = []
    seen_agent = False
    for line in lines:
        head = line.split("#", 1)[0]
        field, colon, _ = head.strip().partition(":")
        if not colon:
            continue
        field = field.strip().lower()
        if field == "user-agent":
            seen_agent = True
        elif seen_agent and field in ("allow", "disallow"):
            written.append((head.partition(":")[2].strip(" \t"), field == "allow"))
    return written


def _robotparser_line(line: str) -> str:
    """``line`` as ``RobotFileParser.parse`` is given it: an ``Allow`` or ``Disallow`` line with
    its value replaced by ``/`` (left empty when it is empty), a ``Crawl-delay`` or
    ``Request-rate`` line with its value removed, and any other line unchanged.

    ``parse`` converts those values, and some of them make the conversion raise ValueError:
    ``RuleLine`` runs a path through ``urlparse``, which rejects one that starts like a malformed
    authority (``//x]``, ``//[x``, or ``//`` followed by a host holding a fullwidth ``#``), and
    ``parse`` hands a crawl rate to ``int`` once ``str.isdigit`` accepts it, as it accepts ``²``.
    None of them is read back: rule paths are taken from the lines as written (see
    :func:`_written_rule_lines`) and crawl rates are not used. The field name is kept, recognised
    as ``parse`` recognises it, so ``parse`` builds the same groups as it would from ``line``.
    Whether a rule's value is empty (an empty ``Disallow`` allows everything) is kept too, judged
    on the value as :func:`_written_rule_lines` takes the path, trimmed of space and tab alone, so
    each rule ``parse`` builds has the kind its written line sets: a value of only other
    whitespace (U+2028, say) is a path, not an empty value, so its ``Disallow`` stays one.
    """
    head = line.split("#", 1)[0]
    field, colon, _ = head.strip().partition(":")
    field = field.strip().lower()
    if not colon or field not in ("allow", "disallow", "crawl-delay", "request-rate"):
        return line
    value = head.partition(":")[2].strip(" \t")
    placeholder = "/" if field in ("allow", "disallow") and value else ""
    return f"{field}: {placeholder}"


def _robots_group(entry: object, written: Iterator[tuple[str, bool]]) -> _RobotsGroup:
    """A robotparser ``Entry`` as a typed group, whose rules take their paths, in order, from
    ``written``: the rule lines of the body as :func:`_written_rule_lines` gives them, the next
    one for each ``RuleLine``.

    Raises RuntimeError unless the entry has the ``useragents`` list of str and the
    ``rulelines`` list, each with a str ``path`` and a bool ``allowance``, that CPython's
    ``Entry`` has, and unless each ``RuleLine`` pairs with a written line of its kind:
    ``allowance`` True for an ``Allow`` and for an empty ``Disallow`` (which ``RuleLine`` turns
    into one), False for any other ``Disallow``.
    """
    agents = getattr(entry, "useragents", None)
    lines = getattr(entry, "rulelines", None)
    if not isinstance(agents, list) or not all(isinstance(agent, str) for agent in agents):
        raise _robotparser_changed("Entry.useragents is not a list of str")
    if not isinstance(lines, list):
        raise _robotparser_changed("Entry.rulelines is not a list")
    rules = []
    for line in lines:
        # The path is not read, since RuleLine rewrites it, but its type shows the line is the
        # RuleLine this pairing was written against.
        path = getattr(line, "path", None)
        allowance = getattr(line, "allowance", None)
        if not isinstance(path, str) or not isinstance(allowance, bool):
            raise _robotparser_changed("a RuleLine lacks a str path or a bool allowance")
        pair = next(written, None)
        if pair is None:
            raise _robotparser_changed("RobotFileParser.parse made a rule of no rule line")
        written_path, allows = pair
        if allowance != (allows or not written_path):
            raise _robotparser_changed("a RuleLine's allowance is not the one its line sets")
        rules.append(_RobotsRule.from_path(written_path, allowance))
    return _RobotsGroup(agents=tuple(agents), rules=tuple(rules))


class _GroupRecordingParser(RobotFileParser):
    """A ``RobotFileParser`` that also records every group ``parse`` completes, in file order.

    ``RobotFileParser`` keeps only the first ``*`` group, and files a group naming both ``*`` and
    an agent under ``*`` alone, so RFC 9309's group selection needs each group as it was parsed.
    Its ``_add_entry`` hook, ``entries`` and ``Entry`` objects are untyped CPython internals (the
    type stubs declare none of them), so :meth:`groups` checks their shape as it reads them.
    """

    def __init__(self) -> None:
        super().__init__()
        self._recorded: list[object] = []

    def _add_entry(self, entry: object) -> None:
        """Record ``entry`` (a completed group), then let ``RobotFileParser`` file it as usual."""
        self._recorded.append(entry)
        add_entry = getattr(RobotFileParser, "_add_entry", None)
        if not callable(add_entry):
            raise _robotparser_changed("RobotFileParser._add_entry is missing")
        add_entry(self, entry)

    def groups(self, written: Iterable[tuple[str, bool]]) -> tuple[_RobotsGroup, ...]:
        """Every group ``parse`` completed, in file order, its rules' paths taken from
        ``written``, the body's rule lines as :func:`_written_rule_lines` gives them: in order,
        one for each rule line of each recorded group.

        Raises:
            RuntimeError: When ``entries`` is not a list, an entry or rule line lacks the
                attributes CPython gives it, the parser kept an entry (in ``entries`` or as
                ``default_entry``) that never passed through ``_add_entry``, or its rule lines
                and ``written`` do not pair one to one, in order and in kind. The fetch then
                fails instead of falling back to ``can_fetch``.
        """
        kept = getattr(self, "entries", None)
        if not isinstance(kept, list):
            raise _robotparser_changed("RobotFileParser.entries is not a list")
        default = getattr(self, "default_entry", None)
        recorded = {id(entry) for entry in self._recorded}
        if any(id(entry) not in recorded for entry in [*kept, default] if entry is not None):
            raise _robotparser_changed("RobotFileParser.parse bypassed _add_entry")
        remaining = iter(written)
        groups = tuple(_robots_group(entry, remaining) for entry in self._recorded)
        if next(remaining, None) is not None:
            raise _robotparser_changed("RobotFileParser.parse made no rule of a rule line")
        return groups


def _parse_robots_txt(text: str) -> tuple[_RobotsRule, ...]:
    """The rules a ``robots.txt`` body, ``text`` (decoded as UTF-8, RFC 9309 §2.3), sets for
    ``OpenNSE-Terminal`` (RFC 9309 §2.2.1): those of every group naming its product token,
    combined in file order; when no group does, those of every ``*`` group, combined; none when
    neither exists.

    ``urllib.robotparser`` parses the body: it decides the groups and which lines are rules.
    Each rule's path, though, is matched as its line writes it (see :func:`_written_rule_lines`),
    because the ``RuleLine`` robotparser builds rewrites the path through ``urlparse`` (``/*?``
    into ``/*``, ``/search?`` into ``/search``, ``?`` or ``;`` into an empty path that matches
    every URL). The body's lines end only at CR, LF or CRLF (:data:`_ROBOTS_EOL`, RFC 9309 §2.2),
    so a character ``str.splitlines`` would also break at (U+0085, U+2028, a form feed) stays
    part of its line, and of the path it stands in. RFC 9309 lets empty lines stand inside a
    group, but ``RobotFileParser.parse`` ends a group at one (discarding it when no rule has been
    read yet), so they are left out; a leading byte order mark is removed so that it cannot hide
    the first line's field name.
    ``parse`` also drops a group still without a rule at the end of the body, so a sentinel line
    closes it, and is given placeholders for the values it would convert but that are never read
    from it (see :func:`_robotparser_line`), so that no line makes it raise. Raises RuntimeError
    when the parser's internals are not CPython's, or when its rules and the body's rule lines do
    not pair one to one, in order and in kind.
    """
    lines = [line for line in _ROBOTS_EOL.split(text.removeprefix("\ufeff")) if line]
    # parse() files a group only once a rule-type line follows its User-agent lines, so a final
    # group with none (RFC 9309: a group that allows every URL) would be lost and a "*" group
    # applied instead. An empty Crawl-delay is a rule-type line that adds no rule: after a
    # User-agent line it files that group with no rules; after a rule it changes nothing (an empty
    # value sets no delay); before any User-agent line parse() ignores it. Being neither Allow nor
    # Disallow, it adds no written rule line either, so the pairing below is unchanged.
    lines.append("Crawl-delay:")
    parser = _GroupRecordingParser()
    parser.parse([_robotparser_line(line) for line in lines])
    groups = parser.groups(_written_rule_lines(lines))
    chosen = [group for group in groups if group.names_product()] or [
        group for group in groups if "*" in group.agents
    ]
    return tuple(rule for group in chosen for rule in group.rules)


def _robots_target(url: httpx.URL) -> str:
    """What ``robots.txt`` rules are matched against for ``url``: the octets of the path and
    query it is requested with (never the fragment), in the form :func:`_robots_canonical` gives
    each piece of a rule's path (see :meth:`_RobotsRule.from_path`), so ``/café`` and
    ``/caf%c3%a9`` are both ``/caf%C3%A9``, a literal ``*`` or ``$`` is ``%2A`` or ``%24``, and
    ``/a%2Fb`` is not ``/a/b``; ``/`` when empty."""
    return _robots_canonical(url.raw_path) or "/"


@dataclass(frozen=True, slots=True)
class _RobotsRules:
    """What one origin's ``robots.txt`` permits ``OpenNSE-Terminal``.

    ``rules`` are the ``Allow`` and ``Disallow`` rules of the groups that apply to the product
    token (see :func:`_parse_robots_txt`). There are none when no group applies, or when the file
    was unavailable, a 4xx other than 429 (RFC 9309 §2.3.1.3: no rule applies). ``unreachable``
    is set for a 5xx or 429 (§2.3.1.4: everything is disallowed). No other final status yields
    rules: loading them raises ``FetchFailed``.
    """

    rules: tuple[_RobotsRule, ...] = ()
    unreachable: bool = False

    def allows(self, target: str) -> bool:
        """Whether the rules let the product fetch ``target``, a URL as :func:`_robots_target`
        gives it (``unreachable`` is the caller's to check). The most specific matching rule
        decides: the longest (see :attr:`_RobotsRule.length`), an ``Allow`` winning a tie with a
        ``Disallow``. A URL no rule matches is allowed (RFC 9309 §2.2.2)."""
        best: _RobotsRule | None = None
        for rule in self.rules:
            if rule.matches(target) and (
                best is None or (rule.length, rule.allowance) > (best.length, best.allowance)
            ):
                best = rule
        return best is None or best.allowance


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
    """The declared ``Content-Length`` in bytes, at most ``MAX_RESPONSE_BYTES + 1``, or None when
    it is absent or not a plain integer.

    Every declaration above :data:`MAX_RESPONSE_BYTES` is reported as ``MAX_RESPONSE_BYTES + 1``,
    which is all the caller needs to reject it. Leading zeros are ignored, and a value with more
    significant digits than the cap has is never converted, so a header of any length cannot
    reach Python's integer-string conversion limit.
    """
    value = headers.get("content-length")
    if value is None:
        return None
    value = value.strip()
    if not (value.isascii() and value.isdigit()):
        return None
    digits = value.lstrip("0") or "0"
    too_large = MAX_RESPONSE_BYTES + 1
    if len(digits) > len(str(MAX_RESPONSE_BYTES)):
        return too_large
    return min(int(digits), too_large)


def _retry_after_seconds(value: str | None) -> float | None:
    """Seconds a ``Retry-After`` header asks the client to wait, capped at 60 s.

    Accepts delta-seconds (``7``, leading zeros ignored) or an HTTP-date, which is compared with
    the current UTC time; a date already past means no wait. A delta with more significant digits
    than the cap has is longer than the cap and gives the cap without being converted, so a
    header of any length cannot overflow a float or reach Python's integer-string conversion
    limit. Returns None when the header is absent or malformed, an HTTP-date with a field too
    large to represent included, so the exponential backoff applies instead.
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped.isascii() and stripped.isdigit():
        digits = stripped.lstrip("0") or "0"
        if len(digits) > len(str(int(BACKOFF_CAP_SECONDS))):
            return BACKOFF_CAP_SECONDS
        seconds = float(int(digits))
    else:
        try:
            when = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, OverflowError):
            # OverflowError: a numeric field, such as a 30-digit year, exceeds a C integer.
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


def _attempt_deadline_seconds(read_timeout: float) -> float:
    """Longest one attempt may run, in seconds, given the read timeout in seconds: the connect
    timeout plus :data:`ATTEMPT_DEADLINE_READ_TIMEOUTS` read timeouts."""
    return CONNECT_TIMEOUT_SECONDS + ATTEMPT_DEADLINE_READ_TIMEOUTS * read_timeout


def describe_url(url: str | httpx.URL) -> str:
    """Name ``url`` for a log line or an exception without anything secret it may carry.

    A URL with a host gives ``<scheme>://<host>[:<port>]<path>`` (``//<host>...`` when it has no
    scheme): the host in its ASCII (IDNA) form, the port only when it is not the scheme's
    default, and the path still percent-encoded, so no control character reaches a log line. A
    relative reference gives its path alone. Userinfo, query and fragment are always dropped.

    Never raises, and never echoes input it cannot read: an unparseable URL gives
    ``"<invalid URL>"``, and one with a scheme but no host (``mailto:...``, ``https:///x``) gives
    ``"<scheme URL without a host>"``, because some parsers read the start of such a path as
    userinfo and a host.
    """
    if isinstance(url, httpx.URL):
        parsed = url
    else:
        try:
            parsed = httpx.URL(url)
        except (httpx.InvalidURL, TypeError, ValueError):
            return "<invalid URL>"
    # raw_host, unlike host, does not decode an IDNA label, which raises for a malformed one.
    if parsed.raw_host:
        scheme = f"{parsed.scheme}:" if parsed.scheme else ""
        netloc = parsed.netloc.decode("ascii", errors="replace")
        path = parsed.raw_path.partition(b"?")[0].decode("ascii", errors="replace")
        return f"{scheme}//{netloc}{path}"
    if parsed.scheme:
        return f"<{parsed.scheme} URL without a host>"
    # A relative reference has no authority, so its text up to a query or fragment is its path.
    return str(parsed).partition("?")[0].partition("#")[0]


def _parse_url(url: str) -> httpx.URL:
    """``url`` as an absolute http(s) URL with a valid host, a port (if any) from 1 to 65535 and
    no userinfo; anything else raises FetchFailed, which names the URL by :func:`describe_url`.
    """
    try:
        parsed = httpx.URL(url)
        # Reading host decodes an IDNA ("xn--") label, raising idna's UnicodeError for a
        # malformed one.
        host = parsed.host
    except (httpx.InvalidURL, UnicodeError) as exc:
        raise FetchFailed(describe_url(url), "invalid_url") from exc
    if parsed.scheme not in _ALLOWED_SCHEMES or not host:
        raise FetchFailed(describe_url(parsed), "unsupported_url")
    if parsed.port is not None and not 0 < parsed.port <= 65535:
        # httpx accepts any digits as a port; the socket layer would then fail the request with
        # an error that is not httpx's.
        raise FetchFailed(describe_url(parsed), "invalid_url")
    if parsed.userinfo:
        # No source needs credentials, and httpx would send userinfo as Basic authentication.
        raise FetchFailed(describe_url(parsed), "url_has_userinfo")
    return parsed


def _origin(url: httpx.URL) -> str:
    """``<scheme>://<host>[:<port>]``, the scope of one ``robots.txt`` (RFC 9309 §2.3)."""
    return f"{url.scheme}://{url.netloc.decode('ascii')}"


def _content_coding(url: str, content_encoding: str | None) -> str | None:
    """The content coding to remove from a body, given its ``Content-Encoding`` header value:
    None when the value is absent or names only ``identity``, else ``gzip``, ``x-gzip`` or
    ``deflate``. Any other coding, or a stack of several, raises FetchFailed for ``url`` with
    reason ``"unsupported_content_encoding"``."""
    named = [coding.strip().lower() for coding in (content_encoding or "").split(",")]
    applied = [coding for coding in named if coding and coding != "identity"]
    if not applied:
        return None
    if len(applied) > 1 or applied[0] not in _DECODED_CODINGS:
        raise FetchFailed(url, "unsupported_content_encoding")
    return applied[0]


def _deflate_window_bits(head: bytes) -> int:
    """The zlib ``wbits`` for a ``deflate`` body starting with the two bytes ``head``.

    ``deflate`` names a zlib stream (RFC 9110 §8.4.1.2), but some servers send raw DEFLATE data;
    a valid zlib header (RFC 1950 §2.2: method 8, a window of at most 32 KiB and a check value
    making the first two bytes a multiple of 31) tells the two apart."""
    method_and_window, flags = head[0], head[1]
    if (
        method_and_window & 0x0F == 8
        and method_and_window >> 4 <= 7
        and (method_and_window << 8 | flags) % 31 == 0
    ):
        return zlib.MAX_WBITS
    return -zlib.MAX_WBITS


class _BodyReader:
    """Assembles one streamed response body from its raw chunks, removing its content coding.

    Neither the bytes received nor the decoded body may exceed :data:`MAX_RESPONSE_BYTES`, and
    each is checked before anything is kept. One decompression call produces at most
    :data:`_DECODE_STEP_BYTES`, and never more than one byte past the remaining capacity, so a
    small compressed body that expands enormously is rejected while memory holds about the cap,
    never its expanded size. A ``gzip`` body may hold several members, decoded in turn (RFC 1952
    §2.2); a ``deflate`` body is one stream.

    Raises:
        FetchFailed: For ``url``, with reason ``"response_too_large"`` once either count would
            pass the cap, or ``"invalid_content_encoding"`` for compressed data that is corrupt,
            truncated, or followed by more data after a ``deflate`` stream has ended.
    """

    def __init__(self, url: str, coding: str | None) -> None:
        self._url = url
        self._coding = coding
        # Created at once for gzip; for deflate once its first two bytes have arrived.
        self._decompressor = (
            zlib.decompressobj(_GZIP_WINDOW_BITS) if coding in _GZIP_CODINGS else None
        )
        self._head = b""
        self._received = 0
        self._body = bytearray()

    def feed(self, chunk: bytes) -> None:
        """Take the next raw chunk of the body, as received."""
        self._received += len(chunk)
        if self._received > MAX_RESPONSE_BYTES:
            raise FetchFailed(self._url, "response_too_large")
        if self._coding is None:
            self._keep(chunk)
            return
        decompressor = self._decompressor
        if decompressor is None:
            self._head += chunk
            if len(self._head) < 2:
                return
            chunk, self._head = self._head, b""
            decompressor = self._decompressor = zlib.decompressobj(_deflate_window_bits(chunk))
        data = chunk
        while True:
            if decompressor.eof:
                if not data:
                    return
                if self._coding not in _GZIP_CODINGS:
                    raise FetchFailed(self._url, "invalid_content_encoding")
                decompressor = self._decompressor = zlib.decompressobj(_GZIP_WINDOW_BITS)
            limit = min(_DECODE_STEP_BYTES, MAX_RESPONSE_BYTES - len(self._body) + 1)
            try:
                decoded = decompressor.decompress(data, limit)
            except zlib.error as exc:
                raise FetchFailed(self._url, "invalid_content_encoding") from exc
            self._keep(decoded)
            # Input left over because the output limit was reached, or after the end of a stream.
            data = decompressor.unused_data if decompressor.eof else decompressor.unconsumed_tail
            # A full output step may leave decoded bytes pending inside zlib with no input left;
            # the next round drains them.
            if not data and len(decoded) < limit:
                return

    def finish(self) -> bytes:
        """The decoded body, once its last chunk has been fed. A coded body that stops before
        the end of its compressed stream raises FetchFailed (``"invalid_content_encoding"``);
        an empty body is empty whatever its coding."""
        if self._coding is not None and self._received:
            if self._decompressor is None or not self._decompressor.eof:
                raise FetchFailed(self._url, "invalid_content_encoding")
        return bytes(self._body)

    def _keep(self, data: bytes) -> None:
        """Append ``data`` to the body, unless that would take it past the cap."""
        if len(data) > MAX_RESPONSE_BYTES - len(self._body):
            raise FetchFailed(self._url, "response_too_large")
        self._body += data


class _RedirectReceived(Exception):  # noqa: N818 - a control-flow signal, not an error
    """A redirect response, handed from :func:`_stop_at_redirect` to the request that got it.

    Attributes:
        url: The URL that answered with the redirect.
        status_code: Its 301, 302, 303, 307 or 308 status.
        headers: Its headers, ``Location`` unparsed among them.
    """

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(response.status_code)
        self.url = response.url
        self.status_code = response.status_code
        self.headers = httpx.Headers(response.headers)


async def _stop_at_redirect(response: httpx.Response) -> None:
    """httpx response hook: end the request at a redirect, before httpx parses its ``Location``.

    httpx resolves the ``Location`` of every redirect it receives, even when it does not follow
    redirects, and one it cannot use escapes the request as ``httpx.InvalidURL`` (``data:`` or
    ``javascript:``) or as a ``RemoteProtocolError`` (an unparseable URL) that would be retried
    like a dropped connection. httpx closes the response and re-raises what a hook raises, so the
    redirect reaches :meth:`PoliteClient._get` with its ``Location`` unread, to be resolved and
    refused there with a ``FetchFailed``.
    """
    if response.has_redirect_location:
        raise _RedirectReceived(response)


class _LibraryLogRedactor(logging.Filter):
    """Holds the httpx and httpcore records of this client's own requests to what
    :func:`describe_url` shows, as this module's own log lines are.

    httpx logs every request at INFO with its full URL, query and fragment included, and httpcore
    traces every step at DEBUG with its arguments and results, raw response headers (a redirect's
    ``Location`` userinfo among them) included. While :data:`_OWN_REQUEST` is set, every
    ``httpx.URL`` argument of an httpx record is replaced by its :func:`describe_url` form, and an
    httpcore record is cut to its event name (``receive_response_headers.complete``). Records of
    any other request, a host application's own httpx clients included, pass unchanged. No record
    is ever dropped, and no logger's level or handlers are changed.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact ``record`` in place when it belongs to this client's own request; keep every
        record."""
        if not _OWN_REQUEST.get():
            return True
        if record.name == _HTTPX_LOGGER:
            if isinstance(record.args, tuple):
                record.args = tuple(
                    describe_url(arg) if isinstance(arg, httpx.URL) else arg for arg in record.args
                )
        else:
            # httpcore preformats each trace as "<event> <name>=<repr(value)> ...".
            record.msg = str(record.msg).partition(" ")[0]
            record.args = ()
        return True


def _redact_library_logs() -> None:
    """Attach the one :class:`_LibraryLogRedactor` to the httpx and httpcore loggers. Attaching
    it again is a no-op, because ``addFilter`` skips a filter already attached."""
    for name in (_HTTPX_LOGGER, *_HTTPCORE_LOGGERS):
        logging.getLogger(name).addFilter(_LIBRARY_LOG_REDACTOR)


_LIBRARY_LOG_REDACTOR: Final = _LibraryLogRedactor()
_redact_library_logs()


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
        """Open the run's ``httpx.AsyncClient``: the user agent, ``Accept-Encoding: identity``, a
        10 s connect and ``scraper_timeout_seconds`` read timeout, no automatic redirects (a
        response hook hands each redirect back unresolved, see :func:`_stop_at_redirect`), and a
        cookie jar whose policy accepts neither Netscape nor RFC 2965 cookies, so a
        ``Set-Cookie`` is never stored and no ``Cookie`` header is ever sent. The ``robots.txt``
        cache starts empty, so every run reads each origin's file afresh."""
        if self._client is not None:
            raise RuntimeError("PoliteClient is already open")
        self._host_locks.clear()
        self._robots.clear()
        self._robots_locks.clear()
        self._client = httpx.AsyncClient(
            headers={"User-Agent": self._user_agent, "Accept-Encoding": "identity"},
            timeout=httpx.Timeout(
                self._settings.scraper_timeout_seconds, connect=CONNECT_TIMEOUT_SECONDS
            ),
            follow_redirects=False,
            event_hooks={"response": [_stop_at_redirect]},
            cookies=CookieJar(DefaultCookiePolicy(netscape=False, rfc2965=False)),
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

        * 2xx: the body is read as UTF-8 whatever charset it declares (RFC 9309 §2.3), its
          rules parsed with ``urllib.robotparser`` and applied as described below;
        * 5xx after retries: unreachable, so every URL of the origin is disallowed. A 429 after
          retries is treated the same way, because being rate-limited is not permission to crawl;
        * any other 4xx, 404 included: unavailable, so no rule applies;
        * any other status (a 301, 302, 303, 307 or 308 without a ``Location``, a 3xx that is not
          followed such as 300 or 304, or a status outside 200-599): it grants nothing, so
          ``FetchFailed`` is raised and no verdict is kept for the origin.

        The rules applied are matched as RFC 9309 §2.2 sets out, not by
        ``RobotFileParser.can_fetch``, whose first-match, literal matching does not:

        * the groups applied are every one with a ``User-agent`` naming the product token (the
          value's leading letters, ``_`` and ``-``, compared case-insensitively), combined, or,
          when there is none, every ``*`` group, combined; with neither, every URL is allowed;
        * each rule's path is matched from the start of the URL's path and query as requested
          (the fragment ignored, percent-encoding normalized), ``*`` matching any characters
          and a final ``$`` the end of the URL;
        * the longest matching path decides, an ``Allow`` winning a tie with a ``Disallow``, and
          a URL no rule matches is allowed.

        Raises:
            RobotsDisallowed: With reason ``"disallowed"`` when the rules block the URL, or
                ``"robots_unreachable"`` when ``robots.txt`` was unreachable. Logged at WARNING.
            RuntimeError: When ``urllib.robotparser`` no longer builds the internals the rules
                are read from; no verdict is kept for the origin.
            FetchFailed: When ``url`` is not an absolute http(s) URL or carries userinfo
                (``"url_has_userinfo"``), ``robots.txt`` could not be fetched at all (network
                failure or the attempt deadline after retries, too many redirects, a redirect
                whose target is invalid or not http(s), oversize, an undecodable body; with the
                reasons :meth:`fetch` lists), or its final status is none of 2xx, 4xx and 5xx
                (reason ``"robots_http_<status>"`` with the ``robots.txt`` URL; logged at
                WARNING). No verdict is kept for the origin.
        """
        parsed = _parse_url(url)
        origin = _origin(parsed)
        described = describe_url(parsed)
        rules = await self._robots_rules(origin)
        if rules.unreachable:
            logger.warning(
                "robots.txt of %s is unreachable; treating %s as disallowed", origin, described
            )
            raise RobotsDisallowed(described, "robots_unreachable")
        # Judged by the URL as it would be requested (dot segments removed), not as passed in.
        if not rules.allows(_robots_target(parsed)):
            logger.warning("robots.txt of %s disallows %s for %s", origin, described, ROBOTS_AGENT)
            raise RobotsDisallowed(described)

    async def fetch(self, url: str) -> FetchedResponse:
        """GET ``url`` politely and return the final 2xx response.

        Checks ``robots.txt`` first and again for every redirect hop; spaces every request to a
        host; retries transient failures and retryable statuses with backoff; follows at most
        :data:`MAX_REDIRECTS` redirects; asks for an unencoded body and decodes a ``gzip`` or
        ``deflate`` one as it streams, reading and keeping at most :data:`MAX_RESPONSE_BYTES`.

        Raises:
            RobotsDisallowed: ``robots.txt`` blocks the URL or a redirect target, which is then
                not requested.
            RuntimeError: ``urllib.robotparser`` no longer builds the internals the rules are
                read from (see :meth:`ensure_allowed`); the URL or redirect target being checked
                is then not requested.
            FetchFailed: With reason ``"http_<status>"`` for a final non-2xx status (a retryable
                one after the last retry, any other at once), ``"<ExceptionName> after <n>
                attempts"`` when transient network errors persist, ``"DeadlineExceeded after <n>
                attempts"`` when the last attempt ran past its deadline (see
                :data:`ATTEMPT_DEADLINE_READ_TIMEOUTS`), ``"too_many_redirects"``,
                ``"response_too_large"`` (the body as received or decoded passes the cap),
                ``"unsupported_content_encoding"`` (a ``Content-Encoding`` other than ``gzip``,
                ``x-gzip`` or ``deflate``, or more than one), ``"invalid_content_encoding"``
                (corrupt or truncated compressed data), ``"invalid_redirect"`` (a redirect's
                ``Location`` is not a URL reference), ``"invalid_url"`` (the URL or a redirect
                target is malformed: an unparseable host or a port outside 1-65535),
                ``"unsupported_url"`` (it is not an absolute http(s) URL with a host, as a
                ``data:`` or ``javascript:`` redirect is not), ``"url_has_userinfo"`` (it
                carries userinfo), and ``"robots_http_<status>"`` when ``robots.txt`` grants
                nothing (see :meth:`ensure_allowed`). A refused redirect fails at once and its
                target is never requested.

        ``RobotsDisallowed`` and ``FetchFailed``, and every log line, name the URL by
        :func:`describe_url`.
        """
        response = await self._get(url, check_robots=True)
        if not 200 <= response.status_code < 300:
            raise FetchFailed(describe_url(response.url), f"http_{response.status_code}")
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
            # RFC 9309 §2.3: the file MUST be UTF-8, so it is read as UTF-8 whatever charset its
            # Content-Type declares; an undecodable byte becomes U+FFFD, never a lone surrogate.
            rules = _parse_robots_txt(response.content.decode("utf-8", errors="replace"))
            logger.debug(
                "robots.txt of %s loaded (%d bytes, %d rules for %s)",
                origin,
                len(response.content),
                len(rules),
                ROBOTS_AGENT,
            )
            return _RobotsRules(rules=rules)
        # RFC 9309 lets a 4xx mean "no rules", but a 429 is a rate limit, not permission to
        # crawl, so it is treated like a 5xx: the whole origin is disallowed for this run.
        if 500 <= status < 600 or status == 429:
            return _RobotsRules(unreachable=True)
        if 400 <= status < 500:
            logger.debug("robots.txt of %s unavailable (HTTP %d); no rules apply", origin, status)
            return _RobotsRules()
        # Only a 4xx means "unavailable". A redirect left without a Location, a 3xx that is not
        # followed (300, 304, 305, 306) or a status outside 200-599 grants nothing, so the fetch
        # fails, as a redirect chain that is too long does, and no verdict is cached.
        logger.warning(
            "robots.txt of %s answered HTTP %d, which is neither rules nor unavailable; "
            "failing the fetch",
            origin,
            status,
        )
        raise FetchFailed(robots_url, f"robots_http_{status}")

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
                raise FetchFailed(describe_url(url), "too_many_redirects")
            redirects += 1
            try:
                target = current.join(location)
            except httpx.InvalidURL as exc:
                raise FetchFailed(describe_url(url), "invalid_redirect") from exc
            current = _parse_url(str(target))

    async def _send(self, url: httpx.URL) -> FetchedResponse:
        """One request with up to ``scraper_max_retries`` retries. Returns the last response,
        even when it still has a retryable status, so callers can interpret it.

        Each attempt, but not the spacing wait before it, is bounded by the attempt deadline
        (see :data:`ATTEMPT_DEADLINE_READ_TIMEOUTS`); one that runs past it is abandoned and
        retried as a read timeout is, with ``DeadlineExceeded`` as its cause."""
        client = self._require_client()
        attempts = 1 + self._settings.scraper_max_retries
        deadline_seconds = _attempt_deadline_seconds(self._settings.scraper_timeout_seconds)
        attempt = 1
        while True:
            await self._wait_turn(url.host)
            deadline = asyncio.timeout(deadline_seconds)
            try:
                async with deadline:
                    response = await self._attempt(client, url)
            except (*_TRANSIENT_ERRORS, TimeoutError) as exc:
                # The deadline raises TimeoutError; one raised inside the attempt while the
                # deadline still stood is not a timeout of this client, so it propagates as is.
                if isinstance(exc, TimeoutError) and not deadline.expired():
                    raise
                # The exception's text can quote the request URL, so only its type is logged.
                cause = "DeadlineExceeded" if isinstance(exc, TimeoutError) else type(exc).__name__
                if attempt >= attempts:
                    raise FetchFailed(
                        describe_url(url), f"{cause} after {attempts} attempts"
                    ) from exc
                delay = _backoff_seconds(attempt, None)
            except httpx.HTTPError as exc:
                raise FetchFailed(describe_url(url), type(exc).__name__) from exc
            else:
                if response.status_code not in RETRY_STATUSES or attempt >= attempts:
                    return response
                cause = f"HTTP {response.status_code}"
                retry_after = _retry_after_seconds(response.headers.get("retry-after"))
                delay = _backoff_seconds(attempt, retry_after)
            logger.warning(
                "GET %s attempt %d of %d failed (%s); retrying in %.1f s",
                describe_url(url),
                attempt,
                attempts,
                cause,
                delay,
            )
            await self._sleep(delay)
            attempt += 1

    async def _attempt(self, client: httpx.AsyncClient, url: httpx.URL) -> FetchedResponse:
        """Stream one GET. Only a 2xx body is read: its raw stream is decoded as it arrives and
        abandoned once the bytes received or decoded would pass :data:`MAX_RESPONSE_BYTES` (or
        the declared ``Content-Length`` already does); see :class:`_BodyReader`. A redirect is
        returned with no body and its ``Location`` unresolved (see :func:`_stop_at_redirect`).
        The httpx and httpcore records the request causes are redacted (see
        :class:`_LibraryLogRedactor`)."""
        own_request = _OWN_REQUEST.set(True)
        try:
            async with client.stream("GET", url) as response:
                final_url = str(response.url)
                described = describe_url(response.url)
                status = response.status_code
                headers = httpx.Headers(response.headers)
                if not response.is_success:
                    # Errors, retryable statuses and 3xx responses that are not followed need no
                    # body; leaving the block closes the stream unread.
                    logger.debug("GET %s -> %d", described, status)
                    return FetchedResponse(final_url, status, headers, b"")
                declared = _content_length(headers)
                if declared is not None and declared > MAX_RESPONSE_BYTES:
                    raise FetchFailed(described, "response_too_large")
                coding = _content_coding(described, headers.get("content-encoding"))
                if response.is_stream_consumed:
                    # A transport that builds its responses in memory (httpx.MockTransport given
                    # byte content) hands them over already read, with any coding already
                    # removed; no stream is left to bound, so only the decoded size is checked.
                    body = response.content
                    if len(body) > MAX_RESPONSE_BYTES:
                        raise FetchFailed(described, "response_too_large")
                else:
                    # The raw stream, decoded here: httpx's own decoders would expand each chunk
                    # without limit before the size could be checked.
                    reader = _BodyReader(described, coding)
                    async for chunk in response.aiter_raw():
                        reader.feed(chunk)
                    body = reader.finish()
        except _RedirectReceived as redirect:
            # httpx has already closed the redirect's stream unread.
            logger.debug("GET %s -> %d", describe_url(redirect.url), redirect.status_code)
            return FetchedResponse(str(redirect.url), redirect.status_code, redirect.headers, b"")
        finally:
            _OWN_REQUEST.reset(own_request)
        logger.debug("GET %s -> %d (%d bytes)", described, status, len(body))
        return FetchedResponse(final_url, status, headers, body)

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
