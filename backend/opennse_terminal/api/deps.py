"""Dependency providers of the terminal router, each one an override target for a host.

A host replaces any of them through ``app.dependency_overrides[<provider>] = <replacement>``:

* ``get_terminal_settings`` -> a zero-argument callable returning ``TerminalSettings(...)`` built
  from the host's own configuration.
* ``get_terminal_session`` -> the host's own provider yielding a SQLAlchemy ``AsyncSession`` on the
  same PostgreSQL database through asyncpg.
* ``get_terminal_principal`` -> a provider mapping the host's authenticated user to a
  ``TerminalPrincipal`` subject, or raising ``HTTPException(401)``. The subject must be 1-255
  characters with no NUL character, which ``terminal_watchlists.owner_key`` can store; the
  principal refuses any other subject with ``ValueError`` (``TypeError`` for a non-``str``).
* ``require_ingest_permission`` -> a provider that raises ``HTTPException(403)`` unless the caller
  may ingest filings (for example, an administrator).

Overrides are keyed by function identity, so the aliases below wrap these exact functions and
``opennse_terminal.api`` re-exports the same objects. Authentication stays pluggable: the terminal
has no user model, user lookup, token parsing or auth table, only the opaque principal subject.

:class:`TerminalJsonRoute` is the route class of every terminal sub-router that binds a JSON body.
It bounds the body to :data:`JSON_BODY_MAX_BYTES` (413 ``request_too_large``) and answers every
undecodable body with FastAPI's ``json_invalid`` 422, and it raises either verdict only after the
router-level and host dependencies have run. The order is host dependencies (the principal), then
the body verdict, then validation, so a host principal override that raises 401 answers 401
whatever the body holds.

Annotations here are evaluated at runtime (no ``from __future__ import annotations``), because
FastAPI reads the ``Annotated`` dependency metadata of every alias used in a route signature.
"""

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Any, Final

from fastapi import Depends, HTTPException, Request, Response, params, status
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import ClientDisconnect
from starlette.types import Message, Receive

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import session_factory
from opennse_terminal.schemas.common import ErrorDetail

__all__ = [
    "JSON_BODY_MAX_BYTES",
    "PrincipalDep",
    "SessionDep",
    "SettingsDep",
    "TerminalJsonRoute",
    "TerminalPrincipal",
    "get_terminal_principal",
    "get_terminal_session",
    "get_terminal_settings",
    "require_ingest_permission",
]

logger = logging.getLogger("opennse_terminal.api.deps")

# Longest subject, in characters: the length of terminal_watchlists.owner_key (VARCHAR(255)) and
# the max_length of TerminalSettings.default_owner_key. This module imports no model, so the value
# is restated here rather than read from the column.
_SUBJECT_MAX_LENGTH: Final = 255


@dataclass(frozen=True, slots=True)
class TerminalPrincipal:
    """The caller of a terminal endpoint, as resolved by ``get_terminal_principal``.

    ``subject`` is the opaque owner key stored in ``terminal_watchlists.owner_key``; the terminal
    never parses or interprets it. ``is_authenticated`` reports whether a host authenticated the
    caller; the standalone default principal is unauthenticated.

    Construction refuses a subject the owner key cannot store: a non-``str`` subject raises
    ``TypeError``, and an empty subject, one longer than 255 characters or one containing a NUL
    character raises ``ValueError``. The subject is stored exactly as given, never trimmed or
    case-folded, so ``"LOCAL"`` and ``" local"`` are subjects distinct from ``"local"``. Error
    messages state the rule and the length, never the subject itself.

    The class satisfies the structural ``OwnerPrincipal`` protocol of the watchlist service (a
    read-only ``subject``), so services accept it without importing this package.
    """

    subject: str
    is_authenticated: bool

    def __post_init__(self) -> None:
        """Refuse a subject that ``terminal_watchlists.owner_key`` cannot store.

        Raises:
            TypeError: If ``subject`` is not a ``str``.
            ValueError: If ``subject`` is empty, longer than 255 characters, or contains a NUL
                character.
        """
        if not isinstance(self.subject, str):
            raise TypeError(
                f"TerminalPrincipal.subject must be a str, got {type(self.subject).__name__}"
            )
        length = len(self.subject)
        if not 1 <= length <= _SUBJECT_MAX_LENGTH:
            raise ValueError(
                f"TerminalPrincipal.subject must be 1-{_SUBJECT_MAX_LENGTH} characters, "
                f"got {length}"
            )
        nul_index = self.subject.find("\x00")
        if nul_index >= 0:
            raise ValueError(
                "TerminalPrincipal.subject must not contain a NUL character, "
                f"found one at index {nul_index}"
            )


@lru_cache(maxsize=1)
def get_terminal_settings() -> TerminalSettings:
    """Return the API process's ``TerminalSettings``, read once and cached.

    Standalone, the settings come from ``TERMINAL_*`` environment variables and ``.env``. A host
    supplies its own with
    ``app.dependency_overrides[get_terminal_settings] = lambda: TerminalSettings(...)``; the
    override must take no arguments, because the upload size guard calls it directly.
    """
    return TerminalSettings()


SettingsDep = Annotated[TerminalSettings, Depends(get_terminal_settings)]


async def get_terminal_session(settings: SettingsDep) -> AsyncIterator[AsyncSession]:
    """Yield one ``AsyncSession`` per request from the process-wide session factory.

    The engine behind the factory is created on the first request and cached per
    ``database_url``, so a host that overrides this provider never creates a terminal engine.
    Nothing is committed here: write services call ``await session.commit()`` themselves, and
    closing the session on exit rolls back anything left uncommitted.

    Host contract: an override is an async generator or function that yields or returns a
    SQLAlchemy ``AsyncSession`` bound to the same PostgreSQL database through asyncpg.
    """
    factory = session_factory(settings)
    async with factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_terminal_session)]


async def get_terminal_principal(settings: SettingsDep) -> TerminalPrincipal:
    """Return the default, unauthenticated principal ``settings.default_owner_key`` (``local``).

    ``terminal_router`` declares ``dependencies=[Depends(get_terminal_principal)]``, so a host
    override that maps its own user to a subject, or raises ``HTTPException(401)``, applies to
    every terminal endpoint.

    Host contract: an override returns a ``TerminalPrincipal`` whose subject is 1-255 characters
    with no NUL character; ``TerminalPrincipal`` raises ``ValueError`` for any other subject.
    ``TerminalSettings`` enforces the same rule on ``default_owner_key``, so the default never
    raises.
    """
    return TerminalPrincipal(subject=settings.default_owner_key, is_authenticated=False)


PrincipalDep = Annotated[TerminalPrincipal, Depends(get_terminal_principal)]


async def require_ingest_permission(principal: PrincipalDep) -> None:
    """Guard ``POST /ingest/pdf``; the default allows every caller.

    A host overrides it to require an administrator and raise ``HTTPException(403)`` otherwise.
    ``principal`` is unused by the default on purpose: declaring it makes FastAPI resolve the
    (possibly host-overridden) principal first, and gives an override's signature the caller.
    """
    return None


_MAX_JSON_NESTING: Final = 200
"""Deepest array or object nesting a terminal JSON body may have, as in pydantic-core's own JSON
parser. Terminal bodies nest at most two levels. FastAPI renders a rejected input with an encoder
that recurses once per level, which exhausts Python's recursion limit near 1,000 levels."""

_JSON_NESTING_MESSAGE: Final = f"JSON nesting exceeds the {_MAX_JSON_NESTING}-level limit"
_LONE_SURROGATE_MESSAGE: Final = "JSON text contains an unpaired UTF-16 surrogate"


def _has_lone_surrogate(text: str) -> bool:
    """Whether ``text`` holds a surrogate code point, which no UTF-8 response can carry.

    The standard-library decoder keeps an unpaired ``\\ud800``-style escape, or the raw bytes of
    one, as a lone surrogate; a valid escaped pair is decoded to a single code point instead.
    """
    if text.isascii():
        return False
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return False


def _undecodable_reason(value: object) -> str | None:
    """Why a decoded JSON body cannot be answered as a JSON 422, or ``None`` when it can.

    The standard-library decoder accepts two inputs on which FastAPI's error rendering fails with
    a 500: arrays or objects nested more than :data:`_MAX_JSON_NESTING` levels, and strings or
    keys holding an unpaired surrogate. Pydantic-core's own JSON parser rejects both. The walk is
    iterative, so it cannot exhaust the interpreter stack itself.
    """
    pending: list[tuple[object, int]] = [(value, 0)]
    while pending:
        node, depth = pending.pop()
        if isinstance(node, str):
            if _has_lone_surrogate(node):
                return _LONE_SURROGATE_MESSAGE
        elif isinstance(node, dict | list):
            if depth >= _MAX_JSON_NESTING:
                return _JSON_NESTING_MESSAGE
            children = [*node, *node.values()] if isinstance(node, dict) else node
            pending.extend((child, depth + 1) for child in children)
    return None


def _json_invalid_error(exc: json.JSONDecodeError) -> RequestValidationError:
    """FastAPI's own answer to an undecodable JSON body, built as its request handler builds it.

    The single ``json_invalid`` entry sits at ``loc = ("body", <position>)`` with the decoder's
    message as ``ctx.error``, and ``body`` holds the decoded text, so FastAPI's default handler
    and a host's ``RequestValidationError`` handler receive exactly what FastAPI would raise.
    """
    error = RequestValidationError(
        [
            {
                "type": "json_invalid",
                "loc": ("body", exc.pos),
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": exc.msg},
            }
        ],
        body=exc.doc,
    )
    error.__cause__ = exc
    return error


JSON_BODY_MAX_BYTES: Final = 65_536
"""Largest JSON request body, in bytes, that a :class:`TerminalJsonRoute` accepts (64 KiB).

A larger body is answered 413 ``request_too_large``. Valid bodies are far smaller, even with every
character written as a ``\\u`` escape: a ``WatchlistCreate`` (a 100-character name and 100 tickers
of up to 16 characters) is at most about 21 KB, and an ``IpoValuationRequest`` (28-digit decimals
and both 9-value sensitivity axes) under 4 KB."""

_DRAIN_ALLOWANCE_BYTES: Final = 1_048_576
"""Bytes past :data:`JSON_BODY_MAX_BYTES` that an over-limit body is drained at most (1 MiB)."""

_DRAIN_TIMEOUT_SECONDS: Final = 1.0
"""Seconds an over-limit body is drained at most: long enough for what a proxy that forwarded just
past the limit still has in flight, short enough that a stalled sender gets its 413 promptly."""

# FastAPI's own answer to a body it cannot read, reused when the client disconnects mid-body.
_BODY_ERROR_MESSAGE: Final = "There was an error parsing the body"
# ASGI scope key under which TerminalJsonRoute records the verdict on a request's body until
# _raise_body_verdict raises it.
_BODY_VERDICT_SCOPE_KEY: Final = "opennse_terminal.json_body_verdict"
# What _TerminalJsonRequest.json() returns for an undecodable body. _raise_body_verdict raises
# that body's verdict before validation, so the placeholder never reaches a model.
_UNDECODABLE_BODY: Final = object()


class _BodyTooLargeError(Exception):
    """Raised by the counting ``receive`` once a body passes :data:`JSON_BODY_MAX_BYTES`."""


class _TerminalJsonRequest(Request):
    """A request whose JSON decoding never raises: an undecodable body records its verdict.

    FastAPI decodes the body before it resolves any dependency. It answers only
    ``json.JSONDecodeError`` from decoding with its standard 422 list; any other failure of the
    standard-library decoder becomes a 400 with a string ``detail``. Each such failure is first
    turned into a ``JSONDecodeError`` that names the cause without Python's own advice text:

    * text that is not valid in its detected encoding (``UnicodeDecodeError``), positioned at the
      first invalid character;
    * nesting too deep for the decoder (``RecursionError``), or deeper than
      :data:`_MAX_JSON_NESTING` levels, and strings holding an unpaired surrogate
      (see :func:`_undecodable_reason`);
    * an integer literal longer than ``sys.get_int_max_str_digits()`` digits (``ValueError``).

    Every ``JSONDecodeError`` is then recorded on the request as FastAPI's own ``json_invalid``
    verdict (:func:`_json_invalid_error`) instead of being raised, and :meth:`json` returns a
    private placeholder. :func:`_raise_body_verdict` raises the verdict once the host
    dependencies have run. Every other body decodes exactly as before.
    """

    async def json(self) -> Any:
        """Return the decoded JSON body; for an undecodable one, record the ``json_invalid``
        verdict on the request and return a placeholder that never reaches validation."""
        try:
            return await self._decode_json()
        except json.JSONDecodeError as exc:
            self.scope[_BODY_VERDICT_SCOPE_KEY] = _json_invalid_error(exc)
            return _UNDECODABLE_BODY

    async def _decode_json(self) -> Any:
        """Return the decoded JSON body, or raise ``json.JSONDecodeError`` for any failure."""
        try:
            decoded = await super().json()
        except json.JSONDecodeError:
            raise
        except UnicodeDecodeError as exc:
            # A subclass of ValueError, so it is handled before the integer-limit branch.
            body = await self.body()
            raise json.JSONDecodeError(
                f"Invalid {exc.encoding} text: {exc.reason}",
                body.decode(exc.encoding, "replace"),
                len(body[: exc.start].decode(exc.encoding, "replace")),
            ) from exc
        except RecursionError as exc:
            body = await self.body()
            raise json.JSONDecodeError(
                _JSON_NESTING_MESSAGE, body.decode("utf-8", "replace"), 0
            ) from exc
        except ValueError as exc:
            # The decoder's only other ValueError: int() refusing a literal over the digit limit.
            body = await self.body()
            raise json.JSONDecodeError(
                f"Integer literal exceeds the {sys.get_int_max_str_digits()}-digit limit",
                body.decode("utf-8", "replace"),
                0,
            ) from exc
        reason = _undecodable_reason(decoded)
        if reason is not None:
            body = await self.body()
            raise json.JSONDecodeError(reason, body.decode("utf-8", "replace"), 0)
        return decoded


def _declares_length_within_bound(header: str) -> bool:
    """Whether a ``Content-Length`` value is a decimal integer of at most the body bound.

    Only ASCII digits count, as in HTTP: a sign, whitespace, an underscore or a non-ASCII digit,
    all of which ``int()`` would accept, makes the value invalid, as does an empty value. Any
    number of leading zeroes is valid and leaves the value unchanged. The comparison holds for a
    value of any length: leading zeroes are dropped, a value with more significant digits than
    the bound is over it, and only the remaining few digits are converted, so a value longer than
    ``sys.get_int_max_str_digits()`` digits never reaches ``int()``.
    """
    if not (header.isascii() and header.isdigit()):
        return False
    significant = header.lstrip("0")
    if len(significant) > len(str(JSON_BODY_MAX_BYTES)):
        return False
    return int(significant or "0") <= JSON_BODY_MAX_BYTES


async def _discard_body(receive: Receive, allowance: int) -> None:
    """Read and drop the rest of a request body whose end has not been received yet.

    Messages are read until one ends the body (``more_body`` false), one is not
    ``http.request`` (a disconnect), more than ``allowance`` bytes have been read, or
    :data:`_DRAIN_TIMEOUT_SECONDS` have passed since the drain began; a negative ``allowance``
    reads nothing. No byte is kept. The deadline bounds the whole drain, so a sender that stalls
    or trickles cannot hold the request. Never call it once the body has ended: an ASGI server's
    ``receive`` then waits for the client to disconnect. An external cancellation
    (``asyncio.CancelledError``) propagates.
    """
    discarded = 0
    deadline = asyncio.timeout(_DRAIN_TIMEOUT_SECONDS)
    try:
        async with deadline:
            while discarded <= allowance:
                message = await receive()
                if message["type"] != "http.request":
                    return
                discarded += len(message.get("body", b""))
                if not message.get("more_body", False):
                    return
    except TimeoutError:
        # Only the deadline's own expiry ends the drain; a TimeoutError raised by ``receive``
        # while the deadline still stood propagates as is.
        if not deadline.expired():
            raise


def _withhold_body(request: Request) -> Request:
    """Record the 413 ``request_too_large`` verdict and return a request with an empty body.

    FastAPI reads no byte of the refused body from the returned request, so it decodes nothing,
    and :func:`_raise_body_verdict` answers 413 once the host dependencies have let the caller
    through. The first ``receive`` of the returned request ends the body with no bytes; every
    later one is the server's own.
    """
    request.scope[_BODY_VERDICT_SCOPE_KEY] = HTTPException(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        detail=ErrorDetail(
            code="request_too_large",
            message=f"Request body exceeds {JSON_BODY_MAX_BYTES} bytes.",
            field=None,
        ).model_dump(),
    )
    body_withheld = False

    async def withheld_receive() -> Message:
        nonlocal body_withheld
        if body_withheld:
            return await request.receive()
        body_withheld = True
        return {"type": "http.request", "body": b"", "more_body": False}

    return _TerminalJsonRequest(request.scope, withheld_receive)


async def _bounded_body_request(request: Request) -> Request:
    """The request FastAPI's handler is given on a route that binds a body: its body bounded.

    A body within :data:`JSON_BODY_MAX_BYTES` is read here through a counting ``receive`` and
    cached on the returned request, from which FastAPI reads it. Any other body is withheld from
    FastAPI and carries the 413 verdict (:func:`_withhold_body`):

    * a ``Content-Length`` above the bound, or one that is not a decimal integer, is refused
      without reading a byte of the body;
    * a body that passes the bound while it is read, sent without ``Content-Length`` (chunked)
      or longer than its declared length on a server that does not enforce it, stops the read at
      once. The rest of it is then read and dropped (:func:`_discard_body`) until it ends, 1 MiB
      past the bound has been read or one second has passed, so the connection closes cleanly
      and a proxy still forwarding the body receives the answer rather than a reset.

    A client that disconnects while its body is read gets FastAPI's own answer, 400 ``There was
    an error parsing the body``.
    """
    request.scope.pop(_BODY_VERDICT_SCOPE_KEY, None)
    header = request.headers.get("content-length")
    if header is not None and not _declares_length_within_bound(header):
        return _withhold_body(request)

    received = 0
    body_ended = False

    async def counting_receive() -> Message:
        nonlocal received, body_ended
        message = await request.receive()
        if message["type"] == "http.request":
            received += len(message.get("body", b""))
            body_ended = not message.get("more_body", False)
            if received > JSON_BODY_MAX_BYTES:
                raise _BodyTooLargeError
        return message

    bounded = _TerminalJsonRequest(request.scope, counting_receive)
    try:
        await bounded.body()
    except _BodyTooLargeError:
        # Answering with body bytes still unread makes the server reset the connection, so a
        # proxy still forwarding them fails its write and never relays the answer.
        if not body_ended:
            await _discard_body(
                request.receive, JSON_BODY_MAX_BYTES + _DRAIN_ALLOWANCE_BYTES - received
            )
        return _withhold_body(request)
    except ClientDisconnect as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=_BODY_ERROR_MESSAGE
        ) from exc
    return bounded


async def _raise_body_verdict(request: Request) -> None:
    """Raise the verdict that :class:`TerminalJsonRoute` recorded on the request's body, if any.

    The route class appends this dependency to every route's own dependencies, so FastAPI
    resolves it after the dependencies of the routers that include the route (the terminal
    principal, and any a host's ``include_router`` adds) and before the endpoint's parameters
    (the session among them). A host dependency that refuses the caller has therefore answered
    before this runs, and a refused body never opens a session. The 413 ``request_too_large``
    verdict is logged at INFO without any of the body; the ``json_invalid`` verdict is FastAPI's
    own ``RequestValidationError``. It takes only the request, so it adds no OpenAPI parameter.
    """
    verdict = request.scope.pop(_BODY_VERDICT_SCOPE_KEY, None)
    if isinstance(verdict, HTTPException):
        logger.info(
            "JSON request body rejected: request_too_large (limit %d bytes)", JSON_BODY_MAX_BYTES
        )
        raise verdict
    if isinstance(verdict, RequestValidationError):
        raise verdict


class TerminalJsonRoute(APIRoute):
    """Route class of every terminal sub-router that binds a JSON request body.

    The sub-routers of ``POST /ipo/valuation`` and ``POST /watchlists`` are declared
    ``APIRouter(route_class=TerminalJsonRoute)``. On a route that binds a body:

    * **Bound.** The body may hold at most :data:`JSON_BODY_MAX_BYTES` (64 KiB). A larger one is
      answered 413 ``{"detail": {"code": "request_too_large", "message", "field": null}}``: it is
      never read when its ``Content-Length`` says so (or is not a decimal integer), and otherwise
      its read stops at the bound (see :func:`_bounded_body_request`).
    * **Decoding.** Every JSON decoding failure (deep nesting, an over-long integer literal,
      invalid UTF-8 or an unpaired surrogate) is answered with FastAPI's standard 422 list, one
      ``json_invalid`` entry at ``loc = ["body", <position>]`` whose ``ctx.error`` names the
      cause, instead of a 400 with a string ``detail`` (see :class:`_TerminalJsonRequest`). A
      host's ``RequestValidationError`` handler still receives it.
    * **Order.** Neither verdict is raised while FastAPI reads the body. Both are recorded on the
      request and raised by :func:`_raise_body_verdict`, which the class appends to the route's
      own dependencies. The order is therefore host dependencies (the principal, and any a host's
      ``include_router`` adds), then the body verdict, then the endpoint's dependencies and
      validation. A host principal override that raises 401 answers 401 for a malformed or
      oversized body, an oversized body is never decoded, and a refused body opens no session.

    A body within the bound that decodes is validated exactly as before, and a route that binds
    no body is served as before. ``include_router`` preserves the route class and the route's
    dependencies, so the behaviour travels with ``terminal_router`` into a host; it registers no
    middleware or exception handler, and the appended dependency adds no OpenAPI parameter.
    """

    def __init__(
        self,
        path: str,
        endpoint: Callable[..., Any],
        *,
        dependencies: Sequence[params.Depends] | None = None,
        **kwargs: Any,
    ) -> None:
        """Build the route with :func:`_raise_body_verdict` last among its own dependencies.

        It is appended once: dependencies that already hold it, as when a route is rebuilt from
        an existing route's dependencies, are kept as given.
        """
        own_dependencies = list(dependencies or ())
        if not any(depends.dependency is _raise_body_verdict for depends in own_dependencies):
            own_dependencies.append(Depends(_raise_body_verdict))
        super().__init__(path, endpoint, dependencies=own_dependencies, **kwargs)

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap FastAPI's handler so that it reads the body through ``_TerminalJsonRequest``,
        bounded by :data:`JSON_BODY_MAX_BYTES` on a route that binds a body."""
        # Built here, synchronously: FastAPI resolves an included route's context during this
        # call, so deferring it to request time would lose that context.
        handler = super().get_route_handler()

        if self.body_field is None:
            # FastAPI never reads the body of a route that binds none, so neither does this one.
            async def terminal_json_handler(request: Request) -> Response:
                return await handler(_TerminalJsonRequest(request.scope, request.receive))

            return terminal_json_handler

        async def bounded_json_handler(request: Request) -> Response:
            return await handler(await _bounded_body_request(request))

        return bounded_json_handler
