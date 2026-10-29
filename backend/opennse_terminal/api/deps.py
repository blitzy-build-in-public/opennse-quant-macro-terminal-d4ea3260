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

Annotations here are evaluated at runtime (no ``from __future__ import annotations``), because
FastAPI reads the ``Annotated`` dependency metadata of every alias used in a route signature.
"""

import json
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Any, Final

from fastapi import Depends, Request, Response
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import session_factory

__all__ = [
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


class _TerminalJsonRequest(Request):
    """A request whose JSON body either decodes to a renderable value or raises ``JSONDecodeError``.

    FastAPI answers only ``json.JSONDecodeError`` from body decoding with its standard 422 list;
    any other failure of the standard-library decoder becomes a 400 with a string ``detail``.
    Each such failure is re-raised here as a ``JSONDecodeError`` that names the cause without
    Python's own advice text, so FastAPI emits its ``json_invalid`` entry:

    * text that is not valid in its detected encoding (``UnicodeDecodeError``), positioned at the
      first invalid character;
    * nesting too deep for the decoder (``RecursionError``), or deeper than
      :data:`_MAX_JSON_NESTING` levels, and strings holding an unpaired surrogate
      (see :func:`_undecodable_reason`);
    * an integer literal longer than ``sys.get_int_max_str_digits()`` digits (``ValueError``).

    Every other body decodes exactly as before.
    """

    async def json(self) -> Any:
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


class TerminalJsonRoute(APIRoute):
    """Route class of every terminal sub-router that binds a JSON request body.

    The sub-routers of ``POST /ipo/valuation`` and ``POST /watchlists`` are declared
    ``APIRouter(route_class=TerminalJsonRoute)``. The route hands its endpoint a request whose
    JSON decoding failures all raise ``json.JSONDecodeError`` (deep nesting, an over-long integer
    literal, invalid UTF-8 or an unpaired surrogate). FastAPI therefore answers every undecodable
    body with its standard 422 list, one ``json_invalid`` entry at ``loc = ["body", <position>]``
    whose ``ctx.error`` names the cause, instead of a 400 with a string ``detail``, and a host's
    ``RequestValidationError`` handler still applies. Every other body is validated as before.

    ``include_router`` preserves the route class, so the behaviour travels with
    ``terminal_router`` into a host; it registers no middleware or exception handler.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap FastAPI's handler so that it reads the body through ``_TerminalJsonRequest``."""
        # Built here, synchronously: FastAPI resolves an included route's context during this
        # call, so deferring it to request time would lose that context.
        handler = super().get_route_handler()

        async def terminal_json_handler(request: Request) -> Response:
            return await handler(_TerminalJsonRequest(request.scope, request.receive))

        return terminal_json_handler
