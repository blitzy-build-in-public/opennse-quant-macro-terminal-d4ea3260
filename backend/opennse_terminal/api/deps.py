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

Annotations here are evaluated at runtime (no ``from __future__ import annotations``), because
FastAPI reads the ``Annotated`` dependency metadata of every alias used in a route signature.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Final

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import session_factory

__all__ = [
    "PrincipalDep",
    "SessionDep",
    "SettingsDep",
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
