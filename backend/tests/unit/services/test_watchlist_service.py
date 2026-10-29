"""Watchlist service: ``create_watchlist`` refuses an owner key the column cannot store.

``create_watchlist`` writes ``owner_key = principal.subject`` into ``terminal_watchlists.owner_key``
(``VARCHAR(255)``, PostgreSQL text). Before any query it refuses a subject that is empty, longer
than ``OWNER_KEY_MAX_LENGTH`` (255) characters or holds a NUL character with ``ValueError``, and a
non-``str`` subject with ``TypeError``, rather than storing an empty owner key or failing at commit
with an uncoded driver error. The principal is a host's own object satisfying ``OwnerPrincipal``,
not ``api.deps.TerminalPrincipal``, which refuses such subjects itself: services accept any
object with a ``subject``. The session is bound to no engine, so any query would raise
``UnboundExecutionError`` instead; the guard's exception shows it runs first, and a 255-character
subject reaching ``UnboundExecutionError`` shows the bound is inclusive. The database paths of the
service belong to the integration suite. No database, network or filesystem is touched.
"""

from dataclasses import dataclass
from typing import Any

import pytest
from sqlalchemy.exc import UnboundExecutionError
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.schemas.watchlists import WatchlistCreate
from opennse_terminal.services.watchlist_service import create_watchlist


@dataclass(frozen=True)
class HostPrincipal:
    """A host's own principal type: it satisfies ``OwnerPrincipal`` and validates nothing."""

    subject: Any


@pytest.mark.parametrize(
    ("subject", "error", "message"),
    [
        pytest.param(
            "", ValueError, r"^principal\.subject must be 1-255 characters, got 0$", id="empty"
        ),
        pytest.param(
            "s" * 256,
            ValueError,
            r"^principal\.subject must be 1-255 characters, got 256$",
            id="256-characters",
        ),
        pytest.param(
            "a\x00b",
            ValueError,
            r"^principal\.subject must not contain a NUL character$",
            id="nul",
        ),
        pytest.param(
            None, TypeError, r"^principal\.subject must be a str, got NoneType$", id="not-str"
        ),
    ],
)
async def test_unstorable_subject_is_refused_before_any_query(
    subject: Any, error: type[Exception], message: str
) -> None:
    """The guard raises before the first query and adds nothing to the session; the message
    never repeats the subject."""
    async with AsyncSession() as session:
        with pytest.raises(error, match=message) as excinfo:
            await create_watchlist(session, HostPrincipal(subject), WatchlistCreate(name="subj"))
        assert not session.new
    assert not subject or subject not in str(excinfo.value)


async def test_255_character_subject_passes_the_guard() -> None:
    """A subject of exactly 255 characters is storable: the call gets past the guard and fails
    only at its first query, on the unbound session."""
    async with AsyncSession() as session:
        with pytest.raises(UnboundExecutionError):
            await create_watchlist(session, HostPrincipal("s" * 255), WatchlistCreate(name="subj"))
