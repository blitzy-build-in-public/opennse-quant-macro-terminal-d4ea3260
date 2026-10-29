"""Principal contract of ``opennse_terminal.api.deps``: the subject bound and the default principal.

``TerminalPrincipal.subject`` is stored as ``terminal_watchlists.owner_key`` (``VARCHAR(255)``), so
construction refuses a subject the column cannot store: an empty one, one longer than 255
characters or one holding a NUL character raises ``ValueError``, and a non-``str`` raises
``TypeError``. The messages state the rule and the length, never the subject. Subjects at the
bounds (1 and 255 characters) and subjects differing only in case or surrounding spaces are kept
exactly as given. ``get_terminal_principal`` returns ``settings.default_owner_key`` (``local``)
unauthenticated, and ``TerminalSettings`` refuses the same subjects the principal refuses, so the
default principal can always be built. Settings are built with ``_env_file=None`` and without
``TERMINAL_DEFAULT_OWNER_KEY``, so the checkout's ``.env`` is never read. Offline: no database,
network or filesystem is touched.
"""

from typing import Any

import pytest
from pydantic import ValidationError

from opennse_terminal.api.deps import TerminalPrincipal, get_terminal_principal
from opennse_terminal.config import TerminalSettings

UNSTORABLE_SUBJECTS = [
    pytest.param("", r"^TerminalPrincipal\.subject must be 1-255 characters, got 0$", id="empty"),
    pytest.param(
        "s" * 256,
        r"^TerminalPrincipal\.subject must be 1-255 characters, got 256$",
        id="256-characters",
    ),
    pytest.param(
        "a\x00b",
        r"^TerminalPrincipal\.subject must not contain a NUL character, found one at index 1$",
        id="nul",
    ),
]


@pytest.fixture(autouse=True)
def _no_owner_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove an inherited default owner key, so each settings object sees only what it sets."""
    monkeypatch.delenv("TERMINAL_DEFAULT_OWNER_KEY", raising=False)


@pytest.mark.parametrize(("subject", "message"), UNSTORABLE_SUBJECTS)
def test_subject_the_owner_key_cannot_store_is_refused(subject: str, message: str) -> None:
    """An empty, a 256-character and a NUL-holding subject raise ``ValueError`` naming the rule;
    the message carries the length or the NUL's index, not the subject."""
    with pytest.raises(ValueError, match=message) as excinfo:
        TerminalPrincipal(subject=subject, is_authenticated=True)
    assert not subject or subject not in str(excinfo.value)


@pytest.mark.parametrize(
    "subject",
    [
        pytest.param("s", id="1-character"),
        pytest.param("s" * 255, id="255-characters"),
        pytest.param(" LOCAL ", id="case-and-spaces-kept"),
    ],
)
def test_storable_subject_is_kept_exactly(subject: str) -> None:
    """Both bounds are inclusive, and the subject is never trimmed or case-folded."""
    principal = TerminalPrincipal(subject=subject, is_authenticated=True)
    assert principal.subject == subject
    assert principal.is_authenticated is True


@pytest.mark.parametrize(
    ("subject", "type_name"),
    [pytest.param(b"local", "bytes", id="bytes"), pytest.param(None, "NoneType", id="none")],
)
def test_non_str_subject_raises_type_error(subject: Any, type_name: str) -> None:
    """A subject that is not a ``str`` is a ``TypeError`` naming its type."""
    with pytest.raises(
        TypeError, match=rf"^TerminalPrincipal\.subject must be a str, got {type_name}$"
    ):
        TerminalPrincipal(subject=subject, is_authenticated=False)


async def test_default_principal_is_local_and_unauthenticated() -> None:
    """Without a host override the caller is ``local`` and unauthenticated."""
    principal = await get_terminal_principal(TerminalSettings(_env_file=None))
    assert principal == TerminalPrincipal(subject="local", is_authenticated=False)


@pytest.mark.parametrize(
    "owner_key",
    [
        pytest.param("", id="empty"),
        pytest.param("s" * 256, id="256-characters"),
        pytest.param("a\x00b", id="nul"),
    ],
)
def test_settings_refuse_a_default_owner_key_the_principal_refuses(owner_key: str) -> None:
    """``default_owner_key`` fails to load for every subject ``TerminalPrincipal`` refuses, so
    ``get_terminal_principal`` never raises for loaded settings."""
    with pytest.raises(ValidationError, match="default_owner_key"):
        TerminalSettings(default_owner_key=owner_key, _env_file=None)


@pytest.mark.parametrize(
    "owner_key", [pytest.param("s", id="1-character"), pytest.param("s" * 255, id="255-characters")]
)
async def test_default_principal_carries_a_bound_owner_key_exactly(owner_key: str) -> None:
    """A default owner key at either bound loads and becomes the default principal's subject."""
    settings = TerminalSettings(default_owner_key=owner_key, _env_file=None)
    principal = await get_terminal_principal(settings)
    assert principal == TerminalPrincipal(subject=owner_key, is_authenticated=False)
