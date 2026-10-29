"""Connect failures of the terminal's migration environment (``migrations/env.py``).

``run_upgrade`` and ``run_downgrade`` must turn an unreachable database into a ``ConnectionError``
that names the migration, its target revision and the database (name, host and port, in the prose
form of ``database_log_label``), within ``CONNECT_TIMEOUT_SECONDS`` rather than asyncpg's 60 s
default. The message is never a connection string: it quotes no driver URL and no credential,
neither the username, the password before ``@`` nor the ``?password=`` query value.

No PostgreSQL is needed. The socket cases bind their own loopback socket on port 0: one that is
closed again before connecting, so the connect is refused, and one that listens but never accepts
or replies, so asyncpg's deadline expires. The remaining cases replace ``asyncpg.connect`` with one
that raises a given error, for causes a loopback socket cannot produce and for a server-reported
error, which must propagate unwrapped. The credentials are synthetic markers, so a leak shows as
the marker in a message or traceback. The tests are synchronous because ``run_upgrade`` refuses
to run inside an event loop.
"""

import errno
import socket
import time
import traceback
from collections.abc import Callable, Iterator
from typing import Any

import asyncpg
import pytest

from opennse_terminal import migrations
from opennse_terminal.migrations import head_revision, run_downgrade, run_upgrade

USER_MARKER = "migrateuserMARK"
USERINFO_PW_MARKER = "migratepwMARK"
QUERY_PW_MARKER = "migrateqpwMARK"
CREDENTIAL_MARKERS = (USER_MARKER, USERINFO_PW_MARKER, QUERY_PW_MARKER)
DATABASE = "terminal_connect_check"
LOOPBACK = "127.0.0.1"

MigrateCommand = Callable[[str, str], None]

COMMANDS = [
    pytest.param(run_upgrade, "head", head_revision(), id="upgrade-head"),
    pytest.param(run_downgrade, "base", "base", id="downgrade-base"),
]


def _marked_url(port: int) -> str:
    return (
        f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@{LOOPBACK}:{port}/{DATABASE}"
        f"?password={QUERY_PW_MARKER}"
    )


def _expected_prefix(target_revision: str, port: int) -> str:
    return (
        f"terminal migration to revision {target_revision!r} could not connect to "
        f'database "{DATABASE}" on {LOOPBACK}:{port}: '
    )


def _assert_no_credential(error: BaseException) -> None:
    """Check the message and the whole chained traceback, as a log handler would print it."""
    rendered = "".join(traceback.format_exception(error))
    for marker in CREDENTIAL_MARKERS:
        assert marker not in rendered


def _assert_no_connection_string(error: BaseException) -> None:
    """Check that the message names the database in prose, never as a ``driver://`` URL."""
    assert "://" not in str(error)


@pytest.fixture
def closed_port() -> int:
    """A loopback port that nothing listens on: bound once by the OS, then released."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((LOOPBACK, 0))
        port: int = probe.getsockname()[1]
    return port


@pytest.fixture
def silent_server_port() -> Iterator[int]:
    """A loopback port whose listener completes TCP handshakes but never accepts or replies."""
    with socket.create_server((LOOPBACK, 0)) as listener:
        port: int = listener.getsockname()[1]
        yield port


@pytest.mark.parametrize(("command", "revision", "target_revision"), COMMANDS)
def test_refused_connect_raises_connection_error_naming_the_target(
    command: MigrateCommand, revision: str, target_revision: str, closed_port: int
) -> None:
    with pytest.raises(ConnectionError) as raised:
        command(_marked_url(closed_port), revision)

    message = str(raised.value)
    assert message.startswith(_expected_prefix(target_revision, closed_port))
    assert "ConnectionRefusedError" in message
    assert isinstance(raised.value.__cause__, OSError)
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


@pytest.mark.parametrize(("command", "revision", "target_revision"), COMMANDS)
def test_unanswered_connect_times_out_within_the_connect_timeout(
    command: MigrateCommand,
    revision: str,
    target_revision: str,
    silent_server_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # env.py is executed afresh by every Alembic command, so it reads the patched value.
    monkeypatch.setattr(migrations, "CONNECT_TIMEOUT_SECONDS", 0.5)
    started = time.monotonic()

    with pytest.raises(ConnectionError) as raised:
        command(_marked_url(silent_server_port), revision)

    elapsed = time.monotonic() - started
    assert elapsed < 10, f"connect took {elapsed:.1f} s; asyncpg's 60 s default applied"
    expected = f"{_expected_prefix(target_revision, silent_server_port)}timed out after 0.5 s"
    assert str(raised.value) == expected
    assert isinstance(raised.value.__cause__, TimeoutError)
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


def _patch_asyncpg_connect(monkeypatch: pytest.MonkeyPatch, error: Exception) -> dict[str, Any]:
    """Make every asyncpg connect raise ``error``; return the keyword arguments it received."""
    received: dict[str, Any] = {}

    async def failing_connect(*args: Any, **kwargs: Any) -> None:
        received.update(kwargs)
        raise error

    # SQLAlchemy's asyncpg adapter looks asyncpg.connect up on the module at connect time.
    monkeypatch.setattr(asyncpg, "connect", failing_connect)
    return received


@pytest.mark.parametrize(
    ("error", "expected_cause"),
    [
        pytest.param(
            TimeoutError(errno.ETIMEDOUT, f"Connect call failed ('{LOOPBACK}', 5432)"),
            f"TimeoutError: Connect call failed ('{LOOPBACK}', 5432)",
            id="os-etimedout-is-not-the-deadline",
        ),
        pytest.param(
            socket.gaierror(socket.EAI_NONAME, "Name or service not known"),
            "gaierror: Name or service not known",
            id="unresolvable-host",
        ),
        pytest.param(OSError("Multiple exceptions: ..."), "OSError", id="no-strerror"),
    ],
)
def test_connect_oserror_cause_names_its_type_and_strerror(
    error: OSError, expected_cause: str, closed_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A closed port, so a patch that failed to apply could never reach a real server.
    received = _patch_asyncpg_connect(monkeypatch, error)

    with pytest.raises(ConnectionError) as raised:
        run_upgrade(_marked_url(closed_port))

    expected = f"{_expected_prefix(head_revision(), closed_port)}{expected_cause}"
    assert str(raised.value) == expected
    assert raised.value.__cause__ is error
    assert received["timeout"] == migrations.CONNECT_TIMEOUT_SECONDS == 10.0
    _assert_no_credential(raised.value)
    _assert_no_connection_string(raised.value)


def test_server_reported_connect_error_propagates_unwrapped(
    closed_port: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The server's own refusal already names the user and never the password.
    refusal = asyncpg.exceptions.InvalidPasswordError(
        f'password authentication failed for user "{USER_MARKER}"'
    )
    _patch_asyncpg_connect(monkeypatch, refusal)

    with pytest.raises(asyncpg.exceptions.InvalidPasswordError) as raised:
        run_upgrade(_marked_url(closed_port))

    assert raised.value is refusal
