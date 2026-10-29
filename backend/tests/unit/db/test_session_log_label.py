"""Tests for the credential-free database label of ``opennse_terminal.db.session``.

``database_log_label`` is the only form in which the session module logs or quotes a database URL.
It must drop the username, the password before ``@`` and every query value, because asyncpg also
reads the password from ``?password=...``, which ``render_as_string(hide_password=True)`` prints.

Every case runs without PostgreSQL: engines are created but never connect. The secrets are
synthetic markers, so a leak shows as the marker in a label, an error message or a log record.
"""

import logging
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db import session as db_session
from opennse_terminal.db.session import (
    build_engine,
    database_log_label,
    dispose_terminal_engine,
    session_factory,
)

USER_MARKER = "labeluserMARK"
USERINFO_PW_MARKER = "pwSECRETmark"
QUERY_PW_MARKER = "QSECRET"
QUERY_VALUE_MARKER = "qvalueMARK"
MARKERS = (USER_MARKER, USERINFO_PW_MARKER, QUERY_PW_MARKER, QUERY_VALUE_MARKER)

MARKED_URL = (
    f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example:5432/opennse"
    f"?password={QUERY_PW_MARKER}&application_name={QUERY_VALUE_MARKER}"
)
MARKED_URL_LABEL = "postgresql+asyncpg://db.example:5432/opennse"
PLACEHOLDER = "<unparseable database URL>"


@pytest.fixture
async def no_cached_engines() -> AsyncIterator[None]:
    """Start and end with an empty engine cache, so no engine outlives its test."""
    await dispose_terminal_engine()
    yield
    await dispose_terminal_engine()


def _assert_no_secret(text: str) -> None:
    for secret in MARKERS:
        assert secret not in text


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param(MARKED_URL, MARKED_URL_LABEL, id="userinfo-and-query-password"),
        pytest.param(make_url(MARKED_URL), MARKED_URL_LABEL, id="url-object"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@localhost/opennse",
            "postgresql+asyncpg://localhost/opennse",
            id="username-only-no-port",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@/opennse_c1?host=/run/pg",
            "postgresql+asyncpg:///opennse_c1",
            id="unix-socket-host-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@/db?host=host-a:5432&host=host-b"
            f"&password={QUERY_PW_MARKER}",
            "postgresql+asyncpg:///db",
            id="multihost-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@[::1]:5433/opennse",
            "postgresql+asyncpg://[::1]:5433/opennse",
            id="ipv6-host",
        ),
        pytest.param(
            f"mysql+pymysql://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example:3306/app"
            f"?password={QUERY_PW_MARKER}",
            "mysql+pymysql://db.example:3306/app",
            id="other-driver",
        ),
    ],
)
def test_label_keeps_only_driver_host_port_and_database(url: str | URL, expected: str) -> None:
    label = database_log_label(url)

    assert label == expected
    _assert_no_secret(label)


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("", id="empty"),
        pytest.param(f"not a url {USERINFO_PW_MARKER}", id="not-a-url"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@h:{QUERY_VALUE_MARKER}/db",
            id="non-numeric-port",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:pw@{USERINFO_PW_MARKER}@db.example/opennse",
            id="unescaped-at-in-password",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}/x:{USERINFO_PW_MARKER}@db.example/opennse",
            id="unescaped-slash-in-username",
        ),
    ],
)
def test_label_of_an_unreliable_url_is_the_fixed_placeholder(url: str) -> None:
    assert database_log_label(url) == PLACEHOLDER


def test_build_engine_scheme_error_quotes_only_the_label() -> None:
    # The settings validator already rejects other drivers; model_construct skips it, as a host
    # that builds the settings object itself could.
    settings = TerminalSettings.model_construct(
        database_url=MARKED_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    )

    with pytest.raises(ValueError, match="postgresql\\+asyncpg:// scheme") as raised:
        build_engine(settings)

    message = str(raised.value)
    assert message.endswith("got postgresql+psycopg://db.example:5432/opennse")
    _assert_no_secret(message)


async def test_build_engine_debug_log_and_connect_url(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="opennse_terminal.db.session")

    engine = build_engine(TerminalSettings(database_url=MARKED_URL, _env_file=None))
    try:
        # The URL the engine connects with keeps its query password.
        assert engine.url.query["password"] == QUERY_PW_MARKER
        assert engine.url.password == USERINFO_PW_MARKER
    finally:
        await engine.dispose()

    messages = [record.getMessage() for record in caplog.records]
    assert f"created engine for {MARKED_URL_LABEL}" in messages
    _assert_no_secret(caplog.text)


@pytest.mark.usefixtures("no_cached_engines")
async def test_session_factory_info_log(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="opennse_terminal.db.session")

    session_factory(TerminalSettings(database_url=MARKED_URL, _env_file=None))

    messages = [record.getMessage() for record in caplog.records]
    assert f"terminal database engine initialised for {MARKED_URL_LABEL}" in messages
    _assert_no_secret(caplog.text)


@pytest.mark.usefixtures("no_cached_engines")
async def test_failing_dispose_log(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_factory(TerminalSettings(database_url=MARKED_URL, _env_file=None))
    engine = db_session._CACHE[MARKED_URL][0]

    async def failing_dispose(self: AsyncEngine, close: bool = True) -> None:
        raise RuntimeError("dispose failed")

    # AsyncEngine has __slots__, so the failure is patched on the class for this test only.
    monkeypatch.setattr(AsyncEngine, "dispose", failing_dispose)
    caplog.set_level(logging.ERROR, logger="opennse_terminal.db.session")

    with pytest.raises(RuntimeError, match="dispose failed"):
        await dispose_terminal_engine()

    messages = [record.getMessage() for record in caplog.records]
    assert f"failed to dispose terminal database engine for {MARKED_URL_LABEL}" in messages
    _assert_no_secret(caplog.text)

    monkeypatch.undo()
    await engine.dispose()
