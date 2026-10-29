"""Tests for the credential-free database label of ``opennse_terminal.db.session``.

``database_log_label`` is the only form in which the session module logs or quotes a database URL.
It is a prose label, never a URL or a libpq ``key=value`` connection string, and must drop the
driver, the username, the password before ``@`` and every query value, because asyncpg also reads
the password from ``?password=...``, which ``render_as_string(hide_password=True)`` prints.

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
MARKED_URL_LABEL = 'database "opennse" on db.example:5432'
PLACEHOLDER = "<unparseable database URL>"
INVALID_URL_MESSAGE = (
    "database URL is not valid; expected postgresql+asyncpg://user:password@host:port/database"
)


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
            'database "opennse" on localhost',
            id="username-only-no-port",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@/opennse_c1?host=/run/pg",
            'database "opennse_c1"',
            id="unix-socket-host-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}@/db?host=host-a:5432&host=host-b"
            f"&password={QUERY_PW_MARKER}",
            'database "db"',
            id="multihost-in-query",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@[::1]:5433/opennse",
            'database "opennse" on [::1]:5433',
            id="ipv6-host",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@localhost:5432",
            "default database on localhost:5432",
            id="no-database",
        ),
        pytest.param("postgresql+asyncpg://", "default database", id="no-host-no-database"),
        pytest.param(
            f'postgresql+asyncpg://{USER_MARKER}@db\nexample/op"en\nnse',
            'database "op\\"en\\nnse" on db\\nexample',
            id="quote-and-newline-escaped",
        ),
        pytest.param(
            f"mysql+pymysql://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example:3306/app"
            f"?password={QUERY_PW_MARKER}",
            'database "app" on db.example:3306',
            id="other-driver",
        ),
    ],
)
def test_label_names_only_the_database_host_and_port(url: str | URL, expected: str) -> None:
    label = database_log_label(url)

    assert label == expected
    # A prose label: no driver, and neither a URL nor a libpq key=value connection string.
    assert "://" not in label
    assert "=" not in label
    assert "postgresql" not in label
    assert "mysql" not in label
    assert "\n" not in label
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


@pytest.mark.parametrize(
    ("database_url", "expected"),
    [
        pytest.param(
            MARKED_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://"),
            "database URL must use the postgresql+asyncpg:// scheme; "
            'got driver "postgresql+psycopg" for database "opennse" on db.example:5432',
            id="other-driver",
        ),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@h:{QUERY_VALUE_MARKER}/db",
            INVALID_URL_MESSAGE,
            id="non-numeric-port",
        ),
        pytest.param(f"not a url {USERINFO_PW_MARKER}", INVALID_URL_MESSAGE, id="not-a-url"),
        pytest.param(
            f"postgresql+asyncpg://{USER_MARKER}:{USERINFO_PW_MARKER}@db.example/opennse"
            f"?prepared_statement_cache_size={QUERY_VALUE_MARKER}",
            'database URL has an invalid query parameter value for database "opennse" on '
            "db.example",
            id="query-value-of-the-wrong-type",
        ),
    ],
)
def test_build_engine_errors_are_source_neutral_and_quote_only_the_label(
    database_url: str, expected: str
) -> None:
    # model_construct skips the settings validator, which rejects the other driver and the
    # non-URL text, as a host that builds the settings object itself could.
    settings = TerminalSettings.model_construct(database_url=database_url)

    with pytest.raises(ValueError) as raised:
        build_engine(settings)

    message = str(raised.value)
    assert message == expected
    # The caller adds where the URL came from; the session layer cannot know it.
    assert "TERMINAL_DATABASE_URL" not in message
    # No parser error, which can quote the URL, is chained into a traceback.
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ or raised.value.__context__ is None
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
