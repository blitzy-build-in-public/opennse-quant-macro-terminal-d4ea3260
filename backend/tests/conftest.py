"""Root pytest configuration: fixture paths, hermetic settings and lazy PostgreSQL primitives.

Why the database primitives live here. The test plan puts the database lifecycle in
``integration/conftest.py`` and has this root conftest register nothing that needs a database.
The CBK pipeline tests and the CSV/Excel importer tests under ``unit/ingestion/`` also run full
pipelines that write rows and record runs in PostgreSQL, and pytest offers a sub-directory
conftest's fixtures only to the tests beneath it. The shared primitives are therefore defined
here, and they are lazy: nothing connects to PostgreSQL until a test requests one of
:data:`DATABASE_FIXTURES`, so pure unit tests stay database-free. ``integration/conftest.py``
builds the integration-only scopes on top of them: module clones, rollback-wrapped sessions,
empty isolation databases and the ASGI client.

Fixtures:

- ``fixtures_dir``, ``fixture_bytes`` and ``fixture_text`` read recorded source content under
  ``tests/fixtures/``.
- ``make_settings`` and ``test_settings`` build ``TerminalSettings`` from explicit values only,
  so neither the developer's ``.env`` nor ``TERMINAL_*`` variables leak into a test.
- ``admin_engine`` is an AUTOCOMMIT engine on ``TERMINAL_TEST_DATABASE_URL``. That database is
  the administrative connection only: it creates and drops databases and never holds data.
- ``seeded_template`` is ``opennse_test_template``, migrated to head and seeded once per session.
- ``database_factory`` creates seeded clones and empty databases, drops them, and drops whatever
  is left at the end of the session.
- ``seeded_database`` is a fresh seeded clone per test, for code that opens its own connections
  (the advisory-lock and run-record connections of the ingestion runs) and so cannot run inside
  a rollback-wrapped session.

Every test that requests a database fixture, directly or through another fixture, is marked
``integration``, so ``pytest -m "not integration"`` runs without PostgreSQL. An unreachable
server fails those tests with an explicit message and never skips them, because the coverage
gates depend on them.

These fixtures create and drop databases only. Every table comes from the terminal's own
migrations, so no ``users``, ``auth`` or other non-``terminal_`` table is created here. Test
modules reach everything through fixtures and never import this module.
"""

import asyncio
import logging
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Protocol

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine, database_log_label, dispose_terminal_engine
from opennse_terminal.migrations import run_upgrade
from opennse_terminal.seed.seed_terminal_db import run_seed

logger = logging.getLogger("tests.conftest")

FIXTURES: Final[Path] = Path(__file__).parent / "fixtures"
"""Root of the recorded source fixtures (``nse/``, ``cbk/``, ``files/``)."""

DEFAULT_TEST_DATABASE_URL: Final[str] = (
    "postgresql+asyncpg://opennse:opennse@localhost:5432/opennse_test"
)
"""Administrative database used when ``TERMINAL_TEST_DATABASE_URL`` is unset. It is created by
``docker/postgres-init.sql``; the credentials are those of the throwaway development container."""

TEMPLATE_DATABASE: Final[str] = "opennse_test_template"
"""The migrated and seeded template every seeded clone is copied from."""

CLONE_PREFIX: Final[str] = "opennse_test"
"""Name prefix of seeded clones: ``opennse_test_<label>_<uuid8>``."""

EMPTY_PREFIX: Final[str] = "opennse_iso"
"""Name prefix of empty databases copied from ``template0``: ``opennse_iso_<label>_<uuid8>``."""

DATABASE_FIXTURES: Final[frozenset[str]] = frozenset(
    {"admin_engine", "seeded_template", "database_factory", "seeded_database"}
)
"""Fixtures that need PostgreSQL; a test depending on any of them is marked ``integration``."""

ADMIN_CONNECT_TIMEOUT_SECONDS: Final[float] = 10.0
"""asyncpg's connect timeout for administrative connections, so a missing server fails fast."""

_MAX_IDENTIFIER_LENGTH: Final[int] = 63
_SUFFIX_LENGTH: Final[int] = 8
_DATABASE_NAME: Final = re.compile(r"[a-z0-9_]{1,63}")
_NAME_PREFIX: Final = re.compile(r"[a-z][a-z0-9_]*")
_UNSAFE_LABEL_CHARACTERS: Final = re.compile(r"[^a-z0-9_]")


def test_database_url() -> str:
    """Return the administrative database URL: ``TERMINAL_TEST_DATABASE_URL``, or the default.

    An empty variable counts as unset. ``.env`` is not read: the backend ignores this key there,
    and the suite must not silently follow a developer's file.
    """
    return os.environ.get("TERMINAL_TEST_DATABASE_URL") or DEFAULT_TEST_DATABASE_URL


def build_test_settings(**overrides: Any) -> TerminalSettings:
    """Build ``TerminalSettings`` from explicit values only.

    Every field is passed as a keyword argument, which outranks environment variables in
    pydantic-settings, and ``_env_file=None`` keeps ``.env`` out. The values are the documented
    defaults, except ``database_url`` (:func:`test_database_url`) and a test scraper contact.
    ``overrides`` replace any of them; an unknown name raises ``TypeError``, because the settings
    ignore extra keys and a misspelt override would otherwise be dropped silently.
    """
    unknown = sorted(set(overrides) - set(TerminalSettings.model_fields))
    if unknown:
        raise TypeError(f"unknown TerminalSettings field(s): {', '.join(unknown)}")
    values: dict[str, Any] = {
        "database_url": test_database_url(),
        "default_owner_key": "local",
        "timezone": "Africa/Nairobi",
        "fundamentals_lag_days": 90,
        "scraper_contact": "tests@example.invalid",
        "scraper_min_interval_seconds": 2.0,
        "scraper_timeout_seconds": 30,
        "scraper_max_retries": 3,
        "pdf_max_bytes": 20_971_520,
        "pdf_max_pages": 400,
        "pdf_max_concurrency": 2,
    }
    values.update(overrides)
    # _env_file rides in the mapping because type checkers model the constructor on the fields
    # alone and would reject it as a literal keyword.
    return TerminalSettings(**{"_env_file": None, **values})


def _fixture_path(relative: str | os.PathLike[str]) -> Path:
    """Resolve ``relative`` under :data:`FIXTURES`, refusing paths that leave it."""
    root = FIXTURES.resolve()
    candidate = (root / relative).resolve()
    shown = os.fspath(relative)
    if not candidate.is_relative_to(root):
        raise ValueError(f"fixture path {shown!r} resolves outside {root}")
    if not candidate.is_file():
        raise FileNotFoundError(f"test fixture {shown!r} not found under {root}")
    return candidate


def read_fixture_bytes(relative: str | os.PathLike[str]) -> bytes:
    """Return the bytes of the fixture at ``relative`` (for example ``"cbk/fx_end_period.csv"``)."""
    return _fixture_path(relative).read_bytes()


def read_fixture_text(relative: str | os.PathLike[str], encoding: str = "utf-8") -> str:
    """Return the fixture at ``relative`` decoded with ``encoding``."""
    return _fixture_path(relative).read_text(encoding=encoding)


class FixtureTextReader(Protocol):
    """Signature of the ``fixture_text`` fixture's reader."""

    def __call__(self, relative: str | os.PathLike[str], encoding: str = "utf-8") -> str: ...


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    """The recorded-fixture root, ``backend/tests/fixtures``."""
    return FIXTURES


@pytest.fixture(scope="session")
def fixture_bytes() -> Callable[[str | os.PathLike[str]], bytes]:
    """Reader of fixture bytes by path relative to ``fixtures_dir``.

    Raises ``FileNotFoundError`` naming a missing fixture and ``ValueError`` for a path that
    resolves outside the fixture root.
    """
    return read_fixture_bytes


@pytest.fixture(scope="session")
def fixture_text() -> FixtureTextReader:
    """Reader of fixture text, ``(relative, encoding="utf-8") -> str``.

    Raises the same errors as ``fixture_bytes``.
    """
    return read_fixture_text


@pytest.fixture(scope="session")
def make_settings() -> Callable[..., TerminalSettings]:
    """Factory ``(**overrides) -> TerminalSettings`` that never reads ``.env`` or the environment.

    For example ``make_settings(database_url=handle.url, pdf_max_bytes=1024)``.
    """
    return build_test_settings


@pytest.fixture
def test_settings() -> TerminalSettings:
    """A fresh ``make_settings()`` per test, pointing at the administrative test database."""
    return build_test_settings()


# ---------------------------------------------------------------------------------------------
# PostgreSQL primitives (lazy: nothing below runs unless a test requests a database fixture)
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseHandle:
    """A database created for tests, with the settings and the engine that address it.

    ``engine`` is owned by the factory that created the handle and is disposed when the database
    is dropped. ``url`` carries the password, so it is left out of ``repr()`` and therefore out of
    assertion and traceback output.
    """

    name: str
    url: str = field(repr=False)
    settings: TerminalSettings
    engine: AsyncEngine


def database_name(prefix: str, label: str) -> str:
    """Return a unique PostgreSQL database name ``<prefix>_<label>_<uuid8>``.

    The label is lowercased, every character outside ``[a-z0-9_]`` becomes ``_``, and it is
    shortened so the name fits PostgreSQL's 63-byte identifier limit; an empty label is left out.
    ``prefix`` must be a lowercase identifier (``ValueError`` otherwise). The result always
    matches ``[a-z0-9_]{1,63}``, so it can be quoted into ``CREATE DATABASE`` safely: database
    identifiers cannot be bound parameters.
    """
    if not _NAME_PREFIX.fullmatch(prefix):
        raise ValueError(f"database name prefix must match {_NAME_PREFIX.pattern}: {prefix!r}")
    fixed = len(prefix) + 1 + _SUFFIX_LENGTH
    if fixed > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"database name prefix is too long: {prefix!r}")
    budget = max(0, _MAX_IDENTIFIER_LENGTH - fixed - 1)
    cleaned = _UNSAFE_LABEL_CHARACTERS.sub("_", label.lower()).strip("_")[:budget].rstrip("_")
    suffix = uuid.uuid4().hex[:_SUFFIX_LENGTH]
    name = "_".join(part for part in (prefix, cleaned, suffix) if part)
    assert _DATABASE_NAME.fullmatch(name), name
    return name


def _unreachable_message(url: str | URL, exc: BaseException) -> str:
    # database_log_label names only the database, host and port: the URL's password and query
    # values (asyncpg also accepts ?password=...) never reach the test report.
    return (
        f"PostgreSQL not reachable at {database_log_label(url)} ({type(exc).__name__}); "
        "run `docker compose up -d postgres`, or point TERMINAL_TEST_DATABASE_URL at a running "
        "server whose role holds CREATEDB"
    )


class DatabaseFactory:
    """Creates and drops the suite's databases on the administrative server.

    Seeded clones are copied from :data:`TEMPLATE_DATABASE`, which is built on first use, once per
    session, and cached; a failed build is cached too, so later requests fail at once instead of
    rebuilding. Empty databases are copied from ``template0``. Every database created here is
    tracked and dropped at the end of the session if a test left it behind.
    """

    def __init__(self, admin_engine: AsyncEngine) -> None:
        self._admin = admin_engine
        self._preparer = admin_engine.dialect.identifier_preparer
        # Database name -> its handle's engine; None between CREATE DATABASE and the engine.
        self._live: dict[str, AsyncEngine | None] = {}
        self._template_lock = asyncio.Lock()
        self._template_created = False
        self._template_ready = False
        self._template_failure: str | None = None

    def url_for(self, name: str) -> str:
        """The administrative URL pointed at database ``name``, password included."""
        return self._admin.url.set(database=name).render_as_string(hide_password=False)

    def _quote(self, name: str) -> str:
        if not _DATABASE_NAME.fullmatch(name):
            raise ValueError(f"not a database name this suite creates: {name!r}")
        return self._preparer.quote(name)

    async def _execute(self, statement: str, **params: object) -> None:
        async with self._admin.connect() as connection:
            await connection.execute(text(statement), params)

    async def seeded_template(self) -> str:
        """Return :data:`TEMPLATE_DATABASE`, building it on the first call of the session."""
        async with self._template_lock:
            if self._template_ready:
                return TEMPLATE_DATABASE
            if self._template_failure is not None:
                pytest.fail(self._template_failure, pytrace=False)
            try:
                failure = await self._build_template()
            except Exception as exc:
                self._template_failure = (
                    f"building {TEMPLATE_DATABASE} failed earlier in this session "
                    f"({type(exc).__name__})"
                )
                raise
            if failure is not None:
                self._template_failure = failure
                pytest.fail(failure, pytrace=False)
            self._template_ready = True
            return TEMPLATE_DATABASE

    async def _build_template(self) -> str | None:
        """Create, migrate and seed the template; return a failure message or ``None``."""
        started = time.perf_counter()
        quoted = self._quote(TEMPLATE_DATABASE)
        logger.info("building seeded template database %s", TEMPLATE_DATABASE)
        await self._execute(f"DROP DATABASE IF EXISTS {quoted} WITH (FORCE)")
        await self._execute(f"CREATE DATABASE {quoted}")
        self._template_created = True
        url = self.url_for(TEMPLATE_DATABASE)
        # env.py drives its own engine with asyncio.run, so the upgrade runs off this loop.
        await asyncio.to_thread(run_upgrade, url, "head")
        exit_code = await run_seed(build_test_settings(database_url=url))
        if exit_code != 0:
            return (
                f"seeding {TEMPLATE_DATABASE} exited with code {exit_code}; "
                "see the captured stderr and log output"
            )
        # run_seed disposes its own engine; engines cached by session_factory are released too.
        await dispose_terminal_engine()
        # CREATE DATABASE ... TEMPLATE fails while the template has any connection. Refusing new
        # connections and ending the remaining ones keeps every later clone from tripping on
        # one; DROP DATABASE and cloning both work on a database that refuses connections.
        await self._execute(f"ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS false")
        await self._execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = :name AND pid <> pg_backend_pid()",
            name=TEMPLATE_DATABASE,
        )
        logger.info(
            "seeded template database %s ready in %.1f s",
            TEMPLATE_DATABASE,
            time.perf_counter() - started,
        )
        return None

    async def create_from_template(self, label: str, template: str | None = None) -> DatabaseHandle:
        """Create ``opennse_test_<label>_<uuid8>`` as a copy of ``template``.

        ``template`` defaults to the seeded template, which is built first if this session has
        not built it yet.
        """
        source = template if template is not None else TEMPLATE_DATABASE
        if source == TEMPLATE_DATABASE:
            source = await self.seeded_template()
        return await self._create(CLONE_PREFIX, label, source)

    async def create_empty(self, label: str) -> DatabaseHandle:
        """Create the empty database ``opennse_iso_<label>_<uuid8>`` from ``template0``."""
        return await self._create(EMPTY_PREFIX, label, "template0")

    async def _create(self, prefix: str, label: str, template: str) -> DatabaseHandle:
        name = database_name(prefix, label)
        await self._execute(f"CREATE DATABASE {self._quote(name)} TEMPLATE {self._quote(template)}")
        self._live[name] = None
        url = self.url_for(name)
        settings = build_test_settings(database_url=url)
        engine = build_engine(settings)
        self._live[name] = engine
        logger.info("created database %s from %s", name, template)
        return DatabaseHandle(name=name, url=url, settings=settings, engine=engine)

    async def drop(self, handle: DatabaseHandle) -> None:
        """Dispose ``handle.engine``, then drop its database; dropping twice is harmless.

        Only clones and empty databases can be dropped this way (``ValueError`` otherwise), so a
        hand-built handle can never remove the template, the administrative database or a
        shared development database.
        """
        name = handle.name
        droppable = name.startswith((f"{CLONE_PREFIX}_", f"{EMPTY_PREFIX}_"))
        if name == TEMPLATE_DATABASE or not droppable:
            raise ValueError(f"refusing to drop a database this factory does not create: {name!r}")
        await handle.engine.dispose()
        await self._drop_database(name)
        self._live.pop(name, None)

    async def _drop_database(self, name: str) -> None:
        await self._execute(f"DROP DATABASE IF EXISTS {self._quote(name)} WITH (FORCE)")
        logger.info("dropped database %s", name)

    async def close(self) -> None:
        """Drop every database still tracked, then the template; re-raise the first failure."""
        first_error: Exception | None = None
        # Engines cached by session_factory (the API's default session provider) may point at
        # these databases; they are released before the databases disappear under them.
        try:
            await dispose_terminal_engine()
        except Exception as exc:
            first_error = exc
        leftovers = list(self._live.items())
        self._live.clear()
        names = [name for name, _engine in leftovers]
        if self._template_created:
            names.append(TEMPLATE_DATABASE)
        for _name, engine in leftovers:
            if engine is not None:
                try:
                    await engine.dispose()
                except Exception as exc:
                    first_error = first_error or exc
        for name in names:
            try:
                await self._drop_database(name)
            except Exception as exc:
                logger.error("could not drop test database %s: %s", name, type(exc).__name__)
                first_error = first_error or exc
        self._template_created = False
        if first_error is not None:
            raise first_error


@pytest_asyncio.fixture(scope="session")
async def admin_engine() -> AsyncIterator[AsyncEngine]:
    """AUTOCOMMIT engine on the administrative test database (``CREATE DATABASE`` needs it).

    Fails, never skips, every requesting test when the server cannot be reached.
    """
    settings = build_test_settings()
    engine = build_engine(
        settings,
        isolation_level="AUTOCOMMIT",
        connect_args={"timeout": ADMIN_CONNECT_TIMEOUT_SECONDS},
    )
    try:
        failure: str | None = None
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception as exc:
            # asyncpg's connect errors (refused, timeout, bad password, missing database) reach
            # here unwrapped by SQLAlchemy. Failing outside this block keeps the original
            # traceback, whose frames show the connection parameters and password, out of the
            # report.
            failure = _unreachable_message(settings.database_url, exc)
        if failure is not None:
            pytest.fail(failure, pytrace=False)
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def database_factory(admin_engine: AsyncEngine) -> AsyncIterator[DatabaseFactory]:
    """The session's :class:`DatabaseFactory`; leftover databases are dropped at session end."""
    factory = DatabaseFactory(admin_engine)
    try:
        yield factory
    finally:
        await factory.close()


@pytest_asyncio.fixture(scope="session")
async def seeded_template(database_factory: DatabaseFactory) -> str:
    """Name of the migrated and seeded template, built once per session and dropped at its end."""
    return await database_factory.seeded_template()


@pytest_asyncio.fixture
async def seeded_database(
    request: pytest.FixtureRequest,
    database_factory: DatabaseFactory,
    seeded_template: str,
) -> AsyncIterator[DatabaseHandle]:
    """A fresh seeded clone for one test, dropped afterwards.

    For tests whose code under test opens its own connections, such as the advisory-lock
    connection and the separate run-record connection of an ingestion run, which an outer
    rollback transaction cannot wrap.
    """
    label = request.module.__name__.rpartition(".")[2].removeprefix("test_")
    handle = await database_factory.create_from_template(label, seeded_template)
    try:
        yield handle
    finally:
        await database_factory.drop(handle)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark ``integration`` every test that needs PostgreSQL through any fixture it requests.

    ``tryfirst`` runs this before pytest's own ``-m`` deselection, so ``-m "not integration"``
    sees the marks. ``fixturenames`` holds the full fixture closure, so a fixture built on a
    database fixture (``integration/conftest.py``'s module clones) marks its tests as well.
    """
    for item in items:
        if DATABASE_FIXTURES.intersection(getattr(item, "fixturenames", ())):
            item.add_marker(pytest.mark.integration)
