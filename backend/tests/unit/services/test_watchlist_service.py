"""Watchlist service: owner-scoped watchlists on seeded PostgreSQL, and the owner-key guard.

``services/watchlist_service.py`` scopes every query to ``terminal_watchlists.owner_key =
principal.subject`` and raises coded ``HTTPException(status, detail={"code", "message",
"field"})`` errors. The contract held here:

- **Owner-key guard.** Before any query, ``create_watchlist`` refuses a subject that is empty,
  longer than ``OWNER_KEY_MAX_LENGTH`` (255) characters or holds a NUL character with
  ``ValueError``, and a non-``str`` subject with ``TypeError``, rather than storing an empty owner
  key or failing at commit with an uncoded driver error. The principal is a host's own object
  satisfying ``OwnerPrincipal``, not ``api.deps.TerminalPrincipal``, which refuses such subjects
  itself. These tests use a session bound to no engine, so any query would raise
  ``UnboundExecutionError`` instead: the guard's exception shows it runs first, and a
  255-character subject reaching ``UnboundExecutionError`` shows the bound is inclusive.
- **Reads.** ``list_watchlists`` returns the caller's lists ordered by name, each with sorted
  tickers and UTC timestamps, and ``[]`` for an owner without lists. ``get_owned_watchlist`` and
  ``owned_watchlist_tickers`` return the seeded "NSE Core 8" of ``local`` with its eight tickers.
- **Ownership.** Another subject's list is answered exactly like a missing id: 404
  ``watchlist_not_found`` (``field="watchlist_id"``) from every function taking a watchlist id,
  with the list left unchanged. Names are unique per owner only.
- **Create.** Tickers are trimmed, upper-cased and de-duplicated. 422 ``unknown_ticker``
  (``field="tickers"``), naming every unknown ticker, is checked before 409
  ``watchlist_name_taken`` (``field="name"``), and an error writes nothing. A rival transaction
  that commits the same name, or deletes a listed equity, between those checks and the insert
  turns the insert's ``IntegrityError`` into the same 409 or 422; an ``IntegrityError`` that the
  database state does not explain is re-raised unchanged.
- **Delete.** The items go with the list through ``ON DELETE CASCADE``, the equities stay, and a
  repeated delete gives 404.
- **Items.** ``add_item`` and ``remove_item`` are idempotent and check the watchlist before the
  ticker: 404 ``watchlist_not_found``, then 404 ``unknown_ticker`` (``field="ticker"``).
  ``updated_at`` advances only when an item is actually inserted or deleted, including inside a
  host's outer transaction, where each service commit only releases a savepoint and ``now()``
  would stay at ``created_at``.

Every database test gets a fresh seeded clone (the root conftest's ``seeded_database``, which
marks it ``integration``) and sessions from ``async_sessionmaker(engine)`` with the default
``expire_on_commit=True`` a host may use. Committed state is read back on a separate session.
The race tests hold the rival's write uncommitted until ``pg_blocking_pids`` shows the service's
connection waiting on it. Nothing outside the test's own clone is touched.
"""

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from types import SimpleNamespace
from typing import Any, Final, Protocol

import pytest
import pytest_asyncio
from fastapi import HTTPException
from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import IntegrityError, UnboundExecutionError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal.models import TerminalEquity, TerminalWatchlist, TerminalWatchlistItem
from opennse_terminal.schemas.watchlists import Watchlist, WatchlistCreate
from opennse_terminal.services import watchlist_service
from opennse_terminal.services.watchlist_service import (
    OwnerPrincipal,
    add_item,
    create_watchlist,
    delete_watchlist,
    get_owned_watchlist,
    list_watchlists,
    owned_watchlist_tickers,
    remove_item,
)


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


# ---------------------------------------------------------------------------------------------
# Database-backed contract, on a fresh seeded clone per test
# ---------------------------------------------------------------------------------------------

SEEDED_NAME: Final = "NSE Core 8"
SEEDED_TICKERS: Final = ["ABSA", "COOP", "EABL", "EQTY", "KCB", "KEGN", "SBIC", "SCOM"]
SEEDED_EQUITY_COUNT: Final = 8
LOCAL: Final = HostPrincipal("local")
OTHER: Final = HostPrincipal("host-user-42")
MISSING_WATCHLIST: Final = uuid.UUID("00000000-0000-4000-8000-000000000000")
LOCK_WAIT_SECONDS: Final = 10
FORGING_TICKER: Final = "Z\r\nINFO FORGED"

SessionFactory = async_sessionmaker[AsyncSession]
WatchlistCall = Callable[[AsyncSession, OwnerPrincipal, uuid.UUID], Awaitable[object]]


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...


@pytest.fixture
def factory(seeded_database: SeededDatabase) -> SessionFactory:
    """Sessions on this test's own seeded clone, with the default ``expire_on_commit=True``."""
    return async_sessionmaker(seeded_database.engine)


@pytest_asyncio.fixture
async def session(factory: SessionFactory) -> AsyncIterator[AsyncSession]:
    """The session the service functions under test run on."""
    async with factory() as db_session:
        yield db_session


@pytest_asyncio.fixture
async def core_id(factory: SessionFactory) -> uuid.UUID:
    """Id of the seeded "NSE Core 8" watchlist of ``local``."""
    async with factory() as fresh:
        watchlist_id = await fresh.scalar(
            select(TerminalWatchlist.id).where(
                TerminalWatchlist.owner_key == LOCAL.subject, TerminalWatchlist.name == SEEDED_NAME
            )
        )
    assert watchlist_id is not None
    return watchlist_id


async def committed_names(factory: SessionFactory, owner_key: str) -> list[str]:
    """Names of the committed watchlists stored under ``owner_key``, sorted."""
    async with factory() as fresh:
        names = await fresh.scalars(
            select(TerminalWatchlist.name)
            .where(TerminalWatchlist.owner_key == owner_key)
            .order_by(TerminalWatchlist.name)
        )
        return list(names)


async def committed_items(factory: SessionFactory, watchlist_id: uuid.UUID) -> list[str]:
    """Committed item tickers of ``watchlist_id``, sorted; ``[]`` once the list is gone."""
    async with factory() as fresh:
        tickers = await fresh.scalars(
            select(TerminalWatchlistItem.ticker)
            .where(TerminalWatchlistItem.watchlist_id == watchlist_id)
            .order_by(TerminalWatchlistItem.ticker)
        )
        return list(tickers)


async def committed_updated_at(factory: SessionFactory, watchlist_id: uuid.UUID) -> datetime:
    """The committed ``updated_at`` of ``watchlist_id``."""
    async with factory() as fresh:
        updated_at = await fresh.scalar(
            select(TerminalWatchlist.updated_at).where(TerminalWatchlist.id == watchlist_id)
        )
    assert updated_at is not None
    return updated_at


def assert_coded(exc: HTTPException, status_code: int, code: str, field: str, message: str) -> None:
    """``exc`` is the coded error ``{code, message, field}`` at ``status_code``."""
    assert exc.status_code == status_code
    assert exc.detail == {"code": code, "message": message, "field": field}


async def backend_pid(session: AsyncSession) -> int:
    """PostgreSQL backend pid of the connection holding ``session``'s current transaction."""
    pid = await session.scalar(text("SELECT pg_backend_pid()"))
    assert isinstance(pid, int)
    return pid


async def commit_once_blocked(
    engine: AsyncEngine, rival: AsyncSession, contender_pid: int, finished: asyncio.Event
) -> bool:
    """Commit ``rival`` once backend ``contender_pid`` waits on a lock the rival holds.

    Returns ``False``, committing nothing, if ``finished`` is set before that wait is seen. On
    any failure, :data:`LOCK_WAIT_SECONDS` elapsing included, the rival is rolled back so the
    contender is never left waiting, and the failure is raised.
    """
    connection = await engine.connect()
    try:
        rival_pid = await backend_pid(rival)
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        async with asyncio.timeout(LOCK_WAIT_SECONDS):
            while not await connection.scalar(
                text("SELECT :rival = ANY(pg_blocking_pids(:contender))"),
                {"rival": rival_pid, "contender": contender_pid},
            ):
                if finished.is_set():
                    return False
                await asyncio.sleep(0)
    except Exception:
        await rival.rollback()
        raise
    finally:
        await connection.close()
    await rival.commit()
    return True


async def against_uncommitted_rival(
    factory: SessionFactory,
    engine: AsyncEngine,
    rival_write: Callable[[AsyncSession], Awaitable[None]],
    call: Callable[[AsyncSession], Awaitable[Watchlist]],
) -> Watchlist:
    """Run ``call`` while ``rival_write`` is uncommitted; commit the rival once ``call`` waits.

    ``call`` runs in the test's own task, the rival's commit in a background task. Returns what
    ``call`` returns and raises what it raises; fails the test if ``call`` never waited on the
    rival.
    """
    async with factory() as rival, factory() as contender:
        await rival_write(rival)
        contender_pid = await backend_pid(contender)
        finished = asyncio.Event()
        releaser = asyncio.create_task(commit_once_blocked(engine, rival, contender_pid, finished))
        try:
            return await call(contender)
        finally:
            finished.set()
            if not await releaser:
                pytest.fail("the service call finished without waiting on the rival transaction")


async def test_list_returns_the_seeded_watchlist_of_local(
    session: AsyncSession, core_id: uuid.UUID
) -> None:
    """``local`` owns exactly the seeded list: its eight tickers sorted, timestamps in UTC."""
    [seeded] = await list_watchlists(session, LOCAL)
    assert (seeded.id, seeded.name, seeded.tickers) == (core_id, SEEDED_NAME, SEEDED_TICKERS)
    assert seeded.created_at.utcoffset() == timedelta(0)
    assert seeded.updated_at.utcoffset() == timedelta(0)


async def test_list_is_ordered_by_name_and_scoped_to_the_owner(
    session: AsyncSession, core_id: uuid.UUID
) -> None:
    """Each owner sees only its own lists, by name; a name is unique per owner only."""
    await create_watchlist(session, LOCAL, WatchlistCreate(name="Zeta", tickers=["SCOM"]))
    await create_watchlist(session, LOCAL, WatchlistCreate(name="Alpha"))
    other = await create_watchlist(
        session, OTHER, WatchlistCreate(name=SEEDED_NAME, tickers=["KCB"])
    )

    local_lists = await list_watchlists(session, LOCAL)
    assert [(item.name, item.tickers) for item in local_lists] == [
        ("Alpha", []),
        (SEEDED_NAME, SEEDED_TICKERS),
        ("Zeta", ["SCOM"]),
    ]
    assert await list_watchlists(session, OTHER) == [other]
    assert other.id != core_id
    assert await list_watchlists(session, HostPrincipal("nobody")) == []


async def test_get_and_tickers_return_the_owned_watchlist(
    session: AsyncSession, core_id: uuid.UUID
) -> None:
    """The owner gets the row with its items loaded, and the sorted tickers on their own."""
    row = await get_owned_watchlist(session, LOCAL, core_id)
    assert (row.id, row.owner_key, row.name) == (core_id, LOCAL.subject, SEEDED_NAME)
    assert [item.ticker for item in row.items] == SEEDED_TICKERS
    assert await owned_watchlist_tickers(session, LOCAL, core_id) == SEEDED_TICKERS


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(get_owned_watchlist, id="get_owned_watchlist"),
        pytest.param(owned_watchlist_tickers, id="owned_watchlist_tickers"),
        pytest.param(delete_watchlist, id="delete_watchlist"),
        pytest.param(partial(add_item, ticker="SCOM"), id="add_item"),
        pytest.param(partial(remove_item, ticker="SCOM"), id="remove_item"),
    ],
)
@pytest.mark.parametrize(
    ("principal", "target_seeded"),
    [
        pytest.param(OTHER, True, id="another-owners-list"),
        pytest.param(LOCAL, False, id="missing-id"),
    ],
)
async def test_another_owners_list_is_answered_like_a_missing_one(
    session: AsyncSession,
    factory: SessionFactory,
    core_id: uuid.UUID,
    call: WatchlistCall,
    principal: HostPrincipal,
    target_seeded: bool,
) -> None:
    """Every id-taking function gives the same 404 for both cases and changes nothing."""
    target = core_id if target_seeded else MISSING_WATCHLIST
    with pytest.raises(HTTPException) as excinfo:
        await call(session, principal, target)
    assert_coded(
        excinfo.value,
        404,
        "watchlist_not_found",
        "watchlist_id",
        f"Watchlist {target} was not found.",
    )
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]
    assert await committed_items(factory, core_id) == SEEDED_TICKERS


@pytest.mark.parametrize(
    ("tickers", "expected"),
    [
        pytest.param([" kcb", "SCOM", "Kcb ", "scom"], ["KCB", "SCOM"], id="normalised-deduped"),
        pytest.param([], [], id="empty"),
    ],
)
async def test_create_stores_the_list_under_the_callers_subject(
    session: AsyncSession, factory: SessionFactory, tickers: list[str], expected: list[str]
) -> None:
    """The name is stripped, tickers normalised and de-duplicated, and the owner is the subject;
    both timestamps come from the same insert."""
    created = await create_watchlist(
        session, LOCAL, WatchlistCreate(name="  Banks  ", tickers=tickers)
    )
    assert (created.name, created.tickers) == ("Banks", expected)
    assert created.updated_at == created.created_at
    assert await committed_names(factory, LOCAL.subject) == ["Banks", SEEDED_NAME]
    assert await committed_items(factory, created.id) == expected


async def test_create_with_a_taken_name_is_409(
    session: AsyncSession, factory: SessionFactory, core_id: uuid.UUID
) -> None:
    """The owner's own name, after stripping, is refused and the existing list is untouched."""
    with pytest.raises(HTTPException) as excinfo:
        await create_watchlist(
            session, LOCAL, WatchlistCreate(name=f" {SEEDED_NAME} ", tickers=["KCB"])
        )
    assert_coded(
        excinfo.value,
        409,
        "watchlist_name_taken",
        "name",
        f"A watchlist named {SEEDED_NAME!r} already exists.",
    )
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]
    assert await committed_items(factory, core_id) == SEEDED_TICKERS


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("Typos", id="new-name"),
        pytest.param(SEEDED_NAME, id="taken-name-checked-after-the-tickers"),
    ],
)
async def test_create_with_unknown_tickers_is_422(
    session: AsyncSession, factory: SessionFactory, name: str
) -> None:
    """Every unknown ticker is named once, normalised and in request order; nothing is written."""
    body = WatchlistCreate(name=name, tickers=["scom", " nope", "XYZ", "NOPE"])
    with pytest.raises(HTTPException) as excinfo:
        await create_watchlist(session, LOCAL, body)
    assert_coded(excinfo.value, 422, "unknown_ticker", "tickers", "Unknown tickers: NOPE, XYZ.")
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]


async def test_name_committed_concurrently_by_a_rival_is_409(
    seeded_database: SeededDatabase, factory: SessionFactory
) -> None:
    """The rival's uncommitted list passes the name check; its commit makes the insert fail on
    the unique (owner_key, name) constraint, which is answered as the 409."""
    rival_id = uuid.uuid4()

    async def rival_write(rival: AsyncSession) -> None:
        rival.add(TerminalWatchlist(id=rival_id, owner_key=LOCAL.subject, name="Race"))
        await rival.flush()

    async def call(contender: AsyncSession) -> Watchlist:
        return await create_watchlist(
            contender, LOCAL, WatchlistCreate(name="Race", tickers=["SCOM"])
        )

    with pytest.raises(HTTPException) as excinfo:
        await against_uncommitted_rival(factory, seeded_database.engine, rival_write, call)
    assert_coded(
        excinfo.value,
        409,
        "watchlist_name_taken",
        "name",
        "A watchlist named 'Race' already exists.",
    )
    assert isinstance(excinfo.value.__cause__, IntegrityError)
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME, "Race"]
    assert await committed_items(factory, rival_id) == []


async def test_equity_deleted_concurrently_by_a_rival_is_422(
    seeded_database: SeededDatabase, factory: SessionFactory
) -> None:
    """The rival's uncommitted delete passes the ticker check; its commit makes the item insert
    fail on the equity foreign key, which is answered as the 422."""
    async with factory() as setup:
        setup.add(
            TerminalEquity(
                ticker="RACE",
                company_name="Race Holdings",
                sector="Banking",
                shares_outstanding=1_000,
                source="csv_import",
            )
        )
        await setup.commit()

    async def rival_write(rival: AsyncSession) -> None:
        await rival.execute(delete(TerminalEquity).where(TerminalEquity.ticker == "RACE"))

    async def call(contender: AsyncSession) -> Watchlist:
        return await create_watchlist(
            contender, LOCAL, WatchlistCreate(name="Vanishing", tickers=["SCOM", "race"])
        )

    with pytest.raises(HTTPException) as excinfo:
        await against_uncommitted_rival(factory, seeded_database.engine, rival_write, call)
    assert_coded(excinfo.value, 422, "unknown_ticker", "tickers", "Unknown tickers: RACE.")
    assert isinstance(excinfo.value.__cause__, IntegrityError)
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]


async def test_integrity_error_the_database_state_does_not_explain_is_reraised(
    session: AsyncSession,
    factory: SessionFactory,
    core_id: uuid.UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new id colliding with an existing list's primary key is neither a taken name nor an
    unknown ticker, so the ``IntegrityError`` propagates rather than becoming a 409 or 422."""
    monkeypatch.setattr(watchlist_service, "uuid", SimpleNamespace(uuid4=lambda: core_id))
    with pytest.raises(IntegrityError):
        await create_watchlist(session, LOCAL, WatchlistCreate(name="Collision", tickers=["KCB"]))
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]
    assert await committed_items(factory, core_id) == SEEDED_TICKERS


async def test_delete_removes_the_list_and_cascades_to_its_items(
    session: AsyncSession, factory: SessionFactory, core_id: uuid.UUID
) -> None:
    """The items go through ``ON DELETE CASCADE``, the equities stay, and a repeat is a 404."""
    assert await delete_watchlist(session, LOCAL, core_id) is None
    assert await committed_names(factory, LOCAL.subject) == []
    assert await committed_items(factory, core_id) == []
    async with factory() as fresh:
        assert await fresh.scalar(select(func.count()).select_from(TerminalEquity)) == (
            SEEDED_EQUITY_COUNT
        )
    with pytest.raises(HTTPException) as excinfo:
        await delete_watchlist(session, LOCAL, core_id)
    assert_coded(
        excinfo.value,
        404,
        "watchlist_not_found",
        "watchlist_id",
        f"Watchlist {core_id} was not found.",
    )


async def test_item_changes_are_idempotent_and_advance_updated_at_only_when_real(
    session: AsyncSession, factory: SessionFactory
) -> None:
    """Create, add KCB twice, remove it twice: a repeat returns the list exactly as it was,
    ``updated_at`` included, while each real change moves ``updated_at`` forward."""
    created = await create_watchlist(
        session, LOCAL, WatchlistCreate(name="Acceptance", tickers=["SCOM"])
    )
    assert created.tickers == ["SCOM"]

    added = await add_item(session, LOCAL, created.id, " kcb ")
    assert added.tickers == ["KCB", "SCOM"]
    assert added.created_at == created.created_at
    assert added.updated_at > created.updated_at
    assert await add_item(session, LOCAL, created.id, "KCB") == added

    removed = await remove_item(session, LOCAL, created.id, "kcb")
    assert removed.tickers == ["SCOM"]
    assert removed.updated_at > added.updated_at
    assert await remove_item(session, LOCAL, created.id, "KCB") == removed

    assert await committed_items(factory, created.id) == ["SCOM"]
    assert await committed_updated_at(factory, created.id) == removed.updated_at


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(partial(add_item, ticker=" nope "), id="add_item"),
        pytest.param(partial(remove_item, ticker=" nope "), id="remove_item"),
    ],
)
@pytest.mark.parametrize(
    ("target_seeded", "code", "field"),
    [
        pytest.param(True, "unknown_ticker", "ticker", id="owned-list"),
        pytest.param(False, "watchlist_not_found", "watchlist_id", id="missing-list-checked-first"),
    ],
)
async def test_item_change_with_an_unknown_ticker_is_404(
    session: AsyncSession,
    factory: SessionFactory,
    core_id: uuid.UUID,
    call: WatchlistCall,
    target_seeded: bool,
    code: str,
    field: str,
) -> None:
    """An unknown ticker on an owned list is 404 ``unknown_ticker`` and leaves the list and its
    ``updated_at`` as they were; on a missing list the watchlist error wins."""
    target = core_id if target_seeded else MISSING_WATCHLIST
    message = (
        "Ticker 'NOPE' is not a known equity."
        if target_seeded
        else f"Watchlist {MISSING_WATCHLIST} was not found."
    )
    updated_before = await committed_updated_at(factory, core_id)
    with pytest.raises(HTTPException) as excinfo:
        await call(session, LOCAL, target)
    assert_coded(excinfo.value, 404, code, field, message)
    assert await committed_items(factory, core_id) == SEEDED_TICKERS
    assert await committed_updated_at(factory, core_id) == updated_before


@pytest.mark.parametrize(
    ("call", "listed_first"),
    [
        pytest.param(partial(add_item, ticker=FORGING_TICKER), False, id="add_item"),
        pytest.param(partial(remove_item, ticker=FORGING_TICKER), True, id="remove_item"),
    ],
)
async def test_item_change_logs_a_control_character_ticker_escaped(
    session: AsyncSession,
    factory: SessionFactory,
    caplog: pytest.LogCaptureFixture,
    call: WatchlistCall,
    listed_first: bool,
) -> None:
    """A stored ticker holding CR and LF reaches the DEBUG record of a real item change as its
    ``repr``, so the record stays on one line and cannot forge another."""
    async with factory() as setup:
        setup.add(
            TerminalEquity(
                ticker=FORGING_TICKER,
                company_name="Forged Holdings",
                sector="Banking",
                shares_outstanding=1_000,
                source="csv_import",
            )
        )
        await setup.commit()
    created = await create_watchlist(session, LOCAL, WatchlistCreate(name="Forgery"))
    if listed_first:
        await add_item(session, LOCAL, created.id, FORGING_TICKER)
    caplog.clear()

    with caplog.at_level(logging.DEBUG, logger=watchlist_service.logger.name):
        await call(session, LOCAL, created.id)

    [message] = [
        record.getMessage()
        for record in caplog.records
        if record.name == watchlist_service.logger.name
    ]
    assert "\r" not in message
    assert "\n" not in message
    assert repr(FORGING_TICKER) in message
    assert message.endswith(": changed=True")


async def test_updated_at_advances_inside_a_hosts_outer_transaction(
    seeded_database: SeededDatabase, factory: SessionFactory
) -> None:
    """With every service commit only releasing a savepoint of one outer transaction, a real
    item change still moves ``updated_at`` past ``created_at`` (``now()`` would not)."""
    async with seeded_database.engine.connect() as connection:
        await connection.begin()
        nested = AsyncSession(bind=connection, join_transaction_mode="create_savepoint")
        try:
            created = await create_watchlist(nested, LOCAL, WatchlistCreate(name="Nested"))
            added = await add_item(nested, LOCAL, created.id, "SCOM")
            unchanged = await add_item(nested, LOCAL, created.id, "SCOM")
        finally:
            await nested.close()
            await connection.rollback()
    assert created.updated_at == created.created_at
    assert added.tickers == ["SCOM"]
    assert added.updated_at > added.created_at == created.created_at
    assert unchanged == added
    assert await committed_names(factory, LOCAL.subject) == [SEEDED_NAME]
