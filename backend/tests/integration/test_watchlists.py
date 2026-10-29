"""Watchlist ownership: the ``/watchlists*`` routes, the watchlist service and the principal.

The stored ``terminal_watchlists.owner_key`` is always the principal's opaque subject. The module
has three sections: the API routes, whose ownership, conflict and idempotency contract through
``/api/v1/terminal/watchlists*`` follows; then the watchlist service called directly, and the
principal contract of ``api.deps``, whose subject bound is what ``owner_key`` can store, each
opening with a banner that states its own contract.

In the API section every request goes through ``create_app()`` (the ``client`` fixture) against
this module's seeded clone. Ownership comes only from the injected principal: identities are
switched exclusively with the ``set_principal`` fixture, which overrides
``get_terminal_principal``, and the stored ``terminal_watchlists.owner_key`` is the principal's
opaque subject. No user table exists or is consulted.

Each API test runs inside the ``db_session`` outer transaction, where a service's ``commit()``
only releases a SAVEPOINT, and that transaction is rolled back afterwards. The seeded "NSE Core 8"
list of the default principal ``local`` is therefore present and unchanged in every test, and no
test depends on another's writes or on their order. Database checks read and write through the
same ``db_session``, with Core ``select()`` and ``update()`` over the ``opennse_terminal.models``
columns and every value bound.
"""

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from types import SimpleNamespace
from typing import Any, Final, Protocol

import httpx
import pytest
import pytest_asyncio
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import IntegrityError, UnboundExecutionError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal.api.deps import TerminalPrincipal, get_terminal_principal
from opennse_terminal.config import TerminalSettings
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

BASE: Final = "/api/v1/terminal"
"""Prefix of every terminal endpoint."""

SEEDED_WATCHLIST: Final = "NSE Core 8"
"""The watchlist the seeder creates for the default principal ``local``."""

SEEDED_TICKERS: Final = ["ABSA", "COOP", "EABL", "EQTY", "KCB", "KEGN", "SBIC", "SCOM"]
"""The eight seeded counters on ``SEEDED_WATCHLIST``, sorted as the API returns them."""


class PrincipalSetter(Protocol):
    """Signature of the integration conftest's ``set_principal`` fixture."""

    def __call__(self, subject: str | None, *, authenticated: bool = True) -> None: ...


async def create(
    client: httpx.AsyncClient, name: str, tickers: Iterable[str] = ()
) -> dict[str, Any]:
    """``POST /watchlists`` as the current principal; assert 201 and return the ``Watchlist``."""
    response = await client.post(
        f"{BASE}/watchlists", json={"name": name, "tickers": list(tickers)}
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def code(response: httpx.Response) -> str:
    """Return the ``code`` of a coded error body ``{"detail": {"code", "message", "field"}}``."""
    detail = response.json()["detail"]
    assert isinstance(detail, dict), f"expected a coded error object, got {detail!r}"
    return str(detail["code"])


def field(response: httpx.Response) -> str | None:
    """Return the ``field`` a coded error body names."""
    detail = response.json()["detail"]
    assert isinstance(detail, dict), f"expected a coded error object, got {detail!r}"
    value = detail["field"]
    return None if value is None else str(value)


def assert_validation_list(response: httpx.Response, *offending: str | int) -> None:
    """Assert FastAPI's standard 422 body: a ``detail`` list with an entry whose ``loc`` ends
    with ``offending``, for example ``"watchlist_id"`` or ``"body", "tickers", 0``."""
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list), f"expected the standard validation list, got {detail!r}"
    suffix = list(offending)
    assert any(entry["loc"][-len(suffix) :] == suffix for entry in detail), detail


async def names(client: httpx.AsyncClient) -> list[str]:
    """``GET /watchlists`` as the current principal; the names, in the API's order."""
    response = await client.get(f"{BASE}/watchlists")
    assert response.status_code == 200, response.text
    return [watchlist["name"] for watchlist in response.json()]


async def stored_items(db_session: AsyncSession, watchlist_id: str) -> list[str]:
    """Tickers of the ``terminal_watchlist_items`` rows referencing ``watchlist_id``, sorted."""
    result = await db_session.execute(
        select(TerminalWatchlistItem.ticker)
        .where(TerminalWatchlistItem.watchlist_id == uuid.UUID(watchlist_id))
        .order_by(TerminalWatchlistItem.ticker)
    )
    return [str(ticker) for ticker in result.scalars().all()]


async def watchlist_count(db_session: AsyncSession) -> int:
    """Number of ``terminal_watchlists`` rows, across every owner."""
    result = await db_session.execute(select(func.count()).select_from(TerminalWatchlist))
    return int(result.scalar_one())


# ---------------------------------------------------------------------------------------------
# Ownership
# ---------------------------------------------------------------------------------------------


async def test_lists_are_scoped_to_principal(
    client: httpx.AsyncClient, db_session: AsyncSession, set_principal: PrincipalSetter
) -> None:
    """Another subject neither sees nor changes a list; the default ``local`` sees only its own."""
    set_principal("alice")
    alpha = await create(client, "Alpha", ["SCOM"])
    watchlist_id = alpha["id"]
    assert alpha["tickers"] == ["SCOM"]

    set_principal("bob")
    listed = await client.get(f"{BASE}/watchlists")
    assert listed.status_code == 200
    assert listed.json() == []
    for response in (
        await client.delete(f"{BASE}/watchlists/{watchlist_id}"),
        await client.put(f"{BASE}/watchlists/{watchlist_id}/items/KCB"),
        await client.delete(f"{BASE}/watchlists/{watchlist_id}/items/SCOM"),
    ):
        assert response.status_code == 404, response.text
        assert code(response) == "watchlist_not_found"

    # Bob's attempts changed nothing, timestamps included.
    set_principal("alice")
    listed = await client.get(f"{BASE}/watchlists")
    assert listed.status_code == 200
    assert listed.json() == [alpha]

    set_principal(None)
    listed = await client.get(f"{BASE}/watchlists")
    assert listed.status_code == 200
    assert [(item["name"], item["tickers"]) for item in listed.json()] == [
        (SEEDED_WATCHLIST, SEEDED_TICKERS)
    ]

    result = await db_session.execute(
        select(TerminalWatchlist.owner_key).where(TerminalWatchlist.id == uuid.UUID(watchlist_id))
    )
    assert result.scalar_one() == "alice"
    assert await stored_items(db_session, watchlist_id) == ["SCOM"]


async def test_screener_watchlist_filter_respects_owner(
    client: httpx.AsyncClient, set_principal: PrincipalSetter
) -> None:
    """The screener's ``watchlist_id`` filter resolves only among the caller's own lists."""
    set_principal("alice")
    pair = await create(client, "Pair", ["KCB", "SCOM"])
    params = {"watchlist_id": pair["id"]}

    response = await client.get(f"{BASE}/market/screener", params=params)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["count"] == 2
    assert {row["ticker"] for row in body["rows"]} == {"KCB", "SCOM"}

    set_principal("bob")
    response = await client.get(f"{BASE}/market/screener", params=params)
    assert response.status_code == 404, response.text
    assert code(response) == "watchlist_not_found"


# ---------------------------------------------------------------------------------------------
# Conflicts and validation
# ---------------------------------------------------------------------------------------------


async def test_duplicate_name_is_conflict_per_owner(
    client: httpx.AsyncClient, set_principal: PrincipalSetter
) -> None:
    """Names are unique per ``(owner_key, name)``: a repeat is 409, another owner may reuse it."""
    set_principal("alice")
    first = await create(client, "Banks", ["KCB"])

    duplicate = await client.post(f"{BASE}/watchlists", json={"name": "Banks", "tickers": []})
    assert duplicate.status_code == 409, duplicate.text
    assert code(duplicate) == "watchlist_name_taken"
    assert field(duplicate) == "name"
    listed = await client.get(f"{BASE}/watchlists")
    assert listed.json() == [first]

    set_principal("bob")
    bobs = await create(client, "Banks")
    assert bobs["id"] != first["id"]
    assert bobs["name"] == "Banks"
    assert bobs["tickers"] == []


async def test_create_rejects_unknown_ticker(
    client: httpx.AsyncClient, db_session: AsyncSession, set_principal: PrincipalSetter
) -> None:
    """One unknown ticker rejects the whole create with the coded 422 and writes nothing."""
    set_principal("alice")
    response = await client.post(
        f"{BASE}/watchlists", json={"name": "Bad", "tickers": ["SCOM", "NOPE"]}
    )
    assert response.status_code == 422, response.text
    assert code(response) == "unknown_ticker"
    assert field(response) == "tickers"
    assert "NOPE" in response.json()["detail"]["message"]

    assert "Bad" not in await names(client)
    result = await db_session.execute(
        select(func.count())
        .select_from(TerminalWatchlist)
        .where(TerminalWatchlist.owner_key == "alice", TerminalWatchlist.name == "Bad")
    )
    assert result.scalar_one() == 0


@pytest.mark.parametrize(
    ("body", "offending"),
    [
        pytest.param({"name": "x" * 101}, ("body", "name"), id="name-too-long"),
        pytest.param({"name": ""}, ("body", "name"), id="name-empty"),
        pytest.param(
            {"name": "x", "owner_key": "y"}, ("body", "owner_key"), id="extra-key-forbidden"
        ),
        pytest.param({"name": "bad\x00name"}, ("body", "name"), id="name-nul"),
        pytest.param({"name": "A\x00B"}, ("body", "name"), id="name-nul-between-letters"),
        pytest.param(
            {"name": "Bad", "tickers": ["SCOM\x00"]}, ("body", "tickers", 0), id="ticker-nul"
        ),
    ],
)
async def test_field_bounds_use_standard_validation_list(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    body: dict[str, Any],
    offending: tuple[str | int, ...],
) -> None:
    """Single-field failures get FastAPI's standard 422 list, with no stable code and no write.

    The extra-key case also shows that a body can never choose the owner. PostgreSQL text cannot
    hold a NUL character, so the NUL cases show validation refusing one before any query.
    """
    before = await watchlist_count(db_session)

    response = await client.post(f"{BASE}/watchlists", json=body)

    assert_validation_list(response, *offending)
    assert await watchlist_count(db_session) == before
    assert await names(client) == [SEEDED_WATCHLIST]


# ---------------------------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------------------------


async def test_items_are_idempotent_and_sorted(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """Tickers come back sorted; repeating an item PUT or DELETE answers 200 and changes nothing."""
    mixed = await create(client, "Mixed", ["SCOM", "ABSA", "KCB"])
    assert mixed["tickers"] == ["ABSA", "KCB", "SCOM"]
    items_url = f"{BASE}/watchlists/{mixed['id']}/items"

    added = [await client.put(f"{items_url}/EQTY") for _ in range(2)]
    for response in added:
        assert response.status_code == 200, response.text
        assert response.json()["tickers"] == ["ABSA", "EQTY", "KCB", "SCOM"]
    # The repeat is a no-op, so it leaves updated_at where the first addition put it.
    assert added[1].json() == added[0].json()
    assert await stored_items(db_session, mixed["id"]) == ["ABSA", "EQTY", "KCB", "SCOM"]

    removed = [await client.delete(f"{items_url}/ABSA") for _ in range(2)]
    for response in removed:
        assert response.status_code == 200, response.text
        assert response.json()["tickers"] == ["EQTY", "KCB", "SCOM"]
    assert removed[1].json() == removed[0].json()
    assert await stored_items(db_session, mixed["id"]) == ["EQTY", "KCB", "SCOM"]


async def test_unknown_watchlist_or_ticker_is_404(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """A missing list or an unknown ticker is a coded 404; a malformed id is the standard 422."""
    missing = uuid.uuid4()
    for method, path in (
        ("PUT", f"/watchlists/{missing}/items/SCOM"),
        ("DELETE", f"/watchlists/{missing}/items/SCOM"),
        ("DELETE", f"/watchlists/{missing}"),
    ):
        response = await client.request(method, f"{BASE}{path}")
        assert response.status_code == 404, f"{method} {path}: {response.text}"
        assert code(response) == "watchlist_not_found"
        assert field(response) == "watchlist_id"

    owned = await create(client, "Owned", ["SCOM"])
    for method in ("PUT", "DELETE"):
        response = await client.request(method, f"{BASE}/watchlists/{owned['id']}/items/NOPE")
        assert response.status_code == 404, f"{method}: {response.text}"
        assert code(response) == "unknown_ticker"
        assert field(response) == "ticker"
    assert await stored_items(db_session, owned["id"]) == ["SCOM"]

    for method, path in (
        ("DELETE", "/watchlists/not-a-uuid"),
        ("PUT", "/watchlists/not-a-uuid/items/SCOM"),
        ("DELETE", "/watchlists/not-a-uuid/items/SCOM"),
    ):
        response = await client.request(method, f"{BASE}{path}")
        assert_validation_list(response, "watchlist_id")


async def test_item_change_bumps_updated_at(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """A real item addition or removal advances ``updated_at`` and leaves ``created_at`` alone.

    The whole test runs inside one outer transaction, whose ``now()`` never moves, so each step
    first backdates both timestamps by an hour; a bump is then observable as a later value.
    """
    clock = await create(client, "Clock")
    watchlist_id = uuid.UUID(clock["id"])

    for method in ("PUT", "DELETE"):
        await db_session.execute(
            update(TerminalWatchlist)
            .where(TerminalWatchlist.id == watchlist_id)
            .values(
                updated_at=TerminalWatchlist.updated_at - timedelta(hours=1),
                created_at=TerminalWatchlist.created_at - timedelta(hours=1),
            )
        )
        # Releases the savepoint only; the outer transaction still rolls everything back.
        await db_session.commit()
        db_session.expire_all()
        result = await db_session.execute(
            select(TerminalWatchlist.created_at, TerminalWatchlist.updated_at).where(
                TerminalWatchlist.id == watchlist_id
            )
        )
        backdated_created, backdated_updated = result.one()

        response = await client.request(method, f"{BASE}/watchlists/{watchlist_id}/items/KCB")
        assert response.status_code == 200, f"{method}: {response.text}"
        body = response.json()
        created_at = datetime.fromisoformat(body["created_at"])
        updated_at = datetime.fromisoformat(body["updated_at"])
        assert created_at.tzinfo is not None
        assert updated_at.tzinfo is not None
        assert created_at == backdated_created, method
        assert updated_at > backdated_updated, method

    first_created = datetime.fromisoformat(clock["created_at"])
    assert created_at == first_created - timedelta(hours=2)
    assert body["tickers"] == []


# ---------------------------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------------------------


async def test_delete_returns_204_and_cascades_items(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """Deleting a list answers 204 with no body, removes its items, and a repeat is 404."""
    gone = await create(client, "Gone", ["EABL", "KEGN"])
    watchlist_id = gone["id"]
    assert await stored_items(db_session, watchlist_id) == ["EABL", "KEGN"]

    response = await client.delete(f"{BASE}/watchlists/{watchlist_id}")
    assert response.status_code == 204, response.text
    assert response.content == b""

    listed = await client.get(f"{BASE}/watchlists")
    assert listed.status_code == 200
    assert watchlist_id not in [watchlist["id"] for watchlist in listed.json()]
    assert [watchlist["name"] for watchlist in listed.json()] == [SEEDED_WATCHLIST]

    again = await client.delete(f"{BASE}/watchlists/{watchlist_id}")
    assert again.status_code == 404, again.text
    assert code(again) == "watchlist_not_found"
    assert await stored_items(db_session, watchlist_id) == []


# ---------------------------------------------------------------------------------------------
# Watchlist service: owner-scoped watchlists on seeded PostgreSQL, and the owner-key guard
# ---------------------------------------------------------------------------------------------
#
# ``services/watchlist_service.py`` scopes every query to ``terminal_watchlists.owner_key =
# principal.subject`` and raises coded ``HTTPException(status, detail={"code", "message",
# "field"})`` errors. The contract held here:
#
# - **Owner-key guard.** Before any query, ``create_watchlist`` refuses a subject that is empty,
#   longer than ``OWNER_KEY_MAX_LENGTH`` (255) characters or holds a NUL character with
#   ``ValueError``, and a non-``str`` subject with ``TypeError``, rather than storing an empty owner
#   key or failing at commit with an uncoded driver error. The principal is a host's own object
#   satisfying ``OwnerPrincipal``, not ``api.deps.TerminalPrincipal``, which refuses such subjects
#   itself. These tests use a session bound to no engine, so any query would raise
#   ``UnboundExecutionError`` instead: the guard's exception shows it runs first, and a
#   255-character subject reaching ``UnboundExecutionError`` shows the bound is inclusive.
# - **Reads.** ``list_watchlists`` returns the caller's lists ordered by name, each with sorted
#   tickers and UTC timestamps, and ``[]`` for an owner without lists. ``get_owned_watchlist`` and
#   ``owned_watchlist_tickers`` return the seeded "NSE Core 8" of ``local`` with its eight tickers.
# - **Ownership.** Another subject's list is answered exactly like a missing id: 404
#   ``watchlist_not_found`` (``field="watchlist_id"``) from every function taking a watchlist id,
#   with the list left unchanged. Names are unique per owner only.
# - **Create.** Tickers are trimmed, upper-cased and de-duplicated. 422 ``unknown_ticker``
#   (``field="tickers"``), naming every unknown ticker, is checked before 409
#   ``watchlist_name_taken`` (``field="name"``), and an error writes nothing. A rival transaction
#   that commits the same name, or deletes a listed equity, between those checks and the insert
#   turns the insert's ``IntegrityError`` into the same 409 or 422; an ``IntegrityError`` that the
#   database state does not explain is re-raised unchanged.
# - **Delete.** The items go with the list through ``ON DELETE CASCADE``, the equities stay, and a
#   repeated delete gives 404.
# - **Items.** ``add_item`` and ``remove_item`` are idempotent and check the watchlist before the
#   ticker: 404 ``watchlist_not_found``, then 404 ``unknown_ticker`` (``field="ticker"``).
#   ``updated_at`` advances only when an item is actually inserted or deleted, including inside a
#   host's outer transaction, where each service commit only releases a savepoint and ``now()``
#   would stay at ``created_at``.
#
# Every database test of this section gets a fresh seeded clone (the root conftest's
# ``seeded_database``, which marks it ``integration``), never the module clone, and sessions from
# ``async_sessionmaker(engine)`` with the default ``expire_on_commit=True`` a host may use. The
# race tests hold the rival's write uncommitted until ``pg_blocking_pids`` shows the service's
# connection waiting on it, on concurrent connections that the API section's rollback-wrapped
# ``db_session`` cannot wrap. Committed state is read back on a separate session. Nothing outside
# the test's own clone is touched.


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

# SEEDED_TICKERS, the seeded list's eight tickers, is the module-level constant above.
SEEDED_NAME: Final = SEEDED_WATCHLIST
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


# ---------------------------------------------------------------------------------------------
# Principal contract of ``opennse_terminal.api.deps``: the subject bound and the default principal
# ---------------------------------------------------------------------------------------------
#
# ``TerminalPrincipal.subject`` is stored as ``terminal_watchlists.owner_key``
# (``VARCHAR(255)``), so construction refuses a subject the column cannot store: an empty one,
# one longer than 255 characters or one holding a NUL character raises ``ValueError``, and a
# non-``str`` raises ``TypeError``. The messages state the rule and the length, never the
# subject. Subjects at the bounds (1 and 255 characters) and subjects differing only in case or
# surrounding spaces are kept exactly as given. ``get_terminal_principal`` returns
# ``settings.default_owner_key`` (``local``) unauthenticated, and ``TerminalSettings`` refuses the
# same subjects the principal refuses, so the default principal can always be built. Settings are
# built with ``_env_file=None`` and without ``TERMINAL_DEFAULT_OWNER_KEY`` (each test of this
# section requests ``_no_owner_key_env``), so the checkout's ``.env`` is never read. Offline: no
# database, network or filesystem is touched, although this directory marks the tests
# ``integration``.


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


@pytest.fixture
def _no_owner_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove an inherited default owner key, so each settings object sees only what it sets.

    Requested through ``usefixtures`` by the principal tests only, never module-wide.
    """
    monkeypatch.delenv("TERMINAL_DEFAULT_OWNER_KEY", raising=False)


@pytest.mark.usefixtures("_no_owner_key_env")
@pytest.mark.parametrize(("subject", "message"), UNSTORABLE_SUBJECTS)
def test_subject_the_owner_key_cannot_store_is_refused(subject: str, message: str) -> None:
    """An empty, a 256-character and a NUL-holding subject raise ``ValueError`` naming the rule;
    the message carries the length or the NUL's index, not the subject."""
    with pytest.raises(ValueError, match=message) as excinfo:
        TerminalPrincipal(subject=subject, is_authenticated=True)
    assert not subject or subject not in str(excinfo.value)


@pytest.mark.usefixtures("_no_owner_key_env")
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


@pytest.mark.usefixtures("_no_owner_key_env")
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


@pytest.mark.usefixtures("_no_owner_key_env")
async def test_default_principal_is_local_and_unauthenticated() -> None:
    """Without a host override the caller is ``local`` and unauthenticated."""
    principal = await get_terminal_principal(TerminalSettings(_env_file=None))
    assert principal == TerminalPrincipal(subject="local", is_authenticated=False)


@pytest.mark.usefixtures("_no_owner_key_env")
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


@pytest.mark.usefixtures("_no_owner_key_env")
@pytest.mark.parametrize(
    "owner_key", [pytest.param("s", id="1-character"), pytest.param("s" * 255, id="255-characters")]
)
async def test_default_principal_carries_a_bound_owner_key_exactly(owner_key: str) -> None:
    """A default owner key at either bound loads and becomes the default principal's subject."""
    settings = TerminalSettings(default_owner_key=owner_key, _env_file=None)
    principal = await get_terminal_principal(settings)
    assert principal == TerminalPrincipal(subject=owner_key, is_authenticated=False)
