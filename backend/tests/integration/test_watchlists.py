"""Watchlist ownership, conflicts and idempotency through ``/api/v1/terminal/watchlists*``.

Every request goes through ``create_app()`` (the ``client`` fixture) against this module's seeded
clone. Ownership comes only from the injected principal: identities are switched exclusively with
the ``set_principal`` fixture, which overrides ``get_terminal_principal``, and the stored
``terminal_watchlists.owner_key`` is the principal's opaque subject. No user table exists or is
consulted.

Each test runs inside the ``db_session`` outer transaction, where a service's ``commit()`` only
releases a SAVEPOINT, and that transaction is rolled back afterwards. The seeded "NSE Core 8" list
of the default principal ``local`` is therefore present and unchanged in every test, and no test
depends on another's writes or on their order. Database checks read through the same
``db_session``, with SQL in ``text()`` and bound parameters only.
"""

import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any, Final, Protocol

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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


def assert_validation_list(response: httpx.Response, offending: str) -> None:
    """Assert FastAPI's standard 422 body: a ``detail`` list with an entry for ``offending``."""
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list), f"expected the standard validation list, got {detail!r}"
    assert any(entry["loc"][-1] == offending for entry in detail), detail


async def names(client: httpx.AsyncClient) -> list[str]:
    """``GET /watchlists`` as the current principal; the names, in the API's order."""
    response = await client.get(f"{BASE}/watchlists")
    assert response.status_code == 200, response.text
    return [watchlist["name"] for watchlist in response.json()]


async def stored_items(db_session: AsyncSession, watchlist_id: str) -> list[str]:
    """Tickers of the ``terminal_watchlist_items`` rows referencing ``watchlist_id``, sorted."""
    result = await db_session.execute(
        text(
            "SELECT ticker FROM terminal_watchlist_items WHERE watchlist_id = :id ORDER BY ticker"
        ),
        {"id": uuid.UUID(watchlist_id)},
    )
    return [str(ticker) for ticker in result.scalars().all()]


async def watchlist_count(db_session: AsyncSession) -> int:
    """Number of ``terminal_watchlists`` rows, across every owner."""
    result = await db_session.execute(text("SELECT count(*) FROM terminal_watchlists"))
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
        text("SELECT owner_key FROM terminal_watchlists WHERE id = :id"),
        {"id": uuid.UUID(watchlist_id)},
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
        text("SELECT count(*) FROM terminal_watchlists WHERE owner_key = :owner AND name = :name"),
        {"owner": "alice", "name": "Bad"},
    )
    assert result.scalar_one() == 0


@pytest.mark.parametrize(
    ("body", "offending"),
    [
        pytest.param({"name": "x" * 101}, "name", id="name-too-long"),
        pytest.param({"name": ""}, "name", id="name-empty"),
        pytest.param({"name": "x", "owner_key": "y"}, "owner_key", id="extra-key-forbidden"),
    ],
)
async def test_field_bounds_use_standard_validation_list(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    body: dict[str, str],
    offending: str,
) -> None:
    """Single-field failures get FastAPI's standard 422 list, with no stable code and no write.

    The extra-key case also shows that a body can never choose the owner.
    """
    before = await watchlist_count(db_session)

    response = await client.post(f"{BASE}/watchlists", json=body)

    assert_validation_list(response, offending)
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
            text(
                "UPDATE terminal_watchlists "
                "SET updated_at = updated_at - interval '1 hour', "
                "created_at = created_at - interval '1 hour' "
                "WHERE id = :id"
            ),
            {"id": watchlist_id},
        )
        # Releases the savepoint only; the outer transaction still rolls everything back.
        await db_session.commit()
        db_session.expire_all()
        result = await db_session.execute(
            text("SELECT created_at, updated_at FROM terminal_watchlists WHERE id = :id"),
            {"id": watchlist_id},
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
