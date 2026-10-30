"""Owner-scoped custom watchlists: the service behind the ``/watchlists`` endpoints.

Paths are relative to ``/api/v1/terminal``:

=====================================================  ==================  ======================
Endpoint                                               Function            Success
=====================================================  ==================  ======================
``GET /watchlists``                                    list_watchlists     ``list[Watchlist]``
``POST /watchlists``                                   create_watchlist    201 ``Watchlist``
``DELETE /watchlists/{watchlist_id}``                  delete_watchlist    204
``PUT /watchlists/{watchlist_id}/items/{ticker}``      add_item            200 ``Watchlist``
``DELETE /watchlists/{watchlist_id}/items/{ticker}``   remove_item         200 ``Watchlist``
=====================================================  ==================  ======================

Ownership comes only from the caller's principal: every query filters on
``terminal_watchlists.owner_key = principal.subject``, the opaque subject resolved by the
router-level, host-overridable principal dependency. No users or auth table exists or is queried.
A watchlist owned by another subject is answered exactly like a missing one (404
``watchlist_not_found``), so its existence is never revealed.

Business errors are coded ``HTTPException`` bodies ``{code, message, field}`` (``ErrorDetail``);
the module registers no exception handlers. Write functions own their transaction and call
``await session.commit()`` themselves. After a commit they never touch an ORM attribute loaded
before it: they re-select the watchlist with ``populate_existing``, so a host session created with
``expire_on_commit=True`` never triggers an implicit lazy load, which fails under ``AsyncSession``.

This module never imports ``opennse_terminal.api``: the routes import the services, so the reverse
import would be a cycle. ``api.deps.TerminalPrincipal`` satisfies :class:`OwnerPrincipal`
structurally.
"""

import logging
import uuid
from collections.abc import Iterable, Sequence
from typing import Protocol

from fastapi import HTTPException, status
from sqlalchemy import Select, delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from opennse_terminal.models import TerminalEquity, TerminalWatchlist, TerminalWatchlistItem
from opennse_terminal.models.watchlists import OWNER_KEY_MAX_LENGTH
from opennse_terminal.schemas.common import ErrorDetail
from opennse_terminal.schemas.watchlists import Watchlist, WatchlistCreate

__all__ = [
    "OwnerPrincipal",
    "add_item",
    "create_watchlist",
    "delete_watchlist",
    "get_owned_watchlist",
    "list_watchlists",
    "owned_watchlist_tickers",
    "remove_item",
]

logger = logging.getLogger("opennse_terminal.services.watchlist_service")


class OwnerPrincipal(Protocol):
    """The caller whose watchlists a service function may read or change.

    ``subject`` is a read-only property rather than a plain attribute so that frozen dataclasses,
    such as ``api.deps.TerminalPrincipal``, satisfy the protocol under ``mypy --strict``.
    """

    @property
    def subject(self) -> str:
        """Opaque owner key stored in ``terminal_watchlists.owner_key``."""
        ...


def _coded_error(
    status_code: int, code: str, message: str, field: str | None = None
) -> HTTPException:
    """Build the coded error ``{"detail": {"code", "message", "field"}}`` for ``status_code``."""
    return HTTPException(
        status_code, detail=ErrorDetail(code=code, message=message, field=field).model_dump()
    )


def _watchlist_not_found(watchlist_id: uuid.UUID) -> HTTPException:
    return _coded_error(
        status.HTTP_404_NOT_FOUND,
        "watchlist_not_found",
        f"Watchlist {watchlist_id} was not found.",
        "watchlist_id",
    )


def _name_taken(name: str) -> HTTPException:
    return _coded_error(
        status.HTTP_409_CONFLICT,
        "watchlist_name_taken",
        f"A watchlist named {name!r} already exists.",
        "name",
    )


def _unknown_create_tickers(unknown: Sequence[str]) -> HTTPException:
    return _coded_error(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "unknown_ticker",
        f"Unknown tickers: {', '.join(unknown)}.",
        "tickers",
    )


def _unknown_item_ticker(ticker: str) -> HTTPException:
    return _coded_error(
        status.HTTP_404_NOT_FOUND,
        "unknown_ticker",
        f"Ticker {ticker!r} is not a known equity.",
        "ticker",
    )


def _require_storable_subject(principal: OwnerPrincipal) -> None:
    """Refuse a ``principal.subject`` that ``terminal_watchlists.owner_key`` cannot store.

    The owner key holds 1 to ``OWNER_KEY_MAX_LENGTH`` characters and, like all PostgreSQL text,
    no NUL character. ``api.deps.TerminalPrincipal`` already refuses any other subject, but a host
    override may return its own object, so the write checks again rather than letting the
    database reject the row with an uncoded driver error. A violation is a broken host contract,
    not a client error: ``TypeError`` for a non-``str`` subject, ``ValueError`` otherwise. The
    message never repeats the subject.
    """
    subject = principal.subject
    if not isinstance(subject, str):
        raise TypeError(f"principal.subject must be a str, got {type(subject).__name__}")
    if not 1 <= len(subject) <= OWNER_KEY_MAX_LENGTH:
        raise ValueError(
            f"principal.subject must be 1-{OWNER_KEY_MAX_LENGTH} characters, got {len(subject)}"
        )
    if "\x00" in subject:
        raise ValueError("principal.subject must not contain a NUL character")


def _normalise_ticker(ticker: str) -> str:
    """Return ``ticker`` as stored in ``terminal_equities``: trimmed and upper-case (``SCOM``)."""
    return ticker.strip().upper()


def _normalise_tickers(tickers: Iterable[str]) -> list[str]:
    """Normalise ``tickers`` and drop repeats, keeping the first occurrence's order."""
    return list(dict.fromkeys(_normalise_ticker(ticker) for ticker in tickers))


def _owned_watchlist_query(
    principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> Select[tuple[TerminalWatchlist]]:
    """Select the watchlist ``watchlist_id`` only if ``principal`` owns it, with its items."""
    return (
        select(TerminalWatchlist)
        .where(
            TerminalWatchlist.id == watchlist_id,
            TerminalWatchlist.owner_key == principal.subject,
        )
        .options(selectinload(TerminalWatchlist.items))
    )


def _to_schema(row: TerminalWatchlist) -> Watchlist:
    """Map a loaded watchlist row, items included, to the API ``Watchlist`` (tickers sorted)."""
    return Watchlist(
        id=row.id,
        name=row.name,
        tickers=sorted(item.ticker for item in row.items),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


async def _unknown_tickers(session: AsyncSession, tickers: Sequence[str]) -> list[str]:
    """Return the members of ``tickers`` absent from ``terminal_equities``, in request order."""
    if not tickers:
        return []
    result = await session.execute(
        select(TerminalEquity.ticker).where(TerminalEquity.ticker.in_(tickers))
    )
    known = set(result.scalars().all())
    return [ticker for ticker in tickers if ticker not in known]


async def _name_exists(session: AsyncSession, principal: OwnerPrincipal, name: str) -> bool:
    """Whether ``principal`` already owns a watchlist called ``name``."""
    result = await session.execute(
        select(TerminalWatchlist.id).where(
            TerminalWatchlist.owner_key == principal.subject,
            TerminalWatchlist.name == name,
        )
    )
    return result.scalar_one_or_none() is not None


async def _lock_owned_watchlist(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> uuid.UUID:
    """Lock the owned watchlist row ``FOR UPDATE`` for this transaction and return its id.

    The lock keeps a concurrent delete of the list from turning an item insert into a foreign-key
    error, and serialises item changes on one list. Raises 404 ``watchlist_not_found`` when the
    list is missing or owned by another subject.
    """
    result = await session.execute(
        select(TerminalWatchlist.id)
        .where(
            TerminalWatchlist.id == watchlist_id,
            TerminalWatchlist.owner_key == principal.subject,
        )
        .with_for_update()
    )
    locked_id = result.scalar_one_or_none()
    if locked_id is None:
        raise _watchlist_not_found(watchlist_id)
    return locked_id


async def _ensure_item_ticker(session: AsyncSession, ticker: str) -> str:
    """Return the normalised ``ticker``, or raise 404 ``unknown_ticker`` if it is not listed."""
    normalised = _normalise_ticker(ticker)
    if await _unknown_tickers(session, [normalised]):
        raise _unknown_item_ticker(normalised)
    return normalised


async def _touch(session: AsyncSession, watchlist_id: uuid.UUID) -> None:
    """Advance ``updated_at`` after an item was actually inserted or deleted.

    Item changes leave ``terminal_watchlists`` untouched, so the model's ``onupdate`` never fires
    for them. ``statement_timestamp()`` is used rather than ``now()``: ``now()`` is frozen at the
    start of the enclosing transaction, so inside a host's or a test's outer transaction (where
    each service commit only releases a savepoint) it would never move past ``created_at``.
    """
    await session.execute(
        update(TerminalWatchlist)
        .where(TerminalWatchlist.id == watchlist_id)
        .values(updated_at=func.statement_timestamp())
    )


async def _reload(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> Watchlist:
    """Re-select the owned watchlist after a commit and return it as the API model.

    ``populate_existing`` overwrites any identity-map copy loaded before the commit, so the
    returned items and timestamps are the committed ones whatever the session's
    ``expire_on_commit`` setting. Raises 404 if the list was deleted concurrently.
    """
    result = await session.execute(
        _owned_watchlist_query(principal, watchlist_id).execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise _watchlist_not_found(watchlist_id)
    return _to_schema(row)


async def get_owned_watchlist(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> TerminalWatchlist:
    """Return the watchlist ``watchlist_id`` of ``principal``, with its items loaded.

    Raises 404 ``watchlist_not_found`` (``field="watchlist_id"``) when no list has that id or the
    list belongs to another subject; the two cases are indistinguishable to the caller. Read-only.
    """
    result = await session.execute(_owned_watchlist_query(principal, watchlist_id))
    row = result.scalar_one_or_none()
    if row is None:
        raise _watchlist_not_found(watchlist_id)
    return row


async def owned_watchlist_tickers(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> list[str]:
    """Return the sorted tickers of the watchlist ``watchlist_id`` owned by ``principal``.

    Used by the screener's ``watchlist_id`` filter. Raises 404 ``watchlist_not_found`` like
    :func:`get_owned_watchlist`. Read-only.
    """
    row = await get_owned_watchlist(session, principal, watchlist_id)
    return sorted(item.ticker for item in row.items)


async def list_watchlists(session: AsyncSession, principal: OwnerPrincipal) -> list[Watchlist]:
    """Return every watchlist of ``principal``, ordered by name, each with sorted tickers.

    Names are compared under collation ``"C"``, which on the UTF-8 database is Unicode code-point
    order: the order of Python's ``sorted`` and of the widgets' own sort, identical on every
    PostgreSQL whatever the database's default collation, which may fold case, accents and
    punctuation differently from host to host. Names are unique per owner, so the order is total.
    An owner without watchlists gets an empty list. Read-only; raises nothing.
    """
    result = await session.execute(
        select(TerminalWatchlist)
        .where(TerminalWatchlist.owner_key == principal.subject)
        .order_by(TerminalWatchlist.name.collate("C"))
        .options(selectinload(TerminalWatchlist.items))
    )
    return [_to_schema(row) for row in result.scalars().all()]


async def create_watchlist(
    session: AsyncSession, principal: OwnerPrincipal, body: WatchlistCreate
) -> Watchlist:
    """Create the watchlist ``body.name`` for ``principal`` holding ``body.tickers``; commit.

    Tickers are trimmed, upper-cased and de-duplicated. Errors, checked in this order:

    * ``ValueError``, before any query, when ``principal.subject`` cannot be stored as
      ``owner_key``: empty, longer than ``OWNER_KEY_MAX_LENGTH`` (255) characters, or containing
      a NUL character (``TypeError`` when it is not a ``str``). This is a broken host principal
      contract, not a client error, so it is not a coded ``HTTPException``;
    * 422 ``unknown_ticker`` (``field="tickers"``) naming every ticker absent from
      ``terminal_equities``;
    * 409 ``watchlist_name_taken`` (``field="name"``) when ``principal`` already owns a list of
      that name, including one created concurrently between the check and the insert.

    Not idempotent: repeating the request gives 409. The route answers 201 with the result.
    """
    _require_storable_subject(principal)
    tickers = _normalise_tickers(body.tickers)
    unknown = await _unknown_tickers(session, tickers)
    if unknown:
        raise _unknown_create_tickers(unknown)
    if await _name_exists(session, principal, body.name):
        raise _name_taken(body.name)

    # The id is generated here so that nothing needs to be read from the ORM object after commit.
    watchlist_id = uuid.uuid4()
    session.add(
        TerminalWatchlist(
            id=watchlist_id,
            owner_key=principal.subject,
            name=body.name,
            items=[
                TerminalWatchlistItem(watchlist_id=watchlist_id, ticker=ticker)
                for ticker in tickers
            ],
        )
    )
    try:
        await session.commit()
    except IntegrityError as exc:
        # A concurrent request won the race: either it created the same name (the unique
        # constraint on owner_key, name) or an equity disappeared after the check (an item's
        # foreign key). The database state, not the driver's error text, decides the answer.
        await session.rollback()
        if await _name_exists(session, principal, body.name):
            raise _name_taken(body.name) from exc
        unknown = await _unknown_tickers(session, tickers)
        if unknown:
            raise _unknown_create_tickers(unknown) from exc
        raise
    logger.debug("Created watchlist %s with %d tickers", watchlist_id, len(tickers))
    return await _reload(session, principal, watchlist_id)


async def delete_watchlist(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID
) -> None:
    """Delete the watchlist ``watchlist_id`` of ``principal`` and its items; commit.

    The items go through the database's ``ON DELETE CASCADE``. Raises 404 ``watchlist_not_found``
    when the list is missing or owned by another subject, so a repeated delete gives 404.
    """
    result = await session.execute(
        delete(TerminalWatchlist)
        .where(
            TerminalWatchlist.id == watchlist_id,
            TerminalWatchlist.owner_key == principal.subject,
        )
        .returning(TerminalWatchlist.id)
    )
    if result.scalar_one_or_none() is None:
        raise _watchlist_not_found(watchlist_id)
    await session.commit()
    logger.debug("Deleted watchlist %s", watchlist_id)


async def add_item(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID, ticker: str
) -> Watchlist:
    """Add ``ticker`` to the watchlist ``watchlist_id`` of ``principal``; commit.

    Idempotent: adding a ticker already on the list returns the list unchanged and leaves
    ``updated_at`` as it was; a real addition advances it. Errors: 404 ``watchlist_not_found``
    (``field="watchlist_id"``), then 404 ``unknown_ticker`` (``field="ticker"``).
    """
    locked_id = await _lock_owned_watchlist(session, principal, watchlist_id)
    normalised = await _ensure_item_ticker(session, ticker)
    result = await session.execute(
        insert(TerminalWatchlistItem)
        .values(watchlist_id=locked_id, ticker=normalised)
        .on_conflict_do_nothing(index_elements=["watchlist_id", "ticker"])
        .returning(TerminalWatchlistItem.ticker)
    )
    added = result.scalar_one_or_none() is not None
    if added:
        await _touch(session, locked_id)
    await session.commit()
    # %r escapes CR, LF and other control characters a stored ticker may hold: no forged lines.
    logger.debug("Watchlist %s add %r: changed=%s", locked_id, normalised, added)
    return await _reload(session, principal, locked_id)


async def remove_item(
    session: AsyncSession, principal: OwnerPrincipal, watchlist_id: uuid.UUID, ticker: str
) -> Watchlist:
    """Remove ``ticker`` from the watchlist ``watchlist_id`` of ``principal``; commit.

    Idempotent: removing a ticker that is not on the list returns the list unchanged and leaves
    ``updated_at`` as it was; a real removal advances it. Errors: 404 ``watchlist_not_found``
    (``field="watchlist_id"``), then 404 ``unknown_ticker`` (``field="ticker"``) for a ticker
    absent from ``terminal_equities``.
    """
    locked_id = await _lock_owned_watchlist(session, principal, watchlist_id)
    normalised = await _ensure_item_ticker(session, ticker)
    result = await session.execute(
        delete(TerminalWatchlistItem)
        .where(
            TerminalWatchlistItem.watchlist_id == locked_id,
            TerminalWatchlistItem.ticker == normalised,
        )
        .returning(TerminalWatchlistItem.ticker)
    )
    removed = result.scalar_one_or_none() is not None
    if removed:
        await _touch(session, locked_id)
    await session.commit()
    # %r escapes CR, LF and other control characters a stored ticker may hold: no forged lines.
    logger.debug("Watchlist %s remove %r: changed=%s", locked_id, normalised, removed)
    return await _reload(session, principal, locked_id)
