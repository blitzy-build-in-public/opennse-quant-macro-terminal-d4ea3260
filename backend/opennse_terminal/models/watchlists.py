"""Custom watchlists: tables ``terminal_watchlists`` and ``terminal_watchlist_items``.

A watchlist is a named set of counters, natural key (``owner_key``, ``name``). Its items reference
``terminal_equities.ticker`` and are keyed by (``watchlist_id``, ``ticker``), so a counter appears
at most once per list.

Ownership needs no user tables. ``owner_key`` stores the opaque ``TerminalPrincipal.subject``
resolved by the overridable ``get_terminal_principal`` dependency: ``"local"`` by default, or
whatever subject a host maps its own users to. The terminal creates no users, accounts, sessions
or auth table and declares no foreign key to any host table, so authentication stays pluggable.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from opennse_terminal.db.base import TerminalBase

__all__ = ["TerminalWatchlist", "TerminalWatchlistItem"]


class TerminalWatchlistItem(TerminalBase):
    """One counter on one watchlist (``terminal_watchlist_items``).

    The composite primary key (``watchlist_id``, ``ticker``) is the conflict target that makes
    adding an item idempotent (``INSERT ... ON CONFLICT DO NOTHING``). Both foreign keys cascade
    on delete: removing a watchlist or a counter removes its items in the database.

    The item declares no relationship back to its watchlist; services never navigate from an
    item to its list, and the absence of the attribute rules out an implicit lazy load under the
    async session.
    """

    __tablename__ = "terminal_watchlist_items"

    watchlist_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("terminal_watchlists.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    ticker: Mapped[str] = mapped_column(
        String(16),
        ForeignKey("terminal_equities.ticker", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TerminalWatchlist(TerminalBase):
    """A named watchlist owned by one principal subject (``terminal_watchlists``).

    Every query must filter on ``owner_key = principal.subject``. A list owned by another subject
    is reported as not found, so its existence is never revealed.

    ``UNIQUE (owner_key, name)`` (``uq_terminal_watchlists_owner_key``) backs the 409
    ``watchlist_name_taken`` response and the seeder's conflict lookup on (``owner_key``,
    ``name``). The seeder creates "NSE Core 8" for owner ``local`` holding all eight seeded
    tickers.

    ``updated_at`` refreshes through ``onupdate`` only when a column of this row changes. Adding or
    removing an item changes only ``terminal_watchlist_items``, so the service bumps
    ``updated_at = func.now()`` explicitly whenever an item is actually inserted or deleted.

    ``items`` loads eagerly with ``selectin`` (an implicit lazy load fails under ``AsyncSession``)
    and is ordered by ticker, so the API returns ``tickers`` sorted. ``passive_deletes`` leaves the
    removal of unloaded items to the database's ``ON DELETE CASCADE``.
    """

    __tablename__ = "terminal_watchlists"
    __table_args__ = (UniqueConstraint("owner_key", "name"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    owner_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    items: Mapped[list[TerminalWatchlistItem]] = relationship(
        cascade="all, delete-orphan",
        lazy="selectin",
        passive_deletes=True,
        order_by=TerminalWatchlistItem.ticker,
    )
