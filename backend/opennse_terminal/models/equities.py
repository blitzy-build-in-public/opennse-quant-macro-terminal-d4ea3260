"""Listed NSE counter master: table ``terminal_equities``, natural key ``ticker``.

Every other per-counter table (daily prices, fundamentals, corporate filings and watchlist items)
references ``terminal_equities.ticker`` from its own side, so a counter must exist here before any
of its history is written.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    Numeric,
    String,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.db.base import TerminalBase

__all__ = ["TerminalEquity"]


class TerminalEquity(TerminalBase):
    """One listed NSE counter (``terminal_equities``).

    Units: ``shares_outstanding`` is whole shares, bounded to 0..10^12 (``MAX_SHARE_COUNT``) by a
    CHECK; ``free_float_pct`` is percentage points (``25.00`` means 25%), bounded to 0..100 and
    nullable. ``currency`` is the ISO 4217 code of the counter's prices and defaults to KES;
    analytics refuse fundamentals in a different currency with ``currency_mismatch``.

    ``sector`` is stored exactly as spelled by the writer (for example ``Banking`` or
    ``Manufacturing & Allied``); sector filters, aggregates and the Banking EV exclusion compare
    against it verbatim.

    ``isin`` is the join key ``ingestion/nse/market_statistics.py`` uses to map the NSE
    market-statistics rows, which carry no ticker, to ``ticker``. ISINs without a row here are
    reported as unmapped and skipped.

    Valuation (market cap, EPS, P/E, P/B, EV and every sector aggregate) uses this table's
    ``shares_outstanding``, never ``terminal_fundamentals.shares_outstanding``, which is kept only
    for reference.

    Writers: the seeder inserts with ``ON CONFLICT (ticker) DO NOTHING`` (``source = 'seed'``), and
    the ``equities`` CSV/Excel import upserts on ``ticker`` (``source = 'csv_import'``).
    ``onupdate`` does not fire for ``INSERT ... ON CONFLICT DO UPDATE``, so ``db/upsert.py`` sets
    ``updated_at`` explicitly.

    The model declares no relationships, so no attribute access triggers an implicit lazy load
    under the async session.
    """

    __tablename__ = "terminal_equities"
    __table_args__ = (
        CheckConstraint(
            f"shares_outstanding BETWEEN 0 AND {MAX_SHARE_COUNT}",
            name="shares_outstanding_range",
        ),
        CheckConstraint("free_float_pct BETWEEN 0 AND 100", name="free_float_pct_range"),
        CheckConstraint("currency = upper(currency)", name="currency_upper"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # The unique constraint (uq_terminal_equities_ticker) also serves as the ticker index and as
    # the target of the per-counter foreign keys, so no separate index is declared.
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, unique=True)
    isin: Mapped[str | None] = mapped_column(String(12), nullable=True, unique=True)
    company_name: Mapped[str] = mapped_column(String(200), nullable=False)
    sector: Mapped[str] = mapped_column(String(100), nullable=False)
    shares_outstanding: Mapped[int] = mapped_column(BigInteger, nullable=False)
    free_float_pct: Mapped[Decimal | None] = mapped_column(Numeric(7, 4), nullable=True)
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        nullable=False,
        default=DEFAULT_CURRENCY,
        server_default=text("'KES'"),
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
