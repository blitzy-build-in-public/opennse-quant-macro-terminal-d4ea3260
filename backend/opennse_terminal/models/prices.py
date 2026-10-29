"""Daily NSE equity sessions: table ``terminal_daily_prices``, natural key ``(ticker, trade_date)``.

One row is one observed trading session of one counter. Rows are written by the NSE
market-statistics pipeline, the ``daily_prices`` and ``foreign_flows`` CSV/Excel imports and the
seeder, always through the column-specific upserts in ``db/upsert.py``.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.config import MAX_SHARE_COUNT
from opennse_terminal.db.base import TerminalBase

__all__ = ["TerminalDailyPrice"]


class TerminalDailyPrice(TerminalBase):
    """One trading session of one counter (``terminal_daily_prices``).

    Key: ``(ticker, trade_date)`` is unique and is the conflict target of every upsert.
    ``trade_date`` is the session date in Africa/Nairobi.

    Units: ``open_price``, ``high_price``, ``low_price`` and ``close_price`` are KES per share;
    ``turnover_kes`` and ``foreign_net_flow_kes`` are whole KES (never thousands); ``volume`` is
    shares, bounded to 0..10^12 (``MAX_SHARE_COUNT``) so it stays an exact JavaScript integer.
    Open, high and low are nullable because the public NSE market-statistics page publishes only
    the last traded price. ``turnover_estimated`` is true when turnover was derived as
    volume x close (the NSE pipeline, and CSV rows without a turnover cell).

    Provenance is tracked per column group:

    - ``source`` covers the price columns (OHLC, close, volume, turnover, ``turnover_estimated``),
      with precedence ``csv_import`` > ``nse_market_statistics`` > ``seed``: the NSE upsert never
      overwrites a ``csv_import`` row.
    - ``flow_source`` covers ``foreign_net_flow_kes`` only (``seed`` or ``csv_import``) and is
      non-null exactly when the flow is non-null, which a CHECK enforces. The NSE pipeline never
      writes either flow column, so an imported flow survives later scrapes.

    A missing row is not a zero-volume day: analytics skip sessions without a row rather than
    zero-filling them, while a present row with ``volume = 0`` is a valid observation.

    ``ingested_at`` has no ``onupdate``, because ``INSERT ... ON CONFLICT DO UPDATE`` does not apply
    it; each upsert sets it together with ``ingestion_run_id``.

    The model declares no relationships, so no attribute access triggers an implicit lazy load
    under the async session.
    """

    __tablename__ = "terminal_daily_prices"
    __table_args__ = (
        # Rendered as uq_terminal_daily_prices_ticker. Its leading column serves per-ticker
        # lookups, so no separate ticker index is declared.
        UniqueConstraint("ticker", "trade_date"),
        CheckConstraint(
            "high_price IS NULL OR low_price IS NULL OR high_price >= low_price",
            name="high_gte_low",
        ),
        CheckConstraint("close_price > 0", name="close_price_positive"),
        CheckConstraint(f"volume BETWEEN 0 AND {MAX_SHARE_COUNT}", name="volume_range"),
        CheckConstraint("turnover_kes >= 0", name="turnover_non_negative"),
        CheckConstraint(
            "(flow_source IS NULL) = (foreign_net_flow_kes IS NULL)",
            name="flow_source_matches_flow",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    ticker: Mapped[str] = mapped_column(
        String(16),
        ForeignKey("terminal_equities.ticker", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    high_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    low_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    turnover_kes: Mapped[Decimal] = mapped_column(Numeric(24, 2), nullable=False)
    turnover_estimated: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    foreign_net_flow_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    flow_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ingestion_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid,
        # Explicit name: the convention would render
        # fk_terminal_daily_prices_ingestion_run_id_terminal_ingestion_runs, 65 characters, past
        # PostgreSQL's 63-character identifier limit.
        ForeignKey(
            "terminal_ingestion_runs.id",
            ondelete="SET NULL",
            name="fk_terminal_daily_prices_ingestion_run_id",
        ),
        nullable=True,
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
