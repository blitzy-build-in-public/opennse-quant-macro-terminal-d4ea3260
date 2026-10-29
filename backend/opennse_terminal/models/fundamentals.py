"""Reported company fundamentals: table ``terminal_fundamentals``, natural key
``(ticker, period_end_date, period_type)``.

One row is one reporting period (full year or half year) of one listed counter. Rows are written by
the seeder, by PDF filing commits (``POST /api/v1/terminal/ingest/pdf`` with ``commit=true``) and
by the ``fundamentals`` CSV/Excel import, always through ``db/upsert.py``.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final

from sqlalchemy import (
    CHAR,
    BigInteger,
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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.db.base import TerminalBase

__all__ = ["FUNDAMENTAL_LINE_ITEM_COLUMNS", "PERIOD_TYPES", "TerminalFundamental"]

PERIOD_TYPES: Final[tuple[str, ...]] = ("FY", "H1")
"""Reporting period kinds: ``FY`` (fiscal year, the default and the only kind valuation reads) and
``H1`` (half year)."""

# Must name the same columns, in the same order, as ``FUNDAMENTAL_VALUE_COLUMNS`` in
# ``db/upsert.py``, which merges and records provenance for exactly these columns.
FUNDAMENTAL_LINE_ITEM_COLUMNS: Final[tuple[str, ...]] = (
    "revenue_kes",
    "ebitda_kes",
    "net_income_kes",
    "total_assets_kes",
    "total_equity_kes",
    "total_debt_kes",
    "cash_and_equivalents_kes",
    "operating_cash_flow_kes",
    "dps_kes",
    "shares_outstanding",
)
"""The value columns of ``terminal_fundamentals``, in table order. Each is nullable, is written
independently by COALESCE merge, and carries its own entry in ``source_by_field``; the
``fundamentals`` CSV/Excel import accepts each of them as an optional column."""


def _in_list_check(column: str, values: tuple[str, ...]) -> str:
    """Render ``<column> IN ('a', 'b', ...)`` from fixed, quote-free module constants."""
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


class TerminalFundamental(TerminalBase):
    """One reporting period of one counter (``terminal_fundamentals``).

    Key: ``(ticker, period_end_date, period_type)`` is unique and is the conflict target of every
    upsert. ``period_type`` is ``FY`` or ``H1`` and defaults to ``FY``.

    Units: every ``*_kes`` column is whole KES, never thousands (the PDF parser and the importer
    normalize statement scale before writing); ``dps_kes`` is KES per share; ``shares_outstanding``
    is whole shares, bounded to 0..10^12 (``MAX_SHARE_COUNT``) by a CHECK. ``currency`` defaults
    to KES; analytics refuse a row whose currency differs from the equity's (``currency_mismatch``),
    and writers never merge values across currencies.

    Every line item is nullable, because a filing or an import can supply a subset. Banks carry
    NULL ``ebitda_kes``, ``total_debt_kes``, ``cash_and_equivalents_kes`` and
    ``operating_cash_flow_kes`` in the seed; EV and EV/EBITDA are null for the Banking sector
    whatever is stored. ``shares_outstanding`` is kept for reference only: valuation uses
    ``terminal_equities.shares_outstanding``.

    Provenance: there is no row-level ``source``. ``source_by_field`` maps each non-null line-item
    column to the writer that supplied it (``seed``, ``pdf_filing`` or ``csv_import``). Writers
    COALESCE-merge only the columns they supply and never null an existing value, so a partial PDF
    commit into a seeded row leaves every retained column marked ``seed``. ``filing_id`` links the
    row to the corporate filing that last wrote it, and is set to NULL if that filing is deleted.

    Availability: analytics treat a row as published ``TERMINAL_FUNDAMENTALS_LAG_DAYS`` (default
    90) days after ``period_end_date``; publication dates themselves are not stored.

    ``onupdate`` does not fire for ``INSERT ... ON CONFLICT DO UPDATE``, so ``db/upsert.py`` sets
    ``updated_at`` explicitly. The model declares no relationships, so no attribute access triggers
    an implicit lazy load under the async session.
    """

    __tablename__ = "terminal_fundamentals"
    __table_args__ = (
        # Rendered as uq_terminal_fundamentals_ticker; this is the upsert conflict key, and its
        # leading column serves per-ticker lookups.
        UniqueConstraint("ticker", "period_end_date", "period_type"),
        CheckConstraint(_in_list_check("period_type", PERIOD_TYPES), name="period_type_valid"),
        CheckConstraint(
            f"shares_outstanding BETWEEN 0 AND {MAX_SHARE_COUNT}",
            name="shares_outstanding_range",
        ),
        CheckConstraint("currency = upper(currency)", name="currency_upper"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # Rendered as fk_terminal_fundamentals_ticker_terminal_equities (49 characters).
    ticker: Mapped[str] = mapped_column(
        String(16),
        ForeignKey("terminal_equities.ticker", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    period_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    period_type: Mapped[str] = mapped_column(
        String(2),
        nullable=False,
        default="FY",
        server_default=text("'FY'"),
    )
    revenue_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    ebitda_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    net_income_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    total_assets_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    total_equity_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    total_debt_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    cash_and_equivalents_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    operating_cash_flow_kes: Mapped[Decimal | None] = mapped_column(Numeric(24, 2), nullable=True)
    dps_kes: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    shares_outstanding: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Rendered as fk_terminal_fundamentals_filing_id_terminal_corporate_filings (61 characters).
    filing_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid,
        ForeignKey("terminal_corporate_filings.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_by_field: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        nullable=False,
        default=DEFAULT_CURRENCY,
        server_default=text("'KES'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
