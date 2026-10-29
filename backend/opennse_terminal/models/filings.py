"""PDF filing and prospectus extractions: table ``terminal_corporate_filings``, natural key
``file_sha256``.

One row per distinct uploaded document (identified by the SHA-256 of its bytes), holding the line
items ``ingestion/files/pdf_line_items.py`` extracted from it. The table is the persistence side of
``POST /api/v1/terminal/ingest/pdf``; ``terminal_fundamentals.filing_id`` references ``id`` from
its own side when a filing's items are committed.
"""

import uuid
from datetime import date, datetime
from typing import Any, Final

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.config import DEFAULT_CURRENCY
from opennse_terminal.db.base import TerminalBase

__all__ = ["DOCUMENT_TYPES", "FILING_STATUSES", "TerminalCorporateFiling"]

# Both tuples must hold exactly the values of the ``DocumentType`` literal and the
# ``IngestPdfResponse.status`` literal in ``opennse_terminal.schemas.ingest``. They live here
# because the CHECK constraints below and the initial migration are built from them, and the
# models never import the schemas.
DOCUMENT_TYPES: Final[tuple[str, ...]] = ("annual_report", "interim_report", "prospectus", "other")
"""Kinds of uploaded document; ``annual_report`` is the endpoint's default."""

FILING_STATUSES: Final[tuple[str, ...]] = ("extracted", "partial", "no_items")
"""Extraction outcome: every requested item found or derived, some missing, or none found."""


def _in_list_check(column: str, values: tuple[str, ...]) -> str:
    """Render ``<column> IN ('a', 'b', ...)`` from fixed, quote-free module constants."""
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


class TerminalCorporateFiling(TerminalBase):
    """One corporate filing or IPO prospectus extraction (``terminal_corporate_filings``).

    Persistence rules of ``POST /ingest/pdf``:

    * Every request answered with HTTP 200 upserts this table on ``file_sha256``, whether or not
      ``commit`` was requested, so re-uploading the same document replaces its extraction. The raw
      PDF bytes are never stored; only the name, size, page count and hash are kept.
    * The filing upsert and the fundamentals upsert share one data transaction. A failed commit
      precondition (``commit_requires_kes``, ``currency_conflict`` ...) rolls it back, so no
      filing row is written or changed; the failure is recorded only in
      ``terminal_ingestion_runs``.
    * ``committed_to_fundamentals`` is true once the filing's ``found`` and ``derived`` items were
      upserted into ``terminal_fundamentals`` with ``filing_id`` pointing at this row. Ambiguous
      items are never committed.

    ``ticker`` is null for unlisted IPO issuers, which are identified by ``company_name``; deleting
    the equity sets it to null rather than removing the extraction. ``period_type`` (``FY`` or
    ``H1``) and ``period_end_date`` are as supplied with the upload.

    ``currency`` defaults to KES. A statement presented in USD keeps ``currency = 'USD'`` and its
    items are stored unconverted; such a filing is never committed to fundamentals.

    ``detected_scale`` is the statement multiplier applied to monetary values: 1, 1,000,
    1,000,000 or 1,000,000,000, or null when the extraction recorded none. ``extraction`` is the
    JSON array of ``ExtractedItem`` objects returned by the endpoint, with decimal values as
    strings.

    ``onupdate`` does not fire for ``INSERT ... ON CONFLICT DO UPDATE``, so the filings upsert
    sets ``updated_at`` explicitly. The model declares no relationships, so no attribute access
    triggers an implicit lazy load under the async session.
    """

    __tablename__ = "terminal_corporate_filings"
    __table_args__ = (
        CheckConstraint(
            _in_list_check("document_type", DOCUMENT_TYPES), name="document_type_valid"
        ),
        CheckConstraint(_in_list_check("status", FILING_STATUSES), name="status_valid"),
        CheckConstraint("currency = upper(currency)", name="currency_upper"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # Rendered as fk_terminal_corporate_filings_ticker_terminal_equities (54 characters).
    ticker: Mapped[str | None] = mapped_column(
        String(16),
        ForeignKey("terminal_equities.ticker", ondelete="SET NULL"),
        nullable=True,
    )
    company_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    document_type: Mapped[str] = mapped_column(String(24), nullable=False)
    period_end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_type: Mapped[str | None] = mapped_column(String(2), nullable=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Rendered as uq_terminal_corporate_filings_file_sha256; this is the upsert conflict key.
    file_sha256: Mapped[str] = mapped_column(CHAR(64), nullable=False, unique=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False)
    detected_scale: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    extraction: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    committed_to_fundamentals: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
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
