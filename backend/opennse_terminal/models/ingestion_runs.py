"""ORM model for ``terminal_ingestion_runs``: one audit row per pipeline, import or seeder run."""

import uuid
from datetime import datetime
from typing import Any, Final

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, Text, Uuid, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.db.base import TerminalBase

__all__ = ["RUN_STATUSES", "RUN_TRIGGERS", "TerminalIngestionRun"]

# Both tuples must hold exactly the values of ``RunTrigger`` (a Literal) and ``RunStatus`` (a
# StrEnum) in ``opennse_terminal.ingestion.base``; tests assert set equality. They live here, not
# there, because the ingestion package imports the models and the models never import ingestion.
RUN_TRIGGERS: Final[tuple[str, ...]] = ("schedule", "cli", "api", "seed")
"""What started a run: the scheduler process, the CLI, an API request or the seeder."""

RUN_STATUSES: Final[tuple[str, ...]] = (
    "running",
    "succeeded",
    "partial",
    "failed",
    "layout_changed",
    "blocked_by_robots",
    "no_new_data",
    "ambiguous_session",
)
"""``running`` while in progress, then exactly one final status from the run-status table."""


def _in_list_check(column: str, values: tuple[str, ...]) -> str:
    """Render ``<column> IN ('a', 'b', ...)`` from fixed, quote-free module constants."""
    quoted = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({quoted})"


class TerminalIngestionRun(TerminalBase):
    """Audit record of one ingestion pipeline, CSV/Excel import, PDF upload or seeder run.

    Lifecycle: the run row is inserted with ``status='running'`` in its own short transaction on a
    connection separate from the run's data transaction (which holds the transaction-scoped
    advisory lock). When the run ends, the row is updated with its final ``status``,
    ``finished_at``, the three row counts, ``error_message`` and ``details``. A data transaction
    that rolls back therefore still leaves a visible run, and run-record writes never release or
    extend the lock.

    ``pipeline`` is the registry name (``nse_market_statistics``, ``cbk_rates_csv`` ...),
    ``csv_import:<kind>``, ``pdf_filing`` or ``seed``. ``error_message`` holds the failure text,
    for example "lock held" or a PDF endpoint error code such as ``commit_requires_kes``.

    ``details`` keys written by the pipelines:

    - ``rejected``: the first 50 rejected rows of a ``partial`` run;
    - ``expected`` and ``found``: the headers or counts compared on ``layout_changed``;
    - ``unmapped_isins``: NSE ISINs with no ``terminal_equities`` row;
    - ``bulletins``: the CBK weekly bulletins a ``cbk_reserves`` run processed;
    - the NSE session-dating evidence (expected, link and label sessions, continuity result);
    - ``file_sha256``, ``ticker`` and ``period_end_date`` of a failed PDF commit;
    - the seeder's per-table inserted-row counts.

    Timestamps are ``timestamptz`` in UTC. The model carries no relationships;
    ``terminal_daily_prices.ingestion_run_id`` references ``id`` from its own side.
    """

    __tablename__ = "terminal_ingestion_runs"
    __table_args__ = (
        CheckConstraint(_in_list_check("trigger", RUN_TRIGGERS), name="trigger_valid"),
        CheckConstraint(_in_list_check("status", RUN_STATUSES), name="status_valid"),
        # Explicit name: the "ix" convention keys on the first column only.
        Index("ix_terminal_ingestion_runs_pipeline_started_at", "pipeline", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    pipeline: Mapped[str] = mapped_column(String(48), nullable=False)
    trigger: Mapped[str] = mapped_column(String(16), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    rows_fetched: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    rows_written: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    rows_rejected: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
