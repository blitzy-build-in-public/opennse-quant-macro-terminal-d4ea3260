"""Event definitions for the cycle backtester: table ``terminal_cycles``, natural key
``event_date``.

The table holds event dates only. Cycle trajectories (price, turnover, P/E and flow paths around
each event) are computed on demand from ``terminal_daily_prices`` and the other history tables, so
seeded and real data share one computation path and no trajectory table exists.
"""

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, String, Uuid, func, text
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.db.base import TerminalBase


class TerminalCycle(TerminalBase):
    """One event, such as a general election, around which the backtester builds a cycle window.

    Default events: when a backtest request carries no ``event_dates``, the backtester uses every
    row of this table. With neither request dates nor rows, the API returns 422
    ``no_event_dates``.

    Seeded rows, all with ``event_type = 'general_election'`` and ``source = 'seed'``:
    "2013 Cycle" 2013-03-04, "2017 Cycle" 2017-08-08, "2022 Cycle" 2022-08-09 and
    "2027 Election Watch" 2027-08-10. The seeder inserts them with
    ``ON CONFLICT (event_date) DO NOTHING``, so a re-seed adds no rows.

    ``label`` is returned as ``CycleResult.label``.

    Future events are valid. An event whose window has no observed data yet is reported as
    ``awaiting_data``, and one whose data ends inside the window as ``partial``; both carry a
    projection averaged from the completed cycles, which is never stored.

    There is no ``updated_at``: rows are reference data that writers insert and never update.
    """

    __tablename__ = "terminal_cycles"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    label: Mapped[str] = mapped_column(String(64), nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="general_election",
        server_default=text("'general_election'"),
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
