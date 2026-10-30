"""ORM models for the 11 ``terminal_`` tables (AAP 0.9.2); importing this package registers them
on ``TerminalBase.metadata``.

``migrations/env.py``, Alembic's ``compare_metadata`` and the schema tests import this package to
obtain the complete table collection, so every model module must be imported here. The metadata
holds exactly these tables: the terminal defines no users, accounts, sessions, roles or auth model,
because authentication is supplied by the host through dependency injection.

``opennse_terminal.db.upsert`` imports this package and must not be imported from it, which would
be an import cycle. Module-level constants such as ``RUN_STATUSES`` or ``MACRO_INDICATOR_COLUMNS``
are imported from their own modules; this barrel re-exports the model classes only.
"""

from opennse_terminal.models.cycles import TerminalCycle
from opennse_terminal.models.equities import TerminalEquity
from opennse_terminal.models.filings import TerminalCorporateFiling
from opennse_terminal.models.fundamentals import TerminalFundamental
from opennse_terminal.models.ingestion_runs import TerminalIngestionRun
from opennse_terminal.models.ipo import TerminalIpoScenario
from opennse_terminal.models.macro import TerminalBondYield, TerminalMacroIndicator
from opennse_terminal.models.prices import TerminalDailyPrice
from opennse_terminal.models.watchlists import TerminalWatchlist, TerminalWatchlistItem

__all__ = [
    "TerminalBondYield",
    "TerminalCorporateFiling",
    "TerminalCycle",
    "TerminalDailyPrice",
    "TerminalEquity",
    "TerminalFundamental",
    "TerminalIngestionRun",
    "TerminalIpoScenario",
    "TerminalMacroIndicator",
    "TerminalWatchlist",
    "TerminalWatchlistItem",
]
