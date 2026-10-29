"""Database layer barrel. ``db.upsert`` is deliberately not re-exported: it imports ``models``,
whose modules import ``db.base`` and so run this file first; a re-export would be an import cycle.
"""

from opennse_terminal.db.base import TABLE_PREFIX, TERMINAL_NAMING_CONVENTION, TerminalBase
from opennse_terminal.db.session import (
    build_engine,
    build_session_factory,
    database_log_label,
    dispose_terminal_engine,
    session_factory,
)

__all__ = [
    "TABLE_PREFIX",
    "TERMINAL_NAMING_CONVENTION",
    "TerminalBase",
    "build_engine",
    "build_session_factory",
    "database_log_label",
    "dispose_terminal_engine",
    "session_factory",
]
