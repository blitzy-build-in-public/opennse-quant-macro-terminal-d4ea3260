"""A host's custom routing ``Session``, for the tests of how the terminal resolves its bind.

SQLAlchemy's documented custom-partitioning pattern subclasses ``Session`` and overrides
``get_bind(mapper=None, clause=None, **kw)``, routing on ``mapper.class_``. Such a session has
neither a default bind nor ``binds``, so the terminal reaches its database through it only when
every connection it asks for names the target model's ``Mapper`` (or its ``terminal_`` table as
the clause), as SQLAlchemy's own ORM statements do. Build one with
``AsyncSession(sync_session_class=RoutingSession, terminal_bind=<sync Engine or Connection>)``.
"""

from typing import Any

from sqlalchemy import Table
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.sql import ClauseElement

from opennse_terminal.db.base import TerminalBase

__all__ = ["RoutingSession"]


class RoutingSession(Session):
    """Routes the terminal's models to ``terminal_bind`` by mapper, or by table as the clause.

    Anything else falls through to ``Session.get_bind``, which, with no default bind and no
    ``binds``, raises ``UnboundExecutionError``. A mapped class passed where SQLAlchemy passes a
    ``Mapper`` has no ``class_`` and raises ``AttributeError``, as in a host's routing session.
    """

    def __init__(self, *, terminal_bind: Engine | Connection, **kw: Any) -> None:
        super().__init__(**kw)
        self.terminal_bind = terminal_bind

    def get_bind(  # type: ignore[override]
        self,
        mapper: Mapper[Any] | None = None,
        clause: ClauseElement | None = None,
        **kw: Any,
    ) -> Engine | Connection:
        if mapper is not None and issubclass(mapper.class_, TerminalBase):
            return self.terminal_bind
        if isinstance(clause, Table) and clause.name.startswith("terminal_"):
            return self.terminal_bind
        return super().get_bind(mapper, clause=clause, **kw)
