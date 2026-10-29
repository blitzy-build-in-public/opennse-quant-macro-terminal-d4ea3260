"""Referential actions of every foreign key to ``terminal_equities.ticker``.

Four tables reference a counter by ticker. Each foreign key cascades on update, so renaming an
equity's ticker carries over to its prices, fundamentals, watchlist items and filings instead of
being rejected while any of them still references the old ticker. On delete, prices,
fundamentals and watchlist items cascade, while a filing keeps its extraction and has its ticker
set to null.

Both schema sources are checked: the ORM models on ``TerminalBase.metadata`` and the DDL that the
migration history emits in Alembic's offline mode, which renders SQL without connecting to a
database. ``terminal_0001`` defines all four keys inline in ``CREATE TABLE``, the watchlist-item
and filing keys without an ON UPDATE action; ``terminal_0002`` drops those two and adds them back
with ON UPDATE CASCADE. The head DDL's last statement that defines or drops each key must
therefore define it with the models' actions, and the DDL up to ``terminal_0001`` must define
each key once with the actions that revision shipped with, so the shipped revision is never
edited in place.

Expected names and actions are literals, never derived from the code under test. The tests are
pure: no database or network.
"""

from __future__ import annotations

import pytest
from alembic import command
from sqlalchemy import ForeignKeyConstraint

# Imported for its side effect: it registers all 11 terminal_ tables on TerminalBase.metadata.
import opennse_terminal.models  # noqa: F401
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.migrations import build_alembic_config

# (table, foreign key name, ON UPDATE action, ON DELETE action)
EXPECTED_TICKER_FKS: list[tuple[str, str, str, str]] = [
    (
        "terminal_daily_prices",
        "fk_terminal_daily_prices_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_fundamentals",
        "fk_terminal_fundamentals_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_watchlist_items",
        "fk_terminal_watchlist_items_ticker_terminal_equities",
        "CASCADE",
        "CASCADE",
    ),
    (
        "terminal_corporate_filings",
        "fk_terminal_corporate_filings_ticker_terminal_equities",
        "CASCADE",
        "SET NULL",
    ),
]
# (table, foreign key name, referential actions the DDL of terminal_0001 renders)
BOUNDARY_TICKER_FKS: list[tuple[str, str, str]] = [
    (
        "terminal_daily_prices",
        "fk_terminal_daily_prices_ticker_terminal_equities",
        "ON DELETE CASCADE ON UPDATE CASCADE",
    ),
    (
        "terminal_fundamentals",
        "fk_terminal_fundamentals_ticker_terminal_equities",
        "ON DELETE CASCADE ON UPDATE CASCADE",
    ),
    (
        "terminal_watchlist_items",
        "fk_terminal_watchlist_items_ticker_terminal_equities",
        "ON DELETE CASCADE",
    ),
    (
        "terminal_corporate_filings",
        "fk_terminal_corporate_filings_ticker_terminal_equities",
        "ON DELETE SET NULL",
    ),
]
BOUNDARY_REVISION = "terminal_0001"
EQUITIES_TABLE = "terminal_equities"
# Offline mode needs only the dialect of the URL; nothing connects to this address.
OFFLINE_URL = "postgresql+asyncpg://localhost/x"
DROPPED = "DROP"


def _offline_upgrade_ddl(capsys: pytest.CaptureFixture[str], revision: str) -> str:
    """The SQL ``upgrade`` to ``revision`` emits from an empty database, without connecting."""
    # In offline mode Alembic writes the migration SQL to stdout instead of executing it.
    command.upgrade(build_alembic_config(OFFLINE_URL), revision, sql=True)
    return capsys.readouterr().out


def _fk_statements(ddl: str, table_name: str, fk_name: str) -> list[str]:
    """Each statement of ``ddl`` that defines or drops ``fk_name``, in order.

    A definition, inline in its table's ``CREATE TABLE`` or through ``ALTER TABLE ... ADD
    CONSTRAINT``, is given as everything after its ``REFERENCES`` target, for example
    ``"ON DELETE CASCADE ON UPDATE CASCADE"``; a drop is given as :data:`DROPPED`.
    """
    clause = f"CONSTRAINT {fk_name} FOREIGN KEY(ticker) REFERENCES {EQUITIES_TABLE} (ticker)"
    found: list[str] = []
    for raw_statement in ddl.split(";\n"):
        # Alembic precedes each revision's first statement with a "-- Running upgrade" comment.
        sql_lines = [line for line in raw_statement.splitlines() if not line.startswith("--")]
        statement = "\n".join(sql_lines).strip()
        if fk_name not in statement:
            continue
        if statement == f"ALTER TABLE {table_name} DROP CONSTRAINT {fk_name}":
            found.append(DROPPED)
            continue
        assert statement.startswith(
            (f"CREATE TABLE {table_name} (", f"ALTER TABLE {table_name} ADD {clause}")
        ), statement
        lines = [line for line in statement.splitlines() if clause in line]
        assert len(lines) == 1, statement
        # An inline constraint ends with ", " unless it is the table's last element.
        found.append(lines[0].split(clause, 1)[1].strip().removesuffix(",").rstrip())
    return found


def _ticker_fk(table_name: str) -> ForeignKeyConstraint:
    """Return the single foreign key of ``table_name`` that references ``terminal_equities``."""
    table = TerminalBase.metadata.tables[table_name]
    matches = [
        constraint
        for constraint in table.foreign_key_constraints
        if constraint.referred_table.name == EQUITIES_TABLE
    ]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.parametrize(("table_name", "fk_name", "onupdate", "ondelete"), EXPECTED_TICKER_FKS)
def test_model_ticker_fk_has_its_name_and_actions(
    table_name: str, fk_name: str, onupdate: str, ondelete: str
) -> None:
    constraint = _ticker_fk(table_name)
    assert constraint.name == fk_name
    assert constraint.column_keys == ["ticker"]
    assert [element.column.name for element in constraint.elements] == ["ticker"]
    assert constraint.onupdate == onupdate
    assert constraint.ondelete == ondelete


def test_model_fks_to_equities_are_exactly_the_four_ticker_fks() -> None:
    referencing = {
        (table.name, constraint.name)
        for table in TerminalBase.metadata.tables.values()
        for constraint in table.foreign_key_constraints
        if constraint.referred_table.name == EQUITIES_TABLE
    }
    assert referencing == {
        (table_name, fk_name) for table_name, fk_name, _, _ in EXPECTED_TICKER_FKS
    }


@pytest.mark.parametrize(("table_name", "fk_name", "onupdate", "ondelete"), EXPECTED_TICKER_FKS)
def test_migration_ddl_renders_the_ticker_fk_actions(
    capsys: pytest.CaptureFixture[str],
    table_name: str,
    fk_name: str,
    onupdate: str,
    ondelete: str,
) -> None:
    statements = _fk_statements(_offline_upgrade_ddl(capsys, "head"), table_name, fk_name)
    # The last statement decides the head schema; a drop there would leave no key at all.
    assert statements, f"the head DDL never defines {fk_name}"
    assert statements[-1] == f"ON DELETE {ondelete} ON UPDATE {onupdate}"


@pytest.mark.parametrize(("table_name", "fk_name", "actions"), BOUNDARY_TICKER_FKS)
def test_first_revision_ddl_keeps_its_shipped_ticker_fk_actions(
    capsys: pytest.CaptureFixture[str], table_name: str, fk_name: str, actions: str
) -> None:
    ddl = _offline_upgrade_ddl(capsys, BOUNDARY_REVISION)
    assert _fk_statements(ddl, table_name, fk_name) == [actions]
