"""Referential actions of every foreign key to ``terminal_equities.ticker``.

Four tables reference a counter by ticker. Each foreign key cascades on update, so renaming an
equity's ticker carries over to its prices, fundamentals, watchlist items and filings instead of
being rejected while any of them still references the old ticker. On delete, prices,
fundamentals and watchlist items cascade, while a filing keeps its extraction and has its ticker
set to null.

Both schema sources are checked: the ORM models on ``TerminalBase.metadata`` and the DDL that
migration ``terminal_0001`` emits in Alembic's offline mode, which renders SQL without connecting
to a database.

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
EQUITIES_TABLE = "terminal_equities"
# Offline mode needs only the dialect of the URL; nothing connects to this address.
OFFLINE_URL = "postgresql+asyncpg://localhost/x"


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
    # In offline mode Alembic writes the migration SQL to stdout instead of executing it.
    command.upgrade(build_alembic_config(OFFLINE_URL), "head", sql=True)
    ddl = capsys.readouterr().out
    # The clause must sit inside its own table's CREATE TABLE statement, which ends at "\n);".
    start = ddl.index(f"CREATE TABLE {table_name} (")
    create_table = ddl[start : ddl.index("\n);", start)]
    assert (
        f"CONSTRAINT {fk_name} FOREIGN KEY(ticker) REFERENCES {EQUITIES_TABLE} (ticker) "
        f"ON DELETE {ondelete} ON UPDATE {onupdate}"
    ) in create_table
