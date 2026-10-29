"""Contract of the ``opennse_terminal.models`` barrel and the terminal's own ``MetaData``.

Importing the barrel registers on ``TerminalBase.metadata`` exactly the 11 ``terminal_`` tables of
the data model. None of them is a users, accounts, sessions, roles or auth table, because the host
supplies authentication through dependency injection, and the naming convention carries the
``terminal_`` prefix into every index and constraint name. ``__all__`` re-exports one mapped class
per table.

Expected class and table names are literals, never derived from the code under test. The tests are
pure: no database, network or filesystem.
"""

from __future__ import annotations

import re

import pytest
from sqlalchemy import Table

import opennse_terminal.models as models
from opennse_terminal.db.base import TerminalBase

EXPECTED_CLASS_TABLES: dict[str, str] = {
    "TerminalBondYield": "terminal_bond_yields",
    "TerminalCorporateFiling": "terminal_corporate_filings",
    "TerminalCycle": "terminal_cycles",
    "TerminalDailyPrice": "terminal_daily_prices",
    "TerminalEquity": "terminal_equities",
    "TerminalFundamental": "terminal_fundamentals",
    "TerminalIngestionRun": "terminal_ingestion_runs",
    "TerminalIpoScenario": "terminal_ipo_scenarios",
    "TerminalMacroIndicator": "terminal_macro_indicators",
    "TerminalWatchlist": "terminal_watchlists",
    "TerminalWatchlistItem": "terminal_watchlist_items",
}
EXPECTED_TABLES = frozenset(EXPECTED_CLASS_TABLES.values())
PREFIX = "terminal_"
# A table name, once the prefix is removed, must not name a generic identity or auth entity.
AUTH_TABLE_PATTERN = re.compile(r"(^|_)(users?|auth\w*|accounts?|sessions?|roles?)(_|$)")


def test_metadata_holds_exactly_the_terminal_tables() -> None:
    assert sorted(TerminalBase.metadata.tables) == sorted(EXPECTED_TABLES)


def test_every_metadata_table_carries_the_prefix() -> None:
    unprefixed = [name for name in TerminalBase.metadata.tables if not name.startswith(PREFIX)]
    assert unprefixed == []


def test_no_metadata_table_is_a_users_or_auth_table() -> None:
    auth_like = [
        name
        for name in TerminalBase.metadata.tables
        if AUTH_TABLE_PATTERN.search(name.removeprefix(PREFIX))
    ]
    assert auth_like == []


@pytest.mark.parametrize(
    "host_style_name", ["users", "user_sessions", "auth_tokens", "accounts", "roles"]
)
def test_auth_table_pattern_rejects_host_style_names(host_style_name: str) -> None:
    # Guards the guard: a pattern that matched nothing would let the test above pass vacuously.
    assert AUTH_TABLE_PATTERN.search(host_style_name)


def test_all_is_sorted_and_names_one_class_per_table() -> None:
    exported = list(models.__all__)
    assert exported == sorted(exported)
    assert len(exported) == len(EXPECTED_CLASS_TABLES)
    assert set(exported) == set(EXPECTED_CLASS_TABLES)


@pytest.mark.parametrize(("class_name", "table_name"), sorted(EXPECTED_CLASS_TABLES.items()))
def test_exported_class_is_mapped_to_its_metadata_table(class_name: str, table_name: str) -> None:
    model = getattr(models, class_name)
    assert isinstance(model, type)
    assert issubclass(model, TerminalBase)
    table = model.__table__
    assert isinstance(table, Table)
    assert table.name == table_name
    assert table is TerminalBase.metadata.tables[table_name]


def test_exported_classes_map_every_metadata_table() -> None:
    mapped = {getattr(models, class_name).__table__.name for class_name in models.__all__}
    assert mapped == set(TerminalBase.metadata.tables)


@pytest.mark.parametrize("table_name", sorted(EXPECTED_TABLES))
def test_every_index_and_constraint_name_contains_the_prefix(table_name: str) -> None:
    table = TerminalBase.metadata.tables[table_name]
    names = [constraint.name for constraint in table.constraints]
    names += [index.name for index in table.indexes]
    # The naming convention names every constraint and index, so an unnamed one fails as well.
    offending = [name for name in names if not (isinstance(name, str) and PREFIX in name)]
    assert names
    assert offending == []
