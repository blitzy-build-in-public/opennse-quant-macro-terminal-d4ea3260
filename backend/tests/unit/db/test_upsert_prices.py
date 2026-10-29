"""Tests for the price precedence guard of ``opennse_terminal.db.upsert.upsert_prices``.

``csv_import``, the highest-ranked writer, must replace a stored row whatever its ``source``, so its
``ON CONFLICT DO UPDATE`` carries no ``WHERE``. A lower writer updates only rows whose stored source
is listed at or below its rank, as a bound ``IN`` list, never inline literals.

Every case runs without PostgreSQL: a recording connection captures each statement, which is
compiled with the PostgreSQL dialect.
"""

import re
from datetime import date
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy import Executable
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.db.upsert import upsert_prices

ROW = {
    "ticker": "SCOM",
    "trade_date": date(2026, 9, 30),
    "close_price": Decimal("25.0000"),
    "volume": 1_000,
    "turnover_kes": Decimal("25000.00"),
}
CONFLICT_UPDATE = "ON CONFLICT (ticker, trade_date) DO UPDATE SET "
# The IN list must hold bound placeholders only, so an inline literal fails the match.
GUARD_RE = re.compile(
    r"DO UPDATE SET .* WHERE terminal_daily_prices\.source IN \(((?:%\(\w+\)s(?:, )?)+)\)"
)
PLACEHOLDER_RE = re.compile(r"%\((\w+)\)s")


class _Result:
    """What ``_count`` reads: ``all()``, one entry per ``RETURNING`` row."""

    def __init__(self, returned: list[tuple[object, ...]]) -> None:
        self._returned = returned

    def all(self) -> list[tuple[object, ...]]:
        return self._returned


class _RecordingConnection:
    """Stands in for an ``AsyncConnection``: records each statement and reports one written row."""

    def __init__(self) -> None:
        self.statements: list[Executable] = []

    async def execute(self, stmt: Executable) -> _Result:
        self.statements.append(stmt)
        return _Result([(ROW["ticker"], ROW["trade_date"])])


@pytest.mark.parametrize(
    ("source", "overwritable"),
    [
        pytest.param("csv_import", None, id="csv_import-overwrites-any-source"),
        pytest.param(
            "nse_market_statistics",
            ("seed", "nse_market_statistics"),
            id="nse_market_statistics-seed-and-nse-only",
        ),
        pytest.param("seed", ("seed",), id="seed-only-seed"),
    ],
)
async def test_conflict_guard_by_writer_rank(
    source: str, overwritable: tuple[str, ...] | None
) -> None:
    conn = _RecordingConnection()

    written = await upsert_prices(
        cast(AsyncConnection, conn), [ROW], source=source, ingestion_run_id=None
    )

    assert written == 1
    assert len(conn.statements) == 1
    compiled = conn.statements[0].compile(
        dialect=postgresql.dialect(), compile_kwargs={"render_postcompile": True}
    )
    sql = compiled.string
    assert CONFLICT_UPDATE in sql
    for name in ("seed", "nse_market_statistics", "csv_import"):
        assert f"'{name}'" not in sql
    if overwritable is None:
        assert "WHERE" not in sql
        return
    guard = GUARD_RE.search(sql)
    assert guard is not None
    placeholders = PLACEHOLDER_RE.findall(guard.group(1))
    assert tuple(compiled.params[name] for name in placeholders) == overwritable
