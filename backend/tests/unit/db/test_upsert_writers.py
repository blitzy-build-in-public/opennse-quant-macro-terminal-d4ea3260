"""Tests for the write helpers of ``opennse_terminal.db.upsert`` other than the price guard's SQL.

The contract held here:

- **Validation before SQL.** Every writer checks its arguments and rows before it builds a
  statement. An empty conflict key or one naming a non-column, an unknown mode or price source, an
  empty source, a row key that is not a column or that the writer does not accept, a missing
  required column, a ``None`` conflict-key value and a ``None`` flow raise ``ValueError``; a
  ``float`` value, an ``update_flows`` key of the wrong type and a model without a ``Table`` raise
  ``TypeError``; a column constant absent from its model raises ``RuntimeError``. Nothing is
  executed.
- **Nothing to write.** Empty input, and COALESCE-merge rows whose written values are all
  ``None``, return 0 (``update_flows``: an empty ``FlowUpdateResult``) without touching the
  executor.
- **Batching.** Rows are de-duplicated and sorted by key, and each statement carries at most 1,000
  rows and 32,000 bind parameters, counting the parameter SQLAlchemy renders per row for an absent
  column with a Python default.
- **Macro and fundamentals merges.** Each written column becomes ``COALESCE(EXCLUDED.c, stored c)``
  and ``source_by_field`` is merged with ``||``, so only the columns a writer supplies change value
  and provenance (seeded columns stay ``"seed"``); ``updated_at`` is refreshed and re-runs change
  nothing. A fundamentals row in another currency is skipped; ``period_type`` defaults to ``FY``
  and ``currency`` to KES; ``filing_id`` is replaced only when given.
- **Generic upserts.** ``do_nothing`` never touches a stored row, so a re-run inserts 0;
  ``do_update`` sets the columns each same-column run supplies, never ``id`` or ``created_at``;
  a run of key columns only falls back to ``DO NOTHING``.
- **Prices and flows.** ``csv_import`` replaces any row and owns the flow columns it supplies;
  ``nse_market_statistics`` replaces seeded and scraped rows only and never writes flows;
  ``update_flows`` sets flows on existing rows only and reports the missing keys sorted.
- **Session executors.** Every writer resolves a session's connection for its model's mapper and
  table, so a host session with no default bind, configured only through ``binds`` (by
  ``TerminalBase`` or per model) or through a routing ``get_bind`` that reads ``mapper.class_``,
  writes inside the session's transaction: visible to it, undone by its rollback and kept by its
  commit.

Pure cases run on stand-in connections. PostgreSQL cases run on the root conftest's
``seeded_database`` clone, which marks them ``integration``, because ``Numeric``, ``JSONB`` and
``ON CONFLICT`` behaviour must be real. Values a writer does not touch are compared with the
row read before the write; every written value is an exact ``Decimal`` or literal.
"""

import copy
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import date
from decimal import Decimal
from typing import Any, Protocol, cast

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Executable,
    MetaData,
    Numeric,
    String,
    Table,
    Uuid,
    delete,
    func,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from helpers.routing_session import RoutingSession
from opennse_terminal.db import upsert as upsert_module
from opennse_terminal.db.base import TerminalBase
from opennse_terminal.db.upsert import (
    FlowUpdateResult,
    UpsertMode,
    update_flows,
    upsert_fundamentals,
    upsert_macro,
    upsert_prices,
    upsert_rows,
)
from opennse_terminal.models import (
    TerminalBondYield,
    TerminalCorporateFiling,
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
    TerminalMacroIndicator,
    TerminalWatchlist,
    TerminalWatchlistItem,
)

ANCHOR = date(2026, 9, 30)
"""The last seeded session and macro observation date."""
AFTER_ANCHOR = date(2026, 10, 1)
"""A date after every seeded row: no price, macro or bond row exists on it."""
SCOM_FY = date(2026, 3, 31)
"""Safaricom's seeded FY2026 period end."""
BOND_KEY = ("observation_date", "bond_code")

WriterCall = Callable[[AsyncConnection], Awaitable[object]]


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...


class _RefusingConnection:
    """Stands in for an ``AsyncConnection`` that a writer must never reach."""

    async def execute(self, stmt: Executable) -> object:
        pytest.fail(f"a statement was executed: {type(stmt).__name__}")


REFUSING = cast(AsyncConnection, _RefusingConnection())


class _Result:
    """What ``_count`` reads: ``all()``, one entry per ``RETURNING`` row."""

    def __init__(self, n_rows: int) -> None:
        self._n_rows = n_rows

    def all(self) -> list[tuple[()]]:
        return [()] * self._n_rows


class _ChunkRecorder:
    """Compiles each statement with the PostgreSQL dialect and reports every row as written."""

    ROW_KEY = re.compile(r"code_m\d+")

    def __init__(self) -> None:
        self.params: list[Mapping[str, Any]] = []

    async def execute(self, stmt: Executable) -> _Result:
        params = stmt.compile(dialect=postgresql.dialect()).params
        self.params.append(params)
        return _Result(sum(1 for name in params if self.ROW_KEY.fullmatch(name)))


def _probe_model(n_values: int) -> type[TerminalBase]:
    """A ``terminal_``-prefixed table that is only compiled, never created: a UUID ``id``, the
    conflict key ``code``, ``n_values`` numeric columns and a ``flag`` whose Python default makes
    SQLAlchemy render one more parameter per row when rows omit it."""
    table = Table(
        "terminal_upsert_probe",
        MetaData(),
        Column("id", Uuid, primary_key=True, default=uuid.uuid4),
        Column("code", String(16), nullable=False, unique=True),
        *(Column(f"v{index:02d}", Numeric(9, 4)) for index in range(n_values)),
        Column("flag", Boolean, nullable=False, default=False),
    )
    return cast(type[TerminalBase], type("TerminalUpsertProbe", (), {"__table__": table}))


async def _one(conn: AsyncConnection, model: type[TerminalBase], **key: object) -> dict[str, Any]:
    """The single row of ``model`` whose columns equal ``key``, as a plain dict."""
    table = model.__table__
    stmt = select(table).where(*(table.c[name] == value for name, value in key.items()))
    return dict((await conn.execute(stmt)).mappings().one())


async def _bond_rows(conn: AsyncConnection, code_pattern: str) -> list[dict[str, Any]]:
    """Every ``terminal_bond_yields`` row whose ``bond_code`` matches ``code_pattern``, in key
    order, as plain dicts."""
    bond = TerminalBondYield.__table__
    stmt = (
        select(bond)
        .where(bond.c["bond_code"].like(code_pattern))
        .order_by(bond.c["observation_date"], bond.c["bond_code"])
    )
    return [dict(row) for row in (await conn.execute(stmt)).mappings()]


def _price(
    ticker: str, trade_date: date, close: str, volume: int, **extra: object
) -> dict[str, object]:
    """A price row; ``turnover_kes`` is ``volume x close`` at the column's 2 dp, as callers
    quantize."""
    close_price = Decimal(close)
    return {
        "ticker": ticker,
        "trade_date": trade_date,
        "close_price": close_price,
        "volume": volume,
        "turnover_kes": (close_price * volume).quantize(Decimal("0.01")),
        **extra,
    }


PRICE_ROW = _price("SCOM", ANCHOR, "33.2500", 2_754_329)
BOND_ROW = {
    "observation_date": AFTER_ANCHOR,
    "bond_code": "FXD1/2026/010",
    "original_tenor_years": Decimal("10.00"),
    "remaining_tenor_years": Decimal("10.00"),
    "maturity_date": date(2036, 9, 22),
    "yield_pct": Decimal("13.7500"),
    "source": "cbk_bonds",
}
FLOW_ROW = {"ticker": "SCOM", "trade_date": ANCHOR, "foreign_net_flow_kes": Decimal("-1250000.00")}
MACRO_ROW = {"observation_date": ANCHOR, "tbill_91d_pct": Decimal("8.7700")}
FUNDAMENTALS_ROW = {
    "ticker": "SCOM",
    "period_end_date": SCOM_FY,
    "revenue_kes": Decimal("388688500000.00"),
}

SCOM_FY2027 = date(2027, 3, 31)
"""A Safaricom period end the seeder does not write."""
NEW_PRICE_ROW = _price("SCOM", AFTER_ANCHOR, "33.9000", 750_000)
NEW_FLOW_ROW = {**FLOW_ROW, "trade_date": AFTER_ANCHOR}
NEW_MACRO_ROW = {**MACRO_ROW, "observation_date": AFTER_ANCHOR}
NEW_FUNDAMENTALS_ROW = {**FUNDAMENTALS_ROW, "period_end_date": SCOM_FY2027}
BINDS_ONLY_MODELS: tuple[type[TerminalBase], ...] = (
    TerminalBondYield,
    TerminalDailyPrice,
    TerminalMacroIndicator,
    TerminalFundamental,
)
"""The models the binds-only session test writes, the only ones its per-model ``binds`` map."""


@pytest.mark.parametrize(
    ("call", "error", "match"),
    [
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [BOND_ROW], (), "do_update"),
            ValueError,
            r"key must name at least one column",
            id="upsert_rows-empty-key",
        ),
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [BOND_ROW], ("bond_kode",), "do_update"),
            ValueError,
            r"key columns \['bond_kode'\] are not columns",
            id="upsert_rows-key-not-a-column",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [BOND_ROW], BOND_KEY, cast(UpsertMode, "merge")
            ),
            ValueError,
            r"unknown mode 'merge'",
            id="upsert_rows-unknown-mode",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [BOND_ROW, {**BOND_ROW, "tenor": 10}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"terminal_bond_yields row 1: 'tenor' is not a column",
            id="upsert_rows-unknown-column-names-the-row",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{"observation_date": AFTER_ANCHOR}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"row 0: missing required columns \['bond_code'\]",
            id="upsert_rows-missing-key-column",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{**BOND_ROW, "bond_code": None}], BOND_KEY, "do_nothing"
            ),
            ValueError,
            r"conflict-key column 'bond_code' is None",
            id="upsert_rows-none-key-value",
        ),
        pytest.param(
            lambda c: upsert_rows(
                c, TerminalBondYield, [{**BOND_ROW, "yield_pct": 13.75}], BOND_KEY, "do_update"
            ),
            TypeError,
            r"column 'yield_pct' holds a float; pass a Decimal",
            id="upsert_rows-float-value",
        ),
        pytest.param(
            lambda c: upsert_prices(c, [PRICE_ROW], source="cbk_fx", ingestion_run_id=None),
            ValueError,
            r"source 'cbk_fx' is not one of \['seed', 'nse_market_statistics', 'csv_import'\]",
            id="upsert_prices-unknown-source",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c,
                [{**PRICE_ROW, "foreign_net_flow_kes": Decimal("1.00")}],
                source="nse_market_statistics",
                ingestion_run_id=None,
            ),
            ValueError,
            r"column 'foreign_net_flow_kes' is not accepted here",
            id="upsert_prices-flow-only-from-csv_import",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c, [{**PRICE_ROW, "source": "seed"}], source="seed", ingestion_run_id=None
            ),
            ValueError,
            r"column 'source' is not accepted here",
            id="upsert_prices-source-column-in-row",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c,
                [{k: v for k, v in PRICE_ROW.items() if k != "turnover_kes"}],
                source="csv_import",
                ingestion_run_id=None,
            ),
            ValueError,
            r"missing required columns \['turnover_kes'\]",
            id="upsert_prices-missing-turnover",
        ),
        pytest.param(
            lambda c: upsert_prices(
                c, [{**PRICE_ROW, "close_price": 33.25}], source="seed", ingestion_run_id=None
            ),
            TypeError,
            r"column 'close_price' holds a float",
            id="upsert_prices-float-close",
        ),
        pytest.param(
            lambda c: update_flows(c, [FLOW_ROW], flow_source=""),
            ValueError,
            r"update_flows: source must be a non-empty string",
            id="update_flows-empty-source",
        ),
        pytest.param(
            lambda c: update_flows(c, [{**FLOW_ROW, "ticker": 7}], flow_source="csv_import"),
            TypeError,
            r"update_flows row 0: ticker must be str and trade_date a date",
            id="update_flows-ticker-not-str",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [FLOW_ROW, {**FLOW_ROW, "trade_date": "2026-09-30"}], flow_source="csv_import"
            ),
            TypeError,
            r"update_flows row 1: ticker must be str and trade_date a date",
            id="update_flows-trade_date-not-date",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "foreign_net_flow_kes": None}], flow_source="csv_import"
            ),
            ValueError,
            r"update_flows row 0: foreign_net_flow_kes must not be None",
            id="update_flows-none-flow",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "close_price": Decimal("33.25")}], flow_source="csv_import"
            ),
            ValueError,
            r"column 'close_price' is not accepted here",
            id="update_flows-price-column",
        ),
        pytest.param(
            lambda c: update_flows(
                c, [{**FLOW_ROW, "foreign_net_flow_kes": -1250000.0}], flow_source="csv_import"
            ),
            TypeError,
            r"column 'foreign_net_flow_kes' holds a float",
            id="update_flows-float-flow",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [MACRO_ROW], source=""),
            ValueError,
            r"upsert_macro: source must be a non-empty string",
            id="upsert_macro-empty-source",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c, [{**MACRO_ROW, "source_by_field": {}}], source="cbk_rates_csv"
            ),
            ValueError,
            r"column 'source_by_field' is not accepted here",
            id="upsert_macro-provenance-column",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [{"tbill_91d_pct": Decimal("8.77")}], source="cbk_rates_csv"),
            ValueError,
            r"missing required columns \['observation_date'\]",
            id="upsert_macro-missing-date",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c, [{**MACRO_ROW, "observation_date": None}], source="cbk_rates_csv"
            ),
            ValueError,
            r"conflict-key column 'observation_date' is None",
            id="upsert_macro-none-date",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(c, [FUNDAMENTALS_ROW], source=""),
            ValueError,
            r"upsert_fundamentals: source must be a non-empty string",
            id="upsert_fundamentals-empty-source",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "filing_id": uuid.uuid4()}], source="pdf_filing"
            ),
            ValueError,
            r"column 'filing_id' is not accepted here",
            id="upsert_fundamentals-filing_id-in-row",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{"ticker": "SCOM", "revenue_kes": Decimal("1.00")}], source="csv_import"
            ),
            ValueError,
            r"missing required columns \['period_end_date'\]",
            id="upsert_fundamentals-missing-period_end_date",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "ticker": None}], source="csv_import"
            ),
            ValueError,
            r"conflict-key column 'ticker' is None",
            id="upsert_fundamentals-none-ticker",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "dps_kes": 1.2}], source="csv_import"
            ),
            TypeError,
            r"column 'dps_kes' holds a float",
            id="upsert_fundamentals-float-dps",
        ),
    ],
)
async def test_writer_rejects_bad_input_before_any_sql(
    call: WriterCall, error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        await call(REFUSING)


async def test_model_whose_table_is_not_a_table_is_rejected() -> None:
    probe_table = _probe_model(1).__table__
    not_a_table = type("TerminalSubqueryProbe", (), {"__table__": select(probe_table).subquery()})

    with pytest.raises(TypeError, match=r"TerminalSubqueryProbe\.__table__ is not a Table"):
        await upsert_rows(
            REFUSING, cast(type[TerminalBase], not_a_table), [], ("code",), "do_nothing"
        )


def test_column_constant_drift_fails_loudly() -> None:
    with pytest.raises(
        RuntimeError,
        match=r"db\.upsert names columns absent from terminal_macro_indicators: \['cpi_pct'\]",
    ):
        upsert_module._require_columns(TerminalMacroIndicator, ("inflation_yoy_pct", "cpi_pct"))


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        pytest.param(
            lambda c: upsert_rows(c, TerminalBondYield, [], BOND_KEY, "do_update"),
            0,
            id="upsert_rows-no-rows",
        ),
        pytest.param(
            lambda c: upsert_prices(c, [], source="csv_import", ingestion_run_id=None),
            0,
            id="upsert_prices-no-rows",
        ),
        pytest.param(
            lambda c: update_flows(c, [], flow_source="csv_import"),
            FlowUpdateResult(rows_updated=0, missing_keys=()),
            id="update_flows-no-rows",
        ),
        pytest.param(
            lambda c: upsert_macro(c, [{"observation_date": ANCHOR}], source="cbk_fx"),
            0,
            id="upsert_macro-no-indicator-column",
        ),
        pytest.param(
            lambda c: upsert_macro(
                c,
                [{"observation_date": ANCHOR, "fx_kes_usd": None, "fx_reserves_usd_m": None}],
                source="cbk_fx",
            ),
            0,
            id="upsert_macro-all-values-none",
        ),
        pytest.param(
            lambda c: upsert_fundamentals(
                c, [{**FUNDAMENTALS_ROW, "revenue_kes": None}], source="csv_import"
            ),
            0,
            id="upsert_fundamentals-all-values-none",
        ),
    ],
)
async def test_nothing_to_write_never_reaches_the_executor(
    call: WriterCall, expected: object
) -> None:
    assert await call(REFUSING) == expected


@pytest.mark.parametrize(
    ("n_values", "expected_chunks"),
    [
        # id, code, v00 and the defaulted flag: 4 parameters a row, so the 1,000-row cap binds.
        pytest.param(1, [1_000, 1_000], id="row-cap"),
        # id, code, v00..v37 and the defaulted flag: 41 a row, so 32,000 // 41 = 780 rows bind.
        pytest.param(38, [780, 780, 440], id="bind-parameter-cap"),
    ],
)
async def test_statements_stay_within_row_and_bind_parameter_bounds(
    n_values: int, expected_chunks: list[int]
) -> None:
    model = _probe_model(n_values)
    values = {f"v{index:02d}": Decimal("1.2500") for index in range(n_values)}
    # Offered in descending key order: the writer sorts them.
    rows = [{"code": f"W{n:04d}", **values} for n in reversed(range(2_000))]
    snapshot = copy.deepcopy(rows)
    recorder = _ChunkRecorder()

    written = await upsert_rows(
        cast(AsyncConnection, recorder), model, rows, ("code",), "do_update"
    )

    assert written == 2_000
    params_per_row = n_values + 3
    assert [len(params) for params in recorder.params] == [
        size * params_per_row for size in expected_chunks
    ]
    assert max(len(params) for params in recorder.params) <= 32_000
    start = 0
    for params, size in zip(recorder.params, expected_chunks, strict=True):
        # Each statement covers the key range right after the previous statement's.
        first, last = params["code_m0"], params[f"code_m{size - 1}"]
        assert (first, last) == (f"W{start:04d}", f"W{start + size - 1:04d}")
        start += size
    assert rows == snapshot


async def test_macro_merge_coalesces_and_relabels_only_written_columns(
    seeded_database: SeededDatabase,
) -> None:
    october = date(2026, 10, 31)
    # CBK's published September 2026 figures replace the seeded ones; CBR is not in this release.
    rows = [
        {
            "observation_date": ANCHOR,
            "tbill_91d_pct": Decimal("8.7700"),
            "interbank_rate_pct": Decimal("8.7500"),
            "cbr_rate_pct": None,
        },
        {"observation_date": october, "tbill_91d_pct": Decimal("8.6900")},
        # A repeated date: the last row for it wins.
        {"observation_date": october, "tbill_91d_pct": Decimal("8.6100")},
    ]
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)

        written = await upsert_macro(conn, rows, source="cbk_rates_csv")

        merged = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)
        inserted = await _one(conn, TerminalMacroIndicator, observation_date=october)
        rerun = await upsert_macro(conn, rows, source="cbk_rates_csv")
        after_rerun = await _one(conn, TerminalMacroIndicator, observation_date=ANCHOR)

    assert written == 2
    assert before["source_by_field"]["cbr_rate_pct"] == "seed"
    assert merged["tbill_91d_pct"] == Decimal("8.7700")
    assert merged["interbank_rate_pct"] == Decimal("8.7500")
    unwritten = ("inflation_yoy_pct", "cbr_rate_pct", "tbill_182d_pct", "tbill_364d_pct")
    for name in (*unwritten, "fx_kes_usd", "fx_reserves_usd_m", "id", "created_at"):
        assert merged[name] == before[name], name
    assert merged["source_by_field"] == {
        **before["source_by_field"],
        "tbill_91d_pct": "cbk_rates_csv",
        "interbank_rate_pct": "cbk_rates_csv",
    }
    assert merged["updated_at"] > before["updated_at"]
    assert inserted["tbill_91d_pct"] == Decimal("8.6100")
    assert inserted["source_by_field"] == {"tbill_91d_pct": "cbk_rates_csv"}
    for name in upsert_module.MACRO_INDICATOR_COLUMNS:
        if name != "tbill_91d_pct":
            assert inserted[name] is None, name
    assert rerun == 2
    assert after_rerun == merged


async def test_fundamentals_merge_keeps_unwritten_columns_and_links_the_filing(
    seeded_database: SeededDatabase,
) -> None:
    sha256 = "a" * 64
    filing = {
        "ticker": "SCOM",
        "document_type": "annual_report",
        "period_end_date": SCOM_FY,
        "period_type": "FY",
        "file_name": "safaricom-fy2026.pdf",
        "file_sha256": sha256,
        "file_size_bytes": 48_211,
        "page_count": 3,
        "extraction": [],
        "status": "partial",
    }
    key = {"ticker": "SCOM", "period_end_date": SCOM_FY, "period_type": "FY"}
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalFundamental, **key)
        filed = await upsert_rows(
            conn, TerminalCorporateFiling, [filing], ("file_sha256",), "do_update"
        )
        filing_id = (await _one(conn, TerminalCorporateFiling, file_sha256=sha256))["id"]

        # period_type is absent: the row merges into the seeded FY row.
        from_pdf = await upsert_fundamentals(
            conn,
            [
                {
                    "ticker": "SCOM",
                    "period_end_date": SCOM_FY,
                    "revenue_kes": Decimal("388688500000.00"),
                    "net_income_kes": Decimal("84500000000.00"),
                    "ebitda_kes": None,
                }
            ],
            source="pdf_filing",
            filing_id=filing_id,
        )
        after_pdf = await _one(conn, TerminalFundamental, **key)
        from_csv = await upsert_fundamentals(
            conn, [{**key, "dps_kes": Decimal("1.2000")}], source="csv_import"
        )
        after_csv = await _one(conn, TerminalFundamental, **key)

    assert filed == 1
    assert from_pdf == 1
    assert before["source_by_field"]["ebitda_kes"] == "seed"
    assert after_pdf["revenue_kes"] == Decimal("388688500000.00")
    assert after_pdf["net_income_kes"] == Decimal("84500000000.00")
    for name in (*upsert_module.FUNDAMENTAL_VALUE_COLUMNS, "id", "created_at", "currency"):
        if name not in ("revenue_kes", "net_income_kes"):
            assert after_pdf[name] == before[name], name
    assert after_pdf["source_by_field"] == {
        **before["source_by_field"],
        "revenue_kes": "pdf_filing",
        "net_income_kes": "pdf_filing",
    }
    assert after_pdf["filing_id"] == filing_id
    assert after_pdf["updated_at"] > before["updated_at"]
    assert from_csv == 1
    assert after_csv["dps_kes"] == Decimal("1.2000")
    assert after_csv["filing_id"] == filing_id
    assert after_csv["source_by_field"] == {**after_pdf["source_by_field"], "dps_kes": "csv_import"}


async def test_fundamentals_currency_guard_and_key_defaults(
    seeded_database: SeededDatabase,
) -> None:
    seeded_key = {"ticker": "SCOM", "period_end_date": SCOM_FY, "period_type": "FY"}
    eabl_fy = date(2027, 6, 30)
    kcb_h1 = date(2026, 6, 30)
    async with seeded_database.engine.begin() as conn:
        before = await _one(conn, TerminalFundamental, **seeded_key)

        usd = await upsert_fundamentals(
            conn,
            [{**seeded_key, "currency": "USD", "revenue_kes": Decimal("3000000000.00")}],
            source="csv_import",
        )
        unchanged = await _one(conn, TerminalFundamental, **seeded_key)
        inserted = await upsert_fundamentals(
            conn,
            [
                # An absent and an explicit FY are one key; the later row wins.
                {"ticker": "EABL", "period_end_date": eabl_fy, "revenue_kes": Decimal("1.00")},
                {
                    "ticker": "EABL",
                    "period_end_date": eabl_fy,
                    "period_type": "FY",
                    "currency": None,
                    "revenue_kes": Decimal("131200000000.00"),
                },
                {
                    "ticker": "KCB",
                    "period_end_date": kcb_h1,
                    "period_type": "H1",
                    "net_income_kes": Decimal("33900000000.00"),
                },
            ],
            source="csv_import",
        )
        eabl = await _one(conn, TerminalFundamental, ticker="EABL", period_end_date=eabl_fy)
        kcb = await _one(conn, TerminalFundamental, ticker="KCB", period_end_date=kcb_h1)

    assert usd == 0
    assert unchanged == before
    assert inserted == 2
    assert (eabl["period_type"], eabl["currency"]) == ("FY", "KES")
    assert eabl["revenue_kes"] == Decimal("131200000000.00")
    assert eabl["net_income_kes"] is None
    assert eabl["source_by_field"] == {"revenue_kes": "csv_import"}
    assert eabl["filing_id"] is None
    assert (kcb["period_type"], kcb["currency"]) == ("H1", "KES")
    assert kcb["net_income_kes"] == Decimal("33900000000.00")
    assert kcb["revenue_kes"] is None
    assert kcb["source_by_field"] == {"net_income_kes": "csv_import"}


async def test_do_nothing_reruns_insert_zero_across_chunks(seeded_database: SeededDatabase) -> None:
    bond = TerminalBondYield.__table__
    rows = [
        {**BOND_ROW, "bond_code": f"FXD-TEST-{n:04d}", "isin": None, "coupon_rate_pct": None}
        for n in range(2_500)
    ]
    repriced = [{**row, "yield_pct": Decimal("14.1250")} for row in rows]
    test_codes = bond.c["bond_code"].like("FXD-TEST-%")
    async with seeded_database.engine.begin() as conn:
        seeded = await _bond_rows(conn, "FXD-SEED-%")

        reseeded = await upsert_rows(conn, TerminalBondYield, seeded, BOND_KEY, "do_nothing")
        first = await upsert_rows(conn, TerminalBondYield, rows, BOND_KEY, "do_nothing")
        ids = set((await conn.execute(select(bond.c["id"]).where(test_codes))).scalars())
        rerun = await upsert_rows(conn, TerminalBondYield, repriced, BOND_KEY, "do_nothing")
        kept = set((await conn.execute(select(bond.c["yield_pct"]).where(test_codes))).scalars())
        updated = await upsert_rows(conn, TerminalBondYield, repriced, BOND_KEY, "do_update")
        after = (
            await conn.execute(select(bond.c["id"], bond.c["yield_pct"]).where(test_codes))
        ).all()
        seeded_after = await _bond_rows(conn, "FXD-SEED-%")

    assert len(seeded) > 0
    assert reseeded == 0
    assert seeded_after == seeded
    assert first == 2_500
    assert len(ids) == 2_500
    assert rerun == 0
    assert kept == {Decimal("13.7500")}
    assert updated == 2_500
    assert {row_id for row_id, _ in after} == ids
    assert {yield_pct for _, yield_pct in after} == {Decimal("14.1250")}


async def test_do_update_writes_each_runs_supplied_columns_only(
    seeded_database: SeededDatabase,
) -> None:
    async with seeded_database.engine.begin() as conn:
        eabl_before = await _one(conn, TerminalEquity, ticker="EABL")
        scom_before = await _one(conn, TerminalEquity, ticker="SCOM")
        required = ("ticker", "company_name", "sector", "shares_outstanding")
        rows: list[dict[str, object]] = [
            # Sorted by ticker, then written as same-column runs: [EABL, NMG], then [SCOM].
            {
                **{name: scom_before[name] for name in required},
                "isin": scom_before["isin"],
                "free_float_pct": Decimal("35.0000"),
                "source": "csv_import",
            },
            {**{name: eabl_before[name] for name in required}, "source": "csv_import"},
            {
                "ticker": "NMG",
                "company_name": "Nation Media Group PLC",
                "sector": "Commercial & Services",
                "shares_outstanding": 207_396_515,
                "source": "csv_import",
            },
        ]
        snapshot = copy.deepcopy(rows)

        written = await upsert_rows(conn, TerminalEquity, rows, ("ticker",), "do_update")

        eabl = await _one(conn, TerminalEquity, ticker="EABL")
        scom = await _one(conn, TerminalEquity, ticker="SCOM")
        nmg = await _one(conn, TerminalEquity, ticker="NMG")

    assert written == 3
    assert rows == snapshot
    assert eabl["source"] == "csv_import"
    for name in ("id", "created_at", "isin", "free_float_pct", "currency"):
        assert eabl[name] == eabl_before[name], name
    assert eabl["updated_at"] > eabl_before["updated_at"]
    assert scom["free_float_pct"] == Decimal("35.0000")
    assert (scom["id"], scom["created_at"]) == (scom_before["id"], scom_before["created_at"])
    assert nmg["currency"] == "KES"
    assert (nmg["isin"], nmg["free_float_pct"]) == (None, None)
    assert nmg["shares_outstanding"] == 207_396_515


async def test_composite_key_without_id_and_key_only_update(
    seeded_database: SeededDatabase,
) -> None:
    items = TerminalWatchlistItem.__table__
    key = ("watchlist_id", "ticker")
    async with seeded_database.engine.begin() as conn:
        watchlist = await _one(conn, TerminalWatchlist, owner_key="local", name="NSE Core 8")
        on_list = items.c["watchlist_id"] == watchlist["id"]
        await conn.execute(delete(items).where(on_list, items.c["ticker"] == "KCB"))
        kcb = {"watchlist_id": watchlist["id"], "ticker": "KCB"}
        scom = {"watchlist_id": watchlist["id"], "ticker": "SCOM"}

        added = await upsert_rows(conn, TerminalWatchlistItem, [scom, kcb], key, "do_nothing")
        # Only key columns: nothing to update, so DO NOTHING applies even in do_update mode.
        key_only = await upsert_rows(conn, TerminalWatchlistItem, [kcb, scom], key, "do_update")
        count = (
            await conn.execute(select(func.count()).select_from(items).where(on_list))
        ).scalar_one()

    assert added == 1
    assert key_only == 0
    assert count == 8


async def test_price_precedence_and_flow_ownership(seeded_database: SeededDatabase) -> None:
    scom = {"ticker": "SCOM", "trade_date": ANCHOR}
    kcb = {"ticker": "KCB", "trade_date": ANCHOR}
    run_id = uuid.uuid4()
    run = {"id": run_id, "pipeline": "nse_market_statistics", "trigger": "cli", "status": "running"}
    async with seeded_database.engine.begin() as conn:
        seeded = await _one(conn, TerminalDailyPrice, **scom)
        kcb_seeded = await _one(conn, TerminalDailyPrice, **kcb)
        run_written = await upsert_rows(conn, TerminalIngestionRun, [run], ("id",), "do_nothing")

        scraped = await upsert_prices(
            conn,
            [_price("SCOM", ANCHOR, "33.5000", 1_000_000, turnover_estimated=True)],
            source="nse_market_statistics",
            ingestion_run_id=run_id,
        )
        after_scrape = await _one(conn, TerminalDailyPrice, **scom)
        imported = await upsert_prices(
            conn,
            [
                _price(
                    "SCOM",
                    ANCHOR,
                    "33.7500",
                    900_000,
                    open_price=Decimal("33.0000"),
                    high_price=Decimal("34.0000"),
                    low_price=Decimal("32.7500"),
                    foreign_net_flow_kes=Decimal("-1250000.00"),
                )
            ],
            source="csv_import",
            ingestion_run_id=None,
        )
        after_import = await _one(conn, TerminalDailyPrice, **scom)
        rescraped = await upsert_prices(
            conn,
            [_price("SCOM", ANCHOR, "34.0000", 500_000, turnover_estimated=True)],
            source="nse_market_statistics",
            ingestion_run_id=None,
        )
        after_rescrape = await _one(conn, TerminalDailyPrice, **scom)
        flowless = await upsert_prices(
            conn,
            [
                _price("SCOM", AFTER_ANCHOR, "33.9000", 750_000),
                _price("KCB", ANCHOR, "85.2500", 400_000, foreign_net_flow_kes=None),
            ],
            source="csv_import",
            ingestion_run_id=None,
        )
        new_row = await _one(conn, TerminalDailyPrice, ticker="SCOM", trade_date=AFTER_ANCHOR)
        kcb_after = await _one(conn, TerminalDailyPrice, **kcb)

    assert run_written == 1
    assert seeded["source"] == "seed"
    assert seeded["flow_source"] == "seed"
    assert scraped == 1
    assert after_scrape["source"] == "nse_market_statistics"
    assert after_scrape["ingestion_run_id"] == run_id
    assert after_scrape["close_price"] == Decimal("33.5000")
    assert after_scrape["volume"] == 1_000_000
    assert after_scrape["turnover_kes"] == Decimal("33500000.00")
    assert after_scrape["turnover_estimated"] is True
    assert (after_scrape["open_price"], after_scrape["high_price"]) == (None, None)
    assert after_scrape["low_price"] is None
    for name in ("id", "foreign_net_flow_kes", "flow_source"):
        assert after_scrape[name] == seeded[name], name
    assert after_scrape["ingested_at"] > seeded["ingested_at"]

    assert imported == 1
    assert after_import["source"] == "csv_import"
    assert after_import["ingestion_run_id"] is None
    assert after_import["open_price"] == Decimal("33.0000")
    assert after_import["high_price"] == Decimal("34.0000")
    assert after_import["low_price"] == Decimal("32.7500")
    assert after_import["close_price"] == Decimal("33.7500")
    assert after_import["turnover_kes"] == Decimal("30375000.00")
    assert after_import["turnover_estimated"] is False
    assert after_import["foreign_net_flow_kes"] == Decimal("-1250000.00")
    assert after_import["flow_source"] == "csv_import"

    assert rescraped == 0
    assert after_rescrape == after_import

    assert flowless == 2
    assert (new_row["source"], new_row["close_price"]) == ("csv_import", Decimal("33.9000"))
    assert (new_row["foreign_net_flow_kes"], new_row["flow_source"]) == (None, None)
    assert kcb_after["source"] == "csv_import"
    assert kcb_after["close_price"] == Decimal("85.2500")
    assert kcb_after["foreign_net_flow_kes"] == kcb_seeded["foreign_net_flow_kes"]
    assert kcb_after["flow_source"] == "seed"


async def test_update_flows_sets_existing_rows_and_reports_missing_keys(
    seeded_database: SeededDatabase,
) -> None:
    key = {"ticker": "SCOM", "trade_date": ANCHOR}
    rows = [
        {**key, "foreign_net_flow_kes": Decimal("5000000.00")},
        {"ticker": "SCOM", "trade_date": AFTER_ANCHOR, "foreign_net_flow_kes": Decimal("1.00")},
        {"ticker": "EQTY", "trade_date": AFTER_ANCHOR, "foreign_net_flow_kes": Decimal("2.00")},
        # The last row for a key wins.
        {**key, "foreign_net_flow_kes": Decimal("-750000.00")},
    ]
    # A session executor: the writer joins the session's transaction.
    async with AsyncSession(seeded_database.engine) as session, session.begin():
        conn = await session.connection()
        before = await _one(conn, TerminalDailyPrice, **key)

        outcome = await update_flows(session, rows, flow_source="csv_import")

        after = await _one(conn, TerminalDailyPrice, **key)
        stray = (
            await conn.execute(
                select(func.count())
                .select_from(TerminalDailyPrice)
                .where(TerminalDailyPrice.trade_date == AFTER_ANCHOR)
            )
        ).scalar_one()

    assert outcome == FlowUpdateResult(
        rows_updated=1, missing_keys=(("EQTY", AFTER_ANCHOR), ("SCOM", AFTER_ANCHOR))
    )
    assert after["foreign_net_flow_kes"] == Decimal("-750000.00")
    assert after["flow_source"] == "csv_import"
    unchanged = {name: value for name, value in before.items() if "flow" not in name}
    assert {name: after[name] for name in unchanged} == unchanged
    assert stray == 0


async def _new_row_values(executor: AsyncSession | AsyncConnection) -> tuple[object, ...]:
    """The values the binds-only writes set, ``None`` where a row is absent.

    Read with ORM column selects, so a session without a default bind resolves each statement's
    bind from its model, as its own queries do.
    """
    statements = (
        select(TerminalBondYield.yield_pct).where(
            TerminalBondYield.observation_date == AFTER_ANCHOR,
            TerminalBondYield.bond_code == BOND_ROW["bond_code"],
        ),
        select(TerminalDailyPrice.close_price).where(
            TerminalDailyPrice.ticker == "SCOM", TerminalDailyPrice.trade_date == AFTER_ANCHOR
        ),
        select(TerminalDailyPrice.foreign_net_flow_kes).where(
            TerminalDailyPrice.ticker == "SCOM", TerminalDailyPrice.trade_date == AFTER_ANCHOR
        ),
        select(TerminalMacroIndicator.tbill_91d_pct).where(
            TerminalMacroIndicator.observation_date == AFTER_ANCHOR
        ),
        select(TerminalFundamental.revenue_kes).where(
            TerminalFundamental.ticker == "SCOM",
            TerminalFundamental.period_end_date == SCOM_FY2027,
            TerminalFundamental.period_type == "FY",
        ),
    )
    return tuple([(await executor.execute(stmt)).scalar_one_or_none() for stmt in statements])


async def _write_with_every_writer(session: AsyncSession) -> tuple[object, ...]:
    """Run each writer once through ``session`` and return what each reports.

    ``update_flows`` targets the price row ``upsert_prices`` inserts just before it, which it finds
    only inside the same transaction.
    """
    return (
        await upsert_rows(session, TerminalBondYield, [BOND_ROW], BOND_KEY, "do_update"),
        await upsert_prices(session, [NEW_PRICE_ROW], source="csv_import", ingestion_run_id=None),
        await update_flows(session, [NEW_FLOW_ROW], flow_source="csv_import"),
        await upsert_macro(session, [NEW_MACRO_ROW], source="cbk_rates_csv"),
        await upsert_fundamentals(session, [NEW_FUNDAMENTALS_ROW], source="csv_import"),
    )


@pytest.mark.parametrize(
    "session_options",
    [
        pytest.param(lambda engine: {"binds": {TerminalBase: engine}}, id="base-class-binds"),
        pytest.param(
            lambda engine: {"binds": {model: engine for model in BINDS_ONLY_MODELS}},
            id="per-model-binds",
        ),
        pytest.param(
            lambda engine: {
                "sync_session_class": RoutingSession,
                "terminal_bind": engine.sync_engine,
            },
            id="routing-get-bind",
        ),
    ],
)
async def test_every_writer_joins_a_binds_only_session(
    seeded_database: SeededDatabase,
    session_options: Callable[[AsyncEngine], dict[str, Any]],
) -> None:
    engine = seeded_database.engine
    written = (1, 1, FlowUpdateResult(rows_updated=1, missing_keys=()), 1, 1)
    values = (
        BOND_ROW["yield_pct"],
        NEW_PRICE_ROW["close_price"],
        NEW_FLOW_ROW["foreign_net_flow_kes"],
        NEW_MACRO_ROW["tbill_91d_pct"],
        NEW_FUNDAMENTALS_ROW["revenue_kes"],
    )
    absent = (None,) * len(values)
    # No default bind: only the binds mapping, or the routing get_bind, locates the database.
    session = AsyncSession(**session_options(engine))
    assert session.sync_session.bind is None
    async with engine.connect() as observer:
        assert await _new_row_values(observer) == absent

        async with session:
            rolled_back = await _write_with_every_writer(session)
            in_session = await _new_row_values(session)
            # The writes are uncommitted session state, not autocommitted on another connection.
            outside_before_rollback = await _new_row_values(observer)
            await session.rollback()
            after_rollback = await _new_row_values(observer)

            committed = await _write_with_every_writer(session)
            await session.commit()
        after_commit = await _new_row_values(observer)

    assert rolled_back == written
    assert in_session == values
    assert outside_before_rollback == absent
    assert after_rollback == absent
    assert committed == written
    assert after_commit == values
