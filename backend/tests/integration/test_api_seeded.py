"""Every terminal endpoint answered from the seeded database alone (the seeded-evidence table).

Each endpoint test drives ``create_app()`` over ASGI through the ``client`` fixture, on this
module's own seeded clone; no scraper, browser or network call is involved. Requests that write
(the PDF commit and the watchlist lifecycle) run inside ``db_session``'s outer transaction, which
is rolled back after each test, so every test reads the seeded state whatever the order.

Decimal fields travel as JSON strings and are compared exactly, as strings or through
:func:`dec`, never as floats. Integer fields (volumes, counts, offsets, share counts) are checked
to be JSON integers.

The service sections after the endpoint tests call the market, macro, cycle and IPO services
behind those endpoints directly. Their database tests each get a fresh seeded clone (the root
conftest's ``seeded_database``) instead of the module clone, so the rows a test changes stay in
its own clone; the rest need no database. Each section opens with the contract it holds.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import logging
import time
import uuid
from bisect import bisect_right
from calendar import monthrange
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from statistics import mean
from types import ModuleType
from typing import Any, Final, Protocol
from zoneinfo import ZoneInfo

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException
from sqlalchemy import Executable, delete, event, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal import __version__
from opennse_terminal.config import TerminalSettings
from opennse_terminal.main import create_app
from opennse_terminal.models import (
    TerminalBondYield,
    TerminalCorporateFiling,
    TerminalCycle,
    TerminalDailyPrice,
    TerminalEquity,
    TerminalFundamental,
    TerminalIngestionRun,
    TerminalIpoScenario,
    TerminalMacroIndicator,
    TerminalWatchlist,
    TerminalWatchlistItem,
)
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS
from opennse_terminal.quant.asof import AsOfValue, MacroColumn, MacroRow, macro_as_of
from opennse_terminal.quant.backtest import (
    BacktestCandidate,
    CycleBacktestResult,
    CycleEvent,
    CyclePointResult,
    CycleWindow,
    run_cycle_backtest,
)
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.schemas.cycles import BacktestResponse, CycleResult
from opennse_terminal.schemas.macro import IndicatorSnapshot, YieldCurvePoint
from opennse_terminal.schemas.market import (
    AnomalyMetric,
    ScreenerParams,
    ScreenerSort,
    SortOrder,
)
from opennse_terminal.services import cycle_service as cs
from opennse_terminal.services import macro_service
from opennse_terminal.services import market_service as ms
from opennse_terminal.services.ipo_service import _scenario_from_row
from opennse_terminal.services.macro_service import (
    BondObservation,
    get_macro_overview,
    yield_curve_at,
)
from opennse_terminal.services.offload import CLIENT_CLOSED_REQUEST, RequestAbandonedError

BASE: Final[str] = "/api/v1/terminal"

SEEDED_TICKERS: Final[frozenset[str]] = frozenset(
    {"SCOM", "EQTY", "KCB", "EABL", "KEGN", "COOP", "ABSA", "SBIC"}
)
BANKS: Final[frozenset[str]] = frozenset({"EQTY", "KCB", "COOP", "ABSA", "SBIC"})
SECTORS: Final[frozenset[str]] = frozenset(
    {"Telecommunications", "Banking", "Manufacturing & Allied", "Energy & Petroleum"}
)

ANCHOR: Final[str] = "2026-09-30"
"""The last seeded session, macro month-end and bond observation date."""

INDICATOR_KEYS: Final[frozenset[str]] = frozenset(
    {
        "inflation_yoy_pct",
        "cbr_rate_pct",
        "interbank_rate_pct",
        "tbill_91d_pct",
        "tbill_182d_pct",
        "tbill_364d_pct",
        "fx_kes_usd",
        "fx_reserves_usd_m",
    }
)

# tenor label -> (tenor in years, yield on 2026-09-30). The bond yields are the 364-day bill
# (16.84) plus the seeded tenor spreads, with no noise on the anchor date.
TBILL_CURVE: Final[Mapping[str, tuple[str, str]]] = {
    "91D": ("0.25", "15.72"),
    "182D": ("0.50", "16.58"),
    "364D": ("1.00", "16.84"),
}
BOND_CURVE: Final[Mapping[str, tuple[str, str]]] = {
    "2Y": ("2", "17.04"),
    "5Y": ("5", "17.29"),
    "10Y": ("10", "17.54"),
    "15Y": ("15", "17.74"),
    "20Y": ("20", "17.89"),
    "25Y": ("25", "17.99"),
}

PLANTED_VOLUME_SPIKES: Final[frozenset[tuple[str, str]]] = frozenset(
    {("SCOM", "2026-09-15"), ("KCB", "2026-09-24"), ("EABL", "2026-09-29")}
)
PLANTED_MIN_Z: Final[Decimal] = Decimal("4.00")

HISTORICAL_CYCLES: Final[tuple[str, ...]] = ("2013 Cycle", "2017 Cycle", "2022 Cycle")
FUTURE_CYCLE: Final[str] = "2027 Election Watch"

HUNDRED: Final[Decimal] = Decimal(100)
HALF_CENT: Final[Decimal] = Decimal("0.005")
"""The most a value moves when it is rounded once to 2 dp, as every response value is."""

PROJECTED_LEVELS: Final[tuple[str, ...]] = ("price_index", "turnover_index", "pe_index")
"""The normalized levels a cycle projection averages, in the order its points carry them."""

RISK_TITLES: Final[tuple[str, ...]] = ("Ownership & Motive", "Growth Financing", "Valuation Floor")
SENSITIVITY_AXES: Final[frozenset[str]] = frozenset(
    {"sensitivity_wacc_pct", "sensitivity_margin_pct"}
)

# Line item of the PDF response -> terminal_fundamentals column it is committed to.
ITEM_COLUMNS: Final[Mapping[str, str]] = {
    "revenue": "revenue_kes",
    "ebitda": "ebitda_kes",
    "net_income": "net_income_kes",
    "total_debt": "total_debt_kes",
    "operating_cash_flow": "operating_cash_flow_kes",
    "shares_outstanding": "shares_outstanding",
}

CORE_WATCHLIST: Final[str] = "NSE Core 8"
CORE_WATCHLIST_TICKERS: Final[list[str]] = sorted(SEEDED_TICKERS)

JsonObject = dict[str, Any]
CommittedQuery = Callable[[Executable], Awaitable[list[RowMapping]]]
"""The ``committed_query`` fixture: ``(statement, /) -> rows`` on a separate connection."""


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------


def dec(value: object) -> Decimal:
    """Return ``value`` as a ``Decimal``, asserting that it arrived as a JSON string."""
    assert isinstance(value, str), f"expected a decimal string, got {value!r}"
    return Decimal(value)


def is_json_int(value: object) -> bool:
    """Whether ``value`` is a JSON integer (``bool`` is an ``int`` subclass and is refused)."""
    return type(value) is int


def earliest_min(points: list[JsonObject], key: str) -> JsonObject | None:
    """The first point, in returned order, holding the minimum non-null ``key``; ``None`` if none.

    Points come back ordered by ``offset_days``, so "first" is the earliest session, which is the
    tie-break the backtester's markers use.
    """
    best: JsonObject | None = None
    best_value: Decimal | None = None
    for point in points:
        raw = point[key]
        if raw is None:
            continue
        value = dec(raw)
        if best_value is None or value < best_value:
            best, best_value = point, value
    return best


def assert_marker(marker: JsonObject | None, expected: JsonObject | None, metric: str) -> None:
    """Assert ``marker`` designates ``expected`` (a returned point) on ``metric``."""
    assert expected is not None, f"no point carries {metric}"
    assert marker is not None
    assert marker["metric"] == metric
    assert marker["offset_days"] == expected["offset_days"]
    assert marker["trade_date"] == expected["trade_date"]
    assert dec(marker["value"]) == dec(expected[metric])


async def get_json(
    client: httpx.AsyncClient, path: str, params: Mapping[str, Any] | None = None
) -> Any:
    """``GET BASE + path``, asserting HTTP 200, and return the decoded body."""
    response = await client.get(f"{BASE}{path}", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def by_key(items: list[JsonObject], key: str) -> dict[Any, JsonObject]:
    """Index ``items`` by ``key``, asserting the key is unique."""
    indexed = {item[key]: item for item in items}
    assert len(indexed) == len(items), f"duplicate {key} values"
    return indexed


async def fetch_one(session: AsyncSession, statement: Executable) -> RowMapping:
    """Run ``statement`` (values bound inside it) on ``session`` and return its single row."""
    result = await session.execute(statement)
    return result.mappings().one()


def date_of(value: str) -> dt.date:
    """Parse an ISO ``YYYY-MM-DD`` response date."""
    return dt.date.fromisoformat(value)


def level_of(point: JsonObject, key: str) -> Decimal | None:
    """``point[key]`` as a ``Decimal``, or ``None`` when the response carries null."""
    raw = point[key]
    return None if raw is None else dec(raw)


Bounds = tuple[Decimal, Decimal]


def assert_rounded_within(value: object, bounds: Bounds | None, name: str) -> None:
    """``value`` is null exactly when ``bounds`` is, and otherwise rounds a figure inside them.

    A response value is its unrounded figure rounded once to 2 dp, so it may lie up to
    :data:`HALF_CENT` outside the range the unrounded figure is known to occupy.
    """
    if bounds is None:
        assert value is None, f"{name}: expected null, got {value!r}"
        return
    low, high = bounds
    assert value is not None, f"{name}: expected a value in [{low}, {high}], got null"
    assert low - HALF_CENT <= dec(value) <= high + HALF_CENT, (name, value, low, high)


Track = tuple[int, int, Sequence[int], Sequence[Sequence[Decimal | None]]]
"""One complete cycle for a projection: the first and last calendar offsets of its window, its
points' ``offset_days`` (ascending) and each point's :data:`PROJECTED_LEVELS`."""


def mean_paths(
    first: int, last: int, tracks: Sequence[Track]
) -> list[tuple[int, list[Decimal | None]]]:
    """The mean of each projected level across ``tracks`` at every offset from ``first`` to
    ``last``, as ``(offset, [price_index, turnover_index, pe_index])``.

    At offset o a track contributes its last point with ``offset_days <= o``, only when o lies
    inside its own window, and only that point's non-null levels; a level no track defines at o
    is ``None``, and an offset where every level is undefined is left out. ``statistics.mean``
    keeps ``Decimal`` inputs ``Decimal``.
    """
    means: list[tuple[int, list[Decimal | None]]] = []
    for offset in range(first, last + 1):
        columns: list[list[Decimal]] = [[] for _ in PROJECTED_LEVELS]
        for start, end, offsets, levels in tracks:
            position = bisect_right(offsets, offset)
            if not start <= offset <= end or position == 0:
                continue
            for column, level in zip(columns, levels[position - 1], strict=True):
                if level is not None:
                    column.append(level)
        row: list[Decimal | None] = [mean(column) if column else None for column in columns]
        if any(level is not None for level in row):
            means.append((offset, row))
    return means


# ---------------------------------------------------------------------------------------------
# Health and macro
# ---------------------------------------------------------------------------------------------


async def test_health_reports_ok_database_and_package_version(client: httpx.AsyncClient) -> None:
    body = await get_json(client, "/health")

    assert body == {"status": "ok", "database": "ok", "version": __version__}


async def test_macro_overview_returns_seeded_indicators_and_yield_curve(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/macro/overview")

    latest = by_key(body["latest"], "key")
    assert set(latest) == INDICATOR_KEYS
    for snapshot in latest.values():
        assert snapshot["as_of"] == ANCHOR
        dec(snapshot["value"])
    assert latest["inflation_yoy_pct"]["value"] == "6.80"
    assert latest["inflation_yoy_pct"]["previous_value"] == "6.60"
    assert latest["cbr_rate_pct"]["value"] == "8.75"

    current = by_key(body["yield_curve"]["current"], "tenor_label")
    assert len(current) == len(TBILL_CURVE) + len(BOND_CURVE) == 9
    for label, (tenor_years, yield_pct) in TBILL_CURVE.items():
        point = current[label]
        assert point["instrument"] == "tbill"
        assert dec(point["tenor_years"]) == Decimal(tenor_years)
        assert point["yield_pct"] == yield_pct
        assert point["observation_date"] == ANCHOR
    for label, (tenor_years, yield_pct) in BOND_CURVE.items():
        point = current[label]
        assert point["instrument"] == "bond"
        assert point["source"] == "seed"
        assert point["bond_code"].startswith("FXD-SEED-")
        assert dec(point["tenor_years"]) == Decimal(tenor_years)
        assert point["yield_pct"] == yield_pct
        assert point["observation_date"] == ANCHOR
    history = body["yield_curve"]["history"]
    assert history
    assert history[-1]["as_of"] == ANCHOR

    series = body["inflation_series"]
    assert len(series) == 24
    assert series[0]["date"] == "2024-10-31"
    assert series[-1] == {"date": ANCHOR, "value": "6.80"}
    assert body["coverage"]["requested_start"] == "2024-10-31"
    assert body["coverage"]["is_partial_coverage"] is False
    assert body["rates_series"]


async def test_macro_overview_240_months_reports_partial_coverage_from_first_seeded_month(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/macro/overview", {"history_months": 240})

    assert body["coverage"] == {
        "requested_start": "2006-10-31",
        "coverage_start": "2012-01-31",
        "is_partial_coverage": True,
    }
    assert body["inflation_series"][0]["date"] == "2012-01-31"


# ---------------------------------------------------------------------------------------------
# Market
# ---------------------------------------------------------------------------------------------


async def test_screener_lists_the_eight_seeded_counters_with_the_bank_ev_policy(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/market/screener")

    assert body["as_of"] == ANCHOR
    assert body["z_window"] == 20
    assert body["count"] == len(body["rows"]) == 8
    rows = by_key(body["rows"], "ticker")
    assert set(rows) == SEEDED_TICKERS
    assert rows["SCOM"]["close_price"] == "33.25"
    for ticker, row in rows.items():
        assert row["currency"] == "KES"
        assert row["trade_date"] == ANCHOR
        assert is_json_int(row["volume"])
        assert dec(row["market_cap_kes"]) > 0
        assert dec(row["pe"]) > 0
        if ticker in BANKS:
            assert row["sector"] == "Banking"
            assert row["ev_ebitda"] is None
            assert "ev_not_meaningful_for_banks" in row["flags"]
        else:
            assert dec(row["ev_ebitda"]) > 0
            assert "ev_not_meaningful_for_banks" not in row["flags"]

    aggregates = by_key(body["sector_aggregates"], "sector")
    assert set(aggregates) == SECTORS
    assert sum(aggregate["constituent_count"] for aggregate in aggregates.values()) == 8
    banking = aggregates["Banking"]
    assert banking["constituent_count"] == 5
    assert banking["ev_ebitda"] is None
    assert "ev_not_meaningful_for_banks" in banking["flags"]
    assert dec(banking["pe"]) > 0
    assert banking["pe_eligible_count"] == 5


async def test_screener_anomalies_only_includes_the_planted_eabl_volume_spike(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(
        client, "/market/screener", {"anomalies_only": "true", "as_of": "2026-09-29"}
    )

    assert body["as_of"] == "2026-09-29"
    assert body["rows"]
    rows = by_key(body["rows"], "ticker")
    eabl = rows["EABL"]
    assert eabl["trade_date"] == "2026-09-29"
    assert eabl["is_volume_anomaly"] is True
    assert dec(eabl["volume_z"]) >= PLANTED_MIN_Z
    for row in rows.values():
        assert row["is_volume_anomaly"] or row["is_turnover_anomaly"]


async def test_screener_sector_keeps_its_constituents_and_unknown_sector_is_404(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/market/screener", {"sector": "Banking"})

    assert body["count"] == len(body["rows"]) == 5
    assert {row["ticker"] for row in body["rows"]} == BANKS

    response = await client.get(f"{BASE}/market/screener", params={"sector": "Agriculture"})
    assert response.status_code == 404, response.text
    assert response.json()["detail"]["code"] == "unknown_sector"
    assert response.json()["detail"]["field"] == "sector"


@pytest.mark.parametrize("sector", ["\x00", "Ban\x00king"], ids=["nul", "nul-inside-name"])
async def test_screener_sector_with_nul_is_the_standard_422(
    client: httpx.AsyncClient, sector: str
) -> None:
    """PostgreSQL text cannot hold a NUL character, so validation refuses one with FastAPI's
    standard list before the sector lookup queries the database."""
    response = await client.get(f"{BASE}/market/screener", params={"sector": sector})

    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list), detail
    assert [(entry["loc"], entry["type"], entry["input"]) for entry in detail] == [
        (["query", "sector"], "string_pattern_mismatch", sector)
    ]


@pytest.mark.parametrize(
    ("path", "parameter", "value"),
    [
        pytest.param("/market/screener", "min_pe", "\u0665", id="screener-min_pe-arabic-indic"),
        pytest.param("/market/screener", "max_pb", "\uff12", id="screener-max_pb-full-width"),
        pytest.param("/market/screener", "min_dividend_yield", " 5", id="screener-dy-padded"),
        pytest.param("/market/screener", "min_volume_z", "1_0", id="screener-volume_z-underscore"),
        pytest.param(
            "/market/screener", "anomaly_threshold", "\u0663", id="screener-threshold-arabic-indic"
        ),
        pytest.param("/market/anomalies", "threshold", "\u0663", id="anomalies-threshold"),
    ],
)
async def test_non_canonical_decimal_query_is_the_standard_422(
    client: httpx.AsyncClient, path: str, parameter: str, value: str
) -> None:
    """A decimal query parameter must be ASCII decimal text: non-ASCII digits, whitespace and
    ``_`` separators, which ``Decimal()`` alone would read, are FastAPI's ``decimal_parsing``."""
    response = await client.get(f"{BASE}{path}", params={parameter: value})

    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": [
            {
                "type": "decimal_parsing",
                "loc": ["query", parameter],
                "msg": "Input should be a valid decimal",
                "input": value,
            }
        ]
    }


async def test_sector_heatmap_covers_four_sectors_over_twelve_month_ends(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/market/sector-heatmap")

    assert body["interval"] == "month"
    period_ends = body["period_ends"]
    assert len(period_ends) == 12
    assert period_ends == sorted(period_ends)
    assert period_ends[-1] == ANCHOR

    assert [cell["period_end"] for cell in body["market"]] == period_ends
    for cell in body["market"]:
        dec(cell["return_pct"])
        assert cell["relative_return_pct"] is None
        assert cell["constituents"] == 8

    sectors = by_key(body["sectors"], "sector")
    assert set(sectors) == SECTORS
    for row in sectors.values():
        assert [cell["period_end"] for cell in row["cells"]] == period_ends
        for cell, market_cell in zip(row["cells"], body["market"], strict=True):
            # The two returns and their difference are each rounded once from unrounded values,
            # so the difference of the rounded returns may differ from the relative by 0.01.
            relative = dec(cell["relative_return_pct"])
            difference = dec(cell["return_pct"]) - dec(market_cell["return_pct"])
            assert abs(relative - difference) <= Decimal("0.01")


@pytest.mark.parametrize(
    ("params", "as_of", "interval", "period_ends"),
    [
        pytest.param(
            {"interval": "week", "periods": 4},
            ANCHOR,
            "week",
            ["2026-09-11", "2026-09-18", "2026-09-25", ANCHOR],
            id="four-weeks",
        ),
        pytest.param(
            {"periods": 3, "as_of": "2026-06-17"},
            "2026-06-17",
            "month",
            ["2026-04-30", "2026-05-29", "2026-06-17"],
            id="three-months-to-mid-june",
        ),
        pytest.param(
            {"interval": "week", "periods": 3, "as_of": "2026-06-17"},
            "2026-06-17",
            "week",
            ["2026-06-05", "2026-06-12", "2026-06-17"],
            id="three-weeks-to-mid-june",
        ),
        pytest.param({"periods": 1}, ANCHOR, "month", [ANCHOR], id="one-month"),
    ],
)
async def test_sector_heatmap_serves_the_requested_interval_periods_and_as_of(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    as_of: str,
    interval: str,
    period_ends: list[str],
) -> None:
    """``interval``, ``periods`` and ``as_of`` reach the service. Boundaries are the last session
    on or before each Friday (week) or month-end (month), the latest being ``as_of`` itself
    (Wednesday 2026-09-30 or 2026-06-17), so a weekly heatmap or one ending mid-June differs from
    the default 12 month-ends to 2026-09-30. The market row (all 8 counters) and each of the 4
    sector rows hold one cell per period, and each cell is the one the same request over 12
    periods gives for that period: fewer periods trim the history, never move a boundary."""
    body = await get_json(client, "/market/sector-heatmap", params)

    assert (body["as_of"], body["interval"], body["period_ends"]) == (as_of, interval, period_ends)
    count = len(period_ends)
    assert [cell["period_end"] for cell in body["market"]] == period_ends
    assert [cell["constituents"] for cell in body["market"]] == [8] * count
    sectors = by_key(body["sectors"], "sector")
    assert set(sectors) == SECTORS
    for row in sectors.values():
        assert [cell["period_end"] for cell in row["cells"]] == period_ends

    longer = await get_json(client, "/market/sector-heatmap", {**params, "periods": 12})
    assert len(longer["period_ends"]) == 12
    assert longer["period_ends"][-count:] == period_ends
    assert longer["market"][-count:] == body["market"]
    longer_sectors = by_key(longer["sectors"], "sector")
    for name, row in sectors.items():
        assert longer_sectors[name]["cells"][-count:] == row["cells"]


@pytest.mark.parametrize(
    ("params", "subject", "subject_type"),
    [
        ({"ticker": "SCOM"}, "SCOM", "ticker"),
        ({"sector": "Banking"}, "Banking", "sector"),
    ],
    ids=["ticker-SCOM", "sector-Banking"],
)
async def test_valuation_bands_are_ok_over_the_default_five_years(
    client: httpx.AsyncClient, params: dict[str, str], subject: str, subject_type: str
) -> None:
    body = await get_json(client, "/market/valuation-bands", params)

    assert body["subject"] == subject
    assert body["subject_type"] == subject_type
    assert body["metric"] == "pe"
    assert body["as_of"] == ANCHOR
    assert body["status"] == "ok"
    assert body["coverage"]["requested_start"] == "2021-09-30"
    assert body["coverage"]["is_partial_coverage"] is False
    assert len(body["series"]) >= 250

    stats = body["stats"]
    assert stats is not None
    assert is_json_int(stats["observations"])
    assert stats["observations"] >= 60
    bands = [dec(stats[key]) for key in ("minus_2sd", "minus_1sd", "mean", "plus_1sd", "plus_2sd")]
    assert bands == sorted(bands)
    spread = [dec(stats[key]) for key in ("min", "p25", "median", "p75", "max")]
    assert spread == sorted(spread)
    assert dec(stats["current"]) == dec(body["series"][-1]["value"])


@pytest.mark.parametrize(
    ("params", "scope", "coverage", "stats", "last_point"),
    [
        pytest.param(
            {"ticker": "scom", "metric": "pb", "lookback_years": 15},
            ("SCOM", "ticker", "pb", ANCHOR, "ok", []),
            ("2011-09-30", "2012-01-02", True),
            (3762, "4.50"),
            {"date": ANCHOR, "value": "4.50"},
            id="ticker-pb-fifteen-years",
        ),
        pytest.param(
            {"ticker": "KCB", "lookback_years": 1, "as_of": "2012-02-15"},
            ("KCB", "ticker", "pe", "2012-02-15", "insufficient_history", ["insufficient_history"]),
            ("2011-02-15", "2012-01-02", True),
            None,
            {"date": "2012-02-15", "value": "6.48"},
            id="ticker-one-year-to-2012",
        ),
        pytest.param(
            {"sector": "Banking", "metric": "pb", "lookback_years": 2, "as_of": "2026-06-30"},
            ("Banking", "sector", "pb", "2026-06-30", "ok", []),
            ("2024-06-30", "2024-07-01", False),
            (508, "0.85"),
            {"date": "2026-06-30", "value": "0.85"},
            id="sector-pb-two-years-to-june",
        ),
    ],
)
async def test_valuation_bands_serve_the_requested_metric_lookback_and_as_of(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    scope: tuple[str, str, str, str, str, list[str]],
    coverage: tuple[str, str, bool],
    stats: tuple[int, str] | None,
    last_point: JsonObject,
) -> None:
    """``ticker`` (normalised) or ``sector``, ``metric``, ``lookback_years`` and ``as_of`` reach
    the service: ``requested_start`` is ``as_of`` minus the lookback and the weekly series ends on
    ``as_of``. Fifteen years of SCOM P/B start before the history on 2012-01-02, so coverage is
    partial over all 3,762 sessions, current 4.50 (P/E would be 14.00). One year of KCB P/E to
    2012-02-15 holds too few sessions for statistics. Two years of Banking P/B to 2026-06-30 start
    on Monday 2024-07-01, within 7 days of the Sunday requested start, over 508 sessions."""
    body = await get_json(client, "/market/valuation-bands", params)

    assert (
        body["subject"],
        body["subject_type"],
        body["metric"],
        body["as_of"],
        body["status"],
        body["flags"],
    ) == scope
    requested_start, coverage_start, is_partial = coverage
    assert body["coverage"] == {
        "requested_start": requested_start,
        "coverage_start": coverage_start,
        "is_partial_coverage": is_partial,
    }
    assert body["series"][-1] == last_point
    assert date_of(body["series"][0]["date"]) >= date_of(coverage_start)
    if stats is None:
        assert body["stats"] is None
    else:
        observations, current = stats
        assert body["stats"]["observations"] == observations
        assert body["stats"]["current"] == current == last_point["value"]


async def test_anomalies_include_the_three_planted_volume_spikes_in_feed_order(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/market/anomalies")

    assert body["window"] == 20
    threshold = dec(body["threshold"])
    assert threshold == Decimal("2")
    items = body["items"]
    for item in items:
        assert dec(item["z_score"]) >= threshold
        assert is_json_int(item["observations_used"])

    # Inclusion, never an exact count: background noise may add further crossings of 2.00.
    volume_z = {
        (item["ticker"], item["trade_date"]): dec(item["z_score"])
        for item in items
        if item["metric"] == "volume"
    }
    for spike in PLANTED_VOLUME_SPIKES:
        assert spike in volume_z, f"planted spike {spike} missing"
        assert volume_z[spike] >= PLANTED_MIN_Z

    order = [(date_of(item["trade_date"]), dec(item["z_score"])) for item in items]
    for earlier, later in zip(order, order[1:], strict=False):
        assert earlier[0] > later[0] or (earlier[0] == later[0] and earlier[1] >= later[1])


# (ticker, trade_date, metric, z_score, observations_used) of a feed item.
AnomalyRow = tuple[str, str, str, str, int]
BOTH_METRICS: Final[frozenset[str]] = frozenset({"volume", "turnover"})


@pytest.mark.parametrize(
    ("params", "window", "threshold", "tickers", "metrics", "first_day", "included"),
    [
        pytest.param(
            {"ticker": "kcb", "metric": "volume", "threshold": "4"},
            20,
            "4",
            frozenset({"KCB"}),
            frozenset({"volume"}),
            date(2026, 9, 3),
            [("KCB", "2026-09-24", "volume", "5.10", 20)],
            id="ticker-volume-threshold-4",
        ),
        pytest.param(
            {"sector": "Banking", "metric": "turnover", "threshold": "5"},
            20,
            "5",
            BANKS,
            frozenset({"turnover"}),
            date(2026, 9, 3),
            [
                ("KCB", "2026-09-24", "turnover", "5.39", 20),
                ("COOP", "2026-09-11", "turnover", "5.31", 20),
            ],
            id="sector-turnover-threshold-5",
        ),
        pytest.param(
            {"ticker": "KCB", "lookback_sessions": 12},
            20,
            "2.0",
            frozenset({"KCB"}),
            BOTH_METRICS,
            date(2026, 9, 15),
            [
                ("KCB", "2026-09-24", "turnover", "5.39", 20),
                ("KCB", "2026-09-24", "volume", "5.10", 20),
            ],
            id="twelve-sessions",
        ),
        pytest.param(
            {"ticker": "SCOM", "lookback_sessions": 11},
            20,
            "2.0",
            frozenset({"SCOM"}),
            BOTH_METRICS,
            date(2026, 9, 16),
            [],
            id="eleven-sessions-start-after-the-spike",
        ),
        pytest.param(
            {"ticker": "SCOM", "window": 10, "lookback_sessions": 12},
            10,
            "2.0",
            frozenset({"SCOM"}),
            BOTH_METRICS,
            date(2026, 9, 15),
            [
                ("SCOM", "2026-09-15", "turnover", "3.20", 10),
                ("SCOM", "2026-09-15", "volume", "3.08", 10),
            ],
            id="ten-row-window",
        ),
        pytest.param(
            {"ticker": "SCOM", "sector": "Banking"},
            20,
            "2.0",
            frozenset(),
            BOTH_METRICS,
            date(2026, 9, 3),
            [],
            id="ticker-outside-the-sector",
        ),
    ],
)
async def test_anomaly_feed_serves_the_requested_metric_window_threshold_lookback_and_subject(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    window: int,
    threshold: str,
    tickers: frozenset[str],
    metrics: frozenset[str],
    first_day: date,
    included: list[AnomalyRow],
) -> None:
    """``metric``, ``window``, ``threshold``, ``lookback_sessions``, ``ticker`` (normalised) and
    ``sector`` reach the service, and ``window`` and ``threshold`` are echoed as given.

    The feed is checked by scope and inclusion, never by count, since background noise may add
    crossings: every item is a candidate ticker's (the ticker, the sector's members, or none for a
    ticker outside the sector), of a requested metric, at a Z-score at or above the threshold,
    dated from the first of the last ``lookback_sessions`` sessions (20 from 2026-09-03, 12 from
    2026-09-15, 11 from 2026-09-16) to 2026-09-30, and scored over at most ``window`` rows; items
    run by date, then Z-score, both descending; and the listed items appear in that order.

    Dropping an argument brings in a seeded crossing outside the requested scope. ``metric``: KCB's
    2026-09-24 turnover (Z 5.39) beside its volume, and COOP's 2026-09-11 volume (5.60) beside
    the Banking turnover. ``threshold``: KCB's 2026-09-09 volume (3.40) under 4, and EQTY's
    2026-09-25 turnover (2.10) under 5. ``lookback_sessions``: KCB's 2026-09-09 spikes before the
    12 sessions, SCOM's 2026-09-15 spikes (4.16, 4.00) before the 11 sessions, and SCOM's
    2026-09-04 turnover (2.12 over 10 rows) before the 12. ``window``: SCOM's 2026-09-15 spikes
    over 20 rows rather than 10 (3.20, 3.08), with 20 echoed. ``ticker``: EABL's 2026-09-29
    volume (5.90) beside KCB's, and the Banking members' spikes, such as KCB's 2026-09-24, into
    SCOM within Banking. ``sector``: EABL's 2026-09-29 (6.17) and KEGN's 2026-09-16 (5.73)
    turnover beside the Banking members', and SCOM's 2026-09-15 spikes into SCOM within Banking,
    whose candidate set is empty because SCOM is not a Banking member."""
    body = await get_json(client, "/market/anomalies", params)

    assert (body["window"], body["threshold"]) == (window, threshold)
    rows: list[AnomalyRow] = []
    for item in body["items"]:
        assert item["ticker"] in tickers, item
        assert item["metric"] in metrics, item
        assert dec(item["z_score"]) >= Decimal(threshold), item
        assert first_day <= date_of(item["trade_date"]) <= date_of(ANCHOR), item
        assert is_json_int(item["observations_used"]), item
        assert item["observations_used"] <= window, item
        rows.append(
            (
                item["ticker"],
                item["trade_date"],
                item["metric"],
                item["z_score"],
                item["observations_used"],
            )
        )
    order = [(date_of(trade_date), dec(z_score)) for _, trade_date, _, z_score, _ in rows]
    assert order == sorted(order, reverse=True)
    assert [row for row in rows if row in included] == included


async def test_flows_for_scom_cover_the_trailing_year_with_local_mirroring_foreign(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/market/flows", {"ticker": "SCOM"})

    assert body["subject"] == "SCOM"
    assert body["as_of"] == body["end"] == ANCHOR
    assert body["start"] == "2025-09-30"
    assert body["status"] == "ok"
    assert body["coverage_pct"] == "100.00"

    totals = body["totals"]
    assert totals is not None
    assert dec(totals["local_net_kes"]) == -dec(totals["foreign_net_kes"])
    assert totals["sessions_with_data"] == totals["sessions_in_range"] > 0

    series = body["series"]
    assert len(series) == totals["sessions_in_range"]
    assert date_of(series[0]["trade_date"]) >= date_of(body["start"])
    assert series[-1]["trade_date"] == ANCHOR
    for point in series:
        assert point["foreign_net_kes"] is not None
        assert dec(point["local_net_kes"]) == -dec(point["foreign_net_kes"])


@pytest.mark.parametrize(
    ("params", "subject", "dates", "sessions", "rolling_window", "foreign_total"),
    [
        pytest.param(
            {"ticker": "KCB", "start": "2026-09-28", "end": ANCHOR, "rolling_window": 5},
            "KCB",
            (ANCHOR, "2026-09-28", ANCHOR, "2026-09-28"),
            3,
            5,
            "-2513051.82",
            id="explicit-range",
        ),
        pytest.param(
            {"ticker": "EABL", "as_of": "2026-06-30"},
            "EABL",
            ("2026-06-30", "2025-06-30", "2026-06-30", "2025-06-30"),
            254,
            20,
            "1424333.20",
            id="as-of",
        ),
        pytest.param(
            {"ticker": "EABL", "end": "2026-06-30"},
            "EABL",
            (ANCHOR, "2025-06-30", "2026-06-30", "2025-06-30"),
            254,
            20,
            "1424333.20",
            id="end",
        ),
        pytest.param(
            {"ticker": "EABL", "start": "2026-06-01"},
            "EABL",
            (ANCHOR, "2026-06-01", ANCHOR, "2026-06-02"),
            87,
            20,
            "653487.05",
            id="start",
        ),
        pytest.param(
            {"sector": "Banking", "rolling_window": 5},
            "Banking",
            (ANCHOR, "2025-09-30", ANCHOR, "2025-09-30"),
            254,
            5,
            "72937052.59",
            id="sector-five-session-rolling-sum",
        ),
    ],
)
async def test_flows_serve_the_requested_subject_range_and_rolling_window(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    subject: str,
    dates: tuple[str, str, str, str],
    sessions: int,
    rolling_window: int,
    foreign_total: str,
) -> None:
    """``ticker`` or ``sector``, ``as_of``, ``start``, ``end`` and ``rolling_window`` reach the
    service. ``dates`` is ``(as_of, start, end, first session)``: ``end`` defaults to ``as_of``
    and ``start`` to 365 days before ``end``, and a given bound is served as given (2026-06-01 is
    not a session, so the series opens on 2026-06-02). On every session local = -foreign, the
    cumulative flow is the running foreign sum and the rolling flow is the sum of the last
    ``rolling_window`` sessions from the ``rolling_window``-th session on (the 20th by default),
    each an exact sum of 2 dp amounts; every seeded row has a flow, so coverage is complete and
    the totals are the last cumulative flow and its negation."""
    body = await get_json(client, "/market/flows", params)

    as_of, start, end, first = dates
    assert (body["subject"], body["as_of"], body["start"], body["end"]) == (
        subject,
        as_of,
        start,
        end,
    )
    assert (body["status"], body["flags"], body["coverage_pct"]) == ("ok", [], "100.00")
    series = body["series"]
    assert len(series) == sessions
    assert (series[0]["trade_date"], series[-1]["trade_date"]) == (first, end)
    foreign = [dec(point["foreign_net_kes"]) for point in series]
    running = Decimal(0)
    for index, point in enumerate(series):
        running += foreign[index]
        assert dec(point["local_net_kes"]) == -foreign[index]
        assert dec(point["cumulative_foreign_net_kes"]) == running
        if index < rolling_window - 1:
            assert point["rolling_foreign_net_kes"] is None
        else:
            window_sum = sum(foreign[index - rolling_window + 1 : index + 1], Decimal(0))
            assert dec(point["rolling_foreign_net_kes"]) == window_sum
    totals = body["totals"]
    assert (
        totals["foreign_net_kes"],
        totals["local_net_kes"],
        totals["sessions_with_data"],
        totals["sessions_in_range"],
    ) == (foreign_total, str(-Decimal(foreign_total)), sessions, sessions)
    assert running == Decimal(foreign_total)


# ---------------------------------------------------------------------------------------------
# Election and event cycles
# ---------------------------------------------------------------------------------------------


def window_span(cycle: JsonObject) -> tuple[int, int]:
    """The calendar offsets of ``cycle``'s window start and end from its event date."""
    event = date_of(cycle["event_date"])
    return (
        (date_of(cycle["window_start"]) - event).days,
        (date_of(cycle["window_end"]) - event).days,
    )


def returned_track(cycle: JsonObject) -> Track:
    """``cycle``'s window and returned points as a projection :data:`Track`."""
    first, last = window_span(cycle)
    points = cycle["points"]
    return (
        first,
        last,
        [point["offset_days"] for point in points],
        [[level_of(point, key) for key in PROJECTED_LEVELS] for point in points],
    )


def assert_summary_from_points(cycle: JsonObject) -> None:
    """``cycle``'s summary is the summary rule recomputed from its own returned points.

    "Index at T" is the price index of the last point on or before T. The pre-event return is
    (index at T / 100 - 1) x 100; the post-event return (last index / index at T - 1) x 100, null
    with no point after T; the maximum drawdown the minimum over the points of (index / running
    maximum - 1) x 100; the P/E compression (P/E at the valuation floor / anchor P/E - 1) x 100,
    which is the P/E index at the floor minus 100, null when the floor fell back to the price
    index or the P/E index is null. A figure is null wherever its input level is.

    The service computes each figure from unrounded levels and rounds it once. Each returned
    level is within :data:`HALF_CENT` of its unrounded level and every formula is monotonic in
    each level, so the unrounded figure lies between the formula's values at the ends of those
    ranges, and the returned figure within a further half cent of it. The floor is the first
    point with the minimum returned P/E, as the response defines it.
    """
    summary = cycle["summary"]
    assert summary is not None, cycle["label"]
    points = cycle["points"]
    event = date_of(cycle["event_date"])
    at_or_before = [point for point in points if date_of(point["trade_date"]) <= event]
    index_at_event = level_of(at_or_before[-1], "price_index") if at_or_before else None
    last_index = level_of(points[-1], "price_index")

    pre_event: Bounds | None = None
    post_event: Bounds | None = None
    if index_at_event is not None:
        pre_event = (index_at_event - HALF_CENT - HUNDRED, index_at_event + HALF_CENT - HUNDRED)
        if last_index is not None and date_of(points[-1]["trade_date"]) > event:
            post_event = (
                ((last_index - HALF_CENT) / (index_at_event + HALF_CENT) - 1) * HUNDRED,
                ((last_index + HALF_CENT) / (index_at_event - HALF_CENT) - 1) * HUNDRED,
            )

    lows: list[Decimal] = []
    highs: list[Decimal] = []
    running_max: Decimal | None = None
    for point in points:
        level = level_of(point, "price_index")
        if level is None:
            continue
        running_max = level if running_max is None else max(running_max, level)
        lows.append(((level - HALF_CENT) / (running_max + HALF_CENT) - 1) * HUNDRED)
        # An index never exceeds its own running maximum, whatever the rounding.
        ratio = min(Decimal(1), (level + HALF_CENT) / (running_max - HALF_CENT))
        highs.append((ratio - 1) * HUNDRED)
    drawdown: Bounds | None = (min(lows), min(highs)) if lows else None

    floor = earliest_min(points, "pe_ratio")
    floor_index = None if floor is None else level_of(floor, "pe_index")
    compression: Bounds | None = (
        None
        if floor_index is None
        else (floor_index - HALF_CENT - HUNDRED, floor_index + HALF_CENT - HUNDRED)
    )

    label = cycle["label"]
    assert_rounded_within(summary["pre_event_return_pct"], pre_event, f"{label} pre-event")
    assert_rounded_within(summary["post_event_return_pct"], post_event, f"{label} post-event")
    assert_rounded_within(summary["max_drawdown_pct"], drawdown, f"{label} drawdown")
    assert_rounded_within(summary["pe_compression_pct"], compression, f"{label} compression")


def assert_complete_cycle(cycle: JsonObject) -> None:
    """A completed historical cycle: points, a summary recomputed from the points, and markers
    recomputable from the points."""
    assert cycle["status"] == "complete", cycle["label"]
    points = cycle["points"]
    assert points
    offsets = [point["offset_days"] for point in points]
    assert all(is_json_int(offset) for offset in offsets)
    assert offsets == sorted(offsets)
    assert cycle["summary"] is not None
    assert_summary_from_points(cycle)

    markers = cycle["markers"]
    assert markers is not None
    floor_point = earliest_min(points, "pe_ratio")
    floor_metric = "pe_ratio"
    if floor_point is None:
        floor_point = earliest_min(points, "price_index")
        floor_metric = "price_index"
    assert_marker(markers["valuation_floor"], floor_point, floor_metric)
    assert_marker(
        markers["volume_exhaustion"], earliest_min(points, "turnover_index"), "turnover_index"
    )


def assert_projection_means(cycle: JsonObject, cycles: Sequence[JsonObject]) -> None:
    """``cycle``'s projection is the mean of the complete ``cycles``' returned paths.

    The basis lists the complete cycles in response order. The projected offsets are exactly the
    offsets of ``cycle``'s window at which a complete cycle defines a level (see
    :func:`mean_paths`), and each projected level is within 0.01 of the mean of the complete
    cycles' returned levels: each returned level is within half a cent of the unrounded level
    the service averages, so their means differ by at most half a cent, and the projection
    rounds the unrounded mean once.
    """
    complete = [other for other in cycles if other["status"] == "complete"]
    projection = cycle["projection"]
    assert projection is not None, cycle["label"]
    assert projection["is_projection"] is True
    assert projection["basis"] == [other["label"] for other in complete]
    first, last = window_span(cycle)
    expected = mean_paths(first, last, [returned_track(other) for other in complete])
    points = projection["points"]
    assert [point["offset_days"] for point in points] == [offset for offset, _ in expected]
    for point, (offset, means) in zip(points, expected, strict=True):
        for key, level in zip(PROJECTED_LEVELS, means, strict=True):
            bounds = None if level is None else (level - HALF_CENT, level + HALF_CENT)
            assert_rounded_within(point[key], bounds, f"{cycle['label']} {key} at {offset}")


def assert_projection_from_history(cycle: JsonObject, cycles: Sequence[JsonObject]) -> None:
    """A labelled projection averaged over the three completed historical cycles of ``cycles``,
    each level recomputed from their returned points."""
    projection = cycle["projection"]
    assert projection is not None
    assert projection["is_projection"] is True
    assert sorted(projection["basis"]) == list(HISTORICAL_CYCLES)
    assert projection["points"]
    for point in projection["points"]:
        assert is_json_int(point["offset_days"])
        if point["price_index"] is not None:
            dec(point["price_index"])
    assert_projection_means(cycle, cycles)


async def test_backtest_default_window_completes_history_and_projects_2027(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/cycles/backtest")

    assert body["window"] == {
        "pre_days": 180,
        "post_days": 180,
        "pre_months": None,
        "post_months": None,
    }
    assert body["data_available_through"] == ANCHOR
    assert set(body["candidates"]) == SEEDED_TICKERS
    cycles = by_key(body["cycles"], "label")
    assert [cycle["label"] for cycle in body["cycles"]] == [*HISTORICAL_CYCLES, FUTURE_CYCLE]

    for label in HISTORICAL_CYCLES:
        assert_complete_cycle(cycles[label])

    future = cycles[FUTURE_CYCLE]
    assert future["status"] == "awaiting_data"
    assert future["event_date"] == "2027-08-10"
    assert future["window_start"] == "2027-02-11"
    assert future["window_end"] == "2028-02-06"
    assert future["points"] == []
    assert future["markers"] is None
    assert future["summary"] is None
    assert is_json_int(future["days_until_window_start"])
    assert future["days_until_window_start"] > 0
    assert future["flags"] == []
    assert_projection_from_history(future, body["cycles"])


async def test_backtest_twelve_month_window_makes_2027_partial_with_observed_points(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/cycles/backtest", {"pre_months": 12, "post_months": 12})

    assert body["window"] == {
        "pre_days": None,
        "post_days": None,
        "pre_months": 12,
        "post_months": 12,
    }
    cycles = by_key(body["cycles"], "label")
    for label in HISTORICAL_CYCLES:
        assert_complete_cycle(cycles[label])

    future = cycles[FUTURE_CYCLE]
    assert future["status"] == "partial"
    assert future["window_start"] == "2026-08-10"
    assert future["window_end"] == "2028-08-10"
    points = future["points"]
    assert points
    assert date_of(points[0]["trade_date"]) >= dt.date(2026, 8, 10)
    assert points[-1]["trade_date"] == ANCHOR
    assert future["summary"] is not None
    # No observed point lies after the event, so there is no post-event return yet.
    assert future["summary"]["post_event_return_pct"] is None
    assert_summary_from_points(future)
    assert future["flags"] == []
    assert_projection_from_history(future, body["cycles"])


# (label, event_date, window_start, window_end, status) of a returned cycle.
CycleRow = tuple[str, str, str, str, str]

DAYS_180: Final[tuple[int | None, ...]] = (180, 180, None, None)
DEFAULT_CYCLE_ROWS: Final[list[CycleRow]] = [
    ("2013 Cycle", "2013-03-04", "2012-09-05", "2013-08-31", "complete"),
    ("2017 Cycle", "2017-08-08", "2017-02-09", "2018-02-04", "complete"),
    ("2022 Cycle", "2022-08-09", "2022-02-10", "2023-02-05", "complete"),
    (FUTURE_CYCLE, "2027-08-10", "2027-02-11", "2028-02-06", "awaiting_data"),
]
"""The four stored events over the default +-180 days."""


@pytest.mark.parametrize(
    ("params", "window", "candidates", "cycles"),
    [
        pytest.param(
            {"event_dates": ["2022-08-09", "2013-03-04"]},
            DAYS_180,
            sorted(SEEDED_TICKERS),
            [DEFAULT_CYCLE_ROWS[0], DEFAULT_CYCLE_ROWS[2]],
            id="repeated-event-dates",
        ),
        pytest.param(
            {"event_dates": "2015-06-15", "pre_days": 30, "post_days": 45},
            (30, 45, None, None),
            sorted(SEEDED_TICKERS),
            [("Event 2015-06-15", "2015-06-15", "2015-05-16", "2015-07-30", "complete")],
            id="unstored-event-day-window",
        ),
        pytest.param(
            {"event_dates": "2022-08-09", "pre_months": 3, "post_months": 6},
            (None, None, 3, 6),
            sorted(SEEDED_TICKERS),
            [("2022 Cycle", "2022-08-09", "2022-05-09", "2023-02-09", "complete")],
            id="month-window",
        ),
        pytest.param(
            {"tickers": ["SCOM", "kcb"]},
            DAYS_180,
            ["KCB", "SCOM"],
            DEFAULT_CYCLE_ROWS,
            id="repeated-tickers",
        ),
        pytest.param(
            {"sector": "Banking"}, DAYS_180, sorted(BANKS), DEFAULT_CYCLE_ROWS, id="sector"
        ),
        pytest.param(
            {"sector": "Banking", "tickers": ["SCOM", "KCB"]},
            DAYS_180,
            ["KCB"],
            DEFAULT_CYCLE_ROWS,
            id="tickers-in-sector",
        ),
        pytest.param(
            {"event_dates": ["2027-08-10", "2013-03-04"]},
            DAYS_180,
            sorted(SEEDED_TICKERS),
            [DEFAULT_CYCLE_ROWS[0], DEFAULT_CYCLE_ROWS[3]],
            id="one-complete-cycle",
        ),
    ],
)
async def test_backtest_serves_the_requested_events_window_and_candidates(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    window: tuple[int | None, ...],
    candidates: list[str],
    cycles: list[CycleRow],
) -> None:
    """``event_dates`` (repeatable, served ascending), ``pre_days``/``post_days`` or
    ``pre_months``/``post_months``, ``tickers`` (repeatable, normalised) and ``sector`` reach
    the service. The window echo carries the form used. A stored date keeps its
    ``terminal_cycles`` label and another is labelled "Event YYYY-MM-DD"; each window is T minus
    and plus the given calendar days or months (2022-08-09 less 3 months is 2022-05-09, plus 6 is
    2023-02-09), and every point's offset ``trade_date - T`` lies inside it. The candidates are
    the tickers, the sector's five banks, or the tickers in the sector (KCB alone); every one
    joins each complete cycle, whose summary and markers are recomputed from its points. A cycle
    awaiting data is projected from at least 2 complete cycles; with 1 it has no projection and
    is flagged ``insufficient_projection_basis``."""
    body = await get_json(client, "/cycles/backtest", params)

    assert body["window"] == dict(
        zip(("pre_days", "post_days", "pre_months", "post_months"), window, strict=True)
    )
    assert (body["candidates"], body["data_available_through"]) == (candidates, ANCHOR)
    assert [
        (
            cycle["label"],
            cycle["event_date"],
            cycle["window_start"],
            cycle["window_end"],
            cycle["status"],
        )
        for cycle in body["cycles"]
    ] == cycles
    complete = [cycle for cycle in body["cycles"] if cycle["status"] == "complete"]
    for cycle in body["cycles"]:
        event = date_of(cycle["event_date"])
        first, last = window_span(cycle)
        for point in cycle["points"]:
            assert point["offset_days"] == (date_of(point["trade_date"]) - event).days
            assert first <= point["offset_days"] <= last
        if cycle["status"] == "complete":
            assert (cycle["constituents"], cycle["excluded_constituents"]) == (candidates, [])
            assert_complete_cycle(cycle)
        elif len(complete) >= 2:
            assert cycle["flags"] == []
            assert_projection_means(cycle, body["cycles"])
        else:
            assert cycle["projection"] is None
            assert cycle["flags"] == ["insufficient_projection_basis"]


# Sessions of the crafted counters below, all seeded weekdays; Monday 2026-06-01 is not one.
FEB_LEAD_IN: Final[list[date]] = [date(2026, 2, day) for day in (5, 6, 9, 10)]
FEB_WINDOW: Final[list[date]] = [date(2026, 2, day) for day in (11, 12, 13, 16, 17, 18, 19, 20)]
JUNE_LEAD_IN: Final[list[date]] = [date(2026, 6, day) for day in (4, 5, 8, 9)]
JUNE_WINDOW: Final[list[date]] = [date(2026, 6, day) for day in (10, 11, 12, 15, 16, 17, 18, 19)]
LATE_LEAD_IN: Final[list[date]] = [date(2026, 6, day) for day in (8, 9, 10, 11, 12)]
LATE_WINDOW: Final[list[date]] = [date(2026, 6, day) for day in (15, 16, 17, 18, 19)]

CYCLE_PATH: Final[tuple[str, ...]] = (
    "10.00",
    "9.50",
    "9.00",
    "9.20",
    "8.80",
    "9.40",
    "9.90",
    "10.50",
)
"""Closes of a crafted cycle's window sessions: price index 100, 95, 90, 92, 88, 94, 99, 105."""


def crafted_closes(lead_in: Sequence[date], window: Sequence[date]) -> dict[date, Decimal]:
    """10.00 on each lead-in session, then :data:`CYCLE_PATH` on the window's sessions in turn."""
    closes = dict.fromkeys(lead_in, Decimal("10.00"))
    closes.update(zip(window, (Decimal(close) for close in CYCLE_PATH), strict=False))
    return closes


async def test_backtest_floor_on_a_rounded_pe_tie_is_the_earlier_session(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """QTIE is added inside this test's rolled-back transaction by ``add_sparse_counter``
    (1,000,000 shares and FY2025 net income 100,000 KES from 2026-03-31, so its P/E is the close
    x 10). It closes 1.2500 on four lead-in sessions from 2026-06-04 and on the anchor 2026-06-10,
    then 1.1000, 1.0004, 1.0500 (T = Monday 2026-06-15), 1.0001, 1.0800, 1.1500 and 1.2000 on
    2026-06-19, the end of the T-5 to T+4 window.

    The raw P/E of 2026-06-12 (10.004) and of 2026-06-16 (10.001) both round to 10.00. The floor
    is the earlier session, the first minimum a client finds in the points, although the later
    raw P/E is lower, and the P/E compression is computed from the floor's own raw P/E:
    (10.004 / 12.5 - 1) x 100 = -19.968, so -19.97, not the later session's -19.99. The price and
    P/E indices are 100 x close / 1.25. The index is 84.00 at T, a pre-event return of -16.00;
    the last is 96.00, a post-event return of (96 / 84 - 1) x 100 = 14.2857, so 14.29; the
    deepest drawdown is 80.008 against the 100.00 anchor, -19.992, so -19.99. Five turnover rows
    by the anchor define the turnover index, so no flag is raised."""
    closes = dict.fromkeys(JUNE_LEAD_IN, Decimal("1.2500"))
    window_closes = ("1.2500", "1.1000", "1.0004", "1.0500", "1.0001", "1.0800", "1.1500", "1.2000")
    closes.update(zip(JUNE_WINDOW, (Decimal(close) for close in window_closes), strict=True))
    await add_sparse_counter(db_session, "QTIE", "QA Tie", closes)

    body = await get_json(
        client,
        "/cycles/backtest",
        {"event_dates": "2026-06-15", "pre_days": 5, "post_days": 4, "tickers": "QTIE"},
    )

    assert (body["candidates"], body["data_available_through"]) == (["QTIE"], "2026-06-19")
    (cycle,) = body["cycles"]
    assert (
        cycle["label"],
        cycle["window_start"],
        cycle["window_end"],
        cycle["status"],
        cycle["anchor_date"],
        cycle["constituents"],
        cycle["flags"],
    ) == ("Event 2026-06-15", "2026-06-10", "2026-06-19", "complete", "2026-06-10", ["QTIE"], [])
    assert [
        (
            point["trade_date"],
            point["offset_days"],
            point["price_index"],
            point["pe_ratio"],
            point["pe_index"],
        )
        for point in cycle["points"]
    ] == [
        ("2026-06-10", -5, "100.00", "12.50", "100.00"),
        ("2026-06-11", -4, "88.00", "11.00", "88.00"),
        ("2026-06-12", -3, "80.03", "10.00", "80.03"),
        ("2026-06-15", 0, "84.00", "10.50", "84.00"),
        ("2026-06-16", 1, "80.01", "10.00", "80.01"),
        ("2026-06-17", 2, "86.40", "10.80", "86.40"),
        ("2026-06-18", 3, "92.00", "11.50", "92.00"),
        ("2026-06-19", 4, "96.00", "12.00", "96.00"),
    ]
    assert cycle["markers"]["valuation_floor"] == {
        "offset_days": -3,
        "trade_date": "2026-06-12",
        "metric": "pe_ratio",
        "value": "10.00",
    }
    assert cycle["summary"] == {
        "pre_event_return_pct": "-16.00",
        "post_event_return_pct": "14.29",
        "max_drawdown_pct": "-19.99",
        "pe_compression_pct": "-19.97",
    }
    assert_complete_cycle(cycle)


FEB_EVENT: Final[dict[str, Any]] = {"event_dates": "2026-02-16", "pre_days": 5, "post_days": 4}
JUNE_EVENT: Final[dict[str, Any]] = {"event_dates": "2026-06-15", "pre_days": 5, "post_days": 4}
SUMMARY_KEYS: Final[tuple[str, ...]] = (
    "pre_event_return_pct",
    "post_event_return_pct",
    "max_drawdown_pct",
    "pe_compression_pct",
)
POINT_LEVELS: Final[tuple[str, ...]] = ("price_index", "turnover_index", "pe_ratio", "pe_index")


@pytest.mark.parametrize(
    ("ticker", "closes", "zeroed", "params", "flags", "null_levels", "floor", "summary"),
    [
        pytest.param(
            "QPF",
            crafted_closes(FEB_LEAD_IN, FEB_WINDOW),
            None,
            FEB_EVENT,
            ["pe_anchor_unavailable"],
            {"pe_ratio", "pe_index"},
            {
                "offset_days": 1,
                "trade_date": "2026-02-17",
                "metric": "price_index",
                "value": "88.00",
            },
            ("-8.00", "14.13", "-12.00", None),
            id="price-floor-fallback",
        ),
        pytest.param(
            "QZS",
            crafted_closes(FEB_LEAD_IN, FEB_WINDOW),
            "shares",
            FEB_EVENT,
            ["zero_anchor_capitalization", "pe_anchor_unavailable"],
            {"price_index", "pe_ratio", "pe_index"},
            None,
            (None, None, None, None),
            id="zero-anchor-capitalization",
        ),
        pytest.param(
            "QNT",
            crafted_closes([], JUNE_WINDOW),
            None,
            JUNE_EVENT,
            ["insufficient_turnover_history"],
            {"turnover_index"},
            {"offset_days": 1, "trade_date": "2026-06-16", "metric": "pe_ratio", "value": "88.00"},
            ("-8.00", "14.13", "-12.00", "-12.00"),
            id="insufficient-turnover-history",
        ),
        pytest.param(
            "QZT",
            crafted_closes(JUNE_LEAD_IN, JUNE_WINDOW),
            "turnover",
            JUNE_EVENT,
            ["zero_anchor_turnover"],
            {"turnover_index"},
            {"offset_days": 1, "trade_date": "2026-06-16", "metric": "pe_ratio", "value": "88.00"},
            ("-8.00", "14.13", "-12.00", "-12.00"),
            id="zero-anchor-turnover",
        ),
        pytest.param(
            "QNP",
            crafted_closes(LATE_LEAD_IN, LATE_WINDOW),
            None,
            {"event_dates": "2026-06-14", "pre_days": 1, "post_days": 5},
            ["no_pre_event_data"],
            set(),
            {"offset_days": 5, "trade_date": "2026-06-19", "metric": "pe_ratio", "value": "88.00"},
            (None, None, "-12.00", "-12.00"),
            id="no-pre-event-data",
        ),
    ],
)
async def test_backtest_undefined_normalizations_are_null_and_flagged(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    ticker: str,
    closes: dict[date, Decimal],
    zeroed: str | None,
    params: dict[str, Any],
    flags: list[str],
    null_levels: set[str],
    floor: JsonObject | None,
    summary: tuple[str | None, ...],
) -> None:
    """A one-counter cohort added inside this test's rolled-back transaction by
    ``add_sparse_counter`` (1,000,000 shares, a turnover of 1,000 x the close, FY2025 net income
    100,000 KES available from 2026-03-31), with its shares or its volume and turnover then set
    to 0 where ``zeroed`` says, closes 10.00 on its lead-in sessions, then follows price index
    100, 95, 90, 92 (T), 88, 94, 99 and 105 over a T-5 to T+4 window: a pre-event return of
    -8.00, a post-event return of (105 / 92 - 1) x 100 = 14.1304, so 14.13, and a drawdown of
    88 / 100 - 1 = -12.00. Each undefined normalization nulls its outputs and flags the cycle.

    - February, before its fundamentals are available: P/E is null throughout
      (``pe_anchor_unavailable``), so the floor falls back to the lowest price index, 88.00, and
      there is no P/E compression.
    - The same with 0 shares: the anchor capitalization is 0 (``zero_anchor_capitalization``), so
      the price index is null as well; with neither a P/E nor a price index there is no valuation
      floor, and every summary figure is null.
    - June without lead-in rows: one turnover row by the anchor is below the 5 the turnover mean
      needs (``insufficient_turnover_history``), so the turnover index and the volume exhaustion
      are null; P/E is the close x 10, so the floor is P/E 88.00 and the compression -12.00.
    - The same June window after four lead-in rows, every row with 0 volume and turnover: the
      turnover mean at the anchor is defined but 0 (``zero_anchor_turnover``), with the same nulls.
    - T on Sunday 2026-06-14 with a T-1 to T+5 window: it opens on Saturday, so its first
      session, the anchor, is after T (``no_pre_event_data``) and neither return is defined; its
      five sessions to 2026-06-19 follow the first five levels, the last being the floor.
    """
    await add_sparse_counter(db_session, ticker, "QA Cycle", closes)
    if zeroed == "shares":
        await db_session.execute(
            update(TerminalEquity)
            .where(TerminalEquity.ticker == ticker)
            .values(shares_outstanding=0)
        )
    elif zeroed == "turnover":
        await db_session.execute(
            update(TerminalDailyPrice)
            .where(TerminalDailyPrice.ticker == ticker)
            .values(volume=0, turnover_kes=Decimal("0.00"))
        )

    body = await get_json(client, "/cycles/backtest", {**params, "tickers": ticker})

    (cycle,) = body["cycles"]
    assert (cycle["status"], cycle["constituents"], cycle["flags"]) == ("complete", [ticker], flags)
    assert cycle["points"]
    for point in cycle["points"]:
        for key in POINT_LEVELS:
            assert (point[key] is None) == (key in null_levels), (point["trade_date"], key)
    markers = cycle["markers"]
    assert markers["valuation_floor"] == floor
    if "turnover_index" in null_levels:
        assert markers["volume_exhaustion"] is None
    else:
        assert_marker(
            markers["volume_exhaustion"],
            earliest_min(cycle["points"], "turnover_index"),
            "turnover_index",
        )
    assert cycle["summary"] == dict(zip(SUMMARY_KEYS, summary, strict=True))
    assert_summary_from_points(cycle)


# ---------------------------------------------------------------------------------------------
# Coded errors of the market and cycle routes
# ---------------------------------------------------------------------------------------------

UNKNOWN_TICKER: Final[str] = "Ticker 'NOPE' is not a known equity."
UNKNOWN_SECTOR: Final[str] = "Sector 'Agriculture' has no listed equity."
ONE_SUBJECT: Final[str] = "Give exactly one of ticker or sector."
WINDOW_UNITS: Final[str] = (
    "Give the window either in days (pre_days, post_days) or in months "
    "(pre_months, post_months), not both."
)


@pytest.mark.parametrize(
    ("path", "params", "status", "code", "field", "message"),
    [
        pytest.param(
            "/market/valuation-bands",
            {"ticker": "SCOM", "sector": "Banking"},
            422,
            "exactly_one_subject",
            None,
            ONE_SUBJECT,
            id="bands-both-subjects",
        ),
        pytest.param(
            "/market/valuation-bands",
            {"metric": "pb"},
            422,
            "exactly_one_subject",
            None,
            ONE_SUBJECT,
            id="bands-no-subject",
        ),
        pytest.param(
            "/market/valuation-bands",
            {"ticker": "nope"},
            404,
            "unknown_ticker",
            "ticker",
            UNKNOWN_TICKER,
            id="bands-unknown-ticker",
        ),
        pytest.param(
            "/market/valuation-bands",
            {"sector": "Agriculture"},
            404,
            "unknown_sector",
            "sector",
            UNKNOWN_SECTOR,
            id="bands-unknown-sector",
        ),
        pytest.param(
            "/market/anomalies",
            {"ticker": "nope"},
            404,
            "unknown_ticker",
            "ticker",
            UNKNOWN_TICKER,
            id="anomalies-unknown-ticker",
        ),
        pytest.param(
            "/market/anomalies",
            {"sector": "Agriculture"},
            404,
            "unknown_sector",
            "sector",
            UNKNOWN_SECTOR,
            id="anomalies-unknown-sector",
        ),
        pytest.param(
            "/market/flows",
            {"ticker": "SCOM", "sector": "Banking"},
            422,
            "conflicting_subjects",
            "sector",
            "Give a ticker or a sector, not both; give neither for the whole market.",
            id="flows-both-subjects",
        ),
        pytest.param(
            "/market/flows",
            {"ticker": "nope"},
            404,
            "unknown_ticker",
            "ticker",
            UNKNOWN_TICKER,
            id="flows-unknown-ticker",
        ),
        pytest.param(
            "/market/flows",
            {"sector": "Agriculture"},
            404,
            "unknown_sector",
            "sector",
            UNKNOWN_SECTOR,
            id="flows-unknown-sector",
        ),
        pytest.param(
            "/market/flows",
            {"start": "2026-10-01"},
            422,
            "start_after_end",
            "start",
            "start (2026-10-01) must not be after end (2026-09-30).",
            id="flows-start-after-default-end",
        ),
        pytest.param(
            "/market/flows",
            {"ticker": "KCB", "start": ANCHOR, "end": "2026-09-29"},
            422,
            "start_after_end",
            "start",
            "start (2026-09-30) must not be after end (2026-09-29).",
            id="flows-start-after-end",
        ),
        pytest.param(
            "/cycles/backtest",
            {"tickers": ["SCOM", "nope"]},
            404,
            "unknown_ticker",
            "tickers",
            UNKNOWN_TICKER,
            id="cycles-unknown-ticker",
        ),
        pytest.param(
            "/cycles/backtest",
            {"sector": "Agriculture"},
            404,
            "unknown_sector",
            "sector",
            UNKNOWN_SECTOR,
            id="cycles-unknown-sector",
        ),
        pytest.param(
            "/cycles/backtest",
            {"pre_days": 30, "pre_months": 3},
            422,
            "conflicting_window_units",
            "pre_months",
            WINDOW_UNITS,
            id="cycles-days-and-pre-months",
        ),
        pytest.param(
            "/cycles/backtest",
            {"post_days": 30, "post_months": 3},
            422,
            "conflicting_window_units",
            "post_months",
            WINDOW_UNITS,
            id="cycles-days-and-post-months",
        ),
    ],
)
async def test_route_rules_and_unknown_entities_are_coded_errors(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any],
    status: int,
    code: str,
    field: str | None,
    message: str,
) -> None:
    """Each route hands its service the parameters the service checks, so a cross-field rule is
    a coded 422 and an unknown ticker (normalised in the message) or sector a coded 404 naming the
    offending parameter, never a 200 over a defaulted or widened scope. ``detail`` is the object
    ``{code, message, field}``, not FastAPI's list."""
    response = await client.get(f"{BASE}{path}", params=params)

    assert response.status_code == status, response.text
    assert response.json() == {"detail": {"code": code, "message": message, "field": field}}


@pytest.mark.parametrize(
    ("key", "values", "limit"),
    [
        pytest.param(
            "event_dates",
            [f"2015-06-{day:02d}" for day in range(1, 10)],
            8,
            id="nine-event-dates",
        ),
        pytest.param("tickers", [f"Q{index}" for index in range(101)], 100, id="101-tickers"),
    ],
)
async def test_backtest_list_bounds_are_the_standard_422(
    client: httpx.AsyncClient, key: str, values: list[str], limit: int
) -> None:
    """At most 8 event dates and 100 tickers are bound; one more is FastAPI's standard 422 list
    (``too_long``), refused before the service runs, while ``limit`` values pass validation: 8
    dates are backtested, and 100 unknown tickers reach the service's coded 404."""
    rejected = await client.get(f"{BASE}/cycles/backtest", params={key: values})

    assert rejected.status_code == 422, rejected.text
    detail = rejected.json()["detail"]
    assert isinstance(detail, list), detail
    assert [(entry["type"], entry["loc"], entry["ctx"]["max_length"]) for entry in detail] == [
        ("too_long", ["query", key], limit)
    ]

    accepted = await client.get(
        f"{BASE}/cycles/backtest", params={key: values[:limit], "pre_days": 1, "post_days": 1}
    )
    if key == "event_dates":
        assert accepted.status_code == 200, accepted.text
        assert [cycle["event_date"] for cycle in accepted.json()["cycles"]] == values[:limit]
    else:
        assert accepted.status_code == 404, accepted.text
        assert accepted.json()["detail"]["code"] == "unknown_ticker"


async def test_backtest_without_stored_or_requested_events_is_a_coded_422(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:
    """With every ``terminal_cycles`` row deleted inside this test's rolled-back transaction, a
    request without ``event_dates`` has no event: 422 ``no_event_dates`` against
    ``event_dates``. A requested date still runs, labelled "Event YYYY-MM-DD" now that no stored
    row names it."""
    await db_session.execute(delete(TerminalCycle))

    response = await client.get(f"{BASE}/cycles/backtest")

    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": {
            "code": "no_event_dates",
            "message": (
                "No event dates were given and terminal_cycles holds no events. Pass event_dates "
                "or run the seeder to load the default election cycles."
            ),
            "field": "event_dates",
        }
    }
    body = await get_json(client, "/cycles/backtest", {"event_dates": "2013-03-04"})
    assert [(cycle["label"], cycle["status"]) for cycle in body["cycles"]] == [
        ("Event 2013-03-04", "complete")
    ]


# ---------------------------------------------------------------------------------------------
# IPO valuation sandbox
# ---------------------------------------------------------------------------------------------


async def test_ipo_scenarios_return_the_quickmart_preset(
    client: httpx.AsyncClient, quickmart_assumptions: dict[str, object]
) -> None:
    body = await get_json(client, "/ipo/scenarios")

    assert len(body) == 1
    preset = body[0]
    uuid.UUID(preset["id"])
    assert preset["company_name"] == "Quickmart Supermarket"
    assert preset["sector"] == "Retail & Consumer Goods"
    assert preset["currency"] == "KES"
    assert [risk["title"] for risk in preset["risk_scenarios"]] == list(RISK_TITLES)
    assert all(risk["description"] for risk in preset["risk_scenarios"])

    assumptions = preset["assumptions"]
    assert assumptions["offer_price_kes"] == "7.50"
    assert set(assumptions) == set(quickmart_assumptions) | SENSITIVITY_AXES
    for axis in SENSITIVITY_AXES:
        assert assumptions[axis] is None
    for key, expected in quickmart_assumptions.items():
        actual = assumptions[key]
        if isinstance(expected, int):
            assert is_json_int(actual), key
            assert actual == expected, key
        else:
            # The stored scale may differ from the request body's; the value may not.
            assert dec(actual) == dec(expected), key


async def test_ipo_valuation_of_the_preset_matches_the_reference_results(
    client: httpx.AsyncClient,
) -> None:
    scenarios = await get_json(client, "/ipo/scenarios")
    response = await client.post(f"{BASE}/ipo/valuation", json=scenarios[0]["assumptions"])

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["blended_fair_value_per_share_kes"] == "6.82"
    assert body["upside_pct"] == "-9.11"
    assert body["offer_price_kes"] == "7.50"
    assert body["dcf"]["enterprise_value_kes"] == "9484146704.50"
    assert body["dcf"]["fair_value_per_share_kes"] == "5.48"
    assert body["comparables"]["fair_value_per_share_kes"] == "8.15"
    assert [year["year"] for year in body["dcf"]["projections"]] == [1, 2, 3, 4, 5]

    sensitivity = body["sensitivity"]
    rows = [dec(value) for value in sensitivity["rows"]]
    cols = [dec(value) for value in sensitivity["cols"]]
    assert rows == [Decimal(value) for value in ("16", "17", "18", "19", "20")]
    assert cols == [Decimal(value) for value in ("3.0", "3.5", "4.0", "4.5", "5.0", "5.5", "6.0")]
    cells = sensitivity["cells"]
    assert [len(row) for row in cells] == [7] * 5
    for wacc, row in zip(rows, cells, strict=True):
        for margin, cell in zip(cols, row, strict=True):
            assert dec(cell["wacc_pct"]) == wacc
            assert dec(cell["ebitda_margin_pct"]) == margin
    assert cells[0][0]["fair_value_per_share_kes"] == "2.97"
    assert cells[0][6]["fair_value_per_share_kes"] == "12.50"
    assert cells[4][0]["fair_value_per_share_kes"] == "2.14"
    assert cells[4][6]["fair_value_per_share_kes"] == "10.16"


async def test_ipo_valuation_primary_proceeds_bound_at_the_offer_size(
    client: httpx.AsyncClient, quickmart_assumptions: dict[str, object]
) -> None:
    # Offer size is 7.50 x 500,000,000 = 3,750,000,000.00 KES.
    over = {**quickmart_assumptions, "primary_proceeds_kes": "3750000000.01"}
    rejected = await client.post(f"{BASE}/ipo/valuation", json=over)

    assert rejected.status_code == 422, rejected.text
    body = rejected.json()
    assert set(body) == {"detail"}
    detail = body["detail"]
    assert isinstance(detail, dict), "a cross-field rule must use the coded body, not the list"
    assert detail["code"] == "primary_exceeds_offer_size"
    assert detail["field"] == "primary_proceeds_kes"
    assert isinstance(detail["message"], str)
    assert detail["message"]

    at_limit = {**quickmart_assumptions, "primary_proceeds_kes": "3750000000.00"}
    accepted = await client.post(f"{BASE}/ipo/valuation", json=at_limit)

    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["primary_proceeds_kes"] == "3750000000.00"
    assert accepted.json()["secondary_proceeds_kes"] == "0.00"


# ---------------------------------------------------------------------------------------------
# PDF filing commit
# ---------------------------------------------------------------------------------------------


async def test_ingest_pdf_commit_merges_all_six_items_into_seeded_scom_fundamentals(
    client: httpx.AsyncClient,
    db_session: AsyncSession,
    statement_pdf: Callable[..., bytes],
    statement_expected: Any,
    committed_query: CommittedQuery,
) -> None:
    pdf = statement_pdf("KES", tag="api seeded commit")
    sha256 = hashlib.sha256(pdf).hexdigest()

    response = await client.post(
        f"{BASE}/ingest/pdf",
        files={"file": ("scom_fy2026.pdf", pdf, "application/pdf")},
        data={
            "document_type": "annual_report",
            "ticker": "SCOM",
            "period_end_date": "2026-03-31",
            "period_type": "FY",
            "commit": "true",
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["file_sha256"] == sha256
    assert body["currency"] == "KES"
    assert body["status"] == "extracted"
    assert body["committed"] is True
    assert body["missing_items"] == []
    assert body["page_count"] == statement_expected.page_count == 4
    assert body["detected_scale"] == statement_expected.detected_scale == 1000
    items = by_key(body["items"], "item")
    assert set(items) == set(ITEM_COLUMNS)
    for item, column in ITEM_COLUMNS.items():
        extracted = items[item]
        assert extracted["status"] == "found", item
        assert extracted["unit"] == ("shares" if item == "shares_outstanding" else "KES")
        assert dec(extracted["value"]) == Decimal(statement_expected[column]), item

    # The filing and fundamentals writes sit inside the rolled-back outer transaction, so they
    # are read through the same session the request used.
    fundamentals = await fetch_one(
        db_session,
        select(
            TerminalFundamental.id,
            TerminalFundamental.filing_id,
            TerminalFundamental.revenue_kes,
            TerminalFundamental.ebitda_kes,
            TerminalFundamental.net_income_kes,
            TerminalFundamental.total_debt_kes,
            TerminalFundamental.operating_cash_flow_kes,
            TerminalFundamental.shares_outstanding,
            TerminalFundamental.source_by_field,
        ).where(
            TerminalFundamental.ticker == "SCOM",
            TerminalFundamental.period_end_date == dt.date(2026, 3, 31),
            TerminalFundamental.period_type == "FY",
        ),
    )
    assert str(fundamentals["id"]) == body["fundamentals_id"]
    assert str(fundamentals["filing_id"]) == body["filing_id"]
    for column in ITEM_COLUMNS.values():
        assert fundamentals[column] == statement_expected[column], column
    provenance = fundamentals["source_by_field"]
    assert isinstance(provenance, dict)
    for column, source in provenance.items():
        expected_source = "pdf_filing" if column in ITEM_COLUMNS.values() else "seed"
        assert source == expected_source, column
    assert set(ITEM_COLUMNS.values()) <= set(provenance)

    filing = await fetch_one(
        db_session,
        select(
            TerminalCorporateFiling.ticker,
            TerminalCorporateFiling.file_sha256,
            TerminalCorporateFiling.committed_to_fundamentals,
            TerminalCorporateFiling.page_count,
            TerminalCorporateFiling.detected_scale,
        ).where(TerminalCorporateFiling.id == uuid.UUID(body["filing_id"])),
    )
    assert filing["ticker"] == "SCOM"
    assert filing["file_sha256"] == sha256
    assert filing["committed_to_fundamentals"] is True
    assert filing["page_count"] == 4
    assert filing["detected_scale"] == 1000

    # The audit run row is written on its own connection and is therefore committed.
    runs = await committed_query(
        select(TerminalIngestionRun.status, TerminalIngestionRun.error_message).where(
            TerminalIngestionRun.pipeline == "pdf_filing",
            TerminalIngestionRun.trigger == "api",
            TerminalIngestionRun.details["file_sha256"].astext == sha256,
        )
    )
    assert [(run["status"], run["error_message"]) for run in runs] == [("succeeded", None)]


# ---------------------------------------------------------------------------------------------
# Watchlists
# ---------------------------------------------------------------------------------------------


async def test_watchlists_list_the_seeded_core_8_for_the_default_principal(
    client: httpx.AsyncClient,
) -> None:
    body = await get_json(client, "/watchlists")

    assert len(body) == 1
    watchlist = body[0]
    uuid.UUID(watchlist["id"])
    assert watchlist["name"] == CORE_WATCHLIST
    assert watchlist["tickers"] == CORE_WATCHLIST_TICKERS
    dt.datetime.fromisoformat(watchlist["created_at"])
    dt.datetime.fromisoformat(watchlist["updated_at"])


async def test_watchlist_create_add_remove_and_delete_lifecycle(
    client: httpx.AsyncClient,
) -> None:
    created = await client.post(
        f"{BASE}/watchlists", json={"name": "Acceptance", "tickers": ["SCOM"]}
    )
    assert created.status_code == 201, created.text
    assert created.json()["name"] == "Acceptance"
    assert created.json()["tickers"] == ["SCOM"]
    watchlist_id = created.json()["id"]
    items_path = f"{BASE}/watchlists/{watchlist_id}/items/KCB"

    added = await client.put(items_path)
    assert added.status_code == 200, added.text
    assert added.json()["tickers"] == ["KCB", "SCOM"]

    removed = await client.delete(items_path)
    assert removed.status_code == 200, removed.text
    assert removed.json()["tickers"] == ["SCOM"]

    deleted = await client.delete(f"{BASE}/watchlists/{watchlist_id}")
    assert deleted.status_code == 204, deleted.text
    assert deleted.content == b""

    remaining = await get_json(client, "/watchlists")
    assert [watchlist["name"] for watchlist in remaining] == [CORE_WATCHLIST]


# ---------------------------------------------------------------------------------------------
# Query parameters of the health, macro, market, cycles, IPO and watchlist endpoints
# ---------------------------------------------------------------------------------------------
#
# Each of these endpoints binds its query through one query-parameter model that forbids unknown
# keys, so a misspelt parameter is FastAPI's standard 422 list rather than a 200 that silently
# applies the defaults or widens the scope. The OpenAPI document lists each endpoint's declared
# parameters, in order, with their defaults and bounds.

# Query parameter -> (default, minimum, maximum, enum); None where the schema has none.
QuerySpec = tuple[object, Decimal | None, Decimal | None, list[str] | None]

NO_BOUNDS: Final[QuerySpec] = (None, None, None, None)


def query_spec(parameter: JsonObject) -> QuerySpec:
    """The default, bounds and enum an OpenAPI query parameter documents.

    A nullable or Decimal parameter documents its bounds inside an ``anyOf`` member; bounds are
    read back through ``str`` into ``Decimal``, so a bound of 0.5 compares exactly. The maximum
    of a repeatable parameter is its most repeats, ``maxItems``.
    """
    schema = parameter["schema"]
    members = [schema, *schema.get("anyOf", [])]

    def first(key: str) -> Any:
        return next((member[key] for member in members if key in member), None)

    minimum, maximum = first("minimum"), first("maximum")
    if maximum is None:
        maximum = first("maxItems")
    return (
        schema.get("default"),
        None if minimum is None else Decimal(str(minimum)),
        None if maximum is None else Decimal(str(maximum)),
        first("enum"),
    )


@pytest.mark.parametrize(
    ("path", "params", "unknown"),
    [
        pytest.param("/health", {"foo": "1"}, "foo", id="health"),
        pytest.param(
            "/macro/overview", {"histroy_months": "240"}, "histroy_months", id="macro-overview"
        ),
        pytest.param("/market/sector-heatmap", {"period": "1"}, "period", id="sector-heatmap"),
        pytest.param(
            "/market/valuation-bands",
            {"ticker": "SCOM", "lookback": "1"},
            "lookback",
            id="valuation-bands",
        ),
        pytest.param("/market/anomalies", {"tickr": "SCOM"}, "tickr", id="anomalies"),
        pytest.param("/market/flows", {"tickr": "SCOM"}, "tickr", id="flows"),
        pytest.param("/market/screener", {"tickr": "SCOM"}, "tickr", id="screener"),
        pytest.param("/ipo/scenarios", {"bogus": "1"}, "bogus", id="ipo-scenarios"),
        pytest.param("/cycles/backtest", {"bogus": "1"}, "bogus", id="cycles-backtest"),
        pytest.param(
            "/cycles/backtest",
            {"pre_months": "12", "bogus": "1"},
            "bogus",
            id="cycles-backtest-beside-declared",
        ),
        pytest.param("/watchlists", {"bogus": "1"}, "bogus", id="watchlists"),
    ],
)
async def test_unknown_query_parameter_is_the_standard_422(
    client: httpx.AsyncClient, path: str, params: Mapping[str, str], unknown: str
) -> None:
    """A misspelt parameter, alone or beside a declared one, is refused with exactly one
    ``extra_forbidden`` entry naming it and echoing its value."""
    response = await client.get(f"{BASE}{path}", params=params)

    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": [
            {
                "type": "extra_forbidden",
                "loc": ["query", unknown],
                "msg": "Extra inputs are not permitted",
                "input": params[unknown],
            }
        ]
    }


async def test_unknown_query_parameter_on_ipo_valuation_is_the_standard_422(
    client: httpx.AsyncClient,
) -> None:
    """``POST /ipo/valuation`` declares no query parameter: with the valid seeded preset as its
    body, an unknown one is refused with exactly one ``extra_forbidden`` entry, and the error body
    carries no valuation."""
    scenarios = await get_json(client, "/ipo/scenarios")
    response = await client.post(
        f"{BASE}/ipo/valuation", params={"bogus": "1"}, json=scenarios[0]["assumptions"]
    )

    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": [
            {
                "type": "extra_forbidden",
                "loc": ["query", "bogus"],
                "msg": "Extra inputs are not permitted",
                "input": "1",
            }
        ]
    }


async def test_backtest_binds_each_repeat_of_event_dates_and_tickers(
    client: httpx.AsyncClient,
) -> None:
    """``event_dates`` and ``tickers`` are repeatable: each repeat is one value of the list."""
    body = await get_json(
        client,
        "/cycles/backtest",
        {"event_dates": ["2017-08-08", "2022-08-09"], "tickers": ["SCOM", "KCB"]},
    )

    assert sorted(body["candidates"]) == ["KCB", "SCOM"]
    assert [cycle["event_date"] for cycle in body["cycles"]] == ["2017-08-08", "2022-08-09"]


@pytest.mark.parametrize(
    ("params", "loc", "error_type"),
    [
        pytest.param(
            {"event_dates": [f"2017-08-{day:02d}" for day in range(1, 10)]},
            ["query", "event_dates"],
            "too_long",
            id="nine-event-dates",
        ),
        pytest.param(
            {"tickers": [f"T{index}" for index in range(101)]},
            ["query", "tickers"],
            "too_long",
            id="101-tickers",
        ),
        pytest.param(
            {"tickers": ["SCOM", "S" * 17]},
            ["query", "tickers", 1],
            "string_too_long",
            id="ticker-too-long",
        ),
        pytest.param(
            {"sector": "Bank\x00ing"},
            ["query", "sector"],
            "string_pattern_mismatch",
            id="sector-with-nul",
        ),
    ],
)
async def test_backtest_single_parameter_bound_is_the_standard_422(
    client: httpx.AsyncClient, params: Mapping[str, Any], loc: list[object], error_type: str
) -> None:
    """At most 8 event dates and 100 tickers, each ticker 1-16 characters and no NUL in a
    sector: a breach is one standard 422 entry at the offending parameter or repeat."""
    response = await client.get(f"{BASE}/cycles/backtest", params=params)

    assert response.status_code == 422, response.text
    [entry] = response.json()["detail"]
    assert (entry["type"], entry["loc"]) == (error_type, loc)


@pytest.fixture(scope="module")
def terminal_openapi() -> JsonObject:
    """The OpenAPI document of the standalone app; reading it sends no request."""
    document: JsonObject = create_app().openapi()
    return document


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        pytest.param("/health", {}, id="health"),
        pytest.param(
            "/macro/overview",
            {"history_months": (24, Decimal(1), Decimal(240), None)},
            id="macro-overview",
        ),
        pytest.param(
            "/market/sector-heatmap",
            {
                "interval": ("month", None, None, ["month", "week"]),
                "periods": (12, Decimal(1), Decimal(36), None),
                "as_of": NO_BOUNDS,
            },
            id="sector-heatmap",
        ),
        pytest.param(
            "/market/valuation-bands",
            {
                "ticker": NO_BOUNDS,
                "sector": NO_BOUNDS,
                "metric": ("pe", None, None, ["pe", "pb"]),
                "lookback_years": (5, Decimal(1), Decimal(15), None),
                "as_of": NO_BOUNDS,
            },
            id="valuation-bands",
        ),
        pytest.param(
            "/market/anomalies",
            {
                "metric": ("both", None, None, ["volume", "turnover", "both"]),
                "window": (20, Decimal(5), Decimal(250), None),
                "threshold": ("2.0", Decimal("0.5"), Decimal(10), None),
                "lookback_sessions": (20, Decimal(1), Decimal(250), None),
                "ticker": NO_BOUNDS,
                "sector": NO_BOUNDS,
            },
            id="anomalies",
        ),
        pytest.param(
            "/market/flows",
            {
                "ticker": NO_BOUNDS,
                "sector": NO_BOUNDS,
                "as_of": NO_BOUNDS,
                "start": NO_BOUNDS,
                "end": NO_BOUNDS,
                "rolling_window": (20, Decimal(5), Decimal(250), None),
            },
            id="flows",
        ),
        pytest.param(
            "/cycles/backtest",
            {
                "event_dates": (None, None, Decimal(8), None),
                "pre_days": (None, Decimal(1), Decimal(365), None),
                "post_days": (None, Decimal(1), Decimal(365), None),
                "pre_months": (None, Decimal(1), Decimal(12), None),
                "post_months": (None, Decimal(1), Decimal(12), None),
                "tickers": (None, None, Decimal(100), None),
                "sector": NO_BOUNDS,
            },
            id="cycles-backtest",
        ),
        pytest.param("/ipo/scenarios", {}, id="ipo-scenarios"),
        pytest.param("/watchlists", {}, id="watchlists"),
    ],
)
def test_documented_query_parameters_keep_their_defaults_and_bounds(
    terminal_openapi: JsonObject, path: str, expected: Mapping[str, QuerySpec]
) -> None:
    """Each endpoint documents exactly its declared query parameters, in declaration order, each
    optional and described, with the default and bounds of the API contract; the threshold
    default is the Decimal string "2.0"."""
    operation = terminal_openapi["paths"][f"{BASE}{path}"]["get"]
    parameters = operation.get("parameters", [])

    assert [parameter["name"] for parameter in parameters] == list(expected)
    for parameter in parameters:
        assert parameter["in"] == "query"
        assert parameter["required"] is False
        assert parameter["description"]
    assert {parameter["name"]: query_spec(parameter) for parameter in parameters} == expected


# ---------------------------------------------------------------------------------------------
# Trailing-slash paths
# ---------------------------------------------------------------------------------------------
#
# The standalone app is built with ``redirect_slashes=False``. Starlette's default would answer a
# path with a trailing slash with a 307 to a ``Location`` built from the request's ``Host``
# header, which the client does not have to tell the truth about, and a 307 resends a POST body.


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        pytest.param("GET", "/health/", None, id="get-health"),
        pytest.param("POST", "/ipo/valuation/", {}, id="post-ipo-valuation"),
    ],
)
async def test_trailing_slash_path_is_a_404_without_a_host_built_redirect(
    client: httpx.AsyncClient, method: str, path: str, body: JsonObject | None
) -> None:
    """A trailing-slash path under a forged ``Host`` is FastAPI's standard 404, with no
    ``Location`` header, rather than a redirect to that host."""
    response = await client.request(
        method, f"{BASE}{path}", headers={"Host": "evil.example"}, json=body
    )

    assert response.status_code == 404, response.text
    assert "location" not in response.headers
    assert response.json() == {"detail": "Not Found"}


# ---------------------------------------------------------------------------------------------
# Market service on per-test seeded clones (services/market_service.py)
# ---------------------------------------------------------------------------------------------
#
# Market service: the five ``/market`` analytics and the shared loaders, on seeded PostgreSQL.
#
# ``services/market_service.py`` reads ``terminal_equities``, ``terminal_daily_prices`` and
# ``terminal_fundamentals`` with bound ``select()`` statements, calls the pure ``quant`` formulas
# and rounds each output once with ``quantize_out`` (ROUND_HALF_UP, 2 dp). The contract held here:
#
# - **Market sessions** are the distinct stored ``trade_date`` values; the default ``as_of`` is the
#   latest of them, and with no price row at all it is today's Africa/Nairobi date and every list
#   is empty.
# - **Screener**: each equity valued at its latest close on or before ``as_of`` with the fiscal
#   year available 90 days after its period end; Banking EV/EBITDA null with
#   ``ev_not_meaningful_for_banks``; filters inclusive on the quantized values, a null metric
#   failing any filter on it; sorting with nulls last and ties by ticker in either order; sector
#   aggregates over the whole universe, independent of the filters.
# - **Heatmap**: the last session on or before each month-end or Friday; only KES equities.
# - **Valuation bands**: ``requested_start`` is ``as_of`` minus the lookback, coverage is partial
#   when data starts more than 7 days later, and ``stats`` exist only for ``status = "ok"``. A
#   sector has a daily value only on the sessions on which a member traded, so a one-member
#   sector's band is its counter's.
# - **Anomalies**: positive spikes at or above the threshold over the last ``lookback_sessions``
#   sessions, sorted by ``trade_date`` then ``z_score``, both descending.
# - **Flows**: ``end`` defaults to ``as_of`` and ``start`` to ``end`` minus 365 days; local net flow
#   is the negated foreign net flow on every session.
# - **Coded errors** ``HTTPException(status, detail={"code", "message", "field"})``: 422
#   ``min_exceeds_max``, ``exactly_one_subject``, ``conflicting_subjects`` and ``start_after_end``;
#   404 ``unknown_ticker``, ``unknown_sector`` and ``watchlist_not_found``.
#
# Seeded values are those of ``seed/reference_data.py``: the final closes (SCOM 33.25, EQTY 95.75,
# KCB 85.00, EABL 269.50, KEGN 10.60, COOP 36.70, ABSA 30.55, SBIC 270.50) times the attachment's
# share counts give the market caps exactly, and the latest fundamentals are set from the target
# P/E, P/B and payout, so P/E, P/B and dividend yield (payout / P/E) are those targets. EV/EBITDA
# is P/E x net margin / EBITDA margin + net debt / EBITDA: SCOM 14 x 0.22 / 0.45 + 0.3 = 7.14.
# Volumes, turnover, flows and Z-scores come from the deterministic synthetic history and are
# asserted at their seeded values, except the planted volume spikes, which are asserted by
# inclusion: the seeder guarantees them but not the absence of other background crossings.
#
# The coded 422s that the services check before their first query run on an ``AsyncSession``
# bound to no engine, which would raise on any query, and the date clamps are pure; neither needs
# a database. Every other test gets a fresh seeded clone (the root conftest's ``seeded_database``,
# which marks it ``integration``); the few that change rows change only their own clone. Every
# expected value is an exact ``Decimal`` or literal.

LATEST = date(2026, 9, 30)
FIRST_SESSION = date(2012, 1, 2)
# Lists in the services' ticker and sector-name order, unlike the BANKS and SECTORS sets above.
SORTED_BANKS = ["ABSA", "COOP", "EQTY", "KCB", "SBIC"]
MANUFACTURING = "Manufacturing & Allied"
SORTED_SECTORS = ["Banking", "Energy & Petroleum", MANUFACTURING, "Telecommunications"]
NAIROBI = ZoneInfo("Africa/Nairobi")
MISSING_WATCHLIST = uuid.UUID("00000000-0000-4000-8000-000000000000")

# Ticker -> (sector, close, market cap, P/E, P/B, EV/EBITDA, dividend yield %) as of 2026-09-30,
# in the screener's default order (market cap descending).
SEEDED_ROWS: dict[str, tuple[str, str, str, str, str, str | None, str]] = {
    "SCOM": ("Telecommunications", "33.25", "1332175481000.00", "14.00", "4.50", "7.14", "5.00"),
    "EQTY": ("Banking", "95.75", "361329381250.00", "4.50", "0.90", None, "6.67"),
    "KCB": ("Banking", "85.00", "273144339275.00", "4.00", "0.80", None, "7.50"),
    "COOP": ("Banking", "36.70", "215325311306.50", "4.00", "0.80", None, "10.00"),
    "EABL": (MANUFACTURING, "269.50", "213113688942.00", "12.00", "5.00", "6.78", "5.00"),
    "ABSA": ("Banking", "30.55", "165933424800.00", "5.50", "1.30", None, "10.00"),
    "SBIC": ("Banking", "270.50", "106934503079.00", "6.00", "1.00", None, "8.33"),
    "KEGN": ("Energy & Petroleum", "10.60", "69901936793.40", "6.00", "0.40", "4.64", "5.00"),
}
DEFAULT_ORDER = list(SEEDED_ROWS)


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...

    @property
    def settings(self) -> TerminalSettings: ...


@dataclass(frozen=True)
class Principal:
    """A caller satisfying ``OwnerPrincipal``: the seeded watchlist belongs to ``local``."""

    subject: str


LOCAL = Principal("local")

ServiceCall = Callable[[AsyncSession, TerminalSettings], Awaitable[object]]


@pytest_asyncio.fixture
async def session(seeded_database: SeededDatabase) -> AsyncIterator[AsyncSession]:
    """A session on this test's own seeded clone."""
    factory = async_sessionmaker(seeded_database.engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session


@pytest.fixture
def settings(seeded_database: SeededDatabase) -> TerminalSettings:
    """The clone's settings: Africa/Nairobi, 90-day fundamentals lag."""
    return seeded_database.settings


def nairobi_today() -> date:
    """The current Africa/Nairobi date, the fallback ``as_of`` of an empty price table."""
    return datetime.now(NAIROBI).date()


def assert_coded(
    exc: HTTPException, status_code: int, code: str, field: str | None, message: str
) -> None:
    """``exc`` is the coded error ``{code, message, field}`` at ``status_code``."""
    assert exc.status_code == status_code
    assert exc.detail == {"code": code, "message": message, "field": field}


async def screener_tickers(
    session: AsyncSession, settings: TerminalSettings, params: ScreenerParams
) -> list[str]:
    """Tickers ``get_screener`` returns for ``params`` and the local owner, in response order."""
    response = await ms.get_screener(session, params, LOCAL, settings)
    assert response.count == len(response.rows)
    return [row.ticker for row in response.rows]


# ---------------------------------------------------------------------------------------------
# Market service: Cross-field checks that run before the first query
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("call", "code", "field", "message"),
    [
        pytest.param(
            lambda s, cfg: ms.get_screener(
                s, ScreenerParams(min_pe=Decimal("12"), max_pe=Decimal("5")), LOCAL, cfg
            ),
            "min_exceeds_max",
            "min_pe",
            "min_pe (12) must not exceed max_pe (5).",
            id="screener_min_pe",
        ),
        pytest.param(
            lambda s, cfg: ms.get_screener(
                s, ScreenerParams(min_pb=Decimal("1.31"), max_pb=Decimal("1.30")), LOCAL, cfg
            ),
            "min_exceeds_max",
            "min_pb",
            "min_pb (1.31) must not exceed max_pb (1.30).",
            id="screener_min_pb",
        ),
        pytest.param(
            lambda s, cfg: ms.get_screener(
                s,
                ScreenerParams(min_ev_ebitda=Decimal("8"), max_ev_ebitda=Decimal("-1")),
                LOCAL,
                cfg,
            ),
            "min_exceeds_max",
            "min_ev_ebitda",
            "min_ev_ebitda (8) must not exceed max_ev_ebitda (-1).",
            id="screener_min_ev_ebitda",
        ),
        pytest.param(
            lambda s, cfg: ms.get_valuation_bands(s, cfg, ticker="SCOM", sector="Banking"),
            "exactly_one_subject",
            None,
            "Give exactly one of ticker or sector.",
            id="bands_both_subjects",
        ),
        pytest.param(
            lambda s, cfg: ms.get_valuation_bands(s, cfg),
            "exactly_one_subject",
            None,
            "Give exactly one of ticker or sector.",
            id="bands_no_subject",
        ),
        pytest.param(
            lambda s, cfg: ms.get_flows(s, ticker="SCOM", sector="Banking", settings=cfg),
            "conflicting_subjects",
            "sector",
            "Give a ticker or a sector, not both; give neither for the whole market.",
            id="flows_both_subjects",
        ),
    ],
)
async def test_cross_field_rule_is_a_coded_422_before_any_query(
    call: ServiceCall,
    code: str,
    field: str | None,
    message: str,
    test_settings: TerminalSettings,
) -> None:
    """A ``min_*`` above its ``max_*``, a valuation band without exactly one subject, and flows
    for a ticker and a sector at once are refused with their codes. The session is bound to no
    engine, so reaching a query would raise ``UnboundExecutionError`` instead."""
    async with AsyncSession() as unbound:
        with pytest.raises(HTTPException) as excinfo:
            await call(unbound, test_settings)
    assert_coded(excinfo.value, 422, code, field, message)


@pytest.mark.parametrize(
    ("clamp", "day", "amount"),
    [
        pytest.param(ms._days_before, date(1, 1, 5), 365, id="days"),
        pytest.param(ms._months_before, date(1, 6, 30), 12, id="months"),
    ],
)
def test_lookback_before_the_first_calendar_day_is_clamped_to_date_min(
    clamp: Callable[[date, int], date], day: date, amount: int
) -> None:
    """A flow range (365 days) or band lookback (12 months) that would start before 0001-01-01
    starts at ``date.min`` instead of raising."""
    assert clamp(day, amount) == date.min


# ---------------------------------------------------------------------------------------------
# Market service: Shared loaders and subject checks
# ---------------------------------------------------------------------------------------------


async def test_market_sessions_are_the_distinct_stored_trade_dates(session: AsyncSession) -> None:
    """The seed calendar has 3,762 weekday sessions from 2012-01-02 to 2026-09-30; bounds are
    inclusive, and the weekend of 2026-09-26/27 is not a session. The latest is the default
    ``as_of``."""
    sessions = await ms.load_market_sessions(session)
    assert (len(sessions), sessions[0], sessions[-1]) == (3762, FIRST_SESSION, LATEST)
    assert sessions == sorted(set(sessions))
    assert await ms.load_market_sessions(session, date(2026, 9, 25), LATEST) == [
        date(2026, 9, 25),
        date(2026, 9, 28),
        date(2026, 9, 29),
        date(2026, 9, 30),
    ]
    assert await ms.load_market_sessions(session, end=date(2012, 1, 4)) == [
        date(2012, 1, 2),
        date(2012, 1, 3),
        date(2012, 1, 4),
    ]
    assert await ms.latest_trade_date(session) == LATEST


@pytest.mark.parametrize(
    ("tickers", "sector", "expected"),
    [
        pytest.param(None, None, sorted(SEEDED_ROWS), id="all"),
        pytest.param([" scom ", "kcb", "SCOM", "NOPE"], None, ["KCB", "SCOM"], id="normalised"),
        pytest.param([], None, [], id="empty_tickers"),
        pytest.param(None, "Banking", SORTED_BANKS, id="sector"),
        pytest.param(["SCOM", "KCB"], "Banking", ["KCB"], id="tickers_and_sector"),
        pytest.param(None, "banking", [], id="sector_matched_as_stored"),
    ],
)
async def test_load_equities_restricts_by_tickers_and_sector(
    session: AsyncSession, tickers: list[str] | None, sector: str | None, expected: list[str]
) -> None:
    """Tickers are trimmed and upper-cased and unknown ones are absent; an empty collection gives
    nothing; the sector matches exactly as stored. Results are ordered by ticker."""
    equities = await ms.load_equities(session, tickers=tickers, sector=sector)
    assert [equity.ticker for equity in equities] == expected


async def test_ensure_ticker_and_sector_accept_known_subjects(session: AsyncSession) -> None:
    """A known ticker, in any case and padding, returns its equity; a known sector passes."""
    equity = await ms.ensure_ticker(session, " scom ")
    assert (equity.ticker, equity.company_name, equity.sector) == (
        "SCOM",
        "Safaricom PLC",
        "Telecommunications",
    )
    assert equity.shares_outstanding == 40_065_428_000
    assert equity.currency == "KES"
    assert await ms.ensure_sector(session, "Banking") is None


@pytest.mark.parametrize(
    ("call", "code", "field", "message"),
    [
        pytest.param(
            lambda s, cfg: ms.ensure_ticker(s, " nope "),
            "unknown_ticker",
            "ticker",
            "Ticker 'NOPE' is not a known equity.",
            id="ensure_ticker",
        ),
        pytest.param(
            lambda s, cfg: ms.ensure_ticker(s, "NOPE", field="tickers"),
            "unknown_ticker",
            "tickers",
            "Ticker 'NOPE' is not a known equity.",
            id="ensure_ticker_field",
        ),
        pytest.param(
            lambda s, cfg: ms.ensure_sector(s, "Agriculture"),
            "unknown_sector",
            "sector",
            "Sector 'Agriculture' has no listed equity.",
            id="ensure_sector",
        ),
        pytest.param(
            lambda s, cfg: ms.get_screener(s, ScreenerParams(sector="Agriculture"), LOCAL, cfg),
            "unknown_sector",
            "sector",
            "Sector 'Agriculture' has no listed equity.",
            id="screener_sector",
        ),
        pytest.param(
            lambda s, cfg: ms.get_screener(
                s, ScreenerParams(watchlist_id=MISSING_WATCHLIST), LOCAL, cfg
            ),
            "watchlist_not_found",
            "watchlist_id",
            f"Watchlist {MISSING_WATCHLIST} was not found.",
            id="screener_watchlist",
        ),
        pytest.param(
            lambda s, cfg: ms.get_valuation_bands(s, cfg, ticker="NOPE"),
            "unknown_ticker",
            "ticker",
            "Ticker 'NOPE' is not a known equity.",
            id="bands_ticker",
        ),
        pytest.param(
            lambda s, cfg: ms.get_valuation_bands(s, cfg, sector="Agriculture"),
            "unknown_sector",
            "sector",
            "Sector 'Agriculture' has no listed equity.",
            id="bands_sector",
        ),
        pytest.param(
            lambda s, cfg: ms.get_anomalies(s, ticker="NOPE"),
            "unknown_ticker",
            "ticker",
            "Ticker 'NOPE' is not a known equity.",
            id="anomalies_ticker",
        ),
        pytest.param(
            lambda s, cfg: ms.get_anomalies(s, sector="Agriculture"),
            "unknown_sector",
            "sector",
            "Sector 'Agriculture' has no listed equity.",
            id="anomalies_sector",
        ),
        pytest.param(
            lambda s, cfg: ms.get_flows(s, ticker="NOPE", settings=cfg),
            "unknown_ticker",
            "ticker",
            "Ticker 'NOPE' is not a known equity.",
            id="flows_ticker",
        ),
        pytest.param(
            lambda s, cfg: ms.get_flows(s, sector="Agriculture", settings=cfg),
            "unknown_sector",
            "sector",
            "Sector 'Agriculture' has no listed equity.",
            id="flows_sector",
        ),
    ],
)
async def test_unknown_subject_is_a_coded_404(
    session: AsyncSession,
    settings: TerminalSettings,
    call: ServiceCall,
    code: str,
    field: str | None,
    message: str,
) -> None:
    """An unknown ticker or sector, or a watchlist id the caller does not own, is a 404 with
    its code, the offending parameter and a message naming the normalised value."""
    with pytest.raises(HTTPException) as excinfo:
        await call(session, settings)
    assert_coded(excinfo.value, 404, code, field, message)


async def test_load_price_rows_groups_history_per_requested_ticker(
    session: AsyncSession,
) -> None:
    """Requested tickers are normalised and each is a key, an unknown one with no rows; rows are
    ascending and bounded inclusively. ``None`` loads every ticker with rows in the range, and
    an empty collection loads nothing. The SCOM 2026-09-30 row carries the seeded close, volume,
    turnover and foreign net flow."""
    prices = await ms.load_price_rows(session, ["scom", "NOPE"], date(2026, 9, 28), LATEST)
    assert sorted(prices) == ["NOPE", "SCOM"]
    assert prices["NOPE"] == []
    assert [row.trade_date for row in prices["SCOM"]] == [
        date(2026, 9, 28),
        date(2026, 9, 29),
        date(2026, 9, 30),
    ]
    latest = prices["SCOM"][-1]
    assert (latest.ticker, latest.close_price, latest.volume) == ("SCOM", Decimal("33.25"), 1393726)
    assert latest.turnover_kes == Decimal("46490053.61")
    assert latest.foreign_net_flow_kes == Decimal("1647872.37")

    everyone = await ms.load_price_rows(session, None, LATEST, LATEST)
    assert sorted(everyone) == sorted(SEEDED_ROWS)
    assert all(len(rows) == 1 for rows in everyone.values())
    assert await ms.load_price_rows(session, [], None, None) == {}


async def test_load_price_rows_in_ranges_reads_only_the_ranges_in_one_statement(
    seeded_database: SeededDatabase, session: AsyncSession
) -> None:
    """Two distant ranges come back as each ticker's rows of both, ascending, exactly the rows
    the two single-range reads give, from one statement; every requested ticker is a key, an
    unknown one empty. No tickers, or no ranges, run no statement."""
    ranges = [(date(2013, 1, 2), date(2013, 1, 8)), (date(2026, 9, 28), LATEST)]
    with captured_statements(seeded_database.engine) as statements:
        prices = await ms.load_price_rows_in_ranges(session, ["kcb", "SCOM", "NOPE"], ranges)

    assert len(reads_of(statements, "FROM terminal_daily_prices")) == 1
    assert list(prices) == ["KCB", "NOPE", "SCOM"]
    assert prices["NOPE"] == []
    for ticker in ("KCB", "SCOM"):
        separately = [
            row
            for start, end in ranges
            for row in (await ms.load_price_rows(session, [ticker], start, end))[ticker]
        ]
        assert prices[ticker] == separately
        assert [row.trade_date for row in prices[ticker]][-3:] == [
            date(2026, 9, 28),
            date(2026, 9, 29),
            LATEST,
        ]
        assert all(row.trade_date.year in (2013, 2026) for row in prices[ticker])

    with captured_statements(seeded_database.engine) as statements:
        assert await ms.load_price_rows_in_ranges(session, [], ranges) == {}
        assert await ms.load_price_rows_in_ranges(session, ["scom", "kcb"], []) == {
            "KCB": [],
            "SCOM": [],
        }
    assert statements == []


async def test_load_fundamentals_rows_returns_every_period_per_ticker(
    session: AsyncSession,
) -> None:
    """The seed holds 131 fiscal years in KES: FY2010-FY2026 for SCOM (31 March) and for EABL
    and KEGN (30 June), FY2010-FY2025 for the five banks (31 December), ascending. Banks have no
    EBITDA, debt or cash. A requested ticker without rows is an empty key; an empty collection
    loads nothing."""
    rows = await ms.load_fundamentals_rows(session, [" scom", "EQTY", "NOPE"])
    assert sorted(rows) == ["EQTY", "NOPE", "SCOM"]
    assert rows["NOPE"] == []
    assert [row.period_end_date for row in rows["SCOM"]] == [
        date(year, 3, 31) for year in range(2010, 2027)
    ]
    assert [row.period_end_date for row in rows["EQTY"]] == [
        date(year, 12, 31) for year in range(2010, 2026)
    ]
    assert all(row.ebitda_kes is not None for row in rows["SCOM"])
    assert {
        (row.ebitda_kes, row.total_debt_kes, row.cash_and_equivalents_kes) for row in rows["EQTY"]
    } == {(None, None, None)}

    everyone = await ms.load_fundamentals_rows(session, None)
    assert sorted(everyone) == sorted(SEEDED_ROWS)
    assert {ticker: len(periods) for ticker, periods in everyone.items()} == {
        **dict.fromkeys(["EABL", "KEGN", "SCOM"], 17),
        **dict.fromkeys(SORTED_BANKS, 16),
    }
    assert {
        (row.period_type, row.currency) for periods in everyone.values() for row in periods
    } == {("FY", "KES")}
    assert [row.period_end_date for row in everyone["EABL"]] == [
        date(year, 6, 30) for year in range(2010, 2027)
    ]
    assert await ms.load_fundamentals_rows(session, []) == {}


# ---------------------------------------------------------------------------------------------
# Market service: GET /market/screener
# ---------------------------------------------------------------------------------------------


async def test_screener_values_every_seeded_counter_at_the_latest_session(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """Defaults: ``as_of`` 2026-09-30, the 20-row Z-score window and the 2.0 threshold, rows by
    market cap descending. Market cap is close x shares exactly; P/E, P/B and dividend yield are
    the seeded targets; the five banks have a null EV/EBITDA with ``ev_not_meaningful_for_banks``
    and the other counters carry no flag."""
    response = await ms.get_screener(session, ScreenerParams(), LOCAL, settings)

    assert (response.as_of, response.z_window, response.anomaly_threshold) == (
        LATEST,
        20,
        Decimal("2.0"),
    )
    assert response.count == 8
    assert [row.ticker for row in response.rows] == DEFAULT_ORDER
    for row in response.rows:
        sector, close, market_cap, pe, pb, ev_ebitda, dividend_yield = SEEDED_ROWS[row.ticker]
        assert (row.sector, row.currency, row.trade_date) == (sector, "KES", LATEST)
        assert row.close_price == Decimal(close)
        assert row.market_cap_kes == Decimal(market_cap)
        assert (row.pe, row.pb) == (Decimal(pe), Decimal(pb))
        assert row.ev_ebitda == (None if ev_ebitda is None else Decimal(ev_ebitda))
        assert row.dividend_yield_pct == Decimal(dividend_yield)
        assert row.flags == (["ev_not_meaningful_for_banks"] if sector == "Banking" else [])
        assert not (row.is_volume_anomaly or row.is_turnover_anomaly)

    scom = response.rows[0]
    assert (scom.company_name, scom.eps_kes, scom.change_pct) == (
        "Safaricom PLC",
        Decimal("2.38"),
        Decimal("-0.06"),
    )
    assert (scom.volume, scom.turnover_kes) == (1393726, Decimal("46490053.61"))
    assert (scom.volume_z, scom.turnover_z) == (Decimal("-1.15"), Decimal("-1.16"))
    assert scom.free_float_pct == Decimal("25.00")
    assert scom.fundamentals_period_end == date(2026, 3, 31)


async def test_screener_sector_aggregates_cover_the_universe_independent_of_filters(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """One aggregate per sector, sorted by name, even when the filters keep one row. Banking sums
    its five members' market caps and has a P/E but a null EV/EBITDA with
    ``ev_not_meaningful_for_banks`` and no EV/EBITDA-eligible member; a one-member sector's
    aggregates equal that member's multiples."""
    response = await ms.get_screener(
        session, ScreenerParams(sector="Telecommunications"), LOCAL, settings
    )
    assert [row.ticker for row in response.rows] == ["SCOM"]
    aggregates = {aggregate.sector: aggregate for aggregate in response.sector_aggregates}
    assert [aggregate.sector for aggregate in response.sector_aggregates] == SORTED_SECTORS

    banking = aggregates["Banking"]
    assert banking.constituent_count == 5
    assert banking.market_cap_kes == Decimal("1122666959710.50")
    assert (banking.pe, banking.pb, banking.ev_ebitda, banking.dividend_yield_pct) == (
        Decimal("4.48"),
        Decimal("0.90"),
        None,
        Decimal("8.16"),
    )
    assert (
        banking.pe_eligible_count,
        banking.pb_eligible_count,
        banking.ev_ebitda_eligible_count,
        banking.dy_eligible_count,
    ) == (5, 5, 0, 5)
    assert banking.flags == ["ev_not_meaningful_for_banks"]

    for sector, ticker in (
        ("Energy & Petroleum", "KEGN"),
        (MANUFACTURING, "EABL"),
        ("Telecommunications", "SCOM"),
    ):
        aggregate = aggregates[sector]
        _sector, _close, market_cap, pe, pb, ev_ebitda, dividend_yield = SEEDED_ROWS[ticker]
        assert ev_ebitda is not None
        assert aggregate.constituent_count == 1
        assert aggregate.market_cap_kes == Decimal(market_cap)
        assert (aggregate.pe, aggregate.pb, aggregate.ev_ebitda, aggregate.dividend_yield_pct) == (
            Decimal(pe),
            Decimal(pb),
            Decimal(ev_ebitda),
            Decimal(dividend_yield),
        )
        assert (
            aggregate.pe_eligible_count,
            aggregate.pb_eligible_count,
            aggregate.ev_ebitda_eligible_count,
            aggregate.dy_eligible_count,
        ) == (1, 1, 1, 1)
        assert aggregate.flags == []


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        pytest.param(
            ScreenerParams(sector="Banking"), ["EQTY", "KCB", "COOP", "ABSA", "SBIC"], id="sector"
        ),
        pytest.param(
            ScreenerParams(min_pe=Decimal("5.50"), max_pe=Decimal("12.00")),
            ["EABL", "ABSA", "SBIC", "KEGN"],
            id="pe_bounds_inclusive",
        ),
        pytest.param(
            ScreenerParams(min_pe=Decimal("4.00"), max_pe=Decimal("4.00")),
            ["KCB", "COOP"],
            id="pe_min_equal_to_max",
        ),
        pytest.param(
            ScreenerParams(min_pb=Decimal("0.90"), max_pb=Decimal("1.30")),
            ["EQTY", "ABSA", "SBIC"],
            id="pb_bounds",
        ),
        pytest.param(
            ScreenerParams(min_ev_ebitda=Decimal("0")),
            ["SCOM", "EABL", "KEGN"],
            id="null_ev_ebitda_fails_filter",
        ),
        pytest.param(
            ScreenerParams(max_ev_ebitda=Decimal("7.00")), ["EABL", "KEGN"], id="ev_ebitda_max"
        ),
        pytest.param(
            ScreenerParams(min_dividend_yield=Decimal("8.33")),
            ["COOP", "ABSA", "SBIC"],
            id="dividend_yield_min",
        ),
        pytest.param(
            ScreenerParams(min_volume_z=Decimal("0.01")), ["COOP", "KEGN"], id="volume_z_min"
        ),
        pytest.param(
            ScreenerParams(sector="Banking", min_dividend_yield=Decimal("10")),
            ["COOP", "ABSA"],
            id="filters_combine",
        ),
        pytest.param(ScreenerParams(anomalies_only=True), [], id="no_spike_on_latest_session"),
    ],
)
async def test_screener_filters_compare_quantized_values_inclusively(
    session: AsyncSession, settings: TerminalSettings, params: ScreenerParams, expected: list[str]
) -> None:
    """Every bound is inclusive on the 2 dp value the client sees, a null metric (bank
    EV/EBITDA) fails any filter on it, filters combine, and the order stays market cap
    descending. No counter spikes on 2026-09-30."""
    assert await screener_tickers(session, settings, params) == expected


@pytest.mark.parametrize(
    ("sort", "order", "expected"),
    [
        ("ticker", "asc", ["ABSA", "COOP", "EABL", "EQTY", "KCB", "KEGN", "SBIC", "SCOM"]),
        ("ticker", "desc", ["SCOM", "SBIC", "KEGN", "KCB", "EQTY", "EABL", "COOP", "ABSA"]),
        ("market_cap_kes", "desc", DEFAULT_ORDER),
        ("market_cap_kes", "asc", DEFAULT_ORDER[::-1]),
        ("pe", "asc", ["COOP", "KCB", "EQTY", "ABSA", "KEGN", "SBIC", "EABL", "SCOM"]),
        ("pe", "desc", ["SCOM", "EABL", "KEGN", "SBIC", "ABSA", "EQTY", "COOP", "KCB"]),
        ("pb", "asc", ["KEGN", "COOP", "KCB", "EQTY", "SBIC", "ABSA", "SCOM", "EABL"]),
        ("pb", "desc", ["EABL", "SCOM", "ABSA", "SBIC", "EQTY", "COOP", "KCB", "KEGN"]),
        ("ev_ebitda", "asc", ["KEGN", "EABL", "SCOM", "ABSA", "COOP", "EQTY", "KCB", "SBIC"]),
        ("ev_ebitda", "desc", ["SCOM", "EABL", "KEGN", "ABSA", "COOP", "EQTY", "KCB", "SBIC"]),
        (
            "dividend_yield_pct",
            "asc",
            ["EABL", "KEGN", "SCOM", "EQTY", "KCB", "SBIC", "ABSA", "COOP"],
        ),
        (
            "dividend_yield_pct",
            "desc",
            ["ABSA", "COOP", "SBIC", "KCB", "EQTY", "EABL", "KEGN", "SCOM"],
        ),
        ("volume_z", "asc", ["EQTY", "SBIC", "SCOM", "ABSA", "KCB", "EABL", "KEGN", "COOP"]),
        ("volume_z", "desc", ["COOP", "KEGN", "EABL", "KCB", "ABSA", "SCOM", "EQTY", "SBIC"]),
    ],
)
async def test_screener_sorts_with_nulls_last_and_ties_by_ticker(
    session: AsyncSession,
    settings: TerminalSettings,
    sort: ScreenerSort,
    order: SortOrder,
    expected: list[str],
) -> None:
    """Rows sort by the quantized column in either direction. The banks' null EV/EBITDA comes
    last in both orders, and equal values (P/E 4.00 for COOP and KCB, P/B 0.80, dividend yield
    5.00 and 10.00, volume Z -1.18) keep ticker order in both directions too."""
    params = ScreenerParams(sort=sort, order=order)
    assert await screener_tickers(session, settings, params) == expected


async def test_screener_watchlist_filter_keeps_only_the_callers_tickers(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """The seeded "NSE Core 8" watchlist of ``local``, cut down to SCOM and KCB on this clone,
    keeps those two rows and combines with the sector filter; another owner gets a 404 for the
    same id, so watchlists never leak across principals."""
    watchlist_id = await session.scalar(
        select(TerminalWatchlist.id).where(
            TerminalWatchlist.owner_key == "local", TerminalWatchlist.name == "NSE Core 8"
        )
    )
    assert watchlist_id is not None
    await session.execute(
        delete(TerminalWatchlistItem).where(
            TerminalWatchlistItem.watchlist_id == watchlist_id,
            TerminalWatchlistItem.ticker.not_in(["SCOM", "KCB"]),
        )
    )
    await session.commit()

    params = ScreenerParams(watchlist_id=watchlist_id)
    assert await screener_tickers(session, settings, params) == ["SCOM", "KCB"]
    params = ScreenerParams(watchlist_id=watchlist_id, sector="Banking")
    assert await screener_tickers(session, settings, params) == ["KCB"]
    with pytest.raises(HTTPException) as excinfo:
        await ms.get_screener(
            session, ScreenerParams(watchlist_id=watchlist_id), Principal("intruder"), settings
        )
    assert_coded(
        excinfo.value,
        404,
        "watchlist_not_found",
        "watchlist_id",
        f"Watchlist {watchlist_id} was not found.",
    )


async def test_screener_anomalies_only_finds_the_planted_eabl_spike(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """As of 2026-09-29 the seeder's planted EABL volume spike (Z >= 4.00) is kept by
    ``anomalies_only``, and every kept row spikes at or above the 2.0 threshold."""
    response = await ms.get_screener(
        session, ScreenerParams(anomalies_only=True, as_of=date(2026, 9, 29)), LOCAL, settings
    )
    assert response.as_of == date(2026, 9, 29)
    rows = {row.ticker: row for row in response.rows}
    eabl = rows["EABL"]
    assert eabl.trade_date == date(2026, 9, 29)
    assert eabl.is_volume_anomaly
    assert eabl.volume_z is not None
    assert eabl.volume_z >= Decimal("4.00")
    for row in response.rows:
        assert row.is_volume_anomaly or row.is_turnover_anomaly
        spikes = [z for z in (row.volume_z, row.turnover_z) if z is not None]
        assert max(spikes) >= Decimal("2.0")


@pytest.mark.parametrize(
    ("as_of", "trade_date", "period_end"),
    [
        pytest.param(date(2026, 9, 27), date(2026, 9, 25), date(2025, 6, 30), id="before_lag"),
        pytest.param(date(2026, 9, 28), date(2026, 9, 28), date(2026, 6, 30), id="lag_reached"),
    ],
)
async def test_screener_values_at_the_latest_close_with_lagged_fundamentals(
    session: AsyncSession,
    settings: TerminalSettings,
    as_of: date,
    trade_date: date,
    period_end: date,
) -> None:
    """A Sunday ``as_of`` uses Friday's close. EABL's FY2026 (30 June 2026) becomes available on
    2026-09-28, exactly 90 days later, and FY2025 is used the day before."""
    response = await ms.get_screener(session, ScreenerParams(as_of=as_of), LOCAL, settings)
    eabl = next(row for row in response.rows if row.ticker == "EABL")
    assert response.as_of == as_of
    assert (eabl.trade_date, eabl.fundamentals_period_end) == (trade_date, period_end)


async def test_screener_on_the_first_session_has_no_change_or_z_score(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """On 2012-01-02 each counter has one row: no previous close gives a null change with
    ``change_pct:no_previous_close``, and an empty window gives null Z-scores with prefixed
    ``insufficient_observations`` reasons and no anomaly. The fiscal years available then are
    FY2010 for the banks and FY2011 for SCOM."""
    response = await ms.get_screener(session, ScreenerParams(as_of=FIRST_SESSION), LOCAL, settings)
    assert response.count == 8
    for row in response.rows:
        assert row.trade_date == FIRST_SESSION
        assert (row.change_pct, row.volume_z, row.turnover_z) == (None, None, None)
        assert "change_pct:no_previous_close" in row.flags
        assert "volume_z:insufficient_observations" in row.flags
        assert "turnover_z:insufficient_observations" in row.flags
        assert not (row.is_volume_anomaly or row.is_turnover_anomaly)
    periods = {row.ticker: row.fundamentals_period_end for row in response.rows}
    assert periods["SCOM"] == date(2011, 3, 31)
    assert {periods[bank] for bank in SORTED_BANKS} == {date(2010, 12, 31)}


async def test_screener_before_the_first_price_is_empty(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """An ``as_of`` before any stored price echoes the date with no row and no aggregate."""
    response = await ms.get_screener(
        session, ScreenerParams(as_of=date(2011, 12, 30)), LOCAL, settings
    )
    assert (response.as_of, response.count, response.rows, response.sector_aggregates) == (
        date(2011, 12, 30),
        0,
        [],
        [],
    )


async def test_constituent_without_prices_is_counted_but_never_valued(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """With SBIC's history removed on this clone it has no screener row, yet it remains one of
    Banking's five constituents; the eligible counts drop to four and the totals exclude it. The
    Banking band leaves it out on every day too, so its current value is the four-bank P/E."""
    await session.execute(delete(TerminalDailyPrice).where(TerminalDailyPrice.ticker == "SBIC"))
    await session.commit()

    response = await ms.get_screener(session, ScreenerParams(), LOCAL, settings)
    assert [row.ticker for row in response.rows] == [t for t in DEFAULT_ORDER if t != "SBIC"]
    banking = next(a for a in response.sector_aggregates if a.sector == "Banking")
    assert banking.constituent_count == 5
    assert (banking.pe_eligible_count, banking.pb_eligible_count, banking.dy_eligible_count) == (
        4,
        4,
        4,
    )
    assert banking.market_cap_kes == Decimal("1015732456631.50")

    bands = await ms.get_valuation_bands(session, settings, sector="Banking")
    assert (bands.status, bands.flags) == ("ok", [])
    assert bands.stats is not None
    assert bands.stats.current == banking.pe


# ---------------------------------------------------------------------------------------------
# Market service: GET /market/sector-heatmap
# ---------------------------------------------------------------------------------------------


async def test_sector_heatmap_has_twelve_month_end_periods_for_four_sectors(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """The default heatmap ends at 2026-09-30 with the last session on or before each of the 12
    month-ends (a weekend month-end moves back to Friday). The market row covers all 8 counters
    with no relative return; each of the 4 sector rows has 12 cells whose relative return is the
    sector return minus the market return, within the 0.01 of rounding each separately."""
    response = await ms.get_sector_heatmap(session, settings=settings)

    assert (response.as_of, response.interval) == (LATEST, "month")
    assert response.period_ends == [
        date(2025, 10, 31),
        date(2025, 11, 28),
        date(2025, 12, 31),
        date(2026, 1, 30),
        date(2026, 2, 27),
        date(2026, 3, 31),
        date(2026, 4, 30),
        date(2026, 5, 29),
        date(2026, 6, 30),
        date(2026, 7, 31),
        date(2026, 8, 31),
        date(2026, 9, 30),
    ]
    assert [cell.period_end for cell in response.market] == response.period_ends
    for cell in response.market:
        assert cell.return_pct is not None
        assert (cell.relative_return_pct, cell.constituents) == (None, 8)
    assert [row.sector for row in response.sectors] == SORTED_SECTORS
    members = dict(zip(SORTED_SECTORS, (5, 1, 1, 1), strict=True))
    for row in response.sectors:
        assert [cell.period_end for cell in row.cells] == response.period_ends
        for cell, market in zip(row.cells, response.market, strict=True):
            assert cell.constituents == members[row.sector]
            assert cell.return_pct is not None
            assert market.return_pct is not None
            assert cell.relative_return_pct is not None
            difference = cell.return_pct - market.return_pct
            assert abs(cell.relative_return_pct - difference) <= Decimal("0.01")


async def test_weekly_sector_heatmap_ends_on_fridays_and_as_of(session: AsyncSession) -> None:
    """Weekly boundaries are Fridays; the latest period runs to the Wednesday ``as_of``. No
    settings are needed when prices exist."""
    response = await ms.get_sector_heatmap(session, interval="week", periods=4)
    assert (response.as_of, response.interval) == (LATEST, "week")
    assert response.period_ends == [
        date(2026, 9, 11),
        date(2026, 9, 18),
        date(2026, 9, 25),
        date(2026, 9, 30),
    ]
    assert all(len(row.cells) == 4 for row in response.sectors)


async def test_sector_heatmap_without_two_boundaries_is_empty(session: AsyncSession) -> None:
    """On the first session the previous month-end has no session, so one boundary remains and
    no period can be formed."""
    response = await ms.get_sector_heatmap(session, periods=1, as_of=FIRST_SESSION)
    assert response.as_of == FIRST_SESSION
    assert (response.period_ends, response.market, response.sectors) == ([], [], [])


# ---------------------------------------------------------------------------------------------
# Market service: GET /market/valuation-bands
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("subject", "subject_type", "current"),
    [
        pytest.param({"ticker": "SCOM"}, "ticker", Decimal("14.00"), id="ticker"),
        pytest.param({"sector": "Banking"}, "sector", Decimal("4.48"), id="sector"),
    ],
)
async def test_valuation_bands_over_the_default_five_years(
    session: AsyncSession,
    settings: TerminalSettings,
    subject: dict[str, str],
    subject_type: str,
    current: Decimal,
) -> None:
    """Five years back from 2026-09-30 is fully covered: ``status = "ok"`` with statistics over
    every daily P/E, at least 250 weekly points ending on ``as_of``, and a current value equal
    to the screener's P/E of the counter (14.00) or the Banking aggregate (4.48)."""
    response = await ms.get_valuation_bands(session, settings, **subject)

    name = next(iter(subject.values()))
    assert (response.subject, response.subject_type, response.metric) == (
        name,
        subject_type,
        "pe",
    )
    assert response.as_of == LATEST
    assert response.coverage.requested_start == date(2021, 9, 30)
    assert response.coverage.coverage_start == date(2021, 9, 30)
    assert not response.coverage.is_partial_coverage
    assert (response.status, response.flags) == ("ok", [])
    assert len(response.series) >= 250
    assert response.series[-1].date == LATEST
    assert response.series[-1].value == current
    stats = response.stats
    assert stats is not None
    assert stats.current == current
    sessions = await ms.load_market_sessions(session, date(2021, 9, 30), LATEST)
    assert stats.observations == len(sessions)
    assert stats.min <= stats.p25 <= stats.median <= stats.p75 <= stats.max
    assert stats.minus_2sd <= stats.minus_1sd <= stats.mean <= stats.plus_1sd <= stats.plus_2sd


async def test_fifteen_year_price_to_book_band_reports_partial_coverage(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """Fifteen years back is 2011-09-30, but history starts on 2012-01-02, so coverage is
    partial while the statistics still use every one of the 3,762 sessions; the current P/B is
    SCOM's 4.50. The ticker is normalised."""
    response = await ms.get_valuation_bands(
        session, settings, ticker="scom", metric="pb", lookback_years=15
    )
    assert (response.subject, response.metric, response.status) == ("SCOM", "pb", "ok")
    assert response.coverage.requested_start == date(2011, 9, 30)
    assert response.coverage.coverage_start == FIRST_SESSION
    assert response.coverage.is_partial_coverage
    assert response.stats is not None
    assert (response.stats.observations, response.stats.current) == (3762, Decimal("4.50"))


async def test_short_valuation_band_has_no_statistics(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """One year back from 2012-02-15 leaves 33 sessions, fewer than the 60 the statistics need:
    ``insufficient_history`` with the weekly series but no ``stats``."""
    response = await ms.get_valuation_bands(
        session, settings, ticker="KCB", lookback_years=1, as_of=date(2012, 2, 15)
    )
    assert (response.status, response.flags, response.stats) == (
        "insufficient_history",
        ["insufficient_history"],
        None,
    )
    assert response.coverage.requested_start == date(2011, 2, 15)
    assert response.coverage.coverage_start == FIRST_SESSION
    assert response.coverage.is_partial_coverage
    assert response.series
    assert response.series[-1].date == date(2012, 2, 15)


async def add_sparse_counter(
    session: AsyncSession, ticker: str, sector: str, closes: dict[date, Decimal]
) -> None:
    """Add a KES counter of ``sector`` to this clone with a price row on each day of ``closes``
    only: 1,000,000 shares and FY2025 net income 100,000 KES (available from 2026-03-31), so its
    P/E on a day is that day's close x 10."""
    session.add(
        TerminalEquity(
            ticker=ticker,
            company_name=f"{ticker} Holdings",
            sector=sector,
            shares_outstanding=1_000_000,
            source="csv_import",
        )
    )
    await session.flush()
    session.add(
        TerminalFundamental(
            ticker=ticker,
            period_end_date=date(2025, 12, 31),
            period_type="FY",
            net_income_kes=Decimal("100000"),
            source_by_field={"net_income_kes": "csv_import"},
        )
    )
    session.add_all(
        TerminalDailyPrice(
            ticker=ticker,
            trade_date=day,
            close_price=close,
            volume=1_000,
            turnover_kes=close * 1_000,
            source="csv_import",
        )
        for day, close in closes.items()
    )
    await session.commit()


async def test_sector_band_has_values_only_on_sessions_a_member_traded(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """Sector "QA Edge": QSTL closes 10.00 only on Thursday 2026-09-03 and Tuesday 2026-09-15;
    QE1 to QE4 close 15.00 only on 2026-09-30. The sector has a daily value on those three
    sessions alone: 100.00, 100.00, then 150.00 (60,000,000 / 400,000), QSTL's 15-day-old close
    being left out (with it, 140.00). The weeks of 2026-09-07 and 2026-09-21, in which no member
    traded, get no point although QSTL's close is at most 10 days old there; each point is dated
    on the last session of its week."""
    await add_sparse_counter(
        session,
        "QSTL",
        "QA Edge",
        {date(2026, 9, 3): Decimal("10.00"), date(2026, 9, 15): Decimal("10.00")},
    )
    for ticker in ("QE1", "QE2", "QE3", "QE4"):
        await add_sparse_counter(session, ticker, "QA Edge", {LATEST: Decimal("15.00")})

    response = await ms.get_valuation_bands(session, settings, sector="QA Edge", lookback_years=1)

    assert (response.status, response.flags, response.stats) == (
        "insufficient_history",
        ["insufficient_history"],
        None,
    )
    coverage = response.coverage
    assert (coverage.requested_start, coverage.coverage_start, coverage.is_partial_coverage) == (
        date(2025, 9, 30),
        date(2026, 9, 3),
        True,
    )
    assert [(point.date, point.value) for point in response.series] == [
        (date(2026, 9, 4), Decimal("100.00")),
        (date(2026, 9, 18), Decimal("100.00")),
        (LATEST, Decimal("150.00")),
    ]


@pytest.mark.parametrize(
    ("sessions_back", "status", "coverage_start", "stats"),
    [
        pytest.param(70, "insufficient_history", date(2026, 6, 25), None, id="56-closes"),
        pytest.param(
            100,
            "ok",
            date(2026, 5, 13),
            (
                80,
                Decimal("115.00"),
                Decimal("11.25"),
                Decimal("115.00"),
                Decimal("130.00"),
                Decimal("100.00"),
            ),
            id="80-closes",
        ),
    ],
)
async def test_one_member_sector_band_equals_its_counter_band(
    session: AsyncSession,
    settings: TerminalSettings,
    sessions_back: int,
    status: str,
    coverage_start: date,
    stats: tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal] | None,
) -> None:
    """QWK, the only member of "QA Week", closes on each of the last ``sessions_back`` sessions
    except Fridays (14 of 70, 20 of 100), cycling 10.00, 11.00, 12.00 and 13.00. The sector has
    the counter's sessions and values, so status, flags, coverage, weekly series and statistics
    are the counter's; no Friday is filled with Thursday's close. 56 values are too few for
    statistics. 80 values are 20 each of P/E 100 to 130: mean and median 115.00, sample deviation
    11.25 (the square root of 10,000 / 79), current 130.00 at percentile 100.00."""
    sessions = (await ms.load_market_sessions(session))[-sessions_back:]
    days = [day for day in sessions if day.isoweekday() != 5]
    await add_sparse_counter(
        session, "QWK", "QA Week", {day: Decimal(10 + i % 4) for i, day in enumerate(days)}
    )

    counter = await ms.get_valuation_bands(session, settings, ticker="QWK", lookback_years=1)
    sector = await ms.get_valuation_bands(session, settings, sector="QA Week", lookback_years=1)

    assert (counter.status, counter.coverage.coverage_start) == (status, coverage_start)
    assert counter.series[-1].date == LATEST
    assert counter.series[-1].value == Decimal("130.00")
    observed = counter.stats
    assert (
        None
        if observed is None
        else (
            observed.observations,
            observed.mean,
            observed.stdev,
            observed.median,
            observed.current,
            observed.current_percentile,
        )
    ) == stats
    assert (sector.status, sector.flags, sector.coverage, sector.series, sector.stats) == (
        counter.status,
        counter.flags,
        counter.coverage,
        counter.series,
        counter.stats,
    )


# ---------------------------------------------------------------------------------------------
# Market service: GET /market/anomalies
# ---------------------------------------------------------------------------------------------


async def test_anomaly_feed_includes_the_three_planted_volume_spikes(
    session: AsyncSession,
) -> None:
    """The default feed (window 20, threshold 2.0, last 20 sessions: 2026-09-03 to 2026-09-30)
    includes the seeder's SCOM 2026-09-15, KCB 2026-09-24 and EABL 2026-09-29 volume spikes, each
    at Z >= 4.00 over 20 observations. Every item is a positive spike in that range, sorted by
    date and then Z-score, both descending."""
    response = await ms.get_anomalies(session)

    assert (response.window, response.threshold) == (20, Decimal("2.0"))
    volume = {
        (item.ticker, item.trade_date): item for item in response.items if item.metric == "volume"
    }
    for planted in (
        ("SCOM", date(2026, 9, 15)),
        ("KCB", date(2026, 9, 24)),
        ("EABL", date(2026, 9, 29)),
    ):
        item = volume[planted]
        assert item.z_score >= Decimal("4.00")
        assert item.observations_used == 20
        assert item.value > item.rolling_mean
        assert item.value == item.value.to_integral_value()
    keys = [(item.trade_date, item.z_score) for item in response.items]
    assert keys == sorted(keys, reverse=True)
    for item in response.items:
        assert date(2026, 9, 3) <= item.trade_date <= LATEST
        assert item.z_score >= Decimal("2.0")
        assert item.metric in ("volume", "turnover")


@pytest.mark.parametrize(
    ("ticker", "sector", "metric", "threshold", "lookback", "first_day", "candidates", "planted"),
    [
        pytest.param(
            "kcb",
            None,
            "volume",
            Decimal("4"),
            20,
            date(2026, 9, 3),
            {"KCB"},
            [("KCB", date(2026, 9, 24), "volume")],
            id="ticker_volume",
        ),
        pytest.param(
            None,
            "Banking",
            "turnover",
            Decimal("5"),
            20,
            date(2026, 9, 3),
            set(SORTED_BANKS),
            [("KCB", date(2026, 9, 24), "turnover")],
            id="sector_turnover",
        ),
        pytest.param(
            "SCOM",
            None,
            "both",
            Decimal("2.0"),
            12,
            date(2026, 9, 15),
            {"SCOM"},
            [("SCOM", date(2026, 9, 15), "turnover"), ("SCOM", date(2026, 9, 15), "volume")],
            id="lookback_reaches_spike",
        ),
        pytest.param(
            "SCOM",
            None,
            "both",
            Decimal("2.0"),
            11,
            date(2026, 9, 16),
            {"SCOM"},
            [],
            id="lookback_stops_after_spike",
        ),
        pytest.param(
            "SCOM", "Banking", "both", Decimal("2.0"), 20, LATEST, set(), [], id="outside_sector"
        ),
    ],
)
async def test_anomaly_feed_filters_by_subject_metric_threshold_and_lookback(
    session: AsyncSession,
    ticker: str | None,
    sector: str | None,
    metric: AnomalyMetric,
    threshold: Decimal,
    lookback: int,
    first_day: date,
    candidates: set[str],
    planted: list[tuple[str, date, str]],
) -> None:
    """Items are restricted to the ticker, or the sector's members (a ticker outside the given
    sector gives none), to the requested metric, to Z-scores at or above the threshold and to the
    last ``lookback_sessions`` sessions: 12 sessions reach back to SCOM's 2026-09-15 spike, 11
    stop the day after. The planted spikes stay included, and on one day the higher Z-score
    (SCOM turnover, 4.16 against 4.00) comes first."""
    response = await ms.get_anomalies(
        session,
        metric=metric,
        threshold=threshold,
        lookback_sessions=lookback,
        ticker=ticker,
        sector=sector,
    )

    assert (response.window, response.threshold) == (20, threshold)
    metrics = {"volume", "turnover"} if metric == "both" else {metric}
    for item in response.items:
        assert item.ticker in candidates
        assert item.metric in metrics
        assert item.z_score >= threshold
        assert first_day <= item.trade_date <= LATEST
    found = [(item.ticker, item.trade_date, item.metric) for item in response.items]
    assert [item for item in found if item in planted] == planted


@pytest.mark.parametrize("metric", ["turnover", "both"])
@pytest.mark.parametrize(
    ("ticker", "kes_turnover_spikes"),
    [
        pytest.param(None, {("SCOM", date(2026, 9, 15)), ("KCB", date(2026, 9, 24))}, id="market"),
        pytest.param("eabl", set(), id="ticker"),
    ],
)
async def test_anomaly_feed_scores_no_turnover_of_an_equity_not_listed_in_kes(
    session: AsyncSession,
    ticker: str | None,
    kes_turnover_spikes: set[tuple[str, date]],
    metric: AnomalyMetric,
) -> None:
    """With EABL switched to USD on this clone, the feed loses exactly EABL's turnover items,
    its 2026-09-29 turnover spike among them, and converts nothing. EABL's volume, in shares, is
    still scored, so ``both`` keeps the planted 2026-09-29 volume spike, and every turnover spike
    of a KES-listed equity stays, SCOM 2026-09-15 and KCB 2026-09-24 in the market feed."""
    before = await ms.get_anomalies(session, metric=metric, ticker=ticker)
    assert ("EABL", date(2026, 9, 29), "turnover") in {
        (item.ticker, item.trade_date, item.metric) for item in before.items
    }

    await session.execute(
        update(TerminalEquity).where(TerminalEquity.ticker == "EABL").values(currency="USD")
    )
    await session.commit()
    after = await ms.get_anomalies(session, metric=metric, ticker=ticker)

    assert after.items == [
        item for item in before.items if (item.ticker, item.metric) != ("EABL", "turnover")
    ]
    found = {(item.ticker, item.trade_date, item.metric) for item in after.items}
    assert (("EABL", date(2026, 9, 29), "volume") in found) == (metric == "both")
    assert kes_turnover_spikes <= {(name, day) for name, day, kind in found if kind == "turnover"}


# ---------------------------------------------------------------------------------------------
# Market service: GET /market/flows
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("subject", "name"),
    [
        pytest.param({"ticker": "SCOM"}, "SCOM", id="ticker"),
        pytest.param({"sector": "Banking"}, "Banking", id="sector"),
        pytest.param({}, "market", id="market"),
    ],
)
async def test_flows_default_to_the_year_to_the_latest_session(
    session: AsyncSession, settings: TerminalSettings, subject: dict[str, str], name: str
) -> None:
    """``as_of`` and ``end`` are 2026-09-30 and ``start`` 2025-09-30; every seeded row has a
    foreign flow, so coverage is 100.00 over all 254 sessions. On every point local = -foreign,
    the cumulative flow is the running sum of the foreign flow, and the 20-session rolling sum
    starts on the 20th session; the totals match the series."""
    response = await ms.get_flows(session, settings=settings, **subject)

    assert (response.subject, response.as_of, response.start, response.end) == (
        name,
        LATEST,
        date(2025, 9, 30),
        LATEST,
    )
    assert (response.status, response.flags, response.coverage_pct) == (
        "ok",
        [],
        Decimal("100.00"),
    )
    assert len(response.series) == 254
    assert response.series[0].trade_date == date(2025, 9, 30)
    assert response.series[-1].trade_date == LATEST
    running = Decimal("0")
    for index, point in enumerate(response.series):
        assert point.foreign_net_kes is not None
        assert point.local_net_kes == -point.foreign_net_kes
        running += point.foreign_net_kes
        assert point.cumulative_foreign_net_kes == running
        assert (point.rolling_foreign_net_kes is None) == (index < 19)
        assert point.turnover_kes is not None
        assert point.turnover_kes > 0
    totals = response.totals
    assert totals is not None
    assert (totals.sessions_with_data, totals.sessions_in_range) == (254, 254)
    assert totals.foreign_net_kes == running
    assert totals.local_net_kes == -running


async def test_flows_over_an_explicit_range_and_rolling_window(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """An explicit three-session range is served as given: the 5-session rolling sum never fills,
    and the KCB total equals the running sum on the last day."""
    response = await ms.get_flows(
        session,
        ticker="KCB",
        start=date(2026, 9, 28),
        end=LATEST,
        rolling_window=5,
        settings=settings,
    )
    assert (response.start, response.end) == (date(2026, 9, 28), LATEST)
    assert [point.trade_date for point in response.series] == [
        date(2026, 9, 28),
        date(2026, 9, 29),
        date(2026, 9, 30),
    ]
    assert all(point.rolling_foreign_net_kes is None for point in response.series)
    assert response.series[-1].foreign_net_kes == Decimal("-4863768.81")
    assert response.series[-1].cumulative_foreign_net_kes == Decimal("-2513051.82")
    assert response.totals is not None
    assert response.totals.foreign_net_kes == Decimal("-2513051.82")
    assert response.totals.local_net_kes == Decimal("2513051.82")
    assert (response.totals.sessions_with_data, response.totals.sessions_in_range) == (3, 3)


async def test_flows_range_follows_an_overridden_as_of(session: AsyncSession) -> None:
    """``end`` defaults to the requested ``as_of`` and ``start`` to 365 days before it."""
    response = await ms.get_flows(session, ticker="EABL", as_of=date(2026, 6, 30))
    assert (response.as_of, response.start, response.end) == (
        date(2026, 6, 30),
        date(2025, 6, 30),
        date(2026, 6, 30),
    )
    assert response.series[-1].trade_date == date(2026, 6, 30)


async def test_flows_without_a_session_in_range_report_no_flow_data(
    session: AsyncSession,
) -> None:
    """A range after the last session has no flow data: ``no_flow_data``, coverage 0.00, null
    totals and an empty series."""
    response = await ms.get_flows(session, start=date(2026, 10, 1), end=date(2026, 10, 31))
    assert (response.as_of, response.status, response.flags) == (
        LATEST,
        "no_flow_data",
        ["no_flow_data"],
    )
    assert (response.coverage_pct, response.totals, response.series) == (
        Decimal("0.00"),
        None,
        [],
    )


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        pytest.param(
            date(2026, 10, 1),
            None,
            "start (2026-10-01) must not be after end (2026-09-30).",
            id="start_after_default_end",
        ),
        pytest.param(
            LATEST,
            date(2026, 9, 1),
            "start (2026-09-30) must not be after end (2026-09-01).",
            id="explicit_range",
        ),
    ],
)
async def test_flows_start_after_end_is_a_coded_422(
    session: AsyncSession, start: date, end: date | None, message: str
) -> None:
    """A ``start`` after the resolved ``end`` (default the latest session) is refused against
    ``start``."""
    with pytest.raises(HTTPException) as excinfo:
        await ms.get_flows(session, start=start, end=end)
    assert_coded(excinfo.value, 422, "start_after_end", "start", message)


@pytest.mark.parametrize(
    ("subject", "flags"),
    [
        pytest.param(
            {"ticker": "SBIC"}, ["no_flow_data", "currency_mismatch"], id="non_kes_ticker"
        ),
        pytest.param({"sector": "Banking"}, ["currency_mismatch"], id="sector_with_non_kes"),
        pytest.param({}, ["currency_mismatch"], id="market_with_non_kes"),
        pytest.param({"ticker": "SCOM"}, [], id="kes_ticker"),
    ],
)
async def test_flows_sum_only_constituents_listed_in_kes(
    session: AsyncSession, settings: TerminalSettings, subject: dict[str, str], flags: list[str]
) -> None:
    """With SBIC switched to USD on this clone its flows are refused, never converted, and the
    series still has the 254 sessions of the observed market calendar. SBIC alone leaves no KES
    constituent: ``no_flow_data`` plus ``currency_mismatch``, coverage 0.00, null totals and a
    null flow on every session. Banking and the market are flagged ``currency_mismatch`` and
    equal their all-KES flows minus SBIC's, session by session and in total; stored amounts have
    2 dp, so the differences are exact. SCOM, listed in KES, is unchanged and unflagged."""
    baseline = await ms.get_flows(session, settings=settings, **subject)
    sbic = await ms.get_flows(session, settings=settings, ticker="SBIC")
    assert (sbic.flags, sbic.coverage_pct) == ([], Decimal("100.00"))
    await session.execute(
        update(TerminalEquity).where(TerminalEquity.ticker == "SBIC").values(currency="USD")
    )
    await session.commit()

    response = await ms.get_flows(session, settings=settings, **subject)

    assert response.flags == flags
    assert (response.subject, response.as_of, response.start, response.end) == (
        baseline.subject,
        LATEST,
        date(2025, 9, 30),
        LATEST,
    )
    days = [point.trade_date for point in baseline.series]
    assert len(days) == 254
    assert [point.trade_date for point in response.series] == days
    if "currency_mismatch" not in flags:
        assert response == baseline
        return
    if "no_flow_data" in flags:
        assert (response.status, response.coverage_pct, response.totals) == (
            "no_flow_data",
            Decimal("0.00"),
            None,
        )
        for point in response.series:
            assert (
                point.foreign_net_kes,
                point.local_net_kes,
                point.rolling_foreign_net_kes,
                point.turnover_kes,
                point.foreign_net_pct_of_turnover,
                point.cumulative_foreign_net_kes,
            ) == (None, None, None, None, None, Decimal("0.00"))
        return

    assert (response.status, response.coverage_pct) == ("ok", Decimal("100.00"))
    turnover_total = Decimal(0)
    for point, whole, own in zip(response.series, baseline.series, sbic.series, strict=True):
        assert whole.foreign_net_kes is not None and own.foreign_net_kes is not None
        assert whole.turnover_kes is not None and own.turnover_kes is not None
        foreign = whole.foreign_net_kes - own.foreign_net_kes
        turnover = whole.turnover_kes - own.turnover_kes
        turnover_total += turnover
        assert (point.foreign_net_kes, point.local_net_kes, point.turnover_kes) == (
            foreign,
            -foreign,
            turnover,
        )
        assert point.foreign_net_pct_of_turnover == quantize_out(foreign * 100 / turnover, "pct")
        assert point.cumulative_foreign_net_kes == (
            whole.cumulative_foreign_net_kes - own.cumulative_foreign_net_kes
        )
        if whole.rolling_foreign_net_kes is None or own.rolling_foreign_net_kes is None:
            assert point.rolling_foreign_net_kes is None
        else:
            assert point.rolling_foreign_net_kes == (
                whole.rolling_foreign_net_kes - own.rolling_foreign_net_kes
            )
    assert baseline.totals is not None and sbic.totals is not None
    totals = response.totals
    assert totals is not None
    foreign_total = baseline.totals.foreign_net_kes - sbic.totals.foreign_net_kes
    assert (totals.foreign_net_kes, totals.local_net_kes) == (foreign_total, -foreign_total)
    assert totals.foreign_net_pct_of_turnover == quantize_out(
        foreign_total * 100 / turnover_total, "pct"
    )
    assert (totals.sessions_with_data, totals.sessions_in_range) == (254, 254)


# ---------------------------------------------------------------------------------------------
# Market service: Empty price table and a non-KES equity
# ---------------------------------------------------------------------------------------------


async def test_empty_price_table_gives_empty_results_as_of_today(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """With every price row removed on this clone there is no session; each endpoint answers
    with today's Africa/Nairobi date as ``as_of`` and empty results instead of an error."""
    await session.execute(delete(TerminalDailyPrice))
    await session.commit()
    before = nairobi_today()

    assert await ms.latest_trade_date(session) is None
    assert await ms.load_market_sessions(session) == []
    screener = await ms.get_screener(session, ScreenerParams(), LOCAL, settings)
    heatmap = await ms.get_sector_heatmap(session)
    bands = await ms.get_valuation_bands(session, settings, ticker="SCOM")
    anomalies = await ms.get_anomalies(session)
    flows = await ms.get_flows(session, settings=settings)

    today = {before, nairobi_today()}
    assert screener.as_of in today
    assert (screener.count, screener.rows, screener.sector_aggregates) == (0, [], [])
    assert heatmap.as_of in today
    assert (heatmap.period_ends, heatmap.market, heatmap.sectors) == ([], [], [])
    assert bands.as_of in today
    assert (bands.status, bands.flags, bands.series, bands.stats) == (
        "insufficient_history",
        ["insufficient_history"],
        [],
        None,
    )
    assert bands.coverage.coverage_start is None
    assert anomalies.items == []
    assert flows.as_of in today
    assert (flows.subject, flows.end, flows.status) == ("market", flows.as_of, "no_flow_data")
    assert (flows.coverage_pct, flows.totals, flows.series) == (Decimal("0.00"), None, [])


async def test_equity_not_listed_in_kes_is_never_converted(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """With SBIC switched to USD on this clone nothing is converted. Its screener row keeps the
    close but has no market cap or multiple, with ``currency_mismatch``, and the Banking
    aggregate is flagged and valued over the other four. The heatmap leaves it out (Banking 4
    constituents, market 7). Its own P/E band has no value (``no_positive_earnings`` plus
    ``currency_mismatch``), and the Banking band is flagged and ends at the four-bank P/E."""
    await session.execute(
        update(TerminalEquity).where(TerminalEquity.ticker == "SBIC").values(currency="USD")
    )
    await session.commit()

    screener = await ms.get_screener(session, ScreenerParams(sector="Banking"), LOCAL, settings)
    sbic = next(row for row in screener.rows if row.ticker == "SBIC")
    assert (sbic.currency, sbic.close_price) == ("USD", Decimal("270.50"))
    assert (sbic.market_cap_kes, sbic.pe, sbic.pb, sbic.dividend_yield_pct) == (
        None,
        None,
        None,
        None,
    )
    assert sbic.flags == ["currency_mismatch", "ev_not_meaningful_for_banks"]
    banking = next(a for a in screener.sector_aggregates if a.sector == "Banking")
    assert banking.flags == ["currency_mismatch", "ev_not_meaningful_for_banks"]
    assert (banking.constituent_count, banking.pe_eligible_count) == (5, 4)
    assert banking.market_cap_kes == Decimal("1015732456631.50")

    heatmap = await ms.get_sector_heatmap(session, periods=1, settings=settings)
    assert heatmap.period_ends == [LATEST]
    assert heatmap.market[0].constituents == 7
    banking_row = next(row for row in heatmap.sectors if row.sector == "Banking")
    assert banking_row.cells[0].constituents == 4

    counter = await ms.get_valuation_bands(session, settings, ticker="SBIC")
    assert (counter.status, counter.flags, counter.series, counter.stats) == (
        "no_positive_earnings",
        ["no_positive_earnings", "currency_mismatch"],
        [],
        None,
    )
    sector = await ms.get_valuation_bands(session, settings, sector="Banking")
    assert (sector.status, sector.flags) == ("ok", ["currency_mismatch"])
    assert sector.stats is not None
    assert sector.stats.current == banking.pe


# ---------------------------------------------------------------------------------------------
# Macro service: KPI snapshots, read bounds, calendar ends and get_macro_overview on seeded clones
# ---------------------------------------------------------------------------------------------
#
# The KPI snapshots are built from each indicator's latest and previous non-null values, which
# the service reads in SQL; the pure step that turns them into snapshots is exercised here on
# literal observations. ``yield_curve_at`` takes the FXD observations dated from ``day`` minus
# 365 days (``date.min`` at the earliest) to ``day``; the opening at ``date.min`` is held here,
# and the rest of ``yield_curve_at`` and the argument check of ``get_macro_overview`` are held
# by ``tests/unit/services/test_macro_service.py``. The read bounds are the pure date arithmetic
# that limits the T-bill and bond reads to the dates the window's curves can use, and the
# window's month-ends and coverage at the calendar's ends are pure as well.
#
# ``get_macro_overview(session, history_months)`` runs on a fresh seeded clone per test (the root
# conftest's ``seeded_database``, through the market section's ``session`` fixture); a test that
# adds or deletes rows changes only its own clone. The contract held there:
#
# - **Window and coverage**: exactly ``history_months`` month-ends ending at the month of the
#   latest macro observation. ``requested_start`` is the first of them (2024-10-31 for 24
#   months, 2006-10-31 for 240), ``coverage_start`` the first window date with any indicator,
#   and coverage is partial when there is none or it falls more than one month later
#   (2012-01-31 at 240 months). Stored dates from 0001-01-01 to 9999-12-31 never raise: a window
#   reaching back before January of year 1 is cut there, and the months it lost still count
#   towards partial coverage.
# - **latest**: all eight indicators, each resolved to its own latest and previous non-null
#   values (inflation 6.80 on 2026-09-30 after 6.60, CBR 8.75), so a newer reserves-only row
#   moves the window and the reserves tile but hides no other tile.
# - **Series**: one point per window row that carries the series, oldest first; a row without
#   any rate is left out of ``rates_series``.
# - **Curves**: ``current`` at the later of the latest macro and latest FXD bond dates (3 seeded
#   T-bills and 6 ``seed`` bonds on 2026-09-30), and one snapshot per window month-end. A real
#   FXD bond outranks the seeded ones in its bucket, and codes other than FXD are never read.
# - **Missing data**: with only bonds stored the window ends at the latest bond month and
#   ``latest`` and both series are empty; with nothing stored every list is empty and the window
#   ends at the current UTC month.
#
# Seeded values are those of ``seed/reference_data.py``: the 2026-09-30 row of the attachment
# verbatim with CBR 8.75 filled from CBK, August 2026 inflation 6.60, the other August values
# from the deterministic synthetic history, and bonds of the 364-day yield plus each tenor's
# spread with no noise on 2026-09-30. Every expected value is an exact literal.


# ---------------------------------------------------------------------------------------------
# Macro service: KPI snapshots
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("column", "observed", "expected"),
    [
        pytest.param(
            "inflation_yoy_pct",
            {
                "inflation_yoy_pct": [
                    AsOfValue(Decimal("6.8050"), date(2026, 9, 30)),
                    AsOfValue(Decimal("6.6040"), date(2026, 8, 31)),
                ]
            },
            ("6.81", "2026-09-30", "6.60", "0.20"),
            id="change_on_unrounded_values",
        ),
        pytest.param(
            "cbr_rate_pct",
            {
                "cbr_rate_pct": [
                    AsOfValue(Decimal("8.7500"), date(2026, 9, 30)),
                    AsOfValue(Decimal("12.9950"), date(2024, 3, 31)),
                ]
            },
            ("8.75", "2026-09-30", "13.00", "-4.25"),
            id="sparse_column_previous_years_back",
        ),
        pytest.param(
            "tbill_91d_pct",
            {"tbill_91d_pct": [AsOfValue(Decimal("15.7234"), date(2026, 9, 30))]},
            ("15.72", "2026-09-30", None, None),
            id="latest_without_previous",
        ),
        pytest.param(
            "interbank_rate_pct",
            {},
            (None, None, None, None),
            id="never_observed_absent",
        ),
        pytest.param(
            "interbank_rate_pct",
            {"interbank_rate_pct": []},
            (None, None, None, None),
            id="never_observed_empty",
        ),
        pytest.param(
            "fx_kes_usd",
            {
                "fx_kes_usd": [
                    AsOfValue(Decimal("129.2049"), date(2026, 9, 30)),
                    AsOfValue(Decimal("129.1650"), date(2026, 8, 31)),
                ]
            },
            ("129.20", "2026-09-30", "129.17", "0.04"),
            id="fx_money_kind",
        ),
        pytest.param(
            "fx_reserves_usd_m",
            {
                "fx_reserves_usd_m": [
                    AsOfValue(Decimal("7850.00"), date(2026, 9, 26)),
                    AsOfValue(Decimal("7819.15"), date(2026, 9, 19)),
                    AsOfValue(Decimal("7001.00"), date(2026, 9, 12)),
                ]
            },
            ("7850.00", "2026-09-26", "7819.15", "30.85"),
            id="entries_after_the_second_ignored",
        ),
    ],
)
def test_indicator_snapshot_takes_the_latest_and_previous_non_null_values(
    column: str,
    observed: dict[str, list[AsOfValue]],
    expected: tuple[str | None, str | None, str | None, str | None],
) -> None:
    """Each tile shows its column's latest value and date and the non-null value before it,
    however far back that lies, and ``change`` is their difference on the unrounded values,
    rounded once half-up to 2 dp: 6.8050 - 6.6040 = 0.2010 -> 0.20 (rounding first would give
    6.81 - 6.60 = 0.21), and 129.2049 - 129.1650 = 0.0399 -> 0.04 (not 0.03). A column with no
    observation has a null value, date, previous value and change; every other tile is null here.
    """
    snapshots = macro_service._indicator_snapshots(observed)
    dumped = [snapshot.model_dump(mode="json") for snapshot in snapshots]

    assert [item["key"] for item in dumped] == list(MACRO_INDICATOR_COLUMNS)
    fields = {
        item["key"]: (item["value"], item["as_of"], item["previous_value"], item["change"])
        for item in dumped
    }
    assert fields.pop(column) == expected
    assert set(fields.values()) == {(None, None, None, None)}


# ---------------------------------------------------------------------------------------------
# Macro service: Yield curve bond window at date.min
# ---------------------------------------------------------------------------------------------


def bond(
    observation_date: date,
    bond_code: str,
    remaining_tenor_years: str,
    yield_pct: str = "13.5000",
    source: str = "cbk_bonds",
) -> BondObservation:
    """One FXD observation; tenor and yield are decimal literals, converted exactly."""
    return BondObservation(
        observation_date=observation_date,
        bond_code=bond_code,
        remaining_tenor_years=Decimal(remaining_tenor_years),
        yield_pct=Decimal(yield_pct),
        source=source,
    )


@pytest.mark.parametrize(
    ("day", "observation_date"),
    [
        pytest.param(date(1, 1, 31), date(1, 1, 15), id="january_of_year_one"),
        pytest.param(date(1, 12, 31), date.min, id="364_days_after_date_min"),
    ],
)
def test_bond_window_opens_at_date_min_within_365_days_of_it(
    day: date, observation_date: date
) -> None:
    """A curve dated less than 365 days after 0001-01-01 cannot look back a full year, so its
    bond window opens at ``date.min`` instead of overflowing, and keeps every FXD observation
    from there up to the day: a 10.00-year bond fills the 10Y bucket at 13.50."""
    observation = bond(observation_date, "FXD1/0001/010", "10.00")

    points = yield_curve_at([], {}, [observation], day)

    observed = observation_date.isoformat()
    assert [point_fields(point) for point in points] == [
        ("10Y", "10.00", "13.50", "bond", "FXD1/0001/010", observed, "cbk_bonds")
    ]


# ---------------------------------------------------------------------------------------------
# Macro service: Read bounds
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "lookback", "start"),
    [
        pytest.param(date(2024, 10, 31), timedelta(days=62), date(2024, 8, 30), id="tbill"),
        pytest.param(date(2024, 10, 31), timedelta(days=365), date(2023, 11, 1), id="bond"),
        pytest.param(date(1, 1, 31), timedelta(days=62), date.min, id="clamped_at_date_min"),
    ],
)
def test_reads_start_the_lookback_before_the_earliest_curve_day(
    day: date, lookback: timedelta, start: date
) -> None:
    """The window reads open 62 days (T-bills) or 365 days (bonds) before the earliest curve
    date: 2024-10-31 gives 2024-08-30 and 2023-11-01. A start before the first calendar day is
    clamped to ``date.min`` rather than overflowing."""
    assert macro_service._lookback_start(day, lookback) == start


@pytest.mark.parametrize(
    ("month_ends", "current_day", "ranges"),
    [
        pytest.param(
            [
                *(date(2024, 10, 31), date(2024, 11, 30), date(2024, 12, 31), date(2025, 1, 31)),
                *(date(2025, 2, 28), date(2025, 3, 31), date(2025, 4, 30), date(2025, 5, 31)),
                *(date(2025, 6, 30), date(2025, 7, 31), date(2025, 8, 31), date(2025, 9, 30)),
                *(date(2025, 10, 31), date(2025, 11, 30), date(2025, 12, 31), date(2026, 1, 31)),
                *(date(2026, 2, 28), date(2026, 3, 31), date(2026, 4, 30), date(2026, 5, 31)),
                *(date(2026, 6, 30), date(2026, 7, 31), date(2026, 8, 31), date(2026, 9, 30)),
            ],
            date(2026, 9, 30),
            ((date(2023, 11, 1), date(2026, 9, 30)),),
            id="seeded_24_months_overlap",
        ),
        pytest.param(
            [date(2000, 1, 31)],
            date(2026, 1, 31),
            (
                (date(1999, 1, 31), date(2000, 1, 31)),
                (date(2025, 1, 31), date(2026, 1, 31)),
            ),
            id="widely_separated_curves",
        ),
        pytest.param(
            [date(2024, 1, 31)],
            date(2025, 1, 31),
            ((date(2023, 1, 31), date(2025, 1, 31)),),
            id="touching_intervals_merged",
        ),
        pytest.param(
            [date(2024, 1, 31)],
            date(2025, 2, 1),
            (
                (date(2023, 1, 31), date(2024, 1, 31)),
                (date(2024, 2, 2), date(2025, 2, 1)),
            ),
            id="one_day_gap_kept_apart",
        ),
        pytest.param(
            [date(2025, 12, 31)],
            date(2025, 12, 15),
            ((date(2024, 12, 15), date(2025, 12, 31)),),
            id="one_month_window_mid_month",
        ),
        pytest.param(
            [date(1, 1, 31), date(1, 2, 28)],
            date(1, 2, 28),
            ((date.min, date(1, 2, 28)),),
            id="clamped_at_date_min",
        ),
    ],
)
def test_bond_reads_cover_only_the_curve_intervals(
    month_ends: list[date], current_day: date, ranges: tuple[tuple[date, date], ...]
) -> None:
    """The bond read spans the history curves, from 365 days before the earliest curve date to
    the last month-end, and the current curve, the 365 days up to ``current_day``. Overlapping
    or touching intervals (2024-01-31 and 2024-02-01) merge into one range; a one-day gap or a
    26-year gap leaves two, so the bonds between them are never loaded. A ``current_day``
    before a one-month window's month-end moves the start back to 365 days before it."""
    assert macro_service._bond_read_ranges(month_ends, current_day) == ranges


# ---------------------------------------------------------------------------------------------
# Macro service: Window and coverage at the calendar's ends
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("end", "history_months", "expected"),
    [
        pytest.param(
            date(2024, 3, 15),
            3,
            [date(2024, 1, 31), date(2024, 2, 29), date(2024, 3, 31)],
            id="fits_the_calendar",
        ),
        pytest.param(
            date(1, 3, 31),
            3,
            [date(1, 1, 31), date(1, 2, 28), date(1, 3, 31)],
            id="opens_exactly_at_january_of_year_one",
        ),
        pytest.param(
            date(1, 3, 31),
            24,
            [date(1, 1, 31), date(1, 2, 28), date(1, 3, 31)],
            id="cut_at_january_of_year_one",
        ),
        pytest.param(date(1, 1, 31), 24, [date(1, 1, 31)], id="only_january_of_year_one"),
        pytest.param(date(9999, 12, 31), 1, [date(9999, 12, 31)], id="december_9999"),
        pytest.param(
            date(9999, 12, 15),
            2,
            [date(9999, 11, 30), date(9999, 12, 31)],
            id="ends_at_december_9999",
        ),
    ],
)
def test_window_holds_history_months_month_ends_cut_at_january_of_year_one(
    end: date, history_months: int, expected: list[date]
) -> None:
    """The window holds exactly ``history_months`` month-ends ending at ``end``'s month (February
    2024 ends on the 29th), fewer only when it would reach back before January of year 1, where
    it opens instead; it always holds ``end``'s own month."""
    assert macro_service._window_month_ends(end, history_months) == expected


@pytest.mark.parametrize(
    ("observed_on", "requested_start", "months_cut", "coverage_start", "is_partial"),
    [
        pytest.param(
            date(2024, 11, 30), date(2024, 10, 31), 0, "2024-11-30", False, id="one_month_later"
        ),
        pytest.param(
            date(2024, 12, 1), date(2024, 10, 31), 0, "2024-12-01", True, id="past_one_month_later"
        ),
        pytest.param(
            date(9999, 12, 31), date(9999, 12, 31), 0, "9999-12-31", False, id="december_9999"
        ),
        pytest.param(None, date(9999, 12, 31), 0, None, True, id="december_9999_unobserved"),
        pytest.param(
            date(9999, 12, 31), date(9999, 11, 30), 0, "9999-12-31", False, id="november_9999"
        ),
        pytest.param(
            date(1, 1, 31), date(1, 1, 31), 1, "0001-01-31", False, id="one_cut_first_month"
        ),
        pytest.param(
            date(1, 2, 28), date(1, 1, 31), 1, "0001-02-28", True, id="one_cut_second_month"
        ),
        pytest.param(None, date(1, 1, 31), 1, None, True, id="one_cut_unobserved"),
        pytest.param(
            date(1, 1, 31), date(1, 1, 31), 2, "0001-01-31", True, id="two_cut_always_partial"
        ),
        pytest.param(
            date(1, 1, 31), date(1, 1, 31), 23, "0001-01-31", True, id="23_cut_always_partial"
        ),
    ],
)
def test_coverage_is_partial_past_one_month_after_the_uncut_window_start(
    observed_on: date | None,
    requested_start: date,
    months_cut: int,
    coverage_start: str | None,
    is_partial: bool,
) -> None:
    """Coverage is partial when nothing is observed in the window, or the first observation falls
    after the month-end one month past the window's first month. A window cut at January of year
    1 still has that first month ``months_cut`` months before ``requested_start``: with one month
    cut the threshold is 0001-01-31 itself, and with two or more it lies before year 1, so every
    observation is later. A window opening in December 9999 has no later month-end, so only a
    missing observation makes it partial."""
    macro_rows = (
        [] if observed_on is None else [MacroRow(observed_on, inflation_yoy_pct=Decimal("5.00"))]
    )

    coverage = macro_service._coverage(macro_rows, requested_start, months_cut)

    assert coverage.model_dump(mode="json") == {
        "requested_start": requested_start.isoformat(),
        "coverage_start": coverage_start,
        "is_partial_coverage": is_partial,
    }


# ---------------------------------------------------------------------------------------------
# Macro service: Endpoint on seeded PostgreSQL
# ---------------------------------------------------------------------------------------------

SEED_ANCHOR = date(2026, 9, 30)
SEED_BOND_TENORS = (2, 5, 10, 15, 20, 25)

LatestRow = tuple[str, str | None, str | None, str | None, str | None]
CurveRow = tuple[str, str, str, str, str | None, str, str]
SnapshotRow = tuple[str, str | None, str, str]

# (key, value, as_of, previous_value, change): the 2026-09-30 row against the 2026-08-31 one.
SEEDED_LATEST: list[LatestRow] = [
    ("inflation_yoy_pct", "6.80", "2026-09-30", "6.60", "0.20"),
    ("cbr_rate_pct", "8.75", "2026-09-30", "8.92", "-0.17"),
    ("interbank_rate_pct", "12.85", "2026-09-30", "12.51", "0.34"),
    ("tbill_91d_pct", "15.72", "2026-09-30", "15.53", "0.19"),
    ("tbill_182d_pct", "16.58", "2026-09-30", "16.46", "0.12"),
    ("tbill_364d_pct", "16.84", "2026-09-30", "16.72", "0.12"),
    ("fx_kes_usd", "129.20", "2026-09-30", "129.17", "0.03"),
    ("fx_reserves_usd_m", "7850.00", "2026-09-30", "7819.15", "30.85"),
]

SEEDED_RATES_ON_ANCHOR = {
    "observation_date": "2026-09-30",
    "cbr_rate_pct": "8.75",
    "interbank_rate_pct": "12.85",
    "tbill_91d_pct": "15.72",
    "tbill_182d_pct": "16.58",
    "tbill_364d_pct": "16.84",
}

# (tenor_label, tenor_years, yield_pct, instrument, bond_code, observation_date, source) on
# 2026-09-30: bond yields are the 364-day 16.84 plus 0.20, 0.45, 0.70, 0.90, 1.05 and 1.15.
SEEDED_TBILL_POINTS: list[CurveRow] = [
    ("91D", "0.25", "15.72", "tbill", None, "2026-09-30", "seed"),
    ("182D", "0.50", "16.58", "tbill", None, "2026-09-30", "seed"),
    ("364D", "1.00", "16.84", "tbill", None, "2026-09-30", "seed"),
]
SEEDED_BOND_POINTS: list[CurveRow] = [
    ("2Y", "2.00", "17.04", "bond", "FXD-SEED-2Y-202609", "2026-09-30", "seed"),
    ("5Y", "5.00", "17.29", "bond", "FXD-SEED-5Y-202609", "2026-09-30", "seed"),
    ("10Y", "10.00", "17.54", "bond", "FXD-SEED-10Y-202609", "2026-09-30", "seed"),
    ("15Y", "15.00", "17.74", "bond", "FXD-SEED-15Y-202609", "2026-09-30", "seed"),
    ("20Y", "20.00", "17.89", "bond", "FXD-SEED-20Y-202609", "2026-09-30", "seed"),
    ("25Y", "25.00", "17.99", "bond", "FXD-SEED-25Y-202609", "2026-09-30", "seed"),
]
SEEDED_CURVE: list[CurveRow] = [*SEEDED_TBILL_POINTS, *SEEDED_BOND_POINTS]


def month_end(day: date, months_later: int = 0) -> date:
    """The last day of the calendar month ``months_later`` months after ``day``'s month."""
    year, month_index = divmod(day.year * 12 + day.month - 1 + months_later, 12)
    return date(year, month_index + 1, monthrange(year, month_index + 1)[1])


def month_ends(first: date, count: int) -> list[date]:
    """``count`` consecutive month-ends, oldest first, starting at ``first``'s month."""
    return [month_end(first, offset) for offset in range(count)]


def latest_fields(snapshot: IndicatorSnapshot) -> LatestRow:
    """A KPI snapshot as ``(key, value, as_of, previous_value, change)`` JSON strings."""
    dumped = snapshot.model_dump(mode="json")
    return (
        dumped["key"],
        dumped["value"],
        dumped["as_of"],
        dumped["previous_value"],
        dumped["change"],
    )


def point_fields(point: YieldCurvePoint) -> CurveRow:
    """A curve point as a :data:`CurveRow` of JSON strings."""
    dumped = point.model_dump(mode="json")
    return (
        dumped["tenor_label"],
        dumped["tenor_years"],
        dumped["yield_pct"],
        dumped["instrument"],
        dumped["bond_code"],
        dumped["observation_date"],
        dumped["source"],
    )


def snapshot_fields(points: Sequence[YieldCurvePoint]) -> list[SnapshotRow]:
    """Each point's ``(tenor_label, bond_code, observation_date, source)``, in curve order."""
    return [
        (point.tenor_label, point.bond_code, point.observation_date.isoformat(), point.source)
        for point in points
    ]


def seeded_snapshot(day: date) -> list[SnapshotRow]:
    """The seeded curve at month-end ``day``: that month's three bills and six seed bonds."""
    observed = day.isoformat()
    bills: list[SnapshotRow] = [
        (label, None, observed, "seed") for label in ("91D", "182D", "364D")
    ]
    bonds: list[SnapshotRow] = [
        (f"{tenor}Y", f"FXD-SEED-{tenor}Y-{day:%Y%m}", observed, "seed")
        for tenor in SEED_BOND_TENORS
    ]
    return [*bills, *bonds]


@pytest.mark.parametrize(
    ("history_months", "requested_start", "coverage_start", "is_partial", "observed_months"),
    [
        pytest.param(24, date(2024, 10, 31), date(2024, 10, 31), False, 24, id="default_24_months"),
        pytest.param(1, date(2026, 9, 30), date(2026, 9, 30), False, 1, id="one_month"),
        pytest.param(
            240, date(2006, 10, 31), date(2012, 1, 31), True, 177, id="maximum_240_months"
        ),
    ],
)
async def test_seeded_overview_window_coverage_series_and_curves(
    session: AsyncSession,
    history_months: int,
    requested_start: date,
    coverage_start: date,
    is_partial: bool,
    observed_months: int,
) -> None:
    """The seeded overview ends its window at 2026-09-30 for every ``history_months``.

    24 months open at 2024-10-31 and are fully covered: 24 inflation and rates points. The
    240-month maximum opens at 2006-10-31, but the 177 seeded month-ends start at 2012-01-31,
    more than one month later, so coverage is partial and the 63 month-end curves before it are
    empty. ``latest`` holds all eight indicators against August 2026, and the current curve is
    the 3 T-bills and 6 ``seed`` bonds of 2026-09-30; every other month-end curve takes that
    month's bills and seed bonds, which shows the reads open early enough for the first one.
    """
    overview = await get_macro_overview(session, history_months)

    window = month_ends(requested_start, history_months)
    observed = month_ends(coverage_start, observed_months)
    assert window[-1] == observed[-1] == SEED_ANCHOR
    assert overview.coverage.model_dump(mode="json") == {
        "requested_start": requested_start.isoformat(),
        "coverage_start": coverage_start.isoformat(),
        "is_partial_coverage": is_partial,
    }
    assert [latest_fields(snapshot) for snapshot in overview.latest] == SEEDED_LATEST
    assert [point.date for point in overview.inflation_series] == observed
    assert overview.inflation_series[-1].model_dump(mode="json") == {
        "date": "2026-09-30",
        "value": "6.80",
    }
    assert [point.observation_date for point in overview.rates_series] == observed
    assert overview.rates_series[-1].model_dump(mode="json") == SEEDED_RATES_ON_ANCHOR
    assert [point_fields(point) for point in overview.yield_curve.current] == SEEDED_CURVE
    history = overview.yield_curve.history
    assert [snapshot.as_of for snapshot in history] == window
    assert [snapshot_fields(snapshot.points) for snapshot in history] == [
        seeded_snapshot(day) if day >= coverage_start else [] for day in window
    ]
    assert [point_fields(point) for point in history[-1].points] == SEEDED_CURVE


async def test_newer_reserves_only_row_moves_the_window_and_hides_no_indicator(
    session: AsyncSession,
) -> None:
    """A weekly reserves bulletin dated 2026-10-02 is the latest macro row, so the 24-month window
    now runs from 2024-11-30 to 2026-10-31.

    Only the reserves tile moves (7901.25 after 7850.00, change 51.25); every other indicator keeps
    its 2026-09-30 value, because each column is resolved on its own. The row carries no
    inflation and no rate, so both series stop at 2026-09-30, and the curves at 2026-10-02 and
    2026-10-31 still take the 2026-09-30 bills, within their 62-day limit.
    """
    session.add(
        TerminalMacroIndicator(
            observation_date=date(2026, 10, 2),
            fx_reserves_usd_m=Decimal("7901.25"),
            source_by_field={"fx_reserves_usd_m": "cbk_reserves"},
        )
    )
    await session.commit()

    overview = await get_macro_overview(session, 24)

    window = month_ends(date(2024, 11, 30), 24)
    assert window[-1] == date(2026, 10, 31)
    assert overview.coverage.model_dump(mode="json") == {
        "requested_start": "2024-11-30",
        "coverage_start": "2024-11-30",
        "is_partial_coverage": False,
    }
    assert [latest_fields(snapshot) for snapshot in overview.latest] == [
        *SEEDED_LATEST[:-1],
        ("fx_reserves_usd_m", "7901.25", "2026-10-02", "7850.00", "51.25"),
    ]
    assert [point.date for point in overview.inflation_series] == window[:-1]
    assert [point.observation_date for point in overview.rates_series] == window[:-1]
    assert [point_fields(point) for point in overview.yield_curve.current] == SEEDED_CURVE
    history = overview.yield_curve.history
    assert [snapshot.as_of for snapshot in history] == window
    assert [point_fields(point) for point in history[-1].points] == SEEDED_CURVE


async def test_real_fxd_bonds_outrank_seeded_ones_and_other_codes_are_never_read(
    session: AsyncSession,
) -> None:
    """Real CBK auctions are stored beside the seeded curve.

    ``FXD1/2026/010`` (2026-06-15, 9.72 years left) takes the 10Y bucket from every later seed
    bond, from the 2026-06-30 month-end on. ``FXD2/2026/005`` (2026-10-14) is the latest FXD
    observation, so the current curve is resolved on 2026-10-14 and takes it for 5Y, while the
    window still ends at the latest macro month, 2026-09, whose curves precede it. The infra
    bond ``IFB1/2026/012`` (2026-09-15, 9.90 years left) is real and newer than ``FXD1/2026/010``
    but is never read. Yields round half-up: 13.9050 -> 13.91 and 16.2049 -> 16.20.
    """
    session.add_all(
        [
            TerminalBondYield(
                observation_date=date(2026, 6, 15),
                bond_code="FXD1/2026/010",
                original_tenor_years=Decimal("10.00"),
                remaining_tenor_years=Decimal("9.72"),
                maturity_date=date(2036, 3, 3),
                yield_pct=Decimal("13.9050"),
                source="cbk_bonds",
            ),
            TerminalBondYield(
                observation_date=date(2026, 9, 15),
                bond_code="IFB1/2026/012",
                original_tenor_years=Decimal("12.00"),
                remaining_tenor_years=Decimal("9.90"),
                maturity_date=date(2036, 8, 11),
                yield_pct=Decimal("14.0000"),
                source="cbk_bonds",
            ),
            TerminalBondYield(
                observation_date=date(2026, 10, 14),
                bond_code="FXD2/2026/005",
                original_tenor_years=Decimal("5.00"),
                remaining_tenor_years=Decimal("5.00"),
                maturity_date=date(2031, 10, 6),
                yield_pct=Decimal("16.2049"),
                source="cbk_bonds",
            ),
        ]
    )
    await session.commit()

    overview = await get_macro_overview(session, 24)

    window = month_ends(date(2024, 10, 31), 24)
    assert overview.coverage.model_dump(mode="json") == {
        "requested_start": "2024-10-31",
        "coverage_start": "2024-10-31",
        "is_partial_coverage": False,
    }
    assert [latest_fields(snapshot) for snapshot in overview.latest] == SEEDED_LATEST
    assert [point_fields(point) for point in overview.yield_curve.current] == [
        *SEEDED_TBILL_POINTS,
        SEEDED_BOND_POINTS[0],
        ("5Y", "5.00", "16.20", "bond", "FXD2/2026/005", "2026-10-14", "cbk_bonds"),
        ("10Y", "9.72", "13.91", "bond", "FXD1/2026/010", "2026-06-15", "cbk_bonds"),
        *SEEDED_BOND_POINTS[3:],
    ]
    expected_history: list[list[SnapshotRow]] = []
    for day in window:
        rows = seeded_snapshot(day)
        if day >= date(2026, 6, 30):
            rows[5] = ("10Y", "FXD1/2026/010", "2026-06-15", "cbk_bonds")
        expected_history.append(rows)
    history = overview.yield_curve.history
    assert [snapshot.as_of for snapshot in history] == window
    assert [snapshot_fields(snapshot.points) for snapshot in history] == expected_history


async def test_bonds_only_database_ends_the_window_at_the_latest_bond(
    session: AsyncSession,
) -> None:
    """With every macro row deleted, the curve still renders: the window ends at the latest FXD
    bond month, 2026-09, and every curve holds that month's six seed bonds. Without an indicator
    ``latest`` and both series are empty, and coverage is partial with no ``coverage_start``."""
    await session.execute(delete(TerminalMacroIndicator))
    await session.commit()

    overview = await get_macro_overview(session, 24)

    window = month_ends(date(2024, 10, 31), 24)
    assert overview.coverage.model_dump(mode="json") == {
        "requested_start": "2024-10-31",
        "coverage_start": None,
        "is_partial_coverage": True,
    }
    assert overview.latest == []
    assert overview.inflation_series == []
    assert overview.rates_series == []
    assert [point_fields(point) for point in overview.yield_curve.current] == SEEDED_BOND_POINTS
    history = overview.yield_curve.history
    assert [snapshot.as_of for snapshot in history] == window
    assert [snapshot_fields(snapshot.points) for snapshot in history] == [
        seeded_snapshot(day)[3:] for day in window
    ]


async def test_empty_database_returns_empty_lists_ending_at_the_current_month(
    session: AsyncSession,
) -> None:
    """With no macro row and no bond, nothing raises: every list is empty, coverage is partial
    with no ``coverage_start``, and the 24-month window ends at the current UTC month, so it
    opens at the month-end 23 months earlier. The date is read before and after the call, so a
    month boundary crossed during it cannot fail the test."""
    await session.execute(delete(TerminalMacroIndicator))
    await session.execute(delete(TerminalBondYield))
    await session.commit()

    before = datetime.now(UTC).date()
    overview = await get_macro_overview(session, 24)
    after = datetime.now(UTC).date()

    dumped = overview.model_dump(mode="json")
    requested_start = dumped["coverage"].pop("requested_start")
    assert requested_start in {month_end(day, -23).isoformat() for day in (before, after)}
    assert dumped == {
        "coverage": {"coverage_start": None, "is_partial_coverage": True},
        "latest": [],
        "inflation_series": [],
        "rates_series": [],
        "yield_curve": {"current": [], "history": []},
    }


YEAR_ONE_CURVE: list[CurveRow] = [
    ("91D", "0.25", "7.50", "tbill", None, "0001-01-31", "csv_import"),
    ("10Y", "10.00", "13.50", "bond", "FXD1/0001/010", "0001-01-15", "cbk_bonds"),
]
YEAR_9999_CURVE: list[CurveRow] = [
    ("91D", "0.25", "7.50", "tbill", None, "9999-12-31", "csv_import"),
]


@pytest.mark.parametrize(
    ("latest_day", "history_months", "requested_start", "is_partial", "window", "curve"),
    [
        pytest.param(
            date(1, 1, 31),
            1,
            "0001-01-31",
            False,
            [date(1, 1, 31)],
            YEAR_ONE_CURVE,
            id="year_one_one_month",
        ),
        pytest.param(
            date(1, 1, 31),
            24,
            "0001-01-31",
            True,
            [date(1, 1, 31)],
            YEAR_ONE_CURVE,
            id="year_one_24_months_cut",
        ),
        pytest.param(
            date(9999, 12, 31),
            1,
            "9999-12-31",
            False,
            [date(9999, 12, 31)],
            YEAR_9999_CURVE,
            id="december_9999_one_month",
        ),
        pytest.param(
            date(9999, 12, 31),
            24,
            "9998-01-31",
            True,
            month_ends(date(9998, 1, 31), 24),
            YEAR_9999_CURVE,
            id="december_9999_24_months",
        ),
    ],
)
async def test_boundary_dates_build_the_overview_without_raising(
    session: AsyncSession,
    latest_day: date,
    history_months: int,
    requested_start: str,
    is_partial: bool,
    window: list[date],
    curve: list[CurveRow],
) -> None:
    """Imported rows may carry any date a ``date`` can hold, and the overview still builds.

    Only one macro row is stored (inflation 5.00 and a 91-day yield of 7.50 from ``csv_import``),
    on 0001-01-31 with a 10.00-year FXD bond auctioned 0001-01-15, or on 9999-12-31 without one.
    A window reaching back before January of year 1 is cut there, so 24 months ending in 0001-01
    hold only 0001-01-31 and are partial: the uncut window would open 23 months before January
    of year 1, so every date falls more than a month after it. A 24-month window ending in
    December 9999 opens at 9998-01-31 and is partial, while one month of 9999-12 is fully
    covered. Every month-end curve before the row is empty; the last one and the current curve
    hold its bill yield and, in year 1, the bond, read from a lookback clamped at ``date.min``.
    """
    await session.execute(delete(TerminalMacroIndicator))
    await session.execute(delete(TerminalBondYield))
    session.add(
        TerminalMacroIndicator(
            observation_date=latest_day,
            inflation_yoy_pct=Decimal("5.0000"),
            tbill_91d_pct=Decimal("7.5000"),
            source_by_field={"inflation_yoy_pct": "csv_import", "tbill_91d_pct": "csv_import"},
        )
    )
    if latest_day.year == 1:
        session.add(
            TerminalBondYield(
                observation_date=date(1, 1, 15),
                bond_code="FXD1/0001/010",
                original_tenor_years=Decimal("10.00"),
                remaining_tenor_years=Decimal("10.00"),
                maturity_date=date(11, 1, 15),
                yield_pct=Decimal("13.5000"),
                source="cbk_bonds",
            )
        )
    await session.commit()

    overview = await get_macro_overview(session, history_months)

    assert overview.coverage.model_dump(mode="json") == {
        "requested_start": requested_start,
        "coverage_start": latest_day.isoformat(),
        "is_partial_coverage": is_partial,
    }
    assert [point.model_dump(mode="json") for point in overview.inflation_series] == [
        {"date": latest_day.isoformat(), "value": "5.00"}
    ]
    assert [point_fields(point) for point in overview.yield_curve.current] == curve
    history = overview.yield_curve.history
    assert [snapshot.as_of for snapshot in history] == window
    assert [[point_fields(point) for point in snapshot.points] for snapshot in history] == [
        *([] for _ in window[:-1]),
        curve,
    ]


# ---------------------------------------------------------------------------------------------
# Cycle service: bounded reads and the log sink of GET /cycles/backtest (services/cycle_service.py)
# ---------------------------------------------------------------------------------------------
#
# ``services/cycle_service.py`` reads the stored history with bound ``select()`` statements,
# runs the pure ``quant.backtest.run_cycle_backtest`` once per event in a worker thread and
# rounds each output once with ``quantize_out``. The contract held here:
#
# - **Bounded session reads, one statement each for every cycle.** One ``DISTINCT trade_date``
#   statement reads every cycle's own sessions, one ``BETWEEN`` per cycle's
#   ``[window_start, min(window_end, data_available_through)]``, overlapping or adjacent ranges
#   merged. One more reads the lead-ins: one backward ``DISTINCT trade_date`` query before each
#   ``window_start`` whose window holds a session, with ``LIMIT 30`` (the 20-session turnover
#   mean and the 10-session base close), joined by ``UNION``.
# - **Bounded price read, one statement for every cycle.** One Core column statement reads the
#   candidates' rows over each cycle's span, from its first lead-in session to its last observed
#   session, spans merged where they overlap or touch, and each cycle gets the rows of its own
#   span.
# - **Constant statement count.** The number of ``terminal_daily_prices`` statements does not
#   grow with the number of events: 2, 4 and 8 events issue the same four (the candidates'
#   latest date, sessions, lead-ins, prices). No statement reads ``terminal_daily_prices``
#   without a predicate, the history between distant events is never read, and a cycle whose
#   window holds no data reads no lead-in and no price row.
# - **Bounded overlay read.** One statement reads the overlay rows over each observed cycle's
#   ``[window_start - 62 days, last observed session]``, overlapping or adjacent ranges merged,
#   so the history between distant events is never read; a request with no observed cycle
#   reads none.
# - **Same answer as the full history.** Status, anchor (the first market session in the
#   window), cohort, every point's trade date and index levels, and every overlay equal what
#   the full observed calendar, every price row and every macro row give: the backtest of
#   ``quant.run_cycle_backtest`` over that full history, and ``macro_as_of`` at most 62 days
#   old. The valuation floor (on the returned values, earliest on a tie), the summary, the
#   flags and the projection equal the summary and projection rules applied in this module to
#   that history's unrounded points, rounded once, independently of the service's assembly.
# - **Log escaping.** The DEBUG summary repr-escapes each stored ``terminal_cycles.label``, so
#   CR, LF and other control or line-separator characters cannot forge a log line; the response
#   carries the stored label unchanged.
#
# Seeded windows: 2013-03-04 +-180 days is 2012-09-05 to 2013-08-31, whose last session is
# 2013-08-30; 2022-08-09 +-180 days is 2022-02-10 to 2023-02-05, whose last session is
# 2023-02-03. The seeded prices end on 2026-09-30 (the market section's ``LATEST``).
#
# Every test gets a fresh seeded clone (the root conftest's ``seeded_database``, which marks it
# ``integration``, through the market section's ``session`` and ``settings`` fixtures); the
# label tests change only their own clone. Every expected value is an exact literal or is
# derived from the full stored history read without bounds.

LOGGER = "opennse_terminal.services.cycle_service"
OVERLAY_COLUMNS: tuple[MacroColumn, ...] = ("inflation_yoy_pct", "tbill_91d_pct", "tbill_364d_pct")
SESSION_READ = "SELECT DISTINCT terminal_daily_prices.trade_date"
PRICE_READ = "SELECT terminal_daily_prices.ticker, terminal_daily_prices.trade_date"
MACRO_READ = "FROM terminal_macro_indicators"

Statement = tuple[str, tuple[Any, ...]]


@contextmanager
def captured_statements(engine: AsyncEngine) -> Iterator[list[Statement]]:
    """Every SQL statement ``engine`` sends while the block runs, with its bound parameters."""
    statements: list[Statement] = []

    def record(
        conn: object,
        cursor: object,
        statement: str,
        parameters: Sequence[Any],
        context: object,
        executemany: bool,
    ) -> None:
        statements.append((statement, tuple(parameters)))

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)


def reads_of(statements: Sequence[Statement], marker: str) -> list[Statement]:
    """The captured statements whose SQL contains ``marker``, in execution order."""
    return [statement for statement in statements if marker in statement[0]]


def assert_daily_price_reads_bounded(statements: Sequence[Statement]) -> None:
    """Every ``terminal_daily_prices`` read has a predicate; session reads bound ``trade_date``."""
    for sql, _ in reads_of(statements, "FROM terminal_daily_prices"):
        assert "WHERE" in sql, sql
    for sql, _ in reads_of(statements, SESSION_READ):
        assert "WHERE terminal_daily_prices.trade_date" in sql, sql


# ---------------------------------------------------------------------------------------------
# Cycle service: Bounded statements
# ---------------------------------------------------------------------------------------------


async def test_distant_events_read_only_their_windows_and_lead_in(
    seeded_database: SeededDatabase, session: AsyncSession, settings: TerminalSettings
) -> None:
    """2013 and 2022 read their own sessions, 30-session lead-ins, prices and overlays, each in
    one statement for both cycles, and nothing of 2014-2021."""
    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(
            session, settings, event_dates=[date(2022, 8, 9), date(2013, 3, 4)]
        )

    assert [cycle.status for cycle in response.cycles] == ["complete", "complete"]
    assert_daily_price_reads_bounded(statements)
    (in_window_sql, in_window_params), (lead_in_sql, lead_in_params) = reads_of(
        statements, SESSION_READ
    )
    in_window = list(zip(in_window_params[::2], in_window_params[1::2], strict=True))
    assert in_window == [
        (date(2012, 9, 5), date(2013, 8, 31)),
        (date(2022, 2, 10), date(2023, 2, 5)),
    ]
    assert not any(start <= date(2016, 1, 1) <= end for start, end in in_window)
    assert in_window_sql.count("terminal_daily_prices.trade_date BETWEEN ") == 2
    assert " LIMIT " not in in_window_sql
    assert lead_in_params == (date(2012, 9, 5), 30, date(2022, 2, 10), 30)
    assert " UNION " in lead_in_sql
    assert lead_in_sql.count("WHERE terminal_daily_prices.trade_date < ") == 2
    assert lead_in_sql.count("ORDER BY terminal_daily_prices.trade_date DESC") == 2
    assert lead_in_sql.count("LIMIT ") == 2

    (macro_sql, macro_params), *others = reads_of(statements, MACRO_READ)
    assert others == []
    ranges = list(zip(macro_params[::2], macro_params[1::2], strict=True))
    assert ranges == [
        (date(2012, 7, 5), date(2013, 8, 30)),
        (date(2021, 12, 10), date(2023, 2, 3)),
    ]
    assert not any(start <= date(2016, 1, 1) <= end for start, end in ranges)
    assert macro_sql.count("terminal_macro_indicators.observation_date >= ") == 2

    ((price_sql, price_params),) = reads_of(statements, PRICE_READ)
    assert price_params[:4] == (
        date(2012, 7, 25),
        date(2013, 8, 30),
        date(2021, 12, 30),
        date(2023, 2, 3),
    )
    assert sorted(price_params[4:]) == sorted(SEEDED_TICKERS)
    assert price_sql.count("terminal_daily_prices.trade_date BETWEEN ") == 2


@pytest.mark.parametrize(
    ("request_args", "ranges"),
    [
        pytest.param(
            {"event_dates": [date(2017, 8, 8), date(2017, 12, 1)]},
            [(date(2016, 12, 9), date(2018, 5, 30))],
            id="overlapping",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 5)], "pre_days": 30, "post_days": 30},
            [(date(2012, 12, 2), date(2013, 8, 2))],
            id="adjacent",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 6)], "pre_days": 30, "post_days": 30},
            [(date(2012, 12, 2), date(2013, 4, 3)), (date(2013, 4, 5), date(2013, 8, 5))],
            id="one_day_apart",
        ),
    ],
)
async def test_overlay_ranges_merge_when_they_overlap_or_touch(
    seeded_database: SeededDatabase,
    session: AsyncSession,
    settings: TerminalSettings,
    request_args: dict[str, Any],
    ranges: list[tuple[date, date]],
) -> None:
    """Each cycle's ``[window_start - 62, last session]`` merges only with a range it meets."""
    with captured_statements(seeded_database.engine) as statements:
        await cs.get_backtest(session, settings, **request_args)

    ((_, macro_params),) = reads_of(statements, MACRO_READ)
    assert list(zip(macro_params[::2], macro_params[1::2], strict=True)) == ranges


@pytest.mark.parametrize(
    ("event_date", "window", "session_reads"),
    [
        pytest.param(date(2027, 8, 10), {}, [], id="window_after_the_data"),
        pytest.param(
            date(2005, 1, 1),
            {"pre_days": 30, "post_days": 30},
            [(date(2004, 12, 2), date(2005, 1, 31))],
            id="window_before_the_data",
        ),
    ],
)
async def test_awaiting_data_request_reads_no_lead_in_prices_or_overlays(
    seeded_database: SeededDatabase,
    session: AsyncSession,
    settings: TerminalSettings,
    event_date: date,
    window: dict[str, int],
    session_reads: list[tuple[date, date]],
) -> None:
    """A window without data reads at most its own bounded sessions, then nothing else."""
    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(session, settings, event_dates=[event_date], **window)

    assert [cycle.status for cycle in response.cycles] == ["awaiting_data"]
    assert_daily_price_reads_bounded(statements)
    assert [params for _, params in reads_of(statements, SESSION_READ)] == session_reads
    assert reads_of(statements, " LIMIT ") == []
    assert reads_of(statements, PRICE_READ) == []
    assert reads_of(statements, MACRO_READ) == []


EIGHT_EVENTS: Final[list[date]] = [
    date(2013, 3, 4),
    date(2014, 6, 1),
    date(2015, 6, 1),
    date(2017, 8, 8),
    date(2019, 6, 1),
    date(2022, 8, 9),
    date(2024, 6, 1),
    date(2027, 8, 10),
]


@pytest.mark.parametrize(
    ("request_args", "statuses"),
    [
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2022, 8, 9)]},
            ["complete", "complete"],
            id="2_events",
        ),
        pytest.param(
            {}, ["complete", "complete", "complete", "awaiting_data"], id="4_default_events"
        ),
        pytest.param(
            {"event_dates": EIGHT_EVENTS, "pre_days": 365, "post_days": 365},
            [*(["complete"] * 7), "partial"],
            id="8_events_365_days",
        ),
    ],
)
async def test_daily_price_statements_do_not_grow_with_the_events(
    seeded_database: SeededDatabase,
    session: AsyncSession,
    settings: TerminalSettings,
    request_args: dict[str, Any],
    statuses: list[str],
) -> None:
    """2, 4 and 8 events issue the same four ``terminal_daily_prices`` statements.

    They are the candidates' latest date, one session read, one lead-in read with a ``LIMIT 30``
    query per window that holds a session, and one price read. No date bound reaches past the
    data, so the 2027 window of the default request, which holds no data, adds nothing to them.
    """
    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(session, settings, **request_args)

    assert [cycle.status for cycle in response.cycles] == statuses
    assert_daily_price_reads_bounded(statements)
    daily_price_reads = reads_of(statements, "FROM terminal_daily_prices")
    assert len(daily_price_reads) == 4
    (_, in_window), (_, lead_ins) = reads_of(statements, SESSION_READ)
    ((_, prices),) = reads_of(statements, PRICE_READ)
    observed = sum(1 for status in statuses if status != "awaiting_data")
    assert lead_ins[1::2] == (30,) * observed
    for bound in (*in_window, *lead_ins[::2], *prices):
        if isinstance(bound, date):
            assert bound <= LATEST


# ---------------------------------------------------------------------------------------------
# Cycle service: Equivalence with the full stored history
# ---------------------------------------------------------------------------------------------


async def full_overlay_rows(session: AsyncSession) -> list[MacroRow]:
    """Every stored macro observation's overlay columns, ascending, read without any bound."""
    result = await session.execute(
        select(
            TerminalMacroIndicator.observation_date,
            TerminalMacroIndicator.inflation_yoy_pct,
            TerminalMacroIndicator.tbill_91d_pct,
            TerminalMacroIndicator.tbill_364d_pct,
        ).order_by(TerminalMacroIndicator.observation_date)
    )
    return [
        MacroRow(
            observation_date=observation_date,
            inflation_yoy_pct=inflation,
            tbill_91d_pct=tbill_91d,
            tbill_364d_pct=tbill_364d,
        )
        for observation_date, inflation, tbill_91d, tbill_364d in result
    ]


async def full_history_cycles(
    session: AsyncSession,
    settings: TerminalSettings,
    response: BacktestResponse,
    macro_rows: Sequence[MacroRow],
) -> list[CycleBacktestResult]:
    """Backtest each returned cycle over the full calendar, every price row and every macro row."""
    sessions = await ms.load_market_sessions(session)
    equities = await ms.load_equities(session, tickers=response.candidates)
    prices = await ms.load_price_rows(session, response.candidates, None, None)
    fundamentals = await ms.load_fundamentals_rows(session, response.candidates)
    candidates = [
        BacktestCandidate(
            ticker=equity.ticker,
            shares_outstanding=equity.shares_outstanding,
            prices=prices[equity.ticker],
            fundamentals=fundamentals[equity.ticker],
            currency=equity.currency,
        )
        for equity in equities
    ]
    return [
        run_cycle_backtest(
            CycleEvent(cycle.label, cycle.event_date),
            CycleWindow(cycle.event_date, cycle.window_start, cycle.window_end),
            candidates,
            sessions,
            macro_rows,
            data_available_through=response.data_available_through,
            lag_days=settings.fundamentals_lag_days,
        )
        for cycle in response.cycles
    ]


def assert_cycle_matches(
    cycle: CycleResult,
    reference: CycleBacktestResult,
    in_window: Sequence[date],
    macro_rows: Sequence[MacroRow],
) -> None:
    """``cycle`` is the full-history ``reference``, rounded once, with overlays as of each day."""
    assert cycle.status == reference.status
    assert cycle.anchor_date == (in_window[0] if in_window else None)
    assert cycle.anchor_date == reference.anchor_date
    assert cycle.days_until_window_start == reference.days_until_window_start
    assert cycle.constituents == sorted(reference.constituents)
    assert cycle.excluded_constituents == sorted(reference.excluded_constituents)
    assert [point.trade_date for point in cycle.points] == list(in_window)
    assert len(cycle.points) == len(reference.points)
    for point, expected in zip(cycle.points, reference.points, strict=True):
        assert point.offset_days == (point.trade_date - cycle.event_date).days
        assert point.price_index == quantize_out(expected.price_index, "index")
        assert point.turnover_index == quantize_out(expected.turnover_index, "index")
        assert point.pe_ratio == quantize_out(expected.pe_ratio, "ratio")
        assert point.pe_index == quantize_out(expected.pe_index, "index")
        assert point.cumulative_foreign_net_kes == quantize_out(
            expected.cumulative_foreign_net_kes, "money"
        )
        for column in OVERLAY_COLUMNS:
            resolved = macro_as_of(macro_rows, column, point.trade_date, 62)
            assert getattr(point, column) == (
                None if resolved is None else quantize_out(resolved.value, "pct")
            )


def earliest_minimum(values: Sequence[Decimal | None]) -> int | None:
    """Position of the first minimum among the non-null ``values``; ``None`` if all are null."""
    best: tuple[int, Decimal] | None = None
    for position, value in enumerate(values):
        if value is not None and (best is None or value < best[1]):
            best = (position, value)
    return None if best is None else best[0]


def formula_summary(
    points: Sequence[CyclePointResult], event_date: date
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """Pre-event return, post-event return and maximum drawdown in %, unrounded, over ``points``.

    "Index at T" is the price index of the last point on or before T: the pre-event return is
    (index at T / 100 - 1) x 100, the post-event return (last index / index at T - 1) x 100 when a
    point lies after T, and the drawdown the minimum of (index / running maximum - 1) x 100. Each
    is ``None`` where its input level is.
    """
    at_or_before = [point for point in points if point.trade_date <= event_date]
    index_at_event = at_or_before[-1].price_index if at_or_before else None
    last = points[-1]
    pre_event = None if index_at_event is None else (index_at_event / HUNDRED - 1) * HUNDRED
    post_event = None
    if index_at_event is not None and last.trade_date > event_date and last.price_index is not None:
        post_event = (last.price_index / index_at_event - 1) * HUNDRED
    drawdowns: list[Decimal] = []
    running_max: Decimal | None = None
    for point in points:
        level = point.price_index
        if level is None:
            continue
        running_max = level if running_max is None else max(running_max, level)
        drawdowns.append((level / running_max - 1) * HUNDRED)
    return pre_event, post_event, min(drawdowns) if drawdowns else None


def result_track(cycle: CycleBacktestResult) -> Track:
    """A full-history cycle's window and unrounded points as a projection :data:`Track`."""
    return (
        (cycle.window_start - cycle.event_date).days,
        (cycle.window_end - cycle.event_date).days,
        [point.offset_days for point in cycle.points],
        [[point.price_index, point.turnover_index, point.pe_index] for point in cycle.points],
    )


def assert_cycle_outputs_match(
    cycle: CycleResult,
    reference: CycleBacktestResult,
    complete: Sequence[CycleBacktestResult],
) -> None:
    """``cycle``'s floor, summary, flags and projection follow from the full-history
    ``reference`` and the request's ``complete`` references, each rounded once.

    The summary figures are :func:`formula_summary` over the reference's unrounded points. The
    valuation floor is chosen as the response defines its markers, on the returned (rounded)
    values: the first minimum P/E, else the first minimum price index. The P/E compression is the
    reference's unrounded P/E at that session over its anchor P/E, (P/E / anchor P/E - 1) x 100,
    and is null when the floor fell back to the price index or the anchor P/E is null or not
    positive. The flags are the reference's, plus ``insufficient_projection_basis`` for a partial
    or awaiting-data cycle when fewer than 2 complete cycles exist. Such a cycle is otherwise
    projected: its basis lists the complete cycles in order, and each point is :func:`mean_paths`
    over their unrounded levels. A complete or empty cycle has no projection.
    """
    projected = reference.status in {"partial", "awaiting_data"}
    unprojectable = projected and len(complete) < 2
    assert cycle.flags == [
        *reference.flags,
        *(["insufficient_projection_basis"] if unprojectable else []),
    ]

    if reference.status in {"awaiting_data", "no_constituents"}:
        assert (cycle.markers, cycle.summary) == (None, None)
    else:
        floor_metric = "pe_ratio"
        floor = earliest_minimum([point.pe_ratio for point in cycle.points])
        if floor is None:
            floor_metric = "price_index"
            floor = earliest_minimum([point.price_index for point in cycle.points])
        assert cycle.markers is not None
        marker = cycle.markers.valuation_floor
        if floor is None:
            assert marker is None
        else:
            assert marker is not None
            floor_point = cycle.points[floor]
            assert (marker.trade_date, marker.offset_days, marker.metric) == (
                floor_point.trade_date,
                floor_point.offset_days,
                floor_metric,
            )
            assert marker.value == getattr(floor_point, floor_metric)

        compression = None
        anchor_pe = reference.points[0].pe_ratio
        if floor is not None and floor_metric == "pe_ratio" and anchor_pe is not None:
            floor_pe = reference.points[floor].pe_ratio
            assert floor_pe is not None
            if anchor_pe > 0:
                compression = (floor_pe / anchor_pe - 1) * HUNDRED
        pre_event, post_event, drawdown = formula_summary(reference.points, reference.event_date)
        assert cycle.summary is not None
        assert (
            cycle.summary.pre_event_return_pct,
            cycle.summary.post_event_return_pct,
            cycle.summary.max_drawdown_pct,
            cycle.summary.pe_compression_pct,
        ) == (
            quantize_out(pre_event, "pct"),
            quantize_out(post_event, "pct"),
            quantize_out(drawdown, "pct"),
            quantize_out(compression, "pct"),
        )

    if not projected or unprojectable:
        assert cycle.projection is None
        return
    assert cycle.projection is not None
    assert cycle.projection.is_projection is True
    assert cycle.projection.basis == [other.label for other in complete]
    expected = mean_paths(
        (reference.window_start - reference.event_date).days,
        (reference.window_end - reference.event_date).days,
        [result_track(other) for other in complete],
    )
    assert [
        (point.offset_days, point.price_index, point.turnover_index, point.pe_index)
        for point in cycle.projection.points
    ] == [
        (offset, *(quantize_out(level, "index") for level in levels)) for offset, levels in expected
    ]


@pytest.mark.parametrize(
    ("request_args", "statuses"),
    [
        pytest.param({}, ["complete", "complete", "complete", "awaiting_data"], id="default"),
        pytest.param(
            {"pre_months": 12, "post_months": 12},
            ["complete", "complete", "complete", "partial"],
            id="twelve_months",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2022, 8, 9)]},
            ["complete", "complete"],
            id="distant_events",
        ),
        pytest.param(
            {"event_dates": [date(2017, 8, 8), date(2017, 12, 1)]},
            ["complete", "complete"],
            id="overlapping_windows",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 5)], "pre_days": 30, "post_days": 30},
            ["complete", "complete"],
            id="adjacent_overlay_ranges",
        ),
        pytest.param(
            {"event_dates": [date(2012, 1, 3)], "pre_days": 365, "post_days": 10},
            ["complete"],
            id="no_session_before_window",
        ),
        pytest.param(
            {"event_dates": [date(2012, 2, 1)], "pre_days": 7, "post_days": 30},
            ["complete"],
            id="short_lead_in",
        ),
        pytest.param(
            {"event_dates": [date(2005, 1, 1)], "pre_days": 30, "post_days": 30},
            ["awaiting_data"],
            id="window_before_the_data",
        ),
        # The Banking 2017 cycle's lowest P/E rounds alike on 2017-08-10 and 2017-08-11, whose
        # raw P/E is lower: the floor and its compression (-14.06, not -14.13) are 2017-08-10's,
        # as test_banking_2017_floor_is_the_earlier_session_of_a_rounded_pe_tie pins.
        pytest.param(
            {"sector": "Banking"},
            ["complete", "complete", "complete", "awaiting_data"],
            id="banking_rounded_pe_tie",
        ),
    ],
)
async def test_bounded_reads_give_the_full_history_answer(
    session: AsyncSession,
    settings: TerminalSettings,
    request_args: dict[str, Any],
    statuses: list[str],
) -> None:
    """Anchors, cohorts, points and overlays equal those of the full stored history, and the
    floors, summaries, flags and projections follow from that history by their rules (see
    :func:`assert_cycle_outputs_match`). Awaiting data (by default) or partly observed (over 12
    months), the 2027 cycle is projected from the three complete cycles; the 2005 cycle, before
    the data, has no complete cycle to project from and is flagged instead."""
    response = await cs.get_backtest(session, settings, **request_args)

    assert response.data_available_through == LATEST
    assert [cycle.status for cycle in response.cycles] == statuses
    full_calendar = await ms.load_market_sessions(session)
    macro_rows = await full_overlay_rows(session)
    references = await full_history_cycles(session, settings, response, macro_rows)
    complete = [reference for reference in references if reference.status == "complete"]
    for cycle, reference in zip(response.cycles, references, strict=True):
        last_day = min(cycle.window_end, LATEST)
        in_window = [day for day in full_calendar if cycle.window_start <= day <= last_day]
        assert_cycle_matches(cycle, reference, in_window, macro_rows)
        assert_cycle_outputs_match(cycle, reference, complete)


async def test_banking_2017_floor_is_the_earlier_session_of_a_rounded_pe_tie(
    session: AsyncSession, settings: TerminalSettings
) -> None:
    """The seeded Banking 2017 Cycle's lowest P/E rounds to 5.88 on 2017-08-10 and on 2017-08-11,
    whose unrounded P/E (the full-history reference's) is the lower. The floor is the earlier
    session, chosen on the rounded values, and the compression is 2017-08-10's, -14.06; floored on
    the unrounded values it would be 2017-08-11's, -14.13."""
    response = await cs.get_backtest(session, settings, sector="Banking")
    macro_rows = await full_overlay_rows(session)
    references = await full_history_cycles(session, settings, response, macro_rows)
    (position,) = [
        position for position, cycle in enumerate(response.cycles) if cycle.label == "2017 Cycle"
    ]
    cycle, reference = response.cycles[position], references[position]
    floor_day, tie_day = date(2017, 8, 10), date(2017, 8, 11)

    assert cycle.markers is not None
    floor = cycle.markers.valuation_floor
    assert floor is not None
    assert (floor.trade_date, floor.offset_days, floor.metric, floor.value) == (
        floor_day,
        2,
        "pe_ratio",
        Decimal("5.88"),
    )
    rounded = {point.trade_date: point.pe_ratio for point in cycle.points}
    assert rounded[floor_day] == rounded[tie_day] == Decimal("5.88")
    unrounded = {point.trade_date: point.pe_ratio for point in reference.points}
    floor_pe, tie_pe = unrounded[floor_day], unrounded[tie_day]
    assert floor_pe is not None
    assert tie_pe is not None
    assert tie_pe < floor_pe
    assert cycle.summary is not None
    assert cycle.summary.pe_compression_pct == Decimal("-14.06")
    anchor_pe = reference.points[0].pe_ratio
    assert anchor_pe is not None
    assert quantize_out((tie_pe / anchor_pe - 1) * HUNDRED, "pct") == Decimal("-14.13")


async def test_short_lead_in_reads_every_earlier_session(
    seeded_database: SeededDatabase, session: AsyncSession, settings: TerminalSettings
) -> None:
    """With 17 sessions before the window, the limited lead-in returns all 17 and no more."""
    full_calendar = await ms.load_market_sessions(session)
    window_start = date(2012, 1, 25)
    earlier = [day for day in full_calendar if day < window_start]
    assert len(earlier) == 17

    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(
            session, settings, event_dates=[date(2012, 2, 1)], pre_days=7, post_days=30
        )

    (cycle,) = response.cycles
    assert cycle.window_start == window_start
    assert cycle.anchor_date == date(2012, 1, 25)
    (price_read,) = reads_of(statements, PRICE_READ)
    assert price_read[1][:2] == (earlier[0], date(2012, 3, 2))


# ---------------------------------------------------------------------------------------------
# Cycle service: Log escaping
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label",
    [
        pytest.param("2032 Cycle\r\nINFO opennse_terminal: forged entry", id="crlf"),
        pytest.param("2032 Cycle\nWARNING forged line", id="lf"),
        pytest.param("2032 Cycle\rERROR overwritten line", id="cr"),
        pytest.param("2032 Cycle\u2028forged line", id="line_separator"),
        pytest.param("2032 Cycle\x85forged line", id="next_line"),
        pytest.param("2032 Cycle\x1b[2K\x1b[1Gforged line", id="ansi_erase"),
    ],
)
async def test_debug_summary_escapes_stored_labels(
    session: AsyncSession,
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    label: str,
) -> None:
    """A label's control characters stay escaped in one log line; the response keeps them."""
    session.add(TerminalCycle(label=label, event_date=date(2032, 8, 10), source="test"))
    await session.commit()
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    response = await cs.get_backtest(session, settings, event_dates=[date(2032, 8, 10)])

    (record,) = [record for record in caplog.records if record.name == LOGGER]
    message = record.getMessage()
    assert message == (
        f"cycle backtest over 8 candidates through 2026-09-30: {label!r}=awaiting_data"
    )
    assert "\r" not in message
    assert "\n" not in message
    assert message.isprintable()
    (cycle,) = response.cycles
    assert cycle.label == label
    assert cycle.status == "awaiting_data"


# ---------------------------------------------------------------------------------------------
# Analytic services: computation in a worker thread and abandoned requests (services/offload.py)
# ---------------------------------------------------------------------------------------------
#
# The screener, valuation bands, anomalies, flows and backtest read on the event loop, then run
# their pure quant computation in a worker thread through ``offload.compute``, in chunks: one per
# ticker series for the anomaly feed and one per cycle for the backtest. Before each chunk they
# await the ``abandoned`` check the route passes (``request.is_disconnected``). The contract held
# here:
#
# - **Early stop.** Once the check reports the client gone, the service raises
#   ``RequestAbandonedError`` before its next chunk, so the remaining series or cycles are never
#   computed.
# - **499 for a gone client.** A request whose client has already disconnected is answered 499
#   (client closed request) with no quant work, on each of the five routes.
# - **Responsive event loop.** While a chunk computes, other coroutines keep running.
#
# Quant work is observed by wrapping the quant function in the service module's namespace, the
# name the service calls; the wrapper delegates to the real function, so answers are unchanged.


class AbandonAfter:
    """An abandon check answering ``False`` for its first ``checks`` calls and ``True`` after."""

    def __init__(self, checks: int) -> None:
        self.checks = checks
        self.calls = 0

    async def __call__(self) -> bool:
        self.calls += 1
        return self.calls > self.checks


def count_calls(monkeypatch: pytest.MonkeyPatch, module: ModuleType, name: str) -> list[None]:
    """Wrap ``module.name`` so that each call appends to the returned list, then delegates."""
    real = getattr(module, name)
    calls: list[None] = []

    def counted(*args: Any, **kwargs: Any) -> Any:
        calls.append(None)
        return real(*args, **kwargs)

    monkeypatch.setattr(module, name, counted)
    return calls


AbandonableCall = Callable[[AsyncSession, TerminalSettings, AbandonAfter], Awaitable[object]]


@pytest.mark.parametrize(
    ("call", "module", "quant_function", "chunks"),
    [
        pytest.param(
            lambda session, settings, abandoned: ms.get_anomalies(session, abandoned=abandoned),
            ms,
            "rolling_zscores",
            16,
            id="anomalies_8_tickers_x_2_series",
        ),
        pytest.param(
            lambda session, settings, abandoned: cs.get_backtest(
                session, settings, abandoned=abandoned
            ),
            cs,
            "run_cycle_backtest",
            4,
            id="backtest_4_cycles",
        ),
    ],
)
async def test_abandoned_request_stops_before_the_next_chunk(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    settings: TerminalSettings,
    call: AbandonableCall,
    module: ModuleType,
    quant_function: str,
    chunks: int,
) -> None:
    """The client leaves after the first chunk: one of the 16 series or 4 cycles is computed."""
    calls = count_calls(monkeypatch, module, quant_function)
    abandoned = AbandonAfter(1)

    with pytest.raises(RequestAbandonedError):
        await call(session, settings, abandoned)

    assert abandoned.calls == 2
    assert len(calls) == 1
    assert len(calls) < chunks


@pytest.mark.parametrize(
    ("path", "module", "quant_function"),
    [
        pytest.param("/market/screener", ms, "compute_valuation", id="screener"),
        pytest.param(
            "/market/valuation-bands?ticker=SCOM", ms, "compute_valuation", id="valuation_bands"
        ),
        pytest.param("/market/anomalies", ms, "rolling_zscores", id="anomalies"),
        pytest.param("/market/flows", ms, "net_flow_series", id="flows"),
        pytest.param("/cycles/backtest", cs, "run_cycle_backtest", id="backtest"),
    ],
)
async def test_disconnected_client_gets_499_and_no_quant_work(
    monkeypatch: pytest.MonkeyPatch,
    app: FastAPI,
    path: str,
    module: ModuleType,
    quant_function: str,
) -> None:
    """A raw ASGI request whose ``receive`` reports ``http.disconnect`` is answered 499, empty.

    The route's reads run, but its service stops before the first chunk of computation.
    """
    calls = count_calls(monkeypatch, module, quant_function)
    route, _, query = f"{BASE}{path}".partition("?")
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": route,
        "raw_path": route.encode(),
        "query_string": query.encode(),
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 50000),
        "server": ("test", 80),
    }
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)

    start, body = sent
    assert start["type"] == "http.response.start"
    assert start["status"] == CLIENT_CLOSED_REQUEST == 499
    assert body["type"] == "http.response.body"
    assert body["body"] == b""
    assert not body.get("more_body", False)
    assert calls == []


async def test_event_loop_keeps_running_while_a_series_is_scored(
    monkeypatch: pytest.MonkeyPatch, session: AsyncSession
) -> None:
    """Each of SCOM's two series is scored by a rolling Z-score slowed by a 0.2 s sleep.

    A coroutine sleeping 10 ms at a time ticks during each of those sleeps, which it could not do
    if the scoring held the event loop, and the feed equals the one scored at full speed.
    """
    expected = await ms.get_anomalies(session, ticker="SCOM")
    real = ms.rolling_zscores
    ticks = 0
    observed: list[tuple[int, int]] = []

    def slow_rolling_zscores(*args: Any, **kwargs: Any) -> list[Any]:
        before = ticks
        time.sleep(0.2)
        observed.append((before, ticks))
        return real(*args, **kwargs)

    async def ticker(stop: asyncio.Event) -> None:
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    monkeypatch.setattr(ms, "rolling_zscores", slow_rolling_zscores)
    stop = asyncio.Event()
    ticking = asyncio.create_task(ticker(stop))
    try:
        response = await ms.get_anomalies(session, ticker="SCOM")
    finally:
        stop.set()
        await ticking

    assert response == expected
    assert len(observed) == 2
    for before, after in observed:
        assert after > before


# ---------------------------------------------------------------------------------------------
# IPO service presets: a stored row in another currency (services/ipo_service.py)
# ---------------------------------------------------------------------------------------------
#
# ``_scenario_from_row`` turns a stored ``terminal_ipo_scenarios`` row into the preset the
# sandbox posts back, and skips a row that cannot form one with a WARNING that names fields
# only. Rows are in-memory ORM instances shaped as PostgreSQL returns them: no database,
# network or filesystem is touched. The other invalid-row cases are held by
# ``tests/unit/services/test_ipo_service.py``.

SERVICE_LOGGER = "opennse_terminal.services.ipo_service"
PRESET_ID = uuid.UUID("5f0c6d1e-8a4b-4c3d-9e2f-1a2b3c4d5e6f")

# SEED_MARKET_DATA.md §2, verbatim: each risk note split at its first colon.
QUICKMART_RISKS: list[dict[str, str]] = [
    {
        "title": "Ownership & Motive",
        "description": "50% secondary exit with zero primary capital injection into Quickmart.",
    },
    {
        "title": "Growth Financing",
        "description": (
            "Future store expansion must rely on retained earnings, supplier credit, or bank "
            "debt rather than IPO cash."
        ),
    },
    {
        "title": "Valuation Floor",
        "description": (
            "Test whether KES 7.50 per share is justified under varying WACC (16%–20%), thin "
            "supermarket operating margins (3%–6%), and working-capital stress scenarios."
        ),
    },
]


def quickmart_row(**overrides: Any) -> TerminalIpoScenario:
    """The seeded Quickmart preset as an in-memory row, values shaped as PostgreSQL returns them.

    ``NUMERIC(18,4)``, ``(9,4)`` and ``(7,4)`` columns come back with 4 decimal places and
    ``NUMERIC(24,2)`` columns with 2, so the service has to quantize ``7.5000`` to ``7.50``.
    """
    columns: dict[str, Any] = {
        "id": PRESET_ID,
        "company_name": "Quickmart Supermarket",
        "sector": "Retail & Consumer Goods",
        "offer_price_kes": Decimal("7.5000"),
        "shares_offered": 500_000_000,
        "shares_outstanding_post_offer": 1_000_000_000,
        "pct_stake_sold": Decimal("50.0000"),
        "primary_proceeds_kes": Decimal("0.00"),
        "base_revenue_kes": Decimal("45000000000.00"),
        "base_ebitda_kes": Decimal("2025000000.00"),
        "net_debt_kes": Decimal("4000000000.00"),
        "assumed_wacc_pct": Decimal("18.0000"),
        "assumed_growth_pct": Decimal("10.0000"),
        "terminal_growth_pct": Decimal("5.0000"),
        "margin_expansion_pct": Decimal("0.5000"),
        "tax_rate_pct": Decimal("30.0000"),
        "capex_pct_revenue": Decimal("2.0000"),
        "da_pct_revenue": Decimal("1.5000"),
        "nwc_pct_revenue": Decimal("-3.0000"),
        "peer_ev_ebitda_multiple": Decimal("6.0000"),
        "dcf_weight_pct": Decimal("50.0000"),
        "forecast_years": 5,
        "risk_scenarios": [dict(risk) for risk in QUICKMART_RISKS],
        "is_preset": True,
        "currency": "KES",
        "source": "seed",
    }
    columns.update(overrides)
    return TerminalIpoScenario(**columns)


@pytest.mark.parametrize(
    ("overrides", "logged"),
    [
        # Every amount is KES and FX conversion is not supported, so a preset stored in another
        # currency is refused before any amount is read. The stored code is not logged.
        pytest.param(
            {"currency": "USD"},
            "skipping IPO preset 'Broken Preset': currency must be KES (currency_mismatch); "
            "preset amounts are never converted",
            id="currency_usd",
        ),
        pytest.param(
            {"currency": "EUR"},
            "skipping IPO preset 'Broken Preset': currency must be KES (currency_mismatch); "
            "preset amounts are never converted",
            id="currency_eur",
        ),
        # The currency check runs first: an out-of-range WACC on a USD preset is not reported.
        pytest.param(
            {"currency": "USD", "assumed_wacc_pct": Decimal("35.0000")},
            "skipping IPO preset 'Broken Preset': currency must be KES (currency_mismatch); "
            "preset amounts are never converted",
            id="currency_checked_before_ranges",
        ),
    ],
)
def test_invalid_row_is_skipped_with_a_warning_naming_fields_only(
    overrides: dict[str, Any], logged: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A row that cannot form a valid preset is a data error: it is logged once at WARNING and
    left out (``None``). The message names the preset and the invalid fields, never the stored
    values."""
    row = quickmart_row(company_name="Broken Preset", **overrides)

    with caplog.at_level(logging.WARNING, logger=SERVICE_LOGGER):
        assert _scenario_from_row(row) is None

    assert [(r.name, r.levelno, r.getMessage()) for r in caplog.records] == [
        (SERVICE_LOGGER, logging.WARNING, logged)
    ]
