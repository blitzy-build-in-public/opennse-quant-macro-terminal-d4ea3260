"""Every terminal endpoint answered from the seeded database alone (the seeded-evidence table).

Each test drives ``create_app()`` over ASGI through the ``client`` fixture, on this module's own
seeded clone; no scraper, browser or network call is involved. Requests that write (the PDF commit
and the watchlist lifecycle) run inside ``db_session``'s outer transaction, which is rolled back
after each test, so every test reads the seeded state whatever the order.

Decimal fields travel as JSON strings and are compared exactly, as strings or through
:func:`dec`, never as floats. Integer fields (volumes, counts, offsets, share counts) are checked
to be JSON integers.
"""

import datetime as dt
import hashlib
import uuid
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import Any, Final

import httpx
import pytest
from sqlalchemy import Executable, select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal import __version__
from opennse_terminal.models import (
    TerminalCorporateFiling,
    TerminalFundamental,
    TerminalIngestionRun,
)

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


# ---------------------------------------------------------------------------------------------
# Election and event cycles
# ---------------------------------------------------------------------------------------------


def assert_complete_cycle(cycle: JsonObject) -> None:
    """A completed historical cycle: points, a summary, and markers recomputable from the points."""
    assert cycle["status"] == "complete", cycle["label"]
    points = cycle["points"]
    assert points
    offsets = [point["offset_days"] for point in points]
    assert all(is_json_int(offset) for offset in offsets)
    assert offsets == sorted(offsets)
    assert cycle["summary"] is not None

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


def assert_projection_from_history(cycle: JsonObject) -> None:
    """A labelled projection averaged over the three completed historical cycles."""
    projection = cycle["projection"]
    assert projection is not None
    assert projection["is_projection"] is True
    assert sorted(projection["basis"]) == list(HISTORICAL_CYCLES)
    assert projection["points"]
    for point in projection["points"]:
        assert is_json_int(point["offset_days"])
        if point["price_index"] is not None:
            dec(point["price_index"])


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
    assert_projection_from_history(future)


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
    assert_projection_from_history(future)


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
