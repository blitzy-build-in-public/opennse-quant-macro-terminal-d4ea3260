"""Market service: the five ``/market`` analytics and the shared loaders, on seeded PostgreSQL.

``services/market_service.py`` reads ``terminal_equities``, ``terminal_daily_prices`` and
``terminal_fundamentals`` with bound ``select()`` statements, calls the pure ``quant`` formulas
and rounds each output once with ``quantize_out`` (ROUND_HALF_UP, 2 dp). The contract held here:

- **Market sessions** are the distinct stored ``trade_date`` values; the default ``as_of`` is the
  latest of them, and with no price row at all it is today's Africa/Nairobi date and every list
  is empty.
- **Screener**: each equity valued at its latest close on or before ``as_of`` with the fiscal
  year available 90 days after its period end; Banking EV/EBITDA null with
  ``ev_not_meaningful_for_banks``; filters inclusive on the quantized values, a null metric
  failing any filter on it; sorting with nulls last and ties by ticker in either order; sector
  aggregates over the whole universe, independent of the filters.
- **Heatmap**: the last session on or before each month-end or Friday; only KES equities.
- **Valuation bands**: ``requested_start`` is ``as_of`` minus the lookback, coverage is partial
  when data starts more than 7 days later, and ``stats`` exist only for ``status = "ok"``.
- **Anomalies**: positive spikes at or above the threshold over the last ``lookback_sessions``
  sessions, sorted by ``trade_date`` then ``z_score``, both descending.
- **Flows**: ``end`` defaults to ``as_of`` and ``start`` to ``end`` minus 365 days; local net flow
  is the negated foreign net flow on every session.
- **Coded errors** ``HTTPException(status, detail={"code", "message", "field"})``: 422
  ``min_exceeds_max``, ``exactly_one_subject``, ``conflicting_subjects`` and ``start_after_end``;
  404 ``unknown_ticker``, ``unknown_sector`` and ``watchlist_not_found``.

Seeded values are those of ``seed/reference_data.py``: the final closes (SCOM 33.25, EQTY 95.75,
KCB 85.00, EABL 269.50, KEGN 10.60, COOP 36.70, ABSA 30.55, SBIC 270.50) times the attachment's
share counts give the market caps exactly, and the latest fundamentals are set from the target
P/E, P/B and payout, so P/E, P/B and dividend yield (payout / P/E) are those targets. EV/EBITDA
is P/E x net margin / EBITDA margin + net debt / EBITDA: SCOM 14 x 0.22 / 0.45 + 0.3 = 7.14.
Volumes, turnover, flows and Z-scores come from the deterministic synthetic history and are
asserted at their seeded values, except the planted volume spikes, which are asserted by
inclusion: the seeder guarantees them but not the absence of other background crossings.

The coded 422s that the services check before their first query run on an ``AsyncSession``
bound to no engine, which would raise on any query, and the date clamps are pure; neither needs
a database. Every other test gets a fresh seeded clone (the root conftest's ``seeded_database``,
which marks it ``integration``); the few that change rows change only their own clone. Every
expected value is an exact ``Decimal`` or literal.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from fastapi import HTTPException
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal.config import TerminalSettings
from opennse_terminal.models import (
    TerminalDailyPrice,
    TerminalEquity,
    TerminalWatchlist,
    TerminalWatchlistItem,
)
from opennse_terminal.schemas.market import (
    AnomalyMetric,
    ScreenerParams,
    ScreenerSort,
    SortOrder,
)
from opennse_terminal.services import market_service as ms

LATEST = date(2026, 9, 30)
FIRST_SESSION = date(2012, 1, 2)
BANKS = ["ABSA", "COOP", "EQTY", "KCB", "SBIC"]
MANUFACTURING = "Manufacturing & Allied"
SECTORS = ["Banking", "Energy & Petroleum", MANUFACTURING, "Telecommunications"]
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
# Cross-field checks that run before the first query
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
# Shared loaders and subject checks
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
        pytest.param(None, "Banking", BANKS, id="sector"),
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
        **dict.fromkeys(BANKS, 16),
    }
    assert {
        (row.period_type, row.currency) for periods in everyone.values() for row in periods
    } == {("FY", "KES")}
    assert [row.period_end_date for row in everyone["EABL"]] == [
        date(year, 6, 30) for year in range(2010, 2027)
    ]
    assert await ms.load_fundamentals_rows(session, []) == {}


# ---------------------------------------------------------------------------------------------
# GET /market/screener
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
    assert [aggregate.sector for aggregate in response.sector_aggregates] == SECTORS

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
    """On 2012-01-02 each counter has one row: no previous close gives a null change, and an
    empty window gives null Z-scores with prefixed ``insufficient_observations`` reasons and no
    anomaly. The fiscal years available then are FY2010 for the banks and FY2011 for SCOM."""
    response = await ms.get_screener(session, ScreenerParams(as_of=FIRST_SESSION), LOCAL, settings)
    assert response.count == 8
    for row in response.rows:
        assert row.trade_date == FIRST_SESSION
        assert (row.change_pct, row.volume_z, row.turnover_z) == (None, None, None)
        assert "volume_z:insufficient_observations" in row.flags
        assert "turnover_z:insufficient_observations" in row.flags
        assert not (row.is_volume_anomaly or row.is_turnover_anomaly)
    periods = {row.ticker: row.fundamentals_period_end for row in response.rows}
    assert periods["SCOM"] == date(2011, 3, 31)
    assert {periods[bank] for bank in BANKS} == {date(2010, 12, 31)}


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
# GET /market/sector-heatmap
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
    assert [row.sector for row in response.sectors] == SECTORS
    members = dict(zip(SECTORS, (5, 1, 1, 1), strict=True))
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
# GET /market/valuation-bands
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


# ---------------------------------------------------------------------------------------------
# GET /market/anomalies
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
            set(BANKS),
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


# ---------------------------------------------------------------------------------------------
# GET /market/flows
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


# ---------------------------------------------------------------------------------------------
# Empty price table and a non-KES equity
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
    assert (bands.status, bands.series, bands.stats) == ("no_positive_earnings", [], None)
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
