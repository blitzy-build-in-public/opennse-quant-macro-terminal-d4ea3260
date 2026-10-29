"""Reference values and fixed parameters of the deterministic fallback seeder.

This is a pure data module: it performs no I/O, draws no random numbers and imports nothing from
``opennse_terminal``. ``seed/synthetic.py`` turns these values into rows. Record field names and
mapping keys are the ORM column names (``TerminalEquity``, ``TerminalMacroIndicator``,
``TerminalIpoScenario``), so that mapping is one to one.

Provenance. Every value states where it comes from, so the README's seed provenance table can be
built from this file, and each value sits in exactly one of these places:

* values from the ``SEED_MARKET_DATA.md`` attachment are copied verbatim and carry a comment citing
  its section (``§N``);
* values taken from a public observation the attachment does not supply sit between
  ``# --- FILLED (source: ...) ---`` and ``# --- END FILLED ---``;
* invented values sit between ``# --- SYNTHETIC --- <reason>`` and ``# --- END SYNTHETIC ---``.

Units: ``*_pct`` values are percentage points (``Decimal("6.80")`` means 6.80%); ``*_kes`` values
are whole KES, and prices are KES per share; ``fx_kes_usd`` is KES per USD; ``fx_reserves_usd_m``
is millions of USD; share counts and volumes are ``int``. Every fractional value is a ``Decimal``
built from a string literal, so no float enters the seed.

No seeded value is dated after :data:`ANCHOR_DATE` (2026-09-30). Rows dated in the future would be
read as observed history: they would move every market endpoint's default ``as_of``, enter the
valuation bands, Z-score windows and flows, and mark the 2027 cycle complete on invented data. The
2027 election is therefore only an event definition in :data:`CYCLES`; its trajectory is the
backtester's labelled projection from the completed cycles.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Final

__all__ = [
    "ANCHOR_DATE",
    "ANNUAL_DRIFT",
    "BANKING_SECTOR",
    "BANK_ASSETS_TO_EQUITY",
    "BASE_VOLUMES",
    "BOND_CODE_PREFIX",
    "BOND_NOISE",
    "BOND_TENOR_SPREADS",
    "CYCLES",
    "EQUITIES",
    "FLOW_FRACTION_CLIP",
    "FLOW_NOISE_SIGMA",
    "FLOW_POST_END",
    "FLOW_POST_FRACTION",
    "FLOW_POST_START",
    "FLOW_PRE_END",
    "FLOW_PRE_FRACTION",
    "FLOW_PRE_START",
    "FUNDAMENTALS_HISTORY_GROWTH",
    "FUNDAMENTALS_HISTORY_NOISE",
    "FUNDAMENTAL_TARGETS",
    "INFLATION_CAP_END",
    "INFLATION_CAP_START",
    "INFLATION_CAP_VALUE",
    "MACRO_ANCHOR",
    "MACRO_BOUNDS",
    "MACRO_COLUMNS",
    "MACRO_FIXED_POINTS",
    "MACRO_KNOTS",
    "MACRO_NOISE",
    "MACRO_START",
    "NON_BANK_ASSETS_TO_EQUITY",
    "NON_BANK_CASH_PCT_REVENUE",
    "OCF_TO_NET_INCOME",
    "OHLC_MAX_EPSILON",
    "OVERLAY_NEUTRAL",
    "PLANTED_SPIKES",
    "PRICE_ANCHORS",
    "PRICE_OVERLAY_DECLINE_START",
    "PRICE_OVERLAY_FLOOR",
    "PRICE_OVERLAY_FLOOR_END",
    "PRICE_OVERLAY_FLOOR_START",
    "PRICE_OVERLAY_POST",
    "PRICE_OVERLAY_PRE_FLOOR",
    "PRICE_OVERLAY_RECOVERY_END",
    "QUICKMART_PRESET",
    "QUICKMART_RISK_SCENARIOS",
    "QUICKMART_RISK_SCENARIO_LINES",
    "SEED_SOURCE",
    "SEED_WATCHLIST_NAME",
    "SEED_WATCHLIST_TICKERS",
    "SERIES_START",
    "SESSION_EXCLUDED_MONTH_DAYS",
    "SPIKE_MIN_Z",
    "SPIKE_SD_MULTIPLE",
    "SPIKE_VOLUME_MULTIPLE",
    "SPIKE_Z_WINDOW",
    "TICKERS",
    "TRADING_DAYS_PER_YEAR",
    "VOLATILITY",
    "VOLUME_LOG_SIGMA",
    "VOLUME_OVERLAY_END",
    "VOLUME_OVERLAY_EXHAUSTION",
    "VOLUME_OVERLAY_PRE_TROUGH",
    "VOLUME_OVERLAY_START",
    "VOLUME_OVERLAY_SURGE",
    "VOLUME_OVERLAY_SURGE_END",
    "VOLUME_OVERLAY_SURGE_START",
    "VOLUME_OVERLAY_TROUGH_START",
    "CycleReference",
    "EquityReference",
    "FundamentalTarget",
    "IpoPresetReference",
]

# Calendar and global constants ------------------------------------------------------------------

SEED_SOURCE: Final = "seed"
"""Provenance label of every seeded value: row-level ``source``, ``flow_source`` and each
``source_by_field`` entry."""

# SEED_MARKET_DATA.md §1: the date of the latest macroeconomic benchmarks.
ANCHOR_DATE: Final = date(2026, 9, 30)
"""Last seeded date for prices, macro indicators and bond yields. Nothing is dated after it."""

# --- SYNTHETIC --- mock-history extent, covering the 2013 cycle's ±12-month window and MA20 lead
SERIES_START: Final = date(2012, 1, 2)
"""First seeded trading session."""
MACRO_START: Final = date(2012, 1, 31)
"""First seeded macro month-end; with ANCHOR_DATE this gives 177 month-ends."""
# --- END SYNTHETIC ---

# --- SYNTHETIC --- simplified mock trading calendar: weekdays minus fixed-date holidays
# This is not an exchange calendar and never dates real data: it omits Easter, Idd-ul-Fitr, Sunday
# substitution and gazetted closures. Live NSE runs use ingestion/calendar.py, and analytics use
# the observed market sessions. Weekdays from SERIES_START to ANCHOR_DATE minus these
# (month, day) pairs give exactly 3,762 sessions, so 30,096 price rows for the eight counters.
SESSION_EXCLUDED_MONTH_DAYS: Final[tuple[tuple[int, int], ...]] = (
    (1, 1),
    (5, 1),
    (6, 1),
    (10, 10),
    (10, 20),
    (12, 12),
    (12, 25),
    (12, 26),
)
# --- END SYNTHETIC ---

# SEED_MARKET_DATA.md §3: the sector name of the five bank counters, spelled as the seed spells it.
BANKING_SECTOR: Final = "Banking"


# Equities ---------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EquityReference:
    """One seeded counter of ``terminal_equities``; field names are the model's column names."""

    ticker: str
    company_name: str
    sector: str
    shares_outstanding: int
    isin: str
    free_float_pct: Decimal


# SEED_MARKET_DATA.md §3, verbatim: ticker, company name, sector and shares outstanding.
_EQUITY_ATTACHMENT_ROWS: Final[tuple[tuple[str, str, str, int], ...]] = (
    ("SCOM", "Safaricom PLC", "Telecommunications", 40_065_428_000),
    ("EQTY", "Equity Group Holdings", "Banking", 3_773_675_000),
    ("KCB", "KCB Group PLC", "Banking", 3_213_462_815),
    ("EABL", "East African Breweries PLC", "Manufacturing & Allied", 790_774_356),
    ("KEGN", "KenGen PLC", "Energy & Petroleum", 6_594_522_339),
    ("COOP", "Co-operative Bank of Kenya", "Banking", 5_867_174_695),
    ("ABSA", "ABSA Bank Kenya PLC", "Banking", 5_431_536_000),
    ("SBIC", "Stanbic Holdings PLC", "Banking", 395_321_638),
)

# --- FILLED (source: NSE market-statistics page (public), "ISIN Code" column) ---
_EQUITY_ISINS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "SCOM": "KE1000001402",
        "EQTY": "KE0000000554",
        "KCB": "KE0000000315",
        "EABL": "KE0000000216",
        "KEGN": "KE0000000547",
        "COOP": "KE1000001568",
        "ABSA": "KE0000000067",
        "SBIC": "KE0000000091",
    }
)
# --- END FILLED ---

# --- SYNTHETIC --- free float is not in the attachment; illustrative percentages
_EQUITY_FREE_FLOAT_PCT: Final[Mapping[str, Decimal]] = MappingProxyType(
    {
        "SCOM": Decimal("25.00"),
        "EQTY": Decimal("85.00"),
        "KCB": Decimal("80.00"),
        "EABL": Decimal("35.00"),
        "KEGN": Decimal("30.00"),
        "COOP": Decimal("35.00"),
        "ABSA": Decimal("31.60"),
        "SBIC": Decimal("25.00"),
    }
)
# --- END SYNTHETIC ---

EQUITIES: Final[tuple[EquityReference, ...]] = tuple(
    EquityReference(
        ticker=ticker,
        company_name=company_name,
        sector=sector,
        shares_outstanding=shares_outstanding,
        isin=_EQUITY_ISINS[ticker],
        free_float_pct=_EQUITY_FREE_FLOAT_PCT[ticker],
    )
    for ticker, company_name, sector, shares_outstanding in _EQUITY_ATTACHMENT_ROWS
)
"""The eight seeded counters in seeding order: four distinct sectors, five of them banks."""

TICKERS: Final[tuple[str, ...]] = tuple(equity.ticker for equity in EQUITIES)
"""Tickers of :data:`EQUITIES`, in the same order."""

# Macro indicators -------------------------------------------------------------------------------

MACRO_COLUMNS: Final[tuple[str, ...]] = (
    "inflation_yoy_pct",
    "cbr_rate_pct",
    "interbank_rate_pct",
    "tbill_91d_pct",
    "tbill_182d_pct",
    "tbill_364d_pct",
    "fx_kes_usd",
    "fx_reserves_usd_m",
)
"""The ``terminal_macro_indicators`` indicator columns in table order; every macro mapping below is
keyed by these names in this order."""

# The 2026-09-30 row. Every entry outside the FILLED block is SEED_MARKET_DATA.md §1, verbatim.
# The attachment's T-bill, interbank and reserves values differ from CBK's published September
# 2026 figures. The seeder follows the attachment verbatim; the first CBK pipeline runs overwrite
# the columns they publish for this date through the COALESCE upsert, and source_by_field records
# which source supplied each column.
MACRO_ANCHOR: Final[Mapping[str, Decimal]] = MappingProxyType(
    {
        "inflation_yoy_pct": Decimal("6.80"),
        # --- FILLED (source: CBK published Central Bank Rate, September 2026) ---
        # The attachment omits the Central Bank Rate.
        "cbr_rate_pct": Decimal("8.75"),
        # --- END FILLED ---
        "interbank_rate_pct": Decimal("12.85"),
        "tbill_91d_pct": Decimal("15.72"),
        "tbill_182d_pct": Decimal("16.58"),
        "tbill_364d_pct": Decimal("16.84"),
        "fx_kes_usd": Decimal("129.20"),
        "fx_reserves_usd_m": Decimal("7850.00"),
    }
)
"""Exact indicator values on :data:`ANCHOR_DATE`; the generator applies them without noise."""

MACRO_FIXED_POINTS: Final[tuple[tuple[str, date, Decimal], ...]] = (
    # SEED_MARKET_DATA.md §1: August 2026 inflation, the month before the anchor.
    ("inflation_yoy_pct", date(2026, 8, 31), Decimal("6.60")),
    # --- SYNTHETIC --- §1 calls 6.80 the highest since January 2024, so January 2024 is higher
    ("inflation_yoy_pct", date(2024, 1, 31), Decimal("6.90")),
    # --- END SYNTHETIC ---
    # --- SYNTHETIC --- the start of the mock KES/USD path, rising from 85 to 129.20
    ("fx_kes_usd", date(2012, 1, 31), Decimal("85.00")),
    # --- END SYNTHETIC ---
)
"""``(column, month-end, value)`` points the generator applies exactly, with no noise."""

# --- SYNTHETIC --- keeps the attachment's "highest since January 2024" true for the 2026-09-30 6.80
# Every inflation month-end from February 2024 to July 2026 stays below 6.80: the generator clamps
# it to at most INFLATION_CAP_VALUE.
INFLATION_CAP_START: Final = date(2024, 2, 29)
INFLATION_CAP_END: Final = date(2026, 7, 31)
INFLATION_CAP_VALUE: Final = Decimal("6.70")
# --- END SYNTHETIC ---

# --- SYNTHETIC --- monthly noise of the mock paths: uniform ± this, in each series' own unit
MACRO_NOISE: Final = Decimal("0.15")
# --- END SYNTHETIC ---

# --- SYNTHETIC --- sanity bounds of the mock paths; the generator clamps every month to them
MACRO_BOUNDS: Final[Mapping[str, tuple[Decimal, Decimal]]] = MappingProxyType(
    {
        "inflation_yoy_pct": (Decimal("3.00"), Decimal("9.50")),
        "cbr_rate_pct": (Decimal("7.00"), Decimal("13.00")),
        "interbank_rate_pct": (Decimal("3.00"), Decimal("14.00")),
        "tbill_91d_pct": (Decimal("6.00"), Decimal("17.00")),
        "tbill_182d_pct": (Decimal("6.00"), Decimal("17.00")),
        "tbill_364d_pct": (Decimal("6.00"), Decimal("17.00")),
        "fx_kes_usd": (Decimal("85.00"), Decimal("129.20")),
        "fx_reserves_usd_m": (Decimal("5500.00"), Decimal("9500.00")),
    }
)
# --- END SYNTHETIC ---

# --- SYNTHETIC --- yearly 31 January knots of the smooth mock macro paths
# Values are in MACRO_COLUMNS order: inflation, CBR, interbank, 91-day, 182-day and 364-day
# T-bill, KES/USD, reserves (USD m). Within each year the T-bill knots are ordered
# 91-day <= 182-day <= 364-day. The generator adds MACRO_FIXED_POINTS and MACRO_ANCHOR, then
# interpolates linearly between knots by calendar days.
_MACRO_KNOT_ROWS: Final[tuple[tuple[int, tuple[str, ...]], ...]] = (
    (2012, ("9.20", "12.80", "13.50", "16.50", "16.80", "16.95", "85.00", "5600.00")),
    (2013, ("4.10", "9.50", "7.00", "8.30", "8.80", "9.20", "85.80", "6200.00")),
    (2014, ("7.20", "8.50", "8.80", "9.30", "9.90", "10.40", "86.30", "6600.00")),
    (2015, ("5.50", "8.50", "6.50", "8.60", "9.90", "10.50", "90.50", "7200.00")),
    (2016, ("7.80", "11.50", "4.70", "10.90", "12.20", "12.80", "102.20", "7300.00")),
    (2017, ("7.00", "10.00", "4.70", "8.60", "10.50", "10.90", "103.60", "7800.00")),
    (2018, ("4.80", "10.00", "5.80", "8.00", "9.90", "11.10", "103.80", "7100.00")),
    (2019, ("4.70", "9.00", "3.30", "7.20", "8.40", "9.70", "104.00", "8700.00")),
    (2020, ("5.80", "8.50", "4.50", "7.20", "8.20", "9.30", "105.00", "8700.00")),
    (2021, ("5.70", "7.00", "5.30", "6.90", "7.60", "8.90", "110.00", "7700.00")),
    (2022, ("5.40", "7.00", "4.90", "7.30", "8.10", "9.70", "113.60", "8300.00")),
    (2023, ("9.00", "8.75", "6.60", "9.40", "9.90", "10.70", "122.00", "6900.00")),
    (2024, ("6.90", "12.50", "13.10", "15.90", "16.10", "16.70", "125.00", "6800.00")),
    (2025, ("3.50", "11.25", "10.00", "13.80", "14.40", "14.90", "127.00", "7400.00")),
    (2026, ("4.40", "9.00", "9.20", "15.10", "15.80", "16.20", "128.50", "7600.00")),
)
MACRO_KNOTS: Final[Mapping[str, tuple[tuple[date, Decimal], ...]]] = MappingProxyType(
    {
        column: tuple(
            (date(year, 1, 31), Decimal(values[index])) for year, values in _MACRO_KNOT_ROWS
        )
        for index, column in enumerate(MACRO_COLUMNS)
    }
)
"""Per column, the 15 ``(31 January, value)`` knots of 2012 to 2026, in date order."""
# --- END SYNTHETIC ---


# Election cycles --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CycleReference:
    """One event definition of ``terminal_cycles``; field names are the model's column names."""

    label: str
    event_date: date
    # --- SYNTHETIC --- event_type label: §4 names the election cycles but gives no type code
    event_type: str = "general_election"
    # --- END SYNTHETIC ---


# SEED_MARKET_DATA.md §4, verbatim: cycle labels and election dates. The 2027 row defines the event
# only: no seed row is dated after ANCHOR_DATE, and its trajectory is the backtester's labelled
# projection from the three completed cycles.
CYCLES: Final[tuple[CycleReference, ...]] = (
    CycleReference(label="2013 Cycle", event_date=date(2013, 3, 4)),
    CycleReference(label="2017 Cycle", event_date=date(2017, 8, 8)),
    CycleReference(label="2022 Cycle", event_date=date(2022, 8, 9)),
    CycleReference(label="2027 Election Watch", event_date=date(2027, 8, 10)),
)

# Quickmart IPO preset ---------------------------------------------------------------------------

# SEED_MARKET_DATA.md §2, verbatim: the preset's three risk scenarios, each "<title>: <text>".
QUICKMART_RISK_SCENARIO_LINES: Final[tuple[str, ...]] = (
    "Ownership & Motive: 50% secondary exit with zero primary capital injection into Quickmart.",
    (
        "Growth Financing: Future store expansion must rely on retained earnings, supplier "
        "credit, or bank debt rather than IPO cash."
    ),
    (
        "Valuation Floor: Test whether KES 7.50 per share is justified under varying WACC "
        "(16%–20%), thin supermarket operating margins (3%–6%), and working-capital stress "
        "scenarios."
    ),
)

QUICKMART_RISK_SCENARIOS: Final[tuple[Mapping[str, str], ...]] = tuple(
    MappingProxyType({"title": title.strip(), "description": description.strip()})
    for title, _, description in (line.partition(":") for line in QUICKMART_RISK_SCENARIO_LINES)
)
"""The risk scenarios as stored in ``risk_scenarios``: each line split at its first colon into
``{"title", "description"}``."""


@dataclass(frozen=True, slots=True)
class IpoPresetReference:
    """The value columns of one ``terminal_ipo_scenarios`` preset, named as the model's columns.

    ``assumed_wacc_pct`` is the sandbox's ``wacc_pct``, ``assumed_growth_pct`` its
    ``revenue_growth_pct`` and ``shares_outstanding_post_offer`` its ``shares_outstanding``. The
    base EBITDA margin is not stored: it is ``base_ebitda_kes / base_revenue_kes * 100``.
    """

    company_name: str
    sector: str
    offer_price_kes: Decimal
    shares_offered: int
    shares_outstanding_post_offer: int
    pct_stake_sold: Decimal
    primary_proceeds_kes: Decimal
    base_revenue_kes: Decimal
    base_ebitda_kes: Decimal
    net_debt_kes: Decimal
    assumed_wacc_pct: Decimal
    assumed_growth_pct: Decimal
    terminal_growth_pct: Decimal
    margin_expansion_pct: Decimal
    tax_rate_pct: Decimal
    capex_pct_revenue: Decimal
    da_pct_revenue: Decimal
    nwc_pct_revenue: Decimal
    peer_ev_ebitda_multiple: Decimal
    dcf_weight_pct: Decimal
    forecast_years: int


# With these values the sandbox gives a blended fair value of KES 6.82 per share, -9.11% against
# the KES 7.50 offer. The DCF unit tests and the seeded endpoint tests assert both figures, so
# changing any value here breaks them.
QUICKMART_PRESET: Final[IpoPresetReference] = IpoPresetReference(
    # SEED_MARKET_DATA.md §2, verbatim: the seed's full company name, sector, offer price, stake
    # sold (%) and primary proceeds (KES).
    company_name="Quickmart Supermarket",
    sector="Retail & Consumer Goods",
    offer_price_kes=Decimal("7.50"),
    pct_stake_sold=Decimal("50.0"),
    primary_proceeds_kes=Decimal("0.00"),
    # --- SYNTHETIC --- share counts and operating assumptions the attachment does not give
    # 500,000,000 of 1,000,000,000 shares offered matches the 50% secondary sale with no new issue.
    shares_outstanding_post_offer=1_000_000_000,
    shares_offered=500_000_000,
    base_revenue_kes=Decimal("45000000000.00"),
    # A 4.50% base EBITDA margin, inside the attachment's 3%-6% margin band.
    base_ebitda_kes=Decimal("2025000000.00"),
    net_debt_kes=Decimal("4000000000.00"),
    # The midpoint of the attachment's 16%-20% WACC range.
    assumed_wacc_pct=Decimal("18.00"),
    assumed_growth_pct=Decimal("10.00"),
    terminal_growth_pct=Decimal("5.00"),
    margin_expansion_pct=Decimal("0.50"),
    # Kenya's corporate income tax rate.
    tax_rate_pct=Decimal("30.00"),
    capex_pct_revenue=Decimal("2.00"),
    da_pct_revenue=Decimal("1.50"),
    # Negative: supermarkets run on supplier credit, so working capital funds growth.
    nwc_pct_revenue=Decimal("-3.00"),
    peer_ev_ebitda_multiple=Decimal("6.00"),
    dcf_weight_pct=Decimal("50"),
    forecast_years=5,
    # --- END SYNTHETIC ---
)


# Prices, volumes and foreign flows --------------------------------------------------------------

# --- FILLED (source: NSE market-statistics page (public), "Last Traded Price" column) ---
# Each counter's synthetic price path is rescaled so that its ANCHOR_DATE close equals this price.
PRICE_ANCHORS: Final[Mapping[str, Decimal]] = MappingProxyType(
    {
        "SCOM": Decimal("33.25"),
        "EQTY": Decimal("95.75"),
        "KCB": Decimal("85.00"),
        "EABL": Decimal("269.50"),
        "KEGN": Decimal("10.60"),
        "COOP": Decimal("36.70"),
        "ABSA": Decimal("30.55"),
        "SBIC": Decimal("270.50"),
    }
)
# --- END FILLED ---

# --- FILLED (source: NSE market-statistics page (public), "Volume" column) ---
# The base daily volume (shares) of each counter, before noise and the election overlay.
BASE_VOLUMES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "SCOM": 2_754_329,
        "EQTY": 1_888_186,
        "KCB": 1_186_912,
        "EABL": 22_494,
        "KEGN": 987_240,
        "COOP": 720_756,
        "ABSA": 81_334,
        "SBIC": 11_221,
    }
)
# --- END FILLED ---

# --- SYNTHETIC --- parameters of the geometric Brownian motion behind each mock price path
ANNUAL_DRIFT: Final = Decimal("0.06")
"""Annual drift of the log price, as a fraction."""
VOLATILITY: Final[Mapping[str, Decimal]] = MappingProxyType(
    {
        "SCOM": Decimal("0.25"),
        "EQTY": Decimal("0.22"),
        "KCB": Decimal("0.22"),
        "EABL": Decimal("0.20"),
        "KEGN": Decimal("0.30"),
        "COOP": Decimal("0.22"),
        "ABSA": Decimal("0.22"),
        "SBIC": Decimal("0.22"),
    }
)
"""Annual volatility per ticker, as a fraction: 0.22 for every bank."""
TRADING_DAYS_PER_YEAR: Final = 252
"""Sessions per year: each step of the path is dt = 1/252 years."""
# --- END SYNTHETIC ---

# --- SYNTHETIC --- noise scales of the mock volumes, OHLC and foreign flows
VOLUME_LOG_SIGMA: Final = Decimal("0.5")
"""Standard deviation of the log-normal noise on daily volume."""
OHLC_MAX_EPSILON: Final = Decimal("0.01")
"""Largest opening gap and high/low extension, as a fraction of price."""
FLOW_NOISE_SIGMA: Final = Decimal("0.05")
"""Standard deviation of the normal noise on the foreign-flow fraction of turnover."""
FLOW_FRACTION_CLIP: Final = Decimal("0.5")
"""The foreign-flow fraction of turnover is clipped to plus or minus this value."""
# --- END SYNTHETIC ---

# --- SYNTHETIC --- election-cycle shapes planted in the mock history
# Offsets o are calendar days from each CYCLES event date; seed/synthetic.py holds the shape
# functions, multiplies the price and volume overlays of successive cycles and sums their flow
# fractions. The volume overlay is OVERLAY_NEUTRAL and the flow fraction zero outside their
# windows. The price overlay is neutral only before its window: it holds PRICE_OVERLAY_POST from
# +150 on, so each completed cycle's level carries forward and compounds across cycles.
OVERLAY_NEUTRAL: Final = Decimal("1.00")
# Price: neutral up to o = -150; linear to PRE_FLOOR at -5; FLOOR from -5 to +5 (the valuation
# floor); linear to POST at +150; POST held afterwards.
PRICE_OVERLAY_DECLINE_START: Final = -150
PRICE_OVERLAY_FLOOR_START: Final = -5
PRICE_OVERLAY_FLOOR_END: Final = 5
PRICE_OVERLAY_RECOVERY_END: Final = 150
PRICE_OVERLAY_PRE_FLOOR: Final = Decimal("0.88")
PRICE_OVERLAY_FLOOR: Final = Decimal("0.85")
PRICE_OVERLAY_POST: Final = Decimal("1.05")
# Volume: neutral before o = -60; linear to PRE_TROUGH at -10; EXHAUSTION from -10 to +5 (volume
# exhaustion); SURGE from +5 to +15; linear back to neutral at +60.
VOLUME_OVERLAY_START: Final = -60
VOLUME_OVERLAY_TROUGH_START: Final = -10
VOLUME_OVERLAY_PRE_TROUGH: Final = Decimal("0.60")
VOLUME_OVERLAY_EXHAUSTION: Final = Decimal("0.45")
VOLUME_OVERLAY_SURGE_START: Final = 5
VOLUME_OVERLAY_SURGE_END: Final = 15
VOLUME_OVERLAY_SURGE: Final = Decimal("1.80")
VOLUME_OVERLAY_END: Final = 60
# Foreign flow fraction of turnover: PRE (pre-election outflow) from o = -120 to -1; POST from +10
# to +120; zero otherwise. FLOW_NOISE_SIGMA noise is added and FLOW_FRACTION_CLIP applied.
FLOW_PRE_START: Final = -120
FLOW_PRE_END: Final = -1
FLOW_PRE_FRACTION: Final = Decimal("-0.20")
FLOW_POST_START: Final = 10
FLOW_POST_END: Final = 120
FLOW_POST_FRACTION: Final = Decimal("0.12")
# --- END SYNTHETIC ---

# Fundamentals -----------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FundamentalTarget:
    """Target ratios from which the seeder derives one counter's synthetic FY fundamentals.

    The latest fiscal year, ``last_fy``, is set from these ratios at the anchor market cap; each
    earlier year scales it back. ``pe``, ``pb`` and ``net_debt_to_ebitda`` are multiples (x);
    ``net_margin_pct``, ``payout_pct`` and ``ebitda_margin_pct`` are percentages. The last two
    fields are ``None`` for banks, whose EBITDA, debt, cash and operating cash flow are not seeded.
    Period ends are ``date(fy, fy_end_month, fy_end_day)`` for ``fy`` in ``first_fy..last_fy``.
    """

    ticker: str
    fy_end_month: int
    fy_end_day: int
    first_fy: int
    last_fy: int
    pe: Decimal
    pb: Decimal
    net_margin_pct: Decimal
    payout_pct: Decimal
    ebitda_margin_pct: Decimal | None
    net_debt_to_ebitda: Decimal | None


# --- SYNTHETIC --- target ratios of the mock fundamentals, in EQUITIES order
# Fiscal year ends follow each company's reporting year: Safaricom 31 March, EABL and KenGen
# 30 June, the banks 31 December. The ranges give 5 x 16 + 2 x 17 + 17 = 131 FY rows, the latest
# period ending before ANCHOR_DATE.
FUNDAMENTAL_TARGETS: Final[tuple[FundamentalTarget, ...]] = (
    FundamentalTarget(
        ticker="SCOM",
        fy_end_month=3,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2026,
        pe=Decimal("14.0"),
        pb=Decimal("4.5"),
        net_margin_pct=Decimal("22"),
        payout_pct=Decimal("70"),
        ebitda_margin_pct=Decimal("45"),
        net_debt_to_ebitda=Decimal("0.3"),
    ),
    FundamentalTarget(
        ticker="EQTY",
        fy_end_month=12,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2025,
        pe=Decimal("4.5"),
        pb=Decimal("0.9"),
        net_margin_pct=Decimal("30"),
        payout_pct=Decimal("30"),
        ebitda_margin_pct=None,
        net_debt_to_ebitda=None,
    ),
    FundamentalTarget(
        ticker="KCB",
        fy_end_month=12,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2025,
        pe=Decimal("4.0"),
        pb=Decimal("0.8"),
        net_margin_pct=Decimal("28"),
        payout_pct=Decimal("30"),
        ebitda_margin_pct=None,
        net_debt_to_ebitda=None,
    ),
    FundamentalTarget(
        ticker="EABL",
        fy_end_month=6,
        fy_end_day=30,
        first_fy=2010,
        last_fy=2026,
        pe=Decimal("12.0"),
        pb=Decimal("5.0"),
        net_margin_pct=Decimal("11"),
        payout_pct=Decimal("60"),
        ebitda_margin_pct=Decimal("25"),
        net_debt_to_ebitda=Decimal("1.5"),
    ),
    FundamentalTarget(
        ticker="KEGN",
        fy_end_month=6,
        fy_end_day=30,
        first_fy=2010,
        last_fy=2026,
        pe=Decimal("6.0"),
        pb=Decimal("0.4"),
        net_margin_pct=Decimal("15"),
        payout_pct=Decimal("30"),
        ebitda_margin_pct=Decimal("55"),
        net_debt_to_ebitda=Decimal("3.0"),
    ),
    FundamentalTarget(
        ticker="COOP",
        fy_end_month=12,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2025,
        pe=Decimal("4.0"),
        pb=Decimal("0.8"),
        net_margin_pct=Decimal("30"),
        payout_pct=Decimal("40"),
        ebitda_margin_pct=None,
        net_debt_to_ebitda=None,
    ),
    FundamentalTarget(
        ticker="ABSA",
        fy_end_month=12,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2025,
        pe=Decimal("5.5"),
        pb=Decimal("1.3"),
        net_margin_pct=Decimal("30"),
        payout_pct=Decimal("55"),
        ebitda_margin_pct=None,
        net_debt_to_ebitda=None,
    ),
    FundamentalTarget(
        ticker="SBIC",
        fy_end_month=12,
        fy_end_day=31,
        first_fy=2010,
        last_fy=2025,
        pe=Decimal("6.0"),
        pb=Decimal("1.0"),
        net_margin_pct=Decimal("30"),
        payout_pct=Decimal("50"),
        ebitda_margin_pct=None,
        net_debt_to_ebitda=None,
    ),
)
# --- END SYNTHETIC ---

# --- SYNTHETIC --- ratios deriving the remaining mock fundamental columns
NON_BANK_CASH_PCT_REVENUE: Final = Decimal("10")
"""Non-bank cash and equivalents, as a percentage of revenue."""
NON_BANK_ASSETS_TO_EQUITY: Final = Decimal("2.0")
"""Non-bank total assets as a multiple of total equity."""
BANK_ASSETS_TO_EQUITY: Final = Decimal("7.5")
"""Bank total assets as a multiple of total equity."""
OCF_TO_NET_INCOME: Final = Decimal("1.2")
"""Non-bank operating cash flow as a multiple of net income."""
FUNDAMENTALS_HISTORY_GROWTH: Final = Decimal("1.08")
"""Year k before the latest is the latest divided by this to the power k, times the noise."""
FUNDAMENTALS_HISTORY_NOISE: Final = Decimal("0.05")
"""Each earlier year is multiplied by (1 + u), u uniform within plus or minus this value."""
# --- END SYNTHETIC ---

# Bond yields ------------------------------------------------------------------------------------

# --- SYNTHETIC --- mock fixed-coupon Treasury bond curve, one observation per tenor per month-end
# Yield = that month-end's 364-day T-bill + the tenor's spread + uniform noise within ±BOND_NOISE,
# with no noise on ANCHOR_DATE: 177 month-ends x 6 tenors = 1,062 rows. Codes read
# FXD-SEED-<tenor>Y-<YYYYMM>. The FXD prefix puts them on the yield curve, which reads fixed-coupon
# bonds only; SEED keeps them from colliding with real CBK issue numbers such as FXD1/2018/025.
BOND_TENOR_SPREADS: Final[tuple[tuple[int, Decimal], ...]] = (
    (2, Decimal("0.20")),
    (5, Decimal("0.45")),
    (10, Decimal("0.70")),
    (15, Decimal("0.90")),
    (20, Decimal("1.05")),
    (25, Decimal("1.15")),
)
"""``(tenor in years, spread over the 364-day T-bill in percentage points)``, by tenor."""
BOND_NOISE: Final = Decimal("0.10")
BOND_CODE_PREFIX: Final = "FXD-SEED-"
# --- END SYNTHETIC ---

# Planted volume spikes --------------------------------------------------------------------------

# --- SYNTHETIC --- spikes planted so the anomaly feed and the screener's filter have results
# On each (ticker, session) below, the generated volume becomes
# max(SPIKE_VOLUME_MULTIPLE x volume, ceil(mean(W) + SPIKE_SD_MULTIPLE x sd(W))), where W is the
# ticker's SPIKE_Z_WINDOW preceding rows; the generator checks that the day's volume Z-score is at
# least SPIKE_MIN_Z before anything is written.
PLANTED_SPIKES: Final[tuple[tuple[str, date], ...]] = (
    ("SCOM", date(2026, 9, 15)),
    ("KCB", date(2026, 9, 24)),
    ("EABL", date(2026, 9, 29)),
)
SPIKE_VOLUME_MULTIPLE: Final = 4
SPIKE_SD_MULTIPLE: Final = Decimal("4")
SPIKE_MIN_Z: Final = Decimal("4.00")
# --- END SYNTHETIC ---

# ARCHITECTURE_AND_SCHEMA.md §3: the screener's 20-day volume Z-score window.
SPIKE_Z_WINDOW: Final = 20

# Watchlist --------------------------------------------------------------------------------------

# --- SYNTHETIC --- a default watchlist holding every seeded counter, so the view is not empty
# Its owner is not stored here: the seeder uses TerminalSettings.default_owner_key ("local").
SEED_WATCHLIST_NAME: Final = "NSE Core 8"
SEED_WATCHLIST_TICKERS: Final[tuple[str, ...]] = TICKERS
# --- END SYNTHETIC ---
