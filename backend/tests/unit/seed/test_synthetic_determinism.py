"""The deterministic fallback seed: determinism, anchors, seeded-history constraints, row counts.

Covers ``opennse_terminal.seed.synthetic`` and the reference values of
``opennse_terminal.seed.reference_data`` with no database, network or file I/O. Every expected
value is a literal of the seed specification (equity records, the Quickmart preset, election
cycles, the 2026-09-30 macro row, closing-price anchors and row counts) or is hand-computed in the
test's docstring; none is read back from the reference module under test.

Comparisons are exact ``Decimal`` equalities or bounds. Nothing is converted to ``float``, and a
value is rounded only where the generator's own rule rounds it (``ROUND_HALF_UP``) or a Z-score
is quantized for display with :func:`quantize_out`.

``generate_all()`` takes a few seconds, so it runs once per module in the :func:`dataset` fixture.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from calendar import monthrange
from collections import Counter
from collections.abc import Callable, Hashable, Sequence
from dataclasses import fields, replace
from datetime import date, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext
from statistics import mean

import pytest

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT, TerminalSettings
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.quant.zscore import Observation, is_spike, rolling_zscores
from opennse_terminal.seed import reference_data as ref
from opennse_terminal.seed.synthetic import (
    EXPECTED_BOND_ROWS,
    EXPECTED_FUNDAMENTAL_ROWS,
    EXPECTED_MACRO_ROWS,
    EXPECTED_PRICE_ROWS,
    EXPECTED_SESSIONS,
    SeedBondRow,
    SeedDataset,
    SeedFundamentalRow,
    SeedMacroRow,
    SeedPriceRow,
    SeedWatchlist,
    add_years,
    canonical_bytes,
    dataset_fingerprint,
    flow_fraction,
    flow_fraction_overlay,
    generate_all,
    generate_bonds,
    generate_fundamentals,
    generate_macro,
    generate_prices,
    generate_watchlist,
    month_ends,
    price_multiplier,
    price_overlay,
    quantize_half_up,
    synthetic_sessions,
    to_row,
    volume_multiplier,
    volume_overlay,
)

ANCHOR = date(2026, 9, 30)
CENT = Decimal("0.01")
HALF_CENT = Decimal("0.005")

TICKERS = ("SCOM", "EQTY", "KCB", "EABL", "KEGN", "COOP", "ABSA", "SBIC")
BANKS = frozenset({"EQTY", "KCB", "COOP", "ABSA", "SBIC"})

# The NSE last traded price each counter's 2026-09-30 close is rescaled to.
ANCHOR_CLOSES = {
    "SCOM": Decimal("33.25"),
    "EQTY": Decimal("95.75"),
    "KCB": Decimal("85.00"),
    "EABL": Decimal("269.50"),
    "KEGN": Decimal("10.60"),
    "COOP": Decimal("36.70"),
    "ABSA": Decimal("30.55"),
    "SBIC": Decimal("270.50"),
}

# Ticker, company name, sector, shares outstanding, ISIN, synthetic free float %.
EQUITY_RECORDS = [
    ("SCOM", "Safaricom PLC", "Telecommunications", 40_065_428_000, "KE1000001402", "25.00"),
    ("EQTY", "Equity Group Holdings", "Banking", 3_773_675_000, "KE0000000554", "85.00"),
    ("KCB", "KCB Group PLC", "Banking", 3_213_462_815, "KE0000000315", "80.00"),
    (
        "EABL",
        "East African Breweries PLC",
        "Manufacturing & Allied",
        790_774_356,
        "KE0000000216",
        "35.00",
    ),
    ("KEGN", "KenGen PLC", "Energy & Petroleum", 6_594_522_339, "KE0000000547", "30.00"),
    ("COOP", "Co-operative Bank of Kenya", "Banking", 5_867_174_695, "KE1000001568", "35.00"),
    ("ABSA", "ABSA Bank Kenya PLC", "Banking", 5_431_536_000, "KE0000000067", "31.60"),
    ("SBIC", "Stanbic Holdings PLC", "Banking", 395_321_638, "KE0000000091", "25.00"),
]
SHARES = {record[0]: record[3] for record in EQUITY_RECORDS}

# The 2026-09-30 macro row: the attachment's values, plus the Central Bank Rate CBK published.
ANCHOR_MACRO = {
    "inflation_yoy_pct": Decimal("6.80"),
    "cbr_rate_pct": Decimal("8.75"),
    "interbank_rate_pct": Decimal("12.85"),
    "tbill_91d_pct": Decimal("15.72"),
    "tbill_182d_pct": Decimal("16.58"),
    "tbill_364d_pct": Decimal("16.84"),
    "fx_kes_usd": Decimal("129.20"),
    "fx_reserves_usd_m": Decimal("7850.00"),
}
MACRO_BOUNDS = {
    "inflation_yoy_pct": (Decimal("3.0"), Decimal("9.5")),
    "cbr_rate_pct": (Decimal("7.0"), Decimal("13.0")),
    "interbank_rate_pct": (Decimal("3.0"), Decimal("14.0")),
    "tbill_91d_pct": (Decimal("6.0"), Decimal("17.0")),
    "tbill_182d_pct": (Decimal("6.0"), Decimal("17.0")),
    "tbill_364d_pct": (Decimal("6.0"), Decimal("17.0")),
    "fx_kes_usd": (Decimal("85"), Decimal("129.20")),
    "fx_reserves_usd_m": (Decimal("5500"), Decimal("9500")),
}

# Bond tenor (years) -> spread over the 364-day T-bill, and the noiseless 2026-09-30 yield.
BOND_SPREADS = {
    2: Decimal("0.20"),
    5: Decimal("0.45"),
    10: Decimal("0.70"),
    15: Decimal("0.90"),
    20: Decimal("1.05"),
    25: Decimal("1.15"),
}
ANCHOR_BOND_YIELDS = {
    2: Decimal("17.04"),
    5: Decimal("17.29"),
    10: Decimal("17.54"),
    15: Decimal("17.74"),
    20: Decimal("17.89"),
    25: Decimal("17.99"),
}

PLANTED_SPIKES = [
    ("SCOM", date(2026, 9, 15)),
    ("KCB", date(2026, 9, 24)),
    ("EABL", date(2026, 9, 29)),
]
SPIKE_MIN_Z = Decimal("4.00")

COMPLETED_CYCLES = [
    ("2013 Cycle", date(2013, 3, 4)),
    ("2017 Cycle", date(2017, 8, 8)),
    ("2022 Cycle", date(2022, 8, 9)),
]
ALL_CYCLES = [*COMPLETED_CYCLES, ("2027 Election Watch", date(2027, 8, 10))]

# The Quickmart preset's risk scenarios, split at the first colon; the dashes are U+2013.
RISK_SCENARIOS = [
    {
        "title": "Ownership & Motive",
        "description": "50% secondary exit with zero primary capital injection into Quickmart.",
    },
    {
        "title": "Growth Financing",
        "description": (
            "Future store expansion must rely on retained earnings, supplier credit, or "
            "bank debt rather than IPO cash."
        ),
    },
    {
        "title": "Valuation Floor",
        "description": (
            "Test whether KES 7.50 per share is justified under varying WACC (16%\u201320%), "
            "thin supermarket operating margins (3%\u20136%), and working-capital stress "
            "scenarios."
        ),
    },
]

# Target P/E and P/B of each counter's latest FY row at its anchor market cap, at 2 dp.
TARGET_MULTIPLES = [
    ("SCOM", "14.00", "4.50"),
    ("EQTY", "4.50", "0.90"),
    ("KCB", "4.00", "0.80"),
    ("EABL", "12.00", "5.00"),
    ("KEGN", "6.00", "0.40"),
    ("COOP", "4.00", "0.80"),
    ("ABSA", "5.50", "1.30"),
    ("SBIC", "6.00", "1.00"),
]

# The terminal_ table each record type fills and its columns, less the database-assigned ones
# (``id``, timestamps, ``ingestion_run_id``, ``filing_id``). The watchlist record holds the
# ``terminal_watchlists.name`` and the tickers of its items; ``owner_key`` is the opaque principal
# subject the seeder assigns, never a user record.
RECORD_COLUMNS = [
    (
        "terminal_daily_prices",
        SeedPriceRow,
        {
            "ticker",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "turnover_kes",
            "turnover_estimated",
            "foreign_net_flow_kes",
            "source",
            "flow_source",
        },
    ),
    (
        "terminal_fundamentals",
        SeedFundamentalRow,
        {
            "ticker",
            "period_end_date",
            "period_type",
            "revenue_kes",
            "ebitda_kes",
            "net_income_kes",
            "total_assets_kes",
            "total_equity_kes",
            "total_debt_kes",
            "cash_and_equivalents_kes",
            "operating_cash_flow_kes",
            "dps_kes",
            "shares_outstanding",
            "currency",
            "source_by_field",
        },
    ),
    (
        "terminal_macro_indicators",
        SeedMacroRow,
        {
            "observation_date",
            "inflation_yoy_pct",
            "cbr_rate_pct",
            "interbank_rate_pct",
            "tbill_91d_pct",
            "tbill_182d_pct",
            "tbill_364d_pct",
            "fx_kes_usd",
            "fx_reserves_usd_m",
            "source_by_field",
        },
    ),
    (
        "terminal_bond_yields",
        SeedBondRow,
        {
            "observation_date",
            "bond_code",
            "isin",
            "original_tenor_years",
            "remaining_tenor_years",
            "maturity_date",
            "coupon_rate_pct",
            "yield_pct",
            "source",
        },
    ),
    ("terminal_watchlists", SeedWatchlist, {"name", "tickers"}),
]

# Columns banks leave NULL: EBITDA and enterprise value are not meaningful for deposit funding.
BANK_NULL_COLUMNS = (
    "ebitda_kes",
    "total_debt_kes",
    "cash_and_equivalents_kes",
    "operating_cash_flow_kes",
)
FUNDAMENTAL_VALUE_COLUMNS = (
    "revenue_kes",
    "ebitda_kes",
    "net_income_kes",
    "total_assets_kes",
    "total_equity_kes",
    "total_debt_kes",
    "cash_and_equivalents_kes",
    "operating_cash_flow_kes",
    "dps_kes",
    "shares_outstanding",
)

_FINGERPRINT_SCRIPT = (
    "from opennse_terminal.seed.synthetic import dataset_fingerprint, generate_all; "
    "print(dataset_fingerprint(generate_all()))"
)


@pytest.fixture(scope="module")
def dataset() -> SeedDataset:
    """The whole seed, generated once for the module."""
    return generate_all()


def prices_of(dataset: SeedDataset, ticker: str) -> list[SeedPriceRow]:
    """Return one counter's price rows in date order."""
    return [row for row in dataset.prices if row.ticker == ticker]


def offset_rows(
    rows: Sequence[SeedPriceRow], event: date, first: int, last: int
) -> list[SeedPriceRow]:
    """Return the rows dated ``first..last`` calendar days (inclusive) from ``event``."""
    return [row for row in rows if first <= (row.trade_date - event).days <= last]


def half_up(value: Decimal, exponent: Decimal = CENT) -> Decimal:
    """Round ``value`` half up to the scale of ``exponent``, as the generator rounds columns."""
    return value.quantize(exponent, rounding=ROUND_HALF_UP)


# Determinism --------------------------------------------------------------------------------------


def test_generate_all_is_byte_identical_across_runs_and_caller_contexts(
    dataset: SeedDataset,
) -> None:
    """A second run, made under a coarse caller context, serializes to the very same bytes.

    The generators enter their own 28-digit context, so a caller's 6-digit ``ROUND_DOWN``
    context cannot change a single digit; the seeder's re-run therefore inserts no rows.
    """
    with localcontext() as context:
        context.prec = 6
        context.rounding = ROUND_DOWN
        rerun = generate_all()
    payload = canonical_bytes(dataset)
    fingerprint = dataset_fingerprint(dataset)

    assert isinstance(payload, bytes)
    assert payload
    assert isinstance(fingerprint, str)
    assert fingerprint
    assert canonical_bytes(rerun) == payload
    assert dataset_fingerprint(rerun) == fingerprint


def test_calendars_are_identical_on_every_call() -> None:
    """AAP 0.8.1: the session calendar, recomputed past its cache, and the month-ends repeat."""
    assert synthetic_sessions.__wrapped__() == synthetic_sessions() == synthetic_sessions()
    assert month_ends() == month_ends()


@pytest.mark.parametrize("hash_seed", ["1", "2"], ids=lambda seed: f"PYTHONHASHSEED={seed}")
def test_output_is_independent_of_the_interpreter_hash_seed(
    dataset: SeedDataset, hash_seed: str
) -> None:
    """A fresh interpreter with a different string-hash seed produces the same fingerprint.

    No set or hash order reaches the output, so the fingerprint of a run under
    ``PYTHONHASHSEED`` 1 or 2 equals this process's, whatever seed it was started with.
    """
    environment = {**os.environ, "PYTHONHASHSEED": hash_seed, "PYTHONDONTWRITEBYTECODE": "1"}
    # The command is this interpreter running a fixed literal script: no untrusted input.
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _FINGERPRINT_SCRIPT],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        timeout=300,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == dataset_fingerprint(dataset)


def test_canonical_form_writes_exact_decimals_iso_dates_and_sorted_compact_json(
    dataset: SeedDataset,
) -> None:
    """Sorted keys with compact separators; decimals keep their exact string (``"7.50"``,
    ``"50.0"``), nulls stay null, dates are ISO and text is UTF-8; the fingerprint is its
    SHA-256."""
    payload = canonical_bytes(dataset)

    assert payload.startswith(
        b'{"bonds":[{"bond_code":"FXD-SEED-2Y-201201","coupon_rate_pct":null,"isin":null,'
    )
    assert b'"offer_price_kes":"7.50","pct_stake_sold":"50.0",' in payload
    assert b'"trade_date":"2026-09-30"' in payload
    assert "(16%–20%)".encode() in payload
    assert payload.endswith(
        b'"watchlist":{"name":"NSE Core 8",'
        b'"tickers":["SCOM","EQTY","KCB","EABL","KEGN","COOP","ABSA","SBIC"]}}'
    )
    assert dataset_fingerprint(dataset) == hashlib.sha256(payload).hexdigest()


def test_canonical_form_rejects_an_unordered_container(dataset: SeedDataset) -> None:
    """A set has no canonical order, so serializing one is a ``TypeError``, never silent."""
    unordered = replace(
        dataset, watchlist=replace(dataset.watchlist, tickers=frozenset(dataset.watchlist.tickers))
    )

    with pytest.raises(TypeError, match="frozenset is not part of the seed's canonical form"):
        canonical_bytes(unordered)


def test_to_row_returns_a_detached_column_mapping(dataset: SeedDataset) -> None:
    """Changing the upsert row's provenance mapping leaves the frozen record untouched."""
    record = dataset.macro[-1]
    row = to_row(record)
    provenance = row["source_by_field"]
    assert isinstance(provenance, dict)
    provenance["inflation_yoy_pct"] = "cbk_inflation"

    assert row["observation_date"] == ANCHOR
    assert "id" not in row
    assert record.source_by_field["inflation_yoy_pct"] == "seed"


# Reference constants ------------------------------------------------------------------------------


def test_reference_constants_equal_the_specified_literals() -> None:
    """AAP 0.8.2 and 0.8.3: the generator's inputs are the specified values, so a wrong constant
    fails here as well as in the generated rows."""
    assert ref.ANCHOR_DATE == date(2026, 9, 30)
    assert ref.SEED_SOURCE == "seed"
    assert ref.BANKING_SECTOR == "Banking"
    assert ref.TICKERS == TICKERS
    assert dict(ref.PRICE_ANCHORS) == ANCHOR_CLOSES
    assert sorted(ref.PLANTED_SPIKES) == sorted(PLANTED_SPIKES)
    assert ref.SPIKE_MIN_Z == SPIKE_MIN_Z
    assert dict(ref.MACRO_ANCHOR) == ANCHOR_MACRO
    assert dict(ref.MACRO_BOUNDS) == MACRO_BOUNDS
    assert dict(ref.BOND_TENOR_SPREADS) == BOND_SPREADS
    assert ref.BOND_CODE_PREFIX == "FXD-SEED-"
    assert ref.SEED_WATCHLIST_NAME == "NSE Core 8"
    assert [(cycle.label, cycle.event_date) for cycle in ref.CYCLES] == ALL_CYCLES
    assert [dict(scenario) for scenario in ref.QUICKMART_RISK_SCENARIOS] == RISK_SCENARIOS
    assert [
        (equity.ticker, equity.company_name, equity.sector, equity.shares_outstanding)
        for equity in ref.EQUITIES
    ] == [record[:4] for record in EQUITY_RECORDS]
    assert DEFAULT_CURRENCY == "KES"
    assert MAX_SHARE_COUNT == 1_000_000_000_000


# Row counts and calendar --------------------------------------------------------------------------


def test_row_counts(dataset: SeedDataset) -> None:
    """3,762 sessions x 8 counters = 30,096 prices; 131 FY rows; 177 month-ends x 6 bonds."""
    assert len(synthetic_sessions()) == 3_762
    assert len(dataset.equities) == 8
    assert len(dataset.prices) == 30_096
    assert len(dataset.fundamentals) == 131
    assert len(dataset.macro) == 177
    assert len(dataset.bonds) == 1_062
    assert len(dataset.cycles) == 4
    assert len(dataset.ipo_scenarios) == 1
    assert (
        EXPECTED_SESSIONS,
        EXPECTED_PRICE_ROWS,
        EXPECTED_FUNDAMENTAL_ROWS,
        EXPECTED_MACRO_ROWS,
        EXPECTED_BOND_ROWS,
    ) == (3_762, 30_096, 131, 177, 1_062)
    assert len(dataset.watchlist.tickers) == 8


def test_natural_keys_are_unique(dataset: SeedDataset) -> None:
    """AAP 0.8.1 and 0.9.2: no two rows share the key the seeder's ``ON CONFLICT`` targets."""
    keys: dict[str, list[Hashable]] = {
        "terminal_daily_prices (ticker, trade_date)": [
            (row.ticker, row.trade_date) for row in dataset.prices
        ],
        "terminal_fundamentals (ticker, period_end_date, period_type)": [
            (row.ticker, row.period_end_date, row.period_type) for row in dataset.fundamentals
        ],
        "terminal_macro_indicators (observation_date)": [
            row.observation_date for row in dataset.macro
        ],
        "terminal_bond_yields (observation_date, bond_code)": [
            (row.observation_date, row.bond_code) for row in dataset.bonds
        ],
        "terminal_equities (ticker)": [row.ticker for row in dataset.equities],
        "terminal_equities (isin)": [row.isin for row in dataset.equities],
        "terminal_cycles (event_date)": [row.event_date for row in dataset.cycles],
        "terminal_ipo_scenarios (company_name)": [
            row.company_name for row in dataset.ipo_scenarios
        ],
        "terminal_watchlist_items (ticker)": list(dataset.watchlist.tickers),
    }

    for key, values in keys.items():
        assert [value for value, count in Counter(values).items() if count > 1] == [], key


@pytest.mark.parametrize(
    ("table", "record_type", "columns"),
    RECORD_COLUMNS,
    ids=[table for table, _, _ in RECORD_COLUMNS],
)
def test_records_carry_the_terminal_table_column_names(
    table: str, record_type: type, columns: set[str]
) -> None:
    """Rule 3: each record's fields are its ``terminal_`` table's column names, with no ``id``.

    Equities, cycles and the IPO preset are checked column by column through :func:`to_row`.
    """
    names = {field.name for field in fields(record_type)}

    assert names == columns, table
    assert "id" not in names
    assert "owner_key" not in names


def test_synthetic_calendar_is_weekdays_minus_eight_fixed_dates() -> None:
    """Weekdays from 2012-01-02 to 2026-09-30 except 1 Jan, 1 May, 1 Jun, 10 Oct, 20 Oct,
    12 Dec, 25 Dec and 26 Dec."""
    excluded = {(1, 1), (5, 1), (6, 1), (10, 10), (10, 20), (12, 12), (12, 25), (12, 26)}
    expected: list[date] = []
    day = date(2012, 1, 2)
    while day <= ANCHOR:
        if day.weekday() < 5 and (day.month, day.day) not in excluded:
            expected.append(day)
        day += timedelta(days=1)

    sessions = synthetic_sessions()

    assert sessions == tuple(expected)
    assert sessions[0] == date(2012, 1, 2)
    assert sessions[-1] == ANCHOR


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (
            date(2024, 1, 31),
            date(2024, 3, 30),
            (date(2024, 1, 31), date(2024, 2, 29)),
        ),
        (
            date(2023, 12, 15),
            date(2024, 1, 31),
            (date(2023, 12, 31), date(2024, 1, 31)),
        ),
    ],
    ids=["inclusive-start-leap-february-end-before-month-end", "year-rollover"],
)
def test_month_ends_within_a_range(start: date, end: date, expected: tuple[date, ...]) -> None:
    """Every calendar month-end ``d`` with ``start <= d <= end``, in order."""
    assert month_ends(start, end) == expected


def test_default_month_ends_are_the_177_seeded_months() -> None:
    """2012-01-31 to 2026-09-30, one per month: each month's last day by ``monthrange``."""
    expected = [
        date(year, month, monthrange(year, month)[1])
        for year in range(2012, 2027)
        for month in range(1, 13)
        if (year, month) <= (2026, 9)
    ]
    ends = month_ends()

    assert list(ends) == expected
    assert len(ends) == 177
    assert ends[0] == date(2012, 1, 31)
    assert ends[-1] == ANCHOR
    assert all((day + timedelta(days=1)).day == 1 for day in ends)


@pytest.mark.parametrize(
    ("day", "years", "expected"),
    [
        (date(2026, 9, 30), 25, date(2051, 9, 30)),
        (date(2016, 2, 29), 2, date(2018, 2, 28)),
        (date(2016, 2, 29), 4, date(2020, 2, 29)),
    ],
    ids=["plain", "29-february-clamped", "29-february-to-leap-year"],
)
def test_add_years(day: date, years: int, expected: date) -> None:
    """Same month and day ``years`` later; 29 February becomes 28 February in a common year."""
    assert add_years(day, years) == expected


@pytest.mark.parametrize(
    ("value", "exponent", "expected"),
    [
        ("2.675", "0.01", "2.68"),
        ("2.665", "0.01", "2.67"),
        ("-2.675", "0.01", "-2.68"),
        ("-0.004", "0.01", "0.00"),
        ("0.33335", "0.0001", "0.3334"),
        ("1234.5", "1", "1235"),
    ],
    ids=["half-up", "half-up-not-half-even", "negative", "negative-zero", "dps-scale", "volume"],
)
def test_quantize_half_up(value: str, exponent: str, expected: str) -> None:
    """Half-up rounding to the column scale; a result rounding to zero is written ``0.00``."""
    assert str(quantize_half_up(Decimal(value), Decimal(exponent))) == expected


# Prices, volumes, turnover and flows --------------------------------------------------------------


@pytest.mark.parametrize("ticker", TICKERS)
def test_each_counter_has_one_row_per_session_ending_on_its_anchor_close(
    dataset: SeedDataset, ticker: str
) -> None:
    """One row per synthetic session, and the 2026-09-30 close equals the NSE last traded price."""
    rows = prices_of(dataset, ticker)

    assert [row.trade_date for row in rows] == list(synthetic_sessions())
    assert rows[-1].trade_date == ANCHOR
    assert rows[-1].close_price == ANCHOR_CLOSES[ticker]


def test_price_rows_are_ordered_by_counter_then_date(dataset: SeedDataset) -> None:
    """Rows come counter by counter in the equity table's order."""
    sessions = len(synthetic_sessions())
    order = [dataset.prices[index * sessions].ticker for index in range(len(TICKERS))]

    assert order == list(TICKERS)


def test_price_rows_follow_the_ohlc_volume_turnover_and_flow_rules(dataset: SeedDataset) -> None:
    """Every row of every counter.

    * prices at 2 dp and above 0; ``low <= min(open, close) <= max(open, close) <= high``;
    * open = previous close x (1 + e), high = max(open, close) x (1 + |e|) and
      low = min(open, close) x (1 - |e|), with |e| <= 1%, each within half a cent of rounding;
    * volume a whole number of shares in 1..10^12;
    * turnover = volume x (high + low + close) / 3 at 2 dp, never estimated;
    * a foreign net flow on every row, at most half the turnover, with seed provenance.
    """
    one_pct = Decimal("0.01")
    previous: SeedPriceRow | None = None
    for row in dataset.prices:
        prices = (row.open_price, row.high_price, row.low_price, row.close_price)
        amounts = (*prices, row.turnover_kes, row.foreign_net_flow_kes)
        assert all(isinstance(amount, Decimal) for amount in amounts), row
        assert all(price > 0 and price.as_tuple().exponent == -2 for price in prices), row
        low_body = min(row.open_price, row.close_price)
        high_body = max(row.open_price, row.close_price)
        assert row.low_price <= low_body <= high_body <= row.high_price, row
        assert row.high_price <= high_body * (1 + one_pct) + HALF_CENT, row
        assert row.low_price >= low_body * (1 - one_pct) - HALF_CENT, row
        if previous is not None and previous.ticker == row.ticker:
            gap = abs(row.open_price - previous.close_price)
            assert gap <= previous.close_price * one_pct + HALF_CENT, row

        # ``type(...) is int`` rather than isinstance, which would also accept a bool.
        assert type(row.volume) is int, row
        assert 1 <= row.volume <= MAX_SHARE_COUNT, row
        typical = (row.high_price + row.low_price + row.close_price) / 3
        assert row.turnover_kes == half_up(row.volume * typical), row
        assert row.turnover_estimated is False, row

        assert row.foreign_net_flow_kes.as_tuple().exponent == -2, row
        assert abs(row.foreign_net_flow_kes) <= row.turnover_kes / 2 + HALF_CENT, row
        assert (row.source, row.flow_source) == ("seed", "seed"), row
        previous = row


@pytest.mark.parametrize(
    ("ticker", "day"), PLANTED_SPIKES, ids=[f"{ticker}-{day}" for ticker, day in PLANTED_SPIKES]
)
def test_planted_spike_volume_zscore_is_at_least_four(
    dataset: SeedDataset, ticker: str, day: date
) -> None:
    """Scored as the screener scores it: N = 20 over the observed market sessions.

    The observed sessions are the distinct seeded trade dates; ``z`` must reach 4.00 both
    unrounded and as displayed, and be a spike at the default 2.00 threshold.
    """
    sessions = sorted({row.trade_date for row in dataset.prices})
    rows = [Observation(row.trade_date, Decimal(row.volume)) for row in prices_of(dataset, ticker)]
    point = next(point for point in rolling_zscores(rows, sessions) if point.trade_date == day)

    assert point.observations_used == 20
    assert point.z is not None, point.flags
    assert point.z >= SPIKE_MIN_Z
    assert quantize_out(point.z, "z") >= SPIKE_MIN_Z
    assert is_spike(point.z)


@pytest.mark.parametrize(
    ("label", "event"), COMPLETED_CYCLES, ids=[label for label, _ in COMPLETED_CYCLES]
)
def test_completed_cycles_are_planted_in_the_daily_series(
    dataset: SeedDataset, label: str, event: date
) -> None:
    """Around each past election, for every counter (offsets in calendar days):

    * pre-election foreign outflow over -120..-1 and inflow over +10..+120;
    * volume exhaustion over -10..+4 below the neutral -150..-61 level;
    * the +5..+14 post-election volume surge above the exhaustion level.
    """
    for ticker in TICKERS:
        rows = prices_of(dataset, ticker)
        pre_flow = sum(row.foreign_net_flow_kes for row in offset_rows(rows, event, -120, -1))
        post_flow = sum(row.foreign_net_flow_kes for row in offset_rows(rows, event, 10, 120))
        neutral = mean(Decimal(row.volume) for row in offset_rows(rows, event, -150, -61))
        exhaustion = mean(Decimal(row.volume) for row in offset_rows(rows, event, -10, 4))
        surge = mean(Decimal(row.volume) for row in offset_rows(rows, event, 5, 14))

        assert pre_flow < 0, (label, ticker)
        assert post_flow > 0, (label, ticker)
        assert exhaustion < neutral, (label, ticker)
        assert surge > exhaustion, (label, ticker)


def test_no_row_is_dated_after_the_anchor(dataset: SeedDataset) -> None:
    """No future history: the 2027 election exists only as an event definition."""
    assert max(row.trade_date for row in dataset.prices) == ANCHOR
    assert max(row.observation_date for row in dataset.macro) == ANCHOR
    assert max(row.observation_date for row in dataset.bonds) == ANCHOR
    assert max(row.period_end_date for row in dataset.fundamentals) <= ANCHOR
    assert dataset.cycles[-1].event_date == date(2027, 8, 10)


# Election-cycle overlays --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (-400, "1.00"),
        (-150, "1.00"),
        (-5, "0.85"),
        (0, "0.85"),
        (5, "0.85"),
        (150, "1.05"),
        (400, "1.05"),
    ],
)
def test_price_overlay_levels(offset: int, expected: str) -> None:
    """1.00 up to -150; the 0.85 valuation floor from -5 to +5; 1.05 from +150 on."""
    assert price_overlay(offset) == Decimal(expected)


def test_price_overlay_declines_into_the_floor_and_recovers_after_it() -> None:
    """Strictly falling from -150 towards 0.88 at -5, strictly rising from +5 towards 1.05."""
    decline = [price_overlay(offset) for offset in range(-150, -5)]
    recovery = [price_overlay(offset) for offset in range(5, 151)]

    assert all(later < earlier for earlier, later in zip(decline, decline[1:], strict=False))
    assert all(later > earlier for earlier, later in zip(recovery, recovery[1:], strict=False))
    assert Decimal("0.88") < decline[-1] < Decimal("1.00")


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (-61, "1.00"),
        (-60, "1.00"),
        (-35, "0.80"),
        (-10, "0.45"),
        (4, "0.45"),
        (5, "1.80"),
        (15, "1.80"),
        (24, "1.64"),
        (60, "1.00"),
        (61, "1.00"),
    ],
)
def test_volume_overlay_levels(offset: int, expected: str) -> None:
    """1.00 at -60 falling linearly to 0.60 at -10 (-35: 1.00 - 0.40 x 25/50 = 0.80); 0.45 from
    -10 to +5; 1.80 from +5 to +15; back to 1.00 at +60 (+24: 1.80 - 0.80 x 9/45 = 1.64)."""
    assert volume_overlay(offset) == Decimal(expected)


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (-121, "0"),
        (-120, "-0.20"),
        (-1, "-0.20"),
        (0, "0"),
        (9, "0"),
        (10, "0.12"),
        (120, "0.12"),
        (121, "0"),
    ],
)
def test_flow_fraction_overlay_levels(offset: int, expected: str) -> None:
    """-0.20 of turnover from -120 to -1, +0.12 from +10 to +120, and 0 otherwise."""
    assert flow_fraction_overlay(offset) == Decimal(expected)


@pytest.mark.parametrize(
    ("overlay", "day", "expected"),
    [
        (price_multiplier, date(2013, 3, 4), "0.85"),
        (price_multiplier, date(2017, 8, 8), "0.8925"),
        (price_multiplier, ANCHOR, "1.157625"),
        (volume_multiplier, date(2013, 3, 4), "0.45"),
        (volume_multiplier, ANCHOR, "1.00"),
        (flow_fraction, date(2022, 8, 8), "-0.20"),
        (flow_fraction, ANCHOR, "0"),
    ],
    ids=[
        "price-2013-floor",
        "price-2017-floor-on-2013-recovery",
        "price-anchor-three-recoveries",
        "volume-2013-exhaustion",
        "volume-anchor-neutral",
        "flow-2022-pre-election",
        "flow-anchor-none",
    ],
)
def test_cycle_overlays_compound_across_cycles(
    overlay: Callable[[date], Decimal], day: date, expected: str
) -> None:
    """Successive cycles multiply: 1.05 x 0.85 = 0.8925, 1.05^3 = 1.157625 on 2026-09-30.

    The 2027 event, 314 days after the anchor, contributes nothing to any seeded day.
    """
    assert overlay(day) == Decimal(expected)


# Macro indicators and bond yields -----------------------------------------------------------------


def test_anchor_macro_row_is_the_attachment_row(dataset: SeedDataset) -> None:
    """2026-09-30 carries the attachment values verbatim, plus the 8.75 Central Bank Rate."""
    row = dataset.macro[-1]

    assert row.observation_date == ANCHOR
    assert {column: getattr(row, column) for column in ANCHOR_MACRO} == ANCHOR_MACRO


def test_anchor_inflation_is_the_highest_since_january_2024(dataset: SeedDataset) -> None:
    """6.90 in January 2024, every month from February 2024 to July 2026 below 6.80, 6.60 in
    August 2026 and 6.80 in September 2026."""
    inflation = {row.observation_date: row.inflation_yoy_pct for row in dataset.macro}
    capped = [day for day in inflation if date(2024, 2, 1) <= day <= date(2026, 7, 31)]
    since_january = [value for day, value in inflation.items() if day > date(2024, 1, 31)]

    assert inflation[date(2024, 1, 31)] == Decimal("6.90")
    assert len(capped) == 30
    assert [day for day in capped if inflation[day] >= Decimal("6.80")] == []
    assert inflation[date(2026, 8, 31)] == Decimal("6.60")
    assert inflation[ANCHOR] == Decimal("6.80")
    assert max(since_january) == Decimal("6.80")


def test_macro_history_spans_bounds_and_orders_the_bills(dataset: SeedDataset) -> None:
    """177 month-ends from 2012-01-31; every value at 2 dp within its bounds; T-bills ordered
    91-day <= 182-day <= 364-day; KES/USD rising from 85.00 to 129.20; all provenance ``seed``."""
    assert [row.observation_date for row in dataset.macro] == list(month_ends())
    assert dataset.macro[0].fx_kes_usd == Decimal("85.00")
    assert dataset.macro[-1].fx_kes_usd == Decimal("129.20")
    for row in dataset.macro:
        for column, (low, high) in MACRO_BOUNDS.items():
            value = getattr(row, column)
            assert isinstance(value, Decimal), (row.observation_date, column, value)
            assert low <= value <= high, (row.observation_date, column, value)
            assert value.as_tuple().exponent == -2, (row.observation_date, column, value)
        assert row.tbill_91d_pct <= row.tbill_182d_pct <= row.tbill_364d_pct, row
        assert row.source_by_field == dict.fromkeys(ANCHOR_MACRO, "seed"), row


def test_anchor_bond_yields_are_the_noiseless_spreads_over_the_364_day_bill(
    dataset: SeedDataset,
) -> None:
    """16.84 + 0.20 / 0.45 / 0.70 / 0.90 / 1.05 / 1.15 for the 2 to 25-year tenors."""
    anchor_bonds = {
        int(row.original_tenor_years): row.yield_pct
        for row in dataset.bonds
        if row.observation_date == ANCHOR
    }

    assert anchor_bonds == ANCHOR_BOND_YIELDS


def test_bond_rows_follow_the_mock_curve_rules(dataset: SeedDataset) -> None:
    """Per month-end, one ``FXD-SEED-<tenor>Y-<YYYYMM>`` bond per tenor; remaining tenor equals
    the nominal one; maturity ``tenor`` years on; yield within 0.10 of 364-day bill + spread."""
    bill_364d = {row.observation_date: row.tbill_364d_pct for row in dataset.macro}
    seen: dict[date, list[int]] = {}
    for row in dataset.bonds:
        tenor = int(row.original_tenor_years)
        day = row.observation_date
        seen.setdefault(day, []).append(tenor)
        # The FXD prefix puts the mock bond on the yield curve; SEED marks it as synthetic.
        assert row.bond_code == f"FXD-SEED-{tenor}Y-{day:%Y%m}", row
        assert isinstance(row.yield_pct, Decimal), row
        assert row.yield_pct > 0, row
        assert row.original_tenor_years == row.remaining_tenor_years == Decimal(tenor), row
        assert row.original_tenor_years.as_tuple().exponent == -2, row
        assert row.maturity_date == add_years(day, tenor), row
        assert (row.isin, row.coupon_rate_pct, row.source) == (None, None, "seed"), row
        assert abs(row.yield_pct - (bill_364d[day] + BOND_SPREADS[tenor])) <= Decimal("0.10"), row

    assert list(seen) == list(month_ends())
    assert all(tenors == list(BOND_SPREADS) for tenors in seen.values())
    assert len({row.bond_code for row in dataset.bonds}) == 1_062


@pytest.mark.parametrize(
    ("build", "match"),
    [
        (
            lambda macro: (replace(macro[-1], observation_date=date(2026, 10, 31)),),
            "after the seed anchor 2026-09-30; no future rows may be seeded",
        ),
        (lambda macro: (macro[1], macro[0]), "macro rows must ascend strictly"),
        (
            lambda macro: (replace(macro[0], tbill_364d_pct=Decimal("-5.00")),),
            "bond yield for 2Y on 2012-01-31 is .*, not > 0",
        ),
    ],
    ids=["future-month", "descending-months", "non-positive-yield"],
)
def test_generate_bonds_rejects_invalid_macro_months(
    dataset: SeedDataset,
    build: Callable[[Sequence[SeedMacroRow]], Sequence[SeedMacroRow]],
    match: str,
) -> None:
    """Bonds are never dated after 2026-09-30, need ascending months and a positive yield."""
    with pytest.raises(RuntimeError, match=match):
        generate_bonds(build(dataset.macro))


# Fundamentals -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ticker", "month", "day", "first_fy", "last_fy"),
    [
        ("SCOM", 3, 31, 2010, 2026),
        ("EQTY", 12, 31, 2010, 2025),
        ("KCB", 12, 31, 2010, 2025),
        ("EABL", 6, 30, 2010, 2026),
        ("KEGN", 6, 30, 2010, 2026),
        ("COOP", 12, 31, 2010, 2025),
        ("ABSA", 12, 31, 2010, 2025),
        ("SBIC", 12, 31, 2010, 2025),
    ],
)
def test_fundamental_periods_follow_each_financial_year_end(
    dataset: SeedDataset, ticker: str, month: int, day: int, first_fy: int, last_fy: int
) -> None:
    """Safaricom 31 March, EABL and KenGen 30 June, banks 31 December; all FY rows in KES with
    the attachment share count."""
    rows = [row for row in dataset.fundamentals if row.ticker == ticker]

    assert [row.period_end_date for row in rows] == [
        date(year, month, day) for year in range(first_fy, last_fy + 1)
    ]
    assert {(row.period_type, row.currency, row.shares_outstanding) for row in rows} == {
        ("FY", "KES", SHARES[ticker])
    }


def test_banks_leave_ebitda_debt_cash_and_operating_cash_flow_null(dataset: SeedDataset) -> None:
    """Banks: the four columns NULL and absent from provenance; non-banks: every column set.

    ``source_by_field`` maps exactly the populated value columns to ``seed``.
    """
    for row in dataset.fundamentals:
        values = {column: getattr(row, column) for column in FUNDAMENTAL_VALUE_COLUMNS}
        null_columns = [column for column, value in values.items() if value is None]
        populated = [column for column in FUNDAMENTAL_VALUE_COLUMNS if column not in null_columns]

        assert null_columns == (list(BANK_NULL_COLUMNS) if row.ticker in BANKS else []), row
        assert row.source_by_field == dict.fromkeys(populated, "seed"), row


def test_fundamental_values_are_exact_decimals_in_kes(dataset: SeedDataset) -> None:
    """Rule 4: every populated money and dividend column is a ``Decimal``, the share count an
    ``int`` within 1..10^12, and the currency KES."""
    amount_columns = [
        column for column in FUNDAMENTAL_VALUE_COLUMNS if column != "shares_outstanding"
    ]
    for row in dataset.fundamentals:
        assert type(row.shares_outstanding) is int, row
        assert 1 <= row.shares_outstanding <= MAX_SHARE_COUNT, row
        assert row.currency == DEFAULT_CURRENCY, row
        for column in amount_columns:
            value = getattr(row, column)
            assert value is None or isinstance(value, Decimal), (row.ticker, column, value)


@pytest.mark.parametrize(("ticker", "pe", "pb"), TARGET_MULTIPLES, ids=TICKERS)
def test_latest_fundamentals_price_at_the_target_multiples(
    dataset: SeedDataset, ticker: str, pe: str, pb: str
) -> None:
    """AAP 0.8.3: at the anchor market cap (2026-09-30 close x the equity's share count), the
    latest FY row's net income and equity give the target P/E and P/B, rounded half up to 2 dp.

    The latest year carries no history noise, so only the 0.01 rounding of a billions-scale net
    income or equity separates the ratio from its target.
    """
    latest = max(
        (row for row in dataset.fundamentals if row.ticker == ticker),
        key=lambda row: row.period_end_date,
    )
    equity = next(row for row in dataset.equities if row.ticker == ticker)
    market_cap = ANCHOR_CLOSES[ticker] * equity.shares_outstanding

    assert half_up(market_cap / latest.net_income_kes) == Decimal(pe)
    assert half_up(market_cap / latest.total_equity_kes) == Decimal(pb)


@pytest.mark.parametrize(
    ("ticker", "period_end", "pe", "expected"),
    [
        (
            "SCOM",
            date(2026, 3, 31),
            "14",
            {
                "net_income_kes": Decimal("95155391500.00"),
                "total_equity_kes": Decimal("296038995777.78"),
                "revenue_kes": Decimal("432524506818.18"),
                "dps_kes": Decimal("1.6625"),
                "ebitda_kes": Decimal("194636028068.18"),
                "cash_and_equivalents_kes": Decimal("43252450681.82"),
                "total_debt_kes": Decimal("101643259102.27"),
                "total_assets_kes": Decimal("592077991555.56"),
                "operating_cash_flow_kes": Decimal("114186469800.00"),
            },
        ),
        (
            "KCB",
            date(2025, 12, 31),
            "4",
            {
                "net_income_kes": Decimal("68286084818.75"),
                "total_equity_kes": Decimal("341430424093.75"),
                "revenue_kes": Decimal("243878874352.68"),
                "dps_kes": Decimal("6.3750"),
                "ebitda_kes": None,
                "cash_and_equivalents_kes": None,
                "total_debt_kes": None,
                "total_assets_kes": Decimal("2560728180703.13"),
                "operating_cash_flow_kes": None,
            },
        ),
    ],
    ids=["SCOM-non-bank", "KCB-bank"],
)
def test_latest_fundamentals_follow_the_target_ratios_at_the_anchor_market_cap(
    dataset: SeedDataset,
    ticker: str,
    period_end: date,
    pe: str,
    expected: dict[str, Decimal | None],
) -> None:
    """Hand-computed from the anchor close, the share count and the target ratios.

    SCOM: mcap = 33.25 x 40,065,428,000 = 1,332,175,481,000; NI = mcap / 14 = 95,155,391,500;
    equity = mcap / 4.5 = 296,038,995,777.78; revenue = NI / 22% = 432,524,506,818.18;
    dps = 33.25 x 70% / 14 = 1.6625; EBITDA = 45% of revenue = 194,636,028,068.18; cash = 10%
    of revenue = 43,252,450,681.82; debt = 0.3 x EBITDA + cash = 101,643,259,102.27;
    assets = 2 x equity = 592,077,991,555.56; operating cash flow = 1.2 x NI = 114,186,469,800.

    KCB: mcap = 85.00 x 3,213,462,815 = 273,144,339,275; NI = mcap / 4.0 = 68,286,084,818.75;
    equity = mcap / 0.8 = 341,430,424,093.75; revenue = NI / 28% = 243,878,874,352.68;
    dps = 85.00 x 30% / 4.0 = 6.3750; assets = 7.5 x equity = 2,560,728,180,703.13 (half up).
    """
    row = next(
        row
        for row in dataset.fundamentals
        if row.ticker == ticker and row.period_end_date == period_end
    )

    assert {column: getattr(row, column) for column in expected} == expected
    assert ANCHOR_CLOSES[ticker] * SHARES[ticker] / row.net_income_kes == Decimal(pe)


@pytest.mark.parametrize("ticker", TICKERS)
def test_earlier_fundamentals_scale_back_eight_percent_a_year_within_five_percent(
    dataset: SeedDataset, ticker: str
) -> None:
    """Year ``k`` before the latest is the latest / 1.08^k x (1 + u), u within +/-5%, for every
    value column alike (each bound widened by the 0.01 of rounding)."""
    rows = [row for row in dataset.fundamentals if row.ticker == ticker]
    latest = rows[-1]
    for years_back, row in enumerate(reversed(rows[:-1]), start=1):
        growth = Decimal("1.08") ** years_back
        for column in ("revenue_kes", "net_income_kes", "total_equity_kes", "total_assets_kes"):
            value, latest_value = getattr(row, column), getattr(latest, column)
            low = latest_value * Decimal("0.95") / growth - CENT
            high = latest_value * Decimal("1.05") / growth + CENT
            assert low <= value <= high, (ticker, row.period_end_date, column, value)
        margin = row.net_income_kes / row.revenue_kes
        latest_margin = latest.net_income_kes / latest.revenue_kes
        assert abs(margin - latest_margin) < Decimal("0.000001"), (ticker, row.period_end_date)


# Reference rows -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ticker", "company_name", "sector", "shares", "isin", "free_float"),
    EQUITY_RECORDS,
    ids=[record[0] for record in EQUITY_RECORDS],
)
def test_equity_records(
    dataset: SeedDataset,
    ticker: str,
    company_name: str,
    sector: str,
    shares: int,
    isin: str,
    free_float: str,
) -> None:
    """Ticker, name, sector and shares from the attachment; ISIN observed; free float synthetic."""
    row = next(row for row in dataset.equities if row.ticker == ticker)

    assert to_row(row) == {
        "ticker": ticker,
        "isin": isin,
        "company_name": company_name,
        "sector": sector,
        "shares_outstanding": shares,
        "free_float_pct": Decimal(free_float),
        "currency": "KES",
        "source": "seed",
    }


def test_equities_are_seeded_in_attachment_order(dataset: SeedDataset) -> None:
    """Eight counters, five of them banks, in four sectors."""
    assert [row.ticker for row in dataset.equities] == list(TICKERS)
    assert {row.ticker for row in dataset.equities if row.sector == "Banking"} == BANKS
    assert len({row.sector for row in dataset.equities}) == 4


def test_equity_values_carry_exact_types(dataset: SeedDataset) -> None:
    """Rule 4: shares are an ``int`` within 0..10^12, free float a ``Decimal``, currency KES.

    ``to_row`` equality cannot tell ``25.0`` or ``Decimal("25.00")`` apart, so types are checked.
    """
    for row in dataset.equities:
        assert type(row.shares_outstanding) is int, row
        assert 0 <= row.shares_outstanding <= MAX_SHARE_COUNT, row
        assert isinstance(row.free_float_pct, Decimal), row
        assert row.currency == DEFAULT_CURRENCY, row


def test_quickmart_preset(dataset: SeedDataset) -> None:
    """The attachment's preset with its synthetic fills: 500,000,000 of 1,000,000,000 shares is
    the 50% secondary sale; EBITDA 2,025,000,000 on revenue 45,000,000,000 is a 4.50% margin."""
    (preset,) = dataset.ipo_scenarios

    assert to_row(preset) == {
        "company_name": "Quickmart Supermarket",
        "sector": "Retail & Consumer Goods",
        "offer_price_kes": Decimal("7.50"),
        "shares_offered": 500_000_000,
        "shares_outstanding_post_offer": 1_000_000_000,
        "pct_stake_sold": Decimal("50.0"),
        "primary_proceeds_kes": Decimal("0.00"),
        "base_revenue_kes": Decimal("45000000000"),
        "base_ebitda_kes": Decimal("2025000000"),
        "net_debt_kes": Decimal("4000000000"),
        "assumed_wacc_pct": Decimal("18.00"),
        "assumed_growth_pct": Decimal("10.00"),
        "terminal_growth_pct": Decimal("5.00"),
        "margin_expansion_pct": Decimal("0.50"),
        "tax_rate_pct": Decimal("30.00"),
        "capex_pct_revenue": Decimal("2.00"),
        "da_pct_revenue": Decimal("1.50"),
        "nwc_pct_revenue": Decimal("-3.00"),
        "peer_ev_ebitda_multiple": Decimal("6.00"),
        "dcf_weight_pct": Decimal("50"),
        "forecast_years": 5,
        "risk_scenarios": RISK_SCENARIOS,
        "currency": "KES",
        "source": "seed",
        "is_preset": True,
    }
    assert preset.base_ebitda_kes / preset.base_revenue_kes * 100 == Decimal("4.5")
    # Decimal division: int / int would compute a float.
    assert Decimal(preset.shares_offered) / preset.shares_outstanding_post_offer * 100 == (
        preset.pct_stake_sold
    )
    assert preset.primary_proceeds_kes <= preset.offer_price_kes * preset.shares_offered
    assert preset.is_preset is True
    assert preset.currency == DEFAULT_CURRENCY


def test_quickmart_preset_values_carry_exact_types(dataset: SeedDataset) -> None:
    """AAP 0.8.2 and Rule 4: money, rates and multiples are ``Decimal``; counts are ``int``.

    ``Decimal("7.50") == 7.5`` is true in Python, so the dict equality above cannot tell a float
    or an int from a ``Decimal``; this checks each field's type.
    """
    (preset,) = dataset.ipo_scenarios
    counts = {"shares_offered", "shares_outstanding_post_offer", "forecast_years"}
    texts = {"company_name", "sector", "currency", "source"}

    for field in fields(preset):
        value = getattr(preset, field.name)
        if field.name in counts:
            assert type(value) is int, field.name
        elif field.name in texts:
            assert type(value) is str, field.name
        elif field.name == "risk_scenarios":
            assert type(value) is list, field.name
        elif field.name == "is_preset":
            assert value is True
        else:
            assert isinstance(value, Decimal), field.name
    assert 1 <= preset.shares_offered <= preset.shares_outstanding_post_offer <= MAX_SHARE_COUNT


def test_election_cycles(dataset: SeedDataset) -> None:
    """The four general-election events, 2027 included as an event definition only."""
    assert [to_row(row) for row in dataset.cycles] == [
        {
            "label": label,
            "event_date": event_date,
            "event_type": "general_election",
            "source": "seed",
        }
        for label, event_date in ALL_CYCLES
    ]


def test_watchlist_holds_the_eight_counters(dataset: SeedDataset) -> None:
    """The "NSE Core 8" watchlist; the seeder assigns its owner from the default principal.

    The record carries no owner (see :func:`test_records_carry_the_terminal_table_column_names`):
    the seeder sets ``owner_key`` to ``TerminalSettings.default_owner_key``, the opaque subject
    ``"local"`` by default, so no user record is involved.
    """
    assert dataset.watchlist.name == "NSE Core 8"
    assert dataset.watchlist.tickers == TICKERS
    assert generate_watchlist() == dataset.watchlist
    assert TerminalSettings.model_fields["default_owner_key"].default == "local"


# Generator guards ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("generate", "attribute", "value", "match"),
    [
        pytest.param(
            synthetic_sessions.__wrapped__,
            "SESSION_EXCLUDED_MONTH_DAYS",
            (),
            "synthetic calendar has 3848 sessions, expected 3762",
            id="calendar-session-count",
        ),
        pytest.param(
            generate_prices,
            "PLANTED_SPIKES",
            (("XXXX", date(2026, 9, 15)),),
            "planted spike XXXX 2026-09-15 is not seeded",
            id="spike-on-unseeded-ticker",
        ),
        pytest.param(
            generate_prices,
            "PLANTED_SPIKES",
            (("SCOM", date(2026, 9, 19)),),
            "planted spike SCOM 2026-09-19 is not a synthetic session",
            id="spike-on-saturday",
        ),
        pytest.param(
            generate_prices,
            "PLANTED_SPIKES",
            (("SCOM", date(2012, 1, 3)),),
            "has no usable Z-score window: flags \\['insufficient_observations'\\]",
            id="spike-without-window",
        ),
        pytest.param(
            generate_prices,
            "SPIKE_MIN_Z",
            Decimal("1000"),
            "planted spike SCOM 2026-09-15 has volume Z-score .*, below the required 1000",
            id="spike-below-required-z",
        ),
        pytest.param(
            generate_fundamentals,
            "FUNDAMENTAL_TARGETS",
            (replace(ref.FUNDAMENTAL_TARGETS[1], ticker="XXXX"),),
            "fundamental target XXXX is not a seeded equity",
            id="fundamentals-unseeded-ticker",
        ),
        pytest.param(
            generate_fundamentals,
            "FUNDAMENTAL_TARGETS",
            (replace(ref.FUNDAMENTAL_TARGETS[0], ebitda_margin_pct=None),),
            "non-bank SCOM needs an EBITDA margin",
            id="fundamentals-non-bank-without-ebitda-target",
        ),
        pytest.param(
            generate_fundamentals,
            "FUNDAMENTAL_TARGETS",
            ref.FUNDAMENTAL_TARGETS[:1],
            "generated 17 fundamentals rows, expected 131",
            id="fundamentals-row-count",
        ),
        pytest.param(
            generate_fundamentals,
            "EQUITIES",
            tuple(replace(equity, shares_outstanding=0) for equity in ref.EQUITIES),
            "SCOM has 0 shares; fundamentals need shares > 0",
            id="fundamentals-zero-shares",
        ),
        pytest.param(
            generate_macro,
            "MACRO_FIXED_POINTS",
            (("inflation_yoy_pct", date(2026, 8, 30), Decimal("6.60")),),
            "exact macro point inflation_yoy_pct 2026-08-30 is not a seeded month-end",
            id="macro-point-not-a-month-end",
        ),
        pytest.param(
            generate_macro,
            "MACRO_FIXED_POINTS",
            (("inflation_yoy_pct", date(2025, 6, 30), Decimal("6.85")),),
            "macro inflation on 2025-06-30 is 6.85, not below the 2026-09-30 value 6.80",
            id="macro-inflation-above-anchor-since-january-2024",
        ),
        pytest.param(
            generate_macro,
            "MACRO_FIXED_POINTS",
            (("cbr_rate_pct", date(2020, 6, 30), Decimal("13.01")),),
            "macro cbr_rate_pct on 2020-06-30 is 13.01, outside 7.00..13.00",
            id="macro-out-of-bounds",
        ),
        pytest.param(
            generate_macro,
            "MACRO_FIXED_POINTS",
            (("tbill_364d_pct", date(2020, 6, 30), Decimal("6.00")),),
            "macro T-bill yields on 2020-06-30 are not ordered 91d <= 182d <= 364d",
            id="macro-bills-out-of-order",
        ),
        pytest.param(
            generate_macro,
            "MACRO_START",
            date(2012, 2, 29),
            "macro rows run 2012-01-31 to 2026-09-30, expected 2012-02-29 to 2026-09-30",
            id="macro-span",
        ),
        pytest.param(
            generate_watchlist,
            "SEED_WATCHLIST_TICKERS",
            ("SCOM", "XXXX"),
            "seed watchlist names unseeded tickers: XXXX",
            id="watchlist-unseeded-ticker",
        ),
    ],
)
def test_generators_refuse_to_emit_a_seed_that_breaks_its_constraints(
    monkeypatch: pytest.MonkeyPatch,
    generate: Callable[[], object],
    attribute: str,
    value: object,
    match: str,
) -> None:
    """Each check runs before the seeder writes: a broken reference value raises, never seeds."""
    monkeypatch.setattr(ref, attribute, value)

    with pytest.raises(RuntimeError, match=match):
        generate()
