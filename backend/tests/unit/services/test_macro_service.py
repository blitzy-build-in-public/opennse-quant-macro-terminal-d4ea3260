"""Macro overview service: the indicator list guard, the yield curve at a date, and its bounds.

``yield_curve_at(macro_rows, sources_by_date, bond_rows, day)`` resolves the curve as it stood on
``day``:

- **T-bills** 91D (0.25 years), 182D (0.50) and 364D (1.00): each tenor independently takes the
  latest non-null value of its column on or before ``day`` and at most 62 days old, with its own
  ``observation_date`` and its column's ``source_by_field`` entry (``unknown`` without one);
- **bonds**: FXD observations dated from ``day`` minus 365 days to ``day``, bucketed to the
  nearest of 2, 5, 10, 15, 20 and 25 years by remaining tenor (a midpoint goes to the lower
  bucket). A bucket takes its most recent real observation (``source != "seed"``, ties by the
  lower ``bond_code``), and a seeded one only when it has no real observation in the window.

Points come back in ascending tenor, T-bills first on an equal tenor, with yields and tenors
quantized to 2 dp (ROUND_HALF_UP). The loading paths of ``get_macro_overview`` need a database
and belong to the integration suite; only its argument check, which runs before any query, is
exercised here. Every expected value is a literal, and no database, network or filesystem is
touched.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from opennse_terminal.models import macro as macro_models
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS
from opennse_terminal.quant.asof import MacroRow
from opennse_terminal.services import macro_service
from opennse_terminal.services.macro_service import (
    INDICATORS,
    BondObservation,
    get_macro_overview,
    yield_curve_at,
)

CURVE_DAY = date(2026, 9, 30)


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


# ---------------------------------------------------------------------------------------------
# KPI indicators and the import-time guard
# ---------------------------------------------------------------------------------------------


def test_indicators_follow_the_model_columns_in_order() -> None:
    """One KPI tile per ``terminal_macro_indicators`` column, in table order, with its unit.

    Rates are percentage points (``pct``); the exchange rate is KES per USD and the reserves are
    USD millions, both rounded with the 2 dp ``money`` kind.
    """
    assert tuple(key for key, _label, _unit, _kind in INDICATORS) == MACRO_INDICATOR_COLUMNS
    assert {key: (unit, kind) for key, _label, unit, kind in INDICATORS} == {
        "inflation_yoy_pct": ("pct", "pct"),
        "cbr_rate_pct": ("pct", "pct"),
        "interbank_rate_pct": ("pct", "pct"),
        "tbill_91d_pct": ("pct", "pct"),
        "tbill_182d_pct": ("pct", "pct"),
        "tbill_364d_pct": ("pct", "pct"),
        "fx_kes_usd": ("kes_per_usd", "money"),
        "fx_reserves_usd_m": ("usd_m", "money"),
    }


@pytest.mark.parametrize(
    "drifted_columns",
    [
        pytest.param(
            (
                "cbr_rate_pct",
                "inflation_yoy_pct",
                "interbank_rate_pct",
                "tbill_91d_pct",
                "tbill_182d_pct",
                "tbill_364d_pct",
                "fx_kes_usd",
                "fx_reserves_usd_m",
            ),
            id="reordered",
        ),
        pytest.param(
            (
                "inflation_yoy_pct",
                "cbr_rate_pct",
                "interbank_rate_pct",
                "tbill_91d_pct",
                "tbill_182d_pct",
                "tbill_364d_pct",
                "fx_kes_usd",
            ),
            id="column_dropped",
        ),
        pytest.param(
            (
                "inflation_yoy_pct",
                "cbr_rate_pct",
                "interbank_rate_pct",
                "tbill_91d_pct",
                "tbill_182d_pct",
                "tbill_364d_pct",
                "fx_kes_usd",
                "fx_reserves_usd_m",
                "core_inflation_pct",
            ),
            id="column_added",
        ),
    ],
)
def test_indicator_list_drifting_from_the_model_fails_at_import(
    drifted_columns: tuple[str, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A KPI list that no longer matches the model would drop or misname a tile silently, so the
    module refuses to load instead.

    The module source is executed again under a throwaway name, which is never registered in
    ``sys.modules``, while the model's column tuple is replaced.
    """
    monkeypatch.setattr(macro_models, "MACRO_INDICATOR_COLUMNS", drifted_columns)
    probe_name = "opennse_terminal.services._macro_service_drift_probe"
    spec = importlib.util.spec_from_file_location(probe_name, macro_service.__file__)
    assert spec is not None
    assert spec.loader is not None
    probe = importlib.util.module_from_spec(spec)

    with pytest.raises(
        RuntimeError, match=r"^INDICATORS keys must equal MACRO_INDICATOR_COLUMNS, in order$"
    ):
        spec.loader.exec_module(probe)
    assert probe_name not in sys.modules


# ---------------------------------------------------------------------------------------------
# Yield curve: T-bills
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("macro_rows", "bond_rows"),
    [
        pytest.param([], [], id="nothing_stored"),
        pytest.param(
            [
                MacroRow(
                    CURVE_DAY,
                    inflation_yoy_pct=Decimal("6.80"),
                    fx_reserves_usd_m=Decimal("7850.00"),
                )
            ],
            [],
            id="macro_row_without_bill_yields",
        ),
        pytest.param(
            [MacroRow(date(2026, 10, 1), tbill_91d_pct=Decimal("15.72"))],
            [],
            id="bill_yield_after_the_day",
        ),
    ],
)
def test_no_usable_observation_gives_an_empty_curve(
    macro_rows: list[MacroRow], bond_rows: list[BondObservation]
) -> None:
    """Without a bill yield on or before the day or an FXD bond in its 365-day window, the curve
    is empty rather than an error. The bond window's edges are covered separately below."""
    assert yield_curve_at(macro_rows, {}, bond_rows, CURVE_DAY) == []


def test_tbill_tenors_resolve_independently_with_their_own_source() -> None:
    """Each bill tenor takes its own latest non-null value, so neither a newer reserves-only row
    nor a September row without a 182-day yield hides a bill yield.

    On 2026-10-05: 91D is September's 15.7234 -> 15.72 (``cbk_rates_csv``); 182D is August's
    16.4049 -> 16.40, 35 days old (``seed``); 364D is September's 16.8250 -> 16.83 by
    ROUND_HALF_UP (half-even would give 16.82), whose row has no source entry for the column, so
    it reads ``unknown``.
    """
    macro_rows = [
        MacroRow(
            date(2026, 8, 31),
            tbill_91d_pct=Decimal("15.5000"),
            tbill_182d_pct=Decimal("16.4049"),
            tbill_364d_pct=Decimal("16.7000"),
        ),
        MacroRow(
            date(2026, 9, 30),
            tbill_91d_pct=Decimal("15.7234"),
            tbill_364d_pct=Decimal("16.8250"),
        ),
        MacroRow(date(2026, 10, 2), fx_reserves_usd_m=Decimal("7850.00")),
    ]
    sources_by_date = {
        date(2026, 8, 31): {"tbill_182d_pct": "seed"},
        date(2026, 9, 30): {"tbill_91d_pct": "cbk_rates_csv", "fx_kes_usd": "cbk_fx"},
    }

    points = yield_curve_at(macro_rows, sources_by_date, [], date(2026, 10, 5))

    assert [point.model_dump(mode="json") for point in points] == [
        {
            "tenor_label": "91D",
            "tenor_years": "0.25",
            "yield_pct": "15.72",
            "instrument": "tbill",
            "bond_code": None,
            "observation_date": "2026-09-30",
            "source": "cbk_rates_csv",
        },
        {
            "tenor_label": "182D",
            "tenor_years": "0.50",
            "yield_pct": "16.40",
            "instrument": "tbill",
            "bond_code": None,
            "observation_date": "2026-08-31",
            "source": "seed",
        },
        {
            "tenor_label": "364D",
            "tenor_years": "1.00",
            "yield_pct": "16.83",
            "instrument": "tbill",
            "bond_code": None,
            "observation_date": "2026-09-30",
            "source": "unknown",
        },
    ]


@pytest.mark.parametrize(
    ("day", "labels"),
    [
        # 2026-07-31 plus 62 days is 2026-10-01 (31 days to 08-31, 61 to 09-30).
        pytest.param(date(2026, 10, 1), ["91D"], id="62_days_old_kept"),
        pytest.param(date(2026, 10, 2), [], id="63_days_old_dropped"),
    ],
)
def test_tbill_older_than_62_days_is_omitted(day: date, labels: list[str]) -> None:
    """A bill yield observed 2026-07-31 still marks the curve 62 days later, but not 63."""
    macro_rows = [MacroRow(date(2026, 7, 31), tbill_91d_pct=Decimal("15.00"))]

    points = yield_curve_at(macro_rows, {}, [], day)

    assert [point.tenor_label for point in points] == labels


# ---------------------------------------------------------------------------------------------
# Yield curve: bonds
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("remaining_tenor", "label", "tenor_years"),
    [
        pytest.param("0.40", "2Y", "0.40", id="seasoned_below_2"),
        pytest.param("2.00", "2Y", "2.00", id="exactly_2"),
        pytest.param("3.50", "2Y", "3.50", id="midpoint_2_5_goes_lower"),
        pytest.param("3.51", "5Y", "3.51", id="just_above_midpoint_2_5"),
        pytest.param("7.50", "5Y", "7.50", id="midpoint_5_10_goes_lower"),
        pytest.param("12.50", "10Y", "12.50", id="midpoint_10_15_goes_lower"),
        pytest.param("14.6", "15Y", "14.60", id="near_15_quantized"),
        pytest.param("17.50", "15Y", "17.50", id="midpoint_15_20_goes_lower"),
        pytest.param("22.50", "20Y", "22.50", id="midpoint_20_25_goes_lower"),
        pytest.param("22.51", "25Y", "22.51", id="just_above_midpoint_20_25"),
        pytest.param("30.00", "25Y", "30.00", id="beyond_25"),
    ],
)
def test_bond_goes_to_the_nearest_tenor_bucket(
    remaining_tenor: str, label: str, tenor_years: str
) -> None:
    """Remaining tenor picks the nearest of 2, 5, 10, 15, 20 and 25 years; the point reports the
    bond's own remaining tenor at 2 dp, not the bucket's."""
    points = yield_curve_at(
        [], {}, [bond(date(2026, 6, 15), "FXD1/2026/010", remaining_tenor)], CURVE_DAY
    )

    assert [(point.tenor_label, str(point.tenor_years)) for point in points] == [
        (label, tenor_years)
    ]


@pytest.mark.parametrize(
    ("days_before", "kept"),
    [
        pytest.param(0, True, id="on_the_day"),
        pytest.param(365, True, id="365_days_before"),
        pytest.param(366, False, id="366_days_before"),
        pytest.param(-1, False, id="day_after"),
    ],
)
def test_bond_window_is_the_365_days_up_to_the_day(days_before: int, kept: bool) -> None:
    """Both ends are inclusive: 2025-09-30 and 2026-09-30 count for a curve at 2026-09-30."""
    observation = bond(CURVE_DAY - timedelta(days=days_before), "FXD1/2026/010", "10.00")

    points = yield_curve_at([], {}, [observation], CURVE_DAY)

    assert [point.bond_code for point in points] == (["FXD1/2026/010"] if kept else [])


@pytest.mark.parametrize(
    ("bond_rows", "chosen"),
    [
        pytest.param(
            [
                bond(date(2026, 3, 16), "FXD1/2026/010", "10.00", "13.9000"),
                bond(date(2026, 9, 30), "FXD-SEED-10Y-202609", "10.00", "17.5400", "seed"),
            ],
            ("FXD1/2026/010", "2026-03-16", "cbk_bonds", "13.90"),
            id="real_beats_later_seed",
        ),
        pytest.param(
            [
                bond(date(2026, 2, 28), "FXD-SEED-10Y-202602", "10.00", "17.4000", "seed"),
                bond(date(2026, 3, 16), "FXD1/2026/010", "9.50", "13.9000"),
            ],
            ("FXD1/2026/010", "2026-03-16", "cbk_bonds", "13.90"),
            id="real_replaces_earlier_seed",
        ),
        pytest.param(
            [
                bond(date(2026, 1, 19), "FXD1/2025/010", "9.60", "14.1000"),
                bond(date(2026, 6, 15), "FXD2/2026/010", "10.40", "13.6500"),
                bond(date(2026, 9, 30), "FXD-SEED-10Y-202609", "10.00", "17.5400", "seed"),
            ],
            ("FXD2/2026/010", "2026-06-15", "cbk_bonds", "13.65"),
            id="most_recent_real_wins",
        ),
        pytest.param(
            [
                bond(date(2026, 6, 15), "FXD3/2026/010", "10.10", "13.7000"),
                bond(date(2026, 6, 15), "FXD1/2019/010", "9.80", "13.2000", "csv_import"),
            ],
            ("FXD1/2019/010", "2026-06-15", "csv_import", "13.20"),
            id="lower_code_wins_same_day_listed_second",
        ),
        pytest.param(
            [
                bond(date(2026, 6, 15), "FXD1/2019/010", "9.80", "13.2000", "csv_import"),
                bond(date(2026, 6, 15), "FXD3/2026/010", "10.10", "13.7000"),
            ],
            ("FXD1/2019/010", "2026-06-15", "csv_import", "13.20"),
            id="lower_code_wins_same_day_listed_first",
        ),
        pytest.param(
            [
                bond(date(2025, 9, 29), "FXD1/2015/010", "10.00", "12.9000"),
                bond(date(2026, 8, 31), "FXD-SEED-10Y-202608", "10.00", "17.4800", "seed"),
                bond(date(2026, 9, 30), "FXD-SEED-10Y-202609", "10.00", "17.5400", "seed"),
            ],
            ("FXD-SEED-10Y-202609", "2026-09-30", "seed", "17.54"),
            id="latest_seed_only_without_real_in_window",
        ),
    ],
)
def test_bucket_prefers_real_then_recent_then_lower_code(
    bond_rows: list[BondObservation], chosen: tuple[str, str, str, str]
) -> None:
    """A real observation always beats a seeded one, so a later synthetic date never displaces
    real data; then the later date wins, then the lower ``bond_code`` (``FXD1/2019/010`` before
    ``FXD3/2026/010``, whichever arrives first). A real bond 366 days old is outside the window,
    so the bucket falls back to its latest seed.

    Remaining tenors 9.50 to 10.40 all fall in the 10Y bucket, so each case yields one point.
    """
    points = yield_curve_at([], {}, bond_rows, CURVE_DAY)

    assert [
        (
            point.tenor_label,
            point.bond_code,
            point.observation_date.isoformat(),
            point.source,
            str(point.yield_pct),
        )
        for point in points
    ] == [("10Y", *chosen)]


def test_curve_orders_tbills_and_bonds_by_tenor() -> None:
    """The attachment's September 2026 bills (15.72 / 16.58 / 16.84) with three FXD buckets.

    A seasoned real bond with 1.00 year left ties the 364-day bill at 1.00: the bill comes first.
    Yields round half-up to 2 dp: 16.9876 -> 16.99 and 15.9050 -> 15.91.
    """
    macro_rows = [
        MacroRow(
            CURVE_DAY,
            tbill_91d_pct=Decimal("15.7200"),
            tbill_182d_pct=Decimal("16.5800"),
            tbill_364d_pct=Decimal("16.8400"),
        )
    ]
    sources_by_date = {
        CURVE_DAY: {"tbill_91d_pct": "seed", "tbill_182d_pct": "seed", "tbill_364d_pct": "seed"}
    }
    bond_rows = [
        bond(date(2026, 4, 30), "FXD-SEED-25Y-202604", "25.00", "17.9900", "seed"),
        bond(date(2026, 6, 15), "FXD1/2026/005", "4.80", "16.9876"),
        bond(date(2026, 9, 14), "FXD1/2017/010", "1.00", "15.9050"),
    ]

    points = yield_curve_at(macro_rows, sources_by_date, bond_rows, CURVE_DAY)

    assert [
        (
            point.tenor_label,
            str(point.tenor_years),
            str(point.yield_pct),
            point.instrument,
            point.bond_code,
            point.observation_date.isoformat(),
            point.source,
        )
        for point in points
    ] == [
        ("91D", "0.25", "15.72", "tbill", None, "2026-09-30", "seed"),
        ("182D", "0.50", "16.58", "tbill", None, "2026-09-30", "seed"),
        ("364D", "1.00", "16.84", "tbill", None, "2026-09-30", "seed"),
        ("2Y", "1.00", "15.91", "bond", "FXD1/2017/010", "2026-09-14", "cbk_bonds"),
        ("5Y", "4.80", "16.99", "bond", "FXD1/2026/005", "2026-06-15", "cbk_bonds"),
        ("25Y", "25.00", "17.99", "bond", "FXD-SEED-25Y-202604", "2026-04-30", "seed"),
    ]


@pytest.mark.parametrize(
    ("source", "is_real"),
    [
        pytest.param("cbk_bonds", True, id="cbk_pipeline"),
        pytest.param("csv_import", True, id="csv_import"),
        pytest.param("seed", False, id="seeder"),
    ],
)
def test_bond_observation_is_real_unless_seeded(source: str, is_real: bool) -> None:
    """Every source other than ``seed`` counts as real data."""
    assert bond(CURVE_DAY, "FXD1/2026/010", "10.00", source=source).is_real is is_real


# ---------------------------------------------------------------------------------------------
# Endpoint argument check
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("history_months", [0, -1])
async def test_history_months_below_one_is_rejected_before_any_query(history_months: int) -> None:
    """The window needs at least one month-end. The session is bound to no engine, so any query
    would raise instead; the ``ValueError`` shows the check runs first."""
    async with AsyncSession() as session:
        with pytest.raises(
            ValueError, match=rf"^history_months must be >= 1, got {history_months}$"
        ):
            await get_macro_overview(session, history_months)
