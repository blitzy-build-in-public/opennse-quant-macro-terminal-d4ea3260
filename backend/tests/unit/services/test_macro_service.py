"""Macro overview service: the indicator list guard, the yield curve at a date, its read bounds,
and ``get_macro_overview`` on seeded PostgreSQL.

``yield_curve_at(macro_rows, sources_by_date, bond_rows, day)`` resolves the curve as it stood on
``day``:

- **T-bills** 91D (0.25 years), 182D (0.50) and 364D (1.00): each tenor independently takes the
  latest non-null value of its column on or before ``day`` and at most 62 days old, with its own
  ``observation_date`` and its column's ``source_by_field`` entry (``unknown`` without one);
- **bonds**: FXD observations dated from ``day`` minus 365 days (``date.min`` at the earliest) to
  ``day``, bucketed to the nearest of 2, 5, 10, 15, 20 and 25 years by remaining tenor (a midpoint
  goes to the lower bucket). A bucket takes its most recent real observation
  (``source != "seed"``, ties by the lower ``bond_code``), and a seeded one only when it has no
  real observation in the window.

Points come back in ascending tenor, T-bills first on an equal tenor, with yields and tenors
quantized to 2 dp (ROUND_HALF_UP). The KPI snapshots are built from each indicator's latest and
previous non-null values, which the service reads in SQL; the pure step that turns them into
snapshots is exercised here on literal observations.

``get_macro_overview(session, history_months)`` runs on a fresh seeded clone per test (the root
conftest's ``seeded_database``, which marks the test ``integration``); a test that adds or
deletes rows changes only its own clone. The contract held there:

- **Window and coverage**: exactly ``history_months`` month-ends ending at the month of the latest
  macro observation. ``requested_start`` is the first of them (2024-10-31 for 24 months, 2006-10-31
  for 240), ``coverage_start`` the first window date with any indicator, and coverage is partial
  when there is none or it falls more than one month later (2012-01-31 at 240 months). Stored dates
  from 0001-01-01 to 9999-12-31 never raise: a window reaching back before January of year 1 is
  cut there, and the months it lost still count towards partial coverage.
- **latest**: all eight indicators, each resolved to its own latest and previous non-null values
  (inflation 6.80 on 2026-09-30 after 6.60, CBR 8.75), so a newer reserves-only row moves the
  window and the reserves tile but hides no other tile.
- **Series**: one point per window row that carries the series, oldest first; a row without any
  rate is left out of ``rates_series``.
- **Curves**: ``current`` at the later of the latest macro and latest FXD bond dates (3 seeded
  T-bills and 6 ``seed`` bonds on 2026-09-30), and one snapshot per window month-end. A real FXD
  bond outranks the seeded ones in its bucket, and codes other than FXD are never read.
- **Missing data**: with only bonds stored the window ends at the latest bond month and ``latest``
  and both series are empty; with nothing stored every list is empty and the window ends at the
  current UTC month.
- **Argument check**: ``history_months < 1`` raises ``ValueError`` before any query.

Seeded values are those of ``seed/reference_data.py``: the 2026-09-30 row of the attachment
verbatim with CBR 8.75 filled from CBK, August 2026 inflation 6.60, the other August values from
the deterministic synthetic history, and bonds of the 364-day yield plus each tenor's spread with
no noise on 2026-09-30. Every expected value is an exact literal, and no network or filesystem is
touched.
"""

from __future__ import annotations

import importlib.util
import sys
from calendar import monthrange
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Protocol

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal.models import TerminalBondYield, TerminalMacroIndicator
from opennse_terminal.models import macro as macro_models
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS
from opennse_terminal.quant.asof import AsOfValue, MacroRow
from opennse_terminal.schemas.macro import IndicatorSnapshot, YieldCurvePoint
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
# KPI snapshots
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
# Read bounds
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
# Window and coverage at the calendar's ends
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


# ---------------------------------------------------------------------------------------------
# Endpoint on seeded PostgreSQL
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


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...


@pytest_asyncio.fixture
async def session(seeded_database: SeededDatabase) -> AsyncIterator[AsyncSession]:
    """A session on this test's own seeded clone."""
    factory = async_sessionmaker(seeded_database.engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session


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
