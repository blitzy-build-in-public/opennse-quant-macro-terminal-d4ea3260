"""IPO sandbox service: request conversion, coded 422s, response quantization and presets.

``services/ipo_service.py`` wraps the single ``quant.dcf`` implementation. It converts an
``IpoValuationRequest`` one-to-one into ``DcfAssumptions``, raises the first cross-field violation
as ``HTTPException(422, detail={"code", "message", "field"})`` and rounds every output exactly
once with ``quantize_out`` (ROUND_HALF_UP): KES, KES per share, % and multiples to 2 dp, discount
factors to 6 dp. ``_scenario_from_row`` turns a stored ``terminal_ipo_scenarios`` row into the
preset the sandbox posts back, and skips a row that cannot form one with a WARNING that names
fields only.

The formulas themselves are covered by ``tests/unit/quant/test_dcf_and_comparables.py``; here the
Quickmart Supermarket preset's hand-computed reference results are asserted on the service's
response, as the JSON strings the endpoint returns, so the exponent of every ``Decimal`` is checked
too. Every expected value is a literal. Rows are in-memory ORM instances: no database, network or
filesystem is touched.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
from fastapi import HTTPException

from opennse_terminal.models import TerminalIpoScenario
from opennse_terminal.quant.dcf import DcfAssumptions
from opennse_terminal.schemas.ipo import IpoValuationRequest
from opennse_terminal.services.ipo_service import (
    _scenario_from_row,
    compute_ipo_valuation,
    to_dcf_assumptions,
    value_ipo,
)

SERVICE_LOGGER = "opennse_terminal.services.ipo_service"
WACC_NOT_ABOVE_TERMINAL_GROWTH = "wacc_not_above_terminal_growth"
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

# The preset as the sandbox posts it and as GET /ipo/scenarios returns it: 2 dp decimal strings.
QUICKMART_ASSUMPTIONS_JSON: dict[str, Any] = {
    "offer_price_kes": "7.50",
    "revenue_growth_pct": "10.00",
    "ebitda_margin_pct": "4.50",
    "margin_expansion_pct": "0.50",
    "wacc_pct": "18.00",
    "terminal_growth_pct": "5.00",
    "net_debt_kes": "4000000000.00",
    "nwc_pct_revenue": "-3.00",
    "peer_ev_ebitda_multiple": "6.00",
    "dcf_weight_pct": "50.00",
    "capex_pct_revenue": "2.00",
    "da_pct_revenue": "1.50",
    "tax_rate_pct": "30.00",
    "forecast_years": 5,
    "base_revenue_kes": "45000000000.00",
    "shares_outstanding": 1_000_000_000,
    "shares_offered": 500_000_000,
    "primary_proceeds_kes": "0.00",
    "sensitivity_wacc_pct": None,
    "sensitivity_margin_pct": None,
}


def quickmart_request(**overrides: object) -> IpoValuationRequest:
    """The Quickmart Supermarket preset body, validated, with some fields replaced."""
    payload: dict[str, object] = {
        "offer_price_kes": "7.50",
        "revenue_growth_pct": "10.00",
        "ebitda_margin_pct": "4.50",
        "margin_expansion_pct": "0.50",
        "wacc_pct": "18.00",
        "terminal_growth_pct": "5.00",
        "net_debt_kes": "4000000000",
        "nwc_pct_revenue": "-3.00",
        "peer_ev_ebitda_multiple": "6.00",
        "dcf_weight_pct": "50",
        "capex_pct_revenue": "2.00",
        "da_pct_revenue": "1.50",
        "tax_rate_pct": "30.00",
        "forecast_years": 5,
        "base_revenue_kes": "45000000000",
        "shares_outstanding": 1_000_000_000,
        "shares_offered": 500_000_000,
        "primary_proceeds_kes": "0.00",
    }
    payload.update(overrides)
    return IpoValuationRequest.model_validate(payload)


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


def decimal_places(node: object, key: str = "") -> Iterator[tuple[str, int]]:
    """Yield ``(field name, decimal places)`` for every ``Decimal`` in a ``model_dump()`` tree.

    A ``float`` anywhere in the tree fails the test: the response carries ``Decimal`` only.
    """
    if isinstance(node, dict):
        for child_key, child in node.items():
            yield from decimal_places(child, child_key)
    elif isinstance(node, list):
        for child in node:
            yield from decimal_places(child, key)
    elif isinstance(node, Decimal):
        exponent = node.as_tuple().exponent
        assert isinstance(exponent, int), f"{key} is not finite: {node}"
        yield key, -exponent
    else:
        assert not isinstance(node, float), f"{key} is a float: {node!r}"


# ---------------------------------------------------------------------------------------------
# Valuation: the Quickmart reference results on the service response
# ---------------------------------------------------------------------------------------------


async def test_quickmart_valuation_response() -> None:
    """Quickmart defaults: EV 9,484,146,704.50; DCF 5.48; comps 8.15; blended 6.82; upside -9.11%.

    EV = PV explicit 3,796,952,900.12 + PV TV 5,687,193,804.38 (TV 13,010,921,740.38), equity
    EV - 4,000,000,000. Comparables 6.00 x EBITDA_0 2,025,000,000 = 12,150,000,000, equity
    8,150,000,000. Market cap at offer 7.50 x 10^9 = 7,500,000,000; implied EV/EBITDA
    11,500,000,000 / 2,025,000,000 = 5.68; offer size 7.50 x 500,000,000 = 3,750,000,000, all
    secondary. ``value_ipo`` is the endpoint function and returns what the pure
    ``compute_ipo_valuation`` returns.
    """
    request = quickmart_request()

    response = await value_ipo(request)

    assert response == compute_ipo_valuation(request)
    body = response.model_dump(mode="json")
    assert set(body) == {
        "dcf",
        "comparables",
        "blended_fair_value_per_share_kes",
        "offer_price_kes",
        "upside_pct",
        "implied_market_cap_at_offer_kes",
        "implied_ev_ebitda_at_offer",
        "offer_size_kes",
        "primary_proceeds_kes",
        "secondary_proceeds_kes",
        "sensitivity",
        "flags",
    }
    assert {key: body[key] for key in set(body) - {"dcf", "comparables", "sensitivity"}} == {
        "blended_fair_value_per_share_kes": "6.82",
        "offer_price_kes": "7.50",
        "upside_pct": "-9.11",
        "implied_market_cap_at_offer_kes": "7500000000.00",
        "implied_ev_ebitda_at_offer": "5.68",
        "offer_size_kes": "3750000000.00",
        "primary_proceeds_kes": "0.00",
        "secondary_proceeds_kes": "3750000000.00",
        "flags": [],
    }
    dcf = body["dcf"]
    assert [projection["year"] for projection in dcf.pop("projections")] == [1, 2, 3, 4, 5]
    assert dcf == {
        "pv_explicit_fcf_kes": "3796952900.12",
        "terminal_value_kes": "13010921740.38",
        "pv_terminal_value_kes": "5687193804.38",
        "enterprise_value_kes": "9484146704.50",
        "equity_value_kes": "5484146704.50",
        "fair_value_per_share_kes": "5.48",
        "flags": [],
    }
    assert body["comparables"] == {
        "ev_ebitda_multiple": "6.00",
        "base_ebitda_kes": "2025000000.00",
        "enterprise_value_kes": "12150000000.00",
        "equity_value_kes": "8150000000.00",
        "fair_value_per_share_kes": "8.15",
        "flags": [],
    }


async def test_every_response_decimal_is_quantized_once() -> None:
    """Every value is a ``Decimal`` at 2 dp, discount factors at 6 dp, and none is null.

    The preset response holds 231 decimals: 5 projection years x 12 plus 6 DCF totals, 5
    comparables values, 8 offer metrics, and the 5 x 7 matrix (5 rows, 7 columns and 35 cells x 4).
    """
    places = list(decimal_places((await value_ipo(quickmart_request())).model_dump()))

    assert len(places) == 5 * 12 + 6 + 5 + 8 + 5 + 7 + 35 * 4
    assert {count for key, count in places if key == "discount_factor"} == {6}
    assert {count for key, count in places if key != "discount_factor"} == {2}


@pytest.mark.parametrize(
    ("year", "expected"),
    [
        pytest.param(
            1,
            {
                "year": 1,
                "revenue_kes": "49500000000.00",
                "ebitda_margin_pct": "4.60",
                "ebitda_kes": "2277000000.00",
                "da_kes": "742500000.00",
                "ebit_kes": "1534500000.00",
                "tax_kes": "460350000.00",
                "nopat_kes": "1074150000.00",
                "capex_kes": "990000000.00",
                "delta_nwc_kes": "-135000000.00",
                "fcf_kes": "961650000.00",
                "discount_factor": "0.847458",
                "pv_fcf_kes": "814957627.12",
            },
            id="year1",
        ),
        pytest.param(
            5,
            {
                "year": 5,
                "revenue_kes": "72472950000.00",
                "ebitda_margin_pct": "5.00",
                "ebitda_kes": "3623647500.00",
                "da_kes": "1087094250.00",
                "ebit_kes": "2536553250.00",
                "tax_kes": "760965975.00",
                "nopat_kes": "1775587275.00",
                "capex_kes": "1449459000.00",
                "delta_nwc_kes": "-197653500.00",
                "fcf_kes": "1610876025.00",
                "discount_factor": "0.437109",
                "pv_fcf_kes": "704128756.73",
            },
            id="year5_terminal_year",
        ),
    ],
)
def test_projection_year_is_quantized(year: int, expected: dict[str, object]) -> None:
    """Forecast years as returned: KES to 2 dp, the margin path in % to 2 dp, DF_t to 6 dp.

    Revenue grows 10% a year from 45,000,000,000 and the margin climbs 0.1 point a year from
    4.5%. Year 1: FCF = 1,074,150,000 + 742,500,000 - 990,000,000 + 135,000,000 = 961,650,000,
    DF 1 / 1.18 = 0.847458 and PV 814,957,627.12. Year 5: DF 1 / 1.18^5 = 0.437109.
    """
    response = compute_ipo_valuation(quickmart_request())

    assert response.dcf.projections[year - 1].model_dump(mode="json") == expected


def test_default_sensitivity_matrix() -> None:
    """Null axes give WACC 16-20% by base margin 3.0-6.0%: rows and columns as 2 dp strings.

    Corners 2.97 (16%, 3.0%), 12.50 (16%, 6.0%), 2.14 (20%, 3.0%) and 10.16 (20%, 6.0%), with
    upside against KES 7.50 of -60.43%, +66.62%, -71.51% and +35.48%; the centre cell (18%,
    4.5%) is the headline 6.82 / -9.11%.
    """
    sensitivity = compute_ipo_valuation(quickmart_request()).sensitivity.model_dump(mode="json")

    assert sensitivity["rows"] == ["16.00", "17.00", "18.00", "19.00", "20.00"]
    assert sensitivity["cols"] == ["3.00", "3.50", "4.00", "4.50", "5.00", "5.50", "6.00"]
    assert [len(row) for row in sensitivity["cells"]] == [7, 7, 7, 7, 7]
    corners = {
        (row, col): sensitivity["cells"][row][col] for row, col in ((0, 0), (0, 6), (4, 0), (4, 6))
    }
    assert corners == {
        (0, 0): {
            "wacc_pct": "16.00",
            "ebitda_margin_pct": "3.00",
            "fair_value_per_share_kes": "2.97",
            "upside_pct": "-60.43",
            "flags": [],
        },
        (0, 6): {
            "wacc_pct": "16.00",
            "ebitda_margin_pct": "6.00",
            "fair_value_per_share_kes": "12.50",
            "upside_pct": "66.62",
            "flags": [],
        },
        (4, 0): {
            "wacc_pct": "20.00",
            "ebitda_margin_pct": "3.00",
            "fair_value_per_share_kes": "2.14",
            "upside_pct": "-71.51",
            "flags": [],
        },
        (4, 6): {
            "wacc_pct": "20.00",
            "ebitda_margin_pct": "6.00",
            "fair_value_per_share_kes": "10.16",
            "upside_pct": "35.48",
            "flags": [],
        },
    }
    assert sensitivity["cells"][2][3]["fair_value_per_share_kes"] == "6.82"
    assert sensitivity["cells"][2][3]["upside_pct"] == "-9.11"


def test_custom_axes_keep_null_cells_null() -> None:
    """Terminal growth 10% with WACC axis [8, 10] and margin axis [4.5, 6]: every cell is null.

    WACC 8% is below and 10% equal to g_T, so each cell has no Gordon value and carries
    ``wacc_not_above_terminal_growth``; the service passes the nulls through unrounded and
    quantizes the axes to 2 dp. The headline WACC of 18% stays above g_T. Its FCFs do not depend
    on g_T, so FCF_5 is the positive 1,610,876,025 and a higher g_T only raises EV and equity:
    the cell flag is the response's only flag.
    """
    response = compute_ipo_valuation(
        quickmart_request(
            terminal_growth_pct="10.00",
            sensitivity_wacc_pct=["8", "10"],
            sensitivity_margin_pct=["4.5", "6"],
        )
    )
    sensitivity = response.sensitivity.model_dump(mode="json")

    assert sensitivity == {
        "rows": ["8.00", "10.00"],
        "cols": ["4.50", "6.00"],
        "cells": [
            [
                {
                    "wacc_pct": wacc,
                    "ebitda_margin_pct": margin,
                    "fair_value_per_share_kes": None,
                    "upside_pct": None,
                    "flags": [WACC_NOT_ABOVE_TERMINAL_GROWTH],
                }
                for margin in ("4.50", "6.00")
            ]
            for wacc in ("8.00", "10.00")
        ],
    }
    assert response.dcf.flags == []
    assert response.comparables.flags == []
    assert response.flags == [WACC_NOT_ABOVE_TERMINAL_GROWTH]


def test_primary_proceeds_equal_to_offer_size_are_accepted() -> None:
    """Primary proceeds of exactly 7.50 x 500,000,000 = 3,750,000,000.00: the whole offer is new
    cash, so no cross-field rule fires and the secondary proceeds are 0.00.

    EV is unchanged; DCF equity 9,484,146,704.50 - 4,000,000,000 + 3,750,000,000 =
    9,234,146,704.50, FV 9.23. Comparables equity 12,150,000,000 - 4,000,000,000 + 3,750,000,000
    = 11,900,000,000, FV 11.90. Blended (9.2341467 + 11.90) / 2 = 10.5670734 -> 10.57; upside
    (10.5670734 - 7.50) / 7.50 x 100 = 40.89%. Implied EV/EBITDA (7,500,000,000 + 4,000,000,000
    - 3,750,000,000) / 2,025,000,000 = 3.83.
    """
    body = compute_ipo_valuation(
        quickmart_request(primary_proceeds_kes="3750000000.00")
    ).model_dump(mode="json")

    assert body["dcf"]["enterprise_value_kes"] == "9484146704.50"
    assert body["dcf"]["equity_value_kes"] == "9234146704.50"
    assert body["dcf"]["fair_value_per_share_kes"] == "9.23"
    assert body["comparables"]["equity_value_kes"] == "11900000000.00"
    assert body["comparables"]["fair_value_per_share_kes"] == "11.90"
    assert body["blended_fair_value_per_share_kes"] == "10.57"
    assert body["upside_pct"] == "40.89"
    assert body["implied_ev_ebitda_at_offer"] == "3.83"
    assert body["offer_size_kes"] == "3750000000.00"
    assert body["primary_proceeds_kes"] == "3750000000.00"
    assert body["secondary_proceeds_kes"] == "0.00"
    assert body["flags"] == []


def test_to_dcf_assumptions_is_one_to_one_and_exact() -> None:
    """The 18 assumption fields pass through unrounded; the sensitivity axes are left out.

    The offer arrives as ``"7.500"`` and stays ``Decimal("7.500")``: the conversion neither
    rounds nor normalizes, so the engine sees exactly what was validated.
    """
    request = quickmart_request(
        offer_price_kes="7.500",
        sensitivity_wacc_pct=["16", "20"],
        sensitivity_margin_pct=["3.0"],
    )

    assumptions = to_dcf_assumptions(request)

    assert assumptions == DcfAssumptions(
        offer_price_kes=Decimal("7.500"),
        revenue_growth_pct=Decimal("10.00"),
        ebitda_margin_pct=Decimal("4.50"),
        margin_expansion_pct=Decimal("0.50"),
        wacc_pct=Decimal("18.00"),
        terminal_growth_pct=Decimal("5.00"),
        net_debt_kes=Decimal("4000000000"),
        nwc_pct_revenue=Decimal("-3.00"),
        peer_ev_ebitda_multiple=Decimal("6.00"),
        dcf_weight_pct=Decimal("50"),
        capex_pct_revenue=Decimal("2.00"),
        da_pct_revenue=Decimal("1.50"),
        tax_rate_pct=Decimal("30.00"),
        forecast_years=5,
        base_revenue_kes=Decimal("45000000000"),
        shares_outstanding=1_000_000_000,
        shares_offered=500_000_000,
        primary_proceeds_kes=Decimal("0.00"),
    )
    assert str(assumptions.offer_price_kes) == "7.500"
    assert str(assumptions.base_revenue_kes) == "45000000000"


# ---------------------------------------------------------------------------------------------
# Cross-field rules: the first violation is a coded 422
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "code", "field", "message"),
    [
        # WACC must be at least 8.00, so equality with g_T is tested at 8.00 = 8.00.
        pytest.param(
            {"wacc_pct": "8.00", "terminal_growth_pct": "8.00"},
            WACC_NOT_ABOVE_TERMINAL_GROWTH,
            "wacc_pct",
            "WACC (8.00%) must be above the terminal growth rate (8.00%).",
            id="wacc_equal_to_terminal_growth",
        ),
        pytest.param(
            {"wacc_pct": "9.50", "terminal_growth_pct": "10.00"},
            WACC_NOT_ABOVE_TERMINAL_GROWTH,
            "wacc_pct",
            "WACC (9.50%) must be above the terminal growth rate (10.00%).",
            id="wacc_below_terminal_growth",
        ),
        pytest.param(
            {"shares_offered": 1_000_000_001},
            "shares_offered_exceeds_outstanding",
            "shares_offered",
            "Shares offered (1,000,000,001) cannot exceed shares outstanding (1,000,000,000).",
            id="shares_offered_over_outstanding",
        ),
        # Offer 7.50 x 500,000,000 = 3,750,000,000.00; one cent more is negative secondary.
        pytest.param(
            {"primary_proceeds_kes": "3750000000.01"},
            "primary_exceeds_offer_size",
            "primary_proceeds_kes",
            "Primary proceeds (KES 3,750,000,000.01) cannot exceed the offer size "
            "(KES 3,750,000,000.00, offer price times shares offered): the difference would be "
            "negative secondary proceeds.",
            id="primary_one_cent_over_offer_size",
        ),
        # Offer size 7.50 x 1,000,000,001 = 7,500,000,007.50 < 8,000,000,000: two rules broken.
        pytest.param(
            {"shares_offered": 1_000_000_001, "primary_proceeds_kes": "8000000000"},
            "shares_offered_exceeds_outstanding",
            "shares_offered",
            "Shares offered (1,000,000,001) cannot exceed shares outstanding (1,000,000,000).",
            id="shares_reported_before_primary",
        ),
        pytest.param(
            {
                "wacc_pct": "8.00",
                "terminal_growth_pct": "8.00",
                "shares_offered": 1_000_000_001,
                "primary_proceeds_kes": "8000000000",
            },
            WACC_NOT_ABOVE_TERMINAL_GROWTH,
            "wacc_pct",
            "WACC (8.00%) must be above the terminal growth rate (8.00%).",
            id="wacc_reported_first_of_three",
        ),
    ],
)
async def test_cross_field_violation_is_a_coded_422(
    overrides: dict[str, object], code: str, field: str, message: str
) -> None:
    """Every field is inside its own bounds, so the request validates; the first broken rule is
    raised as ``HTTPException(422)`` with an object ``detail``, from both entry points.
    """
    request = quickmart_request(**overrides)

    with pytest.raises(HTTPException) as computed:
        compute_ipo_valuation(request)
    with pytest.raises(HTTPException) as awaited:
        await value_ipo(request)

    for raised in (computed.value, awaited.value):
        assert raised.status_code == 422
        assert raised.detail == {"code": code, "message": message, "field": field}


# ---------------------------------------------------------------------------------------------
# Presets: stored rows to IpoScenario
# ---------------------------------------------------------------------------------------------


def test_scenario_from_row_quantizes_the_stored_preset() -> None:
    """The seeded row becomes the preset ``GET /ipo/scenarios`` returns.

    ``7.5000`` reads ``"7.50"``; the columns are renamed (``assumed_growth_pct`` to
    ``revenue_growth_pct``, ``assumed_wacc_pct`` to ``wacc_pct``, ``shares_outstanding_post_offer``
    to ``shares_outstanding``); the margin is derived as 2,025,000,000 / 45,000,000,000 x 100 =
    4.50; the risk notes keep their stored order and both sensitivity axes are null. Posted back
    unchanged, as JSON, the assumptions value to the headline 6.82 / -9.11%.
    """
    scenario = _scenario_from_row(quickmart_row())

    assert scenario is not None
    assert scenario.model_dump(mode="json") == {
        "id": "5f0c6d1e-8a4b-4c3d-9e2f-1a2b3c4d5e6f",
        "company_name": "Quickmart Supermarket",
        "sector": "Retail & Consumer Goods",
        "currency": "KES",
        "risk_scenarios": QUICKMART_RISKS,
        "assumptions": QUICKMART_ASSUMPTIONS_JSON,
    }
    posted = IpoValuationRequest.model_validate(scenario.assumptions.model_dump(mode="json"))
    response = compute_ipo_valuation(posted)
    assert str(response.blended_fair_value_per_share_kes) == "6.82"
    assert str(response.upside_pct) == "-9.11"


@pytest.mark.parametrize(
    ("base_ebitda", "base_revenue", "margin"),
    [
        pytest.param("2025000000.00", "45000000000.00", "4.50", id="quickmart_exact"),
        # 1,500,000,000 / 45,000,000,000 x 100 = 3.3333...
        pytest.param("1500000000.00", "45000000000.00", "3.33", id="repeating_rounded_down"),
        # 41.25 / 1,000 x 100 = 4.125 exactly: ROUND_HALF_UP gives 4.13 (half-even would be 4.12).
        pytest.param("41.25", "1000.00", "4.13", id="half_up"),
    ],
)
def test_scenario_margin_is_derived_and_rounded_half_up(
    base_ebitda: str, base_revenue: str, margin: str
) -> None:
    """``ebitda_margin_pct`` is not stored: it is ``base_ebitda_kes / base_revenue_kes x 100``,
    quantized once to 2 dp."""
    scenario = _scenario_from_row(
        quickmart_row(base_ebitda_kes=Decimal(base_ebitda), base_revenue_kes=Decimal(base_revenue))
    )

    assert scenario is not None
    assert str(scenario.assumptions.ebitda_margin_pct) == margin
    assert str(scenario.assumptions.base_revenue_kes) == base_revenue


@pytest.mark.parametrize(
    ("overrides", "logged"),
    [
        pytest.param(
            {"base_revenue_kes": Decimal("0.00")},
            "skipping IPO preset 'Broken Preset': base_revenue_kes must be positive to derive "
            "the base EBITDA margin",
            id="zero_base_revenue",
        ),
        pytest.param(
            {"base_revenue_kes": Decimal("-45000000000.00")},
            "skipping IPO preset 'Broken Preset': base_revenue_kes must be positive to derive "
            "the base EBITDA margin",
            id="negative_base_revenue",
        ),
        # WACC 35% is above the 30.00 slider maximum.
        pytest.param(
            {"assumed_wacc_pct": Decimal("35.0000")},
            "skipping IPO preset 'Broken Preset': stored values do not form a valid "
            "IpoValuationRequest (wacc_pct)",
            id="wacc_out_of_range",
        ),
        # Two fields out of range are both named, sorted.
        pytest.param(
            {"assumed_wacc_pct": Decimal("35.0000"), "tax_rate_pct": Decimal("60.0000")},
            "skipping IPO preset 'Broken Preset': stored values do not form a valid "
            "IpoValuationRequest (tax_rate_pct, wacc_pct)",
            id="two_fields_out_of_range",
        ),
        pytest.param(
            {"risk_scenarios": [{"title": "Valuation Floor"}]},
            "skipping IPO preset 'Broken Preset': stored values do not form a valid "
            "RiskScenario (description)",
            id="malformed_risk_note",
        ),
        # The JSONB column has no type CHECK: a stored null, scalar or object is not an array of
        # risk notes. The message names the field only, so none of these values appears in it.
        pytest.param(
            {"risk_scenarios": None},
            "skipping IPO preset 'Broken Preset': risk_scenarios must be a JSON array of risk "
            "notes",
            id="risk_scenarios_null",
        ),
        pytest.param(
            {"risk_scenarios": 5},
            "skipping IPO preset 'Broken Preset': risk_scenarios must be a JSON array of risk "
            "notes",
            id="risk_scenarios_number",
        ),
        pytest.param(
            {"risk_scenarios": "text"},
            "skipping IPO preset 'Broken Preset': risk_scenarios must be a JSON array of risk "
            "notes",
            id="risk_scenarios_string",
        ),
        pytest.param(
            {"risk_scenarios": {"title": "x"}},
            "skipping IPO preset 'Broken Preset': risk_scenarios must be a JSON array of risk "
            "notes",
            id="risk_scenarios_object",
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
