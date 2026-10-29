"""Hostile JSON request bodies get one of the two 422 shapes, never a 500 or a 400.

Two layers make every rejected JSON body answer with FastAPI's standard 422 list:

* ``TerminalRequestModel``'s wrap hook re-spells a rejected NaN or infinite float input as its
  JSON literal (``"NaN"``, ``"Infinity"``, ``"-Infinity"``), which FastAPI's default handler can
  serialize. It applies on any route class, so both route classes are tested.
* ``TerminalJsonRoute`` turns every decoding failure of the standard-library JSON decoder (deep
  nesting, over-long integer literals, invalid UTF-8, unpaired surrogates) into FastAPI's own
  ``json_invalid`` entry instead of a 400 with a string ``detail``.

The app mirrors the planned body routes, ``POST /ipo/valuation`` and ``POST /watchlists``, each on
a plain ``APIRouter()`` and on ``APIRouter(route_class=TerminalJsonRoute)``, included under a
prefixed router carrying the terminal principal dependency as ``terminal_router`` does. Bodies are
sent as raw bytes, because the hostile literals are not producible by ``json.dumps``. No database
is touched.

FastAPI reads the endpoint signatures at runtime, so this module does not use
``from __future__ import annotations``.
"""

import json
from collections.abc import AsyncIterator
from typing import Annotated, Any

import httpx
import pytest
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, ValidationError
from pydantic_core import PydanticCustomError

from opennse_terminal.api.deps import TerminalJsonRoute, get_terminal_principal
from opennse_terminal.schemas.common import RequestDecimal, TerminalRequestModel
from opennse_terminal.schemas.ipo import IpoValuationRequest, IpoValuationResponse
from opennse_terminal.schemas.watchlists import WatchlistCreate
from opennse_terminal.services import ipo_service

PLAIN_PREFIX = "/plain/api/v1/terminal"
JSON_ROUTE_PREFIX = "/json-route/api/v1/terminal"
BOTH_PREFIXES = [
    pytest.param(PLAIN_PREFIX, id="APIRoute"),
    pytest.param(JSON_ROUTE_PREFIX, id="TerminalJsonRoute"),
]
IPO = "/ipo/valuation"
WATCHLISTS = "/watchlists"
JSON_HEADERS = {"content-type": "application/json"}

# The Quickmart preset body of tests/unit/quant/test_dcf_and_comparables.py::request_payload().
QUICKMART: dict[str, object] = {
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

FINITE_NUMBER_MSG = "Input should be a finite number"
STRING_TYPE_MSG = "Input should be a valid string"
JSON_NESTING_ERROR = "JSON nesting exceeds the 200-level limit"
LONE_SURROGATE_ERROR = "JSON text contains an unpaired UTF-16 surrogate"
INTEGER_LIMIT_ERROR = "Integer literal exceeds the 4300-digit limit"


def quickmart_raw(**raw_values: str) -> bytes:
    """The Quickmart body as JSON text, with each named member's value replaced by raw JSON text.

    A name that is not a Quickmart field is appended as an extra member.
    """
    members = {name: json.dumps(value) for name, value in QUICKMART.items()}
    members.update(raw_values)
    text = ", ".join(f"{json.dumps(name)}: {value}" for name, value in members.items())
    return ("{" + text + "}").encode()


def nested_array(depth: int) -> str:
    """An empty JSON array nested ``depth`` levels deep."""
    return "[" * depth + "]" * depth


def nested_object(depth: int) -> str:
    """A JSON object nested ``depth`` levels deep around the number 1."""
    return '{"a":' * depth + "1" + "}" * depth


def build_app() -> FastAPI:
    """Both body routes on a plain and a ``TerminalJsonRoute`` sub-router, mounted like a host."""
    app = FastAPI()
    for prefix, sub_router in (
        (PLAIN_PREFIX, APIRouter()),
        (JSON_ROUTE_PREFIX, APIRouter(route_class=TerminalJsonRoute)),
    ):

        @sub_router.post(IPO)
        async def value_ipo(body: IpoValuationRequest) -> IpoValuationResponse:
            return await ipo_service.value_ipo(body)

        @sub_router.post(WATCHLISTS, status_code=201)
        async def create_watchlist(body: WatchlistCreate) -> WatchlistCreate:
            return body

        terminal = APIRouter(prefix=prefix, dependencies=[Depends(get_terminal_principal)])
        terminal.include_router(sub_router)
        app.include_router(terminal)
    return app


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client on the test app; an exception escaping the app fails the test."""
    transport = httpx.ASGITransport(app=build_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        yield http


async def post_raw(client: httpx.AsyncClient, path: str, body: bytes) -> httpx.Response:
    """POST ``body`` unchanged as ``application/json``."""
    return await client.post(path, content=body, headers=JSON_HEADERS)


# ---------------------------------------------------------------------------------------------
# Non-finite JSON number literals: the standard 422 list on every route class
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("path", "body", "expected"),
    [
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=literal),
            {
                "type": "finite_number",
                "loc": ["body", "offer_price_kes"],
                "msg": FINITE_NUMBER_MSG,
                "input": spelled,
            },
            id=f"offer_price_kes_{literal}",
        )
        for literal, spelled in (
            ("NaN", "NaN"),
            ("Infinity", "Infinity"),
            ("-Infinity", "-Infinity"),
            ("1e999999", "Infinity"),
            ("-1e999999", "-Infinity"),
        )
    ]
    + [
        pytest.param(
            IPO,
            quickmart_raw(x="NaN"),
            {
                "type": "extra_forbidden",
                "loc": ["body", "x"],
                "msg": "Extra inputs are not permitted",
                "input": "NaN",
            },
            id="extra_member_NaN",
        ),
        pytest.param(
            IPO,
            quickmart_raw(shares_outstanding="NaN"),
            {
                "type": "finite_number",
                "loc": ["body", "shares_outstanding"],
                "msg": FINITE_NUMBER_MSG,
                "input": "NaN",
            },
            id="shares_outstanding_NaN",
        ),
        pytest.param(
            IPO,
            quickmart_raw(sensitivity_wacc_pct="[NaN]"),
            {
                "type": "finite_number",
                "loc": ["body", "sensitivity_wacc_pct", 0],
                "msg": FINITE_NUMBER_MSG,
                "input": "NaN",
            },
            id="sensitivity_axis_element_NaN",
        ),
        pytest.param(
            IPO,
            b"NaN",
            {
                "type": "model_attributes_type",
                "loc": ["body"],
                "msg": "Input should be a valid dictionary or object to extract fields from",
                "input": "NaN",
            },
            id="whole_body_NaN",
        ),
        pytest.param(
            IPO,
            b"[NaN]",
            {
                "type": "model_attributes_type",
                "loc": ["body"],
                "msg": "Input should be a valid dictionary or object to extract fields from",
                "input": ["NaN"],
            },
            id="whole_body_list_NaN",
        ),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes='{"v": [NaN, 1, {"w": -Infinity}]}'),
            {
                "type": "decimal_type",
                "loc": ["body", "offer_price_kes"],
                "msg": "Decimal input should be an integer, float, string or Decimal object",
                "input": {"v": ["NaN", 1, {"w": "-Infinity"}]},
            },
            id="offer_price_kes_nested_non_finite",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": NaN}',
            {
                "type": "string_type",
                "loc": ["body", "name"],
                "msg": STRING_TYPE_MSG,
                "input": "NaN",
            },
            id="watchlist_name_NaN",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "ok", "tickers": [Infinity]}',
            {
                "type": "string_type",
                "loc": ["body", "tickers", 0],
                "msg": STRING_TYPE_MSG,
                "input": "Infinity",
            },
            id="watchlist_ticker_Infinity",
        ),
    ],
)
async def test_non_finite_literal_returns_standard_422(
    client: httpx.AsyncClient, prefix: str, path: str, body: bytes, expected: dict[str, Any]
) -> None:
    response = await post_raw(client, prefix + path, body)

    assert response.status_code == 422
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {"detail": [expected]}


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
async def test_non_finite_literal_leaves_other_errors_unchanged(
    client: httpx.AsyncClient, prefix: str
) -> None:
    """Every other error of the body keeps its type, location, message and context."""
    with_nan = await post_raw(
        client, prefix + IPO, quickmart_raw(offer_price_kes="NaN", wacc_pct='"100"')
    )
    without_nan = await post_raw(client, prefix + IPO, quickmart_raw(wacc_pct='"100"'))

    assert with_nan.status_code == without_nan.status_code == 422
    finite_error, *other_errors = with_nan.json()["detail"]
    assert finite_error["type"] == "finite_number"
    assert finite_error["input"] == "NaN"
    assert other_errors == without_nan.json()["detail"]
    assert other_errors[0]["type"] == "less_than_equal"
    assert "ctx" in other_errors[0]


def test_custom_error_type_survives_non_finite_rendering() -> None:
    """An error type outside pydantic-core's own keeps its code, message and context."""

    def reject(value: str) -> str:
        raise PydanticCustomError("custom_code", "Custom rejection of {value}", {"value": value})

    class CustomRequest(TerminalRequestModel):
        label: Annotated[str, AfterValidator(reject)]
        amount: RequestDecimal

    with pytest.raises(ValidationError) as caught:
        CustomRequest.model_validate(json.loads('{"label": "abc", "amount": NaN}'))

    errors = caught.value.errors(include_url=False)
    rendered = json.loads(json.dumps(errors, allow_nan=False))
    assert rendered == [
        {
            "type": "custom_code",
            "loc": ["label"],
            "msg": "Custom rejection of abc",
            "input": "abc",
            "ctx": {"value": "abc"},
        },
        {"type": "finite_number", "loc": ["amount"], "msg": FINITE_NUMBER_MSG, "input": "NaN"},
    ]


# ---------------------------------------------------------------------------------------------
# Undecodable bodies on TerminalJsonRoute: FastAPI's json_invalid entry, never a 400
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "body", "position", "error"),
    [
        pytest.param(IPO, nested_array(10_000).encode(), 0, JSON_NESTING_ERROR, id="array_10k"),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=nested_array(10_000)),
            0,
            JSON_NESTING_ERROR,
            id="offer_price_kes_array_10k",
        ),
        pytest.param(IPO, nested_object(10_000).encode(), 0, JSON_NESTING_ERROR, id="object_10k"),
        pytest.param(IPO, nested_array(100_000).encode(), 0, JSON_NESTING_ERROR, id="array_100k"),
        # Decodable by the standard library, but too deep for FastAPI's error renderer.
        pytest.param(IPO, nested_array(5_000).encode(), 0, JSON_NESTING_ERROR, id="array_5k"),
        pytest.param(
            IPO,
            quickmart_raw(offer_price_kes=nested_array(200)),
            0,
            JSON_NESTING_ERROR,
            id="nesting_201_levels",
        ),
        pytest.param(
            IPO,
            quickmart_raw(base_revenue_kes="9" * 1_000_000),
            0,
            INTEGER_LIMIT_ERROR,
            id="base_revenue_kes_1M_digits",
        ),
        pytest.param(
            IPO,
            quickmart_raw(shares_outstanding="9" * 1_000_000),
            0,
            INTEGER_LIMIT_ERROR,
            id="shares_outstanding_1M_digits",
        ),
        pytest.param(
            IPO,
            b'{"offer_price_kes": "\xff"}',
            21,
            "Invalid utf-8 text: invalid start byte",
            id="invalid_utf8",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "a\xc3\x28"}',
            11,
            "Invalid utf-8 text: invalid continuation byte",
            id="watchlist_invalid_utf8",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "\\ud800"}',
            0,
            LONE_SURROGATE_ERROR,
            id="escaped_lone_surrogate",
        ),
        pytest.param(
            WATCHLISTS,
            b'{"name": "ok", "\xed\xa0\x80": 1}',
            0,
            LONE_SURROGATE_ERROR,
            id="raw_lone_surrogate_key",
        ),
    ],
)
async def test_undecodable_body_returns_json_invalid(
    client: httpx.AsyncClient, path: str, body: bytes, position: int, error: str
) -> None:
    response = await post_raw(client, JSON_ROUTE_PREFIX + path, body)

    assert response.status_code == 422
    assert response.json() == {
        "detail": [
            {
                "type": "json_invalid",
                "loc": ["body", position],
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": error},
            }
        ]
    }


async def test_nesting_at_the_limit_is_validated(client: httpx.AsyncClient) -> None:
    """200 levels decode, so the body reaches validation and gets the field's own error."""
    response = await post_raw(
        client, JSON_ROUTE_PREFIX + IPO, quickmart_raw(offer_price_kes=nested_array(199))
    )

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["type"] == "decimal_type"
    assert error["loc"] == ["body", "offer_price_kes"]


async def test_host_validation_handler_receives_undecodable_body() -> None:
    """A host's RequestValidationError handler answers an undecodable body, not a 400."""
    app = build_app()

    async def host_handler(request: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, RequestValidationError)
        return JSONResponse({"host_errors": [error["type"] for error in exc.errors()]}, 418)

    app.add_exception_handler(RequestValidationError, host_handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://terminal.test") as http:
        response = await post_raw(http, JSON_ROUTE_PREFIX + IPO, nested_array(10_000).encode())

    assert response.status_code == 418
    assert response.json() == {"host_errors": ["json_invalid"]}


# ---------------------------------------------------------------------------------------------
# Controls: decodable bodies behave exactly as on a plain route
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(quickmart_raw(), id="quickmart"),
        pytest.param(b"\xef\xbb\xbf" + quickmart_raw(), id="quickmart_utf8_bom"),
    ],
)
async def test_valid_quickmart_body_is_valued(
    client: httpx.AsyncClient, prefix: str, body: bytes
) -> None:
    response = await post_raw(client, prefix + IPO, body)

    assert response.status_code == 200
    assert response.json()["blended_fair_value_per_share_kes"] == "6.82"


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
async def test_truncated_json_keeps_fastapi_json_invalid(
    client: httpx.AsyncClient, prefix: str
) -> None:
    response = await post_raw(client, prefix + IPO, b'{"offer_price_kes": ')

    assert response.status_code == 422
    assert response.json() == {
        "detail": [
            {
                "type": "json_invalid",
                "loc": ["body", 20],
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": "Expecting value"},
            }
        ]
    }


@pytest.mark.parametrize("prefix", BOTH_PREFIXES)
@pytest.mark.parametrize(
    ("body", "name"),
    [
        pytest.param(b'{"name": " Banks ", "tickers": ["KCB"]}', "Banks", id="ascii"),
        pytest.param(
            '{"name": "Benki \u2713", "tickers": ["KCB"]}'.encode(), "Benki \u2713", id="utf8"
        ),
        pytest.param(
            b'{"name": "Top \\ud83d\\ude00", "tickers": ["KCB"]}',
            "Top \U0001f600",
            id="escaped_surrogate_pair",
        ),
    ],
)
async def test_valid_watchlist_body_is_accepted(
    client: httpx.AsyncClient, prefix: str, body: bytes, name: str
) -> None:
    response = await post_raw(client, prefix + WATCHLISTS, body)

    assert response.status_code == 201
    assert response.json() == {"name": name, "tickers": ["KCB"]}
