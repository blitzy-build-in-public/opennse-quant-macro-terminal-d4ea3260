"""OpenAPI documents both 422 shapes on every terminal operation that declares its own 422.

A 422 is either a coded body, ``detail`` an object (``ErrorResponse``), or FastAPI's standard
validation list, ``detail`` an array (``ValidationErrorResponse``). Declaring a 422 response on a
route replaces FastAPI's automatic ``HTTPValidationError`` schema, so the seven operations that
declare one must name the union of both models; every other operation keeps FastAPI's automatic
schema. The app is a bare ``FastAPI()`` that includes ``terminal_router``, as a host does. Only
``app.openapi()`` is read: no request is sent and no database is touched.
"""

from typing import Any

import pytest
from fastapi import FastAPI

from opennse_terminal.api import terminal_router
from opennse_terminal.schemas import ErrorDetail, ErrorResponse, ValidationErrorResponse

REF_PREFIX = "#/components/schemas/"
CODED_REF = REF_PREFIX + ErrorResponse.__name__
VALIDATION_REF = REF_PREFIX + ValidationErrorResponse.__name__
FASTAPI_VALIDATION_REF = REF_PREFIX + "HTTPValidationError"

UNION_422_OPERATIONS = [
    pytest.param("get", "/api/v1/terminal/market/screener", id="screener"),
    pytest.param("get", "/api/v1/terminal/market/valuation-bands", id="valuation-bands"),
    pytest.param("get", "/api/v1/terminal/market/flows", id="flows"),
    pytest.param("get", "/api/v1/terminal/cycles/backtest", id="cycles-backtest"),
    pytest.param("post", "/api/v1/terminal/ipo/valuation", id="ipo-valuation"),
    pytest.param("post", "/api/v1/terminal/ingest/pdf", id="ingest-pdf"),
    pytest.param("post", "/api/v1/terminal/watchlists", id="watchlist-create"),
]


@pytest.fixture(scope="module")
def openapi() -> dict[str, Any]:
    """The OpenAPI document of a bare host app that includes ``terminal_router``."""
    app = FastAPI()
    app.include_router(terminal_router)
    return app.openapi()


def _component(openapi: dict[str, Any], ref: str) -> dict[str, Any]:
    """The ``components/schemas`` entry ``ref`` points at, failing when it does not resolve."""
    assert ref.startswith(REF_PREFIX), ref
    schemas: dict[str, Any] = openapi["components"]["schemas"]
    name = ref.removeprefix(REF_PREFIX)
    assert name in schemas, f"{ref} does not resolve in components/schemas"
    component: dict[str, Any] = schemas[name]
    return component


def _refs(node: object) -> set[str]:
    """Every ``$ref`` value at any depth of ``node``."""
    found: set[str] = set()
    pending = [node]
    while pending:
        current = pending.pop()
        if isinstance(current, dict):
            ref = current.get("$ref")
            if isinstance(ref, str):
                found.add(ref)
            pending.extend(current.values())
        elif isinstance(current, list):
            pending.extend(current)
    return found


def _schema_422(openapi: dict[str, Any], method: str, path: str) -> dict[str, Any] | None:
    """The ``application/json`` schema of the operation's 422 response, or None without one."""
    response = openapi["paths"][path][method]["responses"].get("422")
    if response is None:
        return None
    schema: dict[str, Any] = response["content"]["application/json"]["schema"]
    return schema


@pytest.mark.parametrize(("method", "path"), UNION_422_OPERATIONS)
def test_declared_422_documents_coded_and_validation_shapes(
    openapi: dict[str, Any], method: str, path: str
) -> None:
    """The 422 schema is an anyOf/oneOf of exactly the coded and the validation-list models, every
    reference it reaches resolves, and the two alternatives differ by the JSON type of detail."""
    schema = _schema_422(openapi, method, path)
    assert schema is not None, f"{method.upper()} {path} declares no 422 response"
    alternatives = schema.get("anyOf", schema.get("oneOf"))
    assert isinstance(alternatives, list), schema
    assert {alternative.get("$ref") for alternative in alternatives} == {
        CODED_REF,
        VALIDATION_REF,
    }
    assert openapi["paths"][path][method]["responses"]["422"]["description"]

    # Every reference reachable from the 422 schema resolves, transitively.
    seen: set[str] = set()
    pending = sorted(_refs(schema))
    while pending:
        ref = pending.pop()
        if ref not in seen:
            seen.add(ref)
            pending.extend(_refs(_component(openapi, ref)) - seen)
    assert {CODED_REF, VALIDATION_REF, REF_PREFIX + ErrorDetail.__name__} <= seen

    coded_detail = _component(openapi, CODED_REF)["properties"]["detail"]
    error_detail = _component(openapi, coded_detail["$ref"])
    assert error_detail["type"] == "object"
    assert {"code", "message"} <= set(error_detail["required"])
    assert set(error_detail["properties"]) == {"code", "message", "field"}

    validation_detail = _component(openapi, VALIDATION_REF)["properties"]["detail"]
    assert validation_detail["type"] == "array"
    item = _component(openapi, validation_detail["items"]["$ref"])
    assert item["type"] == "object"
    assert set(item["required"]) == {"loc", "msg", "type"}
    assert {"loc", "msg", "type", "input", "ctx"} == set(item["properties"])
    assert item["properties"]["loc"]["type"] == "array"


def test_every_documented_422_admits_the_validation_list(openapi: dict[str, Any]) -> None:
    """No operation documents its 422 as a coded object alone: the seven declaring operations name
    the union, and every other operation with a 422 keeps FastAPI's automatic schema."""
    declared = {(param.values[0], param.values[1]) for param in UNION_422_OPERATIONS}
    union_operations: set[tuple[str, str]] = set()
    for path, item in openapi["paths"].items():
        for method in item:
            schema = _schema_422(openapi, method, path)
            if schema is None:
                continue
            alternatives = {
                alternative.get("$ref") for alternative in schema.get("anyOf", [schema])
            }
            if VALIDATION_REF in alternatives:
                union_operations.add((method, path))
            else:
                assert alternatives == {FASTAPI_VALIDATION_REF}, (method, path, schema)
    assert union_operations == declared
