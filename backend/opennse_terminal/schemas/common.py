"""Shared building blocks of the terminal's Pydantic v2 request and response schemas.

The schema modules are a pure JSON contract, mirrored one-to-one by
``src/components/terminal/types.ts``. This module therefore imports only the standard library,
Pydantic (with its ``pydantic_core``) and :mod:`opennse_terminal.config`; it never imports
FastAPI, SQLAlchemy, the models or the services. It holds:

* :class:`TerminalRequestModel` and :class:`TerminalResponseModel`, the bases of every schema;
* the coded error contract: :data:`ErrorCode`, :class:`ErrorDetail` and :class:`ErrorResponse`;
* :data:`Flag`, the base reason codes that accompany null or qualified metric values;
* bounded request aliases (:data:`RequestDecimal`, :data:`Kes`, :data:`ShareCount`, ...);
* the shared response shapes :class:`CoverageMeta`, :class:`SeriesPoint` and
  :class:`HealthResponse`.

Pydantic serializes every ``Decimal`` as a JSON string and keeps its exponent, so
``Decimal("7.50")`` becomes ``"7.50"``. Services quantize each value once, with
``quant.numeric.quantize_out``, before building a response model; this module adds no custom
serializer. Requests accept a ``Decimal`` as a JSON number or string.
"""

import datetime as dt
import math
from decimal import Decimal
from typing import Annotated, Any, Final, Literal, Self, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    ValidationError,
    model_validator,
)
from pydantic_core import InitErrorDetails, PydanticCustomError

from opennse_terminal.config import MAX_SHARE_COUNT

__all__ = [
    "ERROR_CODES",
    "FLAG_CODES",
    "CoverageMeta",
    "ErrorCode",
    "ErrorDetail",
    "ErrorResponse",
    "Flag",
    "HealthResponse",
    "Kes",
    "NonNegativeKes",
    "Pct",
    "PositiveShareCount",
    "Ratio",
    "RequestDecimal",
    "SectorName",
    "SeriesPoint",
    "ShareCount",
    "TerminalRequestModel",
    "TerminalResponseModel",
    "Ticker",
]


def _contains_non_finite(value: object) -> bool:
    """Whether ``value`` is a NaN or infinite float, or a dict or list holding one at any depth.

    The walk is iterative, so a deeply nested input cannot exhaust the interpreter stack.
    """
    pending = [value]
    while pending:
        node = pending.pop()
        if isinstance(node, float):
            if not math.isfinite(node):
                return True
        elif isinstance(node, dict):
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return False


def _json_safe_scalar_or_shell(value: object) -> object:
    """One level of :func:`_json_safe_input`.

    Returns an empty dict or list for a dict or list, the JSON literal spelling (``"NaN"``,
    ``"Infinity"`` or ``"-Infinity"``) for a non-finite float, and ``value`` itself otherwise.
    """
    if isinstance(value, dict):
        return {}
    if isinstance(value, list):
        return []
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    return value


def _json_safe_input(value: object) -> object:
    """A copy of ``value`` with every non-finite float spelled as its JSON literal string.

    NaN becomes ``"NaN"``, positive infinity ``"Infinity"`` and negative infinity
    ``"-Infinity"``, at any depth inside dicts and lists; every other value, dict key and list
    position is kept. The copy is built iteratively, so a deeply nested input cannot exhaust the
    interpreter stack, and ``value`` itself is never modified.
    """
    root = _json_safe_scalar_or_shell(value)
    pending: list[tuple[object, object]] = [(value, root)]
    while pending:
        source, target = pending.pop()
        if isinstance(source, dict) and isinstance(target, dict):
            for key, item in source.items():
                copy = _json_safe_scalar_or_shell(item)
                target[key] = copy
                if isinstance(copy, dict | list):
                    pending.append((item, copy))
        elif isinstance(source, list) and isinstance(target, list):
            for item in source:
                copy = _json_safe_scalar_or_shell(item)
                target.append(copy)
                if isinstance(copy, dict | list):
                    pending.append((item, copy))
    return root


class TerminalRequestModel(BaseModel):
    """Base of every request body and parameter model: unknown keys are rejected.

    Subclasses declare single-field bounds only (types, ``ge``/``le``, lengths), which FastAPI
    reports in its standard 422 list. Rules that relate two or more fields are never
    ``model_validator`` or ``field_validator`` hooks, because those errors would carry no stable
    code. Pure check functions such as ``quant.dcf.assumption_violations`` and the services or
    routes test them instead and raise the coded 422 described by :class:`ErrorDetail`.

    The one model-level hook, :meth:`_render_non_finite_inputs`, checks no rule and adds no
    error. FastAPI decodes a JSON body with the standard library, which reads the literals
    ``NaN``, ``Infinity`` and ``-Infinity``, and a number that overflows such as ``1e999999``, as
    non-finite floats. Pydantic rejects them, but FastAPI's standard 422 handler serializes each
    error's ``input`` with ``allow_nan=False`` and would fail with a 500 instead. The hook only
    rewrites such an input as the string of its JSON literal, so the rejection reaches the client
    as FastAPI's standard 422 list.
    """

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="wrap")
    @classmethod
    def _render_non_finite_inputs(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        """Validate ``data`` unchanged, making a rejected non-finite JSON number renderable.

        Valid input returns the validated model and invalid input raises a ``ValidationError``
        with the same errors (type, location, message and context) as without the hook. Only
        when an error's ``input`` holds a NaN or infinite float, at any depth inside dicts and
        lists, are the errors raised again with each input copied by :func:`_json_safe_input`;
        those errors then carry no documentation URL, which FastAPI's 422 list omits anyway.
        """
        try:
            return handler(data)
        except ValidationError as exc:
            errors = exc.errors(include_url=False)
            if not any(_contains_non_finite(error["input"]) for error in errors):
                raise
            # Every error is rebuilt as a custom error that carries its type, rendered message
            # and context verbatim. Rebuilding a pydantic-core error from its type string would
            # render the message again from the context, which can differ: a Decimal bound of
            # 30.00 renders as 30.
            line_errors: list[InitErrorDetails] = [
                {
                    "type": PydanticCustomError(error["type"], error["msg"], error.get("ctx")),
                    "loc": error["loc"],
                    "input": _json_safe_input(error["input"]),
                }
                for error in errors
            ]
            raise ValidationError.from_exception_data(exc.title, line_errors) from None


class TerminalResponseModel(BaseModel):
    """Base of every response model.

    ``extra="forbid"`` makes a misspelled keyword in a service fail at construction instead of
    being dropped silently. Services build responses explicitly from computed values, so
    ``from_attributes`` is not enabled.
    """

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------------------------
# Coded error contract
# ---------------------------------------------------------------------------------------------

ErrorCode = Literal[
    "database_unavailable",
    "unknown_ticker",
    "unknown_sector",
    "watchlist_not_found",
    "min_exceeds_max",
    "exactly_one_subject",
    "conflicting_subjects",
    "start_after_end",
    "no_event_dates",
    "conflicting_window_units",
    "window_out_of_range",
    "wacc_not_above_terminal_growth",
    "shares_offered_exceeds_outstanding",
    "primary_exceeds_offer_size",
    "request_too_large",
    "file_too_large",
    "not_a_pdf",
    "pdf_encrypted",
    "pdf_unreadable",
    "too_many_pages",
    "commit_requires_ticker_and_period",
    "commit_requires_kes",
    "currency_conflict",
    "ingest_busy",
    "watchlist_name_taken",
]
"""Every stable snake_case code a terminal endpoint raises in a coded error body."""

ERROR_CODES: Final[frozenset[str]] = frozenset(get_args(ErrorCode))
"""The members of :data:`ErrorCode`, for membership checks at runtime."""


class ErrorDetail(TerminalResponseModel):
    """The ``detail`` object of a coded error response, at any HTTP status.

    Routes and services raise ``HTTPException(status_code, detail=error.model_dump())`` with
    ``error = ErrorDetail(code=..., message=..., field=...)``, so FastAPI answers
    ``{"detail": {"code": ..., "message": ..., "field": ...}}`` without any app-level exception
    handler, and a host that mounts the router needs nothing else. An error body never carries a
    result payload.
    """

    # Typed str rather than ErrorCode: quant.numeric.Violation.code, raised as-is, is a plain str.
    code: str = Field(
        description=(
            "Stable snake_case error code, one of the documented ErrorCode values, "
            "for example primary_exceeds_offer_size."
        ),
    )
    message: str = Field(description="Human-readable explanation of the error.")
    field: str | None = Field(
        default=None,
        description=(
            "The offending query parameter, path parameter, form field or body field, for "
            "example primary_proceeds_kes; null when the error concerns no single field."
        ),
    )


class ErrorResponse(TerminalResponseModel):
    """OpenAPI documentation of a coded error body, for ``responses={404: {"model": ...}}``.

    FastAPI's standard single-field validation 422, whose ``detail`` is a list of
    ``{type, loc, msg, input}`` entries, is left unchanged and is not modelled here.
    """

    detail: ErrorDetail = Field(description="The coded error: code, message and offending field.")


# ---------------------------------------------------------------------------------------------
# Reason codes for null or qualified values
# ---------------------------------------------------------------------------------------------

Flag = Literal[
    # Valuation multiples, dividend yield and sector aggregates.
    "pe_negative_earnings",
    "pe_zero_earnings",
    "pe_missing_earnings",
    "pb_non_positive_equity",
    "pb_missing_equity",
    "ev_not_meaningful_for_banks",
    "ev_ebitda_non_positive_ebitda",
    "ev_ebitda_missing_ebitda",
    "ev_missing_inputs",
    "negative_enterprise_value",
    "zero_shares",
    "dy_missing_dps",
    # Emitted by quant.valuation.dividend_yield_pct for a close <= 0, so it is listed with the
    # other base codes although the close_price > 0 database check normally prevents it.
    "dy_non_positive_price",
    "no_fundamentals",
    "currency_mismatch",
    "stale_price",
    "sector_pe_unavailable",
    "sector_pb_unavailable",
    "sector_ev_ebitda_unavailable",
    "sector_dy_unavailable",
    # Screener session change.
    "no_previous_close",
    # Emitted by quant.valuation.price_change_pct for a previous close <= 0, so it is listed with
    # the other base codes although the close_price > 0 database check normally prevents it.
    "non_positive_previous_close",
    # Historical valuation bands.
    "insufficient_history",
    "no_positive_earnings",
    "no_positive_equity",
    # Rolling volume and turnover Z-scores.
    "insufficient_observations",
    "zero_variance",
    # Foreign versus local net flows.
    "no_flow_data",
    # Event and election cycle backtester.
    "empty_cohort",
    "zero_anchor_capitalization",
    "insufficient_turnover_history",
    "zero_anchor_turnover",
    "pe_anchor_unavailable",
    "no_pre_event_data",
    "insufficient_projection_basis",
    # IPO DCF, comparables and sensitivity cells.
    "negative_terminal_fcf",
    "negative_equity_value",
    "comps_non_positive_ebitda",
    "wacc_not_above_terminal_growth",
]
"""Base reason codes the quant engine attaches to a null or qualified metric value."""

FLAG_CODES: Final[frozenset[str]] = frozenset(get_args(Flag))
"""The members of :data:`Flag`, for membership checks at runtime."""

# Every ``flags`` field in the schema modules is ``list[str]``, not ``list[Flag]``: the screener
# prefixes a Z-score code and a session-change code with its metric, as in
# ``volume_z:insufficient_observations``, ``turnover_z:zero_variance`` and
# ``change_pct:no_previous_close``, which a Literal would reject. The convention is
#     flags: list[str] = Field(
#         default_factory=list, description="Reason codes for null or qualified values."
#     )


# ---------------------------------------------------------------------------------------------
# Bounded request aliases
# ---------------------------------------------------------------------------------------------
# A field narrows an alias further with its own constraints, for example
#     wacc_pct: Pct = Field(ge=Decimal("8.00"), le=Decimal("30.00"), description=...)
# Pydantic merges the Annotated constraints with the assigned Field. Bounds are written as
# Decimal literals or ints, never floats. Response fields use plain Decimal, Decimal | None and
# int: their values arrive already quantized by the services.

RequestDecimal = Annotated[Decimal, Field(max_digits=28, allow_inf_nan=False)]
"""Base of every request decimal: at most 28 significant digits, the precision of the default
``decimal`` context, and finite (NaN and +/-Infinity are rejected). Accepts a JSON number or a
string; the frontend always sends strings."""

Kes = Annotated[Decimal, Field(max_digits=28, allow_inf_nan=False)]
"""An amount in whole Kenyan shillings (KES, not thousands), of either sign."""

NonNegativeKes = Annotated[Decimal, Field(max_digits=28, allow_inf_nan=False, ge=0)]
"""An amount in whole Kenyan shillings (KES), zero or greater."""

Pct = Annotated[Decimal, Field(max_digits=28, allow_inf_nan=False)]
"""A percentage in percentage points: ``6.80`` means 6.80%."""

Ratio = Annotated[Decimal, Field(max_digits=28, allow_inf_nan=False)]
"""A dimensionless multiple (x), such as P/E or EV/EBITDA."""

ShareCount = Annotated[int, Field(ge=0, le=MAX_SHARE_COUNT)]
"""A share count or traded volume in shares, 0 to 10^12, so the JSON integer is exact in
JavaScript."""

PositiveShareCount = Annotated[int, Field(ge=1, le=MAX_SHARE_COUNT)]
"""A share count in shares, 1 to 10^12."""

Ticker = Annotated[str, Field(min_length=1, max_length=16)]
"""An NSE ticker symbol such as ``SCOM``, as stored in ``terminal_equities.ticker``
(``VARCHAR(16)``). Not case-normalized; an unknown ticker is a coded 404 or 422."""

SectorName = Annotated[str, Field(min_length=1, max_length=100)]
"""A sector name such as ``Banking``, as stored in ``terminal_equities.sector``
(``VARCHAR(100)``). Not normalized; an unknown sector is a coded 404."""


# ---------------------------------------------------------------------------------------------
# Shared response models
# ---------------------------------------------------------------------------------------------


class CoverageMeta(TerminalResponseModel):
    """How much of a requested history window the stored data covers.

    Valuation bands report partial coverage when ``coverage_start`` is more than 7 days after
    ``requested_start``; the macro overview when it is more than one month after the first
    month-end of its full requested window: ``requested_start`` with ordinary dates, while months
    cut before year 1 count toward the one-month tolerance.
    """

    requested_start: dt.date = Field(
        description=(
            "First date of the requested window: as_of minus the lookback, or the first "
            "month-end of the macro history window."
        ),
    )
    coverage_start: dt.date | None = Field(
        description="First date in the window that has a value; null when there is none.",
    )
    is_partial_coverage: bool = Field(
        description="True when the available data starts materially after requested_start.",
    )


class SeriesPoint(TerminalResponseModel):
    """One dated value of a time series, in the unit of the series that holds it."""

    date: dt.date = Field(description="Observation or trade date (YYYY-MM-DD).")
    value: Decimal | None = Field(
        description="The value on that date as a decimal string; null when undefined.",
    )


class HealthResponse(TerminalResponseModel):
    """Body of ``GET /health``. A failed database check is the coded 503
    ``database_unavailable`` error, not this model."""

    status: Literal["ok", "degraded"] = Field(
        description="Overall status of the terminal API; ok when every check passed.",
    )
    database: Literal["ok", "unavailable"] = Field(
        description="Result of the database round-trip check.",
    )
    version: str = Field(description="Installed opennse_terminal package version.")
