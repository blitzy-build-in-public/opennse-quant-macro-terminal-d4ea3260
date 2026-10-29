"""Numeric conventions of the quant engine and its single rounding point.

Every function in ``opennse_terminal.quant`` takes and returns ``Decimal`` (or ``None``) and
computes under the default ``decimal`` context: 28 significant digits, ROUND_HALF_EVEN for
intermediate operations. Formulas never round. Services round each output value exactly once,
with :func:`quantize_out`, while building response models.

An undefined metric is ``None`` (``null`` in JSON) with a reason code in the caller's ``flags``,
never zero; :func:`safe_div` is the building block for that.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation
from types import MappingProxyType
from typing import Final, Literal, overload

__all__ = [
    "HUNDRED",
    "ONE",
    "PLACES",
    "ZERO",
    "QuantizeKind",
    "Violation",
    "quantize_out",
    "safe_div",
    "to_decimal",
]

QuantizeKind = Literal[
    "money", "per_share", "ratio", "pct", "z", "index", "discount_factor", "tenor"
]
"""The output category of a value, which fixes its decimal places in :data:`PLACES`."""

PLACES: Final[Mapping[QuantizeKind, int]] = MappingProxyType(
    {
        "money": 2,  # KES totals
        "per_share": 2,  # KES prices and fair values per share
        "ratio": 2,  # P/E, P/B, EV/EBITDA and other multiples
        "pct": 2,  # percentages and returns, in percentage points
        "z": 2,  # Z-scores
        "index": 2,  # base-100 index levels
        "discount_factor": 6,  # DCF discount factors
        "tenor": 2,  # years
    }
)
"""Decimal places per :data:`QuantizeKind`, applied by :func:`quantize_out`. Read-only."""

ZERO: Final[Decimal] = Decimal(0)
ONE: Final[Decimal] = Decimal(1)
HUNDRED: Final[Decimal] = Decimal(100)

# Used only for the output quantize step. Its wider precision lets a large magnitude, such as a
# 10^16 KES terminal value at 6 dp, keep every requested place instead of raising
# InvalidOperation; formulas still run under the default 28-digit context.
_QUANTIZE_CONTEXT: Final[Context] = Context(prec=60)


@dataclass(frozen=True)
class Violation:
    """A cross-field rule failure reported by a pure check function.

    Rules that relate two or more request fields are not Pydantic validators, which would surface
    only in FastAPI's uncoded 422 list. A pure check returns ``Violation`` records instead, and the
    route raises the first one as a coded 422 whose ``detail`` is
    ``{"code": code, "message": message, "field": field}``.
    """

    code: str
    """Stable snake_case error code, for example ``primary_exceeds_offer_size``."""
    field: str
    """The request field the error is reported against, for example ``primary_proceeds_kes``."""
    message: str
    """Human-readable explanation shown to the user."""


@overload
def quantize_out(value: Decimal, kind: QuantizeKind) -> Decimal: ...


@overload
def quantize_out(value: None, kind: QuantizeKind) -> None: ...


def quantize_out(value: Decimal | None, kind: QuantizeKind) -> Decimal | None:
    """Round ``value`` to the fixed places of ``kind`` with ROUND_HALF_UP.

    This is the only rounding point: services call it once per output field while building
    response models, and formulas never round. Places per kind:

    ===================  ======  =====================================
    kind                 places  applies to
    ===================  ======  =====================================
    ``money``            2       KES totals
    ``per_share``        2       KES prices and fair values per share
    ``ratio``            2       P/E, P/B, EV/EBITDA, multiples
    ``pct``              2       percentages and returns
    ``z``                2       Z-scores
    ``index``            2       base-100 levels
    ``discount_factor``  6       DCF discount factors
    ``tenor``            2       years
    ===================  ======  =====================================

    ``None`` passes through unchanged. A zero result is returned unsigned, so ``-0.001`` becomes
    ``0.00`` rather than ``-0.00``.

    Raises:
        TypeError: ``value`` is neither ``None`` nor a ``Decimal`` (a ``float``, ``int`` or
            ``bool`` included).
        ValueError: ``value`` is NaN or infinite, or ``kind`` is not a known kind.
    """
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise TypeError(f"quantize_out expects a Decimal or None, got {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"cannot quantize a non-finite value: {value}")
    places = PLACES.get(kind)
    if places is None:
        raise ValueError(f"unknown quantize kind: {kind!r}")
    result = value.quantize(ONE.scaleb(-places), rounding=ROUND_HALF_UP, context=_QUANTIZE_CONTEXT)
    if result.is_zero():
        return result.copy_abs()
    return result


def to_decimal(text: str) -> Decimal:
    """Parse a decimal literal strictly, for constants in code and tests.

    Surrounding whitespace is stripped. Thousands separators, parentheses for negatives and unit
    suffixes are not accepted; source-specific cleaning belongs to the ingestion parsers.

    Raises:
        TypeError: ``text`` is not a ``str``; a ``float`` is rejected so no binary-float value
            can enter a calculation.
        ValueError: ``text`` is empty, is not a decimal literal, or denotes NaN or infinity.
    """
    if not isinstance(text, str):
        raise TypeError(f"to_decimal expects a str, got {type(text).__name__}")
    stripped = text.strip()
    if not stripped:
        raise ValueError("to_decimal received an empty string")
    try:
        value = Decimal(stripped)
    except InvalidOperation as exc:
        raise ValueError(f"not a decimal literal: {text!r}") from exc
    # A context that does not trap InvalidOperation returns NaN for bad input instead of raising.
    if not value.is_finite():
        raise ValueError(f"not a finite decimal literal: {text!r}")
    return value


def safe_div(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    """Return ``numerator / denominator`` at full context precision, or ``None`` for a zero divisor.

    Used for ratios that are undefined when the divisor is zero. The caller reports ``None``
    together with a reason flag, never a zero. The quotient is not rounded.
    """
    if denominator.is_zero():
        return None
    return numerator / denominator
