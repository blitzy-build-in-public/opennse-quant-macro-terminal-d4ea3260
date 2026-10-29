"""Response contract of ``POST /ingest/pdf``: line items extracted from an uploaded PDF filing.

Corporate annual and interim reports and IPO prospectuses are uploaded as multipart form data and
parsed with pdfplumber. The multipart fields are declared in ``api.routes.ingest`` with
``Form()`` and ``File()``; this module supplies their literal types (:data:`DocumentType`,
:data:`PeriodType`, :data:`LineItemName`), the default item tuple :data:`ALL_LINE_ITEMS`, and
the success body :class:`IngestPdfResponse`.

Extracted amounts are ``Decimal`` values serialized as JSON strings. A monetary value is the
source token times the detected statement scale, so a "KShs '000" figure of ``1,234,567`` is
``"1234567000"`` whole shillings; ``raw_value`` keeps the token exactly as printed. Nothing is
converted to another currency: a USD statement is returned in USD.

Every rejection (413 ``request_too_large`` or ``file_too_large``, 415 ``not_a_pdf``, 422
``pdf_encrypted``, ``pdf_unreadable``, ``too_many_pages``,
``commit_requires_ticker_and_period`` or ``commit_requires_kes``, 409 ``currency_conflict``,
404 ``unknown_ticker``, 429 ``ingest_busy``) is a coded ``schemas.common.ErrorDetail`` body that
carries no extraction, so none is modelled here.
"""

import uuid
from decimal import Decimal
from typing import Final, Literal

from pydantic import Field

from opennse_terminal.config import DEFAULT_CURRENCY
from opennse_terminal.schemas.common import TerminalResponseModel

__all__ = [
    "ALL_LINE_ITEMS",
    "Candidate",
    "DerivationEntry",
    "DocumentType",
    "ExtractedItem",
    "ExtractedItemStatus",
    "FilingStatus",
    "IngestPdfResponse",
    "IngestWarning",
    "ItemUnit",
    "LineItemName",
    "PeriodType",
]


# ---------------------------------------------------------------------------------------------
# Literals and constants
# ---------------------------------------------------------------------------------------------

LineItemName = Literal[
    "revenue",
    "ebitda",
    "net_income",
    "total_debt",
    "operating_cash_flow",
    "shares_outstanding",
]
"""The six line items the PDF parser can extract, each committed to the ``terminal_fundamentals``
column ``<item>_kes`` (``shares_outstanding`` to ``shares_outstanding``)."""

ALL_LINE_ITEMS: Final[tuple[LineItemName, ...]] = (
    "revenue",
    "ebitda",
    "net_income",
    "total_debt",
    "operating_cash_flow",
    "shares_outstanding",
)
"""Every :data:`LineItemName`, in declaration order: the default of the repeatable ``items``
form field."""

DocumentType = Literal["annual_report", "interim_report", "prospectus", "other"]
"""Kind of uploaded document, as allowed by the ``terminal_corporate_filings.document_type``
CHECK constraint."""

PeriodType = Literal["FY", "H1"]
"""Reporting period of a filing: full financial year or first half, as allowed by the
``terminal_fundamentals.period_type`` CHECK constraint."""

ExtractedItemStatus = Literal["found", "derived", "ambiguous", "not_found"]
"""Outcome for one line item: ``found`` on a matched label, ``derived`` from component lines,
``ambiguous`` when the column or candidate evidence is insufficient, ``not_found`` otherwise."""

ItemUnit = Literal["KES", "USD", "shares"]
"""Unit of an extracted value: the filing's currency for monetary items, ``shares`` for
``shares_outstanding``."""

FilingStatus = Literal["extracted", "partial", "no_items"]
"""Overall extraction outcome, as allowed by the ``terminal_corporate_filings.status`` CHECK
constraint: no requested item ``not_found``, some ``not_found``, or all ``not_found``."""

IngestWarning = Literal["no_text_layer", "scale_not_detected", "non_kes_currency"]
"""Documented codes of :attr:`IngestPdfResponse.warnings`. The field itself is ``list[str]``,
like every ``flags`` field, so a new code never breaks an older client's parsing."""


# ---------------------------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------------------------


class DerivationEntry(TerminalResponseModel):
    """One statement line that a ``derived`` item was summed from: operating profit and
    depreciation and amortisation for EBITDA, or one current and one non-current borrowings
    component for total debt."""

    label: str = Field(description="Statement label of the component line, as printed.")
    page_number: int = Field(description="1-based PDF page the component line was read from.")
    value: Decimal = Field(
        description=(
            "Component value after scale normalization (source token times the page scale), "
            "as a decimal string in the filing's currency."
        ),
    )


class Candidate(TerminalResponseModel):
    """One conflicting match of an ``ambiguous`` item; at most five are returned per item."""

    label: str = Field(description="Statement label of the matching line, as printed.")
    page_number: int = Field(description="1-based PDF page of the matching line.")
    raw_value: str = Field(
        description=(
            "The numeric token exactly as printed in the PDF text layer, for example "
            "'(1,234)' or '1,234.5'."
        ),
    )
    value: Decimal | None = Field(
        description=(
            "The token after sign handling and scale normalization, as a decimal string; "
            "null when it could not be read as a number."
        ),
    )


class ExtractedItem(TerminalResponseModel):
    """Extraction result for one requested line item of an uploaded PDF filing.

    Only ``found`` and ``derived`` items are ever committed to ``terminal_fundamentals``; an
    ``ambiguous`` item is reported with its candidates but never written.
    """

    item: LineItemName = Field(description="Requested line item.")
    status: ExtractedItemStatus = Field(
        description=(
            "found on a matched label, derived from component lines, ambiguous when the "
            "evidence is insufficient (never committed), or not_found."
        ),
    )
    value: Decimal | None = Field(
        description=(
            "Value after scale normalization as a decimal string: whole units of the filing's "
            "currency for monetary items, a share count for shares_outstanding. For ambiguous, "
            "the first match on the primary statement page; null for not_found."
        ),
    )
    unit: ItemUnit = Field(
        description=(
            "KES or USD (the filing's currency, never converted) for monetary items; shares for "
            "shares_outstanding."
        ),
    )
    raw_value: str | None = Field(
        description=(
            "The selected numeric token exactly as printed, before sign and scale handling; "
            "null when no single token was selected, as for not_found items or a derived sum."
        ),
    )
    scale_applied: int | None = Field(
        description=(
            "Multiplier applied to the printed figures: 1, 1000, 1000000 or 1000000000; null "
            "for not_found items."
        ),
    )
    page_number: int | None = Field(
        description="1-based PDF page of the selected line; null for not_found.",
    )
    matched_label: str | None = Field(
        description="Statement label that matched, as printed; null for not_found.",
    )
    derivation: list[DerivationEntry] = Field(
        default_factory=list,
        description="Component lines summed into a derived value; empty for other statuses.",
    )
    candidates: list[Candidate] = Field(
        default_factory=list,
        description="Up to five conflicting matches of an ambiguous item; empty otherwise.",
    )


class IngestPdfResponse(TerminalResponseModel):
    """HTTP 200 body of ``POST /ingest/pdf``, with or without ``commit``.

    A ``commit=false`` preview always returns the full extraction, even for a USD statement.
    With ``commit=true`` the ``found`` and ``derived`` items are merged into the
    ``terminal_fundamentals`` row for the ticker and period, and ``fundamentals_id`` names it.
    """

    filing_id: uuid.UUID = Field(
        description="The terminal_corporate_filings row, upserted on file_sha256 by every upload.",
    )
    file_sha256: str = Field(
        min_length=64,
        max_length=64,
        description=(
            "SHA-256 of the uploaded bytes as lowercase hex; the raw file is never stored."
        ),
    )
    page_count: int = Field(description="Number of pages in the uploaded PDF.")
    detected_scale: int | None = Field(
        description=(
            "Statement scale detected from the page headers (1, 1000, 1000000 or 1000000000), "
            "the most frequent across pages; 1 with the scale_not_detected warning when no page "
            "carries a marker; null when the PDF has no text layer."
        ),
    )
    currency: str = Field(
        default=DEFAULT_CURRENCY,
        description=(
            "ISO 4217 currency of the statements, KES unless a USD or US$ header was detected; "
            "values are never converted."
        ),
    )
    status: FilingStatus = Field(
        description=(
            "extracted when no requested item is not_found, partial when some are, no_items "
            "when all are."
        ),
    )
    items: list[ExtractedItem] = Field(
        description="One result per requested line item.",
    )
    missing_items: list[LineItemName] = Field(
        description="The requested items whose status is not_found.",
    )
    committed: bool = Field(
        description="True when the found and derived items were written to terminal_fundamentals.",
    )
    fundamentals_id: uuid.UUID | None = Field(
        description="The terminal_fundamentals row written by the commit; null when not committed.",
    )
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Extraction warning codes: no_text_layer, scale_not_detected, non_kes_currency."
        ),
    )
