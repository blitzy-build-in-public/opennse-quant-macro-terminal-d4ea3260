"""Tests for the PDF line-item extractor (``ingestion.files.pdf_line_items``).

Every PDF is written in the test by ``helpers.pdf_builder``: minimal PDF 1.4 text documents that
draw each cell at a given x-position, so statement column layouts are reproduced without
committing or downloading any third-party report. Hostile documents (decompression bombs,
operator floods, lying cross-reference streams, page trees without pages) are written by
``raw_pdf`` below, compressed in the test so each file stays small. The extractor reads them with
pdfplumber; no OCR is involved. Expected values are literals taken from the extraction contract
(scale markers, column evidence, the debt rule, EBITDA derivation, share counts, statuses and
warnings), never recomputed with the code under test. Numbers are exact ``Decimal`` values
throughout.

Extraction cases call the in-process ``_extract``; rejections, the parse limits and a full round
trip go through ``extract_line_items`` and its parser process. Nothing here touches the network,
a database or the settings.
"""

import functools
import hashlib
import logging
import subprocess
import sys
import threading
import time
import zlib
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Any

import pdfplumber
import pytest
from pdfplumber.utils.exceptions import PdfminerException

from helpers.pdf_builder import Page, StatementRow, TextCell, build_pdf, statement_page, text_page
from opennse_terminal.ingestion.files import pdf_line_items
from opennse_terminal.ingestion.files.pdf_line_items import (
    LINE_ITEMS,
    Candidate,
    DerivationPart,
    ExtractedLineItem,
    ExtractionResult,
    ParseCancelledError,
    PdfEncrypted,
    PdfParseTimeout,
    PdfUnreadable,
    TooManyPages,
    extract_line_items,
)

# Column layouts. Columns sit at least 70 pt apart, wider than any value drawn under them, so
# each token overlaps exactly the header it is printed under.
NOTE_COLS = [("Note", 300.0), ("2025", 400.0), ("2024", 480.0)]
YEAR_COLS = [("2025", 400.0), ("2024", 480.0)]
PRIOR_FIRST_COLS = [("2024", 400.0), ("2025", 480.0)]
# Value positions without any header token: the "numbers without a header row" layout.
NO_HEADER_COLS = [("", 400.0), ("", 480.0)]
THOUSANDS = "KShs '000"

INCOME_TITLE = "Statement of profit or loss"
POSITION_TITLE = "Statement of financial position"
CASH_FLOW_TITLE = "Statement of cash flows"
NOTES_TITLE = "Notes to the financial statements"


def row(label: str, *values: str) -> StatementRow:
    return StatementRow(label, values)


def statement(
    title: str,
    rows: Sequence[StatementRow],
    *,
    scale_note: str | None = THOUSANDS,
    columns: Sequence[tuple[str, float]] = NOTE_COLS,
    header_above: Sequence[tuple[str, float]] = (),
) -> Page:
    return statement_page(
        title=title,
        scale_note=scale_note,
        columns=columns,
        header_above=header_above,
        rows=rows,
    )


def income_page(
    rows: Sequence[StatementRow],
    *,
    scale_note: str | None = THOUSANDS,
    columns: Sequence[tuple[str, float]] = NOTE_COLS,
    header_above: Sequence[tuple[str, float]] = (),
) -> Page:
    return statement(
        INCOME_TITLE, rows, scale_note=scale_note, columns=columns, header_above=header_above
    )


def position_page(
    rows: Sequence[StatementRow],
    *,
    scale_note: str | None = THOUSANDS,
    columns: Sequence[tuple[str, float]] = NOTE_COLS,
) -> Page:
    return statement(POSITION_TITLE, rows, scale_note=scale_note, columns=columns)


def cashflow_page(
    rows: Sequence[StatementRow],
    *,
    scale_note: str | None = THOUSANDS,
    columns: Sequence[tuple[str, float]] = NOTE_COLS,
) -> Page:
    return statement(CASH_FLOW_TITLE, rows, scale_note=scale_note, columns=columns)


def extract(
    *pages: Page,
    items: Sequence[str] = LINE_ITEMS,
    period_end_date: date | None = None,
) -> ExtractionResult:
    """``extract_line_items`` without its parser process: the same validation and extraction,
    run in this process so a case can observe or patch the module's internals."""
    requested = pdf_line_items._requested_items(items)
    return pdf_line_items._extract(build_pdf(pages), requested, period_end_date, 400)


def only_item(result: ExtractionResult) -> ExtractedLineItem:
    assert len(result.items) == 1
    return result.items[0]


def item_named(result: ExtractionResult, name: str) -> ExtractedLineItem:
    return next(item for item in result.items if item.item == name)


def raw_candidates(item: ExtractedLineItem) -> list[str]:
    return [candidate.raw_value for candidate in item.candidates]


# ---------------------------------------------------------------------------------------------
# Scale, sign and tokenizing
# ---------------------------------------------------------------------------------------------


def test_thousands_statement_reads_current_column_and_discards_note() -> None:
    result = extract(
        income_page([row("Revenue", "5", "1,234,567", "1,100,000")]), items=["revenue"]
    )

    assert only_item(result) == ExtractedLineItem(
        item="revenue",
        status="found",
        value=Decimal("1234567000"),
        unit="KES",
        raw_value="1,234,567",
        scale_applied=1000,
        page_number=1,
        matched_label="Revenue",
    )
    assert result.detected_scale == 1000
    assert result.currency == "KES"
    assert result.status == "extracted"
    assert result.missing_items == []
    assert result.warnings == []
    assert result.page_count == 1


def test_million_statement_keeps_a_genuine_amount_below_100() -> None:
    result = extract(
        income_page([row("Revenue", "5", "87", "80")], scale_note="KShs million"),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal("87000000")
    assert revenue.raw_value == "87"
    assert revenue.scale_applied == 1_000_000
    assert result.detected_scale == 1_000_000


def test_parenthesized_loss_is_negative() -> None:
    result = extract(
        income_page([row("Profit for the year", "", "(12,345)", "20,000")]), items=["net_income"]
    )

    net_income = only_item(result)
    assert net_income.status == "found"
    assert net_income.value == Decimal("-12345000")
    assert net_income.raw_value == "(12,345)"


@pytest.mark.parametrize("dash", ["-", "\u2013", "\u2014"], ids=["hyphen", "en_dash", "em_dash"])
def test_lone_dash_reads_as_zero(dash: str) -> None:
    result = extract(income_page([row("Revenue", "", dash, "1,100,000")]), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal(0)
    assert revenue.raw_value == dash


def test_minus_zero_reads_as_a_positive_zero() -> None:
    result = extract(income_page([row("Revenue", "-0", "4")], columns=YEAR_COLS), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.raw_value == "-0"
    assert revenue.scale_applied == 1000
    assert revenue.value == Decimal(0)
    # Decimal("-0") == 0 too; only its string form, "-0", tells the sign apart.
    assert str(revenue.value) == "0"


def test_decimal_amount_is_scaled_exactly() -> None:
    result = extract(
        income_page([row("Revenue", "", "1,234.5", "1,100")], scale_note="KShs million"),
        items=["revenue"],
    )

    assert only_item(result).value == Decimal("1234500000")


def test_amount_beyond_28_significant_digits_is_scaled_exactly() -> None:
    # 29 significant digits printed; the default decimal context would round the product to 28.
    result = extract(
        text_page([INCOME_TITLE, THOUSANDS, "Revenue 1,234,567,890.1234567890123456789"]),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.raw_value == "1,234,567,890.1234567890123456789"
    assert revenue.scale_applied == 1000
    assert revenue.value == Decimal("1234567890123.4567890123456789")
    assert str(revenue.value) == "1234567890123.4567890123456789"


# terminal_fundamentals money columns are NUMERIC(24,2): a value must round, at 2 places, to a
# magnitude below 10^22.
@pytest.mark.parametrize(
    ("raw", "status", "value"),
    [
        ("9999999999999999999999.99", "found", "9999999999999999999999.99"),
        ("9999999999999999999999.994", "found", "9999999999999999999999.994"),
        ("9999999999999999999999.995", "ambiguous", None),
        ("10000000000000000000000", "ambiguous", None),
        ("(10000000000000000000000)", "ambiguous", None),
        ("1234567890123456789012345678901234567890", "ambiguous", None),
    ],
    ids=[
        "largest_cents",
        "rounds_down_to_the_largest_cents",
        "rounds_up_to_ten_to_the_22",
        "ten_to_the_22",
        "negative_ten_to_the_22",
        "forty_digits",
    ],
)
def test_monetary_value_must_fit_the_money_columns(
    raw: str, status: str, value: str | None
) -> None:
    result = extract(text_page(["Notes", f"Revenue {raw}"]), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == status
    assert revenue.raw_value == raw
    if value is None:
        assert revenue.value is None
        assert revenue.candidates == [Candidate("Revenue", 1, raw, None)]
    else:
        assert revenue.value == Decimal(value)
        assert str(revenue.value) == value
        assert revenue.candidates == []


@pytest.mark.parametrize(
    ("scale_note", "scale", "value", "currency"),
    [
        ("KShs '000", 1_000, Decimal("87000"), "KES"),
        ("KShs \u2019000", 1_000, Decimal("87000"), "KES"),
        ("Shs'000", 1_000, Decimal("87000"), "KES"),
        ("Ksh '000", 1_000, Decimal("87000"), "KES"),
        ("KES 000", 1_000, Decimal("87000"), "KES"),
        ("In thousands of Kenya shillings", 1_000, Decimal("87000"), "KES"),
        ("KShs million", 1_000_000, Decimal("87000000"), "KES"),
        ("KShs mn", 1_000_000, Decimal("87000000"), "KES"),
        ("Shs m", 1_000_000, Decimal("87000000"), "KES"),
        ("KES millions", 1_000_000, Decimal("87000000"), "KES"),
        ("In millions of Kenya shillings", 1_000_000, Decimal("87000000"), "KES"),
        ("KShs billion", 1_000_000_000, Decimal("87000000000"), "KES"),
        ("KShs bn", 1_000_000_000, Decimal("87000000000"), "KES"),
        ("USD '000", 1_000, Decimal("87000"), "USD"),
        ("US$ million", 1_000_000, Decimal("87000000"), "USD"),
        ("All amounts in USD, in thousands", 1_000, Decimal("87000"), "USD"),
    ],
)
def test_scale_and_currency_markers(
    scale_note: str, scale: int, value: Decimal, currency: str
) -> None:
    result = extract(
        income_page([row("Revenue", "", "87", "80")], scale_note=scale_note), items=["revenue"]
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == value
    assert revenue.scale_applied == scale
    assert revenue.unit == currency
    assert result.detected_scale == scale
    assert result.currency == currency
    assert result.warnings == (["non_kes_currency"] if currency == "USD" else [])


def test_scale_marker_printed_under_the_year_headers() -> None:
    # "Shs'000" under each year, the layout of many Kenyan statements: the units line directly
    # below the column-header row still belongs to the page header.
    page = Page(
        [
            TextCell(INCOME_TITLE, 50.0, 780.0),
            TextCell("2025", 400.0, 764.0),
            TextCell("2024", 480.0, 764.0),
            TextCell("Shs'000", 400.0, 748.0),
            TextCell("Shs'000", 480.0, 748.0),
            TextCell("Revenue", 50.0, 732.0),
            TextCell("1,234,567", 400.0, 732.0),
            TextCell("1,100,000", 480.0, 732.0),
        ]
    )

    result = extract(page, items=["revenue"])

    assert only_item(result).value == Decimal("1234567000")
    assert result.detected_scale == 1000
    assert result.warnings == []


@pytest.mark.parametrize(
    "rows",
    [
        [row("Revenue", "", "1,234", "1,100")],
        [row("Revenue", "", "1,234", "1,100"), row("Amounts in KShs '000", "", "", "")],
    ],
    ids=["no_marker", "marker_below_the_header_row"],
)
def test_page_without_scale_marker_reads_units_and_warns(rows: list[StatementRow]) -> None:
    result = extract(income_page(rows, scale_note=None), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal("1234")
    assert revenue.scale_applied == 1
    assert result.detected_scale == 1
    assert result.warnings == ["scale_not_detected"]


@pytest.mark.parametrize(
    ("markers", "scale", "value"),
    [
        (["KShs '000", "KShs '000"], 1_000, Decimal("45000")),
        (["KShs million", "KShs '000"], 1_000_000, Decimal("45000000")),
        (["KShs million", "KShs '000", "KShs '000"], 1_000, Decimal("45000")),
    ],
    ids=["single_marker", "tie_goes_to_earliest_page", "most_frequent_marker"],
)
def test_page_without_marker_inherits_the_document_scale(
    markers: list[str], scale: int, value: Decimal
) -> None:
    marked = [text_page([NOTES_TITLE, marker]) for marker in markers]
    unmarked = cashflow_page(
        [row("Net cash generated from operating activities", "", "45", "40")], scale_note=None
    )

    result = extract(*marked, unmarked, items=["operating_cash_flow"])

    cash_flow = only_item(result)
    assert cash_flow.value == value
    assert cash_flow.scale_applied == scale
    assert cash_flow.page_number == len(markers) + 1
    assert result.detected_scale == scale
    assert result.warnings == []


def test_scale_words_in_narrative_prose_do_not_set_the_document_scale() -> None:
    result = extract(
        text_page(["Chairman's statement", "Revenue grew to KShs 310 billion this year, up 12%."]),
        text_page(["Directors' report", "The group recorded a profit of KShs 42 billion."]),
        income_page(
            [row("Revenue", "310,000", "277,000")], scale_note="KShs million", columns=YEAR_COLS
        ),
        position_page(
            [row("Total borrowings", "90,000", "80,000")], scale_note=None, columns=YEAR_COLS
        ),
        cashflow_page(
            [row("Net cash generated from operating activities", "50,000", "40,000")],
            scale_note="KShs million",
            columns=YEAR_COLS,
        ),
        items=["revenue", "total_debt", "operating_cash_flow"],
    )

    assert item_named(result, "total_debt") == ExtractedLineItem(
        item="total_debt",
        status="found",
        value=Decimal("90000000000"),
        unit="KES",
        raw_value="90,000",
        scale_applied=1_000_000,
        page_number=4,
        matched_label="Total borrowings",
    )
    assert item_named(result, "revenue").value == Decimal("310000000000")
    assert item_named(result, "operating_cash_flow").value == Decimal("50000000000")
    assert result.detected_scale == 1_000_000
    assert result.warnings == []


@pytest.mark.parametrize(
    ("lines", "scale", "value", "warnings"),
    [
        (
            ["Sustainability report", "We reached customers in thousands of villages."],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            ["Financial highlights", "Revenue (KShs bn) 310"],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            [
                "Chairman's statement",
                "The statement of financial position remains strong at KShs 90 billion.",
            ],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            [NOTES_TITLE, "Amounts in KShs million"],
            1_000_000,
            Decimal("310000000000000"),
            [],
        ),
        (
            [NOTES_TITLE, "All figures are in KShs '000, unless otherwise stated"],
            1_000,
            Decimal("310000000000"),
            [],
        ),
        (
            ["Summary financial information (KShs million)"],
            1_000_000,
            Decimal("310000000000000"),
            [],
        ),
        (
            [NOTES_TITLE, "Our customers live in thousands of villages.", "Figures in KShs mn"],
            1_000_000,
            Decimal("310000000000000"),
            [],
        ),
        (
            ["Sustainability report", "We measure customers (in thousands), not revenue."],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            ["Sustainability report", "Our customer base (in millions) continues to grow."],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            ["Financial highlights (in millions)"],
            1,
            Decimal("310000000"),
            ["scale_not_detected"],
        ),
        (
            ["Five-year review (in millions of Kenya shillings)"],
            1_000_000,
            Decimal("310000000000000"),
            [],
        ),
    ],
    ids=[
        "prose",
        "data_line_with_a_unit_label",
        "prose_opening_with_a_statement_title",
        "caption_with_lead_in_words",
        "caption_with_punctuation",
        "parenthesized_caption",
        "caption_after_prose",
        "parenthesized_unit_inside_a_sentence",
        "parenthesized_unit_mid_sentence",
        "parenthesized_unit_without_a_currency",
        "parenthesized_caption_naming_shillings",
    ],
)
def test_only_a_unit_caption_sets_the_scale_of_a_page_that_is_not_a_statement(
    lines: list[str], scale: int, value: Decimal, warnings: list[str]
) -> None:
    result = extract(
        text_page(lines),
        income_page(
            [row("Revenue", "310,000,000", "277,000,000")], scale_note=None, columns=YEAR_COLS
        ),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == value
    assert revenue.scale_applied == scale
    assert result.detected_scale == scale
    assert result.warnings == warnings


# ---------------------------------------------------------------------------------------------
# Currency
# ---------------------------------------------------------------------------------------------


def test_usd_statement_is_returned_in_usd_without_conversion() -> None:
    result = extract(
        income_page([row("Revenue", "5", "1,234,567", "1,100,000")], scale_note="USD '000"),
        text_page([NOTES_TITLE, "Number of ordinary shares in issue 40,065,428,000"]),
        items=["revenue", "shares_outstanding"],
    )

    revenue = item_named(result, "revenue")
    shares = item_named(result, "shares_outstanding")
    assert result.currency == "USD"
    assert result.warnings == ["non_kes_currency"]
    assert revenue.unit == "USD"
    assert revenue.value == Decimal("1234567000")
    assert shares.unit == "shares"
    assert shares.value == Decimal("40065428000")


@pytest.mark.parametrize(
    ("pages", "currency", "warnings", "value"),
    [
        (
            [
                income_page([row("Revenue", "5", "1,234", "1,100")]),
                text_page([NOTES_TITLE, "USD 500 million bond due 2030"]),
            ],
            "KES",
            [],
            Decimal("1234000"),
        ),
        (
            [text_page(["Prospectus summary (USD '000)", "Revenue 1,234"])],
            "USD",
            ["non_kes_currency"],
            Decimal("1234000"),
        ),
    ],
    ids=["usd_on_a_notes_page_of_a_kes_filing", "usd_header_without_statement_pages"],
)
def test_currency_is_read_from_statement_headers(
    pages: list[Page], currency: str, warnings: list[str], value: Decimal
) -> None:
    result = extract(*pages, items=["revenue"])

    revenue = only_item(result)
    assert result.currency == currency
    assert result.warnings == warnings
    assert revenue.unit == currency
    assert revenue.value == value


# ---------------------------------------------------------------------------------------------
# Column selection
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("columns", "values", "period_end_date", "value"),
    [
        (
            PRIOR_FIRST_COLS,
            ("1,100,000", "1,234,567"),
            date(2025, 12, 31),
            Decimal("1234567000"),
        ),
        (PRIOR_FIRST_COLS, ("1,100,000", "1,234,567"), None, Decimal("1234567000")),
        (
            PRIOR_FIRST_COLS,
            ("1,100,000", "1,234,567"),
            date(2024, 12, 31),
            Decimal("1100000000"),
        ),
        (YEAR_COLS, ("1,234,567", "1,100,000"), date(2024, 6, 30), Decimal("1100000000")),
    ],
    ids=["period_year", "latest_year_by_default", "prior_year_requested", "current_first"],
)
def test_period_column_is_chosen_by_year_never_by_position(
    columns: list[tuple[str, float]],
    values: tuple[str, str],
    period_end_date: date | None,
    value: Decimal,
) -> None:
    result = extract(
        income_page([row("Revenue", *values)], columns=columns),
        items=["revenue"],
        period_end_date=period_end_date,
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == value


def test_period_year_without_a_column_is_ambiguous() -> None:
    result = extract(
        income_page([row("Revenue", "1,100,000", "1,234,567")], columns=PRIOR_FIRST_COLS),
        items=["revenue"],
        period_end_date=date(2023, 12, 31),
    )

    revenue = only_item(result)
    assert revenue.status == "ambiguous"
    assert revenue.value is None
    assert raw_candidates(revenue) == ["1,100,000", "1,234,567"]


@pytest.mark.parametrize("period_end_date", [None, date(2025, 12, 31)], ids=["default", "dated"])
def test_current_and_prior_period_labels(period_end_date: date | None) -> None:
    result = extract(
        income_page(
            [row("Revenue", "1,100", "1,234")], columns=[("Prior", 400.0), ("Current", 480.0)]
        ),
        items=["revenue"],
        period_end_date=period_end_date,
    )

    assert only_item(result).value == Decimal("1234000")


GROUP_COMPANY_COLS = [("2025", 300.0), ("2024", 370.0), ("2025", 440.0), ("2024", 510.0)]
GROUP_COMPANY_VALUES = ("1,234,567", "1,100,000", "934,000", "880,000")


@pytest.mark.parametrize(
    ("columns", "header_above", "values", "period_end_date", "value"),
    [
        (
            GROUP_COMPANY_COLS,
            [("Group", 300.0), ("Company", 440.0)],
            GROUP_COMPANY_VALUES,
            None,
            Decimal("1234567000"),
        ),
        (
            GROUP_COMPANY_COLS,
            [("Group", 300.0), ("Company", 440.0)],
            GROUP_COMPANY_VALUES,
            date(2024, 12, 31),
            Decimal("1100000000"),
        ),
        (
            GROUP_COMPANY_COLS,
            [("Group", 300.0), ("Group", 370.0), ("Company", 440.0), ("Company", 510.0)],
            GROUP_COMPANY_VALUES,
            None,
            Decimal("1234567000"),
        ),
        (
            GROUP_COMPANY_COLS,
            [("Consolidated", 300.0), ("Company", 440.0)],
            GROUP_COMPANY_VALUES,
            None,
            Decimal("1234567000"),
        ),
        (
            # Entities centred above the year pairs, overlapping neither year.
            GROUP_COMPANY_COLS,
            [("Group", 330.0), ("Company", 465.0)],
            GROUP_COMPANY_VALUES,
            None,
            Decimal("1234567000"),
        ),
        (
            # Entities on the header row itself, each to the left of its years.
            [
                ("Group", 180.0),
                ("2025", 240.0),
                ("2024", 300.0),
                ("Company", 360.0),
                ("2025", 430.0),
                ("2024", 500.0),
            ],
            [],
            ("", "1,234,567", "1,100,000", "", "934,000", "880,000"),
            None,
            Decimal("1234567000"),
        ),
    ],
    ids=[
        "entities_above_years",
        "entities_above_years_prior_period",
        "entity_above_every_year",
        "consolidated_is_group",
        "entities_centred_between_years",
        "entities_on_the_header_row",
    ],
)
def test_group_column_is_preferred_over_company(
    columns: list[tuple[str, float]],
    header_above: list[tuple[str, float]],
    values: tuple[str, ...],
    period_end_date: date | None,
    value: Decimal,
) -> None:
    result = extract(
        income_page([row("Revenue", *values)], columns=columns, header_above=header_above),
        items=["revenue"],
        period_end_date=period_end_date,
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == value


@pytest.mark.parametrize(
    ("columns", "header_above"),
    [
        ([("2025", 400.0), ("2025", 480.0)], []),
        ([("2025", 400.0), ("", 480.0)], []),
        ([("", 300.0), ("2025", 400.0)], []),
        ([("", 300.0), ("", 440.0)], [("Group", 300.0), ("Company", 440.0)]),
    ],
    ids=[
        "two_columns_of_the_same_year",
        "token_outside_every_header_column",
        "note_reference_without_a_note_header",
        "group_and_company_without_period_headers",
    ],
)
def test_insufficient_column_evidence_is_ambiguous(
    columns: list[tuple[str, float]], header_above: list[tuple[str, float]]
) -> None:
    result = extract(
        income_page([row("Revenue", "5", "999")], columns=columns, header_above=header_above),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "ambiguous"
    assert revenue.value is None
    assert revenue.raw_value is None
    assert raw_candidates(revenue) == ["5", "999"]
    # An ambiguous item is neither found nor missing.
    assert result.status == "extracted"
    assert result.missing_items == []


@pytest.mark.parametrize(
    ("columns", "header_above", "values"),
    [
        (NOTE_COLS, [], ("5", "87", "80")),
        ([("Notes", 300.0), ("2025", 400.0), ("2024", 480.0)], [], ("5", "87", "80")),
        ([("", 300.0), ("2025", 400.0), ("2024", 480.0)], [("Notes", 300.0)], ("5", "87", "80")),
        (YEAR_COLS, [], ("87", "80")),
    ],
    ids=["note_header", "notes_header", "notes_header_above_the_years", "no_note_column"],
)
def test_note_column_tokens_are_discarded(
    columns: list[tuple[str, float]], header_above: list[tuple[str, float]], values: tuple[str, ...]
) -> None:
    result = extract(
        income_page([row("Revenue", *values)], columns=columns, header_above=header_above),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal("87000")


def test_two_numbers_without_a_header_row_are_ambiguous() -> None:
    result = extract(
        income_page([row("Revenue", "1,234", "1,100")], scale_note=None, columns=NO_HEADER_COLS),
        items=["revenue"],
    )

    assert only_item(result) == ExtractedLineItem(
        item="revenue",
        status="ambiguous",
        value=None,
        unit="KES",
        raw_value=None,
        scale_applied=1,
        page_number=1,
        matched_label="Revenue",
        candidates=[
            Candidate("Revenue", 1, "1,234", Decimal("1234")),
            Candidate("Revenue", 1, "1,100", Decimal("1100")),
        ],
    )
    assert result.warnings == ["scale_not_detected"]


def test_one_number_without_a_header_row_is_found() -> None:
    result = extract(
        income_page([row("Revenue", "1,234", "")], scale_note=None, columns=NO_HEADER_COLS),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal("1234")
    assert revenue.candidates == []


def test_candidates_are_capped_at_five() -> None:
    result = extract(
        income_page(
            [
                row("Revenue", "1", "2"),
                row("Revenue", "3", "4"),
                row("Revenue", "5", "6"),
            ],
            scale_note=None,
            columns=NO_HEADER_COLS,
        ),
        items=["revenue"],
    )

    assert raw_candidates(only_item(result)) == ["1", "2", "3", "4", "5"]


@pytest.mark.parametrize(
    ("columns", "values"),
    [
        (YEAR_COLS, ("(1,234", "1,100")),
        (NO_HEADER_COLS, ("(1,234", "")),
    ],
    ids=["under_the_period_column", "without_a_header_row"],
)
def test_malformed_number_is_offered_as_a_candidate(
    columns: list[tuple[str, float]], values: tuple[str, ...]
) -> None:
    result = extract(income_page([row("Revenue", *values)], columns=columns), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == "ambiguous"
    assert revenue.value is None
    assert revenue.candidates[0] == Candidate("Revenue", 1, "(1,234", None)


def test_each_header_row_is_built_once_for_the_lines_below_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Column evidence is computed once per page: each header row is built once and shared by
    # every line below it up to the next header row, never searched for or rebuilt per matched
    # line. The lower header row reverses the years, so a lower row read against the upper
    # header would give 1,100 instead of 1,234 and make the item ambiguous.
    build_header = pdf_line_items._build_header
    built: list[int] = []

    def counting_build_header(
        lines: Sequence[pdf_line_items._Line], index: int
    ) -> pdf_line_items._Header:
        built.append(index)
        return build_header(lines, index)

    monkeypatch.setattr(pdf_line_items, "_build_header", counting_build_header)
    upper = statement_page(
        title=INCOME_TITLE,
        scale_note=THOUSANDS,
        columns=YEAR_COLS,
        rows=[row("Revenue", "1,234", "1,100")] * 20,
    )
    # The upper section takes 23 lines of 16 pt from 780 (title, scale note, header row and 20
    # rows), so the lower one starts on the next line, at 412.
    lower = statement_page(
        title="Continuing operations",
        scale_note=None,
        columns=PRIOR_FIRST_COLS,
        rows=[row("Revenue", "1,100", "1,234")] * 20,
        top=412.0,
    )
    result = extract(Page((*upper.cells, *lower.cells)), items=["revenue"])

    revenue = only_item(result)
    assert revenue.status == "found"
    assert revenue.value == Decimal("1234000")
    # Line 2 is the upper header row; line 24 the lower one, after the heading on line 23.
    assert built == [2, 24]


# ---------------------------------------------------------------------------------------------
# Labels and statement pages
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("item", "title", "label"),
    [
        ("revenue", INCOME_TITLE, "Revenue"),
        ("revenue", INCOME_TITLE, "Revenue from contracts with customers"),
        ("revenue", INCOME_TITLE, "Total revenue"),
        ("revenue", INCOME_TITLE, "Turnover"),
        ("revenue", INCOME_TITLE, "Sales"),
        ("revenue", INCOME_TITLE, "Total operating income"),
        ("revenue", INCOME_TITLE, "Total income"),
        ("revenue", INCOME_TITLE, "Revenue (restated)"),
        ("revenue", INCOME_TITLE, "Revenue note 5(a)"),
        ("revenue", INCOME_TITLE, "Revenue:"),
        ("ebitda", INCOME_TITLE, "EBITDA"),
        ("ebitda", INCOME_TITLE, "Earnings before interest, tax, depreciation and amortisation"),
        (
            "ebitda",
            CASH_FLOW_TITLE,
            "Earnings before interest, taxes, depreciation and amortization",
        ),
        ("net_income", INCOME_TITLE, "Profit for the year"),
        ("net_income", INCOME_TITLE, "Profit for the period"),
        ("net_income", INCOME_TITLE, "Profit/(loss) for the year"),
        ("net_income", INCOME_TITLE, "(Loss)/profit for the period"),
        ("net_income", INCOME_TITLE, "Profit after tax"),
        ("net_income", INCOME_TITLE, "Profit after taxation"),
        ("net_income", INCOME_TITLE, "Net profit"),
        ("net_income", INCOME_TITLE, "Profit attributable to equity holders of the company"),
        ("net_income", INCOME_TITLE, "Profit attributable to the owners of the parent"),
        ("net_income", INCOME_TITLE, "Net income"),
        ("total_debt", POSITION_TITLE, "Total borrowings"),
        ("total_debt", POSITION_TITLE, "Loans and borrowings"),
        ("total_debt", POSITION_TITLE, "Total debt"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash generated from operating activities"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash from operating activities"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash used in operating activities"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash flows from operating activities"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash generated by operating activities"),
        (
            "operating_cash_flow",
            CASH_FLOW_TITLE,
            "Net cash (used in)/generated from operating activities",
        ),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Cash generated from operations"),
        ("shares_outstanding", NOTES_TITLE, "Number of ordinary shares in issue"),
        ("shares_outstanding", NOTES_TITLE, "Ordinary shares in issue"),
        ("shares_outstanding", NOTES_TITLE, "Number of shares"),
        ("shares_outstanding", NOTES_TITLE, "Number of shares in issue"),
        ("shares_outstanding", NOTES_TITLE, "Weighted average number of ordinary shares"),
    ],
)
def test_label_variants(item: str, title: str, label: str) -> None:
    result = extract(
        statement(title, [row(label, "1,234", "1,100")], columns=YEAR_COLS), items=[item]
    )

    extracted = only_item(result)
    assert extracted.status == "found"
    assert extracted.matched_label == label
    # Monetary items carry the page scale; share counts are never scaled by it.
    if item == "shares_outstanding":
        assert extracted.value == Decimal("1234")
        assert extracted.unit == "shares"
        assert extracted.scale_applied == 1
    else:
        assert extracted.value == Decimal("1234000")
        assert extracted.unit == "KES"
        assert extracted.scale_applied == 1000


@pytest.mark.parametrize(
    ("item", "title", "rows", "label", "value"),
    [
        (
            "revenue",
            INCOME_TITLE,
            [row("Total income", "2,000", "1,900"), row("Revenue", "1,234", "1,100")],
            "Revenue",
            Decimal("1234000"),
        ),
        (
            "revenue",
            INCOME_TITLE,
            [row("Cost of sales", "(800)", "(700)"), row("Sales", "1,234", "1,100")],
            "Sales",
            Decimal("1234000"),
        ),
        (
            "revenue",
            INCOME_TITLE,
            [row("Revenue", "", ""), row("Total revenue", "1,234", "1,100")],
            "Total revenue",
            Decimal("1234000"),
        ),
        (
            "net_income",
            INCOME_TITLE,
            [row("Net income", "900", "800"), row("Profit for the year", "1,234", "1,100")],
            "Profit for the year",
            Decimal("1234000"),
        ),
        (
            "total_debt",
            POSITION_TITLE,
            [row("Loans and borrowings", "900", "800"), row("Total borrowings", "1,234", "1,100")],
            "Total borrowings",
            Decimal("1234000"),
        ),
        (
            "operating_cash_flow",
            CASH_FLOW_TITLE,
            [
                row("Cash generated from operations", "900", "800"),
                row("Net cash generated from operating activities", "1,234", "1,100"),
            ],
            "Net cash generated from operating activities",
            Decimal("1234000"),
        ),
        (
            "shares_outstanding",
            NOTES_TITLE,
            [
                row("Weighted average number of ordinary shares", "900", "800"),
                row("Number of ordinary shares in issue", "1,234", "1,100"),
            ],
            "Number of ordinary shares in issue",
            Decimal("1234"),
        ),
    ],
    ids=[
        "revenue_before_total_income",
        "sales_is_not_cost_of_sales",
        "heading_without_values_is_skipped",
        "profit_for_the_year_before_net_income",
        "total_borrowings_before_loans_and_borrowings",
        "net_cash_before_cash_generated",
        "shares_in_issue_before_weighted_average",
    ],
)
def test_highest_priority_label_decides_the_item(
    item: str, title: str, rows: list[StatementRow], label: str, value: Decimal
) -> None:
    result = extract(statement(title, rows, columns=YEAR_COLS), items=[item])

    extracted = only_item(result)
    assert extracted.status == "found"
    assert extracted.matched_label == label
    assert extracted.value == value


@pytest.mark.parametrize(
    ("item", "rows", "status", "value"),
    [
        (
            "net_income",
            [
                row("Profit attributable to owners of the parent per share", "1.23", "1.10"),
                row("Profit attributable to owners of the parent", "500", "450"),
            ],
            "found",
            Decimal("500000"),
        ),
        (
            "net_income",
            [row("Profit attributable to owners of the parent per share", "1.23", "1.10")],
            "not_found",
            None,
        ),
        (
            "revenue",
            [row("Revenue per share", "2.50", "2.10"), row("Revenue", "1,234", "1,100")],
            "found",
            Decimal("1234000"),
        ),
        ("revenue", [row("Revenue per share", "2.50", "2.10")], "not_found", None),
    ],
    ids=[
        "per_share_line_beside_the_total",
        "per_share_line_alone",
        "revenue_per_share_beside_revenue",
        "revenue_per_share_alone",
    ],
)
def test_per_share_labels_are_excluded_for_monetary_items(
    item: str, rows: list[StatementRow], status: str, value: Decimal | None
) -> None:
    result = extract(income_page(rows, columns=YEAR_COLS), items=[item])

    extracted = only_item(result)
    assert extracted.status == status
    assert extracted.value == value


@pytest.mark.parametrize(
    ("item", "title", "label"),
    [
        ("revenue", "Consolidated statement of profit or loss", "Revenue"),
        ("revenue", "Income statement", "Revenue"),
        ("revenue", "Statement of comprehensive income", "Revenue"),
        ("total_debt", POSITION_TITLE, "Total borrowings"),
        ("total_debt", "Consolidated balance sheet", "Total borrowings"),
        ("operating_cash_flow", CASH_FLOW_TITLE, "Net cash from operating activities"),
        ("operating_cash_flow", "Cash flow statement", "Net cash from operating activities"),
    ],
)
def test_items_are_read_from_their_statement_page_not_the_notes(
    item: str, title: str, label: str
) -> None:
    notes = text_page([NOTES_TITLE, f"{label} 999"])
    statement_with_item = statement(title, [row(label, "", "1,234", "1,100")])

    result = extract(notes, statement_with_item, items=[item])

    extracted = only_item(result)
    assert extracted.status == "found"
    assert extracted.value == Decimal("1234000")
    assert extracted.page_number == 2


def test_a_statement_title_mentioned_on_a_notes_page_does_not_classify_it() -> None:
    result = extract(
        cashflow_page(
            [row("Net cash generated from operating activities", "", "120,000", "110,000")]
        ),
        text_page(
            [
                NOTES_TITLE,
                "(c) Statement of cash flows policy",
                "Net cash generated from operating activities 9,999",
            ]
        ),
        items=["operating_cash_flow"],
    )

    cash_flow = only_item(result)
    assert cash_flow.status == "found"
    assert cash_flow.value == Decimal("120000000")
    assert cash_flow.page_number == 1
    assert cash_flow.candidates == []


@pytest.mark.parametrize(
    ("title", "item", "label", "status", "page_number", "candidates"),
    [
        (
            "Consolidated and separate statement of financial position",
            "total_debt",
            "Total borrowings",
            "found",
            2,
            [],
        ),
        (
            "Group statement of cash flows",
            "operating_cash_flow",
            "Net cash from operating activities",
            "found",
            2,
            [],
        ),
        (
            "Interim condensed statement of profit or loss for the period ended 30 June 2025",
            "revenue",
            "Revenue",
            "found",
            2,
            [],
        ),
        (
            "Group and company balance sheets (continued)",
            "total_debt",
            "Total borrowings",
            "found",
            2,
            [],
        ),
        (
            "3. Statement of cash flows",
            "operating_cash_flow",
            "Net cash from operating activities",
            "ambiguous",
            1,
            ["999", "1,234"],
        ),
        (
            "Amounts as shown in the statement of financial position",
            "total_debt",
            "Total borrowings",
            "ambiguous",
            1,
            ["999", "1,234"],
        ),
        (
            "The statement of financial position remains strong",
            "total_debt",
            "Total borrowings",
            "ambiguous",
            1,
            ["999", "1,234"],
        ),
        (
            "Basis of preparation of the income statement",
            "revenue",
            "Revenue",
            "ambiguous",
            1,
            ["999", "1,234"],
        ),
    ],
    ids=[
        "entity_qualifiers",
        "group_qualifier",
        "presentation_qualifiers_and_trailing_text",
        "entities_and_continued",
        "after_a_list_marker",
        "mid_sentence",
        "sentence_opening_with_the_title",
        "after_a_heading",
    ],
)
def test_a_page_is_a_statement_only_when_a_line_opens_with_its_title(
    title: str,
    item: str,
    label: str,
    status: str,
    page_number: int,
    candidates: list[str],
) -> None:
    # A page whose title is a mere mention is no statement page, so every page is searched and
    # the notes page's 999 competes with the page's own 1,234.
    notes = text_page([NOTES_TITLE, f"{label} 999"])
    titled = statement(title, [row(label, "", "1,234", "1,100")])

    result = extract(notes, titled, items=[item])

    extracted = only_item(result)
    assert extracted.status == status
    assert extracted.page_number == page_number
    assert raw_candidates(extracted) == candidates


@pytest.mark.parametrize(
    ("item", "line"),
    [("revenue", "Revenue 1,234"), ("total_debt", "Total borrowings 1,234")],
)
def test_every_page_is_searched_without_a_statement_page(item: str, line: str) -> None:
    result = extract(text_page(["Directors' report", line]), items=[item])

    extracted = only_item(result)
    assert extracted.status == "found"
    assert extracted.value == Decimal("1234")
    assert extracted.page_number == 1
    assert result.warnings == ["scale_not_detected"]


@pytest.mark.parametrize(
    ("second_value", "status", "candidates"),
    [
        ("1,234", "found", []),
        (
            "999",
            "ambiguous",
            [
                Candidate("Revenue", 1, "1,234", Decimal("1234000")),
                Candidate("Revenue", 2, "999", Decimal("999000")),
            ],
        ),
    ],
    ids=["same_value", "different_values"],
)
def test_equal_priority_matches_on_several_pages(
    second_value: str, status: str, candidates: list[Candidate]
) -> None:
    result = extract(
        income_page([row("Revenue", "", "1,234", "1,100")]),
        income_page([row("Revenue", "", second_value, "1,100")]),
        items=["revenue"],
    )

    revenue = only_item(result)
    assert revenue.status == status
    # The reported match is the first one on the primary statement page.
    assert revenue.value == Decimal("1234000")
    assert revenue.page_number == 1
    assert revenue.candidates == candidates


@pytest.mark.parametrize(
    ("cash_flow_value", "status"),
    [("500,000", "found"), ("480,000", "ambiguous")],
    ids=["same_value", "different_values"],
)
def test_primary_page_is_the_first_page_of_the_first_preferred_kind(
    cash_flow_value: str, status: str
) -> None:
    result = extract(
        cashflow_page([row("EBITDA", "", cash_flow_value, "450,000")]),
        income_page([row("EBITDA", "", "500,000", "450,000")]),
        items=["ebitda"],
    )

    ebitda = only_item(result)
    assert ebitda.status == status
    assert ebitda.value == Decimal("500000000")
    assert ebitda.page_number == 2


# ---------------------------------------------------------------------------------------------
# Debt rule
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("Borrowings \u2013 current", "Borrowings \u2013 non-current"),
        ("Current borrowings", "Non-current borrowings"),
        ("Short-term borrowings", "Long-term borrowings"),
        ("Short-term borrowings", "Long-term debt"),
    ],
    ids=["current_and_non_current", "current_first_wording", "short_and_long_term", "long_debt"],
)
def test_debt_components_are_summed(first: str, second: str) -> None:
    result = extract(
        position_page([row(first, "18", "2,000", "1,500"), row(second, "18", "3,000", "3,500")]),
        items=["total_debt"],
    )

    assert only_item(result) == ExtractedLineItem(
        item="total_debt",
        status="derived",
        value=Decimal("5000000"),
        unit="KES",
        raw_value=None,
        scale_applied=1000,
        page_number=1,
        matched_label=None,
        derivation=[
            DerivationPart(first, 1, Decimal("2000000")),
            DerivationPart(second, 1, Decimal("3000000")),
        ],
    )


def test_debt_components_are_summed_exactly() -> None:
    # The 30-digit sum would be rounded to 28 digits by the default decimal context.
    result = extract(
        text_page(
            [
                POSITION_TITLE,
                "Borrowings - current 1234567890123.4567890123456789",
                "Borrowings - non-current 1",
            ]
        ),
        items=["total_debt"],
    )

    debt = only_item(result)
    assert debt.status == "derived"
    assert debt.value == Decimal("1234567890124.4567890123456789")
    assert str(debt.value) == "1234567890124.4567890123456789"
    assert debt.derivation == [
        DerivationPart("Borrowings - current", 1, Decimal("1234567890123.4567890123456789")),
        DerivationPart("Borrowings - non-current", 1, Decimal("1")),
    ]


@pytest.mark.parametrize(
    ("current", "non_current", "value", "candidates"),
    [
        (
            "6000000000000000000000",
            "6000000000000000000000",
            Decimal("6000000000000000000000"),
            [Decimal("6000000000000000000000"), Decimal("6000000000000000000000")],
        ),
        ("10000000000000000000000", "1", None, [None, Decimal("1")]),
    ],
    ids=["sum_beyond_the_money_columns", "component_beyond_the_money_columns"],
)
def test_debt_beyond_the_money_columns_is_ambiguous(
    current: str, non_current: str, value: Decimal | None, candidates: list[Decimal | None]
) -> None:
    result = extract(
        text_page(
            [
                POSITION_TITLE,
                f"Borrowings - current {current}",
                f"Borrowings - non-current {non_current}",
            ]
        ),
        items=["total_debt"],
    )

    debt = only_item(result)
    assert debt.status == "ambiguous"
    assert debt.derivation == []
    # The reported line is the first debt line on the page.
    assert debt.matched_label == "Borrowings - current"
    assert debt.value == value
    assert debt.candidates == [
        Candidate("Borrowings - current", 1, current, candidates[0]),
        Candidate("Borrowings - non-current", 1, non_current, candidates[1]),
    ]


def test_debt_total_line_alone_is_used_beside_components() -> None:
    result = extract(
        position_page(
            [
                row("Borrowings \u2013 current", "18", "2,000", "1,500"),
                row("Borrowings \u2013 non-current", "18", "3,000", "3,500"),
                row("Total borrowings", "18", "5,500", "5,000"),
            ]
        ),
        items=["total_debt"],
    )

    debt = only_item(result)
    assert debt.status == "found"
    assert debt.value == Decimal("5500000")
    assert debt.matched_label == "Total borrowings"
    assert debt.derivation == []


CURRENT = row("Borrowings \u2013 current", "18", "2,000", "1,500")
NON_CURRENT = row("Borrowings \u2013 non-current", "18", "3,000", "3,500")


@pytest.mark.parametrize(
    ("rows", "value", "candidates"),
    [
        (
            [row("Borrowings", "18", "1,000", "900"), CURRENT, NON_CURRENT],
            Decimal("1000000"),
            ["1,000", "2,000", "3,000"],
        ),
        ([CURRENT], Decimal("2000000"), ["2,000"]),
        (
            [CURRENT, NON_CURRENT, row("Bank loans", "18", "1,000", "900")],
            Decimal("2000000"),
            ["2,000", "3,000", "1,000"],
        ),
        (
            [CURRENT, row("Borrowings \u2013 current", "18", "3,000", "3,500")],
            Decimal("2000000"),
            ["2,000", "3,000"],
        ),
        (
            [CURRENT, row("Long-term borrowings", "18", "3,000", "3,500")],
            Decimal("2000000"),
            ["2,000", "3,000"],
        ),
        (
            [row("Borrowings \u2013 current", "18", "(2,000", "1,500"), NON_CURRENT],
            None,
            ["(2,000", "1,500", "3,000"],
        ),
    ],
    ids=[
        "generic_borrowings_beside_components",
        "lone_component",
        "more_than_two_components",
        "repeated_component_type",
        "unpaired_component_types",
        "unread_component",
    ],
)
def test_debt_components_without_a_valid_pair_are_ambiguous(
    rows: list[StatementRow], value: Decimal | None, candidates: list[str]
) -> None:
    result = extract(position_page(rows), items=["total_debt"])

    debt = only_item(result)
    assert debt.status == "ambiguous"
    assert debt.derivation == []
    # The reported line is the first debt line on the page.
    assert debt.matched_label == rows[0].label
    assert debt.value == value
    assert raw_candidates(debt) == candidates


@pytest.mark.parametrize(
    ("second_total", "status"),
    [("5,500", "found"), ("4,000", "ambiguous")],
    ids=["same_total", "different_totals"],
)
def test_debt_totals_on_several_position_pages(second_total: str, status: str) -> None:
    result = extract(
        position_page([row("Total borrowings", "18", "5,500", "5,000")]),
        position_page([row("Total borrowings", "18", second_total, "5,000")]),
        items=["total_debt"],
    )

    debt = only_item(result)
    assert debt.status == status
    assert debt.value == Decimal("5500000")
    assert debt.page_number == 1
    expected = [] if status == "found" else ["5,500", second_total]
    assert raw_candidates(debt) == expected


def test_debt_without_any_debt_line_is_not_found() -> None:
    result = extract(
        position_page([row("Trade payables", "19", "700", "650")]), items=["total_debt"]
    )

    assert only_item(result).status == "not_found"


# ---------------------------------------------------------------------------------------------
# EBITDA
# ---------------------------------------------------------------------------------------------


def test_ebitda_is_derived_from_operating_profit_and_depreciation_and_amortisation() -> None:
    result = extract(
        income_page(
            [
                row("Operating profit", "6", "400,000", "350,000"),
                row("Depreciation and amortisation", "7", "(50,000)", "(45,000)"),
            ]
        ),
        items=["ebitda"],
    )

    assert only_item(result) == ExtractedLineItem(
        item="ebitda",
        status="derived",
        value=Decimal("450000000"),
        unit="KES",
        raw_value=None,
        scale_applied=1000,
        page_number=1,
        matched_label=None,
        derivation=[
            DerivationPart("Operating profit", 1, Decimal("400000000")),
            DerivationPart("Depreciation and amortisation", 1, Decimal("50000000")),
        ],
    )


@pytest.mark.parametrize(
    ("operating_label", "da_rows", "value", "parts"),
    [
        (
            "Profit from operations",
            [
                row("Depreciation of property and equipment", "", "40,000", "35,000"),
                row("Amortisation of intangible assets", "", "10,000", "9,000"),
            ],
            Decimal("450000000"),
            [
                DerivationPart("Profit from operations", 1, Decimal("400000000")),
                DerivationPart("Depreciation of property and equipment", 2, Decimal("40000000")),
                DerivationPart("Amortisation of intangible assets", 2, Decimal("10000000")),
            ],
        ),
        (
            "Earnings before interest and tax",
            [row("Depreciation charge", "", "40,000", "35,000")],
            Decimal("440000000"),
            [
                DerivationPart("Earnings before interest and tax", 1, Decimal("400000000")),
                DerivationPart("Depreciation charge", 2, Decimal("40000000")),
            ],
        ),
    ],
    ids=["depreciation_and_amortisation_lines", "depreciation_only"],
)
def test_ebitda_derivation_reads_d_and_a_from_the_cash_flow_statement(
    operating_label: str, da_rows: list[StatementRow], value: Decimal, parts: list[DerivationPart]
) -> None:
    result = extract(
        income_page([row(operating_label, "6", "400,000", "350,000")]),
        cashflow_page(da_rows),
        items=["ebitda"],
    )

    ebitda = only_item(result)
    assert ebitda.status == "derived"
    assert ebitda.value == value
    assert ebitda.derivation == parts
    assert ebitda.page_number == 1


def test_ebitda_line_is_used_before_any_derivation() -> None:
    result = extract(
        income_page(
            [
                row("Operating profit", "6", "400,000", "350,000"),
                row("Depreciation and amortisation", "7", "(50,000)", "(45,000)"),
                row("EBITDA", "", "460,000", "395,000"),
            ]
        ),
        items=["ebitda"],
    )

    ebitda = only_item(result)
    assert ebitda.status == "found"
    assert ebitda.value == Decimal("460000000")
    assert ebitda.matched_label == "EBITDA"
    assert ebitda.derivation == []


@pytest.mark.parametrize(
    "rows",
    [
        [row("Operating profit", "6", "400,000", "350,000")],
        [row("Depreciation and amortisation", "7", "(50,000)", "(45,000)")],
    ],
    ids=["no_depreciation", "no_operating_profit"],
)
def test_ebitda_without_both_parts_is_not_found(rows: list[StatementRow]) -> None:
    result = extract(income_page(rows), items=["ebitda"])

    assert only_item(result).status == "not_found"
    assert result.missing_items == ["ebitda"]
    assert result.status == "no_items"


@pytest.mark.parametrize(
    ("income_columns", "cash_flow_columns", "label", "candidates"),
    [
        ([("2025", 400.0), ("2025", 480.0)], YEAR_COLS, "Operating profit", ["400,000", "350,000"]),
        (YEAR_COLS, NO_HEADER_COLS, "Depreciation and amortisation", ["(50,000)", "(45,000)"]),
    ],
    ids=["ambiguous_operating_profit", "ambiguous_depreciation"],
)
def test_ebitda_with_an_ambiguous_part_is_ambiguous(
    income_columns: list[tuple[str, float]],
    cash_flow_columns: list[tuple[str, float]],
    label: str,
    candidates: list[str],
) -> None:
    result = extract(
        income_page([row("Operating profit", "400,000", "350,000")], columns=income_columns),
        cashflow_page(
            [row("Depreciation and amortisation", "(50,000)", "(45,000)")],
            columns=cash_flow_columns,
        ),
        items=["ebitda"],
    )

    ebitda = only_item(result)
    assert ebitda.status == "ambiguous"
    assert ebitda.value is None
    assert ebitda.raw_value is None
    assert ebitda.matched_label == label
    assert ebitda.derivation == []
    assert raw_candidates(ebitda) == candidates


def test_ebitda_sum_beyond_the_money_columns_is_ambiguous() -> None:
    # Each part fits NUMERIC(24,2); their sum, 1.1 x 10^22, does not.
    result = extract(
        text_page(
            [
                INCOME_TITLE,
                "Operating profit 9000000000000000000000",
                "Depreciation and amortisation (2000000000000000000000)",
            ]
        ),
        items=["ebitda"],
    )

    assert only_item(result) == ExtractedLineItem(
        item="ebitda",
        status="ambiguous",
        value=None,
        unit="KES",
        raw_value=None,
        scale_applied=1,
        page_number=1,
        matched_label="Operating profit",
        candidates=[
            Candidate(
                "Operating profit",
                1,
                "9000000000000000000000",
                Decimal("9000000000000000000000"),
            ),
            Candidate(
                "Depreciation and amortisation",
                1,
                "(2000000000000000000000)",
                Decimal("-2000000000000000000000"),
            ),
        ],
    )


OPERATING_PROFIT = row("Operating profit", "6", "400,000", "350,000")


@pytest.mark.parametrize(
    ("income_rows", "cash_flow_rows", "label", "candidates"),
    [
        (
            [OPERATING_PROFIT],
            [
                row("Depreciation of equipment", "", "10,000", "9,000"),
                row("Depreciation of buildings", "", "10,000", "8,000"),
            ],
            "Depreciation of equipment",
            [
                Candidate("Depreciation of equipment", 2, "10,000", Decimal("10000000")),
                Candidate("Depreciation of buildings", 2, "10,000", Decimal("10000000")),
            ],
        ),
        (
            [OPERATING_PROFIT],
            [
                row("Depreciation of equipment", "", "40,000", "35,000"),
                row("Amortisation of software", "", "5,000", "4,000"),
                row("Amortisation of licences", "", "5,000", "4,500"),
            ],
            "Amortisation of software",
            [
                Candidate("Amortisation of software", 2, "5,000", Decimal("5000000")),
                Candidate("Amortisation of licences", 2, "5,000", Decimal("5000000")),
            ],
        ),
        (
            [OPERATING_PROFIT, row("Depreciation of buildings", "7", "10,000", "8,000")],
            [row("Depreciation of equipment", "", "10,000", "9,000")],
            "Depreciation of equipment",
            [
                Candidate("Depreciation of equipment", 2, "10,000", Decimal("10000000")),
                Candidate("Depreciation of buildings", 1, "10,000", Decimal("10000000")),
            ],
        ),
        (
            [OPERATING_PROFIT],
            [
                row("Depreciation", "", "10,000", "9,000"),
                row("Depreciation", "", "10,000", "8,000"),
            ],
            "Depreciation",
            [
                Candidate("Depreciation", 2, "10,000", Decimal("10000000")),
                Candidate("Depreciation", 2, "10,000", Decimal("10000000")),
            ],
        ),
    ],
    ids=[
        "two_depreciation_lines",
        "two_amortisation_lines",
        "depreciation_lines_on_two_statements",
        "same_label_twice_on_one_page",
    ],
)
def test_ebitda_with_distinct_d_and_a_components_is_ambiguous(
    income_rows: list[StatementRow],
    cash_flow_rows: list[StatementRow],
    label: str,
    candidates: list[Candidate],
) -> None:
    # Equal amounts do not make distinct component lines one figure: without a combined
    # "depreciation and amortisation" line their total is not established, so nothing commits.
    result = extract(income_page(income_rows), cashflow_page(cash_flow_rows), items=["ebitda"])

    ebitda = only_item(result)
    assert ebitda.status == "ambiguous"
    assert ebitda.value is None
    assert ebitda.raw_value is None
    assert ebitda.matched_label == label
    assert ebitda.derivation == []
    assert ebitda.candidates == candidates


def test_ebitda_depreciation_repeated_on_two_statements_is_one_part() -> None:
    result = extract(
        income_page([OPERATING_PROFIT, row("Depreciation", "7", "40,000", "35,000")]),
        cashflow_page([row("Depreciation", "", "40,000", "35,000")]),
        items=["ebitda"],
    )

    ebitda = only_item(result)
    assert ebitda.status == "derived"
    assert ebitda.value == Decimal("440000000")
    assert ebitda.derivation == [
        DerivationPart("Operating profit", 1, Decimal("400000000")),
        DerivationPart("Depreciation", 2, Decimal("40000000")),
    ]


# ---------------------------------------------------------------------------------------------
# Shares outstanding
# ---------------------------------------------------------------------------------------------


def test_share_count_is_not_scaled_by_the_page_scale() -> None:
    result = extract(
        income_page([row("Revenue", "5", "1,234,567", "1,100,000")]),
        text_page([NOTES_TITLE, "Number of ordinary shares in issue 40,065,428,000"]),
        items=["revenue", "shares_outstanding"],
    )

    shares = item_named(result, "shares_outstanding")
    assert shares == ExtractedLineItem(
        item="shares_outstanding",
        status="found",
        value=Decimal("40065428000"),
        unit="shares",
        raw_value="40,065,428,000",
        scale_applied=1,
        page_number=2,
        matched_label="Number of ordinary shares in issue",
    )
    assert item_named(result, "revenue").value == Decimal("1234567000")


@pytest.mark.parametrize(
    ("label", "raw", "scale", "value"),
    [
        ("Number of ordinary shares in issue (millions)", "40,065", 1_000_000, "40065000000"),
        ("Number of shares ('000)", "40,065,428", 1_000, "40065428000"),
        ("Number of shares (thousands)", "40,065,428", 1_000, "40065428000"),
        ("Number of shares (bn)", "40", 1_000_000_000, "40000000000"),
    ],
)
def test_share_count_is_scaled_by_its_own_label_hint(
    label: str, raw: str, scale: int, value: str
) -> None:
    result = extract(text_page([NOTES_TITLE, f"{label} {raw}"]), items=["shares_outstanding"])

    shares = only_item(result)
    assert shares.status == "found"
    assert shares.value == Decimal(value)
    assert shares.scale_applied == scale


@pytest.mark.parametrize(
    ("hint", "scale", "value"),
    [
        ("Millions", 1_000_000, "40065000000"),
        ("KShs", 1, "40065"),
    ],
    ids=["unit_hint_over_the_column", "currency_over_the_column_is_no_share_hint"],
)
def test_share_count_is_scaled_by_its_column_hint(hint: str, scale: int, value: str) -> None:
    page = statement(
        NOTES_TITLE,
        [row("Number of ordinary shares in issue", "40,065", "40,065")],
        scale_note=None,
        columns=YEAR_COLS,
        header_above=[(hint, 400.0), (hint, 480.0)],
    )

    result = extract(page, items=["shares_outstanding"])

    shares = only_item(result)
    assert shares.status == "found"
    assert shares.value == Decimal(value)
    assert shares.scale_applied == scale


@pytest.mark.parametrize(
    ("raw", "status", "value"),
    [
        ("1,234.5", "ambiguous", None),
        # 32 significant digits: rounded to the default context's 28 it would look whole.
        ("1,000,000.0000000000000000000000001", "ambiguous", None),
        ("(100)", "ambiguous", None),
        ("1,000,000,000,001", "ambiguous", None),
        ("1,000,000,000,000", "found", Decimal("1000000000000")),
    ],
    ids=[
        "fractional",
        "fractional_beyond_28_digits",
        "negative",
        "above_the_maximum",
        "at_the_maximum",
    ],
)
def test_share_count_must_be_a_whole_count_within_bounds(
    raw: str, status: str, value: Decimal | None
) -> None:
    result = extract(
        text_page([NOTES_TITLE, f"Number of shares in issue {raw}"]), items=["shares_outstanding"]
    )

    shares = only_item(result)
    assert shares.status == status
    assert shares.value == value
    if status == "ambiguous":
        assert shares.candidates == [Candidate("Number of shares in issue", 1, raw, None)]


# ---------------------------------------------------------------------------------------------
# Filing status, requested items and missing items
# ---------------------------------------------------------------------------------------------


def test_all_six_items_present_gives_extracted() -> None:
    result = extract(
        income_page(
            [
                row("Revenue", "5", "1,234,567", "1,100,000"),
                row("Operating profit", "6", "400,000", "350,000"),
                row("Depreciation and amortisation", "7", "(50,000)", "(45,000)"),
                row("Profit for the year", "", "250,000", "200,000"),
                row("Basic earnings per share", "8", "1.23", "1.10"),
            ]
        ),
        position_page([row("Total borrowings", "12", "5,500,000", "5,000,000")]),
        cashflow_page([row("Net cash generated from operating activities", "", "600,000", "1")]),
        text_page([NOTES_TITLE, "Number of ordinary shares in issue 40,065,428,000"]),
        period_end_date=date(2025, 12, 31),
    )

    assert [item.item for item in result.items] == list(LINE_ITEMS)
    assert [(item.status, item.value, item.page_number) for item in result.items] == [
        ("found", Decimal("1234567000"), 1),
        ("derived", Decimal("450000000"), 1),
        ("found", Decimal("250000000"), 1),
        ("found", Decimal("5500000000"), 2),
        ("found", Decimal("600000000"), 3),
        ("found", Decimal("40065428000"), 4),
    ]
    assert result.status == "extracted"
    assert result.missing_items == []
    assert result.page_count == 4
    assert result.detected_scale == 1000
    assert result.currency == "KES"
    assert result.warnings == []


def test_missing_item_is_not_found_and_the_filing_partial() -> None:
    result = extract(
        income_page([row("Revenue", "5", "1,234,567", "1,100,000")]),
        items=["revenue", "total_debt"],
    )

    assert item_named(result, "revenue").status == "found"
    assert item_named(result, "total_debt") == ExtractedLineItem(
        item="total_debt",
        status="not_found",
        value=None,
        unit="KES",
        raw_value=None,
        scale_applied=None,
        page_number=None,
        matched_label=None,
        derivation=[],
        candidates=[],
    )
    assert result.missing_items == ["total_debt"]
    assert result.status == "partial"


def test_text_without_any_item_gives_no_items() -> None:
    result = extract(text_page(["Directors' report", "The directors present their report."]))

    assert [item.status for item in result.items] == ["not_found"] * 6
    assert result.missing_items == list(LINE_ITEMS)
    assert result.status == "no_items"
    assert result.detected_scale == 1
    assert result.warnings == ["scale_not_detected"]


def test_requested_items_keep_the_caller_order_without_duplicates() -> None:
    result = extract(
        income_page(
            [
                row("Revenue", "5", "1,234,567", "1,100,000"),
                row("Profit for the year", "", "250,000", "200,000"),
            ]
        ),
        items=("net_income", "revenue", "net_income"),
    )

    assert [item.item for item in result.items] == ["net_income", "revenue"]


def test_pdf_without_a_text_layer_gives_no_items() -> None:
    result = extract_line_items(build_pdf([Page(cells=()), Page(cells=())]))

    assert result == ExtractionResult(
        page_count=2,
        detected_scale=None,
        currency="KES",
        status="no_items",
        items=[
            ExtractedLineItem(item=name, status="not_found", value=None, unit=unit)
            for name, unit in [
                ("revenue", "KES"),
                ("ebitda", "KES"),
                ("net_income", "KES"),
                ("total_debt", "KES"),
                ("operating_cash_flow", "KES"),
                ("shares_outstanding", "shares"),
            ]
        ],
        missing_items=list(LINE_ITEMS),
        warnings=["no_text_layer"],
    )


# ---------------------------------------------------------------------------------------------
# Rejected PDFs and invalid arguments
# ---------------------------------------------------------------------------------------------


def test_too_many_pages_is_rejected_before_extraction() -> None:
    pdf_bytes = build_pdf([Page(cells=()), Page(cells=()), Page(cells=())])

    with pytest.raises(TooManyPages) as raised:
        extract_line_items(pdf_bytes, max_pages=2)

    assert raised.value.page_count == 3
    assert raised.value.max_pages == 2
    assert str(raised.value) == "the PDF has 3 pages; at most 2 can be parsed"
    assert extract_line_items(pdf_bytes, max_pages=3).page_count == 3


# The 32-byte padding string of the PDF standard security handler (ISO 32000-1, 7.6.3.3).
_PASSWORD_PADDING = bytes.fromhex(
    "28bf4e5e4e758a4164004e56fffa01082e2e00b6d0683e802f0ca9fe6453697a"
)
_OWNER_ENTRY = bytes(range(32))
_FILE_ID = bytes(range(16))
_PERMISSIONS = -4


def _rc4(key: bytes, data: bytes) -> bytes:
    """RC4, the cipher of the revision 2 standard security handler."""
    state = list(range(256))
    j = 0
    for i in range(256):
        j = (j + state[i] + key[i % len(key)]) % 256
        state[i], state[j] = state[j], state[i]
    out = bytearray()
    i = j = 0
    for byte in data:
        i = (i + 1) % 256
        j = (j + state[i]) % 256
        state[i], state[j] = state[j], state[i]
        out.append(byte ^ state[(state[i] + state[j]) % 256])
    return bytes(out)


def _empty_password_user_entry() -> bytes:
    """The /U entry with which an empty user password opens the file (revision 2, 40-bit key)."""
    digest = hashlib.md5(
        _PASSWORD_PADDING
        + _OWNER_ENTRY
        + _PERMISSIONS.to_bytes(4, "little", signed=True)
        + _FILE_ID,
        usedforsecurity=False,
    ).digest()
    return _rc4(digest[:5], _PASSWORD_PADDING)


def _with_encrypt_dictionary(pdf_bytes: bytes, user_entry: bytes) -> bytes:
    """Add an /Encrypt dictionary and a file /ID to the trailer, which follows the xref table, so
    every object offset stays valid."""
    encrypt = (
        f"/Encrypt << /Filter /Standard /V 1 /R 2 /O <{_OWNER_ENTRY.hex()}> "
        f"/U <{user_entry.hex()}> /P {_PERMISSIONS} >> /ID [<{_FILE_ID.hex()}> <{_FILE_ID.hex()}>]"
    ).encode("ascii")
    return pdf_bytes.replace(b"/Root 1 0 R >>", b"/Root 1 0 R " + encrypt + b" >>")


@pytest.mark.parametrize(
    "empty_password_opens", [False, True], ids=["password_required", "empty_password_opens"]
)
def test_encrypted_pdf_is_rejected(empty_password_opens: bool) -> None:
    user_entry = _empty_password_user_entry() if empty_password_opens else bytes(32)
    pdf_bytes = _with_encrypt_dictionary(build_pdf([text_page(["Revenue 1,234"])]), user_entry)

    with pytest.raises(PdfEncrypted):
        extract_line_items(pdf_bytes)


@pytest.mark.parametrize(
    "pdf_bytes",
    [
        b"not a pdf at all",
        b"%PDF-1.4\nthis is not a PDF body\n",
        b"%PDF-1.4\n" + b"\x00garbage" * 64,
        b"",
    ],
    ids=["garbage", "magic_bytes_only", "magic_bytes_then_binary_garbage", "empty"],
)
def test_unparseable_bytes_are_unreadable(pdf_bytes: bytes) -> None:
    with pytest.raises(PdfUnreadable, match="could not be parsed"):
        extract_line_items(pdf_bytes)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b"<< /Length", b"<< /Filter /LZWDecode /Length"),
        (b"/Subtype /Type1", b"/Subtype /Type0"),
    ],
    ids=["undecodable_content_stream", "font_without_descendant_fonts"],
)
def test_page_whose_content_cannot_be_read_is_unreadable(old: bytes, new: bytes) -> None:
    pdf_bytes = build_pdf([text_page(["Revenue 1,234"])]).replace(old, new)

    with pytest.raises(PdfUnreadable, match="page 1 of the PDF could not be parsed"):
        extract_line_items(pdf_bytes)


@pytest.mark.parametrize(
    ("items", "max_pages", "message"),
    [
        (["revenue", "dividends"], 400, "unknown line item 'dividends'"),
        ("revenue", 400, "not a single string"),
        ([], 400, "at least one line item"),
        (["revenue"], 0, "max_pages must be at least 1, got 0"),
    ],
    ids=["unknown_item", "single_string", "no_item", "max_pages_below_one"],
)
def test_invalid_arguments_raise_value_error(
    items: Sequence[str], max_pages: int, message: str
) -> None:
    pdf_bytes = build_pdf([text_page(["Revenue 1,234"])])

    with pytest.raises(ValueError, match=message) as raised:
        extract_line_items(pdf_bytes, items, max_pages=max_pages)

    assert type(raised.value) is ValueError


def test_rejection_errors_are_value_errors() -> None:
    assert issubclass(PdfEncrypted, ValueError)
    assert issubclass(PdfUnreadable, ValueError)
    assert issubclass(TooManyPages, ValueError)
    # A timeout is the server's time budget, never reported as an unreadable file.
    assert issubclass(PdfParseTimeout, ValueError)
    assert not issubclass(PdfParseTimeout, PdfUnreadable)


# Pages are enumerated directly from pdfminer's page generator, so the raw error is the cause; it
# never escapes as itself or as pdfplumber's wrapper.
@pytest.mark.parametrize(
    ("old", "new", "raw_error", "message"),
    [
        (
            b"/Type /Page /Parent",
            b"/Type /Page /Rotate /x /Parent",
            TypeError,
            "the PDF could not be parsed (TypeError)",
        ),
        (
            b"/MediaBox [0 0 ",
            b"/MediaBox [0 ",
            IndexError,
            "the PDF could not be parsed (IndexError)",
        ),
        (
            b"/MediaBox [0 0 ",
            b"/MediaBox [/a /b ",
            TypeError,
            "the PDF could not be parsed (TypeError)",
        ),
    ],
    ids=["rotate_not_a_number", "mediabox_three_numbers", "mediabox_not_numbers"],
)
def test_page_tree_that_cannot_be_listed_is_unreadable(
    old: bytes, new: bytes, raw_error: type[Exception], message: str
) -> None:
    pdf_bytes = build_pdf([text_page(["Revenue 1,234"])]).replace(old, new)

    with pytest.raises(PdfUnreadable) as raised:
        pdf_line_items._extract(pdf_bytes, list(LINE_ITEMS), None, 400)

    assert type(raised.value) is PdfUnreadable
    assert not isinstance(raised.value, TypeError | IndexError | PdfminerException)
    assert str(raised.value) == message
    assert type(raised.value.__cause__) is raw_error


# ---------------------------------------------------------------------------------------------
# Hostile PDFs and the parse limits
# ---------------------------------------------------------------------------------------------

MIB = 1 << 20
CATALOG = b"<< /Type /Catalog /Pages 2 0 R >>"
ONE_PAGE_TREE = b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>"
HELVETICA = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
# An income statement whose revenue reads 1,234 in KShs '000.
STATEMENT_TEXT = b"".join(
    b"BT /F1 9 Tf 1 0 0 1 %d %d Tm (%s) Tj ET\n" % cell
    for cell in [
        (50, 800, b"Statement of profit or loss"),
        (50, 780, b"KShs '000"),
        (400, 760, b"2025"),
        (480, 760, b"2024"),
        (50, 740, b"Revenue"),
        (400, 740, b"1,234"),
        (480, 740, b"1,100"),
    ]
)
TEXT_OPERATOR = b"BT /F1 9 Tf 1 0 0 1 50 700 Tm (A) Tj ET\n"


def raw_pdf(objects: Mapping[int, bytes], *, offset_shift: int = 0) -> bytes:
    """A PDF 1.4 file of the numbered object bodies (object 1 is the catalog) with a classic xref
    table, every offset in it moved by ``offset_shift`` bytes (0: exact)."""
    out = bytearray(b"%PDF-1.4\n")
    offsets: dict[int, int] = {}
    for number in sorted(objects):
        offsets[number] = len(out)
        out += b"%d 0 obj\n%s\nendobj\n" % (number, objects[number])
    size = max(objects) + 1
    xref_offset = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % size
    for number in range(1, size):
        if number in offsets:
            out += b"%010d 00000 n \n" % (offsets[number] + offset_shift)
        else:
            out += b"0000000000 65535 f \n"
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (size, xref_offset)
    return bytes(out)


def stream_object(data: bytes, entries: bytes = b"") -> bytes:
    return b"<< %s/Length %d >>\nstream\n%s\nendstream" % (entries, len(data), data)


def page_object(contents: bytes, resources: bytes = b"") -> bytes:
    return (
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 3 0 R >> "
        + resources
        + b">> /Contents "
        + contents
        + b" >>"
    )


@functools.cache
def flate_of_repeated(unit: bytes, size: int) -> bytes:
    """zlib data decoding to ``unit`` repeated to ``size`` bytes, compressed piece by piece so the
    decoded form is never held."""
    compressor = zlib.compressobj(9)
    piece = unit * (MIB // len(unit))
    parts: list[bytes] = []
    written = 0
    while written < size:
        chunk = piece[: size - written]
        parts.append(compressor.compress(chunk))
        written += len(chunk)
    parts.append(compressor.flush())
    return b"".join(parts)


def flood_pdf(layout: str, unit: bytes, size: int) -> bytes:
    """A one-page statement PDF that also draws ``size`` decoded bytes of ``unit``.

    ``content_stream``: a second, Flate-compressed content stream of the page. ``form_xobject``:
    a Flate-compressed Form XObject the page draws. ``nested_form_xobject``: that form, drawn by
    another form through the outer form's own resources. ``two_content_streams``: two content
    streams of ``size // 2`` bytes each.
    """
    flood = flate_of_repeated(unit, size)
    flate = b"/Filter /FlateDecode "
    form = b"/Type /XObject /Subtype /Form /BBox [0 0 595 842] " + flate
    objects = {1: CATALOG, 2: ONE_PAGE_TREE, 3: HELVETICA}
    if layout == "content_stream":
        objects |= {4: page_object(b"[6 0 R 5 0 R]"), 5: stream_object(flood, flate)}
        objects[6] = stream_object(STATEMENT_TEXT)
    elif layout == "two_content_streams":
        half = flate_of_repeated(unit, size // 2)
        objects |= {4: page_object(b"[6 0 R 5 0 R 7 0 R]"), 5: stream_object(half, flate)}
        objects |= {6: stream_object(STATEMENT_TEXT), 7: stream_object(half, flate)}
    elif layout == "form_xobject":
        objects |= {4: page_object(b"6 0 R", b"/XObject << /Fm1 5 0 R >> ")}
        objects |= {5: stream_object(flood, form), 6: stream_object(STATEMENT_TEXT + b"/Fm1 Do\n")}
    elif layout == "nested_form_xobject":
        outer = b"/Type /XObject /Subtype /Form /BBox [0 0 595 842] "
        outer += b"/Resources << /XObject << /Fm1 5 0 R >> >> "
        objects |= {4: page_object(b"6 0 R", b"/XObject << /Fm2 7 0 R >> ")}
        objects |= {5: stream_object(flood, form), 6: stream_object(STATEMENT_TEXT + b"/Fm2 Do\n")}
        objects[7] = stream_object(b"/Fm1 Do\n", outer)
    else:
        raise ValueError(f"unknown layout {layout!r}")
    return raw_pdf(objects)


def xref_stream_pdf(widths: bytes, entries: bytes, claimed: int, entries_filter: bytes) -> bytes:
    """A valid one-page PDF whose cross-reference section is replaced by an xref stream that
    lists ``claimed`` objects from 0, with the given field ``widths`` and ``entries`` data."""
    statement = build_pdf([text_page(["Revenue 1,234"])])
    body = statement[: statement.index(b"xref")]
    xref = stream_object(
        entries,
        b"/Type /XRef /Size %d /W [%s] /Index [0 %d] /Root 1 0 R %s"
        % (claimed, widths, claimed, entries_filter),
    )
    return body + b"9 0 obj\n%s\nendobj\nstartxref\n%d\n%%%%EOF\n" % (xref, len(body))


def many_pages_pdf(count: int, count_entry: bytes) -> bytes:
    """``count`` real page objects sharing one statement content stream, the page tree carrying
    ``count_entry`` (``/Count n`` or nothing)."""
    kids = b" ".join(b"%d 0 R" % (5 + index) for index in range(count))
    objects = {
        1: CATALOG,
        2: b"<< /Type /Pages /Kids [" + kids + b"] " + count_entry + b" >>",
        3: HELVETICA,
        4: stream_object(STATEMENT_TEXT),
    }
    objects |= {5 + index: page_object(b"4 0 R") for index in range(count)}
    return raw_pdf(objects)


def forbid_whole_document_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make pdfplumber's ``PDF.pages`` and ``PDF.close`` fail: both list every page of the
    document, which the extractor must never do."""

    def fail(*_: object) -> None:
        raise AssertionError("pdfplumber listed every page of the document")

    monkeypatch.setattr(pdfplumber.PDF, "pages", property(fail))
    monkeypatch.setattr(pdfplumber.PDF, "close", fail)


XREF_STREAM_SHORT = "the PDF's cross-reference stream lists more objects than it holds entries for"


# pdfminer enumerates every listed object id when it cannot find the page tree, about 20 us each:
# these files would take hours. Each is rejected before any page is enumerated.
@pytest.mark.parametrize(
    ("widths", "entries", "claimed", "entries_filter", "message"),
    [
        (b"1 1000000 1", bytes(16), 999_999_999, b"", XREF_STREAM_SHORT),
        (b"1 1 1", b"\x01\x00\x00", 999_999_999, b"", XREF_STREAM_SHORT),
        # Every entry is present, but 3 MB of entries compress to a few KB: a million objects
        # cannot fit in a file that small.
        (
            b"1 1 1",
            flate_of_repeated(b"\x01\x00\x00", 3_000_000),
            1_000_000,
            b"/Filter /FlateDecode",
            "the PDF's cross-reference sections list more objects than it holds",
        ),
    ],
    ids=["entries_a_megabyte_wide", "one_entry_for_a_billion", "more_objects_than_bytes"],
)
def test_cross_reference_stream_claiming_absent_objects_is_unreadable(
    widths: bytes, entries: bytes, claimed: int, entries_filter: bytes, message: str
) -> None:
    pdf_bytes = xref_stream_pdf(widths, entries, claimed, entries_filter)
    started = time.monotonic()

    with pytest.raises(PdfUnreadable) as raised:
        extract_line_items(pdf_bytes)

    assert str(raised.value) == message
    assert time.monotonic() - started < 10


@pytest.mark.parametrize("unit", [b" ", b"q Q\n", TEXT_OPERATOR], ids=["spaces", "q_Q", "text"])
@pytest.mark.parametrize("layout", ["content_stream", "form_xobject"])
def test_page_content_over_the_budget_is_rejected_before_it_is_decoded(
    layout: str, unit: bytes
) -> None:
    # 16 MiB decoded from a file of a few KB: a decompression bomb of whitespace, and floods of
    # no-op and text operators that pdfminer would interpret one by one.
    pdf_bytes = flood_pdf(layout, unit, 16 * MIB)
    assert len(pdf_bytes) < 200_000

    with pytest.raises(PdfUnreadable) as raised:
        extract_line_items(pdf_bytes)

    assert str(raised.value) == "page 1 has more than 8 MiB of decoded content"


@pytest.mark.parametrize(
    ("layout", "size"),
    [("nested_form_xobject", 16 * MIB), ("two_content_streams", 16 * MIB)],
    ids=["form_drawn_by_a_form", "streams_at_the_budget_each"],
)
def test_page_content_budget_covers_every_stream_the_page_draws(layout: str, size: int) -> None:
    with pytest.raises(PdfUnreadable) as raised:
        pdf_line_items._extract(flood_pdf(layout, b" ", size), ["revenue"], None, 400)

    assert str(raised.value) == "page 1 has more than 8 MiB of decoded content"


@pytest.mark.parametrize(
    "layout", ["content_stream", "form_xobject", "nested_form_xobject", "two_content_streams"]
)
def test_page_content_within_the_budget_is_read(layout: str) -> None:
    result = pdf_line_items._extract(flood_pdf(layout, b" ", MIB), ["revenue"], None, 400)

    revenue = only_item(result)
    assert (revenue.status, revenue.value) == ("found", Decimal("1234000"))


def test_parse_over_the_time_limit_is_a_timeout_not_an_unreadable_pdf() -> None:
    # 4 MiB of "q Q" is within the content budget but takes pdfminer several seconds.
    pdf_bytes = flood_pdf("content_stream", b"q Q\n", 4 * MIB)
    started = time.monotonic()

    with pytest.raises(PdfParseTimeout) as raised:
        pdf_line_items._run_isolated(pdf_bytes, ["revenue"], None, 400, time_limit=1)

    assert not isinstance(raised.value, PdfUnreadable)
    assert raised.value.time_limit == 1
    assert str(raised.value) == "the PDF could not be parsed within 1 seconds"
    assert time.monotonic() - started < 5


@pytest.mark.parametrize(("max_pages", "time_limit"), [(400, 180.0), (10, 24.0)])
def test_time_limit_is_sized_to_the_page_limit(
    monkeypatch: pytest.MonkeyPatch, max_pages: int, time_limit: float
) -> None:
    """20 seconds plus 0.4 per page the parse admits; the cancel event is passed on as given."""
    calls: list[tuple[int, object, object]] = []
    result = ExtractionResult(
        page_count=1,
        detected_scale=None,
        currency="KES",
        status="no_items",
        items=[ExtractedLineItem(item="revenue", status="not_found", value=None, unit="KES")],
        missing_items=["revenue"],
        warnings=["no_text_layer"],
    )

    def capture(
        _pdf_bytes: bytes,
        _requested: Sequence[str],
        _period_end_date: date | None,
        pages: int,
        **limits: object,
    ) -> ExtractionResult:
        calls.append((pages, limits["time_limit"], limits["cancel"]))
        return result

    monkeypatch.setattr(pdf_line_items, "_run_isolated", capture)
    cancel = threading.Event()

    assert extract_line_items(b"%PDF-", ["revenue"], max_pages=max_pages, cancel=cancel) is result
    assert calls == [(max_pages, time_limit, cancel)]


def recording_popen(monkeypatch: pytest.MonkeyPatch) -> list[subprocess.Popen[bytes]]:
    """Record every parser process the extractor starts, in start order."""
    started: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        process: subprocess.Popen[bytes] = real_popen(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr(pdf_line_items.subprocess, "Popen", popen)
    return started


def test_cancel_kills_the_running_parser_process_promptly(monkeypatch: pytest.MonkeyPatch) -> None:
    # The flood takes the parser process about 8 seconds; the event is set half a second into it.
    pdf_bytes = flood_pdf("content_stream", b"q Q\n", 4 * MIB)
    started = recording_popen(monkeypatch)
    cancel = threading.Event()
    cancelled_at: list[float] = []

    def cancel_while_parsing() -> None:
        deadline = time.monotonic() + 10
        while not started and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.5)
        cancelled_at.append(time.monotonic())
        cancel.set()

    canceller = threading.Thread(target=cancel_while_parsing)
    canceller.start()
    try:
        with pytest.raises(ParseCancelledError) as raised:
            extract_line_items(pdf_bytes, ["revenue"], cancel=cancel)
        stopped_at = time.monotonic()
    finally:
        canceller.join()

    assert str(raised.value) == "the PDF parse was cancelled"
    assert stopped_at - cancelled_at[0] < 2
    [process] = started
    # Killed by a signal and already reaped.
    assert process.returncode is not None and process.returncode < 0


def test_cancel_set_before_the_parse_starts_no_parser_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = recording_popen(monkeypatch)
    cancel = threading.Event()
    cancel.set()

    with pytest.raises(ParseCancelledError):
        extract_line_items(build_pdf([text_page(["Revenue 1,234"])]), cancel=cancel)

    assert started == []


@pytest.mark.skipif(sys.platform != "linux", reason="RLIMIT_AS is enforced on Linux")
def test_parse_over_the_memory_limit_is_unreadable() -> None:
    # One string of a million characters: 1 MiB of content, but pdfminer builds a layout object
    # per character and needs gigabytes for them.
    text = STATEMENT_TEXT + b"BT /F1 1 Tf 1 0 0 1 10 10 Tm (" + b"A" * 1_000_000 + b") Tj ET\n"
    pdf_bytes = raw_pdf(
        {
            1: CATALOG,
            2: ONE_PAGE_TREE,
            3: HELVETICA,
            4: page_object(b"5 0 R"),
            5: stream_object(zlib.compress(text), b"/Filter /FlateDecode "),
        }
    )
    started = time.monotonic()

    with pytest.raises(PdfUnreadable) as raised:
        pdf_line_items._run_isolated(pdf_bytes, ["revenue"], None, 400, memory_limit=384 * MIB)

    assert str(raised.value) == "the PDF needs more than 384 MiB of memory to parse"
    assert time.monotonic() - started < 30


@pytest.mark.parametrize(
    "pdf_bytes",
    [
        raw_pdf({1: CATALOG, 2: b"<< /Type /Pages /Kids [] /Count 0 >>"}),
        raw_pdf({1: b"<< /Type /Catalog >>"}),
        raw_pdf(
            {
                1: CATALOG,
                2: ONE_PAGE_TREE,
                3: HELVETICA,
                4: page_object(b"5 0 R"),
                5: stream_object(STATEMENT_TEXT),
            },
            offset_shift=7,
        ),
    ],
    ids=["empty_page_tree", "catalog_without_pages", "every_offset_shifted"],
)
def test_pdf_without_pages_is_unreadable(pdf_bytes: bytes) -> None:
    with pytest.raises(PdfUnreadable) as raised:
        extract_line_items(pdf_bytes)

    assert str(raised.value) == "the PDF has no pages"


@pytest.mark.parametrize(
    ("count_entry", "page_count"),
    [(b"/Count 2000", 2000), (b"", 3), (b"/Count 1", 3)],
    ids=["declared_count", "count_missing", "count_below_the_pages_found"],
)
def test_too_many_pages_are_found_without_listing_every_page(
    monkeypatch: pytest.MonkeyPatch, count_entry: bytes, page_count: int
) -> None:
    forbid_whole_document_pages(monkeypatch)
    constructed: list[int] = []
    plumber_page = pdf_line_items._PlumberPage

    def counting_page(*args: object, **kwargs: object) -> object:
        constructed.append(len(constructed) + 1)
        return plumber_page(*args, **kwargs)

    monkeypatch.setattr(pdf_line_items, "_PlumberPage", counting_page)
    pdf_bytes = many_pages_pdf(2000, count_entry)

    with pytest.raises(TooManyPages) as raised:
        pdf_line_items._extract(pdf_bytes, list(LINE_ITEMS), None, 2)

    assert (raised.value.page_count, raised.value.max_pages) == (page_count, 2)
    assert constructed == [1, 2]


@pytest.mark.parametrize(
    ("pdf_bytes", "error"),
    [
        (build_pdf([text_page(["Revenue 1,234"])]), None),
        (
            _with_encrypt_dictionary(
                build_pdf([text_page(["Revenue 1,234"])]), _empty_password_user_entry()
            ),
            PdfEncrypted,
        ),
        (
            build_pdf([text_page(["Revenue 1,234"])]).replace(
                b"<< /Length", b"<< /Filter /LZWDecode /Length"
            ),
            PdfUnreadable,
        ),
    ],
    ids=["read", "encrypted", "page_unreadable"],
)
def test_no_path_lists_every_page_of_the_document(
    monkeypatch: pytest.MonkeyPatch, pdf_bytes: bytes, error: type[ValueError] | None
) -> None:
    forbid_whole_document_pages(monkeypatch)

    if error is None:
        assert pdf_line_items._extract(pdf_bytes, ["revenue"], None, 400).page_count == 1
    else:
        with pytest.raises(error) as raised:
            pdf_line_items._extract(pdf_bytes, ["revenue"], None, 400)
        assert type(raised.value) is error


def test_parser_process_returns_the_in_process_result_exactly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pdf_bytes = build_pdf(
        [
            income_page(
                [
                    row("Revenue", "5", "1,234.5", "1,100"),
                    row("Operating profit", "6", "400", "350"),
                    row("Depreciation and amortisation", "7", "(50)", "(45)"),
                    row("Profit for the year", "", "(250)", "200"),
                ]
            ),
            position_page(
                [
                    row("Borrowings \u2013 current", "18", "2,000", "1,500"),
                    row("Borrowings \u2013 non-current", "18", "3,000", "3,500"),
                ]
            ),
            cashflow_page(
                [row("Net cash generated from operating activities", "1,234", "1,100")],
                scale_note=None,
                columns=NO_HEADER_COLS,
            ),
        ]
    )
    in_process = pdf_line_items._extract(pdf_bytes, list(LINE_ITEMS), date(2025, 12, 31), 400)

    with caplog.at_level(logging.INFO, logger="opennse_terminal.ingestion.files.pdf_line_items"):
        result = extract_line_items(pdf_bytes, period_end_date=date(2025, 12, 31))

    assert result == in_process
    # repr shows each Decimal's digits and exponent, which == alone would not compare.
    assert repr(result) == repr(in_process)
    assert [item.status for item in result.items] == [
        "found",
        "derived",
        "found",
        "derived",
        "ambiguous",
        "not_found",
    ]
    assert str(item_named(result, "revenue").value) == "1234500"
    assert item_named(result, "net_income").value == Decimal("-250000")
    assert len(item_named(result, "total_debt").derivation) == 2
    assert raw_candidates(item_named(result, "operating_cash_flow")) == ["1,234", "1,100"]
    summaries = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("PDF line items:")
    ]
    assert summaries == [
        "PDF line items: pages=3 found_or_derived=4 ambiguous=1 missing=1 detected_scale=1000 "
        "currency=KES status=partial"
    ]
