"""Tests for the PDF line-item extractor (``ingestion.files.pdf_line_items``).

Every PDF is written in the test by ``helpers.pdf_builder``: minimal PDF 1.4 text documents that
draw each cell at a given x-position, so statement column layouts are reproduced without
committing or downloading any third-party report. The extractor reads them with pdfplumber; no
OCR is involved. Expected values are literals taken from the extraction contract (scale markers,
column evidence, the debt rule, EBITDA derivation, share counts, statuses and warnings), never
recomputed with the code under test. Numbers are exact ``Decimal`` values throughout.

Nothing here touches the network, a database or the settings.
"""

import hashlib
from collections.abc import Sequence
from datetime import date
from decimal import Decimal

import pdfplumber
import pytest
from pdfplumber.utils.exceptions import PdfminerException

from helpers.pdf_builder import Page, StatementRow, TextCell, build_pdf, statement_page, text_page
from opennse_terminal.ingestion.files.pdf_line_items import (
    LINE_ITEMS,
    Candidate,
    DerivationPart,
    ExtractedLineItem,
    ExtractionResult,
    PdfEncrypted,
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
    return extract_line_items(build_pdf(pages), items, period_end_date=period_end_date)


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


def test_decimal_amount_is_scaled_exactly() -> None:
    result = extract(
        income_page([row("Revenue", "", "1,234.5", "1,100")], scale_note="KShs million"),
        items=["revenue"],
    )

    assert only_item(result).value == Decimal("1234500000")


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
    ("rows", "status", "value"),
    [
        (
            [
                row("Profit attributable to owners of the parent per share", "1.23", "1.10"),
                row("Profit attributable to owners of the parent", "500", "450"),
            ],
            "found",
            Decimal("500000"),
        ),
        (
            [row("Profit attributable to owners of the parent per share", "1.23", "1.10")],
            "not_found",
            None,
        ),
    ],
    ids=["per_share_line_beside_the_total", "per_share_line_alone"],
)
def test_per_share_labels_are_excluded_for_monetary_items(
    rows: list[StatementRow], status: str, value: Decimal | None
) -> None:
    result = extract(income_page(rows, columns=YEAR_COLS), items=["net_income"])

    net_income = only_item(result)
    assert net_income.status == status
    assert net_income.value == value


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
        ("(100)", "ambiguous", None),
        ("1,000,000,000,001", "ambiguous", None),
        ("1,000,000,000,000", "found", Decimal("1000000000000")),
    ],
    ids=["fractional", "negative", "above_the_maximum", "at_the_maximum"],
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
    [b"not a pdf at all", b"%PDF-1.4\nthis is not a PDF body\n", b""],
    ids=["garbage", "magic_bytes_only", "empty"],
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


# pdfplumber's close() lists the pages again, so a page tree that cannot be listed also fails to
# close, with the same raw error; that error must never replace the PdfUnreadable raised for it.
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
            PdfminerException,
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
        extract_line_items(pdf_bytes)

    assert type(raised.value) is PdfUnreadable
    assert not isinstance(raised.value, TypeError | IndexError | PdfminerException)
    assert str(raised.value) == message
    assert type(raised.value.__cause__) is raw_error


def _failing_close(monkeypatch: pytest.MonkeyPatch) -> list[pdfplumber.PDF]:
    """Make pdfplumber's ``PDF.close`` close the document and then raise ``OSError``; the list
    returned collects every document it was called on."""
    closed: list[pdfplumber.PDF] = []
    real_close = pdfplumber.PDF.close

    def close(pdf: pdfplumber.PDF) -> None:
        closed.append(pdf)
        real_close(pdf)
        raise OSError("the document stream could not be closed")

    monkeypatch.setattr(pdfplumber.PDF, "close", close)
    return closed


@pytest.mark.parametrize(
    ("pdf_bytes", "max_pages", "error"),
    [
        (build_pdf([Page(cells=()), Page(cells=())]), 1, TooManyPages),
        (
            _with_encrypt_dictionary(
                build_pdf([text_page(["Revenue 1,234"])]), _empty_password_user_entry()
            ),
            400,
            PdfEncrypted,
        ),
        (
            build_pdf([text_page(["Revenue 1,234"])]).replace(
                b"<< /Length", b"<< /Filter /LZWDecode /Length"
            ),
            400,
            PdfUnreadable,
        ),
    ],
    ids=["too_many_pages", "encrypted", "page_unreadable"],
)
def test_close_failure_never_replaces_the_rejection(
    monkeypatch: pytest.MonkeyPatch, pdf_bytes: bytes, max_pages: int, error: type[ValueError]
) -> None:
    closed = _failing_close(monkeypatch)

    with pytest.raises(error) as raised:
        extract_line_items(pdf_bytes, max_pages=max_pages)

    assert type(raised.value) is error
    assert len(closed) == 1


def test_close_failure_after_every_page_was_read_is_unreadable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = _failing_close(monkeypatch)

    with pytest.raises(PdfUnreadable) as raised:
        extract_line_items(build_pdf([text_page(["Revenue 1,234"])]))

    assert type(raised.value) is PdfUnreadable
    assert str(raised.value) == "the PDF could not be parsed (OSError)"
    assert type(raised.value.__cause__) is OSError
    assert len(closed) == 1
