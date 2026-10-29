"""Minimal, deterministic PDF 1.4 text documents for the PDF line-item parser tests.

Every document uses one standard-14 font (Helvetica, WinAnsiEncoding) and draws each
:class:`TextCell` as its own text object, so pdfplumber's ``extract_words()`` reports each token's
left edge (``x0``) at exactly the requested x. Statement column layouts are reproduced this way
without committing any third-party or copyrighted report.

Coordinates are PDF points with the origin at the bottom-left corner of the page, and ``y`` is the
text baseline. Pages default to A4 portrait, 595 x 842 points. pdfplumber measures ``top`` from the
top edge instead, so a larger ``y`` here is a line nearer the top of the page.

Every drawn string is supplied by the caller and written byte for byte (``"(1,234)"``,
``"1,234,567"``, ``"-"``). Nothing here formats an amount from a number, so a test states exactly
the text the parser sees; floats only position text on the page.

The output has no ``/Info`` dictionary, ``/ID``, timestamp or compression, so identical input gives
byte-identical output.

A "KShs '000" income statement with a note column, from which the parser reads revenue as
1,234,567,000 and discards the note reference 5::

    from helpers.pdf_builder import StatementRow, build_pdf, statement_page

    pdf_bytes = build_pdf(
        [
            statement_page(
                title="Statement of profit or loss",
                scale_note="KShs '000",
                columns=[("Note", 300.0), ("2025", 400.0), ("2024", 480.0)],
                rows=[StatementRow("Revenue", ["5", "1,234,567", "1,100,000"])],
            )
        ]
    )
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass

__all__ = ["Page", "StatementRow", "TextCell", "build_pdf", "statement_page", "text_page"]

DEFAULT_FONT_SIZE = 9.0
A4_WIDTH = 595.0
A4_HEIGHT = 842.0
# Lowest baseline the layout helpers write to: a half-inch bottom margin.
MIN_BASELINE_Y = 36.0

# The font's /Encoding is WinAnsiEncoding, whose code points are those of Windows-1252.
_TEXT_ENCODING = "cp1252"
# Bytes that must be backslash-escaped inside a PDF literal string.
_STRING_DELIMITERS = b"\\()"
_HEADER = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
_CATALOG = b"<< /Type /Catalog /Pages 2 0 R >>"
# A standard-14 font needs no /Widths or /FontDescriptor: pdfminer applies its built-in Helvetica
# AFM metrics, which is what makes each word's x1 (and the gap to the next word) correct.
_FONT = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
# Objects 1-3 are the catalog, the page tree and the font; page i is object 4 + 2i and its content
# stream 5 + 2i.
_FIRST_PAGE_OBJECT = 4


@dataclass(frozen=True)
class TextCell:
    """One single-line string with its left edge at ``x`` and its baseline at ``y`` (points).

    ``text`` must be non-empty, hold no line break and be representable in Windows-1252 (the
    font's WinAnsiEncoding), which covers ``’``, ``–``, ``—`` and ``€`` but not, say, ``✓``.
    """

    text: str
    x: float
    y: float
    size: float = DEFAULT_FONT_SIZE

    def __post_init__(self) -> None:
        if not self.text:
            raise ValueError("TextCell text must not be empty")
        if "\n" in self.text or "\r" in self.text:
            raise ValueError(f"TextCell text must be a single line, got {self.text!r}")
        try:
            self.text.encode(_TEXT_ENCODING)
        except UnicodeEncodeError as exc:
            char = exc.object[exc.start]
            raise ValueError(
                f"TextCell text {self.text!r} contains {char!r} (U+{ord(char):04X}), which "
                "WinAnsiEncoding (cp1252) cannot represent"
            ) from None
        for name, value in (("x", self.x), ("y", self.y), ("size", self.size)):
            if not math.isfinite(value):
                raise ValueError(f"TextCell {name} must be finite, got {value!r}")
        if self.size <= 0:
            raise ValueError(f"TextCell size must be positive, got {self.size!r}")


@dataclass(frozen=True)
class Page:
    """One page of text cells. A page without cells is a page with no text layer."""

    cells: Sequence[TextCell]
    width: float = A4_WIDTH
    height: float = A4_HEIGHT

    def __post_init__(self) -> None:
        object.__setattr__(self, "cells", tuple(self.cells))
        for name, value in (("width", self.width), ("height", self.height)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"Page {name} must be finite and positive, got {value!r}")
        for cell in self.cells:
            if not (0 <= cell.x < self.width and 0 <= cell.y <= self.height):
                raise ValueError(
                    f"TextCell {cell.text!r} at ({cell.x!r}, {cell.y!r}) lies outside the "
                    f"{self.width!r} x {self.height!r} page"
                )


@dataclass(frozen=True)
class StatementRow:
    """One statement line: ``label`` at the label position and ``values[j]`` under column ``j``.

    Each value is drawn exactly as given (``"1,234,567"``, ``"(1,234)"``, ``"-"``, ``"—"``,
    ``"40,065,428,000"``); an empty string leaves that column blank on this line.
    """

    label: str
    values: Sequence[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", tuple(self.values))
        if not self.label:
            raise ValueError("StatementRow label must not be empty")


def _fmt(value: float) -> str:
    """Render a PDF number with at most three decimals and no exponent (``595.0`` -> ``595``)."""
    if not math.isfinite(value):
        raise ValueError(f"cannot write the non-finite number {value!r} to a PDF")
    text = f"{value:.3f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _escape(text: str) -> bytes:
    """Encode ``text`` as the body of a 7-bit PDF literal string in WinAnsiEncoding.

    Backslash and parentheses are backslash-escaped; control bytes and every byte above 0x7E
    (``’`` is 0x92, ``–`` 0x96) become three-digit octal escapes.
    """
    out = bytearray()
    for byte in text.encode(_TEXT_ENCODING):
        if byte in _STRING_DELIMITERS:
            out += b"\\" + bytes((byte,))
        elif byte < 0x20 or byte > 0x7E:
            out += f"\\{byte:03o}".encode("ascii")
        else:
            out.append(byte)
    return bytes(out)


def _show_text(cell: TextCell) -> bytes:
    """One self-contained text object placing ``cell`` with an absolute text matrix."""
    position = f"BT /F1 {_fmt(cell.size)} Tf 1 0 0 1 {_fmt(cell.x)} {_fmt(cell.y)} Tm ("
    return position.encode("ascii") + _escape(cell.text) + b") Tj ET"


def build_pdf(pages: Sequence[Page]) -> bytes:
    """Serialize ``pages`` as an uncompressed PDF 1.4 document with an exact xref table.

    A multi-page filing is simply several pages, for example
    ``build_pdf([income_page, balance_sheet_page, cash_flow_page, text_page([...notes...])])``.
    Raw cells give full control over positions::

        build_pdf(
            [
                Page(
                    [
                        TextCell("Revenue", 50.0, 700.0),
                        TextCell("1,234", 400.0, 700.0),
                        TextCell("1,100", 480.0, 700.0),
                    ]
                )
            ]
        )

    ``build_pdf([Page(cells=())])`` is a one-page document with no text layer.
    """
    pages = tuple(pages)
    if not pages:
        raise ValueError("a PDF needs at least one page")
    kids = " ".join(f"{_FIRST_PAGE_OBJECT + 2 * index} 0 R" for index in range(len(pages)))
    bodies: list[bytes] = [
        _CATALOG,
        f"<< /Type /Pages /Kids [{kids}] /Count {len(pages)} >>".encode("ascii"),
        _FONT,
    ]
    for index, page in enumerate(pages):
        contents = _FIRST_PAGE_OBJECT + 2 * index + 1
        bodies.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {_fmt(page.width)} "
                f"{_fmt(page.height)}] /Resources << /Font << /F1 3 0 R >> >> "
                f"/Contents {contents} 0 R >>"
            ).encode("ascii")
        )
        # Joined without a trailing newline, so /Length counts exactly the operator bytes.
        data = b"\n".join(_show_text(cell) for cell in page.cells)
        bodies.append(
            f"<< /Length {len(data)} >>\nstream\n".encode("ascii") + data + b"\nendstream"
        )

    out = bytearray(_HEADER)
    offsets: list[int] = []
    for number, body in enumerate(bodies, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode("ascii") + body + b"\nendobj\n"
    xref_offset = len(out)
    size = len(bodies) + 1
    out += f"xref\n0 {size}\n".encode("ascii")
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode("ascii")
    out += f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode(
        "ascii"
    )
    return bytes(out)


def _baselines(top: float, line_gap: float, count: int) -> list[float]:
    """Baselines of ``count`` consecutive lines from ``top`` down, kept above the bottom margin."""
    if not math.isfinite(line_gap) or line_gap <= 0:
        raise ValueError(f"line_gap must be finite and positive, got {line_gap!r}")
    if not math.isfinite(top):
        raise ValueError(f"top must be finite, got {top!r}")
    baselines = [top - index * line_gap for index in range(count)]
    if baselines and baselines[-1] < MIN_BASELINE_Y:
        raise ValueError(
            f"{count} lines from top={top!r} at line_gap={line_gap!r} reach baseline "
            f"y={baselines[-1]!r}, below the {MIN_BASELINE_Y!r} pt bottom margin; "
            "split the content over more pages"
        )
    return baselines


def text_page(
    lines: Sequence[str],
    *,
    x: float = 50.0,
    top: float = 780.0,
    line_gap: float = 14.0,
) -> Page:
    """A page of free-text lines, line ``k`` with its baseline at ``top - k * line_gap``.

    An empty string is a blank line: it draws nothing but still takes its line. Each line is one
    text object, so pdfplumber splits its words at the spaces. Suited to notes pages
    (``"Number of ordinary shares in issue 40,065,428,000"``) and to free-text tables such as the
    CBK bulletin's ``"Table 2: Foreign Exchange Reserves (USD Million)"``, its date row
    ``"08-Jan-26 15-Jan-26 22-Jan-26 29-Jan-26 05-Feb-26"`` and the
    ``"1. CBK Foreign Exchange Reserves (USD Million) ..."`` value row. A page reconstructing a
    third-party release carries a provenance line, for example
    ``"Reconstructed test fixture — Source: Central Bank of Kenya"``.

    Raises ``ValueError`` when ``line_gap`` is not positive or the last line would fall below
    ``MIN_BASELINE_Y``.
    """
    baselines = _baselines(top, line_gap, len(lines))
    return Page(
        tuple(TextCell(line, x, y) for line, y in zip(lines, baselines, strict=True) if line)
    )


def statement_page(
    *,
    title: str,
    scale_note: str | None,
    columns: Sequence[tuple[str, float]],
    header_above: Sequence[tuple[str, float]] = (),
    rows: Sequence[StatementRow],
    label_x: float = 50.0,
    top: float = 780.0,
    line_gap: float = 16.0,
) -> Page:
    """A financial-statement page laid out in columns, one line every ``line_gap`` points down.

    Lines, from ``top``:

    1. ``title`` at ``label_x``; the parser classifies pages by it ("Consolidated statement of
       profit or loss", "Statement of financial position", "Statement of cash flows");
    2. ``scale_note`` at ``label_x`` unless ``None`` ("KShs '000", "KShs ’000", "Kshs million",
       "USD '000"), where the parser looks for scale and currency markers;
    3. the ``header_above`` cells, each ``(text, x)``, when any has text, for entity headers such
       as ``[("Group", 300.0), ("Company", 440.0)]`` above the years;
    4. the ``columns`` header line, each ``(header_text, x)``. An empty header text marks a value
       position without a header token; when every header text is empty the line is left out and
       takes no space, which yields the "numbers without a header row" layout;
    5. one line per row: its label at ``label_x`` and ``values[j]`` at ``columns[j][1]``, empty
       values skipped.

    Values are drawn left-aligned at their column's x, so each token starts exactly where its
    header starts and overlaps that header's x-range. Callers therefore space columns wider than
    the widest value and keep labels clear of the first column. At Helvetica 9 pt a digit is
    5.004 pt wide, a comma or period 2.502 pt and a parenthesis 2.997 pt: "2025" is about 20.0 pt,
    "1,234,567" about 40.0 pt and "40,065,428,000" about 62.6 pt. Values must not contain spaces,
    because ``extract_words()`` splits at whitespace; labels may.

    Header texts may repeat. Group and Company columns over two years each::

        statement_page(
            title="Consolidated statement of profit or loss",
            scale_note="KShs '000",
            header_above=[("Group", 300.0), ("Company", 440.0)],
            columns=[("2025", 300.0), ("2024", 370.0), ("2025", 440.0), ("2024", 510.0)],
            rows=[StatementRow("Revenue", ["1,234,567", "1,100,000", "934,000", "880,000"])],
        )

    A prior-year-first layout is ``columns=[("2024", 400.0), ("2025", 480.0)]``.

    Raises ``ValueError`` when column x values do not ascend strictly from beyond ``label_x``,
    a column or ``header_above`` x lies off the page, a row's value count differs from the column
    count, ``line_gap`` is not positive, or the last line would fall below ``MIN_BASELINE_Y``.
    """
    if not math.isfinite(label_x):
        raise ValueError(f"label_x must be finite, got {label_x!r}")
    previous_x = label_x
    for header, x in columns:
        if not math.isfinite(x) or not x < A4_WIDTH:
            raise ValueError(f"column {header!r} at x={x!r} lies outside the page")
        if x <= previous_x:
            raise ValueError(
                f"column {header!r} at x={x!r} must lie right of x={previous_x!r}: column x "
                "values must ascend strictly and exceed label_x"
            )
        previous_x = x
    for text, x in header_above:
        if not math.isfinite(x) or not 0 <= x < A4_WIDTH:
            raise ValueError(f"header_above {text!r} at x={x!r} lies outside the page")
    for row in rows:
        if len(row.values) != len(columns):
            raise ValueError(
                f"row {row.label!r} has {len(row.values)} values for {len(columns)} columns"
            )

    lines: list[list[tuple[str, float]]] = [[(title, label_x)]]
    if scale_note is not None:
        lines.append([(scale_note, label_x)])
    entity_headers = [(text, x) for text, x in header_above if text]
    if entity_headers:
        lines.append(entity_headers)
    column_headers = [(text, x) for text, x in columns if text]
    if column_headers:
        lines.append(column_headers)
    for row in rows:
        cells = [(row.label, label_x)]
        cells.extend((value, x) for value, (_, x) in zip(row.values, columns, strict=True) if value)
        lines.append(cells)

    baselines = _baselines(top, line_gap, len(lines))
    return Page(
        tuple(
            TextCell(text, x, y)
            for line, y in zip(lines, baselines, strict=True)
            for text, x in line
        )
    )
