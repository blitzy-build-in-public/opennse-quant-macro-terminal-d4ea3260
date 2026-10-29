"""CBK end-period exchange-rate pipeline (``cbk_fx``): KES per US dollar at each month-end.

Source: Central Bank of Kenya, the public "Exchange Rates End Period" statistical release.

- **Listing page:** ``https://www.centralbank.go.ke/statistics/exchange-rates/``, fetched through
  the run's ``PoliteClient`` (``robots.txt``, per-host spacing, retries, size cap).
- **Discovery:** the CSV is the link whose anchor text contains "Exchange Rates End Period" and
  whose path ends in ``.csv``, on the CBK origin (``cbk.find_link``). Its file name carries an
  upload id that changes, for example
  ``/uploads/exchange_rates/<id>_Monthly exchange rate (end period).csv``, so the link is never
  hard-coded. A run makes exactly two requests after the ``robots.txt`` check: the listing page
  and the CSV.
- **File layout:** a title line, then the header on row 2 (``Year,Month,United States dollar,
  Sterling pound,Euro,...``), then one row per month from January 1993. Only ``Year``, ``Month``
  and ``United States dollar`` are read; every other currency column is ignored. A blank
  ``Year`` takes the last valid year above it; ``Month`` is a number 1-12 (names are accepted
  too). A missing rate (``-`` or blank) is skipped, never written as null.
- **Target:** ``terminal_macro_indicators.fx_kes_usd`` (NUMERIC(12,4)) in KES per one USD,
  dated at the month-end of the reference month and quantized to 4 dp with ``ROUND_HALF_UP``.
- **Validation:** at least 300 parsed rows, and every rate within [10, 1,000] KES per USD: a
  sanity bound wide enough for the full published history (the series starts at 35.92 in
  January 1993) that catches parse, scale and column-shift errors.
- **Write:** COALESCE-merged on the conflict key ``observation_date``. Only ``fx_kes_usd`` is
  set and its ``source_by_field`` entry becomes ``"cbk_fx"``, so CBK's rate replaces a seeded
  one for the same date (the seeded 2026-09-30 rate 129.20 becomes CBK's 129.79), other
  writers' columns are untouched, and a re-run is a value no-op.

Cells go from ``str`` to ``Decimal`` through ``cbk.parse_value_cell``, never through ``float``.
Importing this module does not import pandas; ``cbk.read_cbk_csv`` does when a file is parsed.
"""

import logging
import uuid
from collections.abc import Mapping, Sequence
from decimal import Decimal
from types import MappingProxyType
from typing import ClassVar, Final

from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.ingestion.base import (
    Pipeline,
    RunContext,
    SourceLayoutChanged,
    ValidationOutcome,
)
from opennse_terminal.ingestion.cbk import (
    COLUMN_QUANTUM,
    FetchedFile,
    MacroRecord,
    clean_text,
    find_link,
    is_missing_cell,
    month_end,
    parse_month,
    parse_value_cell,
    parse_year,
    quantize_to,
    read_cbk_csv,
    upsert_macro_records,
    validate_macro_records,
)
from opennse_terminal.ingestion.http import describe_url
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "BOUNDS",
    "EXCHANGE_RATES_PAGE_URL",
    "FX_COLUMN",
    "HEADER_ROW",
    "LINK_SUFFIX",
    "LINK_TEXT",
    "MIN_ROWS",
    "OWNED_COLUMNS",
    "REQUIRED_HEADERS",
    "USD_HEADER",
    "CbkFxPipeline",
    "parse_fx_csv",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk.fx")

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------

EXCHANGE_RATES_PAGE_URL: Final = "https://www.centralbank.go.ke/statistics/exchange-rates/"
"""The CBK exchange-rates listing page the CSV link is discovered on."""

LINK_TEXT: Final = "Exchange Rates End Period"
"""Anchor text of the end-period CSV link (the page also lists an "Average Period" file)."""

LINK_SUFFIX: Final = ".csv"
"""Required ending of the discovered link's path."""

HEADER_ROW: Final = 1
"""0-based line of the CSV header: the file opens with a title line, so the header is row 2."""

USD_HEADER: Final = "united states dollar"
"""Normalized header of the KES-per-USD column; the other currency columns are ignored."""

REQUIRED_HEADERS: Final = ("year", "month", USD_HEADER)
"""Normalized headers the CSV must carry, or the run ends ``layout_changed``."""

FX_COLUMN: Final = "fx_kes_usd"
"""The ``terminal_macro_indicators`` column this pipeline writes, in KES per one USD."""

OWNED_COLUMNS: Final = (FX_COLUMN,)
"""Every column this pipeline writes; ``source_by_field`` records ``"cbk_fx"`` for each."""

BOUNDS: Final[Mapping[str, tuple[Decimal, Decimal]]] = MappingProxyType(
    {FX_COLUMN: (Decimal(10), Decimal(1000))}
)
"""Inclusive sanity range per owned column, in KES per USD (the series starts at 35.92)."""

MIN_ROWS: Final = 300
"""Fewest parsed rows a run accepts; the complete file holds one row per month since 1993."""

# A column the model no longer has would fail every upsert; fail at import instead.
if not set(OWNED_COLUMNS) <= set(MACRO_INDICATOR_COLUMNS):
    raise RuntimeError(
        f"cbk_fx OWNED_COLUMNS {list(OWNED_COLUMNS)!r} are not all in "
        f"models.macro.MACRO_INDICATOR_COLUMNS {list(MACRO_INDICATOR_COLUMNS)!r}"
    )


# ---------------------------------------------------------------------------------------------
# Pure parser
# ---------------------------------------------------------------------------------------------


def parse_fx_csv(data: bytes) -> list[MacroRecord]:
    """Parse the CBK "Exchange Rates End Period" CSV into one record per month, without I/O.

    The header is on row 2 (:data:`HEADER_ROW`); footer rows such as "Source: Central Bank of
    Kenya" and blank rows are dropped by ``cbk.read_cbk_csv``. For each data row, in file order:

    - **Year.** A non-blank ``Year`` cell read by ``cbk.parse_year`` becomes the current year;
      an unreadable one adds ``invalid_year:<text>`` and is not carried to later rows. A blank
      cell takes the current year, or adds ``no_year`` when no year has been seen yet.
    - **Month.** A number 1-12 or a month name (``cbk.parse_month``), else
      ``unknown_month:<text>``.
    - **Date.** ``observation_date`` is the month-end of that year and month, or ``None`` when
      either is unresolved.
    - **Rate.** The ``United States dollar`` cell in KES per USD: a missing cell (``-`` or
      blank) is skipped; otherwise it is read exactly and quantized to 4 dp with
      ``ROUND_HALF_UP``, and an unreadable cell adds ``unparsable:fx_kes_usd:<text>``.

    A row without a rate and without errors is dropped. Every other row becomes a
    :class:`~opennse_terminal.ingestion.cbk.MacroRecord` carrying the row's cells and file line;
    rows with errors are rejected by validation, not here.

    Raises:
        SourceLayoutChanged: The bytes cannot be read as a CSV, or a header in
            :data:`REQUIRED_HEADERS` is missing; ``found`` lists the file's headers.
    """
    table = read_cbk_csv(data, header_row=HEADER_ROW)
    if any(header not in table.headers for header in REQUIRED_HEADERS):
        raise SourceLayoutChanged(
            expected=list(REQUIRED_HEADERS),
            found=list(table.headers),
            message="exchange-rate CSV headers changed",
        )

    quantum = COLUMN_QUANTUM[FX_COLUMN]
    records: list[MacroRecord] = []
    current_year: int | None = None
    for line, row in table.rows:
        errors: list[str] = []

        year_text = clean_text(row["year"])
        year: int | None
        if year_text:
            year = parse_year(year_text)
            if year is None:
                errors.append(f"invalid_year:{year_text}")
            else:
                current_year = year
        else:
            year = current_year
            if year is None:
                errors.append("no_year")

        month_text = clean_text(row["month"])
        month = parse_month(month_text)
        if month is None:
            errors.append(f"unknown_month:{month_text}")

        values: dict[str, Decimal] = {}
        cell = row[USD_HEADER]
        if not is_missing_cell(cell):
            try:
                values[FX_COLUMN] = quantize_to(parse_value_cell(cell), quantum)
            except ValueError:
                errors.append(f"unparsable:{FX_COLUMN}:{clean_text(cell)}")

        if not values and not errors:
            continue
        records.append(
            MacroRecord(
                observation_date=(
                    month_end(year, month) if year is not None and month is not None else None
                ),
                values=values,
                errors=tuple(errors),
                raw=row,
                line=line,
            )
        )
    return records


# ---------------------------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------------------------


class CbkFxPipeline(Pipeline[FetchedFile, MacroRecord]):
    """``cbk_fx``: CBK end-period KES/USD rates into ``terminal_macro_indicators.fx_kes_usd``.

    Run by the scheduler's ``cbk_macro`` job and by ``opennse-terminal ingest cbk_fx`` or
    ``ingest cbk``; constructed as ``CbkFxPipeline(settings)``. Source: Central Bank of Kenya.
    """

    name: ClassVar[str] = "cbk_fx"
    min_rows: ClassVar[int] = MIN_ROWS
    source_url: str | None = EXCHANGE_RATES_PAGE_URL

    async def fetch(self, ctx: RunContext) -> FetchedFile:
        """Fetch the exchange-rates listing page, discover the end-period CSV link on it and
        download the file: two requests through ``ctx.http`` after its ``robots.txt`` check.

        ``ctx.details`` records ``listing_url`` and, once discovered, ``source_url`` (the CSV),
        so a failed download still names the file it tried.

        Raises:
            SourceLayoutChanged: The page has no matching link on the CBK origin.
            FetchFailed, RobotsDisallowed: From ``PoliteClient``.
        """
        ctx.details["listing_url"] = EXCHANGE_RATES_PAGE_URL
        page = await ctx.http.get_text(EXCHANGE_RATES_PAGE_URL)
        csv_url = find_link(
            page, EXCHANGE_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=LINK_SUFFIX
        )
        ctx.details["source_url"] = csv_url
        logger.info("cbk_fx: end-period exchange-rate CSV discovered at %s", describe_url(csv_url))
        content = await ctx.http.get_bytes(csv_url)
        return FetchedFile(url=csv_url, listing_url=EXCHANGE_RATES_PAGE_URL, content=content)

    def parse(self, raw: FetchedFile) -> list[MacroRecord]:
        """Parse the downloaded CSV with :func:`parse_fx_csv`."""
        records = parse_fx_csv(raw.content)
        logger.info("cbk_fx: parsed %d records from %s", len(records), describe_url(raw.url))
        return records

    def validate(self, records: Sequence[MacroRecord]) -> ValidationOutcome[MacroRecord]:
        """Reject undated records, records with parse errors and rates outside :data:`BOUNDS`."""
        return validate_macro_records(records, BOUNDS)

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[MacroRecord], run_id: uuid.UUID
    ) -> int:
        """COALESCE-merge ``fx_kes_usd`` on ``observation_date`` with ``source_by_field``
        ``"cbk_fx"`` and return the rows written. Macro rows carry no run id, so ``run_id`` is
        not stored."""
        return await upsert_macro_records(conn, records, source=self.name)
