"""``cbk_rates_csv``: the Central Bank of Kenya "Central Bank Rates" CSV.

Source: Central Bank of Kenya, public statistical release.

Discovery. The listing page :data:`INTEREST_RATES_PAGE_URL` links the file as
``/uploads/interest_rates/<id>_Central Bank Rates_with Heads.csv``. The numeric id changes with
every upload, so the link is found by its anchor text "Central Bank Rates" and its ``.csv``
suffix (``cbk.find_link``), never hard-coded. A run makes exactly two requests after the shared
``robots.txt`` check, both through the run's ``PoliteClient``: the listing page, then the file.

Layout. Line 1 is a title; line 2 is the header
``YEAR,MONTH,Repo,Reverse Repo,Interbank Rate,91-Day Tbill,182-days Tbill,364-days Tbill,
Cash Reserve Requirement,Central Bank Rate``. Every cell is read as text (``cbk.read_cbk_csv``),
and the footer ("Source: Central Bank of Kenya") and blank rows are dropped. ``YEAR`` appears
only on the first month of each year and is forward-filled; ``MONTH`` is a full or three-letter
name (``Sept`` too), and each row is dated at that month's last day. Data starts in July 1991.

Columns (percentage points: ``8.75`` means 8.75%), into ``terminal_macro_indicators``:

=====================  ======================  ============
CSV header             Column                  Bounds
=====================  ======================  ============
``Interbank Rate``     ``interbank_rate_pct``  [0, 100]
``91-Day Tbill``       ``tbill_91d_pct``       [0, 100]
``182-days Tbill``     ``tbill_182d_pct``      [0, 100]
``364-days Tbill``     ``tbill_364d_pct``      [0, 100]
``Central Bank Rate``  ``cbr_rate_pct``        [0, 30]
=====================  ======================  ============

Repo, Reverse Repo and Cash Reserve Requirement are read but not stored. A ``-`` or blank cell is
skipped, never written as null. The bounds are sanity limits wide enough for the full published
history (CBK lists 84.67 for the 91-day rate of July 1993) that catch parse, scale and
column-shift errors. Cells go from ``str`` to ``Decimal`` without ``float`` and are quantized to
NUMERIC(9,4) with ``ROUND_HALF_UP``.

A run with a missing header, fewer than :data:`MIN_ROWS` parsed rows or more than 10% rejected
rows ends ``layout_changed`` and writes nothing. Rows are COALESCE-merged on the conflict key
``observation_date``: each of the five columns takes CBK's value, replacing a seeded one, and its
``source_by_field`` entry becomes ``"cbk_rates_csv"``; other columns of the row are untouched, so
a re-run changes no value.
"""

import logging
import uuid
from collections.abc import Mapping, Sequence
from datetime import date
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
    "COLUMN_MAP",
    "CSV_HEADER_ROW",
    "INTEREST_RATES_PAGE_URL",
    "LINK_SUFFIX",
    "LINK_TEXT",
    "MIN_ROWS",
    "OWNED_COLUMNS",
    "REQUIRED_HEADERS",
    "CbkRatesCsvPipeline",
    "parse_rates_csv",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk.rates_csv")

INTEREST_RATES_PAGE_URL: Final = "https://www.centralbank.go.ke/statistics/interest-rates/"
"""The CBK interest-rates listing page that links the CSV."""

LINK_TEXT: Final = "Central Bank Rates"
"""Anchor text of the CSV link on the listing page."""

LINK_SUFFIX: Final = ".csv"
"""Path suffix of the CSV link."""

CSV_HEADER_ROW: Final = 1
"""0-based line of the CSV header: the second line, below the title."""

COLUMN_MAP: Final[Mapping[str, str]] = MappingProxyType(
    {
        "interbank rate": "interbank_rate_pct",
        "91-day tbill": "tbill_91d_pct",
        "182-days tbill": "tbill_182d_pct",
        "364-days tbill": "tbill_364d_pct",
        "central bank rate": "cbr_rate_pct",
    }
)
"""Normalized CSV header to ``terminal_macro_indicators`` column, for the stored series."""

REQUIRED_HEADERS: Final = ("year", "month", *COLUMN_MAP)
"""Normalized headers the file must carry; any one missing ends the run ``layout_changed``."""

OWNED_COLUMNS: Final = tuple(COLUMN_MAP.values())
"""The ``terminal_macro_indicators`` columns this pipeline writes."""

_PCT_BOUNDS: Final = (Decimal(0), Decimal(100))

BOUNDS: Final[Mapping[str, tuple[Decimal, Decimal]]] = MappingProxyType(
    {
        "interbank_rate_pct": _PCT_BOUNDS,
        "tbill_91d_pct": _PCT_BOUNDS,
        "tbill_182d_pct": _PCT_BOUNDS,
        "tbill_364d_pct": _PCT_BOUNDS,
        "cbr_rate_pct": (Decimal(0), Decimal(30)),
    }
)
"""Inclusive sanity bounds per column, in percentage points."""

MIN_ROWS: Final = 300
"""Fewest parsed rows a run accepts; the full file holds more than 400 months."""

# A renamed model column or a column without bounds would otherwise surface only as rejected
# rows (unexpected_column) at run time; fail at import instead.
_UNKNOWN_COLUMNS: Final = [
    column for column in OWNED_COLUMNS if column not in MACRO_INDICATOR_COLUMNS
]
if _UNKNOWN_COLUMNS:
    raise RuntimeError(
        f"cbk_rates_csv columns {_UNKNOWN_COLUMNS!r} are not in "
        f"models.macro.MACRO_INDICATOR_COLUMNS {list(MACRO_INDICATOR_COLUMNS)!r}"
    )
if set(BOUNDS) != set(OWNED_COLUMNS):
    raise RuntimeError(
        f"cbk_rates_csv BOUNDS keys {sorted(BOUNDS)!r} do not match "
        f"its owned columns {sorted(OWNED_COLUMNS)!r}"
    )


def parse_rates_csv(data: bytes) -> list[MacroRecord]:
    """Parse the "Central Bank Rates" CSV into one :class:`MacroRecord` per data row, in file
    order and without I/O.

    Each record is dated at the month-end of its ``YEAR`` and ``MONTH``, the year forward-filled
    from the last row that states one, and carries the parsed values of the five stored columns,
    quantized to NUMERIC(9,4). A ``-`` or blank cell is left out of ``values``; a row whose five
    cells are all missing and that has no error is dropped. Problems become ``errors`` for
    ``validate`` to reject, with ``observation_date`` ``None`` when the row cannot be dated:

    - ``no_year``: no year is in force (before the first stated year, or after one of the next
      two errors);
    - ``invalid_year:<text>``: the year cell is not a four-digit year; it is not carried forward;
    - ``month_out_of_sequence:<text>``: a row without a year cell whose month does not follow the
      previous row's month;
    - ``unknown_month:<text>``: the month cell is not a month name or number;
    - ``unparsable:<column>:<text>``: a value cell is not a finite number.

    Duplicate dates stated with an explicit year are kept; the macro upsert keeps the last.

    Raises:
        SourceLayoutChanged: A header of :data:`REQUIRED_HEADERS` is missing ("interest-rate CSV
            headers changed"), or the file is not readable CSV text.
    """
    table = read_cbk_csv(data, header_row=CSV_HEADER_ROW)
    if any(header not in table.headers for header in REQUIRED_HEADERS):
        raise SourceLayoutChanged(
            expected=list(REQUIRED_HEADERS),
            found=list(table.headers),
            message="interest-rate CSV headers changed",
        )

    records: list[MacroRecord] = []
    current_year: int | None = None
    # Month of the last dated row under current_year; None right after a stated year.
    previous_month: int | None = None
    for line, row in table.rows:
        errors: list[str] = []
        year_text = row["year"]
        month_text = row["month"]
        month = parse_month(month_text)

        if year_text:
            current_year = parse_year(year_text)
            previous_month = None
            if current_year is None:
                errors.append(f"invalid_year:{year_text}")
        elif current_year is None:
            errors.append("no_year")
        elif month is not None and previous_month is not None and month <= previous_month:
            # A forward-filled year is only as good as the next stated one: a January row that
            # lost its YEAR cell would otherwise date twelve months in the previous year and
            # overwrite that year's stored values. The carried year is dropped until a row
            # states one again.
            errors.append(f"month_out_of_sequence:{month_text}")
            current_year = None
        if month is None:
            errors.append(f"unknown_month:{month_text}")

        observation_date: date | None = None
        if current_year is not None and month is not None:
            observation_date = month_end(current_year, month)
            previous_month = month

        values: dict[str, Decimal] = {}
        for header, column in COLUMN_MAP.items():
            cell = row[header]
            if is_missing_cell(cell):
                continue
            try:
                values[column] = quantize_to(parse_value_cell(cell), COLUMN_QUANTUM[column])
            except ValueError:
                errors.append(f"unparsable:{column}:{cell}")

        if not values and not errors:
            continue
        records.append(
            MacroRecord(
                observation_date=observation_date,
                values=values,
                errors=tuple(errors),
                raw=row,
                line=line,
            )
        )
    logger.debug("parsed %d interest-rate records from %d CSV rows", len(records), len(table.rows))
    return records


class CbkRatesCsvPipeline(Pipeline[FetchedFile, MacroRecord]):
    """Load the CBK "Central Bank Rates" CSV into ``terminal_macro_indicators``.

    Fetches the listing page and the CSV it links through ``ctx.http``, parses it with
    :func:`parse_rates_csv`, validates against :data:`BOUNDS` and COALESCE-merges the interbank
    rate, the 91/182/364-day T-bill rates and the Central Bank Rate on ``observation_date``,
    recording ``"cbk_rates_csv"`` in ``source_by_field`` for each. Source: Central Bank of Kenya.
    """

    name: ClassVar[str] = "cbk_rates_csv"
    min_rows: ClassVar[int] = MIN_ROWS
    source_url: str | None = INTEREST_RATES_PAGE_URL

    async def fetch(self, ctx: RunContext) -> FetchedFile:
        """Fetch the listing page, discover the CSV link by its anchor text and download it.

        The discovered link is recorded in ``ctx.details`` (``listing_url``, ``source_url``) and
        as the run's ``source_url`` before the download, so a failed download names the file.

        Raises:
            SourceLayoutChanged: The listing page has no "Central Bank Rates" ``.csv`` link on
                the CBK origin.
            RobotsDisallowed, FetchFailed: From ``PoliteClient``.
        """
        page = await ctx.http.get_text(INTEREST_RATES_PAGE_URL)
        csv_url = find_link(
            page, INTEREST_RATES_PAGE_URL, text_contains=LINK_TEXT, href_suffix=LINK_SUFFIX
        )
        ctx.details["listing_url"] = INTEREST_RATES_PAGE_URL
        ctx.details["source_url"] = csv_url
        self.source_url = csv_url
        logger.info("cbk_rates_csv: discovered CSV %s", describe_url(csv_url))
        data = await ctx.http.get_bytes(csv_url)
        return FetchedFile(url=csv_url, listing_url=INTEREST_RATES_PAGE_URL, content=data)

    def parse(self, raw: FetchedFile) -> list[MacroRecord]:
        """Parse the downloaded CSV with :func:`parse_rates_csv`."""
        records = parse_rates_csv(raw.content)
        logger.info(
            "cbk_rates_csv: parsed %d records from %s (%d bytes)",
            len(records),
            describe_url(raw.url),
            len(raw.content),
        )
        return records

    def validate(self, records: Sequence[MacroRecord]) -> ValidationOutcome[MacroRecord]:
        """Reject undated or unparsable records and values outside :data:`BOUNDS`."""
        return validate_macro_records(records, BOUNDS)

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[MacroRecord], run_id: uuid.UUID
    ) -> int:
        """COALESCE-merge the records on ``observation_date`` with source ``cbk_rates_csv``.

        Macro rows carry no run id, so ``run_id`` is not written.
        """
        return await upsert_macro_records(conn, records, source=self.name)
