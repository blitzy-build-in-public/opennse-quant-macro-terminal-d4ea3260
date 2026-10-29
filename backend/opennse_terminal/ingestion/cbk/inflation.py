"""CBK inflation-rates pipeline (``cbk_inflation``): 12-month inflation into
``terminal_macro_indicators.inflation_yoy_pct``.

Source: Central Bank of Kenya, the public page ``https://www.centralbank.go.ke/inflation-rates/``.
The page is server-rendered, so one ``GET`` through ``PoliteClient`` (after its ``robots.txt``
check) returns the whole series; no browser is needed.

Table selection: the first ``<table>`` whose header cells, normalized with
``base.normalize_header``, are exactly ``year | month | annual average inflation | 12-month
inflation``, in that order. No such table ends the run ``layout_changed`` with nothing written.

Rows: a blank ``Year`` cell carries the previous row's year forward; ``Month`` is a full or
abbreviated English name (``September``, ``Sep``, ``Sept``). Each row is dated at the month-end
of its reference month (September 2026 is stored at 2026-09-30). Only ``12-Month Inflation`` is
stored; ``Annual Average Inflation`` is not. A ``-`` or blank value cell is no value, never a
zero. The table runs from January 2005, newest first.

Unit: percentage points (``6.76`` means 6.76%), parsed from text to ``Decimal`` without
``float`` and quantized to NUMERIC(9,4) with ``ROUND_HALF_UP``. Sanity bounds: [-20, 100]; a
parse needs at least 100 rows. Conflict key: ``observation_date``; the value is COALESCE-merged
and ``source_by_field["inflation_yoy_pct"]`` becomes ``"cbk_inflation"``, so a CBK value
replaces a seeded one for the same month.
"""

import logging
import uuid
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import ClassVar, Final

from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.ingestion.base import Pipeline, RunContext, ValidationOutcome
from opennse_terminal.ingestion.cbk import (
    COLUMN_QUANTUM,
    FetchedPage,
    MacroRecord,
    extract_table,
    is_missing_cell,
    month_end,
    parse_month,
    parse_value_cell,
    parse_year,
    quantize_to,
    upsert_macro_records,
    validate_macro_records,
)
from opennse_terminal.models.macro import MACRO_INDICATOR_COLUMNS

__all__ = [
    "BOUNDS",
    "EXPECTED_HEADERS",
    "INFLATION_URL",
    "MIN_ROWS",
    "OWNED_COLUMNS",
    "VALUE_HEADER",
    "CbkInflationPipeline",
    "parse_inflation_table",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk.inflation")

INFLATION_URL: Final = "https://www.centralbank.go.ke/inflation-rates/"
"""The CBK page holding the inflation table."""

EXPECTED_HEADERS: Final = ("year", "month", "annual average inflation", "12-month inflation")
"""The table's header cells after ``base.normalize_header``, compared exactly and in order."""

VALUE_HEADER: Final = "12-month inflation"
"""The header of the one stored column."""

OWNED_COLUMNS: Final = ("inflation_yoy_pct",)
"""The ``terminal_macro_indicators`` columns this pipeline writes."""

BOUNDS: Final[Mapping[str, tuple[Decimal, Decimal]]] = MappingProxyType(
    {"inflation_yoy_pct": (Decimal(-20), Decimal(100))}
)
"""Inclusive sanity bounds in percentage points, wide enough for the full published history."""

MIN_ROWS: Final = 100
"""Fewest parsed rows a run accepts; the full table holds more than 250."""

# A column the model no longer has would fail every upsert; fail at import instead.
if not set(OWNED_COLUMNS) <= set(MACRO_INDICATOR_COLUMNS):
    raise RuntimeError(
        f"OWNED_COLUMNS {OWNED_COLUMNS!r} are not all in "
        f"models.macro.MACRO_INDICATOR_COLUMNS {MACRO_INDICATOR_COLUMNS!r}"
    )

_COLUMN: Final = OWNED_COLUMNS[0]
_QUANTUM: Final = COLUMN_QUANTUM[_COLUMN]
_YEAR_INDEX: Final = EXPECTED_HEADERS.index("year")
_MONTH_INDEX: Final = EXPECTED_HEADERS.index("month")
_VALUE_INDEX: Final = EXPECTED_HEADERS.index(VALUE_HEADER)


def parse_inflation_table(html: str) -> list[MacroRecord]:
    """Parse the CBK inflation page into one :class:`MacroRecord` per month, without I/O.

    Each body row becomes a record dated ``month_end(year, month)`` whose ``values`` hold
    ``inflation_yoy_pct`` from the ``12-Month Inflation`` cell, in percentage points quantized to
    4 decimal places. Rows whose value cell is missing (``-`` or blank) and that have no error
    are dropped. Problems are returned on the record for ``validate`` to reject, never raised:

    - ``short_row``: fewer than four cells;
    - ``invalid_year:<text>``: a non-blank year cell that is not a year from 1900 to 2100;
    - ``no_year``: a blank year cell with no year to carry forward, either before the first year
      or after an invalid one (which is never skipped over, so no row is dated by the wrong year);
    - ``unknown_month:<text>``: a month cell that is not a month name or number;
    - ``unparsable:inflation_yoy_pct:<text>``: a value cell that is not a finite number.

    Raises:
        SourceLayoutChanged: No table with :data:`EXPECTED_HEADERS`.
    """
    table = extract_table(html, EXPECTED_HEADERS)
    records: list[MacroRecord] = []
    carried_year: int | None = None
    for index, cells in table.rows:
        errors: list[str] = []
        if len(cells) < len(EXPECTED_HEADERS):
            errors.append("short_row")

        # A short row's year still moves the carried year, so blank-year rows after it are not
        # dated by an older one.
        year_text = cells[_YEAR_INDEX] if len(cells) > _YEAR_INDEX else ""
        if year_text:
            carried_year = parse_year(year_text)
            if carried_year is None:
                errors.append(f"invalid_year:{year_text}")
        elif carried_year is None:
            errors.append("no_year")

        month: int | None = None
        if len(cells) > _MONTH_INDEX:
            month_text = cells[_MONTH_INDEX]
            month = parse_month(month_text)
            if month is None:
                errors.append(f"unknown_month:{month_text}")

        values: dict[str, Decimal] = {}
        if len(cells) > _VALUE_INDEX:
            value_text = cells[_VALUE_INDEX]
            if not is_missing_cell(value_text):
                try:
                    values[_COLUMN] = quantize_to(parse_value_cell(value_text), _QUANTUM)
                except ValueError:
                    errors.append(f"unparsable:{_COLUMN}:{value_text}")

        if not values and not errors:
            continue
        observation_date: date | None = None
        if carried_year is not None and month is not None:
            observation_date = month_end(carried_year, month)
        records.append(
            MacroRecord(
                observation_date=observation_date,
                values=values,
                errors=tuple(errors),
                raw=dict(zip(table.headers, cells, strict=False)),
                line=index,
            )
        )
    return records


class CbkInflationPipeline(Pipeline[FetchedPage, MacroRecord]):
    """Load CBK 12-month inflation into ``terminal_macro_indicators.inflation_yoy_pct``.

    Fetches :data:`INFLATION_URL` once through the run's ``PoliteClient``, parses it with
    :func:`parse_inflation_table`, rejects records outside :data:`BOUNDS`, and COALESCE-merges
    the rest on ``observation_date`` with ``source_by_field["inflation_yoy_pct"] =
    "cbk_inflation"``. Fewer than :data:`MIN_ROWS` parsed rows, or more than 10% rejected, ends
    the run ``layout_changed`` with nothing written. Source: Central Bank of Kenya.
    """

    name: ClassVar[str] = "cbk_inflation"
    min_rows: ClassVar[int] = MIN_ROWS
    source_url: str | None = INFLATION_URL

    async def fetch(self, ctx: RunContext) -> FetchedPage:
        """GET the inflation page: one request, after the client's ``robots.txt`` check.

        Raises:
            RobotsDisallowed: ``robots.txt`` blocks the page.
            FetchFailed: The page could not be fetched after the client's retries.
        """
        # Recorded before the request, so a failed or blocked fetch still names the page.
        ctx.details["source_url"] = INFLATION_URL
        html = await ctx.http.get_text(INFLATION_URL)
        return FetchedPage(url=INFLATION_URL, html=html)

    def parse(self, raw: FetchedPage) -> list[MacroRecord]:
        """Parse the fetched page with :func:`parse_inflation_table`.

        Raises:
            SourceLayoutChanged: The page has no table with :data:`EXPECTED_HEADERS`.
        """
        records = parse_inflation_table(raw.html)
        latest = max(
            (record.observation_date for record in records if record.observation_date is not None),
            default=None,
        )
        logger.info(
            "parsed %d CBK inflation records; latest observation %s",
            len(records),
            latest.isoformat() if latest is not None else "none",
        )
        return records

    def validate(self, records: Sequence[MacroRecord]) -> ValidationOutcome[MacroRecord]:
        """Reject records with parse errors, no date, or a value outside :data:`BOUNDS`."""
        return validate_macro_records(records, BOUNDS)

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[MacroRecord], run_id: uuid.UUID
    ) -> int:
        """COALESCE-merge the accepted records into ``terminal_macro_indicators`` on
        ``observation_date`` and return the rows written. ``terminal_macro_indicators`` carries
        no run id, so ``run_id`` is not stored; the run's transaction commits."""
        return await upsert_macro_records(conn, records, source=self.name)
