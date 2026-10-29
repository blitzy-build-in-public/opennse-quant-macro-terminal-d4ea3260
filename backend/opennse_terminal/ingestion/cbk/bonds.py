"""CBK Treasury bond auction results: the ``cbk_bonds`` pipeline (long-term leg of the yield curve).

Source: Central Bank of Kenya, https://www.centralbank.go.ke/bills-bonds/treasury-bonds/ -- a
public, server-rendered page, fetched once per run through the run's ``PoliteClient`` (after its
``robots.txt`` check, with per-host spacing and retries) and parsed with BeautifulSoup4. No browser
is needed, and no exchange or third-party API is used.

Table selection: the first ``<table>`` whose header cells, normalized with
``base.normalize_header``, are exactly :data:`EXPECTED_HEADERS`, in order: ``Issue Date | Issue No
| ISIN Number | Tenor | Face Value (Kshs Millions) | Maturity Date | Coupon Rate | Redemption
Yield``. When no table matches, the run ends ``layout_changed`` and writes nothing.

Column map into ``terminal_bond_yields`` (rates in percentage points, tenors in years):

- ``Issue Date`` (``DD/MM/YYYY``) -> ``observation_date``, the auction issue date;
- ``Issue No``, trimmed -> ``bond_code`` (for example ``FXD1/2018/025``), at most 32 characters;
- ``ISIN Number`` -> ``isin``, optional; 12 characters when present;
- ``Tenor`` -> ``original_tenor_years``, the first number in the cell, 2 decimal places;
- ``Face Value (Kshs Millions)`` is read but not stored;
- ``Maturity Date`` (``DD/MM/YYYY``) -> ``maturity_date``;
- ``Coupon Rate`` -> ``coupon_rate_pct``, optional, 4 decimal places;
- ``Redemption Yield`` -> ``yield_pct``, 4 decimal places. A row without one (``-`` or blank, as
  for a tap sale) is skipped: it is neither a record nor a rejection, because the column is
  NOT NULL;
- derived: ``remaining_tenor_years = (maturity_date - observation_date).days / 365.25``, 2 decimal
  places.

Cells go from ``str`` to ``Decimal`` through ``cbk.parse_value_cell`` (``base.parse_decimal_text``),
never through ``float``, and are quantized with ``ROUND_HALF_UP`` to the column scales:
NUMERIC(9,4) for the rates and NUMERIC(6,2) for the tenors.

Every issue code is stored (FXD, IFB, SDB, MAB, ...); the yield curve later reads only codes that
begin with ``FXD``, because tax-exempt infrastructure bonds and special bonds are not comparable.
Rows are upserted on the conflict key ``(observation_date, bond_code)`` with ``DO UPDATE``, so a
re-run rewrites identical values. The pipeline writes ``terminal_bond_yields`` only.
"""

import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import ClassVar, Final

from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.db.upsert import upsert_rows
from opennse_terminal.ingestion.base import (
    Pipeline,
    RejectedRow,
    RunContext,
    ValidationOutcome,
    dedupe_by_key,
)
from opennse_terminal.ingestion.cbk import (
    TENOR_QUANTUM,
    UPSERT_CHUNK_ROWS,
    FetchedPage,
    clean_text,
    extract_table,
    is_missing_cell,
    parse_value_cell,
    quantize_to,
)
from opennse_terminal.models import TerminalBondYield

__all__ = [
    "BOND_SOURCE",
    "CONFLICT_KEY",
    "COUPON_BOUNDS",
    "DAYS_PER_YEAR",
    "EXPECTED_HEADERS",
    "ISIN_RE",
    "MAX_BOND_CODE_CHARS",
    "MAX_TENOR_YEARS",
    "MIN_ROWS",
    "RATE_QUANTUM",
    "TREASURY_BONDS_URL",
    "YIELD_BOUNDS",
    "BondRecord",
    "CbkBondsPipeline",
    "parse_bond_date",
    "parse_bonds_table",
    "parse_tenor_years",
]

logger = logging.getLogger("opennse_terminal.ingestion.cbk.bonds")

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------

TREASURY_BONDS_URL: Final = "https://www.centralbank.go.ke/bills-bonds/treasury-bonds/"
"""The public CBK page listing Treasury bond auction results."""

EXPECTED_HEADERS: Final = (
    "issue date",
    "issue no",
    "isin number",
    "tenor",
    "face value (kshs millions)",
    "maturity date",
    "coupon rate",
    "redemption yield",
)
"""The bond table's header cells after ``base.normalize_header``, exactly and in order."""

BOND_SOURCE: Final = "cbk_bonds"
"""The ``source`` written on every row."""

MIN_ROWS: Final = 50
"""Fewer parsed rows than this end the run ``layout_changed``."""

CONFLICT_KEY: Final = ("observation_date", "bond_code")
"""The upsert key, the table's unique constraint."""

DAYS_PER_YEAR: Final = Decimal("365.25")
"""Days per year in ``remaining_tenor_years``."""

RATE_QUANTUM: Final = Decimal("0.0001")
"""Scale of ``coupon_rate_pct`` and ``yield_pct`` (NUMERIC(9,4)), in percentage points."""

# Sanity limits for parse, scale and column-shift errors, wide enough for the full published
# table (CBK tenors run to 30 years); they also keep every value inside its NUMERIC column.
YIELD_BOUNDS: Final = (Decimal(0), Decimal(100))
"""``yield_pct`` must lie in ``(low, high]``: the low end is exclusive (the column's CHECK)."""

COUPON_BOUNDS: Final = (Decimal(0), Decimal(100))
"""``coupon_rate_pct``, when present, must lie in ``[low, high]``: zero-coupon bonds exist."""

MAX_TENOR_YEARS: Final = Decimal(100)
"""Both tenors must lie in ``(0, MAX_TENOR_YEARS]``."""

MAX_BOND_CODE_CHARS: Final = 32
"""Longest ``bond_code`` the column (VARCHAR(32)) holds."""

ISIN_RE: Final = re.compile(r"^[A-Z]{2}[0-9A-Z]{9}[0-9]$")
"""An ISIN: country code, nine alphanumerics and a check digit (12 characters, VARCHAR(12))."""

_DATE_RE: Final = re.compile(r"([0-9]{1,2})/([0-9]{1,2})/([0-9]{4})")
_NUMBER_RE: Final = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?")
_FXD_PREFIX: Final = "FXD"


# ---------------------------------------------------------------------------------------------
# Record
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BondRecord:
    """One parsed row of the CBK Treasury bond table.

    Attributes:
        observation_date: The issue date; ``None`` when the cell did not parse.
        bond_code: The trimmed issue number, for example ``FXD1/2018/025``; empty when missing.
        isin: The uppercased ISIN, or ``None`` when the cell is blank, a dash or invalid.
        original_tenor_years: The issue's nominal tenor in years, at 2 decimal places.
        remaining_tenor_years: ``(maturity_date - observation_date).days / 365.25`` at 2 decimal
            places, when both dates parsed.
        maturity_date: The maturity date; ``None`` when the cell did not parse.
        coupon_rate_pct: The coupon in percentage points at 4 decimal places; ``None`` when the
            cell is missing or did not parse.
        yield_pct: The redemption yield in percentage points at 4 decimal places; ``None`` only
            when the cell did not parse.
        errors: Machine-readable parse problems, for example ``"invalid_issue_date:31/02/2026"``
            or ``"unparsable:yield_pct:abc"``; a record with any is rejected by validation.
        raw: The source cells keyed by normalized header, recorded in ``details.rejected``.
        line: The 1-based body row of the source table, for diagnostics.
    """

    observation_date: date | None
    bond_code: str
    isin: str | None
    original_tenor_years: Decimal | None
    remaining_tenor_years: Decimal | None
    maturity_date: date | None
    coupon_rate_pct: Decimal | None
    yield_pct: Decimal | None
    errors: tuple[str, ...] = ()
    raw: Mapping[str, str] = field(default_factory=dict)
    line: int | None = None


# ---------------------------------------------------------------------------------------------
# Pure helpers and parser
# ---------------------------------------------------------------------------------------------


def parse_bond_date(text: str) -> date | None:
    """Read a ``DD/MM/YYYY`` date cell (one- or two-digit day and month, as in ``16/02/2026`` or
    ``1/7/2026``); return ``None`` for any other text or an impossible date such as
    ``31/02/2026``."""
    match = _DATE_RE.fullmatch(clean_text(text))
    if match is None:
        return None
    day, month, year = (int(part) for part in match.groups())
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_tenor_years(text: str) -> Decimal | None:
    """Read a tenor cell as years: the first number in it (``25``, ``25 Years``, ``15.0``),
    exactly, without rounding. Returns ``None`` when the cell holds no number."""
    match = _NUMBER_RE.search(clean_text(text))
    if match is None:
        return None
    return parse_value_cell(match.group())


def _parse_rate(text: str) -> Decimal | None:
    """A rate cell in percentage points at :data:`RATE_QUANTUM`, or ``None`` when unparsable."""
    try:
        return quantize_to(parse_value_cell(text), RATE_QUANTUM)
    except ValueError:
        return None


def _parse_tenor(text: str) -> Decimal | None:
    """A tenor cell in years at ``TENOR_QUANTUM``, or ``None`` when it holds no usable number."""
    try:
        tenor = parse_tenor_years(text)
        return None if tenor is None else quantize_to(tenor, TENOR_QUANTUM)
    except ValueError:
        return None


def _raw_cells(headers: Sequence[str], cells: Sequence[str]) -> dict[str, str]:
    """The source cells keyed by header; cells beyond the headers are kept as ``extra_<n>``."""
    raw = dict(zip(headers, cells, strict=False))
    for position, cell in enumerate(cells[len(headers) :], start=1):
        raw[f"extra_{position}"] = cell
    return raw


def parse_bonds_table(html: str) -> list[BondRecord]:
    """Parse the CBK Treasury bond page into one :class:`BondRecord` per table row.

    The table is the first whose normalized headers equal :data:`EXPECTED_HEADERS`. Each body
    row becomes a record; problems are recorded in ``errors`` rather than raised, so validation
    rejects the row and the run's thresholds apply:

    - fewer cells than headers: ``short_row``; non-blank cells beyond them: ``long_row``;
    - an Issue Date or Maturity Date that is not a valid ``DD/MM/YYYY`` date:
      ``invalid_issue_date:<text>`` or ``invalid_maturity_date:<text>``;
    - a blank Issue No: ``missing_bond_code``; one over 32 characters: ``bond_code_too_long``;
    - an ISIN that is neither blank, a dash nor a 12-character ISIN: ``invalid_isin:<text>``;
    - a Tenor without a number: ``invalid_tenor:<text>``;
    - an unparsable Coupon Rate or Redemption Yield: ``unparsable:coupon_rate_pct:<text>`` or
      ``unparsable:yield_pct:<text>``.

    A row whose Redemption Yield is missing (blank or a dash) is skipped, because the column is
    NOT NULL. A missing Coupon Rate is ``None``. Every issue code is kept. No I/O.

    Raises:
        SourceLayoutChanged: No table on the page has the expected headers (from
            ``cbk.extract_table``).
    """
    table = extract_table(html, EXPECTED_HEADERS)
    width = len(EXPECTED_HEADERS)
    records: list[BondRecord] = []
    skipped_without_yield = 0
    for line, cells in table.rows:
        raw = _raw_cells(table.headers, cells)
        if len(cells) < width:
            records.append(
                BondRecord(
                    observation_date=None,
                    bond_code=clean_text(cells[1]) if len(cells) > 1 else "",
                    isin=None,
                    original_tenor_years=None,
                    remaining_tenor_years=None,
                    maturity_date=None,
                    coupon_rate_pct=None,
                    yield_pct=None,
                    errors=("short_row",),
                    raw=raw,
                    line=line,
                )
            )
            continue
        issue_text, code_text, isin_text, tenor_text, _face_value, maturity_text = cells[:6]
        coupon_text, yield_text = cells[6], cells[7]
        if is_missing_cell(yield_text):
            skipped_without_yield += 1
            continue

        errors: list[str] = []
        # Values beyond the eight headers mean the cells may be shifted; trailing blanks do not.
        if any(cells[width:]):
            errors.append("long_row")

        observation_date = parse_bond_date(issue_text)
        if observation_date is None:
            errors.append(f"invalid_issue_date:{issue_text}")

        bond_code = clean_text(code_text)
        if not bond_code:
            errors.append("missing_bond_code")
        elif len(bond_code) > MAX_BOND_CODE_CHARS:
            errors.append("bond_code_too_long")

        isin: str | None = None
        if not is_missing_cell(isin_text):
            candidate = clean_text(isin_text).upper()
            if ISIN_RE.fullmatch(candidate) is None:
                errors.append(f"invalid_isin:{isin_text}")
            else:
                isin = candidate

        original_tenor = _parse_tenor(tenor_text)
        if original_tenor is None:
            errors.append(f"invalid_tenor:{tenor_text}")

        maturity_date = parse_bond_date(maturity_text)
        if maturity_date is None:
            errors.append(f"invalid_maturity_date:{maturity_text}")

        coupon: Decimal | None = None
        if not is_missing_cell(coupon_text):
            coupon = _parse_rate(coupon_text)
            if coupon is None:
                errors.append(f"unparsable:coupon_rate_pct:{coupon_text}")

        yield_pct = _parse_rate(yield_text)
        if yield_pct is None:
            errors.append(f"unparsable:yield_pct:{yield_text}")

        remaining_tenor: Decimal | None = None
        if observation_date is not None and maturity_date is not None:
            days = Decimal((maturity_date - observation_date).days)
            remaining_tenor = quantize_to(days / DAYS_PER_YEAR, TENOR_QUANTUM)

        records.append(
            BondRecord(
                observation_date=observation_date,
                bond_code=bond_code,
                isin=isin,
                original_tenor_years=original_tenor,
                remaining_tenor_years=remaining_tenor,
                maturity_date=maturity_date,
                coupon_rate_pct=coupon,
                yield_pct=yield_pct,
                errors=tuple(errors),
                raw=raw,
                line=line,
            )
        )
    logger.debug(
        "parsed CBK bond table: %d records, %d rows without a redemption yield skipped",
        len(records),
        skipped_without_yield,
    )
    return records


# ---------------------------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------------------------


def _out_of_range(value: Decimal, low: Decimal, high: Decimal, *, low_inclusive: bool) -> bool:
    if not value.is_finite():
        return True
    above_low = value >= low if low_inclusive else value > low
    return not (above_low and value <= high)


def _record_problems(record: BondRecord) -> list[str]:
    """Every reason ``record`` cannot be written, in a stable order and without duplicates.

    Parse ``errors`` come first. A field that is ``None`` because its cell failed to parse is
    already explained by them, so ``missing_<field>`` is added only for a record without parse
    errors (one built elsewhere than :func:`parse_bonds_table`).
    """
    reasons: dict[str, None] = dict.fromkeys(record.errors)
    clean = not record.errors

    def add(reason: str) -> None:
        reasons.setdefault(reason, None)

    if not record.bond_code:
        add("missing_bond_code")
    elif len(record.bond_code) > MAX_BOND_CODE_CHARS:
        add("bond_code_too_long")
    if record.isin is not None and ISIN_RE.fullmatch(record.isin) is None:
        add(f"invalid_isin:{record.isin}")

    issue, maturity = record.observation_date, record.maturity_date
    if issue is None and clean:
        add("missing_issue_date")
    if maturity is None and clean:
        add("missing_maturity_date")
    dates_inverted = issue is not None and maturity is not None and maturity <= issue
    if dates_inverted:
        add("maturity_not_after_issue")

    low, high = YIELD_BOUNDS
    if record.yield_pct is None:
        if clean:
            add("missing_yield_pct")
    elif _out_of_range(record.yield_pct, low, high, low_inclusive=False):
        add(f"out_of_range:yield_pct:{record.yield_pct}")

    tenors = [("original_tenor_years", record.original_tenor_years)]
    # An inverted date pair is already the reason; its non-positive remaining tenor adds nothing.
    if not dates_inverted:
        tenors.append(("remaining_tenor_years", record.remaining_tenor_years))
    for column, tenor in tenors:
        if tenor is None:
            if clean:
                add(f"missing_{column}")
        elif _out_of_range(tenor, Decimal(0), MAX_TENOR_YEARS, low_inclusive=False):
            add(f"out_of_range:{column}:{tenor}")

    coupon_low, coupon_high = COUPON_BOUNDS
    if record.coupon_rate_pct is not None and _out_of_range(
        record.coupon_rate_pct, coupon_low, coupon_high, low_inclusive=True
    ):
        add(f"out_of_range:coupon_rate_pct:{record.coupon_rate_pct}")
    return list(reasons)


def _rejected_payload(record: BondRecord) -> Mapping[str, object]:
    """The row recorded in ``details.rejected``: the source cells, else the parsed values."""
    if record.raw:
        return dict(record.raw)
    return {
        "observation_date": record.observation_date,
        "bond_code": record.bond_code,
        "isin": record.isin,
        "original_tenor_years": record.original_tenor_years,
        "remaining_tenor_years": record.remaining_tenor_years,
        "maturity_date": record.maturity_date,
        "coupon_rate_pct": record.coupon_rate_pct,
        "yield_pct": record.yield_pct,
    }


def _bond_row(record: BondRecord) -> tuple[tuple[date, str], dict[str, object]]:
    """A validated record's conflict key and its ``terminal_bond_yields`` row.

    Raises:
        ValueError: A NOT NULL value is missing, which validation rejects first.
    """
    observation_date, maturity_date = record.observation_date, record.maturity_date
    original, remaining = record.original_tenor_years, record.remaining_tenor_years
    yield_pct = record.yield_pct
    if (
        observation_date is None
        or maturity_date is None
        or original is None
        or remaining is None
        or yield_pct is None
        or not record.bond_code
    ):
        raise ValueError(
            f"cbk_bonds: the record of line {record.line} lacks a NOT NULL value; "
            "validation must have rejected it"
        )
    row: dict[str, object] = {
        "observation_date": observation_date,
        "bond_code": record.bond_code,
        "isin": record.isin,
        "original_tenor_years": original,
        "remaining_tenor_years": remaining,
        "maturity_date": maturity_date,
        "coupon_rate_pct": record.coupon_rate_pct,
        "yield_pct": yield_pct,
        "source": BOND_SOURCE,
    }
    return (observation_date, record.bond_code), row


# ---------------------------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------------------------


class CbkBondsPipeline(Pipeline[FetchedPage, BondRecord]):
    """``cbk_bonds``: CBK Treasury bond auction results into ``terminal_bond_yields``.

    One request per run (after ``robots.txt``): :data:`TREASURY_BONDS_URL`. Fewer than
    :data:`MIN_ROWS` parsed rows, a missing table or more than 10% of rows rejected end the run
    ``layout_changed`` with nothing written. Source: Central Bank of Kenya.
    """

    name: ClassVar[str] = "cbk_bonds"
    min_rows: ClassVar[int] = MIN_ROWS
    source_url: str | None = TREASURY_BONDS_URL

    async def fetch(self, ctx: RunContext) -> FetchedPage:
        """GET the Treasury bonds page through the run's polite client.

        Raises:
            RobotsDisallowed: ``robots.txt`` blocks the page.
            FetchFailed: The page could not be fetched after retries.
        """
        ctx.details["source_url"] = TREASURY_BONDS_URL
        html = await ctx.http.get_text(TREASURY_BONDS_URL)
        return FetchedPage(url=TREASURY_BONDS_URL, html=html)

    def parse(self, raw: FetchedPage) -> list[BondRecord]:
        """Parse the page with :func:`parse_bonds_table`."""
        return parse_bonds_table(raw.html)

    def validate(self, records: Sequence[BondRecord]) -> ValidationOutcome[BondRecord]:
        """Reject records that carry parse errors or break the table's rules.

        Rejection reasons, joined with ``;``: the parse ``errors``; ``missing_bond_code`` or
        ``bond_code_too_long``; ``invalid_isin:<isin>``; ``missing_issue_date`` or
        ``missing_maturity_date``; ``maturity_not_after_issue`` when the maturity is on or
        before the issue date; ``out_of_range:yield_pct:<value>`` outside ``(0, 100]``;
        ``out_of_range:original_tenor_years:<value>`` or
        ``out_of_range:remaining_tenor_years:<value>`` outside ``(0, 100]``; and
        ``out_of_range:coupon_rate_pct:<value>`` outside ``[0, 100]``. ``RejectedRow.index`` is
        the record's source row, or its 0-based position when that is unknown. The shared run
        thresholds are applied by ``execute_pipeline``.
        """
        valid: list[BondRecord] = []
        rejected: list[RejectedRow] = []
        for position, record in enumerate(records):
            reasons = _record_problems(record)
            if reasons:
                rejected.append(
                    RejectedRow(
                        index=record.line if record.line is not None else position,
                        reason=";".join(reasons),
                        row=_rejected_payload(record),
                    )
                )
            else:
                valid.append(record)
        return ValidationOutcome(valid=valid, rejected=rejected)

    async def upsert(
        self, conn: AsyncConnection, records: Sequence[BondRecord], run_id: uuid.UUID
    ) -> int:
        """Upsert validated records into ``terminal_bond_yields`` on
        ``(observation_date, bond_code)`` and return the number of rows inserted or updated.

        Records sharing a key (a reopened issue listed twice for one auction date) are collapsed
        with the last one winning, because one ``INSERT ... ON CONFLICT DO UPDATE`` cannot touch
        a row twice. Rows are written in key order, in chunks of ``cbk.UPSERT_CHUNK_ROWS``,
        through ``db.upsert.upsert_rows`` with ``mode="do_update"``, which sets every supplied
        column and generates each new row's ``id``. Nothing is committed here: the run's
        transaction, which holds its advisory lock, does that.

        Raises:
            ValueError: A record lacks a NOT NULL value (validation must have rejected it).
        """
        unique, duplicates = dedupe_by_key(
            records, lambda record: (record.observation_date, record.bond_code)
        )
        # upsert_rows orders rows within one call only; sorting first keeps the chunks in one
        # global key order, so every statement locks keys above the previous statement's.
        keyed = sorted((_bond_row(record) for record in unique), key=lambda pair: pair[0])
        rows = [row for _key, row in keyed]
        written = 0
        for start in range(0, len(rows), UPSERT_CHUNK_ROWS):
            written += await upsert_rows(
                conn,
                TerminalBondYield,
                rows[start : start + UPSERT_CHUNK_ROWS],
                key=CONFLICT_KEY,
                mode="do_update",
            )
        fxd = sum(1 for record in unique if record.bond_code.startswith(_FXD_PREFIX))
        logger.info(
            "cbk_bonds run %s: %d bond records (%d FXD, %d other codes), %d duplicate keys "
            "dropped, %d rows written",
            run_id,
            len(unique),
            fxd,
            len(unique) - fxd,
            duplicates,
            written,
        )
        return written
