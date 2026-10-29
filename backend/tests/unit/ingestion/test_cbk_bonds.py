"""Tests for the CBK Treasury bonds pipeline (``opennse_terminal.ingestion.cbk.bonds``).

``fixtures/cbk/treasury_bonds.html`` is the complete public "Issues of Treasury Bonds" table of
https://www.centralbank.go.ke/bills-bonds/treasury-bonds/ (326 rows, curated so a full run ends
``succeeded``). The parser is tested on it directly. Full runs serve it through respx at its real
URL, with ``assert_all_mocked=True`` so nothing reaches the network, into a fresh seeded clone
(``seeded_database``, which marks those tests ``integration``); per-host spacing runs on a fake
clock, so no test waits. Every value is compared as an exact ``Decimal``. The single ``live``
test reads the real page and is deselected by default.
"""

import asyncio
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Protocol

import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.ingestion.base import (
    RunResult,
    RunStatus,
    SourceLayoutChanged,
    execute_pipeline,
)
from opennse_terminal.ingestion.cbk.bonds import (
    BOND_SOURCE,
    EXPECTED_HEADERS,
    ISIN_RE,
    MIN_ROWS,
    TREASURY_BONDS_URL,
    BondRecord,
    CbkBondsPipeline,
    parse_bond_date,
    parse_bonds_table,
    parse_tenor_years,
)
from opennse_terminal.ingestion.http import PoliteClient
from opennse_terminal.models import (
    TerminalBondYield,
    TerminalIngestionRun,
    TerminalMacroIndicator,
)

FIXTURE = "cbk/treasury_bonds.html"
ROBOTS_URL = "https://www.centralbank.go.ke/robots.txt"
# The CBK site's own robots.txt: every path allowed except the CMS admin area.
ALLOW_ALL = "User-agent: *\nDisallow: /wp-admin/\n"

REFERENCE_CODE = "FXD1/2018/025"
REFERENCE_ISSUE = date(2026, 2, 16)
REFERENCE_MATURITY = date(2043, 5, 25)
TENOR_QUANTUM = Decimal("0.01")  # NUMERIC(6,2)
RATE_EXPONENT = -4  # NUMERIC(9,4)
TENOR_EXPONENT = -2

SEEDED_BOND_ROWS = 1062
"""Seeded ``terminal_bond_yields``: 177 month-ends x 6 tenors, all ``source='seed'``."""
SEED_CODE_PREFIX = "FXD-SEED-"

BondRow = tuple[date, str, str | None, Decimal, Decimal, date, Decimal | None, Decimal, str]
"""(observation_date, bond_code, isin, original_tenor_years, remaining_tenor_years,
maturity_date, coupon_rate_pct, yield_pct, source)"""
BondKey = tuple[date, str]
SOURCE_INDEX = 8


class _Clone(Protocol):
    """The part of the root conftest's ``seeded_database`` handle these tests use."""

    @property
    def settings(self) -> Any: ...

    @property
    def engine(self) -> AsyncEngine: ...


class FakeClock:
    """Monotonic seconds that move only when :class:`FakeSleep` advances them."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class FakeSleep:
    """Records every wait and advances the fake clock by it instead of sleeping; it still yields
    to the event loop once, as a real sleep would."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.now += seconds
        await asyncio.sleep(0)


def expected_remaining_tenor(issue: date, maturity: date) -> Decimal:
    """``(maturity - issue).days / 365.25`` at NUMERIC(6,2), rounded ``ROUND_HALF_UP``."""
    days = Decimal((maturity - issue).days)
    return (days / Decimal("365.25")).quantize(TENOR_QUANTUM, rounding=ROUND_HALF_UP)


def find_record(records: Sequence[BondRecord], issue: date, code: str) -> BondRecord:
    """The last record for ``(issue, code)``, the one the upsert keeps."""
    matches = [r for r in records if r.observation_date == issue and r.bond_code == code]
    assert matches, f"no record for {code} issued {issue}"
    return matches[-1]


def expected_rows(records: Sequence[BondRecord]) -> dict[BondKey, BondRow]:
    """The ``terminal_bond_yields`` row each distinct key should hold after a run: the last
    record per ``(observation_date, bond_code)``, written with ``source='cbk_bonds'``."""
    rows: dict[BondKey, BondRow] = {}
    for record in records:
        issue, maturity = record.observation_date, record.maturity_date
        original, remaining = record.original_tenor_years, record.remaining_tenor_years
        assert issue is not None and maturity is not None
        assert original is not None and remaining is not None and record.yield_pct is not None
        rows[(issue, record.bond_code)] = (
            issue,
            record.bond_code,
            record.isin,
            original,
            remaining,
            maturity,
            record.coupon_rate_pct,
            record.yield_pct,
            BOND_SOURCE,
        )
    return rows


@contextmanager
def serve(html: str) -> Iterator[respx.MockRouter]:
    """Serve the CBK ``robots.txt`` and ``html`` at :data:`TREASURY_BONDS_URL`; any other URL
    fails the request."""
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        router.get(ROBOTS_URL).respond(200, text=ALLOW_ALL)
        router.get(TREASURY_BONDS_URL).respond(200, html=html)
        yield router


async def run(handle: _Clone, html: str) -> tuple[RunResult, list[str]]:
    """Run ``cbk_bonds`` against ``handle`` with ``html`` served; return the result and the URLs
    requested, in order."""
    clock = FakeClock()
    with serve(html) as router:
        async with PoliteClient(handle.settings, sleep=FakeSleep(clock), clock=clock) as client:
            result = await execute_pipeline(
                CbkBondsPipeline(handle.settings), handle.engine, trigger="cli", http=client
            )
        requested = [str(call.request.url) for call in router.calls]
    return result, requested


async def bond_rows(handle: _Clone) -> list[BondRow]:
    """Every ``terminal_bond_yields`` row, ordered by ``observation_date`` then ``bond_code``."""
    statement = select(
        TerminalBondYield.observation_date,
        TerminalBondYield.bond_code,
        TerminalBondYield.isin,
        TerminalBondYield.original_tenor_years,
        TerminalBondYield.remaining_tenor_years,
        TerminalBondYield.maturity_date,
        TerminalBondYield.coupon_rate_pct,
        TerminalBondYield.yield_pct,
        TerminalBondYield.source,
    ).order_by(TerminalBondYield.observation_date, TerminalBondYield.bond_code)
    async with handle.engine.connect() as conn:
        result = await conn.execute(statement)
        return [
            (
                row.observation_date,
                row.bond_code,
                row.isin,
                row.original_tenor_years,
                row.remaining_tenor_years,
                row.maturity_date,
                row.coupon_rate_pct,
                row.yield_pct,
                row.source,
            )
            for row in result
        ]


async def run_row(handle: _Clone, result: RunResult) -> dict[str, Any]:
    """The recorded ``terminal_ingestion_runs`` row of a run."""
    statement = select(
        TerminalIngestionRun.pipeline,
        TerminalIngestionRun.trigger,
        TerminalIngestionRun.status,
        TerminalIngestionRun.source_url,
        TerminalIngestionRun.rows_fetched,
        TerminalIngestionRun.rows_written,
        TerminalIngestionRun.rows_rejected,
        TerminalIngestionRun.error_message,
    ).where(TerminalIngestionRun.id == result.run_id)
    async with handle.engine.connect() as conn:
        return dict((await conn.execute(statement)).mappings().one())


async def macro_snapshot(handle: _Clone) -> list[tuple[Any, ...]]:
    """Every ``terminal_macro_indicators`` row's date, provenance and update time."""
    statement = select(
        TerminalMacroIndicator.observation_date,
        TerminalMacroIndicator.source_by_field,
        TerminalMacroIndicator.updated_at,
    ).order_by(TerminalMacroIndicator.observation_date)
    async with handle.engine.connect() as conn:
        return [tuple(row) for row in await conn.execute(statement)]


def seed_rows(rows: Sequence[BondRow]) -> list[BondRow]:
    return [row for row in rows if row[1].startswith(SEED_CODE_PREFIX)]


@pytest.fixture(scope="module")
def bonds_html(fixture_text: Callable[[str], str]) -> str:
    return fixture_text(FIXTURE)


@pytest.fixture(scope="module")
def records(bonds_html: str) -> list[BondRecord]:
    return parse_bonds_table(bonds_html)


@pytest.fixture(scope="module")
def mutated_html(bonds_html: str) -> str:
    """The fixture with its header ``Redemption Yield`` renamed ``Yield to Maturity``."""
    original = "<th>Redemption Yield</th>"
    assert bonds_html.count(original) == 1
    mutated = bonds_html.replace(original, "<th>Yield to Maturity</th>")
    assert mutated != bonds_html
    return mutated


# --- Parsing the recorded page (no database) -------------------------------------------------


def test_parse_keeps_every_row_and_every_issue_code(records: list[BondRecord]) -> None:
    assert len(records) >= MIN_ROWS
    assert [r.line for r in records if r.errors] == []
    prefixes = {record.bond_code[:3] for record in records}
    # Every code is stored; only the yield curve narrows to FXD later.
    assert {"FXD", "IFB", "SDB"} <= prefixes


def test_parse_reference_bond_fxd1_2018_025(records: list[BondRecord]) -> None:
    record = find_record(records, REFERENCE_ISSUE, REFERENCE_CODE)
    assert record.yield_pct == Decimal("13.3621")
    issue, maturity = record.observation_date, record.maturity_date
    assert issue is not None and maturity is not None
    assert (issue, maturity) == (REFERENCE_ISSUE, REFERENCE_MATURITY)
    assert maturity > issue
    assert record.remaining_tenor_years == expected_remaining_tenor(
        REFERENCE_ISSUE, REFERENCE_MATURITY
    )
    assert record.remaining_tenor_years == Decimal("17.27")  # 6,307 days / 365.25
    assert record.raw["tenor"] == "25"
    assert record.original_tenor_years == Decimal("25")
    assert record.isin == "KE5000008549"
    assert record.coupon_rate_pct == Decimal("13.4")
    assert record.errors == ()


def test_parse_trims_codes_and_isins(bonds_html: str, records: list[BondRecord]) -> None:
    assert "<td>10/03/2025</td><td> FXD1/2018/025 </td>" in bonds_html
    assert "<td>FXD1/2023/010</td><td> KE8000005432 </td>" in bonds_html
    assert find_record(records, date(2025, 3, 10), REFERENCE_CODE).yield_pct == Decimal("13.8033")
    assert find_record(records, date(2024, 12, 9), "FXD1/2023/010").isin == "KE8000005432"
    for record in records:
        assert record.bond_code == record.bond_code.strip() != ""
        assert record.isin is None or record.isin == record.isin.strip()


def test_parse_every_record_meets_the_table_rules(records: list[BondRecord]) -> None:
    for record in records:
        issue, maturity = record.observation_date, record.maturity_date
        assert issue is not None and maturity is not None, record.line
        assert maturity > issue, record.line
        assert isinstance(record.yield_pct, Decimal), record.line
        assert Decimal("0") < record.yield_pct <= Decimal("100"), record.line
        assert record.yield_pct.as_tuple().exponent == RATE_EXPONENT, record.line
        for tenor in (record.original_tenor_years, record.remaining_tenor_years):
            assert isinstance(tenor, Decimal), record.line
            assert tenor > Decimal("0"), record.line
            assert tenor.as_tuple().exponent == TENOR_EXPONENT, record.line
        assert record.remaining_tenor_years == expected_remaining_tenor(issue, maturity)
        if record.coupon_rate_pct is not None:
            assert isinstance(record.coupon_rate_pct, Decimal), record.line
            assert record.coupon_rate_pct.as_tuple().exponent == RATE_EXPONENT, record.line
        assert record.isin is None or ISIN_RE.fullmatch(record.isin), record.line


@pytest.mark.parametrize("blank", ["", " ", "-", "\u2013"])
def test_parse_blank_isin_becomes_none(bonds_html: str, blank: str) -> None:
    row = "<td>16/02/2026</td><td>FXD3/2019/015</td><td>KE6000001328</td>"
    assert bonds_html.count(row) == 1
    html = bonds_html.replace(row, f"<td>16/02/2026</td><td>FXD3/2019/015</td><td>{blank}</td>")
    record = find_record(parse_bonds_table(html), REFERENCE_ISSUE, "FXD3/2019/015")
    assert record.isin is None
    assert record.errors == ()
    assert record.yield_pct == Decimal("12.1835")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("16/02/2026", date(2026, 2, 16)),
        (" 1/7/2026 ", date(2026, 7, 1)),
        ("31/02/2026", None),
        ("2026-02-16", None),
        ("", None),
    ],
)
def test_parse_bond_date(text: str, expected: date | None) -> None:
    assert parse_bond_date(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("25", Decimal("25")),
        ("25 Years", Decimal("25")),
        ("15.0", Decimal("15.0")),
        ("Years", None),
    ],
)
def test_parse_tenor_years(text: str, expected: Decimal | None) -> None:
    assert parse_tenor_years(text) == expected


def test_parse_changed_header_raises_layout_changed(mutated_html: str) -> None:
    with pytest.raises(SourceLayoutChanged) as caught:
        parse_bonds_table(mutated_html)
    assert caught.value.expected == list(EXPECTED_HEADERS)
    found = caught.value.found
    assert isinstance(found, list)
    assert [*EXPECTED_HEADERS[:-1], "yield to maturity"] in found


def test_parse_page_without_table_raises_layout_changed() -> None:
    page = "<html><body><h2>Issues of Treasury Bonds</h2><p>No data.</p></body></html>"
    with pytest.raises(SourceLayoutChanged) as caught:
        parse_bonds_table(page)
    assert caught.value.expected == list(EXPECTED_HEADERS)
    assert caught.value.found == ["no <table> found"]


# --- Full runs into a seeded clone (integration) ---------------------------------------------


async def test_layout_changed_run_writes_nothing(
    seeded_database: _Clone, mutated_html: str
) -> None:
    before = await bond_rows(seeded_database)
    assert len(before) == SEEDED_BOND_ROWS
    assert seed_rows(before) == before
    assert {row[SOURCE_INDEX] for row in before} == {"seed"}

    result, requested = await run(seeded_database, mutated_html)

    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.rows_written == 0
    assert result.details["expected"] == list(EXPECTED_HEADERS)
    assert [*EXPECTED_HEADERS[:-1], "yield to maturity"] in result.details["found"]
    assert requested == [ROBOTS_URL, TREASURY_BONDS_URL]
    assert await run_row(seeded_database, result) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "layout_changed",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": 0,
        "rows_written": 0,
        "rows_rejected": 0,
        "error_message": "expected table not found",
    }
    assert await bond_rows(seeded_database) == before


async def test_full_run_upserts_every_issue_and_is_idempotent(
    seeded_database: _Clone, bonds_html: str, records: list[BondRecord]
) -> None:
    before = await bond_rows(seeded_database)
    macro_before = await macro_snapshot(seeded_database)
    assert len(before) == SEEDED_BOND_ROWS
    expected = expected_rows(records)
    # The page lists some issues twice for one issue date; one row per key is written.
    assert len(expected) < len(records)

    first, requested = await run(seeded_database, bonds_html)

    assert first.status is RunStatus.SUCCEEDED
    assert first.rows_rejected == 0
    assert first.rows_fetched == len(records)
    assert first.rows_written == len(expected)
    assert requested == [ROBOTS_URL, TREASURY_BONDS_URL]
    assert await run_row(seeded_database, first) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "succeeded",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": len(records),
        "rows_written": len(expected),
        "rows_rejected": 0,
        "error_message": None,
    }

    after = await bond_rows(seeded_database)
    assert {row[SOURCE_INDEX] for row in after} == {"seed", BOND_SOURCE}
    written = {(row[0], row[1]): row for row in after if row[SOURCE_INDEX] == BOND_SOURCE}
    assert written == expected
    assert len(after) == SEEDED_BOND_ROWS + len(expected)
    assert seed_rows(after) == before

    reference = written[(REFERENCE_ISSUE, REFERENCE_CODE)]
    assert reference[7] == Decimal("13.3621")
    assert reference[4] == expected_remaining_tenor(REFERENCE_ISSUE, REFERENCE_MATURITY)
    assert reference[4] == Decimal("17.27")
    # FXD1/2020/015 is listed twice for 2020-02-24 (12.756, then 12.512): the last one is kept.
    assert written[(date(2020, 2, 24), "FXD1/2020/015")][7] == Decimal("12.5120")

    second, _ = await run(seeded_database, bonds_html)

    assert second.status is RunStatus.SUCCEEDED
    assert second.rows_rejected == 0
    assert await bond_rows(seeded_database) == after
    assert await macro_snapshot(seeded_database) == macro_before


# --- The real page (deselected by default) ---------------------------------------------------


@pytest.mark.live
async def test_live_treasury_bonds_page_keeps_its_layout(
    make_settings: Callable[..., Any],
) -> None:
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        html = await client.get_text(TREASURY_BONDS_URL)
    assert len(parse_bonds_table(html)) >= MIN_ROWS
