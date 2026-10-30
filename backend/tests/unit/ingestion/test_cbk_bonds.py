"""Tests for the CBK Treasury bonds pipeline (``opennse_terminal.ingestion.cbk.bonds``).

``fixtures/cbk/treasury_bonds.html`` is the complete public "Issues of Treasury Bonds" table of
https://www.centralbank.go.ke/bills-bonds/treasury-bonds/ (326 rows, curated so a full run ends
``succeeded``: the seven published rows the validator rejects are left out). The parser is tested
on it directly. The rejection paths are driven by rows built in the tests instead: small tables
of published rows with one row broken, records built without the parser, and copies of the
fixture with rows removed or broken. They check every rejection reason with its source row, the
bounds a value may still take, the 50-row minimum, and the 10% rejection limit, at exactly 10% and
one row past it, with its partial write and its rollback.

Full runs serve a page through respx at its real URL, with ``assert_all_mocked=True`` so nothing
reaches the network, into a fresh seeded clone (``seeded_database``, which marks those tests
``integration``). Per-host spacing runs on a fake clock, so no test waits, and every run must
read ``robots.txt`` and then the page, starting the page 2.0 s later on that clock. Every value
is compared as an exact ``Decimal``. The single ``live`` test reads the real page and is
deselected by default.
"""

import asyncio
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from html import escape
from itertools import pairwise
from typing import Any, Protocol

import httpx
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

SPACING_SECONDS = 2.0
"""The least gap between the starts of two requests to one host: the default
``TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS``, which the clone's settings keep."""

FIXTURE_ROWS = 326
"""Body rows of the recorded table, each on a line of its own."""
ROW_MINIMUM = 50
"""The fewest rows a run may parse; one fewer ends it ``layout_changed``."""
REJECTED_SAMPLE = 50
"""The most rejected rows a run records in ``details.rejected``."""
THRESHOLDS = {"min_rows": ROW_MINIMUM, "max_invalid_fraction": "0.10"}
"""``details.expected`` of a run that breaches the row minimum or the 10% rejection limit."""

# The published header labels, which normalize to EXPECTED_HEADERS.
HEADER_LABELS = (
    "Issue Date",
    "Issue No",
    "ISIN Number",
    "Tenor",
    "Face Value (Kshs Millions)",
    "Maturity Date",
    "Coupon Rate",
    "Redemption Yield",
)
CELL_NAMES = (
    "issue_date",
    "issue_no",
    "isin",
    "tenor",
    "face_value",
    "maturity_date",
    "coupon_rate",
    "redemption_yield",
)
"""Keyword names of the eight cells for :func:`probe`, in column order."""
# Three published rows of fixtures/cbk/treasury_bonds.html. The rejection tests break the probe
# row and keep it between the two neighbours, which must stay valid.
NEIGHBOUR_BEFORE = (
    "16/02/2026",
    "FXD3/2019/015",
    "KE6000001328",
    "15",
    "54294.1",
    "10/07/2034",
    "12.34",
    "12.1835",
)
PROBE_ROW = (
    "12/05/2025",
    "FXD1/2012/020",
    "KE4000003949",
    "20",
    "47065.5",
    "01/11/2032",
    "12",
    "13.6489",
)
NEIGHBOUR_AFTER = (
    "16/02/2026",
    "FXD1/2018/025",
    "KE5000008549",
    "25",
    "45593.6",
    "25/05/2043",
    "13.4",
    "13.3621",
)
# The probe row as a record built without the parser (no raw cells, no source line).
BUILT_PROBE: dict[str, Any] = {
    "observation_date": date(2025, 5, 12),
    "bond_code": "FXD1/2012/020",
    "isin": "KE4000003949",
    "original_tenor_years": Decimal("20.00"),
    "remaining_tenor_years": Decimal("7.47"),
    "maturity_date": date(2032, 11, 1),
    "coupon_rate_pct": Decimal("12.0000"),
    "yield_pct": Decimal("13.6489"),
}
LONG_CODE = "FXD1/2012/020-REOPENED-TAP-SALE-X"
"""33 characters, one more than ``bond_code`` (VARCHAR(32)) holds."""

_CELL = re.compile(r"<td>([^<>&]*)</td>")


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


Responder = Callable[[httpx.Request], httpx.Response]


def logged(clock: FakeClock, log: list[tuple[float, str]], respond: Responder) -> Responder:
    """``respond``, first appending ``(fake time, URL)`` to ``log`` as each request reaches the
    mock, so request starts are measured on the fake timeline."""

    def side_effect(request: httpx.Request) -> httpx.Response:
        log.append((clock(), str(request.url)))
        return respond(request)

    return side_effect


@dataclass(frozen=True, slots=True)
class Served:
    """One full run and the traffic it sent to the mocked CBK site."""

    result: RunResult
    # Every request in the order it reached the mock, as (fake time at its start, URL).
    requests: tuple[tuple[float, str], ...]
    # Every wait the client asked its sleep for, in order.
    delays: tuple[float, ...]


async def run(handle: _Clone, html: str) -> Served:
    """Run ``cbk_bonds`` against ``handle`` with respx serving the CBK ``robots.txt`` and
    ``html`` at :data:`TREASURY_BONDS_URL` (any other URL fails the request), logging the fake
    time at which each request starts."""
    clock = FakeClock()
    sleep = FakeSleep(clock)
    log: list[tuple[float, str]] = []
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        router.get(ROBOTS_URL).mock(
            side_effect=logged(clock, log, lambda _request: httpx.Response(200, text=ALLOW_ALL))
        )
        router.get(TREASURY_BONDS_URL).mock(
            side_effect=logged(clock, log, lambda _request: httpx.Response(200, html=html))
        )
        async with PoliteClient(handle.settings, sleep=sleep, clock=clock) as client:
            result = await execute_pipeline(
                CbkBondsPipeline(handle.settings), handle.engine, trigger="cli", http=client
            )
    return Served(result=result, requests=tuple(log), delays=tuple(sleep.delays))


def assert_one_polite_request(served: Served) -> None:
    """The run read ``robots.txt`` once, then requested the bonds page once, and each request to
    the CBK host started at least ``SPACING_SECONDS`` after the one before it on the fake clock.

    The mocked site answers in no fake time, so the client's single spacing wait, which the fake
    sleep absorbed, is the whole interval: exactly ``SPACING_SECONDS``, putting the page's start
    exactly that far after the start of ``robots.txt``.
    """
    assert [url for _, url in served.requests] == [ROBOTS_URL, TREASURY_BONDS_URL]
    starts = [at for at, _ in served.requests]
    assert all(later - earlier >= SPACING_SECONDS for earlier, later in pairwise(starts))
    assert served.delays == (SPACING_SECONDS,)
    assert starts[1] - starts[0] == SPACING_SECONDS


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


async def run_details(handle: _Clone, result: RunResult) -> dict[str, Any]:
    """The ``details`` JSON recorded on a run's ``terminal_ingestion_runs`` row."""
    statement = select(TerminalIngestionRun.details).where(TerminalIngestionRun.id == result.run_id)
    async with handle.engine.connect() as conn:
        details: dict[str, Any] = (await conn.execute(statement)).scalar_one()
    return details


def probe(**cells: str) -> tuple[str, ...]:
    """:data:`PROBE_ROW` with the cells named as in :data:`CELL_NAMES` replaced."""
    unknown = sorted(set(cells) - set(CELL_NAMES))
    assert not unknown, f"unknown cell names {unknown}"
    return tuple(cells.get(name, cell) for name, cell in zip(CELL_NAMES, PROBE_ROW, strict=True))


def table_body(rows: Sequence[Sequence[str]]) -> str:
    """``rows`` as table body markup, one ``<td>`` per cell, in the recorded page's layout."""
    return "".join(
        "<tr>" + "".join(f"<td>{escape(cell, quote=False)}</td>" for cell in row) + "</tr>\n"
        for row in rows
    )


def bonds_page(*rows: Sequence[str]) -> str:
    """A Treasury bonds page with one table: the published headers, then ``rows``."""
    head = "".join(f"<th>{label}</th>" for label in HEADER_LABELS)
    return (
        "<html><body><h2>Issues of Treasury Bonds</h2><table>"
        f"<thead><tr>{head}</tr></thead><tbody>\n{table_body(rows)}</tbody></table></body></html>"
    )


def fixture_rows(html: str) -> list[tuple[str, ...]]:
    """The cells of every body row of the recorded page, verbatim: item ``n - 1`` holds source
    row ``n``."""
    lines = html.splitlines()
    rows = [tuple(_CELL.findall(line)) for line in lines if line.startswith("<tr><td>")]
    assert len(rows) == FIXTURE_ROWS
    assert all(len(cells) == len(EXPECTED_HEADERS) for cells in rows)
    return rows


def with_body(html: str, rows: Sequence[Sequence[str]]) -> str:
    """The recorded page ``html`` with its table body replaced by ``rows``."""
    head, opening, rest = html.partition("<tbody>\n")
    _, closing, tail = rest.partition("</tbody>")
    assert opening and closing
    return f"{head}{opening}{table_body(rows)}{closing}{tail}"


def source_cells(cells: Sequence[str]) -> dict[str, str]:
    """``cells`` as ``details.rejected`` records them: keyed by normalized header, cells past the
    eighth as ``extra_<n>``, each with its whitespace collapsed."""
    cleaned = [" ".join(cell.split()) for cell in cells]
    row = dict(zip(EXPECTED_HEADERS, cleaned, strict=False))
    extras = cleaned[len(EXPECTED_HEADERS) :]
    row.update({f"extra_{position}": cell for position, cell in enumerate(extras, start=1)})
    return row


def break_cells(cells: Sequence[str], kind: int) -> tuple[tuple[str, ...], str]:
    """``cells`` with one value broken, and the rejection reason validation gives it.

    ``kind`` cycles through the source data-entry errors of the published rows the curated
    fixture leaves out (a blank Issue Date, Tenor 0, a maturity that is not after the issue,
    ISIN ``KE``), then a zero Redemption Yield, outside ``(0, 100]``.
    """
    broken = list(cells)
    match kind % 5:
        case 0:
            broken[0] = ""
            reason = "invalid_issue_date:"
        case 1:
            broken[3] = "0"
            reason = "out_of_range:original_tenor_years:0.00"
        case 2:
            broken[5] = broken[0]
            reason = "maturity_not_after_issue"
        case 3:
            broken[2] = "KE"
            reason = "invalid_isin:KE"
        case _:
            broken[7] = "0"
            reason = "out_of_range:yield_pct:0.0000"
    return tuple(broken), reason


@dataclass(frozen=True, slots=True)
class Broken:
    """The recorded page with some rows broken, and what validation must reject."""

    html: str
    lines: frozenset[int]
    # The details.rejected entries validation must give, in source order.
    rejected: tuple[dict[str, Any], ...]


def page_rows(recorded: Sequence[tuple[str, ...]], parsed: int) -> list[tuple[str, ...]]:
    """``parsed`` rows of the recorded table, listed from its first row over and over: its first
    rows for a shorter page, the whole table twice for twice its length."""
    return [recorded[index % len(recorded)] for index in range(parsed)]


def break_rows(html: str, rows: Sequence[tuple[str, ...]], count: int) -> Broken:
    """The recorded page ``html`` with its table body replaced by ``rows`` and ``count`` of them,
    spread evenly from source row 1, broken in turn by :func:`break_cells`; every other row is
    left as given."""
    body = list(rows)
    lines = [1 + index * len(body) // count for index in range(count)]
    assert len(set(lines)) == count
    rejected: list[dict[str, Any]] = []
    for kind, line in enumerate(lines):
        cells, reason = break_cells(body[line - 1], kind)
        body[line - 1] = cells
        rejected.append({"index": line, "reason": reason, "row": source_cells(cells)})
    return Broken(html=with_body(html, body), lines=frozenset(lines), rejected=tuple(rejected))


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


@pytest.fixture(scope="module")
def recorded_rows(bonds_html: str) -> tuple[tuple[str, ...], ...]:
    """The cells of the fixture's 326 body rows, in source order."""
    rows = fixture_rows(bonds_html)
    # The page rebuilt from its rows is the recording byte for byte, so a page built from them
    # differs from the recording only where a test changes it.
    assert with_body(bonds_html, rows) == bonds_html
    return tuple(rows)


@pytest.fixture
def pipeline(make_settings: Callable[..., Any]) -> CbkBondsPipeline:
    """A fresh pipeline for the validation tests, which need no database."""
    return CbkBondsPipeline(make_settings())


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


# --- Rejected and accepted rows (no database) ------------------------------------------------


@pytest.mark.parametrize(
    ("cells", "reason"),
    [
        pytest.param(PROBE_ROW[:7], "short_row", id="short-row"),
        pytest.param(PROBE_ROW[:1], "short_row;missing_bond_code", id="short-row-without-issue-no"),
        pytest.param((*PROBE_ROW, "9.5"), "long_row", id="long-row"),
        pytest.param(
            probe(issue_date="31/02/2026"),
            "invalid_issue_date:31/02/2026",
            id="impossible-issue-date",
        ),
        pytest.param(
            probe(issue_date="2026-02-16"), "invalid_issue_date:2026-02-16", id="iso-issue-date"
        ),
        pytest.param(probe(issue_date=""), "invalid_issue_date:", id="blank-issue-date"),
        pytest.param(
            probe(maturity_date="25/13/2043"),
            "invalid_maturity_date:25/13/2043",
            id="impossible-maturity-date",
        ),
        pytest.param(probe(issue_no=""), "missing_bond_code", id="blank-issue-no"),
        pytest.param(probe(issue_no=LONG_CODE), "bond_code_too_long", id="issue-no-of-33-chars"),
        pytest.param(probe(isin="KE"), "invalid_isin:KE", id="isin-country-code-only"),
        pytest.param(
            probe(isin="KE400000394X"),
            "invalid_isin:KE400000394X",
            id="isin-without-check-digit",
        ),
        pytest.param(probe(tenor="Years"), "invalid_tenor:Years", id="tenor-without-number"),
        pytest.param(
            probe(coupon_rate="abc"), "unparsable:coupon_rate_pct:abc", id="unparsable-coupon"
        ),
        pytest.param(
            probe(redemption_yield="abc"), "unparsable:yield_pct:abc", id="unparsable-yield"
        ),
        pytest.param(
            probe(maturity_date="12/05/2025"),
            "maturity_not_after_issue",
            id="maturity-on-issue-date",
        ),
        pytest.param(
            probe(maturity_date="11/05/2025"),
            "maturity_not_after_issue",
            id="maturity-before-issue",
        ),
        pytest.param(probe(redemption_yield="0"), "out_of_range:yield_pct:0.0000", id="zero-yield"),
        pytest.param(
            probe(redemption_yield="-0.5"), "out_of_range:yield_pct:-0.5000", id="negative-yield"
        ),
        pytest.param(
            probe(redemption_yield="100.0001"),
            "out_of_range:yield_pct:100.0001",
            id="yield-above-100",
        ),
        pytest.param(probe(tenor="0"), "out_of_range:original_tenor_years:0.00", id="tenor-zero"),
        pytest.param(
            probe(tenor="100.01"),
            "out_of_range:original_tenor_years:100.01",
            id="tenor-above-100",
        ),
        pytest.param(
            probe(maturity_date="13/05/2025"),
            "out_of_range:remaining_tenor_years:0.00",
            id="matures-the-day-after-issue",
        ),
        pytest.param(
            probe(maturity_date="12/05/2126"),
            "out_of_range:remaining_tenor_years:101.00",
            id="matures-101-years-after-issue",
        ),
        pytest.param(
            probe(coupon_rate="-0.5"),
            "out_of_range:coupon_rate_pct:-0.5000",
            id="negative-coupon",
        ),
        pytest.param(
            probe(coupon_rate="100.5"),
            "out_of_range:coupon_rate_pct:100.5000",
            id="coupon-above-100",
        ),
        pytest.param(
            (
                *probe(
                    issue_date="31/02/2026",
                    issue_no="",
                    isin="KE",
                    tenor="Years",
                    maturity_date="25/13/2043",
                    coupon_rate="abc",
                    redemption_yield="abc",
                ),
                "9.5",
            ),
            "long_row;invalid_issue_date:31/02/2026;missing_bond_code;invalid_isin:KE;"
            "invalid_tenor:Years;invalid_maturity_date:25/13/2043;"
            "unparsable:coupon_rate_pct:abc;unparsable:yield_pct:abc",
            id="every-cell-unreadable",
        ),
        pytest.param(
            probe(
                issue_no=LONG_CODE,
                isin="KE",
                tenor="0",
                maturity_date="12/05/2025",
                coupon_rate="-1",
                redemption_yield="0",
            ),
            "bond_code_too_long;invalid_isin:KE;maturity_not_after_issue;"
            "out_of_range:yield_pct:0.0000;out_of_range:original_tenor_years:0.00;"
            "out_of_range:coupon_rate_pct:-1.0000",
            id="every-value-out-of-bounds",
        ),
    ],
)
def test_validate_rejects_a_broken_row_with_its_reasons(
    pipeline: CbkBondsPipeline, cells: tuple[str, ...], reason: str
) -> None:
    """The broken row is rejected with every reason, in column and rule order and each once,
    under its source row number, with its cells as recorded; its neighbours stay valid."""
    assert len(LONG_CODE) == 33
    records = parse_bonds_table(bonds_page(NEIGHBOUR_BEFORE, cells, NEIGHBOUR_AFTER))

    outcome = pipeline.validate(records)

    assert [record.line for record in records] == [1, 2, 3]
    assert outcome.valid == [records[0], records[2]]
    assert [(row.index, row.reason) for row in outcome.rejected] == [(2, reason)]
    codes = outcome.rejected[0].reason.split(";")
    assert len(codes) == len(set(codes))
    assert outcome.rejected[0].row == source_cells(cells)


@pytest.mark.parametrize(
    ("cells", "attribute", "value"),
    [
        pytest.param(
            (*PROBE_ROW, "", " "), "yield_pct", Decimal("13.6489"), id="trailing-blank-cells"
        ),
        pytest.param(
            probe(issue_no=LONG_CODE[:32]),
            "bond_code",
            LONG_CODE[:32],
            id="issue-no-of-32-chars",
        ),
        pytest.param(probe(isin="ke4000003949"), "isin", "KE4000003949", id="lowercase-isin"),
        pytest.param(probe(isin="-"), "isin", None, id="dash-isin"),
        pytest.param(
            probe(redemption_yield="100"), "yield_pct", Decimal("100.0000"), id="yield-100"
        ),
        pytest.param(
            probe(redemption_yield="0.0001"),
            "yield_pct",
            Decimal("0.0001"),
            id="smallest-positive-yield",
        ),
        pytest.param(probe(tenor="100"), "original_tenor_years", Decimal("100.00"), id="tenor-100"),
        pytest.param(
            # 36,524 days: 99.997 years, 100.00 at the column's scale.
            probe(maturity_date="12/05/2125"),
            "remaining_tenor_years",
            Decimal("100.00"),
            id="matures-100-years-after-issue",
        ),
        pytest.param(
            probe(coupon_rate="0"), "coupon_rate_pct", Decimal("0.0000"), id="zero-coupon"
        ),
        pytest.param(
            probe(coupon_rate="100"), "coupon_rate_pct", Decimal("100.0000"), id="coupon-100"
        ),
        pytest.param(probe(coupon_rate="-"), "coupon_rate_pct", None, id="dash-coupon"),
    ],
)
def test_validate_accepts_a_row_on_a_bound(
    pipeline: CbkBondsPipeline, cells: tuple[str, ...], attribute: str, value: object
) -> None:
    records = parse_bonds_table(bonds_page(NEIGHBOUR_BEFORE, cells, NEIGHBOUR_AFTER))

    outcome = pipeline.validate(records)

    assert [record.line for record in records] == [1, 2, 3]
    assert outcome.rejected == []
    assert outcome.valid == records
    assert records[1].errors == ()
    assert getattr(records[1], attribute) == value


@pytest.mark.parametrize("missing", ["-", "", "\u2013"], ids=["dash", "blank", "en-dash"])
def test_row_without_a_redemption_yield_is_skipped_not_rejected(
    pipeline: CbkBondsPipeline, missing: str
) -> None:
    records = parse_bonds_table(
        bonds_page(NEIGHBOUR_BEFORE, probe(redemption_yield=missing), NEIGHBOUR_AFTER)
    )

    outcome = pipeline.validate(records)

    assert [record.line for record in records] == [1, 3]
    assert outcome.rejected == []
    assert outcome.valid == records


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        pytest.param({"observation_date": None}, "missing_issue_date", id="no-issue-date"),
        pytest.param({"maturity_date": None}, "missing_maturity_date", id="no-maturity-date"),
        pytest.param({"yield_pct": None}, "missing_yield_pct", id="no-yield"),
        pytest.param(
            {"original_tenor_years": None},
            "missing_original_tenor_years",
            id="no-original-tenor",
        ),
        pytest.param(
            {"remaining_tenor_years": None},
            "missing_remaining_tenor_years",
            id="no-remaining-tenor",
        ),
        pytest.param({"bond_code": ""}, "missing_bond_code", id="no-bond-code"),
        pytest.param({"bond_code": LONG_CODE}, "bond_code_too_long", id="bond-code-too-long"),
        pytest.param({"isin": "ke4000003949"}, "invalid_isin:ke4000003949", id="lowercase-isin"),
        pytest.param(
            {"yield_pct": Decimal("Infinity")},
            "out_of_range:yield_pct:Infinity",
            id="infinite-yield",
        ),
        pytest.param(
            dict.fromkeys(
                (
                    "observation_date",
                    "maturity_date",
                    "yield_pct",
                    "original_tenor_years",
                    "remaining_tenor_years",
                )
            ),
            "missing_issue_date;missing_maturity_date;missing_yield_pct;"
            "missing_original_tenor_years;missing_remaining_tenor_years",
            id="no-values",
        ),
        pytest.param(
            {"bond_code": "", "isin": "KE", "observation_date": None, "yield_pct": None},
            "missing_bond_code;invalid_isin:KE;missing_issue_date;missing_yield_pct",
            id="several-problems",
        ),
    ],
)
def test_validate_checks_a_record_built_without_the_parser(
    pipeline: CbkBondsPipeline, changes: dict[str, Any], reason: str
) -> None:
    """A record without parse errors is checked field by field, so a value it lacks is named.
    Without a source row, its index is its position; its parsed values are recorded."""
    valid = BondRecord(**BUILT_PROBE)
    broken = BondRecord(**{**BUILT_PROBE, **changes})

    outcome = pipeline.validate([valid, broken])

    assert outcome.valid == [valid]
    assert [(row.index, row.reason) for row in outcome.rejected] == [(1, reason)]
    assert outcome.rejected[0].row == {**BUILT_PROBE, **changes}


# --- Full runs into a seeded clone (integration) ---------------------------------------------


async def test_layout_changed_run_writes_nothing(
    seeded_database: _Clone, mutated_html: str
) -> None:
    before = await bond_rows(seeded_database)
    assert len(before) == SEEDED_BOND_ROWS
    assert seed_rows(before) == before
    assert {row[SOURCE_INDEX] for row in before} == {"seed"}

    served = await run(seeded_database, mutated_html)
    result = served.result

    assert result.status is RunStatus.LAYOUT_CHANGED
    assert result.rows_written == 0
    assert result.details["expected"] == list(EXPECTED_HEADERS)
    assert [*EXPECTED_HEADERS[:-1], "yield to maturity"] in result.details["found"]
    assert_one_polite_request(served)
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

    first_served = await run(seeded_database, bonds_html)
    first = first_served.result

    assert first.status is RunStatus.SUCCEEDED
    assert first.rows_rejected == 0
    assert first.rows_fetched == len(records)
    assert first.rows_written == len(expected)
    assert_one_polite_request(first_served)
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

    second_served = await run(seeded_database, bonds_html)
    second = second_served.result

    assert second.status is RunStatus.SUCCEEDED
    assert second.rows_rejected == 0
    assert_one_polite_request(second_served)
    assert await bond_rows(seeded_database) == after
    assert await macro_snapshot(seeded_database) == macro_before


async def test_run_below_the_row_minimum_writes_nothing(
    seeded_database: _Clone, bonds_html: str, recorded_rows: tuple[tuple[str, ...], ...]
) -> None:
    before = await bond_rows(seeded_database)
    page = with_body(bonds_html, recorded_rows[: ROW_MINIMUM - 1])

    served = await run(seeded_database, page)
    result = served.result

    assert_one_polite_request(served)
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert (result.rows_fetched, result.rows_rejected, result.rows_written) == (49, 0, 0)
    assert result.error_message == "49 rows parsed, below the minimum of 50"
    assert result.details["expected"] == THRESHOLDS
    assert result.details["found"] == {"parsed": 49, "rejected": 0}
    assert "rejected" not in result.details
    assert await run_row(seeded_database, result) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "layout_changed",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": 49,
        "rows_written": 0,
        "rows_rejected": 0,
        "error_message": "49 rows parsed, below the minimum of 50",
    }
    assert await run_details(seeded_database, result) == result.details
    assert await bond_rows(seeded_database) == before


async def test_run_at_the_row_minimum_succeeds(
    seeded_database: _Clone,
    bonds_html: str,
    recorded_rows: tuple[tuple[str, ...], ...],
    records: list[BondRecord],
) -> None:
    before = await bond_rows(seeded_database)
    expected = expected_rows(records[:ROW_MINIMUM])

    served = await run(seeded_database, with_body(bonds_html, recorded_rows[:ROW_MINIMUM]))
    result = served.result

    assert_one_polite_request(served)
    assert result.status is RunStatus.SUCCEEDED
    assert (result.rows_fetched, result.rows_rejected) == (ROW_MINIMUM, 0)
    assert result.rows_written == len(expected)
    assert await run_row(seeded_database, result) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "succeeded",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": ROW_MINIMUM,
        "rows_written": len(expected),
        "rows_rejected": 0,
        "error_message": None,
    }
    after = await bond_rows(seeded_database)
    assert {(row[0], row[1]): row for row in after if row[SOURCE_INDEX] == BOND_SOURCE} == expected
    assert seed_rows(after) == before


@pytest.mark.parametrize(
    ("parsed", "count"),
    [
        # The most a run may reject and still write: 32 of 326 rows (10% is 32.6); 65 of the 652
        # rows of the table listed twice (10% is 65.2), more than details.rejected keeps; and 5
        # of the first 50 rows, exactly 10%, which the limit still admits.
        pytest.param(FIXTURE_ROWS, 32, id="32-of-326-rows"),
        pytest.param(2 * FIXTURE_ROWS, 65, id="65-of-652-rows"),
        pytest.param(ROW_MINIMUM, 5, id="5-of-50-rows-exactly-a-tenth"),
    ],
)
async def test_run_rejecting_up_to_a_tenth_of_the_rows_writes_the_valid_rows(
    seeded_database: _Clone,
    bonds_html: str,
    recorded_rows: tuple[tuple[str, ...], ...],
    parsed: int,
    count: int,
) -> None:
    rows = page_rows(recorded_rows, parsed)
    broken = break_rows(bonds_html, rows, count)
    listed = parse_bonds_table(with_body(bonds_html, rows))
    assert len(listed) == parsed
    valid = [record for record in listed if record.line not in broken.lines]
    assert len(valid) == parsed - count
    expected = expected_rows(valid)
    before = await bond_rows(seeded_database)

    served = await run(seeded_database, broken.html)
    result = served.result

    assert_one_polite_request(served)
    assert result.status is RunStatus.PARTIAL
    assert (result.rows_fetched, result.rows_rejected) == (parsed, count)
    assert result.rows_written == len(expected)
    assert result.error_message is None
    # The first 50 rejections are kept as evidence, in source order; the count stays complete.
    assert result.details["rejected"] == list(broken.rejected[:REJECTED_SAMPLE])
    assert await run_row(seeded_database, result) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "partial",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": parsed,
        "rows_written": len(expected),
        "rows_rejected": count,
        "error_message": None,
    }
    assert await run_details(seeded_database, result) == result.details
    after = await bond_rows(seeded_database)
    assert {(row[0], row[1]): row for row in after if row[SOURCE_INDEX] == BOND_SOURCE} == expected
    assert len(after) == SEEDED_BOND_ROWS + len(expected)
    assert seed_rows(after) == before


@pytest.mark.parametrize(
    ("parsed", "count"),
    [
        # 33 of 326 rows, one more than the most a run may reject (10% is 32.6); 60 of 326, more
        # than details.rejected keeps; and 6 of the first 50 rows, one past exactly 10%.
        pytest.param(FIXTURE_ROWS, 33, id="33-of-326-rows"),
        pytest.param(FIXTURE_ROWS, 60, id="60-of-326-rows"),
        pytest.param(ROW_MINIMUM, 6, id="6-of-50-rows-one-past-a-tenth"),
    ],
)
async def test_run_rejecting_more_than_a_tenth_of_the_rows_writes_nothing(
    seeded_database: _Clone,
    bonds_html: str,
    recorded_rows: tuple[tuple[str, ...], ...],
    parsed: int,
    count: int,
) -> None:
    broken = break_rows(bonds_html, page_rows(recorded_rows, parsed), count)
    message = f"{count} of {parsed} rows rejected, more than 10% of the rows parsed"
    before = await bond_rows(seeded_database)

    served = await run(seeded_database, broken.html)
    result = served.result

    assert_one_polite_request(served)
    assert result.status is RunStatus.LAYOUT_CHANGED
    assert (result.rows_fetched, result.rows_rejected, result.rows_written) == (parsed, count, 0)
    assert result.error_message == message
    assert result.details["expected"] == THRESHOLDS
    assert result.details["found"] == {"parsed": parsed, "rejected": count}
    # The first 50 rejections are kept as evidence, in source order; the count stays complete.
    assert result.details["rejected"] == list(broken.rejected[:REJECTED_SAMPLE])
    assert await run_row(seeded_database, result) == {
        "pipeline": "cbk_bonds",
        "trigger": "cli",
        "status": "layout_changed",
        "source_url": TREASURY_BONDS_URL,
        "rows_fetched": parsed,
        "rows_written": 0,
        "rows_rejected": count,
        "error_message": message,
    }
    assert await run_details(seeded_database, result) == result.details
    # The data transaction rolled back: every stored row is as it was before the run.
    assert await bond_rows(seeded_database) == before


# --- The real page (deselected by default) ---------------------------------------------------


@pytest.mark.live
async def test_live_treasury_bonds_page_keeps_its_layout(
    make_settings: Callable[..., Any],
) -> None:
    async with PoliteClient(make_settings(scraper_contact="tests@example.invalid")) as client:
        html = await client.get_text(TREASURY_BONDS_URL)
    assert len(parse_bonds_table(html)) >= MIN_ROWS
