"""Cycle service: bounded reads and the log sink of ``GET /cycles/backtest``, on seeded PostgreSQL.

``services/cycle_service.py`` reads the stored history with bound ``select()`` statements, runs
the pure ``quant.backtest.run_cycle_backtest`` once per event and rounds each output once with
``quantize_out``. The contract held here:

- **Bounded session reads.** Each cycle reads its own observed sessions: one ``DISTINCT
  trade_date`` statement over ``[window_start, min(window_end, data_available_through)]`` and, when
  that window holds a session, one backward ``DISTINCT trade_date`` statement before
  ``window_start`` with ``LIMIT 30`` (the 20-session turnover mean and the 10-session base close).
  No statement reads ``terminal_daily_prices`` without a predicate, and a cycle whose window holds
  no data reads no lead-in and no price row.
- **Bounded overlay read.** One statement reads the overlay rows over each observed cycle's
  ``[window_start - 62 days, last observed session]``, overlapping or adjacent ranges merged, so
  the history between distant events is never read; a request with no observed cycle reads none.
- **Same answer as the full history.** Status, anchor (the first market session in the window),
  cohort, every point's trade date and index levels, and every overlay equal what the full
  observed calendar, every price row and every macro row give: the backtest of
  ``quant.run_cycle_backtest`` over that full history, and ``macro_as_of`` at most 62 days old.
- **Log escaping.** The DEBUG summary repr-escapes each stored ``terminal_cycles.label``, so CR,
  LF and other control or line-separator characters cannot forge a log line; the response
  carries the stored label unchanged.

Seeded windows: 2013-03-04 +-180 days is 2012-09-05 to 2013-08-31, whose last session is
2013-08-30; 2022-08-09 +-180 days is 2022-02-10 to 2023-02-05, whose last session is 2023-02-03.
The seeded prices end on 2026-09-30.

Every test gets a fresh seeded clone (the root conftest's ``seeded_database``, which marks it
``integration``); the label tests change only their own clone. Every expected value is an exact
literal or is derived from the full stored history read without bounds.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import Any, Protocol

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from opennse_terminal.config import TerminalSettings
from opennse_terminal.models import TerminalCycle, TerminalMacroIndicator
from opennse_terminal.quant.asof import MacroColumn, MacroRow, macro_as_of
from opennse_terminal.quant.backtest import (
    BacktestCandidate,
    CycleBacktestResult,
    CycleEvent,
    CycleWindow,
    run_cycle_backtest,
)
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.schemas.cycles import BacktestResponse, CycleResult
from opennse_terminal.services import cycle_service as cs
from opennse_terminal.services import market_service as ms

LOGGER = "opennse_terminal.services.cycle_service"
LATEST = date(2026, 9, 30)
OVERLAY_COLUMNS: tuple[MacroColumn, ...] = ("inflation_yoy_pct", "tbill_91d_pct", "tbill_364d_pct")
SESSION_READ = "SELECT DISTINCT terminal_daily_prices.trade_date"
PRICE_READ = "SELECT terminal_daily_prices.ticker, terminal_daily_prices.trade_date"
MACRO_READ = "FROM terminal_macro_indicators"

Statement = tuple[str, tuple[Any, ...]]


class SeededDatabase(Protocol):
    """The part of the root conftest's ``seeded_database`` handle this module uses."""

    @property
    def engine(self) -> AsyncEngine: ...

    @property
    def settings(self) -> TerminalSettings: ...


@pytest_asyncio.fixture
async def session(seeded_database: SeededDatabase) -> AsyncIterator[AsyncSession]:
    """A session on this test's own seeded clone."""
    factory = async_sessionmaker(seeded_database.engine, expire_on_commit=False)
    async with factory() as db_session:
        yield db_session


@pytest.fixture
def settings(seeded_database: SeededDatabase) -> TerminalSettings:
    """The clone's settings: Africa/Nairobi, 90-day fundamentals lag."""
    return seeded_database.settings


@contextmanager
def captured_statements(engine: AsyncEngine) -> Iterator[list[Statement]]:
    """Every SQL statement ``engine`` sends while the block runs, with its bound parameters."""
    statements: list[Statement] = []

    def record(
        conn: object,
        cursor: object,
        statement: str,
        parameters: Sequence[Any],
        context: object,
        executemany: bool,
    ) -> None:
        statements.append((statement, tuple(parameters)))

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)


def reads_of(statements: Sequence[Statement], marker: str) -> list[Statement]:
    """The captured statements whose SQL contains ``marker``, in execution order."""
    return [statement for statement in statements if marker in statement[0]]


def assert_daily_price_reads_bounded(statements: Sequence[Statement]) -> None:
    """Every ``terminal_daily_prices`` read has a predicate; session reads bound ``trade_date``."""
    for sql, _ in reads_of(statements, "FROM terminal_daily_prices"):
        assert "WHERE" in sql, sql
    for sql, _ in reads_of(statements, SESSION_READ):
        assert "WHERE terminal_daily_prices.trade_date" in sql, sql


# ---------------------------------------------------------------------------------------------
# Bounded statements
# ---------------------------------------------------------------------------------------------


async def test_distant_events_read_only_their_windows_and_lead_in(
    seeded_database: SeededDatabase, session: AsyncSession, settings: TerminalSettings
) -> None:
    """2013 and 2022 read their own sessions, 30-session lead-ins and overlays, not 2014-2021."""
    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(
            session, settings, event_dates=[date(2022, 8, 9), date(2013, 3, 4)]
        )

    assert [cycle.status for cycle in response.cycles] == ["complete", "complete"]
    assert_daily_price_reads_bounded(statements)
    session_reads = reads_of(statements, SESSION_READ)
    assert [params for _, params in session_reads] == [
        (date(2012, 9, 5), date(2013, 8, 31)),
        (date(2012, 9, 5), 30),
        (date(2022, 2, 10), date(2023, 2, 5)),
        (date(2022, 2, 10), 30),
    ]
    for sql, _ in session_reads[1::2]:
        assert "WHERE terminal_daily_prices.trade_date < " in sql
        assert "ORDER BY terminal_daily_prices.trade_date DESC" in sql
        assert "LIMIT " in sql

    (macro_sql, macro_params), *others = reads_of(statements, MACRO_READ)
    assert others == []
    ranges = list(zip(macro_params[::2], macro_params[1::2], strict=True))
    assert ranges == [
        (date(2012, 7, 5), date(2013, 8, 30)),
        (date(2021, 12, 10), date(2023, 2, 3)),
    ]
    assert not any(start <= date(2016, 1, 1) <= end for start, end in ranges)
    assert macro_sql.count("terminal_macro_indicators.observation_date >= ") == 2

    price_reads = reads_of(statements, PRICE_READ)
    assert [params[:2] for _, params in price_reads] == [
        (date(2012, 7, 25), date(2013, 8, 30)),
        (date(2021, 12, 30), date(2023, 2, 3)),
    ]


@pytest.mark.parametrize(
    ("request_args", "ranges"),
    [
        pytest.param(
            {"event_dates": [date(2017, 8, 8), date(2017, 12, 1)]},
            [(date(2016, 12, 9), date(2018, 5, 30))],
            id="overlapping",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 5)], "pre_days": 30, "post_days": 30},
            [(date(2012, 12, 2), date(2013, 8, 2))],
            id="adjacent",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 6)], "pre_days": 30, "post_days": 30},
            [(date(2012, 12, 2), date(2013, 4, 3)), (date(2013, 4, 5), date(2013, 8, 5))],
            id="one_day_apart",
        ),
    ],
)
async def test_overlay_ranges_merge_when_they_overlap_or_touch(
    seeded_database: SeededDatabase,
    session: AsyncSession,
    settings: TerminalSettings,
    request_args: dict[str, Any],
    ranges: list[tuple[date, date]],
) -> None:
    """Each cycle's ``[window_start - 62, last session]`` merges only with a range it meets."""
    with captured_statements(seeded_database.engine) as statements:
        await cs.get_backtest(session, settings, **request_args)

    ((_, macro_params),) = reads_of(statements, MACRO_READ)
    assert list(zip(macro_params[::2], macro_params[1::2], strict=True)) == ranges


@pytest.mark.parametrize(
    ("event_date", "window", "session_reads"),
    [
        pytest.param(date(2027, 8, 10), {}, [], id="window_after_the_data"),
        pytest.param(
            date(2005, 1, 1),
            {"pre_days": 30, "post_days": 30},
            [(date(2004, 12, 2), date(2005, 1, 31))],
            id="window_before_the_data",
        ),
    ],
)
async def test_awaiting_data_request_reads_no_lead_in_prices_or_overlays(
    seeded_database: SeededDatabase,
    session: AsyncSession,
    settings: TerminalSettings,
    event_date: date,
    window: dict[str, int],
    session_reads: list[tuple[date, date]],
) -> None:
    """A window without data reads at most its own bounded sessions, then nothing else."""
    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(session, settings, event_dates=[event_date], **window)

    assert [cycle.status for cycle in response.cycles] == ["awaiting_data"]
    assert_daily_price_reads_bounded(statements)
    assert [params for _, params in reads_of(statements, SESSION_READ)] == session_reads
    assert reads_of(statements, " LIMIT ") == []
    assert reads_of(statements, PRICE_READ) == []
    assert reads_of(statements, MACRO_READ) == []


# ---------------------------------------------------------------------------------------------
# Equivalence with the full stored history
# ---------------------------------------------------------------------------------------------


async def full_overlay_rows(session: AsyncSession) -> list[MacroRow]:
    """Every stored macro observation's overlay columns, ascending, read without any bound."""
    result = await session.execute(
        select(
            TerminalMacroIndicator.observation_date,
            TerminalMacroIndicator.inflation_yoy_pct,
            TerminalMacroIndicator.tbill_91d_pct,
            TerminalMacroIndicator.tbill_364d_pct,
        ).order_by(TerminalMacroIndicator.observation_date)
    )
    return [
        MacroRow(
            observation_date=observation_date,
            inflation_yoy_pct=inflation,
            tbill_91d_pct=tbill_91d,
            tbill_364d_pct=tbill_364d,
        )
        for observation_date, inflation, tbill_91d, tbill_364d in result
    ]


async def full_history_cycles(
    session: AsyncSession,
    settings: TerminalSettings,
    response: BacktestResponse,
    macro_rows: Sequence[MacroRow],
) -> list[CycleBacktestResult]:
    """Backtest each returned cycle over the full calendar, every price row and every macro row."""
    sessions = await ms.load_market_sessions(session)
    equities = await ms.load_equities(session, tickers=response.candidates)
    prices = await ms.load_price_rows(session, response.candidates, None, None)
    fundamentals = await ms.load_fundamentals_rows(session, response.candidates)
    candidates = [
        BacktestCandidate(
            ticker=equity.ticker,
            shares_outstanding=equity.shares_outstanding,
            prices=prices[equity.ticker],
            fundamentals=fundamentals[equity.ticker],
            currency=equity.currency,
        )
        for equity in equities
    ]
    return [
        run_cycle_backtest(
            CycleEvent(cycle.label, cycle.event_date),
            CycleWindow(cycle.event_date, cycle.window_start, cycle.window_end),
            candidates,
            sessions,
            macro_rows,
            data_available_through=response.data_available_through,
            lag_days=settings.fundamentals_lag_days,
        )
        for cycle in response.cycles
    ]


def assert_cycle_matches(
    cycle: CycleResult,
    reference: CycleBacktestResult,
    in_window: Sequence[date],
    macro_rows: Sequence[MacroRow],
) -> None:
    """``cycle`` is the full-history ``reference``, rounded once, with overlays as of each day."""
    assert cycle.status == reference.status
    assert cycle.anchor_date == (in_window[0] if in_window else None)
    assert cycle.anchor_date == reference.anchor_date
    assert cycle.days_until_window_start == reference.days_until_window_start
    assert cycle.constituents == sorted(reference.constituents)
    assert cycle.excluded_constituents == sorted(reference.excluded_constituents)
    assert [point.trade_date for point in cycle.points] == list(in_window)
    assert len(cycle.points) == len(reference.points)
    for point, expected in zip(cycle.points, reference.points, strict=True):
        assert point.offset_days == (point.trade_date - cycle.event_date).days
        assert point.price_index == quantize_out(expected.price_index, "index")
        assert point.turnover_index == quantize_out(expected.turnover_index, "index")
        assert point.pe_ratio == quantize_out(expected.pe_ratio, "ratio")
        assert point.pe_index == quantize_out(expected.pe_index, "index")
        assert point.cumulative_foreign_net_kes == quantize_out(
            expected.cumulative_foreign_net_kes, "money"
        )
        for column in OVERLAY_COLUMNS:
            resolved = macro_as_of(macro_rows, column, point.trade_date, 62)
            assert getattr(point, column) == (
                None if resolved is None else quantize_out(resolved.value, "pct")
            )


@pytest.mark.parametrize(
    ("request_args", "statuses"),
    [
        pytest.param({}, ["complete", "complete", "complete", "awaiting_data"], id="default"),
        pytest.param(
            {"pre_months": 12, "post_months": 12},
            ["complete", "complete", "complete", "partial"],
            id="twelve_months",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2022, 8, 9)]},
            ["complete", "complete"],
            id="distant_events",
        ),
        pytest.param(
            {"event_dates": [date(2017, 8, 8), date(2017, 12, 1)]},
            ["complete", "complete"],
            id="overlapping_windows",
        ),
        pytest.param(
            {"event_dates": [date(2013, 3, 4), date(2013, 7, 5)], "pre_days": 30, "post_days": 30},
            ["complete", "complete"],
            id="adjacent_overlay_ranges",
        ),
        pytest.param(
            {"event_dates": [date(2012, 1, 3)], "pre_days": 365, "post_days": 10},
            ["complete"],
            id="no_session_before_window",
        ),
        pytest.param(
            {"event_dates": [date(2012, 2, 1)], "pre_days": 7, "post_days": 30},
            ["complete"],
            id="short_lead_in",
        ),
        pytest.param(
            {"event_dates": [date(2005, 1, 1)], "pre_days": 30, "post_days": 30},
            ["awaiting_data"],
            id="window_before_the_data",
        ),
    ],
)
async def test_bounded_reads_give_the_full_history_answer(
    session: AsyncSession,
    settings: TerminalSettings,
    request_args: dict[str, Any],
    statuses: list[str],
) -> None:
    """Anchors, cohorts, points and overlays equal those of the full stored history."""
    response = await cs.get_backtest(session, settings, **request_args)

    assert response.data_available_through == LATEST
    assert [cycle.status for cycle in response.cycles] == statuses
    full_calendar = await ms.load_market_sessions(session)
    macro_rows = await full_overlay_rows(session)
    references = await full_history_cycles(session, settings, response, macro_rows)
    for cycle, reference in zip(response.cycles, references, strict=True):
        last_day = min(cycle.window_end, LATEST)
        in_window = [day for day in full_calendar if cycle.window_start <= day <= last_day]
        assert_cycle_matches(cycle, reference, in_window, macro_rows)


async def test_short_lead_in_reads_every_earlier_session(
    seeded_database: SeededDatabase, session: AsyncSession, settings: TerminalSettings
) -> None:
    """With 17 sessions before the window, the limited lead-in returns all 17 and no more."""
    full_calendar = await ms.load_market_sessions(session)
    window_start = date(2012, 1, 25)
    earlier = [day for day in full_calendar if day < window_start]
    assert len(earlier) == 17

    with captured_statements(seeded_database.engine) as statements:
        response = await cs.get_backtest(
            session, settings, event_dates=[date(2012, 2, 1)], pre_days=7, post_days=30
        )

    (cycle,) = response.cycles
    assert cycle.window_start == window_start
    assert cycle.anchor_date == date(2012, 1, 25)
    (price_read,) = reads_of(statements, PRICE_READ)
    assert price_read[1][:2] == (earlier[0], date(2012, 3, 2))


# ---------------------------------------------------------------------------------------------
# Log escaping
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "label",
    [
        pytest.param("2032 Cycle\r\nINFO opennse_terminal: forged entry", id="crlf"),
        pytest.param("2032 Cycle\nWARNING forged line", id="lf"),
        pytest.param("2032 Cycle\rERROR overwritten line", id="cr"),
        pytest.param("2032 Cycle\u2028forged line", id="line_separator"),
        pytest.param("2032 Cycle\x85forged line", id="next_line"),
        pytest.param("2032 Cycle\x1b[2K\x1b[1Gforged line", id="ansi_erase"),
    ],
)
async def test_debug_summary_escapes_stored_labels(
    session: AsyncSession,
    settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    label: str,
) -> None:
    """A label's control characters stay escaped in one log line; the response keeps them."""
    session.add(TerminalCycle(label=label, event_date=date(2032, 8, 10), source="test"))
    await session.commit()
    caplog.set_level(logging.DEBUG, logger=LOGGER)

    response = await cs.get_backtest(session, settings, event_dates=[date(2032, 8, 10)])

    (record,) = [record for record in caplog.records if record.name == LOGGER]
    message = record.getMessage()
    assert message == (
        f"cycle backtest over 8 candidates through 2026-09-30: {label!r}=awaiting_data"
    )
    assert "\r" not in message
    assert "\n" not in message
    assert message.isprintable()
    (cycle,) = response.cycles
    assert cycle.label == label
    assert cycle.status == "awaiting_data"
