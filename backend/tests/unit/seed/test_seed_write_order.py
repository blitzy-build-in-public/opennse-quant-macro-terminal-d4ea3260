"""Natural-key write order of the seeder's table writer (``seed_terminal_db._TableWriter``).

* ``write`` de-duplicates and sorts a table's complete input by its natural key before slicing it
  into ``SEED_BATCH_SIZE`` batches, so the batches, concatenated, are in one strictly ascending
  key order. Concurrent writers such as the CSV importer lock rows in that order too, so a seed
  and an import inserting the same new keys cannot deadlock.
* The 30,096 generated price rows, which ``synthetic.generate_prices`` yields per counter with
  SCOM first, reach ``upsert_rows`` in ``(ticker, trade_date)`` order, at most ``SEED_BATCH_SIZE``
  rows per call, each call ``DO NOTHING`` on the natural key with the caller's own mappings; the
  tallies are 30,096 offered and 30,096 inserted.
* An input spanning several batches, in reverse or per-counter order with a repeated key: the last
  occurrence of the key is the one written, ``candidates`` counts every row offered and
  ``inserted`` sums what each call reports.
* A natural-key column that is missing or ``None`` raises ``ValueError`` naming the table, row and
  column before any batch is written.

``upsert_rows`` is replaced by a recorder that keeps every batch, so no database is needed;
writing the seed for real is covered by ``tests/integration/test_seeder.py``.
"""

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from math import ceil

import pytest

from opennse_terminal.models import TerminalDailyPrice, TerminalEquity
from opennse_terminal.seed import seed_terminal_db, synthetic
from opennse_terminal.seed.seed_terminal_db import SEED_BATCH_SIZE

PRICE_KEY = ("ticker", "trade_date")
EQUITY_KEY = ("ticker",)
SEEDED_PRICE_ROWS = 30_096
# Small enough that the short inputs below span several batches.
SMALL_BATCH_SIZE = 2
# Stands in for the AsyncSession: the recorder only checks that each call receives it.
SESSION = object()


@dataclass
class _UpsertCall:
    executor: object
    model: type
    rows: list[Mapping[str, object]]
    key: tuple[str, ...]
    mode: str


@dataclass
class _UpsertRecorder:
    """Stands in for ``upsert_rows``: keeps each batch and reports its rows inserted, except rows
    whose natural key is in ``existing``, which ``DO NOTHING`` would skip."""

    existing: frozenset[tuple[object, ...]] = frozenset()
    calls: list[_UpsertCall] = field(default_factory=list)

    async def __call__(
        self,
        executor: object,
        model: type,
        rows: Sequence[Mapping[str, object]],
        key: Sequence[str],
        mode: str,
    ) -> int:
        self.calls.append(_UpsertCall(executor, model, list(rows), tuple(key), mode))
        return sum(1 for row in rows if _key_of(row, key) not in self.existing)

    @property
    def written(self) -> list[Mapping[str, object]]:
        """Every row offered to ``upsert_rows``, in call order."""
        return [row for call in self.calls for row in call.rows]


def _key_of(row: Mapping[str, object], key: Sequence[str]) -> tuple[object, ...]:
    return tuple(row[name] for name in key)


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> _UpsertRecorder:
    upsert = _UpsertRecorder()
    monkeypatch.setattr(seed_terminal_db, "upsert_rows", upsert)
    return upsert


def _price(ticker: str, day: int, close: str) -> dict[str, object]:
    return {"ticker": ticker, "trade_date": date(2026, 9, day), "close_price": Decimal(close)}


def _equity(ticker: str, name: str) -> dict[str, object]:
    return {"ticker": ticker, "company_name": name}


def _assert_strictly_ascending(keys: Sequence[tuple[object, ...]]) -> None:
    out_of_order = [(a, b) for a, b in zip(keys, keys[1:], strict=False) if not a < b]
    assert out_of_order == []


async def test_generated_prices_are_written_in_natural_key_order(
    recorder: _UpsertRecorder,
) -> None:
    run_id = uuid.uuid4()
    rows = [
        seed_terminal_db._row_with_id(row, ingestion_run_id=run_id)
        for row in synthetic.generate_prices()
    ]
    offered_keys = [_key_of(row, PRICE_KEY) for row in rows]
    # The generator's per-counter order is not key order, so the writer has something to sort.
    assert rows[0]["ticker"] == "SCOM"
    assert offered_keys != sorted(offered_keys)

    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]
    await writer.write(TerminalDailyPrice, PRICE_KEY, rows)

    written_keys = [_key_of(row, PRICE_KEY) for row in recorder.written]
    assert written_keys == sorted(offered_keys)
    _assert_strictly_ascending(written_keys)
    assert len(recorder.calls) == ceil(SEEDED_PRICE_ROWS / SEED_BATCH_SIZE)
    assert all(len(call.rows) <= SEED_BATCH_SIZE for call in recorder.calls)
    assert {(call.executor, call.model, call.key, call.mode) for call in recorder.calls} == {
        (SESSION, TerminalDailyPrice, PRICE_KEY, "do_nothing")
    }
    assert sorted(map(id, recorder.written)) == sorted(map(id, rows))
    assert writer.candidates == {"terminal_daily_prices": SEEDED_PRICE_ROWS}
    assert writer.inserted == {"terminal_daily_prices": SEEDED_PRICE_ROWS}


@pytest.mark.parametrize(
    ("model", "key", "rows", "existing"),
    [
        pytest.param(
            TerminalDailyPrice,
            PRICE_KEY,
            [
                _price("SCOM", 30, "28.10"),
                _price("SCOM", 29, "28.00"),
                _price("KCB", 30, "61.25"),
                _price("SCOM", 30, "28.15"),
                _price("EQTY", 29, "60.00"),
                _price("ABSA", 30, "24.80"),
                _price("ABSA", 29, "24.70"),
            ],
            frozenset({("KCB", date(2026, 9, 30))}),
            id="prices-reverse-order",
        ),
        pytest.param(
            TerminalDailyPrice,
            PRICE_KEY,
            [
                _price("SCOM", 29, "28.00"),
                _price("SCOM", 30, "28.10"),
                _price("EQTY", 29, "60.00"),
                _price("EQTY", 30, "60.50"),
                _price("ABSA", 29, "24.70"),
                _price("EQTY", 29, "60.05"),
                _price("ABSA", 30, "24.80"),
            ],
            frozenset(),
            id="prices-per-counter-order",
        ),
        pytest.param(
            TerminalEquity,
            EQUITY_KEY,
            [
                _equity("SCOM", "Safaricom PLC"),
                _equity("KEGN", "KenGen"),
                _equity("EQTY", "Equity Group Holdings PLC"),
                _equity("KEGN", "Kenya Electricity Generating Company PLC"),
                _equity("ABSA", "Absa Bank Kenya PLC"),
            ],
            frozenset({("EQTY",)}),
            id="equities-single-column-key",
        ),
    ],
)
async def test_batches_follow_the_sorted_unique_keys(
    recorder: _UpsertRecorder,
    monkeypatch: pytest.MonkeyPatch,
    model: type[TerminalDailyPrice] | type[TerminalEquity],
    key: tuple[str, ...],
    rows: list[dict[str, object]],
    existing: frozenset[tuple[object, ...]],
) -> None:
    monkeypatch.setattr(seed_terminal_db, "SEED_BATCH_SIZE", SMALL_BATCH_SIZE)
    recorder.existing = existing
    last_by_key = {_key_of(row, key): row for row in rows}
    expected = [last_by_key[row_key] for row_key in sorted(last_by_key)]
    assert len(expected) > SMALL_BATCH_SIZE

    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]
    await writer.write(model, key, rows)

    # Identity, not equality: each repeated key is written as its last occurrence, as given.
    assert [id(row) for row in recorder.written] == [id(row) for row in expected]
    _assert_strictly_ascending([_key_of(row, key) for row in recorder.written])
    assert [len(call.rows) for call in recorder.calls] == [
        min(SMALL_BATCH_SIZE, len(expected) - start)
        for start in range(0, len(expected), SMALL_BATCH_SIZE)
    ]
    table = model.__tablename__
    assert writer.candidates == {table: len(rows)}
    assert writer.inserted == {table: len(expected) - len(existing)}


@pytest.mark.parametrize(
    ("bad_row", "message"),
    [
        pytest.param(
            {"ticker": "EQTY", "trade_date": None},
            "terminal_daily_prices row 1: natural-key column 'trade_date' is None",
            id="none-value",
        ),
        pytest.param(
            {"trade_date": date(2026, 9, 30)},
            "terminal_daily_prices row 1: natural-key column 'ticker' is missing",
            id="missing-column",
        ),
    ],
)
async def test_unusable_natural_key_raises_before_any_batch(
    recorder: _UpsertRecorder, bad_row: dict[str, object], message: str
) -> None:
    rows = [_price("SCOM", 30, "28.10"), bad_row, _price("ABSA", 30, "24.80")]
    writer = seed_terminal_db._TableWriter(SESSION)  # type: ignore[arg-type]

    with pytest.raises(ValueError) as raised:
        await writer.write(TerminalDailyPrice, PRICE_KEY, rows)

    assert str(raised.value) == message
    assert recorder.calls == []
    assert writer.inserted == {}
    assert writer.candidates == {}
