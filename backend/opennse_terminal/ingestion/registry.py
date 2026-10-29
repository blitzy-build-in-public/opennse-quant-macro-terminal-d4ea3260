"""The ingestion pipeline registry: each public-source pipeline by name, and the run helpers.

The CLI (``opennse-terminal ingest <name>``, and ``ingest cbk`` for the five CBK pipelines) and
the scheduler process resolve pipelines here, so a new public source plugs in as a new module
plus one :data:`PIPELINES` entry. Every entry is a public source: the NSE's public
market-statistics web page or a Central Bank of Kenya statistical release.

Registered pipelines, in :data:`PIPELINES` order:

- ``nse_market_statistics``: the public NSE market-statistics page
  (https://www.nse.co.ke/market-statistics/, "Equity Statistics"), rendered with Playwright and
  parsed with BeautifulSoup4, into ``terminal_daily_prices`` (close, volume and estimated
  turnover). Option ``trade_date``: the operator's ``--trade-date`` for an ambiguous session.
- ``cbk_rates_csv``: the CBK "Central Bank Rates" CSV linked from
  https://www.centralbank.go.ke/statistics/interest-rates/, into ``terminal_macro_indicators``
  (``interbank_rate_pct``, ``tbill_91d_pct``, ``tbill_182d_pct``, ``tbill_364d_pct``,
  ``cbr_rate_pct``).
- ``cbk_inflation``: the CBK inflation-rates table at
  https://www.centralbank.go.ke/inflation-rates/, into
  ``terminal_macro_indicators.inflation_yoy_pct``.
- ``cbk_fx``: the CBK "Exchange Rates End Period" CSV linked from
  https://www.centralbank.go.ke/statistics/exchange-rates/, into
  ``terminal_macro_indicators.fx_kes_usd``.
- ``cbk_bonds``: the CBK Treasury bonds table at
  https://www.centralbank.go.ke/bills-bonds/treasury-bonds/, into ``terminal_bond_yields``.
- ``cbk_reserves``: the CBK Weekly Bulletin PDFs listed at
  https://www.centralbank.go.ke/releases/weekly-bulletin/, into
  ``terminal_macro_indicators.fx_reserves_usd_m``. Option ``limit`` (1-26): backfill the newest
  N bulletins instead of the newest one not yet ingested.

CSV/Excel imports (``csv_import:<kind>``) are not registered: the CLI calls
``ingestion.files.tabular_import.import_file`` directly. PDF filings arrive through
``POST /ingest/pdf``.

Importing this module imports no pipeline module. Each :class:`PipelineSpec` names its module and
class as strings, and :func:`load_pipeline` imports them on demand, so the CLI can build its
argparse choices without loading Playwright, BeautifulSoup4, pandas or pdfplumber.
"""

import importlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Any, Final

from sqlalchemy.ext.asyncio import AsyncEngine

from opennse_terminal.config import TerminalSettings
from opennse_terminal.db.session import build_engine
from opennse_terminal.ingestion.base import Pipeline, RunResult, RunTrigger, execute_pipeline

__all__ = [
    "CBK_PIPELINES",
    "MAX_LIMIT",
    "MIN_LIMIT",
    "PIPELINES",
    "PipelineSpec",
    "create_pipeline",
    "load_pipeline",
    "run_pipeline",
    "run_pipelines",
]

logger = logging.getLogger("opennse_terminal.ingestion.registry")


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    """Where a registered pipeline class lives, and which run options it accepts.

    Attributes:
        module: Dotted path of the source module, imported only by :func:`load_pipeline`.
        attr: Name of the :class:`~opennse_terminal.ingestion.base.Pipeline` subclass in it.
        supports_trade_date: The class takes ``trade_date: date`` (the CLI's ``--trade-date``).
        supports_limit: The class takes ``limit: int`` (the CLI's ``--limit``).
    """

    module: str
    attr: str
    supports_trade_date: bool = False
    supports_limit: bool = False


PIPELINES: Final[Mapping[str, PipelineSpec]] = MappingProxyType(
    {
        "nse_market_statistics": PipelineSpec(
            module="opennse_terminal.ingestion.nse.market_statistics",
            attr="MarketStatisticsPipeline",
            supports_trade_date=True,
        ),
        "cbk_rates_csv": PipelineSpec(
            module="opennse_terminal.ingestion.cbk.rates_csv",
            attr="CbkRatesCsvPipeline",
        ),
        "cbk_inflation": PipelineSpec(
            module="opennse_terminal.ingestion.cbk.inflation",
            attr="CbkInflationPipeline",
        ),
        "cbk_fx": PipelineSpec(
            module="opennse_terminal.ingestion.cbk.fx",
            attr="CbkFxPipeline",
        ),
        "cbk_bonds": PipelineSpec(
            module="opennse_terminal.ingestion.cbk.bonds",
            attr="CbkBondsPipeline",
        ),
        "cbk_reserves": PipelineSpec(
            module="opennse_terminal.ingestion.cbk.reserves",
            attr="CbkReservesPipeline",
            supports_limit=True,
        ),
    }
)
"""Every registered pipeline by name, in the order above. Read-only."""

CBK_PIPELINES: Final[tuple[str, ...]] = (
    "cbk_rates_csv",
    "cbk_inflation",
    "cbk_fx",
    "cbk_bonds",
    "cbk_reserves",
)
"""The five CBK pipelines in run order: ``ingest cbk`` and the scheduler's ``cbk_macro`` job."""

MIN_LIMIT: Final = 1
MAX_LIMIT: Final = 26
"""Bounds of ``limit``, the number of newest weekly bulletins a ``cbk_reserves`` run backfills.

``CbkReservesPipeline`` enforces the same bounds; they are repeated here so an out-of-range value
is rejected before the pipeline module is imported."""


def _spec(name: str) -> PipelineSpec:
    """The spec registered under ``name``; ``KeyError`` listing the valid names otherwise."""
    try:
        return PIPELINES[name]
    except KeyError:
        raise KeyError(f"unknown pipeline {name!r}; valid names: {', '.join(PIPELINES)}") from None


def _names_where(option: str) -> str:
    """The registered names whose spec accepts ``option`` (a ``supports_*`` field), for errors."""
    return ", ".join(name for name, spec in PIPELINES.items() if getattr(spec, option))


def load_pipeline(name: str) -> type[Pipeline[Any, Any]]:
    """Import the module registered under ``name`` and return its pipeline class.

    Raises:
        KeyError: ``name`` is not registered; the message lists the valid names.
        ImportError: The module cannot be imported.
        AttributeError: The module has no attribute named by the spec.
        TypeError: That attribute is not a ``Pipeline`` subclass, or its ``name`` is not
            ``name``, so its runs would be recorded and locked under another name.
    """
    spec = _spec(name)
    module = importlib.import_module(spec.module)
    candidate: object = getattr(module, spec.attr)
    if not (isinstance(candidate, type) and issubclass(candidate, Pipeline)):
        raise TypeError(f"{spec.module}.{spec.attr} is not a Pipeline subclass")
    registered = getattr(candidate, "name", None)
    if registered != name:
        raise TypeError(
            f"{spec.module}.{spec.attr}.name is {registered!r}; the registry expects {name!r}"
        )
    return candidate


def create_pipeline(
    name: str,
    settings: TerminalSettings,
    *,
    trade_date: date | None = None,
    limit: int | None = None,
) -> Pipeline[Any, Any]:
    """Construct a new, single-use instance of the pipeline registered under ``name``.

    Options are checked before the pipeline module is imported. Only the options that are given
    are passed, as keywords: ``cls(settings, trade_date=...)``, ``cls(settings, limit=...)`` or
    ``cls(settings)``.

    Raises:
        KeyError: ``name`` is not registered.
        ValueError: ``trade_date`` is given for a pipeline other than ``nse_market_statistics``,
            ``limit`` for one other than ``cbk_reserves``, or ``limit`` is outside
            :data:`MIN_LIMIT`-:data:`MAX_LIMIT`.
        ImportError, AttributeError, TypeError: From :func:`load_pipeline`, or from the
            pipeline's own constructor.
    """
    spec = _spec(name)
    options: dict[str, Any] = {}
    if trade_date is not None:
        if not spec.supports_trade_date:
            raise ValueError(
                f"pipeline {name!r} does not take a trade date; "
                f"only {_names_where('supports_trade_date')} does"
            )
        options["trade_date"] = trade_date
    if limit is not None:
        if not spec.supports_limit:
            raise ValueError(
                f"pipeline {name!r} does not take a limit; "
                f"only {_names_where('supports_limit')} does"
            )
        if not MIN_LIMIT <= limit <= MAX_LIMIT:
            raise ValueError(f"limit must be between {MIN_LIMIT} and {MAX_LIMIT}, got {limit}")
        options["limit"] = limit
    return load_pipeline(name)(settings, **options)


async def _dispose(engine: AsyncEngine) -> None:
    """Release ``engine``'s pool, logging rather than raising a failure.

    It runs once the runs on ``engine`` have ended, so their results are final: a pool that fails
    to close must not replace a run's result, or an exception already propagating, with its own
    error. Only the type name is logged, as elsewhere in ingestion, so no connection detail of the
    database URL reaches a log line.
    """
    try:
        await engine.dispose()
    except Exception as exc:
        logger.warning("could not dispose the ingestion engine: %s", type(exc).__name__)


async def run_pipeline(
    name: str,
    settings: TerminalSettings,
    *,
    trigger: RunTrigger,
    trade_date: date | None = None,
    limit: int | None = None,
) -> RunResult:
    """Run one pipeline end to end on an engine of its own, and return its recorded result.

    The run's lock, transaction and ``terminal_ingestion_runs`` row are those of
    :func:`~opennse_terminal.ingestion.base.execute_pipeline`, so a pipeline failure is a status
    on the result rather than an exception. The engine comes from
    :func:`~opennse_terminal.db.session.build_engine` and is disposed when the run ends.

    Raises:
        KeyError, ValueError, ImportError, AttributeError, TypeError: From
            :func:`create_pipeline`, before any engine is created or run row written.
        ValueError: ``settings.database_url`` is not a usable ``postgresql+asyncpg://`` URL.
        Exception: What ``execute_pipeline`` propagates: an error writing the run row.
        asyncio.CancelledError: Re-raised once the cancelled run is recorded ``failed``.
    """
    pipeline = create_pipeline(name, settings, trade_date=trade_date, limit=limit)
    engine = build_engine(settings)
    try:
        return await execute_pipeline(pipeline, engine, trigger=trigger)
    finally:
        await _dispose(engine)


async def run_pipelines(
    names: Sequence[str], settings: TerminalSettings, *, trigger: RunTrigger
) -> list[RunResult]:
    """Run the named pipelines one after another on a shared engine, and return their results.

    Each name gets a fresh pipeline instance and a run of its own through
    :func:`~opennse_terminal.ingestion.base.execute_pipeline`: its own run row, transaction and
    advisory lock, taken only after the previous run has released its own. A failing pipeline
    ends with its status on its result, and the remaining pipelines still run. Used by
    ``ingest cbk`` and the scheduler's ``cbk_macro`` job with :data:`CBK_PIPELINES`.

    Every name is checked before anything runs, so a misspelt name starts no run. A run that
    raises instead of returning a result (its pipeline could not be constructed, or its run row
    could not be written) is logged at ERROR and does not stop the sequence either; once every
    name has run, those exceptions are raised together as an ``ExceptionGroup``, each with a note
    naming its pipeline. The completed runs' results are then in ``terminal_ingestion_runs``.
    A cancellation stops the sequence at once, after the cancelled run is recorded.

    Raises:
        TypeError: ``names`` is a single string rather than a sequence of names.
        KeyError: A name is not registered; nothing has run.
        ValueError: ``settings.database_url`` is not a usable ``postgresql+asyncpg://`` URL.
        ExceptionGroup: One or more runs raised instead of returning a result.
        asyncio.CancelledError: The sequence was cancelled.
    """
    if isinstance(names, str):
        raise TypeError("names must be a sequence of pipeline names, not a single string")
    for name in names:
        _spec(name)
    results: list[RunResult] = []
    errors: list[Exception] = []
    engine = build_engine(settings)
    try:
        for name in names:
            # A run that raises still lets the rest run, and is surfaced afterwards rather than
            # dropped, so a caller deriving an exit code from the results never misses it.
            try:
                pipeline = create_pipeline(name, settings)
                results.append(await execute_pipeline(pipeline, engine, trigger=trigger))
            except Exception as exc:
                logger.error(
                    "ingestion run of %s raised %s; continuing with the remaining pipelines",
                    name,
                    type(exc).__name__,
                )
                exc.add_note(f"raised by the {name} ingestion run")
                errors.append(exc)
    finally:
        await _dispose(engine)
    if errors:
        raise ExceptionGroup(
            f"{len(errors)} of {len(names)} ingestion runs raised instead of returning a result",
            errors,
        )
    return results
