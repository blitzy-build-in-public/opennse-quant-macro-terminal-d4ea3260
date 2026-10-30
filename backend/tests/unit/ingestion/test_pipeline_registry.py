"""Tests for the ingestion pipeline registry (``opennse_terminal.ingestion.registry``).

Covered: the registered names, their specs and run options (:data:`PIPELINES`,
:data:`CBK_PIPELINES` and the ``limit`` bounds); :func:`load_pipeline` for every registered name
and its error paths; that importing the registry imports no pipeline module; the option routing
of :func:`create_pipeline` (``trade_date`` for ``nse_market_statistics`` only, ``limit`` 1-26 for
``cbk_reserves`` only); and the engine, spacing, error and cancellation handling of
:func:`run_pipeline` and :func:`run_pipelines`.

Every test is offline: no network, no database and no browser. The pipeline loader, the engine
factory, ``execute_pipeline`` and ``HostSpacing`` are replaced by capturing doubles wherever a run
is involved, so nothing is fetched, connected to or slept on. Expected names, classes, bounds and
messages are literals from the plan and the registry's documented contract, never recomputed from
the module under test.
"""

import asyncio
import inspect
import logging
import os
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from opennse_terminal.config import TerminalSettings
from opennse_terminal.ingestion import http as ingestion_http
from opennse_terminal.ingestion import registry
from opennse_terminal.ingestion.base import (
    Pipeline,
    RunContext,
    RunResult,
    RunStatus,
    RunTrigger,
    ValidationOutcome,
)
from opennse_terminal.ingestion.cbk.reserves import CbkReservesPipeline
from opennse_terminal.ingestion.http import HostSpacing
from opennse_terminal.ingestion.registry import PipelineSpec

LOGGER = "opennse_terminal.ingestion.registry"

REGISTERED_NAMES = (
    "nse_market_statistics",
    "cbk_rates_csv",
    "cbk_inflation",
    "cbk_fx",
    "cbk_bonds",
    "cbk_reserves",
)
CBK_NAMES = ("cbk_rates_csv", "cbk_inflation", "cbk_fx", "cbk_bonds", "cbk_reserves")
NOT_TAKING_LIMIT = (
    "nse_market_statistics",
    "cbk_rates_csv",
    "cbk_inflation",
    "cbk_fx",
    "cbk_bonds",
)
VALID_NAMES_MESSAGE = (
    "valid names: nse_market_statistics, cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, "
    "cbk_reserves"
)
PIPELINE_MODULES = (
    "opennse_terminal.ingestion.nse.market_statistics",
    "opennse_terminal.ingestion.cbk.rates_csv",
    "opennse_terminal.ingestion.cbk.inflation",
    "opennse_terminal.ingestion.cbk.fx",
    "opennse_terminal.ingestion.cbk.bonds",
    "opennse_terminal.ingestion.cbk.reserves",
)
HEAVY_LIBRARIES = ("playwright", "pdfplumber", "pandas", "bs4")
TRADE_DATE = date(2026, 10, 27)

# Stands for connection detail a real database error could carry; no log line may repeat an
# exception's message.
SENTINEL = "sentinel-connection-detail-4f1c"

FAKE_MODULE = "registry_test_fake_sources"
MISSING_MODULE = "registry_test_absent_source_module"

# The child interpreter imports the registry, then prints which watched modules are loaded.
_IMPORT_PROBE_SCRIPT = (
    "import sys\n"
    "import opennse_terminal.ingestion.registry\n"
    "print('\\n'.join(name for name in sys.argv[1:] if name in sys.modules))\n"
)
_PROBE_TIMEOUT_SECONDS = 120


def run_result(
    pipeline: str,
    run_id: str,
    status: RunStatus = RunStatus.SUCCEEDED,
    *,
    fetched: int = 0,
    written: int = 0,
    rejected: int = 0,
    error_message: str | None = None,
) -> RunResult:
    """A recorded result with the given literal values and empty ``details``."""
    return RunResult(
        run_id=uuid.UUID(run_id),
        pipeline=pipeline,
        status=status,
        rows_fetched=fetched,
        rows_written=written,
        rows_rejected=rejected,
        error_message=error_message,
        details={},
    )


RESULTS: Mapping[str, RunResult] = MappingProxyType(
    {
        "nse_market_statistics": run_result(
            "nse_market_statistics",
            "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a01",
            fetched=70,
            written=68,
            rejected=2,
        ),
        "cbk_rates_csv": run_result(
            "cbk_rates_csv", "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a02", fetched=423, written=423
        ),
        "cbk_inflation": run_result(
            "cbk_inflation", "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a03", fetched=261, written=261
        ),
        "cbk_fx": run_result(
            "cbk_fx", "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a04", fetched=405, written=405
        ),
        "cbk_bonds": run_result(
            "cbk_bonds", "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a05", fetched=326, written=326
        ),
        "cbk_reserves": run_result(
            "cbk_reserves", "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a06", fetched=5, written=5
        ),
    }
)
"""One literal result per registered pipeline, returned by :class:`ExecuteDouble` by default."""

LOCK_HELD = run_result(
    "cbk_inflation",
    "0b6a3d6e-1c1f-4f0e-9a51-6b3f8d1e2a13",
    RunStatus.FAILED,
    error_message="lock held",
)


# --- Doubles ---------------------------------------------------------------------------------


class FakeSourcePipeline(Pipeline[str, str]):
    """A well-formed pipeline class for the fake source module. The registry only loads and
    constructs pipelines, so each stage fails the test if it is ever called."""

    name = "fake_source"

    async def fetch(self, ctx: RunContext) -> str:
        raise AssertionError("the registry never fetches")

    def parse(self, raw: str) -> list[str]:
        raise AssertionError("the registry never parses")

    def validate(self, records: Sequence[str]) -> ValidationOutcome[str]:
        raise AssertionError("the registry never validates")

    async def upsert(self, conn: AsyncConnection, records: Sequence[str], run_id: uuid.UUID) -> int:
        raise AssertionError("the registry never upserts")


class MisnamedPipeline(FakeSourcePipeline):
    """A pipeline whose ``name`` differs from the name it is registered under."""

    name = "another_source"


class PlainClass:
    """A class that is not a ``Pipeline`` subclass, although it carries the registered name."""

    name = "fake_plain_class"


@dataclass(eq=False)
class BuiltPipeline:
    """What a :class:`RecordingLoader` class constructs: the registry name it was loaded under and
    the exact positional and keyword arguments of the constructor call. Compared by identity, so
    two runs never share one."""

    name: str
    args: tuple[object, ...]
    kwargs: dict[str, object]


class RecordingLoader:
    """Stands in for ``registry.load_pipeline``: records each name loaded and returns a
    constructor for it, which records its call as a :class:`BuiltPipeline` or raises the error
    scripted for that name."""

    def __init__(self, constructor_errors: Mapping[str, Exception] | None = None) -> None:
        self.loaded: list[str] = []
        self.built: list[BuiltPipeline] = []
        self._constructor_errors = dict(constructor_errors or {})

    def __call__(self, name: str) -> Callable[..., BuiltPipeline]:
        self.loaded.append(name)

        def construct(*args: object, **kwargs: object) -> BuiltPipeline:
            error = self._constructor_errors.get(name)
            if error is not None:
                raise error
            built = BuiltPipeline(name=name, args=args, kwargs=kwargs)
            self.built.append(built)
            return built

        return construct


class FakeEngine:
    """Stands in for the ``AsyncEngine`` from ``build_engine``: counts ``dispose()`` calls and
    raises ``dispose_error`` from each when given."""

    def __init__(self, dispose_error: Exception | None = None) -> None:
        self.dispose_calls = 0
        self._dispose_error = dispose_error

    async def dispose(self) -> None:
        self.dispose_calls += 1
        if self._dispose_error is not None:
            raise self._dispose_error


class EngineFactory:
    """Stands in for ``registry.build_engine``: records the settings of every call and returns
    the same :class:`FakeEngine` each time."""

    def __init__(self, engine: FakeEngine) -> None:
        self.engine = engine
        self.calls: list[TerminalSettings] = []

    def __call__(self, settings: TerminalSettings) -> FakeEngine:
        self.calls.append(settings)
        return self.engine


@dataclass(frozen=True, slots=True)
class ExecuteCall:
    """One call of :class:`ExecuteDouble`, with the arguments the registry passed."""

    pipeline: BuiltPipeline
    engine: FakeEngine
    trigger: RunTrigger
    spacing: HostSpacing | None


class ExecuteDouble:
    """Stands in for ``registry.execute_pipeline``: records every call, then raises the exception
    scripted for the pipeline's name or returns its result (:data:`RESULTS` by default). Its
    signature has no ``http`` parameter, so a registry that passed one would fail the run."""

    def __init__(self, outcomes: Mapping[str, RunResult | BaseException] | None = None) -> None:
        self.calls: list[ExecuteCall] = []
        self._outcomes: dict[str, RunResult | BaseException] = {**RESULTS, **(outcomes or {})}

    async def __call__(
        self,
        pipeline: BuiltPipeline,
        engine: FakeEngine,
        *,
        trigger: RunTrigger,
        spacing: HostSpacing | None,
    ) -> RunResult:
        self.calls.append(ExecuteCall(pipeline, engine, trigger, spacing))
        outcome = self._outcomes[pipeline.name]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class SpacingFactory:
    """Stands in for ``ingestion.http.HostSpacing`` in ``run_pipelines``: records every instance
    it creates. The instances are real, and are only passed along, never awaited."""

    def __init__(self) -> None:
        self.created: list[HostSpacing] = []

    def __call__(self) -> HostSpacing:
        spacing = HostSpacing()
        self.created.append(spacing)
        return spacing


@dataclass
class RunDoubles:
    """The doubles installed in place of the registry's run dependencies."""

    loader: RecordingLoader
    engines: EngineFactory
    execute: ExecuteDouble
    spacings: SpacingFactory = field(default_factory=SpacingFactory)


def install_run_doubles(
    monkeypatch: pytest.MonkeyPatch,
    *,
    outcomes: Mapping[str, RunResult | BaseException] | None = None,
    constructor_errors: Mapping[str, Exception] | None = None,
    dispose_error: Exception | None = None,
) -> RunDoubles:
    """Replace the pipeline loader, ``build_engine``, ``execute_pipeline`` and ``HostSpacing``."""
    doubles = RunDoubles(
        loader=RecordingLoader(constructor_errors),
        engines=EngineFactory(FakeEngine(dispose_error)),
        execute=ExecuteDouble(outcomes),
    )
    monkeypatch.setattr(registry, "load_pipeline", doubles.loader)
    monkeypatch.setattr(registry, "build_engine", doubles.engines)
    monkeypatch.setattr(registry, "execute_pipeline", doubles.execute)
    monkeypatch.setattr(ingestion_http, "HostSpacing", doubles.spacings)
    return doubles


def registry_records(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    """The registry's captured log records as ``(level, message)`` pairs, in order."""
    return [
        (record.levelno, record.getMessage()) for record in caplog.records if record.name == LOGGER
    ]


@pytest.fixture
def fake_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register a fake source module and a registry that points into it (and at a missing one)."""
    module = ModuleType(FAKE_MODULE)
    module.__dict__.update(
        FakeSourcePipeline=FakeSourcePipeline,
        MisnamedPipeline=MisnamedPipeline,
        PlainClass=PlainClass,
        source_pipeline_name="fake_source",
    )
    monkeypatch.setitem(sys.modules, FAKE_MODULE, module)
    monkeypatch.delitem(sys.modules, MISSING_MODULE, raising=False)
    monkeypatch.setattr(
        registry,
        "PIPELINES",
        MappingProxyType(
            {
                "fake_source": PipelineSpec(module=FAKE_MODULE, attr="FakeSourcePipeline"),
                "fake_plain_class": PipelineSpec(module=FAKE_MODULE, attr="PlainClass"),
                "fake_not_a_class": PipelineSpec(module=FAKE_MODULE, attr="source_pipeline_name"),
                "fake_misnamed": PipelineSpec(module=FAKE_MODULE, attr="MisnamedPipeline"),
                "fake_missing_attr": PipelineSpec(module=FAKE_MODULE, attr="AbsentPipeline"),
                "fake_missing_module": PipelineSpec(
                    module=MISSING_MODULE, attr="FakeSourcePipeline"
                ),
            }
        ),
    )


# --- The registered pipelines ----------------------------------------------------------------


def test_registered_names_are_the_six_public_sources_in_order() -> None:
    assert list(registry.PIPELINES) == list(REGISTERED_NAMES)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param(
            "nse_market_statistics",
            PipelineSpec(
                module="opennse_terminal.ingestion.nse.market_statistics",
                attr="MarketStatisticsPipeline",
                supports_trade_date=True,
                supports_limit=False,
            ),
            id="nse_market_statistics",
        ),
        pytest.param(
            "cbk_rates_csv",
            PipelineSpec(
                module="opennse_terminal.ingestion.cbk.rates_csv",
                attr="CbkRatesCsvPipeline",
                supports_trade_date=False,
                supports_limit=False,
            ),
            id="cbk_rates_csv",
        ),
        pytest.param(
            "cbk_inflation",
            PipelineSpec(
                module="opennse_terminal.ingestion.cbk.inflation",
                attr="CbkInflationPipeline",
                supports_trade_date=False,
                supports_limit=False,
            ),
            id="cbk_inflation",
        ),
        pytest.param(
            "cbk_fx",
            PipelineSpec(
                module="opennse_terminal.ingestion.cbk.fx",
                attr="CbkFxPipeline",
                supports_trade_date=False,
                supports_limit=False,
            ),
            id="cbk_fx",
        ),
        pytest.param(
            "cbk_bonds",
            PipelineSpec(
                module="opennse_terminal.ingestion.cbk.bonds",
                attr="CbkBondsPipeline",
                supports_trade_date=False,
                supports_limit=False,
            ),
            id="cbk_bonds",
        ),
        pytest.param(
            "cbk_reserves",
            PipelineSpec(
                module="opennse_terminal.ingestion.cbk.reserves",
                attr="CbkReservesPipeline",
                supports_trade_date=False,
                supports_limit=True,
            ),
            id="cbk_reserves",
        ),
    ],
)
def test_each_spec_names_its_module_class_and_options(name: str, expected: PipelineSpec) -> None:
    """``--trade-date`` belongs to ``nse_market_statistics`` only, ``--limit`` to
    ``cbk_reserves`` only."""
    assert registry.PIPELINES[name] == expected


def test_registry_mapping_is_read_only() -> None:
    writable_view: Any = registry.PIPELINES
    assert isinstance(registry.PIPELINES, MappingProxyType)
    with pytest.raises(TypeError):
        writable_view["cbk_new_source"] = PipelineSpec(module="x", attr="y")
    with pytest.raises(TypeError):
        del writable_view["cbk_fx"]
    assert list(registry.PIPELINES) == list(REGISTERED_NAMES)


def test_cbk_pipelines_run_in_the_documented_order() -> None:
    assert registry.CBK_PIPELINES == (
        "cbk_rates_csv",
        "cbk_inflation",
        "cbk_fx",
        "cbk_bonds",
        "cbk_reserves",
    )


def test_limit_bounds_are_one_to_twenty_six() -> None:
    assert registry.MIN_LIMIT == 1
    assert registry.MAX_LIMIT == 26


# --- load_pipeline ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "module", "class_name"),
    [
        pytest.param(
            "nse_market_statistics",
            "opennse_terminal.ingestion.nse.market_statistics",
            "MarketStatisticsPipeline",
            id="nse_market_statistics",
        ),
        pytest.param(
            "cbk_rates_csv",
            "opennse_terminal.ingestion.cbk.rates_csv",
            "CbkRatesCsvPipeline",
            id="cbk_rates_csv",
        ),
        pytest.param(
            "cbk_inflation",
            "opennse_terminal.ingestion.cbk.inflation",
            "CbkInflationPipeline",
            id="cbk_inflation",
        ),
        pytest.param("cbk_fx", "opennse_terminal.ingestion.cbk.fx", "CbkFxPipeline", id="cbk_fx"),
        pytest.param(
            "cbk_bonds", "opennse_terminal.ingestion.cbk.bonds", "CbkBondsPipeline", id="cbk_bonds"
        ),
        pytest.param(
            "cbk_reserves",
            "opennse_terminal.ingestion.cbk.reserves",
            "CbkReservesPipeline",
            id="cbk_reserves",
        ),
    ],
)
def test_load_pipeline_returns_the_registered_class(
    name: str, module: str, class_name: str
) -> None:
    """The class is a ``Pipeline`` subclass whose ``name`` is the name it is registered under,
    so its runs are recorded and locked under that name."""
    loaded = registry.load_pipeline(name)

    assert issubclass(loaded, Pipeline)
    assert (loaded.__module__, loaded.__name__) == (module, class_name)
    assert loaded is getattr(sys.modules[module], class_name)
    assert loaded.name == name


def test_load_pipeline_rejects_an_unknown_name_listing_the_valid_ones() -> None:
    with pytest.raises(KeyError) as excinfo:
        registry.load_pipeline("nse_bonds")

    assert excinfo.value.args == (f"unknown pipeline 'nse_bonds'; {VALID_NAMES_MESSAGE}",)


@pytest.mark.usefixtures("fake_sources")
def test_load_pipeline_imports_the_module_its_spec_names() -> None:
    """The positive control of the fake registry the error cases below use."""
    assert registry.load_pipeline("fake_source") is FakeSourcePipeline


@pytest.mark.usefixtures("fake_sources")
@pytest.mark.parametrize(
    ("name", "error", "message"),
    [
        pytest.param(
            "fake_plain_class",
            TypeError,
            "registry_test_fake_sources.PlainClass is not a Pipeline subclass",
            id="class-not-a-pipeline",
        ),
        pytest.param(
            "fake_not_a_class",
            TypeError,
            "registry_test_fake_sources.source_pipeline_name is not a Pipeline subclass",
            id="attribute-not-a-class",
        ),
        pytest.param(
            "fake_misnamed",
            TypeError,
            "registry_test_fake_sources.MisnamedPipeline.name is 'another_source'; "
            "the registry expects 'fake_misnamed'",
            id="pipeline-with-another-name",
        ),
        pytest.param(
            "fake_missing_attr",
            AttributeError,
            "module 'registry_test_fake_sources' has no attribute 'AbsentPipeline'",
            id="missing-attribute",
        ),
        pytest.param(
            "fake_missing_module",
            ModuleNotFoundError,
            "No module named 'registry_test_absent_source_module'",
            id="missing-module",
        ),
        pytest.param(
            "cbk_fx",
            KeyError,
            "unknown pipeline 'cbk_fx'; valid names: fake_source, fake_plain_class, "
            "fake_not_a_class, fake_misnamed, fake_missing_attr, fake_missing_module",
            id="name-not-in-this-registry",
        ),
    ],
)
def test_load_pipeline_rejects_a_spec_that_does_not_resolve_to_its_pipeline(
    name: str, error: type[Exception], message: str
) -> None:
    with pytest.raises(error) as excinfo:
        registry.load_pipeline(name)

    assert type(excinfo.value) is error
    assert excinfo.value.args == (message,)


async def test_importing_the_registry_imports_no_pipeline_module(tmp_path: Path) -> None:
    """A fresh interpreter that imports only the registry has loaded none of the six pipeline
    modules, nor Playwright, pdfplumber, pandas or BeautifulSoup4, so the CLI can list the names
    without loading them."""
    watched = ["opennse_terminal.ingestion.registry", *PIPELINE_MODULES, *HEAVY_LIBRARIES]
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _IMPORT_PROBE_SCRIPT,
        *watched,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        cwd=tmp_path,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), _PROBE_TIMEOUT_SECONDS)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

    assert process.returncode == 0, stderr.decode()
    assert stdout.decode().split() == ["opennse_terminal.ingestion.registry"]


# --- create_pipeline: option routing ---------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "options", "expected_kwargs"),
    [
        *(pytest.param(name, {}, {}, id=f"{name}-no-options") for name in REGISTERED_NAMES),
        pytest.param(
            "cbk_reserves",
            {"trade_date": None, "limit": None},
            {},
            id="cbk_reserves-options-given-as-none",
        ),
        pytest.param(
            "nse_market_statistics",
            {"trade_date": TRADE_DATE},
            {"trade_date": TRADE_DATE},
            id="nse_market_statistics-trade-date",
        ),
        pytest.param("cbk_reserves", {"limit": 1}, {"limit": 1}, id="cbk_reserves-limit-1"),
        pytest.param("cbk_reserves", {"limit": 26}, {"limit": 26}, id="cbk_reserves-limit-26"),
    ],
)
def test_create_pipeline_passes_only_the_given_options_as_keywords(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    name: str,
    options: dict[str, Any],
    expected_kwargs: dict[str, object],
) -> None:
    loader = RecordingLoader()
    monkeypatch.setattr(registry, "load_pipeline", loader)

    # Typed as object: the loader double's constructors return BuiltPipeline records.
    created: object = registry.create_pipeline(name, test_settings, **options)

    assert loader.loaded == [name]
    [built] = loader.built
    assert created is built
    assert len(built.args) == 1
    assert built.args[0] is test_settings
    assert built.kwargs == expected_kwargs


@pytest.mark.parametrize(
    ("name", "options", "error", "message"),
    [
        *(
            pytest.param(
                name,
                {"trade_date": TRADE_DATE},
                ValueError,
                f"pipeline '{name}' does not take a trade date; only nse_market_statistics does",
                id=f"trade-date-for-{name}",
            )
            for name in CBK_NAMES
        ),
        *(
            pytest.param(
                name,
                {"limit": 2},
                ValueError,
                f"pipeline '{name}' does not take a limit; only cbk_reserves does",
                id=f"limit-for-{name}",
            )
            for name in NOT_TAKING_LIMIT
        ),
        pytest.param(
            "nse_market_statistics",
            {"trade_date": TRADE_DATE, "limit": 2},
            ValueError,
            "pipeline 'nse_market_statistics' does not take a limit; only cbk_reserves does",
            id="trade-date-with-limit-for-nse",
        ),
        pytest.param(
            "cbk_reserves",
            {"trade_date": TRADE_DATE, "limit": 2},
            ValueError,
            "pipeline 'cbk_reserves' does not take a trade date; only nse_market_statistics does",
            id="trade-date-with-limit-for-reserves",
        ),
        pytest.param(
            "cbk_reserves",
            {"limit": 0},
            ValueError,
            "limit must be between 1 and 26, got 0",
            id="limit-0",
        ),
        pytest.param(
            "cbk_reserves",
            {"limit": 27},
            ValueError,
            "limit must be between 1 and 26, got 27",
            id="limit-27",
        ),
        pytest.param(
            "cbk_reserves",
            {"limit": -1},
            ValueError,
            "limit must be between 1 and 26, got -1",
            id="limit-minus-1",
        ),
        pytest.param(
            "cbk_reserve",
            {"limit": 2},
            KeyError,
            f"unknown pipeline 'cbk_reserve'; {VALID_NAMES_MESSAGE}",
            id="unknown-name",
        ),
    ],
)
def test_create_pipeline_rejects_a_name_or_option_before_importing_the_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    name: str,
    options: dict[str, Any],
    error: type[Exception],
    message: str,
) -> None:
    loader = RecordingLoader()
    monkeypatch.setattr(registry, "load_pipeline", loader)

    with pytest.raises(error) as excinfo:
        registry.create_pipeline(name, test_settings, **options)

    assert type(excinfo.value) is error
    assert excinfo.value.args == (message,)
    assert loader.loaded == []


@pytest.mark.parametrize(
    ("limit", "expected_limit"),
    [
        pytest.param(None, None, id="newest-not-yet-ingested"),
        pytest.param(1, 1, id="limit-1"),
        pytest.param(26, 26, id="limit-26"),
    ],
)
def test_create_pipeline_constructs_the_real_reserves_pipeline(
    test_settings: TerminalSettings, limit: int | None, expected_limit: int | None
) -> None:
    """Un-doubled: the real class takes the routed ``limit``. Its constructor does no I/O."""
    pipeline = registry.create_pipeline("cbk_reserves", test_settings, limit=limit)

    assert type(pipeline) is CbkReservesPipeline
    assert pipeline.settings is test_settings
    assert pipeline.limit == expected_limit


@pytest.mark.parametrize(
    ("name", "option"),
    [
        pytest.param("nse_market_statistics", "trade_date", id="nse_market_statistics-trade_date"),
        pytest.param("cbk_reserves", "limit", id="cbk_reserves-limit"),
    ],
)
def test_real_pipeline_classes_take_their_option_as_a_keyword(name: str, option: str) -> None:
    """The keyword the routing passes is the one the real constructor declares, keyword-only.

    ``MarketStatisticsPipeline`` is checked by signature rather than constructed: its constructor
    loads the shipped exchange-calendar file, and these tests do no I/O.
    """
    parameter = inspect.signature(registry.load_pipeline(name)).parameters[option]

    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None


# --- run_pipeline ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("trigger", "with_spacing"),
    [
        pytest.param("cli", True, id="cli-with-shared-spacing"),
        pytest.param("schedule", False, id="schedule-without-spacing"),
    ],
)
async def test_run_pipeline_runs_on_its_own_engine_and_disposes_it(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    trigger: RunTrigger,
    with_spacing: bool,
) -> None:
    doubles = install_run_doubles(monkeypatch)
    spacing = HostSpacing() if with_spacing else None

    result = await registry.run_pipeline(
        "cbk_reserves", test_settings, trigger=trigger, limit=3, spacing=spacing
    )

    assert result == RESULTS["cbk_reserves"]
    assert doubles.engines.calls == [test_settings]
    assert doubles.engines.calls[0] is test_settings
    [built] = doubles.loader.built
    assert built.kwargs == {"limit": 3}
    assert doubles.execute.calls == [
        ExecuteCall(built, doubles.engines.engine, trigger, spacing),
    ]
    assert doubles.execute.calls[0].spacing is spacing
    assert doubles.engines.engine.dispose_calls == 1


@pytest.mark.parametrize(
    "error_type",
    [
        pytest.param(RuntimeError, id="execute-raises"),
        pytest.param(asyncio.CancelledError, id="cancelled"),
    ],
)
async def test_run_pipeline_disposes_the_engine_when_the_run_raises(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    error_type: type[BaseException],
) -> None:
    error = error_type(f"run row not written: {SENTINEL}")
    doubles = install_run_doubles(monkeypatch, outcomes={"cbk_fx": error})

    with pytest.raises(type(error)) as excinfo:
        await registry.run_pipeline("cbk_fx", test_settings, trigger="cli")

    assert excinfo.value is error
    assert len(doubles.execute.calls) == 1
    assert doubles.engines.engine.dispose_calls == 1


@pytest.mark.parametrize(
    ("name", "options", "error"),
    [
        pytest.param("cbk_fxx", {}, KeyError, id="unknown-name"),
        pytest.param("cbk_fx", {"trade_date": TRADE_DATE}, ValueError, id="trade-date-for-cbk"),
        pytest.param("cbk_reserves", {"limit": 27}, ValueError, id="limit-27"),
    ],
)
async def test_run_pipeline_rejects_options_before_building_an_engine(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    name: str,
    options: dict[str, Any],
    error: type[Exception],
) -> None:
    doubles = install_run_doubles(monkeypatch)

    with pytest.raises(error):
        await registry.run_pipeline(name, test_settings, trigger="cli", **options)

    assert doubles.loader.loaded == []
    assert doubles.engines.calls == []
    assert doubles.execute.calls == []


async def test_run_pipeline_logs_a_failed_dispose_by_type_and_returns_the_result(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    doubles = install_run_doubles(monkeypatch, dispose_error=OSError(f"pool close: {SENTINEL}"))

    result = await registry.run_pipeline("cbk_bonds", test_settings, trigger="cli")

    assert result == RESULTS["cbk_bonds"]
    assert doubles.engines.engine.dispose_calls == 1
    assert registry_records(caplog) == [
        (logging.WARNING, "could not dispose the ingestion engine: OSError"),
    ]
    assert SENTINEL not in caplog.text


async def test_run_pipeline_keeps_the_run_error_when_dispose_also_fails(
    monkeypatch: pytest.MonkeyPatch, test_settings: TerminalSettings
) -> None:
    """A pool that fails to close never replaces the exception already propagating."""
    error = RuntimeError("run row not written")
    doubles = install_run_doubles(
        monkeypatch, outcomes={"cbk_bonds": error}, dispose_error=OSError("pool close")
    )

    with pytest.raises(RuntimeError) as excinfo:
        await registry.run_pipeline("cbk_bonds", test_settings, trigger="cli")

    assert excinfo.value is error
    assert doubles.engines.engine.dispose_calls == 1


# --- run_pipelines ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("names", "error", "message"),
    [
        pytest.param(
            "cbk_fx",
            TypeError,
            "names must be a sequence of pipeline names, not a single string",
            id="single-string",
        ),
        pytest.param(
            ["cbk_rates_csv", "cbk_inflaton", "cbk_fx"],
            KeyError,
            f"unknown pipeline 'cbk_inflaton'; {VALID_NAMES_MESSAGE}",
            id="unknown-name-among-valid-ones",
        ),
    ],
)
async def test_run_pipelines_checks_the_names_before_running_any(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    names: str | list[str],
    error: type[Exception],
    message: str,
) -> None:
    """A misspelt name, or a single string passed for the sequence, starts no run."""
    doubles = install_run_doubles(monkeypatch)

    with pytest.raises(error) as excinfo:
        await registry.run_pipelines(names, test_settings, trigger="schedule")

    assert type(excinfo.value) is error
    assert excinfo.value.args == (message,)
    assert doubles.loader.loaded == []
    assert doubles.spacings.created == []
    assert doubles.engines.calls == []
    assert doubles.execute.calls == []


@pytest.mark.parametrize(
    ("outcomes", "expected_results"),
    [
        pytest.param(
            {},
            [RESULTS[name] for name in CBK_NAMES],
            id="all-succeed",
        ),
        pytest.param(
            {"cbk_inflation": LOCK_HELD},
            [
                RESULTS["cbk_rates_csv"],
                LOCK_HELD,
                RESULTS["cbk_fx"],
                RESULTS["cbk_bonds"],
                RESULTS["cbk_reserves"],
            ],
            id="a-failed-status-does-not-stop-the-sequence",
        ),
    ],
)
async def test_run_pipelines_runs_each_name_in_order_on_one_engine_and_spacing(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    outcomes: Mapping[str, RunResult],
    expected_results: list[RunResult],
) -> None:
    doubles = install_run_doubles(monkeypatch, outcomes=outcomes)

    results = await registry.run_pipelines(CBK_NAMES, test_settings, trigger="schedule")

    assert results == expected_results
    assert doubles.loader.loaded == list(CBK_NAMES)
    assert [call.pipeline.name for call in doubles.execute.calls] == list(CBK_NAMES)
    assert [call.pipeline for call in doubles.execute.calls] == doubles.loader.built
    assert len({id(call.pipeline) for call in doubles.execute.calls}) == len(CBK_NAMES)
    assert all(
        built.args == (test_settings,) and built.args[0] is test_settings and built.kwargs == {}
        for built in doubles.loader.built
    )
    assert doubles.engines.calls == [test_settings]
    assert all(call.engine is doubles.engines.engine for call in doubles.execute.calls)
    [spacing] = doubles.spacings.created
    assert all(call.spacing is spacing for call in doubles.execute.calls)
    assert {call.trigger for call in doubles.execute.calls} == {"schedule"}
    assert doubles.engines.engine.dispose_calls == 1


@pytest.mark.parametrize(
    ("execute_raises", "constructor_raises", "logged", "message", "executed"),
    [
        pytest.param(
            ("cbk_inflation",),
            (),
            [
                "ingestion run of cbk_inflation raised RuntimeError; "
                "continuing with the remaining pipelines",
            ],
            "1 of 5 ingestion runs raised instead of returning a result",
            ["cbk_rates_csv", "cbk_inflation", "cbk_fx", "cbk_bonds", "cbk_reserves"],
            id="one-run-raises",
        ),
        pytest.param(
            ("cbk_inflation",),
            ("cbk_bonds",),
            [
                "ingestion run of cbk_inflation raised RuntimeError; "
                "continuing with the remaining pipelines",
                "ingestion run of cbk_bonds raised TypeError; "
                "continuing with the remaining pipelines",
            ],
            "2 of 5 ingestion runs raised instead of returning a result",
            ["cbk_rates_csv", "cbk_inflation", "cbk_fx", "cbk_reserves"],
            id="a-run-and-a-constructor-raise",
        ),
    ],
)
async def test_run_pipelines_runs_the_rest_and_raises_the_errors_together(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
    execute_raises: tuple[str, ...],
    constructor_raises: tuple[str, ...],
    logged: list[str],
    message: str,
    executed: list[str],
) -> None:
    """A run that raises (``execute_pipeline`` failing to write its run row, or a pipeline that
    cannot be constructed) is logged by type only; the remaining pipelines still run, and the
    errors are raised together at the end, each noted with its pipeline."""
    caplog.set_level(logging.INFO, logger=LOGGER)
    execute_errors = {
        name: RuntimeError(f"run row not written: {SENTINEL}") for name in execute_raises
    }
    constructor_errors = {
        name: TypeError(f"bad argument: {SENTINEL}") for name in constructor_raises
    }
    doubles = install_run_doubles(
        monkeypatch, outcomes=execute_errors, constructor_errors=constructor_errors
    )
    raised_by = [name for name in CBK_NAMES if name in {*execute_raises, *constructor_raises}]
    expected_errors = [{**execute_errors, **constructor_errors}[name] for name in raised_by]

    with pytest.raises(ExceptionGroup) as excinfo:
        await registry.run_pipelines(CBK_NAMES, test_settings, trigger="schedule")

    group = excinfo.value
    assert group.message == message
    assert len(group.exceptions) == len(expected_errors)
    assert all(
        raised is expected
        for raised, expected in zip(group.exceptions, expected_errors, strict=True)
    )
    assert [raised.__notes__ for raised in group.exceptions] == [
        [f"raised by the {name} ingestion run"] for name in raised_by
    ]
    assert registry_records(caplog) == [(logging.ERROR, line) for line in logged]
    assert SENTINEL not in caplog.text
    assert doubles.loader.loaded == list(CBK_NAMES)
    assert [call.pipeline.name for call in doubles.execute.calls] == executed
    assert doubles.engines.calls == [test_settings]
    assert doubles.engines.engine.dispose_calls == 1


async def test_run_pipelines_stops_at_once_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    test_settings: TerminalSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    cancellation = asyncio.CancelledError()
    doubles = install_run_doubles(monkeypatch, outcomes={"cbk_inflation": cancellation})

    with pytest.raises(asyncio.CancelledError) as excinfo:
        await registry.run_pipelines(CBK_NAMES, test_settings, trigger="schedule")

    assert excinfo.value is cancellation
    assert doubles.loader.loaded == ["cbk_rates_csv", "cbk_inflation"]
    assert [call.pipeline.name for call in doubles.execute.calls] == [
        "cbk_rates_csv",
        "cbk_inflation",
    ]
    assert doubles.engines.engine.dispose_calls == 1
    assert registry_records(caplog) == []
