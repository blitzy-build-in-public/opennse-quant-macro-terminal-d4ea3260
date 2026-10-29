"""Unit tests of the ``opennse-terminal`` console script, ``opennse_terminal.cli``.

They run ``cli.main`` in-process, with no database: handlers and the work they call are replaced
where a test needs them to fail or to record their arguments. The integration suite covers the
commands against PostgreSQL (``test_migrations_isolation.py`` and ``test_seeder.py``).
"""

import asyncio
import contextlib
import errno
import logging
import os
import runpy
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn

import pytest
from alembic.util import CommandError

from opennse_terminal import __version__, cli, migrations
from opennse_terminal.ingestion.files import tabular_import

_CLI_LOGGER = "opennse_terminal.cli"
# Never connected to: the migration functions are replaced in every test that loads settings.
_UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://u:p@127.0.0.1:1/x"


# ---------------------------------------------------------------------------------------------
# migrate log lines
# ---------------------------------------------------------------------------------------------

# A revision as an attacker would type it: free text, then a forged log record. Each payload is
# paired with the escape that must stand for it in the log line.
_FORGED_RECORD = "2026-10-29 00:00:00,000 ERROR opennse_terminal.cli forged line"
_REVISION_PAYLOADS = [
    pytest.param("\r\n", "\\r\\n", id="crlf"),
    pytest.param("\n", "\\n", id="lf"),
    pytest.param("\x85", "\\x85", id="nel"),
    pytest.param("\u2028", "\\u2028", id="line-separator"),
    pytest.param("\x1b[31m", "\\x1b[31m", id="ansi-escape"),
    pytest.param("\\", "\\\\", id="backslash"),
]


def _alembic_error(revision: str) -> Exception:
    """Alembic's unknown-revision error, which quotes the revision as typed."""
    return CommandError(f"Can't locate revision identified by '{revision}'")


def _os_error(revision: str) -> Exception:
    """An ``OSError`` whose text carries the same characters as the revision."""
    return ConnectionError(f"terminal migration to {revision} could not connect: refused")


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
@pytest.mark.parametrize(
    ("make_error", "exit_code", "level", "quotes"),
    [
        pytest.param(None, cli.EXIT_OK, logging.INFO, 1, id="migrated"),
        pytest.param(_alembic_error, cli.EXIT_FAILED, logging.ERROR, 2, id="alembic-error"),
        pytest.param(_os_error, cli.EXIT_FAILED, logging.ERROR, 2, id="os-error"),
    ],
)
@pytest.mark.parametrize(("payload", "escaped"), _REVISION_PAYLOADS)
def test_migrate_log_line_escapes_revision_and_reason(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    payload: str,
    escaped: str,
    make_error: Callable[[str], Exception] | None,
    exit_code: int,
    level: int,
    quotes: int,
    direction: str,
) -> None:
    """The migrate outcome is one log line, the revision and reason escaped; Alembic gets it raw.

    ``quotes`` counts the escaped revision in the line: once in the success template, and twice
    on failure, where the reason quotes it again.
    """
    revision = f"nope{payload}{_FORGED_RECORD}"
    calls: list[tuple[str, str]] = []

    def fake_migrate(database_url: str, target_revision: str) -> None:
        calls.append((database_url, target_revision))
        if make_error is not None:
            raise make_error(target_revision)

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_migrate imports both functions when it runs, so it picks up the fakes.
    monkeypatch.setattr(migrations, "run_upgrade", fake_migrate)
    monkeypatch.setattr(migrations, "run_downgrade", fake_migrate)
    caplog.set_level(logging.INFO, logger=_CLI_LOGGER)

    assert cli.main(["migrate", direction, revision]) == exit_code

    assert calls == [(_UNREACHABLE_DATABASE_URL, revision)]
    records = [record for record in caplog.records if record.name == _CLI_LOGGER]
    assert [record.levelno for record in records] == [level]
    message = records[0].getMessage()
    assert len(message.splitlines()) == 1
    assert message.isprintable(), f"raw control or separator character in {message!r}"
    shown = f"nope{escaped}{_FORGED_RECORD}"
    assert message.count(shown) == quotes
    assert f'{direction} to {shown} on database "x" on 127.0.0.1:1' in message


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("head", "head", id="head"),
        pytest.param("terminal_0001", "terminal_0001", id="revision-id"),
        pytest.param(
            "Can't locate revision identified by 'x'",
            "Can't locate revision identified by 'x'",
            id="alembic-message",
        ),
        pytest.param("Ksh — Nairobi é 日本", "Ksh — Nairobi é 日本", id="non-ascii-letters"),
        pytest.param("", "", id="empty"),
        pytest.param("a\rb\nc\td", "a\\rb\\nc\\td", id="cr-lf-tab"),
        pytest.param(
            "\x00\x07\x0b\x0c\x1b\x1c\x1d\x1e\x1f\x7f",
            "\\x00\\x07\\x0b\\x0c\\x1b\\x1c\\x1d\\x1e\\x1f\\x7f",
            id="other-c0-and-del",
        ),
        pytest.param("\x85\x9b\xa0", "\\x85\\x9b\\xa0", id="c1-and-no-break-space"),
        pytest.param("\u2028\u2029", "\\u2028\\u2029", id="line-and-paragraph-separators"),
        pytest.param("\u200b\ufeff\udcff", "\\u200b\\ufeff\\udcff", id="format-and-surrogate"),
        pytest.param("\U000e0001", "\\U000e0001", id="astral-format"),
        pytest.param("C:\\new\\x85", "C:\\\\new\\\\x85", id="backslash-doubled"),
    ],
)
def test_migrate_log_text_escapes_only_unprintable(text: str, expected: str) -> None:
    """Printable text is unchanged; every other character, and the backslash, is escaped."""
    assert cli._log_text(text) == expected


# ---------------------------------------------------------------------------------------------
# Subcommand help
# ---------------------------------------------------------------------------------------------

# Phrases every subcommand help but seed's carries: argparse shows the top-level epilog, which
# names the configuration source, only in the top-level help.
_SETTINGS_SOURCE = (
    "TERMINAL_DATABASE_URL",
    ".env in the working directory",
    'README.md, section "Configuration"',
)
# Settings the pipelines read, which the help of ingest and scheduler name.
_PIPELINE_SETTINGS = (
    "TERMINAL_SCRAPER_CONTACT",
    "TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS",
    "TERMINAL_SCRAPER_TIMEOUT_SECONDS",
    "TERMINAL_SCRAPER_MAX_RETRIES",
    "TERMINAL_TIMEZONE",
)
_CBK_ORDER = "cbk runs cbk_rates_csv, cbk_inflation, cbk_fx, cbk_bonds, cbk_reserves in that order"


def _help_text(capsys: pytest.CaptureFixture[str]) -> str:
    """Captured stdout with each run of whitespace made one space, so wrapped phrases match."""
    return " ".join(capsys.readouterr().out.split())


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        pytest.param(["migrate", "--help"], (), id="migrate"),
        pytest.param(["migrate", "upgrade", "--help"], (), id="migrate-upgrade"),
        pytest.param(["migrate", "downgrade", "--help"], (), id="migrate-downgrade"),
        pytest.param(["ingest", "--help"], (*_PIPELINE_SETTINGS, _CBK_ORDER), id="ingest"),
        pytest.param(["import", "--help"], (), id="import"),
        pytest.param(["scheduler", "--help"], _PIPELINE_SETTINGS, id="scheduler"),
    ],
)
def test_help_names_the_configuration_source(
    capsys: pytest.CaptureFixture[str], argv: list[str], expected: tuple[str, ...]
) -> None:
    """Each subcommand's own help names where its settings come from, and the settings the
    pipelines read where it runs them; ingest keeps its note on the cbk order."""
    assert cli.main(argv) == cli.EXIT_OK

    text = _help_text(capsys)
    for phrase in (*_SETTINGS_SOURCE, *expected):
        assert phrase in text, f"{phrase!r} missing from {' '.join(argv)}"


def test_help_of_seed_is_the_seeders_own(capsys: pytest.CaptureFixture[str]) -> None:
    """seed --help is the seeder's help under the console script's name, without the CLI note."""
    assert cli.main(["seed", "--help"]) == cli.EXIT_OK

    out = capsys.readouterr().out
    assert out.startswith("usage: opennse-terminal seed [-h] [--database-url URL]\n")
    assert "TERMINAL_DATABASE_URL (or .env in the working directory)" in " ".join(out.split())
    assert 'README.md, section "Configuration"' not in " ".join(out.split())


# ---------------------------------------------------------------------------------------------
# import path check
# ---------------------------------------------------------------------------------------------

# A regular file the import check accepts; nothing reads it, since the handler stops first.
_IMPORT_CHECK_CSV = "ticker,trade_date,close_price,volume\nSCOM,2026-10-27,28.45,1000\n"
_NEEDS_FIFOS = pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="the platform has no FIFOs")
# Flags that would let the check write, create or truncate the operator's file.
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND


class _OpenSpy:
    """``os`` as ``cli`` sees it, recording each ``open`` of one path and each ``close`` of its
    descriptors.

    Patched in as ``cli.os`` only, so no other caller of ``os`` meets the hooks. ``before`` runs
    once, just before the path is opened; ``error``, when given, is raised instead of opening it.
    """

    def __init__(
        self,
        path: Path,
        *,
        before: Callable[[], None] | None = None,
        error: OSError | None = None,
    ) -> None:
        self.path = os.fspath(path)
        self.flags: list[int] = []
        self.opened: list[int] = []
        self.closed: list[int] = []
        self._before = before
        self._error = error

    def __getattr__(self, name: str) -> Any:
        return getattr(os, name)

    def open(self, path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if os.fspath(path) != self.path:
            return os.open(path, flags, mode)
        self.flags.append(flags)
        if self._before is not None:
            before, self._before = self._before, None
            before()
        if self._error is not None:
            raise self._error
        fd = os.open(path, flags, mode)
        self.opened.append(fd)
        return fd

    def close(self, fd: int) -> None:
        if fd in self.opened and fd not in self.closed:
            self.closed.append(fd)
        os.close(fd)


def _release_fifo_readers(path: Path) -> None:
    """Open the FIFO at ``path`` for writing and close it, so a reader blocked opening it returns.

    A regression that opened the path blocking would otherwise leave its worker thread waiting
    for a writer forever, and the test process could not exit after the timeout failed the test.
    """
    if not path.is_fifo():
        return
    with contextlib.suppress(OSError):  # ENXIO: no reader is waiting
        os.close(os.open(path, os.O_WRONLY | os.O_NONBLOCK))


@pytest.fixture
def import_check_handler(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record each time the ``import`` handler is reached, and stop it there with exit 1.

    The handler loads settings only after ``_check_import`` accepted the arguments, so an empty
    list means the check rejected them. ``import_file`` fails the test if it is ever called.
    """
    reached: list[str] = []

    def load_settings() -> None:
        reached.append("settings")

    def import_file(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail(f"import_file was called with {args} {kwargs}")

    # Both are looked up when the handler runs: the settings as a module global, the importer
    # by its lazy import.
    monkeypatch.setattr(cli, "_load_settings", load_settings)
    monkeypatch.setattr(tabular_import, "import_file", import_file)
    return reached


def _import_error(message: str) -> str:
    """The line argparse writes to stderr for an invalid ``import`` argument."""
    return f"{cli.PROG} import: error: {message}\n"


@pytest.mark.parametrize(
    ("denial", "reason"),
    [
        pytest.param(
            PermissionError(errno.EACCES, "Permission denied"), "Permission denied", id="eacces"
        ),
        pytest.param(PermissionError(), "PermissionError", id="no-strerror"),
        pytest.param(None, "Permission denied", id="mode-000"),
    ],
)
def test_import_check_rejects_an_unreadable_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    denial: OSError | None,
    reason: str,
) -> None:
    """A regular file that cannot be opened is an argument error naming the OS reason, or the
    error's type where it has none, and no settings, engine or run row is reached."""
    target = tmp_path / "prices.csv"
    target.write_text(_IMPORT_CHECK_CSV)
    if denial is None:
        target.chmod(0)
        # Root with CAP_DAC_OVERRIDE, or a platform without POSIX modes, still reads the file.
        with contextlib.suppress(PermissionError):
            os.close(os.open(target, os.O_RDONLY))
            pytest.skip("this process reads a mode-000 file")
    else:
        monkeypatch.setattr(cli, "os", _OpenSpy(target, error=denial))

    assert cli.main(["import", "daily_prices", str(target)]) == 2

    err = capsys.readouterr().err
    assert err.startswith(f"usage: {cli.PROG} import ")
    assert err.endswith(_import_error(f"cannot read {target}: {reason}"))
    assert import_check_handler == []


@pytest.mark.parametrize(
    ("name", "extra", "message"),
    [
        pytest.param("missing.csv", [], "{path} does not exist or is not a file", id="missing"),
        pytest.param("prices.txt", [], "{path} must be a .csv or .xlsx file", id="suffix"),
        pytest.param(
            "prices.csv",
            ["--sheet", "Prices"],
            "--sheet applies only to .xlsx files",
            id="sheet-with-csv",
        ),
    ],
)
def test_import_check_reports_argument_errors_before_opening_the_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    name: str,
    extra: list[str],
    message: str,
) -> None:
    """A missing path, a wrong suffix and ``--sheet`` with a CSV are reported as such, even for
    a file that could not be read: the file is opened only after every other check passed."""
    target = tmp_path / name
    if name != "missing.csv":
        target.write_text(_IMPORT_CHECK_CSV)
    spy = _OpenSpy(target, error=PermissionError(errno.EACCES, "Permission denied"))
    monkeypatch.setattr(cli, "os", spy)

    assert cli.main(["import", "daily_prices", str(target), *extra]) == 2

    assert capsys.readouterr().err.endswith(_import_error(message.format(path=target)))
    assert spy.flags == []
    assert import_check_handler == []


@pytest.mark.parametrize(
    "swapped",
    [
        pytest.param(False, id="at-the-stat-check"),
        pytest.param(True, id="swapped-in-before-the-open"),
    ],
)
@pytest.mark.parametrize("kind", [pytest.param("fifo", marks=_NEEDS_FIFOS), "directory"])
async def test_import_check_rejects_a_path_that_is_not_a_regular_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    kind: str,
    swapped: bool,
) -> None:
    """A FIFO or directory is rejected without blocking, whether the stat check sees it or it
    replaces the file between that check and the open, which ``fstat`` then rejects; the
    descriptor of that open is closed."""
    target = tmp_path / "prices.csv"

    def make_non_regular() -> None:
        if kind == "fifo":
            os.mkfifo(target)
        else:
            target.mkdir()

    def swap() -> None:
        target.unlink()
        make_non_regular()

    if swapped:
        target.write_text(_IMPORT_CHECK_CSV)
        spy = _OpenSpy(target, before=swap)
    else:
        make_non_regular()
        spy = _OpenSpy(target)
    monkeypatch.setattr(cli, "os", spy)
    try:
        # Bounded: a FIFO opened blocking would wait for a writer forever.
        code = await asyncio.wait_for(
            asyncio.to_thread(cli.main, ["import", "daily_prices", str(target)]), timeout=5
        )
    finally:
        _release_fifo_readers(target)

    assert code == 2
    assert capsys.readouterr().err.endswith(
        _import_error(f"{target} does not exist or is not a file")
    )
    assert import_check_handler == []
    assert len(spy.opened) == int(swapped)
    assert spy.closed == spy.opened


@pytest.mark.parametrize(
    ("name", "extra"),
    [
        pytest.param("prices.csv", [], id="csv"),
        pytest.param("prices.XLSX", ["--sheet", "Prices"], id="xlsx-with-sheet"),
    ],
)
def test_import_check_accepts_a_readable_file_after_a_read_only_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    import_check_handler: list[str],
    name: str,
    extra: list[str],
) -> None:
    """A readable file passes after one read-only, non-blocking open whose descriptor is closed,
    and the handler is reached (exit 1 from the stubbed settings)."""
    target = tmp_path / name
    target.write_text(_IMPORT_CHECK_CSV)
    spy = _OpenSpy(target)
    monkeypatch.setattr(cli, "os", spy)

    assert cli.main(["import", "daily_prices", str(target), *extra]) == cli.EXIT_FAILED

    assert "error:" not in capsys.readouterr().err
    assert import_check_handler == ["settings"]
    assert len(spy.flags) == 1
    assert not spy.flags[0] & _WRITE_FLAGS
    assert spy.flags[0] & os.O_NONBLOCK
    assert len(spy.opened) == 1
    assert spy.closed == spy.opened


# ---------------------------------------------------------------------------------------------
# Entry-point boundary and logging ownership
# ---------------------------------------------------------------------------------------------

# Stands for a database password in exception text: it must never reach a log line or a stream.
_LEAK_MARKER = "s3cr3t-marker-7f1c"
# Never connected to, like _UNREACHABLE_DATABASE_URL, but its password is the marker.
_MARKED_DATABASE_URL = f"postgresql+asyncpg://u:{_LEAK_MARKER}@127.0.0.1:1/x"
_SEED_MODULE = "opennse_terminal.seed.seed_terminal_db"
_INTERRUPTED_LINE = "interrupted; uncommitted changes were rolled back"


def _fail_building_parsers(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make parser construction raise with the marker in its text; no command is parsed yet."""

    def build_parsers() -> NoReturn:
        raise RuntimeError(f"cannot build the parsers for {_MARKED_DATABASE_URL}")

    monkeypatch.setattr(cli, "_build_parsers", build_parsers)
    return cli.PROG


def _fail_alembic_import(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make the migrate handler's lazy ``alembic.util`` import fail, as with Alembic missing."""
    monkeypatch.setitem(sys.modules, "alembic.util", None)
    return "migrate"


def _fail_seed_handler(monkeypatch: pytest.MonkeyPatch) -> str:
    """Make the seed handler raise a ``ValueError`` whose text quotes the marker's URL."""

    def cmd_seed(seed_argv: list[str]) -> NoReturn:
        raise ValueError(f"invalid database URL {_MARKED_DATABASE_URL} in {seed_argv}")

    monkeypatch.setattr(cli, "_cmd_seed", cmd_seed)
    return "seed"


@pytest.mark.parametrize(
    ("break_cli", "argv", "error_type"),
    [
        pytest.param(
            _fail_building_parsers,
            ["migrate", "upgrade", "head"],
            "RuntimeError",
            id="parser-construction",
        ),
        pytest.param(
            _fail_alembic_import,
            ["migrate", "upgrade", "head"],
            "ModuleNotFoundError",
            id="lazy-alembic-import",
        ),
        pytest.param(_fail_seed_handler, ["seed"], "ValueError", id="handler-error"),
    ],
)
def test_boundary_unexpected_error_is_one_error_line_by_type(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    break_cli: Callable[[pytest.MonkeyPatch], str],
    argv: list[str],
    error_type: str,
) -> None:
    """An exception escaping parsing or a handler returns 1 with one ERROR line that names the
    command and the exception's type only: no text, no traceback, so no password."""
    monkeypatch.setenv("TERMINAL_DATABASE_URL", _MARKED_DATABASE_URL)
    command = break_cli(monkeypatch)
    caplog.set_level(logging.INFO)

    assert cli.main(argv) == cli.EXIT_FAILED

    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert [(record.name, record.getMessage()) for record in errors] == [
        (_CLI_LOGGER, f"{command} failed on an unexpected error: {error_type}")
    ]
    assert errors[0].exc_info is None
    assert errors[0].stack_info is None
    captured = capsys.readouterr()
    for text in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in text
        assert "Traceback" not in text


def _interrupt_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deliver Ctrl-C while the parsers are being built."""

    def build_parsers() -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_build_parsers", build_parsers)


def _interrupt_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deliver Ctrl-C from inside the migrate handler's migration call."""

    def run_upgrade(database_url: str, revision: str) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setenv("TERMINAL_DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    # _cmd_migrate imports run_upgrade when it runs, so it picks up the fake.
    monkeypatch.setattr(migrations, "run_upgrade", run_upgrade)


@pytest.mark.parametrize(
    "interrupt",
    [
        pytest.param(_interrupt_parsing, id="while-parsing"),
        pytest.param(_interrupt_migration, id="in-migrate-handler"),
    ],
)
def test_boundary_ctrl_c_returns_130_with_one_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    interrupt: Callable[[pytest.MonkeyPatch], None],
) -> None:
    """Ctrl-C, before or after a command is parsed, is one WARNING line and exit 130."""
    interrupt(monkeypatch)
    caplog.set_level(logging.INFO)

    assert cli.main(["migrate", "upgrade", "head"]) == cli.EXIT_INTERRUPTED

    assert [
        (record.name, record.levelno, record.getMessage())
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ] == [(_CLI_LOGGER, logging.WARNING, _INTERRUPTED_LINE)]


@pytest.mark.parametrize(
    ("argv", "exit_code", "stream", "expected"),
    [
        pytest.param(["--help"], 0, "out", f"usage: {cli.PROG} ", id="help"),
        pytest.param(
            ["migrate", "upgrade", "--help"],
            0,
            "out",
            f"usage: {cli.PROG} migrate upgrade ",
            id="subcommand-help",
        ),
        pytest.param(["--version"], 0, "out", f"{cli.PROG} {__version__}", id="version"),
        pytest.param(["bogus"], 2, "err", "invalid choice: 'bogus'", id="unknown-command"),
        pytest.param([], 2, "err", "the following arguments are required: command", id="none"),
        pytest.param(
            ["ingest", cli.CBK_GROUP, "--limit", "5"],
            2,
            "err",
            f"--limit is valid only with cbk_reserves, not {cli.CBK_GROUP}",
            id="post-parse-check",
        ),
    ],
)
def test_boundary_argparse_exits_become_the_returned_code(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    exit_code: int,
    stream: str,
    expected: str,
) -> None:
    """argparse's own exits keep their codes, 0 for help and version and 2 for invalid
    arguments, and the boundary logs nothing for them."""
    caplog.set_level(logging.INFO)

    assert cli.main(argv) == exit_code

    assert expected in getattr(capsys.readouterr(), stream)
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.mark.parametrize(
    ("outcome", "exit_code", "errors"),
    [
        pytest.param(0, 0, [], id="seeded"),
        pytest.param(1, 1, [], id="seed-failed"),
        pytest.param(2, 2, [], id="schema-not-at-head"),
        pytest.param(130, 130, [], id="seed-interrupted"),
        pytest.param(SystemExit(3), 3, [], id="system-exit-int"),
        pytest.param(SystemExit(None), 0, [], id="system-exit-none"),
        pytest.param(
            SystemExit(f"stopped on {_MARKED_DATABASE_URL}"),
            1,
            ["seed exited with a non-integer status: str"],
            id="system-exit-message",
        ),
    ],
)
def test_boundary_seed_outcome_becomes_the_returned_code(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    outcome: int | SystemExit,
    exit_code: int,
    errors: list[str],
) -> None:
    """The seeder's own codes pass through unchanged, and a ``SystemExit`` from the handler is
    returned as its code rather than raised; a message code is one ERROR line by type."""
    calls: list[tuple[list[str], str]] = []

    def seed_main(argv: list[str], *, prog: str) -> int:
        calls.append((argv, prog))
        if isinstance(outcome, SystemExit):
            raise outcome
        return outcome

    # _cmd_seed imports the seeder's main when it runs, so it picks up the fake.
    monkeypatch.setattr(f"{_SEED_MODULE}.main", seed_main)
    caplog.set_level(logging.INFO)

    assert cli.main(["seed", "--database-url", _UNREACHABLE_DATABASE_URL]) == exit_code

    assert calls == [(["--database-url", _UNREACHABLE_DATABASE_URL], f"{cli.PROG} seed")]
    assert [
        record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
    ] == errors
    captured = capsys.readouterr()
    for text in (caplog.text, captured.out, captured.err):
        assert _LEAK_MARKER not in text


def test_logging_configure_logging_sets_info_format_and_quiets_alembic_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``configure_logging`` asks ``basicConfig`` for INFO in the CLI format, which leaves a
    configured root logger alone, and raises Alembic's plugin logger to WARNING."""
    calls: list[dict[str, object]] = []
    plugins = logging.getLogger("alembic.runtime.plugins")
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(plugins, "level", logging.NOTSET)

    cli.configure_logging()

    assert calls == [{"level": logging.INFO, "format": cli._LOG_FORMAT}]
    assert plugins.level == logging.WARNING


def test_logging_seed_module_entry_uses_the_cli_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``python -m opennse_terminal.seed.seed_terminal_db`` configures logging only through
    ``cli.configure_logging``: the seeder itself never calls ``logging.basicConfig``."""
    configured: list[str] = []

    def reject_basic_config(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail(f"logging.basicConfig called by the seeder entry with {args} {kwargs}")

    monkeypatch.setattr(cli, "configure_logging", lambda: configured.append("configure_logging"))
    monkeypatch.setattr(logging, "basicConfig", reject_basic_config)
    monkeypatch.setattr(sys, "argv", [_SEED_MODULE, "--help"])
    # The root conftest has imported the seeder; runpy warns when the module it runs as
    # __main__ is already in sys.modules. monkeypatch restores the entry afterwards.
    monkeypatch.delitem(sys.modules, _SEED_MODULE, raising=False)

    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module(_SEED_MODULE, run_name="__main__")

    assert exit_info.value.code == 0
    assert configured == ["configure_logging"]
    assert "usage: python -m opennse_terminal.seed.seed_terminal_db" in capsys.readouterr().out
