"""The ``opennse-terminal`` console script: migrations, seeding, ingestion, import and scheduling.

Subcommands, each reading the database from ``TERMINAL_DATABASE_URL`` (or ``.env``):

* ``migrate upgrade [REVISION]`` and ``migrate downgrade REVISION`` apply the terminal's own
  Alembic history. It is recorded in ``terminal_alembic_version`` and creates or drops only
  ``terminal_`` tables, so hosts run the same command against their own database.
* ``seed [--database-url URL]`` runs the fallback seeder; every argument after ``seed`` is passed
  unchanged to ``opennse_terminal.seed.seed_terminal_db.main``, which owns its options and exit
  codes, so it behaves exactly like ``python -m opennse_terminal.seed.seed_terminal_db``.
* ``ingest NAME [--trade-date YYYY-MM-DD] [--limit N]`` runs one public-source pipeline of
  ``ingestion.registry``, or ``ingest cbk`` the five CBK pipelines in sequence.
* ``import KIND PATH [--sheet NAME]`` imports an operator-supplied CSV or ``.xlsx`` file.
* ``scheduler`` runs the APScheduler process until SIGINT or SIGTERM. It is a dedicated process
  and is never started inside the API.

Exit codes (:data:`EXIT_CODES` for run statuses): ``0`` succeeded, partial or no new data; ``1``
failed, or from ``seed`` its own invalid arguments; ``2`` invalid arguments to any other
command, or from ``seed`` the schema is not at head; ``3`` layout changed; ``4`` blocked by
robots.txt; ``5`` ambiguous NSE session; ``130`` interrupted (Ctrl-C); ``143`` terminated by
SIGTERM during ``ingest``, ``import`` or ``seed``, after the run was recorded ``failed``
("cancelled").

Importing this module is cheap: the migrations, seeder, ingestion and scheduler modules are
imported inside the handlers, and the registry and importer modules that supply the argparse
choices load neither Playwright, pdfplumber nor pandas. Nothing here imports the API package.
"""

import argparse
import asyncio
import logging
import os
import re
import signal
import stat
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from datetime import date
from pathlib import Path
from types import FrameType, MappingProxyType
from typing import TYPE_CHECKING, Any, Final, TypeVar, cast

from pydantic import ValidationError

from opennse_terminal import __version__
from opennse_terminal.config import TerminalSettings

if TYPE_CHECKING:
    from opennse_terminal.ingestion.base import RunResult

__all__ = [
    "CBK_GROUP",
    "EXIT_CODES",
    "EXIT_DB_NOT_AT_HEAD",
    "EXIT_FAILED",
    "EXIT_INTERRUPTED",
    "EXIT_OK",
    "EXIT_TERMINATED",
    "PROG",
    "TerminatedError",
    "build_parser",
    "configure_logging",
    "main",
    "run_terminable",
]

logger = logging.getLogger("opennse_terminal.cli")

PROG: Final = "opennse-terminal"
"""Program name shown in usage lines, help and error messages."""

CBK_GROUP: Final = "cbk"
"""``ingest`` name that runs every pipeline of ``ingestion.registry.CBK_PIPELINES`` in order."""

EXIT_OK: Final = 0
EXIT_FAILED: Final = 1
EXIT_DB_NOT_AT_HEAD: Final = 2
"""Returned by ``seed`` when ``terminal_alembic_version`` is not at the packaged head. The seeder
returns it itself, and exits 1 on its own usage errors so the two never read alike; every other
subcommand exits 2 only for invalid arguments (argparse)."""
EXIT_INTERRUPTED: Final = 130
"""128 + SIGINT: the shell's code for a command stopped by Ctrl-C."""
EXIT_TERMINATED: Final = 143
"""128 + SIGTERM: returned when ``ingest``, ``import`` or ``seed`` is stopped by SIGTERM (see
:func:`run_terminable`), after its run was recorded ``failed`` ("cancelled") and its uncommitted
changes rolled back."""

EXIT_CODES: Final[Mapping[str, int]] = MappingProxyType(
    {
        "succeeded": 0,
        "partial": 0,
        "no_new_data": 0,
        "failed": 1,
        "layout_changed": 3,
        "blocked_by_robots": 4,
        "ambiguous_session": 5,
    }
)
"""Exit code of a run by its final status; any other status, ``running`` included, exits 1."""

_LOG_FORMAT: Final = "%(asctime)s %(levelname)s %(name)s %(message)s"
_ISO_DATE_RE: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_XLSX_SUFFIX: Final = ".xlsx"
# Width of the hand-wrapped help texts below; RawDescriptionHelpFormatter keeps their lines as is.
_HELP_WIDTH: Final = 80

# argparse expands %(prog)s in these texts, so a literal percent sign is written %%.
_MAIN_EPILOG: Final = """\
Every command reads the database URL from TERMINAL_DATABASE_URL (or .env in the
working directory).

examples:
  %(prog)s migrate upgrade head
  %(prog)s seed
  %(prog)s ingest cbk
  %(prog)s ingest nse_market_statistics --trade-date 2026-10-27
  %(prog)s import daily_prices prices.csv
  %(prog)s scheduler

exit codes:
  0    succeeded, partial or no_new_data
  1    failed (a network failure, a held lock, an unverified exchange calendar,
       or an error before the run could be recorded); seed also exits 1 for its
       own invalid arguments
  2    invalid arguments, except seed's own (those exit 1); from seed, only that
       the schema is not at migration head
  3    layout_changed: the source layout changed and nothing was written
  4    blocked_by_robots: robots.txt disallows the source URL
  5    ambiguous_session: the NSE session could not be dated; re-run with --trade-date
  130  interrupted (Ctrl-C); uncommitted changes were rolled back
  143  terminated (SIGTERM) during ingest, import or seed; uncommitted changes
       were rolled back and the run recorded failed (cancelled)
"""

_MIGRATE_DESCRIPTION: Final = """\
Apply the terminal's own Alembic history to the database named by TERMINAL_DATABASE_URL.

Safe on a host database: the history is recorded in terminal_alembic_version, and only
terminal_ tables are created or dropped. A host's own tables and its alembic_version
table are never read or changed.
"""

_INGEST_DESCRIPTION: Final = """\
Run one ingestion pipeline. Pipelines read only public sources: the NSE
market-statistics web page and the Central Bank of Kenya's statistical releases.
Each run is recorded in terminal_ingestion_runs and holds its own advisory lock, so
it never overlaps a scheduled run of the same pipeline.
"""

_SCHEDULER_DESCRIPTION: Final = """\
Run the ingestion scheduler until SIGINT or SIGTERM: nse_market_statistics
Tuesday to Saturday at 07:30, and the CBK pipelines Monday to Friday at 18:15, in
the configured timezone (TERMINAL_TIMEZONE, default Africa/Nairobi). Run exactly
one scheduler process, separate from the API.
"""

# argparse prints a parser's epilog only in that parser's own help, so the configuration source
# that _MAIN_EPILOG names is repeated in the help of every subcommand that reads it. seed --help
# is the seeder's own and states its source itself.
_SETTINGS_NOTE: Final = """\
The database URL (postgresql+asyncpg://) is read from TERMINAL_DATABASE_URL,
or from .env in the working directory when that variable is not set. README.md,
section "Configuration", lists every TERMINAL_* setting with its default.
"""

# The settings the ingestion pipelines read, for the help of ingest and scheduler.
_PIPELINE_SETTINGS_NOTE: Final = """\
Pipelines send TERMINAL_SCRAPER_CONTACT in their user agent and space requests
to one host at least TERMINAL_SCRAPER_MIN_INTERVAL_SECONDS apart. HTTP requests
have a read timeout of TERMINAL_SCRAPER_TIMEOUT_SECONDS and are retried up to
TERMINAL_SCRAPER_MAX_RETRIES times after a timeout or an HTTP 429, 500, 502,
503 or 504. nse_market_statistics dates its session in TERMINAL_TIMEZONE.
"""


# ---------------------------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------------------------


def _iso_date(text: str) -> date:
    """argparse type of ``--trade-date``: a calendar date written exactly ``YYYY-MM-DD``.

    ``date.fromisoformat`` alone would also accept ``20261027`` and ISO week dates.
    """
    if _ISO_DATE_RE.fullmatch(text) is None:
        raise argparse.ArgumentTypeError(f"expected a date as YYYY-MM-DD, got {text!r}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a valid date: {exc}") from None


def _bounded_int(low: int, high: int) -> Callable[[str], int]:
    """Return an argparse type accepting an integer from ``low`` to ``high`` inclusive."""

    def parse(text: str) -> int:
        try:
            value = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"expected an integer, got {text!r}") from None
        if not low <= value <= high:
            raise argparse.ArgumentTypeError(f"must be from {low} to {high}, got {value}")
        return value

    return parse


def _migration_revision(text: str) -> str:
    """argparse type of ``migrate``'s ``REVISION``: Alembic revision text, returned unchanged.

    Rejects only text that no revision can match, which Alembic would otherwise reject only after
    connecting, most of it with a bare ``AssertionError``: empty or whitespace-only text, and
    text ending in ``@``, which leaves nothing after the ``@`` to name a revision (a revision id
    never contains ``@``). Everything else, ``head``, ``base``, ``+1``, ``terminal_0001@+1`` and
    ``@head`` included, goes to Alembic as typed, and Alembic reports any revision it cannot
    resolve. The rejected text is quoted with ``repr``, so the error stays on one line.
    """
    if not text or text.isspace():
        raise argparse.ArgumentTypeError(f"must not be empty or whitespace only, got {text!r}")
    if text.endswith("@"):
        raise argparse.ArgumentTypeError(
            f"no revision follows '@' in {text!r}; expected LABEL@REVISION, such as "
            "terminal_0001@+1"
        )
    return text


def _names_supporting(option: str) -> str:
    """Registered pipeline names whose spec sets ``supports_<option>``, for help and errors."""
    from opennse_terminal.ingestion.registry import PIPELINES

    return " or ".join(
        name for name, spec in PIPELINES.items() if getattr(spec, f"supports_{option}")
    )


def _build_parsers() -> tuple[argparse.ArgumentParser, Mapping[str, argparse.ArgumentParser]]:
    """Build the top-level parser and return it with each subcommand's parser by name.

    The subcommand parsers are returned so that checks made after parsing report through the
    subcommand's own usage line.
    """
    from opennse_terminal.ingestion.files.tabular_import import (
        IMPORT_KINDS,
        MAX_FILE_BYTES,
        MAX_ROWS,
        SUPPORTED_SUFFIXES,
    )
    from opennse_terminal.ingestion.registry import CBK_PIPELINES, MAX_LIMIT, MIN_LIMIT, PIPELINES

    raw = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=(
            "OpenNSE Quant & Macro Terminal — migrations, seeding, ingestion, import and scheduling"
        ),
        epilog=_MAIN_EPILOG,
        formatter_class=raw,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True, title="commands")

    migrate = commands.add_parser(
        "migrate",
        help="upgrade or downgrade the terminal_ schema (never touches host tables)",
        description=_MIGRATE_DESCRIPTION,
        epilog=_SETTINGS_NOTE,
        formatter_class=raw,
    )
    directions = migrate.add_subparsers(dest="direction", required=True, title="directions")
    upgrade = directions.add_parser(
        "upgrade",
        help="apply migrations up to REVISION",
        description="Apply the terminal_ migrations up to REVISION.",
        epilog=_SETTINGS_NOTE,
        formatter_class=raw,
    )
    upgrade.add_argument(
        "revision",
        nargs="?",
        type=_migration_revision,
        default="head",
        metavar="REVISION",
        help="target revision, such as head or terminal_0001 (default: %(default)s)",
    )
    downgrade = directions.add_parser(
        "downgrade",
        help="revert migrations down to REVISION",
        description=textwrap.fill(
            "Revert the terminal_ migrations down to REVISION; base drops every terminal_ "
            "table the history created and nothing else.",
            width=_HELP_WIDTH,
        ),
        epilog=_SETTINGS_NOTE,
        formatter_class=raw,
    )
    downgrade.add_argument(
        "revision",
        type=_migration_revision,
        metavar="REVISION",
        help="target revision, such as base or terminal_0001",
    )

    # No options of its own: every argument after "seed" goes to the seeder's parser unchanged,
    # --help included, so the seeder stays the only owner of its options.
    seed = commands.add_parser(
        "seed",
        add_help=False,
        help="load the deterministic fallback data [--database-url URL] (see seed --help)",
    )

    cbk_names = ", ".join(CBK_PIPELINES)
    cbk_order = textwrap.fill(
        f"{CBK_GROUP} runs {cbk_names} in that order, each as its own run, and exits with "
        "the code of the first of them that did not exit 0.",
        width=_HELP_WIDTH,
    )
    ingest = commands.add_parser(
        "ingest",
        help="run a public-source ingestion pipeline, or all CBK pipelines with cbk",
        description=_INGEST_DESCRIPTION,
        epilog=f"{cbk_order}\n\n{_PIPELINE_SETTINGS_NOTE}\n{_SETTINGS_NOTE}",
        formatter_class=raw,
    )
    ingest.add_argument(
        "name",
        choices=(*sorted(PIPELINES), CBK_GROUP),
        help=f"pipeline to run; {CBK_GROUP} runs the five CBK pipelines in sequence",
    )
    ingest.add_argument(
        "--trade-date",
        type=_iso_date,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "trading session the NSE page shows, to resolve an ambiguous_session run "
            f"(only with {_names_supporting('trade_date')})"
        ),
    )
    ingest.add_argument(
        "--limit",
        type=_bounded_int(MIN_LIMIT, MAX_LIMIT),
        default=None,
        metavar="N",
        help=(
            f"backfill the newest N weekly bulletins ({MIN_LIMIT}-{MAX_LIMIT}) instead of the "
            f"newest one not yet ingested (only with {_names_supporting('limit')})"
        ),
    )

    importer = commands.add_parser(
        "import",
        help="import a CSV or Excel file of equities, prices, fundamentals, macro or flows",
        description=textwrap.fill(
            f"Import a {' or '.join(SUPPORTED_SUFFIXES)} file of up to "
            f"{MAX_FILE_BYTES // (1024 * 1024)} MiB and {MAX_ROWS:,} rows. The run is recorded "
            "in terminal_ingestion_runs as csv_import:KIND.",
            width=_HELP_WIDTH,
        ),
        epilog=_SETTINGS_NOTE,
        formatter_class=raw,
    )
    importer.add_argument("kind", choices=IMPORT_KINDS, help="what the file holds")
    importer.add_argument("path", type=Path, help="the .csv or .xlsx file to import")
    importer.add_argument(
        "--sheet",
        default=None,
        metavar="NAME",
        help="worksheet of an .xlsx file to read (default: the first sheet)",
    )

    scheduler = commands.add_parser(
        "scheduler",
        help="run the ingestion scheduler process until SIGINT or SIGTERM",
        description=_SCHEDULER_DESCRIPTION,
        epilog=f"{_PIPELINE_SETTINGS_NOTE}\n{_SETTINGS_NOTE}",
        formatter_class=raw,
    )
    subcommands = {
        "migrate": migrate,
        "seed": seed,
        "ingest": ingest,
        "import": importer,
        "scheduler": scheduler,
    }
    return parser, MappingProxyType(subcommands)


def build_parser() -> argparse.ArgumentParser:
    """Return the ``opennse-terminal`` argument parser.

    Its subcommands are ``migrate``, ``seed``, ``ingest``, ``import`` and ``scheduler``; the
    ``ingest`` names come from ``ingestion.registry.PIPELINES`` plus ``cbk``, and the ``import``
    kinds from ``tabular_import.IMPORT_KINDS``. Checks that span several arguments, such as
    ``--limit`` with a pipeline other than ``cbk_reserves``, are made by :func:`main`.
    """
    return _build_parsers()[0]


def _check_ingest(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Reject ``--trade-date`` and ``--limit`` for pipelines that do not take them."""
    from opennse_terminal.ingestion.registry import PIPELINES

    name = cast(str, args.name)
    spec = PIPELINES.get(name)
    if args.trade_date is not None and not (spec is not None and spec.supports_trade_date):
        parser.error(
            f"--trade-date is valid only with {_names_supporting('trade_date')}, not {name}"
        )
    if args.limit is not None and not (spec is not None and spec.supports_limit):
        parser.error(f"--limit is valid only with {_names_supporting('limit')}, not {name}")


def _check_import(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Reject a path that is not a readable ``.csv``/``.xlsx`` file, and ``--sheet`` for CSV.

    Each check fails as an argument error (exit 2) before any settings, engine or run row exists,
    in this order: the path is an existing regular file, its suffix is supported, ``--sheet`` is
    not given for a CSV file and, last, the file opens for reading, so an unreadable file is named
    here rather than ending a recorded run ``failed``. The open is read-only and non-blocking: a
    FIFO or device swapped in after the first check opens at once and ``fstat`` rejects it, where
    a blocking open would wait for a writer. Nothing is read, and the descriptor is closed at once.

    ``import_file`` opens and checks the file again on its own descriptor, so a file changed after
    this check still ends its run ``failed``; it also enforces the size and row limits.
    """
    from opennse_terminal.ingestion.files.tabular_import import SUPPORTED_SUFFIXES

    path = cast(Path, args.path)
    try:
        is_file = path.is_file()
    except OSError as exc:
        parser.error(f"cannot access {path}: {exc.strerror or type(exc).__name__}")
    if not is_file:
        parser.error(f"{path} does not exist or is not a file")
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        parser.error(f"{path} must be a {' or '.join(SUPPORTED_SUFFIXES)} file")
    if args.sheet is not None and suffix != _XLSX_SUFFIX:
        parser.error(f"--sheet applies only to {_XLSX_SUFFIX} files")
    # A flag the platform lacks counts as 0; O_NOCTTY keeps a terminal swapped in for the file
    # from becoming this process's controlling terminal.
    flags: int = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOCTTY", 0)
    try:
        fd = os.open(path, flags)
        try:
            is_regular = stat.S_ISREG(os.fstat(fd).st_mode)
        finally:
            os.close(fd)
    except OSError as exc:
        parser.error(f"cannot read {path}: {exc.strerror or type(exc).__name__}")
    if not is_regular:
        parser.error(f"{path} does not exist or is not a file")


# ---------------------------------------------------------------------------------------------
# Event loop with SIGTERM cancellation
# ---------------------------------------------------------------------------------------------

_T = TypeVar("_T")


class TerminatedError(BaseException):
    """Raised by :func:`run_terminable` when SIGTERM cancelled the coroutine it ran.

    A ``BaseException``, like ``KeyboardInterrupt``, so that the handlers' ``except Exception``
    blocks never read a termination as a failed run; :func:`main` and the seeder's ``main`` turn
    it into :data:`EXIT_TERMINATED`.
    """


def _install_sigterm_handler(
    loop: asyncio.AbstractEventLoop, on_sigterm: Callable[[], None]
) -> Callable[[], None] | None:
    """Make SIGTERM call ``on_sigterm`` on ``loop``; return the function that restores SIG_DFL.

    Returns ``None``, having installed nothing, outside the main thread, while SIGTERM's
    disposition is anything but ``SIG_DFL`` (a host's own handler, or ``SIG_IGN``), and where the
    interpreter cannot install a handler; SIGTERM then keeps its disposition. This is the rule
    ``asyncio.run`` applies to SIGINT. ``loop.add_signal_handler`` is used where the loop supports
    it (Unix); where it raises ``NotImplementedError`` (the Windows event loops), ``signal.signal``
    installs a handler that calls ``on_sigterm`` through ``loop.call_soon_threadsafe``, the one
    loop call that is safe from a signal handler, as the scheduler's stop handlers do.
    """
    if threading.current_thread() is not threading.main_thread():
        return None
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        return None
    try:
        loop.add_signal_handler(signal.SIGTERM, on_sigterm)
    except NotImplementedError:

        def handle_signal(_signum: int, _frame: FrameType | None) -> None:
            loop.call_soon_threadsafe(on_sigterm)

        try:
            signal.signal(signal.SIGTERM, handle_signal)
        except ValueError:
            # An interpreter whose main thread does not handle signals.
            return None

        def restore_default() -> None:
            if signal.getsignal(signal.SIGTERM) is handle_signal:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)

        return restore_default
    except (RuntimeError, OSError):
        # The loop could not route the signal to itself (set_wakeup_fd refused, or the signal
        # cannot be caught here).
        return None

    def remove_handler() -> None:
        # remove_signal_handler reinstates SIG_DFL, the disposition found before installing.
        loop.remove_signal_handler(signal.SIGTERM)

    return remove_handler


def run_terminable(main: Coroutine[Any, Any, _T]) -> _T:
    """Run ``main`` with ``asyncio.run`` and return its result, cancelling it on SIGTERM.

    ``asyncio.run`` turns the first SIGINT into a cancellation of its main task, but leaves
    SIGTERM at its default disposition, which kills the process before an ingestion run or the
    seeder can roll back and record itself. Here the first SIGTERM cancels the main task too, and
    only it: tasks ``main`` started, such as ``execute_pipeline``'s commit and run-row writes, run
    on until the main task has waited for them. A run is therefore recorded ``failed``
    ("cancelled") and its data transaction rolled back, and once ``main`` has ended cancelled,
    :class:`TerminatedError` is raised. A further SIGTERM while ``main`` unwinds is logged and
    ignored. An outcome other than a cancellation (a result, or another exception) is kept even
    after SIGTERM, as ``asyncio.run`` keeps it after Ctrl-C; a ``CancelledError`` that no SIGTERM
    preceded propagates unchanged.

    The handler is installed only in the main thread and only while SIGTERM has its default
    disposition (see :func:`_install_sigterm_handler`), so a host's own handler, an ignored
    SIGTERM and calls from worker threads (``asyncio.to_thread(main, argv)``) are left as they
    are; SIG_DFL is restored when ``main`` ends. Like ``asyncio.run``, this must be called
    outside a running event loop.

    Raises:
        TerminatedError: SIGTERM cancelled ``main``.
    """
    terminated = False

    async def run_main() -> _T:
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()

        def on_sigterm() -> None:
            nonlocal terminated
            if terminated:
                logger.info("received SIGTERM again; already stopping, waiting for the rollback")
                return
            terminated = True
            logger.info("received SIGTERM; cancelling and rolling back")
            if task is not None:
                task.cancel()

        restore = _install_sigterm_handler(loop, on_sigterm)
        try:
            return await main
        finally:
            if restore is not None:
                restore()

    try:
        return asyncio.run(run_main())
    except asyncio.CancelledError:
        if terminated:
            raise TerminatedError("terminated by SIGTERM") from None
        raise


# ---------------------------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------------------------


def _load_settings() -> TerminalSettings | None:
    """Build ``TerminalSettings()``, or log why it is invalid and return ``None``.

    The validation message names the field and the rule, never the rejected value: the settings
    hide their input in errors, since ``database_url`` carries the password.
    """
    try:
        return TerminalSettings()
    except ValidationError as exc:
        logger.error("invalid terminal settings: %s", exc)
        return None


def _log_text(text: str) -> str:
    r"""Return ``text`` escaped so that it stays on one log line, and otherwise unchanged.

    Free text from outside the program, such as a revision typed on the command line, Alembic's
    message quoting it, or an ``OSError`` quoting an operator's file path, must neither break the
    line it is logged on nor forge another (CWE-117), and ``%s`` formatting escapes nothing.
    Every character that ``str.isprintable()`` rejects is therefore written as a backslash
    escape: ``\r``, ``\n`` and ``\t`` by name, any other as ``\xNN``, ``\uNNNN`` or
    ``\UNNNNNNNN``, its code point in hexadecimal as in a Python string literal. That covers the
    other C0 and C1 controls (``ESC`` and ``NEL`` included), the line and paragraph separators
    U+2028 and U+2029, and every other separator, format, surrogate, private-use or unassigned
    code point. A backslash is doubled, so the input cannot spell an escape that reads like one
    of these. Printable text, non-ASCII letters included, is kept as is, so ``head``,
    ``terminal_0001`` and Alembic's ``Can't locate revision identified by 'x'`` read unchanged.
    """
    if text.isprintable() and "\\" not in text:
        return text
    named = {"\\": "\\\\", "\r": "\\r", "\n": "\\n", "\t": "\\t"}
    parts: list[str] = []
    for char in text:
        code = ord(char)
        if char in named:
            parts.append(named[char])
        elif char.isprintable():
            parts.append(char)
        elif code <= 0xFF:
            parts.append(f"\\x{code:02x}")
        elif code <= 0xFFFF:
            parts.append(f"\\u{code:04x}")
        else:
            parts.append(f"\\U{code:08x}")
    return "".join(parts)


def _failure_reason(exc: Exception) -> str:
    """Describe ``exc`` for one log line without letting connection details through.

    ``OSError`` (a refused or timed-out connection, the migrations' ``ConnectionError``, which
    names only the database, host and port, or a file error naming an operator-supplied path) and
    ``ValueError`` (the URL checks, which quote the URL only through ``database_log_label``) are
    given with their text, escaped by :func:`_log_text` because it can quote free text. Anything
    else, such as a database server error that can quote the user name or a SQLAlchemy error
    embedding statement parameters, is named by its type only, as everywhere in ingestion and the
    seeder.
    """
    if isinstance(exc, OSError | ValueError):
        return _log_text(str(exc)) or type(exc).__name__
    return type(exc).__name__


def _raised_in_revision_map(exc: BaseException) -> bool:
    """Whether ``exc`` was raised inside Alembic's revision map (``alembic.script.revision``).

    The innermost frame of its traceback is the code that raised it. ``env.py`` runs Alembic
    under ``asyncio.run`` and ``AsyncConnection.run_sync``, and both keep those frames.
    """
    from alembic.script.revision import RevisionMap

    frame = exc.__traceback__
    if frame is None:
        return False
    while frame.tb_next is not None:
        frame = frame.tb_next
    module: object = frame.tb_frame.f_globals.get("__name__")
    return module == RevisionMap.__module__


def _cmd_migrate(args: argparse.Namespace) -> int:
    """``migrate upgrade|downgrade REVISION`` against ``TerminalSettings().database_url``.

    Logs one INFO line on success and one ERROR line on failure. ``REVISION`` has passed
    :func:`_migration_revision` and otherwise goes to Alembic as typed; on both lines it, and
    Alembic's message, which can quote it, are escaped by :func:`_log_text`, so neither can break
    or forge a line. Alembic's revision map raises an ``AssertionError`` without a message for
    some revisions it cannot resolve from the database's current revision, such as ``head+1``, a
    step past the head; the line then names the revision as invalid. Any other failure that is
    not Alembic's own error is described by :func:`_failure_reason`. The database is named only
    by its credential-free ``database_log_label``.
    """
    from alembic.util import CommandError

    from opennse_terminal.db.session import database_log_label
    from opennse_terminal.migrations import run_downgrade, run_upgrade

    direction = cast(str, args.direction)
    revision = cast(str, args.revision)
    settings = _load_settings()
    if settings is None:
        return EXIT_FAILED
    # The database is named only through its credential-free label: render_as_string(
    # hide_password=True) would still print query values, where asyncpg also accepts a password.
    target = database_log_label(settings.database_url)
    migrate: Callable[[str, str], None] = run_upgrade if direction == "upgrade" else run_downgrade
    try:
        migrate(settings.database_url, revision)
    except CommandError as exc:
        # Alembic's own errors, such as an unknown revision, name only revisions, but quote the
        # revision as typed, so they are escaped like it.
        reason = _log_text(str(exc))
    except AssertionError as exc:
        # Only an assertion of the revision map is the revision's fault; any other, from a
        # driver or a migration script, is named by its type like every unexpected error.
        if _raised_in_revision_map(exc):
            reason = (
                f"invalid revision '{_log_text(revision)}': Alembic could not resolve it from "
                "the database's current revision"
            )
        else:
            reason = _failure_reason(exc)
    except Exception as exc:
        reason = _failure_reason(exc)
    else:
        logger.info("migrated %s to %s on %s", direction, _log_text(revision), target)
        return EXIT_OK
    logger.error(
        "migrate %s to %s on %s failed: %s", direction, _log_text(revision), target, reason
    )
    return EXIT_FAILED


def _cmd_seed(seed_argv: list[str]) -> int:
    """``seed``: hand the arguments after ``seed`` to the seeder's own ``main``."""
    from opennse_terminal.seed.seed_terminal_db import main as seed_main

    return seed_main(seed_argv, prog=f"{PROG} seed")


def _report(label: str, result: "RunResult") -> int:
    """Log the one-line outcome of a recorded run and return its exit code.

    The run's ``error_message`` is not logged, as in ingestion, where exception text never
    reaches a log line; it is on the ``terminal_ingestion_runs`` row named by ``run_id``.
    """
    status = str(result.status)
    code = EXIT_CODES.get(status, EXIT_FAILED)
    logger.info(
        "%s: pipeline=%s status=%s exit_code=%d fetched=%d written=%d rejected=%d run_id=%s",
        label,
        result.pipeline,
        status,
        code,
        result.rows_fetched,
        result.rows_written,
        result.rows_rejected,
        result.run_id,
    )
    return code


async def _ingest(
    names: Sequence[str],
    settings: TerminalSettings,
    *,
    trade_date: date | None,
    limit: int | None,
) -> list[tuple[str, int]]:
    """Run ``names`` one after another and return each name with its exit code, in order.

    Each name is a separate ``registry.run_pipeline`` call, so each run takes its own advisory
    lock and run row. A run that raises instead of returning a result, for example because its
    run row could not be written, is logged and counted as failed, and the sequence continues.
    Cancellation (Ctrl-C or SIGTERM) stops the sequence. Every run shares one ``HostSpacing``, so
    requests to one host stay ``scraper_min_interval_seconds`` apart across the runs as within
    each.
    """
    from opennse_terminal.ingestion import registry
    from opennse_terminal.ingestion.http import HostSpacing

    spacing = HostSpacing()
    outcomes: list[tuple[str, int]] = []
    for name in names:
        try:
            result = await registry.run_pipeline(
                name, settings, trigger="cli", trade_date=trade_date, limit=limit, spacing=spacing
            )
        except Exception as exc:
            logger.error("ingest %s: the run could not be recorded: %s", name, _failure_reason(exc))
            outcomes.append((name, EXIT_FAILED))
            continue
        outcomes.append((name, _report(f"ingest {name}", result)))
    return outcomes


def _cmd_ingest(args: argparse.Namespace) -> int:
    """``ingest NAME``, or ``ingest cbk``: exit with the first non-zero code in run order.

    The runs go through :func:`run_terminable`, so SIGTERM, like Ctrl-C, records the current run
    ``failed`` ("cancelled") and stops the sequence (:class:`TerminatedError`).
    """
    from opennse_terminal.ingestion.registry import CBK_PIPELINES

    name = cast(str, args.name)
    trade_date = cast(date | None, args.trade_date)
    limit = cast(int | None, args.limit)
    settings = _load_settings()
    if settings is None:
        return EXIT_FAILED
    names = CBK_PIPELINES if name == CBK_GROUP else (name,)
    outcomes = run_terminable(_ingest(names, settings, trade_date=trade_date, limit=limit))
    code = next((code for _name, code in outcomes if code != EXIT_OK), EXIT_OK)
    if name == CBK_GROUP:
        logger.info(
            "ingest %s: exit_code=%d (%s)",
            CBK_GROUP,
            code,
            ", ".join(f"{run_name}={run_code}" for run_name, run_code in outcomes),
        )
    return code


def _cmd_import(args: argparse.Namespace) -> int:
    """``import KIND PATH [--sheet NAME]`` through ``tabular_import.import_file``.

    The import goes through :func:`run_terminable`, so SIGTERM, like Ctrl-C, records the run
    ``failed`` ("cancelled") and rolls its data back (:class:`TerminatedError`).
    """
    from opennse_terminal.ingestion.files.tabular_import import import_file

    kind = cast(str, args.kind)
    path = cast(Path, args.path)
    sheet = cast(str | None, args.sheet)
    settings = _load_settings()
    if settings is None:
        return EXIT_FAILED
    try:
        result = run_terminable(import_file(kind, path, sheet, settings=settings, trigger="cli"))
    except Exception as exc:
        logger.error("import %s: the run could not be recorded: %s", kind, _failure_reason(exc))
        return EXIT_FAILED
    return _report(f"import {kind}", result)


def _cmd_scheduler(args: argparse.Namespace) -> int:
    """``scheduler``: serve the cron jobs in this process until SIGINT or SIGTERM."""
    from opennse_terminal.ingestion.scheduler import run_forever

    settings = _load_settings()
    if settings is None:
        return EXIT_FAILED
    try:
        run_forever(settings)
    except Exception as exc:
        logger.error("scheduler stopped on an error: %s", _failure_reason(exc))
        return EXIT_FAILED
    return EXIT_OK


_HANDLERS: Final[Mapping[str, Callable[[argparse.Namespace], int]]] = MappingProxyType(
    {
        "migrate": _cmd_migrate,
        "ingest": _cmd_ingest,
        "import": _cmd_import,
        "scheduler": _cmd_scheduler,
    }
)

_CHECKS: Final[Mapping[str, Callable[[argparse.Namespace, argparse.ArgumentParser], None]]] = (
    MappingProxyType({"ingest": _check_ingest, "import": _check_import})
)


# ---------------------------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------------------------


class _UtcFormatter(logging.Formatter):
    """Render ``%(asctime)s`` in UTC as ISO-8601 with milliseconds and a ``Z`` suffix.

    For example ``2026-10-29T23:19:45.871Z``. The stdlib default renders host-local time with no
    offset, which on an Africa/Nairobi host reads three hours off the UTC run rows.
    """

    # Formatter calls self.converter(record.created); staticmethod stops mypy from reading the
    # assigned function as a method whose first argument is self.
    converter = staticmethod(time.gmtime)
    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03dZ"


def configure_logging() -> None:
    """Log INFO and above to stderr as ``<time> <LEVEL> <logger> <message>``, the time in UTC.

    The time is ISO-8601 with milliseconds and a ``Z`` suffix (``2026-10-29T23:19:45.871Z``), the
    clock of the ``timestamptz`` ``started_at`` and ``finished_at`` of ``terminal_ingestion_runs``,
    so log lines and run rows correlate whatever the host's time zone.

    The one logging setup of the command-line entry points, shared by the ``opennse-terminal``
    console script (:func:`main`) and ``python -m opennse_terminal.seed.seed_terminal_db``, so no
    library module adds a handler. A root logger that already has handlers is left unchanged, as
    ``logging.basicConfig`` leaves it, so a host or test harness that configured logging keeps its
    own setup.
    """
    if not logging.getLogger().handlers:
        # Checked here as well as in basicConfig, so no handler is built only to be discarded.
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_UtcFormatter(_LOG_FORMAT))
        logging.basicConfig(level=logging.INFO, handlers=[handler])
    # Alembic logs one INFO line per built-in plugin when it is first imported, which the
    # handlers' lazy imports would print as noise after logging is configured.
    logging.getLogger("alembic.runtime.plugins").setLevel(logging.WARNING)


def _parse(tokens: list[str]) -> tuple[str, argparse.Namespace, list[str]]:
    """Parse ``tokens`` into the command, its namespace and, for ``seed``, its raw arguments.

    Raises ``SystemExit`` as argparse does: 0 after ``--help`` or ``--version``, 2 for invalid
    arguments.
    """
    parser, subcommands = _build_parsers()
    args, extras = parser.parse_known_args(tokens)
    command = cast(str, args.command)
    if command == "seed":
        # Subcommand names cannot be abbreviated and no top-level option takes a value, so the
        # first "seed" token is the command; everything after it is the seeder's.
        position = tokens.index(command)
        if position:
            parser.error(f"unrecognized arguments: {' '.join(tokens[:position])}")
        return command, args, tokens[position + 1 :]
    if extras:
        parser.error(f"unrecognized arguments: {' '.join(extras)}")
    check = _CHECKS.get(command)
    if check is not None:
        check(args, subcommands[command])
    return command, args, []


def main(argv: list[str] | None = None) -> int:
    """Run ``opennse-terminal`` with ``argv`` (default ``sys.argv[1:]``) and return its exit code.

    Never raises ``SystemExit`` and never ends in a traceback, so in-process callers, tests
    included, always receive an int:

    * argparse's exits become the returned code: 0 after ``--help`` or ``--version``, 2 for
      invalid arguments. A ``SystemExit`` raised by a handler becomes its code the same way
      (``None`` is 0; a non-integer code is logged at ERROR by its type and returns 1).
    * Ctrl-C, while parsing or while a command runs, logs one WARNING and returns 130.
    * SIGTERM while ``ingest`` or ``import`` runs (:func:`run_terminable`) cancels the run, which
      is recorded ``failed`` ("cancelled") with its data rolled back, then logs one WARNING and
      returns 143. ``migrate`` keeps SIGTERM's default disposition, and ``scheduler`` handles it
      itself as a request to stop (exit 0).
    * Any other exception that escapes parsing or a handler, such as a failed lazy import, logs
      one ERROR naming the command (``opennse-terminal`` before one is parsed) and the
      exception's type, never its text or traceback, which can carry the database URL, and
      returns 1.
    * ``seed`` returns the seeder's own code unchanged.

    The migration, ingestion, import and scheduler handlers each start their own event loop, so
    this must be called outside a running loop (async code uses ``asyncio.to_thread(main,
    argv)``). Configures logging through :func:`configure_logging`.
    """
    command = PROG
    try:
        configure_logging()
        tokens = list(sys.argv[1:] if argv is None else argv)
        command, args, seed_argv = _parse(tokens)
        if command == "seed":
            return _cmd_seed(seed_argv)
        return _HANDLERS[command](args)
    except SystemExit as exc:
        # argparse always exits with an int, and None is sys.exit()'s success.
        if exc.code is None:
            return EXIT_OK
        if isinstance(exc.code, int):
            return exc.code
        logger.error("%s exited with a non-integer status: %s", command, type(exc.code).__name__)
        return EXIT_FAILED
    except KeyboardInterrupt:
        # asyncio.run cancels the running task first, so an ingestion run is recorded failed
        # ("cancelled") and its data transaction rolled back, as is a migration's.
        logger.warning("interrupted; uncommitted changes were rolled back")
        return EXIT_INTERRUPTED
    except TerminatedError:
        # run_terminable cancelled the running task on SIGTERM and let it unwind, so the run is
        # recorded failed ("cancelled") and its data transaction rolled back.
        logger.warning("terminated (SIGTERM); uncommitted changes were rolled back")
        return EXIT_TERMINATED
    except Exception as exc:
        # Type name only, with no traceback: exception text can quote the database URL and its
        # password, as in _failure_reason and the seeder.
        logger.error("%s failed on an unexpected error: %s", command, type(exc).__name__)
        return EXIT_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
