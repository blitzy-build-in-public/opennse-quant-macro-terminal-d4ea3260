"""Verified NSE exchange calendar, used only to date live market-statistics snapshots.

A session is a weekday that is not an exchange closure. Closures come from two places:

* Rules from the Public Holidays Act (Cap. 110),
  https://new.kenyalaw.org/akn/ke/act/1912/21/eng%402024-04-26/source: the fixed-date holidays of
  its Schedule (:data:`FIXED_HOLIDAYS`); Good Friday and Easter Monday, computed from the Gregorian
  Easter date (:func:`easter_sunday`); and Sunday substitution, under which a holiday falling on a
  Sunday makes the next following day that is not itself a public holiday a holiday.
* ``ingestion/data/nse_closures.csv``: gazetted closures the rules cannot compute, such as
  Idd-ul-Fitr, a gazetted Idd-ul-Azha or an ad hoc public holiday or exchange closure. Each row is
  an observed weekday closure citing its Kenya Gazette notice or NSE market notice, so no
  substitution is applied to these rows.

Verified-years contract: the CSV's ``# verified_years:`` comment names the years whose gazetted
closures are complete. Every lookup that touches any other year raises
:class:`CalendarNotVerified` instead of guessing, and the ingestion framework ends the run
``failed`` with its message. Adding a year is a data-only change: add that year's rows to
``nse_closures.csv`` and extend its ``verified_years`` line.

Analytics never use this calendar: they count the observed market sessions present in
``terminal_daily_prices``, and the seeder generates its own synthetic calendar. Importing this
module reads no file; the CSV is read on the first :meth:`ExchangeCalendar.load_default` call.
"""

import csv
import functools
import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from importlib.resources import files
from types import MappingProxyType
from typing import Final

__all__ = [
    "FIXED_HOLIDAYS",
    "CalendarNotVerified",
    "Closure",
    "ExchangeCalendar",
    "easter_sunday",
    "parse_closures_csv",
]

logger = logging.getLogger("opennse_terminal.ingestion.calendar")

FIXED_HOLIDAYS: Final[tuple[tuple[int, int, str], ...]] = (
    (1, 1, "New Year's Day"),
    (5, 1, "Labour Day"),
    (6, 1, "Madaraka Day"),
    (10, 10, "Mazingira Day"),
    (10, 20, "Mashujaa Day"),
    (12, 12, "Jamhuri Day"),
    (12, 25, "Christmas Day"),
    (12, 26, "Boxing Day"),
)
"""``(month, day, name)`` of every fixed-date holiday in the Public Holidays Act's Schedule."""

CLOSURES_HEADER: Final[tuple[str, ...]] = ("date", "reason", "reference")
"""The exact header row of ``nse_closures.csv``."""

_CLOSURES_PACKAGE: Final = "opennse_terminal.ingestion.data"
_CLOSURES_RESOURCE: Final = "nse_closures.csv"
_VERIFIED_YEARS_RE: Final = re.compile(r"^#\s*verified_years\s*:(?P<years>.*)$")
_YEAR_RE: Final = re.compile(r"^\d{4}$")
_ISO_DATE_RE: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ONE_DAY: Final = timedelta(days=1)
_SATURDAY: Final = 5
_SUNDAY: Final = 6
_FIRST_GREGORIAN_EASTER_YEAR: Final = 1583


# The ingestion framework and its run-status mapping import this exception by this name, so it
# keeps it rather than taking the Error suffix that ruff's N818 asks for.
class CalendarNotVerified(LookupError):  # noqa: N818
    """A lookup touched a year whose gazetted closures are not in ``nse_closures.csv``.

    Attributes:
        year: The year that was looked up.
        verified_years: The years the calendar is verified for.
    """

    def __init__(self, year: int, verified_years: frozenset[int]) -> None:
        self.year = year
        self.verified_years = verified_years
        listed = ", ".join(str(y) for y in sorted(verified_years)) or "none"
        super().__init__(
            f"exchange calendar not verified for {year}; verified years: {listed} — add the "
            "year's gazetted closures to ingestion/data/nse_closures.csv"
        )


@dataclass(frozen=True, slots=True)
class Closure:
    """One gazetted exchange closure from ``nse_closures.csv``.

    Attributes:
        date: The observed closure date, always a weekday.
        reason: The holiday or closure name, for example ``"Idd-ul-Fitr"``.
        reference: The Kenya Gazette notice or NSE market notice that fixes the date.
    """

    date: date
    reason: str
    reference: str


def easter_sunday(year: int) -> date:
    """Return the Gregorian Easter Sunday of ``year``.

    Uses the anonymous Gregorian (Meeus–Jones–Butcher) computus in integer arithmetic. Good Friday
    is two days earlier and Easter Monday one day later.

    Raises:
        ValueError: ``year`` precedes 1583, the first full year of the Gregorian calendar.
    """
    if year < _FIRST_GREGORIAN_EASTER_YEAR:
        raise ValueError(f"Gregorian Easter is defined from {_FIRST_GREGORIAN_EASTER_YEAR}: {year}")
    golden = year % 19
    century, year_of_century = divmod(year, 100)
    leap_centuries, century_rest = divmod(century, 4)
    lunar_correction = (century + 8) // 25
    solar_correction = (century - lunar_correction + 1) // 3
    epact = (19 * golden + century - leap_centuries - solar_correction + 15) % 30
    leap_years, year_rest = divmod(year_of_century, 4)
    weekday_shift = (32 + 2 * century_rest + 2 * leap_years - epact - year_rest) % 7
    correction = (golden + 11 * epact + 22 * weekday_shift) // 451
    month, day = divmod(epact + weekday_shift - 7 * correction + 114, 31)
    return date(year, month, day + 1)


def _parse_verified_years(text: str, line_number: int) -> frozenset[int]:
    parts = [part.strip() for part in text.split(",")]
    if not all(_YEAR_RE.fullmatch(part) for part in parts):
        raise ValueError(
            f"line {line_number}: verified_years must be comma-separated four-digit years, "
            f"found {text.strip()!r}"
        )
    return frozenset(int(part) for part in parts)


def parse_closures_csv(text: str) -> tuple[frozenset[int], tuple[Closure, ...]]:
    """Parse the gazetted-closures CSV into its verified years and closures.

    Lines whose first non-whitespace character is ``#`` are comments, and exactly one of them must
    declare the verified years as ``# verified_years: <year>[,<year>...]``: one or more
    comma-separated four-digit years, with whitespace allowed around ``#``, ``:`` and each year.
    The shipped file, for example, declares ``# verified_years: 2026,2027``. The first other
    non-blank line is the header, exactly ``date,reason,reference``. Each row holds an ISO
    ``YYYY-MM-DD`` weekday date in a verified year, unique across the file, with a non-empty reason
    and reference.

    Returns:
        The verified years, and the closures in file order.

    Raises:
        ValueError: The file breaks any rule above or holds malformed CSV quoting, such as an
            unterminated quoted field; the message names the offending line.
    """
    verified_years: frozenset[int] | None = None
    data_lines: list[str] = []
    data_line_numbers: list[int] = []
    for line_number, line in enumerate(text.removeprefix("\ufeff").splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            match = _VERIFIED_YEARS_RE.match(stripped)
            if match is not None:
                if verified_years is not None:
                    raise ValueError(f"line {line_number}: verified_years is declared twice")
                verified_years = _parse_verified_years(match["years"], line_number)
            continue
        if stripped:
            data_lines.append(line)
            data_line_numbers.append(line_number)
    if verified_years is None:
        raise ValueError("missing the '# verified_years: <year>,<year>' comment line")
    if not data_lines:
        raise ValueError(f"missing the header row {','.join(CLOSURES_HEADER)!r}")

    reader = csv.DictReader(data_lines, strict=True)
    # A csv.Error leaves the DictReader's own line_num stale, so failed reads are located with
    # the underlying reader's count, which includes the line the error was found on.
    try:
        fieldnames = reader.fieldnames
    except csv.Error as exc:
        raise _malformed_csv_error(exc, 0, reader.reader.line_num, data_line_numbers) from exc
    if tuple(fieldnames or ()) != CLOSURES_HEADER:
        raise ValueError(
            f"line {data_line_numbers[0]}: header must be exactly "
            f"{','.join(CLOSURES_HEADER)!r}, found {data_lines[0].strip()!r}"
        )

    closures: list[Closure] = []
    seen: dict[date, int] = {}
    while True:
        lines_before = reader.reader.line_num
        try:
            row = next(reader, None)
        except csv.Error as exc:
            raise _malformed_csv_error(
                exc, lines_before, reader.reader.line_num, data_line_numbers
            ) from exc
        if row is None:
            break
        # line_num counts the lines the reader has consumed from data_lines, so it maps back to
        # the file line on which this record ends.
        line_number = data_line_numbers[reader.line_num - 1]
        closure = _parse_closure_row(row, line_number, verified_years)
        if closure.date in seen:
            raise ValueError(
                f"line {line_number}: duplicate closure date {closure.date.isoformat()} "
                f"(first listed on line {seen[closure.date]})"
            )
        seen[closure.date] = line_number
        closures.append(closure)
    return verified_years, tuple(closures)


def _malformed_csv_error(
    exc: csv.Error, lines_before: int, lines_after: int, data_line_numbers: list[int]
) -> ValueError:
    # lines_before and lines_after are the reader's line_num before and after the failed read.
    # An unterminated quote consumes every remaining line before failing at the end of the data,
    # so the record's first line is named too when it differs. Both counts are clamped to the
    # data lines, so the message always names a real file line.
    last = len(data_line_numbers) - 1
    line_number = data_line_numbers[min(max(lines_after - 1, 0), last)]
    start_line = data_line_numbers[min(lines_before, last)]
    where = "" if start_line == line_number else f" in the record starting on line {start_line}"
    return ValueError(f"line {line_number}: malformed CSV{where}: {exc}")


def _parse_closure_row(
    row: Mapping[str | None, object], line_number: int, verified_years: frozenset[int]
) -> Closure:
    # DictReader files surplus cells under the key None and fills absent cells with None.
    if None in row:
        raise ValueError(f"line {line_number}: more than {len(CLOSURES_HEADER)} columns")
    cells: list[str] = []
    for name in CLOSURES_HEADER:
        value = row.get(name)
        if not isinstance(value, str):
            raise ValueError(f"line {line_number}: missing the {name!r} column")
        cells.append(value.strip())
    date_text, reason, reference = cells
    if not _ISO_DATE_RE.fullmatch(date_text):
        raise ValueError(f"line {line_number}: date must be YYYY-MM-DD, found {date_text!r}")
    try:
        day = date.fromisoformat(date_text)
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid date {date_text!r}") from exc
    if not reason or not reference:
        raise ValueError(f"line {line_number}: reason and reference must be non-empty")
    if day.year not in verified_years:
        raise ValueError(
            f"line {line_number}: {date_text} lies outside the verified years "
            f"{', '.join(str(y) for y in sorted(verified_years))}"
        )
    if day.weekday() >= _SATURDAY:
        raise ValueError(f"line {line_number}: {date_text} is a weekend day, not a closure")
    return Closure(date=day, reason=reason, reference=reference)


class ExchangeCalendar:
    """NSE sessions for the verified years: weekdays that are not exchange closures.

    Instances are immutable apart from a per-year holiday cache. Concurrent first lookups of a
    year may each build its mapping, but the result is deterministic, so whichever is cached is
    identical and the instance is safe to share across threads.
    """

    __slots__ = ("_gazetted", "_holidays_by_year", "_verified_years")

    def __init__(self, closures: Iterable[Closure], verified_years: Iterable[int]) -> None:
        """Build a calendar from gazetted closures and the years they are complete for.

        Raises:
            ValueError: Two closures share a date, or a closure lies outside the verified years.
        """
        self._verified_years: frozenset[int] = frozenset(verified_years)
        gazetted: dict[date, Closure] = {}
        for closure in closures:
            if closure.date in gazetted:
                raise ValueError(f"duplicate closure date {closure.date.isoformat()}")
            if closure.date.year not in self._verified_years:
                raise ValueError(
                    f"closure {closure.date.isoformat()} lies outside the verified years"
                )
            gazetted[closure.date] = closure
        self._gazetted: Mapping[date, Closure] = MappingProxyType(gazetted)
        self._holidays_by_year: dict[int, Mapping[date, str]] = {}

    @classmethod
    def load_default(cls) -> "ExchangeCalendar":
        """Return the shared calendar built from the shipped ``nse_closures.csv``.

        The file is read and parsed once per process, on the first call.

        Raises:
            ValueError: The shipped file is malformed (see :func:`parse_closures_csv`).
        """
        return _load_default_calendar()

    @property
    def verified_years(self) -> frozenset[int]:
        """The years whose closures are complete; every other year raises on lookup."""
        return self._verified_years

    def holidays(self, year: int) -> Mapping[date, str]:
        """Return every closure date of ``year`` mapped to its reason.

        The mapping holds the fixed-date holidays, Good Friday, Easter Monday, the Sunday
        substitutes and the gazetted closures. Ordinary weekend days are not listed, since they
        are never sessions, but a holiday falling on a Saturday or Sunday stays in the mapping. A
        Sunday holiday is listed alongside its substitute, the next following day that is not
        itself a holiday; a Saturday holiday gets no substitute.

        Raises:
            CalendarNotVerified: ``year`` is not a verified year.
        """
        if year not in self._verified_years:
            raise CalendarNotVerified(year, self._verified_years)
        cached = self._holidays_by_year.get(year)
        if cached is None:
            cached = MappingProxyType(self._build_holidays(year))
            self._holidays_by_year[year] = cached
        return cached

    def is_session(self, day: date) -> bool:
        """Return whether ``day`` is an NSE trading session.

        Raises:
            CalendarNotVerified: ``day`` lies in a year that is not verified.
            TypeError: ``day`` is a ``datetime``, which never equals a ``date`` key and would
                silently miss every closure.
        """
        if isinstance(day, datetime):
            raise TypeError("is_session takes a date, not a datetime; pass value.date()")
        closures = self.holidays(day.year)
        return day.weekday() < _SATURDAY and day not in closures

    def previous_session(self, day: date) -> date:
        """Return the latest session strictly before ``day``.

        ``day``'s own year must be verified, so ``previous_session(date(2028, 1, 1))`` raises for
        2028 even though 2027-12-31 is a session. The walk then steps back one day at a time and
        never guesses: it raises as soon as it reaches a day in an unverified year, so
        ``previous_session(date(2026, 1, 2))`` raises for 2025.

        Raises:
            CalendarNotVerified: ``day``, or a day the walk reached, lies in a year that is not
                verified.
            TypeError: ``day`` is a ``datetime``, which never equals a ``date`` key and would
                silently miss every closure.
        """
        if isinstance(day, datetime):
            raise TypeError("previous_session takes a date, not a datetime; pass value.date()")
        if day.year not in self._verified_years:
            raise CalendarNotVerified(day.year, self._verified_years)
        candidate = day - _ONE_DAY
        while not self.is_session(candidate):
            candidate -= _ONE_DAY
        return candidate

    def _build_holidays(self, year: int) -> dict[date, str]:
        easter = easter_sunday(year)
        statutory: dict[date, str] = {
            date(year, month, day): name for month, day, name in FIXED_HOLIDAYS
        }
        statutory[easter - 2 * _ONE_DAY] = "Good Friday"
        statutory[easter + _ONE_DAY] = "Easter Monday"

        holidays = dict(statutory)
        for closure in self._gazetted.values():
            if closure.date.year != year:
                continue
            existing = holidays.get(closure.date)
            holidays[closure.date] = (
                closure.reason if existing is None else f"{existing}; {closure.reason}"
            )

        # The Act substitutes "the next following day, not being itself a public holiday", and
        # that wording is implemented rather than the looser "the next following day, if it is
        # not itself a holiday", which would add no substitute when that day is a holiday. Both
        # give the same dates for 2026 and 2027; they differ only when a Sunday holiday is
        # followed by another holiday, as with Christmas on a Sunday, where the substitute moves
        # past Boxing Day. Holidays are taken in date order and each substitute is added before
        # the next is placed, so a chain resolves deterministically. Gazetted rows are already
        # observed dates and get no substitute.
        for day in sorted(statutory):
            if day.weekday() != _SUNDAY:
                continue
            substitute = day + _ONE_DAY
            while substitute in holidays:
                substitute += _ONE_DAY
            holidays[substitute] = f"{statutory[day]} (substitute)"
        return holidays


@functools.cache
def _load_default_calendar() -> ExchangeCalendar:
    text = files(_CLOSURES_PACKAGE).joinpath(_CLOSURES_RESOURCE).read_text(encoding="utf-8")
    verified_years, closures = parse_closures_csv(text)
    logger.debug(
        "exchange calendar loaded: verified years %s, %d gazetted closures",
        ", ".join(str(y) for y in sorted(verified_years)),
        len(closures),
    )
    return ExchangeCalendar(closures, verified_years)
