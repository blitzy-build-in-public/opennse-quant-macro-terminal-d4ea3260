"""Tests for the verified NSE exchange calendar (``opennse_terminal.ingestion.calendar``).

The calendar only dates live snapshots of the public NSE market-statistics page, so every test
here is offline and synchronous: no network, no database. Expected dates are literals checked
against a printed calendar; the tests never recompute Easter or the Sunday-substitution rule.
"""

import csv
from datetime import UTC, date, datetime
from importlib.resources import files

import pytest

from opennse_terminal.ingestion.calendar import (
    CalendarNotVerified,
    Closure,
    ExchangeCalendar,
    easter_sunday,
    parse_closures_csv,
)

SATURDAY = 5
SUNDAY = 6
VERIFIED_YEARS = frozenset({2026, 2027})


def _read_shipped_closures() -> str:
    """Read ``nse_closures.csv`` as installed package data, the way the calendar loads it."""
    resource = files("opennse_terminal.ingestion.data").joinpath("nse_closures.csv")
    return resource.read_text(encoding="utf-8")


# Parsed at collection time so each gazetted closure becomes its own test case.
_SHIPPED_VERIFIED_YEARS, _SHIPPED_CLOSURES = parse_closures_csv(_read_shipped_closures())


@pytest.fixture(scope="module")
def cal() -> ExchangeCalendar:
    return ExchangeCalendar.load_default()


# --- Easter computus -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("year", "expected"),
    [
        pytest.param(2026, date(2026, 4, 5), id="2026"),
        pytest.param(2027, date(2027, 3, 28), id="2027"),
        pytest.param(2019, date(2019, 4, 21), id="2019"),
        pytest.param(2024, date(2024, 3, 31), id="2024"),
        pytest.param(1818, date(1818, 3, 22), id="1818-earliest-possible"),
        pytest.param(2038, date(2038, 4, 25), id="2038-latest-possible"),
    ],
)
def test_easter_sunday_matches_published_dates(year: int, expected: date) -> None:
    """Easter is a pure computation, so it is not bound by the verified years."""
    assert easter_sunday(year) == expected
    assert easter_sunday(year).weekday() == SUNDAY


def test_easter_sunday_rejects_years_before_the_gregorian_calendar() -> None:
    with pytest.raises(ValueError, match="1583"):
        easter_sunday(1582)


# --- Rule holidays and sessions --------------------------------------------------------------


def test_load_default_returns_one_shared_calendar() -> None:
    assert ExchangeCalendar.load_default() is ExchangeCalendar.load_default()


@pytest.mark.parametrize(
    ("day", "reason"),
    [
        pytest.param(date(2026, 4, 3), "Good Friday", id="good-friday-2026"),
        pytest.param(date(2026, 4, 6), "Easter Monday", id="easter-monday-2026"),
        pytest.param(date(2027, 3, 26), "Good Friday", id="good-friday-2027"),
        pytest.param(date(2027, 3, 29), "Easter Monday", id="easter-monday-2027"),
        pytest.param(
            date(2027, 12, 13), "Jamhuri Day (substitute)", id="jamhuri-2027-sunday-substitute"
        ),
        pytest.param(
            date(2027, 10, 11), "Mazingira Day (substitute)", id="mazingira-2027-sunday-substitute"
        ),
        pytest.param(
            date(2027, 12, 27), "Boxing Day (substitute)", id="boxing-2027-sunday-substitute"
        ),
        pytest.param(date(2026, 12, 25), "Christmas Day", id="christmas-2026-friday"),
        pytest.param(date(2027, 1, 1), "New Year's Day", id="new-year-2027"),
        pytest.param(date(2026, 6, 1), "Madaraka Day", id="madaraka-2026"),
        pytest.param(date(2026, 10, 20), "Mashujaa Day", id="mashujaa-2026"),
    ],
)
def test_rule_holidays_on_weekdays_are_closed(
    cal: ExchangeCalendar, day: date, reason: str
) -> None:
    assert day.weekday() < SATURDAY
    assert cal.is_session(day) is False
    assert cal.holidays(day.year)[day] == reason


def test_saturday_christmas_2027_adds_no_substitute(cal: ExchangeCalendar) -> None:
    """Christmas 2027-12-25 is a Saturday, and a Saturday holiday adds no substitute day.

    Friday 2027-12-24 and Tuesday 2027-12-28 stay sessions. Monday 2027-12-27 is closed only
    because Boxing Day 2027-12-26 is a Sunday and is substituted onto it, which its reason shows.
    """
    holidays = cal.holidays(2027)
    assert date(2027, 12, 25).weekday() == SATURDAY
    assert holidays[date(2027, 12, 25)] == "Christmas Day"
    assert holidays[date(2027, 12, 27)] == "Boxing Day (substitute)"
    assert date(2027, 12, 28) not in holidays
    assert cal.is_session(date(2027, 12, 24)) is True
    assert cal.is_session(date(2027, 12, 28)) is True


@pytest.mark.parametrize(
    ("saturday_holiday", "reason", "following_monday"),
    [
        pytest.param(date(2026, 12, 26), "Boxing Day", date(2026, 12, 28), id="boxing-2026"),
        pytest.param(date(2026, 10, 10), "Mazingira Day", date(2026, 10, 12), id="mazingira-2026"),
        pytest.param(date(2026, 12, 12), "Jamhuri Day", date(2026, 12, 14), id="jamhuri-2026"),
        pytest.param(date(2027, 5, 1), "Labour Day", date(2027, 5, 3), id="labour-2027"),
    ],
)
def test_saturday_holiday_leaves_the_following_monday_a_session(
    cal: ExchangeCalendar, saturday_holiday: date, reason: str, following_monday: date
) -> None:
    assert saturday_holiday.weekday() == SATURDAY
    assert cal.holidays(saturday_holiday.year)[saturday_holiday] == reason
    assert cal.is_session(saturday_holiday) is False
    assert cal.is_session(following_monday) is True


@pytest.mark.parametrize(
    "day",
    [
        pytest.param(date(2026, 10, 24), id="sat-2026-10-24"),
        pytest.param(date(2026, 10, 25), id="sun-2026-10-25"),
        pytest.param(date(2027, 3, 27), id="sat-2027-03-27"),
        pytest.param(date(2027, 3, 28), id="sun-2027-03-28-easter"),
    ],
)
def test_weekends_are_closed_and_not_listed_as_holidays(cal: ExchangeCalendar, day: date) -> None:
    assert day.weekday() >= SATURDAY
    assert cal.is_session(day) is False
    assert day not in cal.holidays(day.year)


@pytest.mark.parametrize(
    "day",
    [
        pytest.param(date(2026, 10, 27), id="tue-2026-10-27"),
        pytest.param(date(2026, 10, 28), id="wed-2026-10-28"),
        pytest.param(date(2026, 4, 2), id="thu-2026-04-02-before-good-friday"),
        pytest.param(date(2026, 12, 31), id="thu-2026-12-31"),
    ],
)
def test_ordinary_weekdays_are_sessions(cal: ExchangeCalendar, day: date) -> None:
    assert cal.is_session(day) is True


@pytest.mark.parametrize(
    ("year", "rule_closures"),
    [
        pytest.param(
            2026,
            {
                date(2026, 1, 1),
                date(2026, 4, 3),
                date(2026, 4, 6),
                date(2026, 5, 1),
                date(2026, 6, 1),
                date(2026, 10, 10),
                date(2026, 10, 20),
                date(2026, 12, 12),
                date(2026, 12, 25),
                date(2026, 12, 26),
            },
            id="2026-no-sunday-holidays",
        ),
        pytest.param(
            2027,
            {
                date(2027, 1, 1),
                date(2027, 3, 26),
                date(2027, 3, 29),
                date(2027, 5, 1),
                date(2027, 6, 1),
                date(2027, 10, 10),
                date(2027, 10, 11),
                date(2027, 10, 20),
                date(2027, 12, 12),
                date(2027, 12, 13),
                date(2027, 12, 25),
                date(2027, 12, 26),
                date(2027, 12, 27),
            },
            id="2027-three-sunday-substitutes",
        ),
    ],
)
def test_holidays_are_exactly_the_rule_dates_plus_gazetted_closures(
    cal: ExchangeCalendar, year: int, rule_closures: set[date]
) -> None:
    """The Act's rules give these dates; every other listed date must be a gazetted CSV row."""
    gazetted = {closure.date for closure in _SHIPPED_CLOSURES if closure.date.year == year}
    assert set(cal.holidays(year)) == rule_closures | gazetted


@pytest.mark.parametrize("method", ["is_session", "previous_session"])
@pytest.mark.parametrize(
    "moment",
    [
        pytest.param(datetime(2026, 4, 3, 9, 30, tzinfo=UTC), id="verified-2026"),
        # The type is checked before the year, so an unverified-year datetime is a TypeError too.
        pytest.param(datetime(2028, 1, 1, 9, 30, tzinfo=UTC), id="unverified-2028"),
    ],
)
def test_lookup_rejects_a_datetime(cal: ExchangeCalendar, method: str, moment: datetime) -> None:
    """A datetime never equals a date key, so it would silently miss every closure."""
    with pytest.raises(TypeError, match=f"^{method} takes a date, not a datetime"):
        getattr(cal, method)(moment)


# --- Gazetted closures data ------------------------------------------------------------------


def test_shipped_closures_declare_the_verified_years(cal: ExchangeCalendar) -> None:
    verified_years, closures = parse_closures_csv(_read_shipped_closures())
    assert verified_years == VERIFIED_YEARS
    assert cal.verified_years == verified_years
    assert len({closure.date for closure in closures}) == len(closures)


@pytest.mark.parametrize(
    "closure", _SHIPPED_CLOSURES, ids=[str(closure.date) for closure in _SHIPPED_CLOSURES]
)
def test_every_gazetted_closure_is_a_closed_weekday(
    cal: ExchangeCalendar, closure: Closure
) -> None:
    assert closure.date.year in _SHIPPED_VERIFIED_YEARS
    assert closure.date.weekday() < SATURDAY
    assert closure.reason
    assert closure.reference
    assert cal.is_session(closure.date) is False
    assert closure.reason in cal.holidays(closure.date.year)[closure.date]


def test_parse_closures_csv_tolerates_bom_comments_blank_lines_and_padding() -> None:
    text = (
        "\ufeff# Gazetted closures\n"
        "#verified_years : 2027 , 2026\n"
        "\n"
        "date,reason,reference\n"
        "# a comment between rows\n"
        "2027-03-10, Idd-ul-Fitr , Kenya Gazette Notice No. 1\n"
        "2026-03-20,Idd-ul-Fitr,Kenya Gazette Notice No. 2\n"
    )
    assert parse_closures_csv(text) == (
        VERIFIED_YEARS,
        (
            Closure(date(2027, 3, 10), "Idd-ul-Fitr", "Kenya Gazette Notice No. 1"),
            Closure(date(2026, 3, 20), "Idd-ul-Fitr", "Kenya Gazette Notice No. 2"),
        ),
    )


def test_parse_closures_csv_accepts_a_header_without_rows() -> None:
    assert parse_closures_csv("# verified_years: 2026\ndate,reason,reference\n") == (
        frozenset({2026}),
        (),
    )


_HEAD = "# verified_years: 2026,2027\ndate,reason,reference\n"

# Malformed quoting makes the csv module raise csv.Error; the parser re-raises it as a ValueError
# naming the file line, counted with the comment and blank lines the csv reader never sees.
_MALFORMED_QUOTING = [
    pytest.param(
        '# verified_years: 2026,2027\n"date,reason,reference\n',
        "^line 2: malformed CSV: unexpected end of data$",
        id="unterminated-quote-in-header",
    ),
    pytest.param(
        _HEAD + '2026-03-20,"Idd-ul-Fitr,Notice 1\n',
        "^line 3: malformed CSV: unexpected end of data$",
        id="unterminated-quote-in-record",
    ),
    pytest.param(
        _HEAD + '2026-03-20,"Idd-ul-Fitr"x,Notice 1\n',
        "^line 3: malformed CSV: ',' expected after '\"'$",
        id="character-after-closing-quote",
    ),
    pytest.param(
        _HEAD + '2026-03-20,Idd-ul-Fitr,Notice 1\n# a comment\n\n2026-03-23,"Ad hoc"x,Notice 2\n',
        "^line 6: malformed CSV: ',' expected after '\"'$",
        id="malformed-record-after-comment-and-blank-line",
    ),
    pytest.param(
        _HEAD + '2026-03-20,"Idd-ul-Fitr,Notice 1\n# a comment\n2026-03-23,Ad hoc,Notice 2\n',
        "^line 5: malformed CSV in the record starting on line 3: unexpected end of data$",
        id="unterminated-quote-consumes-later-lines",
    ),
]


@pytest.mark.parametrize(
    ("text", "message"),
    [
        pytest.param(
            "date,reason,reference\n2026-03-20,Idd-ul-Fitr,Notice 1\n",
            "missing the '# verified_years",
            id="missing-verified-years-line",
        ),
        pytest.param(
            "# verified_years: 2026\n" + _HEAD,
            "declared twice",
            id="verified-years-declared-twice",
        ),
        pytest.param(
            "# verified_years: 2026, 27\ndate,reason,reference\n",
            "four-digit years",
            id="verified-years-not-four-digit",
        ),
        pytest.param("# verified_years: 2026\n", "missing the header row", id="missing-header"),
        pytest.param(
            "# verified_years: 2026\nday,reason,reference\n",
            "header must be exactly",
            id="wrong-header",
        ),
        pytest.param(
            _HEAD + "2028-01-03,Ad hoc closure,Notice 1\n",
            "outside the verified years",
            id="closure-outside-verified-years",
        ),
        pytest.param(
            _HEAD + "2026-10-24,Ad hoc closure,Notice 1\n",
            "weekend day",
            id="weekend-closure",
        ),
        pytest.param(
            _HEAD + "2026-03-20,Idd-ul-Fitr,Notice 1\n2026-03-20,Ad hoc closure,Notice 2\n",
            "duplicate closure date 2026-03-20",
            id="duplicate-closure-date",
        ),
        pytest.param(
            _HEAD + "20/03/2026,Idd-ul-Fitr,Notice 1\n",
            "date must be YYYY-MM-DD",
            id="non-iso-date",
        ),
        pytest.param(
            _HEAD + "2026-02-30,Ad hoc closure,Notice 1\n",
            "invalid date",
            id="impossible-date",
        ),
        pytest.param(
            _HEAD + "2026-03-20,,Notice 1\n",
            "must be non-empty",
            id="empty-reason",
        ),
        pytest.param(
            _HEAD + "2026-03-20,Idd-ul-Fitr, \n",
            "must be non-empty",
            id="blank-reference",
        ),
        pytest.param(
            _HEAD + "2026-03-20,Idd-ul-Fitr\n",
            "missing the 'reference' column",
            id="missing-column",
        ),
        pytest.param(
            _HEAD + "2026-03-20,Idd-ul-Fitr,Notice 1,surplus\n",
            "more than 3 columns",
            id="surplus-column",
        ),
        *_MALFORMED_QUOTING,
    ],
)
def test_parse_closures_csv_rejects_malformed_input(text: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_closures_csv(text)


@pytest.mark.parametrize("text", [pytest.param(p.values[0], id=p.id) for p in _MALFORMED_QUOTING])
def test_parse_closures_csv_keeps_the_csv_error_as_the_cause(text: str) -> None:
    with pytest.raises(ValueError) as exc:
        parse_closures_csv(text)
    assert isinstance(exc.value.__cause__, csv.Error)


@pytest.mark.parametrize(
    ("closures", "message"),
    [
        pytest.param(
            [
                Closure(date(2026, 3, 20), "Idd-ul-Fitr", "Notice 1"),
                Closure(date(2026, 3, 20), "Ad hoc closure", "Notice 2"),
            ],
            "duplicate closure date",
            id="duplicate-date",
        ),
        pytest.param(
            [Closure(date(2028, 1, 3), "Ad hoc closure", "Notice 1")],
            "outside the verified years",
            id="outside-verified-years",
        ),
    ],
)
def test_calendar_rejects_inconsistent_closures(closures: list[Closure], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ExchangeCalendar(closures, VERIFIED_YEARS)


# --- Verified-year boundary and previous sessions --------------------------------------------


@pytest.mark.parametrize(
    ("method", "day", "year"),
    [
        pytest.param("is_session", date(2028, 1, 3), 2028, id="is_session-2028"),
        pytest.param("previous_session", date(2028, 1, 10), 2028, id="previous_session-2028"),
        # The predecessor, 2027-12-31, is a valid 2027 session, but the input's own year is not.
        pytest.param(
            "previous_session",
            date(2028, 1, 1),
            2028,
            id="previous_session-2028-predecessor-verified",
        ),
        pytest.param("is_session", date(2025, 12, 31), 2025, id="is_session-2025"),
        # The previous weekday, 2025-12-31, lies in an unverified year, so the walk never guesses.
        pytest.param("previous_session", date(2026, 1, 2), 2025, id="previous_session-into-2025"),
    ],
)
def test_lookup_outside_the_verified_years_raises(
    cal: ExchangeCalendar, method: str, day: date, year: int
) -> None:
    with pytest.raises(CalendarNotVerified) as exc:
        getattr(cal, method)(day)
    assert exc.value.year == year
    assert exc.value.verified_years == VERIFIED_YEARS
    assert isinstance(exc.value, LookupError)
    assert "nse_closures.csv" in str(exc.value)


@pytest.mark.parametrize("year", [2025, 2028])
def test_holidays_outside_the_verified_years_raises(cal: ExchangeCalendar, year: int) -> None:
    with pytest.raises(CalendarNotVerified) as exc:
        cal.holidays(year)
    assert exc.value.year == year


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        pytest.param(date(2026, 10, 28), date(2026, 10, 27), id="strictly-before-a-session"),
        pytest.param(date(2026, 4, 7), date(2026, 4, 2), id="skips-easter-monday-to-good-friday"),
        pytest.param(date(2027, 1, 4), date(2026, 12, 31), id="crosses-into-verified-2026"),
        pytest.param(date(2027, 12, 28), date(2027, 12, 24), id="skips-boxing-substitute"),
        pytest.param(date(2027, 12, 14), date(2027, 12, 10), id="skips-jamhuri-substitute"),
    ],
)
def test_previous_session_returns_the_latest_session_strictly_before(
    cal: ExchangeCalendar, day: date, expected: date
) -> None:
    assert cal.previous_session(day) == expected
