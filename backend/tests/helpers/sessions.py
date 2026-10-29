"""Synthetic market-session calendars for the pure quant unit tests.

Sessions are plain Monday-Friday dates with no holiday logic, because the analytics count the
observed market sessions they are given and never consult a holiday list. A test that needs a
missing session removes that date from the list itself.
"""

from datetime import date, timedelta

__all__ = ["weekdays"]


def weekdays(start: date, count: int) -> list[date]:
    """Return ``count`` consecutive Monday-Friday dates from ``start``, with no holiday logic.

    ``start`` is the first date returned when it is a weekday; otherwise the list begins on the
    following Monday. Each call builds a new list, so a caller may change its copy freely.
    """
    days: list[date] = []
    day = start
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days
