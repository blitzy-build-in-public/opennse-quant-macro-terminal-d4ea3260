"""Tests for the scraper duration bounds of ``TerminalSettings`` (``opennse_terminal.config``).

``scraper_min_interval_seconds`` feeds ``asyncio.sleep`` and ``scraper_timeout_seconds`` feeds
``httpx.Timeout`` in the polite client, so both must load as finite numbers: zero spacing is
allowed, the timeout must be positive, and neither has an upper bound. Every case runs through
keyword arguments and through the ``TERMINAL_*`` environment variables, with ``_env_file=None`` so
the checkout's ``.env`` is never read. Offline and synchronous: no network, no database.
"""

import sys

import pytest
from pydantic import ValidationError

from opennse_terminal.config import TerminalSettings

INTERVAL = "scraper_min_interval_seconds"
TIMEOUT = "scraper_timeout_seconds"
DURATION_FIELDS = (INTERVAL, TIMEOUT)
SOURCES = ("keyword", "environment")
LARGEST_FINITE = repr(sys.float_info.max)


def _env_var(field: str) -> str:
    """The environment variable a settings field is read from."""
    return f"TERMINAL_{field.upper()}"


@pytest.fixture(autouse=True)
def _no_duration_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any inherited duration variables, so each test sees only what it sets."""
    for field in DURATION_FIELDS:
        monkeypatch.delenv(_env_var(field), raising=False)


def _load(monkeypatch: pytest.MonkeyPatch, source: str, field: str, raw: str) -> TerminalSettings:
    """Build settings with ``field`` set to ``raw`` through the given source."""
    if source == "environment":
        monkeypatch.setenv(_env_var(field), raw)
        return TerminalSettings(_env_file=None)
    return TerminalSettings(_env_file=None, **{field: raw})


def test_duration_defaults() -> None:
    settings = TerminalSettings(_env_file=None)

    assert settings.scraper_min_interval_seconds == 2.0
    assert settings.scraper_timeout_seconds == 30.0


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("raw", ["inf", "+inf", "-inf", "nan", "1e309"])
@pytest.mark.parametrize("field", DURATION_FIELDS)
def test_non_finite_duration_is_rejected(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, source: str
) -> None:
    # "1e309" overflows to inf when parsed, so it is rejected the same way.
    with pytest.raises(ValidationError) as exc_info:
        _load(monkeypatch, source, field, raw)

    errors = exc_info.value.errors()
    assert [(error["loc"], error["type"]) for error in errors] == [((field,), "finite_number")]
    assert field in str(exc_info.value)


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw", "expected"),
    [
        (INTERVAL, "0", 0.0),
        (INTERVAL, "0.5", 0.5),
        (INTERVAL, LARGEST_FINITE, sys.float_info.max),
        (TIMEOUT, "0.001", 0.001),
        (TIMEOUT, LARGEST_FINITE, sys.float_info.max),
    ],
)
def test_finite_duration_in_range_is_accepted(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, expected: float, source: str
) -> None:
    settings = _load(monkeypatch, source, field, raw)

    assert getattr(settings, field) == expected


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize(
    ("field", "raw", "error_type"),
    [
        (INTERVAL, "-0.001", "greater_than_equal"),
        (INTERVAL, "-1", "greater_than_equal"),
        (TIMEOUT, "0", "greater_than"),
        (TIMEOUT, "-0.001", "greater_than"),
        (TIMEOUT, "-30", "greater_than"),
    ],
)
def test_duration_below_lower_bound_is_rejected(
    monkeypatch: pytest.MonkeyPatch, field: str, raw: str, error_type: str, source: str
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _load(monkeypatch, source, field, raw)

    errors = exc_info.value.errors()
    assert [(error["loc"], error["type"]) for error in errors] == [((field,), error_type)]
    assert field in str(exc_info.value)
