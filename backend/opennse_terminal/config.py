"""Central configuration of the OpenNSE terminal.

This module sits at the bottom of the package's dependency graph and imports nothing from
``opennse_terminal``. It holds:

* the package-wide constants :data:`MAX_SHARE_COUNT` and :data:`DEFAULT_CURRENCY`, which models,
  schemas, ingestion and the seeder import from here rather than restating;
* :class:`TerminalSettings`, the typed runtime configuration.

``TerminalSettings`` is deliberately not cached here. The API caches one instance in
``api.deps.get_terminal_settings``, which a host may override; the CLI, ingestion pipelines,
scheduler and seeder construct ``TerminalSettings()`` once per process, so an environment change
(for example ``monkeypatch.setenv("TERMINAL_DATABASE_URL", ...)`` in tests) is picked up by the
next construction.
"""

from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["DEFAULT_CURRENCY", "MAX_SHARE_COUNT", "TerminalSettings"]

MAX_SHARE_COUNT: Final[int] = 10**12
"""Upper bound on every share count and volume: database CHECKs, request schemas, parsers and
importers all reject larger values. It is below 2**53, so these JSON integers stay exact in
JavaScript."""

DEFAULT_CURRENCY: Final[str] = "KES"
"""ISO 4217 code every ``currency`` column and monetary value defaults to (Kenyan shilling)."""

_ASYNC_DRIVER_PREFIX: Final = "postgresql+asyncpg://"


class TerminalSettings(BaseSettings):
    """Runtime configuration, read from ``TERMINAL_<FIELD_NAME_UPPER>`` variables or ``.env``.

    A process environment variable takes precedence over ``.env``, which is resolved relative to
    the working directory (the documented commands run from the repository root). Keys in
    ``.env`` that are not fields here, such as ``TERMINAL_TEST_DATABASE_URL`` and
    ``TERMINAL_API_ORIGIN``, are ignored, because the file is shared with Next.js and the tests.

    A host need not use the environment at all: every field can be passed as a keyword argument,
    so ``app.dependency_overrides[get_terminal_settings] = lambda: TerminalSettings(
    database_url=...)`` configures the mounted router from the host's own configuration.

    ``database_url`` carries the database password. Never log the settings object, the raw URL or
    any other connection string; log ``opennse_terminal.db.session.database_log_label(url)``
    instead, a prose label naming only the database, host and port (for example
    ``database "opennse" on localhost:5432``). ``render_as_string(hide_password=True)`` is not a
    safe substitute: it prints query values, and asyncpg accepts the password as
    ``?password=...``.
    ``repr()`` and ``str()`` leave ``database_url`` out, but ``model_dump()`` still includes it.
    """

    # hide_input_in_errors keeps a rejected database_url, password included, out of the
    # ValidationError text that the CLI, scheduler and seeder print when settings fail to load.
    # Error messages name the field and the rule; the timezone message repeats its own input.
    model_config = SettingsConfigDict(
        env_prefix="TERMINAL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: str = Field(
        default="postgresql+asyncpg://opennse:opennse@localhost:5432/opennse",
        description=(
            "SQLAlchemy async database URL using the asyncpg driver; also used by migrations, "
            "the seeder, the ingestion CLI and the scheduler."
        ),
        repr=False,
    )
    default_owner_key: str = Field(
        default="local",
        min_length=1,
        max_length=255,
        description=(
            "Subject of the default principal when no host authentication is injected, and owner "
            "of the seeded watchlist. An opaque identifier, not a credential."
        ),
    )
    timezone: str = Field(
        default="Africa/Nairobi",
        description=(
            "IANA timezone of the exchange calendar and the scheduler's cron triggers "
            "(Africa/Nairobi is UTC+3 with no daylight saving)."
        ),
    )
    fundamentals_lag_days: int = Field(
        default=90,
        ge=0,
        le=365,
        description=(
            "Days after period_end_date before a fundamentals row is treated as published by "
            "the as-of lookups."
        ),
    )
    scraper_contact: str = Field(
        default="ops@example.invalid",
        min_length=3,
        description=(
            "Contact address in the scrapers' user agent, "
            "OpenNSE-Terminal/<version> (+mailto:<contact>)."
        ),
    )
    # The two *_seconds fields are durations, not money, so float is appropriate for them. They
    # must be finite: allow_inf_nan=False rejects "inf" and overflowing strings such as "1e309",
    # which would otherwise reach asyncio.sleep and httpx.Timeout and stall ingestion.
    scraper_min_interval_seconds: float = Field(
        default=2.0,
        ge=0,
        allow_inf_nan=False,
        description="Minimum spacing between requests to the same host, in seconds.",
    )
    scraper_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        allow_inf_nan=False,
        description=(
            "Read timeout per scraper request, in seconds. The connect timeout is fixed at 10 s."
        ),
    )
    scraper_max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Retries on connect or read timeouts and on HTTP 429, 500, 502, 503 and 504.",
    )
    pdf_max_bytes: int = Field(
        default=20_971_520,
        gt=0,
        description=(
            "Per-file size limit for POST /ingest/pdf, in bytes. The whole request may carry "
            "1 MiB more for multipart overhead."
        ),
    )
    pdf_max_pages: int = Field(
        default=400,
        ge=1,
        description="Maximum number of pages parsed per uploaded PDF.",
    )
    pdf_max_concurrency: int = Field(
        default=2,
        ge=1,
        description=(
            "Concurrent PDF parses per API process; a request beyond it is answered with 429."
        ),
    )

    @field_validator("database_url")
    @classmethod
    def _require_asyncpg_driver(cls, value: str) -> str:
        if not value.startswith(_ASYNC_DRIVER_PREFIX):
            # The URL itself is left out of the message: it may carry the password.
            raise ValueError(
                "database_url must use the asyncpg driver and start with "
                f"'{_ASYNC_DRIVER_PREFIX}' (SQLAlchemy async engine)"
            )
        return value

    @field_validator("default_owner_key")
    @classmethod
    def _refuse_nul_in_owner_key(cls, value: str) -> str:
        # PostgreSQL text cannot hold NUL, and api.deps.TerminalPrincipal refuses such a subject,
        # so the default principal built from this value could never be constructed.
        if "\x00" in value:
            raise ValueError("default_owner_key must not contain a NUL character")
        return value

    @field_validator("timezone")
    @classmethod
    def _require_known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
            # ZoneInfo raises ValueError for malformed keys and OSError when the key names a
            # directory of the tz database (for example "Africa"), besides the not-found error.
            raise ValueError(
                f"timezone must be an IANA time zone name such as 'Africa/Nairobi', got {value!r}"
            ) from exc
        return value

    @property
    def tzinfo(self) -> ZoneInfo:
        """The configured timezone, for building aware datetimes and local dates."""
        return ZoneInfo(self.timezone)
