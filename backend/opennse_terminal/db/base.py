"""Declarative base and naming convention owned by the OpenNSE terminal.

``TerminalBase`` carries the terminal's own ``MetaData``. It must never be merged with, reflected
from or bound to a host application's ``MetaData``: Alembic autogenerate and the isolated
``terminal_alembic_version`` history compare against this collection alone.

Every table is named ``terminal_*`` and every index and constraint name embeds the table name, so
every name the schema creates contains ``terminal_``. PostgreSQL indexes share the relation
namespace with tables, so this also keeps index names clear of a host's.

Obligations for model and migration authors:

1. Every ``CheckConstraint`` needs an explicit short ``name=``. The ``ck`` pattern interpolates
   ``%(constraint_name)s``, so an unnamed CHECK raises ``InvalidRequestError``.
   ``CheckConstraint("volume BETWEEN 0 AND 1000000000000", name="volume_range")`` on
   ``terminal_daily_prices`` becomes ``ck_terminal_daily_prices_volume_range``.
2. Every generated name must fit in ``POSTGRES_MAX_IDENTIFIER_LENGTH`` (63) characters. Past that,
   SQLAlchemy truncates with a hash suffix that the frozen initial migration would have to
   reproduce exactly. The FK from ``terminal_daily_prices.ingestion_run_id`` would render as
   ``fk_terminal_daily_prices_ingestion_run_id_terminal_ingestion_runs`` (65 characters), so it is
   named explicitly ``fk_terminal_daily_prices_ingestion_run_id``. An explicit FK, index, unique or
   primary-key ``name=`` is used verbatim, because those patterns have no
   ``%(constraint_name)s``; such a name must itself start with its kind prefix and contain
   ``terminal_``.
3. The ``ix``, ``uq`` and ``fk`` patterns key on the first column only. Two constraints of one
   kind on one table that share a first column would collide, so one of them needs an explicit
   name. ``UNIQUE (ticker, trade_date)`` renders as ``uq_terminal_daily_prices_ticker``, and
   ``UNIQUE (owner_key, name)`` as ``uq_terminal_watchlists_owner_key``, which differs from the
   index ``ix_terminal_watchlists_owner_key`` by its kind prefix.
"""

import datetime
import uuid
from decimal import Decimal
from typing import Any, Final

from sqlalchemy import DateTime, MetaData, Numeric, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase

TABLE_PREFIX: Final[str] = "terminal_"
"""Prefix carried by every table, and through the naming convention by every index and
constraint, that the terminal creates."""

POSTGRES_MAX_IDENTIFIER_LENGTH: Final[int] = 63
"""PostgreSQL's identifier limit (NAMEDATALEN - 1); every table, index and constraint name must
fit within it."""

TERMINAL_NAMING_CONVENTION: Final[dict[str, str]] = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""Constraint and index naming patterns. Each embeds ``%(table_name)s``, so every generated name
contains ``terminal_``."""


class TerminalBase(DeclarativeBase):
    """Declarative base for every ``terminal_`` ORM model.

    Consumers are the models in ``opennse_terminal.models``, ``migrations/env.py`` (as Alembic's
    ``target_metadata``) and the tests that inspect the schema. ``metadata`` is a fresh collection
    with ``TERMINAL_NAMING_CONVENTION``; the module docstring lists the naming obligations it
    places on models.

    ``type_annotation_map`` resolves bare annotations safely. Columns still declare their exact
    types, such as ``Numeric(18, 4)``, but a bare ``Mapped[Decimal]`` becomes ``NUMERIC``
    returning ``Decimal`` rather than a float type, ``Mapped[datetime.datetime]`` becomes
    ``TIMESTAMPTZ``, ``Mapped[uuid.UUID]`` the native ``UUID``, and the JSON document annotations
    become ``JSONB``.
    """

    metadata = MetaData(naming_convention=TERMINAL_NAMING_CONVENTION)

    type_annotation_map = {
        Decimal: Numeric(asdecimal=True),
        uuid.UUID: Uuid(),
        datetime.datetime: DateTime(timezone=True),
        dict[str, Any]: JSONB(),
        list[dict[str, Any]]: JSONB(),
    }
