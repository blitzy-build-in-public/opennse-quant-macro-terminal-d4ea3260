"""ORM models for the CBK macro series: ``terminal_macro_indicators`` and ``terminal_bond_yields``.

``terminal_macro_indicators`` holds one row per observation date with the eight indicator columns
named in ``MACRO_INDICATOR_COLUMNS``. ``terminal_bond_yields`` holds Treasury bond auction results,
the long-term leg of the yield curve.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.db.base import TerminalBase

MACRO_INDICATOR_COLUMNS: Final[tuple[str, ...]] = (
    "inflation_yoy_pct",
    "cbr_rate_pct",
    "interbank_rate_pct",
    "tbill_91d_pct",
    "tbill_182d_pct",
    "tbill_364d_pct",
    "fx_kes_usd",
    "fx_reserves_usd_m",
)
"""The indicator columns of ``terminal_macro_indicators``, in table order. Each one is written
independently and carries its own entry in ``source_by_field``."""


class TerminalMacroIndicator(TerminalBase):
    """One observation date of Kenyan macro indicators (``terminal_macro_indicators``).

    Units: ``*_pct`` columns are percentage points (``6.80`` means 6.80%), ``fx_kes_usd`` is KES
    per USD and ``fx_reserves_usd_m`` is millions of USD. Monthly series are dated at the month-end
    of their reference period; FX reserves are dated at each weekly bulletin column's header date.

    Provenance: there is no row-level ``source``. ``source_by_field`` maps each written column name
    to its source (``seed``, ``cbk_rates_csv``, ``cbk_inflation``, ``cbk_fx``, ``cbk_reserves`` or
    ``csv_import``), because several releases with different cadences fill one row.

    Writes: every indicator is nullable and each writer COALESCE-merges only its own columns on
    ``observation_date``, so CBK values replace seeded values for the same date column by column
    and a later partial release never nulls another writer's value. ``ON CONFLICT DO UPDATE`` does
    not apply ``onupdate``, so upserts set ``updated_at`` explicitly.
    """

    __tablename__ = "terminal_macro_indicators"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # The unique constraint also serves as the lookup index on observation_date.
    observation_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    inflation_yoy_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    cbr_rate_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    interbank_rate_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    tbill_91d_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    tbill_182d_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    tbill_364d_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    fx_kes_usd: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    fx_reserves_usd_m: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    source_by_field: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class TerminalBondYield(TerminalBase):
    """One Treasury bond auction observation (``terminal_bond_yields``).

    ``observation_date`` is the auction issue date. ``remaining_tenor_years`` is
    ``(maturity_date - observation_date) / 365.25``; ``original_tenor_years`` is the issue's
    nominal tenor. Rates are percentage points.

    Every issue code is stored (FXD, IFB, SDB), but the yield curve reads only codes beginning with
    ``FXD``, because tax-exempt infrastructure bonds and special bonds are not comparable. Seeded
    codes read ``FXD-SEED-<tenor>Y-<YYYYMM>``, so they never collide with real CBK issue numbers
    such as ``FXD1/2018/025``; within a tenor bucket a real observation (``source != 'seed'``)
    always takes precedence over a seed one.

    ``source`` is ``cbk_bonds`` or ``seed``. Rows are upserted on ``(observation_date, bond_code)``.
    """

    __tablename__ = "terminal_bond_yields"
    __table_args__ = (
        # Rendered as uq_terminal_bond_yields_observation_date; this is the cbk_bonds upsert key.
        UniqueConstraint("observation_date", "bond_code"),
        CheckConstraint("original_tenor_years > 0", name="original_tenor_positive"),
        CheckConstraint("remaining_tenor_years > 0", name="remaining_tenor_positive"),
        CheckConstraint("yield_pct > 0", name="yield_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    observation_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    bond_code: Mapped[str] = mapped_column(String(32), nullable=False)
    isin: Mapped[str | None] = mapped_column(String(12), nullable=True)
    original_tenor_years: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    remaining_tenor_years: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    maturity_date: Mapped[date] = mapped_column(Date, nullable=False)
    coupon_rate_pct: Mapped[Decimal | None] = mapped_column(Numeric(9, 4), nullable=True)
    yield_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
