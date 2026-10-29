"""IPO sandbox presets: table ``terminal_ipo_scenarios``, natural key ``company_name``.

Each row stores every assumption the IPO valuation sandbox needs to restore a preset, plus the
preset's qualitative risk notes. Presets are read-only reference data: valuation is stateless and
saving user-defined scenarios is out of scope, so the table has no owner column.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Numeric,
    SmallInteger,
    String,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from opennse_terminal.config import DEFAULT_CURRENCY, MAX_SHARE_COUNT
from opennse_terminal.db.base import TerminalBase


class TerminalIpoScenario(TerminalBase):
    """One IPO sandbox preset (``terminal_ipo_scenarios``).

    The table keeps the schema attachment's name, ``terminal_ipo_scenarios``, rather than the
    ``terminal_ipo_models`` that Rule 3 gives as an example: both name this entity, the attachment
    is the column-level specification, and the ``terminal_`` prefix the rule requires is met.

    Units:

    * ``*_kes`` columns are whole KES; ``offer_price_kes`` is KES per share.
    * ``*_pct`` columns are percentage points: ``assumed_wacc_pct = 18.00`` means 18%.
      ``margin_expansion_pct`` is the points added to the EBITDA margin by forecast year N.
    * ``peer_ev_ebitda_multiple`` is a multiple (x).
    * Share counts are integers bounded to ``MAX_SHARE_COUNT``.

    Mapping to ``IpoValuationRequest`` and ``quant.dcf.DcfAssumptions``: ``assumed_wacc_pct`` is
    ``wacc_pct``, ``assumed_growth_pct`` is ``revenue_growth_pct`` and
    ``shares_outstanding_post_offer`` is ``shares_outstanding``; every other assumption column has
    the request field's name. ``ebitda_margin_pct`` is not stored: ``services/ipo_service.py``
    derives it as ``base_ebitda_kes / base_revenue_kes * 100``.

    ``risk_scenarios`` is a JSON array of ``{"title", "description"}`` objects.

    The seeder writes the Quickmart Supermarket preset (sector "Retail & Consumer Goods",
    ``source = 'seed'``) with: offer 7.50; stake sold 50.0; primary proceeds 0.00; shares
    outstanding after the offer 1,000,000,000 and offered 500,000,000; base revenue
    45,000,000,000; base EBITDA 2,025,000,000; net debt 4,000,000,000; WACC 18.00; revenue growth
    10.00; terminal growth 5.00; margin expansion 0.50; tax 30.00; capex 2.00; D&A 1.50;
    NWC -3.00; peer EV/EBITDA 6.00; DCF weight 50; forecast years 5.
    """

    __tablename__ = "terminal_ipo_scenarios"
    __table_args__ = (
        CheckConstraint("offer_price_kes > 0", name="offer_price_positive"),
        CheckConstraint(
            f"shares_offered BETWEEN 1 AND {MAX_SHARE_COUNT}", name="shares_offered_range"
        ),
        CheckConstraint(
            f"shares_outstanding_post_offer BETWEEN 1 AND {MAX_SHARE_COUNT}",
            name="shares_post_offer_range",
        ),
        CheckConstraint(
            "shares_offered <= shares_outstanding_post_offer",
            name="shares_offered_lte_post_offer",
        ),
        CheckConstraint("pct_stake_sold BETWEEN 0 AND 100", name="pct_stake_sold_range"),
        CheckConstraint("dcf_weight_pct BETWEEN 0 AND 100", name="dcf_weight_range"),
        CheckConstraint("forecast_years BETWEEN 3 AND 10", name="forecast_years_range"),
        CheckConstraint("currency = upper(currency)", name="currency_upper"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # Rendered as uq_terminal_ipo_scenarios_company_name; this is the seeder's conflict key.
    company_name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    sector: Mapped[str | None] = mapped_column(String(100), nullable=True)
    offer_price_kes: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    shares_offered: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shares_outstanding_post_offer: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pct_stake_sold: Mapped[Decimal] = mapped_column(Numeric(7, 4), nullable=False)
    primary_proceeds_kes: Mapped[Decimal] = mapped_column(
        Numeric(24, 2),
        nullable=False,
        default=Decimal("0"),
        server_default=text("0"),
    )
    base_revenue_kes: Mapped[Decimal] = mapped_column(Numeric(24, 2), nullable=False)
    base_ebitda_kes: Mapped[Decimal] = mapped_column(Numeric(24, 2), nullable=False)
    net_debt_kes: Mapped[Decimal] = mapped_column(Numeric(24, 2), nullable=False)
    assumed_wacc_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    assumed_growth_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    terminal_growth_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    margin_expansion_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    tax_rate_pct: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    capex_pct_revenue: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    da_pct_revenue: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    nwc_pct_revenue: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    peer_ev_ebitda_multiple: Mapped[Decimal] = mapped_column(Numeric(9, 4), nullable=False)
    dcf_weight_pct: Mapped[Decimal] = mapped_column(Numeric(7, 4), nullable=False)
    forecast_years: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    risk_scenarios: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    is_preset: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
    )
    currency: Mapped[str] = mapped_column(
        CHAR(3),
        nullable=False,
        default=DEFAULT_CURRENCY,
        server_default=text("'KES'"),
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
