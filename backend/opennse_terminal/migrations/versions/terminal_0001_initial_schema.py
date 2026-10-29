"""Initial terminal_ schema: the 11 product tables.

Revision ID: terminal_0001
Revises:
Create Date: 2026-10-28 00:00:00.000000+00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "terminal_0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# This revision is a frozen snapshot of the schema: it never imports the ORM models or package
# constants, so the share-count bound 10^12 is the literal 1000000000000. Every index and
# constraint takes its final name through op.f(), which stops the TerminalBase naming convention
# (supplied by env.py's target_metadata) from prefixing it a second time.


def upgrade() -> None:
    """Create the 11 ``terminal_`` tables and their indexes, parents before children."""
    op.create_table(
        "terminal_equities",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("isin", sa.String(length=12), nullable=True),
        sa.Column("company_name", sa.String(length=200), nullable=False),
        sa.Column("sector", sa.String(length=100), nullable=False),
        sa.Column("shares_outstanding", sa.BigInteger(), nullable=False),
        sa.Column("free_float_pct", sa.Numeric(precision=7, scale=4), nullable=True),
        sa.Column("currency", sa.CHAR(length=3), server_default=sa.text("'KES'"), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_equities")),
        sa.UniqueConstraint("ticker", name=op.f("uq_terminal_equities_ticker")),
        sa.UniqueConstraint("isin", name=op.f("uq_terminal_equities_isin")),
        sa.CheckConstraint(
            "shares_outstanding BETWEEN 0 AND 1000000000000",
            name=op.f("ck_terminal_equities_shares_outstanding_range"),
        ),
        sa.CheckConstraint(
            "free_float_pct BETWEEN 0 AND 100",
            name=op.f("ck_terminal_equities_free_float_pct_range"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency)", name=op.f("ck_terminal_equities_currency_upper")
        ),
    )

    op.create_table(
        "terminal_ingestion_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pipeline", sa.String(length=48), nullable=False),
        sa.Column("trigger", sa.String(length=16), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("rows_fetched", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("rows_written", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("rows_rejected", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_ingestion_runs")),
        sa.CheckConstraint(
            "trigger IN ('schedule', 'cli', 'api', 'seed')",
            name=op.f("ck_terminal_ingestion_runs_trigger_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'partial', 'failed', 'layout_changed', "
            "'blocked_by_robots', 'no_new_data', 'ambiguous_session')",
            name=op.f("ck_terminal_ingestion_runs_status_valid"),
        ),
    )
    op.create_index(
        op.f("ix_terminal_ingestion_runs_pipeline_started_at"),
        "terminal_ingestion_runs",
        ["pipeline", "started_at"],
        unique=False,
    )

    op.create_table(
        "terminal_corporate_filings",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=True),
        sa.Column("company_name", sa.String(length=200), nullable=True),
        sa.Column("document_type", sa.String(length=24), nullable=False),
        sa.Column("period_end_date", sa.Date(), nullable=True),
        sa.Column("period_type", sa.String(length=2), nullable=True),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("file_sha256", sa.CHAR(length=64), nullable=False),
        sa.Column("file_size_bytes", sa.Integer(), nullable=False),
        sa.Column("page_count", sa.Integer(), nullable=False),
        sa.Column("detected_scale", sa.BigInteger(), nullable=True),
        sa.Column("extraction", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "committed_to_fundamentals",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("currency", sa.CHAR(length=3), server_default=sa.text("'KES'"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_corporate_filings")),
        sa.ForeignKeyConstraint(
            ["ticker"],
            ["terminal_equities.ticker"],
            name=op.f("fk_terminal_corporate_filings_ticker_terminal_equities"),
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("file_sha256", name=op.f("uq_terminal_corporate_filings_file_sha256")),
        sa.CheckConstraint(
            "document_type IN ('annual_report', 'interim_report', 'prospectus', 'other')",
            name=op.f("ck_terminal_corporate_filings_document_type_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('extracted', 'partial', 'no_items')",
            name=op.f("ck_terminal_corporate_filings_status_valid"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency)",
            name=op.f("ck_terminal_corporate_filings_currency_upper"),
        ),
    )

    op.create_table(
        "terminal_daily_prices",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open_price", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("high_price", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("low_price", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("close_price", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("turnover_kes", sa.Numeric(precision=24, scale=2), nullable=False),
        sa.Column(
            "turnover_estimated", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("foreign_net_flow_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("flow_source", sa.String(length=32), nullable=True),
        sa.Column("ingestion_run_id", sa.Uuid(), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_daily_prices")),
        sa.ForeignKeyConstraint(
            ["ticker"],
            ["terminal_equities.ticker"],
            name=op.f("fk_terminal_daily_prices_ticker_terminal_equities"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        # Shortened by hand: the convention's name for this FK would be 65 characters, past
        # PostgreSQL's 63-character identifier limit.
        sa.ForeignKeyConstraint(
            ["ingestion_run_id"],
            ["terminal_ingestion_runs.id"],
            name=op.f("fk_terminal_daily_prices_ingestion_run_id"),
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("ticker", "trade_date", name=op.f("uq_terminal_daily_prices_ticker")),
        sa.CheckConstraint(
            "high_price IS NULL OR low_price IS NULL OR high_price >= low_price",
            name=op.f("ck_terminal_daily_prices_high_gte_low"),
        ),
        sa.CheckConstraint(
            "close_price > 0", name=op.f("ck_terminal_daily_prices_close_price_positive")
        ),
        sa.CheckConstraint(
            "volume BETWEEN 0 AND 1000000000000",
            name=op.f("ck_terminal_daily_prices_volume_range"),
        ),
        sa.CheckConstraint(
            "turnover_kes >= 0", name=op.f("ck_terminal_daily_prices_turnover_non_negative")
        ),
        sa.CheckConstraint(
            "(flow_source IS NULL) = (foreign_net_flow_kes IS NULL)",
            name=op.f("ck_terminal_daily_prices_flow_source_matches_flow"),
        ),
    )
    op.create_index(
        op.f("ix_terminal_daily_prices_trade_date"),
        "terminal_daily_prices",
        ["trade_date"],
        unique=False,
    )

    op.create_table(
        "terminal_fundamentals",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column("period_end_date", sa.Date(), nullable=False),
        sa.Column(
            "period_type", sa.String(length=2), server_default=sa.text("'FY'"), nullable=False
        ),
        sa.Column("revenue_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("ebitda_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("net_income_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("total_assets_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("total_equity_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("total_debt_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("cash_and_equivalents_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("operating_cash_flow_kes", sa.Numeric(precision=24, scale=2), nullable=True),
        sa.Column("dps_kes", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("shares_outstanding", sa.BigInteger(), nullable=True),
        sa.Column("filing_id", sa.Uuid(), nullable=True),
        sa.Column(
            "source_by_field",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("currency", sa.CHAR(length=3), server_default=sa.text("'KES'"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_fundamentals")),
        sa.ForeignKeyConstraint(
            ["ticker"],
            ["terminal_equities.ticker"],
            name=op.f("fk_terminal_fundamentals_ticker_terminal_equities"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["filing_id"],
            ["terminal_corporate_filings.id"],
            name=op.f("fk_terminal_fundamentals_filing_id_terminal_corporate_filings"),
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "ticker",
            "period_end_date",
            "period_type",
            name=op.f("uq_terminal_fundamentals_ticker"),
        ),
        sa.CheckConstraint(
            "period_type IN ('FY', 'H1')", name=op.f("ck_terminal_fundamentals_period_type_valid")
        ),
        sa.CheckConstraint(
            "shares_outstanding BETWEEN 0 AND 1000000000000",
            name=op.f("ck_terminal_fundamentals_shares_outstanding_range"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency)", name=op.f("ck_terminal_fundamentals_currency_upper")
        ),
    )

    op.create_table(
        "terminal_macro_indicators",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("inflation_yoy_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("cbr_rate_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("interbank_rate_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("tbill_91d_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("tbill_182d_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("tbill_364d_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("fx_kes_usd", sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column("fx_reserves_usd_m", sa.Numeric(precision=14, scale=2), nullable=True),
        sa.Column(
            "source_by_field",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_macro_indicators")),
        sa.UniqueConstraint(
            "observation_date", name=op.f("uq_terminal_macro_indicators_observation_date")
        ),
    )

    op.create_table(
        "terminal_bond_yields",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("bond_code", sa.String(length=32), nullable=False),
        sa.Column("isin", sa.String(length=12), nullable=True),
        sa.Column("original_tenor_years", sa.Numeric(precision=6, scale=2), nullable=False),
        sa.Column("remaining_tenor_years", sa.Numeric(precision=6, scale=2), nullable=False),
        sa.Column("maturity_date", sa.Date(), nullable=False),
        sa.Column("coupon_rate_pct", sa.Numeric(precision=9, scale=4), nullable=True),
        sa.Column("yield_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_bond_yields")),
        sa.UniqueConstraint(
            "observation_date",
            "bond_code",
            name=op.f("uq_terminal_bond_yields_observation_date"),
        ),
        sa.CheckConstraint(
            "original_tenor_years > 0",
            name=op.f("ck_terminal_bond_yields_original_tenor_positive"),
        ),
        sa.CheckConstraint(
            "remaining_tenor_years > 0",
            name=op.f("ck_terminal_bond_yields_remaining_tenor_positive"),
        ),
        sa.CheckConstraint("yield_pct > 0", name=op.f("ck_terminal_bond_yields_yield_positive")),
    )
    op.create_index(
        op.f("ix_terminal_bond_yields_observation_date"),
        "terminal_bond_yields",
        ["observation_date"],
        unique=False,
    )

    op.create_table(
        "terminal_ipo_scenarios",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("company_name", sa.String(length=200), nullable=False),
        sa.Column("sector", sa.String(length=100), nullable=True),
        sa.Column("offer_price_kes", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("shares_offered", sa.BigInteger(), nullable=False),
        sa.Column("shares_outstanding_post_offer", sa.BigInteger(), nullable=False),
        sa.Column("pct_stake_sold", sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column(
            "primary_proceeds_kes",
            sa.Numeric(precision=24, scale=2),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("base_revenue_kes", sa.Numeric(precision=24, scale=2), nullable=False),
        sa.Column("base_ebitda_kes", sa.Numeric(precision=24, scale=2), nullable=False),
        sa.Column("net_debt_kes", sa.Numeric(precision=24, scale=2), nullable=False),
        sa.Column("assumed_wacc_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("assumed_growth_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("terminal_growth_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("margin_expansion_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("tax_rate_pct", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("capex_pct_revenue", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("da_pct_revenue", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("nwc_pct_revenue", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("peer_ev_ebitda_multiple", sa.Numeric(precision=9, scale=4), nullable=False),
        sa.Column("dcf_weight_pct", sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column("forecast_years", sa.SmallInteger(), nullable=False),
        sa.Column(
            "risk_scenarios",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("is_preset", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("currency", sa.CHAR(length=3), server_default=sa.text("'KES'"), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_ipo_scenarios")),
        sa.UniqueConstraint("company_name", name=op.f("uq_terminal_ipo_scenarios_company_name")),
        sa.CheckConstraint(
            "offer_price_kes > 0", name=op.f("ck_terminal_ipo_scenarios_offer_price_positive")
        ),
        sa.CheckConstraint(
            "shares_offered BETWEEN 1 AND 1000000000000",
            name=op.f("ck_terminal_ipo_scenarios_shares_offered_range"),
        ),
        sa.CheckConstraint(
            "shares_outstanding_post_offer BETWEEN 1 AND 1000000000000",
            name=op.f("ck_terminal_ipo_scenarios_shares_post_offer_range"),
        ),
        sa.CheckConstraint(
            "shares_offered <= shares_outstanding_post_offer",
            name=op.f("ck_terminal_ipo_scenarios_shares_offered_lte_post_offer"),
        ),
        sa.CheckConstraint(
            "pct_stake_sold BETWEEN 0 AND 100",
            name=op.f("ck_terminal_ipo_scenarios_pct_stake_sold_range"),
        ),
        sa.CheckConstraint(
            "dcf_weight_pct BETWEEN 0 AND 100",
            name=op.f("ck_terminal_ipo_scenarios_dcf_weight_range"),
        ),
        sa.CheckConstraint(
            "forecast_years BETWEEN 3 AND 10",
            name=op.f("ck_terminal_ipo_scenarios_forecast_years_range"),
        ),
        sa.CheckConstraint(
            "currency = upper(currency)", name=op.f("ck_terminal_ipo_scenarios_currency_upper")
        ),
    )

    op.create_table(
        "terminal_cycles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("label", sa.String(length=64), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column(
            "event_type",
            sa.String(length=32),
            server_default=sa.text("'general_election'"),
            nullable=False,
        ),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_cycles")),
        sa.UniqueConstraint("event_date", name=op.f("uq_terminal_cycles_event_date")),
    )

    # owner_key is the opaque principal subject supplied by the host; it deliberately has no
    # foreign key, because the terminal owns no users or auth table.
    op.create_table(
        "terminal_watchlists",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_key", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_terminal_watchlists")),
        sa.UniqueConstraint("owner_key", "name", name=op.f("uq_terminal_watchlists_owner_key")),
    )
    op.create_index(
        op.f("ix_terminal_watchlists_owner_key"),
        "terminal_watchlists",
        ["owner_key"],
        unique=False,
    )

    op.create_table(
        "terminal_watchlist_items",
        sa.Column("watchlist_id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(length=16), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("watchlist_id", "ticker", name=op.f("pk_terminal_watchlist_items")),
        sa.ForeignKeyConstraint(
            ["watchlist_id"],
            ["terminal_watchlists.id"],
            name=op.f("fk_terminal_watchlist_items_watchlist_id_terminal_watchlists"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["ticker"],
            ["terminal_equities.ticker"],
            name=op.f("fk_terminal_watchlist_items_ticker_terminal_equities"),
            ondelete="CASCADE",
        ),
    )


def downgrade() -> None:
    """Drop exactly the indexes and tables ``upgrade()`` created, children before parents."""
    op.drop_table("terminal_watchlist_items")
    op.drop_index(op.f("ix_terminal_watchlists_owner_key"), table_name="terminal_watchlists")
    op.drop_table("terminal_watchlists")
    op.drop_table("terminal_cycles")
    op.drop_table("terminal_ipo_scenarios")
    op.drop_index(
        op.f("ix_terminal_bond_yields_observation_date"), table_name="terminal_bond_yields"
    )
    op.drop_table("terminal_bond_yields")
    op.drop_table("terminal_macro_indicators")
    op.drop_table("terminal_fundamentals")
    op.drop_index(op.f("ix_terminal_daily_prices_trade_date"), table_name="terminal_daily_prices")
    op.drop_table("terminal_daily_prices")
    op.drop_table("terminal_corporate_filings")
    op.drop_index(
        op.f("ix_terminal_ingestion_runs_pipeline_started_at"),
        table_name="terminal_ingestion_runs",
    )
    op.drop_table("terminal_ingestion_runs")
    op.drop_table("terminal_equities")
