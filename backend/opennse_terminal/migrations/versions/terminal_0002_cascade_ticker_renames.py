"""Cascade ticker renames to watchlist items and corporate filings.

``terminal_0001`` created the ticker foreign keys of ``terminal_watchlist_items`` and
``terminal_corporate_filings`` with ON UPDATE NO ACTION, so PostgreSQL rejected renaming a
``terminal_equities.ticker`` that any watchlist item or filing still referenced. This revision
recreates both constraints, under their existing names, with ON UPDATE CASCADE, matching the
ticker foreign keys of ``terminal_daily_prices`` and ``terminal_fundamentals``. Their delete
actions are unchanged: a watchlist item is deleted with its equity (CASCADE) and a filing keeps
its extraction with its ticker set to null (SET NULL).

It applies to any database at ``terminal_0001``, whatever rows it holds: the referencing columns
do not change, so every existing row still satisfies the recreated constraints. The drop and the
re-creation run in the migration's single transaction, so no committed state lacks either key.
``downgrade()`` restores the two constraints exactly as ``terminal_0001`` defines them.

Revision ID: terminal_0002
Revises: terminal_0001
Create Date: 2026-10-29 18:26:29.313370

"""

from collections.abc import Sequence

import sqlalchemy as sa  # noqa: F401
from alembic import op  # noqa: F401

# revision identifiers, used by Alembic.
revision: str = "terminal_0002"
down_revision: str | Sequence[str] | None = "terminal_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Like terminal_0001, this revision is a frozen snapshot: it never imports the ORM models or
# package constants. Each constraint keeps its terminal_0001 name through op.f(), which stops the
# TerminalBase naming convention (supplied by env.py's target_metadata) from applying twice.


def upgrade() -> None:
    """Recreate the watchlist-item and filing ticker foreign keys with ON UPDATE CASCADE.

    Only these two ``terminal_`` constraints change; their names and delete actions stay.
    """
    op.drop_constraint(
        op.f("fk_terminal_corporate_filings_ticker_terminal_equities"),
        "terminal_corporate_filings",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("fk_terminal_corporate_filings_ticker_terminal_equities"),
        "terminal_corporate_filings",
        "terminal_equities",
        ["ticker"],
        ["ticker"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )
    op.drop_constraint(
        op.f("fk_terminal_watchlist_items_ticker_terminal_equities"),
        "terminal_watchlist_items",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("fk_terminal_watchlist_items_ticker_terminal_equities"),
        "terminal_watchlist_items",
        "terminal_equities",
        ["ticker"],
        ["ticker"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )


def downgrade() -> None:
    """Restore both ticker foreign keys exactly as ``terminal_0001`` created them.

    ON UPDATE reverts to NO ACTION and the delete actions stay, in reverse order of
    ``upgrade()``; no non-``terminal_`` object is touched.
    """
    op.drop_constraint(
        op.f("fk_terminal_watchlist_items_ticker_terminal_equities"),
        "terminal_watchlist_items",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("fk_terminal_watchlist_items_ticker_terminal_equities"),
        "terminal_watchlist_items",
        "terminal_equities",
        ["ticker"],
        ["ticker"],
        ondelete="CASCADE",
    )
    op.drop_constraint(
        op.f("fk_terminal_corporate_filings_ticker_terminal_equities"),
        "terminal_corporate_filings",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("fk_terminal_corporate_filings_ticker_terminal_equities"),
        "terminal_corporate_filings",
        "terminal_equities",
        ["ticker"],
        ["ticker"],
        ondelete="SET NULL",
    )
