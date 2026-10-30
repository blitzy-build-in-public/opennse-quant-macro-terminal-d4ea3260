"""Request and response models of the custom watchlist endpoints.

Paths are relative to ``/api/v1/terminal``:

* ``GET /watchlists`` returns ``list[Watchlist]`` for the calling principal;
* ``POST /watchlists`` takes :class:`WatchlistCreate` and returns 201 :class:`Watchlist`;
* ``DELETE /watchlists/{watchlist_id}`` returns 204;
* ``PUT`` and ``DELETE /watchlists/{watchlist_id}/items/{ticker}`` return :class:`Watchlist`.

A watchlist belongs to the opaque ``TerminalPrincipal.subject`` resolved by the router-level
dependency and stored by the service in ``terminal_watchlists.owner_key``. Neither model has an
owner field, so a client can never set or read the owner, and no user or auth schema exists.

Business errors are coded ``ErrorDetail`` bodies raised by ``services.watchlist_service``:
409 ``watchlist_name_taken``, 422 ``unknown_ticker`` and 404 ``watchlist_not_found``.
"""

import datetime as dt
import uuid
from typing import Annotated, Final

from pydantic import Field, StringConstraints

from opennse_terminal.schemas.common import TerminalRequestModel, TerminalResponseModel

__all__ = [
    "MAX_WATCHLIST_NAME_LENGTH",
    "MAX_WATCHLIST_TICKERS",
    "Watchlist",
    "WatchlistCreate",
]

MAX_WATCHLIST_NAME_LENGTH: Final = 100
"""Longest watchlist name, matching ``terminal_watchlists.name`` (``VARCHAR(100)``)."""

MAX_WATCHLIST_TICKERS: Final = 100
"""Most tickers a ``POST /watchlists`` body may list."""

# PostgreSQL text cannot hold a NUL character, so a name carrying one is refused as a
# single-field 422 here rather than failing as a 500 when the service queries it.
_NO_NUL_PATTERN: Final = r"^[^\x00]*$"

# Longest ticker, matching terminal_equities.ticker (VARCHAR(16)).
_MAX_TICKER_LENGTH: Final = 16

# A ticker refuses NUL for the same reason as the name, and also U+001C-U+001F. The service trims
# tickers with str.strip(), which treats those four separators as whitespace, while Pydantic's
# strip_whitespace does not, so a ticker made of them would pass the length check below and reach
# the service empty. No ticker contains them, so they are refused with the standard 422 as well.
_TICKER_PATTERN: Final = r"^[^\x00\x1c-\x1f]*$"

# Whitespace is stripped before the length check, as for the name, so a blank ticker fails
# min_length=1 with the standard 422 rather than reaching the service as an empty ticker.
_WatchlistTicker = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=_MAX_TICKER_LENGTH,
        pattern=_TICKER_PATTERN,
    ),
]


class WatchlistCreate(TerminalRequestModel):
    """Body of ``POST /watchlists``.

    Ticker elements are bounded to 1-16 characters here after surrounding whitespace is removed,
    so a blank ticker, like a blank name, is FastAPI's standard 422. So is a NUL character in the
    name or a ticker, and a U+001C-U+001F separator in a ticker. The service normalizes tickers to
    upper case, removes duplicates and answers an unknown ticker with the coded 422
    ``unknown_ticker``.
    """

    # Whitespace is stripped before the length check, so a blank name fails min_length=1 with
    # the standard 422 rather than being stored as an empty or padded name.
    name: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=MAX_WATCHLIST_NAME_LENGTH,
            pattern=_NO_NUL_PATTERN,
        ),
    ] = Field(
        description=(
            "Watchlist name, 1-100 characters after surrounding whitespace is removed, with no "
            "NUL character; unique per owner."
        ),
    )
    tickers: list[_WatchlistTicker] = Field(
        default_factory=list,
        max_length=MAX_WATCHLIST_TICKERS,
        description=(
            "Initial NSE tickers such as SCOM, at most 100, each 1-16 characters after surrounding "
            "whitespace is removed, with no NUL or U+001C-U+001F character; each must exist in "
            "terminal_equities. Omitted means an empty watchlist."
        ),
    )


class Watchlist(TerminalResponseModel):
    """One watchlist of the calling principal.

    ``GET /watchlists`` returns a list of them, ``POST /watchlists`` returns one with 201, and
    ``PUT`` and ``DELETE /watchlists/{watchlist_id}/items/{ticker}`` return it with 200.
    ``DELETE /watchlists/{watchlist_id}`` returns 204 with no body.
    """

    id: uuid.UUID = Field(description="Watchlist identifier (UUID).")
    name: str = Field(description="Watchlist name, unique per owner.")
    tickers: list[str] = Field(
        description="NSE tickers on the watchlist, sorted ascending; empty when it has none.",
    )
    created_at: dt.datetime = Field(
        description="When the watchlist was created (ISO 8601 timestamp with UTC offset).",
    )
    updated_at: dt.datetime = Field(
        description=(
            "Creation time, or the time of the latest tracked change (ISO 8601 timestamp with UTC "
            "offset). Tracked changes are an update of the watchlist row through SQLAlchemy (the "
            "column's onupdate) and a ticker actually added or removed by PUT or DELETE "
            "/watchlists/{watchlist_id}/items/{ticker}; an idempotent no-op is not one. Item rows "
            "changed directly in the database, such as by the cascade when an equity's ticker is "
            "renamed or the equity is deleted, change tickers without advancing it."
        ),
    )
