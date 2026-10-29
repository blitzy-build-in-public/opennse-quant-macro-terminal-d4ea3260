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

from opennse_terminal.schemas.common import TerminalRequestModel, TerminalResponseModel, Ticker

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


class WatchlistCreate(TerminalRequestModel):
    """Body of ``POST /watchlists``.

    Ticker elements are bounded to 1-16 characters here. The service normalizes them to upper
    case, removes duplicates and answers an unknown ticker with the coded 422 ``unknown_ticker``.
    """

    # Whitespace is stripped before the length check, so a blank name fails min_length=1 with
    # the standard 422 rather than being stored as an empty or padded name.
    name: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True, min_length=1, max_length=MAX_WATCHLIST_NAME_LENGTH
        ),
    ] = Field(
        description=(
            "Watchlist name, 1-100 characters after surrounding whitespace is removed; unique "
            "per owner."
        ),
    )
    tickers: list[Ticker] = Field(
        default_factory=list,
        max_length=MAX_WATCHLIST_TICKERS,
        description=(
            "Initial NSE tickers such as SCOM, at most 100; each must exist in terminal_equities. "
            "Omitted means an empty watchlist."
        ),
    )


class Watchlist(TerminalResponseModel):
    """One watchlist of the calling principal, as returned by every watchlist endpoint."""

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
            "When the watchlist or its tickers last changed (ISO 8601 timestamp with UTC "
            "offset); advanced only by a real item addition or removal."
        ),
    )
