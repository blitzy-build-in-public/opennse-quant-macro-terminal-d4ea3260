"""Custom watchlist endpoints of the terminal router, scoped to the calling principal.

Paths are relative to ``/api/v1/terminal``; ``api/router.py`` mounts this sub-router, which carries
no prefix, tags or dependencies of its own:

* ``GET /watchlists`` lists the caller's watchlists;
* ``POST /watchlists`` creates one (201);
* ``DELETE /watchlists/{watchlist_id}`` deletes one (204, empty body);
* ``PUT`` and ``DELETE /watchlists/{watchlist_id}/items/{ticker}`` add or remove one ticker,
  idempotently (200 with the watchlist either way).

Ownership comes only from ``PrincipalDep``: the opaque ``TerminalPrincipal.subject``, ``local`` by
default and host-overridable through ``get_terminal_principal``. No endpoint accepts an owner in its
path, query or body, and nothing here looks up users. ``services.watchlist_service`` scopes every
query to that subject, commits its own writes and raises the coded errors (409
``watchlist_name_taken``, 422 or 404 ``unknown_ticker``, 404 ``watchlist_not_found``); a watchlist
owned by another subject is answered exactly like a missing one. A malformed watchlist id, a ticker
outside 1-16 characters or an invalid body gets FastAPI's standard 422 list, and a body over
64 KiB gets the coded 413 ``request_too_large``.

No endpoint declares a query parameter. Each binds the empty query-parameter model
``NoQueryParams``, so any query parameter is FastAPI's standard 422 list with one
``extra_forbidden`` entry per unknown name, and the service does not run.

FastAPI reads the ``Annotated`` metadata of the route signatures at runtime, so this module does not
use ``from __future__ import annotations``.
"""

import uuid
from typing import Annotated, Final

from fastapi import APIRouter, Path, Query, status

from opennse_terminal.api.deps import PrincipalDep, SessionDep, TerminalJsonRoute
from opennse_terminal.schemas import (
    ErrorResponse,
    NoQueryParams,
    ValidationErrorResponse,
    Watchlist,
    WatchlistCreate,
)
from opennse_terminal.services import watchlist_service

__all__ = [
    "TICKER_MAX_LENGTH",
    "TickerPath",
    "WatchlistIdPath",
    "add_watchlist_item",
    "create_watchlist",
    "delete_watchlist",
    "list_watchlists",
    "remove_watchlist_item",
    "router",
]

TICKER_MAX_LENGTH: Final = 16
"""Longest ticker path segment, matching ``terminal_equities.ticker`` (``VARCHAR(16)``)."""

# PostgreSQL text cannot hold a NUL character, so a ticker segment carrying one is refused as a
# single-field 422 here rather than failing as a 500 when the service queries it.
_NO_NUL_PATTERN: Final = r"^[^\x00]*$"

# FastAPI validates the query string into a fresh NoQueryParams on every request, so this default
# is used only when a route function is awaited directly without it. The model has no fields, so
# the shared instance holds no state.
_NO_QUERY_PARAMS: Final = NoQueryParams()

WatchlistIdPath = Annotated[uuid.UUID, Path(description="Watchlist id")]
TickerPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=TICKER_MAX_LENGTH,
        pattern=_NO_NUL_PATTERN,
        description="NSE ticker",
    ),
]

# TerminalJsonRoute rather than the default APIRoute: POST /watchlists binds a JSON body, and the
# route class bounds it to JSON_BODY_MAX_BYTES (64 KiB, 413 request_too_large) and answers every
# undecodable body with FastAPI's standard 422 list instead of a 400. Both verdicts are raised
# after the router-level principal and before the session is opened, so a host that refuses the
# caller answers first whatever the body.
# It is router-local and preserved by include_router, so it travels with terminal_router.
router = APIRouter(route_class=TerminalJsonRoute)


@router.get(
    "/watchlists",
    response_model=list[Watchlist],
    summary="List the caller's watchlists",
)
async def list_watchlists(
    session: SessionDep,
    principal: PrincipalDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> list[Watchlist]:
    """List the watchlists owned by the caller's principal subject, ordered by name.

    Names are in Unicode code-point order (collation ``"C"``), whatever the database's default
    collation. Each watchlist's ``tickers`` are sorted ascending. A caller without watchlists gets
    ``[]``.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and no watchlist is read.
    """
    return await watchlist_service.list_watchlists(session, principal)


@router.post(
    "/watchlists",
    response_model=Watchlist,
    status_code=status.HTTP_201_CREATED,
    summary="Create a watchlist",
    responses={
        409: {"model": ErrorResponse, "description": "watchlist_name_taken"},
        413: {
            "model": ErrorResponse,
            "description": "request_too_large: the JSON body exceeds 65536 bytes",
        },
        422: {
            "model": ErrorResponse | ValidationErrorResponse,
            "description": "unknown_ticker or standard validation error",
        },
    },
)
async def create_watchlist(
    body: WatchlistCreate,
    session: SessionDep,
    principal: PrincipalDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> Watchlist:
    """Create a watchlist owned by the caller's principal subject, optionally with tickers.

    Tickers are upper-cased and de-duplicated, and the result lists them sorted. The name must be
    unique per owner (409 ``watchlist_name_taken``) and every ticker must be a known equity (422
    ``unknown_ticker``). A body over 64 KiB is refused with 413 ``request_too_large`` before any
    session is opened. Not idempotent: repeating the request gives 409.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and nothing is created.
    """
    return await watchlist_service.create_watchlist(session, principal, body)


@router.delete(
    "/watchlists/{watchlist_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    summary="Delete a watchlist",
    responses={404: {"model": ErrorResponse, "description": "watchlist_not_found"}},
)
async def delete_watchlist(
    watchlist_id: WatchlistIdPath,
    session: SessionDep,
    principal: PrincipalDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> None:
    """Delete one of the caller's watchlists with its items; answers 204 with an empty body.

    A watchlist that is missing or owned by another subject gives 404 ``watchlist_not_found``, so
    a repeated delete gives 404.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and nothing is deleted.
    """
    await watchlist_service.delete_watchlist(session, principal, watchlist_id)


@router.put(
    "/watchlists/{watchlist_id}/items/{ticker}",
    response_model=Watchlist,
    summary="Add a ticker (idempotent)",
    responses={
        404: {"model": ErrorResponse, "description": "watchlist_not_found or unknown_ticker"}
    },
)
async def add_watchlist_item(
    watchlist_id: WatchlistIdPath,
    ticker: TickerPath,
    session: SessionDep,
    principal: PrincipalDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> Watchlist:
    """Add a ticker to one of the caller's watchlists and return the watchlist, tickers sorted.

    Idempotent: adding a ticker already on the list answers 200 with the list unchanged. Gives 404
    ``watchlist_not_found`` for a list that is missing or another subject's, then 404
    ``unknown_ticker`` for a ticker that is not a known equity.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and the list is not changed.
    """
    return await watchlist_service.add_item(session, principal, watchlist_id, ticker)


@router.delete(
    "/watchlists/{watchlist_id}/items/{ticker}",
    response_model=Watchlist,
    summary="Remove a ticker (idempotent)",
    responses={
        404: {"model": ErrorResponse, "description": "watchlist_not_found or unknown_ticker"}
    },
)
async def remove_watchlist_item(
    watchlist_id: WatchlistIdPath,
    ticker: TickerPath,
    session: SessionDep,
    principal: PrincipalDep,
    params: Annotated[NoQueryParams, Query()] = _NO_QUERY_PARAMS,
) -> Watchlist:
    """Remove a ticker from one of the caller's watchlists and return it, tickers sorted.

    Idempotent: removing a ticker that is not on the list answers 200 with the list unchanged.
    Gives 404 ``watchlist_not_found`` for a list that is missing or another subject's, then 404
    ``unknown_ticker`` for a ticker that is not a known equity.

    The endpoint takes no query parameter: ``params`` carries none and is not read, and any query
    parameter, for example ``?foo=1``, is FastAPI's standard 422 list with one
    ``extra_forbidden`` entry per unknown name, and the list is not changed.
    """
    return await watchlist_service.remove_item(session, principal, watchlist_id, ticker)
