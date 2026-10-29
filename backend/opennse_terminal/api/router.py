"""``terminal_router``: the single ``APIRouter`` that carries every terminal endpoint.

A host mounts the whole API with one line, and the standalone app in ``main.py`` does the same::

    from opennse_terminal.api import terminal_router

    app.include_router(terminal_router)

Every path lives under :data:`TERMINAL_API_PREFIX`. The router declares
``dependencies=[Depends(get_terminal_principal)]``, so a host that overrides
``get_terminal_principal`` (mapping its user to a subject, or raising ``HTTPException(401)``)
protects every terminal endpoint, ``/health`` included. FastAPI resolves that dependency once
per request, also for routes that declare ``PrincipalDep`` themselves.

Operations, relative to the prefix; each ``operationId`` is ``terminal_<function name>``:

==========  ====================================================  =========================
Module      Method and path                                       Endpoint function
==========  ====================================================  =========================
health      ``GET /health``                                       ``get_health``
macro       ``GET /macro/overview``                               ``get_macro_overview``
market      ``GET /market/screener``                              ``get_screener``
market      ``GET /market/sector-heatmap``                        ``get_sector_heatmap``
market      ``GET /market/valuation-bands``                       ``get_valuation_bands``
market      ``GET /market/anomalies``                             ``get_anomalies``
market      ``GET /market/flows``                                 ``get_flows``
cycles      ``GET /cycles/backtest``                              ``get_cycle_backtest``
ipo         ``GET /ipo/scenarios``                                ``list_ipo_scenarios``
ipo         ``POST /ipo/valuation``                               ``value_ipo``
ingest      ``POST /ingest/pdf``                                  ``ingest_pdf``
watchlists  ``GET /watchlists``                                   ``list_watchlists``
watchlists  ``POST /watchlists``                                  ``create_watchlist``
watchlists  ``DELETE /watchlists/{watchlist_id}``                 ``delete_watchlist``
watchlists  ``PUT /watchlists/{watchlist_id}/items/{ticker}``     ``add_watchlist_item``
watchlists  ``DELETE /watchlists/{watchlist_id}/items/{ticker}``  ``remove_watchlist_item``
==========  ====================================================  =========================

Collision invariants a host relies on:

* Every path starts with ``/api/v1/terminal/``, so terminal routes cannot collide with a host's
  own routes outside that prefix.
* Every ``operationId`` starts with ``terminal_`` (:func:`terminal_unique_id`), so a host's
  OpenAPI schema has no duplicate IDs. Endpoint function names are unique across the
  sub-routers; importing this module raises ``RuntimeError`` when two would share an ID.
* The sub-routers are included as their modules define them, with no prefix, tags,
  dependencies or route class added. ``include_router`` keeps each route's class, so the ingest
  router's ``TerminalUploadRoute`` (the whole-request 413 guard) and the ``TerminalJsonRoute``
  body decoding of the IPO and watchlist routers travel with ``terminal_router`` into a host.
* The router registers no middleware, exception handlers, startup, shutdown or lifespan hooks,
  CORS settings or other global state, none of which a router can carry into a host. Routes and
  services raise ``HTTPException(status, detail={"code", "message", "field"})`` instead.

``opennse_terminal.api`` re-exports this module's router while that package is initialising, so
this module imports its dependencies from the ``deps`` and ``routes`` submodules, never from the
``opennse_terminal.api`` barrel, which would be a circular import.
"""

from collections.abc import Iterable
from typing import Final

from fastapi import APIRouter, Depends
from fastapi.routing import APIRoute

from opennse_terminal.api.deps import get_terminal_principal
from opennse_terminal.api.routes import ALL_ROUTERS

__all__ = ["TERMINAL_API_PREFIX", "terminal_router", "terminal_unique_id"]

TERMINAL_API_PREFIX: Final[str] = "/api/v1/terminal"
"""Path prefix of every terminal endpoint, in standalone mode and inside a host alike."""


def terminal_unique_id(route: APIRoute) -> str:
    """Return the OpenAPI ``operationId`` of a terminal route: ``terminal_<endpoint name>``.

    The ``terminal_`` prefix keeps every terminal ``operationId`` distinct from a host's own, so
    a host's merged schema has no duplicates. ``route.name`` is the endpoint function name, which
    is unique across the terminal sub-routers (checked when this module is imported).

    It is the router's ``generate_unique_id_function``. The sub-routers keep FastAPI's default,
    so this function applies to all of their routes and still applies after a host's own
    ``include_router(terminal_router)``.
    """
    return f"terminal_{route.name}"


def _check_unique_operation_ids(routers: Iterable[APIRouter]) -> None:
    """Raise ``RuntimeError`` if two routes of ``routers`` would get the same ``operationId``.

    The walk reads each sub-router's own routes rather than ``terminal_router.routes``: FastAPI
    records an included router as a single entry there, not as the routes it contains. An
    explicit ``operation_id`` on a route takes precedence, as it does in FastAPI.
    """
    owners: dict[str, str] = {}
    for router in routers:
        for route in router.routes:
            if not isinstance(route, APIRoute):
                continue
            operation_id = route.operation_id or terminal_unique_id(route)
            owner = f"{' '.join(sorted(route.methods or ()))} {route.path}"
            previous = owners.get(operation_id)
            if previous is not None:
                raise RuntimeError(
                    f"Terminal operationId {operation_id!r} is shared by {previous} and "
                    f"{owner}; endpoint function names must be unique across the sub-routers"
                )
            owners[operation_id] = owner


def _build_terminal_router() -> APIRouter:
    """Build the prefixed router and include every sub-router of ``ALL_ROUTERS`` in order.

    ``generate_unique_id_function`` is set when the router is constructed, before any
    sub-router is included, so every included route takes :func:`terminal_unique_id`.
    """
    _check_unique_operation_ids(ALL_ROUTERS)
    router = APIRouter(
        prefix=TERMINAL_API_PREFIX,
        tags=["terminal"],
        dependencies=[Depends(get_terminal_principal)],
        generate_unique_id_function=terminal_unique_id,
    )
    for sub_router in ALL_ROUTERS:
        router.include_router(sub_router)
    return router


terminal_router = _build_terminal_router()
"""The mountable terminal API: ``app.include_router(terminal_router)`` adds all 16 operations."""
