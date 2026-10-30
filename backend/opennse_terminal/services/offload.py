"""Pure computation of the analytic services, run in a worker thread and stopped when abandoned.

The ``quant`` formulas are pure ``Decimal`` Python and CPU-bound: a rolling Z-score over 250
sessions, a 15-year valuation band or an eight-event backtest takes from milliseconds to
seconds. Run inline in a service coroutine, that work would hold the event loop, and every other
request on the process (a ``GET /health`` included) would wait for it. :func:`compute` hands one
chunk of pure work to Starlette's threadpool (``run_in_threadpool``, an AnyIO worker thread), so
the event loop keeps serving other requests while the chunk runs.

Only pure work goes to the thread. An ``AsyncSession`` is not thread-safe, so a service performs
every database read on the event loop first and passes the plain rows it read to the chunk.

A worker thread cannot be stopped once started, so stopping is cooperative. Before each chunk,
:func:`compute` awaits the caller's :data:`AbandonCheck`, which a route supplies as
``request.is_disconnected``, and raises :class:`RequestAbandonedError` instead of starting the
chunk when the client has gone away. A service splits long work into chunks (one per Z-score
series, one per backtest cycle), so an abandoned request stops within one chunk. The route
answers it with :data:`CLIENT_CLOSED_REQUEST`, which the server discards because nobody is
listening.

Python's global interpreter lock still lets only one thread of a process run Python code at a
time: the offload keeps the event loop responsive, while CPU-bound throughput grows with uvicorn
worker processes (``--workers N``), which are safe because the API is stateless.

The module registers nothing global and never imports ``opennse_terminal.api``.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Final, ParamSpec, TypeVar

from starlette.concurrency import run_in_threadpool

__all__ = ["CLIENT_CLOSED_REQUEST", "AbandonCheck", "RequestAbandonedError", "compute"]

logger = logging.getLogger("opennse_terminal.services.offload")

_P = ParamSpec("_P")
_T = TypeVar("_T")

AbandonCheck = Callable[[], Awaitable[bool]]
"""An awaitable probe that returns ``True`` once the requesting client has gone away.

Routes pass ``request.is_disconnected``: it never blocks, and reports the client's disconnect
once the server has seen it."""

CLIENT_CLOSED_REQUEST: Final = 499
"""Status a route answers a :class:`RequestAbandonedError` request with: "client closed request".

The client has disconnected, so the server discards the response and nobody receives it; the
code only labels the request in server logs."""


class RequestAbandonedError(Exception):
    """The requesting client went away, so the service stopped before its next chunk of work.

    Raised by :func:`compute` instead of starting a chunk. The work already done is discarded;
    the route answers :data:`CLIENT_CLOSED_REQUEST`.
    """


async def compute(
    abandoned: AbandonCheck | None,
    func: Callable[_P, _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    """Return ``func(*args, **kwargs)`` computed in a worker thread, unless the client has left.

    ``abandoned`` is awaited first, on the event loop. When it returns ``True``,
    :class:`RequestAbandonedError` is raised and ``func`` is never called; with ``None`` the
    check is skipped. Otherwise ``func`` runs to completion in Starlette's threadpool while the
    event loop serves other requests, and its result or exception is returned or raised
    unchanged. A thread cannot be interrupted, so a cancellation of the awaiting task never stops
    a ``func`` that has started: the chunk runs to its end.

    ``func`` must be pure computation: it must not touch an ``AsyncSession`` or any other
    object bound to the event loop. Positional-only ``abandoned`` and ``func`` leave every
    keyword name free for ``func``'s own arguments.

    Raises:
        RequestAbandonedError: When ``abandoned`` reports that the client went away.
    """
    if abandoned is not None and await abandoned():
        logger.debug(
            "client went away; %s not started", getattr(func, "__qualname__", type(func).__name__)
        )
        raise RequestAbandonedError
    return await run_in_threadpool(func, *args, **kwargs)
