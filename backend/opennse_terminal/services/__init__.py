"""Services layer of the terminal (``api -> services -> quant``).

Each service module backs one group of routes, in one of these roles:

- ``macro_service``, ``market_service`` and ``cycle_service`` query PostgreSQL through SQLAlchemy
  2.0 async sessions, call the pure ``opennse_terminal.quant`` functions, round each output value
  once with ``quant.numeric.quantize_out`` and return ``opennse_terminal.schemas`` models.
- ``ipo_service`` lists the read-only IPO presets stored in the database, and values an offer
  with the pure ``quant.dcf`` formulas without reading or writing the database; both round each
  output value once with ``quantize_out``.
- ``filings_service`` runs the pdfplumber-based extractor of ``ingestion.files.pdf_line_items``
  in a worker thread, records an ingestion run, upserts the filing and, with ``commit``, merges
  its line items into the fundamentals, rounding each value once with ``quantize_out``.
- ``watchlist_service`` reads and writes the owner-scoped watchlist tables; it calls no quant
  function and rounds nothing.
- ``health_service`` only probes the database with ``SELECT 1`` and computes nothing.

``offload`` is the helper ``market_service`` and ``cycle_service`` share: it runs a chunk of pure
computation in a worker thread, so the event loop keeps serving other requests, and stops before
the next chunk once the requesting client has gone away.

The package is an empty barrel (``__all__ = []``): importing it eagerly imports no service
submodule, so it neither loads pdfplumber through ``filings_service`` nor creates an import cycle
with ``opennse_terminal.api``. Callers import a submodule explicitly
(``from opennse_terminal.services import market_service``). Services may import one another's
helpers, so loading one service can load others: ``cycle_service`` reuses the loaders of
``market_service``, which reuses the watchlist lookup of ``watchlist_service``. Importing the
package has no side effects.
"""

__all__: list[str] = []
