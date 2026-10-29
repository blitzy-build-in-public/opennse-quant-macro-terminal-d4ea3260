"""Services layer of the terminal (``api -> services -> quant``).

Each service queries PostgreSQL through SQLAlchemy 2.0 async sessions, calls the pure
``opennse_terminal.quant`` functions, rounds once with ``quant.numeric.quantize_out`` and returns
``opennse_terminal.schemas`` models. The modules are ``health_service``, ``macro_service``,
``market_service``, ``cycle_service``, ``ipo_service``, ``filings_service`` and
``watchlist_service``; ``health_service`` only probes the database with ``SELECT 1`` and computes
nothing.

Callers import a submodule explicitly (``from opennse_terminal.services import market_service``).
This package imports none of them, so loading one service never loads the others: in particular
``filings_service`` pulls in pdfplumber, and a re-exporting barrel here could create import cycles
with ``opennse_terminal.api``. Importing the package has no side effects.
"""

__all__: list[str] = []
