"""File-based ingestion: corporate PDFs and operator-supplied CSV/Excel files.

- ``pdf_line_items`` extracts Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and
  Shares Outstanding from corporate filing and IPO prospectus PDFs with pdfplumber. It parses in a
  separate, time- and memory-limited Python process and blocks the calling thread until that
  process ends, so ``services/filings_service.py`` runs it in a thread pool for
  ``POST /api/v1/terminal/ingest/pdf``. Its ``iter_page_text`` streams the first pages' text
  layer from that same limited process, page by page as it is read, for the ``cbk_reserves``
  pipeline's Weekly Bulletin PDFs, whose parse stage already runs in a worker thread; the
  pipeline closes it at the reserves table, which kills the process, so later pages never decide
  the outcome.
- ``tabular_import`` backs ``opennse-terminal import <kind> <path> [--sheet NAME]``, importing
  CSV/Excel equities, daily prices, fundamentals, macro indicators and foreign flows into the
  ``terminal_`` tables through the shared pipeline framework.

Import each submodule explicitly. This package re-exports nothing, so loading one submodule never
pulls in the other's heavy dependency (pdfplumber or pandas).
"""

__all__: list[str] = []
