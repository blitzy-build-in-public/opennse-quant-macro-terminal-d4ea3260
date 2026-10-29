"""File-based ingestion: corporate PDFs and operator-supplied CSV/Excel files.

- ``pdf_line_items`` extracts Revenue, EBITDA, Net Income, Total Debt, operating Cash Flow and
  Shares Outstanding from corporate filing and IPO prospectus PDFs with pdfplumber. It is pure and
  synchronous; ``services/filings_service.py`` runs it in a thread pool for
  ``POST /api/v1/terminal/ingest/pdf``.
- ``tabular_import`` backs ``opennse-terminal import <kind> <path> [--sheet NAME]``, importing
  CSV/Excel equities, daily prices, fundamentals, macro indicators and foreign flows into the
  ``terminal_`` tables through the shared pipeline framework.

Import each submodule explicitly. This package re-exports nothing, so loading one submodule never
pulls in the other's heavy dependency (pdfplumber or pandas).
"""

__all__: list[str] = []
