"""Ingestion pipelines that load public sources into the ``terminal_`` tables.

``base``, ``http``, ``calendar``, ``registry`` and ``scheduler`` are the shared framework, ``data``
holds exchange-closure dates, and the sources are ``nse`` (the public NSE market-statistics web
table, Playwright and BeautifulSoup4), ``cbk`` (CBK statistical releases: httpx, BeautifulSoup4,
pandas, pdfplumber) and ``files`` (PDF corporate filings via pdfplumber, CSV/Excel via pandas and
openpyxl). Depends only on ``opennse_terminal.config``, ``.db`` and ``.models``, never on ``api``,
``services``, ``schemas`` or ``quant``. Units: ``*_pct`` percentage points, ``*_kes`` whole KES,
volumes and share counts integers, ``fx_kes_usd`` KES per USD, ``fx_reserves_usd_m`` USD millions.
Imports and re-exports nothing, so each pipeline's libraries load only with its own module.
"""

__all__: list[str] = []
