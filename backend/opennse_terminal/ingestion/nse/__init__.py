"""NSE daily equity prices, volumes and turnover pipeline (``market_statistics.py``).

Its only source is the public page https://www.nse.co.ke/market-statistics/ ("Equity
Statistics" tab), rendered with Playwright and parsed with BeautifulSoup4. Per the NSE terms,
data from that page is for personal or internal business use only unless the operator is
licensed to distribute it.
"""

__all__: list[str] = []
