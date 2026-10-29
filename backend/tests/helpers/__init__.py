"""Importable helpers for the opennse_terminal test suite.

pdf_builder writes minimal, deterministic PDF 1.4 text documents (Helvetica, cells at exact
x-positions) so the pdfplumber line-item parser is tested without committing any third-party report.

sessions builds synthetic market-session calendars (plain Monday-Friday dates, no holiday logic)
for the pure quant tests, which take the observed market sessions they are given.

routing_session provides a host's custom routing SQLAlchemy ``Session`` (a ``get_bind`` override
with no default bind), for the tests of how the terminal resolves its database bind.
"""
