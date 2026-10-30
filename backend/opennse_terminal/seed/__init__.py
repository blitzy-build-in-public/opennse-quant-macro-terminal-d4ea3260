"""Deterministic fallback seeder for the OpenNSE terminal (``terminal_`` tables only).

Run with ``python -m opennse_terminal.seed.seed_terminal_db`` or ``opennse-terminal seed``.

This package marker imports nothing: ``python -m`` imports it before running
``seed_terminal_db``, so re-exporting that module here would import it twice and make
``runpy`` warn.
"""
