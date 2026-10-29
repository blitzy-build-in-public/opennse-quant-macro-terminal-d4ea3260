"""Quantitative engine of the terminal: the single ``Decimal`` implementation of every formula.

Every function is pure: ``Decimal`` (or ``None``) in and out, with no I/O, no database and no
HTTP, computed under the default ``decimal`` context, which this package never changes. Formulas
never round; services round each output value exactly once with :func:`quantize_out`
(ROUND_HALF_UP) while building response models. An undefined metric is ``None`` with a reason
code in ``flags``, never zero. Session counts (Z-score windows, rolling sums, coverage, cohort
anchors, heatmap boundaries, band sampling) use the observed market calendar that the caller
supplies, never a holiday list.

This facade re-exports the headline formulas. The input and result types, constants and helpers
live in the submodules (``numeric``, ``asof``, ``valuation``, ``bands``, ``zscore``, ``flows``,
``heatmap``, ``backtest``, ``dcf``) and are imported from there.
"""

from opennse_terminal.quant.backtest import run_cycle_backtest
from opennse_terminal.quant.bands import valuation_bands
from opennse_terminal.quant.dcf import blend, run_comparables, run_dcf, sensitivity_matrix
from opennse_terminal.quant.flows import net_flow_series
from opennse_terminal.quant.heatmap import sector_returns
from opennse_terminal.quant.numeric import quantize_out
from opennse_terminal.quant.valuation import (
    dividend_yield_pct,
    enterprise_value,
    ev_ebitda,
    market_cap,
    pb_ratio,
    pe_ratio,
)
from opennse_terminal.quant.zscore import rolling_zscores

__all__ = [
    "blend",
    "dividend_yield_pct",
    "enterprise_value",
    "ev_ebitda",
    "market_cap",
    "net_flow_series",
    "pb_ratio",
    "pe_ratio",
    "quantize_out",
    "rolling_zscores",
    "run_comparables",
    "run_cycle_backtest",
    "run_dcf",
    "sector_returns",
    "sensitivity_matrix",
    "valuation_bands",
]
