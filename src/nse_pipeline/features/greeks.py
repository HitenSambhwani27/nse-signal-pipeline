"""
Placeholder Greeks interface for Stage 2 options features.

Replace the body of `compute_greeks` with your Phase 1 formulas / Black-Scholes
implementation. Callers must pass spot from the live index feed (NIFTY 50 /
NIFTY BANK), never a hardcoded level.
"""

from __future__ import annotations

from typing import Any


def compute_greeks(
    *,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    volatility: float,
    rate: float = 0.06,
    option_type: str,
) -> dict[str, Any]:
    """
    Return Delta/Gamma/Theta/Vega/Rho stubs.

    Parameters mirror a standard BS surface so your real solver can drop in.
    `option_type` is 'CE' or 'PE'.
    """
    # Explicit stub — do not invent fake Greeks that look real.
    return {
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "rho": None,
        "iv_input": volatility,
        "spot": spot,
        "strike": strike,
        "tte_years": time_to_expiry_years,
        "rate": rate,
        "option_type": option_type,
        "stub": True,
        "note": "Plug real greeks.py formulas here before trusting risk numbers.",
    }
