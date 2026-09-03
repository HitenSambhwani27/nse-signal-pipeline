"""Black-Scholes Greeks for index options.

IV is the caller-supplied input (currently an assumed 0.15 until a market-IV
solver exists). Delta/Gamma/Theta/Vega/Rho are the closed-form BS values at
that IV — not placeholders.
"""

from __future__ import annotations

import math
from typing import Any


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


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
    Black-Scholes Delta/Gamma/Theta/Vega/Rho at the given IV.

    `option_type` is 'CE' or 'PE'. Theta is per calendar year of price;
    Vega is dPrice/dσ for σ in decimal (not per vol-point).
    """
    kind = str(option_type or "").upper()
    is_call = kind in {"CE", "C", "CALL"}
    payload: dict[str, Any] = {
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
        "option_type": kind,
        "stub": False,
        "iv_source": "assumed",
    }
    if (
        spot is None
        or strike is None
        or spot <= 0
        or strike <= 0
        or time_to_expiry_years is None
        or time_to_expiry_years <= 0
        or volatility is None
        or volatility <= 0
    ):
        payload["note"] = "BS skipped: non-positive spot/strike/T/vol"
        return payload

    sqrt_t = math.sqrt(time_to_expiry_years)
    sigma_sqrt_t = volatility * sqrt_t
    d1 = (
        math.log(spot / strike)
        + (rate + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    nd1 = _norm_cdf(d1)
    nd2 = _norm_cdf(d2)
    pdf = _norm_pdf(d1)
    disc = math.exp(-rate * time_to_expiry_years)

    if is_call:
        delta = nd1
        theta = (
            -spot * pdf * volatility / (2.0 * sqrt_t)
            - rate * strike * disc * nd2
        )
        rho = strike * time_to_expiry_years * disc * nd2
    else:
        delta = nd1 - 1.0
        theta = (
            -spot * pdf * volatility / (2.0 * sqrt_t)
            + rate * strike * disc * _norm_cdf(-d2)
        )
        rho = -strike * time_to_expiry_years * disc * _norm_cdf(-d2)

    payload["delta"] = float(delta)
    payload["gamma"] = float(pdf / (spot * sigma_sqrt_t))
    payload["theta"] = float(theta)
    payload["vega"] = float(spot * pdf * sqrt_t)
    payload["rho"] = float(rho)
    return payload
