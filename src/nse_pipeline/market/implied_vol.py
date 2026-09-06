"""Black-Scholes implied volatility from observed option LTP.

IV is calculated, never observed from Kite. Missing or invalid inputs return null.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
DAYS_PER_YEAR = 365.0
DEFAULT_RATE = 0.06
DEFAULT_DIVIDEND_YIELD = 0.0
EUROPEAN_INDEX_UNDERLYINGS = frozenset({"NIFTY", "BANKNIFTY"})
_MAX_SIGMA = 5.0
_MIN_SIGMA = 1e-6
_PRICE_TOL = 1e-6
_VEGA_FLOOR = 1e-12


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _parse_as_of(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_expiry(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def time_to_expiry_years(
    expiry: Any,
    as_of: Any = None,
    *,
    close_hhmm: str = "15:30",
) -> float | None:
    """Years from as_of to expiry at NSE close (Asia/Kolkata)."""
    exp = _parse_expiry(expiry)
    if exp is None:
        return None
    hour, minute = 15, 30
    parts = str(close_hhmm).split(":")
    if len(parts) >= 2:
        try:
            hour, minute = int(parts[0]), int(parts[1])
        except ValueError:
            hour, minute = 15, 30
    expiry_dt = datetime.combine(exp, time(hour, minute), tzinfo=IST).astimezone(timezone.utc)
    now = _parse_as_of(as_of) or datetime.now(timezone.utc)
    seconds = (expiry_dt - now).total_seconds()
    if seconds <= 0:
        return None
    return seconds / (DAYS_PER_YEAR * 24.0 * 3600.0)


def _is_call(option_type: str) -> bool:
    kind = str(option_type or "").upper()
    return kind in {"CE", "C", "CALL"}


def black_scholes_price(
    *,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    rate: float,
    volatility: float,
    option_type: str,
) -> float | None:
    if (
        spot <= 0
        or strike <= 0
        or time_to_expiry_years <= 0
        or volatility <= 0
    ):
        return None
    sqrt_t = math.sqrt(time_to_expiry_years)
    sigma_sqrt_t = volatility * sqrt_t
    d1 = (
        math.log(spot / strike)
        + (rate + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    disc = math.exp(-rate * time_to_expiry_years)
    if _is_call(option_type):
        return spot * _norm_cdf(d1) - strike * disc * _norm_cdf(d2)
    return strike * disc * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def black_scholes_vega(
    *,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    rate: float,
    volatility: float,
) -> float | None:
    if spot <= 0 or strike <= 0 or time_to_expiry_years <= 0 or volatility <= 0:
        return None
    sqrt_t = math.sqrt(time_to_expiry_years)
    sigma_sqrt_t = volatility * sqrt_t
    d1 = (
        math.log(spot / strike)
        + (rate + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / sigma_sqrt_t
    return spot * _norm_pdf(d1) * sqrt_t


def theoretical_bounds(
    *,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    rate: float,
    option_type: str,
) -> tuple[float, float]:
    disc = math.exp(-rate * time_to_expiry_years)
    discounted_k = strike * disc
    if _is_call(option_type):
        return max(spot - discounted_k, 0.0), spot
    return max(discounted_k - spot, 0.0), discounted_k


def iv_assumptions(*, rate: float = DEFAULT_RATE) -> dict[str, Any]:
    """Documented European Black-Scholes assumptions. Always attached."""
    return {
        "iv_kind": "derived",
        "iv_model": "black_scholes",
        "iv_rate": float(rate),
        "iv_dividend_yield": DEFAULT_DIVIDEND_YIELD,
        "iv_exercise_style": "european",
    }


def _empty(reason: str, *, rate: float = DEFAULT_RATE, status: str = "unavailable") -> dict[str, Any]:
    return {
        "iv": None,
        "iv_pct": None,
        "iv_source": None,
        "iv_status": status,
        "iv_reason": reason,
        "rate": float(rate),
        "tte_years": None,
        **iv_assumptions(rate=rate),
    }


def implied_volatility(
    *,
    option_price: Any,
    spot: Any,
    strike: Any,
    expiry: Any,
    option_type: str,
    rate: float = DEFAULT_RATE,
    as_of: Any = None,
    close_hhmm: str = "15:30",
    underlying: str | None = None,
) -> dict[str, Any]:
    """
    Invert Black-Scholes for sigma.

    European BS at the documented rate and zero dividend yield. Only
    NIFTY/BANKNIFTY (European index options) are valued. Stock options
    return null with unsupported_exercise_style — no American model exists.

    Returns null IV when any required input is missing, the price is outside
    the theoretical range, or Newton/bisection cannot converge.
    """
    r = float(rate)
    name = str(underlying or "").strip().upper()
    if name and name not in EUROPEAN_INDEX_UNDERLYINGS:
        return _empty("unsupported_exercise_style", rate=r, status="unsupported_exercise_style")
    price = _f(option_price)
    s = _f(spot)
    k = _f(strike)
    if price is None:
        return _empty("missing_price", rate=r)
    if s is None:
        return _empty("missing_underlying", rate=r)
    if k is None:
        return _empty("missing_strike", rate=r)
    if price <= 0 or s <= 0 or k <= 0:
        return _empty("invalid_price", rate=r)
    tte = time_to_expiry_years(expiry, as_of, close_hhmm=close_hhmm)
    if tte is None:
        return _empty("missing_or_expired", rate=r)
    low, high = theoretical_bounds(
        spot=s,
        strike=k,
        time_to_expiry_years=tte,
        rate=r,
        option_type=option_type,
    )
    if price < low - 1e-8 or price > high + 1e-8:
        return {**_empty("invalid_price", rate=r), "tte_years": tte}

    sigma = 0.2
    solved: float | None = None
    for _ in range(40):
        model = black_scholes_price(
            spot=s,
            strike=k,
            time_to_expiry_years=tte,
            rate=r,
            volatility=sigma,
            option_type=option_type,
        )
        vega = black_scholes_vega(
            spot=s,
            strike=k,
            time_to_expiry_years=tte,
            rate=r,
            volatility=sigma,
        )
        if model is None or vega is None or vega < _VEGA_FLOOR:
            break
        diff = model - price
        if abs(diff) < _PRICE_TOL:
            solved = sigma
            break
        sigma = sigma - diff / vega
        if sigma <= _MIN_SIGMA or sigma >= _MAX_SIGMA:
            break

    if solved is None:
        lo, hi = _MIN_SIGMA, _MAX_SIGMA
        flo = black_scholes_price(
            spot=s, strike=k, time_to_expiry_years=tte, rate=r, volatility=lo, option_type=option_type
        )
        fhi = black_scholes_price(
            spot=s, strike=k, time_to_expiry_years=tte, rate=r, volatility=hi, option_type=option_type
        )
        if flo is None or fhi is None or (flo - price) * (fhi - price) > 0:
            return {**_empty("solver_failed", rate=r), "tte_years": tte}
        mid = 0.2
        for _ in range(80):
            mid = 0.5 * (lo + hi)
            fmid = black_scholes_price(
                spot=s,
                strike=k,
                time_to_expiry_years=tte,
                rate=r,
                volatility=mid,
                option_type=option_type,
            )
            if fmid is None:
                return {**_empty("solver_failed", rate=r), "tte_years": tte}
            if abs(fmid - price) < _PRICE_TOL:
                solved = mid
                break
            if (fmid - price) * (flo - price) <= 0:
                hi = mid
                fhi = fmid
            else:
                lo = mid
                flo = fmid
        else:
            solved = mid

    if solved is None or solved <= _MIN_SIGMA or solved >= _MAX_SIGMA:
        return {**_empty("solver_failed", rate=r), "tte_years": tte}
    return {
        "iv": float(solved),
        "iv_pct": float(solved) * 100.0,
        "iv_source": "calculated",
        "iv_status": "ok",
        "iv_reason": None,
        "rate": r,
        "tte_years": tte,
        **iv_assumptions(rate=r),
    }
