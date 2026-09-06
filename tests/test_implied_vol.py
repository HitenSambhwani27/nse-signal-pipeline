"""Black-Scholes IV solver. Calculated only; null when inputs are unusable."""

from __future__ import annotations

from nse_pipeline.market.implied_vol import (
    black_scholes_price,
    implied_volatility,
    time_to_expiry_years,
)

AS_OF = "2026-09-01T03:45:00+00:00"
EXPIRY = "2026-12-31"


def _price(*, option_type: str, vol: float = 0.20, spot: float = 100.0, strike: float = 100.0) -> float:
    tte = time_to_expiry_years(EXPIRY, AS_OF)
    assert tte is not None
    px = black_scholes_price(
        spot=spot,
        strike=strike,
        time_to_expiry_years=tte,
        rate=0.06,
        volatility=vol,
        option_type=option_type,
    )
    assert px is not None
    return px


def test_index_european_call_recovers_sigma() -> None:
    price = _price(option_type="CE", vol=0.20)
    out = implied_volatility(
        option_price=price,
        spot=100,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="NIFTY",
    )
    assert out["iv_status"] == "ok"
    assert out["iv_source"] == "calculated"
    assert out["iv_kind"] == "derived"
    assert out["iv_model"] == "black_scholes"
    assert out["iv_rate"] == 0.06
    assert out["iv_dividend_yield"] == 0.0
    assert out["iv_exercise_style"] == "european"
    assert out["iv"] is not None
    assert abs(out["iv"] - 0.20) < 1e-4
    assert abs(out["iv_pct"] - 20.0) < 1e-2


def test_index_european_put_recovers_sigma() -> None:
    price = _price(option_type="PE", vol=0.25)
    out = implied_volatility(
        option_price=price,
        spot=100,
        strike=100,
        expiry=EXPIRY,
        option_type="PE",
        as_of=AS_OF,
        underlying="BANKNIFTY",
    )
    assert out["iv_status"] == "ok"
    assert out["iv"] is not None
    assert abs(out["iv"] - 0.25) < 1e-4


def test_stock_option_unsupported_exercise_style() -> None:
    price = _price(option_type="CE", vol=0.20)
    out = implied_volatility(
        option_price=price,
        spot=100,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="HDFCBANK",
    )
    assert out["iv"] is None
    assert out["iv_status"] == "unsupported_exercise_style"
    assert out["iv_reason"] == "unsupported_exercise_style"
    assert out["iv_exercise_style"] == "european"
    assert out["iv_model"] == "black_scholes"


def test_missing_underlying_spot_returns_null() -> None:
    out = implied_volatility(
        option_price=2.0,
        spot=None,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="NIFTY",
    )
    assert out["iv"] is None
    assert out["iv_reason"] == "missing_underlying"
    assert out["iv_rate"] == 0.06
    assert out["iv_dividend_yield"] == 0.0


def test_missing_price_returns_null() -> None:
    out = implied_volatility(
        option_price=None,
        spot=100,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="NIFTY",
    )
    assert out["iv"] is None
    assert out["iv_reason"] == "missing_price"


def test_invalid_price_below_intrinsic() -> None:
    out = implied_volatility(
        option_price=0.01,
        spot=200,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="NIFTY",
    )
    assert out["iv"] is None
    assert out["iv_reason"] == "invalid_price"


def test_solver_failure_near_spot_bound() -> None:
    out = implied_volatility(
        option_price=99.9,
        spot=100,
        strike=100,
        expiry=EXPIRY,
        option_type="CE",
        as_of=AS_OF,
        underlying="NIFTY",
    )
    assert out["iv"] is None
    assert out["iv_reason"] == "solver_failed"
    assert out["iv_source"] is None
