"""Options Greeks/DTE/VWAP and futures VWAP/live L2 must land in feature_log."""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pandas as pd

from nse_pipeline.features.batch import _stamp_source
from nse_pipeline.features.futures import bucket_futures_series, compute_futures_features
from nse_pipeline.features.greeks import compute_greeks
from nse_pipeline.features.options import bucket_option_series, compute_option_features_for_symbol
from nse_pipeline.features.tick_stats import add_session_vwap, depth_features_by_bucket


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def test_bs_atm_call_delta_matches_closed_form() -> None:
    # S=K=100, T=1y, σ=0.20, r=0 → d1 = 0.10, Δ = Φ(0.10)
    g = compute_greeks(
        spot=100.0,
        strike=100.0,
        time_to_expiry_years=1.0,
        volatility=0.20,
        rate=0.0,
        option_type="CE",
    )
    assert g["stub"] is False
    assert abs(g["delta"] - _norm_cdf(0.10)) < 1e-9
    assert g["gamma"] > 0
    put = compute_greeks(
        spot=100.0,
        strike=100.0,
        time_to_expiry_years=1.0,
        volatility=0.20,
        rate=0.0,
        option_type="PE",
    )
    assert abs(g["delta"] - put["delta"] - 1.0) < 1e-9
    assert put["delta"] < 0


def test_options_write_dte_vwap_and_top_level_greeks() -> None:
    base = datetime(2026, 8, 12, 4, 0, tzinfo=timezone.utc)
    ticks = pd.DataFrame(
        [
            {
                "timestamp": base,
                "last_price": 120.0,
                "volume": 10,
                "last_quantity": 10,
                "oi": 1000,
            },
            {
                "timestamp": base.replace(minute=1),
                "last_price": 122.0,
                "volume": 25,
                "last_quantity": 15,
                "oi": 1010,
            },
        ]
    )
    ticks = add_session_vwap(ticks, source="live")
    buckets = bucket_option_series(ticks, bucket_minutes=5)
    spot_ts = pd.Timestamp(buckets.iloc[0]["bucket"])
    rows = compute_option_features_for_symbol(
        buckets,
        symbol="NIFTY26AUG24500CE",
        underlying="NIFTY",
        option_type="CE",
        strike=24500.0,
        expiry="2026-08-18",
        lot_size=65,
        spot_by_bucket={spot_ts: 24500.0},
        pcr_by_bucket={},
        bucket_minutes=5,
    )
    assert len(rows) == 1
    feats = rows[0]["features"]
    assert feats["days_to_expiry"] == 6
    assert feats["vwap"] is not None
    assert feats["vwap_deviation_bps"] is not None
    assert feats["delta"] is not None
    assert feats["gamma"] is not None
    assert 0.4 < feats["delta"] < 0.7
    assert feats["greeks"]["stub"] is False
    assert feats["greeks"]["iv_source"] == "assumed"


def test_options_greeks_survive_naive_spot_key() -> None:
    """Live 2026-08-12 had spot in JSON but greeks=None from the stub; keys must still match."""
    base = datetime(2026, 8, 12, 4, 2, tzinfo=timezone.utc)
    ticks = pd.DataFrame(
        [
            {
                "timestamp": base,
                "last_price": 80.0,
                "volume": 5,
                "last_quantity": 5,
                "oi": 500,
            }
        ]
    )
    ticks = add_session_vwap(ticks, source="live")
    buckets = bucket_option_series(ticks, bucket_minutes=5)
    aware = pd.Timestamp(buckets.iloc[0]["bucket"])
    naive = aware.tz_localize(None) if aware.tzinfo else aware
    rows = compute_option_features_for_symbol(
        buckets,
        symbol="NIFTY26AUG24500PE",
        underlying="NIFTY",
        option_type="PE",
        strike=24500.0,
        expiry="2026-08-18",
        lot_size=65,
        spot_by_bucket={naive: 24500.0},
        pcr_by_bucket={},
        bucket_minutes=5,
    )
    assert rows[0]["features"]["delta"] is not None
    assert rows[0]["features"]["delta"] < 0


def test_futures_live_writes_vwap_and_l2() -> None:
    base = datetime(2026, 8, 12, 4, 0, tzinfo=timezone.utc)
    ticks = pd.DataFrame(
        [
            {
                "timestamp": base,
                "last_price": 24510.0,
                "volume": 100,
                "last_quantity": 20,
                "oi": 1_000_000,
                "bid_prices": [24509.0],
                "bid_quantities": [50],
                "ask_prices": [24511.0],
                "ask_quantities": [40],
            },
            {
                "timestamp": base.replace(minute=1),
                "last_price": 24512.0,
                "volume": 130,
                "last_quantity": 30,
                "oi": 1_000_100,
                "bid_prices": [24511.0],
                "bid_quantities": [60],
                "ask_prices": [24513.0],
                "ask_quantities": [35],
            },
        ]
    )
    ticks = add_session_vwap(ticks, source="live")
    buckets = bucket_futures_series(ticks, bucket_minutes=5)
    depth = depth_features_by_bucket(ticks, bucket_minutes=5)
    rows = compute_futures_features(
        buckets,
        symbol="NIFTY26AUGFUT",
        underlying="NIFTY",
        expiry="2026-08-27",
        lot_size=65,
        spot_by_bucket={pd.Timestamp(buckets.iloc[0]["bucket"]): 24500.0},
        near_ltp_by_bucket=None,
        next_ltp_by_bucket=None,
        is_near=True,
        bucket_minutes=5,
        basis_days_per_year=365,
        depth_by_bucket=depth,
    )
    _stamp_source(rows, "live")
    feats = rows[0]["features"]
    assert feats["vwap"] is not None
    assert feats["vwap_deviation_bps"] is not None
    assert feats["spread_bps"] is not None
    assert feats["depth_ratio_bid_ask"] is not None
    assert "ofi_bucket_sum" in feats
    assert feats.get("depth_features_skipped") is None


def test_futures_historical_skips_l2_keeps_vwap() -> None:
    base = datetime(2026, 8, 12, 4, 0, tzinfo=timezone.utc)
    ticks = pd.DataFrame(
        [
            {
                "timestamp": base,
                "last_price": 24510.0,
                "volume": 100,
                "last_quantity": 0,
                "oi": 1_000_000,
                "high": 24520.0,
                "low": 24500.0,
                "close": 24510.0,
                "bid_prices": [],
                "bid_quantities": [],
                "ask_prices": [],
                "ask_quantities": [],
                "source": "historical",
            }
        ]
    )
    ticks = add_session_vwap(ticks, source="historical")
    buckets = bucket_futures_series(ticks, bucket_minutes=5)
    rows = compute_futures_features(
        buckets,
        symbol="NIFTY26AUGFUT",
        underlying="NIFTY",
        expiry="2026-08-27",
        lot_size=65,
        spot_by_bucket={pd.Timestamp(buckets.iloc[0]["bucket"]): 24500.0},
        near_ltp_by_bucket=None,
        next_ltp_by_bucket=None,
        is_near=True,
        bucket_minutes=5,
        basis_days_per_year=365,
        depth_by_bucket=None,
    )
    _stamp_source(rows, "historical")
    feats = rows[0]["features"]
    assert feats["vwap"] is not None
    assert feats["days_to_expiry"] == 15
    assert "ofi_bucket_sum" not in feats
    assert "spread_bps" not in feats
    assert "depth_features_skipped" in feats
