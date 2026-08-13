"""Stage 2A equity feature tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from nse_pipeline.features.equity import compute_equity_features, compute_ofi


def test_ofi_bid_up_ask_down() -> None:
    ofi = compute_ofi(
        bid_price_prev=100.0,
        bid_qty_prev=10.0,
        ask_price_prev=101.0,
        ask_qty_prev=10.0,
        bid_price=100.5,
        bid_qty=20.0,
        ask_price=100.8,
        ask_qty=15.0,
    )
    # bid up → +bid_qty; ask down → +ask_qty; ofi = bid_ofi - ask_ofi
    assert ofi == 20.0 - 15.0


def test_depth_features_have_spread_and_ofi() -> None:
    rows = []
    base = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    for i in range(3):
        rows.append(
            {
                "timestamp": base.replace(minute=i),
                "last_price": 100.0 + i,
                "volume": 100 + i * 10,
                "last_quantity": 10,
                "bid_prices": [99.0, 98.9],
                "bid_quantities": [50, 40],
                "ask_prices": [101.0, 101.1],
                "ask_quantities": [40, 30],
                "oi": None,
            }
        )
    df = pd.DataFrame(rows)
    feats = compute_equity_features(
        df, symbol="ABB", subscribe_mode="full", bucket_minutes=5, lot_size=1
    )
    assert len(feats) >= 1
    f0 = feats[0]["features"]
    assert feats[0]["track"] == "equity_depth"
    assert f0["spread_abs"] is not None
    assert f0["depth_ratio_bid_ask"] is not None
    assert "ofi_bucket_sum" in f0


def test_quote_features_omit_depth() -> None:
    rows = [
        {
            "timestamp": datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc),
            "last_price": 50.0,
            "volume": 1000,
            "last_quantity": 5,
            "bid_prices": [],
            "bid_quantities": [],
            "ask_prices": [],
            "ask_quantities": [],
            "oi": None,
        }
    ]
    feats = compute_equity_features(
        pd.DataFrame(rows), symbol="AARTIIND", subscribe_mode="quote", bucket_minutes=5
    )
    assert feats[0]["track"] == "equity_quote"
    assert feats[0]["features"]["spread_abs"] is None
    assert feats[0]["features"]["ofi_bucket_sum"] is None
    assert feats[0]["features"]["quote_only"] is True
