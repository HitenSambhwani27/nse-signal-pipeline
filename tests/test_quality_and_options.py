"""Stage 2B/2D quality gate + OI buildup tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from nse_pipeline.features.options import classify_oi_buildup
from nse_pipeline.features.quality import filter_bad_ticks


def test_oi_buildup_states() -> None:
    assert classify_oi_buildup(1, 1) == "long_buildup"
    assert classify_oi_buildup(-1, 1) == "short_buildup"
    assert classify_oi_buildup(-1, -1) == "long_unwinding"
    assert classify_oi_buildup(1, -1) == "short_covering"
    assert classify_oi_buildup(0, 0) == "neutral"


def test_options_jump_threshold_looser_than_equity() -> None:
    """A ~12% options move fails at 5% but passes at 25%."""
    ts0 = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 8, 13, 4, 0, 1, tzinfo=timezone.utc)
    df = pd.DataFrame(
        [
            {
                "timestamp": ts0,
                "last_price": 100.0,
                "volume": 1,
                "bid_prices": [],
                "ask_prices": [],
                "bid_quantities": [],
                "ask_quantities": [],
            },
            {
                "timestamp": ts1,
                "last_price": 112.0,
                "volume": 2,
                "bid_prices": [],
                "ask_prices": [],
                "bid_quantities": [],
                "ask_quantities": [],
            },
        ]
    )
    tight = filter_bad_ticks(
        df, symbol="OPT", subscribe_mode="full", max_tick_return_pct=5.0
    )
    loose = filter_bad_ticks(
        df, symbol="OPT", subscribe_mode="full", max_tick_return_pct=25.0
    )
    assert any(r.reason == "impossible_jump" for r in tight.rejects)
    assert len(loose.rejects) == 0
    assert len(loose.clean) == 2


def test_quality_rejects_zero_ltp_and_crossed_book() -> None:
    ts0 = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 8, 13, 4, 0, 1, tzinfo=timezone.utc)
    ts2 = datetime(2026, 8, 13, 4, 0, 2, tzinfo=timezone.utc)
    df = pd.DataFrame(
        [
            {
                "timestamp": ts0,
                "last_price": 0.0,
                "volume": 1,
                "bid_prices": [10],
                "ask_prices": [11],
                "bid_quantities": [1],
                "ask_quantities": [1],
            },
            {
                "timestamp": ts1,
                "last_price": 100.0,
                "volume": 1,
                "bid_prices": [101.0],
                "ask_prices": [100.0],
                "bid_quantities": [1],
                "ask_quantities": [1],
            },
            {
                "timestamp": ts2,
                "last_price": 100.5,
                "volume": 2,
                "bid_prices": [100.0],
                "ask_prices": [100.6],
                "bid_quantities": [1],
                "ask_quantities": [1],
            },
        ]
    )
    result = filter_bad_ticks(
        df,
        symbol="ABB",
        subscribe_mode="full",
        max_tick_return_pct=5.0,
        require_depth=True,
    )
    reasons = {r.reason for r in result.rejects}
    assert "null_or_nonpositive_ltp" in reasons
    assert "crossed_book" in reasons
    assert len(result.clean) == 1


def test_historical_source_skips_crossed_book() -> None:
    """2D historical OHLCV: jump + LTP checks apply; crossed-book does not."""
    ts0 = datetime(2026, 8, 13, 4, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 8, 13, 4, 1, tzinfo=timezone.utc)
    df = pd.DataFrame(
        [
            {
                "timestamp": ts0,
                "last_price": 100.0,
                "volume": 1,
                "bid_prices": [101.0],
                "ask_prices": [100.0],
                "bid_quantities": [1],
                "ask_quantities": [1],
            },
            {
                "timestamp": ts1,
                "last_price": 100.2,
                "volume": 2,
                "bid_prices": [101.0],
                "ask_prices": [100.0],
                "bid_quantities": [1],
                "ask_quantities": [1],
            },
        ]
    )
    result = filter_bad_ticks(
        df,
        symbol="RELIANCE",
        subscribe_mode="full",
        max_tick_return_pct=5.0,
        require_depth=True,
        source="historical",
    )
    assert all(r.reason != "crossed_book" for r in result.rejects)
    assert len(result.clean) == 2
