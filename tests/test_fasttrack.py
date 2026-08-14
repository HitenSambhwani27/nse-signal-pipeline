"""Baseline scorer skips missing depth features; walk-forward has no leakage."""

from __future__ import annotations

from nse_pipeline.backtest.harness import iter_walk_forward_windows
from nse_pipeline.config import MaturityGateSettings
from nse_pipeline.models.panel import build_options_panels, panel_summary
from nse_pipeline.scoring.baseline import score_feature_row
from nse_pipeline.signals.maturity import classify_tier


def test_baseline_skips_depth_on_historical_partial() -> None:
    weights = {
        "equity": {
            "depth_ratio": 0.25,
            "spread_bps": -0.15,
            "ofi": 0.35,
            "vwap_deviation": 0.25,
            "intercept": 0.0,
        }
    }
    row = {
        "timestamp": "2022-01-03T04:00:00+00:00",
        "trade_date": "2022-01-03",
        "symbol": "RELIANCE",
        "track": "equity_depth",
        "source": "historical",
        "features": {
            "vwap_deviation_bps": 10.0,
            "depth_features_skipped": ["depth_ratio_bid_ask", "spread_bps", "ofi_bucket_sum"],
            "feature_completeness": "historical_partial",
        },
    }
    scored = score_feature_row(row, weights)
    assert "depth_ratio" in scored["attribution"]["skipped_missing"]
    assert "ofi" in scored["attribution"]["skipped_missing"]
    assert "vwap_deviation" in scored["attribution"]["terms"]


def test_walk_forward_windows_are_strictly_temporal() -> None:
    dates = [f"2022-01-{d:02d}" for d in range(1, 21)]
    windows = iter_walk_forward_windows(
        dates, train_days=5, test_days=2, step_days=2, min_train_days=5
    )
    assert windows
    for train, test in windows:
        assert train[-1] < test[0]
        assert not set(train) & set(test)


def test_options_panels_split_nifty_and_banknifty() -> None:
    rows = [
        {
            "track": "options",
            "symbol": "NIFTY25AUG1924500CE",
            "trade_date": "2026-08-12",
            "timestamp": "2026-08-12T04:00:00+00:00",
            "actual_outcome": 1.0,
            "features": {
                "underlying": "NIFTY",
                "spot": 24500,
                "strike": 24600,
                "expiry": "2026-08-18",
            },
        },
        {
            "track": "options",
            "symbol": "BANKNIFTY25AUG55000CE",
            "trade_date": "2026-08-12",
            "timestamp": "2026-08-12T04:00:00+00:00",
            "actual_outcome": -1.0,
            "features": {
                "underlying": "BANKNIFTY",
                "spot": 55000,
                "strike": 55200,
                "expiry": "2026-08-25",
            },
        },
    ]
    panels = build_options_panels(rows)
    summary = panel_summary(panels)
    assert summary["NIFTY"]["n"] == 1
    assert summary["BANKNIFTY"]["n"] == 1
    assert "moneyness" in panels["NIFTY"].columns


def test_maturity_tiers() -> None:
    class _S:
        maturity_gate = MaturityGateSettings(suppress_below_days=10, provisional_below_days=60)

    settings = _S()  # type: ignore[assignment]
    assert classify_tier(5, settings) == "suppressed"  # type: ignore[arg-type]
    assert classify_tier(20, settings) == "provisional"  # type: ignore[arg-type]
    assert classify_tier(60, settings) == "full"  # type: ignore[arg-type]
