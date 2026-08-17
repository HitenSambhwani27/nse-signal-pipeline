"""Tests for NSE session partial-coverage detection."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from nse_pipeline.config import (
    CompactionSettings,
    FeaturesSettings,
    FuturesSettings,
    HistoricalSettings,
    IngestionSettings,
    KiteCredentials,
    OptionsSettings,
    PathsSettings,
    ResilienceSettings,
    SessionSettings,
    Settings,
    SignalSettings,
    TripleBarrierSettings,
    UniverseSettings,
)
from nse_pipeline.session_coverage import assess_session_coverage, scoring_skip_trade_dates


def _settings(tmp: Path) -> Settings:
    return Settings(
        paths=PathsSettings(
            data_dir=tmp,
            raw_dir=tmp / "raw",
            compacted_dir=tmp / "compacted",
            features_dir=tmp / "features",
            models_dir=tmp / "models",
            sqlite_db=tmp / "test.db",
            logs_dir=tmp / "logs",
            instruments_cache=tmp / "cache.json",
            membership_dir=tmp / "membership",
        ),
        universe=UniverseSettings(
            nifty100_csv_url="x",
            nifty500_csv_url="x",
            nifty100_csv_fallback_url="x",
            nifty500_csv_fallback_url="x",
            membership_max_age_days=7,
        ),
        index_symbols=[],
        options=OptionsSettings(exchange="NFO", underlyings=[]),
        futures=FuturesSettings(
            exchange="NFO", underlyings=[], contract_count=2, subscribe_mode="full"
        ),
        ingestion=IngestionSettings(
            flush_interval_seconds=45, flush_max_rows=5000, websocket_mode="full"
        ),
        compaction=CompactionSettings(
            archive_minute_files=False, archive_subdir="_minute_parts"
        ),
        session=SessionSettings(
            timezone="Asia/Kolkata",
            market_open="09:15",
            market_close="15:30",
            open_grace_minutes=15,
            close_grace_minutes=15,
        ),
        historical=HistoricalSettings(interval="5minute", lookback_days=1),
        features=FeaturesSettings(
            oi_bucket_minutes=5,
            max_tick_return_pct=5.0,
            options_max_tick_return_pct=25.0,
            basis_days_per_year=365,
        ),
        signal=SignalSettings(
            candle_interval_minutes=5,
            threshold_pct=0.15,
            label_mode="triple_barrier",
            triple_barrier=TripleBarrierSettings(
                vol_method="ewma",
                vol_window=20,
                barrier_multiplier=1.0,
                min_threshold_pct=0.05,
                max_threshold_pct=5.0,
                path_rule="close_vs_barriers",
                options_label_mode="raw_premium",
            ),
            vol_min_periods=5,
        ),
        resilience=ResilienceSettings(2.0, 60.0, 2.0),
        kite=KiteCredentials("", "", ""),
    )


def _write_ticks(path: Path, start_ist: str, end_ist: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range(start_ist, end_ist, freq="1min", tz="Asia/Kolkata")
    df = pd.DataFrame(
        {
            "timestamp": idx.tz_convert("UTC"),
            "instrument_token": 1,
            "symbol": path.parent.name,
            "exchange": "NSE",
            "last_price": 100.0,
            "volume": 1,
            "last_quantity": 1,
            "average_price": 100.0,
            "oi": None,
            "bid_prices": [[99.0]] * len(idx),
            "bid_quantities": [[10]] * len(idx),
            "bid_orders": [[1]] * len(idx),
            "ask_prices": [[101.0]] * len(idx),
            "ask_quantities": [[10]] * len(idx),
            "ask_orders": [[1]] * len(idx),
        }
    )
    df.to_parquet(path, index=False)


def test_partial_late_start(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    # Midday-only slice like 2026-08-13 live capture.
    _write_ticks(
        settings.paths.compacted_dir / "2026-08-13" / "RELIANCE" / "ticks.parquet",
        "2026-08-13 11:55",
        "2026-08-13 12:10",
    )
    cov = assess_session_coverage(settings, "2026-08-13")
    assert cov.status == "partial"
    assert cov.is_partial
    assert any("late_start" in r for r in cov.reasons)
    assert any("early_end" in r for r in cov.reasons)


def test_full_session(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_ticks(
        settings.paths.compacted_dir / "2026-08-14" / "RELIANCE" / "ticks.parquet",
        "2026-08-14 09:16",
        "2026-08-14 15:25",
    )
    cov = assess_session_coverage(settings, "2026-08-14")
    assert cov.status == "full"
    assert not cov.is_partial
    assert cov.reasons == []


def test_coverage_uses_cash_session_not_fo_sample(tmp_path: Path) -> None:
    """FO full-span contracts must not mark a live-pilot equity day as full."""
    settings = _settings(tmp_path)
    settings.paths.instruments_cache.write_text(
        json.dumps(
            {
                "equity_depth": {"RELIANCE": {}},
                "equity_quote": {},
                "index": {},
                "options": [],
                "futures": [],
            }
        ),
        encoding="utf-8",
    )
    _write_ticks(
        settings.paths.compacted_dir / "2026-08-13" / "RELIANCE" / "ticks.parquet",
        "2026-08-13 11:55",
        "2026-08-13 12:10",
    )
    _write_ticks(
        settings.paths.compacted_dir / "2026-08-13" / "NIFTY26AUG25000CE" / "ticks.parquet",
        "2026-08-13 09:15",
        "2026-08-13 15:29",
    )
    cov = assess_session_coverage(
        settings,
        "2026-08-13",
        symbols=["NIFTY26AUG25000CE"],
    )
    assert cov.status == "partial"
    assert any("late_start" in r for r in cov.reasons)


def test_scoring_skip_live_pilot_keeps_muhurat() -> None:
    skip = scoring_skip_trade_dates(
        [
            {
                "trade_date": "2026-08-13",
                "track": "equity_quote",
                "source": "live",
                "timestamp": "2026-08-13T06:30:00+00:00",
            },
            {
                "trade_date": "2026-08-13",
                "track": "equity_quote",
                "source": "live",
                "timestamp": "2026-08-13T06:40:00+00:00",
            },
            {
                "trade_date": "2025-10-21",
                "track": "equity_quote",
                "source": "historical",
                "timestamp": "2025-10-21T08:20:00+00:00",
            },
            {
                "trade_date": "2025-10-21",
                "track": "equity_quote",
                "source": "historical",
                "timestamp": "2025-10-21T09:15:00+00:00",
            },
            {
                "trade_date": "2026-08-14",
                "track": "equity_quote",
                "source": "historical",
                "timestamp": "2026-08-14T03:45:00+00:00",
            },
            {
                "trade_date": "2026-08-14",
                "track": "equity_quote",
                "source": "historical",
                "timestamp": "2026-08-14T10:00:00+00:00",
            },
        ]
    )
    assert skip == {"2026-08-13"}
