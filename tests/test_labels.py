"""Unit tests for Stage 3A triple-barrier / legacy labeling."""

from __future__ import annotations

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
from nse_pipeline.labels.triple_barrier import (
    _clip_threshold,
    label_from_return,
    label_series_legacy,
    label_series_triple_barrier,
)


def _settings(tmp: Path | None = None, **tb_overrides) -> Settings:
    base = tmp or Path("data")
    tb = dict(
        vol_method="ewma",
        vol_window=5,
        barrier_multiplier=1.0,
        min_threshold_pct=0.05,
        max_threshold_pct=5.0,
        path_rule="close_vs_barriers",
        options_label_mode="raw_premium",
    )
    tb.update(tb_overrides)
    return Settings(
        paths=PathsSettings(
            data_dir=base,
            raw_dir=base / "raw",
            compacted_dir=base / "compacted",
            features_dir=base / "features",
            models_dir=base / "models",
            sqlite_db=base / "test.db",
            logs_dir=base / "logs",
            instruments_cache=base / "cache.json",
            membership_dir=base / "membership",
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
            triple_barrier=TripleBarrierSettings(**tb),
            vol_min_periods=3,
        ),
        resilience=ResilienceSettings(2.0, 60.0, 2.0),
        kite=KiteCredentials("", "", ""),
    )


def test_label_from_return() -> None:
    assert label_from_return(0.2, 0.15) == ("up", 1.0)
    assert label_from_return(-0.2, 0.15) == ("down", -1.0)
    assert label_from_return(0.01, 0.15) == ("flat", 0.0)


def test_clip_threshold_floor_and_cap() -> None:
    settings = _settings()
    tb = settings.signal.triple_barrier
    thr, floor, cap, reason = _clip_threshold(0.01, tb)
    assert thr == 0.05 and floor and not cap and reason == "below_min"
    thr, floor, cap, reason = _clip_threshold(9.0, tb)
    assert thr == 5.0 and not floor and cap and reason is None
    thr, floor, cap, reason = _clip_threshold(None, tb)
    assert thr == 0.05 and floor and not cap and reason == "missing_vol"
    thr, floor, cap, reason = _clip_threshold(0.2, tb)
    assert thr == 0.2 and not floor and not cap and reason is None


def test_legacy_vs_tbm_floor_binding() -> None:
    # Nearly flat series → raw vol tiny → floor binds → thr=0.05
    idx = pd.date_range(
        "2026-08-13 09:15", periods=12, freq="5min", tz="Asia/Kolkata"
    )
    closes = pd.Series(
        [100.0 + 0.001 * i for i in range(12)], index=idx.tz_convert("UTC")
    )
    settings = _settings(min_threshold_pct=0.05, max_threshold_pct=5.0, vol_window=5)

    legacy = label_series_legacy(
        closes, symbol="X", track="equity_quote", horizon_bars=1, threshold_pct=0.15
    )
    tbm = label_series_triple_barrier(
        closes, symbol="X", track="equity_quote", horizon_bars=1, settings=settings
    )
    assert len(legacy) == len(tbm) == 11
    warm = [r for r in tbm if r.vol is not None]
    assert warm
    assert any(r.floor_bound for r in warm)
    assert sum(1 for r in tbm if r.label == "flat") <= sum(
        1 for r in legacy if r.label == "flat"
    )
