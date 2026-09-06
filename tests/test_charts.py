"""Chart payload: volume_delta, real OHLC, compacted history. No fabricated bars."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.market.assemble import assemble_charts
from nse_pipeline.market.charts import chart_payload
from nse_pipeline.market.ohlc import nse_session_bucket
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings

IST = ZoneInfo("Asia/Kolkata")


def _ist(h: int, m: int, s: int = 0) -> datetime:
    return datetime(2026, 9, 4, h, m, s, tzinfo=IST)


def _write_ticks(settings, date_str: str, symbol: str, rows: list[dict]) -> None:
    path = settings.paths.compacted_dir / date_str / symbol
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path / "ticks.parquet", index=False)


def test_nse_session_bucket_boundaries() -> None:
    cases = {
        "1m": ((_ist(9, 15, 30), _ist(9, 15)), (_ist(9, 16, 0), _ist(9, 16))),
        "3m": ((_ist(9, 17, 0), _ist(9, 15)), (_ist(9, 18, 0), _ist(9, 18))),
        "5m": ((_ist(9, 19, 0), _ist(9, 15)), (_ist(9, 20, 0), _ist(9, 20))),
        "10m": ((_ist(9, 24, 59), _ist(9, 15)), (_ist(9, 25, 0), _ist(9, 25))),
        "15m": ((_ist(9, 29, 0), _ist(9, 15)), (_ist(9, 30, 0), _ist(9, 30))),
        "30m": ((_ist(9, 44, 0), _ist(9, 15)), (_ist(9, 45, 0), _ist(9, 45))),
        "60m": ((_ist(10, 14, 0), _ist(9, 15)), (_ist(10, 15, 0), _ist(10, 15))),
    }
    for interval, ((inside, want_inside), (next_tick, want_next)) in cases.items():
        assert nse_session_bucket(inside, interval) == want_inside, interval
        assert nse_session_bucket(next_tick, interval) == want_next, interval
    assert nse_session_bucket(_ist(9, 14), "1m") is None
    assert nse_session_bucket(_ist(15, 31), "10m") is None
    assert nse_session_bucket(_ist(15, 30), "60m") == _ist(15, 15)


def test_chart_payload_includes_volume_delta() -> None:
    rows = [
        {
            "timestamp": "2026-09-04T03:45:00+00:00",
            "last_price": 100,
            "volume": 10,
            "volume_delta": 4,
            "oi": 1,
            "oi_delta": 1,
        }
    ]
    payload = chart_payload(
        rows,
        max_points=50,
        fields=("last_price", "volume", "volume_delta", "oi", "oi_delta"),
    )
    assert payload["points"][0]["volume_delta"] == 4
    assert payload["points"][0]["oi_delta"] == 1


def test_activity_sample_volume_delta_reaches_api(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    store.insert_activity_samples(
        [
            {
                "instrument_token": 1,
                "symbol": "HDFCBANK",
                "timestamp": "2026-09-04T03:45:00+00:00",
                "last_price": 1600.0,
                "last_quantity": 10,
                "volume": 100,
                "volume_delta": 12,
                "oi": None,
                "oi_delta": None,
                "trade_notional": 16000,
                "bid_depth_5": None,
                "ask_depth_5": None,
                "spread": 0.1,
                "depth_imbalance": 0.0,
                "tod_bucket": "03:45",
            }
        ]
    )
    series = assemble_charts(settings, store, "HDFCBANK")
    assert series["points"]
    assert series["points"][0]["volume_delta"] == 12


def test_historical_ticks_and_ohlc_from_compacted(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    rows = []
    for i, px in enumerate((100.0, 102.0, 99.0, 101.0)):
        rows.append(
            {
                "timestamp": datetime(2026, 9, 4, 3, 45, i * 10, tzinfo=timezone.utc),
                "last_price": px,
                "volume": 10 + i,
                "oi": 5,
            }
        )
    _write_ticks(settings, "2026-09-04", "HDFCBANK", rows)
    series = assemble_charts(settings, store, "HDFCBANK", interval="1m")
    assert series["points"]
    assert series["points"][0]["last_price"] == 100.0
    assert series["source"] == "compacted_ticks"
    assert series["candles_status"] == "ok"
    assert series["candles"]
    first = series["candles"][0]
    assert first["open"] == 100.0
    assert first["high"] == 102.0
    assert first["low"] == 99.0
    assert first["close"] == 101.0
    assert first["open"] is not None and first["high"] is not None

    client = TestClient(create_app(settings))
    payload = client.get("/api/v1/charts/HDFCBANK?interval=1m").json()
    assert payload["found"] is True
    assert payload["chart"]["candles_status"] == "ok"
    assert payload["chart"]["points"][0]["last_price"] == 100.0


def test_session_ohlc_fields_are_not_used_as_bars(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _write_ticks(
        settings,
        "2026-09-04",
        "INFY",
        [
            {
                "timestamp": datetime(2026, 9, 4, 3, 45, tzinfo=timezone.utc),
                "last_price": 101.0,
                "volume": 10,
                "oi": None,
                "ohlc_open": 90.0,
                "ohlc_high": 200.0,
                "ohlc_low": 80.0,
                "ohlc_close": 101.0,
            }
        ],
    )
    series = assemble_charts(settings, store, "INFY", interval="1m")
    assert series["candles_status"] == "ok"
    candle = series["candles"][0]
    assert candle["open"] == 101.0
    assert candle["high"] == 101.0
    assert candle["low"] == 101.0
    assert candle["close"] == 101.0
    assert candle["high"] != 200.0


def test_stored_bar_ohlc_is_used_instead_of_last_price(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _write_ticks(
        settings,
        "2026-09-04",
        "NIFTY 50",
        [
            {
                "timestamp": _ist(9, 16),
                "last_price": 105.0,
                "volume": 10,
                "oi": None,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "interval": "minute",
            },
            {
                "timestamp": _ist(9, 17),
                "last_price": 107.0,
                "volume": 5,
                "oi": None,
                "open": 105.0,
                "high": 108.0,
                "low": 104.0,
                "close": 107.0,
                "interval": "minute",
            },
        ],
    )
    series = assemble_charts(settings, store, "NIFTY 50", interval="10m")
    assert series["candles_status"] == "ok"
    candle = series["candles"][0]
    assert candle["open"] == 100.0
    assert candle["high"] == 110.0
    assert candle["low"] == 90.0
    assert candle["close"] == 107.0
    ts = pd.Timestamp(candle["timestamp"]).tz_convert(IST)
    assert ts.hour == 9 and ts.minute == 15


def test_api_10m_and_60m_are_session_aligned(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    rows = [
        {"timestamp": _ist(9, 16), "last_price": 100.0, "volume": 10, "oi": None},
        {"timestamp": _ist(10, 16), "last_price": 102.0, "volume": 20, "oi": None},
    ]
    _write_ticks(settings, "2026-09-04", "NIFTY 50", rows)
    client = TestClient(create_app(settings))
    ten = client.get("/api/v1/charts/NIFTY 50?interval=10m").json()["chart"]
    sixty = client.get("/api/v1/charts/NIFTY 50?interval=60m").json()["chart"]
    assert ten["candles_status"] == "ok"
    assert sixty["candles_status"] == "ok"
    t0 = pd.Timestamp(ten["candles"][0]["timestamp"]).tz_convert(IST)
    t1 = pd.Timestamp(ten["candles"][1]["timestamp"]).tz_convert(IST)
    s0 = pd.Timestamp(sixty["candles"][0]["timestamp"]).tz_convert(IST)
    s1 = pd.Timestamp(sixty["candles"][1]["timestamp"]).tz_convert(IST)
    assert (t0.hour, t0.minute) == (9, 15)
    assert (t1.hour, t1.minute) == (10, 15)
    assert (s0.hour, s0.minute) == (9, 15)
    assert (s1.hour, s1.minute) == (10, 15)


def test_unsupported_interval_is_unavailable(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    series = assemble_charts(settings, store, "HDFCBANK", interval="2h")
    assert series["candles"] == []
    assert series["candles_status"] == "unavailable"
    assert series["candles_reason"] == "unsupported_interval"


def test_missing_history_is_unavailable_not_fabricated(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    series = assemble_charts(settings, store, "MISSING", interval="5m")
    assert series["points"] == []
    assert series["candles"] == []
    assert series["candles_status"] == "unavailable"
    assert series["candles_reason"] == "no_compacted_data"
