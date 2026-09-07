"""Deterministic tests for market-data foundation. No live Kite."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from nse_pipeline.market.cache_health import (
    coverage_from_cache,
    instrument_cache_health,
    lookup_instrument_meta,
)
from nse_pipeline.market.derived import (
    ask_depth_n,
    best_ask,
    best_bid,
    bid_depth_n,
    depth_imbalance,
    mid_price,
    numeric_delta,
    spread,
)
from nse_pipeline.market.latest import LatestQuoteTracker, public_quote, snapshot_from_tick
from nse_pipeline.market.normalize import normalize_tick
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter
from nse_pipeline.storage.schemas import InstrumentInfo
from nse_pipeline.storage.sqlite_store import SQLiteStore


TOKEN = 256265
SYMBOL = "NIFTY 50"
MAP = {TOKEN: SYMBOL}
EXCH = {TOKEN: "NSE"}


def _level(price: float, quantity: int, orders: int = 1) -> dict:
    return {"price": price, "quantity": quantity, "orders": orders}


def _full_tick(**overrides) -> dict:
    tick = {
        "instrument_token": TOKEN,
        "timestamp": datetime(2026, 9, 4, 3, 45, tzinfo=timezone.utc),
        "exchange_timestamp": datetime(2026, 9, 4, 3, 45, 1, tzinfo=timezone.utc),
        "last_price": 24850.0,
        "last_traded_quantity": 75,
        "volume_traded": 10_000,
        "average_traded_price": 24840.0,
        "oi": 1_200_000,
        "total_buy_quantity": 5_000,
        "total_sell_quantity": 4_800,
        "last_trade_time": datetime(2026, 9, 4, 3, 44, 59, tzinfo=timezone.utc),
        "ohlc": {"open": 24700.0, "high": 24900.0, "low": 24680.0, "close": 24750.0},
        "depth": {
            "buy": [
                _level(24849.5, 100, 2),
                _level(24849.0, 150, 3),
                _level(24848.5, 80, 1),
                _level(24848.0, 60, 1),
                _level(24847.5, 40, 1),
            ],
            "sell": [
                _level(24850.5, 90, 2),
                _level(24851.0, 120, 2),
                _level(24851.5, 70, 1),
                _level(24852.0, 50, 1),
                _level(24852.5, 30, 1),
            ],
        },
    }
    tick.update(overrides)
    return tick


def test_normalize_full_tick_preserves_identity_time_price_depth() -> None:
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    assert tick.instrument_token == TOKEN
    assert tick.symbol == SYMBOL
    assert tick.exchange == "NSE"
    assert tick.trade_date == "2026-09-04"
    assert tick.last_price == 24850.0
    assert tick.last_quantity == 75
    assert tick.volume == 10_000
    assert tick.average_price == 24840.0
    assert tick.oi == 1_200_000
    assert tick.total_buy_quantity == 5_000
    assert tick.total_sell_quantity == 4_800
    assert tick.ohlc_open == 24700.0
    assert tick.ohlc_high == 24900.0
    assert tick.ohlc_low == 24680.0
    assert tick.ohlc_close == 24750.0
    assert tick.last_trade_time is not None
    assert tick.exchange_timestamp is not None
    assert tick.ingested_at is not None
    assert tick.bid_prices == [24849.5, 24849.0, 24848.5, 24848.0, 24847.5]
    assert tick.bid_quantities == [100, 150, 80, 60, 40]
    assert tick.bid_orders == [2, 3, 1, 1, 1]
    assert tick.ask_prices == [24850.5, 24851.0, 24851.5, 24852.0, 24852.5]
    assert tick.ask_quantities == [90, 120, 70, 50, 30]
    assert len(tick.ask_orders) == 5


def test_normalize_missing_token_and_unknown_symbol() -> None:
    assert normalize_tick({"last_price": 1}, MAP, EXCH) is None
    assert normalize_tick({"instrument_token": 1, "last_price": 1}, MAP, EXCH) is None


def test_normalize_missing_fields_and_quote_mode_without_depth() -> None:
    tick = normalize_tick(
        {
            "instrument_token": TOKEN,
            "last_price": 100.0,
            "volume": 50,
            "last_quantity": 2,
        },
        MAP,
        EXCH,
    )
    assert tick is not None
    assert tick.oi is None
    assert tick.last_trade_time is None
    assert tick.total_buy_quantity is None
    assert tick.ohlc_open is None
    assert tick.bid_prices == []
    assert tick.ask_quantities == []
    assert tick.volume == 50
    assert tick.last_quantity == 2


def test_normalize_empty_depth() -> None:
    tick = normalize_tick(_full_tick(depth={"buy": [], "sell": []}), MAP, EXCH)
    assert tick is not None
    assert tick.bid_prices == []
    assert tick.ask_prices == []


def test_normalize_malformed_ohlc_and_string_timestamps() -> None:
    tick = normalize_tick(
        _full_tick(
            ohlc="bad",
            exchange_timestamp="2026-09-04T09:15:00+05:30",
            last_trade_time="2026-09-04T09:14:59+05:30",
        ),
        MAP,
        EXCH,
    )
    assert tick is not None
    assert tick.ohlc_open is None
    assert tick.exchange_timestamp is not None
    assert tick.trade_date == "2026-09-04"


def test_spread_mid_depth_imbalance_and_zero_book() -> None:
    assert spread(100.0, 101.0) == 1.0
    assert mid_price(100.0, 101.0) == 100.5
    assert spread(None, 101.0) is None
    assert spread(0.0, 101.0) is None
    assert mid_price(None, None) is None
    assert bid_depth_n([10, 20, 30, 40, 50, 999], n=5) == 150
    assert ask_depth_n(None) == 0
    assert bid_depth_n([]) == 0
    assert depth_imbalance(150, 50) == 0.5
    assert depth_imbalance(0, 0) is None
    assert depth_imbalance(10, 0) == 1.0
    assert depth_imbalance(0, 10) == -1.0
    assert best_bid([0.0, 99.0], [0, 5]) == (99.0, 5)
    assert best_ask([], []) == (None, None)


def test_numeric_delta_and_session_volume_reset() -> None:
    assert numeric_delta(110, 100) == 10
    assert numeric_delta(None, 100) is None
    first = normalize_tick(_full_tick(volume_traded=1000), MAP, EXCH)
    second = normalize_tick(_full_tick(volume_traded=1250, last_price=24851.0), MAP, EXCH)
    reset = normalize_tick(_full_tick(volume_traded=80), MAP, EXCH)
    assert first is not None and second is not None and reset is not None
    snap1 = snapshot_from_tick(first)
    snap2 = snapshot_from_tick(second, snap1)
    snap_reset = snapshot_from_tick(reset, snap2)
    assert snap1["volume_delta"] is None
    assert snap2["volume_delta"] == 250
    assert snap2["price_delta"] == 1.0
    assert snap_reset["volume_delta"] is None


def test_snapshot_derived_metrics_from_full_book() -> None:
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    snap = snapshot_from_tick(tick)
    assert snap["best_bid_price"] == 24849.5
    assert snap["best_ask_price"] == 24850.5
    assert snap["spread"] == 1.0
    assert snap["mid_price"] == 24850.0
    assert snap["bid_depth_5"] == 430
    assert snap["ask_depth_5"] == 360
    assert snap["depth_imbalance"] == (430 - 360) / (430 + 360)
    assert snap["total_buy_quantity"] == 5_000


def test_duplicate_observations_keep_latest_and_zero_delta() -> None:
    tracker = LatestQuoteTracker()
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    first = tracker.observe(tick)
    second = tracker.observe(tick)
    assert second["volume"] == first["volume"]
    assert second["volume_delta"] == 0
    dirty = tracker.drain_dirty()
    assert len(dirty) == 1


def test_out_of_order_timestamp_does_not_replace_latest() -> None:
    tracker = LatestQuoteTracker()
    newer = normalize_tick(_full_tick(last_price=24900.0, volume_traded=20_000), MAP, EXCH)
    older = normalize_tick(
        _full_tick(
            last_price=24000.0,
            volume_traded=1,
            exchange_timestamp=datetime(2026, 9, 4, 3, 40, tzinfo=timezone.utc),
            timestamp=datetime(2026, 9, 4, 3, 40, tzinfo=timezone.utc),
        ),
        MAP,
        EXCH,
    )
    assert newer is not None and older is not None
    tracker.observe(newer)
    kept = tracker.observe(older)
    assert kept["last_price"] == 24900.0
    assert kept["volume"] == 20_000


def test_parquet_roundtrip_preserves_new_columns(tmp_path: Path) -> None:
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    writer = BufferedParquetWriter(raw_dir=tmp_path, flush_interval_seconds=1, flush_max_rows=1)
    writer.add_tick(tick)
    files = list(tmp_path.glob("*/*/*.parquet"))
    assert len(files) == 1
    df = BufferedParquetWriter.read_parquet(files[0])
    row = df.iloc[0]
    assert row["trade_date"] == "2026-09-04"
    assert row["last_quantity"] == 75
    assert row["total_buy_quantity"] == 5_000
    assert row["ohlc_high"] == 24900.0
    assert list(row["bid_orders"]) == [2, 3, 1, 1, 1]


def test_latest_quotes_sqlite_upsert(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "q.db")
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    tracker = LatestQuoteTracker()
    store.upsert_latest_quotes([tracker.observe(tick)])
    row = store.fetch_latest_quote("nifty 50")
    assert row is not None
    assert row["symbol"] == SYMBOL
    assert row["last_price"] == 24850.0
    assert row["bid_depth_5"] == 430


def test_public_quote_uses_visible_book_not_executed_fills() -> None:
    dto = public_quote(
        {
            "symbol": "RELIANCE",
            "last_price": 1400.0,
            "price_delta": 1.5,
            "oi_delta": 10,
            "total_buy_quantity": 9_000,
            "total_sell_quantity": 8_000,
            "best_bid_price": 1399.9,
            "best_ask_price": 1400.1,
        },
        meta={"instrument_type": "EQ", "lot_size": 1, "tick_size": 0.05, "name": "RELIANCE"},
    )
    assert dto["buy_quantity"] == 9_000
    assert dto["sell_quantity"] == 8_000
    assert dto["change"] == 1.5
    assert dto["oi_change"] == 10
    assert dto["instrument_type"] == "EQ"
    assert "executed" not in str(dto).lower()
    assert "features_json" not in dto


def test_eq_fut_ce_pe_metadata_roundtrip() -> None:
    eq = InstrumentInfo.from_cache_dict(
        {
            "instrument_token": 1,
            "tradingsymbol": "RELIANCE",
            "exchange": "NSE",
            "name": "RELIANCE",
            "segment": "NSE",
            "instrument_type": "EQ",
            "lot_size": 1,
            "tick_size": 0.05,
            "subscribe_mode": "full",
        }
    )
    fut = InstrumentInfo.from_cache_dict(
        {
            "instrument_token": 2,
            "tradingsymbol": "NIFTY26SEPFUT",
            "exchange": "NFO",
            "name": "NIFTY",
            "segment": "NFO-FUT",
            "instrument_type": "FUT",
            "expiry": "2026-09-24",
            "lot_size": 75,
            "tick_size": 0.05,
        }
    )
    ce = InstrumentInfo.from_cache_dict(
        {
            "instrument_token": 3,
            "tradingsymbol": "NIFTY2690824850CE",
            "exchange": "NFO",
            "name": "NIFTY",
            "segment": "NFO-OPT",
            "instrument_type": "CE",
            "strike": 24850.0,
            "expiry": "2026-09-08",
            "lot_size": 75,
            "tick_size": 0.05,
        }
    )
    pe = InstrumentInfo.from_cache_dict(
        {
            "instrument_token": 4,
            "tradingsymbol": "BANKNIFTY26SEP55000PE",
            "exchange": "NFO",
            "name": "BANKNIFTY",
            "segment": "NFO-OPT",
            "instrument_type": "PE",
            "strike": 55000.0,
            "expiry": "2026-09-29",
            "lot_size": 30,
            "tick_size": 0.05,
        }
    )
    assert eq.instrument_type == "EQ" and eq.tick_size == 0.05
    assert fut.instrument_type == "FUT" and fut.lot_size == 75 and fut.expiry == "2026-09-24"
    assert ce.instrument_type == "CE" and ce.strike == 24850.0
    assert pe.instrument_type == "PE" and pe.name == "BANKNIFTY"


def test_instrument_cache_health_and_coverage() -> None:
    cache = {
        "updated_at": "2026-08-15T10:00:00Z",
        "schema_version": 1,
        "counts": {"options": 2, "futures": 1},
        "equity_depth": {"RELIANCE": {"tradingsymbol": "RELIANCE", "instrument_type": "EQ"}},
        "equity_quote": {},
        "index": {"NIFTY 50": {"tradingsymbol": "NIFTY 50"}, "NIFTY BANK": {"tradingsymbol": "NIFTY BANK"}},
        "options": [
            {
                "tradingsymbol": "NIFTY2581824850CE",
                "name": "NIFTY",
                "instrument_type": "CE",
                "expiry": "2026-08-18",
                "strike": 24850.0,
            },
            {
                "tradingsymbol": "BANKNIFTY26SEP55000PE",
                "name": "BANKNIFTY",
                "instrument_type": "PE",
                "expiry": "2026-09-29",
                "strike": 55000.0,
            },
        ],
        "futures": [
            {
                "tradingsymbol": "NIFTY26AUGFUT",
                "name": "NIFTY",
                "instrument_type": "FUT",
                "expiry": "2026-08-28",
            }
        ],
    }
    health = instrument_cache_health(cache, today=datetime(2026, 9, 4).date())
    assert health["status"] == "stale_expiries"
    assert health["needs_refresh"] is True
    assert health["expired_option_count"] == 1
    assert health["expired_future_count"] == 1
    coverage = coverage_from_cache(cache, membership={"nifty100": ["RELIANCE", "INFY"]})
    assert coverage["nifty100_listed"] == 2
    assert coverage["nifty100_missing_from_depth"] == ["INFY"]
    assert coverage["nifty_spot"] is True
    assert coverage["banknifty_spot"] is True
    assert coverage["nifty_option_ce"] == 1
    assert lookup_instrument_meta(cache, "NIFTY2581824850CE")["instrument_type"] == "CE"
    assert lookup_instrument_meta(cache, "reliance")["tradingsymbol"] == "RELIANCE"


def test_instrument_cache_missing_schema_version_is_schema_stale() -> None:
    health = instrument_cache_health(
        {"updated_at": "2026-09-04T05:13:00Z", "options": [], "futures": []},
        today=datetime(2026, 9, 7).date(),
    )
    assert health["status"] == "schema_stale"
    assert health["schema_ok"] is False
    assert health["needs_refresh"] is True


def test_consecutive_snapshots_preserve_inputs_for_later_depth_diff() -> None:
    first = normalize_tick(_full_tick(), MAP, EXCH)
    second = normalize_tick(
        _full_tick(
            depth={
                "buy": [_level(24850.0, 200, 1)] + [_level(24849.0, 150, 3)] * 4,
                "sell": [_level(24851.0, 40, 1)] + [_level(24852.0, 50, 1)] * 4,
            },
            exchange_timestamp=datetime(2026, 9, 4, 3, 45, 2, tzinfo=timezone.utc),
        ),
        MAP,
        EXCH,
    )
    assert first is not None and second is not None
    a = snapshot_from_tick(first)
    b = snapshot_from_tick(second, a)
    assert b["bid_depth_5"] != a["bid_depth_5"]
    assert b["ask_depth_5"] != a["ask_depth_5"]
    assert b["best_bid_price"] != a["best_bid_price"]
    assert max(0, b["bid_depth_5"] - a["bid_depth_5"]) >= 0
    assert max(0, a["bid_depth_5"] - b["bid_depth_5"]) >= 0
