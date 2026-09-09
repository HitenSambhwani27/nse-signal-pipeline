"""LIVE -> LAST COMPLETED SESSION fallback for the read layer.

Covers the explicit backend decision (market_state / data_status / as_of), that
latest_quotes is never backfilled, and that historical timestamps survive.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.api.read_model import SqliteUiReadModel
from nse_pipeline.market.data_state import (
    MARKET_CLOSED,
    MARKET_OPEN,
    MARKET_POST_CLOSE,
    MARKET_PRE_OPEN,
    MARKET_WEEKEND,
    STATUS_LAST_SESSION,
    STATUS_LIVE,
    STATUS_NO_DATA,
    market_state,
    resolve,
)
from nse_pipeline.market.read_policy import MarketDataPolicy
from nse_pipeline.storage.bars import materialize_symbol_session
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings

IST = ZoneInfo("Asia/Kolkata")

# 2026-09-04 is a Friday; 2026-09-06 a Sunday; 2026-09-07 a Monday.
LAST_SESSION = "2026-09-04"
CLOSE_IST = datetime(2026, 9, 4, 15, 30, tzinfo=IST)
SUNDAY_IST = datetime(2026, 9, 6, 15, 29, tzinfo=IST)
MONDAY_OPEN_IST = datetime(2026, 9, 7, 10, 32, tzinfo=IST)


def _tick(
    symbol: str,
    token: int,
    *,
    when: datetime,
    last_price: float,
    volume: int,
    oi: int | None = None,
    bid: float = 0.0,
    ask: float = 0.0,
    last_quantity: int = 5,
) -> dict:
    return {
        "timestamp": when.astimezone(timezone.utc),
        "instrument_token": token,
        "symbol": symbol,
        "exchange": "NSE",
        "last_price": last_price,
        "last_quantity": last_quantity,
        "volume": volume,
        "average_price": last_price,
        "oi": oi,
        "bid_prices": [bid or last_price - 0.1, 0.0, 0.0, 0.0, 0.0],
        "bid_quantities": [40, 0, 0, 0, 0],
        "bid_orders": [1, 0, 0, 0, 0],
        "ask_prices": [ask or last_price + 0.1, 0.0, 0.0, 0.0, 0.0],
        "ask_quantities": [25, 0, 0, 0, 0],
        "ask_orders": [1, 0, 0, 0, 0],
        "ingested_at": when.astimezone(timezone.utc),
        "exchange_timestamp": when.astimezone(timezone.utc),
        "last_trade_time": when.astimezone(timezone.utc),
        "trade_date": when.astimezone(IST).date().isoformat(),
        "total_buy_quantity": 500,
        "total_sell_quantity": 400,
        "ohlc_open": last_price - 2,
        "ohlc_high": last_price + 3,
        "ohlc_low": last_price - 4,
        "ohlc_close": last_price - 1,
    }


def _write_ticks(settings, date_str: str, symbol: str, rows: list[dict]) -> None:
    folder = settings.paths.compacted_dir / date_str / symbol
    folder.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(folder / "ticks.parquet", index=False)


def _cache(settings) -> None:
    settings.paths.instruments_cache.write_text(
        json.dumps(
            {
                "updated_at": "2026-09-04T00:00:00Z",
                "index": {
                    "NIFTY 50": {
                        "instrument_token": 256265,
                        "tradingsymbol": "NIFTY 50",
                        "name": "NIFTY",
                        "exchange": "NSE",
                        "instrument_type": "EQ",
                    }
                },
                "equity_depth": {
                    "HDFCBANK": {
                        "instrument_token": 341249,
                        "tradingsymbol": "HDFCBANK",
                        "name": "HDFCBANK",
                        "exchange": "NSE",
                        "instrument_type": "EQ",
                        "lot_size": 1,
                        "tick_size": 0.05,
                    }
                },
                "options": [
                    {
                        "tradingsymbol": "NIFTY26908100CE",
                        "name": "NIFTY",
                        "instrument_type": "CE",
                        "strike": 100,
                        "expiry": "2026-09-08",
                        "lot_size": 65,
                        "tick_size": 0.05,
                    },
                    {
                        "tradingsymbol": "NIFTY26908100PE",
                        "name": "NIFTY",
                        "instrument_type": "PE",
                        "strike": 100,
                        "expiry": "2026-09-08",
                        "lot_size": 65,
                        "tick_size": 0.05,
                    },
                ],
                "futures": [
                    {
                        "tradingsymbol": "NIFTY26SEPFUT",
                        "name": "NIFTY",
                        "instrument_type": "FUT",
                        "expiry": "2026-09-29",
                        "lot_size": 65,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _seed_last_session(settings) -> None:
    """Two observations per symbol so deltas are real, not manufactured."""
    earlier = CLOSE_IST - timedelta(minutes=1)
    _write_ticks(
        settings,
        LAST_SESSION,
        "NIFTY 50",
        [
            _tick("NIFTY 50", 256265, when=earlier, last_price=99.0, volume=900),
            _tick("NIFTY 50", 256265, when=CLOSE_IST, last_price=100.0, volume=1000),
        ],
    )
    _write_ticks(
        settings,
        LAST_SESSION,
        "HDFCBANK",
        [
            _tick("HDFCBANK", 341249, when=earlier, last_price=1980.0, volume=4000),
            _tick("HDFCBANK", 341249, when=CLOSE_IST, last_price=2000.5, volume=4500),
        ],
    )
    _write_ticks(
        settings,
        LAST_SESSION,
        "NIFTY26SEPFUT",
        [
            _tick("NIFTY26SEPFUT", 111, when=earlier, last_price=100.4, volume=80, oi=4900),
            _tick("NIFTY26SEPFUT", 111, when=CLOSE_IST, last_price=101.2, volume=90, oi=5000),
        ],
    )
    for symbol, token, price, oi in (
        ("NIFTY26908100CE", 222, 6.5, 1200),
        ("NIFTY26908100PE", 333, 3.25, 800),
    ):
        _write_ticks(
            settings,
            LAST_SESSION,
            symbol,
            [
                _tick(symbol, token, when=earlier, last_price=price - 0.5, volume=50, oi=oi - 100),
                _tick(symbol, token, when=CLOSE_IST, last_price=price, volume=70, oi=oi),
            ],
        )
    for symbol in (
        "NIFTY 50",
        "HDFCBANK",
        "NIFTY26SEPFUT",
        "NIFTY26908100CE",
        "NIFTY26908100PE",
    ):
        materialize_symbol_session(settings, LAST_SESSION, symbol)


def _read_model(tmp_path: Path, *, now: datetime, seed_history: bool = True):
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    if seed_history:
        _seed_last_session(settings)
    read = SqliteUiReadModel(settings)
    read.policy = MarketDataPolicy(settings, read.store, now_fn=lambda: now)
    return settings, store, read


def _live_row(symbol: str, token: int, *, when: datetime, last_price: float) -> dict:
    return {
        "instrument_token": token,
        "symbol": symbol,
        "exchange": "NSE",
        "timestamp": when.astimezone(timezone.utc).isoformat(),
        "ingested_at": when.astimezone(timezone.utc).isoformat(),
        "last_price": last_price,
        "last_quantity": 12,
        "volume": 9999,
        "average_price": last_price,
        "oi": None,
        "total_buy_quantity": 100,
        "total_sell_quantity": 90,
        "best_bid_price": last_price - 0.1,
        "best_bid_quantity": 10,
        "best_ask_price": last_price + 0.1,
        "best_ask_quantity": 10,
        "bid_depth_5": 10,
        "ask_depth_5": 10,
        "spread": 0.2,
        "mid_price": last_price,
        "depth_imbalance": 0.0,
        "volume_delta": 3,
        "oi_delta": None,
        "price_delta": 0.5,
    }


# --- clock / resolver units -------------------------------------------------


def test_market_state_clock(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    assert market_state(session, now=MONDAY_OPEN_IST) == MARKET_OPEN
    assert market_state(session, now=SUNDAY_IST) == MARKET_WEEKEND
    assert market_state(session, now=datetime(2026, 9, 7, 9, 14, tzinfo=IST)) == MARKET_PRE_OPEN
    assert market_state(session, now=datetime(2026, 9, 7, 15, 31, tzinfo=IST)) == MARKET_POST_CLOSE
    assert market_state(session, now=datetime(2026, 9, 7, 9, 15, tzinfo=IST)) == MARKET_OPEN
    assert market_state(session, now=datetime(2026, 9, 7, 8, 59, tzinfo=IST)) == MARKET_CLOSED
    assert market_state(session, now=datetime(2026, 9, 7, 16, 1, tzinfo=IST)) == MARKET_CLOSED


def test_resolver_never_calls_a_stale_row_live(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    stale = MONDAY_OPEN_IST - timedelta(minutes=30)
    _choice, state = resolve(
        session=session,
        live_timestamp=stale.isoformat(),
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=MONDAY_OPEN_IST,
    )
    assert state.market_state == MARKET_OPEN
    assert state.data_status == STATUS_LAST_SESSION
    assert state.reason == "live_snapshot_stale"
    assert state.as_of == stale.isoformat()


# --- A: market open + fresh live data --------------------------------------


def test_market_open_with_fresh_snapshot_reports_live(tmp_path: Path) -> None:
    _settings_obj, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST)
    store.upsert_latest_quotes(
        [_live_row("HDFCBANK", 341249, when=MONDAY_OPEN_IST - timedelta(seconds=5), last_price=2011.0)]
    )
    payload = read.quote("HDFCBANK")
    state = payload["data_state"]
    assert payload["found"] is True
    assert payload["quote"]["last_price"] == 2011.0
    assert state["market_state"] == MARKET_OPEN
    assert state["data_status"] == STATUS_LIVE
    assert state["source"] == "latest_quotes"
    assert state["session_date"] == "2026-09-07"
    assert state["as_of"].endswith("+05:30")


# --- B / E: market closed + historical data --------------------------------


def test_market_closed_serves_last_session_with_real_timestamp(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.quote("HDFCBANK")
    state = payload["data_state"]
    assert payload["found"] is True
    assert payload["quote"]["last_price"] == 2000.5
    assert state["market_state"] == MARKET_WEEKEND
    assert state["data_status"] == STATUS_LAST_SESSION
    assert state["source"] == "compacted_ticks"
    assert state["session_date"] == LAST_SESSION
    assert state["reason"] == "market_closed"
    # The observation timestamp is preserved, not replaced with "now".
    assert state["as_of"] == CLOSE_IST.isoformat()
    assert state["as_of"] != SUNDAY_IST.isoformat()
    # Deltas come from the preceding observation of the same session.
    assert payload["quote"]["change"] == 20.5
    assert payload["quote"]["volume_delta"] == 500


def test_last_session_quote_state_is_per_symbol(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.watchlist_quotes()
    by_symbol = {row["symbol"]: row for row in payload["quotes"]}
    assert by_symbol["HDFCBANK"]["found"] is True
    assert by_symbol["HDFCBANK"]["data_state"]["data_status"] == STATUS_LAST_SESSION
    # A watchlist name with no observation at all must stay honest.
    assert by_symbol["INFY"]["found"] is False
    assert by_symbol["INFY"]["data_state"]["data_status"] == STATUS_NO_DATA
    assert by_symbol["INFY"]["data_state"]["as_of"] is None
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION


# --- C: no data at all ------------------------------------------------------


def test_no_history_and_no_live_row_reports_no_data(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST, seed_history=False)
    payload = read.quote("HDFCBANK")
    assert payload["found"] is False
    assert payload["quote"] is None
    assert payload["data_state"]["data_status"] == STATUS_NO_DATA
    assert payload["data_state"]["as_of"] is None
    assert payload["data_state"]["session_date"] is None
    assert payload["data_state"]["market_state"] == MARKET_WEEKEND


# --- D: latest_quotes stays live-only --------------------------------------


def test_historical_fallback_never_writes_latest_quotes(tmp_path: Path) -> None:
    _settings_obj, store, read = _read_model(tmp_path, now=SUNDAY_IST)

    def _count() -> int:
        with store.connection() as conn:
            return int(conn.execute("SELECT count(*) FROM latest_quotes").fetchone()[0])

    assert _count() == 0
    read.quote("HDFCBANK")
    read.futures_book("NIFTY")
    read.option_chain("NIFTY", "2026-09-08")
    read.market_activity("HDFCBANK")
    read.unusual_activity(limit=5)
    read.charts("HDFCBANK", interval="1m")
    assert _count() == 0

    seeded = _live_row("HDFCBANK", 341249, when=CLOSE_IST, last_price=1234.5)
    store.upsert_latest_quotes([seeded])
    read.quote("HDFCBANK")
    read.watchlist_quotes()
    assert _count() == 1
    row = store.fetch_latest_quote("HDFCBANK")
    assert row["last_price"] == 1234.5
    assert row["timestamp"] == seeded["timestamp"]


# --- F: futures basis from compatible observations -------------------------


def test_futures_last_session_basis_uses_same_session_observations(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.futures_book("NIFTY")
    book = payload["futures"]
    contract = book["contracts"][0]
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert book["spot"] == 100.0
    assert contract["ltp"] == 101.2
    assert contract["spot_as_of"] == contract["futures_as_of"]
    assert contract["data_age_seconds"] == 0.0
    assert contract["basis_status"] == "fresh"
    assert abs(contract["basis"] - 1.2) < 1e-9
    assert contract["oi"] == 5000
    assert contract["oi_change"] == 100.0
    assert contract["price_oi"]["label"] == "long_buildup"


def test_futures_basis_null_when_session_observations_do_not_match(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    _write_ticks(
        settings,
        LAST_SESSION,
        "NIFTY 50",
        [_tick("NIFTY 50", 256265, when=CLOSE_IST, last_price=100.0, volume=1000)],
    )
    _write_ticks(
        settings,
        LAST_SESSION,
        "NIFTY26SEPFUT",
        [
            _tick(
                "NIFTY26SEPFUT",
                111,
                when=CLOSE_IST - timedelta(minutes=20),
                last_price=101.2,
                volume=90,
                oi=5000,
            )
        ],
    )
    read = SqliteUiReadModel(settings)
    read.policy = MarketDataPolicy(settings, read.store, now_fn=lambda: SUNDAY_IST)
    contract = read.futures_book("NIFTY")["futures"]["contracts"][0]
    assert contract["basis"] is None
    assert contract["basis_pct"] is None
    assert contract["basis_status"] == "stale"
    assert contract["data_age_seconds"] == 1200.0
    # A single observation must not manufacture an OI change.
    assert contract["oi_change"] is None
    assert contract["price_oi"]["label"] is None


# --- G: option chain from the completed session ----------------------------


def test_option_chain_available_from_last_session(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.option_chain("NIFTY", "2026-09-08")
    chain = payload["chain"]
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert payload["data_state"]["session_date"] == LAST_SESSION
    assert chain["found"] is True
    assert chain["spot"] == 100.0
    assert chain["atm"] == 100.0
    assert chain["chain_status"] == "complete"
    assert chain["quote_status"] == "complete"
    assert chain["quoted_contract_count"] == 2
    row = chain["strikes"][0]
    assert row["ce"]["ltp"] == 6.5
    assert row["ce"]["oi"] == 1200
    assert row["ce"]["oi_change"] == 100.0
    assert row["pe"]["ltp"] == 3.25
    assert row["pe"]["oi"] == 800
    # Maturity suppression and IV assumptions are untouched by the fallback.
    assert row["ce"]["iv_model"] == "black_scholes"
    assert row["ce"]["iv_exercise_style"] == "european"
    oi_payload = read.option_oi("NIFTY", "2026-09-08")
    assert oi_payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert oi_payload["oi"]["quote_status"] == "complete"


def test_option_chain_oi_change_null_without_prior_observation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    _write_ticks(
        settings,
        LAST_SESSION,
        "NIFTY26908100CE",
        [_tick("NIFTY26908100CE", 222, when=CLOSE_IST, last_price=6.5, volume=70, oi=1200)],
    )
    read = SqliteUiReadModel(settings)
    read.policy = MarketDataPolicy(settings, read.store, now_fn=lambda: SUNDAY_IST)
    chain = read.option_chain("NIFTY", "2026-09-08")["chain"]
    ce = chain["strikes"][0]["ce"]
    assert ce["oi"] == 1200
    assert ce["oi_change"] is None
    assert ce["oi_change_pct"] is None
    assert chain["largest_ce_oi_increase"] is None


# --- market activity / unusual activity ------------------------------------


def test_market_activity_visible_from_last_session(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.market_activity("HDFCBANK")
    assert payload["found"] is True
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert payload["data_state"]["session_date"] == LAST_SESSION
    assert payload["activity"]["price"] == 2000.5
    assert payload["activity"]["volume_delta"] == 500
    # No activity_samples baseline exists, so levels stay null rather than guessed.
    assert payload["activity"]["baseline_available"] is False
    assert payload["activity"]["volume_level"] is None
    assert payload["activity"]["large_trade"] is None


def test_unusual_activity_ranks_last_session_observations(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.unusual_activity(limit=10)
    rows = payload["unusual_activity"]
    assert rows, "historical observations should still rank"
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    for row in rows:
        assert row["data_state"]["data_status"] == STATUS_LAST_SESSION
        assert row["data_state"]["session_date"] == LAST_SESSION
        assert row["activity_score"] > 0


def test_unusual_historical_candidate_cap(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.analytics.unusual_historical_max_candidates = 2
    settings.analytics.unusual_historical_candidate_multiplier = 2
    SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    earlier = CLOSE_IST - timedelta(minutes=1)
    for i, symbol in enumerate(("AAA", "BBB", "CCC", "DDD")):
        _write_ticks(
            settings,
            LAST_SESSION,
            symbol,
            [
                _tick(symbol, 1000 + i, when=earlier, last_price=10.0, volume=10, oi=100),
                _tick(symbol, 1000 + i, when=CLOSE_IST, last_price=11.0, volume=20, oi=140),
            ],
        )
    read = SqliteUiReadModel(settings)
    read.policy = MarketDataPolicy(settings, read.store, now_fn=lambda: SUNDAY_IST)
    rows = read.unusual_activity(limit=10)["unusual_activity"]
    # hist_limit = min(2, 10*2) = 2. No F&O/watchlist names, so AAA, BBB.
    assert {row["symbol"] for row in rows} <= {"AAA", "BBB"}
    assert len(rows) <= 2
    for row in rows:
        assert row["data_state"]["data_status"] == STATUS_LAST_SESSION
        assert row["activity_score"] > 0


def test_read_last_rows_can_omit_depth_arrays(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    _write_ticks(
        settings,
        LAST_SESSION,
        "HDFCBANK",
        [
            _tick("HDFCBANK", 341249, when=CLOSE_IST - timedelta(minutes=1), last_price=10.0, volume=10),
            _tick("HDFCBANK", 341249, when=CLOSE_IST, last_price=11.0, volume=20),
        ],
    )
    with DuckDBTickStore(settings) as duck:
        with_depth = duck.read_last_rows(LAST_SESSION, ["HDFCBANK"], include_depth=True)
        without = duck.read_last_rows(LAST_SESSION, ["HDFCBANK"], include_depth=False)
    assert "bid_prices" in with_depth.columns
    assert "ask_quantities" in with_depth.columns
    assert "bid_prices" not in without.columns
    assert "ask_prices" not in without.columns
    assert "last_price" in without.columns
    assert "oi" in without.columns


def test_unusual_historical_prefers_fo_and_watchlist_names(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.analytics.unusual_historical_max_candidates = 2
    settings.analytics.unusual_historical_candidate_multiplier = 1
    settings.analytics.watchlist_default = ["HDFCBANK"]
    SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    earlier = CLOSE_IST - timedelta(minutes=1)
    for symbol, token, oi0, oi1 in (
        ("AAA", 1, 10, 20),
        ("HDFCBANK", 2, 10, 20),
        ("ZZZFUT", 3, 10, 20),
    ):
        _write_ticks(
            settings,
            LAST_SESSION,
            symbol,
            [
                _tick(symbol, token, when=earlier, last_price=10.0, volume=10, oi=oi0),
                _tick(symbol, token, when=CLOSE_IST, last_price=11.0, volume=20, oi=oi1),
            ],
        )
    read = SqliteUiReadModel(settings)
    read.policy = MarketDataPolicy(settings, read.store, now_fn=lambda: SUNDAY_IST)
    symbols = {row["symbol"] for row in read.unusual_activity(limit=2)["unusual_activity"]}
    assert "AAA" not in symbols
    assert symbols <= {"HDFCBANK", "ZZZFUT"}


def test_unusual_live_path_ignores_historical_candidate_cap(tmp_path: Path) -> None:
    settings, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST)
    settings.analytics.unusual_historical_max_candidates = 1
    fresh = MONDAY_OPEN_IST - timedelta(seconds=3)
    store.upsert_latest_quotes(
        [
            _live_row("AAA", 1, when=fresh, last_price=10.0),
            _live_row("BBB", 2, when=fresh, last_price=11.0),
            _live_row("CCC", 3, when=fresh, last_price=12.0),
        ]
    )
    # Live ranking still walks latest_quotes (cap 500), not the historical 1.
    snaps = read.policy.all_snapshots(limit=500, historical_limit=1)
    assert set(snaps) == {"AAA", "BBB", "CCC"}
    assert all(snap.state.data_status == STATUS_LIVE for snap in snaps.values())


# --- H: charts --------------------------------------------------------------


def test_charts_keep_historical_candles_after_close(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    payload = read.charts("HDFCBANK", interval="1m")
    chart = payload["chart"]
    assert payload["found"] is True
    assert chart["candles_status"] == "partial"
    assert chart["candles_source"] == "bars_1m"
    assert chart["candles"]
    assert chart["source"] == "bars_1m"
    assert payload["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert payload["data_state"]["session_date"] == LAST_SESSION
    assert payload["data_state"]["as_of"] is not None
    assert payload["data_state"]["as_of"].startswith("2026-09-04T")
    stamps = [candle["timestamp"] for candle in chart["candles"]]
    assert all(stamp.startswith("2026-09-04") for stamp in stamps)


# --- I: maturity untouched --------------------------------------------------


def test_maturity_suppression_unchanged_by_historical_fallback(tmp_path: Path) -> None:
    bare_settings = _settings(tmp_path / "bare")
    SQLiteStore(bare_settings.paths.sqlite_db)
    baseline = SqliteUiReadModel(bare_settings).maturity()["maturity"]

    _settings_obj, _store, read = _read_model(tmp_path / "seeded", now=SUNDAY_IST)
    read.quote("HDFCBANK")
    read.option_chain("NIFTY", "2026-09-08")
    after = read.maturity()["maturity"]

    for class_key in ("equity", "options_nifty", "options_banknifty", "futures"):
        assert after[class_key]["display"] == baseline[class_key]["display"]
        assert after[class_key]["pooled_live_days"] == baseline[class_key]["pooled_live_days"]
        assert after[class_key]["tier"] == baseline[class_key]["tier"]
        assert after[class_key]["probability_permitted"] is False
        assert "insufficient data" in after[class_key]["display"]
        assert "/60 pooled days" in after[class_key]["display"]


# --- J: envelope compatibility ---------------------------------------------


def test_existing_envelope_survives_and_gains_data_state(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    _seed_last_session(settings)
    client = TestClient(create_app(settings))
    for path in (
        "/api/v1/overview",
        "/api/v1/health",
        "/api/v1/quotes/HDFCBANK",
        "/api/v1/options/NIFTY",
        "/api/v1/options/NIFTY/2026-09-08/oi",
        "/api/v1/futures/NIFTY",
        "/api/v1/market-activity/HDFCBANK",
        "/api/v1/unusual-activity",
        "/api/v1/charts/HDFCBANK",
        "/api/v1/watchlists/quotes",
        "/api/v1/cross-market/NIFTY",
    ):
        response = client.get(path)
        assert response.status_code == 200, path
        payload = response.json()
        assert "maturity" in payload, path
        assert "as_of" in payload, path
        state = payload.get("data_state")
        assert state is not None, path
        assert state["data_status"] in {STATUS_LIVE, STATUS_LAST_SESSION, STATUS_NO_DATA}, path
        assert state["market_state"] in {
            MARKET_OPEN,
            MARKET_CLOSED,
            MARKET_PRE_OPEN,
            MARKET_POST_CLOSE,
            MARKET_WEEKEND,
        }, path
        # Envelope as_of stays the response time; data_state.as_of is the data time.
        assert payload["as_of"] != state["as_of"], path
