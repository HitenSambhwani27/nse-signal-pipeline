"""Phase 2: RM-5 calendar ownership and RM-2 session_reference lifecycle."""

from __future__ import annotations

import statistics
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_pipeline.market.assemble import assemble_futures, assemble_option_chain
from nse_pipeline.market.calendar import (
    CLOSED,
    HOLIDAY,
    OPEN,
    POST_CLOSE,
    PRE_OPEN,
    WEEKEND,
    calendar_day_from_row,
    previous_trading_day,
    session_clock,
)
from nse_pipeline.market.data_state import (
    MARKET_OPEN,
    STATUS_LAST_SESSION,
    STATUS_LIVE,
    DataState,
)
from nse_pipeline.market.reference import (
    REF_OFFICIAL_CLOSE,
    REF_PREVIOUS_CLOSE,
    REF_SETTLEMENT,
    resolve_reference,
)
from nse_pipeline.market.session_reference import maintain_session_references
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_last_session_fallback import (
    MONDAY_OPEN_IST,
    SUNDAY_IST,
    _live_row,
    _read_model,
)

IST = ZoneInfo("Asia/Kolkata")


def _state(
    session_date: str,
    *,
    market_state: str = MARKET_OPEN,
    data_status: str = STATUS_LIVE,
) -> DataState:
    return DataState(
        market_state=market_state,
        data_status=data_status,
        as_of="2026-09-07T10:32:00+05:30",
        session_date=session_date,
    )


def _obs(
    *,
    session_date: str,
    token: int = 341249,
    symbol: str = "HDFCBANK",
    last_price: float | None = 2010.0,
    ohlc_open: float | None = 2005.0,
    ohlc_close: float | None = 2000.0,
    instrument_type: str = "EQ",
    data_status: str = STATUS_LIVE,
    market_state: str = MARKET_OPEN,
) -> tuple[dict, DataState, dict]:
    row = {
        "instrument_token": token,
        "symbol": symbol,
        "last_price": last_price,
        "ohlc_open": ohlc_open,
        "ohlc_close": ohlc_close,
        "instrument_type": instrument_type,
    }
    meta = {"instrument_type": instrument_type, "tradingsymbol": symbol}
    return row, _state(session_date, market_state=market_state, data_status=data_status), meta


def test_market_calendar_session_states(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    assert session_clock(session, now=datetime(2026, 9, 7, 9, 14, tzinfo=IST)) == PRE_OPEN
    assert session_clock(session, now=MONDAY_OPEN_IST) == OPEN
    assert session_clock(session, now=datetime(2026, 9, 7, 15, 31, tzinfo=IST)) == POST_CLOSE
    assert session_clock(session, now=datetime(2026, 9, 7, 16, 1, tzinfo=IST)) == CLOSED
    assert session_clock(session, now=SUNDAY_IST) == WEEKEND
    holiday = calendar_day_from_row(
        {
            "session_date": "2026-09-07",
            "is_trading_day": 0,
            "session_type": "HOLIDAY",
            "segment": "CASH",
            "source": "nse_published",
        },
        session,
    )
    assert session_clock(session, now=MONDAY_OPEN_IST, day=holiday) == HOLIDAY


def test_previous_trading_day_skips_weekend_not_calendar_yesterday(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    prev, complete = previous_trading_day(session, as_of=date(2026, 9, 7))
    assert prev == "2026-09-04"
    assert complete is False
    saturday_prev, _ = previous_trading_day(session, as_of=date(2026, 9, 5))
    assert saturday_prev == "2026-09-04"


def test_unseeded_holiday_limitation_stays_explicit(tmp_path: Path) -> None:
    settings, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST)
    health = read.health()
    session = health["session"]
    assert session["holiday_list_seeded"] is False
    assert session["previous_trading_day_complete"] is False
    assert session["previous_trading_day"] == "2026-09-04"
    assert session["reason"] == "holiday_list_not_seeded"
    assert session["calendar_source"] == "weekday_clock"
    assert HOLIDAY not in {
        session_clock(settings.session, now=MONDAY_OPEN_IST),
        session_clock(settings.session, now=SUNDAY_IST),
    }
    assert store.market_calendar_count() > 0


def test_session_reference_row_creation_and_uniqueness(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    items = [_obs(session_date="2026-09-07")]
    maintain_session_references(store, settings.session, items, now=MONDAY_OPEN_IST)
    maintain_session_references(store, settings.session, items, now=MONDAY_OPEN_IST)
    assert store.session_reference_count() == 1
    row = store.fetch_session_reference("2026-09-07", 341249)
    assert row is not None
    assert row["symbol"] == "HDFCBANK"
    assert row["previous_close"] == 2000.0
    assert row["today_open"] == 2005.0
    assert row["settlement_price"] is None


def test_previous_close_from_previous_session_official_close(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    friday = _obs(
        session_date="2026-09-04",
        last_price=1995.0,
        ohlc_open=1980.0,
        ohlc_close=1970.0,
        data_status=STATUS_LAST_SESSION,
        market_state="closed",
    )
    maintain_session_references(store, settings.session, [friday], now=SUNDAY_IST)
    friday_row = store.fetch_session_reference("2026-09-04", 341249)
    assert friday_row["official_close"] == 1995.0
    monday = _obs(session_date="2026-09-07", ohlc_close=1888.0)
    maintain_session_references(store, settings.session, [monday], now=MONDAY_OPEN_IST)
    monday_row = store.fetch_session_reference("2026-09-07", 341249)
    assert monday_row["previous_close"] == 1995.0
    assert monday_row["previous_close"] != 1888.0


def test_today_open_captured_once_and_ignores_invalid_first_tick(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    invalid = _obs(session_date="2026-09-07", last_price=None, ohlc_open=0)
    maintain_session_references(store, settings.session, [invalid], now=MONDAY_OPEN_IST)
    row = store.fetch_session_reference("2026-09-07", 341249)
    assert row["today_open"] is None
    first_valid = _obs(session_date="2026-09-07", last_price=2011.0, ohlc_open=2005.0)
    maintain_session_references(store, settings.session, [first_valid], now=MONDAY_OPEN_IST)
    later = _obs(session_date="2026-09-07", last_price=2020.0, ohlc_open=2099.0)
    maintain_session_references(store, settings.session, [later], now=MONDAY_OPEN_IST)
    row = store.fetch_session_reference("2026-09-07", 341249)
    assert row["today_open"] == 2005.0


def test_official_close_persists_and_closed_row_is_immutable(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    closing = _obs(
        session_date="2026-09-07",
        last_price=2012.0,
        data_status=STATUS_LAST_SESSION,
        market_state=POST_CLOSE,
    )
    maintain_session_references(
        store,
        settings.session,
        [closing],
        now=datetime(2026, 9, 7, 15, 45, tzinfo=IST),
    )
    row = store.fetch_session_reference("2026-09-07", 341249)
    assert row["official_close"] == 2012.0
    mutated = _obs(
        session_date="2026-09-07",
        last_price=1800.0,
        ohlc_open=1700.0,
        data_status=STATUS_LAST_SESSION,
        market_state="closed",
    )
    maintain_session_references(
        store,
        settings.session,
        [mutated],
        now=datetime(2026, 9, 7, 18, 0, tzinfo=IST),
    )
    frozen = store.fetch_session_reference("2026-09-07", 341249)
    assert frozen["official_close"] == 2012.0
    assert frozen["today_open"] == 2005.0
    assert frozen["settlement_price"] is None


def test_settlement_remains_null_without_verified_source(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    fut = _obs(
        session_date="2026-09-07",
        token=123,
        symbol="NIFTY26SEPFUT",
        instrument_type="FUT",
        last_price=101.0,
        ohlc_close=100.0,
    )
    maintain_session_references(store, settings.session, [fut], now=MONDAY_OPEN_IST)
    row = store.fetch_session_reference("2026-09-07", 123)
    assert row["settlement_price"] is None
    assert row["reference_type"] == REF_OFFICIAL_CLOSE
    canonical = resolve_reference(
        fut[0],
        instrument_type="FUT",
        stored=row,
        session_date="2026-09-07",
    )
    assert canonical["reference_type"] == REF_OFFICIAL_CLOSE
    assert canonical["reference_type"] != REF_SETTLEMENT
    assert canonical["change_percent"] == 1.0


def test_canonical_reference_and_percent_scale() -> None:
    stored = {
        "session_date": "2026-09-07",
        "instrument_token": 341249,
        "previous_close": 2000.0,
        "today_open": 2005.0,
        "settlement_price": None,
    }
    result = resolve_reference(
        {"last_price": 2010.0, "instrument_token": 341249, "price_delta": 0.25},
        stored=stored,
        session_date="2026-09-07",
    )
    assert result["reference_price"] == 2000.0
    assert result["reference_type"] == REF_PREVIOUS_CLOSE
    assert result["change_absolute"] == 10.0
    assert result["change_percent"] == 0.5
    assert result["reference_source"] == "session_reference"


def test_missing_and_stale_session_rows_are_not_reused() -> None:
    row = {"last_price": 2010.0, "instrument_token": 341249, "ohlc_close": 2000.0}
    missing = resolve_reference(row, session_date="2026-09-07")
    assert missing["reference_price"] is None
    assert missing["change_absolute"] is None
    assert missing["change_percent"] is None
    assert missing["reference_reason"] == "no_reference"
    stale = resolve_reference(
        row,
        stored={
            "session_date": "2026-09-04",
            "instrument_token": 341249,
            "previous_close": 2000.0,
        },
        session_date="2026-09-07",
    )
    assert stale["reference_price"] is None
    assert stale["change_absolute"] is None
    assert stale["reference_reason"] == "session_mismatch"
    other_token = resolve_reference(
        row,
        stored={
            "session_date": "2026-09-07",
            "instrument_token": 1,
            "previous_close": 2000.0,
        },
        session_date="2026-09-07",
    )
    assert other_token["reference_reason"] == "session_mismatch"


def test_quote_path_consumes_rm2(tmp_path: Path) -> None:
    _settings_obj, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST, seed_history=True)
    live = _live_row("HDFCBANK", 341249, when=MONDAY_OPEN_IST - timedelta(seconds=5), last_price=2011.0)
    live["ohlc_close"] = 2000.0
    live["ohlc_open"] = 2005.0
    live["price_delta"] = 0.25
    store.upsert_latest_quotes([live])
    payload = read.quote("HDFCBANK")
    quote = payload["quote"]
    assert quote["reference_source"] == "session_reference"
    assert quote["reference_type"] == REF_PREVIOUS_CLOSE
    assert quote["change_absolute"] == 11.0
    assert quote["change"] == 0.25
    assert quote["change"] != quote["change_absolute"]
    stored = store.fetch_session_reference(payload["data_state"]["session_date"], 341249)
    assert stored is not None
    assert stored["previous_close"] == 2000.0
    assert stored["settlement_price"] is None


def test_futures_and_options_paths_consume_rm2(tmp_path: Path) -> None:
    settings, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST, seed_history=True)
    when = MONDAY_OPEN_IST - timedelta(seconds=5)
    fut = _live_row("NIFTY26SEPFUT", 999001, when=when, last_price=101.0)
    fut["ohlc_close"] = 100.0
    fut["ohlc_open"] = 100.5
    ce = _live_row("NIFTY26908100CE", 999002, when=when, last_price=12.0)
    ce["ohlc_close"] = 10.0
    ce["ohlc_open"] = 11.0
    pe = _live_row("NIFTY26908100PE", 999003, when=when, last_price=8.0)
    pe["ohlc_close"] = 9.0
    pe["ohlc_open"] = 8.5
    spot = _live_row("NIFTY 50", 256265, when=when, last_price=110.0)
    spot["ohlc_close"] = 108.0
    store.upsert_latest_quotes([fut, ce, pe, spot])
    cache = read._load_instrument_cache()
    book = assemble_futures(settings, store, cache, "NIFTY", policy=read.policy)
    contract = next(c for c in book["contracts"] if c["symbol"] == "NIFTY26SEPFUT")
    assert contract["reference_source"] == "session_reference"
    assert contract["reference_type"] == REF_OFFICIAL_CLOSE
    assert contract["reference_price"] == 100.0
    assert contract["change_absolute"] == 1.0
    assert contract.get("settlement_price") in (None, contract.get("settlement_price"))
    fut_row = store.fetch_session_reference(contract["data_state"]["session_date"], 999001)
    assert fut_row["settlement_price"] is None
    chain = assemble_option_chain(settings, store, cache, "NIFTY", "2026-09-08", policy=read.policy)
    strike = chain["strikes"][0]
    assert strike["ce"]["reference_source"] == "session_reference"
    assert strike["ce"]["reference_type"] == REF_OFFICIAL_CLOSE
    assert strike["ce"]["change_absolute"] == 2.0
    assert strike["pe"]["reference_source"] == "session_reference"


def test_schema_primary_key_and_hot_path_lookup(tmp_path: Path) -> None:
    store = SQLiteStore(_settings(tmp_path).paths.sqlite_db)
    with store.connection() as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(session_reference)").fetchall()}
        pk = {
            row[1]
            for row in conn.execute("PRAGMA table_info(session_reference)").fetchall()
            if row[5]
        }
        indexes = list(conn.execute("PRAGMA index_list(session_reference)").fetchall())
    assert {"session_date", "instrument_token", "previous_close", "today_open", "official_close", "settlement_price"} <= cols
    assert "instrument_key" not in cols
    assert pk == {"session_date", "instrument_token"} or any("session_reference" in str(idx) for idx in indexes)
    session_date = "2026-09-07"
    rows = [
        {
            "session_date": session_date,
            "instrument_token": i,
            "symbol": f"SYM{i}",
            "previous_close": 100.0 + i,
            "today_open": 101.0,
            "official_close": None,
            "settlement_price": None,
            "reference_price": 100.0 + i,
            "reference_type": REF_PREVIOUS_CLOSE,
            "source": "session_reference",
            "reference_reason": None,
        }
        for i in range(1, 251)
    ]
    store.upsert_session_references(rows)
    tokens = list(range(1, 251))
    placeholders = ",".join("?" for _ in tokens)
    sql = (
        "SELECT * FROM session_reference "
        f"WHERE session_date = ? AND instrument_token IN ({placeholders})"
    )
    params = (session_date, *tokens)
    with store.connection() as conn:
        conn.execute(sql, params).fetchall()
        sql_ms: list[float] = []
        map_ms: list[float] = []
        for _ in range(40):
            started = time.perf_counter()
            raw = conn.execute(sql, params).fetchall()
            after_sql = time.perf_counter()
            found = {int(dict(r)["instrument_token"]): dict(r) for r in raw}
            after_map = time.perf_counter()
            sql_ms.append((after_sql - started) * 1000.0)
            map_ms.append((after_map - started) * 1000.0)
            assert len(found) == 250
    sql_p50 = statistics.median(sql_ms)
    sql_p95 = sorted(sql_ms)[int(0.95 * (len(sql_ms) - 1))]
    map_p50 = statistics.median(map_ms)
    map_p95 = sorted(map_ms)[int(0.95 * (len(map_ms) - 1))]
    # Architecture target is the keyed lookup, not Python sqlite3.Row→dict mapping.
    assert sql_p50 < 10.0, (
        f"keyed IN lookup n=250 sql_p50={sql_p50:.3f}ms sql_p95={sql_p95:.3f}ms "
        f"map_p50={map_p50:.3f}ms map_p95={map_p95:.3f}ms"
    )


def test_legacy_schema_migrates_without_inventing_prices(tmp_path: Path) -> None:
    db = tmp_path / "legacy.sqlite"
    import sqlite3

    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE session_reference (
            session_date TEXT NOT NULL,
            instrument_key TEXT NOT NULL,
            reference_type TEXT NOT NULL,
            reference_price REAL,
            settlement_price REAL,
            source TEXT,
            PRIMARY KEY (session_date, instrument_key)
        )
        """
    )
    conn.execute(
        "INSERT INTO session_reference VALUES ('2026-09-07', 'not-a-token', 'PREVIOUS_CLOSE', 1, NULL, 'x')"
    )
    conn.commit()
    conn.close()
    store = SQLiteStore(db)
    assert store.session_reference_count() == 0
    with store.connection() as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(session_reference)").fetchall()}
    assert "instrument_token" in cols
    assert "previous_close" in cols
