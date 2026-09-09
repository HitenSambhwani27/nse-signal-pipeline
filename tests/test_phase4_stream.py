"""Phase 4: LiveSource, SSE, subscriptions, admission, monotonic latest_quotes."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.live.frames import changed_fields, parse_groups, parse_tokens
from nse_pipeline.live.source import SqlitePollSource
from nse_pipeline.live.stream import StreamAdmissionError, StreamRegistry
from nse_pipeline.live.subscriptions import SubscriptionManager, resolve_mode
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings

IST = ZoneInfo("Asia/Kolkata")


def _quote_row(token: int, symbol: str, last: float, when: datetime, **extra) -> dict:
    return {
        "instrument_token": token,
        "symbol": symbol,
        "exchange": "NSE",
        "timestamp": when.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "last_price": last,
        "last_quantity": 1,
        "volume": extra.get("volume", 100),
        "average_price": last,
        "oi": extra.get("oi"),
        "total_buy_quantity": 10,
        "total_sell_quantity": 8,
        "best_bid_price": last - 0.05,
        "best_bid_quantity": 2,
        "best_ask_price": last + 0.05,
        "best_ask_quantity": 3,
        "bid_depth_5": 10,
        "ask_depth_5": 9,
        "spread": 0.10,
        "mid_price": last,
        "depth_imbalance": 0.1,
        "volume_delta": extra.get("volume_delta", 1),
        "oi_delta": extra.get("oi_delta"),
        "price_delta": 0.5,
        "ohlc_open": last,
        "ohlc_high": last,
        "ohlc_low": last,
        "ohlc_close": last - 1,
        "last_trade_time": when.isoformat(),
        "bid_levels": extra.get("bid_levels"),
        "ask_levels": extra.get("ask_levels"),
    }


def test_changed_fields_only_and_group_parse() -> None:
    assert parse_groups("price,oi") == ["price", "oi"]
    assert parse_tokens("1,1,2") == [1, 2]
    prev = {"last_price": 10.0, "oi": 1}
    cur = {"last_price": 10.0, "oi": 2}
    assert changed_fields(prev, cur, ["last_price", "oi"]) == {"oi": 2}


def test_capability_mode_resolution() -> None:
    assert resolve_mode(["price", "chart"]) == "quote"
    assert resolve_mode(["price", "depth"]) == "full"
    assert resolve_mode(["oi"]) == "full"


def test_monotonic_latest_quotes_rejects_older_observation(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "m.db")
    newer = datetime(2026, 9, 9, 10, 0, tzinfo=IST)
    older = newer - timedelta(seconds=30)
    store.upsert_latest_quotes([_quote_row(408065, "HDFCBANK", 1712.4, newer)])
    store.upsert_latest_quotes([_quote_row(408065, "HDFCBANK", 1.0, older)])
    row = store.fetch_latest_quote("HDFCBANK")
    assert row is not None
    assert row["last_price"] == 1712.4
    later = store.fetch_latest_quotes_by_tokens([408065])
    assert later[0]["last_price"] == 1712.4


def test_sqlite_wal_and_busy_timeout(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "w.db", busy_timeout_ms=5000)
    with store.connection(read_only=True) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert str(mode).lower() == "wal"
    assert int(timeout) == 5000


def test_livesource_shared_poll_and_diff(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    now = datetime(2026, 9, 9, 10, 1, tzinfo=IST)
    store.upsert_latest_quotes([_quote_row(1, "AAA", 10.0, now, volume=100)])
    source = SqlitePollSource(settings, store)

    async def run() -> list[dict]:
        sub = await source.subscribe({1}, ["price", "volume"])
        await source._poll_once()
        first = []
        while not sub.queue.empty():
            first.append(sub.queue.get_nowait())
        store.upsert_latest_quotes([_quote_row(1, "AAA", 10.0, now, volume=150)])
        await source._poll_once()
        second = []
        while not sub.queue.empty():
            second.append(sub.queue.get_nowait())
        await source.unsubscribe(sub)
        return first, second

    first, second = asyncio.run(run())
    ticks1 = [e for e in first if e["event"] == "tick"]
    ticks2 = [e for e in second if e["event"] == "tick"]
    assert ticks1
    assert ticks1[0]["data"]["seq"] == 1
    assert "ltp" in ticks1[0]["data"]
    assert len(source._subscribers) == 0
    assert ticks2
    assert "vol" in ticks2[0]["data"]
    assert "ltp" not in ticks2[0]["data"]
    assert ticks2[0]["data"]["seq"] == 2


def test_stream_hello_heartbeat_headers_and_caps(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    store.upsert_latest_quotes(
        [_quote_row(408065, "HDFCBANK", 1712.4, datetime(2026, 9, 9, 10, 2, tzinfo=IST))]
    )
    app = create_app(settings)
    with TestClient(app) as client:
        too_many = ",".join(str(i) for i in range(1, 252))
        bad = client.get(f"/api/v1/stream?tokens={too_many}")
        assert bad.status_code == 400
        assert bad.json()["reason"] == "instrument_limit_exceeded"

        registry: StreamRegistry = app.state.stream_registry
        registry._current = 16
        full = client.get("/api/v1/stream?tokens=408065")
        assert full.status_code == 503
        assert full.headers.get("retry-after") == "5"
        assert full.json()["reason"] == "stream_capacity_exceeded"
        registry._current = 0

        health = client.get("/api/v1/health").json()
        assert "stream" in health["health"]
        assert "sqlite_busy_retries" in health["health"]
        assert "current_connections" in health["health"]["stream"]

    source = SqlitePollSource(settings, store)
    registry = StreamRegistry(settings, source)

    async def collect() -> str:
        from nse_pipeline.live.stream import stream_events

        agen = stream_events(
            registry=registry,
            source=source,
            tokens=[408065],
            groups=["price"],
            last_event_id="999999",
            session={"market_state": "open", "session_date": "2026-09-09"},
            rejected=[],
        )
        chunks = [await agen.__anext__(), await agen.__anext__()]
        await agen.aclose()
        return b"".join(chunks).decode("utf-8")

    text = asyncio.run(collect())
    assert "event: hello" in text
    assert "schema_version" in text
    assert "408065" in text
    assert "event: resync" in text
    assert "stream_process_restart" in text or "unavailable_continuity" in text


def test_subscription_refcount_ttl_lru_and_static(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.broker_limits.max_total_subscriptions = 3
    store = SQLiteStore(settings.paths.sqlite_db)
    mgr = SubscriptionManager(settings, store, static_full=[1], static_quote=[2])

    class FakeTicker:
        MODE_FULL = "full"
        MODE_QUOTE = "quote"

        def __init__(self) -> None:
            self.subscribed: list[int] = []
            self.unsubscribed: list[int] = []
            self.modes: list[tuple[str, list[int]]] = []

        def subscribe(self, tokens):
            self.subscribed.extend(tokens)

        def unsubscribe(self, tokens):
            self.unsubscribed.extend(tokens)

        def set_mode(self, mode, tokens):
            self.modes.append((mode, list(tokens)))

    ticker = FakeTicker()
    for requester in ("a", "b", "c"):
        req = mgr.validate_request(
            token=9,
            symbol=None,
            requester=requester,
            capabilities=["price"],
            action="subscribe",
            ttl_seconds=300,
        )
        mgr.enqueue(req)
    result = mgr.reconcile(ticker)
    assert 9 in result["added"]
    mgr.enqueue(
        mgr.validate_request(
            token=9, symbol=None, requester="a", capabilities=["price"], action="release", ttl_seconds=300
        )
    )
    mgr.enqueue(
        mgr.validate_request(
            token=9, symbol=None, requester="b", capabilities=["price"], action="release", ttl_seconds=300
        )
    )
    mgr.reconcile(ticker)
    assert 9 in mgr.dynamic_quote
    mgr.enqueue(
        mgr.validate_request(
            token=9, symbol=None, requester="c", capabilities=["price"], action="release", ttl_seconds=300
        )
    )
    mgr.reconcile(ticker)
    assert 9 not in mgr.dynamic_quote

    # Budget 3 = static 1,2 + one dynamic. Second dynamic evicts LRU, never static.
    mgr.enqueue(
        mgr.validate_request(
            token=20, symbol=None, requester="x", capabilities=["depth"], action="subscribe", ttl_seconds=1
        )
    )
    mgr.enqueue(
        mgr.validate_request(
            token=21, symbol=None, requester="y", capabilities=["price"], action="subscribe", ttl_seconds=300
        )
    )
    mgr.reconcile(ticker)
    assert 1 in mgr.static_all
    assert 2 in mgr.static_all
    assert not ({20, 21} <= (mgr.dynamic_full | mgr.dynamic_quote))
    events = store.fetch_ingestion_events() if hasattr(store, "fetch_ingestion_events") else []
    _ = events
    rows = store.fetch_subscription_requests()
    assert any(r.get("status") in {"evicted", "applied", "expired", "pending"} for r in rows)


def test_subscription_api_and_health_fields(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        denied = client.post(
            "/api/v1/subscriptions",
            json={"token": 5, "action": "subscribe", "capabilities": ["price"]},
        )
        assert denied.status_code == 400
        ok = client.post(
            "/api/v1/subscriptions",
            json={
                "token": 5,
                "requester": "panel:workspace",
                "action": "subscribe",
                "capabilities": ["depth"],
                "ttl_seconds": 300,
            },
        )
        assert ok.status_code == 200
        body = ok.json()
        assert body["ok"] is True
        assert body["mode"] == "full"
        store = SQLiteStore(settings.paths.sqlite_db)
        rows = store.fetch_subscription_requests()
        assert rows
        assert rows[0]["token"] == 5


def test_admission_16_then_17(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = SqlitePollSource(settings, SQLiteStore(settings.paths.sqlite_db))
    registry = StreamRegistry(settings, source)

    async def run() -> None:
        for _ in range(16):
            await registry.acquire()
        try:
            await registry.acquire()
            raise AssertionError("17th must fail")
        except StreamAdmissionError as exc:
            assert exc.status == 503
            assert exc.retry_after == 5
        await registry.release()
        await registry.acquire()
        assert registry.current == 16

    asyncio.run(run())
