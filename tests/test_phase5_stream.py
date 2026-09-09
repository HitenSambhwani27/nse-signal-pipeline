"""Phase 5: reconnect/resync, Last-Event-ID, ingest generation, replay, 250-cap."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.live.replay import (
    REQUIRED_SCENARIOS,
    StreamIntegrity,
    assert_scenario,
    parse_sse_chunk,
    play_scenario,
)
from nse_pipeline.live.source import SqlitePollSource
from nse_pipeline.live.stream import StreamAdmissionError, StreamRegistry, stream_events
from nse_pipeline.live.subscriptions import SubscriptionManager
from nse_pipeline.market.latest import LatestQuoteTracker
from nse_pipeline.storage.schemas import NormalizedTick
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_phase4_stream import _quote_row

IST = ZoneInfo("Asia/Kolkata")


def test_replay_harness_required_scenarios() -> None:
    for name in REQUIRED_SCENARIOS:
        report = play_scenario(name)
        assert_scenario(name, report)
        assert report.to_dict()["kind"] == "replay_fixture"


def test_last_event_id_same_process_continuity(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = SqlitePollSource(settings, SQLiteStore(settings.paths.sqlite_db))
    registry = StreamRegistry(settings, source)
    source.seq = 7

    async def collect() -> str:
        agen = stream_events(
            registry=registry,
            source=source,
            tokens=[1],
            groups=["price"],
            last_event_id="7",
            session={"market_state": "open", "session_date": "2026-09-09"},
            rejected=[],
        )
        chunks = [await agen.__anext__(), await agen.__anext__()]
        await agen.aclose()
        return b"".join(chunks).decode("utf-8")

    text = asyncio.run(collect())
    assert "event: hello" in text
    assert "event: resync" not in text


def test_last_event_id_unavailable_continuity_and_process_restart(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = SqlitePollSource(settings, SQLiteStore(settings.paths.sqlite_db))
    registry = StreamRegistry(settings, source)
    source.seq = 7

    async def collect(last_id: str) -> str:
        agen = stream_events(
            registry=registry,
            source=source,
            tokens=[1],
            groups=["price"],
            last_event_id=last_id,
            session={"market_state": "open", "session_date": "2026-09-09"},
            rejected=[],
        )
        chunks = [await agen.__anext__(), await agen.__anext__()]
        await agen.aclose()
        return b"".join(chunks).decode("utf-8")

    gap = asyncio.run(collect("3"))
    assert "unavailable_continuity" in gap
    restarted = asyncio.run(collect("99"))
    assert "stream_process_restart" in restarted


def test_ingestion_generation_change_emits_resync(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    now = datetime(2026, 9, 9, 10, 5, tzinfo=IST)
    store.upsert_latest_quotes([_quote_row(1, "AAA", 10.0, now)])
    store.log_ingestion_event("startup", message="g1")
    source = SqlitePollSource(settings, store)

    async def run() -> list[dict]:
        sub = await source.subscribe({1}, ["price"])
        await source._poll_once()
        first = []
        while not sub.queue.empty():
            first.append(sub.queue.get_nowait())
        store.log_ingestion_event("startup", message="g2")
        await source._poll_once()
        second = []
        while not sub.queue.empty():
            second.append(sub.queue.get_nowait())
        await source.unsubscribe(sub)
        return first, second

    first, second = asyncio.run(run())
    assert any(e["event"] == "tick" for e in first)
    resyncs = [e for e in second if e["event"] == "resync"]
    assert resyncs
    assert resyncs[0]["data"]["reason"] == "ingestion_restart"


def test_warm_start_first_observation_has_null_deltas() -> None:
    tracker = LatestQuoteTracker()
    ts = datetime(2026, 9, 9, 10, 10, tzinfo=IST)
    first = NormalizedTick(
        timestamp=ts,
        instrument_token=341249,
        symbol="HDFCBANK",
        exchange="NSE",
        last_price=1712.4,
        volume=1000,
        last_quantity=1,
        average_price=1712.4,
        oi=None,
        ingested_at=datetime.now(timezone.utc),
    )
    snap = tracker.observe(first)
    assert snap["volume_delta"] is None
    later = NormalizedTick(
        timestamp=ts + timedelta(seconds=1),
        instrument_token=341249,
        symbol="HDFCBANK",
        exchange="NSE",
        last_price=1712.6,
        volume=1010,
        last_quantity=1,
        average_price=1712.5,
        oi=None,
        ingested_at=datetime.now(timezone.utc),
    )
    snap2 = tracker.observe(later)
    assert snap2["volume_delta"] == 10


def test_partial_session_from_warm_start(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    source = SqlitePollSource(settings, store)
    view = source._session_snapshot()
    session_date = str(view["session_date"])
    assert view.get("coverage") is None
    store.log_ingestion_event(
        "warm_start",
        message="Ingestion started after market open",
        details={"session_date": session_date},
    )
    labeled = source._session_snapshot()
    assert labeled["coverage"] == "PARTIAL_SESSION"
    assert store.session_coverage_label(session_date) == "PARTIAL_SESSION"


def test_dynamic_subscription_survives_manager_restart(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    first = SubscriptionManager(settings, store, static_full=[1], static_quote=[2])
    pending = first.enqueue(
        first.validate_request(
            token=99,
            symbol=None,
            requester="phase5",
            capabilities=["price"],
            action="subscribe",
            ttl_seconds=300,
        )
    )
    assert pending["status"] == "pending"
    rows = store.fetch_subscription_requests()
    assert rows[0]["status"] == "pending"

    class FakeTicker:
        MODE_FULL = "full"
        MODE_QUOTE = "quote"

        def __init__(self) -> None:
            self.subscribed: list[int] = []
            self.modes: list[tuple[str, list[int]]] = []

        def subscribe(self, tokens):
            self.subscribed.extend(tokens)

        def unsubscribe(self, tokens):
            return None

        def set_mode(self, mode, tokens):
            self.modes.append((mode, list(tokens)))

    restarted = SubscriptionManager(settings, store, static_full=[1], static_quote=[2])
    ticker = FakeTicker()
    result = restarted.reconcile(ticker)
    assert 99 in result["added"]
    assert 99 in ticker.subscribed
    applied = store.fetch_subscription_requests()
    assert applied[0]["status"] == "applied"
    assert any(mode == "quote" for mode, _ in ticker.modes)
    assert 1 in restarted.static_all
    assert 2 in restarted.static_all


def test_canonical_change_on_stream_matches_reference(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    now = datetime(2026, 9, 9, 10, 20, tzinfo=IST)
    store.upsert_latest_quotes([_quote_row(408065, "HDFCBANK", 1712.4, now)])
    store.upsert_session_references(
        [
            {
                "session_date": "2026-09-09",
                "instrument_token": 408065,
                "symbol": "HDFCBANK",
                "previous_close": 1700.0,
                "reference_price": 1700.0,
                "reference_type": "PREVIOUS_CLOSE",
            }
        ]
    )
    source = SqlitePollSource(settings, store)
    source._session = {
        "market_state": "open",
        "session_date": "2026-09-09",
        "data_status": "live",
        "coverage": None,
    }

    async def run() -> dict:
        sub = await source.subscribe({408065}, ["price"])
        await source._poll_once()
        ticks = []
        while not sub.queue.empty():
            item = sub.queue.get_nowait()
            if item["event"] == "tick":
                ticks.append(item["data"])
        await source.unsubscribe(sub)
        return ticks[0]

    frame = asyncio.run(run())
    assert abs(float(frame["chg"]) - 12.4) < 1e-6
    assert abs(float(frame["chgp"]) - ((1712.4 - 1700.0) / 1700.0 * 100.0)) < 1e-6
    assert frame["reft"] == "PREVIOUS_CLOSE"


def test_250_instrument_livesource_and_251_rejected(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    now = datetime(2026, 9, 9, 10, 30, tzinfo=IST)
    store.upsert_latest_quotes(
        [_quote_row(i, f"S{i}", 10.0 + i / 100.0, now) for i in range(1, 251)]
    )
    source = SqlitePollSource(settings, store)
    registry = StreamRegistry(settings, source)

    async def run() -> int:
        sub = await source.subscribe(set(range(1, 251)), ["price"])
        await source._poll_once()
        n = 0
        while not sub.queue.empty():
            item = sub.queue.get_nowait()
            if item["event"] == "tick":
                n += 1
        await source.unsubscribe(sub)
        return n

    ticks = asyncio.run(run())
    assert ticks == 250
    try:
        registry.validate_tokens(",".join(str(i) for i in range(1, 252)))
        raise AssertionError("251 must fail")
    except StreamAdmissionError as exc:
        assert exc.reason == "instrument_limit_exceeded"


def test_admission_releases_once_per_connection(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = SqlitePollSource(settings, SQLiteStore(settings.paths.sqlite_db))
    registry = StreamRegistry(settings, source)
    original = registry.release
    releases = {"n": 0}

    async def counted() -> None:
        releases["n"] += 1
        await original()

    registry.release = counted  # type: ignore[method-assign]

    async def run() -> None:
        await registry.acquire()
        agen = stream_events(
            registry=registry,
            source=source,
            tokens=[1],
            groups=["price"],
            last_event_id=None,
            session={"market_state": "open"},
            rejected=[],
        )
        await agen.__anext__()
        await agen.aclose()

    asyncio.run(run())
    assert releases["n"] == 1
    assert registry.current == 0


def test_health_reads_subscription_counts_from_reconcile(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    store.log_ingestion_event(
        "subscription_reconcile",
        message="kite_subscription_set_changed",
        details={"static": 1285, "dynamic": 1, "added": [9]},
    )
    app = create_app(settings)
    with TestClient(app) as client:
        stream = client.get("/api/v1/health").json()["health"]["stream"]
        assert stream["static_token_count"] == 1285
        assert stream["dynamic_token_count"] == 1
    settings = _settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        health = client.get("/api/v1/health").json()["health"]
        stream = health["stream"]
        for key in (
            "current_connections",
            "peak_connections",
            "rejected_connections",
            "last_emitted_sequence",
            "live_source_poll_latency_ms",
            "last_successful_poll",
            "ingest_generation",
            "static_token_count",
            "dynamic_token_count",
        ):
            assert key in stream
        assert "sqlite_busy_retries" in health
        assert int(health["sqlite_busy_retries"]) >= 0
        store = SQLiteStore(settings.paths.sqlite_db)
        with store.connection(read_only=True) as conn:
            assert str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal"


def test_sse_parse_and_integrity_no_regressions() -> None:
    raw = (
        "id: 1\nevent: tick\ndata: {\"t\":1,\"ts\":\"2026-09-09T04:00:00+00:00\",\"seq\":1,\"ltp\":10}\n\n"
        "id: 2\nevent: tick\ndata: {\"t\":1,\"ts\":\"2026-09-09T04:00:01+00:00\",\"seq\":2,\"ltp\":11}\n\n"
    )
    frames = parse_sse_chunk(raw, received_at=1.0)
    integrity = StreamIntegrity()
    for frame in frames:
        integrity.consume(frame)
    assert integrity.report.duplicates == 0
    assert integrity.report.gaps == 0
    assert integrity.report.out_of_order == 0
    assert integrity.report.regressions == 0


def test_monotonic_latest_quotes_phase5_regression(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "m.db")
    newer = datetime(2026, 9, 9, 11, 0, tzinfo=IST)
    older = newer - timedelta(seconds=30)
    store.upsert_latest_quotes([_quote_row(408065, "HDFCBANK", 1712.4, newer)])
    store.upsert_latest_quotes([_quote_row(408065, "HDFCBANK", 1.0, older)])
    row = store.fetch_latest_quote("HDFCBANK")
    assert row is not None
    assert row["last_price"] == 1712.4
