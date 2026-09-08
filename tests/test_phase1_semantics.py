"""Phase 1: session clock, canonical change, freshness, health. No fabricated data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from fastapi.testclient import TestClient

from nse_pipeline.api.app import create_app
from nse_pipeline.api.contracts import (
    CANONICAL_CHANGE_FIELDS,
    HEALTH_COMPONENT_STATUSES,
    MATURITY_CONTRACT,
    over_budget,
)
from nse_pipeline.api.read_model import SqliteUiReadModel
from nse_pipeline.market.calendar import (
    HOLIDAY,
    cash_session_open,
    previous_trading_day,
    session_clock,
)
from nse_pipeline.market.data_state import (
    MARKET_OPEN,
    MARKET_PRE_OPEN,
    MARKET_WEEKEND,
    STATUS_LAST_SESSION,
    STATUS_LIVE,
    STATUS_NO_DATA,
    resolve,
)
from nse_pipeline.market.freshness import (
    FRESH_LIVE,
    LAST_SESSION,
    NO_DATA,
    STALE_LIVE,
    describe_freshness,
)
from nse_pipeline.market.observability import build_health_components
from nse_pipeline.market.quotes import enrich_quote
from nse_pipeline.market.reference import (
    REF_OFFICIAL_CLOSE,
    REF_PREVIOUS_CLOSE,
    REF_TODAY_OPEN,
    resolve_reference,
)
from nse_pipeline.storage.retention import disk_usage_report
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_last_session_fallback import (
    MONDAY_OPEN_IST,
    SUNDAY_IST,
    _cache,
    _live_row,
    _read_model,
)

IST = ZoneInfo("Asia/Kolkata")


def _row(**overrides) -> dict:
    base = {
        "symbol": "HDFCBANK",
        "instrument_type": "EQ",
        "last_price": 2010.0,
        "ohlc_open": 2005.0,
        "ohlc_close": 2000.0,
        "price_delta": 0.25,
        "timestamp": MONDAY_OPEN_IST.astimezone(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


def test_session_clock_and_live_expectation(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    assert session_clock(session, now=datetime(2026, 9, 7, 9, 14, tzinfo=IST)) == MARKET_PRE_OPEN
    assert cash_session_open(MARKET_PRE_OPEN) is False
    assert cash_session_open(MARKET_OPEN) is True
    assert session_clock(session, now=SUNDAY_IST) == MARKET_WEEKEND
    assert HOLIDAY not in {
        session_clock(session, now=MONDAY_OPEN_IST),
        session_clock(session, now=SUNDAY_IST),
    }
    prev, complete = previous_trading_day(session, as_of=datetime(2026, 9, 7).date())
    assert prev == "2026-09-04"
    assert complete is False


def test_canonical_change_positive_negative_zero() -> None:
    up = resolve_reference(_row(last_price=2010.0, ohlc_close=2000.0))
    assert up["reference_type"] == REF_PREVIOUS_CLOSE
    assert up["reference_price"] == 2000.0
    assert up["change_absolute"] == 10.0
    assert up["change_percent"] == 0.5
    down = resolve_reference(_row(last_price=1990.0, ohlc_close=2000.0))
    assert down["change_absolute"] == -10.0
    assert down["change_percent"] == -0.5
    flat = resolve_reference(_row(last_price=2000.0, ohlc_close=2000.0))
    assert flat["change_absolute"] == 0.0
    assert flat["change_percent"] == 0.0


def test_canonical_change_missing_and_invalid_reference() -> None:
    missing = resolve_reference(_row(ohlc_close=None, ohlc_open=None))
    assert missing["reference_price"] is None
    assert missing["change_absolute"] is None
    assert missing["change_percent"] is None
    assert missing["reference_reason"] == "no_reference"
    zero = resolve_reference(_row(ohlc_close=0, ohlc_open=None))
    assert zero["change_absolute"] is None
    assert zero["reference_reason"] == "no_reference"
    opened = resolve_reference(_row(ohlc_close=None, ohlc_open=2005.0, last_price=2010.0))
    assert opened["reference_type"] == REF_TODAY_OPEN
    assert opened["change_absolute"] == 5.0
    fut = resolve_reference(_row(instrument_type="FUT", ohlc_close=100.0, last_price=101.0))
    assert fut["reference_type"] == REF_OFFICIAL_CLOSE
    assert fut["change_percent"] == 1.0


def test_enrich_quote_keeps_legacy_change_and_adds_canonical() -> None:
    dto = enrich_quote(_row(price_delta=0.25, last_price=2010.0, ohlc_close=2000.0))
    assert dto["change"] == 0.25
    assert dto["change_absolute"] == 10.0
    assert dto["change_percent"] == 0.5
    for field in CANONICAL_CHANGE_FIELDS:
        assert field in dto


def test_pre_open_fresh_quote_is_last_session_not_live(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    now = datetime(2026, 9, 7, 9, 14, tzinfo=IST)
    _choice, state = resolve(
        session=session,
        live_timestamp=(now - timedelta(seconds=5)).isoformat(),
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=now,
    )
    assert state.market_state == MARKET_PRE_OPEN
    assert state.data_status == STATUS_LAST_SESSION
    assert state.reason == "market_closed"


def test_freshness_live_stale_last_session_no_data(tmp_path: Path) -> None:
    session = _settings(tmp_path).session
    now = MONDAY_OPEN_IST
    live_ts = (now - timedelta(seconds=8)).isoformat()
    _choice, live_state = resolve(
        session=session,
        live_timestamp=live_ts,
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=now,
    )
    assert live_state.data_status == STATUS_LIVE
    fresh = describe_freshness(live_state, {"timestamp": live_ts}, now=now, max_age_seconds=120.0)
    assert fresh["status"] == FRESH_LIVE
    assert fresh["freshness_age_seconds"] == pytest.approx(8.0, abs=0.05)
    assert fresh["observed_at"] == live_state.as_of

    stale_ts = (now - timedelta(seconds=400)).isoformat()
    _choice, stale_state = resolve(
        session=session,
        live_timestamp=stale_ts,
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=now,
    )
    stale = describe_freshness(stale_state, {"timestamp": stale_ts}, now=now, max_age_seconds=120.0)
    assert stale_state.data_status == STATUS_LAST_SESSION
    assert stale["status"] == LAST_SESSION

    sunday = SUNDAY_IST
    friday_close = datetime(2026, 9, 4, 15, 30, tzinfo=IST).isoformat()
    _choice, closed_state = resolve(
        session=session,
        live_timestamp=friday_close,
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=sunday,
    )
    closed = describe_freshness(
        closed_state, {"timestamp": friday_close}, now=sunday, max_age_seconds=120.0
    )
    assert closed_state.data_status == STATUS_LAST_SESSION
    assert closed["status"] == LAST_SESSION
    assert closed["freshness_age_seconds"] is not None
    assert closed["freshness_age_seconds"] > 0

    _choice, empty = resolve(
        session=session,
        live_timestamp=None,
        historical_timestamp=None,
        max_age_seconds=120.0,
        now=now,
    )
    none = describe_freshness(empty, None, now=now, max_age_seconds=120.0)
    assert empty.data_status == STATUS_NO_DATA
    assert none["status"] == NO_DATA
    assert none["freshness_age_seconds"] is None


def test_stale_during_open_session_is_stale_live_quality() -> None:
    """When resolve still labels LIVE, age past the budget is stale_live."""
    from nse_pipeline.market.data_state import DataState, SOURCE_LATEST_QUOTES

    now = MONDAY_OPEN_IST
    ts = (now - timedelta(seconds=400)).isoformat()
    state = DataState(
        market_state=MARKET_OPEN,
        data_status=STATUS_LIVE,
        as_of=ts,
        session_date="2026-09-07",
        source=SOURCE_LATEST_QUOTES,
    )
    payload = describe_freshness(state, {"timestamp": ts}, now=now, max_age_seconds=120.0)
    assert payload["status"] == STALE_LIVE


def test_live_quote_uses_observed_previous_close(tmp_path: Path) -> None:
    _settings_obj, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST, seed_history=True)
    live = _live_row("HDFCBANK", 341249, when=MONDAY_OPEN_IST - timedelta(seconds=5), last_price=2011.0)
    live["ohlc_close"] = 2000.0
    live["ohlc_open"] = 2005.0
    store.upsert_latest_quotes([live])
    payload = read.quote("HDFCBANK")
    quote = payload["quote"]
    assert payload["data_state"]["data_status"] == STATUS_LIVE
    assert quote["reference_type"] == REF_PREVIOUS_CLOSE
    assert quote["change_absolute"] == 11.0
    assert quote["change_percent"] == pytest.approx(0.55)
    assert quote["freshness"]["status"] == FRESH_LIVE
    assert payload["session"]["holiday_list_seeded"] is False
    assert payload["session"]["live_data_expected"] is True


def test_last_session_quote_uses_observed_reference(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=SUNDAY_IST)
    last = read.quote("HDFCBANK")
    assert last["data_state"]["data_status"] == STATUS_LAST_SESSION
    assert last["session"]["live_data_expected"] is False
    assert last["quote"]["freshness"]["status"] == LAST_SESSION
    assert last["quote"]["change_absolute"] is not None
    assert last["quote"]["reference_source"] == "kite_tick_ohlc_close"


def test_no_data_quote_and_degraded_subsystem(tmp_path: Path) -> None:
    _settings_obj, _store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST, seed_history=False)
    payload = read.quote("MISSING")
    assert payload["found"] is False
    assert payload["data_state"]["data_status"] == STATUS_NO_DATA
    components = build_health_components(
        clock=MARKET_OPEN,
        now=MONDAY_OPEN_IST,
        max_age_seconds=120.0,
        quotes_count=1,
        quotes_max_timestamp=(MONDAY_OPEN_IST - timedelta(seconds=400)).isoformat(),
        quotes_max_ingested_at=(MONDAY_OPEN_IST - timedelta(seconds=400)).isoformat(),
        clock_skew_seconds=19800.0,
        activity_max_timestamp=None,
        cache_health={"status": "unknown", "reason": "cache_file_missing"},
        calendar_rows=0,
        holiday_list_seeded=False,
        account_status="auth_invalid",
        disk={"disk_free_gb": 1.0, "sessions_of_headroom": 0},
        disk_warning=True,
        maturity={"equity": {"probability_permitted": False}},
        ingest_fresh=False,
    )
    assert components["latest_quotes"]["status"] == "stale"
    assert components["activity_samples"]["status"] == "unavailable"
    assert components["instrument_cache"]["status"] == "unavailable"
    assert components["account"]["status"] == "unavailable"
    assert components["disk"]["status"] == "degraded"
    assert components["market_calendar"]["reason"] == "holiday_list_not_seeded"
    assert components["tick_stream_counters"]["status"] == "not_applicable"
    assert components["maturity"]["probability_permitted"] is False
    for block in components.values():
        assert block["status"] in HEALTH_COMPONENT_STATUSES


def test_disk_health_contract_and_health_endpoint(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    _cache(settings)
    report = disk_usage_report(settings.paths.data_dir, raw_bytes_per_session=2_500_000_000)
    assert "disk_total_gb" in report
    assert "disk_used_gb" in report
    assert "disk_free_gb" in report
    client = TestClient(create_app(settings))
    health = client.get("/api/v1/health").json()
    blob = health["health"]
    assert blob["disk_total_gb"] is not None
    assert blob["disk_used_gb"] is not None
    assert blob["disk_free_gb"] is not None
    assert blob["retention"]["dry_run"] is True
    assert blob["retention"]["applied"] is False
    assert blob["retention_mode"] == "dry_run"
    assert blob["retention_applied"] is False
    assert "components" in blob
    assert set(blob["components"]) >= {
        "ingestion",
        "latest_quotes",
        "activity_samples",
        "instrument_cache",
        "market_calendar",
        "account",
        "api",
        "disk",
        "maturity",
    }
    assert health["session"]["holiday_list_seeded"] is False
    assert health["subsystems"]["intelligence"]["status"] == "NOT_MATURE"
    assert not over_budget(health, endpoint="health")
    with SQLiteStore(settings.paths.sqlite_db).connection() as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "market_calendar" in tables
    assert "session_reference" in tables


def test_maturity_gate_still_suppresses_below_60_days(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)
    read = SqliteUiReadModel(settings)
    snap = read._maturity()
    assert settings.maturity_gate.provisional_below_days == 60
    assert MATURITY_CONTRACT["provisional_below_days"] == 60
    assert snap["equity"]["probability_permitted"] is False
    assert snap["equity"]["tier"] == "suppressed"
    client = TestClient(create_app(settings))
    payload = client.get("/api/v1/maturity").json()
    assert payload["maturity"]["equity"]["probability_permitted"] is False
    assert payload["subsystems"]["intelligence"]["status"] == "NOT_MATURE"


def test_phase0_live_quote_path_still_attaches_data_state(tmp_path: Path) -> None:
    _settings_obj, store, read = _read_model(tmp_path, now=MONDAY_OPEN_IST)
    store.upsert_latest_quotes(
        [_live_row("HDFCBANK", 341249, when=MONDAY_OPEN_IST - timedelta(seconds=5), last_price=2011.0)]
    )
    payload = read.quote("HDFCBANK")
    assert payload["found"] is True
    assert payload["data_state"]["data_status"] == STATUS_LIVE
    assert payload["quote"]["last_price"] == 2011.0
    assert "freshness" in payload["quote"]
    assert "session" in payload
