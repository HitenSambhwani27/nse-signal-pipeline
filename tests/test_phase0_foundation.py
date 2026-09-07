"""Phase 0 foundation: cache schema, token backoff, retention, contracts.

Does not start ingestion, call Kite, or fabricate market data.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from nse_pipeline.api.contracts import (
    ABSOLUTE_MAX_BYTES,
    CANONICAL_TYPES,
    MATURITY_CONTRACT,
    RESPONSE_SIZE_LIMITS,
    SUBSYSTEM_STATUSES,
    intelligence_status,
    over_budget,
    payload_bytes,
)
from nse_pipeline.broker import instruments as instruments_mod
from nse_pipeline.broker.session import AUTH_BACKOFF_SECONDS, TokenState, classify_exception, public_token_status
from nse_pipeline.config import load_settings
from nse_pipeline.market.cache_health import CACHE_SCHEMA_VERSION, cache_file_needs_refresh, durable_instrument_key
from nse_pipeline.market.latest import LatestQuoteTracker, snapshot_from_tick
from nse_pipeline.market.normalize import normalize_tick
from nse_pipeline.storage.retention import apply_retention, plan_retention
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_market_foundation import EXCH, MAP, _full_tick


def test_atm_strike_is_imported_for_index_option_cache_refresh() -> None:
    assert callable(instruments_mod.atm_strike)
    source = Path(instruments_mod.__file__).read_text(encoding="utf-8")
    assert "atm_strike(" in source
    assert "from nse_pipeline.market.universe import" in source


def test_schema_stale_cache_needs_refresh() -> None:
    assert CACHE_SCHEMA_VERSION == 1
    assert cache_file_needs_refresh(
        {"updated_at": "2026-09-04T05:13:00Z", "options": [], "futures": []},
        membership_stale=False,
        cache_exists=True,
        today=date(2026, 9, 7),
    )
    assert not cache_file_needs_refresh(
        {
            "schema_version": CACHE_SCHEMA_VERSION,
            "updated_at": "2026-09-07T05:00:00Z",
            "options": [{"tradingsymbol": "NIFTY26SEP25000CE", "expiry": "2026-09-29"}],
            "futures": [
                {"tradingsymbol": "NIFTY26SEPFUT", "expiry": "2026-09-29"},
                {"tradingsymbol": "NIFTY26OCTFUT", "expiry": "2026-10-27"},
            ],
        },
        membership_stale=False,
        cache_exists=True,
        today=date(2026, 9, 7),
        future_contract_count=2,
    )


def test_durable_instrument_key_is_not_a_kite_token() -> None:
    key = durable_instrument_key(
        exchange="NFO",
        tradingsymbol="NIFTY26SEP25000CE",
        name="NIFTY",
        instrument_type="CE",
        expiry="2026-09-29",
        strike=25000.0,
    )
    assert key == "NFO:NIFTY:CE:2026-09-29:25000"
    assert "256265" not in key


def test_token_exception_is_classified_and_backs_off() -> None:
    class TokenException(Exception):
        pass

    assert AUTH_BACKOFF_SECONDS >= 300
    assert classify_exception(TokenException("Incorrect api_key or access_token")) == TokenState.INVALID
    assert classify_exception(RuntimeError("network timeout")) is None
    inferred = public_token_status(
        access_token_present=True,
        last_account_status="auth_invalid",
        last_account_error="TokenException: Incorrect api_key or access_token",
        ingest_fresh=True,
    )
    assert inferred["kite_token_state"] == TokenState.VALID.value
    dead = public_token_status(
        access_token_present=True,
        last_account_status="auth_invalid",
        ingest_fresh=False,
    )
    assert dead["kite_token_state"] == TokenState.INVALID.value


def test_ingest_startup_falls_back_to_existing_cache_on_refresh_failure() -> None:
    text = (Path(__file__).resolve().parents[1] / "scripts" / "01_run_ingestion.py").read_text(
        encoding="utf-8"
    )
    assert "Continuing with the existing instrument cache" in text
    assert "schema-stale" in text


def test_first_tick_volume_and_oi_delta_are_null() -> None:
    tick = normalize_tick(_full_tick(), MAP, EXCH)
    assert tick is not None
    snap = snapshot_from_tick(tick, None)
    assert snap["volume_delta"] is None
    assert snap["oi_delta"] is None
    tracker = LatestQuoteTracker()
    first = tracker.observe(tick)
    assert first["volume_delta"] is None


def test_retention_dry_run_never_deletes_signal_log(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    raw_old = settings.paths.raw_dir / "2026-01-01"
    raw_new = settings.paths.raw_dir / "2026-09-07"
    raw_old.mkdir(parents=True)
    raw_new.mkdir(parents=True)
    (raw_old / "keep.txt").write_text("x", encoding="utf-8")
    store = SQLiteStore(settings.paths.sqlite_db)
    store.insert_signal_logs(
        [
            {
                "timestamp": "2026-01-01T10:00:00+00:00",
                "trade_date": "2026-01-01",
                "symbol": "RELIANCE",
                "track": "equity_depth",
                "model_version": "none",
                "score": None,
                "probability": None,
                "maturity_tier": "suppressed",
                "features": {},
                "attribution": {},
                "source": "live",
            }
        ]
    )
    plan = plan_retention(settings, today=date(2026, 9, 7), dry_run=True)
    assert "2026-01-01" in plan.raw_delete
    assert "2026-09-07" not in plan.raw_delete
    assert plan.signal_log == "keep_forever"
    assert plan.dry_run is True
    try:
        apply_retention(settings, plan)
        raise AssertionError("dry-run plan must not apply")
    except RuntimeError as exc:
        assert "dry_run" in str(exc)
    assert (raw_old / "keep.txt").exists()
    with store.connection() as conn:
        n = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
    assert n == 1

    live = plan_retention(settings, today=date(2026, 9, 7), dry_run=False)
    apply_retention(settings, live)
    assert not raw_old.exists()
    assert raw_new.exists()
    with store.connection() as conn:
        n = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
        forbidden = conn.execute(
            "SELECT sql FROM sqlite_master WHERE sql LIKE '%DELETE FROM signal_log%'"
        ).fetchone()
    assert n == 1
    assert forbidden is None


def test_phase0_contracts_freeze_maturity_and_response_size() -> None:
    settings = load_settings()
    assert settings.maturity_gate.provisional_below_days == 60
    assert settings.maturity_gate.suppress_below_days == 10
    assert MATURITY_CONTRACT["provisional_below_days"] == 60
    assert MATURITY_CONTRACT["no_fabricated_probability"] is True
    assert ABSOLUTE_MAX_BYTES == 262_144
    assert RESPONSE_SIZE_LIMITS["instruments"]["max_rows"] == 50
    assert set(SUBSYSTEM_STATUSES) == {
        "OK",
        "PARTIAL",
        "UNAVAILABLE",
        "NOT_APPLICABLE",
        "NOT_MATURE",
    }
    assert "CanonicalQuote" in CANONICAL_TYPES
    assert intelligence_status({"equity": {"probability_permitted": False}})["status"] == "NOT_MATURE"
    tiny = {"ok": True}
    assert payload_bytes(tiny) < ABSOLUTE_MAX_BYTES
    assert not over_budget(tiny, endpoint="health")


def test_fixture_capture_script_covers_required_states() -> None:
    text = (Path(__file__).resolve().parents[1] / "scripts" / "16_capture_fixtures.py").read_text(
        encoding="utf-8"
    )
    for name in (
        "health",
        "quote_nifty",
        "quote_missing",
        "options_nifty",
        "options_banknifty",
        "futures_nifty",
        "unusual_activity",
    ):
        assert name in text
    assert "no_data" in text
    assert "last_session" in text
    assert "live" in text
    assert "<redacted>" in text
