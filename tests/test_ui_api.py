"""API must return the frozen N/60 view and never a public probability before 60 days."""

from __future__ import annotations

from pathlib import Path

from nse_pipeline.api.app import create_app
from nse_pipeline.signals.engine import LiveSignalEngine
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_maturity_engine import _insert_live


def test_api_maturity_and_signals_are_suppressed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = SQLiteStore(settings.paths.sqlite_db)
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=3,
        minute=45,
    )
    _insert_live(
        store,
        trade_date="2026-09-01",
        symbol="RELIANCE",
        track="equity_depth",
        hour=10,
        minute=0,
    )
    engine = LiveSignalEngine(settings)
    engine.score_and_log(store.fetch_feature_logs_range("2026-09-01", "2026-09-01"))

    from fastapi.testclient import TestClient

    client = TestClient(create_app(settings))
    mat = client.get("/api/v1/maturity").json()
    eq = mat["maturity"]["equity"]
    assert eq["display"] == "insufficient data, 1/60 pooled days"
    assert eq["probability_permitted"] is False
    assert "as_of" in mat
    sig = client.get("/api/v1/signals").json()
    assert sig["signals"]
    for row in sig["signals"]:
        assert row["probability"] is None
        assert row["score"] is None
        assert "insufficient data" in row["display"]
        assert "0.62" not in str(row["display"])
        assert "attribution" not in row
        assert "features_json" not in row
    health = client.get("/api/v1/health").json()
    assert "maturity" in health
    assert health["maturity"]["equity"]["probability_permitted"] is False
    decisions = client.get("/api/v1/decisions").json()
    assert decisions["decisions"] == []
    account = client.get("/api/v1/account").json()
    assert account["fills"] == []
    alias = client.get("/v1/signals/latest").json()
    assert alias["signals"]
