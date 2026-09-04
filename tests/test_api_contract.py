"""Frozen /api/v1 contract: GET-only, no features_json, no invented probabilities."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from nse_pipeline.algorithms.unavailable import UnavailableAlgorithm
from nse_pipeline.api.app import create_app
from nse_pipeline.signals.engine import LiveSignalEngine
from nse_pipeline.storage.sqlite_store import SQLiteStore
from tests.test_compaction import _settings
from tests.test_maturity_engine import _insert_live


def _seed(tmp_path: Path):
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
    engine = LiveSignalEngine(
        settings,
        algorithms={k: UnavailableAlgorithm() for k in ("equity", "options", "futures")},
    )
    engine.score_and_log(store.fetch_feature_logs_range("2026-09-01", "2026-09-01"))
    return settings, store


def test_api_v1_envelope_and_null_probability(tmp_path: Path) -> None:
    settings, _store = _seed(tmp_path)
    client = TestClient(create_app(settings))
    for path in (
        "/api/v1/overview",
        "/api/v1/maturity",
        "/api/v1/signals",
        "/api/v1/account",
        "/api/v1/decisions",
        "/api/v1/health",
    ):
        payload = client.get(path).json()
        assert "maturity" in payload
        assert "as_of" in payload
        eq = payload["maturity"]["equity"]
        assert eq["display"] == "insufficient data, 1/60 pooled days"
        assert eq["probability_permitted"] is False
        blob = str(payload)
        assert "features_json" not in blob
        assert "0.62" not in (eq.get("display") or "")

    sig = client.get("/api/v1/signals").json()
    assert sig["signals"]
    for row in sig["signals"]:
        assert row["probability"] is None
        assert row["score"] is None
        assert row["tier"] == "suppressed"
        assert "insufficient data" in row["display"]
        assert "attribution" not in row
        assert "features" not in row
        assert "features_json" not in row

    health = client.get("/api/v1/health").json()
    assert health["health"]["processing_status"]["features"]["status"] == "unknown"
    assert health["health"]["api"] == "ok"
    assert health["health"]["database"] == "ok"
    assert health["health"]["processing_lag"] == "unknown"
    assert client.get("/v1/maturity").json()["maturity"]["equity"]["tier"] == "suppressed"


def test_api_is_get_only(tmp_path: Path) -> None:
    settings, _store = _seed(tmp_path)
    client = TestClient(create_app(settings))
    for path in (
        "/api/v1/overview",
        "/api/v1/maturity",
        "/api/v1/signals",
        "/api/v1/account",
        "/api/v1/decisions",
        "/api/v1/health",
    ):
        assert client.post(path).status_code == 405
        assert client.put(path).status_code == 405


def test_unavailable_algorithm_does_not_emit_a_number() -> None:
    algo = UnavailableAlgorithm()
    assert algo.available("equity") is False
    result = algo.score({"symbol": "RELIANCE", "track": "equity_depth"}, class_key="equity")
    assert result.available is False
    assert result.probability is None
    assert result.score is None
    assert result.details["reason"] == "algorithm_not_implemented"


def test_processing_status_upsert(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "status.db")
    store.upsert_processing_status(
        "features",
        status="ok",
        last_trade_date="2026-09-01",
        last_timestamp="2026-09-01T10:00:00+00:00",
        rows_written=12,
    )
    row = store.fetch_processing_status("features")
    assert row["status"] == "ok"
    assert row["last_trade_date"] == "2026-09-01"
    assert row["rows_written"] == 12
    missing = store.fetch_processing_status("labels")
    assert missing["status"] == "unknown"


def test_schema_has_processing_status_and_query_indexes(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "idx.db")
    with store.connection() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "processing_status" in tables
        sig_idx = {
            row[1] for row in conn.execute("PRAGMA index_list(signal_log)")
        }
        meta_idx = {
            row[1] for row in conn.execute("PRAGMA index_list(ingestion_meta)")
        }
    assert "idx_signal_log_date_tier" in sig_idx
    assert "idx_ingestion_meta_event_ts" in meta_idx


def test_create_app_does_not_construct_engine(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    SQLiteStore(settings.paths.sqlite_db)

    def _boom(*_a, **_k):
        raise AssertionError("LiveSignalEngine must not be constructed on the read path")

    monkeypatch.setattr("nse_pipeline.signals.engine.LiveSignalEngine", _boom)
    client = TestClient(create_app(settings))
    assert client.get("/api/v1/health").status_code == 200
    assert client.get("/api/v1/maturity").status_code == 200


def test_read_path_does_not_import_sklearn_or_logistic_models() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "src" / "nse_pipeline"
    for rel in ("api/app.py", "api/read_model.py"):
        text = (root / rel).read_text(encoding="utf-8")
        assert "sklearn" not in text
        assert "models.logistic" not in text
        assert "import joblib" not in text
        assert "from joblib" not in text
        assert "LiveSignalEngine" not in text
        assert "KiteConnect" not in text
