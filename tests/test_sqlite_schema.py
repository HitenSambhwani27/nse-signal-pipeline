"""Tests for SQLite schema initialization and ingestion metadata writes."""

from pathlib import Path

from nse_pipeline.storage.sqlite_store import SQLiteStore


def test_sqlite_schema_and_ingestion_meta(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SQLiteStore(db_path)

    store.log_ingestion_event(
        event_type="startup",
        message="test startup",
        rows_written=0,
        details={"ok": True},
    )

    events = store.get_recent_ingestion_events(limit=5)
    assert len(events) == 1
    assert events[0]["event_type"] == "startup"
    assert events[0]["details"]["ok"] is True

    with store.connection() as conn:
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(label_audit)")}
    assert "idx_label_audit_trade_date_track" in indexes


def test_fetch_feature_logs_track_filter_skips_json(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "test.db")
    store.insert_feature_logs(
        [
            {
                "timestamp": "2026-08-13T03:45:00+00:00",
                "trade_date": "2026-08-13",
                "symbol": "RELIANCE",
                "track": "equity_depth",
                "features": {"x": 1},
            },
            {
                "timestamp": "2026-08-13T03:45:00+00:00",
                "trade_date": "2026-08-13",
                "symbol": "AARTIIND",
                "track": "equity_quote",
                "features": {"x": 2},
            },
        ]
    )
    rows = store.fetch_feature_logs(
        "2026-08-13", tracks=("equity_quote",), include_features=False
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "AARTIIND"
    assert "features" not in rows[0]
    assert "features_json" not in rows[0]

    ranged = store.fetch_feature_logs_range(
        "2026-08-13", "2026-08-13", tracks=("equity_depth",)
    )
    assert len(ranged) == 1
    assert ranged[0]["symbol"] == "RELIANCE"
    assert ranged[0]["features"] == {"x": 1}
