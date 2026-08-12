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
