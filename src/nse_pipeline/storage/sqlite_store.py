"""
SQLite persistence for feature logs, signal logs, and ingestion metadata.

SQLite is a single-file embedded database — like a lightweight local SQL Server Express
file, but with no separate server process.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS feature_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    track TEXT NOT NULL,
    features_json TEXT NOT NULL,
    actual_outcome REAL
);

CREATE INDEX IF NOT EXISTS idx_feature_log_ts ON feature_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_feature_log_symbol ON feature_log(symbol);

CREATE TABLE IF NOT EXISTS signal_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    track TEXT NOT NULL,
    model_version TEXT,
    score REAL,
    probability REAL,
    features_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_signal_log_ts ON signal_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_signal_log_symbol ON signal_log(symbol);

CREATE TABLE IF NOT EXISTS ingestion_meta (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT,
    symbol TEXT,
    rows_written INTEGER DEFAULT 0,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_meta_ts ON ingestion_meta(timestamp);
"""


class SQLiteStore:
    """Thin wrapper around sqlite3 with schema initialization."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _initialize(self) -> None:
        with self.connection() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for DB connections.

        'with' blocks auto-close resources — like C# 'using' statements.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def log_ingestion_event(
        self,
        event_type: str,
        message: str = "",
        symbol: str | None = None,
        rows_written: int = 0,
        details: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        details_json = json.dumps(details) if details else None
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO ingestion_meta
                    (timestamp, event_type, message, symbol, rows_written, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (now, event_type, message, symbol, rows_written, details_json),
            )

    def get_recent_ingestion_events(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, event_type, message, symbol, rows_written, details_json
                FROM ingestion_meta
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if item.get("details_json"):
                item["details"] = json.loads(item["details_json"])
            results.append(item)
        return results

    def insert_feature_logs(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO feature_log
                    (timestamp, symbol, track, features_json, actual_outcome)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["timestamp"],
                        r["symbol"],
                        r["track"],
                        json.dumps(r["features"]),
                        r.get("actual_outcome"),
                    )
                    for r in payload
                ],
            )
        return len(payload)

    def insert_signal_logs(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO signal_log
                    (timestamp, symbol, track, model_version, score, probability, features_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["timestamp"],
                        r["symbol"],
                        r["track"],
                        r.get("model_version"),
                        r.get("score"),
                        r.get("probability"),
                        json.dumps(r["features"]),
                    )
                    for r in payload
                ],
            )
        return len(payload)
