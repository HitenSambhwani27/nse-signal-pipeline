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

import pandas as pd


def _ts_iso(ts: Any) -> str:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return t.isoformat()


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS feature_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    trade_date TEXT,
    symbol TEXT NOT NULL,
    track TEXT NOT NULL,
    features_json TEXT NOT NULL,
    actual_outcome REAL,
    source TEXT,
    feature_completeness TEXT
);

CREATE INDEX IF NOT EXISTS idx_feature_log_ts ON feature_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_feature_log_symbol ON feature_log(symbol);

CREATE TABLE IF NOT EXISTS signal_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    trade_date TEXT,
    symbol TEXT NOT NULL,
    track TEXT NOT NULL,
    model_version TEXT,
    score REAL,
    probability REAL,
    features_json TEXT NOT NULL,
    source TEXT,
    attribution_json TEXT,
    maturity_tier TEXT
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

CREATE TABLE IF NOT EXISTS quality_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    trade_date TEXT,
    symbol TEXT,
    event_type TEXT NOT NULL,
    reason TEXT,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_quality_log_trade_date ON quality_log(trade_date);
CREATE INDEX IF NOT EXISTS idx_quality_log_event ON quality_log(event_type);

CREATE TABLE IF NOT EXISTS label_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    symbol TEXT NOT NULL,
    track TEXT NOT NULL,
    mode TEXT NOT NULL,
    label TEXT NOT NULL,
    label_code REAL NOT NULL,
    fwd_ret_pct REAL,
    thr_pct REAL,
    thr_raw_pct REAL,
    floor_bound INTEGER NOT NULL DEFAULT 0,
    cap_bound INTEGER NOT NULL DEFAULT 0,
    vol REAL,
    hour_ist INTEGER
);

CREATE INDEX IF NOT EXISTS idx_label_audit_trade_date ON label_audit(trade_date);
CREATE INDEX IF NOT EXISTS idx_label_audit_trade_date_track ON label_audit(trade_date, track);
CREATE INDEX IF NOT EXISTS idx_label_audit_mode ON label_audit(mode);

CREATE TABLE IF NOT EXISTS retrain_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL,
    window_start TEXT,
    window_end TEXT,
    source_composition_json TEXT,
    comparison_json TEXT,
    promoted INTEGER NOT NULL DEFAULT 0,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_retrain_log_ts ON retrain_log(timestamp);

CREATE TABLE IF NOT EXISTS model_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    track TEXT NOT NULL,
    role TEXT NOT NULL,
    version TEXT NOT NULL,
    path TEXT NOT NULL,
    harness_passed INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_model_registry_track ON model_registry(track, role, created_at);
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
            # Migrate older DBs that predate trade_date (index created after column exists).
            cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(feature_log)").fetchall()
            }
            if "trade_date" not in cols:
                conn.execute("ALTER TABLE feature_log ADD COLUMN trade_date TEXT")
            if "source" not in cols:
                conn.execute("ALTER TABLE feature_log ADD COLUMN source TEXT")
            if "feature_completeness" not in cols:
                conn.execute(
                    "ALTER TABLE feature_log ADD COLUMN feature_completeness TEXT"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feature_log_trade_date "
                "ON feature_log(trade_date)"
            )
            sig_cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(signal_log)").fetchall()
            }
            for col, sql in (
                ("trade_date", "ALTER TABLE signal_log ADD COLUMN trade_date TEXT"),
                ("source", "ALTER TABLE signal_log ADD COLUMN source TEXT"),
                (
                    "attribution_json",
                    "ALTER TABLE signal_log ADD COLUMN attribution_json TEXT",
                ),
                ("maturity_tier", "ALTER TABLE signal_log ADD COLUMN maturity_tier TEXT"),
            ):
                if col not in sig_cols:
                    conn.execute(sql)

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for DB connections.

        'with' blocks auto-close resources — like C# 'using' statements.
        """
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
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

    def log_quality_event(
        self,
        event_type: str,
        reason: str,
        trade_date: str | None = None,
        symbol: str | None = None,
        timestamp: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        Persist quality_reject / stock_quote_freeze events.

        stock_quote_freeze = per-symbol freeze suspect (not market-wide NSE halt);
        intentionally distinct from websocket disconnect event_types.
        """
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        details_json = json.dumps(details) if details else None
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO quality_log
                    (timestamp, trade_date, symbol, event_type, reason, details_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ts, trade_date, symbol, event_type, reason, details_json),
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

    def delete_feature_logs_for_trade_date(
        self, trade_date: str, symbols: list[str] | None = None
    ) -> int:
        with self.connection() as conn:
            if symbols is None:
                cur = conn.execute(
                    "DELETE FROM feature_log WHERE trade_date = ?", (trade_date,)
                )
            elif not symbols:
                return 0
            else:
                placeholders = ",".join("?" * len(symbols))
                cur = conn.execute(
                    f"DELETE FROM feature_log WHERE trade_date = ? AND symbol IN ({placeholders})",
                    (trade_date, *symbols),
                )
            return int(cur.rowcount or 0)

    def delete_quality_logs_for_trade_date(
        self, trade_date: str, symbols: list[str] | None = None
    ) -> int:
        with self.connection() as conn:
            if symbols is None:
                cur = conn.execute(
                    "DELETE FROM quality_log WHERE trade_date = ?", (trade_date,)
                )
            elif not symbols:
                return 0
            else:
                placeholders = ",".join("?" * len(symbols))
                cur = conn.execute(
                    f"DELETE FROM quality_log WHERE trade_date = ? AND symbol IN ({placeholders})",
                    (trade_date, *symbols),
                )
            return int(cur.rowcount or 0)

    def insert_feature_logs(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        chunk_size = 500
        with self.connection() as conn:
            for offset in range(0, len(payload), chunk_size):
                chunk = payload[offset : offset + chunk_size]
                conn.executemany(
                    """
                    INSERT INTO feature_log
                        (timestamp, trade_date, symbol, track, features_json,
                         actual_outcome, source, feature_completeness)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["timestamp"],
                            r.get("trade_date"),
                            r["symbol"],
                            r["track"],
                            json.dumps(r["features"]),
                            r.get("actual_outcome"),
                            r.get("source"),
                            r.get("feature_completeness"),
                        )
                        for r in chunk
                    ],
                )
                conn.commit()
        return len(payload)

    def count_features(self, trade_date: str | None = None) -> int:
        with self.connection() as conn:
            if trade_date:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM feature_log WHERE trade_date = ?",
                    (trade_date,),
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM feature_log").fetchone()
        return int(row["c"])

    def insert_signal_logs(self, rows: Iterable[dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO signal_log
                    (timestamp, trade_date, symbol, track, model_version, score,
                     probability, features_json, source, attribution_json, maturity_tier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["timestamp"],
                        r.get("trade_date"),
                        r["symbol"],
                        r["track"],
                        r.get("model_version"),
                        r.get("score"),
                        r.get("probability"),
                        json.dumps(r.get("features") or {}),
                        r.get("source"),
                        json.dumps(r["attribution"]) if r.get("attribution") is not None else None,
                        r.get("maturity_tier"),
                    )
                    for r in payload
                ],
            )
        return len(payload)

    def fetch_feature_logs(
        self,
        trade_date: str,
        *,
        tracks: tuple[str, ...] | None = None,
        include_features: bool = True,
    ) -> list[dict[str, Any]]:
        columns = "id, timestamp, trade_date, symbol, track"
        if include_features:
            columns += ", features_json, actual_outcome, source, feature_completeness"
        sql = f"SELECT {columns} FROM feature_log WHERE trade_date = ?"
        params: list[Any] = [trade_date]
        if tracks:
            placeholders = ",".join("?" * len(tracks))
            sql += f" AND track IN ({placeholders})"
            params.extend(tracks)
        sql += " ORDER BY symbol, timestamp"
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if include_features:
                item["features"] = json.loads(item.pop("features_json"))
            out.append(item)
        return out

    def fetch_feature_logs_range(
        self,
        start_date: str,
        end_date: str,
        *,
        tracks: tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT id, timestamp, trade_date, symbol, track, features_json,
                   actual_outcome, source, feature_completeness
            FROM feature_log
            WHERE trade_date >= ? AND trade_date <= ?
        """
        params: list[Any] = [start_date, end_date]
        if tracks:
            placeholders = ",".join("?" * len(tracks))
            sql += f" AND track IN ({placeholders})"
            params.extend(tracks)
        sql += " ORDER BY trade_date, symbol, timestamp"
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["features"] = json.loads(item.pop("features_json"))
            out.append(item)
        return out

    def list_feature_trade_dates(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT trade_date FROM feature_log
                WHERE trade_date IS NOT NULL
                ORDER BY trade_date
                """
            ).fetchall()
        return [str(r[0]) for r in rows]

    def delete_signal_logs_for_trade_date(
        self, trade_date: str, tracks: tuple[str, ...] | None = None
    ) -> int:
        with self.connection() as conn:
            if tracks:
                placeholders = ",".join("?" * len(tracks))
                cur = conn.execute(
                    f"DELETE FROM signal_log WHERE trade_date = ? AND track IN ({placeholders})",
                    (trade_date, *tracks),
                )
            else:
                cur = conn.execute(
                    "DELETE FROM signal_log WHERE trade_date = ?", (trade_date,)
                )
            return int(cur.rowcount or 0)

    def log_retrain_event(
        self,
        event_type: str,
        *,
        window_start: str | None = None,
        window_end: str | None = None,
        source_composition: dict[str, Any] | None = None,
        comparison: dict[str, Any] | None = None,
        promoted: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO retrain_log (
                    timestamp, event_type, window_start, window_end,
                    source_composition_json, comparison_json, promoted, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    event_type,
                    window_start,
                    window_end,
                    json.dumps(source_composition) if source_composition else None,
                    json.dumps(comparison) if comparison else None,
                    1 if promoted else 0,
                    json.dumps(details) if details else None,
                ),
            )

    def register_model(
        self,
        *,
        track: str,
        role: str,
        version: str,
        path: str,
        harness_passed: bool,
        metadata: dict[str, Any],
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO model_registry
                    (created_at, track, role, version, path, harness_passed, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    track,
                    role,
                    version,
                    path,
                    1 if harness_passed else 0,
                    json.dumps(metadata),
                ),
            )

    def update_feature_outcomes(
        self, updates: Iterable[tuple[float | None, int]]
    ) -> int:
        payload = list(updates)
        if not payload:
            return 0
        with self.connection() as conn:
            conn.executemany(
                "UPDATE feature_log SET actual_outcome = ? WHERE id = ?",
                payload,
            )
        return len(payload)

    def clear_label_audit(
        self, trade_date: str, tracks: tuple[str, ...] | None = None
    ) -> int:
        with self.connection() as conn:
            if tracks:
                placeholders = ",".join("?" * len(tracks))
                cur = conn.execute(
                    f"DELETE FROM label_audit WHERE trade_date = ? AND track IN ({placeholders})",
                    (trade_date, *tracks),
                )
            else:
                cur = conn.execute(
                    "DELETE FROM label_audit WHERE trade_date = ?", (trade_date,)
                )
            return int(cur.rowcount or 0)

    def insert_label_audit(
        self, results: Iterable[Any], *, trade_date: str
    ) -> int:
        payload = list(results)
        if not payload:
            return 0
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO label_audit (
                    trade_date, timestamp, symbol, track, mode, label, label_code,
                    fwd_ret_pct, thr_pct, thr_raw_pct, floor_bound, cap_bound, vol, hour_ist
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        trade_date,
                        _ts_iso(r.timestamp),
                        r.symbol,
                        r.track,
                        r.mode,
                        r.label,
                        r.label_code,
                        r.fwd_ret_pct,
                        r.thr_pct,
                        r.thr_raw_pct,
                        1 if r.floor_bound else 0,
                        1 if r.cap_bound else 0,
                        r.vol,
                        r.hour_ist,
                    )
                    for r in payload
                ],
            )
        return len(payload)