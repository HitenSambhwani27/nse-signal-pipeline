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
CREATE INDEX IF NOT EXISTS idx_signal_log_date_tier ON signal_log(trade_date, maturity_tier);

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
CREATE INDEX IF NOT EXISTS idx_ingestion_meta_event_ts ON ingestion_meta(event_type, timestamp);

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

CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    order_id TEXT NOT NULL,
    status TEXT,
    symbol TEXT,
    exchange TEXT,
    transaction_type TEXT,
    quantity REAL,
    price REAL,
    filled_quantity REAL,
    average_price REAL,
    order_timestamp TEXT,
    payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id, status, order_timestamp);

CREATE TABLE IF NOT EXISTS trade_fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    kite_trade_id TEXT,
    order_id TEXT,
    trade_id TEXT,
    symbol TEXT,
    exchange TEXT,
    transaction_type TEXT,
    quantity REAL,
    price REAL,
    fill_timestamp TEXT,
    fill_vs_decision_json TEXT,
    payload_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_fills_kite_id ON trade_fills(kite_trade_id);
CREATE INDEX IF NOT EXISTS idx_trade_fills_trade_id ON trade_fills(trade_id);

CREATE TABLE IF NOT EXISTS positions_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS margin_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS holdings_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT NOT NULL,
    profile_json TEXT,
    margins_json TEXT
);

CREATE TABLE IF NOT EXISTS decision_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    trade_id TEXT NOT NULL UNIQUE,
    timestamp TEXT NOT NULL,
    trade_date TEXT,
    symbol TEXT NOT NULL,
    track TEXT,
    side TEXT,
    suggested_qty REAL,
    suggested_price REAL,
    actor TEXT NOT NULL,
    signal_log_id INTEGER,
    strike_rationale TEXT,
    sizing_rationale TEXT,
    timing_rationale TEXT,
    iv_rank REAL,
    risk_json TEXT,
    maturity_view_json TEXT NOT NULL,
    extras_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_decision_log_symbol ON decision_log(symbol, timestamp);

CREATE TABLE IF NOT EXISTS outcome_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL UNIQUE,
    entry_fill_id INTEGER,
    exit_fill_id INTEGER,
    checkpoints_json TEXT,
    entry_quality REAL,
    exit_quality REAL,
    outcome_good INTEGER,
    decision_good INTEGER,
    classification TEXT,
    pnl REAL,
    details_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_outcome_log_class ON outcome_log(classification);

CREATE TABLE IF NOT EXISTS processing_status (
    job TEXT PRIMARY KEY,
    last_trade_date TEXT,
    last_timestamp TEXT,
    status TEXT,
    rows_written INTEGER,
    updated_at TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS latest_quotes (
    instrument_token INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT,
    timestamp TEXT,
    ingested_at TEXT,
    last_price REAL,
    last_quantity INTEGER,
    volume INTEGER,
    average_price REAL,
    oi INTEGER,
    total_buy_quantity INTEGER,
    total_sell_quantity INTEGER,
    best_bid_price REAL,
    best_bid_quantity INTEGER,
    best_ask_price REAL,
    best_ask_quantity INTEGER,
    bid_depth_5 INTEGER,
    ask_depth_5 INTEGER,
    spread REAL,
    mid_price REAL,
    depth_imbalance REAL,
    volume_delta INTEGER,
    oi_delta INTEGER,
    price_delta REAL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_latest_quotes_symbol ON latest_quotes(symbol);
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_feature_log_source_track_date "
                "ON feature_log(source, track, trade_date)"
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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_log_date_tier "
                "ON signal_log(trade_date, maturity_tier)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ingestion_meta_event_ts "
                "ON ingestion_meta(event_type, timestamp)"
            )

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

    def count_pooled_live_days(
        self,
        *,
        tracks: tuple[str, ...],
        skip_dates: Iterable[str] = (),
        underlying: str | None = None,
        min_span_hours: float | None = None,
        span_tracks: tuple[str, ...] | None = None,
    ) -> int:
        """Distinct source=live trade_dates for a class, without loading feature JSON."""
        skip = {str(d) for d in skip_dates}
        placeholders = ",".join("?" * len(tracks))
        sql = f"""
            SELECT DISTINCT trade_date FROM feature_log
            WHERE source = 'live'
              AND track IN ({placeholders})
              AND trade_date IS NOT NULL
        """
        params: list[Any] = list(tracks)
        if underlying:
            sql += """
              AND UPPER(COALESCE(
                    json_extract(features_json, '$.underlying'),
                    CASE WHEN UPPER(symbol) LIKE 'BANKNIFTY%' THEN 'BANKNIFTY'
                         ELSE 'NIFTY' END
              )) = ?
            """
            params.append(underlying.upper())
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        dates = {str(r[0]) for r in rows if r[0] and str(r[0]) not in skip}
        if min_span_hours is None or not dates:
            return len(dates)
        span_tracks = span_tracks or tracks
        short = self._live_dates_shorter_than(
            hours=min_span_hours, tracks=span_tracks, dates=dates
        )
        return len(dates - short)

    def _live_dates_shorter_than(
        self,
        *,
        hours: float,
        tracks: tuple[str, ...],
        dates: set[str],
    ) -> set[str]:
        if not dates:
            return set()
        placeholders_t = ",".join("?" * len(tracks))
        placeholders_d = ",".join("?" * len(dates))
        sql = f"""
            SELECT trade_date, MIN(timestamp), MAX(timestamp)
            FROM feature_log
            WHERE source = 'live'
              AND track IN ({placeholders_t})
              AND trade_date IN ({placeholders_d})
            GROUP BY trade_date
        """
        params: list[Any] = [*tracks, *dates]
        short: set[str] = set()
        with self.connection() as conn:
            for trade_date, first_ts, last_ts in conn.execute(sql, params):
                if first_ts is None or last_ts is None:
                    short.add(str(trade_date))
                    continue
                first = pd.Timestamp(first_ts)
                last = pd.Timestamp(last_ts)
                span_h = (last - first).total_seconds() / 3600.0
                if span_h < hours:
                    short.add(str(trade_date))
        return short

    def delete_live_engine_signals(
        self, trade_date: str, tracks: tuple[str, ...] | None = None
    ) -> int:
        """Remove live-engine rows only; baseline rows keep maturity_tier NULL."""
        with self.connection() as conn:
            if tracks:
                placeholders = ",".join("?" * len(tracks))
                cur = conn.execute(
                    f"""
                    DELETE FROM signal_log
                    WHERE trade_date = ? AND maturity_tier IS NOT NULL
                      AND track IN ({placeholders})
                    """,
                    (trade_date, *tracks),
                )
            else:
                cur = conn.execute(
                    """
                    DELETE FROM signal_log
                    WHERE trade_date = ? AND maturity_tier IS NOT NULL
                    """,
                    (trade_date,),
                )
            return int(cur.rowcount or 0)

    def fetch_latest_signal_logs(
        self, *, limit: int = 200, trade_date: str | None = None
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT id, timestamp, trade_date, symbol, track, model_version,
                   score, probability, features_json, source, attribution_json,
                   maturity_tier
            FROM signal_log
        """
        params: list[Any] = []
        if trade_date:
            sql += " WHERE trade_date = ?"
            params.append(trade_date)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(int(limit))
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["features"] = json.loads(item.pop("features_json") or "{}")
            attr = item.pop("attribution_json")
            item["attribution"] = json.loads(attr) if attr else None
            out.append(item)
        return out

    def fetch_public_signal_logs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Slim SELECT for the UI API — no features_json."""
        sql = """
            SELECT id, timestamp, trade_date, symbol, track, model_version,
                   score, probability, source, attribution_json, maturity_tier
            FROM signal_log
            WHERE maturity_tier IS NOT NULL
            ORDER BY timestamp DESC LIMIT ?
        """
        with self.connection() as conn:
            rows = conn.execute(sql, (int(limit),)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            attr_raw = item.pop("attribution_json")
            attr = json.loads(attr_raw) if attr_raw else {}
            maturity = attr.get("maturity") if isinstance(attr, dict) else None
            if not isinstance(maturity, dict):
                maturity = {}
            item["display"] = maturity.get("display") or attr.get("display")
            item["reason"] = maturity.get("reason") or attr.get("reason")
            item["maturity"] = maturity
            out.append(item)
        return out

    def signal_counts(self) -> dict[str, int]:
        with self.connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()
            live = conn.execute(
                "SELECT COUNT(*) FROM signal_log WHERE maturity_tier IS NOT NULL"
            ).fetchone()
            by_tier = conn.execute(
                """
                SELECT maturity_tier, COUNT(*) FROM signal_log
                WHERE maturity_tier IS NOT NULL
                GROUP BY maturity_tier
                """
            ).fetchall()
        counts = {
            "total": int(total[0] if total else 0),
            "live_engine": int(live[0] if live else 0),
        }
        for tier, n in by_tier:
            counts[str(tier)] = int(n)
        return counts

    def upsert_processing_status(
        self,
        job: str,
        *,
        status: str,
        last_trade_date: str | None = None,
        last_timestamp: str | None = None,
        rows_written: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO processing_status (
                    job, last_trade_date, last_timestamp, status,
                    rows_written, updated_at, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job) DO UPDATE SET
                    last_trade_date=excluded.last_trade_date,
                    last_timestamp=excluded.last_timestamp,
                    status=excluded.status,
                    rows_written=excluded.rows_written,
                    updated_at=excluded.updated_at,
                    details_json=excluded.details_json
                """,
                (
                    job,
                    last_trade_date,
                    last_timestamp,
                    status,
                    rows_written,
                    now,
                    json.dumps(details) if details else None,
                ),
            )

    def fetch_processing_status(self, job: str | None = None) -> dict[str, Any]:
        with self.connection() as conn:
            if job:
                row = conn.execute(
                    "SELECT * FROM processing_status WHERE job = ?", (job,)
                ).fetchone()
                if row is None:
                    return {"job": job, "status": "unknown"}
                item = dict(row)
                raw = item.pop("details_json", None)
                item["details"] = json.loads(raw) if raw else None
                return item
            rows = conn.execute("SELECT * FROM processing_status").fetchall()
        out: dict[str, Any] = {}
        for row in rows:
            item = dict(row)
            raw = item.pop("details_json", None)
            item["details"] = json.loads(raw) if raw else None
            out[str(item["job"])] = item
        return out

    def upsert_latest_quotes(self, rows: Iterable[dict[str, Any]]) -> int:
        """Replace the latest snapshot for each instrument_token. Not a tick log."""
        payload = list(rows)
        if not payload:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO latest_quotes (
                    instrument_token, symbol, exchange, timestamp, ingested_at,
                    last_price, last_quantity, volume, average_price, oi,
                    total_buy_quantity, total_sell_quantity,
                    best_bid_price, best_bid_quantity,
                    best_ask_price, best_ask_quantity,
                    bid_depth_5, ask_depth_5, spread, mid_price, depth_imbalance,
                    volume_delta, oi_delta, price_delta, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(instrument_token) DO UPDATE SET
                    symbol=excluded.symbol,
                    exchange=excluded.exchange,
                    timestamp=excluded.timestamp,
                    ingested_at=excluded.ingested_at,
                    last_price=excluded.last_price,
                    last_quantity=excluded.last_quantity,
                    volume=excluded.volume,
                    average_price=excluded.average_price,
                    oi=excluded.oi,
                    total_buy_quantity=excluded.total_buy_quantity,
                    total_sell_quantity=excluded.total_sell_quantity,
                    best_bid_price=excluded.best_bid_price,
                    best_bid_quantity=excluded.best_bid_quantity,
                    best_ask_price=excluded.best_ask_price,
                    best_ask_quantity=excluded.best_ask_quantity,
                    bid_depth_5=excluded.bid_depth_5,
                    ask_depth_5=excluded.ask_depth_5,
                    spread=excluded.spread,
                    mid_price=excluded.mid_price,
                    depth_imbalance=excluded.depth_imbalance,
                    volume_delta=excluded.volume_delta,
                    oi_delta=excluded.oi_delta,
                    price_delta=excluded.price_delta,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        r["instrument_token"],
                        r.get("symbol"),
                        r.get("exchange"),
                        r.get("timestamp"),
                        r.get("ingested_at"),
                        r.get("last_price"),
                        r.get("last_quantity"),
                        r.get("volume"),
                        r.get("average_price"),
                        r.get("oi"),
                        r.get("total_buy_quantity"),
                        r.get("total_sell_quantity"),
                        r.get("best_bid_price"),
                        r.get("best_bid_quantity"),
                        r.get("best_ask_price"),
                        r.get("best_ask_quantity"),
                        r.get("bid_depth_5"),
                        r.get("ask_depth_5"),
                        r.get("spread"),
                        r.get("mid_price"),
                        r.get("depth_imbalance"),
                        r.get("volume_delta"),
                        r.get("oi_delta"),
                        r.get("price_delta"),
                        now,
                    )
                    for r in payload
                ],
            )
        return len(payload)

    def fetch_latest_quote(self, symbol: str) -> dict[str, Any] | None:
        wanted = symbol.strip()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM latest_quotes
                WHERE symbol = ? COLLATE NOCASE
                LIMIT 1
                """,
                (wanted,),
            ).fetchone()
        return dict(row) if row else None

    def count_feature_logs_by_source(self, start: str, end: str) -> dict[str, int]:
        """Retrain composition — GROUP BY only, never fetch features_json."""
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT COALESCE(source, 'unknown'), COUNT(*)
                FROM feature_log
                WHERE trade_date >= ? AND trade_date <= ?
                GROUP BY COALESCE(source, 'unknown')
                """,
                (start, end),
            ).fetchall()
        return {str(k): int(v) for k, v in rows}

    def harness_passed_classes(self) -> dict[str, bool]:
        """SQL check only — does not load joblib."""
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT track FROM model_registry
                WHERE harness_passed = 1
                """
            ).fetchall()
        passed = {str(r[0]) for r in rows}
        return {
            key: key in passed for key in ("equity", "options", "futures")
        }

    def latest_ingestion_event(self, event_type: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT timestamp, event_type, message, rows_written
                FROM ingestion_meta
                WHERE event_type = ?
                ORDER BY timestamp DESC LIMIT 1
                """,
                (event_type,),
            ).fetchone()
        return dict(row) if row else None

    def fetch_feature_logs_since(
        self,
        trade_date: str,
        *,
        after_timestamp: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT id, timestamp, trade_date, symbol, track, features_json,
                   actual_outcome, source, feature_completeness
            FROM feature_log
            WHERE trade_date = ?
        """
        params: list[Any] = [trade_date]
        if after_timestamp:
            sql += " AND timestamp > ?"
            params.append(after_timestamp)
        sql += " ORDER BY timestamp, symbol"
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

    def insert_order_events(self, events: Iterable[dict[str, Any]]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        payload = list(events)
        if not payload:
            return 0
        inserted = 0
        with self.connection() as conn:
            for ev in payload:
                order_id = str(ev.get("order_id") or "")
                status = ev.get("status")
                ots = str(ev.get("order_timestamp") or ev.get("exchange_timestamp") or "")
                exists = conn.execute(
                    """
                    SELECT 1 FROM order_events
                    WHERE order_id = ? AND IFNULL(status,'') = IFNULL(?, '')
                      AND IFNULL(order_timestamp,'') = ?
                    LIMIT 1
                    """,
                    (order_id, status, ots),
                ).fetchone()
                if exists:
                    continue
                conn.execute(
                    """
                    INSERT INTO order_events (
                        captured_at, order_id, status, symbol, exchange,
                        transaction_type, quantity, price, filled_quantity,
                        average_price, order_timestamp, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        order_id,
                        status,
                        ev.get("tradingsymbol") or ev.get("symbol"),
                        ev.get("exchange"),
                        ev.get("transaction_type"),
                        ev.get("quantity"),
                        ev.get("price"),
                        ev.get("filled_quantity"),
                        ev.get("average_price"),
                        ots or None,
                        json.dumps(ev, default=str),
                    ),
                )
                inserted += 1
        return inserted

    def insert_trade_fills(self, fills: Iterable[dict[str, Any]]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        with self.connection() as conn:
            for fill in fills:
                kite_id = str(fill.get("trade_id") or fill.get("kite_trade_id") or "") or None
                if kite_id:
                    exists = conn.execute(
                        "SELECT 1 FROM trade_fills WHERE kite_trade_id = ? LIMIT 1",
                        (kite_id,),
                    ).fetchone()
                    if exists:
                        continue
                conn.execute(
                    """
                    INSERT INTO trade_fills (
                        captured_at, kite_trade_id, order_id, trade_id, symbol,
                        exchange, transaction_type, quantity, price, fill_timestamp,
                        fill_vs_decision_json, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        kite_id,
                        fill.get("order_id"),
                        fill.get("linked_trade_id"),
                        fill.get("tradingsymbol") or fill.get("symbol"),
                        fill.get("exchange"),
                        fill.get("transaction_type"),
                        fill.get("quantity"),
                        fill.get("average_price") or fill.get("price"),
                        str(fill.get("fill_timestamp") or fill.get("exchange_timestamp") or "")
                        or None,
                        json.dumps(fill.get("fill_vs_decision"))
                        if fill.get("fill_vs_decision") is not None
                        else None,
                        json.dumps(fill, default=str),
                    ),
                )
                inserted += 1
        return inserted

    def insert_positions_snapshot(self, payload: dict[str, Any], *, kind: str = "net") -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO positions_snapshot (captured_at, kind, payload_json)
                VALUES (?, ?, ?)
                """,
                (now, kind, json.dumps(payload, default=str)),
            )

    def insert_margin_snapshot(self, payload: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO margin_snapshot (captured_at, payload_json) VALUES (?, ?)",
                (now, json.dumps(payload, default=str)),
            )

    def insert_holdings_snapshot(self, payload: Any) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO holdings_snapshot (captured_at, payload_json) VALUES (?, ?)",
                (now, json.dumps(payload, default=str)),
            )

    def insert_session_audit(
        self, *, profile: dict[str, Any] | None, margins: dict[str, Any] | None
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO session_audit (captured_at, profile_json, margins_json)
                VALUES (?, ?, ?)
                """,
                (
                    now,
                    json.dumps(profile, default=str) if profile is not None else None,
                    json.dumps(margins, default=str) if margins is not None else None,
                ),
            )

    def insert_decision(self, row: dict[str, Any]) -> str:
        now = datetime.now(timezone.utc).isoformat()
        trade_id = str(row["trade_id"])
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO decision_log (
                    created_at, trade_id, timestamp, trade_date, symbol, track, side,
                    suggested_qty, suggested_price, actor, signal_log_id,
                    strike_rationale, sizing_rationale, timing_rationale, iv_rank,
                    risk_json, maturity_view_json, extras_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    trade_id,
                    row["timestamp"],
                    row.get("trade_date"),
                    row["symbol"],
                    row.get("track"),
                    row.get("side"),
                    row.get("suggested_qty"),
                    row.get("suggested_price"),
                    row["actor"],
                    row.get("signal_log_id"),
                    row.get("strike_rationale"),
                    row.get("sizing_rationale"),
                    row.get("timing_rationale"),
                    row.get("iv_rank"),
                    json.dumps(row.get("risk") or {}),
                    json.dumps(row["maturity_view"]),
                    json.dumps(row.get("extras") or {}),
                ),
            )
        return trade_id

    def fetch_decision(self, trade_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM decision_log WHERE trade_id = ?", (trade_id,)
            ).fetchone()
        if row is None:
            return None
        return _decode_decision(dict(row))

    def fetch_decisions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM decision_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_decode_decision(dict(r)) for r in rows]

    def link_fill_trade_id(
        self,
        *,
        fill_pk: int,
        trade_id: str,
        fill_vs_decision: dict[str, Any] | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE trade_fills
                SET trade_id = ?, fill_vs_decision_json = ?
                WHERE id = ?
                """,
                (
                    trade_id,
                    json.dumps(fill_vs_decision) if fill_vs_decision is not None else None,
                    fill_pk,
                ),
            )

    def fetch_fills(self, *, limit: int = 100, unlinked_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM trade_fills"
        if unlinked_only:
            sql += " WHERE trade_id IS NULL OR trade_id = ''"
        sql += " ORDER BY id DESC LIMIT ?"
        with self.connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def fetch_public_fills(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Compact fill rows for the UI: ids, symbol, qty, price, time."""
        sql = """
            SELECT id, kite_trade_id, order_id, trade_id, symbol,
                   quantity, price, fill_timestamp
            FROM trade_fills
            ORDER BY id DESC LIMIT ?
        """
        with self.connection() as conn:
            rows = conn.execute(sql, (int(limit),)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            out.append(
                {
                    "id": item.get("id"),
                    "kite_trade_id": item.get("kite_trade_id"),
                    "order_id": item.get("order_id"),
                    "trade_id": item.get("trade_id"),
                    "symbol": item.get("symbol"),
                    "qty": item.get("quantity"),
                    "price": item.get("price"),
                    "time": item.get("fill_timestamp"),
                }
            )
        return out

    def fetch_fills_for_trade(self, trade_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM trade_fills WHERE trade_id = ? ORDER BY id", (trade_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    def upsert_outcome(self, row: dict[str, Any]) -> None:
        with self.connection() as conn:
            existing = conn.execute(
                "SELECT id FROM outcome_log WHERE trade_id = ?", (row["trade_id"],)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE outcome_log SET
                        entry_fill_id=?, exit_fill_id=?, checkpoints_json=?,
                        entry_quality=?, exit_quality=?, outcome_good=?,
                        decision_good=?, classification=?, pnl=?, details_json=?
                    WHERE trade_id=?
                    """,
                    (
                        row.get("entry_fill_id"),
                        row.get("exit_fill_id"),
                        json.dumps(row.get("checkpoints") or {}),
                        row.get("entry_quality"),
                        row.get("exit_quality"),
                        row.get("outcome_good"),
                        row.get("decision_good"),
                        row.get("classification"),
                        row.get("pnl"),
                        json.dumps(row.get("details") or {}),
                        row["trade_id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO outcome_log (
                        trade_id, entry_fill_id, exit_fill_id, checkpoints_json,
                        entry_quality, exit_quality, outcome_good, decision_good,
                        classification, pnl, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["trade_id"],
                        row.get("entry_fill_id"),
                        row.get("exit_fill_id"),
                        json.dumps(row.get("checkpoints") or {}),
                        row.get("entry_quality"),
                        row.get("exit_quality"),
                        row.get("outcome_good"),
                        row.get("decision_good"),
                        row.get("classification"),
                        row.get("pnl"),
                        json.dumps(row.get("details") or {}),
                    ),
                )

    def fetch_outcomes(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outcome_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["checkpoints"] = json.loads(item.pop("checkpoints_json") or "{}")
            item["details"] = json.loads(item.pop("details_json") or "{}")
            out.append(item)
        return out

    def outcome_class_counts(self) -> dict[str, int]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT classification, COUNT(*) FROM outcome_log GROUP BY classification"
            ).fetchall()
        return {str(k): int(v) for k, v in rows if k}

    def latest_json_row(self, table: str) -> dict[str, Any] | None:
        allowed = {
            "positions_snapshot",
            "margin_snapshot",
            "holdings_snapshot",
            "session_audit",
        }
        if table not in allowed:
            raise ValueError(f"unsupported table {table}")
        with self.connection() as conn:
            row = conn.execute(
                f"SELECT * FROM {table} ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        if "payload_json" in item and item["payload_json"]:
            item["payload"] = json.loads(item["payload_json"])
        return item

    def pipeline_health(self) -> dict[str, Any]:
        with self.connection() as conn:
            ingest = conn.execute(
                """
                SELECT timestamp, event_type, message FROM ingestion_meta
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            last_feature = conn.execute(
                "SELECT MAX(trade_date) FROM feature_log"
            ).fetchone()
            last_live = conn.execute(
                "SELECT MAX(trade_date) FROM feature_log WHERE source='live'"
            ).fetchone()
            last_signal = conn.execute(
                "SELECT MAX(trade_date) FROM signal_log WHERE maturity_tier IS NOT NULL"
            ).fetchone()
            n_decisions = conn.execute("SELECT COUNT(*) FROM decision_log").fetchone()
            n_fills = conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()
        jobs = ("features", "labels", "signals", "account")
        status_map = self.fetch_processing_status()
        processing = {
            job: status_map.get(job) or {"job": job, "status": "unknown"}
            for job in jobs
        }
        compact = self.latest_ingestion_event("compact") or self.latest_ingestion_event(
            "compaction"
        )
        coverage = self.latest_ingestion_event("session_coverage")
        return {
            "last_ingestion_meta": dict(ingest) if ingest else None,
            "last_compact": compact,
            "last_coverage": coverage,
            "last_feature_trade_date": last_feature[0] if last_feature else None,
            "last_live_feature_trade_date": last_live[0] if last_live else None,
            "last_live_signal_trade_date": last_signal[0] if last_signal else None,
            "processing_status": processing,
            "decision_count": int(n_decisions[0] if n_decisions else 0),
            "fill_count": int(n_fills[0] if n_fills else 0),
        }


def _decode_decision(item: dict[str, Any]) -> dict[str, Any]:
    item["maturity_view"] = json.loads(item.pop("maturity_view_json") or "{}")
    item["risk"] = json.loads(item.pop("risk_json") or "{}")
    item["extras"] = json.loads(item.pop("extras_json") or "{}")
    return item
