"""
DuckDB query layer over compacted Parquet (and optional raw candles).

Stage 2+ feature jobs should use this module instead of globbing minute parts
with pandas.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from nse_pipeline.config import Settings


class DuckDBTickStore:
    """
    Thin wrapper around an in-process DuckDB connection.

    Partition layout (compacted):
      {compacted_dir}/{YYYY-MM-DD}/{symbol}/ticks.parquet
      {compacted_dir}/{YYYY-MM-DD}/{symbol}/candles.parquet  (copied when present)

    Raw candles (if not yet copied) remain readable via read_candles().
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.compacted_dir = settings.paths.compacted_dir
        self.raw_dir = settings.paths.raw_dir
        self._con = duckdb.connect(database=":memory:")

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "DuckDBTickStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _ticks_path(self, date_str: str, symbol: str) -> Path:
        return self.compacted_dir / date_str / symbol / "ticks.parquet"

    def _candles_path(self, date_str: str, symbol: str) -> Path:
        compacted = self.compacted_dir / date_str / symbol / "candles.parquet"
        if compacted.exists():
            return compacted
        return self.raw_dir / date_str / symbol / "candles.parquet"

    def list_compacted_dates(self) -> list[str]:
        if not self.compacted_dir.exists():
            return []
        return sorted(p.name for p in self.compacted_dir.iterdir() if p.is_dir())

    def list_symbols(self, date_str: str) -> list[str]:
        day = self.compacted_dir / date_str
        if not day.exists():
            return []
        return sorted(
            p.name
            for p in day.iterdir()
            if p.is_dir()
            and ((p / "ticks.parquet").exists() or (p / "daily.parquet").exists())
        )

    def list_tick_symbols(self, date_str: str) -> list[str]:
        day = self.compacted_dir / date_str
        if not day.exists():
            return []
        return sorted(
            p.name
            for p in day.iterdir()
            if p.is_dir() and (p / "ticks.parquet").exists()
        )

    def query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Run arbitrary SQL (advanced). Prefer typed helpers below for Stage 2."""
        if params:
            return self._con.execute(sql, params).fetchdf()
        return self._con.execute(sql).fetchdf()

    def read_ticks(
        self,
        date_str: str,
        symbol: str,
        *,
        start_ts: str | None = None,
        end_ts: str | None = None,
    ) -> pd.DataFrame:
        path = self._ticks_path(date_str, symbol)
        if not path.exists():
            raise FileNotFoundError(
                f"Compacted ticks not found: {path}. Run scripts/03_run_compaction.py first."
            )
        path_sql = str(path).replace("\\", "/")
        clauses: list[str] = []
        params: list[Any] = []
        if start_ts:
            clauses.append("timestamp >= ?")
            params.append(start_ts)
        if end_ts:
            clauses.append("timestamp <= ?")
            params.append(end_ts)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM read_parquet('{path_sql}'){where} ORDER BY timestamp"
        return self.query(sql, params or None)

    def read_candles(self, date_str: str, symbol: str) -> pd.DataFrame:
        path = self._candles_path(date_str, symbol)
        if not path.exists():
            raise FileNotFoundError(f"Candles not found for {date_str}/{symbol}: {path}")
        path_sql = str(path).replace("\\", "/")
        return self.query(
            f"SELECT * FROM read_parquet('{path_sql}') ORDER BY timestamp"
        )

    def tick_row_counts(self, date_str: str) -> pd.DataFrame:
        """Return symbol, row_count for all compacted symbols on a date."""
        rows: list[dict[str, Any]] = []
        for symbol in self.list_tick_symbols(date_str):
            path_sql = str(self._ticks_path(date_str, symbol)).replace("\\", "/")
            count = int(
                self._con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{path_sql}')"
                ).fetchone()[0]
            )
            rows.append({"date": date_str, "symbol": symbol, "rows": count})
        return pd.DataFrame(rows)

    def tick_time_span(
        self, date_str: str, symbols: list[str] | None = None
    ) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        """
        Min/max tick timestamps across compacted symbols for a date.

        Uses VARCHAR cast for min/max to avoid DuckDB tz/pytz requirements on
        timestamp aggregates; samples symbols on large days for speed.
        Pass `symbols` to avoid opening parquet for unrelated names (e.g. while
        another process is still writing other symbols' files).
        """
        symbols = list(symbols) if symbols is not None else self.list_tick_symbols(date_str)
        if not symbols:
            return None, None
        if len(symbols) <= 50:
            sample = symbols
        else:
            step = max(1, len(symbols) // 50)
            sample = symbols[::step]
        first: pd.Timestamp | None = None
        last: pd.Timestamp | None = None
        for symbol in sample:
            path_sql = str(self._ticks_path(date_str, symbol)).replace("\\", "/")
            row = self._con.execute(
                f"""
                SELECT min(CAST(timestamp AS VARCHAR)), max(CAST(timestamp AS VARCHAR))
                FROM read_parquet('{path_sql}')
                """
            ).fetchone()
            if not row or row[0] is None:
                continue
            t0 = pd.Timestamp(row[0])
            t1 = pd.Timestamp(row[1])
            if t0.tzinfo is None:
                t0 = t0.tz_localize("UTC")
            if t1.tzinfo is None:
                t1 = t1.tz_localize("UTC")
            first = t0 if first is None or t0 < first else first
            last = t1 if last is None or t1 > last else last
        return first, last

    def scan_day_ticks(self, date_str: str) -> pd.DataFrame:
        """
        Union all compacted ticks for a date (hive-style glob).

        Useful for cross-symbol features; may be large — filter in SQL when possible.
        """
        glob_path = str(self.compacted_dir / date_str / "*" / "ticks.parquet").replace(
            "\\", "/"
        )
        return self.query(
            f"""
            SELECT * FROM read_parquet('{glob_path}', union_by_name=true, hive_partitioning=false)
            ORDER BY symbol, timestamp
            """
        )

    def _daily_path(self, date_str: str, symbol: str) -> Path:
        return self.compacted_dir / date_str / symbol / "daily.parquet"

    def close_on(self, date_str: str, symbol: str) -> float | None:
        """Last close for a symbol on a date (ticks, else daily.parquet)."""
        from nse_pipeline.features.quality import session_close_price

        ticks_path = self._ticks_path(date_str, symbol)
        if ticks_path.exists():
            try:
                df = self.read_ticks(date_str, symbol)
            except FileNotFoundError:
                df = pd.DataFrame()
            px = session_close_price(df)
            if px is not None:
                return px
        daily = self._daily_path(date_str, symbol)
        if daily.exists():
            df = pd.read_parquet(daily)
            return session_close_price(df)
        return None

    def open_on(self, date_str: str, symbol: str) -> float | None:
        from nse_pipeline.features.quality import session_open_price

        ticks_path = self._ticks_path(date_str, symbol)
        if ticks_path.exists():
            try:
                df = self.read_ticks(date_str, symbol)
            except FileNotFoundError:
                df = pd.DataFrame()
            px = session_open_price(df)
            if px is not None:
                return px
        daily = self._daily_path(date_str, symbol)
        if daily.exists():
            df = pd.read_parquet(daily)
            return session_open_price(df)
        return None

    def last_close_before(self, date_str: str, symbol: str) -> float | None:
        """Most recent close strictly before date_str (ticks or daily)."""
        for prior in reversed(self.list_compacted_dates()):
            if prior >= date_str:
                continue
            px = self.close_on(prior, symbol)
            if px is not None:
                return px
        return None
