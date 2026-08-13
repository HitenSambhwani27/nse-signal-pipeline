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
        for symbol in self.list_symbols(date_str):
            path_sql = str(self._ticks_path(date_str, symbol)).replace("\\", "/")
            count = int(
                self._con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{path_sql}')"
                ).fetchone()[0]
            )
            rows.append({"date": date_str, "symbol": symbol, "rows": count})
        return pd.DataFrame(rows)

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
