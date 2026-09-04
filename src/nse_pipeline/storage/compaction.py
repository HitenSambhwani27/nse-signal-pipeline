"""
Daily tick compaction: merge per-minute Parquet parts into one file per symbol/day.

Output layout:
  {compacted_dir}/{YYYY-MM-DD}/{symbol}/ticks.parquet

Idempotent — re-running overwrites the compacted file for that symbol/day.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from nse_pipeline.config import Settings


logger = logging.getLogger(__name__)


@dataclass
class CompactSymbolResult:
    date_str: str
    symbol: str
    source_files: int
    rows: int
    output_path: Path | None
    archived: bool
    skipped_reason: str | None = None


def list_minute_tick_files(symbol_dir: Path) -> list[Path]:
    """Return ticks_HHMM.parquet parts (excludes already-compacted ticks.parquet)."""
    return sorted(symbol_dir.glob("ticks_*.parquet"))


def compact_symbol_day(
    *,
    raw_symbol_dir: Path,
    compacted_symbol_dir: Path,
    archive_minute_files: bool,
    archive_subdir: str,
) -> CompactSymbolResult:
    """
    Merge all ticks_*.parquet in raw_symbol_dir into compacted_symbol_dir/ticks.parquet.

    Rows are ordered by timestamp. Duplicate *files* from re-flush still appear
    as extra observations if the same packet is written twice; this merge does
    not invent unique trade IDs and does not drop same-price consecutive ticks.
    """
    date_str = raw_symbol_dir.parent.name
    symbol = raw_symbol_dir.name
    parts = list_minute_tick_files(raw_symbol_dir)
    if not parts:
        return CompactSymbolResult(
            date_str=date_str,
            symbol=symbol,
            source_files=0,
            rows=0,
            output_path=None,
            archived=False,
            skipped_reason="no_minute_parts",
        )

    # DuckDB glob over the symbol folder — path uses forward slashes for SQL.
    glob_path = str(raw_symbol_dir / "ticks_*.parquet").replace("\\", "/")
    out_dir = compacted_symbol_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "ticks.parquet"
    out_sql = str(out_path).replace("\\", "/")

    con = duckdb.connect(database=":memory:")
    try:
        # Rows are ordered by timestamp. All observations are kept (union_by_name
        # so additive tick columns survive). Re-flush of the same minute file
        # concatenates; this is not a unique-trade filter.
        con.execute(
            f"""
            COPY (
                SELECT *, 'live' AS source
                FROM read_parquet('{glob_path}', union_by_name=true)
                ORDER BY timestamp, instrument_token, last_price, volume
            ) TO '{out_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        row_count = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_sql}')").fetchone()[0])
    finally:
        con.close()

    archived = False
    if archive_minute_files:
        archive_dir = raw_symbol_dir / archive_subdir
        archive_dir.mkdir(parents=True, exist_ok=True)
        for part in parts:
            dest = archive_dir / part.name
            if dest.exists():
                dest.unlink()
            shutil.move(str(part), str(dest))
        archived = True

    # Also copy candles.parquet into compacted tree when present (already one file/day).
    candles_src = raw_symbol_dir / "candles.parquet"
    if candles_src.exists():
        shutil.copy2(candles_src, out_dir / "candles.parquet")

    return CompactSymbolResult(
        date_str=date_str,
        symbol=symbol,
        source_files=len(parts),
        rows=row_count,
        output_path=out_path,
        archived=archived,
    )


def compact_date(settings: Settings, date_str: str) -> list[CompactSymbolResult]:
    """Compact every symbol folder under data/raw/{date_str}/."""
    day_dir = settings.paths.raw_dir / date_str
    if not day_dir.exists():
        raise FileNotFoundError(f"No raw data for date {date_str} under {day_dir}")

    results: list[CompactSymbolResult] = []
    for symbol_dir in sorted(p for p in day_dir.iterdir() if p.is_dir()):
        result = compact_symbol_day(
            raw_symbol_dir=symbol_dir,
            compacted_symbol_dir=settings.paths.compacted_dir / date_str / symbol_dir.name,
            archive_minute_files=settings.compaction.archive_minute_files,
            archive_subdir=settings.compaction.archive_subdir,
        )
        results.append(result)
        if result.skipped_reason:
            logger.info("Skip %s/%s: %s", date_str, result.symbol, result.skipped_reason)
        else:
            logger.info(
                "Compacted %s/%s: %s files -> %s rows (%s)",
                date_str,
                result.symbol,
                result.source_files,
                result.rows,
                result.output_path,
            )
    return results


def compact_all_dates(settings: Settings) -> list[CompactSymbolResult]:
    """Compact every YYYY-MM-DD folder under raw_dir that has minute tick parts."""
    raw = settings.paths.raw_dir
    if not raw.exists():
        return []
    results: list[CompactSymbolResult] = []
    for day_dir in sorted(p for p in raw.iterdir() if p.is_dir()):
        # Only process days that have at least one ticks_*.parquet somewhere.
        if not any(day_dir.glob("*/ticks_*.parquet")):
            continue
        results.extend(compact_date(settings, day_dir.name))
    return results


def verify_compacted_file(path: Path) -> pd.DataFrame:
    """Read compacted ticks for smoke checks / tests."""
    return pd.read_parquet(path)
