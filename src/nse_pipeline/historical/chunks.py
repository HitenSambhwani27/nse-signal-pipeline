"""Date-range chunking for Kite historical_data per-interval caps."""

from __future__ import annotations

from datetime import date, timedelta

from nse_pipeline.config import HistoricalSettings


def chunk_range(
    start: date, end: date, *, max_days: int
) -> list[tuple[date, date]]:
    if end < start:
        return []
    chunks: list[tuple[date, date]] = []
    cur = start
    step = max(1, max_days)
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=step - 1))
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def max_days_for_interval(historical: HistoricalSettings, interval: str) -> int:
    return int(historical.interval_max_days.get(interval, 60))


def calendar_days(start: date, end: date) -> int:
    return (end - start).days + 1
