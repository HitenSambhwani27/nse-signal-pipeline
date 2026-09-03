"""Observed --execute pacing: depth vs quote, remaining ETA."""

from __future__ import annotations

import json
import statistics
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings
from nse_pipeline.historical.universe import cache_counts


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _stats(seconds: list[float]) -> dict[str, float | None]:
    if not seconds:
        return {"n": 0, "mean_s": None, "median_s": None, "p10_s": None, "p90_s": None}
    ordered = sorted(seconds)
    n = len(ordered)

    def pct(p: float) -> float:
        idx = min(n - 1, max(0, int(round((p / 100.0) * (n - 1)))))
        return round(ordered[idx], 1)

    return {
        "n": n,
        "mean_s": round(statistics.mean(ordered), 1),
        "median_s": round(statistics.median(ordered), 1),
        "p10_s": pct(10),
        "p90_s": pct(90),
    }


def main() -> int:
    settings = load_settings()
    counts = cache_counts(settings)
    con = sqlite3.connect(settings.paths.sqlite_db)
    execute_start = "2026-08-14T11:42:00"
    rows = con.execute(
        "select timestamp, symbol, details_json from ingestion_meta "
        "where event_type='historical_backfill_symbol' and timestamp >= ? "
        "order by id",
        (execute_start,),
    ).fetchall()

    by_class: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    seen: set[str] = set()
    for ts, symbol, details_json in rows:
        payload = json.loads(details_json) if details_json else {}
        cls = str(payload.get("class") or "?")
        key = f"{cls}:{symbol}:{payload.get('expiry')}"
        if key in seen:
            continue
        seen.add(key)
        by_class[cls].append((_parse_ts(ts), symbol))

    gaps: dict[str, list[float]] = defaultdict(list)
    chronological = []
    for cls, items in by_class.items():
        items.sort(key=lambda x: x[0])
        for prev, cur in zip(items, items[1:]):
            gaps[cls].append((cur[0] - prev[0]).total_seconds())
        chronological.extend((ts, cls, sym) for ts, sym in items)
    chronological.sort(key=lambda x: x[0])

    # Recent quote-only pace (last 25 quote completions).
    quote_items = by_class.get("equity_quote", [])
    recent_quote_gaps: list[float] = []
    if len(quote_items) >= 2:
        recent = quote_items[-26:] if len(quote_items) >= 26 else quote_items
        recent_quote_gaps = [
            (b[0] - a[0]).total_seconds() for a, b in zip(recent, recent[1:])
        ]

    now = datetime.now(timezone.utc)
    last_ts = chronological[-1][0] if chronological else None
    quote_done = len(by_class.get("equity_quote", []))
    depth_done = len(by_class.get("equity_depth", []))
    quote_left = counts["equity_quote"] - quote_done
    index_left = counts["index"] - len(by_class.get("index", []))
    fut_left = counts["futures"] - len(by_class.get("futures", []))
    opt_left = counts["options"] - len(by_class.get("options", []))

    quote_median = _stats(gaps.get("equity_quote", [])).get("median_s") or 0
    recent_median = _stats(recent_quote_gaps).get("median_s") or quote_median
    depth_median = _stats(gaps.get("equity_depth", [])).get("median_s") or 0
    pace = recent_median or quote_median or depth_median

    # Historical path is the same candles for depth and quote (no L2 history).
    # F&O is lifetime-bound: ~2-4 chunks vs ~8 equity chunks, and far fewer
    # parquet partitions. Use 0.35× recent quote median as a conservative FO pace
    # until those classes actually start (do not pretend we observed it).
    fo_pace = max(30.0, pace * 0.35) if pace else None
    equity_like_left = quote_left + index_left
    fo_left = fut_left + opt_left
    eta_equity_s = equity_like_left * pace if pace else None
    eta_fo_s = fo_left * fo_pace if fo_pace else None
    eta_s = (eta_equity_s or 0) + (eta_fo_s or 0)

    def hours(sec: float | None) -> float | None:
        return None if sec is None else round(sec / 3600.0, 2)

    print(
        json.dumps(
            {
                "now_utc": now.isoformat(),
                "last_symbol_utc": last_ts.isoformat() if last_ts else None,
                "seconds_since_last_symbol": (
                    round((now - last_ts).total_seconds(), 1) if last_ts else None
                ),
                "completed": {k: len(v) for k, v in by_class.items()},
                "universe": counts,
                "remaining": {
                    "equity_quote": quote_left,
                    "index": index_left,
                    "futures": fut_left,
                    "options": opt_left,
                    "total": quote_left + index_left + fut_left + opt_left,
                },
                "observed_sec_per_symbol": {
                    "equity_depth": _stats(gaps.get("equity_depth", [])),
                    "equity_quote": _stats(gaps.get("equity_quote", [])),
                    "equity_quote_recent_25": _stats(recent_quote_gaps),
                },
                "pace_used_sec": pace,
                "fo_assumed_sec": fo_pace,
                "eta_hours": {
                    "remaining_quote_plus_index": hours(eta_equity_s),
                    "remaining_fo_assumed": hours(eta_fo_s),
                    "total": hours(eta_s),
                },
                "eta_finish_utc": (
                    datetime.fromtimestamp(now.timestamp() + eta_s, tz=timezone.utc).isoformat()
                    if eta_s
                    else None
                ),
                "note": (
                    "Depth and quote historical pulls are the same Kite candle "
                    "chunks (no L2 history). Quote pace is observed, not assumed faster. "
                    "F&O ETA is a chunk-count assumption until those classes start."
                ),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
