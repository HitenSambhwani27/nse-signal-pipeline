"""Analyze timeout WARNINGs vs compacted minute coverage (post-backfill)."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from nse_pipeline.config import Settings

_WARN = re.compile(
    r"historical_data failed (?P<symbol>.+?) (?P<interval>\S+) "
    r"(?P<start>\d{4}-\d{2}-\d{2})(?:→|->|\\u2192)(?P<end>\d{4}-\d{2}-\d{2})"
)


def _normalize_log(text: str) -> str:
    """Cursor terminal snapshots often store → as the six-char escape \\u2192."""
    return text.replace("\\u2192", "→")


def parse_warning_windows(log_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for match in _WARN.finditer(_normalize_log(log_text)):
        key = (
            match.group("symbol").strip(),
            match.group("interval"),
            match.group("start"),
            match.group("end"),
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "symbol": key[0],
                "interval": key[1],
                "start": key[2],
                "end": key[3],
            }
        )
    return rows


def dates_in_window(start: str, end: str) -> list[str]:
    cur = date.fromisoformat(start)
    last = date.fromisoformat(end)
    out: list[str] = []
    while cur <= last:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _has_ticks(settings: Settings, day: str, symbol: str) -> bool:
    return (settings.paths.compacted_dir / day / symbol / "ticks.parquet").exists()


def window_coverage(
    settings: Settings,
    symbol: str,
    start: str,
    end: str,
    *,
    reference_symbol: str = "RELIANCE",
) -> dict[str, Any]:
    """
    Compare a timeout window against a reference symbol.

    Weekdays the reference also lacks (holidays) are not counted as gaps.
    A day the reference has and this symbol does not is a real hole.
    """
    present: list[str] = []
    missing_vs_ref: list[str] = []
    for day in dates_in_window(start, end):
        if _has_ticks(settings, day, symbol):
            present.append(day)
            continue
        if date.fromisoformat(day).weekday() >= 5:
            continue
        if _has_ticks(settings, day, reference_symbol):
            missing_vs_ref.append(day)
    recent_missing = [d for d in missing_vs_ref if d >= "2026-04-01"]
    return {
        "symbol": symbol,
        "window": [start, end],
        "present_days": len(present),
        "missing_vs_reference": missing_vs_ref,
        "recent_missing_vs_reference": recent_missing,
        "recovered": len(missing_vs_ref) == 0 and len(present) > 0,
        "recent_gap": len(recent_missing) > 0,
        "empty_window": len(present) == 0,
    }


def analyze_log(
    settings: Settings,
    log_path: Path,
    *,
    reference_symbol: str = "RELIANCE",
) -> dict[str, Any]:
    text = _normalize_log(log_path.read_text(encoding="utf-8", errors="replace"))
    windows = parse_warning_windows(text)
    details = [
        window_coverage(
            settings,
            w["symbol"],
            w["start"],
            w["end"],
            reference_symbol=reference_symbol,
        )
        for w in windows
    ]
    recovered = [d["symbol"] for d in details if d["recovered"]]
    gaps = [d for d in details if not d["recovered"]]
    recent = [d for d in details if d["recent_gap"]]
    return {
        "warning_windows": windows,
        "window_details": [
            {
                "symbol": d["symbol"],
                "window": d["window"],
                "present_days": d["present_days"],
                "missing_n": len(d["missing_vs_reference"]),
                "recent_missing_n": len(d["recent_missing_vs_reference"]),
                "recovered": d["recovered"],
                "empty_window": d["empty_window"],
                "missing_sample": d["missing_vs_reference"][:5],
            }
            for d in details
        ],
        "recovered_on_later_retry": recovered,
        "permanent_gaps": gaps,
        "recent_gaps_2026_04_onward": recent,
    }
