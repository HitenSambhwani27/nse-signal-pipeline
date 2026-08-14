"""Pooled live-day maturity gate (per class, not per contract)."""

from __future__ import annotations

from typing import Any, Literal

from nse_pipeline.config import Settings
from nse_pipeline.storage.sqlite_store import SQLiteStore

Tier = Literal["suppressed", "provisional", "full"]

CLASS_FROM_TRACK = {
    "equity_depth": "equity",
    "equity_quote": "equity",
    "index": "equity",
    "options": "options",
    "futures": "futures",
}


def pooled_live_days(
    settings: Settings,
    *,
    class_key: str,
    underlying: str | None = None,
) -> int:
    """
    Count distinct trade_dates with source=live feature rows for a class.

    For options, pass underlying=NIFTY or BANKNIFTY so weekly vs monthly
    cadences accumulate separately. Counting per contract would leave Nifty
    weeklies permanently provisional — that is a bug.
    """
    store = SQLiteStore(settings.paths.sqlite_db)
    rows = store.fetch_feature_logs_range("0000-01-01", "9999-12-31")
    dates: set[str] = set()
    for row in rows:
        if str(row.get("source")) != "live":
            continue
        track = str(row.get("track") or "")
        if CLASS_FROM_TRACK.get(track, "equity") != class_key:
            continue
        if class_key == "options" and underlying:
            feats = row.get("features") or {}
            name = str(feats.get("underlying") or "")
            symbol = str(row.get("symbol") or "").upper()
            got = name.upper() if name else (
                "BANKNIFTY" if symbol.startswith("BANKNIFTY") else "NIFTY"
            )
            if got != underlying.upper():
                continue
        d = row.get("trade_date")
        if d:
            dates.add(str(d))
    return len(dates)


def classify_tier(days: int, settings: Settings) -> Tier:
    if days < settings.maturity_gate.suppress_below_days:
        return "suppressed"
    if days < settings.maturity_gate.provisional_below_days:
        return "provisional"
    return "full"


def maturity_snapshot(settings: Settings) -> dict[str, Any]:
    """Report pooled live days before finalizing thresholds."""
    out: dict[str, Any] = {}
    for class_key in ("equity", "futures"):
        days = pooled_live_days(settings, class_key=class_key)
        out[class_key] = {
            "pooled_live_days": days,
            "tier": classify_tier(days, settings),
        }
    for und in ("NIFTY", "BANKNIFTY"):
        days = pooled_live_days(settings, class_key="options", underlying=und)
        out[f"options_{und.lower()}"] = {
            "pooled_live_days": days,
            "tier": classify_tier(days, settings),
            "note": (
                "Cumulative trading days across the rolling contract sequence, "
                "not per individual weekly/monthly contract."
            ),
        }
    return out
