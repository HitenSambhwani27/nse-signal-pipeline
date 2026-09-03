"""Pooled live-day maturity gate (per class, not per contract).

The public view is the only formatter allowed to produce the N/60 display
string. API and dashboard must pass this payload through unchanged.
"""

from __future__ import annotations

from typing import Any, Literal

from nse_pipeline.config import Settings
from nse_pipeline.session_coverage import LIVE_PILOT_TRADE_DATES, _LIVE_PARTIAL_MAX_HOURS
from nse_pipeline.storage.sqlite_store import SQLiteStore

Tier = Literal["suppressed", "provisional", "full"]

CLASS_FROM_TRACK = {
    "equity_depth": "equity",
    "equity_quote": "equity",
    "index": "equity",
    "options": "options",
    "futures": "futures",
}

TRACKS_FOR_CLASS: dict[str, tuple[str, ...]] = {
    "equity": ("equity_depth", "equity_quote"),
    "options": ("options",),
    "futures": ("futures",),
}


def classify_tier(days: int, settings: Settings) -> Tier:
    if days < settings.maturity_gate.suppress_below_days:
        return "suppressed"
    if days < settings.maturity_gate.provisional_below_days:
        return "provisional"
    return "full"


def maturity_public_view(
    days: int,
    settings: Settings,
    *,
    class_key: str,
    underlying: str | None = None,
    has_model: bool = True,
    reason: str | None = None,
) -> dict[str, Any]:
    """Frozen payload: never fabricate a confident probability before 60 pooled days."""
    tier = classify_tier(days, settings)
    threshold = int(settings.maturity_gate.provisional_below_days)
    suppress_below = int(settings.maturity_gate.suppress_below_days)
    if tier != "full":
        display = f"insufficient data, {days}/{threshold} pooled days"
    else:
        display = f"full — {days}/{threshold} pooled days"
    if reason is None:
        if not has_model:
            reason = "no_harness_passed_model"
        elif tier == "suppressed":
            reason = "suppressed_insufficient_live_days"
        elif tier == "provisional":
            reason = "provisional_accumulating_live_days"
        else:
            reason = None
    probability_permitted = tier == "full" and has_model
    payload: dict[str, Any] = {
        "class_key": class_key,
        "underlying": underlying,
        "tier": tier,
        "pooled_live_days": int(days),
        "threshold_days": threshold,
        "suppress_below_days": suppress_below,
        "display": display,
        "probability_permitted": probability_permitted,
        "reason": reason,
    }
    return payload


def signal_public_view(
    row: dict[str, Any],
    maturity: dict[str, Any],
) -> dict[str, Any]:
    """Strip research scores from any payload the UI/API may show."""
    permitted = bool(maturity.get("probability_permitted"))
    return {
        "id": row.get("id"),
        "timestamp": row.get("timestamp"),
        "trade_date": row.get("trade_date"),
        "symbol": row.get("symbol"),
        "track": row.get("track"),
        "display": maturity.get("display") or row.get("display"),
        "probability": row.get("probability") if permitted else None,
        "score": row.get("score") if permitted else None,
        "tier": maturity.get("tier") or row.get("maturity_tier") or row.get("tier"),
        "reason": maturity.get("reason") or row.get("reason"),
        "probability_permitted": permitted,
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

    Live-pilot 2026-08-13 and equity live slices shorter than two hours are
    excluded (same dates baseline/harness skip).
    """
    store = SQLiteStore(settings.paths.sqlite_db)
    tracks = TRACKS_FOR_CLASS.get(class_key, ("equity_depth", "equity_quote"))
    extra_skip = set(LIVE_PILOT_TRADE_DATES)
    min_span = _LIVE_PARTIAL_MAX_HOURS if class_key == "equity" else None
    return store.count_pooled_live_days(
        tracks=tracks,
        skip_dates=extra_skip,
        underlying=underlying if class_key == "options" else None,
        min_span_hours=min_span,
        span_tracks=("equity_depth", "equity_quote") if class_key == "equity" else tracks,
    )


def maturity_snapshot(settings: Settings, *, has_model: dict[str, bool] | None = None) -> dict[str, Any]:
    """Report pooled live days and the frozen public view per class."""
    models = has_model or {}
    out: dict[str, Any] = {}
    for class_key in ("equity", "futures"):
        days = pooled_live_days(settings, class_key=class_key)
        out[class_key] = maturity_public_view(
            days,
            settings,
            class_key=class_key,
            has_model=models.get(class_key, True),
        )
    for und in ("NIFTY", "BANKNIFTY"):
        days = pooled_live_days(settings, class_key="options", underlying=und)
        key = f"options_{und.lower()}"
        view = maturity_public_view(
            days,
            settings,
            class_key="options",
            underlying=und,
            has_model=models.get("options", True),
        )
        view["note"] = (
            "Cumulative trading days across the rolling contract sequence, "
            "not per individual weekly/monthly contract."
        )
        out[key] = view
    return out


class SqlMaturityGate:
    """Adapter: existing SQL counts + maturity_public_view. No invented scores."""

    def pooled_live_days(
        self,
        settings: Settings,
        *,
        class_key: str,
        underlying: str | None = None,
    ) -> int:
        return pooled_live_days(settings, class_key=class_key, underlying=underlying)

    def public_view(
        self,
        days: int,
        settings: Settings,
        *,
        class_key: str,
        underlying: str | None = None,
        has_model: bool = True,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return maturity_public_view(
            days,
            settings,
            class_key=class_key,
            underlying=underlying,
            has_model=has_model,
            reason=reason,
        )

    def snapshot(
        self, settings: Settings, *, has_model: dict[str, bool] | None = None
    ) -> dict[str, Any]:
        return maturity_snapshot(settings, has_model=has_model)
