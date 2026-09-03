"""Stage 8 — mint trade_id and link decision_log ↔ trade_fills ↔ outcome_log."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from nse_pipeline.signals.maturity import maturity_public_view
from nse_pipeline.storage.sqlite_store import SQLiteStore


def mint_trade_id() -> str:
    return str(uuid.uuid4())


def log_decision(
    store: SQLiteStore,
    *,
    symbol: str,
    actor: str,
    maturity_view: dict[str, Any],
    timestamp: str | None = None,
    trade_date: str | None = None,
    track: str | None = None,
    side: str | None = None,
    suggested_qty: float | None = None,
    suggested_price: float | None = None,
    signal_log_id: int | None = None,
    strike_rationale: str | None = None,
    sizing_rationale: str | None = None,
    timing_rationale: str | None = None,
    iv_rank: float | None = None,
    risk: dict[str, Any] | None = None,
    extras: dict[str, Any] | None = None,
    trade_id: str | None = None,
) -> str:
    """
    Persist a decision. System-suggested rows are refused unless the frozen
    public view has probability_permitted (gate cleared). Manual rows always
    record, including the N/60 view at that moment.
    """
    if actor not in {"system", "manual", "overridden"}:
        raise ValueError(f"invalid actor {actor!r}")
    if actor == "system" and not maturity_view.get("probability_permitted"):
        raise PermissionError(
            "system decisions are refused while the maturity gate is not cleared: "
            + str(maturity_view.get("display"))
        )
    tid = trade_id or mint_trade_id()
    ts = timestamp or datetime.now(timezone.utc).isoformat()
    store.insert_decision(
        {
            "trade_id": tid,
            "timestamp": ts,
            "trade_date": trade_date,
            "symbol": symbol,
            "track": track,
            "side": side,
            "suggested_qty": suggested_qty,
            "suggested_price": suggested_price,
            "actor": actor,
            "signal_log_id": signal_log_id,
            "strike_rationale": strike_rationale,
            "sizing_rationale": sizing_rationale,
            "timing_rationale": timing_rationale,
            "iv_rank": iv_rank,
            "risk": risk or {},
            "maturity_view": maturity_view,
            "extras": extras or {},
        }
    )
    return tid


def _as_side(value: str | None) -> str | None:
    if not value:
        return None
    v = str(value).upper()
    if v in {"BUY", "LONG", "B"}:
        return "BUY"
    if v in {"SELL", "SHORT", "S"}:
        return "SELL"
    return v


def fill_vs_decision(fill: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    rec_px = decision.get("suggested_price")
    rec_qty = decision.get("suggested_qty")
    fill_px = fill.get("price")
    fill_qty = fill.get("quantity")
    rec_side = _as_side(decision.get("side"))
    fill_side = _as_side(fill.get("transaction_type"))
    return {
        "price_diff": (float(fill_px) - float(rec_px))
        if rec_px is not None and fill_px is not None
        else None,
        "qty_diff": (float(fill_qty) - float(rec_qty))
        if rec_qty is not None and fill_qty is not None
        else None,
        "side_match": rec_side == fill_side if rec_side and fill_side else None,
        "recommended_price": rec_px,
        "fill_price": fill_px,
        "recommended_qty": rec_qty,
        "fill_qty": fill_qty,
    }


def link_fill(
    store: SQLiteStore,
    *,
    fill_pk: int,
    trade_id: str,
) -> dict[str, Any] | None:
    decision = store.fetch_decision(trade_id)
    if decision is None:
        store.link_fill_trade_id(fill_pk=fill_pk, trade_id=trade_id)
        return None
    fills = [f for f in store.fetch_fills(limit=500) if f["id"] == fill_pk]
    fill = fills[0] if fills else {"id": fill_pk}
    divergence = fill_vs_decision(fill, decision)
    store.link_fill_trade_id(
        fill_pk=fill_pk, trade_id=trade_id, fill_vs_decision=divergence
    )
    return divergence


def match_unlinked_fills(
    store: SQLiteStore,
    *,
    window_seconds: float = 300.0,
) -> int:
    """Fallback: same symbol + side within a time window when trade_id was not set."""
    decisions = store.fetch_decisions(limit=500)
    fills = store.fetch_fills(unlinked_only=True, limit=500)
    linked = 0
    for fill in fills:
        fill_side = _as_side(fill.get("transaction_type"))
        fill_ts = fill.get("fill_timestamp") or fill.get("captured_at")
        best = None
        for dec in decisions:
            if str(dec.get("symbol")) != str(fill.get("symbol")):
                continue
            if fill_side and _as_side(dec.get("side")) not in {None, fill_side}:
                continue
            dec_ts = dec.get("timestamp")
            if fill_ts and dec_ts:
                from pandas import Timestamp

                dt = abs(
                    (Timestamp(fill_ts) - Timestamp(dec_ts)).total_seconds()
                )
                if dt > window_seconds:
                    continue
                if best is None or dt < best[0]:
                    best = (dt, dec)
            elif best is None:
                best = (window_seconds, dec)
        if best:
            link_fill(store, fill_pk=int(fill["id"]), trade_id=str(best[1]["trade_id"]))
            linked += 1
    return linked


def require_public_view(view: dict[str, Any]) -> dict[str, Any]:
    """Pass-through check so callers cannot invent a display string."""
    if "display" not in view or "probability_permitted" not in view:
        raise ValueError("maturity_view must come from maturity_public_view")
    return view


def view_for_settings(settings: Any, **kwargs: Any) -> dict[str, Any]:
    days = int(kwargs.pop("days"))
    return maturity_public_view(days, settings, **kwargs)
