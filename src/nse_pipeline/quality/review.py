"""Stage 9 — entry/exit decision quality, independent of raw P&L.

Anti-hindsight: entry quality uses only information known at entry time
(the frozen model probability and the frozen maturity view). Checkpoint
prices after the fact are used solely for exit timing and outcome, never
to relabel whether the entry itself was justified.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pandas import Timestamp

from nse_pipeline.storage.sqlite_store import SQLiteStore

CHECKPOINT_MINUTES = (5, 15, 30)
CLASS_INSUFFICIENT = "not_scored_insufficient_data"
CLASS_GG = "good_decision_good_outcome"
CLASS_GB = "good_decision_bad_outcome"
CLASS_BG = "bad_decision_good_outcome"
CLASS_BB = "bad_decision_bad_outcome"

IST = ZoneInfo("Asia/Kolkata")


def classify_2x2(*, decision_good: bool | None, outcome_good: bool | None) -> str:
    if decision_good is None or outcome_good is None:
        return CLASS_INSUFFICIENT
    if decision_good and outcome_good:
        return CLASS_GG
    if decision_good and not outcome_good:
        return CLASS_GB
    if (not decision_good) and outcome_good:
        return CLASS_BG
    return CLASS_BB


def entry_quality(
    decision: dict[str, Any],
    *,
    model_probability_at_entry: float | None,
    abs_p_tol: float = 0.15,
) -> dict[str, Any]:
    """
    Was the decision consistent with the model's belief at that moment?

    If the frozen maturity view forbids a probability, return insufficient
    data — never a fabricated entry-quality number.
    """
    view = decision.get("maturity_view") or {}
    if not view.get("probability_permitted"):
        return {
            "decision_good": None,
            "entry_quality": None,
            "display": view.get("display") or "insufficient data",
            "reason": "maturity_gate_not_cleared",
        }
    extras = decision.get("extras") or {}
    stated = extras.get("stated_probability")
    calibrated = model_probability_at_entry
    if calibrated is None:
        calibrated = extras.get("model_probability_at_entry")
    if stated is None or calibrated is None:
        return {
            "decision_good": None,
            "entry_quality": None,
            "display": view.get("display"),
            "reason": "missing_probability_at_entry",
        }
    gap = abs(float(stated) - float(calibrated))
    side_ok = True
    rec_side = str(decision.get("side") or extras.get("model_side") or "").upper()
    model_side = str(extras.get("model_side") or rec_side).upper()
    if rec_side and model_side:
        side_ok = rec_side == model_side
    actor = decision.get("actor")
    if actor == "system":
        decision_good = gap <= abs_p_tol and side_ok
    else:
        # Manual / overridden: good only if they still agreed with the model.
        decision_good = side_ok and gap <= abs_p_tol
    return {
        "decision_good": decision_good,
        "entry_quality": 1.0 - min(gap, 1.0),
        "probability_gap": gap,
        "side_ok": side_ok,
        "reason": None,
    }


def _session_eod(ts: Any) -> datetime:
    t = Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    ist = t.tz_convert(IST).to_pydatetime()
    eod = datetime.combine(ist.date(), time(15, 30), tzinfo=IST)
    return eod


def build_checkpoints(
    *,
    origin_ts: Any,
    price_at: callable,
    include_eod: bool = True,
) -> dict[str, dict[str, Any]]:
    """price_at(timestamp) -> {price, iv, oi} using only that timestamp's market state."""
    origin = Timestamp(origin_ts)
    if origin.tzinfo is None:
        origin = origin.tz_localize("UTC")
    out: dict[str, dict[str, Any]] = {}
    for minutes in CHECKPOINT_MINUTES:
        ts = origin + timedelta(minutes=minutes)
        out[f"+{minutes}m"] = {"timestamp": ts.isoformat(), **(price_at(ts) or {})}
    if include_eod:
        eod = Timestamp(_session_eod(origin))
        out["eod"] = {"timestamp": eod.isoformat(), **(price_at(eod) or {})}
    return out


def exit_quality(
    *,
    side: str | None,
    exit_price: float | None,
    post_exit: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Continuation after exit → exited early; reversal → well timed."""
    if exit_price is None or not post_exit:
        return {"exit_quality": None, "exited_early": None, "reason": "missing_exit_path"}
    side_u = str(side or "").upper()
    long = side_u in {"BUY", "LONG", "B"}
    marks = []
    for key in (f"+{m}m" for m in CHECKPOINT_MINUTES):
        px = (post_exit.get(key) or {}).get("price")
        if px is not None:
            marks.append(float(px))
    if not marks:
        eod_px = (post_exit.get("eod") or {}).get("price")
        if eod_px is not None:
            marks.append(float(eod_px))
    if not marks:
        return {"exit_quality": None, "exited_early": None, "reason": "no_checkpoint_prices"}
    last = marks[-1]
    if long:
        continued = last > float(exit_price)
    else:
        continued = last < float(exit_price)
    # exited_early if the move kept going in the trade's favor
    exited_early = continued
    score = 0.0 if exited_early else 1.0
    return {
        "exit_quality": score,
        "exited_early": exited_early,
        "reason": "continuation_after_exit" if exited_early else "reversal_or_flat_after_exit",
    }


def pair_entry_exit_fills(fills: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not fills:
        return None, None
    ordered = sorted(fills, key=lambda f: str(f.get("fill_timestamp") or f.get("id")))
    if len(ordered) == 1:
        return ordered[0], None
    return ordered[0], ordered[-1]


def score_closed_trade(
    store: SQLiteStore,
    *,
    trade_id: str,
    price_at: callable,
    model_probability_at_entry: float | None = None,
) -> dict[str, Any]:
    decision = store.fetch_decision(trade_id)
    if decision is None:
        raise KeyError(f"no decision for trade_id={trade_id}")
    fills = store.fetch_fills_for_trade(trade_id)
    entry, exit_fill = pair_entry_exit_fills(fills)
    entry_q = entry_quality(
        decision, model_probability_at_entry=model_probability_at_entry
    )
    entry_px = float(entry["price"]) if entry and entry.get("price") is not None else None
    exit_px = (
        float(exit_fill["price"]) if exit_fill and exit_fill.get("price") is not None else None
    )
    qty = float(entry["quantity"]) if entry and entry.get("quantity") else 1.0
    side = decision.get("side") or (entry or {}).get("transaction_type")
    pnl = None
    if entry_px is not None and exit_px is not None:
        if str(side or "").upper() in {"BUY", "LONG", "B"}:
            pnl = (exit_px - entry_px) * qty
        else:
            pnl = (entry_px - exit_px) * qty
    outcome_good = None if pnl is None else bool(pnl > 0)

    checkpoints: dict[str, Any] = {}
    if entry:
        checkpoints["after_entry"] = build_checkpoints(
            origin_ts=entry.get("fill_timestamp") or decision.get("timestamp"),
            price_at=price_at,
        )
    post_exit: dict[str, dict[str, Any]] = {}
    if exit_fill:
        post_exit = build_checkpoints(
            origin_ts=exit_fill.get("fill_timestamp"),
            price_at=price_at,
        )
        checkpoints["after_exit"] = post_exit
    exit_q = exit_quality(side=side, exit_price=exit_px, post_exit=post_exit)

    classification = classify_2x2(
        decision_good=entry_q.get("decision_good"),
        outcome_good=outcome_good,
    )
    payload = {
        "trade_id": trade_id,
        "entry_fill_id": entry["id"] if entry else None,
        "exit_fill_id": exit_fill["id"] if exit_fill else None,
        "checkpoints": checkpoints,
        "entry_quality": entry_q.get("entry_quality"),
        "exit_quality": exit_q.get("exit_quality"),
        "outcome_good": None if outcome_good is None else (1 if outcome_good else 0),
        "decision_good": None
        if entry_q.get("decision_good") is None
        else (1 if entry_q["decision_good"] else 0),
        "classification": classification,
        "pnl": pnl,
        "details": {
            "entry": entry_q,
            "exit": exit_q,
            "actor": decision.get("actor"),
            "maturity_view": decision.get("maturity_view"),
        },
    }
    store.upsert_outcome(payload)
    return payload


def behavior_rollup(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    by_actor: dict[str, dict[str, int]] = {}
    for row in outcomes:
        actor = str((row.get("details") or {}).get("actor") or "unknown")
        bucket = by_actor.setdefault(
            actor, {"n": 0, "good_decision": 0, "insufficient": 0, "lucky": 0, "unlucky": 0}
        )
        bucket["n"] += 1
        klass = row.get("classification")
        if klass == CLASS_INSUFFICIENT:
            bucket["insufficient"] += 1
        if row.get("decision_good") == 1:
            bucket["good_decision"] += 1
        if klass == CLASS_BG:
            bucket["lucky"] += 1
        if klass == CLASS_GB:
            bucket["unlucky"] += 1
    return by_actor
