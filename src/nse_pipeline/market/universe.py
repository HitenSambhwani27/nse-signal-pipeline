"""ATM, strike windows, F&O eligibility — no Kite calls."""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence


INDEX_SPOT_BY_NAME = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
}


def spot_symbol_for_underlying(name: str) -> str:
    return INDEX_SPOT_BY_NAME.get(name.upper(), name.upper())


def infer_strike_interval(strikes: Sequence[float]) -> float | None:
    ordered = sorted({float(s) for s in strikes})
    if len(ordered) < 2:
        return None
    gaps = [b - a for a, b in zip(ordered, ordered[1:]) if b > a]
    return min(gaps) if gaps else None


def atm_strike(
    spot: float | None,
    listed: Sequence[float],
    *,
    interval: float | None = None,
    method: str = "nearest_listed",
) -> float | None:
    """
    ATM is the listed strike nearest to spot.

    If interval is known, first round spot to that interval, then snap to a
    listed strike. This is nearest_listed, not a hardcoded ATM value.
    """
    if spot is None or not listed:
        return None
    ordered = sorted({float(s) for s in listed})
    step = interval if interval and interval > 0 else infer_strike_interval(ordered)
    target = float(spot)
    if method == "nearest_listed" and step:
        target = round(float(spot) / step) * step
    return min(ordered, key=lambda s: (abs(s - target), abs(s - float(spot))))


def moneyness(
    *,
    option_type: str,
    strike: float | None,
    spot: float | None,
    atm: float | None,
) -> str | None:
    if strike is None or spot is None:
        return None
    kind = (option_type or "").upper()
    if atm is not None and abs(strike - atm) < 1e-9:
        return "ATM"
    if kind == "CE":
        if strike < spot:
            return "ITM"
        if strike > spot:
            return "OTM"
        return "ATM"
    if kind == "PE":
        if strike > spot:
            return "ITM"
        if strike < spot:
            return "OTM"
        return "ATM"
    return None


def strike_window(listed: Sequence[float], atm: float | None, each_side: int) -> list[float]:
    ordered = sorted({float(s) for s in listed})
    if not ordered or atm is None:
        return ordered
    if atm not in ordered:
        atm = min(ordered, key=lambda s: abs(s - atm))
    idx = ordered.index(atm)
    start = max(0, idx - int(each_side))
    end = min(len(ordered), idx + int(each_side) + 1)
    return ordered[start:end]


def fo_names_from_nfo_rows(nfo_rows: list[dict[str, Any]]) -> set[str]:
    names: set[str] = set()
    for row in nfo_rows:
        kind = str(row.get("instrument_type") or "").upper()
        if kind not in {"FUT", "CE", "PE"}:
            continue
        name = str(row.get("name") or "").upper()
        if name:
            names.add(name)
    return names


def nifty100_fo_eligible(nifty100: Sequence[str], nfo_names: set[str]) -> list[str]:
    """NIFTY100 constituents that currently have listed F&O. Not every NIFTY100 name."""
    eligible = []
    for symbol in nifty100:
        if str(symbol).upper() in nfo_names:
            eligible.append(str(symbol))
    return eligible


def current_and_next_expiries(expiries: Sequence[date], today: date, count: int) -> list[date]:
    future = sorted({d for d in expiries if d >= today})
    return future[: max(0, int(count))]


def option_type_rank(kind: str) -> int:
    token = (kind or "").upper()
    if token == "CE":
        return 0
    if token == "PE":
        return 1
    return 2


def stock_option_subscribe_mode(
    distance_from_atm: int,
    *,
    atm_mode: str = "full",
    wing_mode: str = "full",
) -> str:
    """ATM CE/PE are always FULL so OI/depth exist. Extra wing strikes follow wing_mode."""
    if int(distance_from_atm) == 0:
        return "full"
    return (wing_mode or "full").lower()


class StockOptionSlot:
    """One listed stock-option contract inside the configured ATM window."""

    __slots__ = (
        "underlying",
        "membership_rank",
        "expiry",
        "strike",
        "option_type",
        "distance_from_atm",
        "phase",
        "row",
    )

    def __init__(
        self,
        *,
        underlying: str,
        membership_rank: int,
        expiry: date,
        strike: float,
        option_type: str,
        distance_from_atm: int,
        phase: int,
        row: dict[str, Any],
    ) -> None:
        self.underlying = underlying
        self.membership_rank = membership_rank
        self.expiry = expiry
        self.strike = strike
        self.option_type = option_type
        self.distance_from_atm = distance_from_atm
        self.phase = phase
        self.row = row


def collect_stock_option_slots(
    eligible: Sequence[str],
    rows_by_underlying: dict[str, list[dict[str, Any]]],
    spots: dict[str, float],
    *,
    today: date,
    expiry_count: int,
    strikes_each_side: int,
) -> tuple[list[StockOptionSlot], list[str], list[str]]:
    """
    Build the intended ATM-window universe in membership order.

    Returns (slots, eligible_underlyings_with_listed_options, uncovered_no_atm).
    """
    slots: list[StockOptionSlot] = []
    listed_names: list[str] = []
    uncovered: list[str] = []
    half = int(strikes_each_side)
    for rank, symbol in enumerate(eligible):
        name = str(symbol).upper()
        rows = rows_by_underlying.get(name) or []
        if not rows:
            continue
        listed_names.append(name)
        spot = spots.get(symbol)
        if spot is None:
            spot = spots.get(name)
        expiries = current_and_next_expiries(
            [r["_expiry_date"] for r in rows if r.get("_expiry_date") is not None],
            today,
            expiry_count,
        )
        if not expiries:
            uncovered.append(name)
            continue
        got_atm = False
        for target in expiries:
            expiry_rows = [r for r in rows if r.get("_expiry_date") == target]
            strikes = sorted(
                {
                    float(r["strike"])
                    for r in expiry_rows
                    if r.get("strike") is not None
                }
            )
            if not strikes:
                continue
            atm = atm_strike(spot, strikes) if spot is not None else None
            if atm is None:
                continue
            got_atm = True
            idx = strikes.index(atm) if atm in strikes else min(
                range(len(strikes)), key=lambda i: abs(strikes[i] - atm)
            )
            window = set(strikes[max(0, idx - half) : min(len(strikes), idx + half + 1)])
            for row in expiry_rows:
                if row.get("strike") is None:
                    continue
                strike = float(row["strike"])
                if strike not in window:
                    continue
                try:
                    distance = abs(strikes.index(strike) - idx)
                except ValueError:
                    distance = abs(strike - atm)
                kind = str(row.get("instrument_type") or "").upper()
                slots.append(
                    StockOptionSlot(
                        underlying=name,
                        membership_rank=rank,
                        expiry=target,
                        strike=strike,
                        option_type=kind,
                        distance_from_atm=int(distance),
                        phase=1 if distance == 0 else 2,
                        row=row,
                    )
                )
        if not got_atm:
            uncovered.append(name)
    return slots, listed_names, uncovered


def allocate_stock_option_slots(
    slots: Sequence[StockOptionSlot],
    *,
    max_contracts: int,
) -> tuple[list[StockOptionSlot], bool]:
    """
    Breadth-first deterministic allocation.

    Phase 1: ATM CE + ATM PE for every eligible name/expiry.
    Phase 2: remaining window strikes, nearer strikes first, then membership order.
    """
    cap = max(0, int(max_contracts))
    phase1 = sorted(
        (s for s in slots if s.phase == 1),
        key=lambda s: (
            s.membership_rank,
            s.underlying,
            s.expiry,
            option_type_rank(s.option_type),
            s.strike,
        ),
    )
    phase2 = sorted(
        (s for s in slots if s.phase != 1),
        key=lambda s: (
            s.distance_from_atm,
            s.membership_rank,
            s.underlying,
            s.expiry,
            option_type_rank(s.option_type),
            s.strike,
        ),
    )
    ordered = phase1 + phase2
    selected = list(ordered[:cap])
    truncated = len(ordered) > cap
    return selected, truncated


def summarize_stock_option_allocation(
    *,
    all_slots: Sequence[StockOptionSlot],
    selected: Sequence[StockOptionSlot],
    listed_underlyings: Sequence[str],
    uncovered_no_atm: Sequence[str],
    truncated: bool,
    atm_mode: str = "full",
    wing_mode: str = "full",
) -> dict[str, Any]:
    selected_by_name: dict[str, list[StockOptionSlot]] = {}
    for slot in selected:
        selected_by_name.setdefault(slot.underlying, []).append(slot)
    intended_by_name: dict[str, list[StockOptionSlot]] = {}
    for slot in all_slots:
        intended_by_name.setdefault(slot.underlying, []).append(slot)

    by_underlying: dict[str, Any] = {}
    names = []
    seen: set[str] = set()
    for name in list(listed_underlyings) + [s.underlying for s in all_slots]:
        if name in seen:
            continue
        seen.add(name)
        names.append(name)

    full_count = quote_count = 0
    atm_covered: list[str] = []
    for name in names:
        intended = intended_by_name.get(name) or []
        kept = selected_by_name.get(name) or []
        by_expiry: dict[str, dict[str, Any]] = {}
        expiries = sorted({s.expiry for s in intended} | {s.expiry for s in kept})
        for expiry in expiries:
            exp_key = expiry.isoformat()
            exp_intended = [s for s in intended if s.expiry == expiry]
            exp_kept = [s for s in kept if s.expiry == expiry]
            exp_truncated = truncated and len(exp_kept) < len(exp_intended)
            by_expiry[exp_key] = {
                "eligible_contract_count": len(exp_intended),
                "selected_contract_count": len(exp_kept),
                "truncated": exp_truncated,
            }
        atm_ce = any(s.phase == 1 and s.option_type == "CE" for s in kept)
        atm_pe = any(s.phase == 1 and s.option_type == "PE" for s in kept)
        if atm_ce and atm_pe:
            atm_covered.append(name)
        modes = [
            stock_option_subscribe_mode(
                s.distance_from_atm, atm_mode=atm_mode, wing_mode=wing_mode
            )
            for s in kept
        ]
        full_count += sum(1 for m in modes if m != "quote")
        quote_count += sum(1 for m in modes if m == "quote")
        by_underlying[name] = {
            "eligible_contract_count": len(intended),
            "selected_contract_count": len(kept),
            "truncated": truncated and len(kept) < len(intended),
            "atm_ce_selected": atm_ce,
            "atm_pe_selected": atm_pe,
            "atm_covered": atm_ce and atm_pe,
            "by_expiry": by_expiry,
        }

    return {
        "truncated": bool(truncated),
        "stock_options_eligible_count": len(listed_underlyings),
        "stock_options_full_count": full_count,
        "stock_options_quote_count": quote_count,
        "eligible_contract_count": len(all_slots),
        "selected_contract_count": len(selected),
        "atm_covered_underlyings": atm_covered,
        "uncovered_no_atm": list(uncovered_no_atm),
        "by_underlying": by_underlying,
    }
