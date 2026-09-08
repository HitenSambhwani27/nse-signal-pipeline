"""Option-chain analytics. Derived from latest quotes + instrument cache. No ML."""

from __future__ import annotations

from typing import Any, Iterable

from nse_pipeline.market.implied_vol import implied_volatility
from nse_pipeline.market.reference import resolve_reference
from nse_pipeline.market.universe import atm_strike, infer_strike_interval, moneyness, strike_window


def chain_coverage(
    *,
    eligible_contract_count: int,
    selected_contract_count: int,
    truncated: bool = False,
) -> dict[str, Any]:
    """
    Universe coverage, not quote-fill.

    complete  — selected coverage matches eligible coverage
    partial   — some eligible contracts are missing, not because of a cap
    truncated — resource caps stopped selection
    empty     — no eligible contracts
    """
    eligible = max(0, int(eligible_contract_count))
    selected = max(0, int(selected_contract_count))
    missing = max(0, eligible - selected)
    if eligible <= 0:
        status = "empty"
    elif truncated:
        status = "truncated"
    elif selected < eligible:
        status = "partial"
    else:
        status = "complete"
    completeness = (selected / eligible) if eligible else 0.0
    return {
        "eligible_contract_count": eligible,
        "selected_contract_count": selected,
        "missing_contract_count": missing,
        "chain_status": status,
        "truncated": status == "truncated",
        "partial": status in {"partial", "truncated"},
        "complete": status == "complete",
        "chain_completeness": completeness,
    }


def quote_fill(
    *,
    selected_contract_count: int,
    quoted_contract_count: int,
) -> dict[str, Any]:
    """Live quote coverage for selected contracts. Distinct from chain_status."""
    selected = max(0, int(selected_contract_count))
    quoted = max(0, int(quoted_contract_count))
    if selected <= 0 or quoted <= 0:
        status = "empty"
    elif quoted < selected:
        status = "partial"
    else:
        status = "complete"
    return {
        "quoted_contract_count": quoted,
        "quote_coverage": (quoted / selected) if selected else 0.0,
        "quote_status": status,
    }


def _quote_is_live(quote: dict[str, Any] | None) -> bool:
    if not quote:
        return False
    return _f(quote.get("last_price")) is not None or _f(quote.get("oi")) is not None


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _ratio(numer: float | None, denom: float | None) -> float | None:
    if numer is None or denom is None or denom == 0:
        return None
    return float(numer) / float(denom)


def pcr(put_total: float | None, call_total: float | None) -> float | None:
    return _ratio(put_total, call_total)


def max_pain_strike(
    strikes: Iterable[dict[str, Any]],
    *,
    min_strikes: int,
    min_completeness: float,
) -> dict[str, Any]:
    """
    Pain at K = sum_i CE_OI_i * max(Si-K, 0) + PE_OI_i * max(K-Si, 0).
    Returns null when the chain is too small or incomplete.
    """
    rows = [s for s in strikes if s.get("strike") is not None]
    listed = sorted({float(s["strike"]) for s in rows})
    present = {float(s["strike"]) for s in rows if (s.get("ce") or {}).get("oi") is not None or (s.get("pe") or {}).get("oi") is not None}
    completeness = (len(present) / len(listed)) if listed else 0.0
    payload = {
        "max_pain_strike": None,
        "expiry": None,
        "number_of_strikes": len(listed),
        "chain_completeness": completeness,
        "status": "insufficient_chain",
    }
    if len(listed) < int(min_strikes) or completeness < float(min_completeness):
        return payload
    best_k = None
    best_pain = None
    for k in listed:
        pain = 0.0
        for row in rows:
            si = float(row["strike"])
            ce_oi = _f((row.get("ce") or {}).get("oi")) or 0.0
            pe_oi = _f((row.get("pe") or {}).get("oi")) or 0.0
            pain += ce_oi * max(si - k, 0.0) + pe_oi * max(k - si, 0.0)
        if best_pain is None or pain < best_pain:
            best_pain = pain
            best_k = k
    payload.update(
        {
            "max_pain_strike": best_k,
            "status": "ok",
            "kind": "derived",
        }
    )
    return payload


def _side_from_quote(
    quote: dict[str, Any] | None,
    meta: dict[str, Any],
    *,
    spot: float | None,
    rate: float,
    as_of: Any = None,
    close_hhmm: str = "15:30",
    underlying: str | None = None,
) -> dict[str, Any]:
    q = quote or {}
    oi = _f(q.get("oi"))
    prev_oi = None
    oi_change = _f(q.get("oi_delta") if "oi_delta" in q else q.get("oi_change"))
    if oi is not None and oi_change is not None:
        prev_oi = oi - oi_change
    ltp = _f(q.get("last_price"))
    iv_row = implied_volatility(
        option_price=ltp,
        spot=spot,
        strike=meta.get("strike"),
        expiry=meta.get("expiry"),
        option_type=str(meta.get("instrument_type") or ""),
        rate=rate,
        as_of=as_of or q.get("timestamp"),
        close_hhmm=close_hhmm,
        underlying=str(meta.get("name") or underlying or ""),
    )
    return {
        "symbol": q.get("symbol") or meta.get("tradingsymbol"),
        "ltp": ltp,
        "price_change": _f(q.get("price_delta") if "price_delta" in q else q.get("change")),
        "price_change_pct": _f(q.get("change_pct")),
        "volume": q.get("volume"),
        "volume_delta": q.get("volume_delta"),
        "oi": q.get("oi"),
        "oi_change": oi_change,
        "oi_change_pct": _ratio(oi_change, prev_oi),
        "last_quantity": q.get("last_quantity"),
        "best_bid": q.get("best_bid_price", q.get("best_bid")),
        "best_ask": q.get("best_ask_price", q.get("best_ask")),
        "spread": q.get("spread"),
        "mid_price": q.get("mid_price"),
        "bid_depth_5": q.get("bid_depth_5"),
        "ask_depth_5": q.get("ask_depth_5"),
        "depth_imbalance": q.get("depth_imbalance"),
        "lot_size": meta.get("lot_size"),
        "tick_size": meta.get("tick_size"),
        "iv": iv_row["iv"],
        "iv_pct": iv_row["iv_pct"],
        "iv_source": iv_row["iv_source"],
        "iv_kind": iv_row["iv_kind"],
        "iv_status": iv_row["iv_status"],
        "iv_reason": iv_row["iv_reason"],
        "iv_model": iv_row.get("iv_model"),
        "iv_rate": iv_row.get("iv_rate"),
        "iv_dividend_yield": iv_row.get("iv_dividend_yield"),
        "iv_exercise_style": iv_row.get("iv_exercise_style"),
        **resolve_reference(q, instrument_type=meta.get("instrument_type")),
    }


def build_option_chain(
    *,
    underlying: str,
    expiry: str,
    contracts: list[dict[str, Any]],
    quotes_by_symbol: dict[str, dict[str, Any]],
    spot: float | None,
    interval: float | None,
    atm_method: str,
    pcr_window: int,
    max_pain_min_strikes: int,
    max_pain_min_completeness: float,
    eligible_contract_count: int | None = None,
    selected_contract_count: int | None = None,
    truncated: bool = False,
    rate: float = 0.06,
    as_of: Any = None,
    close_hhmm: str = "15:30",
) -> dict[str, Any]:
    by_strike: dict[float, dict[str, Any]] = {}
    for meta in contracts:
        strike = _f(meta.get("strike"))
        if strike is None:
            continue
        row = by_strike.setdefault(
            strike,
            {"strike": strike, "ce": None, "pe": None, "ce_meta": None, "pe_meta": None},
        )
        kind = str(meta.get("instrument_type") or "").upper()
        quote = quotes_by_symbol.get(str(meta.get("tradingsymbol")))
        side = _side_from_quote(
            quote,
            meta,
            spot=spot,
            rate=rate,
            as_of=as_of,
            close_hhmm=close_hhmm,
            underlying=underlying,
        )
        if kind == "CE":
            row["ce"] = side
            row["ce_meta"] = meta
        elif kind == "PE":
            row["pe"] = side
            row["pe_meta"] = meta

    listed = sorted(by_strike)
    interval = interval or infer_strike_interval(listed)
    atm = atm_strike(spot, listed, interval=interval, method=atm_method)
    strikes_out: list[dict[str, Any]] = []
    ce_oi = pe_oi = ce_vol = pe_vol = 0.0
    have_ce_oi = have_pe_oi = have_ce_vol = have_pe_vol = False
    for strike in listed:
        row = by_strike[strike]
        ce = row["ce"] or {}
        pe = row["pe"] or {}
        ce_m = moneyness(option_type="CE", strike=strike, spot=spot, atm=atm)
        pe_m = moneyness(option_type="PE", strike=strike, spot=spot, atm=atm)
        distance = None
        if atm is not None and interval:
            distance = round((strike - atm) / interval)
        elif atm is not None:
            distance = strike - atm
        item = {
            "strike": strike,
            "distance_from_atm": distance,
            "ce": {**ce, "moneyness": ce_m} if row["ce"] else None,
            "pe": {**pe, "moneyness": pe_m} if row["pe"] else None,
        }
        strikes_out.append(item)
        if _f(ce.get("oi")) is not None:
            ce_oi += float(ce["oi"])
            have_ce_oi = True
        if _f(pe.get("oi")) is not None:
            pe_oi += float(pe["oi"])
            have_pe_oi = True
        if _f(ce.get("volume")) is not None:
            ce_vol += float(ce["volume"])
            have_ce_vol = True
        if _f(pe.get("volume")) is not None:
            pe_vol += float(pe["volume"])
            have_pe_vol = True

    window = strike_window(listed, atm, pcr_window)
    near_ce = sum(float((by_strike[s]["ce"] or {}).get("oi") or 0) for s in window)
    near_pe = sum(float((by_strike[s]["pe"] or {}).get("oi") or 0) for s in window)
    selected = (
        int(selected_contract_count)
        if selected_contract_count is not None
        else len(contracts)
    )
    eligible = (
        int(eligible_contract_count)
        if eligible_contract_count is not None
        else selected
    )
    quoted = sum(
        1
        for meta in contracts
        if _quote_is_live(quotes_by_symbol.get(str(meta.get("tradingsymbol"))))
    )
    coverage = chain_coverage(
        eligible_contract_count=eligible,
        selected_contract_count=selected,
        truncated=truncated,
    )
    fill = quote_fill(selected_contract_count=selected, quoted_contract_count=quoted)
    observation_completeness = fill["quote_coverage"]

    def _ext(side: str, field: str, reverse: bool) -> dict[str, Any] | None:
        scored = []
        for item in strikes_out:
            block = item.get(side) or {}
            val = _f(block.get(field))
            if val is None:
                continue
            scored.append((val, item["strike"], block.get("symbol")))
        if not scored:
            return None
        scored.sort(key=lambda t: t[0], reverse=reverse)
        val, strike, symbol = scored[0]
        return {"strike": strike, "symbol": symbol, field: val}

    multi = []
    for strike in strike_window(listed, atm, pcr_window):
        row = by_strike[strike]
        ce = row["ce"] or {}
        pe = row["pe"] or {}
        multi.append(
            {
                "strike": strike,
                "ce_oi": ce.get("oi"),
                "pe_oi": pe.get("oi"),
                "ce_oi_change": ce.get("oi_change"),
                "pe_oi_change": pe.get("oi_change"),
                "ce_volume": ce.get("volume"),
                "pe_volume": pe.get("volume"),
                "ce_last_quantity": ce.get("last_quantity"),
                "pe_last_quantity": pe.get("last_quantity"),
                "ce_depth_imbalance": ce.get("depth_imbalance"),
                "pe_depth_imbalance": pe.get("depth_imbalance"),
            }
        )

    pain = max_pain_strike(
        [{"strike": s["strike"], "ce": s["ce"] or {}, "pe": s["pe"] or {}} for s in strikes_out],
        min_strikes=max_pain_min_strikes,
        min_completeness=max_pain_min_completeness,
    )
    pain["expiry"] = expiry

    return {
        "underlying": underlying,
        "expiry": expiry,
        "spot": spot,
        "atm": atm,
        "atm_method": atm_method,
        "strike_interval": interval,
        "chain_completeness": coverage["chain_completeness"],
        "complete": coverage["complete"],
        "chain_status": coverage["chain_status"],
        "eligible_contract_count": coverage["eligible_contract_count"],
        "selected_contract_count": coverage["selected_contract_count"],
        "missing_contract_count": coverage["missing_contract_count"],
        "quoted_contract_count": fill["quoted_contract_count"],
        "quote_coverage": fill["quote_coverage"],
        "quote_status": fill["quote_status"],
        "truncated": coverage["truncated"],
        "partial": coverage["partial"],
        "observation_count": fill["quoted_contract_count"],
        "observation_completeness": observation_completeness,
        "number_of_strikes": len(listed),
        "pcr_oi": pcr(pe_oi if have_pe_oi else None, ce_oi if have_ce_oi else None),
        "pcr_volume": pcr(pe_vol if have_pe_vol else None, ce_vol if have_ce_vol else None),
        "pcr_near_atm_oi": pcr(near_pe, near_ce),
        "pcr_window_strikes": pcr_window,
        "total_ce_oi": ce_oi if have_ce_oi else None,
        "total_pe_oi": pe_oi if have_pe_oi else None,
        "highest_ce_oi": _ext("ce", "oi", True),
        "highest_pe_oi": _ext("pe", "oi", True),
        "largest_ce_oi_increase": _ext("ce", "oi_change", True),
        "largest_pe_oi_increase": _ext("pe", "oi_change", True),
        "largest_ce_oi_decrease": _ext("ce", "oi_change", False),
        "largest_pe_oi_decrease": _ext("pe", "oi_change", False),
        "max_pain": pain,
        "multi_strike": multi,
        "strikes": strikes_out,
        "kind": "derived",
    }
