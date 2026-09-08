"""Assemble analytics DTOs from SQLite latest state + instrument cache. No Kite."""

from __future__ import annotations

from typing import Any

from nse_pipeline.config import MarketAnalyticsSettings, Settings
from nse_pipeline.market.activity import market_activity_row, tod_bucket
from nse_pipeline.market.cache_health import lookup_instrument_meta
from nse_pipeline.market.charts import CHART_POINT_FIELDS, chart_payload, merge_chart_rows
from nse_pipeline.market.ohlc import load_historical_observations, load_ohlc_candles
from nse_pipeline.market.cross_market import describe_cross_market
from nse_pipeline.market.flow import (
    aggressive_side_proxy,
    displayed_depth_changes,
    liquidity_events,
    price_impact_note,
)
from nse_pipeline.market.futures_analytics import futures_snapshot
from nse_pipeline.market.options_analytics import build_option_chain
from nse_pipeline.market.quotes import enrich_quote
from nse_pipeline.market.read_policy import MarketDataPolicy, SymbolSnapshot
from nse_pipeline.market.data_state import resolve
from nse_pipeline.market.session_reference import maintain_session_references, stored_for_snapshot
from nse_pipeline.market.universe import INDEX_SPOT_BY_NAME, infer_strike_interval
from nse_pipeline.market.unusual import unusual_activity_score
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _policy(
    settings: Settings, store: SQLiteStore, policy: MarketDataPolicy | None
) -> MarketDataPolicy:
    """Reuse the read model's policy; build a transient one for direct calls."""
    return policy if policy is not None else MarketDataPolicy(settings, store)


def _rows(snapshots: dict[str, SymbolSnapshot]) -> dict[str, dict[str, Any]]:
    return {name: snap.row for name, snap in snapshots.items() if snap.row is not None}


def _blank(symbol: str, policy: MarketDataPolicy) -> SymbolSnapshot:
    return SymbolSnapshot(symbol, None, None, policy.no_data(reason="symbol_not_requested"))


def _chain_selection_counts(
    cache: dict[str, Any],
    underlying: str,
    expiry: str,
    *,
    selected_fallback: int,
) -> tuple[int, int, bool]:
    """Read eligible vs selected coverage from the instrument cache, if present."""
    universe = cache.get("universe") or {}
    selection = universe.get("option_selection") or {}
    by_u = (selection.get("by_underlying") or {}).get(underlying.upper()) or {}
    want_exp = (expiry or "")[:10]
    by_exp = (by_u.get("by_expiry") or {}).get(want_exp) or {}
    if by_exp:
        eligible = int(by_exp.get("eligible_contract_count", selected_fallback))
        selected = int(by_exp.get("selected_contract_count", selected_fallback))
        truncated = bool(by_exp.get("truncated", False))
        return eligible, selected, truncated
    return int(selected_fallback), int(selected_fallback), False


def _cfg(settings: Settings) -> MarketAnalyticsSettings:
    return settings.analytics


def _thresholds(settings: Settings) -> dict[str, float]:
    cfg = _cfg(settings)
    return {
        "large_trade_percentile": cfg.large_trade_percentile,
        "very_large_trade_percentile": cfg.very_large_trade_percentile,
        "extreme_trade_percentile": cfg.extreme_trade_percentile,
        "volume_elevated_ratio": cfg.volume_elevated_ratio,
        "volume_burst_ratio": cfg.volume_burst_ratio,
        "volume_extreme_ratio": cfg.volume_extreme_ratio,
        "activity_elevated_ratio": cfg.activity_elevated_ratio,
        "activity_burst_ratio": cfg.activity_burst_ratio,
        "tod_bucket_minutes": float(cfg.tod_bucket_minutes),
    }


def _option_contracts(cache: dict[str, Any], underlying: str, expiry: str | None) -> list[dict[str, Any]]:
    want = underlying.upper()
    want_exp = (expiry or "").strip()[:10]
    out = []
    for row in cache.get("options") or []:
        if str(row.get("name") or "").upper() != want:
            continue
        exp = str(row.get("expiry") or "")[:10]
        if want_exp and exp != want_exp:
            continue
        out.append(row)
    return out


def _expiries_for(cache: dict[str, Any], underlying: str) -> list[str]:
    found = sorted(
        {
            str(row.get("expiry"))[:10]
            for row in cache.get("options") or []
            if str(row.get("name") or "").upper() == underlying.upper() and row.get("expiry")
        }
    )
    return found


def _future_contracts(cache: dict[str, Any], underlying: str) -> list[dict[str, Any]]:
    want = underlying.upper()
    return [
        row
        for row in cache.get("futures") or []
        if str(row.get("name") or "").upper() == want
    ]


def _bind_session_refs(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    snapshots: dict[str, SymbolSnapshot],
    *,
    now=None,
) -> dict[tuple[str, int], dict[str, Any]]:
    items = []
    for symbol, snap in snapshots.items():
        if snap.row is None:
            continue
        meta = lookup_instrument_meta(cache or {}, symbol)
        items.append((snap.row, snap.state, meta))
    if not items:
        return {}
    return maintain_session_references(store, settings.session, items, now=now)


def assemble_quote(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    symbol: str,
    *,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any] | None:
    bundled = assemble_quotes(settings, store, cache, [symbol], policy=policy)
    return bundled.get(symbol)


def assemble_quotes(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    symbols: list[str],
    *,
    policy: MarketDataPolicy | None = None,
    snapshots: dict[str, SymbolSnapshot] | None = None,
) -> dict[str, dict[str, Any] | None]:
    read = _policy(settings, store, policy)
    snaps = snapshots if snapshots is not None else read.snapshots(symbols)
    refs = _bind_session_refs(settings, store, cache, snaps, now=read.now())
    out: dict[str, dict[str, Any] | None] = {}
    for symbol in symbols:
        snap = snaps.get(symbol) or _blank(symbol, read)
        if snap.row is None:
            out[symbol] = None
            continue
        meta = lookup_instrument_meta(cache or {}, symbol)
        stored = stored_for_snapshot(refs, snap.row, snap.state)
        out[symbol] = snap.attach(
            enrich_quote(
                snap.row,
                meta=meta,
                stored=stored,
                session_date=snap.state.session_date,
            )
        )
    return out


def assemble_option_chain(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any],
    underlying: str,
    expiry: str | None,
    *,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any]:
    read = _policy(settings, store, policy)
    expiries = _expiries_for(cache, underlying)
    chosen = (expiry or "").strip()[:10] or (expiries[0] if expiries else "")
    contracts = _option_contracts(cache, underlying, chosen) if chosen else []
    spot_sym = INDEX_SPOT_BY_NAME.get(underlying.upper(), underlying.upper())
    # One coherent read: spot and every selected contract resolve together.
    wanted = [spot_sym] + [str(c.get("tradingsymbol")) for c in contracts]
    snapshots = read.snapshots(wanted)
    quotes = _rows(snapshots)
    refs = _bind_session_refs(settings, store, cache, snapshots, now=read.now())
    stored_by_symbol: dict[str, dict[str, Any]] = {}
    session_date_by_symbol: dict[str, str | None] = {}
    for name, snap in snapshots.items():
        stored = stored_for_snapshot(refs, snap.row, snap.state)
        if stored is not None:
            stored_by_symbol[name] = stored
        session_date_by_symbol[name] = snap.state.session_date
    spot_snap = snapshots.get(spot_sym) or _blank(spot_sym, read)
    spot_row = spot_snap.row
    spot = None if spot_row is None else spot_row.get("last_price")
    interval = None
    cfg = _cfg(settings)
    for u in settings.options.underlyings:
        if u.name.upper() == underlying.upper():
            interval = u.strike_interval
            break
    if interval is None:
        interval = infer_strike_interval(
            [float(c["strike"]) for c in contracts if c.get("strike") is not None]
        )
    eligible_count, selected_count, truncated = _chain_selection_counts(
        cache, underlying.upper(), chosen, selected_fallback=len(contracts)
    )
    chain = build_option_chain(
        underlying=underlying.upper(),
        expiry=chosen,
        contracts=contracts,
        quotes_by_symbol=quotes,
        spot=None if spot is None else float(spot),
        interval=interval,
        atm_method=cfg.atm_method,
        pcr_window=cfg.pcr_atm_strikes,
        max_pain_min_strikes=cfg.max_pain_min_strikes,
        max_pain_min_completeness=cfg.max_pain_min_completeness,
        eligible_contract_count=eligible_count,
        selected_contract_count=selected_count,
        truncated=truncated,
        rate=cfg.option_risk_free_rate,
        close_hhmm=settings.session.market_close,
        stored_by_symbol=stored_by_symbol,
        session_date_by_symbol=session_date_by_symbol,
    )
    chain["available_expiries"] = expiries
    chain["found"] = bool(contracts)
    chain["data_state"] = read.merged_state(
        [snapshots[name] for name in wanted if name in snapshots]
    ).to_dict()
    return chain


def assemble_futures(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any],
    underlying: str,
    *,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any]:
    read = _policy(settings, store, policy)
    contracts = _future_contracts(cache, underlying)
    spot_sym = INDEX_SPOT_BY_NAME.get(underlying.upper(), underlying.upper())
    # Spot and futures resolve in one read so basis compares the same session.
    wanted = [spot_sym] + [str(c.get("tradingsymbol")) for c in contracts]
    snapshots = read.snapshots(wanted)
    refs = _bind_session_refs(settings, store, cache, snapshots, now=read.now())
    spot_snap = snapshots.get(spot_sym) or _blank(spot_sym, read)
    spot_row = spot_snap.row
    spot = None if spot_row is None else spot_row.get("last_price")
    spot_as_of = None if spot_row is None else spot_row.get("timestamp")
    items = []
    for meta in contracts:
        name = str(meta.get("tradingsymbol"))
        snap = snapshots.get(name) or _blank(name, read)
        items.append(
            snap.attach(
                futures_snapshot(
                    snap.row,
                    meta=meta,
                    spot=None if spot is None else float(spot),
                    spot_as_of=spot_as_of,
                    spot_symbol=spot_sym,
                    max_age_seconds=_cfg(settings).futures_basis_max_age_seconds,
                    stored=stored_for_snapshot(refs, snap.row, snap.state),
                    session_date=snap.state.session_date,
                )
            )
        )
    return {
        "underlying": underlying.upper(),
        "spot": spot,
        "spot_symbol": spot_sym,
        "spot_as_of": spot_as_of,
        "contracts": items,
        "found": bool(contracts),
        "kind": "derived",
        "data_state": read.merged_state(
            [snapshots[name] for name in wanted if name in snapshots]
        ).to_dict(),
    }


def _activity_bundle(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    symbol: str,
    *,
    snapshot: SymbolSnapshot,
    stored: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    quote = snapshot.row
    if quote is None:
        return None
    meta = lookup_instrument_meta(cache or {}, symbol) or {}
    cfg = _cfg(settings)
    bucket = tod_bucket(quote.get("timestamp"), cfg.tod_bucket_minutes)
    samples = store.fetch_activity_baseline(
        symbol, tod_bucket=bucket, limit=max(cfg.baseline_min_observations * 3, 90)
    )
    notionals = [float(s["trade_notional"]) for s in samples if s.get("trade_notional") is not None]
    sizes = [float(s["last_quantity"]) for s in samples if s.get("last_quantity") is not None]
    vol_deltas = [float(s["volume_delta"]) for s in samples if s.get("volume_delta") is not None]
    baseline_vol = (sum(vol_deltas) / len(vol_deltas)) if vol_deltas else None
    activity_rate = 1.0 if quote.get("last_quantity") else None
    baseline_act = (len(samples) / max(len(vol_deltas), 1)) if samples else None
    # Live baselines come from activity_samples; a last-session snapshot
    # compares against the preceding observation of that same session.
    prev = snapshot.previous or (samples[1] if len(samples) > 1 else None)
    activity = market_activity_row(
        quote,
        lot_size=meta.get("lot_size"),
        notional_sample=notionals,
        size_sample=sizes,
        volume_rate=quote.get("volume_delta"),
        baseline_volume_rate=baseline_vol,
        activity_rate=activity_rate,
        baseline_activity_rate=baseline_act if samples else None,
        thresholds=_thresholds(settings),
        baseline_min=cfg.baseline_min_observations,
    )
    aggressor = aggressive_side_proxy(
        quote.get("last_price"),
        quote.get("best_bid_price"),
        quote.get("best_ask_price"),
        tolerance_bps=cfg.aggressor_tolerance_bps,
    )
    depth = displayed_depth_changes(quote, prev)
    events = liquidity_events(
        depth,
        depth_shock_pct=cfg.depth_shock_pct,
        spread_widen_pct=cfg.spread_widen_pct,
        previous=prev,
    )
    impact = price_impact_note(
        activity.get("trade_notional"),
        quote.get("price_delta"),
        events,
        activity.get("large_trade"),
    )
    unusual = unusual_activity_score(
        large_trade=activity.get("large_trade"),
        trade_notional_percentile=activity.get("trade_notional_percentile"),
        volume_level=activity.get("volume_level"),
        volume_ratio=activity.get("volume_ratio"),
        activity_level=activity.get("activity_level"),
        oi_delta=quote.get("oi_delta"),
        liquidity_events=events,
        aggressor=aggressor.get("label"),
        high_score=cfg.unusual_high_score,
    )
    return {
        "activity": activity,
        "aggressive_proxy": aggressor,
        "displayed_depth_changes": depth,
        "liquidity_events": events,
        "price_impact": impact,
        "unusual": unusual,
        "quote": enrich_quote(
            quote,
            meta=meta,
            stored=stored,
            session_date=snapshot.state.session_date,
        ),
        "data_state": snapshot.state.to_dict(),
    }


def assemble_market_activity(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    symbol: str,
    *,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any] | None:
    read = _policy(settings, store, policy)
    snapshot = read.snapshot(symbol)
    refs = _bind_session_refs(settings, store, cache, {symbol: snapshot}, now=read.now())
    return _activity_bundle(
        settings,
        store,
        cache,
        symbol,
        snapshot=snapshot,
        stored=stored_for_snapshot(refs, snapshot.row, snapshot.state),
    )


# Live unusual-activity still ranks up to this many latest_quotes rows.
UNUSUAL_LIVE_CANDIDATE_LIMIT = 500


def _unusual_historical_candidate_limit(settings: Settings, limit: int) -> int:
    """Bound the last-session Parquet scan. Live ranking does not use this."""
    cfg = _cfg(settings)
    requested = max(int(limit), 1)
    multiplied = requested * max(int(cfg.unusual_historical_candidate_multiplier), 1)
    return max(1, min(int(cfg.unusual_historical_max_candidates), multiplied))


def assemble_unusual(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any] | None,
    *,
    limit: int = 50,
    policy: MarketDataPolicy | None = None,
) -> list[dict[str, Any]]:
    read = _policy(settings, store, policy)
    snapshots = read.all_snapshots(
        limit=UNUSUAL_LIVE_CANDIDATE_LIMIT,
        historical_limit=_unusual_historical_candidate_limit(settings, limit),
    )
    refs = _bind_session_refs(settings, store, cache, snapshots, now=read.now())
    ranked = []
    for symbol, snap in snapshots.items():
        bundle = _activity_bundle(
            settings,
            store,
            cache,
            symbol,
            snapshot=snap,
            stored=stored_for_snapshot(refs, snap.row, snap.state),
        )
        if not bundle:
            continue
        score = bundle["unusual"]["activity_score"]
        if score <= 0:
            continue
        ranked.append(
            {
                "symbol": symbol,
                **bundle["unusual"],
                "liquidity_events": bundle["liquidity_events"],
                "aggressive_proxy": bundle["aggressive_proxy"]["label"],
                "data_state": snap.state.to_dict(),
            }
        )
    ranked.sort(key=lambda r: r["activity_score"], reverse=True)
    return ranked[:limit]


def assemble_charts(
    settings: Settings,
    store: SQLiteStore,
    symbol: str,
    *,
    interval: str | None = None,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any]:
    cfg = _cfg(settings)
    samples = store.fetch_activity_samples(symbol, limit=5000)
    historical, hist_source = load_historical_observations(
        settings, symbol, lookback_days=cfg.chart_lookback_days
    )
    rows = merge_chart_rows(historical, samples)
    payload = chart_payload(
        rows,
        max_points=cfg.chart_max_points,
        fields=CHART_POINT_FIELDS,
    )
    payload.update(
        load_ohlc_candles(
            settings,
            symbol,
            interval=interval,
            lookback_days=cfg.chart_lookback_days,
        )
    )
    sources = []
    if historical:
        sources.append(hist_source or "compacted_ticks")
    if samples:
        sources.append("activity_samples")
    payload["source"] = "+".join(sources) if sources else None
    # activity_samples is the live path; compacted Parquet is the historical one.
    _choice, state = resolve(
        session=settings.session,
        live_timestamp=samples[-1].get("timestamp") if samples else None,
        historical_timestamp=historical[-1].get("timestamp") if historical else None,
        historical_source=hist_source,
        max_age_seconds=cfg.live_quote_max_age_seconds,
    )
    payload["data_state"] = state.to_dict()
    return payload


def assemble_cross_market(
    settings: Settings,
    store: SQLiteStore,
    cache: dict[str, Any],
    underlying: str,
    *,
    policy: MarketDataPolicy | None = None,
) -> dict[str, Any]:
    read = _policy(settings, store, policy)
    fut = assemble_futures(settings, store, cache, underlying, policy=read)
    opt = assemble_option_chain(settings, store, cache, underlying, None, policy=read)
    spot_sym = INDEX_SPOT_BY_NAME.get(underlying.upper(), underlying.upper())
    spot_row = read.snapshot(spot_sym).row
    near = fut["contracts"][0] if fut["contracts"] else {}
    notes = describe_cross_market(
        spot_change=None if spot_row is None else spot_row.get("price_delta"),
        future_oi_change=near.get("oi_change"),
        basis=near.get("basis"),
        prev_basis=None,
        call_oi_change=None,
        put_oi_change=None,
        volume_level=None,
        liquidity_events=[],
    )
    if opt.get("largest_ce_oi_increase"):
        notes.append("call OI build-up")
    if opt.get("largest_pe_oi_increase"):
        notes.append("put OI build-up")
    return {
        "underlying": underlying.upper(),
        "relationships": notes,
        "kind": "inferred",
        "note": "Descriptive relationships, not prediction probabilities.",
        "data_state": fut.get("data_state"),
        "futures": fut,
        "options_summary": {
            "expiry": opt.get("expiry"),
            "pcr_oi": opt.get("pcr_oi"),
            "atm": opt.get("atm"),
            "chain_completeness": opt.get("chain_completeness"),
            "chain_status": opt.get("chain_status"),
            "eligible_contract_count": opt.get("eligible_contract_count"),
            "selected_contract_count": opt.get("selected_contract_count"),
            "missing_contract_count": opt.get("missing_contract_count"),
            "quoted_contract_count": opt.get("quoted_contract_count"),
            "quote_coverage": opt.get("quote_coverage"),
            "quote_status": opt.get("quote_status"),
        },
    }
