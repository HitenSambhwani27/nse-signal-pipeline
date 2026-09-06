"""
Resolve NSE equity / index / options / futures instrument lists for Stage 1G.

Equity membership comes from official NSE constituent CSVs (refreshed
periodically). Quote-mode equities are always Nifty 500 \\ Nifty 100.
Expiry and lot size always come from the live Kite instrument master —
never hardcoded.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

from kiteconnect import KiteConnect

from nse_pipeline.broker.nse_membership import refresh_membership
from nse_pipeline.broker.option_series import filter_rows_by_series
from nse_pipeline.config import OptionUnderlyingSettings, Settings
from nse_pipeline.market.universe import (
    allocate_stock_option_slots,
    collect_stock_option_slots,
    current_and_next_expiries,
    fo_names_from_nfo_rows,
    nifty100_fo_eligible,
    stock_option_subscribe_mode,
    summarize_stock_option_allocation,
)
from nse_pipeline.storage.schemas import InstrumentInfo


logger = logging.getLogger(__name__)


def _parse_expiry(expiry_value: Any) -> date | None:
    if expiry_value is None:
        return None
    if isinstance(expiry_value, date):
        return expiry_value
    text = str(expiry_value)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _row_to_equity_info(
    row: dict[str, Any],
    *,
    exchange: str,
    subscribe_mode: str,
) -> InstrumentInfo:
    return InstrumentInfo(
        instrument_token=int(row["instrument_token"]),
        tradingsymbol=str(row["tradingsymbol"]),
        exchange=str(row.get("exchange", exchange)),
        name=str(row.get("name", row["tradingsymbol"])),
        segment=str(row.get("segment", "")),
        instrument_type=str(row.get("instrument_type", "EQ")),
        lot_size=int(row["lot_size"]) if row.get("lot_size") is not None else None,
        tick_size=float(row["tick_size"]) if row.get("tick_size") is not None else None,
        subscribe_mode=subscribe_mode,
    )


def fetch_equity_instruments(
    kite: KiteConnect,
    symbols: list[str],
    *,
    subscribe_mode: str,
    exchange: str = "NSE",
) -> dict[str, InstrumentInfo]:
    """Map tradingsymbol -> InstrumentInfo for cash equities (excludes INDICES)."""
    instruments = kite.instruments(exchange)
    wanted = set(symbols)
    result: dict[str, InstrumentInfo] = {}

    for row in instruments:
        tradingsymbol = row.get("tradingsymbol")
        if tradingsymbol not in wanted:
            continue
        if str(row.get("segment", "")) == "INDICES":
            continue
        if str(row.get("instrument_type", "")) not in {"EQ", ""}:
            continue
        result[str(tradingsymbol)] = _row_to_equity_info(
            row, exchange=exchange, subscribe_mode=subscribe_mode
        )

    # Second pass for any missing symbols that might only appear as non-EQ series.
    if wanted - set(result.keys()):
        for row in instruments:
            tradingsymbol = row.get("tradingsymbol")
            if tradingsymbol not in wanted or tradingsymbol in result:
                continue
            if str(row.get("segment", "")) == "INDICES":
                continue
            result[str(tradingsymbol)] = _row_to_equity_info(
                row, exchange=exchange, subscribe_mode=subscribe_mode
            )

    missing = wanted - set(result.keys())
    if missing:
        # Log and skip rather than abort the whole universe for a few delisted names.
        logger.warning(
            "Could not resolve %s equity symbols on %s (skipped): %s",
            len(missing),
            exchange,
            sorted(missing)[:20],
        )

    return result


def fetch_index_instruments(
    kite: KiteConnect,
    symbols: list[str],
    exchange: str = "NSE",
) -> dict[str, InstrumentInfo]:
    """
    Resolve NSE index instruments (e.g. 'NIFTY 50', 'NIFTY BANK') for live spot.

    Requires segment=INDICES — Kite labels these instrument_type='EQ'.
    """
    if not symbols:
        return {}

    instruments = kite.instruments(exchange)
    wanted = set(symbols)
    result: dict[str, InstrumentInfo] = {}

    for row in instruments:
        tradingsymbol = row.get("tradingsymbol")
        if tradingsymbol not in wanted:
            continue
        segment = str(row.get("segment", ""))
        if segment != "INDICES":
            continue
        result[str(tradingsymbol)] = InstrumentInfo(
            instrument_token=int(row["instrument_token"]),
            tradingsymbol=str(tradingsymbol),
            exchange=str(row.get("exchange", exchange)),
            name=str(row.get("name", tradingsymbol)),
            segment=segment,
            instrument_type=str(row.get("instrument_type", "")),
            lot_size=int(row["lot_size"]) if row.get("lot_size") is not None else None,
            tick_size=float(row["tick_size"]) if row.get("tick_size") is not None else None,
            subscribe_mode="full",
        )

    missing = wanted - set(result.keys())
    if missing:
        raise ValueError(
            f"Could not resolve INDEX symbols on {exchange} with segment=INDICES: "
            f"{sorted(missing)}"
        )

    return result


def _pick_nearest_expiries(candidates: list[date], today: date, count: int) -> list[date]:
    future = sorted({d for d in candidates if d >= today})
    if not future:
        raise ValueError("No future expiries found in instrument master.")
    return future[:count]


def build_index_option_chain(
    kite: KiteConnect,
    underlying: OptionUnderlyingSettings,
    *,
    exchange: str,
    nfo_rows: list[dict[str, Any]] | None = None,
) -> list[InstrumentInfo]:
    """Build ATM ± N strikes for the nearest in-scope expiry (CE + PE).

    Nifty uses the configured series (weekly); Bank Nifty uses monthly (its
    only series). Nifty monthly contracts are excluded — TRADE_OFFS.md.
    """
    rows = nfo_rows if nfo_rows is not None else kite.instruments(exchange)

    option_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("name", "")) != underlying.name:
            continue
        if str(row.get("instrument_type", "")) not in {"CE", "PE"}:
            continue
        expiry = _parse_expiry(row.get("expiry"))
        if expiry is None:
            continue
        row_copy = dict(row)
        row_copy["_expiry_date"] = expiry
        option_rows.append(row_copy)

    option_rows, series_note = filter_rows_by_series(
        option_rows, underlying=underlying.name, series=underlying.series
    )
    logger.info(
        "Option series filter %s series=%s note=%s rows=%s",
        underlying.name,
        underlying.series,
        series_note,
        len(option_rows),
    )

    if not option_rows:
        raise ValueError(f"No options found for underlying {underlying.name} on {exchange}")

    today = date.today()
    expiries = sorted({row["_expiry_date"] for row in option_rows})
    selected_expiries = current_and_next_expiries(
        expiries, today, getattr(underlying, "expiry_count", 1) or 1
    )
    if not selected_expiries:
        raise ValueError(f"No current/next expiries for {underlying.name} on {exchange}")

    quote = kite.quote([underlying.spot_quote])
    if underlying.spot_quote in quote:
        spot_price = float(quote[underlying.spot_quote]["last_price"])
    else:
        first_key = next(iter(quote))
        spot_price = float(quote[first_key]["last_price"])

    interval = underlying.strike_interval
    subscribe_mode = getattr(underlying, "subscribe_mode", "full") or "full"
    chain: list[InstrumentInfo] = []
    for target_expiry in selected_expiries:
        expiry_rows = [row for row in option_rows if row["_expiry_date"] == target_expiry]
        strikes = sorted({float(row["strike"]) for row in expiry_rows})
        if not strikes:
            continue
        atm = atm_strike(spot_price, strikes, interval=interval)
        selected_strikes: set[float] = set()
        if atm is not None:
            for offset in range(-underlying.strikes_each_side, underlying.strikes_each_side + 1):
                strike = atm + (offset * interval)
                if strike in strikes:
                    selected_strikes.add(strike)
        if not selected_strikes:
            closest = min(strikes, key=lambda s: abs(s - spot_price))
            center_idx = strikes.index(closest)
            half = underlying.strikes_each_side
            start = max(0, center_idx - half)
            end = min(len(strikes), center_idx + half + 1)
            selected_strikes = set(strikes[start:end])

        for row in expiry_rows:
            strike = float(row["strike"])
            if strike not in selected_strikes:
                continue
            chain.append(
                InstrumentInfo(
                    instrument_token=int(row["instrument_token"]),
                    tradingsymbol=str(row["tradingsymbol"]),
                    exchange=str(row.get("exchange", exchange)),
                    name=str(row.get("name", underlying.name)),
                    segment=str(row.get("segment", "")),
                    instrument_type=str(row.get("instrument_type", "")),
                    strike=strike,
                    expiry=str(row.get("expiry")),
                    lot_size=int(row["lot_size"]) if row.get("lot_size") is not None else None,
                    tick_size=float(row["tick_size"]) if row.get("tick_size") is not None else None,
                    subscribe_mode=subscribe_mode,
                )
            )

    chain.sort(key=lambda x: (x.name, x.expiry or "", x.strike or 0.0, x.instrument_type))
    return chain


def build_index_futures(
    kite: KiteConnect,
    settings: Settings,
    *,
    nfo_rows: list[dict[str, Any]] | None = None,
) -> list[InstrumentInfo]:
    """Near + next month futures for configured index underlyings."""
    rows = nfo_rows if nfo_rows is not None else kite.instruments(settings.futures.exchange)
    today = date.today()
    wanted = set(settings.futures.underlyings)
    by_name: dict[str, list[dict[str, Any]]] = {name: [] for name in wanted}

    for row in rows:
        name = str(row.get("name", ""))
        if name not in wanted:
            continue
        if str(row.get("instrument_type", "")) != "FUT":
            continue
        expiry = _parse_expiry(row.get("expiry"))
        if expiry is None:
            continue
        row_copy = dict(row)
        row_copy["_expiry_date"] = expiry
        by_name[name].append(row_copy)

    futures: list[InstrumentInfo] = []
    for name, fut_rows in by_name.items():
        if not fut_rows:
            raise ValueError(f"No futures found for underlying {name}")
        expiries = _pick_nearest_expiries(
            [r["_expiry_date"] for r in fut_rows],
            today,
            settings.futures.contract_count,
        )
        for row in fut_rows:
            if row["_expiry_date"] not in expiries:
                continue
            futures.append(
                InstrumentInfo(
                    instrument_token=int(row["instrument_token"]),
                    tradingsymbol=str(row["tradingsymbol"]),
                    exchange=str(row.get("exchange", settings.futures.exchange)),
                    name=str(row.get("name", name)),
                    segment=str(row.get("segment", "")),
                    instrument_type="FUT",
                    expiry=str(row.get("expiry")),
                    lot_size=int(row["lot_size"]) if row.get("lot_size") is not None else None,
                    tick_size=float(row["tick_size"]) if row.get("tick_size") is not None else None,
                    subscribe_mode=settings.futures.subscribe_mode,
                )
            )

    futures.sort(key=lambda x: (x.name, x.expiry or "", x.tradingsymbol))
    return futures


def _quote_last_prices(kite: KiteConnect, keys: list[str]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for start in range(0, len(keys), 200):
        chunk = keys[start : start + 200]
        payload = kite.quote(chunk)
        for key, data in payload.items():
            symbol = str(key).split(":")[-1]
            last = (data or {}).get("last_price")
            if last is not None:
                prices[symbol] = float(last)
    return prices


def _info_from_nfo_row(
    row: dict[str, Any],
    *,
    subscribe_mode: str,
    exchange: str,
) -> InstrumentInfo:
    return InstrumentInfo(
        instrument_token=int(row["instrument_token"]),
        tradingsymbol=str(row["tradingsymbol"]),
        exchange=str(row.get("exchange", exchange)),
        name=str(row.get("name", "")),
        segment=str(row.get("segment", "")),
        instrument_type=str(row.get("instrument_type", "")),
        strike=float(row["strike"]) if row.get("strike") not in (None, 0, 0.0) else None,
        expiry=str(row.get("expiry")) if row.get("expiry") else None,
        lot_size=int(row["lot_size"]) if row.get("lot_size") is not None else None,
        tick_size=float(row["tick_size"]) if row.get("tick_size") is not None else None,
        subscribe_mode=subscribe_mode,
    )


def build_stock_futures(
    settings: Settings,
    eligible: list[str],
    nfo_rows: list[dict[str, Any]],
    *,
    token_budget: int,
) -> list[InstrumentInfo]:
    cfg = settings.stock_derivatives
    wanted = {s.upper() for s in eligible}
    by_name: dict[str, list[dict[str, Any]]] = {name: [] for name in wanted}
    today = date.today()
    for row in nfo_rows:
        name = str(row.get("name", "")).upper()
        if name not in wanted:
            continue
        if str(row.get("instrument_type", "")).upper() != "FUT":
            continue
        expiry = _parse_expiry(row.get("expiry"))
        if expiry is None:
            continue
        copy = dict(row)
        copy["_expiry_date"] = expiry
        by_name[name].append(copy)

    futures: list[InstrumentInfo] = []
    for name in eligible:
        fut_rows = by_name.get(name.upper()) or []
        if not fut_rows:
            continue
        try:
            expiries = _pick_nearest_expiries(
                [r["_expiry_date"] for r in fut_rows],
                today,
                cfg.futures_contract_count,
            )
        except ValueError:
            continue
        for row in fut_rows:
            if row["_expiry_date"] not in expiries:
                continue
            if len(futures) >= token_budget:
                logger.warning("Stock futures truncated at token budget %s", token_budget)
                return futures
            futures.append(
                _info_from_nfo_row(
                    row,
                    subscribe_mode=cfg.futures_subscribe_mode,
                    exchange=settings.futures.exchange,
                )
            )
    futures.sort(key=lambda x: (x.name, x.expiry or "", x.tradingsymbol))
    return futures


def build_stock_option_chains(
    kite: KiteConnect,
    settings: Settings,
    eligible: list[str],
    nfo_rows: list[dict[str, Any]],
    *,
    token_budget: int,
) -> tuple[list[InstrumentInfo], dict[str, Any]]:
    cfg = settings.stock_derivatives
    wanted = {s.upper() for s in eligible}
    by_name: dict[str, list[dict[str, Any]]] = {name: [] for name in wanted}
    today = date.today()
    for row in nfo_rows:
        name = str(row.get("name", "")).upper()
        if name not in wanted:
            continue
        if str(row.get("instrument_type", "")).upper() not in {"CE", "PE"}:
            continue
        expiry = _parse_expiry(row.get("expiry"))
        if expiry is None:
            continue
        copy = dict(row)
        copy["_expiry_date"] = expiry
        by_name[name].append(copy)

    spots = _quote_last_prices(
        kite, [f"NSE:{symbol}" for symbol in eligible if (by_name.get(symbol.upper()) or [])]
    )
    slots, listed_names, uncovered = collect_stock_option_slots(
        eligible,
        by_name,
        spots,
        today=today,
        expiry_count=cfg.options_expiry_count,
        strikes_each_side=cfg.options_strikes_each_side,
    )
    cap = min(max(0, int(token_budget)), int(cfg.max_stock_option_contracts))
    selected, truncated = allocate_stock_option_slots(slots, max_contracts=cap)
    atm_mode = cfg.options_atm_subscribe_mode
    wing_mode = cfg.options_subscribe_mode
    chain: list[InstrumentInfo] = []
    for slot in selected:
        chain.append(
            _info_from_nfo_row(
                slot.row,
                subscribe_mode=stock_option_subscribe_mode(
                    slot.distance_from_atm,
                    atm_mode=atm_mode,
                    wing_mode=wing_mode,
                ),
                exchange=settings.options.exchange,
            )
        )
    chain.sort(key=lambda x: (x.name, x.expiry or "", x.strike or 0.0, x.instrument_type))
    meta = summarize_stock_option_allocation(
        all_slots=slots,
        selected=selected,
        listed_underlyings=listed_names,
        uncovered_no_atm=uncovered,
        truncated=truncated,
        atm_mode=atm_mode,
        wing_mode=wing_mode,
    )
    if truncated:
        logger.warning(
            "Stock options truncated at budget contracts=%s eligible=%s",
            len(chain),
            len(slots),
        )
    logger.info(
        "Stock options allocation eligible_underlyings=%s selected=%s full=%s quote=%s truncated=%s",
        meta["stock_options_eligible_count"],
        meta["selected_contract_count"],
        meta["stock_options_full_count"],
        meta["stock_options_quote_count"],
        meta["truncated"],
    )
    return chain, meta


def refresh_instrument_cache(
    kite: KiteConnect,
    settings: Settings,
    cache_path: Path | None = None,
    *,
    force_membership: bool = True,
) -> dict[str, Any]:
    """
    Refresh NSE membership CSVs (if forced/stale), resolve all universe tokens,
    and write instruments_cache.json.
    """
    cache_file = cache_path or settings.paths.instruments_cache
    membership = refresh_membership(settings, force=force_membership)

    depth_equities = fetch_equity_instruments(
        kite, membership.nifty100, subscribe_mode="full"
    )
    quote_equities = fetch_equity_instruments(
        kite, membership.quote_only, subscribe_mode="quote"
    )

    # Enforce no overlap even if membership math drifts.
    overlap = set(depth_equities) & set(quote_equities)
    if overlap:
        logger.warning("Removing %s overlapping symbols from quote set", len(overlap))
        for symbol in overlap:
            quote_equities.pop(symbol, None)

    indices = fetch_index_instruments(kite, settings.index_symbols)

    nfo_rows = kite.instruments(settings.options.exchange)
    options: list[InstrumentInfo] = []
    for underlying in settings.options.underlyings:
        options.extend(
            build_index_option_chain(
                kite,
                underlying,
                exchange=settings.options.exchange,
                nfo_rows=nfo_rows,
            )
        )

    futures = build_index_futures(kite, settings, nfo_rows=nfo_rows)

    fo_names = fo_names_from_nfo_rows(nfo_rows)
    fo_eligible = nifty100_fo_eligible(membership.nifty100, fo_names)
    stock_futures: list[InstrumentInfo] = []
    stock_options: list[InstrumentInfo] = []
    sd = settings.stock_derivatives
    base_total = (
        len(depth_equities)
        + len(quote_equities)
        + len(indices)
        + len(options)
        + len(futures)
    )
    remaining = max(0, sd.max_total_tokens - base_total)
    option_selection: dict[str, Any] = {
        "truncated": False,
        "stock_options_eligible_count": 0,
        "stock_options_full_count": 0,
        "stock_options_quote_count": 0,
        "eligible_contract_count": 0,
        "selected_contract_count": 0,
        "atm_covered_underlyings": [],
        "uncovered_no_atm": [],
        "by_underlying": {},
    }
    if sd.enabled and remaining > 0:
        if sd.futures_enabled:
            stock_futures = build_stock_futures(
                settings, fo_eligible, nfo_rows, token_budget=remaining
            )
            remaining = max(0, remaining - len(stock_futures))
        if sd.options_enabled and remaining > 0:
            stock_options, option_selection = build_stock_option_chains(
                kite, settings, fo_eligible, nfo_rows, token_budget=remaining
            )
        elif sd.options_enabled:
            option_selection["truncated"] = True
    elif sd.enabled and sd.options_enabled:
        option_selection["truncated"] = True

    all_options = options + stock_options
    all_futures = futures + stock_futures

    payload: dict[str, Any] = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "membership": membership.to_dict(),
        "equity_depth": {
            symbol: info.to_cache_dict() for symbol, info in depth_equities.items()
        },
        "equity_quote": {
            symbol: info.to_cache_dict() for symbol, info in quote_equities.items()
        },
        "index": {symbol: info.to_cache_dict() for symbol, info in indices.items()},
        "options": [info.to_cache_dict() for info in all_options],
        "futures": [info.to_cache_dict() for info in all_futures],
        "fo_eligible_nifty100": fo_eligible,
        "universe": {
            "configured_index_options": [u.name for u in settings.options.underlyings],
            "configured_index_futures": list(settings.futures.underlyings),
            "stock_derivatives_enabled": sd.enabled,
            "nifty100_fo_eligible_count": len(fo_eligible),
            "selected_stock_futures": len(stock_futures),
            "selected_stock_options": len(stock_options),
            "stock_options_full_count": option_selection.get("stock_options_full_count", 0),
            "stock_options_quote_count": option_selection.get("stock_options_quote_count", 0),
            "stock_options_eligible_count": option_selection.get(
                "stock_options_eligible_count", 0
            ),
            "option_selection": option_selection,
            "note": (
                "configured != selected != live. Live tokens are whatever the "
                "currently running ingest process loaded at startup."
            ),
        },
        "counts": {
            "equity_depth": len(depth_equities),
            "equity_quote": len(quote_equities),
            "index": len(indices),
            "options": len(all_options),
            "index_options": len(options),
            "stock_options": len(stock_options),
            "stock_options_full": option_selection.get("stock_options_full_count", 0),
            "stock_options_quote": option_selection.get("stock_options_quote_count", 0),
            "stock_options_eligible": option_selection.get(
                "stock_options_eligible_count", 0
            ),
            "futures": len(all_futures),
            "index_futures": len(futures),
            "stock_futures": len(stock_futures),
            "fo_eligible_nifty100": len(fo_eligible),
            "total": (
                len(depth_equities)
                + len(quote_equities)
                + len(indices)
                + len(all_options)
                + len(all_futures)
            ),
        },
    }

    # Safety: unique tokens across modes.
    full_tokens = {
        info.instrument_token
        for info in list(depth_equities.values())
        + list(indices.values())
        + all_options
        + all_futures
        if info.subscribe_mode != "quote"
    }
    quote_tokens = {
        info.instrument_token
        for info in list(quote_equities.values())
        + all_options
        + all_futures
        if info.subscribe_mode == "quote"
    }
    # Equities quote set already quote-mode.
    quote_tokens |= {info.instrument_token for info in quote_equities.values()}
    token_overlap = full_tokens & quote_tokens
    if token_overlap:
        raise RuntimeError(
            f"Subscribe-mode token overlap detected ({len(token_overlap)} tokens). "
            "Aborting cache write."
        )

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def load_instrument_cache(cache_path: Path) -> dict[str, Any]:
    if not cache_path.exists():
        raise FileNotFoundError(
            f"Instrument cache not found at {cache_path}. "
            "Run refresh during auth/smoke test first."
        )
    return json.loads(cache_path.read_text(encoding="utf-8"))


def all_subscribed_instruments(cache: dict[str, Any]) -> list[InstrumentInfo]:
    """Flatten depth equities + quote equities + index + options + futures."""
    instruments: list[InstrumentInfo] = []

    # Backward compatible with Stage 1 pilot cache shape (`equity`).
    for _symbol, data in cache.get("equity", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))

    for _symbol, data in cache.get("equity_depth", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for _symbol, data in cache.get("equity_quote", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for _symbol, data in cache.get("index", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for data in cache.get("options", []):
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for data in cache.get("futures", []):
        instruments.append(InstrumentInfo.from_cache_dict(data))
    return instruments


def tokens_by_subscribe_mode(cache: dict[str, Any]) -> tuple[list[int], list[int]]:
    """Return (full_mode_tokens, quote_mode_tokens) with no duplicates."""
    full: list[int] = []
    quote: list[int] = []
    seen: set[int] = set()
    for info in all_subscribed_instruments(cache):
        token = info.instrument_token
        if token in seen:
            continue
        seen.add(token)
        if info.subscribe_mode == "quote":
            quote.append(token)
        else:
            full.append(token)
    return full, quote


def token_to_symbol_map(instruments: list[InstrumentInfo]) -> dict[int, str]:
    return {info.instrument_token: info.tradingsymbol for info in instruments}
