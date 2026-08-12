"""
Resolve NSE equity tokens and build NIFTY weekly option chain instrument lists.

Kite does not stream an entire option chain as one object — we subscribe to
individual instrument tokens discovered from kite.instruments('NFO').
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from kiteconnect import KiteConnect

from nse_pipeline.config import Settings
from nse_pipeline.storage.schemas import InstrumentInfo


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


def fetch_equity_instruments(
    kite: KiteConnect,
    symbols: list[str],
    exchange: str = "NSE",
) -> dict[str, InstrumentInfo]:
    """Map tradingsymbol -> InstrumentInfo for configured equity symbols."""
    instruments = kite.instruments(exchange)
    wanted = set(symbols)
    result: dict[str, InstrumentInfo] = {}

    for row in instruments:
        tradingsymbol = row.get("tradingsymbol")
        if tradingsymbol not in wanted:
            continue
        # Equities live on NSE cash — reject index rows that share instrument_type=EQ.
        if str(row.get("segment", "")) == "INDICES":
            continue
        info = InstrumentInfo(
            instrument_token=int(row["instrument_token"]),
            tradingsymbol=str(tradingsymbol),
            exchange=str(row.get("exchange", exchange)),
            name=str(row.get("name", tradingsymbol)),
            segment=str(row.get("segment", "")),
            instrument_type=str(row.get("instrument_type", "EQ")),
        )
        result[tradingsymbol] = info

    missing = wanted - set(result.keys())
    if missing:
        raise ValueError(f"Could not resolve equity symbols on {exchange}: {sorted(missing)}")

    return result


def fetch_index_instruments(
    kite: KiteConnect,
    symbols: list[str],
    exchange: str = "NSE",
) -> dict[str, InstrumentInfo]:
    """
    Resolve NSE index instruments (e.g. 'NIFTY 50') for live spot subscription.

    Separate from fetch_equity_instruments: Kite labels indices with
    instrument_type='EQ' but segment='INDICES'. We require INDICES so we never
    accidentally subscribe a cash equity with a similar name.
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
            # Same tradingsymbol on a non-index segment would be wrong for spot.
            continue
        info = InstrumentInfo(
            instrument_token=int(row["instrument_token"]),
            tradingsymbol=str(tradingsymbol),
            exchange=str(row.get("exchange", exchange)),
            name=str(row.get("name", tradingsymbol)),
            segment=segment,
            instrument_type=str(row.get("instrument_type", "")),
        )
        result[str(tradingsymbol)] = info

    missing = wanted - set(result.keys())
    if missing:
        raise ValueError(
            f"Could not resolve INDEX symbols on {exchange} with segment=INDICES: "
            f"{sorted(missing)}"
        )

    return result


def _pick_nearest_weekly_expiry(candidates: list[date], today: date) -> date:
    """Pick the nearest expiry on or after today (current weekly series)."""
    future = [d for d in candidates if d >= today]
    if not future:
        raise ValueError("No future NIFTY weekly expiries found in instrument master.")
    return min(future)


def build_nifty_weekly_option_chain(
    kite: KiteConnect,
    settings: Settings,
    spot_price: float | None = None,
) -> list[InstrumentInfo]:
    """
    Build ATM ± N strikes for current weekly NIFTY expiry (CE + PE).

    If spot_price is None, uses NSE:NIFTY index LTP from quote API.
    """
    options_cfg = settings.options
    nfo_rows = kite.instruments(options_cfg.exchange)

    # Filter to NIFTY index options (exclude NIFTY BANK here).
    nifty_rows: list[dict[str, Any]] = []
    for row in nfo_rows:
        name = str(row.get("name", ""))
        if name != options_cfg.underlying:
            continue
        if str(row.get("instrument_type", "")) not in {"CE", "PE"}:
            continue
        expiry = _parse_expiry(row.get("expiry"))
        if expiry is None:
            continue
        row_copy = dict(row)
        row_copy["_expiry_date"] = expiry
        nifty_rows.append(row_copy)

    if not nifty_rows:
        raise ValueError("No NIFTY options found in NFO instrument master.")

    today = date.today()
    expiries = sorted({row["_expiry_date"] for row in nifty_rows})
    target_expiry = _pick_nearest_weekly_expiry(expiries, today)

    expiry_rows = [row for row in nifty_rows if row["_expiry_date"] == target_expiry]
    strikes = sorted({float(row["strike"]) for row in expiry_rows})
    if not strikes:
        raise ValueError(f"No strikes found for expiry {target_expiry}")

    if spot_price is None:
        index_quote = kite.quote(["NSE:NIFTY 50"])
        # Kite symbol may be 'NSE:NIFTY 50' depending on account; fallback keys handled below.
        if "NSE:NIFTY 50" in index_quote:
            spot_price = float(index_quote["NSE:NIFTY 50"]["last_price"])
        else:
            first_key = next(iter(index_quote))
            spot_price = float(index_quote[first_key]["last_price"])

    interval = options_cfg.strike_interval
    atm_strike = round(spot_price / interval) * interval

    selected_strikes: set[float] = set()
    for offset in range(-options_cfg.strikes_each_side, options_cfg.strikes_each_side + 1):
        strike = atm_strike + (offset * interval)
        if strike in strikes:
            selected_strikes.add(strike)

    # If rounded ATM not present, pick closest listed strike.
    if not selected_strikes:
        closest = min(strikes, key=lambda s: abs(s - atm_strike))
        center_idx = strikes.index(closest)
        half = options_cfg.strikes_each_side
        start = max(0, center_idx - half)
        end = min(len(strikes), center_idx + half + 1)
        selected_strikes = set(strikes[start:end])

    chain: list[InstrumentInfo] = []
    for row in expiry_rows:
        strike = float(row["strike"])
        if strike not in selected_strikes:
            continue
        chain.append(
            InstrumentInfo(
                instrument_token=int(row["instrument_token"]),
                tradingsymbol=str(row["tradingsymbol"]),
                exchange=str(row.get("exchange", options_cfg.exchange)),
                name=str(row.get("name", options_cfg.underlying)),
                segment=str(row.get("segment", "")),
                instrument_type=str(row.get("instrument_type", "")),
                strike=strike,
                expiry=str(row.get("expiry")),
            )
        )

    chain.sort(key=lambda x: (x.strike or 0.0, x.instrument_type))
    return chain


def refresh_instrument_cache(
    kite: KiteConnect,
    settings: Settings,
    cache_path: Path | None = None,
) -> dict[str, Any]:
    """
    Fetch equity + option instruments and write instruments_cache.json.

    Returns the cache dict also written to disk.
    """
    cache_file = cache_path or settings.paths.instruments_cache
    equities = fetch_equity_instruments(kite, settings.equity_symbols)
    indices = fetch_index_instruments(kite, settings.index_symbols)
    options = build_nifty_weekly_option_chain(kite, settings)

    payload: dict[str, Any] = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "equity": {symbol: info.to_cache_dict() for symbol, info in equities.items()},
        "index": {symbol: info.to_cache_dict() for symbol, info in indices.items()},
        "options": [info.to_cache_dict() for info in options],
    }

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
    """Flatten cached equity + index + options into one instrument list."""
    instruments: list[InstrumentInfo] = []
    for _symbol, data in cache.get("equity", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for _symbol, data in cache.get("index", {}).items():
        instruments.append(InstrumentInfo.from_cache_dict(data))
    for data in cache.get("options", []):
        instruments.append(InstrumentInfo.from_cache_dict(data))
    return instruments


def token_to_symbol_map(instruments: list[InstrumentInfo]) -> dict[int, str]:
    return {info.instrument_token: info.tradingsymbol for info in instruments}
