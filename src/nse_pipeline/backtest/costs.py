"""Transaction cost helpers for the walk-forward harness."""

from __future__ import annotations

from nse_pipeline.config import InstrumentCostSettings, Settings


def track_to_cost_key(track: str) -> str:
    if track.startswith("equity") or track == "index":
        return "equity"
    if track == "options":
        return "options"
    if track == "futures":
        return "futures"
    return "equity"


def round_trip_cost_pct(
    settings: Settings,
    *,
    track: str,
    price: float,
    quantity: int = 1,
) -> float:
    """
    Approximate round-trip cost as a fraction of notional.

    Options spread-crossing is an explicit extra line (ticks × tick_size),
    not folded into generic slippage.
    """
    key = track_to_cost_key(track)
    block: InstrumentCostSettings = getattr(settings.costs, key)
    notional = max(abs(price) * max(quantity, 1), 1e-9)
    brokerage = 2.0 * block.brokerage_flat / notional
    stt = block.stt_sell_pct
    slip = block.slippage_pct
    if block.slippage_tick and price:
        slip += block.slippage_tick / abs(price)
    if block.slippage_ticks and block.tick_size and price:
        slip += (block.slippage_ticks * block.tick_size) / abs(price)
    spread = 0.0
    if key == "options" and block.spread_crossing_ticks and block.tick_size and price:
        spread = (block.spread_crossing_ticks * block.tick_size) / abs(price)
    return brokerage + stt + slip + spread


def cost_breakdown(
    settings: Settings, *, track: str, price: float, quantity: int = 1
) -> dict[str, float]:
    key = track_to_cost_key(track)
    block: InstrumentCostSettings = getattr(settings.costs, key)
    notional = max(abs(price) * max(quantity, 1), 1e-9)
    spread = 0.0
    if key == "options" and block.spread_crossing_ticks and block.tick_size and price:
        spread = (block.spread_crossing_ticks * block.tick_size) / abs(price)
    slip = block.slippage_pct
    if block.slippage_tick and price:
        slip += block.slippage_tick / abs(price)
    if block.slippage_ticks and block.tick_size and price:
        slip += (block.slippage_ticks * block.tick_size) / abs(price)
    return {
        "brokerage_pct": 2.0 * block.brokerage_flat / notional,
        "stt_pct": block.stt_sell_pct,
        "slippage_pct": slip,
        "spread_crossing_pct": spread,
        "total_pct": round_trip_cost_pct(
            settings, track=track, price=price, quantity=quantity
        ),
    }
