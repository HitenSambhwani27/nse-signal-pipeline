"""Quote enrichment. Observed fields stay None when the packet has no data."""

from __future__ import annotations

from typing import Any

from nse_pipeline.market.latest import public_quote


def _ratio(numer: float | None, denom: float | None) -> float | None:
    if numer is None or denom is None or denom == 0:
        return None
    return (float(numer) / float(denom)) * 100.0


def enrich_quote(row: dict[str, Any], *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    dto = public_quote(row, meta=meta)
    last = row.get("last_price")
    prev_close = row.get("ohlc_close")
    change = dto.get("change")
    change_pct = None
    if change is not None and prev_close not in (None, 0):
        change_pct = _ratio(float(change), float(prev_close))
    elif change is not None and last not in (None, 0) and change != last:
        base = float(last) - float(change)
        change_pct = _ratio(float(change), base) if base else None
    oi = row.get("oi")
    oi_change = dto.get("oi_change")
    oi_change_pct = _ratio(
        float(oi_change) if oi_change is not None else None,
        float(oi) - float(oi_change) if oi is not None and oi_change is not None else None,
    )
    dto.update(
        {
            "change_pct": change_pct,
            "average_price": row.get("average_price"),
            "ohlc": {
                "open": row.get("ohlc_open"),
                "high": row.get("ohlc_high"),
                "low": row.get("ohlc_low"),
                "close": row.get("ohlc_close"),
            },
            "oi_change_pct": oi_change_pct,
            "best_bid_quantity": row.get("best_bid_quantity"),
            "best_ask_quantity": row.get("best_ask_quantity"),
            "displayed_bid_quantity": dto.get("buy_quantity"),
            "displayed_ask_quantity": dto.get("sell_quantity"),
            "exchange_timestamp": row.get("timestamp"),
            "last_trade_time": row.get("last_trade_time"),
            "ingested_at": row.get("ingested_at"),
            "missing_fields": missing_quote_fields(row),
            "kind": "observed+derived",
        }
    )
    return dto


def missing_quote_fields(row: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    if not row.get("bid_depth_5") and not row.get("best_bid_price"):
        missing.append("depth")
    if row.get("oi") in (None,):
        missing.append("oi")
    if row.get("volume") in (None, 0) and row.get("last_quantity") in (None, 0):
        missing.append("volume")
    if row.get("ohlc_open") is None:
        missing.append("ohlc")
    return missing
