"""
Data schemas for ticks, candles, and ingestion metadata.

Python dataclasses auto-generate __init__ — similar to C# records,
but you can also add helper methods on the class.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class DepthLevel:
    """Single price level in the order book."""

    price: float
    quantity: int
    orders: int = 0


@dataclass
class NormalizedTick:
    """
    Flat tick record written to Parquet.

    All depth levels are stored as parallel lists (column-friendly for Parquet).
    """

    timestamp: datetime
    instrument_token: int
    symbol: str
    exchange: str
    last_price: float
    volume: int
    last_quantity: int
    average_price: float
    oi: int | None
    bid_prices: list[float] = field(default_factory=list)
    bid_quantities: list[int] = field(default_factory=list)
    bid_orders: list[int] = field(default_factory=list)
    ask_prices: list[float] = field(default_factory=list)
    ask_quantities: list[int] = field(default_factory=list)
    ask_orders: list[int] = field(default_factory=list)

    def to_row_dict(self) -> dict[str, Any]:
        """Convert to a flat dict suitable for pandas DataFrame rows."""
        row = asdict(self)
        # Parquet stores timestamps natively; keep as datetime object.
        return row


@dataclass
class CandleRecord:
    """OHLCV + OI candle for historical backfill."""

    timestamp: datetime
    instrument_token: int
    symbol: str
    exchange: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int | None


@dataclass
class InstrumentInfo:
    """Resolved Kite instrument metadata."""

    instrument_token: int
    tradingsymbol: str
    exchange: str
    name: str
    segment: str
    instrument_type: str
    strike: float | None = None
    expiry: str | None = None

    def to_cache_dict(self) -> dict[str, Any]:
        return {
            "instrument_token": self.instrument_token,
            "tradingsymbol": self.tradingsymbol,
            "exchange": self.exchange,
            "name": self.name,
            "segment": self.segment,
            "instrument_type": self.instrument_type,
            "strike": self.strike,
            "expiry": self.expiry,
        }

    @classmethod
    def from_cache_dict(cls, data: dict[str, Any]) -> "InstrumentInfo":
        return cls(
            instrument_token=int(data["instrument_token"]),
            tradingsymbol=str(data["tradingsymbol"]),
            exchange=str(data["exchange"]),
            name=str(data.get("name", "")),
            segment=str(data.get("segment", "")),
            instrument_type=str(data.get("instrument_type", "")),
            strike=float(data["strike"]) if data.get("strike") is not None else None,
            expiry=str(data["expiry"]) if data.get("expiry") else None,
        )
