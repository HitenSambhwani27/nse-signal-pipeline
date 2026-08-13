"""
Live WebSocket tick listener with reconnect and Parquet persistence.

Uses KiteTicker from kiteconnect for real-time market depth (mode='full').
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any

from kiteconnect import KiteConnect
from kiteconnect import KiteTicker

from nse_pipeline.broker.instruments import (
    all_subscribed_instruments,
    load_instrument_cache,
    token_to_symbol_map,
    tokens_by_subscribe_mode,
)
from nse_pipeline.config import Settings
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter
from nse_pipeline.storage.schemas import NormalizedTick
from nse_pipeline.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)


def _parse_depth_side(levels: list[dict[str, Any]] | None) -> tuple[list[float], list[int], list[int]]:
    prices: list[float] = []
    quantities: list[int] = []
    orders: list[int] = []
    if not levels:
        return prices, quantities, orders
    for level in levels:
        prices.append(float(level.get("price", 0.0)))
        quantities.append(int(level.get("quantity", 0)))
        orders.append(int(level.get("orders", 0)))
    return prices, quantities, orders


def normalize_tick(
    tick: dict[str, Any],
    token_symbol_map: dict[int, str],
    exchange_by_token: dict[int, str],
) -> NormalizedTick | None:
    """Convert raw Kite tick dict into our flat NormalizedTick schema."""
    token = tick.get("instrument_token")
    if token is None:
        return None

    token_int = int(token)
    symbol = token_symbol_map.get(token_int)
    if not symbol:
        return None

    timestamp = tick.get("timestamp")
    if isinstance(timestamp, datetime):
        ts = timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = datetime.now(timezone.utc)

    depth = tick.get("depth") or {}
    bid_prices, bid_quantities, bid_orders = _parse_depth_side(depth.get("buy"))
    ask_prices, ask_quantities, ask_orders = _parse_depth_side(depth.get("sell"))

    oi_value = tick.get("oi")
    oi_int = int(oi_value) if oi_value is not None else None

    return NormalizedTick(
        timestamp=ts,
        instrument_token=token_int,
        symbol=symbol,
        exchange=exchange_by_token.get(token_int, "NSE"),
        last_price=float(tick.get("last_price", 0.0)),
        volume=int(tick.get("volume_traded", tick.get("volume", 0)) or 0),
        last_quantity=int(tick.get("last_traded_quantity", tick.get("last_quantity", 0)) or 0),
        average_price=float(tick.get("average_traded_price", tick.get("average_price", 0.0)) or 0.0),
        oi=oi_int,
        bid_prices=bid_prices,
        bid_quantities=bid_quantities,
        bid_orders=bid_orders,
        ask_prices=ask_prices,
        ask_quantities=ask_quantities,
        ask_orders=ask_orders,
    )


class WebSocketIngestionService:
    """
    Manages KiteTicker subscription, buffering, reconnect, and metadata logging.

    Designed to run unattended during market hours.
    """

    def __init__(self, settings: Settings, kite: KiteConnect) -> None:
        self.settings = settings
        self.kite = kite
        self.store = SQLiteStore(settings.paths.sqlite_db)
        self.writer = BufferedParquetWriter(
            raw_dir=settings.paths.raw_dir,
            flush_interval_seconds=settings.ingestion.flush_interval_seconds,
            flush_max_rows=settings.ingestion.flush_max_rows,
        )

        cache = load_instrument_cache(settings.paths.instruments_cache)
        self.instruments = all_subscribed_instruments(cache)
        self.token_symbol_map = token_to_symbol_map(self.instruments)
        self.exchange_by_token = {i.instrument_token: i.exchange for i in self.instruments}
        self.full_tokens, self.quote_tokens = tokens_by_subscribe_mode(cache)
        self.tokens = self.full_tokens + self.quote_tokens

        self._stop_event = threading.Event()
        self._connection_lost_event = threading.Event()
        self._ticker: KiteTicker | None = None
        self._reconnect_delay = settings.resilience.reconnect_initial_seconds
        self._rows_since_meta = 0

    def _build_ticker(self) -> KiteTicker:
        creds = self.settings.kite
        ticker = KiteTicker(creds.api_key, creds.access_token)
        ticker.on_ticks = self._on_ticks
        ticker.on_connect = self._on_connect
        ticker.on_close = self._on_close
        ticker.on_error = self._on_error
        ticker.on_reconnect = self._on_reconnect
        ticker.on_noreconnect = self._on_noreconnect
        return ticker

    def _on_connect(self, ws, response) -> None:
        logger.info("WebSocket connected: %s", response)
        self._connection_lost_event.clear()
        self.store.log_ingestion_event(
            event_type="connect",
            message="WebSocket connected",
            details={
                "token_count": len(self.tokens),
                "full_mode": len(self.full_tokens),
                "quote_mode": len(self.quote_tokens),
            },
        )
        self._reconnect_delay = self.settings.resilience.reconnect_initial_seconds

        # Dual-mode subscribe: Nifty 100 / F&O / spots = FULL; N500\\N100 = QUOTE.
        if self.tokens:
            ws.subscribe(self.tokens)
        if self.full_tokens:
            ws.set_mode(ws.MODE_FULL, self.full_tokens)
        if self.quote_tokens:
            ws.set_mode(ws.MODE_QUOTE, self.quote_tokens)
        logger.info(
            "Subscribed full=%s quote=%s total=%s",
            len(self.full_tokens),
            len(self.quote_tokens),
            len(self.tokens),
        )

    def _on_close(self, ws, code, reason) -> None:
        logger.warning("WebSocket closed: code=%s reason=%s", code, reason)
        self._connection_lost_event.set()
        self.store.log_ingestion_event(
            event_type="disconnect",
            message=f"WebSocket closed code={code}",
            details={"reason": str(reason)},
        )

    def _on_error(self, ws, code, reason) -> None:
        logger.error("WebSocket error: code=%s reason=%s", code, reason)
        self.store.log_ingestion_event(
            event_type="error",
            message=f"WebSocket error code={code}",
            details={"reason": str(reason)},
        )

    def _on_reconnect(self, ws, attempts_count) -> None:
        logger.info("WebSocket reconnect attempt #%s", attempts_count)
        self.store.log_ingestion_event(
            event_type="reconnect",
            message=f"Reconnect attempt {attempts_count}",
        )

    def _on_noreconnect(self, ws) -> None:
        logger.error("WebSocket gave up reconnecting.")
        self.store.log_ingestion_event(
            event_type="noreconnect",
            message="WebSocket will not reconnect further",
        )
        self._connection_lost_event.set()
        self._stop_event.set()

    def _on_ticks(self, ws, ticks: list[dict[str, Any]]) -> None:
        for raw_tick in ticks:
            normalized = normalize_tick(
                raw_tick,
                self.token_symbol_map,
                self.exchange_by_token,
            )
            if normalized is None:
                continue

            flushed = self.writer.add_tick(normalized)
            self._rows_since_meta += 1

            if flushed > 0:
                self.store.log_ingestion_event(
                    event_type="flush",
                    message="Parquet flush",
                    symbol=normalized.symbol,
                    rows_written=flushed,
                )
                self._rows_since_meta = 0

    def _install_signal_handlers(self) -> None:
        def handle_stop(signum, frame) -> None:
            logger.info("Received signal %s — shutting down gracefully.", signum)
            self.stop()

        signal.signal(signal.SIGINT, handle_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handle_stop)

    def stop(self) -> None:
        self._stop_event.set()
        if self._ticker is not None:
            try:
                self._ticker.close()
            except Exception as exc:
                logger.warning("Error closing ticker: %s", exc)

    def run_forever(self) -> None:
        """Blocking run loop with manual reconnect if KiteTicker stops."""
        self._install_signal_handlers()
        self.store.log_ingestion_event(
            event_type="startup",
            message="Ingestion service starting",
            details={"tokens": len(self.tokens)},
        )

        while not self._stop_event.is_set():
            self._connection_lost_event.clear()
            self._ticker = self._build_ticker()
            try:
                # connect(threaded=True) runs the socket loop on a background thread.
                self._ticker.connect(threaded=True)
            except Exception as exc:
                logger.exception("Failed to connect WebSocket: %s", exc)
                self.store.log_ingestion_event(
                    event_type="connect_failed",
                    message=str(exc),
                )

            # Wait until user stop, socket close, or noreconnect.
            while not self._stop_event.is_set() and not self._connection_lost_event.is_set():
                time.sleep(1.0)

            if self._stop_event.is_set():
                break

            # Exponential backoff before rebuilding ticker.
            delay = min(self._reconnect_delay, self.settings.resilience.reconnect_max_seconds)
            logger.info("Reconnecting in %.1f seconds...", delay)
            time.sleep(delay)
            self._reconnect_delay = min(
                self._reconnect_delay * self.settings.resilience.reconnect_multiplier,
                self.settings.resilience.reconnect_max_seconds,
            )

        flushed = self.writer.flush_all()
        self.store.log_ingestion_event(
            event_type="shutdown",
            message="Ingestion service stopped",
            rows_written=flushed,
        )
        logger.info("Shutdown complete. Final flush rows=%s", flushed)


def configure_logging(logs_dir) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "ingestion.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
