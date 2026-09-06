"""
Live WebSocket tick listener with reconnect and Parquet persistence.

Uses KiteTicker from kiteconnect. Dual-mode subscribe:
Nifty 100 / index / options / futures = MODE_FULL; Nifty 500 minus 100 = MODE_QUOTE.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
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
from nse_pipeline.market.activity import tod_bucket, trade_notional
from nse_pipeline.market.latest import LatestQuoteTracker
from nse_pipeline.market.normalize import normalize_tick
from nse_pipeline.storage.parquet_writer import BufferedParquetWriter
from nse_pipeline.storage.sqlite_store import SQLiteStore


logger = logging.getLogger(__name__)

_LATEST_PERSIST_SECONDS = 2.0


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
        self._quotes = LatestQuoteTracker()
        self._last_quote_persist = 0.0
        self._last_activity_sample: dict[int, float] = {}

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
            self._quotes.observe(normalized)
            self._rows_since_meta += 1

            if flushed > 0:
                self.store.log_ingestion_event(
                    event_type="flush",
                    message="Parquet flush",
                    symbol=normalized.symbol,
                    rows_written=flushed,
                )
                self._rows_since_meta = 0

    def _persist_latest_quotes(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_quote_persist) < _LATEST_PERSIST_SECONDS:
            return
        rows = self._quotes.drain_dirty()
        self._last_quote_persist = now
        if not rows:
            return
        try:
            self.store.upsert_latest_quotes(rows)
            interval = float(getattr(self.settings.analytics, "sample_every_seconds", 60))
            samples = []
            for row in rows:
                token = int(row["instrument_token"])
                last = self._last_activity_sample.get(token, 0.0)
                if not force and (now - last) < interval:
                    continue
                self._last_activity_sample[token] = now
                samples.append(
                    {
                        "instrument_token": token,
                        "symbol": row.get("symbol"),
                        "timestamp": row.get("timestamp"),
                        "last_price": row.get("last_price"),
                        "last_quantity": row.get("last_quantity"),
                        "volume": row.get("volume"),
                        "volume_delta": row.get("volume_delta"),
                        "oi": row.get("oi"),
                        "oi_delta": row.get("oi_delta"),
                        "trade_notional": trade_notional(
                            row.get("last_price"), row.get("last_quantity")
                        ),
                        "bid_depth_5": row.get("bid_depth_5"),
                        "ask_depth_5": row.get("ask_depth_5"),
                        "spread": row.get("spread"),
                        "depth_imbalance": row.get("depth_imbalance"),
                        "tod_bucket": tod_bucket(
                            row.get("timestamp"),
                            int(getattr(self.settings.analytics, "tod_bucket_minutes", 15)),
                        ),
                    }
                )
            if samples:
                self.store.insert_activity_samples(samples)
        except Exception:
            logger.exception("Failed to persist latest_quotes (%s instruments)", len(rows))

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
                self._persist_latest_quotes()

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
        self._persist_latest_quotes(force=True)
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
