"""Stage 7 — poll Kite REST for account/order ground truth. Independent of models."""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol

from nse_pipeline.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class KiteAccountClient(Protocol):
    def orders(self) -> list[dict[str, Any]]: ...
    def trades(self) -> list[dict[str, Any]]: ...
    def positions(self) -> dict[str, Any]: ...
    def margins(self) -> dict[str, Any]: ...
    def holdings(self) -> list[dict[str, Any]]: ...
    def profile(self) -> dict[str, Any]: ...


def capture_once(
    store: SQLiteStore,
    kite: KiteAccountClient,
    *,
    include_holdings: bool = False,
    include_session_audit: bool = False,
) -> dict[str, int]:
    """One REST poll. Safe to run while the maturity gate is suppressed."""
    orders = list(kite.orders() or [])
    trades = list(kite.trades() or [])
    positions = kite.positions() or {}
    margins = kite.margins() or {}
    n_orders = store.insert_order_events(orders)
    n_fills = store.insert_trade_fills(trades)
    store.insert_positions_snapshot(positions.get("net") or positions, kind="net")
    if isinstance(positions, dict) and positions.get("day") is not None:
        store.insert_positions_snapshot(positions.get("day") or [], kind="day")
    store.insert_margin_snapshot(margins)
    n_hold = 0
    if include_holdings:
        store.insert_holdings_snapshot(kite.holdings() or [])
        n_hold = 1
    if include_session_audit:
        store.insert_session_audit(profile=kite.profile(), margins=margins)
    return {
        "order_events": n_orders,
        "trade_fills": n_fills,
        "holdings": n_hold,
        "positions": 1,
        "margins": 1,
    }


def run_capture_loop(
    store: SQLiteStore,
    kite: KiteAccountClient,
    *,
    poll_seconds: int = 30,
    once: bool = False,
) -> dict[str, int]:
    first = capture_once(
        store, kite, include_holdings=True, include_session_audit=True
    )
    if once:
        return first
    while True:
        time.sleep(max(5, int(poll_seconds)))
        try:
            capture_once(store, kite)
        except Exception:
            logger.exception("account capture poll failed")
