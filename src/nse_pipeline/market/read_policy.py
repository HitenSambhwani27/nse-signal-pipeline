"""Centralized live -> last-completed-session read policy for the API.

Live state is read from SQLite ``latest_quotes``. The historical fallback reads
the existing compacted-Parquet path through :class:`DuckDBTickStore`.

    latest_quotes            -> live read path
    compacted ticks / daily  -> historical read path
    this policy              -> chooses live OR latest completed session

Historical rows are never written back into ``latest_quotes``; that table keeps
meaning "current live state" only. Historical snapshots keep their own
observation timestamps, and derived fields stay null when the inputs for them
are not stored.
"""

from __future__ import annotations

import threading
import time as _time
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import pandas as pd

from nse_pipeline.config import Settings
from nse_pipeline.market.data_state import (
    CHOICE_HISTORICAL,
    CHOICE_LIVE,
    MARKET_OPEN,
    SOURCE_COMPACTED_DAILY,
    SOURCE_COMPACTED_TICKS,
    STATUS_NO_DATA,
    DataState,
    is_fresh,
    market_state,
    merge_states,
    no_data_state,
    resolve,
)
from nse_pipeline.market.derived import (
    ask_depth_n,
    best_ask,
    best_bid,
    bid_depth_n,
    depth_imbalance,
    mid_price,
    numeric_delta,
    spread,
)
from nse_pipeline.storage.duckdb_store import DuckDBTickStore
from nse_pipeline.storage.sqlite_store import SQLiteStore

# Above this many symbols one full latest_quotes scan beats N point lookups.
_BULK_LIVE_LOOKUP_SYMBOLS = 50


def _scalar(value: Any) -> Any:
    """Plain Python value from a Parquet cell. NaN/NaT become None."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    if value is pd.NaT:
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (AttributeError, ValueError):
            return value
    return value


def _number(value: Any) -> float | None:
    scalar = _scalar(value)
    if scalar is None or isinstance(scalar, (str, bytes)):
        return None
    try:
        number = float(scalar)
    except (TypeError, ValueError):
        return None
    return None if number != number else number


def _sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [_scalar(item) for item in value]
    if hasattr(value, "tolist"):
        try:
            return [_scalar(item) for item in value.tolist()]
        except (AttributeError, ValueError):
            return []
    return []


def _iso_utc(value: Any) -> str | None:
    scalar = _scalar(value)
    if scalar is None:
        return None
    if isinstance(scalar, str):
        return scalar or None
    try:
        stamp = pd.Timestamp(scalar)
    except (TypeError, ValueError):
        return None
    if stamp is pd.NaT:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert("UTC").isoformat()


def snapshot_from_observation(
    row: dict[str, Any], previous: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """latest_quotes-shaped snapshot from one compacted tick observation.

    Mirrors ``market.latest.snapshot_from_tick`` so downstream analytics need no
    special case. Deltas come from the preceding stored observation of the same
    session, exactly as the live tracker computes them.
    """
    symbol = _scalar(row.get("symbol"))
    timestamp = _iso_utc(row.get("timestamp"))
    if not symbol or timestamp is None:
        return None
    bid_px, bid_qty = best_bid(_sequence(row.get("bid_prices")), _sequence(row.get("bid_quantities")))
    ask_px, ask_qty = best_ask(_sequence(row.get("ask_prices")), _sequence(row.get("ask_quantities")))
    bid5 = bid_depth_n(_sequence(row.get("bid_quantities")), n=5)
    ask5 = ask_depth_n(_sequence(row.get("ask_quantities")), n=5)
    prev = previous or {}
    volume = _number(row.get("volume"))
    oi = _number(row.get("oi"))
    last_price = _number(row.get("last_price"))
    volume_delta = numeric_delta(volume, _number(prev.get("volume")))
    if volume_delta is not None and volume_delta < 0:
        volume_delta = None
    oi_delta = numeric_delta(oi, _number(prev.get("oi")))
    price_delta = numeric_delta(last_price, _number(prev.get("last_price")))
    return {
        "instrument_token": _scalar(row.get("instrument_token")),
        "symbol": str(symbol),
        "exchange": _scalar(row.get("exchange")),
        "timestamp": timestamp,
        "ingested_at": _iso_utc(row.get("ingested_at")),
        "last_price": last_price,
        "last_quantity": _scalar(row.get("last_quantity")),
        "volume": _scalar(row.get("volume")),
        "average_price": _number(row.get("average_price")),
        "oi": _scalar(row.get("oi")),
        "total_buy_quantity": _scalar(row.get("total_buy_quantity")),
        "total_sell_quantity": _scalar(row.get("total_sell_quantity")),
        "best_bid_price": bid_px,
        "best_bid_quantity": bid_qty,
        "best_ask_price": ask_px,
        "best_ask_quantity": ask_qty,
        "bid_depth_5": bid5,
        "ask_depth_5": ask5,
        "spread": spread(bid_px, ask_px),
        "mid_price": mid_price(bid_px, ask_px),
        "depth_imbalance": depth_imbalance(bid5, ask5),
        "volume_delta": int(volume_delta) if volume_delta is not None else None,
        "oi_delta": int(oi_delta) if oi_delta is not None else None,
        "price_delta": price_delta,
        "ohlc_open": _number(row.get("ohlc_open")),
        "ohlc_high": _number(row.get("ohlc_high")),
        "ohlc_low": _number(row.get("ohlc_low")),
        "ohlc_close": _number(row.get("ohlc_close")),
        "last_trade_time": _iso_utc(row.get("last_trade_time")),
    }


def snapshot_from_daily_bar(row: dict[str, Any], *, symbol: str) -> dict[str, Any] | None:
    """Snapshot from a completed-session daily bar. No depth, no deltas."""
    close = _number(row.get("close"))
    timestamp = _iso_utc(row.get("timestamp"))
    if close is None or timestamp is None:
        return None
    return {
        "instrument_token": _scalar(row.get("instrument_token")),
        "symbol": symbol,
        "exchange": _scalar(row.get("exchange")),
        "timestamp": timestamp,
        "ingested_at": None,
        "last_price": close,
        "last_quantity": None,
        "volume": _scalar(row.get("volume")),
        "average_price": None,
        "oi": _scalar(row.get("oi")),
        "total_buy_quantity": None,
        "total_sell_quantity": None,
        "best_bid_price": None,
        "best_bid_quantity": None,
        "best_ask_price": None,
        "best_ask_quantity": None,
        "bid_depth_5": None,
        "ask_depth_5": None,
        "spread": None,
        "mid_price": None,
        "depth_imbalance": None,
        "volume_delta": None,
        "oi_delta": None,
        "price_delta": None,
        "ohlc_open": _number(row.get("open")),
        "ohlc_high": _number(row.get("high")),
        "ohlc_low": _number(row.get("low")),
        "ohlc_close": close,
        "last_trade_time": None,
    }


class SymbolSnapshot:
    """Best available snapshot for one symbol plus the state that produced it."""

    __slots__ = ("symbol", "row", "previous", "state")

    def __init__(
        self,
        symbol: str,
        row: dict[str, Any] | None,
        previous: dict[str, Any] | None,
        state: DataState,
    ) -> None:
        self.symbol = symbol
        self.row = row
        self.previous = previous
        self.state = state

    @property
    def found(self) -> bool:
        return self.row is not None

    def attach(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["data_state"] = self.state.to_dict()
        return payload


class MarketDataPolicy:
    """get_best_available_data for the read layer. Read-only, no writes."""

    def __init__(
        self,
        settings: Settings,
        store: SQLiteStore,
        *,
        now_fn: Any = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._max_age_s = float(settings.analytics.live_quote_max_age_seconds)
        self._ttl_s = float(settings.analytics.last_session_cache_ttl_seconds)
        self._lock = threading.Lock()
        self._session_date: str | None = None
        self._session_date_at = 0.0
        self._rows: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]] = {}
        self._rows_at = 0.0
        self._day_rows: dict[
            tuple[str, int, bool],
            dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]],
        ] = {}
        self._day_rows_at = 0.0

    # ---- clock / session -------------------------------------------------

    def now(self) -> datetime:
        return self._now_fn()

    def market_state(self) -> str:
        return market_state(self.settings.session, now=self.now())

    def session_date(self) -> str | None:
        """Newest compacted date holding completed-session data."""
        with self._lock:
            if self._session_date is not None and (_time.monotonic() - self._session_date_at) < self._ttl_s:
                return self._session_date
        try:
            with DuckDBTickStore(self.settings) as duck:
                found = duck.latest_session_date()
        except Exception:
            found = None
        with self._lock:
            self._session_date = found
            self._session_date_at = _time.monotonic()
        return found

    # ---- snapshots -------------------------------------------------------

    def snapshot(self, symbol: str) -> SymbolSnapshot:
        name = str(symbol or "").strip()
        if not name:
            return SymbolSnapshot(name, None, None, self.no_data(reason="symbol_missing"))
        return self.snapshots([name])[name]

    def snapshots(self, symbols: Iterable[str]) -> dict[str, SymbolSnapshot]:
        names: list[str] = []
        for symbol in symbols:
            text = str(symbol or "").strip()
            if text and text not in names:
                names.append(text)
        if not names:
            return {}
        moment = self.now()
        clock = market_state(self.settings.session, now=moment)
        live = self._live_rows(names)

        needs_history: list[str] = []
        for name in names:
            row = live.get(name)
            stamp = None if row is None else row.get("timestamp")
            if clock == MARKET_OPEN and is_fresh(stamp, max_age_seconds=self._max_age_s, now=moment):
                continue
            needs_history.append(name)
        history = self._historical_rows(needs_history) if needs_history else {}

        out: dict[str, SymbolSnapshot] = {}
        for name in names:
            out[name] = self._decide(
                name,
                live.get(name),
                history.get(name, (None, None, None)),
                moment=moment,
            )
        return out

    def all_snapshots(
        self, *, limit: int, historical_limit: int | None = None
    ) -> dict[str, SymbolSnapshot]:
        """Whole-universe snapshots for ranking endpoints, capped at `limit`.

        Live path: latest_quotes only, no Parquet, first `limit` symbols.
        Historical path: one completed session, at most `historical_limit`
        symbols. Depth arrays stay in this query because aggressor and
        liquidity_events are part of the unusual-activity score; the
        last two ticks of a session often have a zero OI delta.
        """
        moment = self.now()
        clock = market_state(self.settings.session, now=moment)
        live = self.store.fetch_latest_quotes_map()
        if clock == MARKET_OPEN and any(
            is_fresh(row.get("timestamp"), max_age_seconds=self._max_age_s, now=moment)
            for row in live.values()
        ):
            names = list(live)[: int(limit)]
            return {
                name: self._decide(name, live.get(name), (None, None, None), moment=moment)
                for name in names
            }

        hist_limit = int(historical_limit) if historical_limit is not None else int(limit)
        hist_limit = max(hist_limit, 1)
        history = self._day_snapshots(limit=hist_limit, include_depth=True)
        names = list(history) or list(live)[:hist_limit]
        return {
            name: self._decide(name, live.get(name), history.get(name, (None, None, None)), moment=moment)
            for name in names[:hist_limit]
        }

    def overall_state(self) -> DataState:
        """Session-level state for overview/health, without a per-symbol read."""
        moment = self.now()
        clock = market_state(self.settings.session, now=moment)
        live_ts = self.store.latest_quote_max_timestamp()
        if clock == MARKET_OPEN and is_fresh(live_ts, max_age_seconds=self._max_age_s, now=moment):
            _choice, state = resolve(
                session=self.settings.session,
                live_timestamp=live_ts,
                max_age_seconds=self._max_age_s,
                now=moment,
            )
            return state
        hist_ts: Any = None
        hist_source: str | None = None
        for name in self._reference_symbols():
            row, _prev, source = self._historical_rows([name]).get(name, (None, None, None))
            if row is not None:
                hist_ts = row.get("timestamp")
                hist_source = source
                break
        _choice, state = resolve(
            session=self.settings.session,
            live_timestamp=live_ts,
            historical_timestamp=hist_ts,
            historical_source=hist_source,
            max_age_seconds=self._max_age_s,
            now=moment,
        )
        return state

    def merged_state(self, snapshots: Iterable[SymbolSnapshot]) -> DataState:
        fallback = no_data_state(
            self.settings.session,
            now=self.now(),
            reason="no_live_or_historical_observation",
        )
        return merge_states([snap.state for snap in snapshots], fallback=fallback)

    def no_data(self, *, reason: str) -> DataState:
        return no_data_state(self.settings.session, now=self.now(), reason=reason)

    # ---- internals -------------------------------------------------------

    def _historical_rank_names(self, names: Sequence[str], cap: int) -> list[str]:
        """Prefer index, watchlist, and F&O names so a bounded scan can fill `limit`."""
        wanted = {
            str(name).strip().upper()
            for name in list(self.settings.index_symbols)
            + list(self.settings.analytics.watchlist_default)
            if str(name).strip()
        }
        preferred: list[str] = []
        rest: list[str] = []
        for name in names:
            key = str(name).upper()
            if (
                key in wanted
                or key.startswith(("NIFTY", "BANKNIFTY"))
                or key.endswith(("CE", "PE", "FUT"))
            ):
                preferred.append(name)
            else:
                rest.append(name)
        return (preferred + rest)[: max(int(cap), 1)]

    def _reference_symbols(self) -> list[str]:
        names: list[str] = []
        for name in list(self.settings.index_symbols) + list(self.settings.analytics.watchlist_default):
            text = str(name or "").strip()
            if text and text not in names:
                names.append(text)
        return names

    def _live_rows(self, names: Sequence[str]) -> dict[str, dict[str, Any] | None]:
        if len(names) > _BULK_LIVE_LOOKUP_SYMBOLS:
            table = self.store.fetch_latest_quotes_map()
            folded = {key.casefold(): value for key, value in table.items()}
            return {name: folded.get(name.casefold()) for name in names}
        return {name: self.store.fetch_latest_quote(name) for name in names}

    def _decide(
        self,
        name: str,
        live_row: dict[str, Any] | None,
        history: tuple[dict[str, Any] | None, dict[str, Any] | None, str | None],
        *,
        moment: datetime,
    ) -> SymbolSnapshot:
        hist_row, hist_prev, hist_source = history
        choice, state = resolve(
            session=self.settings.session,
            live_timestamp=None if live_row is None else live_row.get("timestamp"),
            historical_timestamp=None if hist_row is None else hist_row.get("timestamp"),
            historical_source=hist_source,
            max_age_seconds=self._max_age_s,
            now=moment,
        )
        if choice == CHOICE_HISTORICAL:
            return SymbolSnapshot(name, hist_row, hist_prev, state)
        if choice == CHOICE_LIVE:
            return SymbolSnapshot(name, live_row, None, state)
        return SymbolSnapshot(name, None, None, state)

    def _expire_locked(self) -> None:
        now = _time.monotonic()
        if self._rows and (now - self._rows_at) >= self._ttl_s:
            self._rows.clear()
        if self._day_rows and (now - self._day_rows_at) >= self._ttl_s:
            self._day_rows.clear()

    def _historical_rows(
        self, names: Sequence[str]
    ) -> dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]]:
        date_str = self.session_date()
        if not date_str or not names:
            return {}
        out: dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]] = {}
        missing: list[str] = []
        with self._lock:
            self._expire_locked()
            for name in names:
                cached = self._rows.get((date_str, name))
                if cached is not None:
                    out[name] = cached
                else:
                    missing.append(name)
        if not missing:
            return out
        fetched = self._read_historical(date_str, missing)
        with self._lock:
            if not self._rows:
                self._rows_at = _time.monotonic()
            for name in missing:
                value = fetched.get(name, (None, None, None))
                self._rows[(date_str, name)] = value
                out[name] = value
        return out

    def _read_historical(
        self, date_str: str, names: Sequence[str]
    ) -> dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]]:
        try:
            with DuckDBTickStore(self.settings) as duck:
                frame = duck.read_last_rows(date_str, list(names), per_symbol=2)
                found = self._snapshots_from_frame(frame)
                for name in names:
                    if name in found:
                        continue
                    daily = duck.read_daily_row(name, date_str)
                    if daily is None:
                        continue
                    row = snapshot_from_daily_bar(daily, symbol=name)
                    if row is not None:
                        found[name] = (row, None, SOURCE_COMPACTED_DAILY)
                return found
        except Exception:
            return {}

    def _day_snapshots(
        self, *, limit: int, include_depth: bool = True
    ) -> dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]]:
        date_str = self.session_date()
        if not date_str:
            return {}
        cap = max(int(limit), 1)
        cache_key = (date_str, cap, bool(include_depth))
        with self._lock:
            self._expire_locked()
            cached = self._day_rows.get(cache_key)
            if cached is not None:
                return cached
        try:
            with DuckDBTickStore(self.settings) as duck:
                names = self._historical_rank_names(duck.list_tick_symbols(date_str), cap)
                frame = duck.read_last_rows(
                    date_str, names, per_symbol=2, include_depth=include_depth
                )
                found = self._snapshots_from_frame(frame)
        except Exception:
            found = {}
        with self._lock:
            if not self._day_rows:
                self._day_rows_at = _time.monotonic()
            self._day_rows[cache_key] = found
        return found

    @staticmethod
    def _snapshots_from_frame(
        frame: pd.DataFrame,
    ) -> dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]]:
        out: dict[str, tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]] = {}
        if frame is None or frame.empty or "symbol" not in frame.columns:
            return out
        records = frame.to_dict(orient="records")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            symbol = _scalar(record.get("symbol"))
            if not symbol:
                continue
            grouped.setdefault(str(symbol), []).append(record)
        for symbol, rows in grouped.items():
            rows.sort(key=lambda item: str(_iso_utc(item.get("timestamp")) or ""), reverse=True)
            latest = rows[0]
            prior = rows[1] if len(rows) > 1 else None
            snapshot = snapshot_from_observation(latest, prior)
            if snapshot is None:
                continue
            previous = snapshot_from_observation(prior) if prior is not None else None
            out[symbol] = (snapshot, previous, SOURCE_COMPACTED_TICKS)
        return out


__all__ = [
    "MarketDataPolicy",
    "SymbolSnapshot",
    "snapshot_from_daily_bar",
    "snapshot_from_observation",
    "STATUS_NO_DATA",
]
