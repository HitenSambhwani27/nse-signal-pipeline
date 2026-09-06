"""SQLite-backed UiReadModel. Read path only — no engine, no Kite, no joblib."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

from nse_pipeline.config import Settings
from nse_pipeline.market.assemble import (
    assemble_charts,
    assemble_cross_market,
    assemble_futures,
    assemble_market_activity,
    assemble_option_chain,
    assemble_quote,
    assemble_unusual,
)
from nse_pipeline.market.cache_health import instrument_cache_health
from nse_pipeline.signals.maturity import (
    CLASS_FROM_TRACK,
    maturity_snapshot,
    signal_public_view,
)
from nse_pipeline.storage.sqlite_store import SQLiteStore

MATURITY_CACHE_TTL_S = 30.0


def _as_of() -> str:
    return datetime.now(timezone.utc).isoformat()


def _options_key(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "").upper()
    und = "BANKNIFTY" if symbol.startswith("BANKNIFTY") else "NIFTY"
    return f"options_{und.lower()}"


def view_from_snapshot(snapshot: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    track = str(row.get("track") or "")
    class_key = CLASS_FROM_TRACK.get(track, "equity")
    if class_key != "options":
        return snapshot[class_key]
    return snapshot.get(_options_key(row), snapshot["options_nifty"])


class SqliteUiReadModel:
    def __init__(self, settings: Settings, *, cache_ttl_s: float = MATURITY_CACHE_TTL_S) -> None:
        self.settings = settings
        self.store = SQLiteStore(settings.paths.sqlite_db)
        self._cache_ttl_s = cache_ttl_s
        self._cached: dict[str, Any] | None = None
        self._cached_at = 0.0

    def _has_model(self) -> dict[str, bool]:
        return self.store.harness_passed_classes()

    def _maturity(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._cached is None or (now - self._cached_at) >= self._cache_ttl_s:
            self._cached = maturity_snapshot(self.settings, has_model=self._has_model())
            self._cached_at = now
        return self._cached

    def _envelope(self, **payload: Any) -> dict[str, Any]:
        return {"maturity": self._maturity(), "as_of": _as_of(), **payload}

    def overview(self) -> dict[str, Any]:
        health = self.store.pipeline_health()
        return self._envelope(
            last_ingest=health.get("last_ingestion_meta"),
            last_feature=health.get("last_feature_trade_date"),
            last_signal=health.get("last_live_signal_trade_date"),
            processing_status=health.get("processing_status"),
            signal_counts=self.store.signal_counts(),
        )

    def maturity(self) -> dict[str, Any]:
        return self._envelope()

    def signals(self, *, limit: int = 200) -> dict[str, Any]:
        snap = self._maturity()
        rows = self.store.fetch_public_signal_logs(limit=limit)
        public = [signal_public_view(row, view_from_snapshot(snap, row)) for row in rows]
        return self._envelope(signals=public)

    def account(self, *, fill_limit: int = 100) -> dict[str, Any]:
        return self._envelope(
            positions=self.store.latest_json_row("positions_snapshot"),
            margins=self.store.latest_json_row("margin_snapshot"),
            fills=self.store.fetch_public_fills(limit=fill_limit),
        )

    def decisions(self, *, limit: int = 100) -> dict[str, Any]:
        return self._envelope(
            decisions=self.store.fetch_decisions(limit=limit),
            class_counts=self.store.outcome_class_counts(),
        )

    def health(self) -> dict[str, Any]:
        blob = self.store.pipeline_health()
        blob.setdefault("api", "ok")
        blob.setdefault("database", "ok")
        blob.setdefault("processing_lag", "unknown")
        blob["instrument_cache"] = self._instrument_cache_health()
        return self._envelope(health=blob)

    def quote(self, symbol: str) -> dict[str, Any]:
        wanted = unquote(symbol).strip()
        cache = self._load_instrument_cache()
        dto = assemble_quote(self.store, cache, wanted)
        if dto is None:
            return self._envelope(found=False, quote=None)
        return self._envelope(found=True, quote=dto)

    def option_chain(self, underlying: str, expiry: str | None = None) -> dict[str, Any]:
        cache = self._require_cache()
        if cache is None:
            return self._envelope(found=False, chain=None, reason="cache_file_missing")
        chain = assemble_option_chain(
            self.settings, self.store, cache, unquote(underlying).strip(), expiry
        )
        return self._envelope(found=chain.get("found"), chain=chain)

    def option_oi(self, underlying: str, expiry: str | None = None) -> dict[str, Any]:
        payload = self.option_chain(underlying, expiry)
        chain = payload.get("chain") or {}
        return self._envelope(
            found=payload.get("found"),
            oi={
                "underlying": chain.get("underlying"),
                "expiry": chain.get("expiry"),
                "atm": chain.get("atm"),
                "pcr_oi": chain.get("pcr_oi"),
                "pcr_volume": chain.get("pcr_volume"),
                "pcr_near_atm_oi": chain.get("pcr_near_atm_oi"),
                "total_ce_oi": chain.get("total_ce_oi"),
                "total_pe_oi": chain.get("total_pe_oi"),
                "highest_ce_oi": chain.get("highest_ce_oi"),
                "highest_pe_oi": chain.get("highest_pe_oi"),
                "largest_ce_oi_increase": chain.get("largest_ce_oi_increase"),
                "largest_pe_oi_increase": chain.get("largest_pe_oi_increase"),
                "largest_ce_oi_decrease": chain.get("largest_ce_oi_decrease"),
                "largest_pe_oi_decrease": chain.get("largest_pe_oi_decrease"),
                "max_pain": chain.get("max_pain"),
                "chain_completeness": chain.get("chain_completeness"),
                "chain_status": chain.get("chain_status"),
                "eligible_contract_count": chain.get("eligible_contract_count"),
                "selected_contract_count": chain.get("selected_contract_count"),
                "missing_contract_count": chain.get("missing_contract_count"),
                "quoted_contract_count": chain.get("quoted_contract_count"),
                "quote_coverage": chain.get("quote_coverage"),
                "quote_status": chain.get("quote_status"),
                "truncated": chain.get("truncated"),
                "multi_strike": chain.get("multi_strike"),
            },
        )

    def option_activity(self, underlying: str, expiry: str | None = None) -> dict[str, Any]:
        payload = self.option_chain(underlying, expiry)
        chain = payload.get("chain") or {}
        cache = self._load_instrument_cache()
        rows = []
        for strike in chain.get("strikes") or []:
            for side in ("ce", "pe"):
                block = strike.get(side) or {}
                symbol = block.get("symbol")
                if not symbol:
                    continue
                bundle = assemble_market_activity(self.settings, self.store, cache, symbol)
                if bundle:
                    rows.append(
                        {
                            "strike": strike.get("strike"),
                            "side": side.upper(),
                            "symbol": symbol,
                            "unusual": bundle["unusual"],
                            "volume_level": bundle["activity"].get("volume_level"),
                            "large_trade": bundle["activity"].get("large_trade"),
                        }
                    )
        return self._envelope(found=payload.get("found"), activity=rows, expiry=chain.get("expiry"))

    def futures_book(self, underlying: str) -> dict[str, Any]:
        cache = self._require_cache()
        if cache is None:
            return self._envelope(found=False, futures=None, reason="cache_file_missing")
        book = assemble_futures(
            self.settings, self.store, cache, unquote(underlying).strip()
        )
        return self._envelope(found=book.get("found"), futures=book)

    def market_activity(self, symbol: str) -> dict[str, Any]:
        cache = self._load_instrument_cache()
        bundle = assemble_market_activity(
            self.settings, self.store, cache, unquote(symbol).strip()
        )
        if bundle is None:
            return self._envelope(found=False, activity=None)
        return self._envelope(found=True, **bundle)

    def unusual_activity(self, *, limit: int = 50) -> dict[str, Any]:
        cache = self._load_instrument_cache()
        rows = assemble_unusual(self.settings, self.store, cache, limit=limit)
        return self._envelope(unusual_activity=rows)

    def charts(self, symbol: str, *, interval: str | None = None) -> dict[str, Any]:
        series = assemble_charts(
            self.settings, self.store, unquote(symbol).strip(), interval=interval
        )
        found = bool(series.get("points") or series.get("candles"))
        return self._envelope(found=found, chart=series)

    def watchlists(self) -> dict[str, Any]:
        self.store.ensure_default_watchlist(
            "default", list(self.settings.analytics.watchlist_default)
        )
        return self._envelope(watchlists=self.store.list_watchlists())

    def watchlist_quotes(self) -> dict[str, Any]:
        self.store.ensure_default_watchlist(
            "default", list(self.settings.analytics.watchlist_default)
        )
        cache = self._load_instrument_cache()
        lists = self.store.list_watchlists()
        quotes = []
        seen: set[str] = set()
        for item in lists:
            for symbol in item.get("symbols") or []:
                if symbol in seen:
                    continue
                seen.add(symbol)
                dto = assemble_quote(self.store, cache, symbol)
                quotes.append({"symbol": symbol, "quote": dto, "found": dto is not None})
        return self._envelope(quotes=quotes)

    def cross_market(self, underlying: str) -> dict[str, Any]:
        cache = self._require_cache()
        if cache is None:
            return self._envelope(found=False, cross_market=None, reason="cache_file_missing")
        payload = assemble_cross_market(
            self.settings, self.store, cache, unquote(underlying).strip()
        )
        return self._envelope(found=True, cross_market=payload)

    def _require_cache(self) -> dict[str, Any] | None:
        return self._load_instrument_cache()

    def _load_instrument_cache(self) -> dict[str, Any] | None:
        path = self.settings.paths.instruments_cache
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _instrument_cache_health(self) -> dict[str, Any]:
        cache = self._load_instrument_cache()
        if cache is None:
            return {"status": "unknown", "reason": "cache_file_missing"}
        return instrument_cache_health(cache)
