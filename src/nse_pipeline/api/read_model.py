"""SQLite-backed UiReadModel. Read path only — no engine, no Kite, no joblib."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

from nse_pipeline.config import Settings
from nse_pipeline.market.cache_health import instrument_cache_health, lookup_instrument_meta
from nse_pipeline.market.latest import public_quote
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
        row = self.store.fetch_latest_quote(wanted)
        cache = self._load_instrument_cache()
        meta = lookup_instrument_meta(cache, wanted) if cache else None
        if row is None:
            return self._envelope(found=False, quote=None)
        return self._envelope(found=True, quote=public_quote(row, meta=meta))

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
