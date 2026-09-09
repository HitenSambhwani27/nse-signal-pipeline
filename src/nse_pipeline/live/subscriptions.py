"""Subscription manager inside nse-ingest. Never opens a second Kite WebSocket."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from nse_pipeline.market.cache_health import lookup_instrument_meta
from nse_pipeline.config import Settings
from nse_pipeline.live.observability import record_subscription_counts
from nse_pipeline.storage.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

QUOTE_CAPABILITIES = frozenset({"price", "volume", "chart", "index", "session", "quality"})
FULL_CAPABILITIES = frozenset({"depth", "oi", "trades", "workspace"})
KNOWN_CAPABILITIES = QUOTE_CAPABILITIES | FULL_CAPABILITIES | frozenset({"full", "quote"})


def resolve_mode(capabilities: Iterable[str]) -> str:
    """Panels request capabilities. This chooses the minimum Kite mode."""
    caps = {str(c).strip().lower() for c in capabilities if str(c).strip()}
    if "full" in caps or caps & FULL_CAPABILITIES:
        return "full"
    return "quote"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class SubscriptionManager:
    def __init__(
        self,
        settings: Settings,
        store: SQLiteStore,
        *,
        static_full: list[int],
        static_quote: list[int],
        instrument_cache: dict[str, Any] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.static_full = {int(t) for t in static_full}
        self.static_quote = {int(t) for t in static_quote}
        self.static_all = self.static_full | self.static_quote
        self.cache = instrument_cache or {}
        self.dynamic_full: set[int] = set()
        self.dynamic_quote: set[int] = set()
        self._applied_full = set(self.static_full)
        self._applied_quote = set(self.static_quote)
        self.rejected_focus = 0

    def validate_request(
        self,
        *,
        token: int | None,
        symbol: str | None,
        requester: str,
        capabilities: list[str],
        action: str,
        ttl_seconds: int | None,
    ) -> dict[str, Any]:
        if action not in {"subscribe", "release"}:
            raise ValueError("unsupported_action")
        if not requester or not str(requester).strip():
            raise ValueError("requester_required")
        unknown = [c for c in capabilities if str(c).strip().lower() not in KNOWN_CAPABILITIES]
        if unknown:
            raise ValueError("unsupported_capability")
        resolved_token = token
        meta = None
        if symbol:
            meta = lookup_instrument_meta(self.cache, symbol)
            if meta and resolved_token is None:
                resolved_token = meta.get("instrument_token")
        if resolved_token is None:
            raise ValueError("unknown_instrument")
        try:
            resolved_token = int(resolved_token)
        except (TypeError, ValueError) as exc:
            raise ValueError("unknown_instrument") from exc
        if resolved_token <= 0:
            raise ValueError("unknown_instrument")
        ttl = int(ttl_seconds or self.settings.stream.dynamic_ttl_seconds)
        if ttl <= 0:
            raise ValueError("invalid_ttl")
        return {
            "token": resolved_token,
            "symbol": symbol,
            "requester": str(requester).strip(),
            "capabilities": [str(c).strip().lower() for c in capabilities],
            "action": action,
            "ttl_seconds": ttl,
            "mode": resolve_mode(capabilities),
        }

    def enqueue(self, request: dict[str, Any]) -> dict[str, Any]:
        now = _now()
        expires = now + timedelta(seconds=int(request["ttl_seconds"]))
        if request["action"] == "release":
            self.store.delete_subscription_request(int(request["token"]), request["requester"])
            return {"status": "released", **request}
        self.store.upsert_subscription_request(
            {
                "token": int(request["token"]),
                "requester": request["requester"],
                "action": "subscribe",
                "capabilities_json": json.dumps(request["capabilities"]),
                "ttl_seconds": int(request["ttl_seconds"]),
                "requested_at": now.isoformat(),
                "expires_at": expires.isoformat(),
                "status": "pending",
                "reason": None,
            }
        )
        return {"status": "pending", "expires_at": expires.isoformat(), **request}

    def _active_leases(self) -> list[dict[str, Any]]:
        now = _now()
        active: list[dict[str, Any]] = []
        for row in self.store.fetch_subscription_requests(active_only=True):
            expires = _parse_iso(row.get("expires_at"))
            if expires is not None and expires <= now:
                self.store.update_subscription_request(
                    int(row["token"]),
                    str(row["requester"]),
                    status="expired",
                    reason="ttl",
                )
                continue
            if str(row.get("action")) == "release":
                continue
            active.append(row)
        return active

    def _desired(self, leases: list[dict[str, Any]]) -> tuple[set[int], set[int], dict[int, datetime]]:
        refs: dict[int, list[dict[str, Any]]] = {}
        for row in leases:
            refs.setdefault(int(row["token"]), []).append(row)
        dynamic_full: set[int] = set()
        dynamic_quote: set[int] = set()
        last_used: dict[int, datetime] = {}
        for token, holders in refs.items():
            if token in self.static_all:
                continue
            modes = []
            latest = datetime.fromtimestamp(0, tz=timezone.utc)
            for holder in holders:
                caps = []
                raw = holder.get("capabilities_json")
                if raw:
                    try:
                        caps = list(json.loads(raw))
                    except Exception:
                        caps = []
                modes.append(resolve_mode(caps))
                requested = _parse_iso(holder.get("requested_at")) or latest
                if requested > latest:
                    latest = requested
            last_used[token] = latest
            if any(mode == "full" for mode in modes):
                dynamic_full.add(token)
            else:
                dynamic_quote.add(token)
        return dynamic_full, dynamic_quote, last_used

    def _evict_to_budget(
        self,
        dynamic_full: set[int],
        dynamic_quote: set[int],
        last_used: dict[int, datetime],
    ) -> tuple[set[int], set[int]]:
        budget = int(self.settings.broker_limits.max_total_subscriptions)
        static_n = len(self.static_all)
        dynamic = dynamic_full | dynamic_quote
        overflow = static_n + len(dynamic) - budget
        if overflow <= 0:
            return dynamic_full, dynamic_quote
        lru = sorted(dynamic, key=lambda token: last_used.get(token, datetime.fromtimestamp(0, tz=timezone.utc)))
        evicted = 0
        for token in lru:
            if evicted >= overflow:
                break
            if token in self.static_all:
                continue
            dynamic_full.discard(token)
            dynamic_quote.discard(token)
            evicted += 1
            self.rejected_focus += 1
            self.store.log_ingestion_event(
                event_type="subscription_evict",
                message="lru_dynamic_eviction",
                details={"token": token, "reason": "token_budget"},
            )
            for row in self.store.fetch_subscription_requests(active_only=True):
                if int(row["token"]) == token:
                    self.store.update_subscription_request(
                        token, str(row["requester"]), status="evicted", reason="token_budget"
                    )
        remaining = static_n + len(dynamic_full | dynamic_quote)
        if remaining > budget:
            self.rejected_focus += 1
        return dynamic_full, dynamic_quote

    def reconcile(self, ticker: Any | None) -> dict[str, Any]:
        leases = self._active_leases()
        dynamic_full, dynamic_quote, last_used = self._desired(leases)
        dynamic_full, dynamic_quote = self._evict_to_budget(dynamic_full, dynamic_quote, last_used)
        self.dynamic_full = dynamic_full
        self.dynamic_quote = dynamic_quote
        desired_full = set(self.static_full) | dynamic_full
        desired_quote = (set(self.static_quote) | dynamic_quote) - desired_full
        desired_all = desired_full | desired_quote
        current_all = self._applied_full | self._applied_quote
        to_add = sorted(desired_all - current_all)
        to_drop = sorted(current_all - desired_all - self.static_all)
        mode_full = sorted(desired_full - self._applied_full)
        mode_quote = sorted(desired_quote - self._applied_quote)
        batch = int(self.settings.stream.mode_change_batch_size)
        if ticker is not None:
            if to_add:
                ticker.subscribe(to_add)
            if to_drop:
                try:
                    ticker.unsubscribe(to_drop)
                except Exception:
                    logger.exception("unsubscribe failed")
            for i in range(0, len(mode_full), batch):
                chunk = mode_full[i : i + batch]
                ticker.set_mode(getattr(ticker, "MODE_FULL", "full"), chunk)
            for i in range(0, len(mode_quote), batch):
                chunk = mode_quote[i : i + batch]
                ticker.set_mode(getattr(ticker, "MODE_QUOTE", "quote"), chunk)
        if to_add or to_drop or mode_full or mode_quote:
            self.store.log_ingestion_event(
                event_type="subscription_reconcile",
                message="kite_subscription_set_changed",
                details={
                    "added": to_add,
                    "dropped": to_drop,
                    "mode_full": mode_full,
                    "mode_quote": mode_quote,
                    "static": len(self.static_all),
                    "dynamic": len(dynamic_full | dynamic_quote),
                },
            )
        self._applied_full = desired_full
        self._applied_quote = desired_quote
        for row in leases:
            if int(row["token"]) in desired_all and str(row.get("status")) == "pending":
                self.store.update_subscription_request(
                    int(row["token"]), str(row["requester"]), status="applied", reason=None
                )
        record_subscription_counts(
            static_n=len(self.static_all),
            dynamic_n=len(dynamic_full | dynamic_quote),
        )
        return {
            "static": len(self.static_all),
            "dynamic": len(dynamic_full | dynamic_quote),
            "added": to_add,
            "dropped": to_drop,
        }
