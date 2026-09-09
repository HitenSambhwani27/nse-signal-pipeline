"""One shared latest_quotes poll. Client count does not multiply queries."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

from nse_pipeline.config import Settings
from nse_pipeline.live.frames import (
    STREAM_SCHEMA_VERSION,
    _ABBREV,
    changed_fields,
    fields_for_groups,
    tick_frame,
    utc_now,
)
from nse_pipeline.live.observability import record_poll
from nse_pipeline.market.calendar import build_session_view, calendar_lookup, load_calendar_day
from nse_pipeline.market.reference import resolve_reference
from nse_pipeline.market.timestamps import AMBIGUOUS_UTC_HOUR, canonical_observation
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _decode_levels(value: Any) -> Any:
    if value is None or isinstance(value, list):
        return value
    if isinstance(value, str) and value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _lag_ms(rows: list[dict[str, Any]]) -> float | None:
    newest: datetime | None = None
    now = datetime.now(timezone.utc)
    for row in rows:
        observed, repair = canonical_observation(row.get("timestamp"), ingested_at=row.get("ingested_at"))
        if observed is None or repair == AMBIGUOUS_UTC_HOUR:
            continue
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        if newest is None or observed > newest:
            newest = observed
    if newest is None:
        return None
    return max((now - newest.astimezone(timezone.utc)).total_seconds() * 1000.0, 0.0)


class Subscriber:
    def __init__(self, tokens: set[int], groups: list[str]) -> None:
        self.tokens = set(tokens)
        self.groups = list(groups)
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self.fields = fields_for_groups(groups)


class SqlitePollSource:
    """ADR-2 LiveSource: one 1 Hz indexed latest_quotes SELECT, fan-out diffs."""

    def __init__(self, settings: Settings, store: SQLiteStore) -> None:
        self.settings = settings
        self.store = store
        self.seq = 0
        self.generation: int | None = None
        self.schema_version = STREAM_SCHEMA_VERSION
        self._subscribers: list[Subscriber] = []
        self._last: dict[int, dict[str, Any]] = {}
        self._session: dict[str, Any] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self.last_lag_ms: float | None = None

    async def subscribe(self, tokens: set[int], groups: list[str]) -> Subscriber:
        sub = Subscriber(tokens, groups)
        async with self._lock:
            self._subscribers.append(sub)
        return sub

    async def unsubscribe(self, sub: Subscriber) -> None:
        async with self._lock:
            if sub in self._subscribers:
                self._subscribers.remove(sub)

    def union_tokens(self) -> set[int]:
        tokens: set[int] = set()
        for sub in self._subscribers:
            tokens.update(sub.tokens)
        return tokens

    async def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        interval = float(self.settings.stream.poll_interval_seconds)
        while not self._stop.is_set():
            started = time.perf_counter()
            try:
                await self._poll_once()
            except Exception:
                # Keep the shared loop alive; next tick retries.
                pass
            elapsed = time.perf_counter() - started
            remain = max(interval - elapsed, 0.05)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=remain)
            except asyncio.TimeoutError:
                continue

    async def _poll_once(self) -> None:
        tokens = self.union_tokens()
        t0 = time.perf_counter()
        if self._session is None:
            self._session = self._session_snapshot()
        generation, rows, refs = await asyncio.to_thread(self._read, tokens)
        lag = _lag_ms(rows)
        self.last_lag_ms = lag
        events = self._diff(generation, rows, refs)
        record_poll(
            latency_ms=(time.perf_counter() - t0) * 1000.0,
            at=utc_now(),
            age_ms=lag,
            seq=self.seq,
            generation=generation,
        )
        if events:
            self._fanout(events)

    def _read(self, tokens: set[int]) -> tuple[int, list[dict[str, Any]], dict[int, dict[str, Any]]]:
        try:
            generation = self.store.ingest_generation(read_only=True)
            rows = self.store.fetch_latest_quotes_by_tokens(tokens, read_only=True) if tokens else []
        except Exception:
            generation = self.store.ingest_generation(read_only=False)
            rows = self.store.fetch_latest_quotes_by_tokens(tokens, read_only=False) if tokens else []
        session_date = (self._session or {}).get("session_date")
        refs: dict[int, dict[str, Any]] = {}
        if session_date and rows:
            try:
                refs = self.store.fetch_session_reference_map(
                    str(session_date),
                    [int(r["instrument_token"]) for r in rows],
                )
            except Exception:
                refs = {}
        return generation, rows, refs

    def _session_snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        day = load_calendar_day(self.store, self.settings.session, now=now)
        return build_session_view(
            self.settings.session,
            now=now,
            day=day,
            lookup=calendar_lookup(self.store, self.settings.session),
        )

    def _diff(
        self,
        generation: int,
        rows: list[dict[str, Any]],
        refs: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        if self.generation is None:
            self.generation = generation
        elif generation != self.generation:
            events.append(
                {
                    "event": "resync",
                    "reason": "ingestion_restart",
                    "from_seq": self.seq,
                }
            )
            self.generation = generation
            self._last.clear()
        session = self._session_snapshot()
        if self._session is None:
            self._session = session
        else:
            keys = ("market_state", "data_status", "session_date", "coverage")
            if any(session.get(k) != self._session.get(k) for k in keys):
                events.append({"event": "session", "payload": session})
                self._session = session
        by_token = {int(r["instrument_token"]): self._normalize_row(r, refs) for r in rows}
        wanted_fields: set[str] = set()
        for sub in self._subscribers:
            wanted_fields.update(sub.fields)
        field_list = sorted(wanted_fields)
        for token, current in by_token.items():
            prev = self._last.get(token)
            delta = changed_fields(prev, current, field_list)
            self._last[token] = current
            if not delta:
                continue
            self.seq += 1
            events.append(
                {
                    "event": "tick",
                    "token": token,
                    "seq": self.seq,
                    "frame": tick_frame(
                        token=token,
                        ts=current.get("timestamp"),
                        seq=self.seq,
                        changed=delta,
                    ),
                }
            )
        return events

    def _reference_map(self, rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        tokens = [int(r["instrument_token"]) for r in rows]
        dates = {str(r.get("timestamp") or "")[:10] for r in rows}
        # session_reference is keyed by session_date (IST calendar), not UTC prefix.
        session_date = (self._session or {}).get("session_date")
        if session_date:
            try:
                return self.store.fetch_session_reference_map(str(session_date), tokens)
            except Exception:
                return {}
        _ = dates
        return {}

    def _normalize_row(self, row: dict[str, Any], refs: dict[int, dict[str, Any]]) -> dict[str, Any]:
        token = int(row["instrument_token"])
        stored = refs.get(token)
        canonical = resolve_reference(row, stored=stored)
        return {
            "instrument_token": token,
            "timestamp": row.get("timestamp"),
            "ingested_at": row.get("ingested_at"),
            "last_price": row.get("last_price"),
            "volume": row.get("volume"),
            "volume_delta": row.get("volume_delta"),
            "oi": row.get("oi"),
            "oi_delta": row.get("oi_delta"),
            "best_bid_price": row.get("best_bid_price"),
            "best_ask_price": row.get("best_ask_price"),
            "best_bid_quantity": row.get("best_bid_quantity"),
            "best_ask_quantity": row.get("best_ask_quantity"),
            "bid_levels": _decode_levels(row.get("bid_levels")),
            "ask_levels": _decode_levels(row.get("ask_levels")),
            "spread": row.get("spread"),
            "depth_imbalance": row.get("depth_imbalance"),
            "change_absolute": canonical.get("change_absolute"),
            "change_percent": canonical.get("change_percent"),
            "reference_price": canonical.get("reference_price"),
            "reference_type": canonical.get("reference_type"),
        }

    def _fanout(self, events: list[dict[str, Any]]) -> None:
        for event in events:
            kind = event["event"]
            for sub in list(self._subscribers):
                if kind == "tick" and event["token"] not in sub.tokens:
                    continue
                if kind == "tick":
                    allowed = set(sub.fields)
                    frame = dict(event["frame"])
                    # Drop abbreviated fields the subscriber did not request.
                    keep = {"t", "ts", "seq"}
                    abbrev_allowed = {_ABBREV.get(name, name) for name in allowed}
                    trimmed = {k: v for k, v in frame.items() if k in keep or k in abbrev_allowed}
                    if set(trimmed) <= keep:
                        continue
                    payload = {"event": "tick", "id": event["seq"], "data": trimmed}
                elif kind == "session":
                    payload = {"event": "session", "id": None, "data": event["payload"]}
                else:
                    payload = {
                        "event": "resync",
                        "id": None,
                        "data": {"reason": event["reason"], "from_seq": event.get("from_seq")},
                    }
                try:
                    sub.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    try:
                        sub.queue.put_nowait(
                            {
                                "event": "resync",
                                "id": None,
                                "data": {"reason": "stream_overflow", "from_seq": self.seq},
                            }
                        )
                    except asyncio.QueueFull:
                        pass
