"""SSE admission and per-connection generator. One shared LiveSource."""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator

from nse_pipeline.config import Settings
from nse_pipeline.live.frames import (
    format_sse,
    heartbeat_payload,
    hello_payload,
    new_connection_id,
    parse_groups,
    parse_tokens,
    resync_payload,
)
from nse_pipeline.live.observability import record_connections
from nse_pipeline.live.source import SqlitePollSource, Subscriber


class StreamAdmissionError(Exception):
    def __init__(self, status: int, reason: str, *, retry_after: int | None = None) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason
        self.retry_after = retry_after


class StreamRegistry:
    def __init__(self, settings: Settings, source: SqlitePollSource) -> None:
        self.settings = settings
        self.source = source
        self._lock = asyncio.Lock()
        self._current = 0

    @property
    def current(self) -> int:
        return self._current

    async def acquire(self) -> None:
        async with self._lock:
            cap = int(self.settings.stream.max_connections)
            if self._current >= cap:
                record_connections(self._current, rejected=True)
                raise StreamAdmissionError(503, "stream_capacity_exceeded", retry_after=5)
            self._current += 1
            record_connections(self._current)

    async def release(self) -> None:
        async with self._lock:
            self._current = max(0, self._current - 1)
            record_connections(self._current)

    def validate_tokens(self, raw: str | None) -> tuple[list[int], list[dict[str, Any]]]:
        try:
            tokens = parse_tokens(raw)
        except ValueError as exc:
            raise StreamAdmissionError(400, str(exc)) from exc
        limit = int(self.settings.stream.max_instruments_per_connection)
        if len(tokens) > limit:
            raise StreamAdmissionError(400, "instrument_limit_exceeded")
        if not tokens:
            raise StreamAdmissionError(400, "tokens_required")
        return tokens, []


async def stream_events(
    *,
    registry: StreamRegistry,
    source: SqlitePollSource,
    tokens: list[int],
    groups: list[str],
    last_event_id: str | None,
    session: dict[str, Any],
    rejected: list[dict[str, Any]],
) -> AsyncIterator[bytes]:
    connection_id = new_connection_id()
    heartbeat_ms = int(registry.settings.stream.heartbeat_ms)
    max_instruments = int(registry.settings.stream.max_instruments_per_connection)
    hello = hello_payload(
        connection_id=connection_id,
        session=session,
        heartbeat_ms=heartbeat_ms,
        max_instruments=max_instruments,
        subscribed=tokens,
        rejected=rejected,
        schema_version=source.schema_version,
    )
    yield format_sse("hello", hello).encode("utf-8")

    if last_event_id:
        try:
            last_seq = int(str(last_event_id).strip())
        except ValueError:
            last_seq = -1
        if last_seq == source.seq:
            pass
        elif 0 <= last_seq < source.seq:
            yield format_sse(
                "resync",
                resync_payload(reason="unavailable_continuity", from_seq=source.seq),
            ).encode("utf-8")
        else:
            yield format_sse(
                "resync",
                resync_payload(reason="stream_process_restart", from_seq=source.seq),
            ).encode("utf-8")

    sub: Subscriber | None = None
    try:
        sub = await source.subscribe(set(tokens), groups)
        timeout = max(heartbeat_ms / 1000.0, 1.0)
        last_beat = time.monotonic()
        while True:
            remain = max(timeout - (time.monotonic() - last_beat), 0.05)
            try:
                item = await asyncio.wait_for(sub.queue.get(), timeout=remain)
            except asyncio.TimeoutError:
                yield format_sse(
                    "heartbeat",
                    heartbeat_payload(subscribed=len(tokens), lag_ms=source.last_lag_ms),
                ).encode("utf-8")
                last_beat = time.monotonic()
                continue
            yield format_sse(item["event"], item["data"], event_id=item.get("id")).encode("utf-8")
            if time.monotonic() - last_beat >= timeout:
                yield format_sse(
                    "heartbeat",
                    heartbeat_payload(subscribed=len(tokens), lag_ms=source.last_lag_ms),
                ).encode("utf-8")
                last_beat = time.monotonic()
    finally:
        if sub is not None:
            await source.unsubscribe(sub)
        await registry.release()
