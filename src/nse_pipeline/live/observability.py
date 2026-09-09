"""Process-wide Phase 4 counters. No payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass
class StreamStats:
    current_connections: int = 0
    peak_connections: int = 0
    rejected_connections: int = 0
    last_seq: int = 0
    last_poll_ms: float | None = None
    last_poll_at: str | None = None
    live_source_age_ms: float | None = None
    last_generation: int = 0
    static_tokens: int = 0
    dynamic_tokens: int = 0
    rejected_focus: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "current_connections": self.current_connections,
            "peak_connections": self.peak_connections,
            "rejected_connections": self.rejected_connections,
            "last_emitted_sequence": self.last_seq,
            "live_source_poll_latency_ms": self.last_poll_ms,
            "last_successful_poll": self.last_poll_at,
            "live_source_age_ms": self.live_source_age_ms,
            "ingest_generation": self.last_generation,
            "static_token_count": self.static_tokens,
            "dynamic_token_count": self.dynamic_tokens,
            "rejected_focus_requests": self.rejected_focus,
        }


_LOCK = Lock()
STATS = StreamStats()


def record_poll(*, latency_ms: float, at: str, age_ms: float | None, seq: int, generation: int) -> None:
    with _LOCK:
        STATS.last_poll_ms = round(latency_ms, 3)
        STATS.last_poll_at = at
        STATS.live_source_age_ms = None if age_ms is None else round(age_ms, 1)
        STATS.last_seq = int(seq)
        STATS.last_generation = int(generation)


def record_connections(current: int, *, rejected: bool = False) -> None:
    with _LOCK:
        STATS.current_connections = current
        STATS.peak_connections = max(STATS.peak_connections, current)
        if rejected:
            STATS.rejected_connections += 1


def record_subscription_counts(*, static_n: int, dynamic_n: int, rejected_focus: int = 0) -> None:
    with _LOCK:
        STATS.static_tokens = int(static_n)
        STATS.dynamic_tokens = int(dynamic_n)
        STATS.rejected_focus += int(rejected_focus)


def stats_dict() -> dict[str, Any]:
    with _LOCK:
        return STATS.snapshot()
