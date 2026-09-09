"""SSE frame encoding. Abbreviated names stay inside this adapter only."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

STREAM_SCHEMA_VERSION = "1"

GROUPS = ("price", "volume", "oi", "depth", "session", "quality")

GROUP_FIELDS: dict[str, tuple[str, ...]] = {
    "price": (
        "last_price",
        "change_absolute",
        "change_percent",
        "reference_price",
        "reference_type",
    ),
    "volume": ("volume", "volume_delta"),
    "oi": ("oi", "oi_delta"),
    "depth": (
        "best_bid_price",
        "best_ask_price",
        "best_bid_quantity",
        "best_ask_quantity",
        "bid_levels",
        "ask_levels",
        "spread",
        "depth_imbalance",
    ),
    "quality": ("timestamp", "ingested_at"),
}

# Stream-only abbreviations. Canonical names remain everywhere else.
_ABBREV = {
    "last_price": "ltp",
    "change_absolute": "chg",
    "change_percent": "chgp",
    "volume": "vol",
    "volume_delta": "vd",
    "oi": "oi",
    "oi_delta": "oid",
    "best_bid_price": "bb",
    "best_ask_price": "ba",
    "best_bid_quantity": "bbq",
    "best_ask_quantity": "baq",
    "bid_levels": "bid",
    "ask_levels": "ask",
    "spread": "spr",
    "depth_imbalance": "imb",
    "reference_price": "ref",
    "reference_type": "reft",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_connection_id() -> str:
    return uuid4().hex


def parse_groups(raw: str | None) -> list[str]:
    if not raw or not str(raw).strip():
        return ["price", "volume", "oi", "depth", "quality"]
    wanted = []
    for part in str(raw).split(","):
        name = part.strip().lower()
        if name in GROUPS and name not in wanted:
            wanted.append(name)
    return wanted or ["price"]


def parse_tokens(raw: str | None) -> list[int]:
    if not raw or not str(raw).strip():
        return []
    out: list[int] = []
    seen: set[int] = set()
    for part in str(raw).split(","):
        text = part.strip()
        if not text:
            continue
        try:
            token = int(text)
        except ValueError as exc:
            raise ValueError(f"invalid_token:{text}") from exc
        if token <= 0:
            raise ValueError(f"invalid_token:{text}")
        if token not in seen:
            seen.add(token)
            out.append(token)
    return out


def format_sse(event: str, data: dict[str, Any], *, event_id: int | str | None = None) -> str:
    payload = json.dumps(data, separators=(",", ":"), default=str)
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(f"data: {payload}")
    lines.append("")
    lines.append("")
    return "\n".join(lines)


def hello_payload(
    *,
    connection_id: str,
    session: dict[str, Any],
    heartbeat_ms: int,
    max_instruments: int,
    subscribed: list[int],
    rejected: list[dict[str, Any]],
    schema_version: str = STREAM_SCHEMA_VERSION,
) -> dict[str, Any]:
    return {
        "connection_id": connection_id,
        "server_time": utc_now(),
        "session": session,
        "heartbeat_ms": int(heartbeat_ms),
        "max_instruments": int(max_instruments),
        "subscribed": subscribed,
        "rejected": rejected,
        "schema_version": schema_version,
    }


def heartbeat_payload(*, subscribed: int, lag_ms: float | None) -> dict[str, Any]:
    return {
        "server_time": utc_now(),
        "subscribed": int(subscribed),
        "lag_ms": None if lag_ms is None else round(float(lag_ms), 1),
    }


def resync_payload(*, reason: str, from_seq: int | None) -> dict[str, Any]:
    return {"reason": reason, "from_seq": from_seq}


def tick_frame(
    *,
    token: int,
    ts: str | None,
    seq: int,
    changed: dict[str, Any],
) -> dict[str, Any]:
    frame: dict[str, Any] = {"t": int(token), "ts": ts, "seq": int(seq)}
    for key, value in changed.items():
        frame[_ABBREV.get(key, key)] = value
    return frame


def fields_for_groups(groups: list[str]) -> list[str]:
    names: list[str] = []
    for group in groups:
        for field in GROUP_FIELDS.get(group, ()):
            if field not in names:
                names.append(field)
    return names


def changed_fields(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    fields: list[str],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    prev = previous or {}
    for field in fields:
        new = current.get(field)
        old = prev.get(field)
        if new != old:
            out[field] = new
    return out
