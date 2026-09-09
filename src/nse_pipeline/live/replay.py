"""Deterministic stream replay. Never connects to Kite. Never writes production data."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from nse_pipeline.live.frames import format_sse, heartbeat_payload, hello_payload, resync_payload


REPLAY_KIND = "replay_fixture"


@dataclass
class StreamFrame:
    event: str
    data: dict[str, Any]
    event_id: int | str | None = None
    raw: str = ""
    received_at: float = 0.0


@dataclass
class IntegrityReport:
    total_frames: int = 0
    by_event: dict[str, int] = field(default_factory=dict)
    sequences: list[int] = field(default_factory=list)
    duplicates: int = 0
    gaps: int = 0
    out_of_order: int = 0
    regressions: int = 0
    reconnects: int = 0
    resyncs: list[dict[str, Any]] = field(default_factory=list)
    heartbeat_intervals_ms: list[float] = field(default_factory=list)
    last_state: dict[int, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": REPLAY_KIND,
            "total_frames": self.total_frames,
            "by_event": dict(self.by_event),
            "duplicate_count": self.duplicates,
            "gap_count": self.gaps,
            "out_of_order_count": self.out_of_order,
            "regression_count": self.regressions,
            "reconnect_count": self.reconnects,
            "resyncs": list(self.resyncs),
            "heartbeat_intervals_ms": list(self.heartbeat_intervals_ms),
            "sequences": list(self.sequences),
        }


class StreamIntegrity:
    """Consumer-side sequence/state checks. Measurement only — no synthetic prices."""

    def __init__(self) -> None:
        self.report = IntegrityReport()
        self._last_seq: int | None = None
        self._last_heartbeat_at: float | None = None
        self._seen_seq: set[int] = set()

    def consume(self, frame: StreamFrame) -> None:
        self.report.total_frames += 1
        self.report.by_event[frame.event] = self.report.by_event.get(frame.event, 0) + 1
        if frame.event == "resync":
            self.report.resyncs.append(dict(frame.data))
            self._last_seq = None
            self._seen_seq.clear()
            return
        if frame.event == "hello":
            self.report.reconnects += 1
            return
        if frame.event == "heartbeat":
            if self._last_heartbeat_at is not None and frame.received_at:
                delta = (frame.received_at - self._last_heartbeat_at) * 1000.0
                self.report.heartbeat_intervals_ms.append(delta)
            self._last_heartbeat_at = frame.received_at or self._last_heartbeat_at
            return
        if frame.event != "tick":
            return
        seq_raw = frame.event_id if frame.event_id is not None else frame.data.get("seq")
        try:
            seq = int(seq_raw)
        except (TypeError, ValueError):
            return
        self.report.sequences.append(seq)
        token = frame.data.get("t") or frame.data.get("token")
        if seq in self._seen_seq:
            self.report.duplicates += 1
        self._seen_seq.add(seq)
        if self._last_seq is not None:
            if seq < self._last_seq:
                self.report.out_of_order += 1
            elif seq > self._last_seq + 1:
                self.report.gaps += 1
        self._last_seq = seq
        if token is None:
            return
        try:
            token_i = int(token)
        except (TypeError, ValueError):
            return
        prev = self.report.last_state.get(token_i)
        ts = str(frame.data.get("ts") or "")
        if prev is not None:
            prev_ts = str(prev.get("ts") or "")
            if ts and prev_ts and ts < prev_ts:
                self.report.regressions += 1
            ltp = frame.data.get("ltp")
            if ltp is not None and "ltp" in prev and ts and prev_ts and ts < prev_ts:
                self.report.regressions += 1
        merged = dict(prev or {})
        merged.update({k: v for k, v in frame.data.items() if v is not None})
        self.report.last_state[token_i] = merged


def parse_sse_chunk(text: str, *, received_at: float = 0.0) -> list[StreamFrame]:
    frames: list[StreamFrame] = []
    event = "message"
    event_id: int | str | None = None
    data_lines: list[str] = []
    raw_parts: list[str] = []
    for line in text.splitlines():
        raw_parts.append(line)
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event = line[6:].strip()
            continue
        if line.startswith("id:"):
            event_id = line[3:].strip()
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
            continue
        if line == "":
            if data_lines:
                payload = "\n".join(data_lines)
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    data = {"raw": payload}
                frames.append(
                    StreamFrame(
                        event=event,
                        data=data if isinstance(data, dict) else {"value": data},
                        event_id=event_id,
                        raw="\n".join(raw_parts),
                        received_at=received_at,
                    )
                )
            event = "message"
            event_id = None
            data_lines = []
            raw_parts = []
    return frames


def _tick(seq: int, token: int, ltp: float, ts: str) -> StreamFrame:
    data = {"t": token, "ts": ts, "seq": seq, "ltp": ltp}
    return StreamFrame(event="tick", data=data, event_id=seq, raw=format_sse("tick", data, event_id=seq))


def scenario_events(name: str) -> list[StreamFrame]:
    """Named fixtures. Prices are recorded placeholders, not live market data."""
    hello = StreamFrame(
        event="hello",
        data=hello_payload(
            connection_id="replay",
            session={"market_state": "open", "session_date": "2026-09-09", "coverage": None},
            heartbeat_ms=5000,
            max_instruments=250,
            subscribed=[1],
            rejected=[],
        ),
    )
    ts = "2026-09-09T04:00:00+00:00"
    if name == "normal_ordered":
        return [hello, _tick(1, 1, 100.0, ts), _tick(2, 1, 100.5, ts), _tick(3, 1, 101.0, ts)]
    if name == "duplicate_frame":
        t = _tick(1, 1, 100.0, ts)
        return [hello, t, t]
    if name == "out_of_order":
        return [hello, _tick(2, 1, 101.0, ts), _tick(1, 1, 100.0, ts)]
    if name == "sequence_gap":
        return [hello, _tick(1, 1, 100.0, ts), _tick(4, 1, 102.0, ts)]
    if name == "reconnect":
        second = StreamFrame(
            event="hello",
            data=hello_payload(
                connection_id="replay-2",
                session={"market_state": "open", "session_date": "2026-09-09"},
                heartbeat_ms=5000,
                max_instruments=250,
                subscribed=[1],
                rejected=[],
            ),
        )
        resync = StreamFrame(
            event="resync",
            data=resync_payload(reason="unavailable_continuity", from_seq=3),
        )
        return [hello, _tick(1, 1, 100.0, ts), second, resync, _tick(1, 1, 101.0, ts)]
    if name == "ingestion_restart":
        resync = StreamFrame(
            event="resync",
            data=resync_payload(reason="ingestion_restart", from_seq=5),
        )
        return [hello, _tick(1, 1, 100.0, ts), resync, _tick(1, 1, 101.0, ts)]
    if name == "stale_data":
        session = StreamFrame(
            event="session",
            data={
                "market_state": "open",
                "data_status": "stale",
                "session_date": "2026-09-09",
                "coverage": None,
            },
        )
        return [hello, _tick(1, 1, 100.0, "2026-09-09T03:50:00+00:00"), session]
    if name == "partial_session":
        session = StreamFrame(
            event="session",
            data={
                "market_state": "open",
                "data_status": "live",
                "session_date": "2026-09-09",
                "coverage": "PARTIAL_SESSION",
            },
        )
        hb = StreamFrame(event="heartbeat", data=heartbeat_payload(subscribed=1, lag_ms=2100.0))
        return [hello, session, hb, _tick(1, 1, 100.0, ts)]
    raise KeyError(name)


REQUIRED_SCENARIOS = (
    "normal_ordered",
    "duplicate_frame",
    "out_of_order",
    "sequence_gap",
    "reconnect",
    "ingestion_restart",
    "stale_data",
    "partial_session",
)


EXPECTED_INTEGRITY = {
    "normal_ordered": {"duplicates": 0, "gaps": 0, "out_of_order": 0, "regressions": 0},
    "duplicate_frame": {"duplicates": 1},
    "out_of_order": {"out_of_order": 1},
    "sequence_gap": {"gaps": 1},
    "reconnect": {"resync_reason": "unavailable_continuity"},
    "ingestion_restart": {"resync_reason": "ingestion_restart"},
    "stale_data": {"data_status": "stale"},
    "partial_session": {"coverage": "PARTIAL_SESSION"},
}


def play_scenario(name: str) -> IntegrityReport:
    integrity = StreamIntegrity()
    now = 0.0
    for frame in scenario_events(name):
        now += 0.2
        frame.received_at = now
        integrity.consume(frame)
    return integrity.report


def play_all_scenarios() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in REQUIRED_SCENARIOS:
        report = play_scenario(name)
        out[name] = report.to_dict()
    return out


def assert_scenario(name: str, report: IntegrityReport) -> None:
    expect = EXPECTED_INTEGRITY[name]
    if "duplicates" in expect:
        assert report.duplicates == expect["duplicates"], (name, report.to_dict())
    if "gaps" in expect:
        assert report.gaps == expect["gaps"], (name, report.to_dict())
    if "out_of_order" in expect:
        assert report.out_of_order == expect["out_of_order"], (name, report.to_dict())
    if "regressions" in expect:
        assert report.regressions == expect["regressions"], (name, report.to_dict())
    if "resync_reason" in expect:
        reasons = [r.get("reason") for r in report.resyncs]
        assert expect["resync_reason"] in reasons, (name, reasons)
    if "data_status" in expect:
        assert report.by_event.get("session", 0) >= 1
    if "coverage" in expect:
        assert report.by_event.get("session", 0) >= 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iter_named_sse(name: str) -> Iterable[str]:
    for frame in scenario_events(name):
        yield format_sse(frame.event, frame.data, event_id=frame.event_id)
