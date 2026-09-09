#!/usr/bin/env python3
"""Bounded 16-connection SSE soak. Backend only. Does not restart ingest."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

DB = Path("/home/nse/nse-signal-pipeline/data/nse_pipeline.db")
API = "http://127.0.0.1:8080"


def sample_tokens(limit: int = 40) -> list[int]:
    con = sqlite3.connect(f"file:{DB.as_posix()}?mode=ro", uri=True, timeout=5)
    rows = con.execute(
        "SELECT instrument_token FROM latest_quotes ORDER BY volume DESC LIMIT ?",
        (limit,),
    ).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def sse_worker(idx: int, path: str, seconds: float, bucket: dict) -> None:
    req = urllib.request.Request(API + path, headers={"Accept": "text/event-stream"})
    ticks = 0
    hellos = 0
    heartbeats = 0
    first_ms = None
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            deadline = time.time() + seconds
            event = None
            while time.time() < deadline:
                line = resp.readline()
                if not line:
                    break
                if first_ms is None:
                    first_ms = (time.perf_counter() - t0) * 1000
                text = line.decode("utf-8").rstrip("\n")
                if text.startswith("event:"):
                    event = text.split(":", 1)[1].strip()
                elif text == "":
                    if event == "hello":
                        hellos += 1
                    elif event == "tick":
                        ticks += 1
                    elif event == "heartbeat":
                        heartbeats += 1
                    event = None
        bucket[idx] = {
            "ok": True,
            "status": 200,
            "hello": hellos,
            "ticks": ticks,
            "heartbeats": heartbeats,
            "first_byte_ms": first_ms,
        }
    except urllib.error.HTTPError as exc:
        bucket[idx] = {"ok": False, "status": exc.code}
    except Exception as exc:
        bucket[idx] = {"ok": False, "status": None, "error": type(exc).__name__}


def main() -> int:
    tokens = sample_tokens(40)
    path = "/api/v1/stream?tokens=" + ",".join(str(t) for t in tokens) + "&groups=price,volume"
    results: dict[int, dict] = {}
    threads = [
        threading.Thread(target=sse_worker, args=(i, path, 12.0, results), daemon=True)
        for i in range(16)
    ]
    t0 = time.perf_counter()
    for th in threads:
        th.start()
    time.sleep(0.4)
    rejected = None
    try:
        urllib.request.urlopen(
            urllib.request.Request(API + path, headers={"Accept": "text/event-stream"}),
            timeout=8,
        )
        rejected = {"status": "unexpected_200"}
    except urllib.error.HTTPError as exc:
        rejected = {
            "status": exc.code,
            "retry_after": exc.headers.get("Retry-After"),
            "body": exc.read().decode()[:300],
        }
    for th in threads:
        th.join(timeout=20)
    health = json.loads(urllib.request.urlopen(API + "/api/v1/health", timeout=15).read())
    firsts = [r.get("first_byte_ms") for r in results.values() if r.get("first_byte_ms") is not None]
    print(
        json.dumps(
            {
                "connections_ok": sum(1 for r in results.values() if r.get("ok")),
                "results": results,
                "connection_17": rejected,
                "first_byte_p50_ms": sorted(firsts)[len(firsts) // 2] if firsts else None,
                "first_byte_max_ms": max(firsts) if firsts else None,
                "elapsed_ms": (time.perf_counter() - t0) * 1000,
                "tokens": len(tokens),
                "health_stream": (health.get("health") or {}).get("stream"),
                "sqlite_busy_retries": (health.get("health") or {}).get("sqlite_busy_retries"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
