#!/usr/bin/env python3
"""Read-only Phase 4 live validation. Does not restart ingest."""

from __future__ import annotations

import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DB = Path("/home/nse/nse-signal-pipeline/data/nse_pipeline.db")
API = "http://127.0.0.1:8080"


def tokens_for(symbols: list[str]) -> dict[str, int]:
    con = sqlite3.connect(f"file:{DB.as_posix()}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    out = {}
    for symbol in symbols:
        row = con.execute(
            "SELECT instrument_token, last_price, timestamp FROM latest_quotes WHERE symbol=? COLLATE NOCASE",
            (symbol,),
        ).fetchone()
        if row:
            out[symbol] = dict(row)
    con.close()
    return out


def read_sse(path: str, seconds: float = 6.0, headers: dict | None = None) -> list[tuple[str, dict]]:
    req = urllib.request.Request(API + path, headers={"Accept": "text/event-stream", **(headers or {})})
    events: list[tuple[str, dict]] = []
    with urllib.request.urlopen(req, timeout=30) as resp:
        deadline = time.time() + seconds
        event = None
        data_lines: list[str] = []
        while time.time() < deadline:
            line = resp.readline()
            if not line:
                break
            text = line.decode("utf-8").rstrip("\n")
            if text.startswith("event:"):
                event = text.split(":", 1)[1].strip()
            elif text.startswith("data:"):
                data_lines.append(text.split(":", 1)[1].strip())
            elif text == "":
                if event and data_lines:
                    payload = json.loads("".join(data_lines))
                    events.append((event, payload))
                event = None
                data_lines = []
                if any(name == "tick" for name, _ in events) and any(name == "hello" for name, _ in events):
                    if time.time() > deadline - 2:
                        break
    return events


def main() -> int:
    wanted = ["NIFTY 50", "NIFTY BANK", "HDFCBANK"]
    rows = tokens_for(wanted)
    con = sqlite3.connect(f"file:{DB.as_posix()}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    fut = con.execute(
        "SELECT symbol, instrument_token, last_price, timestamp FROM latest_quotes WHERE symbol LIKE '%FUT' ORDER BY volume DESC LIMIT 1"
    ).fetchone()
    opt = con.execute(
        "SELECT symbol, instrument_token, last_price, timestamp FROM latest_quotes WHERE symbol LIKE 'NIFTY%CE' OR symbol LIKE 'NIFTY%PE' ORDER BY volume DESC LIMIT 1"
    ).fetchone()
    con.close()
    extra = {}
    if fut:
        extra[fut["symbol"]] = dict(fut)
    if opt:
        extra[opt["symbol"]] = dict(opt)
    all_rows = {**rows, **extra}
    token_list = [str(int(v["instrument_token"])) for v in all_rows.values()]
    path = "/api/v1/stream?tokens=" + ",".join(token_list) + "&groups=price,volume,oi,depth"
    t0 = time.perf_counter()
    events = read_sse(path, seconds=8)
    first_byte_ms = None
    # urllib doesn't expose TTFB easily; use total to first event as proxy after connect.
    kinds = [name for name, _ in events]
    ticks = [p for n, p in events if n == "tick"]
    hellos = [p for n, p in events if n == "hello"]
    beats = [p for n, p in events if n == "heartbeat"]
    seqs = [p.get("seq") for p in ticks if p.get("seq") is not None]
    comparisons = []
    live = tokens_for(list(all_rows))
    for symbol, before in all_rows.items():
        after = live.get(symbol) or {}
        token = int(before["instrument_token"])
        frame = next((p for p in ticks if p.get("t") == token), None)
        quote = json.loads(
            urllib.request.urlopen(API + "/api/v1/quotes/" + urllib.parse.quote(symbol), timeout=15).read()
        )
        q = (quote.get("quote") or {})
        comparisons.append(
            {
                "symbol": symbol,
                "token": token,
                "latest_ltp": after.get("last_price"),
                "stream_ltp": None if frame is None else frame.get("ltp"),
                "quote_change_absolute": q.get("change_absolute"),
                "stream_chg": None if frame is None else frame.get("chg"),
                "latest_ts": after.get("timestamp"),
                "data_status": (quote.get("data_state") or {}).get("data_status"),
            }
        )
    cap = json.loads(
        urllib.request.urlopen(
            urllib.request.Request(
                API + "/api/v1/stream?tokens=" + ",".join(str(i) for i in range(1, 252)),
                headers={"Accept": "text/event-stream"},
            ),
            timeout=10,
        ).read()
    ) if False else None
    # 251-token rejection
    try:
        urllib.request.urlopen(API + "/api/v1/stream?tokens=" + ",".join(str(i) for i in range(1, 252)), timeout=10)
        cap251 = {"status": "unexpected_200"}
    except urllib.error.HTTPError as exc:
        cap251 = {"status": exc.code, "body": json.loads(exc.read().decode())}

    health = json.loads(urllib.request.urlopen(API + "/api/v1/health", timeout=15).read())
    print(
        json.dumps(
            {
                "symbols": list(all_rows),
                "event_kinds": kinds,
                "hello": hellos[:1],
                "tick_count": len(ticks),
                "heartbeat_count": len(beats),
                "seq": seqs,
                "seq_monotonic": seqs == sorted(seqs) and len(seqs) == len(set(seqs)),
                "elapsed_ms": (time.perf_counter() - t0) * 1000,
                "comparisons": comparisons,
                "instrument_cap": cap251,
                "health_stream": (health.get("health") or {}).get("stream"),
                "sqlite_busy_retries": (health.get("health") or {}).get("sqlite_busy_retries"),
                "ingest_pid_hint": "check separately",
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
