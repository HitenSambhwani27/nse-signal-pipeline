#!/usr/bin/env python3
import json
import time
import urllib.request

t0 = time.perf_counter()
req = urllib.request.Request(
    "http://127.0.0.1:8080/api/v1/stream?tokens=341249",
    headers={"Accept": "text/event-stream"},
)
with urllib.request.urlopen(req, timeout=15) as resp:
    line = resp.readline()
    print("ttfb_ms", (time.perf_counter() - t0) * 1000)
    print("first", line[:80])
body = json.dumps(
    {
        "token": 341249,
        "requester": "phase4-validate",
        "action": "subscribe",
        "capabilities": ["price"],
        "ttl_seconds": 300,
    }
).encode()
req2 = urllib.request.Request(
    "http://127.0.0.1:8080/api/v1/subscriptions",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
print(urllib.request.urlopen(req2, timeout=10).read().decode())
