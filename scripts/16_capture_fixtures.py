#!/usr/bin/env python3
"""Capture real API envelopes for tests. Does not invent market data."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "fixtures" / "api"


PATHS = {
    "health": "/api/v1/health",
    "overview": "/api/v1/overview",
    "maturity": "/api/v1/maturity",
    "quote_nifty": "/api/v1/quotes/NIFTY%2050",
    "quote_hdfcbank": "/api/v1/quotes/HDFCBANK",
    "quote_missing": "/api/v1/quotes/__NO_SUCH_SYMBOL__",
    "options_nifty": "/api/v1/options/NIFTY",
    "options_banknifty": "/api/v1/options/BANKNIFTY",
    "futures_nifty": "/api/v1/futures/NIFTY",
    "unusual_activity": "/api/v1/unusual-activity?limit=10",
    "watchlist_quotes": "/api/v1/watchlists/quotes",
}


def _redact(obj):
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            low = str(key).lower()
            if any(s in low for s in ("token", "secret", "api_key", "password", "authorization")):
                out[key] = "<redacted>"
            else:
                out[key] = _redact(value)
        return out
    if isinstance(obj, list):
        return [_redact(x) for x in obj]
    return obj


def _get(base: str, path: str) -> tuple[int, dict | list | str]:
    url = base.rstrip("/") + path
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8")
            code = resp.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        code = exc.code
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        payload = body
    return code, payload


def _classify(name: str, payload: object) -> str:
    if not isinstance(payload, dict):
        return "raw"
    state = (payload.get("data_state") or {}) if isinstance(payload.get("data_state"), dict) else {}
    status = str(state.get("data_status") or "")
    if name == "quote_missing" or status == "no_data":
        return "no_data"
    if status == "last_session":
        return "last_session"
    if status == "live":
        return "live"
    return status or "unclassified"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    index: dict[str, dict] = {}
    for name, path in PATHS.items():
        code, payload = _get(args.base, path)
        kind = _classify(name, payload)
        filename = f"{kind}__{name}.json"
        target = args.out / filename
        target.write_text(
            json.dumps(
                {"http_status": code, "path": path, "payload": _redact(payload)},
                indent=2,
            ),
            encoding="utf-8",
        )
        index[name] = {"file": filename, "http_status": code, "kind": kind, "path": path}
        print(f"{name}: {code} -> {filename} ({kind})")
    (args.out / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
