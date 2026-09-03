#!/usr/bin/env python3
"""Read-only UI API. Dashboard talks only to this process — not to SQLite."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import uvicorn  # noqa: E402

from nse_pipeline.api.app import create_app  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only pipeline API for the dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    uvicorn.run(create_app, factory=True, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
