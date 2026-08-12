#!/usr/bin/env python3
"""
Interactive Kite Connect login helper.

Usage:
  1. Copy .env.example to .env and set KITE_API_KEY + KITE_API_SECRET
  2. python scripts/00_kite_auth.py
  3. Open the printed login URL, authenticate, copy request_token from redirect URL
  4. Paste request_token when prompted
  5. Script saves access_token to .env and runs smoke test (profile + quote)

Non-interactive cache refresh (existing valid access_token in .env):
  python scripts/00_kite_auth.py --refresh-cache
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add src/ to import path when running as a script (like a local project reference).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.broker.auth import (  # noqa: E402
    exchange_request_token,
    get_authenticated_kite,
    get_login_url,
    save_access_token_to_env,
    smoke_test_connection,
)
from nse_pipeline.broker.instruments import (  # noqa: E402
    fetch_index_instruments,
    refresh_instrument_cache,
)
from nse_pipeline.config import load_settings  # noqa: E402


def _print_cache_summary(cache: dict, cache_path: Path) -> None:
    equity_count = len(cache.get("equity", {}))
    index_count = len(cache.get("index", {}))
    options_count = len(cache.get("options", []))
    print(
        f"Cached {equity_count} equity, {index_count} index, "
        f"and {options_count} option instruments."
    )
    for symbol, info in cache.get("index", {}).items():
        print(
            f"  index {symbol}: token={info.get('instrument_token')} "
            f"segment={info.get('segment')} exchange={info.get('exchange')}"
        )
    print(f"Cache file: {cache_path}")


def refresh_cache_only() -> int:
    """Use existing .env access_token to rebuild instruments_cache.json."""
    settings = load_settings()
    if not settings.kite.api_key or not settings.kite.access_token:
        print("ERROR: Need KITE_API_KEY and KITE_ACCESS_TOKEN in .env for --refresh-cache.")
        return 1

    kite = get_authenticated_kite(settings)

    print("=== Verifying index instrument lookup ===")
    try:
        indices = fetch_index_instruments(kite, settings.index_symbols)
    except Exception as exc:
        print(f"Index lookup failed: {exc}")
        return 1

    for symbol, info in indices.items():
        print(
            f"MATCHED {symbol}: token={info.instrument_token} "
            f"exchange={info.exchange} segment={info.segment} "
            f"instrument_type={info.instrument_type} name={info.name}"
        )

    print("\n=== Smoke Test ===")
    try:
        result = smoke_test_connection(kite, quote_symbol="NSE:NIFTY 50")
        print(f"User: {result['user_name']} ({result['user_id']})")
        print(f"Quote {result['quote_symbol']}: LTP={result['last_price']}")
    except Exception as exc:
        print(f"Smoke test failed: {exc}")
        return 1

    print("\n=== Refreshing instrument cache ===")
    try:
        cache = refresh_instrument_cache(kite, settings)
        _print_cache_summary(cache, settings.paths.instruments_cache)
    except Exception as exc:
        print(f"Instrument cache refresh failed: {exc}")
        return 1

    print("\nCache refresh complete.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Kite Connect auth + instrument cache helper")
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Skip interactive login; refresh instruments_cache.json with existing access_token",
    )
    args = parser.parse_args()

    if args.refresh_cache:
        return refresh_cache_only()

    settings = load_settings()

    if not settings.kite.api_key or not settings.kite.api_secret:
        print("ERROR: Set KITE_API_KEY and KITE_API_SECRET in .env first.")
        print("Copy .env.example -> .env and fill in your Kite Connect app credentials.")
        return 1

    login_url = get_login_url(settings.kite)
    print("\n=== Kite Connect Login ===")
    print("1) Open this URL in your browser and log in:")
    print(login_url)
    print("\n2) After login, your browser redirect URL contains request_token=...")
    print("   Copy ONLY the request_token value.\n")

    request_token = input("Paste request_token here: ").strip()
    if not request_token:
        print("No request_token provided.")
        return 1

    try:
        access_token = exchange_request_token(settings.kite, request_token)
    except Exception as exc:
        print(f"Token exchange failed: {exc}")
        return 1

    env_path = save_access_token_to_env(access_token)
    print(f"\nSaved KITE_ACCESS_TOKEN to {env_path}")

    # Reload settings so new token is picked up from environment.
    settings = load_settings()
    kite = get_authenticated_kite(settings)

    print("\n=== Smoke Test ===")
    try:
        result = smoke_test_connection(kite, quote_symbol="NSE:RELIANCE")
        print(f"User: {result['user_name']} ({result['user_id']})")
        print(f"Quote {result['quote_symbol']}: LTP={result['last_price']}")
    except Exception as exc:
        print(f"Smoke test failed: {exc}")
        return 1

    print("\n=== Refreshing instrument cache ===")
    try:
        cache = refresh_instrument_cache(kite, settings)
        _print_cache_summary(cache, settings.paths.instruments_cache)
    except Exception as exc:
        print(f"Instrument cache refresh failed: {exc}")
        return 1

    print("\nAuth setup complete. You can now run scripts/01_run_ingestion.py during market hours.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
