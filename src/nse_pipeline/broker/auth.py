"""
Kite Connect authentication helpers.

Kite uses a daily OAuth flow:
  1. Redirect user to login URL with api_key
  2. User logs in; redirect URL contains request_token
  3. Exchange request_token + api_secret for access_token (valid until ~6 AM next day)
"""

from __future__ import annotations

import os
from pathlib import Path

from kiteconnect import KiteConnect

from nse_pipeline.config import KiteCredentials, PROJECT_ROOT, Settings


def create_kite_client(credentials: KiteCredentials) -> KiteConnect:
    """Create a KiteConnect SDK client from credentials."""
    if not credentials.api_key:
        raise ValueError("KITE_API_KEY is missing. Copy .env.example to .env and fill credentials.")

    kite = KiteConnect(api_key=credentials.api_key)
    if credentials.access_token:
        kite.set_access_token(credentials.access_token)
    return kite


def get_login_url(credentials: KiteCredentials) -> str:
    """Return the Kite login URL for the user to authenticate in a browser."""
    kite = create_kite_client(credentials)
    return kite.login_url()


def exchange_request_token(
    credentials: KiteCredentials,
    request_token: str,
) -> str:
    """
    Exchange one-time request_token for access_token.

    Raises KiteConnect exceptions on invalid token/secret.
    """
    if not credentials.api_secret:
        raise ValueError("KITE_API_SECRET is missing in .env")

    kite = create_kite_client(credentials)
    session = kite.generate_session(request_token, api_secret=credentials.api_secret)
    access_token = session["access_token"]
    return str(access_token)


def save_access_token_to_env(access_token: str, env_path: Path | None = None) -> Path:
    """
    Upsert KITE_ACCESS_TOKEN in .env file.

    We read/write line-by-line instead of python-dotenv's set_key for clarity.
    """
    env_file = env_path or (PROJECT_ROOT / ".env")
    lines: list[str] = []
    token_written = False

    if env_file.exists():
        lines = env_file.read_text(encoding="utf-8").splitlines()

    new_lines: list[str] = []
    for line in lines:
        if line.startswith("KITE_ACCESS_TOKEN="):
            new_lines.append(f"KITE_ACCESS_TOKEN={access_token}")
            token_written = True
        else:
            new_lines.append(line)

    if not token_written:
        new_lines.append(f"KITE_ACCESS_TOKEN={access_token}")

    env_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    os.environ["KITE_ACCESS_TOKEN"] = access_token
    return env_file


def get_authenticated_kite(settings: Settings) -> KiteConnect:
    """Return Kite client with access token set; raises if token missing."""
    if not settings.kite.access_token:
        raise ValueError(
            "KITE_ACCESS_TOKEN is missing. Run: python scripts/00_kite_auth.py"
        )
    return create_kite_client(settings.kite)


def smoke_test_connection(kite: KiteConnect, quote_symbol: str = "NSE:RELIANCE") -> dict:
    """
    Basic connectivity test: profile + one quote.

    Returns a dict with profile name and last traded price.
    """
    profile = kite.profile()
    quote = kite.quote([quote_symbol])
    symbol_data = quote[quote_symbol]
    return {
        "user_name": profile.get("user_name"),
        "user_id": profile.get("user_id"),
        "quote_symbol": quote_symbol,
        "last_price": symbol_data.get("last_price"),
    }
