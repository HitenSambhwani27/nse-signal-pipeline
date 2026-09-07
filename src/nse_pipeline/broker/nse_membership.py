"""
Download and cache official NSE index constituent lists (Nifty 100 / Nifty 500).

Membership is never a one-time hardcoded symbol list — CSVs are refreshed on
instrument-cache rebuild and when the on-disk snapshot is older than
settings.universe.membership_max_age_days.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from nse_pipeline.config import Settings, UniverseSettings


logger = logging.getLogger(__name__)

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Referer": "https://www.nseindia.com/",
}


@dataclass
class MembershipSnapshot:
    nifty100: list[str]
    nifty500: list[str]
    quote_only: list[str]
    source_nifty100: str
    source_nifty500: str
    refreshed_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "nifty100": self.nifty100,
            "nifty500": self.nifty500,
            "quote_only": self.quote_only,
            "nifty100_count": len(self.nifty100),
            "nifty500_count": len(self.nifty500),
            "quote_only_count": len(self.quote_only),
            "source_nifty100": self.source_nifty100,
            "source_nifty500": self.source_nifty500,
            "refreshed_at": self.refreshed_at,
        }


def _download_csv(primary_url: str, fallback_url: str) -> tuple[str, str]:
    """Return (csv_text, source_url). Tries primary then fallback."""
    last_error: Exception | None = None
    for url in (primary_url, fallback_url):
        req = Request(url, headers=_NSE_HEADERS)
        try:
            with urlopen(req, timeout=45) as response:
                text = response.read().decode("utf-8-sig")
            if "Symbol" not in text.splitlines()[0]:
                raise ValueError(f"CSV from {url} missing Symbol header")
            return text, url
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            logger.warning("Membership download failed for %s: %s", url, exc)
    raise RuntimeError(f"Could not download NSE constituent CSV: {last_error}")


def _parse_symbols(csv_text: str) -> list[str]:
    reader = csv.DictReader(io.StringIO(csv_text))
    symbols: list[str] = []
    for row in reader:
        symbol = (row.get("Symbol") or row.get("symbol") or "").strip()
        if not symbol:
            continue
        symbols.append(symbol)
    # Preserve order but drop duplicates.
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered


def membership_snapshot_path(settings: Settings) -> Path:
    return settings.paths.membership_dir / "nse_index_membership.json"


def membership_is_stale(settings: Settings) -> bool:
    path = membership_snapshot_path(settings)
    if not path.exists():
        return True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        refreshed_at = datetime.fromisoformat(
            str(payload["refreshed_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError, json.JSONDecodeError):
        return True
    age_days = (datetime.now(timezone.utc) - refreshed_at).total_seconds() / 86400.0
    return age_days >= settings.universe.membership_max_age_days


def load_membership_snapshot(settings: Settings) -> MembershipSnapshot | None:
    path = membership_snapshot_path(settings)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return MembershipSnapshot(
        nifty100=list(payload["nifty100"]),
        nifty500=list(payload["nifty500"]),
        quote_only=list(payload["quote_only"]),
        source_nifty100=str(payload.get("source_nifty100", "")),
        source_nifty500=str(payload.get("source_nifty500", "")),
        refreshed_at=str(payload["refreshed_at"]),
    )


def refresh_membership(
    settings: Settings,
    *,
    force: bool = False,
) -> MembershipSnapshot:
    """
    Download official Nifty 100 / 500 lists and compute quote-only = N500 \\ N100.

    Called from instrument-cache refresh and when membership is stale at startup.
    """
    if not force and not membership_is_stale(settings):
        existing = load_membership_snapshot(settings)
        if existing is not None:
            logger.info(
                "Membership snapshot fresh (age < %s days); reusing %s",
                settings.universe.membership_max_age_days,
                membership_snapshot_path(settings),
            )
            return existing

    uni: UniverseSettings = settings.universe
    nifty100_text, src100 = _download_csv(
        uni.nifty100_csv_url, uni.nifty100_csv_fallback_url
    )
    nifty500_text, src500 = _download_csv(
        uni.nifty500_csv_url, uni.nifty500_csv_fallback_url
    )

    nifty100 = _parse_symbols(nifty100_text)
    nifty500 = _parse_symbols(nifty500_text)
    set100 = set(nifty100)
    # Quote mode: Nifty 500 names that are NOT in the Nifty 100 depth set.
    quote_only = [s for s in nifty500 if s not in set100]

    missing_from_500 = sorted(set100 - set(nifty500))
    if missing_from_500:
        logger.warning(
            "Nifty 100 symbols missing from Nifty 500 list (kept in depth set): %s",
            missing_from_500,
        )

    snapshot = MembershipSnapshot(
        nifty100=nifty100,
        nifty500=nifty500,
        quote_only=quote_only,
        source_nifty100=src100,
        source_nifty500=src500,
        refreshed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )

    path = membership_snapshot_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot.to_dict(), indent=2), encoding="utf-8")
    # Also keep raw CSV copies for audit.
    (path.parent / "ind_nifty100list.csv").write_text(nifty100_text, encoding="utf-8")
    (path.parent / "ind_nifty500list.csv").write_text(nifty500_text, encoding="utf-8")

    logger.info(
        "Membership refreshed: nifty100=%s nifty500=%s quote_only=%s",
        len(nifty100),
        len(nifty500),
        len(quote_only),
    )
    return snapshot


def load_sector_map(settings: Settings) -> dict[str, str]:
    """Industry column from the NSE constituent CSVs. Empty if files are missing."""
    mapping: dict[str, str] = {}
    for filename in ("ind_nifty500list.csv", "ind_nifty100list.csv"):
        path = settings.paths.membership_dir / filename
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            symbol = (row.get("Symbol") or row.get("symbol") or "").strip().upper()
            industry = (row.get("Industry") or row.get("industry") or "").strip()
            if symbol and industry and symbol not in mapping:
                mapping[symbol] = industry
    return mapping
