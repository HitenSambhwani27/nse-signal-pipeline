"""
Classify Nifty weekly vs monthly option series from Kite tradingsymbols.

Nifty weekly:  NIFTY{YY}{MON}{DD}{strike}{CE|PE}  e.g. NIFTY25AUG1924500CE
Nifty monthly: NIFTY{YY}{MON}{strike}{CE|PE}      e.g. NIFTY25AUG24500CE

Bank Nifty has no weekly series (discontinued Nov 2024). Nifty monthly is
out of scope — see TRADE_OFFS.md.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Weekly has an extra 2-digit day after the month code (6–7 digits after MON).
_NIFTY_WEEKLY = re.compile(r"^NIFTY\d{2}[A-Z]{3}\d{2}\d{4,5}(CE|PE)$")
# Monthly: 4–5 digit strike immediately after the month code.
_NIFTY_MONTHLY = re.compile(r"^NIFTY\d{2}[A-Z]{3}\d{4,5}(CE|PE)$")


def classify_nifty_option_symbol(tradingsymbol: str) -> str | None:
    """Return 'weekly', 'monthly', or None if the symbol does not match."""
    text = str(tradingsymbol).upper().replace(" ", "")
    if _NIFTY_WEEKLY.match(text):
        return "weekly"
    if _NIFTY_MONTHLY.match(text):
        return "monthly"
    return None


def filter_rows_by_series(
    rows: list[dict[str, Any]],
    *,
    underlying: str,
    series: str,
) -> tuple[list[dict[str, Any]], str]:
    """
    Keep only the configured series for an underlying.

    Returns (filtered_rows, note). For Bank Nifty every listed option is
    monthly — no filter. For Nifty weekly, if the weekly-pattern set is
    empty (last-Tuesday coincide week where only the monthly symbol exists),
    keep the unfiltered rows and log the coincide fallback.
    """
    name = underlying.upper()
    wanted = series.lower()
    if name == "BANKNIFTY":
        return rows, "banknifty_monthly_only"

    if name != "NIFTY":
        return rows, "unfiltered_unknown_underlying"

    classified: list[tuple[str, dict[str, Any]]] = []
    unknown = 0
    for row in rows:
        kind = classify_nifty_option_symbol(str(row.get("tradingsymbol", "")))
        if kind is None:
            unknown += 1
            continue
        classified.append((kind, row))

    matched = [row for kind, row in classified if kind == wanted]
    if matched:
        return matched, f"nifty_{wanted}"

    if wanted == "weekly" and rows:
        logger.warning(
            "Nifty weekly-pattern symbols empty (%s unknown of %s rows); "
            "using unfiltered nearest expiry (weekly/monthly coincide week). "
            "Do not treat this as adding the monthly series to the panel.",
            unknown,
            len(rows),
        )
        return rows, "nifty_weekly_monthly_coincide_fallback"

    logger.warning(
        "No %s Nifty option rows after series filter (%s input, %s unknown)",
        wanted,
        len(rows),
        unknown,
    )
    return [], f"nifty_{wanted}_empty"
