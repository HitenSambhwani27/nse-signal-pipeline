"""Pearson correlations of individual features vs actual_outcome, per track."""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_pipeline.config import load_settings  # noqa: E402
from nse_pipeline.scoring.baseline import OI_LONG, OI_SHORT  # noqa: E402
from nse_pipeline.session_coverage import scoring_skip_trade_dates  # noqa: E402
from nse_pipeline.storage.sqlite_store import SQLiteStore  # noqa: E402

TRACKS = ("equity_depth", "equity_quote", "options", "futures")
FEATURES = {
    "equity_depth": (
        "vwap_deviation_bps",
        "depth_ratio_bid_ask",
        "spread_bps",
        "ofi_bucket_sum",
    ),
    "equity_quote": ("vwap_deviation_bps",),
    "options": (
        "pcr",
        "oi_buildup_long",
        "oi_buildup_short",
        "delta",
        "gamma",
        "vwap_deviation_bps",
        "depth_ratio_bid_ask",
        "ofi_bucket_sum",
        "moneyness",
        "days_to_expiry",
    ),
    "futures": (
        "basis_bps",
        "calendar_spread_near_minus_next",
        "vwap_deviation_bps",
        "depth_ratio_bid_ask",
        "ofi_bucket_sum",
    ),
}


def _scalar(feats: dict, name: str) -> float | None:
    if name == "oi_buildup_long":
        return 1.0 if str(feats.get("oi_buildup_state") or "") in OI_LONG else 0.0
    if name == "oi_buildup_short":
        return 1.0 if str(feats.get("oi_buildup_state") or "") in OI_SHORT else 0.0
    if name == "moneyness":
        spot, strike = feats.get("spot"), feats.get("strike")
        if spot not in (None, 0) and strike is not None:
            return (float(strike) - float(spot)) / float(spot)
        return None
    if name == "delta" or name == "gamma":
        greeks = feats.get("greeks") if isinstance(feats.get("greeks"), dict) else {}
        val = feats.get(name)
        if val is None:
            val = greeks.get(name)
        if val is None:
            return None
        return float(val)
    val = feats.get(name)
    if val is None or isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if math.isfinite(f) else None
    return None


class RunningCorr:
    def __init__(self) -> None:
        self.n = 0
        self.sx = 0.0
        self.sy = 0.0
        self.sxy = 0.0
        self.sx2 = 0.0
        self.sy2 = 0.0

    def add(self, x: float, y: float) -> None:
        self.n += 1
        self.sx += x
        self.sy += y
        self.sxy += x * y
        self.sx2 += x * x
        self.sy2 += y * y

    def r(self) -> float | None:
        n = self.n
        if n < 3:
            return None
        num = n * self.sxy - self.sx * self.sy
        den_x = n * self.sx2 - self.sx * self.sx
        den_y = n * self.sy2 - self.sy * self.sy
        if den_x <= 0 or den_y <= 0:
            return None
        return num / math.sqrt(den_x * den_y)


def main() -> int:
    settings = load_settings()
    store = SQLiteStore(settings.paths.sqlite_db)
    dates = [
        d
        for d in store.list_feature_trade_dates()
        if "2025-08-18" <= d <= "2026-08-14"
    ]
    acc: dict[str, dict[str, dict[str, RunningCorr]]] = {
        t: {f: {"outcome": RunningCorr(), "up": RunningCorr()} for f in FEATURES[t]}
        for t in TRACKS
    }
    n_rows = {t: 0 for t in TRACKS}
    n_labeled = {t: 0 for t in TRACKS}
    n_up = {t: 0 for t in TRACKS}
    n_down = {t: 0 for t in TRACKS}
    n_flat = {t: 0 for t in TRACKS}
    skipped: list[str] = []

    for i, d in enumerate(dates, 1):
        # One fetch per date for all four tracks (indexed by trade_date).
        rows = store.fetch_feature_logs(d, tracks=TRACKS, include_features=True)
        if not rows:
            continue
        if d in scoring_skip_trade_dates(rows):
            skipped.append(d)
            continue
        for row in rows:
            track = str(row.get("track") or "")
            if track not in acc:
                continue
            n_rows[track] += 1
            outcome = row.get("actual_outcome")
            if outcome is None:
                continue
            y = float(outcome)
            if not math.isfinite(y):
                continue
            n_labeled[track] += 1
            if y > 0:
                n_up[track] += 1
            elif y < 0:
                n_down[track] += 1
            else:
                n_flat[track] += 1
            y_up = 1.0 if y > 0 else 0.0
            feats = row.get("features") or {}
            for name, buckets in acc[track].items():
                x = _scalar(feats, name)
                if x is None:
                    continue
                buckets["outcome"].add(x, y)
                buckets["up"].add(x, y_up)
        if i % 20 == 0 or i == len(dates):
            print(f"progress {i}/{len(dates)} date={d}", flush=True)

    out: dict = {
        "window": ["2025-08-18", "2026-08-14"],
        "skipped_dates": skipped,
        "method": "pearson pairwise-complete vs actual_outcome (+1/0/-1 TBM) and vs up_indicator (1 if outcome>0 else 0)",
        "tracks": {},
    }
    for track in TRACKS:
        features = []
        for name in FEATURES[track]:
            oc = acc[track][name]["outcome"]
            up = acc[track][name]["up"]
            features.append(
                {
                    "feature": name,
                    "n": oc.n,
                    "r_vs_actual_outcome": oc.r(),
                    "r_vs_up_indicator": up.r(),
                }
            )
        labeled = n_labeled[track] or 1
        out["tracks"][track] = {
            "n_rows": n_rows[track],
            "n_labeled": n_labeled[track],
            "n_up": n_up[track],
            "n_down": n_down[track],
            "n_flat": n_flat[track],
            "pct_up": n_up[track] / labeled,
            "pct_down": n_down[track] / labeled,
            "pct_flat": n_flat[track] / labeled,
            "features": features,
        }
    dest = PROJECT_ROOT / "data" / "features" / "backtest" / "feature_outcome_corr.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
