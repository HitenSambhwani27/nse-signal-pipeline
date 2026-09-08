"""Retention planner. Default is dry-run; never deletes signal_log."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from nse_pipeline.config import RetentionSettings, Settings
from nse_pipeline.storage.sqlite_store import SQLiteStore

_DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class RetentionPlan:
    dry_run: bool
    as_of: str
    disk_free_gb: float | None
    sessions_of_headroom: int | None
    raw_delete: list[str] = field(default_factory=list)
    compacted_delete: list[str] = field(default_factory=list)
    bars_1m_delete: list[str] = field(default_factory=list)
    feature_log_before: str | None = None
    quality_log_before: str | None = None
    signal_log: str = "keep_forever"
    applied: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "as_of": self.as_of,
            "disk_free_gb": self.disk_free_gb,
            "sessions_of_headroom": self.sessions_of_headroom,
            "raw_delete": self.raw_delete,
            "compacted_delete": self.compacted_delete,
            "bars_1m_delete": self.bars_1m_delete,
            "feature_log_before": self.feature_log_before,
            "quality_log_before": self.quality_log_before,
            "signal_log": self.signal_log,
            "applied": self.applied,
            "notes": self.notes,
        }


def _session_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and _DATE_DIR.match(p.name))


def _older_than(paths: list[Path], *, keep: int, today: date) -> list[Path]:
    cutoff = today - timedelta(days=max(0, keep))
    return [p for p in paths if date.fromisoformat(p.name) < cutoff]


def disk_usage_report(path: Path, *, raw_bytes_per_session: int) -> dict[str, Any]:
    try:
        import shutil

        usage = shutil.disk_usage(path)
    except OSError:
        return {
            "disk_total_gb": None,
            "disk_used_gb": None,
            "disk_free_gb": None,
            "sessions_of_headroom": None,
        }
    total = float(usage.total)
    free = float(usage.free)
    used = total - free
    per = max(int(raw_bytes_per_session), 1)
    return {
        "disk_total_gb": round(total / (1024**3), 2),
        "disk_used_gb": round(used / (1024**3), 2),
        "disk_free_gb": round(free / (1024**3), 2),
        "sessions_of_headroom": int(free // per),
    }


def disk_stats(path: Path, *, raw_bytes_per_session: int) -> tuple[float | None, int | None]:
    report = disk_usage_report(path, raw_bytes_per_session=raw_bytes_per_session)
    return report["disk_free_gb"], report["sessions_of_headroom"]


def plan_retention(
    settings: Settings,
    *,
    today: date | None = None,
    dry_run: bool = True,
) -> RetentionPlan:
    cfg: RetentionSettings = getattr(settings, "retention", RetentionSettings())
    today = today or datetime.now(timezone.utc).date()
    free_gb, headroom = disk_stats(
        settings.paths.data_dir, raw_bytes_per_session=cfg.raw_bytes_per_session_estimate
    )
    bars_root = settings.paths.data_dir / "bars"
    plan = RetentionPlan(
        dry_run=dry_run,
        as_of=today.isoformat(),
        disk_free_gb=free_gb,
        sessions_of_headroom=headroom,
        raw_delete=[
            p.name for p in _older_than(_session_dirs(settings.paths.raw_dir), keep=cfg.raw_sessions, today=today)
        ],
        compacted_delete=[
            p.name
            for p in _older_than(
                _session_dirs(settings.paths.compacted_dir), keep=cfg.compacted_tick_sessions, today=today
            )
        ],
        bars_1m_delete=[
            p.name
            for p in _older_than(_session_dirs(bars_root), keep=cfg.bars_1m_sessions, today=today)
        ],
        feature_log_before=(today - timedelta(days=cfg.feature_log_days)).isoformat(),
        quality_log_before=(today - timedelta(days=cfg.quality_log_days)).isoformat(),
        notes=[],
    )
    if not bars_root.exists():
        plan.notes.append("bars store not present; 1m bar retention is a no-op")
    if cfg.daily_bars != "forever":
        plan.notes.append("daily_bars is configured away from forever; Phase 0 still never deletes daily bars")
    if cfg.signal_log != "forever":
        raise RuntimeError("retention.signal_log must remain forever")
    if headroom is not None and headroom < 10:
        plan.notes.append(f"WARNING: sessions_of_headroom={headroom} (<10)")
    plan.notes.append("signal_log is never deleted")
    return plan


def apply_retention(settings: Settings, plan: RetentionPlan) -> RetentionPlan:
    """Destructive. Callers must have already shown the dry-run plan."""
    if plan.dry_run:
        raise RuntimeError("apply_retention refused: plan is dry_run")
    import shutil

    for name in plan.raw_delete:
        shutil.rmtree(settings.paths.raw_dir / name, ignore_errors=False)
    for name in plan.compacted_delete:
        shutil.rmtree(settings.paths.compacted_dir / name, ignore_errors=False)
    bars_root = settings.paths.data_dir / "bars"
    for name in plan.bars_1m_delete:
        target = bars_root / name
        if target.exists():
            shutil.rmtree(target, ignore_errors=False)
    if settings.paths.sqlite_db.exists() and plan.feature_log_before:
        store = SQLiteStore(settings.paths.sqlite_db)
        with store.connection() as conn:
            conn.execute(
                "DELETE FROM feature_log WHERE trade_date IS NOT NULL AND trade_date < ?",
                (plan.feature_log_before,),
            )
            conn.execute(
                "DELETE FROM quality_log WHERE trade_date IS NOT NULL AND trade_date < ?",
                (plan.quality_log_before,),
            )
            signal_count = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()
            plan.notes.append(
                f"signal_log rows left untouched: {int(signal_count[0] if signal_count else 0)}"
            )
    plan.applied = True
    plan.notes.append("applied")
    return plan
