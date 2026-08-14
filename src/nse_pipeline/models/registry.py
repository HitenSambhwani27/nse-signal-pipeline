"""Stage 5C — model registry. Only harness-passed models are loadable for live."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

from nse_pipeline.config import Settings
from nse_pipeline.storage.sqlite_store import SQLiteStore


def _version_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_model_bundle(
    settings: Settings,
    *,
    track: str,
    role: str,
    estimator: Any,
    metadata: dict[str, Any],
    harness_report: dict[str, Any] | None,
) -> Path:
    """
    Write joblib + metadata JSON. Never overwrite a previous version directory.
    Live loading requires metadata.harness_passed is true.
    """
    version = _version_stamp()
    out_dir = settings.paths.models_dir / track / role / version
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / "model.joblib"
    joblib.dump(estimator, model_path)
    passed = bool(harness_report and harness_report.get("harness_passed"))
    payload = {
        **metadata,
        "track": track,
        "role": role,
        "version": version,
        "harness_passed": passed,
        "harness_report_path": (harness_report or {}).get("report_path"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / "metadata.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    if harness_report:
        (out_dir / "backtest.json").write_text(
            json.dumps(harness_report, indent=2, default=str), encoding="utf-8"
        )
    SQLiteStore(settings.paths.sqlite_db).register_model(
        track=track,
        role=role,
        version=version,
        path=str(out_dir),
        harness_passed=passed,
        metadata=payload,
    )
    return out_dir


def _read_meta(path: Path) -> dict[str, Any] | None:
    meta_path = path / "metadata.json"
    if not meta_path.exists():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def list_models(settings: Settings, track: str, role: str) -> list[Path]:
    root = settings.paths.models_dir / track / role
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir()], reverse=True)


def load_latest_model(
    settings: Settings,
    track: str,
    role: str,
    *,
    require_harness_passed: bool = True,
) -> tuple[Any, dict[str, Any], Path]:
    """
    Most recent coarse or fine bundle by timestamp directory name.
    Refuses models that have not passed the Part 5 walk-forward harness.
    """
    for path in list_models(settings, track, role):
        meta = _read_meta(path)
        if meta is None:
            continue
        if require_harness_passed and not meta.get("harness_passed"):
            continue
        model = joblib.load(path / "model.joblib")
        return model, meta, path
    raise FileNotFoundError(
        f"No {'harness-passed ' if require_harness_passed else ''}"
        f"model for track={track} role={role} under {settings.paths.models_dir}"
    )


def load_latest_pair(
    settings: Settings, track: str, *, require_harness_passed: bool = True
) -> dict[str, Any]:
    coarse_m, coarse_meta, coarse_path = load_latest_model(
        settings, track, "coarse", require_harness_passed=require_harness_passed
    )
    pair: dict[str, Any] = {
        "coarse": {"model": coarse_m, "metadata": coarse_meta, "path": coarse_path},
        "fine": None,
    }
    try:
        fine_m, fine_meta, fine_path = load_latest_model(
            settings, track, "fine", require_harness_passed=require_harness_passed
        )
        pair["fine"] = {"model": fine_m, "metadata": fine_meta, "path": fine_path}
    except FileNotFoundError:
        pass
    return pair
