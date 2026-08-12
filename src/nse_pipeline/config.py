"""
Load application settings from YAML config + environment variables.

In C# you might use IOptions<T> bound from appsettings.json.
Here we use a @dataclass (similar to a record) populated once at startup.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def _project_root() -> Path:
    """Resolve project root (folder containing pyproject.toml)."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: three levels up from src/nse_pipeline/config.py
    return current.parents[2]


PROJECT_ROOT = _project_root()


@dataclass
class PathsSettings:
    data_dir: Path
    raw_dir: Path
    features_dir: Path
    models_dir: Path
    sqlite_db: Path
    logs_dir: Path
    instruments_cache: Path


@dataclass
class OptionsSettings:
    underlying: str
    exchange: str
    strikes_each_side: int
    strike_interval: int


@dataclass
class IngestionSettings:
    flush_interval_seconds: int
    flush_max_rows: int
    websocket_mode: str


@dataclass
class HistoricalSettings:
    interval: str
    lookback_days: int


@dataclass
class ResilienceSettings:
    reconnect_initial_seconds: float
    reconnect_max_seconds: float
    reconnect_multiplier: float


@dataclass
class KiteCredentials:
    api_key: str
    api_secret: str
    access_token: str


@dataclass
class Settings:
    """Top-level settings container — like a strongly-typed appsettings root."""

    paths: PathsSettings
    equity_symbols: list[str]
    index_symbols: list[str]
    options: OptionsSettings
    ingestion: IngestionSettings
    historical: HistoricalSettings
    resilience: ResilienceSettings
    kite: KiteCredentials
    raw_config: dict[str, Any] = field(repr=False, default_factory=dict)

    def ensure_directories(self) -> None:
        """Create data/log directories if they do not exist."""
        for path in (
            self.paths.data_dir,
            self.paths.raw_dir,
            self.paths.features_dir,
            self.paths.models_dir,
            self.paths.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def _resolve_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute():
        return path
    return root / path


def load_settings(config_path: Path | None = None) -> Settings:
    """
    Load settings.yaml and overlay Kite credentials from .env.

    load_dotenv() reads KEY=VALUE pairs from .env into os.environ —
    similar to environment variables in launchSettings.json / user secrets.
    """
    root = PROJECT_ROOT
    config_file = config_path or (root / "config" / "settings.yaml")

    with config_file.open("r", encoding="utf-8") as handle:
        # yaml.safe_load returns plain Python dicts/lists (like JSON deserialize).
        config: dict[str, Any] = yaml.safe_load(handle)

    load_dotenv(root / ".env")

    paths_cfg = config["paths"]
    paths = PathsSettings(
        data_dir=_resolve_path(root, paths_cfg["data_dir"]),
        raw_dir=_resolve_path(root, paths_cfg["raw_dir"]),
        features_dir=_resolve_path(root, paths_cfg["features_dir"]),
        models_dir=_resolve_path(root, paths_cfg["models_dir"]),
        sqlite_db=_resolve_path(root, paths_cfg["sqlite_db"]),
        logs_dir=_resolve_path(root, paths_cfg["logs_dir"]),
        instruments_cache=_resolve_path(root, paths_cfg["instruments_cache"]),
    )

    options_cfg = config["options"]
    options = OptionsSettings(
        underlying=options_cfg["underlying"],
        exchange=options_cfg["exchange"],
        strikes_each_side=int(options_cfg["strikes_each_side"]),
        strike_interval=int(options_cfg["strike_interval"]),
    )

    ingestion_cfg = config["ingestion"]
    ingestion = IngestionSettings(
        flush_interval_seconds=int(ingestion_cfg["flush_interval_seconds"]),
        flush_max_rows=int(ingestion_cfg["flush_max_rows"]),
        websocket_mode=str(ingestion_cfg["websocket_mode"]),
    )

    historical_cfg = config["historical"]
    historical = HistoricalSettings(
        interval=str(historical_cfg["interval"]),
        lookback_days=int(historical_cfg["lookback_days"]),
    )

    resilience_cfg = config["resilience"]
    resilience = ResilienceSettings(
        reconnect_initial_seconds=float(resilience_cfg["reconnect_initial_seconds"]),
        reconnect_max_seconds=float(resilience_cfg["reconnect_max_seconds"]),
        reconnect_multiplier=float(resilience_cfg["reconnect_multiplier"]),
    )

    kite = KiteCredentials(
        api_key=os.getenv("KITE_API_KEY", ""),
        api_secret=os.getenv("KITE_API_SECRET", ""),
        access_token=os.getenv("KITE_ACCESS_TOKEN", ""),
    )

    settings = Settings(
        paths=paths,
        equity_symbols=list(config["equity_symbols"]),
        # Optional for older settings.yaml files — default empty list.
        index_symbols=list(config.get("index_symbols", [])),
        options=options,
        ingestion=ingestion,
        historical=historical,
        resilience=resilience,
        kite=kite,
        raw_config=config,
    )
    settings.ensure_directories()
    return settings
