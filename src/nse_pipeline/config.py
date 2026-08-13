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
    compacted_dir: Path
    features_dir: Path
    models_dir: Path
    sqlite_db: Path
    logs_dir: Path
    instruments_cache: Path
    membership_dir: Path


@dataclass
class CompactionSettings:
    archive_minute_files: bool
    archive_subdir: str


@dataclass
class UniverseSettings:
    nifty100_csv_url: str
    nifty500_csv_url: str
    nifty100_csv_fallback_url: str
    nifty500_csv_fallback_url: str
    membership_max_age_days: int


@dataclass
class OptionUnderlyingSettings:
    name: str
    spot_quote: str
    strikes_each_side: int
    strike_interval: float


@dataclass
class OptionsSettings:
    exchange: str
    underlyings: list[OptionUnderlyingSettings]


@dataclass
class FuturesSettings:
    exchange: str
    underlyings: list[str]
    contract_count: int
    subscribe_mode: str


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
class FeaturesSettings:
    oi_bucket_minutes: int
    max_tick_return_pct: float
    options_max_tick_return_pct: float
    basis_days_per_year: int


@dataclass
class TripleBarrierSettings:
    vol_method: str
    vol_window: int
    barrier_multiplier: float
    min_threshold_pct: float
    max_threshold_pct: float
    path_rule: str
    options_label_mode: str  # raw_premium now; delta_residual reserved (Phase B)


@dataclass
class SignalSettings:
    """Stage 3 labeling rules — always read from settings, never hardcode."""

    candle_interval_minutes: int
    threshold_pct: float  # legacy flat % for comparison / old mode
    label_mode: str
    triple_barrier: TripleBarrierSettings
    vol_min_periods: int


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
    universe: UniverseSettings
    index_symbols: list[str]
    options: OptionsSettings
    futures: FuturesSettings
    ingestion: IngestionSettings
    compaction: CompactionSettings
    historical: HistoricalSettings
    features: FeaturesSettings
    signal: SignalSettings
    resilience: ResilienceSettings
    kite: KiteCredentials
    raw_config: dict[str, Any] = field(repr=False, default_factory=dict)

    def ensure_directories(self) -> None:
        """Create data/log directories if they do not exist."""
        for path in (
            self.paths.data_dir,
            self.paths.raw_dir,
            self.paths.compacted_dir,
            self.paths.features_dir,
            self.paths.models_dir,
            self.paths.logs_dir,
            self.paths.membership_dir,
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
        compacted_dir=_resolve_path(
            root, paths_cfg.get("compacted_dir", "data/compacted")
        ),
        features_dir=_resolve_path(root, paths_cfg["features_dir"]),
        models_dir=_resolve_path(root, paths_cfg["models_dir"]),
        sqlite_db=_resolve_path(root, paths_cfg["sqlite_db"]),
        logs_dir=_resolve_path(root, paths_cfg["logs_dir"]),
        instruments_cache=_resolve_path(root, paths_cfg["instruments_cache"]),
        membership_dir=_resolve_path(
            root, paths_cfg.get("membership_dir", "config/membership")
        ),
    )

    uni_cfg = config["universe"]
    universe = UniverseSettings(
        nifty100_csv_url=str(uni_cfg["nifty100_csv_url"]),
        nifty500_csv_url=str(uni_cfg["nifty500_csv_url"]),
        nifty100_csv_fallback_url=str(uni_cfg["nifty100_csv_fallback_url"]),
        nifty500_csv_fallback_url=str(uni_cfg["nifty500_csv_fallback_url"]),
        membership_max_age_days=int(uni_cfg["membership_max_age_days"]),
    )

    options_cfg = config["options"]
    underlyings = [
        OptionUnderlyingSettings(
            name=str(row["name"]),
            spot_quote=str(row["spot_quote"]),
            strikes_each_side=int(row["strikes_each_side"]),
            strike_interval=float(row["strike_interval"]),
        )
        for row in options_cfg["underlyings"]
    ]
    options = OptionsSettings(
        exchange=str(options_cfg["exchange"]),
        underlyings=underlyings,
    )

    fut_cfg = config["futures"]
    futures = FuturesSettings(
        exchange=str(fut_cfg["exchange"]),
        underlyings=[str(x) for x in fut_cfg["underlyings"]],
        contract_count=int(fut_cfg["contract_count"]),
        subscribe_mode=str(fut_cfg.get("subscribe_mode", "full")),
    )

    ingestion_cfg = config["ingestion"]
    ingestion = IngestionSettings(
        flush_interval_seconds=int(ingestion_cfg["flush_interval_seconds"]),
        flush_max_rows=int(ingestion_cfg["flush_max_rows"]),
        websocket_mode=str(ingestion_cfg["websocket_mode"]),
    )

    compaction_cfg = config.get("compaction", {})
    compaction = CompactionSettings(
        archive_minute_files=bool(compaction_cfg.get("archive_minute_files", False)),
        archive_subdir=str(compaction_cfg.get("archive_subdir", "_minute_parts")),
    )

    historical_cfg = config["historical"]
    historical = HistoricalSettings(
        interval=str(historical_cfg["interval"]),
        lookback_days=int(historical_cfg["lookback_days"]),
    )

    features_cfg = config.get("features", {})
    features = FeaturesSettings(
        oi_bucket_minutes=int(features_cfg.get("oi_bucket_minutes", 5)),
        max_tick_return_pct=float(features_cfg.get("max_tick_return_pct", 5.0)),
        options_max_tick_return_pct=float(
            features_cfg.get("options_max_tick_return_pct", 25.0)
        ),
        basis_days_per_year=int(features_cfg.get("basis_days_per_year", 365)),
    )

    signal_cfg = config["signal"]
    tb_cfg = signal_cfg.get("triple_barrier", {})
    triple_barrier = TripleBarrierSettings(
        vol_method=str(tb_cfg.get("vol_method", "ewma")),
        vol_window=int(tb_cfg.get("vol_window", 20)),
        barrier_multiplier=float(tb_cfg.get("barrier_multiplier", 1.0)),
        min_threshold_pct=float(tb_cfg.get("min_threshold_pct", 0.05)),
        max_threshold_pct=float(tb_cfg.get("max_threshold_pct", 5.0)),
        path_rule=str(tb_cfg.get("path_rule", "close_vs_barriers")),
        options_label_mode=str(tb_cfg.get("options_label_mode", "raw_premium")),
    )
    signal = SignalSettings(
        candle_interval_minutes=int(signal_cfg["candle_interval_minutes"]),
        threshold_pct=float(signal_cfg["threshold_pct"]),
        label_mode=str(signal_cfg["label_mode"]),
        triple_barrier=triple_barrier,
        vol_min_periods=int(signal_cfg.get("vol_min_periods", 5)),
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
        universe=universe,
        index_symbols=list(config.get("index_symbols", [])),
        options=options,
        futures=futures,
        ingestion=ingestion,
        compaction=compaction,
        historical=historical,
        features=features,
        signal=signal,
        resilience=resilience,
        kite=kite,
        raw_config=config,
    )
    settings.ensure_directories()
    return settings
