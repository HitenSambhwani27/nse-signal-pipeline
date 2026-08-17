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
class SessionSettings:
    """NSE cash session bounds for partial-coverage detection."""

    timezone: str
    market_open: str  # HH:MM IST
    market_close: str  # HH:MM IST
    open_grace_minutes: int
    close_grace_minutes: int


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
    # weekly = Nifty Tuesday series; monthly = Bank Nifty (its only series).
    # Nifty monthly is out of scope — see TRADE_OFFS.md.
    series: str = "weekly"


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
    equity_start_date: str = "2022-01-01"
    minute_lookback_days: int | None = None
    minute_interval: str = "minute"
    daily_interval: str = "day"
    rate_limit_per_second: float = 3.0
    sleep_seconds: float = 0.40
    retry_max: int = 5
    retry_429_cooldown_seconds: float = 10.0
    http_timeout_seconds: float = 45.0
    http_timeout_ceiling_seconds: float = 90.0
    retry_backoff_seconds: float = 2.0
    retry_backoff_multiplier: float = 2.0
    interval_max_days: dict[str, int] = field(
        default_factory=lambda: {
            "minute": 60,
            "3minute": 100,
            "5minute": 100,
            "10minute": 100,
            "15minute": 200,
            "30minute": 200,
            "60minute": 400,
            "day": 2000,
        }
    )


@dataclass
class FeaturesSettings:
    oi_bucket_minutes: int
    max_tick_return_pct: float
    options_max_tick_return_pct: float
    basis_days_per_year: int
    corporate_action_gap_pct: float = 15.0
    corporate_action_index_wide_pct: float = 5.0


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
class BacktestSettings:
    train_days: int = 20
    test_days: int = 5
    step_days: int = 5
    min_train_days: int = 5
    overfit_hit_rate_gap: float = 0.15
    annualization_days: int = 252
    signal_threshold: float = 0.0


@dataclass
class InstrumentCostSettings:
    brokerage_flat: float = 20.0
    stt_sell_pct: float = 0.0
    slippage_tick: float = 0.0
    slippage_pct: float = 0.0
    slippage_ticks: int = 0
    tick_size: float = 0.05
    spread_crossing_ticks: int = 0


@dataclass
class CostsSettings:
    equity: InstrumentCostSettings = field(default_factory=InstrumentCostSettings)
    options: InstrumentCostSettings = field(default_factory=InstrumentCostSettings)
    futures: InstrumentCostSettings = field(default_factory=InstrumentCostSettings)


@dataclass
class TrainingSettings:
    include_historical_partial: dict[str, bool] = field(
        default_factory=lambda: {"equity": True, "options": True, "futures": True}
    )
    coarse_features: dict[str, list[str]] = field(default_factory=dict)
    fine_features: dict[str, list[str]] = field(default_factory=dict)
    blend_coarse_weight: float = 0.6
    blend_fine_weight: float = 0.4


@dataclass
class MaturityGateSettings:
    """Pooled live trading days per instrument class — not per contract."""

    suppress_below_days: int = 10
    provisional_below_days: int = 60


@dataclass
class LiveSignalSettings:
    score_every_seconds: int = 5


@dataclass
class RetrainSettings:
    frequency: str = "weekly"
    window_days: int = 60
    auto_promote: bool = False


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
    session: SessionSettings
    historical: HistoricalSettings
    features: FeaturesSettings
    signal: SignalSettings
    resilience: ResilienceSettings
    kite: KiteCredentials
    backtest: BacktestSettings = field(default_factory=BacktestSettings)
    costs: CostsSettings = field(default_factory=CostsSettings)
    training: TrainingSettings = field(default_factory=TrainingSettings)
    maturity_gate: MaturityGateSettings = field(default_factory=MaturityGateSettings)
    live_signal: LiveSignalSettings = field(default_factory=LiveSignalSettings)
    retrain: RetrainSettings = field(default_factory=RetrainSettings)
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
    underlyings = []
    for row in options_cfg["underlyings"]:
        name = str(row["name"])
        default_series = "monthly" if name.upper() == "BANKNIFTY" else "weekly"
        underlyings.append(
            OptionUnderlyingSettings(
                name=name,
                spot_quote=str(row["spot_quote"]),
                strikes_each_side=int(row["strikes_each_side"]),
                strike_interval=float(row["strike_interval"]),
                series=str(row.get("series") or default_series),
            )
        )
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

    session_cfg = config.get("session", {})
    session = SessionSettings(
        timezone=str(session_cfg.get("timezone", "Asia/Kolkata")),
        market_open=str(session_cfg.get("market_open", "09:15")),
        market_close=str(session_cfg.get("market_close", "15:30")),
        open_grace_minutes=int(session_cfg.get("open_grace_minutes", 15)),
        close_grace_minutes=int(session_cfg.get("close_grace_minutes", 15)),
    )

    historical_cfg = config["historical"]
    minute_lb = historical_cfg.get("minute_lookback_days")
    interval_caps = historical_cfg.get("interval_max_days") or {}
    default_caps = {
        "minute": 60,
        "3minute": 100,
        "5minute": 100,
        "10minute": 100,
        "15minute": 200,
        "30minute": 200,
        "60minute": 400,
        "day": 2000,
    }
    default_caps.update({str(k): int(v) for k, v in interval_caps.items()})
    historical = HistoricalSettings(
        interval=str(historical_cfg["interval"]),
        lookback_days=int(historical_cfg["lookback_days"]),
        equity_start_date=str(historical_cfg.get("equity_start_date", "2022-01-01")),
        minute_lookback_days=int(minute_lb) if minute_lb is not None else None,
        minute_interval=str(historical_cfg.get("minute_interval", "minute")),
        daily_interval=str(historical_cfg.get("daily_interval", "day")),
        rate_limit_per_second=float(historical_cfg.get("rate_limit_per_second", 3.0)),
        sleep_seconds=float(historical_cfg.get("sleep_seconds", 0.40)),
        retry_max=int(historical_cfg.get("retry_max", 5)),
        retry_429_cooldown_seconds=float(
            historical_cfg.get("retry_429_cooldown_seconds", 10.0)
        ),
        http_timeout_seconds=float(historical_cfg.get("http_timeout_seconds", 45.0)),
        http_timeout_ceiling_seconds=float(
            historical_cfg.get("http_timeout_ceiling_seconds", 90.0)
        ),
        retry_backoff_seconds=float(historical_cfg.get("retry_backoff_seconds", 2.0)),
        retry_backoff_multiplier=float(
            historical_cfg.get("retry_backoff_multiplier", 2.0)
        ),
        interval_max_days=default_caps,
    )

    features_cfg = config.get("features", {})
    features = FeaturesSettings(
        oi_bucket_minutes=int(features_cfg.get("oi_bucket_minutes", 5)),
        max_tick_return_pct=float(features_cfg.get("max_tick_return_pct", 5.0)),
        options_max_tick_return_pct=float(
            features_cfg.get("options_max_tick_return_pct", 25.0)
        ),
        basis_days_per_year=int(features_cfg.get("basis_days_per_year", 365)),
        corporate_action_gap_pct=float(
            features_cfg.get("corporate_action_gap_pct", 15.0)
        ),
        corporate_action_index_wide_pct=float(
            features_cfg.get("corporate_action_index_wide_pct", 5.0)
        ),
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

    bt_cfg = config.get("backtest", {})
    backtest = BacktestSettings(
        train_days=int(bt_cfg.get("train_days", 20)),
        test_days=int(bt_cfg.get("test_days", 5)),
        step_days=int(bt_cfg.get("step_days", 5)),
        min_train_days=int(bt_cfg.get("min_train_days", 5)),
        overfit_hit_rate_gap=float(bt_cfg.get("overfit_hit_rate_gap", 0.15)),
        annualization_days=int(bt_cfg.get("annualization_days", 252)),
        signal_threshold=float(bt_cfg.get("signal_threshold", 0.0)),
    )

    def _cost_block(block: dict[str, Any] | None) -> InstrumentCostSettings:
        b = block or {}
        return InstrumentCostSettings(
            brokerage_flat=float(b.get("brokerage_flat", 20.0)),
            stt_sell_pct=float(b.get("stt_sell_pct", 0.0)),
            slippage_tick=float(b.get("slippage_tick", 0.0)),
            slippage_pct=float(b.get("slippage_pct", 0.0)),
            slippage_ticks=int(b.get("slippage_ticks", 0)),
            tick_size=float(b.get("tick_size", 0.05)),
            spread_crossing_ticks=int(b.get("spread_crossing_ticks", 0)),
        )

    costs_cfg = config.get("costs", {})
    costs = CostsSettings(
        equity=_cost_block(costs_cfg.get("equity")),
        options=_cost_block(costs_cfg.get("options")),
        futures=_cost_block(costs_cfg.get("futures")),
    )

    tr_cfg = config.get("training", {})
    include_hp = tr_cfg.get("include_historical_partial") or {}
    blend_cfg = tr_cfg.get("blend") or {}
    training = TrainingSettings(
        include_historical_partial={
            "equity": bool(include_hp.get("equity", True)),
            "options": bool(include_hp.get("options", True)),
            "futures": bool(include_hp.get("futures", True)),
        },
        coarse_features={
            str(k): [str(x) for x in v]
            for k, v in (tr_cfg.get("coarse_features") or {}).items()
        },
        fine_features={
            str(k): [str(x) for x in v]
            for k, v in (tr_cfg.get("fine_features") or {}).items()
        },
        blend_coarse_weight=float(blend_cfg.get("coarse_weight", 0.6)),
        blend_fine_weight=float(blend_cfg.get("fine_weight", 0.4)),
    )

    mg_cfg = config.get("maturity_gate", {})
    maturity_gate = MaturityGateSettings(
        suppress_below_days=int(mg_cfg.get("suppress_below_days", 10)),
        provisional_below_days=int(mg_cfg.get("provisional_below_days", 60)),
    )

    ls_cfg = config.get("live_signal", {})
    live_signal = LiveSignalSettings(
        score_every_seconds=int(ls_cfg.get("score_every_seconds", 5)),
    )

    rt_cfg = config.get("retrain", {})
    retrain = RetrainSettings(
        frequency=str(rt_cfg.get("frequency", "weekly")),
        window_days=int(rt_cfg.get("window_days", 60)),
        auto_promote=bool(rt_cfg.get("auto_promote", False)),
    )

    settings = Settings(
        paths=paths,
        universe=universe,
        index_symbols=list(config.get("index_symbols", [])),
        options=options,
        futures=futures,
        ingestion=ingestion,
        compaction=compaction,
        session=session,
        historical=historical,
        features=features,
        signal=signal,
        resilience=resilience,
        kite=kite,
        backtest=backtest,
        costs=costs,
        training=training,
        maturity_gate=maturity_gate,
        live_signal=live_signal,
        retrain=retrain,
        raw_config=config,
    )
    settings.ensure_directories()
    return settings
