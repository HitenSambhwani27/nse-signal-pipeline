"""Read-only HTTP contract for the sibling dashboard. No Kite writes, no scoring."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from nse_pipeline.api.read_model import SqliteUiReadModel
from nse_pipeline.config import Settings, load_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    read = SqliteUiReadModel(settings)

    app = FastAPI(title="NSE pipeline UI API", version="v1")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:8501",
            "http://localhost:8501",
        ],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/api/v1/overview")
    def overview() -> dict[str, Any]:
        return read.overview()

    @app.get("/api/v1/maturity")
    def maturity() -> dict[str, Any]:
        return read.maturity()

    @app.get("/api/v1/signals")
    def signals(limit: int = 200) -> dict[str, Any]:
        return read.signals(limit=limit)

    @app.get("/api/v1/account")
    def account(fill_limit: int = 100) -> dict[str, Any]:
        return read.account(fill_limit=fill_limit)

    @app.get("/api/v1/decisions")
    def decisions(limit: int = 100) -> dict[str, Any]:
        return read.decisions(limit=limit)

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return read.health()

    @app.get("/api/v1/quotes/{symbol:path}")
    def quotes(symbol: str) -> dict[str, Any]:
        return read.quote(symbol)

    @app.get("/api/v1/options/{underlying}/{expiry}/oi")
    def option_oi(underlying: str, expiry: str) -> dict[str, Any]:
        return read.option_oi(underlying, expiry)

    @app.get("/api/v1/options/{underlying}/{expiry}/activity")
    def option_activity(underlying: str, expiry: str) -> dict[str, Any]:
        return read.option_activity(underlying, expiry)

    @app.get("/api/v1/options/{underlying}/{expiry}")
    def option_chain_expiry(underlying: str, expiry: str) -> dict[str, Any]:
        return read.option_chain(underlying, expiry)

    @app.get("/api/v1/options/{underlying}")
    def option_chain(underlying: str) -> dict[str, Any]:
        return read.option_chain(underlying, None)

    @app.get("/api/v1/futures/{underlying}")
    def futures_book(underlying: str) -> dict[str, Any]:
        return read.futures_book(underlying)

    @app.get("/api/v1/market-activity/{symbol:path}")
    def market_activity(symbol: str) -> dict[str, Any]:
        return read.market_activity(symbol)

    @app.get("/api/v1/unusual-activity")
    def unusual_activity(limit: int = 50) -> dict[str, Any]:
        return read.unusual_activity(limit=limit)

    @app.get("/api/v1/charts/{symbol:path}")
    def charts(symbol: str) -> dict[str, Any]:
        return read.charts(symbol)

    @app.get("/api/v1/watchlists")
    def watchlists() -> dict[str, Any]:
        return read.watchlists()

    @app.get("/api/v1/watchlists/quotes")
    def watchlist_quotes() -> dict[str, Any]:
        return read.watchlist_quotes()

    @app.get("/api/v1/cross-market/{underlying}")
    def cross_market(underlying: str) -> dict[str, Any]:
        return read.cross_market(underlying)

    # One-release aliases of the previous /v1 paths.
    @app.get("/v1/health")
    def health_alias() -> dict[str, Any]:
        return read.health()

    @app.get("/v1/maturity")
    def maturity_alias() -> dict[str, Any]:
        return read.maturity()

    @app.get("/v1/signals/latest")
    def signals_alias(limit: int = 200) -> dict[str, Any]:
        return read.signals(limit=limit)

    @app.get("/v1/account/positions")
    def positions_alias() -> dict[str, Any]:
        payload = read.account()
        return {
            "maturity": payload["maturity"],
            "as_of": payload["as_of"],
            "positions": payload["positions"],
        }

    @app.get("/v1/account/margins")
    def margins_alias() -> dict[str, Any]:
        payload = read.account()
        return {
            "maturity": payload["maturity"],
            "as_of": payload["as_of"],
            "margins": payload["margins"],
        }

    @app.get("/v1/account/fills")
    def fills_alias(limit: int = 100) -> dict[str, Any]:
        payload = read.account(fill_limit=limit)
        return {
            "maturity": payload["maturity"],
            "as_of": payload["as_of"],
            "fills": payload["fills"],
        }

    @app.get("/v1/decisions")
    def decisions_alias(limit: int = 100) -> dict[str, Any]:
        return read.decisions(limit=limit)

    @app.get("/v1/outcomes")
    def outcomes_alias(limit: int = 100) -> dict[str, Any]:
        payload = read.decisions(limit=limit)
        return {
            "maturity": payload["maturity"],
            "as_of": payload["as_of"],
            "outcomes": read.store.fetch_outcomes(limit=limit),
            "class_counts": payload["class_counts"],
        }

    return app
