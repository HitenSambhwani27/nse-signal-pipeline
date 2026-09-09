"""Read-only HTTP contract for the sibling dashboard. No Kite writes, no scoring."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from nse_pipeline.api.read_model import SqliteUiReadModel
from nse_pipeline.config import Settings, load_settings
from nse_pipeline.live.frames import parse_groups
from nse_pipeline.live.source import SqlitePollSource
from nse_pipeline.live.stream import StreamAdmissionError, StreamRegistry, stream_events
from nse_pipeline.live.subscriptions import SubscriptionManager


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    read = SqliteUiReadModel(settings)
    read.store.busy_timeout_ms = int(settings.stream.api_busy_timeout_ms)
    live_source = SqlitePollSource(settings, read.store)
    stream_registry = StreamRegistry(settings, live_source)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.live_source = live_source
        app.state.stream_registry = stream_registry
        task = asyncio.create_task(live_source.run_forever())
        try:
            yield
        finally:
            await live_source.stop()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="NSE pipeline UI API", version="v1", lifespan=lifespan)
    app.state.live_source = live_source
    app.state.stream_registry = stream_registry
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:8501",
            "http://localhost:8501",
        ],
        allow_methods=["GET", "POST"],
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
    def charts(symbol: str, interval: str | None = None) -> dict[str, Any]:
        return read.charts(symbol, interval=interval)

    @app.get("/api/v1/candles/{symbol:path}")
    def candles(
        symbol: str,
        interval: str | None = None,
        range_from: str | None = Query(None, alias="from"),
        range_to: str | None = Query(None, alias="to"),
        max_points: int = 500,
    ) -> dict[str, Any]:
        return read.candles(
            symbol,
            interval=interval,
            range_from=range_from,
            range_to=range_to,
            max_points=max_points,
        )

    @app.get("/api/v1/series/{symbol:path}")
    def series(
        symbol: str,
        fields: str | None = None,
        interval: str | None = None,
        range_from: str | None = Query(None, alias="from"),
        range_to: str | None = Query(None, alias="to"),
    ) -> dict[str, Any]:
        return read.series(
            symbol,
            fields=fields,
            interval=interval,
            range_from=range_from,
            range_to=range_to,
        )

    @app.get("/api/v1/stream")
    async def stream(
        request: Request,
        tokens: str | None = None,
        groups: str | None = None,
    ):
        registry: StreamRegistry = request.app.state.stream_registry
        source: SqlitePollSource = request.app.state.live_source
        try:
            parsed, rejected = registry.validate_tokens(tokens)
            group_list = parse_groups(groups)
            await registry.acquire()
        except StreamAdmissionError as exc:
            headers = {}
            if exc.retry_after is not None:
                headers["Retry-After"] = str(exc.retry_after)
            return JSONResponse(
                {"error": exc.reason, "reason": exc.reason},
                status_code=exc.status,
                headers=headers,
            )
        session = source._session or source._session_snapshot()
        last_id = request.headers.get("last-event-id") or request.headers.get("Last-Event-ID")

        async def generate():
            try:
                async for chunk in stream_events(
                    registry=registry,
                    source=source,
                    tokens=parsed,
                    groups=group_list,
                    last_event_id=last_id,
                    session=session,
                    rejected=rejected,
                ):
                    yield chunk
            finally:
                await registry.release()

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-store",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @app.post("/api/v1/subscriptions")
    def subscriptions(body: dict[str, Any]) -> Any:
        cache = read._load_instrument_cache() or {}
        manager = SubscriptionManager(
            settings,
            read.store,
            static_full=[],
            static_quote=[],
            instrument_cache=cache,
        )
        try:
            request = manager.validate_request(
                token=body.get("token"),
                symbol=body.get("symbol"),
                requester=str(body.get("requester") or ""),
                capabilities=list(body.get("capabilities") or ["price"]),
                action=str(body.get("action") or "subscribe"),
                ttl_seconds=body.get("ttl_seconds") or body.get("ttl"),
            )
        except ValueError as exc:
            return JSONResponse(
                {"error": str(exc), "reason": str(exc)},
                status_code=400,
            )
        result = manager.enqueue(request)
        return {"ok": True, **result}

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
