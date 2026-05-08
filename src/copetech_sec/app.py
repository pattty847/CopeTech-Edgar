from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import re
from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from .aws_resources import AwsResourceManager
from .market_data import PriceCandleFetcher
from .sec_api import SECDataFetcher
from .settings import ServiceSettings
from .thirteenf_processor import SIG_CIK, normalize_cik


TICKER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9.-]{0,9}$")

settings = ServiceSettings.from_env()
logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
aws_resources = AwsResourceManager(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    fetcher = getattr(app.state, "fetcher", None)
    if fetcher is not None:
        await fetcher.close()


app = FastAPI(
    title="CopeTech SEC API",
    version="0.1.0",
    description="HTTP API for CopeTech EDGAR/SEC demos.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_allow_origins),
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["x-demo-key", "x-backend-secret", "content-type"],
)


def normalize_ticker(ticker: str) -> str:
    value = ticker.strip().upper()
    if not TICKER_RE.match(value):
        raise HTTPException(status_code=400, detail="Invalid ticker symbol.")
    return value


def normalize_manager_cik(cik: str) -> str:
    try:
        return normalize_cik(cik)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def get_client_id(request: Request) -> str:
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def get_fetcher() -> SECDataFetcher:
    if not hasattr(app.state, "fetcher"):
        app.state.fetcher = SECDataFetcher(
            user_agent=settings.sec_user_agent,
            cache_dir=settings.cache_dir,
            rate_limit_sleep=settings.sec_request_sleep,
        )
    return app.state.fetcher


def get_price_fetcher() -> PriceCandleFetcher:
    if not hasattr(app.state, "price_fetcher"):
        app.state.price_fetcher = PriceCandleFetcher(
            cache_dir=settings.cache_dir,
            ttl_seconds=settings.market_cache_ttl_seconds,
        )
    return app.state.price_fetcher


def get_demo_key(request: Request) -> str:
    demo_key = request.headers.get("x-demo-key") or request.query_params.get("demo_key")
    if not demo_key or not demo_key.strip():
        raise HTTPException(status_code=401, detail="Missing demo access key.")
    cleaned = demo_key.strip()
    if not settings.demo_access_keys:
        raise HTTPException(status_code=503, detail="Demo access keys are not configured.")
    if not settings.demo_key_allowed(cleaned):
        raise HTTPException(status_code=403, detail="Invalid demo access key.")
    return cleaned


def enforce_backend_secret(request: Request) -> None:
    if settings.secret_matches(request.headers.get("x-backend-secret")):
        return
    raise HTTPException(status_code=401, detail="Invalid backend credentials.")


async def enforce_demo_access(request: Request) -> dict:
    enforce_backend_secret(request)
    result = aws_resources.check_rate_limit(get_demo_key(request), get_client_id(request))
    if not result["allowed"]:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Daily demo limit reached.",
                "limit": result["limit"],
                "count": result["count"],
            },
        )
    return result


DemoAccess = Annotated[dict, Depends(enforce_demo_access)]
Fetcher = Annotated[SECDataFetcher, Depends(get_fetcher)]
PriceFetcher = Annotated[PriceCandleFetcher, Depends(get_price_fetcher)]


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "service": "copetech-sec-api", "region": settings.aws_region}


@app.get("/config")
async def config() -> dict:
    return aws_resources.public_config()


@app.get("/api/sec/company/{ticker}")
async def company_info(ticker: str, fetcher: Fetcher, _demo_access: DemoAccess) -> dict:
    normalized = normalize_ticker(ticker)
    data = await fetcher.get_company_info(normalized)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No company info found for {normalized}.")
    aws_resources.record_sec_cache_lookup(normalized, "company_info", True, {"source": "sec_fetcher"})
    return data


@app.get("/api/sec/transactions/{ticker}")
async def insider_transactions(
    ticker: str,
    fetcher: Fetcher,
    _demo_access: DemoAccess,
    days_back: Annotated[int, Query(ge=1, le=730)] = 180,
    filing_limit: Annotated[int, Query(ge=1, le=80)] = 25,
) -> dict:
    normalized = normalize_ticker(ticker)
    transactions = await fetcher.get_recent_insider_transactions(
        normalized,
        days_back=days_back,
        filing_limit=filing_limit,
    )
    aws_resources.record_sec_cache_lookup(
        normalized,
        "transactions",
        True,
        {"days_back": days_back, "filing_limit": filing_limit, "count": len(transactions)},
    )
    return {"ticker": normalized, "days_back": days_back, "transactions": transactions}


async def build_insiders_payload(
    ticker: str,
    fetcher: SECDataFetcher,
    days_back: int,
    filing_limit: int,
    anchor_type: str,
) -> dict:
    normalized = normalize_ticker(ticker)
    payload = await fetcher.get_insider_signal_payload(
        normalized,
        days_back=days_back,
        filing_limit=filing_limit,
        anchor_type=anchor_type,
    )
    aws_resources.record_sec_cache_lookup(
        normalized,
        "insiders",
        True,
        {
            "days_back": days_back,
            "filing_limit": filing_limit,
            "anchor_type": anchor_type,
            "event_count": len(payload.get("events", [])),
        },
    )
    return payload


@app.get("/sec/insiders")
@app.get("/api/sec/insiders")
async def insiders_by_symbol(
    symbol: Annotated[str, Query(min_length=1, max_length=10)],
    fetcher: Fetcher,
    _demo_access: DemoAccess,
    days_back: Annotated[int, Query(ge=1, le=730)] = 180,
    filing_limit: Annotated[int, Query(ge=1, le=120)] = 40,
    anchor_type: Annotated[str, Query(pattern="^(filing_date|transaction_date)$")] = "filing_date",
) -> dict:
    return await build_insiders_payload(symbol, fetcher, days_back, filing_limit, anchor_type)


@app.get("/api/sec/chart")
async def insider_chart(
    symbol: Annotated[str, Query(min_length=1, max_length=10)],
    fetcher: Fetcher,
    price_fetcher: PriceFetcher,
    _demo_access: DemoAccess,
    days_back: Annotated[int, Query(ge=1, le=730)] = 180,
    filing_limit: Annotated[int, Query(ge=1, le=120)] = 40,
    anchor_type: Annotated[str, Query(pattern="^(filing_date|transaction_date)$")] = "filing_date",
) -> dict:
    normalized = normalize_ticker(symbol)
    payload = await build_insiders_payload(normalized, fetcher, days_back, filing_limit, anchor_type)
    payload["candles"] = await price_fetcher.get_daily_candles(normalized, days_back)
    aws_resources.record_sec_cache_lookup(
        normalized,
        "chart",
        True,
        {
            "days_back": days_back,
            "filing_limit": filing_limit,
            "anchor_type": anchor_type,
            "candle_count": len(payload["candles"]),
        },
    )
    return payload


@app.get("/api/sec/13f/{cik}")
async def thirteenf_holdings(
    cik: str,
    fetcher: Fetcher,
    _demo_access: DemoAccess,
    days_back: Annotated[int, Query(ge=1, le=3650)] = 365 * 3,
    row_limit: Annotated[int, Query(ge=1, le=5000)] = 5000,
) -> dict:
    normalized = normalize_manager_cik(cik)
    try:
        payload = await fetcher.get_latest_13f_holdings(
            normalized,
            days_back=days_back,
            row_limit=row_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if payload.get("filing") is None:
        raise HTTPException(status_code=404, detail=f"No 13F-HR filing found for CIK {normalized}.")
    aws_resources.record_sec_cache_lookup(
        normalized,
        "13f_holdings",
        True,
        {"days_back": days_back, "row_limit": row_limit, "holdings_count": payload.get("holdings_count", 0)},
    )
    return payload


@app.get("/api/sec/debug/13f/sig")
async def sig_13f_debug(
    fetcher: Fetcher,
    _demo_access: DemoAccess,
    row_limit: Annotated[int, Query(ge=1, le=100)] = 25,
) -> dict:
    payload = await fetcher.get_latest_13f_holdings(SIG_CIK, row_limit=row_limit)
    if payload.get("filing") is None:
        raise HTTPException(status_code=404, detail="No 13F-HR filing found for SIG.")
    return payload


@app.get("/api/sec/insider-signals/{ticker}")
async def insider_signals(
    ticker: str,
    fetcher: Fetcher,
    _demo_access: DemoAccess,
    days_back: Annotated[int, Query(ge=1, le=730)] = 180,
    filing_limit: Annotated[int, Query(ge=1, le=120)] = 40,
    anchor_type: Annotated[str, Query(pattern="^(filing_date|transaction_date)$")] = "filing_date",
) -> dict:
    return await build_insiders_payload(ticker, fetcher, days_back, filing_limit, anchor_type)


def run() -> None:
    uvicorn.run("copetech_sec.app:app", host="0.0.0.0", port=settings.port)


if __name__ == "__main__":
    run()
