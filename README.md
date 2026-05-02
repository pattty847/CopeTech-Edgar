# CopeTech-Edgar

Reusable SEC EDGAR backend extracted from Sentinel.

## What this package is

`copetech-edgar` is a Python package centered on a single async facade: `SECDataFetcher`.
It wraps SEC API fetches, filing document retrieval, and parser/processor logic into a
consistent interface for downstream services.

Core capabilities today:
- SEC ticker → CIK resolution and submissions/company facts retrieval.
- Filing discovery by form type (`4`, `4/A`, `10-K`, `10-Q`, `8-K`, etc.).
- Form 4 XML parsing into normalized insider transactions.
- Insider signal payload generation (`events`, `daily_aggregates`, `llm_digest`).
- Protected insider chart payloads with cached daily OHLC candles for demo overlays.
- Financial summary extraction from XBRL company facts.
- Optional file cache and SQLite persistence helpers.

This package preserves Sentinel's existing SEC backend behavior as closely as possible while making it reusable across projects.

## Repository layout

- `src/copetech_sec/sec_api.py` – `SECDataFetcher` orchestration facade.
- `src/copetech_sec/form4_processor.py` – Form 4 parsing + signal normalization/aggregation.
- `src/copetech_sec/document_handler.py` – SEC archive document discovery/download.
- `src/copetech_sec/http_client.py` – async SEC HTTP client (rate limiting + retries).
- `src/copetech_sec/financial_processor.py` – company facts normalization and summary shaping.
- `src/copetech_sec/cache_manager.py` / `sql_cache_manager.py` – filesystem/SQLite persistence.
- `tests/test_form4_signals.py` – unit tests for signal event and aggregation logic.

## Install

```bash
uv pip install -e .
```

Python 3.12+ is required.

## CopeNet EDGAR parser quickstart

The parser is used via `SECDataFetcher` methods (async).

### 1) Configure a SEC-compliant user agent

SEC requests should include a descriptive user agent. You can pass one directly or use `SEC_API_USER_AGENT`.

```bash
export SEC_API_USER_AGENT="Your Name your-email@example.com"
```

### 2) Fetch a structured insider signal payload (Form 4)

```python
import asyncio
from copetech_sec import SECDataFetcher


async def main():
    fetcher = SECDataFetcher()
    try:
        payload = await fetcher.get_insider_signal_payload(
            ticker="AAPL",
            days_back=180,
            filing_limit=20,
        )

        print(payload["symbol"])
        print(payload["window"])
        print("events:", len(payload["events"]))
        print("daily aggregates:", len(payload["daily_aggregates"]))
        print("llm digest keys:", sorted(payload["llm_digest"].keys()))
    finally:
        await fetcher.close()


asyncio.run(main())
```

Expected top-level payload shape:

```json
{
  "symbol": "AAPL",
  "window": {"days_back": 180, "filing_limit": 20},
  "as_of": "...UTC timestamp...",
  "events": [...],
  "daily_aggregates": [...],
  "llm_digest": {
    "summary": {...},
    "key_events": [...],
    "anomalies": [...],
    "caveats": [...]
  }
}
```

### 3) Other common workflows

```python
# Recent parsed Form 4 transactions for display
transactions = await fetcher.get_recent_insider_transactions("MSFT", days_back=90)

# Filing metadata by form
filings_10k = await fetcher.fetch_annual_reports("MSFT")

# Company facts summary
financials = await fetcher.get_financial_summary("MSFT")
```

## HTTP API

The repo also exposes a small FastAPI service for cloud demos.

```bash
cp .env.example .env
docker compose up --build
```

Local endpoints:

- `GET /health`
- `GET /config`
- `GET /api/sec/company/{ticker}`
- `GET /api/sec/transactions/{ticker}?days_back=180&filing_limit=25`
- `GET /sec/insiders?symbol=AAPL`
- `GET /api/sec/insiders?symbol=AAPL`
- `GET /api/sec/chart?symbol=AAPL&days_back=180&filing_limit=40`
- `GET /api/sec/insider-signals/{ticker}?days_back=180&filing_limit=40&anchor_type=filing_date`

Protected demo endpoints require:

- `x-backend-secret`: private proxy secret from `BACKEND_API_SECRET`
- `x-demo-key`: friend/demo invite key from `DEMO_ACCESS_KEYS`, used for rate-limit accounting

Required SEC setting:

- `SEC_API_USER_AGENT` should identify the app and contact email for SEC requests.

AWS deployment settings:

- `AWS_REGION=us-east-1`
- `S3_BUCKET=copeharder-artifacts`
- `DYNAMODB_RATE_LIMITS_TABLE=rate_limits`
- `DYNAMODB_DEMO_JOBS_TABLE=demo_jobs`
- `DYNAMODB_SEC_CACHE_INDEX_TABLE=sec_cache_index`
- `DYNAMODB_RATE_LIMITS_PK=ip`
- `DYNAMODB_DEMO_JOBS_PK=job_id`
- `DYNAMODB_SEC_CACHE_INDEX_PK=cache_key`
- `BACKEND_API_SECRET=<long random secret for the Vercel proxy>`
- `DEMO_ACCESS_KEYS=<comma-separated friend invite keys>`
- `CORS_ALLOW_ORIGINS=https://lolcopeharder.com,https://www.lolcopeharder.com,http://localhost:5173`
- `MARKET_CACHE_TTL_SECONDS=21600`

The service never hardcodes AWS credentials. On EC2, attach an instance profile/IAM role with scoped DynamoDB and S3 permissions. For local testing, use your normal AWS CLI profile if you want DynamoDB writes to work.

Rate limits are keyed by `demo_key + IP + YYYY-MM-DD`. The demo key is hashed before it is written to DynamoDB.

The current AWS tables use these partition keys:

- `DYNAMODB_RATE_LIMITS_PK`
- `DYNAMODB_DEMO_JOBS_PK`
- `DYNAMODB_SEC_CACHE_INDEX_PK`

If DynamoDB is unavailable locally, rate limiting falls back to in-memory counters so the API can still run.

## EC2 shape

Suggested first deployment on the Ubuntu EC2 box:

```bash
git clone <repo-url>
cd CopeTech-Edgar
cp .env.example .env
docker compose up -d --build
```

Put Caddy or nginx in front of the container for public HTTPS, then point `api.lolcopeharder.com` at the EC2 instance.

See `README_DEPLOY.md` for exact EC2 commands.

## Caveats and current limitations

- The library is async-first; callers should use an event loop and close the fetcher session.
- SEC access quality depends on user-agent quality and network/rate-limit conditions.
- Helper scripts under `src/copetech_sec/sec_fetch_*.py` are source-layout wrappers;
  for production integrations, prefer importing the package directly.

## Deeper documentation

- [EDGAR parser overview](docs/edgar-parser-overview.md)
- [EDGAR parser tutorial](docs/edgar-parser-tutorial.md)
- [EDGAR parser architecture and data flow](docs/edgar-parser-architecture.md)
- [EDGAR parser limitations and troubleshooting](docs/edgar-parser-limitations.md)
