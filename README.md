# CopeTech-Edgar

Reusable SEC EDGAR backend extracted from Sentinel.

## What this package is

`copetech-edgar` is a Python package centered on the namespaced async `EdgarClient`.
`SECDataFetcher` remains a deprecated forwarding facade throughout `0.2.x` so existing
integrations can migrate without a flag day.

Core capabilities today, labelled **Stable** (fixture-backed contract and mature error
semantics), **Beta** (works, actively changing), or
**Experimental** (heuristic output; don't build on the shape):

- **Beta** — Ownership-form parsing (Forms 3/4/5): non-derivative and derivative
  transactions *and* holdings, all reporting owners on joint filings, footnotes, the
  `aff10b5One` Rule 10b5-1 flag, and acquisition/disposition direction taken from the
  filing's own `transactionAcquiredDisposedCode`.
- **Beta** — Form 13F-HR institutional holdings for a manager CIK, plus quarter-over-quarter
  changes. Values are normalized to whole US dollars using the filing-date-dependent Form
  13F unit transition; raw reported value/unit/scale and FIGI remain available. A security
  reported across several `otherManager` rows is rolled up to one position.
- **Beta** — Raw filing/document access backed by an immutable, download-once local store.
- **Beta** — SEC ticker → CIK resolution, sibling share classes, former names, mutual-fund
  series/class mappings, and submissions/company facts retrieval.
- **Beta** — Filing discovery by form type (`4`, `4/A`, `10-K`, `10-Q`, `8-K`, `144`, etc.).
- **Beta** — Point-in-time SEC revenue, basic EPS, and diluted EPS series with provenance,
  concept stitching, revenue-only derived Q4, and quarterly/annual/TTM views. Interim
  TTM diluted EPS uses weighted-average diluted shares rather than adding per-share facts.
- **Beta** — Historical trailing P/E from split-adjusted caller prices and then-known TTM
  diluted EPS, including amendment, split-basis, staleness, and filing provenance.
- **Beta** — Bounded Company Concept and cross-issuer XBRL Frames queries with validated
  paths and daily local caching.
- **Beta** — Provider-neutral read-only agent tool contracts and lazy pandas adapters.
- **Beta** — Form 144 planned-sale records; Form 8-K item-code events.
- **Beta** — Optional file cache and SQLite persistence helpers.
- **Experimental** — Insider signal payloads (`events`, `daily_aggregates`, `clusters`,
  `llm_digest`) and protected insider chart payloads with cached daily OHLC candles.
- **Experimental** — `get_financial_summary` / `get_financial_trend`, and 10-K supply-chain
  relationship extraction.

### Known limitations

- **`get_financial_summary` / `get_financial_trend` can return duplicated and mislabeled
  periods**, because they derive period labels from `fy`/`fp`, which describe the *filing*
  rather than the fact. Prefer `get_financial_series`.
- **Insider signal scores are heuristics for triage**, not investment advice.

See [the architecture and capability audit](docs/audit-2026-07.md) for the full capability
matrix, the evidence behind each status label, and the roadmap.

This package preserves Sentinel's existing SEC backend behavior as closely as possible while making it reusable across projects.

## Repository layout

- `src/copetech_sec/client.py` / `resources/` – namespaced `EdgarClient` public API.
- `src/copetech_sec/sec_api.py` – deprecated `SECDataFetcher` compatibility facade.
- `src/copetech_sec/ownership/` – ownership parsing, normalization, signals, and acquisition.
- `src/copetech_sec/form4_processor.py` – Form 4 compatibility facade and aggregate shaping.
- `src/copetech_sec/document_handler.py` – SEC archive document discovery/download.
- `src/copetech_sec/http_client.py` – async SEC HTTP client (rate limiting + retries).
- `src/copetech_sec/financial_processor.py` – company facts normalization and summary shaping.
- `src/copetech_sec/financial_metrics.py` / `financial_series.py` – metric registry and
  canonical point-in-time series normalization.
- `src/copetech_sec/financial_series_store.py` / `financial_series_service.py` –
  accession-keyed persistence and the acquisition/query boundary.
- `src/copetech_sec/cache_manager.py` / `sql_cache_manager.py` – filesystem/SQLite persistence.
- `tests/test_form4_signals.py` – unit tests for signal event and aggregation logic.

## Install

The core install is deliberately small—an async HTTP client plus the SQLite cache helper:

```bash
uv pip install -e .
```

Optional extras, so parsing a Form 4 doesn't pull a web framework and an AWS SDK:

```bash
uv pip install -e '.[dataframes]'   # pandas: analyze_insider_transactions, market data
uv pip install -e '.[service]'      # the FastAPI demo service + AWS + yfinance
uv pip install -e '.[dev]'          # test suite
```

Python 3.12+ is required.

CopeTech-Edgar is available under the [MIT License](LICENSE).

## CopeNet EDGAR parser quickstart

New integrations use `EdgarClient` resource namespaces.

### 1) Configure a SEC-compliant user agent

SEC requests should include a descriptive user agent. You can pass one directly or use `SEC_API_USER_AGENT`.

```bash
export SEC_API_USER_AGENT="Your Name your-email@example.com"
```

### 2) Fetch a structured insider signal payload (Form 4)

```python
import asyncio
from copetech_sec import EdgarClient


async def main():
    async with EdgarClient() as client:
        payload = await client.ownership.signals(
            ticker="AAPL",
            days_back=180,
            filing_limit=20,
        )

        print(payload["symbol"])
        print(payload["window"])
        print("events:", len(payload["events"]))
        print("daily aggregates:", len(payload["daily_aggregates"]))
        print("llm digest keys:", sorted(payload["llm_digest"].keys()))


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
transactions = await client.ownership.transactions("MSFT", days_back=90)

# First-class Forms 3/4/5 transactions and holdings
ownership = await client.ownership.entries(
    "MSFT",
    forms=["3", "3/A", "4", "4/A", "5", "5/A"],
)

# Filing metadata by form
filings_10k = await client.filings.annual("MSFT")

# Complete filing history with source and cursor metadata
filings_page = await client.filings.query(
    "MSFT",
    "10-K",
    days_back=3650,
    limit=50,
)

# Latest institutional holdings for one manager CIK, e.g. SIG
holdings = await client.institutions.latest_holdings("0001446194", row_limit=25)

# Company facts summary
financials = await client.financials.summary("MSFT")

# Canonical revenue history with filing-date availability and SEC provenance
revenue = await client.financials.series(
    "NVDA",
    metric="revenue",
    frequency="quarterly",  # quarterly | annual | ttm
    alignment="availability",
)

# Aggregate debt wins over current/noncurrent components; missing debt stays unknown.
net_debt = await client.financials.series(
    "SOFI",
    metric="net_debt",
    frequency="annual",
    alignment="availability",
)

# Historical trailing P/E on a split-adjusted price timeline
pe = await client.financials.valuation(
    "NVDA",
    price_observations=split_adjusted_prices,
    split_events=split_history,
)

# One bounded issuer/concept query instead of the full Company Facts payload
assets = await client.financials.concept(
    "MSFT",
    taxonomy="us-gaap",
    concept="Assets",
)

# One SEC calendar frame across issuers
frame = await client.financials.frame(
    taxonomy="us-gaap",
    concept="Assets",
    unit="USD",
    period="CY2025Q4I",
)

# Mutual-fund class resolution keeps SEC series and class identifiers
fund_class = await client.companies.funds.get("LACAX")
```

See [Financial series](docs/financial-series.md) for the data contract,
point-in-time semantics, source tradeoffs, and metric roadmap.

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
- `GET /api/sec/13f/{cik}?row_limit=5000`
- `GET /api/sec/13f/{cik}/changes?days_back=1095&top_n=25`
- `GET /api/sec/debug/13f/sig?row_limit=25`
- `GET /api/sec/insider-signals/{ticker}?days_back=180&filing_limit=40&anchor_type=filing_date`
- `GET /api/sec/insider-signals/{ticker}/clusters?window_days=14&min_unique_insiders=3`
- `GET /api/sec/events/{ticker}?days_back=180&filing_limit=50&categories=exec_change,financial_results`
- `GET /api/sec/planned-sales/{ticker}?days_back=90&filing_limit=25`
- `GET /api/sec/financials/{ticker}/trend?periods=8`

Note: `anchor_type` is accepted and validated but not yet honoured — events are always
anchored on `filing_date`. `get_financial_series` has no HTTP endpoint yet.

Protected demo endpoints require:

- `x-backend-secret`: private proxy secret from `BACKEND_API_SECRET`
- `x-demo-key`: friend/demo invite key from `DEMO_ACCESS_KEYS`, used for rate-limit accounting

Required SEC setting:

- `SEC_API_USER_AGENT` should identify the app and contact email for SEC requests.

Example deployment settings:

- `AWS_REGION=us-east-1`
- `S3_BUCKET=<artifact-bucket>`
- `DYNAMODB_RATE_LIMITS_TABLE=<rate-limit-table>`
- `DYNAMODB_DEMO_JOBS_TABLE=<demo-jobs-table>`
- `DYNAMODB_SEC_CACHE_INDEX_TABLE=<sec-cache-table>`
- `DYNAMODB_RATE_LIMITS_PK=ip`
- `DYNAMODB_DEMO_JOBS_PK=job_id`
- `DYNAMODB_SEC_CACHE_INDEX_PK=cache_key`
- `BACKEND_API_SECRET=<long random secret for the Vercel proxy>`
- `DEMO_ACCESS_KEYS=<comma-separated friend invite keys>`
- `CORS_ALLOW_ORIGINS=https://<frontend-host>,http://localhost:5173`
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

- The library is async-first; use `EdgarClient` as an async context manager so its HTTP
  session is always closed.
- SEC access quality depends on user-agent quality and network/rate-limit conditions.
- Helper scripts under `src/copetech_sec/sec_fetch_*.py` are source-layout wrappers;
  for production integrations, prefer importing the package directly.

## Deeper documentation

- [EDGAR parser overview](docs/edgar-parser-overview.md)
- [EDGAR parser tutorial](docs/edgar-parser-tutorial.md)
- [EDGAR parser architecture and data flow](docs/edgar-parser-architecture.md)
- [EDGAR parser limitations and troubleshooting](docs/edgar-parser-limitations.md)
