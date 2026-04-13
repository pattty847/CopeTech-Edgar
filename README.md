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
- Financial summary extraction from XBRL company facts.
- Optional file cache and SQLite persistence helpers.

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
