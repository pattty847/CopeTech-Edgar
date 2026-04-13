# CopeNet EDGAR parser tutorial

This tutorial uses actual public entrypoints in this repository.

## Prerequisites

- Python 3.12+
- Package installed in your environment
- Internet access to SEC endpoints
- SEC-compliant user agent

```bash
uv pip install -e .
export SEC_API_USER_AGENT="Your Name your-email@example.com"
```

## Tutorial 1: Build insider signal payload for a ticker

```python
import asyncio
from copetech_sec import SECDataFetcher


async def run():
    fetcher = SECDataFetcher()
    try:
        payload = await fetcher.get_insider_signal_payload(
            ticker="AAPL",
            days_back=180,
            use_cache=True,
            filing_limit=20,
        )

        print("symbol:", payload["symbol"])
        print("window:", payload["window"])
        print("event_count:", len(payload["events"]))
        print("aggregate_count:", len(payload["daily_aggregates"]))
        print("digest_fields:", sorted(payload["llm_digest"].keys()))

        if payload["events"]:
            sample_event = payload["events"][0]
            print("sample_event_keys:", sorted(sample_event.keys())[:12], "...")
    finally:
        await fetcher.close()


asyncio.run(run())
```

What this does:
1. Fetches Form 4/4-A filing metadata for the lookback window.
2. Downloads Form 4 XML per filing.
3. Parses transactions.
4. Normalizes events and applies amendment dedupe.
5. Builds daily aggregates and digest output.

## Tutorial 2: Get display-friendly recent insider transactions

```python
import asyncio
from copetech_sec import SECDataFetcher


async def run():
    fetcher = SECDataFetcher()
    try:
        txs = await fetcher.get_recent_insider_transactions(
            ticker="MSFT",
            days_back=90,
            filing_limit=10,
        )
        print("transactions:", len(txs))
        if txs:
            print(txs[0])
    finally:
        await fetcher.close()


asyncio.run(run())
```

This path is useful for UI-like transaction tables.

## Tutorial 3: Retrieve financial summary

```python
import asyncio
from copetech_sec import SECDataFetcher


async def run():
    fetcher = SECDataFetcher()
    try:
        summary = await fetcher.get_financial_summary("MSFT")
        print(summary.keys() if summary else "No summary")
    finally:
        await fetcher.close()


asyncio.run(run())
```

## Troubleshooting quick hits

- `None`/empty results for SEC requests:
  - check `SEC_API_USER_AGENT` format and network access.
  - verify ticker is valid and has recent filings of requested form.
- Slow runs:
  - expected for first fetch due to network calls; cache can improve subsequent calls.
- Script confusion:
  - prefer direct imports (`from copetech_sec import SECDataFetcher`) over legacy script wrappers.
