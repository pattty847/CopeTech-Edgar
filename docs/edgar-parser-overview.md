# CopeNet EDGAR parser overview

## Purpose

The CopeNet EDGAR parser is the SEC ingestion/parsing subsystem in this repository. It is packaged as
`copetech-edgar` and exposed primarily through `copetech_sec.SECDataFetcher`.

The goal is to provide reusable async primitives for:
- SEC metadata retrieval (submissions, company facts).
- Filing/document resolution and download from SEC archives.
- Form 4 insider transaction parsing and signal generation.
- Financial metric extraction from XBRL company facts.

## Component type

This is a **library-style subsystem** (not a standalone daemon/service). It is intended to be imported
and used by other applications.

## Main entrypoint

- `SECDataFetcher` (`src/copetech_sec/sec_api.py`)

`SECDataFetcher` composes and delegates to:
- `SecHttpClient` for HTTP transport and SEC-specific request behavior.
- `SecCacheManager` for filesystem caching.
- `FilingDocumentHandler` for archive document resolution/download.
- `Form4Processor` for Form 4 parsing + signal payload logic.
- `FinancialDataProcessor` for company facts summarization.
- `SqlCacheManager` for optional persisted outputs.

## What is currently supported

### SEC endpoints and metadata
- Ticker→CIK map retrieval from `https://www.sec.gov/files/company_tickers.json`.
- Company submissions from `https://data.sec.gov/submissions/CIK{cik}.json`.
- Company facts from `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`.

### Filing retrieval helpers
- Generic by-form retrieval via `get_filings_by_form`.
- Convenience methods for Form 4, 10-K, 10-Q, and 8-K.

### Form 4 parsing and signals
- XML parsing for non-derivative and derivative transactions.
- Transaction code mapping and acquisition/disposition classification.
- Event normalization with amendment-aware dedupe behavior.
- Daily aggregates and digest payload geared toward downstream summarization/LLM use.

### Financial summaries
- Key metric extraction from company facts (revenue, net income, EPS, assets/liabilities/equity,
  operating/investing/financing cash flow).
- Quarterly/annual shaping in the processor.

## Tests that currently anchor behavior

- `tests/test_form4_signals.py` validates signal classification, amendment replacement, daily aggregate behavior,
  and `llm_digest` shape.

## Suggested first-use workflow

1. Set `SEC_API_USER_AGENT` (or pass `user_agent=` when creating `SECDataFetcher`).
2. Initialize `SECDataFetcher` in async code.
3. Call a high-level method such as `get_insider_signal_payload`.
4. Close the fetcher (`await fetcher.close()`).
