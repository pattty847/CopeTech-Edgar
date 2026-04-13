# CopeNet EDGAR parser architecture and data flow

## High-level architecture

`SECDataFetcher` is the orchestrator/facade. It owns the major components and routes calls to them.

- `SecHttpClient` (`http_client.py`)
  - Async HTTP transport with SEC-oriented headers, rate limiting, and retries.
  - Separate archive request path for `www.sec.gov/Archives` access.

- `SecCacheManager` (`cache_manager.py`)
  - Filesystem cache for submissions/forms/facts/mappings.

- `FilingDocumentHandler` (`document_handler.py`)
  - Derives filing archive URL parts (CIK + accession).
  - Lists filing documents via `index.json`.
  - Downloads specific documents or infers primary document names.

- `Form4Processor` (`form4_processor.py`)
  - Parses XML transactions.
  - Maps transaction codes and classifies signal classes.
  - Normalizes to canonical event objects.
  - Dedupe/amendment handling and daily signal aggregates.

- `FinancialDataProcessor` (`financial_processor.py`)
  - Pulls key metric histories from company facts.
  - Structures quarterly/annual metric summaries.

- `SqlCacheManager` (`sql_cache_manager.py`)
  - Optional persistence for financial history and supply chain edges.

## Request/data flow (insider signals)

1. `SECDataFetcher.get_insider_signal_payload(ticker, ...)`
2. Delegates to `Form4Processor.get_insider_signal_payload(...)`
3. Uses injected `fetch_filings_metadata` callback (wired to `SECDataFetcher.fetch_insider_filings`) to get Form 4 metadata.
4. For each accession:
   - `FilingDocumentHandler.download_form_xml(...)` fetches filing XML (using `SecHttpClient`).
   - `Form4Processor.parse_form4_xml(...)` extracts transaction rows.
5. Transactions are normalized to event schema (`_normalize_signal_event`).
6. Amendment-aware dedupe (`_dedupe_and_apply_amendments`).
7. Daily aggregation (`_build_daily_aggregates`) and summary digest (`_build_llm_digest`).
8. Return payload with `events`, `daily_aggregates`, `llm_digest`.

## Request/data flow (financial summary)

1. `SECDataFetcher.get_financial_summary(ticker)`
2. Delegates to `FinancialDataProcessor.get_financial_summary`.
3. Processor fetches company facts via injected callback (`SECDataFetcher.get_company_facts`).
4. Processor extracts mapped metrics and periodized entries.
5. `SECDataFetcher` persists summary to SQLite via `SqlCacheManager.save_financial_history`.

## Important implementation constraints

- Async-first API: all networked operations are `async def`.
- SEC user-agent is effectively mandatory for reliable operation.
- Form 4 signal logic is anchored by unit tests in `tests/test_form4_signals.py`.
- Form list retrieval relies on submissions JSON "recent" arrays and date filtering.
