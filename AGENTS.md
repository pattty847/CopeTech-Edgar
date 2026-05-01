# AGENTS.md

## Project overview

This repository is a reusable SEC EDGAR backend package (`copetech-edgar`) with async APIs centered on
`copetech_sec.SECDataFetcher`. It is library-style code intended to be imported by other systems.

Primary domain areas:
- Form 4 insider transaction parsing and signal payload generation.
- SEC filings metadata/document retrieval.
- XBRL company facts processing into financial summaries.
- Optional file/SQLite persistence utilities.

## Repository map

- `src/copetech_sec/__init__.py` – package export (`SECDataFetcher`).
- `src/copetech_sec/sec_api.py` – orchestration facade and public API surface.
- `src/copetech_sec/form4_processor.py` – Form 4 XML parsing + signal logic.
- `src/copetech_sec/document_handler.py` – SEC archive document lookup/download.
- `src/copetech_sec/http_client.py` – async SEC HTTP client, retry/rate-limit behavior.
- `src/copetech_sec/financial_processor.py` – company facts metric extraction.
- `src/copetech_sec/cache_manager.py` – filesystem cache.
- `src/copetech_sec/sql_cache_manager.py` – SQLite persistence.
- `tests/test_form4_signals.py` – critical behavior tests for insider signal modeling.
- `README.md` + `docs/` – contributor-facing docs.

## Architectural concepts to preserve

1. **Facade + delegated processors**
   - Keep `SECDataFetcher` as orchestration layer.
   - Keep specialized logic in processor/handler classes.

2. **Dependency injection between components**
   - `Form4Processor` receives a document handler + filing fetch function.
   - `FinancialDataProcessor` receives a facts fetch function.
   - Preserve this testability pattern.

3. **Async-first interface**
   - Networked/public data methods are async.
   - Avoid introducing blocking I/O into async paths.

4. **Signal payload contract**
   - `get_insider_signal_payload` returns `symbol`, `window`, `as_of`, `events`,
     `daily_aggregates`, and `llm_digest`.
   - Treat these keys as compatibility-sensitive.

## Invariants and constraints

- Use SEC-compliant user-agent handling; do not remove user-agent propagation paths.
- Preserve amendment dedupe behavior in Form 4 signal generation unless intentionally redesigning with tests/docs updates.
- `tests/test_form4_signals.py` is the minimum guardrail; changes touching signal logic must run it.
- Do not silently rename public methods on `SECDataFetcher` without updating docs and consumers.

## How to run / validate changes

From repo root:

```bash
uv pip install -e .
python -m unittest tests/test_form4_signals.py
```

If you change package import surfaces, also run:

```bash
python -c "from copetech_sec import SECDataFetcher; print(SECDataFetcher.__name__)"
```

## Current EC2 demo deployment

- AWS region: `us-east-1`.
- EC2 public IP: `54.162.23.10`.
- SSH user/host: `ubuntu@54.162.23.10`.
- Local SSH key path on Patrick's Mac: `/Users/copeharder/Downloads/copeharder-key.pem` (never commit this; `*.pem` is ignored).
- Remote checkout path: `/home/ubuntu/CopeTech-Edgar`.
- Runtime: Docker Compose service `sec-api`, mapping host `80` to container `8000`.
- IAM instance role: `copeharder-ec2-backend-role`.
- Public healthcheck: `curl http://54.162.23.10/health`.
- Protected SEC test: `curl -H "x-backend-secret: $BACKEND_API_SECRET" -H "x-demo-key: friend-demo-key-1" "http://54.162.23.10/api/sec/insiders?symbol=AAPL&days_back=1&filing_limit=1"`.
- Restart command: `ssh -i /Users/copeharder/Downloads/copeharder-key.pem ubuntu@54.162.23.10 'cd ~/CopeTech-Edgar && git pull --ff-only && docker compose up -d --build'`.
- Current `.env` lives only on EC2 and should not be committed.
- DynamoDB rate limiting keys by `demo_key + IP + YYYY-MM-DD`; without credentials, the app falls back to in-memory rate limiting and local file cache.
- Existing DynamoDB partition keys are `rate_limits.ip`, `demo_jobs.job_id`, and `sec_cache_index.cache_key`.

## Style expectations

- Keep changes focused and minimal; avoid unrelated refactors.
- Match existing typing/logging style in touched files.
- Prefer small helper methods over deeply nested logic.
- Do not add speculative features without tests/docs.

## Documentation expectations

When behavior changes, update:
1. `README.md` (quickstart or capability summary if user-visible).
2. Relevant file(s) under `docs/` for deeper details.
3. This `AGENTS.md` if contributor workflow/invariants changed.

## Before changing code, check

1. Tests under `tests/` for expected behavior.
2. Existing public method signatures in `sec_api.py`.
3. Call flows in `form4_processor.py` and `document_handler.py`.
4. Cache side effects in `cache_manager.py` and `sql_cache_manager.py`.

## Keep changes safe

- If behavior is ambiguous, document conservatively instead of over-claiming support.
- For API-shape changes, include migration notes in README/docs.
- Preserve backwards compatibility where practical; if not, call out breaking changes explicitly.
