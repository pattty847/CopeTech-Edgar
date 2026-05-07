# 13F Phase 0 Thesis Sprint

This is an isolated research harness for testing whether curated 13F manager flows are worth turning into a production feature.

It does not add FastAPI routes or modify production ingestion. The runner writes local DuckDB/CSV artifacts under this folder.

## Managers

- Berkshire Hathaway Inc
- Susquehanna International Group, LLP
- Citadel Advisors LLC
- Tudor Investment Corp Et Al
- Coatue Management LLC
- Tiger Global Management LLC
- Pershing Square Capital Management, L.P.
- Third Point LLC

CIKs are defined in `config.py` and were verified against SEC submissions JSON on 2026-05-03.

## Fast Demo Run

Use summary-only mode first. It ingests all manager filings, computes Finviz-lite statistics, and maps only the top changed CUSIPs:

```bash
SEC_API_USER_AGENT="Your Name your@email.com" \
  .venv/bin/python -m analysis.phase0_thesis.run_all --summary-only --map-limit 250
```

Outputs:

- `phase0.duckdb` — local analytical database
- `manager_summary.csv` — Finviz-lite manager statistics
- `top_changes.csv` — top latest-quarter adds/reductions/sellouts per manager
- `REPORT.md` — readable summary report

`phase0.duckdb` and CSV outputs are gitignored.

## Full Thesis Run

The full mode maps every unique CUSIP it can and runs yfinance forward-return backtests:

```bash
SEC_API_USER_AGENT="Your Name your@email.com" \
  .venv/bin/python -m analysis.phase0_thesis.run_all
```

Run this only when you are ready to let the machine work for a while. Without an OpenFIGI key, the mapper uses anonymous OpenFIGI limits and conservative batching.

## Current Practical Pivot

The summary-only output is already useful for a Finviz-lite institutional dashboard:

- market value this/previous quarter
- holdings count
- new purchases
- added/reduced/sold-out counts
- top 10 concentration
- top quarter changes with call/put labels

Use this for the first UI/demo pass. Save the full backtest for a narrower mapped signal set.
