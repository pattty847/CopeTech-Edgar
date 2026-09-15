# Financial-series audit

This workflow tests the complete public fundamentals contract against a fixed set
of accounting shapes. It prefers an explicit unavailable result to a value that
the filing does not support.

The audit covers all 44 public reported/derived metrics returned by
`FinancialSeriesService.supported_metrics()`. It records the seven valuation
series as `external_input_missing`: real valuation output also needs a separately
versioned split-adjusted price and split-event source, which this SEC-only audit
does not invent.

## Corpus

`corpus.py` pins 20 issuers by ticker and CIK. The set covers ordinary operating
companies, non-calendar and 52/53-week fiscal years, a smaller issuer, negative
equity, captive finance, a utility, leases, a REIT, banks/lenders/insurance,
multi-class identity, and the 20-F/40-F IFRS boundary.

Applicability is deliberately conservative:

- `expected`: absence is an audit warning that needs review.
- `optional`: a traceable value or an explicit unavailable result is acceptable.
- `not_comparable`: arithmetic can exist, but the result needs a semantic warning.

An unspecified metric is optional. The audit never upgrades an unreviewed absence
to a correctness failure.

## SEC access

Live acquisition is sequential and cache-first. The runner uses a minimum
0.5-second request interval, for no more than two SEC requests per second. One
ticker-map request is shared by the run, and each issuer gets at most one Company
Facts request. A same-day rerun uses the local cache.

Set a truthful SEC user agent before a live run:

```bash
export SEC_API_USER_AGENT="Your Name your-email@example.com"
PYTHONPATH=src .venv/bin/python -m analysis.financial_series_audit.run
```

Use `--offline` to prohibit network access. An offline run fails if any requested
issuer is absent from the cache.

Raw downloads and run output stay under ignored `.cache/` and `output/`
directories. Never commit a complete Company Facts response.

## Outputs

Each run produces:

- `summary.json`: versioned issuer and metric results.
- `findings.json`: structural errors and review warnings.
- `coverage.csv`: one row per issuer, metric, and frequency.
- `manual-review.md`: one broad-coverage annual period and quarter per issuer,
  with values, concepts, warnings, and primary SEC filing links.

The review packet is evidence for a human check, not evidence that the check
already happened.

## Recorded fixtures

Fixture generation starts from a local raw response. It never performs network
I/O. A fixture keeps accepted concepts, explicit negative controls, and every
comparative or amendment fact needed for the target windows. Its manifest records
the source URL, retrieval time, raw and minimized hashes, accessions, and filter
version. A concept index separately lists every raw concept so missing mappings do
not disappear during minimization.

Independent manual expectations belong beside the fixture. Parser-produced output
must not become its own expected answer.

After review, create a minimized fixture from the locally cached raw file:

```bash
PYTHONPATH=src .venv/bin/python -m analysis.financial_series_audit.record_fixture \
  analysis/financial_series_audit/.cache/facts/<local-file>.json \
  --symbol SOFI \
  --retrieved-at 2026-09-15T00:00:00Z \
  --period-end 2023-12-31 \
  --control-concept us-gaap:DebtInstrumentCarryingAmount
```

Add independently transcribed expectations only after checking the primary filing.

## Scope stop

This directory implements acquisition, corpus analysis, review evidence, and
hermetic fixture generation. It intentionally does not make the audit a required
mapping-change or CI gate. Decide that policy only after reviewing real findings,
coverage gaps, run cost, and false positives.
