# Financial-series audit

This workflow tests the complete public fundamentals contract against a fixed set
of accounting shapes. It prefers an explicit unavailable result to a value that
the filing does not support.

The audit covers all 46 public reported/derived metrics returned by
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

The production store reads only the current normalization version. Increment
`NORMALIZATION_VERSION` whenever a registry mapping changes; old fact versions remain
as immutable evidence but cannot leak obsolete concepts into current output.

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

The committed fixture corpus contains all 20 issuers from `corpus.py`. It includes
independent expectations for ordinary US-GAAP issuers, financial companies, a
REIT, a utility, a foreign-to-domestic reporting transition, and an unsupported
IFRS monetary-data boundary. `tests/test_recorded_companyfacts.py` runs the same
normalization and derivation code without SEC access.

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

Run the complete recorded corpus with:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/test_recorded_companyfacts.py -q
```

## Review findings

The first 20-issuer review found several patterns that require explicit rules:

- A debt parent can omit separate short-term borrowings, or represent only the
  noncurrent portion despite its broad label.
- A short-term debt parent must take precedence over its child components.
- An incomplete debt hierarchy must produce an unavailable result, not zero.
- A broad revenue concept must take precedence over a contract-revenue subtotal.
- A rolling 12-month 10-Q fact is not an annual fact.
- Balance-equation residuals can represent noncontrolling or temporary equity,
  rather than an arithmetic error.
- Generic net debt and some operating-company ratios are not comparable for banks,
  lenders, and insurers without an explicit product policy.

The production rules resolve the evidence-backed hierarchy cases. The audit keeps
semantic and scope findings visible for manual review.

## Automation policy

The `Fundamentals fixtures (20 issuers)` CI job is the deterministic merge gate.
It runs without network access and fails when an independently reviewed value,
concept, unavailable state, hierarchy, or economic window changes.

The separate `Fundamentals live audit` workflow refreshes the complete corpus once
a week at no more than one SEC request per second. It also supports a manual run.
The workflow requires the repository secret `SEC_API_USER_AGENT`, uploads the full
review packet, and fails only for structural errors. Semantic warnings remain
visible in the workflow summary and report but do not fail the job.

When a taxonomy mapping changes, verify the source filing manually and update its
independent fixture expectation in the same pull request. Do not approve a changed
fixture only because it matches parser output.
