# Financial series

CopeTech-Edgar turns SEC Company Facts into auditable financial time series.
Company Facts is the acquisition source, not the final series: issuers can change
concepts, later filings repeat comparative periods, amendments can revise values,
and annual filings often omit a standalone fourth quarter.

## Public API

```python
payload = await fetcher.get_financial_series(
    "GOOGL",
    metric="revenue",
    frequency="quarterly",
    basis="canonical",
    alignment="availability",
    as_of="2024-06-30",
    start="2018-01-01",
    include_provenance=True,
)
```

`frequency` accepts `quarterly`, `annual`, or `ttm`. `basis="reported"` excludes
derived fourth quarters; `canonical` includes them. `as_of` filters by filing date,
so a historical query cannot see facts that had not yet been published.
`alignment="availability"` exposes the filing date as `alignedAt`; `period_end`
is available for accounting-period analysis but must not be used in a
point-in-time price overlay.

Each observation includes:

- `periodStart`, `periodEnd`, `availableAt`, and `alignedAt`
- `value`, `unit`, fiscal year/period, and frequency
- `reported`, `derived`, `derivation`, `confidence`, and `qualityFlags`
- `sources` with taxonomy, concept, form, filing date, accession number, frame,
  and the SEC archive URL

## Normalization rules

The metric registry defines an ordered concept family rather than selecting one
concept for an issuer's entire history. Facts are classified by their actual
duration and resolved per economic window. Repeated comparative facts are
deduplicated, later amendments win, and the earliest filing date carrying the
selected value remains its availability date.

Canonical quarterly revenue derives Q4 only when a compatible annual value and
three standalone quarters exist:

`Q4 = annual revenue - Q1 - Q2 - Q3`

TTM revenue is the sum of four contiguous canonical quarters. Derived rows keep
all contributing sources and are explicitly flagged; they are never presented as
reported facts.

Raw normalized facts are persisted in SQLite by symbol, metric, taxonomy,
concept, accession, unit, and economic window. This retains enough provenance to
re-resolve a series when normalization rules evolve. Company Facts snapshots are
refreshed daily; if acquisition fails, persisted facts can serve a clearly warned
stale result.

## Source choices

- **SEC Company Facts** is the canonical open source for reported US issuer
  history and provenance. It is free and auditable, but requires issuer-specific
  concept stitching and does not provide analyst estimates.
- **Filing-level XBRL** is the planned fallback for companies or periods that
  cannot be resolved confidently from Company Facts.
- **yfinance valuation measures** can provide convenient historical P/E and
  related fields for display or cross-checking, but its short/opaque history is
  not a substitute for a reproducible SEC denominator.
- **Paid normalized vendors** can improve international coverage, estimate data,
  restatement handling, and support guarantees. The open pipeline should preserve
  its own provenance contract so a vendor can be added without changing clients.

## Scope and roadmap

The first production metric is USD revenue for duration facts. The registry is
designed to add gross profit, operating income, net income, operating cash flow,
capital expenditure, free cash flow, diluted EPS, and carefully distinguished
share-count metrics.

P/E requires a price convention and an earnings denominator. A defensible
historical trailing P/E series will combine split-adjusted price with point-in-time
TTM diluted EPS; forward P/E additionally requires timestamped consensus
estimates. Those valuation metrics should be separate registry entries rather
than implicit fields on revenue observations.

Current limitations include USD-only revenue, Company Facts rather than
filing-level fallback, and no estimate/forward-metric source. Quality flags expose
conflicting filing values, multiple available concepts, amendments, derived Q4s,
and implausible residuals instead of silently hiding them.
