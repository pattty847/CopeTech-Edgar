# Financial series

CopeTech-Edgar turns SEC Company Facts into auditable financial time series.
Company Facts is the acquisition source, not the final series: issuers can change
concepts, later filings repeat comparative periods, amendments can revise values,
and annual filings often omit a standalone fourth quarter.

## Public API

```python
payload = await client.financials.series(
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
derived fourth quarters for additive metrics such as revenue; `canonical` includes
them. `as_of` filters by filing date,
so a historical query cannot see facts that had not yet been published.
`alignment="availability"` exposes the filing date as `alignedAt`; `period_end`
is available for accounting-period analysis but must not be used in a
point-in-time price overlay.

Canonical `diluted_eps` TTM queries also accept `split_events=[(date, ratio), ...]`.
The list must describe the same fully split-adjusted share basis used by the price
consumer; an unverified split history yields no canonical TTM observations.

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

Instant annual series keep only the latest eligible balance date from each
annual filing. They do not treat every older comparison repeated in a 10-K as a
new annual period. Annual fiscal labels identify the economic balance date,
because Company Facts `fy` and `fp` describe the filing context and can belong
to a later year for comparative facts.

`net_debt` prefers a reported aggregate `total_debt` fact. It falls back to the
sum of separately reported current and noncurrent debt only when no aggregate
exists, so parent totals and their components are never added together. Missing
debt facts produce no net-debt observation; absence is not treated as zero.
The result is the generic formula `total debt - cash - short-term investments`.
This metric is not comparable for financial companies, where deposits and
other funding liabilities are part of operations rather than ordinary corporate
leverage, and the payload carries that warning.

Canonical quarterly revenue derives Q4 only when a compatible annual value and
three standalone quarters exist:

`Q4 = annual revenue - Q1 - Q2 - Q3`

TTM revenue is the sum of four contiguous canonical quarters. Derived rows keep
all contributing sources and are explicitly flagged; they are never presented as
reported facts.

Raw normalized facts are persisted in an append-only SQLite version ledger keyed by
CIK, metric, taxonomy, concept, accession, unit, economic window, normalization
version, and content hash. Acquisition time is separate from the SEC filing date.
Reads select the newest normalization without rewriting prior evidence; the `0.1`
UPSERT table is retained as a migration source throughout `0.2.x`.

`diluted_eps` and `basic_eps` are separate registered metrics. Diluted EPS never
falls back to basic EPS. Both retain negative values, but per-share facts are
weighted averages rather than additive amounts: the resolver never derives Q4 EPS
as annual EPS minus three quarterly EPS values and never sums four EPS facts.

Canonical interim TTM diluted EPS is reconstructed from paired
`EarningsPerShareDiluted` and weighted-average diluted-share facts:

`TTM numerator = prior annual numerator + current YTD numerator - prior comparable YTD numerator`

The denominator applies the same annual/YTD bridge to weighted share-days. This
handles issuers whose share count changes materially and prevents pre-split and
post-split comparative facts from being mixed. Annual diluted EPS remains a direct
reported TTM observation. If the supporting share facts are unavailable, the
resolver keeps the latest valid annual observation rather than inventing an
interim value.

## Historical trailing P/E

```python
payload = await client.financials.valuation(
    "AAPL",
    price_observations=split_adjusted_prices,
    split_events=split_history,
    price_source="your-price-provider",
)
```

Prices and EPS are normalized to the supplied fully split-adjusted share basis.
Each price timestamp resolves the TTM diluted EPS that was knowable on that date.
Amendments affect only timestamps on or after their filing date. Zero or negative
TTM EPS produces `null`, not a misleading negative multiple. A denominator older
than the configured staleness window also produces `null`; consumers can render
that missing coverage as a gap rather than extending an obsolete multiple.
Observations carry the price basis, price timestamp, TTM EPS availability, all
contributing SEC filing sources, and quality flags such as `eps_ttm_reconstructed`,
`stale_eps`, `eps_split_adjusted`, and `non_positive_ttm_eps`.

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

The production registry currently includes USD revenue, diluted EPS, basic EPS,
and weighted-average diluted shares.
It is designed to add gross profit, operating income, net income, operating cash
flow, capital expenditure, free cash flow, and carefully distinguished share-count
metrics.

Historical trailing P/E uses split-adjusted price with point-in-time TTM diluted
EPS. Forward P/E remains out of scope because it requires timestamped consensus
estimates.

Current limitations include USD-only facts, Company Facts rather than
filing-level fallback, and no estimate/forward-metric source. Company extension
tags remain unavailable unless the SEC maps them into a standard taxonomy
concept. Quality flags expose
conflicting filing values, multiple available concepts, amendments, derived Q4s,
split adjustments, and missing point-in-time TTM EPS instead of silently hiding
them.
