# Phase 2 execution plan: canonical financials and public client

Phase 1 established trustworthy acquisition boundaries: typed errors, validated SEC
identifiers, CIK-keyed caches, complete submissions history, safe XML, and recorded
fixtures. Phase 2 will prove that architecture with the financial vertical slice that
unlocks historical valuation plots.

## Implementation status — 2026-07-30

- Complete: immutable content-addressed fact versions and legacy-table migration.
- Complete: separate US-GAAP diluted/basic EPS metrics, amendment timing, negative EPS,
  canonical Q4/TTM behavior, and CopeNet migration off legacy EPS trends.
- Complete: split-consistent, point-in-time trailing P/E plus the CopeNet chart overlay.
- Complete: ownership parser, normalization, signal-classification, and acquisition
  namespaces with `Form4Processor` retained as a thin compatibility facade and the
  superseded in-class parser removed.
- Complete: namespaced `EdgarClient` and `SECDataFetcher` compatibility path.
- Pending release evidence: recorded Company Facts fixtures for every issuer pattern below.
  The implementation has deterministic synthetic coverage; the 2026-07-30 recording attempt
  was rejected with HTTP 403 by SEC from the development environment, so no payload was
  fabricated or relabeled as recorded evidence.

## Slice 1: immutable fact versions

`FinancialSeriesStore` is currently a deduplicating UPSERT store, not an append-only fact
ledger. Replace it with immutable fact versions keyed by:

`(cik, metric, taxonomy, concept, unit, period_start, period_end, accession, normalization_version, content_hash)`

Record acquisition time separately from SEC filing availability. Query resolution chooses
the latest normalization version without rewriting prior evidence. Migrate existing rows
in place and retain the old table until parity tests pass.

## Slice 2: canonical diluted EPS

Add `diluted_eps` to the financial metric registry with explicit unit and concept priority:

1. `us-gaap:EarningsPerShareDiluted`
2. IFRS diluted EPS concepts where the taxonomy contract is verified by fixtures

Keep basic EPS separate; never silently substitute it for diluted EPS. Preserve the same
window resolution, amendment timing, and availability/selection provenance used for
revenue, but do not apply revenue arithmetic to per-share facts. EPS is not additive:
never derive Q4 as annual minus three quarters and never sum four quarterly EPS values.
Reconstruct interim TTM diluted EPS from the diluted numerator and weighted share-days
implied by paired diluted EPS and weighted-average diluted-share facts. Add real recorded
fixtures covering:

- standard calendar issuer;
- non-calendar fiscal year;
- later comparative repeat;
- amendment changing a reported value;
- negative earnings;
- large share-count changes;
- split-basis changes and retroactively restated comparatives;
- foreign private issuer with annual-only facts.

Move CopeNet from legacy `get_financial_trend().metrics.eps` to the canonical EPS series.
Only then retire EPS handling from `FinancialDataProcessor`.

## Slice 3: valuation series

Define historical trailing P/E as:

`split-adjusted close / point-in-time TTM diluted EPS`

Rules:

- price bars must remain split-adjusted;
- an EPS observation becomes usable only at `availableAt`;
- use stepwise carry-forward from the latest then-known TTM EPS;
- emit `null` when TTM EPS is zero or negative;
- include price timestamp plus every contributing EPS filing source;
- expose quality flags for derived Q4, amendments, concept conflicts, and stale EPS;
- yfinance may be used as a comparison probe, never as canonical P/E evidence.

The backend should return a generic derived financial/valuation series. CopeNet owns chart
alignment and presentation, not SEC accounting normalization.

## Slice 4: parser/module boundaries

Split the oversized ownership module into:

- `ownership/parser.py`: safe XML to source-shaped records;
- `ownership/normalization.py`: canonical transaction/holding DTOs;
- `ownership/signals.py`: classifications, clusters, aggregates;
- `ownership/service.py`: acquisition, immutable raw store, cache fingerprints.

Keep `Form4Processor` as a compatibility facade until all consumers migrate. Extract shared
numeric, namespace, footnote, and source helpers only when both ownership and 13F use the
same tested contract.

## Slice 5: namespaced public client

Introduce `EdgarClient` with stable resource namespaces:

- `client.companies`
- `client.filings`
- `client.ownership`
- `client.institutions`
- `client.financials`

`SECDataFetcher` remains a deprecated forwarding facade throughout `0.2.x`. Every public
resource method returns a documented domain object or raises the Phase 1 typed error
contract. No new public methods should be added to the legacy facade unless required for
compatibility.

## Release gates

- recorded SEC fixtures for every promoted Beta capability;
- CopeTech full suite, isolated minimal wheel install, build, and whitespace checks;
- full CopeNet suite plus frontend lint/build;
- migration and compatibility tests for persisted caches;
- public license selected by the repository owner;
- no Stable label until fixture, error, and provenance contracts are all present.
