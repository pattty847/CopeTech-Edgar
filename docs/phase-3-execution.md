# Phase 3 execution: bounded expansion and integration contracts

Phase 3 completes the engineering roadmap from the July 2026 audit without weakening the
Phase 1 acquisition boundaries or the Phase 2 point-in-time financial contracts.

## Implementation status — 2026-07-30

- Complete: bounded Company Concept and XBRL Frames queries with strict path validation and
  daily local caches.
- Complete: first-class Forms 3/4/5 discovery plus parsed transaction and holding payloads.
- Complete: issuer payloads expose all SEC-listed share classes and former names.
- Complete: SEC mutual-fund class resolution preserves fund CIK, series ID, class ID, and
  ticker.
- Complete: Form 13F values normalize historical thousands to dollars using the filing
  date that selects the applicable form version; raw reported value/unit/scale remain
  auditable and FIGI is retained.
- Complete: provider-neutral read-only agent tool definitions and lazy pandas adapters.

## Public contracts

```python
concept = await client.financials.concept(
    "AAPL",
    taxonomy="us-gaap",
    concept="Assets",
)

frame = await client.financials.frame(
    taxonomy="us-gaap",
    concept="Assets",
    unit="USD",
    period="CY2025Q4I",
)

ownership = await client.ownership.entries(
    "AAPL",
    forms=["3", "3/A", "4", "4/A", "5", "5/A"],
)

fund_class = await client.companies.funds.get("LACAX")
```

Agent runtimes can use `EdgarAgentTools.manifest()` and
`await EdgarAgentTools.invoke(...)`. Every declared tool is read-only. A successful lookup
with no matching SEC record returns `status="not_found"`; acquisition and parsing
exceptions continue to propagate, so absence is never collapsed into failure.

Dataframe consumers can import focused functions from
`copetech_sec.integrations.pandas`. Pandas remains an optional dependency and is imported
only when an adapter is invoked. Nested filing provenance is preserved instead of flattened
away.

## Audit correction: the 13F transition key

The original roadmap said to choose pre-2023 value scaling by report date. SEC Form 13F
FAQ 62 says the updated form applies to every report *filed on or after 2023-01-03*,
including amendments for older quarters. The implementation therefore keys the scale to
`filing_date`:

- before 2023-01-03: reported values are thousands of USD and normalize by `×1000`;
- on/after 2023-01-03: reported values are whole USD and normalize by `×1`;
- missing filing date: preserve the reported magnitude, mark the unit unknown, and expose
  `value_scale_basis="filing_date_missing_assumed_current"` rather than guessing silently.

## Remaining release gate

Phase 4 is intentionally not automatic publication. Before the `0.2.0` PyPI release:

1. record the remaining Company Facts issuer fixtures when SEC access permits;
2. cross-check a representative asset matrix against filing exhibits and independent
   financial references;
3. document every expected reconciliation difference (GAAP vs non-GAAP, basic vs diluted,
   quarterly vs YTD, split basis, filing availability);
4. run the release suite, isolated wheel install, build, and secret scan;
5. publish with the 13F migration note.

This keeps external validation as a release gate instead of turning a comparison website
into canonical evidence.
