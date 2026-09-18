# Fundamentals validation: evidence, coverage, and remaining work

The goal is reliable fundamentals histories for a broad range of issuers. A
successful parser run alone does not meet that goal. Every plotted value needs
a defined meaning, supported reporting period, units, and an evidence trail.

## Current, measured coverage

- 20 pinned issuers spanning retail, technology, banks, insurance, a REIT, a
  utility, non-calendar years, 52/53-week calendars, IFRS, and reporting transitions.
- 46 public fundamentals metrics; 114 metric/frequency combinations per issuer.
- 70 numeric expectations across the entire recorded corpus. AAPL has 16 and
  SOFI 14; the other 18 issuers share 40. Only AAPL and SOFI currently include
  explicit `reviewedFilings` citations in their expectations.
- All recorded combinations now undergo structural and exception checks across
  the history present in the input, including combinations without numeric
  expectations. CI publishes per-issuer assertion counts and citation presence.
- Fixture hashes and exact issuer identities are checked. A correct hash proves
  input identity, not accounting accuracy or independent review.
- Seven valuation series are explicitly excluded from this SEC-only audit.
  Price observations and split histories are separate required inputs.

These recordings are canaries, not full-history validation. Passing the gate
does not establish complete market coverage or independent correctness for all
46 metrics. Existing expectations were preserved in this review; the missing
filing transcriptions were not fabricated or retroactively marked reviewed.

## Three distinct layers

1. **Unit/regression tests:** hand-calculated examples exercise formulas,
   fiscal-year boundaries, cumulative-to-quarter conversion, stock splits,
   restatements, absent inputs, debt overlap, and information availability.
2. **Recorded integration tests:** immutable source snapshots make failures
   reproducible. Run every supported metric on all available history. Check
   values against separately transcribed filing answers, and verify dates,
   units, selected concepts, input evidence, warnings, and unavailable states.
3. **Live audits:** acquire current SEC inputs at a low rate, then run the same
   checks. Live inputs can change; archive a failing snapshot before debugging.
   A live run becomes deterministic only after its inputs and policies are pinned.

Never copy resolver output into `expected.json` and call it independent truth.
Never waive a disagreement simply because another website displays a different
number: first compare definitions, currencies, dates, and restatement policies.

## Work required for the intended 20-issuer full-history gate

1. Capture complete Company Facts histories for the pinned issuers with source
   URLs, acquisition times, and hashes. Store large raw archives outside git;
   keep immutable references and focused regression recordings in the repo.
   Existing minimized files cannot recreate discarded quarters or concepts.
2. For each of the 114 combinations per issuer, explicitly classify support:
   supported, not applicable, unsupported taxonomy/currency, missing evidence,
   or dependent on external prices/splits. Optional cannot mean unreviewed
   forever. Publish unsupported combinations and reasons.
3. Independently transcribe reported line items from primary filings. Record
   accession, statement/note, economic window, unit/scale, fiscal labels, value,
   and filing/acceptance time. For derived answers, retain the independently
   transcribed operands, formula version, and calculation. Include at least
   two adjacent complete fiscal years, their quarters, and a TTM bridge for
   each supported combination, then add event-specific historical cases.
4. Run structural checks over the complete downloaded history, not just those
   selected answer periods. Reconcile annual and quarterly additive flows only
   when periods, concepts, and units match. Do not add EPS, ratios, or balances.
   Reconcile balance-sheet residuals with minority/temporary equity before
   treating them as arithmetic errors.
5. Cover amendments and later comparatives with as-of queries. Before a filing
   becomes available, no selected value or formula decision may use it. Daily
   filing dates alone do not establish intraday tradability; historical trading
   needs acceptance timestamps and an explicit session policy.
6. Record independently sourced daily prices, splits, share-class/ADR ratios,
   and price-adjustment semantics for valuation. Ordinary revenue, margins,
   cash flow, and balance sheets do not require candles. Historical P/E and
   enterprise-value multiples do. Forward estimates are not supplied by filings.
7. Make new structural errors, resolver crashes, missing required disclosures,
   unexpected support loss, and mismatched reviewed answers blocking. Keep
   approved accounting exceptions narrowly scoped with evidence and review
   rationale. New live semantic warnings require triage, not an automatic
   rewrite of expectations.

## Product scope and architecture

SEC data is standardized using XBRL with US-GAAP/IFRS taxonomies, but issuers can
extend taxonomies. Company Facts aggregates standard-taxonomy, entity-wide
facts; it is not every fact or relationship in a filing. See the
[SEC API documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

Keep acquisition, immutable evidence storage, reported-series normalization,
derived formulas, and price-based valuation separate. Add filing-level XBRL
fallbacks for custom tags, dimensional contexts, exact report dates, and debt
relationships rather than growing ticker-specific guesses in formulas.

The current USD-only registry and limited IFRS mappings cannot promise every
stock worldwide. Start with an explicit supported US-issuer scope, then expand
taxonomy, currency, filing-form, and sector policies using reviewed fixtures.
An unsupported result with a reason is preferable to a plausible wrong chart.

## PR #3 follow-up verification

Review regressions cover fiscal years ending in the next calendar year, sparse
instant facts, minimized filing context, persisted schema migration, later debt
decision evidence, historical revisions, resolver fault injection, required
comparability warnings, and corruption outside the previous five-year audit cut.
The original 20-issuer numeric expectations remain unchanged.
