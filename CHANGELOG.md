# Changelog

All notable changes to `copetech-edgar`. This project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html); until `1.0.0`, minor versions
may contain breaking changes, which are always listed first.

## [Unreleased]

Correctness and SEC-compliance pass from the 2026-07 architecture audit
(see [docs/audit-2026-07.md](docs/audit-2026-07.md)). Every fix below was reproduced against
a real SEC document or SEC's official XML technical specifications before being made, and
re-verified afterwards.

This work is targeted for `0.2.0`.

### Breaking

- **Form 13F holdings: `value` is no longer multiplied by 1000, and `value_thousands` is
  removed.** Form 13F as amended effective 2023-01-03 (SEC Release 34-96492) reports value
  in whole US dollars. The old scaling inflated every modern filing 1000× — Berkshire
  Hathaway's ~$263B portfolio was reported as ~$263 *trillion*. `value` now passes through
  as reported, with `value_usd` as an explicit alias. Consumers reading `value_thousands`
  must switch to `value`. Persisted 13F values from earlier versions are wrong by 1000× and
  should be recomputed.
- **Form 4 rows may include holdings.** `parse_form4_xml` now returns
  `nonDerivativeHolding` / `derivativeHolding` rows, flagged `is_holding=True`. Callers that
  assumed every row was an executed transaction should filter on `is_holding`.
  `get_insider_signal_payload` excludes them, so its event and aggregate counts are
  unchanged.
- **Form 4 numeric fields can be `None`.** `shares`, `price_per_share`, `value` and
  `shares_owned_after` return `None` when the filing omits the value or it cannot be parsed,
  instead of silently reporting `0.0`. An unpriced award is now unknown value, not $0.
- **`copetech_sec.tag_mapping` removed.** Dead module with no references; it was a third,
  divergent copy of the financial concept chain. Use
  `copetech_sec.financial_metrics.METRIC_REGISTRY`.
- **Optional dependencies moved to extras.** `pandas`, `fastapi`, `uvicorn`, `boto3`,
  `yfinance` and `duckdb` are no longer installed by default. Install
  `.[dataframes]`, `.[service]`, `.[analysis]` or `.[dev]` as needed. The core install drops
  from ~40 packages (~130 MB of wheels) to 11.
- **`copetech_sec.sec_api` no longer calls `load_dotenv()` at import time.** Importing the
  library no longer mutates the host process's environment. Applications should call
  `copetech_sec.sec_api.load_dotenv_settings()` explicitly; `app.py` does.

### Fixed

- Financial-series observations now distinguish the filing that first made a value
  available from a later comparative filing selected for presentation. `availableAt` is
  always substantiated by `availabilitySource`, while `selectedSource` identifies the
  selected fact.
- Derived fourth quarters now require three contiguous reported quarters; three arbitrary
  windows contained by an annual period can no longer produce a false residual.
- Form 13F top-10 concentration is calculated from aggregated economic positions rather
  than raw other-manager rows, and large integer fields no longer pass through `float`.
- High-level insider transaction APIs exclude Form 3/4/5 holding rows.
- Public filing-download boundaries validate accessions and document names before building
  SEC URLs or filesystem paths.
- The base installation's `copetech-sec-api` command now explains that the `service` extra
  is required instead of failing with an opaque optional-dependency import error.
- Backend authentication now fails closed with a service-configuration error when
  `BACKEND_API_SECRET` is unset.

- **SEC fair-access: the rate limiter provided no protection under concurrency.** Requests
  compared `time.time()` against a shared `last_request_time` with no lock, so concurrent
  callers all read the same stale value, slept in parallel, and dispatched together — 50
  concurrent requests at a configured 10 req/s went out within 101 ms (~492 req/s), against
  SEC's stated limit of "no more than 10 requests per second, regardless of the number of
  machines used to submit requests". Replaced with a monotonic slot-reservation cursor
  guarded by an `asyncio.Lock`, shared by the data and archive request paths.
- **Form 13F quarter-over-quarter diffs silently dropped ~68% of holdings.** Building the
  comparison map with `{key: row for row in holdings}` kept only the last row per security,
  but filers report one security across many rows (one per `otherManager` attribution).
  Berkshire's 90-row filing collapsed to 29 positions, and bucket sums never reconciled with
  reported totals. Rows are now rolled up per security, summing value and shares.
- **Form 4 acquisition/disposition direction contradicted the filing.** Direction was
  derived from a transaction-code table, but codes `M`, `C`, `G`, `J`, `W`, `K`, `Z` and `I`
  are direction-neutral — the filing's `transactionAcquiredDisposedCode` is authoritative and
  was parsed but never used. Both legs of an option/RSU exercise were counted as
  acquisitions, double-counting the shares in `analyze_insider_transactions`'s buy/sell/net
  values.
- **Form 4 transaction-code table was incomplete and partly wrong.** `W` was labelled
  "Warrant Exercise"; per SEC it is acquisition or disposition by will or the laws of descent
  and distribution. Codes `V`, `E`, `H`, `O`, `K` and `L` were missing. All 21 codes now
  present with SEC's wording.
- **Only the first reporting owner was read.** The Ownership XML Technical Specification
  allows up to 10 (`reportingOwner` `maxOccurs="10"`); co-owners on joint filings were
  dropped entirely. All owners are now exposed via `reporting_owners`, `owner_count` and
  `is_joint_filing`, with `owner_*` still carrying the primary owner. Row counts are
  unchanged — the schema has no per-row owner attribution, so records are never duplicated
  per owner.
- **Forms 3 and 5 returned no rows.** Only `*Transaction` elements were parsed, and the
  Form 3 schema contains none. SEC's own sample `doc3.xml` now yields 2 rows (was 0) and
  `doc5.xml` 6 rows (was 4).
- **Valid numeric values parsed as zero.** The guard
  `raw.replace('.','',1).isdigit()` rejected whitespace-padded values (emitted by some filer
  agents), negatives, thousands separators and scientific notation, returning `0.0` for all
  of them.
- **Rule 10b5-1 plan sales were never detected.** Classification searched `security_title`
  for "10b5-1", which holds values like "Common Stock". Now reads the `aff10b5One` element,
  which the Ownership specification defines as a required *document-level* field on Forms
  4/5, and falls back to footnote text.
- **Form 4 footnotes were discarded.** Footnotes carry material qualifications ("no shares
  were sold", plan adoption dates) and are now attached per row via `footnote_ids` /
  `footnotes`.
- **Derivative and non-derivative rows had different key sets**, so consumers indexing a
  column raised `KeyError` on half the rows and `pandas.DataFrame` produced ragged output.
  All rows now share one schema.
- **`_role_weight` never matched 10% owners** — it compared `'10% owner'` against the
  original-case role string while the parser emits `'10% Owner'`.
- **Document selection relied on a field SEC doesn't populate.** SEC's `index.json` reports
  `type` as a directory-listing icon name (`text.gif`, `compressed.gif`), not an EDGAR
  document type, so four selection branches were unreachable. Selection now relies on
  filename conventions, guards against icon names, excludes SEC's XSL-rendered HTML views
  (served under `.xml` names) from machine-readable XML candidates, and returns `None`
  instead of a wrong pick when only excluded candidates remain.
- **Two primary-document regexes could never match**, using `\\d` inside raw strings (a
  literal backslash followed by `d`) instead of `\d`.
- **The exhibit filter discarded valid primary documents** whose filename contained `ex-`
  (for example `flex-20240101.htm`).
- **`Retry-After` as an HTTP-date raised `ValueError`** out of the retry loop; RFC 9110
  permits both forms and both are now handled.
- **HTTP 503 is now retried** with backoff. SEC uses it as a throttle signal alongside 429.
- **Large SEC JSON responses were truncated at the first buffered network chunk.**
  `StreamReader.read(n)` may return fewer than `n` bytes before EOF, so the bounded reader
  sometimes parsed only the first ~50–200 KiB of Company Facts or submissions data and
  reported misleading malformed-JSON errors. It now drains the stream to EOF in bounded
  chunks while preserving the 256 MiB decompressed-body ceiling.
- **Form 144 signature-block lookup chained Element objects with `or`.** ElementTree defines
  an element's truth value as its child count, so a present-but-childless
  `<noticeSignature>` was treated as absent and fell through to the legacy `<signature>`
  path — dropping the signer and notice date. (This also emitted a `DeprecationWarning` on
  every parse.)
- **`tests/test_app.py` could not be collected**, aborting the entire suite;
  `starlette.testclient` requires an httpx transport, now a dev dependency. 29 previously
  unrunnable tests now run.

### Added

- MIT license.
- Namespaced `EdgarClient` resources: `companies`, `filings`, `ownership`,
  `institutions`, and `financials`; `SECDataFetcher` remains compatible for `0.2.x`.
- Append-only, content-addressed financial fact versions with in-place legacy-table
  migration and latest-normalization resolution.
- Separate canonical `diluted_eps` and `basic_eps` metrics.
- Point-in-time historical trailing P/E using split-adjusted prices, TTM diluted EPS,
  split-basis normalization, amendment timing, null non-positive multiples, and full
  price/filing provenance.
- Ownership module boundaries for parsing, transaction semantics, signal
  classification, and raw-filing acquisition; `Form4Processor` remains the compatibility
  facade.
- Typed SEC acquisition errors distinguish absence, access denial, exhausted throttling,
  transport failures, malformed JSON, and oversized bodies.
- Canonical `Cik`, `Accession`, and `Ticker` identifier types.
- Complete submissions-history traversal through `filings.files[]`, exposed through
  `get_filings_page` with source-file, warning, truncation, and cursor metadata.
- CIK-keyed issuer caches and a metadata-bearing 24-hour ticker-map TTL.
- Entity-safe XML parsing through `defusedxml`.
- A recorded public SEC ownership fixture with accession and transformation provenance.
- Public security and contribution policies.
- Bounded response reads (256 MiB cap, `Content-Length` pre-check) so a large or
  hostile-encoded body cannot exhaust memory.
- Explicit `aiohttp.ClientTimeout` with separate connect and socket-read budgets, replacing
  bare integer timeouts (removed in aiohttp 4).
- `Accept-Encoding` is capability-detected: `br` is only advertised when a Brotli decoder is
  installed.
- Archive document names from `index.json` are validated against path traversal before being
  used in URLs or local output paths.
- Form 4 parsing now exposes `document_type`, `period_of_report`, `transaction_form_type`,
  `equity_swap_involved`, `indirect_ownership_nature`, `remarks` and
  `not_subject_to_section_16`.
- 13F holdings expose `other_manager` and `row_index` so multi-row positions stay traceable.
- New test modules `tests/test_form4_parser.py` (32 tests) and
  `tests/test_document_handler.py` (18 tests) covering code that previously had none;
  `tests/test_http_client.py` gains concurrency, bounded-read and retry coverage. Suite: 115
  → 215 tests.
- `docs/audit-2026-07.md` — full architecture and capability audit.

### Changed

- Unconfigured requests send an honest placeholder User-Agent naming the library instead of a
  fabricated `email@example.com` contact address.
- Removed an unused `pandas` import from `sec_api.py`.
