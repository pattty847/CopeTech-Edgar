# Recorded SEC fixtures

These fixtures are deterministic parser contracts derived from public SEC filings or
official SEC technical-specification samples. Tests must never fetch SEC over the network.

## Ownership

`ownership/apple-2026-rsu-settlement.xml` is a minimized recording of Apple Form 4
accession `0001140361-26-025622`, filed June 17, 2026:

`https://www.sec.gov/Archives/edgar/data/320193/000114036126025622/form4.xml`

The recording preserves the source document's three economically linked rows and footnote
semantics while removing unrelated boilerplate. It is intentionally small enough to review
in source control. Update it only by recording a new public source and documenting the
accession and transformation here.

SEC filings are public records. These fixtures are redistributed solely as factual test
inputs; project code and documentation remain subject to the repository's project license.

## Company Facts

`companyfacts/` contains the 20 minimized recordings declared by the financial-series
canary corpus. Each fixture keeps reviewed periods, comparative repeats, and relevant
negative controls. `expected.json` contains values transcribed from filing statements,
rather than output copied from the parser.

- AAPL: fiscal 2024 and fiscal Q1 2025, accessions `0000320193-24-000123` and
  `0000320193-25-000008`.
- SOFI: fiscal 2023 and fiscal Q1 2024, accessions `0001818874-24-000026` and
  `0001818874-24-000121`.

The SOFI balance equation includes $320.374 million of temporary equity outside
stockholders' equity. The fixture retains that concept as a negative control.

The fixture gate covers AAPL, MSFT, GOOGL, META, AMZN, WMT, COST, CALM, MCD, GM,
CVX, NEE, DAL, O, JPM, SOFI, PGR, BRK-B, TSM, and SHOP. The set deliberately spans
ordinary operating companies, financial companies, a REIT, a utility, non-calendar
years, 52/53-week reporting, IFRS, and a foreign-to-domestic reporting transition.
