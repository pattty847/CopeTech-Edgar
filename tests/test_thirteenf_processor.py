import unittest

from copetech_sec.thirteenf_processor import ThirteenFProcessor, normalize_cik


SAMPLE_INFORMATION_TABLE = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>1450000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>8123456</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>8123456</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>TESLA INC</nameOfIssuer>
    <titleOfClass>CALL</titleOfClass>
    <cusip>88160R101</cusip>
    <value>198700</value>
    <shrsOrPrnAmt>
      <sshPrnamt>250000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <putCall>Call</putCall>
    <investmentDiscretion>DFND</investmentDiscretion>
    <votingAuthority>
      <Sole>0</Sole>
      <Shared>250000</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
</informationTable>
"""


class ThirteenFProcessorTests(unittest.TestCase):
    def test_normalize_cik_accepts_digits_and_pads(self):
        self.assertEqual(normalize_cik("1614314"), "0001614314")

    def test_normalize_cik_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            normalize_cik("SIG")

    def test_parse_information_table_xml_extracts_holdings(self):
        holdings = ThirteenFProcessor.parse_information_table_xml(SAMPLE_INFORMATION_TABLE)

        self.assertEqual(len(holdings), 2)
        self.assertEqual(holdings[0]["issuer"], "APPLE INC")
        self.assertEqual(holdings[0]["cusip"], "037833100")
        self.assertEqual(holdings[0]["value_thousands"], 1450000)
        self.assertEqual(holdings[0]["value"], 1450000000)
        self.assertEqual(holdings[0]["shares"], 8123456)
        self.assertEqual(holdings[0]["share_type"], "SH")
        self.assertEqual(holdings[0]["voting_authority"]["sole"], 8123456)
        self.assertEqual(holdings[1]["put_call"], "Call")

    def test_choose_information_table_document_prefers_info_xml(self):
        document = ThirteenFProcessor.choose_information_table_document(
            [
                {"name": "primary_doc.xml", "type": "13F-HR"},
                {"name": "form13fInfoTable.xml", "type": "INFORMATION TABLE"},
                {"name": "xslForm13F_X01.xml", "type": "XML"},
            ]
        )

        self.assertEqual(document, "form13fInfoTable.xml")


def _holding(issuer: str, cusip: str, value: int, shares: int = 0,
             put_call: str | None = None, title: str = "COM") -> dict:
    return {
        "issuer": issuer,
        "title_of_class": title,
        "cusip": cusip,
        "value_thousands": value // 1000 if value else 0,
        "value": value,
        "shares": shares,
        "share_type": "SH",
        "put_call": put_call,
    }


class QuarterChangesTests(unittest.TestCase):
    def test_categorizes_new_increased_reduced_and_sold_out(self):
        prior = [
            _holding("APPLE INC", "037833100", 1_000_000_000, shares=100_000),
            _holding("TESLA INC", "88160R101", 500_000_000, shares=50_000),
            _holding("MICROSOFT", "594918104", 700_000_000, shares=70_000),
            _holding("META", "30303M102", 300_000_000, shares=30_000),  # will be sold out
        ]
        current = [
            _holding("APPLE INC", "037833100", 1_200_000_000, shares=120_000),  # increased
            _holding("TESLA INC", "88160R101", 200_000_000, shares=20_000),     # reduced
            _holding("MICROSOFT", "594918104", 700_000_000, shares=70_000),     # unchanged
            _holding("NVIDIA", "67066G104", 800_000_000, shares=80_000),        # new
        ]
        diff = ThirteenFProcessor.compute_quarter_changes(prior, current)
        self.assertEqual([row["issuer"] for row in diff["new_positions"]], ["NVIDIA"])
        self.assertEqual([row["issuer"] for row in diff["increased"]], ["APPLE INC"])
        self.assertEqual([row["issuer"] for row in diff["reduced"]], ["TESLA INC"])
        self.assertEqual([row["issuer"] for row in diff["sold_out"]], ["META"])
        self.assertEqual(diff["unchanged_count"], 1)

        nvidia = diff["new_positions"][0]
        self.assertEqual(nvidia["prior_value"], 0)
        self.assertEqual(nvidia["current_value"], 800_000_000)
        self.assertEqual(nvidia["value_change"], 800_000_000)

        meta = diff["sold_out"][0]
        self.assertEqual(meta["current_value"], 0)
        self.assertEqual(meta["prior_value"], 300_000_000)
        self.assertEqual(meta["value_change"], -300_000_000)

    def test_totals_include_turnover_and_top10_concentration(self):
        prior = [_holding("ACME", "AAA", 1_000_000_000)]
        current = [
            _holding("ACME", "AAA", 600_000_000),
            _holding("BETA", "BBB", 400_000_000),
        ]
        diff = ThirteenFProcessor.compute_quarter_changes(prior, current)
        self.assertEqual(diff["totals"]["prior_value"], 1_000_000_000)
        self.assertEqual(diff["totals"]["current_value"], 1_000_000_000)
        self.assertEqual(diff["totals"]["value_change"], 0)
        # Reduced ACME by 400m + new BETA 400m = 800m gross / 1B prior = 0.8
        self.assertAlmostEqual(diff["totals"]["turnover_pct"], 0.8)
        # Only two positions → top10 == 100%
        self.assertEqual(diff["totals"]["top10_concentration"], 1.0)

    def test_distinguishes_call_and_put_holdings_with_same_cusip(self):
        prior: list[dict] = []
        current = [
            _holding("ACME", "AAA", 100_000_000, put_call="Call"),
            _holding("ACME", "AAA", 50_000_000, put_call="Put"),
        ]
        diff = ThirteenFProcessor.compute_quarter_changes(prior, current)
        self.assertEqual(len(diff["new_positions"]), 2)
        put_calls = {row["put_call"] for row in diff["new_positions"]}
        self.assertEqual(put_calls, {"CALL", "PUT"})

    def test_handles_empty_prior_quarter(self):
        current = [_holding("ACME", "AAA", 100_000_000)]
        diff = ThirteenFProcessor.compute_quarter_changes([], current)
        self.assertEqual(len(diff["new_positions"]), 1)
        self.assertEqual(diff["totals"]["prior_value"], 0)
        self.assertIsNone(diff["totals"]["turnover_pct"])

    def test_handles_both_quarters_empty(self):
        diff = ThirteenFProcessor.compute_quarter_changes([], [])
        for bucket in ("new_positions", "increased", "reduced", "sold_out"):
            self.assertEqual(diff[bucket], [])
        self.assertEqual(diff["unchanged_count"], 0)
        self.assertEqual(diff["totals"]["prior_value"], 0)
        self.assertIsNone(diff["totals"]["turnover_pct"])
        self.assertIsNone(diff["totals"]["top10_concentration"])


class RecordingDocumentHandler:
    """Captures get_filing_documents_list / download_form_document calls so we can
    assert that ThirteenFProcessor threads the manager CIK through."""

    def __init__(self, info_table_xml: str):
        self.info_table_xml = info_table_xml
        self.list_calls: list[dict] = []
        self.download_calls: list[dict] = []

    async def get_filing_documents_list(self, accession_no, ticker=None, cik=None):
        self.list_calls.append({"accession_no": accession_no, "ticker": ticker, "cik": cik})
        return [{"name": "form13fInfoTable.xml", "type": "INFORMATION TABLE", "size": "1000"}]

    async def download_form_document(self, accession_no, document_name, ticker=None, cik=None):
        self.download_calls.append({
            "accession_no": accession_no, "document_name": document_name,
            "ticker": ticker, "cik": cik,
        })
        return self.info_table_xml


class CIKThreadingTests(unittest.IsolatedAsyncioTestCase):
    """Regression: when 13F filings are submitted by a filer-agent (e.g. Donnelley),
    the accession-prefix CIK is wrong. ThirteenFProcessor must thread the manager
    CIK through the document handler so the archive URL points to the actual filer."""

    async def _build_processor_with_recording_handler(self, info_table_xml: str):
        from copetech_sec.thirteenf_processor import ThirteenFProcessor

        recording = RecordingDocumentHandler(info_table_xml)
        # http_client and cache_manager are only used for the submissions JSON path,
        # which we sidestep by stubbing get_13f_filings on the processor instance.
        processor = ThirteenFProcessor.__new__(ThirteenFProcessor)
        processor.http_client = None
        processor.cache_manager = None
        processor.document_handler = recording

        async def fake_get_filings(cik, *, days_back, use_cache):
            return [
                {
                    "accession_no": "0001193125-26-054580",
                    "filing_date": "2026-04-30",
                    "form": "13F-HR",
                    "report_date": "2026-03-31",
                    "url": "x",
                    "primary_document": None,
                    "primary_document_description": None,
                },
                {
                    "accession_no": "0001193125-26-001234",
                    "filing_date": "2026-01-30",
                    "form": "13F-HR",
                    "report_date": "2025-12-31",
                    "url": "x",
                    "primary_document": None,
                    "primary_document_description": None,
                },
            ]

        async def fake_get_manager_name(cik, use_cache=True):
            return "Test Manager"

        processor.get_13f_filings = fake_get_filings
        processor._get_manager_name = fake_get_manager_name
        return processor, recording

    async def test_get_latest_13f_holdings_passes_manager_cik(self):
        processor, recording = await self._build_processor_with_recording_handler(SAMPLE_INFORMATION_TABLE)

        await processor.get_latest_13f_holdings("0001067983")

        # The manager CIK must be passed to the document handler — otherwise the
        # accession prefix (1193125 = Donnelley) becomes the wrong archive root.
        self.assertEqual(len(recording.list_calls), 1)
        self.assertEqual(recording.list_calls[0]["cik"], "0001067983")
        self.assertEqual(recording.download_calls[0]["cik"], "0001067983")

    async def test_get_holdings_changes_passes_manager_cik_for_both_filings(self):
        processor, recording = await self._build_processor_with_recording_handler(SAMPLE_INFORMATION_TABLE)

        await processor.get_holdings_changes("0001067983")

        # Both the current and prior filings need the explicit cik
        self.assertEqual(len(recording.list_calls), 2)
        for call in recording.list_calls:
            self.assertEqual(call["cik"], "0001067983")
        for call in recording.download_calls:
            self.assertEqual(call["cik"], "0001067983")


class CIKPrecedenceTests(unittest.IsolatedAsyncioTestCase):
    """_get_cik_for_filing: explicit cik > ticker > accession prefix (unsafe)."""

    async def _build_handler(self, ticker_to_cik: dict[str, str]):
        from copetech_sec.document_handler import FilingDocumentHandler

        async def fake_lookup(ticker: str):
            return ticker_to_cik.get(ticker.upper())

        handler = FilingDocumentHandler.__new__(FilingDocumentHandler)
        handler.http_client = None
        handler.get_cik_for_ticker = fake_lookup
        return handler

    async def test_explicit_cik_takes_priority_over_ticker(self):
        handler = await self._build_handler({"AAPL": "0000320193"})
        # Explicit cik for a 13F should win even if ticker would resolve to something else
        result = await handler._get_cik_for_filing("0001193125-26-054580", ticker="AAPL", cik="0001067983")
        self.assertEqual(result, "1067983")

    async def test_ticker_used_when_no_explicit_cik(self):
        handler = await self._build_handler({"AAPL": "0000320193"})
        result = await handler._get_cik_for_filing("0001193125-26-054580", ticker="AAPL")
        self.assertEqual(result, "320193")

    async def test_accession_prefix_is_last_resort(self):
        handler = await self._build_handler({})
        result = await handler._get_cik_for_filing("0001193125-26-054580")
        # Donnelley's CIK from the accession prefix — wrong for the actual filer,
        # but we surface it as a last-resort fallback (with a logged warning).
        self.assertEqual(result, "1193125")

    async def test_returns_none_when_nothing_resolves(self):
        handler = await self._build_handler({})
        result = await handler._get_cik_for_filing("non-numeric-accession")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
