#!/usr/bin/env python3
"""Tests for Form144Processor.

Covers the XML parser and the high-level `get_planned_insider_sales` orchestrator
with stubbed document_handler + filings fetch (no network).
"""

from __future__ import annotations

import unittest
from typing import Any

from copetech_sec.form144_processor import Form144Processor


SAMPLE_FORM144_XML = """<?xml version="1.0"?>
<edgarSubmission>
  <schemaVersion>X0101</schemaVersion>
  <submissionType>144</submissionType>
  <issuerInfo>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
    <nameOfPersonForWhoseAccount>Tim Cook</nameOfPersonForWhoseAccount>
  </issuerInfo>
  <relationshipWithIssuer>
    <isOfficer>1</isOfficer>
    <officerTitle>CEO</officerTitle>
    <isDirector>1</isDirector>
    <isTenPercentOwner>0</isTenPercentOwner>
    <isOther>0</isOther>
  </relationshipWithIssuer>
  <securitiesToBeSoldInfo>
    <secsToBeSold>
      <securitiesClassTitleOfIssuer>Common Stock</securitiesClassTitleOfIssuer>
      <brokerName>Morgan Stanley</brokerName>
      <noOfUnitsSold>10000</noOfUnitsSold>
      <aggregateMarketValue>1750000.00</aggregateMarketValue>
      <noOfUnitsOutstanding>15000000000</noOfUnitsOutstanding>
      <approxSaleDate>2026-05-15</approxSaleDate>
      <natOfSecuritiesExchange>NASDAQ</natOfSecuritiesExchange>
      <acquiredFromIssuer>1</acquiredFromIssuer>
    </secsToBeSold>
  </securitiesToBeSoldInfo>
  <securitiesSoldInPast3MonthsInfo>
    <securitiesSoldInPast3Months>
      <saleDate>2026-04-15</saleDate>
      <amountOfSecuritiesSold>5000</amountOfSecuritiesSold>
      <grossProceeds>875000</grossProceeds>
    </securitiesSoldInPast3Months>
  </securitiesSoldInPast3MonthsInfo>
  <signature>
    <signature>Tim Cook</signature>
    <signatureDate>2026-05-10</signatureDate>
  </signature>
</edgarSubmission>
"""


MULTI_BLOCK_FORM144_XML = """<?xml version="1.0"?>
<edgarSubmission>
  <issuerInfo>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
    <nameOfPersonForWhoseAccount>Jane Roe</nameOfPersonForWhoseAccount>
  </issuerInfo>
  <relationshipWithIssuer>
    <isOfficer>0</isOfficer>
    <isDirector>1</isDirector>
    <isTenPercentOwner>0</isTenPercentOwner>
  </relationshipWithIssuer>
  <securitiesToBeSoldInfo>
    <secsToBeSold>
      <securitiesClassTitleOfIssuer>Common Stock</securitiesClassTitleOfIssuer>
      <noOfUnitsSold>1000</noOfUnitsSold>
      <aggregateMarketValue>175000</aggregateMarketValue>
    </secsToBeSold>
    <secsToBeSold>
      <securitiesClassTitleOfIssuer>Class B Common Stock</securitiesClassTitleOfIssuer>
      <noOfUnitsSold>500</noOfUnitsSold>
      <aggregateMarketValue>87500</aggregateMarketValue>
    </secsToBeSold>
  </securitiesToBeSoldInfo>
  <signature>
    <signature>Jane Roe</signature>
    <signatureDate>2026-05-08</signatureDate>
  </signature>
</edgarSubmission>
"""


class FakeDocumentHandler:
    def __init__(self, xml_by_accession: dict[str, str]):
        self.xml_by_accession = xml_by_accession
        self.download_calls: list[tuple[str, str | None]] = []

    async def download_form_xml(self, accession_no: str, ticker: str | None = None) -> str | None:
        self.download_calls.append((accession_no, ticker))
        return self.xml_by_accession.get(accession_no)


class Form144ParseTests(unittest.TestCase):
    def setUp(self):
        self.processor = Form144Processor(
            document_handler=FakeDocumentHandler({}),
            fetch_filings_func=self._unused,
        )

    @staticmethod
    async def _unused(*args: Any, **kwargs: Any) -> list[dict]:
        return []

    def test_parse_extracts_core_fields(self):
        records = self.processor.parse_form144_xml(SAMPLE_FORM144_XML)
        self.assertEqual(len(records), 1)
        record = records[0]

        self.assertEqual(record["issuer_cik"], "0000320193")
        self.assertEqual(record["issuer_name"], "Apple Inc.")
        self.assertEqual(record["issuer_symbol"], "AAPL")
        self.assertEqual(record["account_name"], "Tim Cook")
        self.assertEqual(record["signer"], "Tim Cook")
        self.assertEqual(record["signature_date"], "2026-05-10")
        self.assertEqual(record["security_class"], "Common Stock")
        self.assertEqual(record["broker_name"], "Morgan Stanley")
        self.assertEqual(record["planned_shares"], 10000)
        self.assertEqual(record["aggregate_market_value"], 1_750_000.0)
        self.assertEqual(record["shares_outstanding"], 15_000_000_000)
        self.assertEqual(record["approx_sale_date"], "2026-05-15")
        self.assertEqual(record["exchange"], "NASDAQ")
        self.assertTrue(record["acquired_from_issuer"])

    def test_parse_computes_implied_price(self):
        record = self.processor.parse_form144_xml(SAMPLE_FORM144_XML)[0]
        # 1_750_000 / 10000 = 175.0 per share
        self.assertEqual(record["implied_price_per_share"], 175.0)

    def test_parse_extracts_relationship_with_officer_title(self):
        record = self.processor.parse_form144_xml(SAMPLE_FORM144_XML)[0]
        self.assertEqual(record["relationship"], "Officer (CEO), Director")

    def test_parse_extracts_recent_sales(self):
        record = self.processor.parse_form144_xml(SAMPLE_FORM144_XML)[0]
        self.assertEqual(len(record["recent_sales"]), 1)
        prior = record["recent_sales"][0]
        self.assertEqual(prior["sale_date"], "2026-04-15")
        self.assertEqual(prior["shares_sold"], 5000)
        self.assertEqual(prior["gross_proceeds"], 875_000.0)

    def test_parse_returns_record_per_secsToBeSold_block(self):
        records = self.processor.parse_form144_xml(MULTI_BLOCK_FORM144_XML)
        self.assertEqual(len(records), 2)
        classes = {record["security_class"] for record in records}
        self.assertEqual(classes, {"Common Stock", "Class B Common Stock"})
        for record in records:
            self.assertEqual(record["account_name"], "Jane Roe")
            self.assertEqual(record["relationship"], "Director")

    def test_parse_returns_empty_list_on_malformed_xml(self):
        self.assertEqual(self.processor.parse_form144_xml("<not xml"), [])

    def test_parse_returns_empty_list_when_no_secsToBeSold(self):
        xml = """<?xml version="1.0"?>
        <edgarSubmission>
          <issuerInfo>
            <issuerCik>0001</issuerCik>
            <issuerName>Empty</issuerName>
          </issuerInfo>
        </edgarSubmission>"""
        self.assertEqual(self.processor.parse_form144_xml(xml), [])


class Form144OrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_planned_insider_sales_aggregates_records(self):
        async def fetch_filings(ticker: str, *, days_back: int, use_cache: bool) -> list[dict]:
            return [
                {
                    "accession_no": "ACC-1",
                    "filing_date": "2026-05-10",
                    "url": "https://example.com/acc-1",
                },
                {
                    "accession_no": "ACC-2",
                    "filing_date": "2026-05-08",
                    "url": "https://example.com/acc-2",
                },
            ]

        document_handler = FakeDocumentHandler(
            {
                "ACC-1": SAMPLE_FORM144_XML,
                "ACC-2": MULTI_BLOCK_FORM144_XML,
            }
        )
        processor = Form144Processor(
            document_handler=document_handler,
            fetch_filings_func=fetch_filings,
        )

        payload = await processor.get_planned_insider_sales("aapl", days_back=180, filing_limit=10)

        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["window"], {"days_back": 180, "filing_limit": 10})
        self.assertEqual(len(payload["records"]), 3)  # 1 + 2 blocks

        totals = payload["totals"]
        self.assertEqual(totals["record_count"], 3)
        self.assertEqual(totals["planned_shares"], 10_000 + 1_000 + 500)
        self.assertEqual(totals["aggregate_market_value"], 1_750_000.0 + 175_000 + 87_500)
        self.assertEqual(totals["unique_filers"], 2)

        # Newest signature_date comes first (2026-05-10 > 2026-05-08)
        self.assertEqual(payload["records"][0]["signature_date"], "2026-05-10")

    async def test_filing_with_missing_xml_is_skipped(self):
        async def fetch_filings(ticker: str, *, days_back: int, use_cache: bool) -> list[dict]:
            return [{"accession_no": "MISSING-XML", "filing_date": "2026-05-01", "url": "https://x"}]

        document_handler = FakeDocumentHandler(xml_by_accession={})
        processor = Form144Processor(
            document_handler=document_handler,
            fetch_filings_func=fetch_filings,
        )

        payload = await processor.get_planned_insider_sales("AAPL")
        self.assertEqual(payload["records"], [])
        self.assertEqual(payload["totals"]["record_count"], 0)

    async def test_filing_without_accession_is_skipped(self):
        async def fetch_filings(ticker: str, *, days_back: int, use_cache: bool) -> list[dict]:
            return [{"filing_date": "2026-05-01"}]  # no accession_no

        processor = Form144Processor(
            document_handler=FakeDocumentHandler({}),
            fetch_filings_func=fetch_filings,
        )
        payload = await processor.get_planned_insider_sales("AAPL")
        self.assertEqual(payload["records"], [])


if __name__ == "__main__":
    unittest.main()
