from __future__ import annotations

import tempfile
import unittest
from unittest.mock import AsyncMock, Mock

from copetech_sec.cache_manager import SecCacheManager
from copetech_sec.resources.funds import FundsResource
from copetech_sec.resources.ownership import OwnershipResource


class FundHttp:
    def __init__(self):
        self.calls = 0

    async def make_request(self, url: str, *, is_json: bool):
        self.calls += 1
        return {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [
                [2110, "S000009184", "C000024954", "LACAX"],
                [2110, "S000009184", "C000024956", "LIACX"],
            ],
        }


class ExpansionResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_fund_directory_preserves_series_and_share_classes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = Mock()
            fetcher.http_client = FundHttp()
            fetcher.cache_manager = SecCacheManager(tmpdir)
            resource = FundsResource(fetcher)

            fund = await resource.get("lacax")
            classes = await resource.for_cik("2110")

        self.assertEqual(fund["cik"], "0000002110")
        self.assertEqual(fund["series_id"], "S000009184")
        self.assertEqual(fund["class_id"], "C000024954")
        self.assertEqual([row["ticker"] for row in classes], ["LACAX", "LIACX"])
        self.assertEqual(fetcher.http_client.calls, 1)

    async def test_ownership_entries_discovers_forms_3_4_5_and_keeps_holdings(self):
        fetcher = Mock()
        fetcher.get_filings_page = AsyncMock(
            return_value={
                "items": [
                    {
                        "accession_no": "0000320193-26-000001",
                        "filing_date": "2026-07-01",
                        "report_date": "2026-06-30",
                        "form": "3",
                        "url": "https://example.test/filing/",
                    },
                    {
                        "accession_no": "0000320193-26-000002",
                        "filing_date": "2026-07-02",
                        "report_date": "2026-06-30",
                        "form": "5",
                        "url": "https://example.test/filing-2/",
                    },
                ],
                "metadata": {"warnings": []},
            }
        )
        fetcher.form4_processor.process_form4_filing = AsyncMock(
            side_effect=[
                [{"document_type": "3", "is_holding": True, "shares": 100.0}],
                [{"document_type": "5", "is_holding": False, "shares": 10.0}],
            ]
        )
        resource = OwnershipResource(fetcher)

        payload = await resource.entries(
            "AAPL",
            forms=["3", "3/A", "5", "5/A"],
        )

        self.assertEqual(len(payload["entries"]), 2)
        self.assertTrue(payload["entries"][0]["is_holding"])
        self.assertEqual(payload["entries"][0]["filing_form"], "3")
        self.assertEqual(
            payload["entries"][1]["accession_no"],
            "0000320193-26-000002",
        )
        fetcher.get_filings_page.assert_awaited_once()

    async def test_ownership_resource_rejects_non_ownership_form(self):
        resource = OwnershipResource(Mock())
        with self.assertRaises(ValueError):
            await resource.filings("AAPL", forms=["10-K"])
